//! Ordered snapshot aggregation for durable S3 watermarks (P0.4).
//!
//! Parallel snapshot workers may finish chunks out of order. A durable snapshot
//! watermark must represent a **contiguous durable prefix per table**, never the
//! maximum cursor observed - otherwise a later chunk completing before an earlier
//! one would make recovery treat the missing range as durable (silent loss).
//!
//! [`TableFrontier`] therefore buffers out-of-order completed chunks and advances
//! a table's frontier only when every preceding range is complete. [`SnapshotAggregator`]
//! owns one frontier per table plus the full vector; it is the single owner of
//! that state, so building the vector and emitting the boundary is one serialized
//! operation - a later chunk can never leak into an earlier boundary.
//! [`SnapshotPublisher`] wraps the aggregator and the output channel behind one
//! async lock, so a blocked channel send holds the lock and no other worker can
//! advance its frontier or enqueue a later boundary ahead of it.
//!
//! Chunks are half-open ranges `[start, end)` that abut (each chunk's `start` is
//! the previous chunk's `end`); the frontier is the exclusive upper bound of the
//! contiguous durable prefix (also the resume cursor). A gap stalls it (fail-safe).

use std::collections::BTreeMap;

use deltaforge_core::{CheckpointMeta, SourceBoundary};

use crate::durable_checkpoint::{
    CursorKind, DurableWatermark, SnapshotCursor, WmPos,
};
use crate::snapshot_generation::PersistedLineage;

/// A single table's contiguous durable frontier, built from possibly-out-of-order
/// completed chunks. All cursors are one [`SnapshotCursor`] kind (fixed at
/// construction); the derived `Ord` gives correct within-kind ordering.
#[derive(Debug, Clone)]
pub struct TableFrontier {
    /// Exclusive upper bound of the contiguous durable prefix: every row with
    /// cursor `< frontier` is durable. Also the resume cursor.
    frontier: SnapshotCursor,
    /// Completed chunks not yet contiguous with the frontier, keyed by `start`
    /// (value = `end`, half-open).
    pending: BTreeMap<SnapshotCursor, SnapshotCursor>,
    completed: bool,
}

impl TableFrontier {
    pub fn new(start: SnapshotCursor) -> Self {
        Self {
            frontier: start,
            pending: BTreeMap::new(),
            completed: false,
        }
    }

    pub fn frontier(&self) -> SnapshotCursor {
        self.frontier
    }

    pub fn completed(&self) -> bool {
        self.completed
    }

    /// Record a completed half-open chunk `[start, end)`. The frontier advances
    /// only through chunks that abut it (one starting exactly at `frontier`);
    /// non-contiguous chunks are buffered until the gap fills. A chunk of a
    /// different cursor kind than the frontier is rejected (never mixed). Returns
    /// `true` if the frontier advanced.
    pub fn complete_chunk(
        &mut self,
        start: SnapshotCursor,
        end: SnapshotCursor,
    ) -> bool {
        if start.kind() != self.frontier.kind()
            || end.kind() != self.frontier.kind()
            || end <= start
        {
            return false;
        }
        // Buffer (keep the widest range for a given start).
        let e = self.pending.entry(start).or_insert(end);
        *e = (*e).max(end);

        let before = self.frontier;
        // Cascade through abutting buffered ranges.
        while let Some(next_end) = self.pending.remove(&self.frontier) {
            if next_end > self.frontier {
                self.frontier = next_end;
            }
        }
        self.frontier > before
    }

    /// Mark the table complete. Only valid once its frontier has absorbed every
    /// range (no pending gaps remain): completion is explicit and never implied
    /// by a high cursor.
    pub fn try_complete(&mut self) -> bool {
        if self.pending.is_empty() {
            self.completed = true;
            true
        } else {
            false
        }
    }
}

/// Owns the per-table frontiers and the full snapshot vector, and emits a
/// [`SourceBoundary`] (checkpoint + durable watermark) each time a frontier
/// advances. Single owner = the aggregation task; building the vector and
/// producing the boundary is one serialized step.
pub struct SnapshotAggregator {
    generation: u64,
    lineage: PersistedLineage,
    /// The frozen CDC checkpoint captured at snapshot start - constant across the
    /// snapshot (post-snapshot CDC resumes from here); only the watermark moves.
    snapshot_checkpoint: CheckpointMeta,
    tables: BTreeMap<String, TableFrontier>,
}

impl SnapshotAggregator {
    /// Start a generation over `tables`, each `(name, cursor kind)`; every
    /// frontier begins at its kind's minimum so the vector's key set and cursor
    /// kinds are fixed from the first batch.
    pub fn new(
        generation: u64,
        lineage: PersistedLineage,
        snapshot_checkpoint: CheckpointMeta,
        tables: &[(String, CursorKind)],
    ) -> Self {
        Self {
            generation,
            lineage,
            snapshot_checkpoint,
            tables: tables
                .iter()
                .map(|(t, k)| (t.clone(), TableFrontier::new(k.min())))
                .collect(),
        }
    }

    /// Rebuild from a stored snapshot watermark (restart init) BEFORE any worker
    /// emits, so progress resumes from the verified HEAD vector.
    pub fn restore_from(
        generation: u64,
        lineage: PersistedLineage,
        snapshot_checkpoint: CheckpointMeta,
        head: &DurableWatermark,
    ) -> Option<Self> {
        let WmPos::Snapshot {
            generation: g,
            table_cursors,
            ..
        } = &head.pos
        else {
            return None;
        };
        if *g != generation {
            return None;
        }
        Some(Self {
            generation,
            lineage,
            snapshot_checkpoint,
            tables: table_cursors
                .iter()
                .map(|(t, c)| (t.clone(), TableFrontier::new(*c)))
                .collect(),
        })
    }

    fn fully_completed(&self) -> bool {
        !self.tables.is_empty()
            && self.tables.values().all(TableFrontier::completed)
    }

    /// Snapshot the complete current vector into a boundary.
    fn boundary(&self) -> SourceBoundary {
        let table_cursors: BTreeMap<String, SnapshotCursor> = self
            .tables
            .iter()
            .map(|(t, f)| (t.clone(), f.frontier()))
            .collect();
        let wm = DurableWatermark::new(
            self.lineage.clone(),
            WmPos::Snapshot {
                generation: self.generation,
                completed: self.fully_completed(),
                table_cursors,
            },
        );
        SourceBoundary {
            checkpoint: self.snapshot_checkpoint.clone(),
            durable_watermark: Some(std::sync::Arc::from(wm.to_bytes())),
        }
    }

    /// Record a completed half-open chunk `[start, end)` for `table`. Returns a
    /// boundary (full vector) ONLY when the table's contiguous frontier advanced -
    /// so an out-of-order later chunk never produces a boundary that covers rows
    /// before its preceding ranges are durable.
    pub fn complete_chunk(
        &mut self,
        table: &str,
        start: SnapshotCursor,
        end: SnapshotCursor,
    ) -> Option<SourceBoundary> {
        let f = self.tables.get_mut(table)?;
        if f.complete_chunk(start, end) {
            Some(self.boundary())
        } else {
            None
        }
    }

    /// Mark a table complete (all its ranges incorporated). Returns a boundary if
    /// this completed the whole snapshot (enabling snapshot->CDC ordering).
    pub fn complete_table(&mut self, table: &str) -> Option<SourceBoundary> {
        let f = self.tables.get_mut(table)?;
        if !f.try_complete() {
            return None;
        }
        self.fully_completed().then(|| self.boundary())
    }

    pub fn is_complete(&self) -> bool {
        self.fully_completed()
    }
}

/// The output channel closed (coordinator gone / shutdown).
#[derive(Debug)]
pub struct SnapshotClosed;

impl std::fmt::Display for SnapshotClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("snapshot output channel closed")
    }
}
impl std::error::Error for SnapshotClosed {}

/// The single owner of the [`SnapshotAggregator`] and the output channel. All
/// parallel table workers publish their completed chunks through it. Advancing a
/// frontier, snapshotting the full vector into a boundary, and enqueuing the
/// chunk's events happen under ONE async lock, so:
///
/// - a boundary always reflects a full vector consistent across tables, and
/// - a blocked channel send holds the lock, so no other worker can advance its
///   frontier or enqueue a later boundary ahead of the blocked chunk (later
///   progress can never appear in an earlier boundary).
///
/// Workers may scan concurrently; only publication is serialized here.
pub struct SnapshotPublisher {
    inner: tokio::sync::Mutex<PublisherInner>,
}

struct PublisherInner {
    agg: SnapshotAggregator,
    tx: tokio::sync::mpsc::Sender<deltaforge_core::SourceItem>,
}

impl SnapshotPublisher {
    pub fn new(
        agg: SnapshotAggregator,
        tx: tokio::sync::mpsc::Sender<deltaforge_core::SourceItem>,
    ) -> Self {
        Self {
            inner: tokio::sync::Mutex::new(PublisherInner { agg, tx }),
        }
    }

    /// Publish one completed, contiguous chunk `[start, end)` of `table`: attach
    /// the advanced boundary (the full vector) to the chunk's LAST event, then
    /// send every event - all under one lock. If the chunk did not advance the
    /// contiguous frontier (an out-of-order arrival), its events still flow but
    /// carry no boundary, so no watermark ever covers a not-yet-durable range.
    pub async fn publish_chunk(
        &self,
        table: &str,
        start: SnapshotCursor,
        end: SnapshotCursor,
        mut events: Vec<deltaforge_core::Event>,
    ) -> Result<(), SnapshotClosed> {
        let mut g = self.inner.lock().await;
        if let Some(boundary) = g.agg.complete_chunk(table, start, end) {
            if let Some(last) = events.last_mut() {
                last.set_boundary(boundary);
            }
        }
        for ev in events {
            g.tx.send(deltaforge_core::SourceItem::Event(ev))
                .await
                .map_err(|_| SnapshotClosed)?;
        }
        Ok(())
    }

    /// Mark `table`'s scan complete (all its ranges incorporated). Returns the
    /// completed-snapshot boundary once every table is complete, for the caller to
    /// record before the snapshot->CDC transition. Serialized with publication.
    pub async fn complete_table(&self, table: &str) -> Option<SourceBoundary> {
        let mut g = self.inner.lock().await;
        g.agg.complete_table(table)
    }

    /// Whether the whole snapshot is complete.
    pub async fn is_complete(&self) -> bool {
        self.inner.lock().await.agg.is_complete()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lineage() -> PersistedLineage {
        PersistedLineage::Postgres {
            system_identifier: 1,
        }
    }

    fn agg(tables: &[&str]) -> SnapshotAggregator {
        let t: Vec<(String, CursorKind)> = tables
            .iter()
            .map(|s| (s.to_string(), CursorKind::Unsigned))
            .collect();
        SnapshotAggregator::new(
            1,
            lineage(),
            CheckpointMeta::from_vec(b"snap-start".to_vec()),
            &t,
        )
    }

    /// Test cursor constructor (all frontier tests use the unsigned domain; the
    /// signed domain is covered by durable_checkpoint's comparator tests).
    fn u(v: u64) -> SnapshotCursor {
        SnapshotCursor::Unsigned(v)
    }

    /// Extract table -> unsigned cursor value from a boundary's watermark.
    fn cursors(b: &SourceBoundary) -> BTreeMap<String, u64> {
        let wm = DurableWatermark::parse(b.durable_watermark.as_ref().unwrap())
            .unwrap();
        match wm.pos {
            WmPos::Snapshot { table_cursors, .. } => table_cursors
                .into_iter()
                .map(|(t, c)| match c {
                    SnapshotCursor::Unsigned(v) => (t, v),
                    other => panic!("expected unsigned cursor, got {other:?}"),
                })
                .collect(),
            _ => panic!("expected snapshot"),
        }
    }

    #[test]
    fn frontier_advances_only_through_contiguous_ranges() {
        let mut f = TableFrontier::new(u(0));
        assert!(f.complete_chunk(u(0), u(10)));
        assert_eq!(f.frontier(), u(10));
        // A gap: chunk [20,30) is buffered, frontier does not move.
        assert!(!f.complete_chunk(u(20), u(30)));
        assert_eq!(f.frontier(), u(10));
        // Filling the gap cascades through both buffered ranges.
        assert!(f.complete_chunk(u(10), u(20)));
        assert_eq!(f.frontier(), u(30));
    }

    #[test]
    fn adversarial_completion_order_3_1_2_never_skips_a_range() {
        // Chunks [0,10)=1, [10,20)=2, [20,30)=3 complete in order 3, 1, 2.
        // Emitted watermarks must advance only 1, then 1+2+3 - never counting
        // chunk 3 before chunks 1 and 2 are durable.
        let mut a = agg(&["orders"]);

        // Chunk 3 first: NO boundary (frontier cannot advance past the gap).
        assert!(
            a.complete_chunk("orders", u(20), u(30)).is_none(),
            "chunk 3 out of order must not advance the frontier"
        );

        // Chunk 1: frontier -> 10.
        let b1 = a
            .complete_chunk("orders", u(0), u(10))
            .expect("advance to 10");
        assert_eq!(cursors(&b1)["orders"], 10);

        // Chunk 2: cascades [10,20) then the buffered [20,30) -> 30.
        let b2 = a
            .complete_chunk("orders", u(10), u(20))
            .expect("advance to 30");
        assert_eq!(cursors(&b2)["orders"], 30);

        // At no point did a boundary report a cursor of 30 before chunks 1+2.
        // (b1 == 10, b2 == 30; there was no boundary after the lone chunk 3.)
    }

    #[test]
    fn later_chunk_never_carries_rows_beyond_the_watermark() {
        // A boundary's cursor is always the contiguous frontier, so every row it
        // implies covered (< cursor) is genuinely durable.
        let mut a = agg(&["orders"]);
        a.complete_chunk("orders", u(50), u(60)); // buffered, no boundary
        a.complete_chunk("orders", u(30), u(40)); // buffered, no boundary
        let b = a.complete_chunk("orders", u(0), u(30)); // frontier -> 40 ([0,30),[30,40))
        let c = cursors(&b.unwrap());
        assert_eq!(
            c["orders"], 40,
            "[50,60) stays buffered until [40,50) lands"
        );
    }

    #[test]
    fn parallel_tables_have_independent_frontiers() {
        let mut a = agg(&["orders", "users"]);
        let b1 = a.complete_chunk("orders", u(0), u(10)).unwrap();
        assert_eq!(cursors(&b1)["orders"], 10);
        assert_eq!(cursors(&b1)["users"], 0);
        let b2 = a.complete_chunk("users", u(0), u(5)).unwrap();
        assert_eq!(cursors(&b2)["orders"], 10);
        assert_eq!(cursors(&b2)["users"], 5);
    }

    #[test]
    fn completion_is_explicit_and_gated_on_no_gaps() {
        let mut a = agg(&["orders"]);
        a.complete_chunk("orders", u(20), u(30)); // gap remains
        // Cannot complete while a gap is buffered.
        assert!(a.complete_table("orders").is_none());
        assert!(!a.is_complete());
        a.complete_chunk("orders", u(0), u(20)); // fills the gap -> frontier 30
        let done = a.complete_table("orders").expect("snapshot complete");
        assert!(a.is_complete());
        // The completed boundary's watermark is marked completed.
        let wm =
            DurableWatermark::parse(done.durable_watermark.as_ref().unwrap())
                .unwrap();
        match wm.pos {
            WmPos::Snapshot { completed, .. } => assert!(completed),
            _ => panic!(),
        }
    }

    #[test]
    fn full_completion_requires_all_tables() {
        let mut a = agg(&["orders", "users"]);
        a.complete_chunk("orders", u(0), u(10));
        a.complete_chunk("users", u(0), u(10));
        // Completing one table does not complete the snapshot.
        assert!(a.complete_table("orders").is_none());
        assert!(!a.is_complete());
        let done = a.complete_table("users").expect("all complete");
        assert!(a.is_complete());
        let wm =
            DurableWatermark::parse(done.durable_watermark.as_ref().unwrap())
                .unwrap();
        assert!(matches!(
            wm.pos,
            WmPos::Snapshot {
                completed: true,
                ..
            }
        ));
    }

    #[test]
    fn restart_resumes_from_head_vector_and_continues_monotonically() {
        // Simulate a prior run that reached orders=100, users=50.
        let mut prior = agg(&["orders", "users"]);
        prior.complete_chunk("orders", u(0), u(100));
        let head_b = prior.complete_chunk("users", u(0), u(50)).unwrap();
        let head =
            DurableWatermark::parse(head_b.durable_watermark.as_ref().unwrap())
                .unwrap();

        // Restart: rebuild from the verified HEAD vector before emitting.
        let mut resumed = SnapshotAggregator::restore_from(
            1,
            lineage(),
            CheckpointMeta::from_vec(b"snap-start".to_vec()),
            &head,
        )
        .unwrap();
        // A next contiguous chunk continues from the restored frontier (100).
        let b = resumed.complete_chunk("orders", u(100), u(200)).unwrap();
        assert_eq!(cursors(&b)["orders"], 200);
        assert_eq!(cursors(&b)["users"], 50);
    }

    #[test]
    fn boundary_checkpoint_is_the_frozen_snapshot_checkpoint() {
        let mut a = agg(&["orders"]);
        let b = a.complete_chunk("orders", u(0), u(10)).unwrap();
        assert_eq!(b.checkpoint.as_bytes(), b"snap-start");
    }

    // ── SnapshotPublisher: the aggregation owner (live-worker concurrency) ──────

    use deltaforge_core::{Event, EventId, SourceInfo, SourceItem};

    fn snap_event(id: u32) -> Event {
        let source = SourceInfo {
            version: "t".into(),
            connector: "mysql".into(),
            name: "t".into(),
            db: "shop".into(),
            schema: None,
            table: "orders".into(),
            ts_ms: 0,
            snapshot: Some("true".into()),
            position: Default::default(),
        };
        Event::new_snapshot(
            EventId::mysql_row_server(1, "orders", 1, id),
            source,
            serde_json::json!({ "id": id }),
            0,
            0,
        )
    }

    fn publisher(
        tables: &[&str],
        cap: usize,
    ) -> (
        std::sync::Arc<SnapshotPublisher>,
        tokio::sync::mpsc::Receiver<SourceItem>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::channel(cap);
        let p = SnapshotPublisher::new(agg(tables), tx);
        (std::sync::Arc::new(p), rx)
    }

    /// Drain currently-available items without blocking, returning the boundary
    /// cursor of the last event that carried one (if any).
    fn drain_last_cursor(
        rx: &mut tokio::sync::mpsc::Receiver<SourceItem>,
    ) -> Option<BTreeMap<String, u64>> {
        let mut last = None;
        while let Ok(item) = rx.try_recv() {
            if let SourceItem::Event(e) = item {
                if let Some(b) = e.boundary.as_ref() {
                    last = Some(cursors(b));
                }
            }
        }
        last
    }

    #[tokio::test]
    async fn publisher_out_of_order_chunks_emit_only_contiguous_boundaries() {
        // A single table's chunks [0,10),[10,20),[20,30) are published in the
        // adversarial order 3, 1, 2. The boundaries that reach the channel must
        // advance only 1 then 1+2+3 - chunk 3 alone must carry NO boundary.
        let (p, mut rx) = publisher(&["shop.orders"], 64);

        // Chunk 3 out of order: events flow but carry no boundary.
        p.publish_chunk("shop.orders", u(20), u(30), vec![snap_event(20)])
            .await
            .unwrap();
        assert!(
            drain_last_cursor(&mut rx).is_none(),
            "out-of-order chunk 3 must not emit a boundary"
        );

        // Chunk 1: boundary advances to 10.
        p.publish_chunk("shop.orders", u(0), u(10), vec![snap_event(0)])
            .await
            .unwrap();
        assert_eq!(drain_last_cursor(&mut rx).unwrap()["shop.orders"], 10);

        // Chunk 2: cascades through the buffered chunk 3 -> 30.
        p.publish_chunk("shop.orders", u(10), u(20), vec![snap_event(10)])
            .await
            .unwrap();
        assert_eq!(drain_last_cursor(&mut rx).unwrap()["shop.orders"], 30);
    }

    #[tokio::test]
    async fn publisher_blocked_channel_prevents_later_progress_in_an_earlier_boundary()
     {
        // Capacity 1: a first worker's chunk fills the channel and blocks on its
        // second event while holding the lock. A second worker (another table)
        // must NOT be able to advance its frontier and enqueue a boundary until
        // the first chunk drains - proving later progress can't leak ahead.
        let (p, mut rx) = publisher(&["shop.orders", "shop.users"], 1);

        let p1 = std::sync::Arc::clone(&p);
        let worker1 = tokio::spawn(async move {
            // Two events, channel cap 1 -> the send of the 2nd blocks until a
            // consumer frees a slot, all while holding the publisher lock.
            p1.publish_chunk(
                "shop.orders",
                u(0),
                u(10),
                vec![snap_event(1), snap_event(2)],
            )
            .await
        });

        // Give worker1 time to take the lock and block on the second send.
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let p2 = std::sync::Arc::clone(&p);
        let worker2 = tokio::spawn(async move {
            p2.publish_chunk("shop.users", u(0), u(5), vec![snap_event(3)])
                .await
        });

        // worker2 cannot make progress while worker1 holds the lock.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !worker2.is_finished(),
            "worker2 must be blocked by the lock"
        );

        // Drain the channel: worker1 unblocks, finishes, releases the lock, then
        // worker2 proceeds.
        let mut seen = Vec::new();
        while seen.len() < 3 {
            if let Some(SourceItem::Event(e)) = rx.recv().await {
                seen.push(e);
            }
        }
        worker1.await.unwrap().unwrap();
        worker2.await.unwrap().unwrap();

        // The users boundary (cursor 5) can only have been produced after the
        // orders chunk was fully enqueued: the users event is the LAST of the 3.
        let users_last = seen.last().unwrap();
        let b = users_last.boundary.as_ref().expect("users boundary");
        let c = cursors(b);
        assert_eq!(c["shop.users"], 5);
        assert_eq!(
            c["shop.orders"], 10,
            "orders progress was durable before the users boundary"
        );
    }

    #[tokio::test]
    async fn publisher_parallel_tables_reach_completion() {
        let (p, mut rx) = publisher(&["shop.orders", "shop.users"], 64);
        p.publish_chunk("shop.orders", u(0), u(10), vec![snap_event(1)])
            .await
            .unwrap();
        p.publish_chunk("shop.users", u(0), u(10), vec![snap_event(2)])
            .await
            .unwrap();
        assert!(p.complete_table("shop.orders").await.is_none());
        let done = p.complete_table("shop.users").await.expect("all done");
        assert!(p.is_complete().await);
        let wm =
            DurableWatermark::parse(done.durable_watermark.as_ref().unwrap())
                .unwrap();
        assert!(matches!(
            wm.pos,
            WmPos::Snapshot {
                completed: true,
                ..
            }
        ));
        // Drain so the channel does not report closed early.
        while rx.try_recv().is_ok() {}
    }
}

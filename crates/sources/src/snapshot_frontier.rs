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

    /// An already-durable (done) table restored from the source checkpoint: its
    /// frontier is the kind's maximum and it is complete. The source checkpoint
    /// records only table-level completion, not a per-table cursor.
    fn completed_at_max(kind: CursorKind) -> Self {
        Self {
            frontier: kind.max(),
            pending: BTreeMap::new(),
            completed: true,
        }
    }
}

/// One table's state when restoring the source snapshot vector from the
/// authoritative source/coordinator checkpoint (never from any sink's HEAD).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableResume {
    pub kind: CursorKind,
    /// The source checkpoint records this table as fully snapshotted.
    pub done: bool,
}

/// Converting a table-level source checkpoint to the per-table vector failed
/// because it is an interrupted legacy snapshot: some tables are done and some
/// are not, but the legacy format kept no per-table cursor for the unfinished
/// ones. Such a snapshot cannot be adopted by durable mode without possibly
/// claiming durability the sink never received.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AmbiguousLegacySnapshot;

impl std::fmt::Display for AmbiguousLegacySnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "in-progress legacy snapshot cannot be converted to a durable \
             source vector without ambiguity; finish it under legacy mode or \
             start a new snapshot generation",
        )
    }
}
impl std::error::Error for AmbiguousLegacySnapshot {}

/// Whether a table-level source checkpoint is an interrupted legacy snapshot
/// (some tables done, some pending, not finished) - the ambiguous case durable
/// startup must reject. Names-only, so a source can check it without resolving
/// cursor kinds. See [`convert_legacy_progress`] for the full conversion and
/// [`LegacyProgressScan`] to check discovered tables page by page.
pub fn is_ambiguous_legacy_progress(
    all_tables: &[String],
    done_tables: &[String],
    finished: bool,
) -> bool {
    let done: std::collections::HashSet<&str> =
        done_tables.iter().map(String::as_str).collect();
    let mut scan = LegacyProgressScan::default();
    for t in all_tables {
        scan.observe(done.contains(t.as_str()));
    }
    scan.ambiguous(finished)
}

/// [`is_ambiguous_legacy_progress`] over tables seen one at a time (the
/// expanded tables of a paged discovery): whether any is done and any is
/// pending.
#[derive(Debug, Default, Clone, Copy)]
pub struct LegacyProgressScan {
    any_done: bool,
    any_pending: bool,
}

impl LegacyProgressScan {
    pub fn observe(&mut self, done: bool) {
        if done {
            self.any_done = true;
        } else {
            self.any_pending = true;
        }
    }

    /// Both seen: the answer can no longer change.
    pub fn settled(&self) -> bool {
        self.any_done && self.any_pending
    }

    pub fn ambiguous(&self, finished: bool) -> bool {
        !finished && self.settled()
    }
}

/// Convert a legacy table-level source checkpoint (`done_tables` + `finished`)
/// into the per-table resume states for the source vector. This reads the
/// SOURCE's own progress - never any sink's HEAD, so the source is never
/// fast-forwarded by how far one sink happens to be durable. `finished` (all
/// tables done) or a clean start (nothing done) converts unambiguously; an
/// interrupted legacy snapshot is [`AmbiguousLegacySnapshot`] and the caller
/// (durable startup) must fail closed.
pub fn convert_legacy_progress(
    tables: &[(String, CursorKind)],
    done_tables: &[String],
    finished: bool,
) -> Result<Vec<(String, TableResume)>, AmbiguousLegacySnapshot> {
    let names: Vec<String> = tables.iter().map(|(t, _)| t.clone()).collect();
    if is_ambiguous_legacy_progress(&names, done_tables, finished) {
        return Err(AmbiguousLegacySnapshot);
    }
    Ok(tables
        .iter()
        .map(|(t, kind)| {
            let done = finished || done_tables.contains(t);
            (t.clone(), TableResume { kind: *kind, done })
        })
        .collect())
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
    /// The checkpoint of the boundary that completes the snapshot (an
    /// ordinary resume position at the anchor), carried only by
    /// [`SnapshotPublisher::finish`]; every other boundary carries
    /// `snapshot_checkpoint`, an incomplete-snapshot position.
    completing_checkpoint: Option<CheckpointMeta>,
    tables: BTreeMap<String, TableFrontier>,
}

impl SnapshotAggregator {
    /// The checkpoint the completing boundary carries.
    pub fn with_completing_checkpoint(mut self, cp: CheckpointMeta) -> Self {
        self.completing_checkpoint = Some(cp);
        self
    }

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
            completing_checkpoint: None,
            tables: tables
                .iter()
                .map(|(t, k)| (t.clone(), TableFrontier::new(k.min())))
                .collect(),
        }
    }

    /// Build the source vector from the authoritative source/coordinator
    /// checkpoint (via [`convert_legacy_progress`]). A table the source records as
    /// done starts complete at its kind's maximum; a pending table starts at the
    /// minimum for a fresh scan. This NEVER reads a sink's HEAD, so the source is
    /// not fast-forwarded by how far one sink is durable - each sink recovers its
    /// own cursor independently and its comparator skips already-durable replay.
    pub fn from_source_progress(
        generation: u64,
        lineage: PersistedLineage,
        snapshot_checkpoint: CheckpointMeta,
        resume: &[(String, TableResume)],
    ) -> Self {
        Self {
            generation,
            lineage,
            snapshot_checkpoint,
            completing_checkpoint: None,
            tables: resume
                .iter()
                .map(|(t, r)| {
                    let f = if r.done {
                        TableFrontier::completed_at_max(r.kind)
                    } else {
                        TableFrontier::new(r.kind.min())
                    };
                    (t.clone(), f)
                })
                .collect(),
        }
    }

    fn fully_completed(&self) -> bool {
        !self.tables.is_empty()
            && self.tables.values().all(TableFrontier::completed)
    }

    /// Snapshot the complete current vector into a boundary (its `completed` flag
    /// reflects whether every table is done).
    pub fn current_boundary(&self) -> SourceBoundary {
        let started = std::time::Instant::now();
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
        let bytes = wm.to_bytes();
        crate::snapshot_probe::record_boundary(bytes.len(), started.elapsed());
        SourceBoundary {
            checkpoint: self.snapshot_checkpoint.clone(),
            durable_watermark: Some(std::sync::Arc::from(bytes)),
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
            Some(self.current_boundary())
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
        self.fully_completed().then(|| self.current_boundary())
    }

    pub fn is_complete(&self) -> bool {
        self.fully_completed()
    }

    /// The boundary completing the snapshot: the completing checkpoint and
    /// the full vector, every table complete. `None` unless both hold.
    fn completing_boundary(&self) -> Option<SourceBoundary> {
        let cp = self.completing_checkpoint.clone()?;
        self.fully_completed().then(|| SourceBoundary {
            checkpoint: cp,
            ..self.current_boundary()
        })
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
    /// The last event published, held back so the boundary that completes
    /// the snapshot can ride on it ([`SnapshotPublisher::finish`]): it is
    /// sent before any later event, or by `finish`. A later boundary
    /// replaces its own (every range it covers was sent before it).
    held: Option<deltaforge_core::Event>,
}

impl PublisherInner {
    async fn send(
        &self,
        item: deltaforge_core::SourceItem,
    ) -> Result<(), SnapshotClosed> {
        self.tx.send(item).await.map_err(|_| SnapshotClosed)
    }

    /// Send the held event, if any.
    async fn release(&mut self) -> Result<(), SnapshotClosed> {
        match self.held.take() {
            Some(ev) => self.send(deltaforge_core::SourceItem::Event(ev)).await,
            None => Ok(()),
        }
    }

    /// Attach `boundary` to the held event, or send it on its own when
    /// nothing is held.
    async fn emit(
        &mut self,
        boundary: SourceBoundary,
    ) -> Result<(), SnapshotClosed> {
        match self.held.as_mut() {
            Some(ev) => {
                ev.set_boundary(boundary);
                Ok(())
            }
            None => {
                self.send(deltaforge_core::SourceItem::Boundary { boundary })
                    .await
            }
        }
    }
}

impl SnapshotPublisher {
    pub fn new(
        agg: SnapshotAggregator,
        tx: tokio::sync::mpsc::Sender<deltaforge_core::SourceItem>,
    ) -> Self {
        Self {
            inner: tokio::sync::Mutex::new(PublisherInner {
                agg,
                tx,
                held: None,
            }),
        }
    }

    /// Publish one completed, contiguous chunk `[start, end)` of `table`: attach
    /// the advanced boundary (the full vector) to the chunk's LAST event, then
    /// send every event - all under one lock. If the chunk did not advance the
    /// contiguous frontier (an out-of-order arrival), its events still flow but
    /// carry no boundary, so no watermark ever covers a not-yet-durable range.
    /// The chunk's last event is held back (sent before the next chunk's events
    /// or by [`Self::finish`]).
    pub async fn publish_chunk(
        &self,
        table: &str,
        start: SnapshotCursor,
        end: SnapshotCursor,
        mut events: Vec<deltaforge_core::Event>,
    ) -> Result<(), SnapshotClosed> {
        let mut g = self.inner.lock().await;
        let boundary = g.agg.complete_chunk(table, start, end);
        let Some(mut last) = events.pop() else {
            // Nothing to carry it: a later boundary covers this range.
            if let Some(boundary) = boundary {
                if g.held.is_some() {
                    g.emit(boundary).await?;
                }
            }
            return Ok(());
        };
        g.release().await?;
        for ev in events {
            g.send(deltaforge_core::SourceItem::Event(ev)).await?;
        }
        if let Some(boundary) = boundary {
            last.set_boundary(boundary);
        }
        g.held = Some(last);
        Ok(())
    }

    /// Mark `table`'s scan complete (all its ranges incorporated) and emit a
    /// table-complete boundary carrying the current full vector (on the held
    /// event, or on its own). When this completes the whole snapshot nothing
    /// is emitted: only [`Self::finish`] emits the completing boundary. The
    /// send is under the lock (ordered with publication). Returns whether the
    /// whole snapshot is now complete.
    pub async fn complete_table(
        &self,
        table: &str,
    ) -> Result<bool, SnapshotClosed> {
        let mut g = self.inner.lock().await;
        g.agg.complete_table(table);
        if g.agg.is_complete() {
            return Ok(true);
        }
        let boundary = g.agg.current_boundary();
        g.emit(boundary).await?;
        Ok(false)
    }

    /// Emit the boundary that completes the snapshot - its completing
    /// checkpoint - on the held event (the globally last non-empty chunk's
    /// last event), or on its own when no event is held. Call only after
    /// every table finished successfully and every final check passed; a
    /// snapshot that fails before this never sends a completing checkpoint.
    pub async fn finish(&self) -> Result<(), SnapshotFinishError> {
        let mut g = self.inner.lock().await;
        let boundary = g
            .agg
            .completing_boundary()
            .ok_or(SnapshotFinishError::Incomplete)?;
        g.emit(boundary).await?;
        g.release().await?;
        Ok(())
    }

    /// Whether the whole snapshot is complete.
    pub async fn is_complete(&self) -> bool {
        self.inner.lock().await.agg.is_complete()
    }
}

/// Why [`SnapshotPublisher::finish`] could not complete the snapshot.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotFinishError {
    /// A table is not complete, or no completing checkpoint was set.
    #[error("the snapshot is not complete")]
    Incomplete,
    #[error(transparent)]
    Closed(#[from] SnapshotClosed),
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
    fn unsigned_frontier_crosses_i64_max_to_u64_max() {
        // Unsigned cursors above i64::MAX order and advance correctly (they never
        // pass through a signed cast).
        let mut a = agg(&["t"]);
        let mid = i64::MAX as u64; // 2^63 - 1
        let b1 = a.complete_chunk("t", u(0), u(mid + 1)).unwrap();
        assert_eq!(cursors(&b1)["t"], mid + 1);
        let b2 = a.complete_chunk("t", u(mid + 1), u(u64::MAX)).unwrap();
        assert_eq!(cursors(&b2)["t"], u64::MAX);
    }

    #[test]
    fn unsigned_out_of_order_preserves_frontier_above_i64_max() {
        // The high chunk (above i64::MAX) completes first: buffered until the low
        // chunk fills the gap, then the frontier cascades to u64::MAX.
        let mut a = agg(&["t"]);
        let mid = i64::MAX as u64;
        assert!(a.complete_chunk("t", u(mid + 1), u(u64::MAX)).is_none());
        let b = a.complete_chunk("t", u(0), u(mid + 1)).unwrap();
        assert_eq!(cursors(&b)["t"], u64::MAX);
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
    fn from_source_progress_done_tables_complete_pending_fresh() {
        // Per-table resume states known unambiguously (e.g. a durable resume):
        // `orders` done, `users` pending. `orders` starts complete at its kind's
        // max; `users` starts fresh at min and is scanned this run. NOTHING here
        // reads a sink HEAD.
        let resume = vec![
            (
                "orders".to_string(),
                TableResume {
                    kind: CursorKind::Unsigned,
                    done: true,
                },
            ),
            (
                "users".to_string(),
                TableResume {
                    kind: CursorKind::Unsigned,
                    done: false,
                },
            ),
        ];
        let mut a = SnapshotAggregator::from_source_progress(
            1,
            lineage(),
            CheckpointMeta::from_vec(b"snap-start".to_vec()),
            &resume,
        );
        // orders is already complete; users completes after its scan.
        assert!(!a.is_complete());
        let b = a.complete_chunk("users", u(0), u(50)).unwrap();
        assert_eq!(cursors(&b)["users"], 50);
        assert_eq!(
            cursors(&b)["orders"],
            u64::MAX,
            "done table sits at the kind's max"
        );
        assert!(a.complete_table("users").is_some());
        assert!(a.is_complete(), "all tables complete");
    }

    #[test]
    fn is_ambiguous_legacy_progress_only_for_interrupted() {
        let all = vec!["a".to_string(), "b".to_string()];
        // Interrupted: one done, one pending, not finished.
        assert!(is_ambiguous_legacy_progress(
            &all,
            &["a".to_string()],
            false
        ));
        // Finished: never ambiguous.
        assert!(!is_ambiguous_legacy_progress(
            &all,
            &["a".to_string()],
            true
        ));
        // Clean start: nothing done.
        assert!(!is_ambiguous_legacy_progress(&all, &[], false));
        // All done but not marked finished: not ambiguous (no pending).
        assert!(!is_ambiguous_legacy_progress(
            &all,
            &["a".to_string(), "b".to_string()],
            false
        ));
    }

    #[test]
    fn convert_legacy_progress_rejects_interrupted_snapshot() {
        // finished = false with one done and one pending = interrupted legacy
        // snapshot: unconvertible for durable mode (fail closed).
        let err = convert_legacy_progress(
            &[
                ("orders".to_string(), CursorKind::Signed),
                ("users".to_string(), CursorKind::Signed),
            ],
            &["orders".to_string()],
            false,
        );
        assert_eq!(err, Err(AmbiguousLegacySnapshot));

        // finished = true (all done) converts to an all-complete vector.
        let ok = convert_legacy_progress(
            &[("orders".to_string(), CursorKind::Signed)],
            &["orders".to_string()],
            true,
        )
        .unwrap();
        assert!(ok[0].1.done);

        // Clean start (nothing done) converts to all-pending.
        let fresh = convert_legacy_progress(
            &[("orders".to_string(), CursorKind::Signed)],
            &[],
            false,
        )
        .unwrap();
        assert!(!fresh[0].1.done);
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
        let p = SnapshotPublisher::new(
            agg(tables).with_completing_checkpoint(done_checkpoint()),
            tx,
        );
        (std::sync::Arc::new(p), rx)
    }

    /// The completing checkpoint the test publishers carry.
    fn done_checkpoint() -> CheckpointMeta {
        CheckpointMeta::from_vec(b"done".to_vec())
    }

    /// Publisher over tables of a given cursor kind (PG: Signed or CtidBlock).
    fn publisher_kind(
        tables: &[&str],
        cap: usize,
        kind: CursorKind,
    ) -> (
        std::sync::Arc<SnapshotPublisher>,
        tokio::sync::mpsc::Receiver<SourceItem>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::channel(cap);
        let t: Vec<(String, CursorKind)> =
            tables.iter().map(|s| (s.to_string(), kind)).collect();
        let a = SnapshotAggregator::new(
            1,
            lineage(),
            CheckpointMeta::from_vec(b"0/1A2B3C".to_vec()),
            &t,
        )
        .with_completing_checkpoint(done_checkpoint());
        (std::sync::Arc::new(SnapshotPublisher::new(a, tx)), rx)
    }

    /// The last boundary drained, as its typed cursor for `table`.
    fn drain_last_typed(
        rx: &mut tokio::sync::mpsc::Receiver<SourceItem>,
        table: &str,
    ) -> Option<SnapshotCursor> {
        let mut last = None;
        while let Ok(item) = rx.try_recv() {
            if let SourceItem::Event(e) = item {
                if let Some(b) = e.boundary.as_ref() {
                    let wm = DurableWatermark::parse(
                        b.durable_watermark.as_ref().unwrap(),
                    )
                    .unwrap();
                    if let WmPos::Snapshot { table_cursors, .. } = wm.pos {
                        last = table_cursors.get(table).copied();
                    }
                }
            }
        }
        last
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
        // The last event published is held until the next chunk or `finish`.
        let (p, mut rx) = publisher(&["shop.orders"], 64);

        // Chunk 3 out of order: its event (held) carries no boundary.
        p.publish_chunk("shop.orders", u(20), u(30), vec![snap_event(20)])
            .await
            .unwrap();
        assert!(drain_last_cursor(&mut rx).is_none());

        // Chunk 1: boundary 10 on its (held) event; chunk 3's event is sent
        // without one.
        p.publish_chunk("shop.orders", u(0), u(10), vec![snap_event(0)])
            .await
            .unwrap();
        assert!(
            drain_last_cursor(&mut rx).is_none(),
            "out-of-order chunk 3 must not emit a boundary"
        );

        // Chunk 2 releases chunk 1's event (boundary 10) and cascades through
        // the buffered chunk 3 -> 30 on its own held event.
        p.publish_chunk("shop.orders", u(10), u(20), vec![snap_event(10)])
            .await
            .unwrap();
        assert_eq!(drain_last_cursor(&mut rx).unwrap()["shop.orders"], 10);

        assert!(p.complete_table("shop.orders").await.unwrap());
        assert!(
            drain_last_cursor(&mut rx).is_none(),
            "nothing before finish"
        );
        p.finish().await.unwrap();
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
                vec![snap_event(1), snap_event(2), snap_event(4)],
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
        // worker2 proceeds (its event held until `finish`).
        let mut seen = Vec::new();
        let drain = async {
            while seen.len() < 4 {
                if let Some(SourceItem::Event(e)) = rx.recv().await {
                    seen.push(e);
                }
            }
        };
        let finish = async {
            worker1.await.unwrap().unwrap();
            worker2.await.unwrap().unwrap();
            assert!(!p.complete_table("shop.orders").await.unwrap());
            assert!(p.complete_table("shop.users").await.unwrap());
            p.finish().await.unwrap();
        };
        tokio::join!(drain, finish);

        // The users boundary (cursor 5) can only have been produced after the
        // orders chunk was fully enqueued: the users event is the LAST of the 4.
        let users_last = seen.last().unwrap();
        let b = users_last.boundary.as_ref().expect("users boundary");
        let c = cursors(b);
        assert_eq!(c["shop.users"], 5);
        assert_eq!(
            c["shop.orders"], 10,
            "orders progress was durable before the users boundary"
        );
    }

    /// Extract the boundary carried by the last `SourceItem::Boundary` drained.
    fn drain_last_boundary(
        rx: &mut tokio::sync::mpsc::Receiver<SourceItem>,
    ) -> Option<SourceBoundary> {
        let mut last = None;
        while let Ok(item) = rx.try_recv() {
            if let SourceItem::Boundary { boundary } = item {
                last = Some(boundary);
            }
        }
        last
    }

    /// The boundary on the last event drained.
    fn drain_last_event_boundary(
        rx: &mut tokio::sync::mpsc::Receiver<SourceItem>,
    ) -> Option<SourceBoundary> {
        let mut last = None;
        while let Ok(item) = rx.try_recv() {
            if let SourceItem::Event(e) = item {
                last = e.boundary.clone();
            }
        }
        last
    }

    fn completed(b: &SourceBoundary) -> bool {
        matches!(
            DurableWatermark::parse(b.durable_watermark.as_ref().unwrap())
                .unwrap()
                .pos,
            WmPos::Snapshot {
                completed: true,
                ..
            }
        )
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
        // Completing one table moves the held event's boundary forward (not
        // `completed`); nothing is sent on its own.
        assert!(!p.complete_table("shop.orders").await.unwrap());
        let mut items = Vec::new();
        while let Ok(item) = rx.try_recv() {
            items.push(item);
        }
        assert!(
            !items
                .iter()
                .any(|i| matches!(i, SourceItem::Boundary { .. })),
            "no boundary on its own while an event is held"
        );
        let first = match items.last() {
            Some(SourceItem::Event(e)) => {
                e.boundary.clone().expect("first chunk")
            }
            _ => panic!("the first chunk's event"),
        };
        assert!(!completed(&first));

        // Completing the last table sends nothing: completion waits for
        // `finish`, which puts the completing checkpoint on the held event.
        assert!(p.complete_table("shop.users").await.unwrap());
        assert!(p.is_complete().await);
        assert!(drain_last_event_boundary(&mut rx).is_none());
        p.finish().await.unwrap();
        let done = drain_last_event_boundary(&mut rx).expect("completing");
        assert!(completed(&done));
        assert_eq!(done.checkpoint.as_bytes(), done_checkpoint().as_bytes());
    }

    /// The completing checkpoint is never sent before every table is
    /// complete, and only by `finish`; a snapshot without any event completes
    /// on a boundary of its own.
    #[tokio::test]
    async fn only_finish_sends_the_completing_checkpoint() {
        let (p, mut rx) = publisher(&["shop.orders", "shop.users"], 64);
        p.publish_chunk("shop.orders", u(0), u(10), vec![snap_event(1)])
            .await
            .unwrap();
        assert!(!p.complete_table("shop.orders").await.unwrap());
        assert!(matches!(
            p.finish().await,
            Err(SnapshotFinishError::Incomplete)
        ));
        assert!(drain_last_event_boundary(&mut rx).is_none(), "still held");

        // Every table empty: no event to carry it.
        let (p, mut rx) = publisher(&["shop.orders", "shop.users"], 64);
        assert!(!p.complete_table("shop.orders").await.unwrap());
        assert!(p.complete_table("shop.users").await.unwrap());
        p.finish().await.unwrap();
        let done = drain_last_boundary(&mut rx).expect("completing boundary");
        assert!(completed(&done));
        assert_eq!(done.checkpoint.as_bytes(), done_checkpoint().as_bytes());
    }

    // ── PostgreSQL-flavoured coverage: signed integer-PK intra-table parallelism
    //    (negative min) and the ctid page-block frontier. Both drive the same
    //    SnapshotPublisher. ──────────────────────────────────────────────────────

    use SnapshotCursor::{CtidBlock, Signed};

    /// PG integer-PK intra-table parallelism: sub-ranges of a signed-PK table
    /// (min_pk negative) publish out of order. A vacuous prefix chunk abuts the
    /// frontier at min_pk; the frontier then advances only through contiguous
    /// ranges - a later sub-range never counts before an earlier one is durable.
    #[tokio::test]
    async fn publisher_pg_signed_intra_table_out_of_order() {
        let (p, mut rx) = publisher_kind(&["pg.t"], 64, CursorKind::Signed);

        // Vacuous prefix [i64::MIN, -50): advances the frontier to -50, no rows.
        p.publish_chunk("pg.t", Signed(i64::MIN), Signed(-50), vec![])
            .await
            .unwrap();

        // Sub-range B = [25, 100) completes before sub-range A: buffered (a gap
        // [-50, 25) remains), so no boundary is emitted.
        p.publish_chunk("pg.t", Signed(25), Signed(100), vec![snap_event(1)])
            .await
            .unwrap();
        assert_eq!(
            drain_last_typed(&mut rx, "pg.t"),
            None,
            "sub-range B before A must not advance past the gap"
        );

        // Sub-range A = [-50, 25) fills the gap and cascades into buffered B
        // (on A's held event, sent by `finish`).
        p.publish_chunk("pg.t", Signed(-50), Signed(25), vec![snap_event(2)])
            .await
            .unwrap();
        assert!(p.complete_table("pg.t").await.unwrap());
        p.finish().await.unwrap();
        assert_eq!(
            drain_last_typed(&mut rx, "pg.t"),
            Some(Signed(100)),
            "frontier cascades to 100 once [-50,25) lands"
        );
    }

    /// PG ctid page-block frontier: a blocked channel holds the publisher lock, so
    /// a second table's boundary cannot appear before the first chunk drains.
    #[tokio::test]
    async fn publisher_pg_ctid_blocked_channel() {
        let (p, mut rx) =
            publisher_kind(&["pg.a", "pg.b"], 1, CursorKind::CtidBlock);

        let p1 = std::sync::Arc::clone(&p);
        let w1 = tokio::spawn(async move {
            p1.publish_chunk(
                "pg.a",
                CtidBlock(0),
                CtidBlock(4),
                vec![snap_event(1), snap_event(2), snap_event(4)],
            )
            .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let p2 = std::sync::Arc::clone(&p);
        let w2 = tokio::spawn(async move {
            p2.publish_chunk(
                "pg.b",
                CtidBlock(0),
                CtidBlock(4),
                vec![snap_event(3)],
            )
            .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!w2.is_finished(), "w2 blocked by the held publisher lock");

        let mut seen = Vec::new();
        let drain = async {
            while seen.len() < 4 {
                if let Some(SourceItem::Event(e)) = rx.recv().await {
                    seen.push(e);
                }
            }
        };
        let finish = async {
            w1.await.unwrap().unwrap();
            w2.await.unwrap().unwrap();
            assert!(!p.complete_table("pg.a").await.unwrap());
            assert!(p.complete_table("pg.b").await.unwrap());
            p.finish().await.unwrap();
        };
        tokio::join!(drain, finish);

        // pg.b's boundary rides its last event, produced only after pg.a drained.
        let last = seen.last().unwrap();
        let b = last.boundary.as_ref().expect("pg.b boundary");
        let wm = DurableWatermark::parse(b.durable_watermark.as_ref().unwrap())
            .unwrap();
        match wm.pos {
            WmPos::Snapshot { table_cursors, .. } => {
                assert_eq!(table_cursors["pg.b"], CtidBlock(4));
                assert_eq!(
                    table_cursors["pg.a"],
                    CtidBlock(4),
                    "pg.a durable before pg.b's boundary"
                );
            }
            _ => panic!(),
        }
    }
}

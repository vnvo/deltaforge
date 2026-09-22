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
//! that state (the aggregation task), so building the vector and emitting the
//! boundary is one serialized operation - a later chunk can never leak into an
//! earlier boundary. Chunks tile the table by cursor (each chunk's `lo` is the
//! previous chunk's `hi + 1`); a gap simply stalls the frontier (fail-safe).

use std::collections::BTreeMap;

use deltaforge_core::{CheckpointMeta, SourceBoundary};

use crate::durable_checkpoint::{DurableWatermark, WmPos};
use crate::snapshot_generation::PersistedLineage;

/// A single table's contiguous durable frontier, built from possibly-out-of-order
/// completed chunks.
#[derive(Debug, Clone)]
pub struct TableFrontier {
    /// All rows with cursor `<= frontier` are contiguously durable.
    frontier: u64,
    /// Completed chunks not yet contiguous with the frontier, keyed by `lo`.
    pending: BTreeMap<u64, u64>,
    completed: bool,
}

impl TableFrontier {
    pub fn new(start: u64) -> Self {
        Self {
            frontier: start,
            pending: BTreeMap::new(),
            completed: false,
        }
    }

    pub fn frontier(&self) -> u64 {
        self.frontier
    }

    pub fn completed(&self) -> bool {
        self.completed
    }

    /// Record a completed chunk `[lo, hi]`. The frontier advances only through
    /// contiguous ranges starting at `frontier + 1`; non-contiguous chunks are
    /// buffered. Returns `true` if the frontier advanced.
    pub fn complete_chunk(&mut self, lo: u64, hi: u64) -> bool {
        if hi < lo {
            return false;
        }
        // Buffer (keep the widest range for a given lo).
        let e = self.pending.entry(lo).or_insert(hi);
        *e = (*e).max(hi);

        let before = self.frontier;
        // Cascade through contiguous buffered ranges.
        while let Some(&next_hi) = self.pending.get(&(self.frontier + 1)) {
            self.pending.remove(&(self.frontier + 1));
            if next_hi > self.frontier {
                self.frontier = next_hi;
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
    /// Start a generation over `tables` (all frontiers at 0).
    pub fn new(
        generation: u64,
        lineage: PersistedLineage,
        snapshot_checkpoint: CheckpointMeta,
        tables: &[String],
    ) -> Self {
        Self {
            generation,
            lineage,
            snapshot_checkpoint,
            tables: tables
                .iter()
                .map(|t| (t.clone(), TableFrontier::new(0)))
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
        let table_cursors: BTreeMap<String, u64> = self
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

    /// Record a completed chunk for `table`. Returns a boundary (full vector)
    /// ONLY when the table's contiguous frontier advanced - so an out-of-order
    /// later chunk never produces a boundary that covers rows before its
    /// preceding ranges are durable.
    pub fn complete_chunk(
        &mut self,
        table: &str,
        lo: u64,
        hi: u64,
    ) -> Option<SourceBoundary> {
        let f = self.tables.get_mut(table)?;
        if f.complete_chunk(lo, hi) {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn lineage() -> PersistedLineage {
        PersistedLineage::Postgres {
            system_identifier: 1,
        }
    }

    fn agg(tables: &[&str]) -> SnapshotAggregator {
        let t: Vec<String> = tables.iter().map(|s| s.to_string()).collect();
        SnapshotAggregator::new(
            1,
            lineage(),
            CheckpointMeta::from_vec(b"snap-start".to_vec()),
            &t,
        )
    }

    fn cursors(b: &SourceBoundary) -> BTreeMap<String, u64> {
        let wm = DurableWatermark::parse(b.durable_watermark.as_ref().unwrap())
            .unwrap();
        match wm.pos {
            WmPos::Snapshot { table_cursors, .. } => table_cursors,
            _ => panic!("expected snapshot"),
        }
    }

    #[test]
    fn frontier_advances_only_through_contiguous_ranges() {
        let mut f = TableFrontier::new(0);
        assert!(f.complete_chunk(1, 10));
        assert_eq!(f.frontier(), 10);
        // A gap: chunk [21,30] is buffered, frontier does not move.
        assert!(!f.complete_chunk(21, 30));
        assert_eq!(f.frontier(), 10);
        // Filling the gap cascades through both buffered ranges.
        assert!(f.complete_chunk(11, 20));
        assert_eq!(f.frontier(), 30);
    }

    #[test]
    fn adversarial_completion_order_3_1_2_never_skips_a_range() {
        // Chunks [1,10]=1, [11,20]=2, [21,30]=3 complete in order 3, 1, 2.
        // Emitted watermarks must advance only 1, then 1+2+3 - never counting
        // chunk 3 before chunks 1 and 2 are durable.
        let mut a = agg(&["orders"]);

        // Chunk 3 first: NO boundary (frontier cannot advance past the gap).
        assert!(
            a.complete_chunk("orders", 21, 30).is_none(),
            "chunk 3 out of order must not advance the frontier"
        );

        // Chunk 1: frontier -> 10.
        let b1 = a.complete_chunk("orders", 1, 10).expect("advance to 10");
        assert_eq!(cursors(&b1)["orders"], 10);

        // Chunk 2: cascades 11..20 then the buffered 21..30 -> 30.
        let b2 = a.complete_chunk("orders", 11, 20).expect("advance to 30");
        assert_eq!(cursors(&b2)["orders"], 30);

        // At no point did a boundary report a cursor of 30 before chunks 1+2.
        // (b1 == 10, b2 == 30; there was no boundary after the lone chunk 3.)
    }

    #[test]
    fn later_chunk_never_carries_rows_beyond_the_watermark() {
        // A boundary's cursor is always the contiguous frontier, so every row it
        // implies covered (<= cursor) is genuinely durable.
        let mut a = agg(&["orders"]);
        a.complete_chunk("orders", 51, 60); // buffered, no boundary
        a.complete_chunk("orders", 31, 40); // buffered, no boundary
        let b = a.complete_chunk("orders", 1, 30); // frontier -> 40 (1..30,31..40)
        let c = cursors(&b.unwrap());
        assert_eq!(c["orders"], 40, "51..60 stays buffered until 41..50 lands");
    }

    #[test]
    fn parallel_tables_have_independent_frontiers() {
        let mut a = agg(&["orders", "users"]);
        let b1 = a.complete_chunk("orders", 1, 10).unwrap();
        assert_eq!(cursors(&b1)["orders"], 10);
        assert_eq!(cursors(&b1)["users"], 0);
        let b2 = a.complete_chunk("users", 1, 5).unwrap();
        assert_eq!(cursors(&b2)["orders"], 10);
        assert_eq!(cursors(&b2)["users"], 5);
    }

    #[test]
    fn completion_is_explicit_and_gated_on_no_gaps() {
        let mut a = agg(&["orders"]);
        a.complete_chunk("orders", 21, 30); // gap remains
        // Cannot complete while a gap is buffered.
        assert!(a.complete_table("orders").is_none());
        assert!(!a.is_complete());
        a.complete_chunk("orders", 1, 20); // fills the gap -> frontier 30
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
        a.complete_chunk("orders", 1, 10);
        a.complete_chunk("users", 1, 10);
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
        prior.complete_chunk("orders", 1, 100);
        let head_b = prior.complete_chunk("users", 1, 50).unwrap();
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
        // A next contiguous chunk continues from the restored frontier.
        let b = resumed.complete_chunk("orders", 101, 200).unwrap();
        assert_eq!(cursors(&b)["orders"], 200);
        assert_eq!(cursors(&b)["users"], 50);
    }

    #[test]
    fn boundary_checkpoint_is_the_frozen_snapshot_checkpoint() {
        let mut a = agg(&["orders"]);
        let b = a.complete_chunk("orders", 1, 10).unwrap();
        assert_eq!(b.checkpoint.as_bytes(), b"snap-start");
    }
}

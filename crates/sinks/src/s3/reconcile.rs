//! Orphan reconciliation + operational metrics for the durable S3 path (P0.4,
//! Commit 10).
//!
//! Reconciliation is owner-fenced and reachability-driven: it deletes only objects
//! that are NOT reachable from the authoritative HEAD state and have outlived a
//! grace period. It never touches a HEAD-reachable entry, compaction record, rollup,
//! inventory, replacement, or uncompacted object; never a retained old rollup /
//! inventory generation; and never a GC mark. A referenced object that is MISSING is
//! a hard alarm, never a cleanup. It re-reads HEAD and revalidates ownership before
//! every destructive unit, and stops on epoch change, listing uncertainty, an
//! ambiguous provider response, or any integrity failure - so it is idempotent and
//! safely resumable after a crash.
//!
//! durable_v2 writes every object with a single create-only PUT (no multipart
//! uploads), so there are no in-flight MPUs to abort; `mpu_aborts` is reported for
//! completeness and is always 0. A provider-specific MPU sweep would be added here if
//! the write path ever adopts multipart uploads.

#![allow(dead_code)]

/// Configuration for a reconciliation pass.
#[derive(Debug, Clone, Copy)]
pub struct ReconcileConfig {
    /// Current wall-clock time (ms since epoch), injected for testability.
    pub now_ms: u64,
    /// An orphan is only deleted once its age exceeds this, so an object that was just
    /// created and is about to be referenced is never mistaken for an orphan.
    pub grace_period_ms: u64,
}

/// The outcome + operational metrics of one reconciliation pass.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Objects reachable from authoritative HEAD state (never orphans).
    pub referenced: usize,
    /// Objects seen in the listing.
    pub listed: usize,
    /// Referenced data (`/wm-` + `/compacted/`) objects - committed durable content.
    pub committed_objects: usize,
    /// Unreferenced objects deleted (past the grace period).
    pub orphans_deleted: usize,
    /// Unreferenced objects left in place because they are still within the grace
    /// period (or the backend reported no age).
    pub orphans_within_grace: usize,
    /// Aborted multipart uploads. Always 0 on the single-PUT durable_v2 path.
    pub mpu_aborts: usize,
    /// Referenced objects that were expected but ABSENT. Non-empty is a hard alarm;
    /// reconciliation stops and deletes nothing further.
    pub missing_referenced: Vec<String>,
    /// Acknowledged original objects not yet superseded by a compaction (GC backlog).
    pub compaction_lag: usize,
    /// Set when the pass stopped early (epoch change, listing uncertainty, ambiguous
    /// provider response, integrity failure). Reconciliation never deletes past this.
    pub stopped: Option<String>,
}

/// A cheap operational snapshot of the durable writer, for dashboards/tooling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableMetrics {
    pub epoch: u64,
    pub seq: u64,
    pub watermark_hex: Option<String>,
    /// Current / previous cumulative rollup end sequences (None if not yet rolled up).
    pub rollup_end_seq: Option<u64>,
    pub prev_rollup_end_seq: Option<u64>,
    /// The shared GC horizon (`prev_rollup.end_seq`, else 0).
    pub horizon_seq: u64,
    /// Active compaction records reachable from HEAD.
    pub compaction_records: usize,
    /// Acknowledged originals not yet superseded (compaction backlog).
    pub compaction_lag: usize,
    /// Cumulative HEAD CAS conflicts/retries observed by this writer.
    pub cas_conflicts: u64,
}

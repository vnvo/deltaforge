//! Test observability seam for snapshot discovery and preparation.
//!
//! Structural counters, incremented on the real snapshot path, so tests can
//! assert the shape of the work (catalog pages, schema resolutions, worker
//! concurrency) independent of timing. Process-wide: the snapshot suites run
//! single-threaded. Each update is one relaxed atomic operation.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

/// Operations a snapshot start performs a fixed number of times, whatever
/// the table count (per-page and per-table work is counted separately).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FixedOp {
    /// The catalog session discovery and preparation run on.
    CatalogSession,
    LineageCapture,
    GenerationAllocation,
    Preflight,
    /// The consistent anchor (PostgreSQL exported snapshot, MySQL read lock).
    Anchor,
    /// MySQL: the `binlog_row_image` check.
    RowImageCheck,
    /// MySQL: the re-discovery under the anchor's read lock.
    CatalogVerification,
}

const FIXED_OPS: usize = 7;
static FIXED: [AtomicU64; FIXED_OPS] = [const { AtomicU64::new(0) }; FIXED_OPS];
static VERIFICATION_QUERIES: AtomicU64 = AtomicU64::new(0);
static REGISTRY_READS: AtomicU64 = AtomicU64::new(0);
static DISCOVERY_MICROS: AtomicU64 = AtomicU64::new(0);
static PREPARATION_MICROS: AtomicU64 = AtomicU64::new(0);
static PLAN_BYTES: AtomicU64 = AtomicU64::new(0);
static FRONTIER_TABLES: AtomicU64 = AtomicU64::new(0);
static PREPARATION_LIVE_FETCHES: AtomicU64 = AtomicU64::new(0);

/// Coarse phases of a snapshot run (after preparation).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Preflight, the consistent anchor and the anchor's progress write.
    PreflightAnchor,
    /// Table workers running (row copying, with the progress and frontier
    /// writes that happen while they run).
    RowCopy,
    /// After the last table: final checks and the finished marker.
    Finalization,
}

const PHASES: usize = 3;
static PHASE_MICROS: [AtomicU64; PHASES] =
    [const { AtomicU64::new(0) }; PHASES];
static PROGRESS_WRITES: AtomicU64 = AtomicU64::new(0);
static PROGRESS_BYTES: AtomicU64 = AtomicU64::new(0);
static PROGRESS_MICROS: AtomicU64 = AtomicU64::new(0);
static BOUNDARIES: AtomicU64 = AtomicU64::new(0);
static OWNER_CHECKS: AtomicU64 = AtomicU64::new(0);
static BOUNDARY_BYTES: AtomicU64 = AtomicU64::new(0);
static BOUNDARY_MICROS: AtomicU64 = AtomicU64::new(0);

static DISCOVERY_QUERIES: AtomicU64 = AtomicU64::new(0);
static DISCOVERED_TABLES: AtomicU64 = AtomicU64::new(0);
static MAX_DISCOVERY_PAGE: AtomicU64 = AtomicU64::new(0);
static PREPARED_TABLES: AtomicU64 = AtomicU64::new(0);
static WORKER_LIVE_FETCHES: AtomicU64 = AtomicU64::new(0);
static MAX_TABLE_TASKS: AtomicU64 = AtomicU64::new(0);

/// What one snapshot start did, structurally.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SnapshotShape {
    /// Catalog queries issued by table discovery (one per page).
    pub discovery_queries: u64,
    /// Tables discovery produced (after the CDC matcher).
    pub discovered_tables: u64,
    /// Largest catalog page held at once.
    pub max_discovery_page: u64,
    /// Tables passed through the preparation pass.
    pub prepared_tables: u64,
    /// Live catalog schema fetches made by the snapshot run after preparation.
    pub worker_live_fetches: u64,
    /// Largest number of table tasks alive at once.
    pub max_table_tasks: u64,
    /// Fixed operations, by [`FixedOp`] (identical for any table count).
    pub fixed: [u64; FIXED_OPS],
    /// MySQL: catalog pages read again under the anchor's read lock.
    pub verification_queries: u64,
    /// Schema-registry reads during preparation (one per table at most).
    pub registry_reads: u64,
    /// Time spent in discovery catalog queries.
    pub discovery_micros: u64,
    /// Time of the whole preparation pass (discovery included).
    pub preparation_micros: u64,
    /// Approximate size of the compact plan (one entry per table).
    pub plan_bytes: u64,
    /// Tables in the snapshot frontier (one entry per table).
    pub frontier_tables: u64,
    /// Live catalog schema fetches during preparation (tables the registry
    /// did not know; at most one per table).
    pub preparation_live_fetches: u64,
    /// Wall time per [`Phase`].
    pub phase_micros: [u64; PHASES],
    /// Durable snapshot-progress writes: count, bytes written, time.
    pub progress_writes: u64,
    pub progress_bytes: u64,
    pub progress_micros: u64,
    /// Frontier boundaries built (each serializes the whole watermark):
    /// count, watermark bytes, time.
    pub boundaries: u64,
    pub boundary_bytes: u64,
    pub boundary_micros: u64,
    /// Control-record reads checking the publishing run still owns its
    /// generation (one before every publish: each chunk and each barrier).
    pub owner_checks: u64,
}

/// The counters since the last [`reset`].
pub fn shape() -> SnapshotShape {
    SnapshotShape {
        discovery_queries: DISCOVERY_QUERIES.load(Ordering::Relaxed),
        discovered_tables: DISCOVERED_TABLES.load(Ordering::Relaxed),
        max_discovery_page: MAX_DISCOVERY_PAGE.load(Ordering::Relaxed),
        prepared_tables: PREPARED_TABLES.load(Ordering::Relaxed),
        worker_live_fetches: WORKER_LIVE_FETCHES.load(Ordering::Relaxed),
        max_table_tasks: MAX_TABLE_TASKS.load(Ordering::Relaxed),
        fixed: std::array::from_fn(|i| FIXED[i].load(Ordering::Relaxed)),
        verification_queries: VERIFICATION_QUERIES.load(Ordering::Relaxed),
        registry_reads: REGISTRY_READS.load(Ordering::Relaxed),
        discovery_micros: DISCOVERY_MICROS.load(Ordering::Relaxed),
        preparation_micros: PREPARATION_MICROS.load(Ordering::Relaxed),
        plan_bytes: PLAN_BYTES.load(Ordering::Relaxed),
        frontier_tables: FRONTIER_TABLES.load(Ordering::Relaxed),
        preparation_live_fetches: PREPARATION_LIVE_FETCHES
            .load(Ordering::Relaxed),
        phase_micros: std::array::from_fn(|i| {
            PHASE_MICROS[i].load(Ordering::Relaxed)
        }),
        progress_writes: PROGRESS_WRITES.load(Ordering::Relaxed),
        progress_bytes: PROGRESS_BYTES.load(Ordering::Relaxed),
        progress_micros: PROGRESS_MICROS.load(Ordering::Relaxed),
        boundaries: BOUNDARIES.load(Ordering::Relaxed),
        boundary_bytes: BOUNDARY_BYTES.load(Ordering::Relaxed),
        boundary_micros: BOUNDARY_MICROS.load(Ordering::Relaxed),
        owner_checks: OWNER_CHECKS.load(Ordering::Relaxed),
    }
}

/// Reset every counter (call right before driving a source).
pub fn reset() {
    for c in [
        &DISCOVERY_QUERIES,
        &DISCOVERED_TABLES,
        &MAX_DISCOVERY_PAGE,
        &PREPARED_TABLES,
        &WORKER_LIVE_FETCHES,
        &MAX_TABLE_TASKS,
        &VERIFICATION_QUERIES,
        &REGISTRY_READS,
        &DISCOVERY_MICROS,
        &PREPARATION_MICROS,
        &PLAN_BYTES,
        &FRONTIER_TABLES,
        &PREPARATION_LIVE_FETCHES,
        &PROGRESS_WRITES,
        &PROGRESS_BYTES,
        &PROGRESS_MICROS,
        &BOUNDARIES,
        &BOUNDARY_BYTES,
        &BOUNDARY_MICROS,
        &OWNER_CHECKS,
    ] {
        c.store(0, Ordering::Relaxed);
    }
    for c in FIXED.iter().chain(PHASE_MICROS.iter()) {
        c.store(0, Ordering::Relaxed);
    }
}

pub(crate) fn record_fixed(op: FixedOp) {
    FIXED[op as usize].fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_verification_page() {
    VERIFICATION_QUERIES.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_registry_read() {
    REGISTRY_READS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_discovery_time(d: std::time::Duration) {
    DISCOVERY_MICROS.fetch_add(d.as_micros() as u64, Ordering::Relaxed);
}

pub(crate) fn record_preparation(
    d: std::time::Duration,
    plan_bytes: usize,
    live_fetches: u64,
) {
    PREPARATION_MICROS.fetch_add(d.as_micros() as u64, Ordering::Relaxed);
    PLAN_BYTES.fetch_add(plan_bytes as u64, Ordering::Relaxed);
    PREPARATION_LIVE_FETCHES.fetch_add(live_fetches, Ordering::Relaxed);
}

/// One armed hold after the first discovery page: `(reached, release)`.
static AFTER_DISCOVERY_PAGE: Mutex<Option<(Arc<Notify>, Arc<Notify>)>> =
    Mutex::new(None);

/// Arm a one-shot hold in the real discovery path: the next snapshot
/// preparation notifies `reached` after its first catalog page, then waits
/// for `release`. Test seam: lets a test change the catalog between pages.
pub fn hold_after_discovery_page() -> (Arc<Notify>, Arc<Notify>) {
    let gate = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    *AFTER_DISCOVERY_PAGE.lock().expect("discovery hold") = Some(gate.clone());
    gate
}

/// The hold point; a no-op unless a test armed it.
pub(crate) async fn after_discovery_page() {
    let gate = AFTER_DISCOVERY_PAGE.lock().expect("discovery hold").take();
    if let Some((reached, release)) = gate {
        reached.notify_one();
        release.notified().await;
    }
}

pub(crate) fn record_discovery_page(rows: usize, kept: usize) {
    DISCOVERY_QUERIES.fetch_add(1, Ordering::Relaxed);
    DISCOVERED_TABLES.fetch_add(kept as u64, Ordering::Relaxed);
    MAX_DISCOVERY_PAGE.fetch_max(rows as u64, Ordering::Relaxed);
}

pub(crate) fn record_prepared_table() {
    PREPARED_TABLES.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_worker_live_fetches(n: u64) {
    WORKER_LIVE_FETCHES.fetch_add(n, Ordering::Relaxed);
}

pub(crate) fn record_table_tasks(alive: usize) {
    MAX_TABLE_TASKS.fetch_max(alive as u64, Ordering::Relaxed);
}

pub(crate) fn record_phase(phase: Phase, d: std::time::Duration) {
    PHASE_MICROS[phase as usize]
        .fetch_add(d.as_micros() as u64, Ordering::Relaxed);
}

pub(crate) fn record_owner_check() {
    OWNER_CHECKS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_boundary(bytes: usize, d: std::time::Duration) {
    BOUNDARIES.fetch_add(1, Ordering::Relaxed);
    BOUNDARY_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
    BOUNDARY_MICROS.fetch_add(d.as_micros() as u64, Ordering::Relaxed);
}

/// One armed hold right after the first chunk a generation published.
static DURING_COPY: Mutex<Option<(Arc<Notify>, Arc<Notify>)>> =
    Mutex::new(None);

/// Arm a hold right after the next generation's first published chunk: the
/// copy pauses there (its guard keeps running) until released.
pub fn hold_during_copy() -> (Arc<Notify>, Arc<Notify>) {
    let pair = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    *DURING_COPY.lock().expect("not poisoned") = Some(pair.clone());
    pair
}

pub(crate) async fn during_copy() {
    let armed = DURING_COPY.lock().expect("not poisoned").take();
    if let Some((reached, release)) = armed {
        reached.notify_one();
        release.notified().await;
    }
}

/// One armed hold right after a generation's terminal barrier was sent.
static AFTER_TERMINAL: Mutex<Option<(Arc<Notify>, Arc<Notify>)>> =
    Mutex::new(None);

/// Arm a hold right after the next terminal barrier is sent: the first
/// notify fires when it is reached, the second releases it.
pub fn hold_after_terminal() -> (Arc<Notify>, Arc<Notify>) {
    let pair = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    *AFTER_TERMINAL.lock().expect("not poisoned") = Some(pair.clone());
    pair
}

pub(crate) async fn after_terminal() {
    let armed = AFTER_TERMINAL.lock().expect("not poisoned").take();
    if let Some((reached, release)) = armed {
        reached.notify_one();
        release.notified().await;
    }
}

/// One armed hold after preparation, right before the snapshot anchor.
static BEFORE_ANCHOR: Mutex<Option<(Arc<Notify>, Arc<Notify>)>> =
    Mutex::new(None);

/// Arm a one-shot hold in the real snapshot path: the next snapshot run
/// notifies `reached` after preparation, right before its consistent anchor
/// (PostgreSQL exported snapshot, MySQL read lock), then waits for
/// `release`. Test seam: lets a test change a prepared table's schema.
pub fn hold_before_anchor() -> (Arc<Notify>, Arc<Notify>) {
    let gate = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    *BEFORE_ANCHOR.lock().expect("anchor hold") = Some(gate.clone());
    gate
}

/// The hold point; a no-op unless a test armed it.
pub(crate) async fn before_anchor() {
    let gate = BEFORE_ANCHOR.lock().expect("anchor hold").take();
    if let Some((reached, release)) = gate {
        reached.notify_one();
        release.notified().await;
    }
}

/// A point of the authorized slot recreation at a generation start
/// (`docs/design/recovery-cli.md`, section 5.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotRecreationPoint {
    /// The authorization was consumed; the slot is not created yet.
    BeforeCreate,
    /// The slot was created; its completion is not recorded yet.
    AfterCreate,
}

type CrashPoint = (
    SlotRecreationPoint,
    Arc<Notify>,
    Arc<Notify>,
    Arc<std::sync::atomic::AtomicBool>,
);

static SLOT_RECREATION: Mutex<Option<CrashPoint>> = Mutex::new(None);

/// Arm a hold at `point` of the next authorized slot recreation: `reached`
/// fires when it is reached; setting `crash` before notifying `release`
/// makes the start stop right there (a process dying at that point:
/// nothing after it runs), otherwise it continues.
pub fn hold_slot_recreation(
    point: SlotRecreationPoint,
) -> (Arc<Notify>, Arc<Notify>, Arc<std::sync::atomic::AtomicBool>) {
    let armed = (
        point,
        Arc::new(Notify::new()),
        Arc::new(Notify::new()),
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    );
    let (_, reached, release, crash) = armed.clone();
    *SLOT_RECREATION.lock().expect("not poisoned") = Some(armed);
    (reached, release, crash)
}

/// At `point`: whether the start must stop here (an injected crash).
pub(crate) async fn slot_recreation_point(point: SlotRecreationPoint) -> bool {
    let armed = {
        let mut slot = SLOT_RECREATION.lock().expect("not poisoned");
        match slot.as_ref() {
            Some((p, ..)) if *p == point => slot.take(),
            _ => None,
        }
    };
    let Some((_, reached, release, crash)) = armed else {
        return false;
    };
    reached.notify_one();
    release.notified().await;
    crash.load(std::sync::atomic::Ordering::SeqCst)
}

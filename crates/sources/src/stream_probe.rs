//! Test observability seam for replication-stream opens.
//!
//! The source must resolve and persist identity *before* it ever opens a
//! replication/binlog stream. A post-mortem count of server-side walsenders or
//! binlog-dump threads cannot prove that a stream was never opened - a buggy
//! ordering could open a stream, fail identity persistence, drop the connection,
//! and still read back zero afterwards.
//!
//! This counter is incremented in the real connect path (the same call the
//! source uses to open a stream), so a test can reset it, drive a startup fault,
//! and assert deterministically that zero streams were opened. It is always
//! compiled (the integration tests link the library without `cfg(test)`); the
//! cost is a single relaxed atomic add per real connection, negligible in
//! production.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

static STREAMS_OPENED: AtomicU64 = AtomicU64::new(0);

/// Record that a replication/binlog stream was successfully opened. Called from
/// the real connect path (Postgres `connect_replication_with_retries`, MySQL
/// `connect_binlog_with_retries`).
#[inline]
pub(crate) fn record_stream_opened() {
    STREAMS_OPENED.fetch_add(1, Ordering::SeqCst);
}

/// Total replication/binlog streams opened process-wide since start (or since
/// the last [`reset_streams_opened`]). Test seam.
pub fn streams_opened() -> u64 {
    STREAMS_OPENED.load(Ordering::SeqCst)
}

/// Reset the counter to zero. Tests call this immediately before driving a
/// source so the subsequent [`streams_opened`] reflects only that run. Because
/// the counter is process-wide, the failover suite must run single-threaded
/// (`--test-threads=1`), which it already requires.
pub fn reset_streams_opened() {
    STREAMS_OPENED.store(0, Ordering::SeqCst);
}

/// One armed hold between a PostgreSQL stream's pre-open slot proof and its
/// START_REPLICATION: `(proved, release)`.
static AFTER_SLOT_PROOF: Mutex<Option<(Arc<Notify>, Arc<Notify>)>> =
    Mutex::new(None);

/// Arm a one-shot hold in the real connect path: the next PostgreSQL stream
/// open notifies `proved` right after its pre-open slot proof passed, then
/// waits for `release` before opening the stream. Test seam: lets a test
/// change the slot exactly between the proof and START_REPLICATION.
pub fn hold_after_slot_proof() -> (Arc<Notify>, Arc<Notify>) {
    let gate = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    *AFTER_SLOT_PROOF.lock().expect("slot proof hold") = Some(gate.clone());
    gate
}

/// The hold point; a no-op unless a test armed it.
pub(crate) async fn after_slot_proof() {
    let gate = AFTER_SLOT_PROOF.lock().expect("slot proof hold").take();
    if let Some((proved, release)) = gate {
        proved.notify_one();
        release.notified().await;
    }
}

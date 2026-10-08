//! Test observability seam for PostgreSQL catalog reads: how many proven
//! stamps and locked captures ran (design test P10: steady DML must cause no
//! catalog work beyond one stamp per Relation). Always compiled, like
//! [`crate::stream_probe`]; one relaxed atomic add per read.

use std::sync::atomic::{AtomicU64, Ordering};

static STAMPS: AtomicU64 = AtomicU64::new(0);
static CAPTURES: AtomicU64 = AtomicU64::new(0);

pub(crate) fn record_stamp() {
    STAMPS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_capture() {
    CAPTURES.fetch_add(1, Ordering::Relaxed);
}

/// `(stamps, captures)` since start or the last [`reset`].
pub fn counts() -> (u64, u64) {
    (
        STAMPS.load(Ordering::Relaxed),
        CAPTURES.load(Ordering::Relaxed),
    )
}

/// Reset both counters (tests run single-threaded).
pub fn reset() {
    STAMPS.store(0, Ordering::Relaxed);
    CAPTURES.store(0, Ordering::Relaxed);
}

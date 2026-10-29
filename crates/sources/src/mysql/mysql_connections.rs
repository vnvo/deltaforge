//! A source's MySQL connections by kind (`docs/design/mysql-mass-invalidation.md`,
//! section 5, "Total source connections"): replication, control-plane,
//! cancellation and snapshot-copy connections are counted separately, each
//! now and at its peak, and exported as
//! `deltaforge_mysql_connections{source, kind}`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

/// What a connection is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Kind {
    /// A binlog session: the stream, or a proof scan.
    Replication,
    /// A control-plane connection (captures, catalog, position, health).
    Control,
    /// A cancellation (`KILL`) connection.
    Cancellation,
    /// A snapshot connection (coordinator, lock, workers).
    SnapshotCopy,
}

impl Kind {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Replication => "replication",
            Self::Control => "control",
            Self::Cancellation => "cancellation",
            Self::SnapshotCopy => "snapshot_copy",
        }
    }
}

#[derive(Default)]
struct Count {
    now: AtomicI64,
    peak: AtomicI64,
}

type Counts = Mutex<HashMap<(String, Kind), Arc<Count>>>;

static COUNTS: OnceLock<Counts> = OnceLock::new();

fn count(source: &str, kind: Kind) -> Arc<Count> {
    COUNTS
        .get_or_init(Mutex::default)
        .lock()
        .expect("counts lock")
        .entry((source.to_string(), kind))
        .or_default()
        .clone()
}

/// One open connection of `kind`, counted until dropped.
pub(crate) struct Counted {
    count: Arc<Count>,
    source: String,
    kind: Kind,
}

impl Counted {
    pub(crate) fn new(source: &str, kind: Kind) -> Self {
        let count = count(source, kind);
        let now = count.now.fetch_add(1, Ordering::SeqCst) + 1;
        count.peak.fetch_max(now, Ordering::SeqCst);
        gauge(source, kind, now);
        Self {
            count,
            source: source.to_string(),
            kind,
        }
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        let now = self.count.now.fetch_sub(1, Ordering::SeqCst) - 1;
        gauge(&self.source, self.kind, now);
    }
}

fn gauge(source: &str, kind: Kind, now: i64) {
    metrics::gauge!(
        "deltaforge_mysql_connections",
        "source" => source.to_string(),
        "kind" => kind.label(),
    )
    .set(now as f64);
}

/// `source`'s open connections of `kind` now, and the most at once since
/// the last [`reset_peak`].
pub(crate) fn connections(source: &str, kind: Kind) -> (i64, i64) {
    let c = count(source, kind);
    (c.now.load(Ordering::SeqCst), c.peak.load(Ordering::SeqCst))
}

/// Start a new peak at the current count (measurements).
pub(crate) fn reset_peak(source: &str, kind: Kind) {
    let c = count(source, kind);
    c.peak.store(c.now.load(Ordering::SeqCst), Ordering::SeqCst);
}

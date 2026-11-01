//! A run's capture boundary (`docs/design/startup-capture-boundary.md`): the
//! source position after which every change to a configured table is
//! delivered, published once it is fixed and protected against start-up
//! retries. A pipeline reports `running` only once its current run has
//! published one.

use std::sync::{Arc, OnceLock};

use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

/// Why a run starts where it does.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureOrigin {
    /// A first start without a checkpoint and without a snapshot: the
    /// server's position when the boundary was read.
    FirstStart,
    /// A restart from the committed resume position.
    Resume,
    /// A snapshot generation's anchor, once the generation is durably
    /// completed: rows as of the anchor were delivered as snapshot rows.
    Snapshot,
    /// A resume moved by a recorded maintenance decision (PostgreSQL
    /// publication maintenance).
    Maintenance,
}

/// A published capture boundary, typed per source. Changes to the
/// pipeline's configured tables committed after it are captured; it says
/// nothing about delivery latency, tables excluded by configuration, or
/// delivery while paused.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum CaptureStart {
    Mysql {
        origin: CaptureOrigin,
        /// The source's schema-registry lineage hash.
        lineage: Option<String>,
        server_uuid: String,
        file: String,
        pos: u64,
        /// The boundary in GTID mode; `file`/`pos` are consistent with it.
        gtid_set: Option<String>,
    },
    Postgres {
        origin: CaptureOrigin,
        /// The source's schema-registry lineage hash.
        lineage: Option<String>,
        system_identifier: u64,
        slot: String,
        lsn: String,
    },
}

impl CaptureStart {
    pub fn origin(&self) -> CaptureOrigin {
        match self {
            Self::Mysql { origin, .. } | Self::Postgres { origin, .. } => {
                *origin
            }
        }
    }
}

/// One run's boundary, published at most once by its source. Each run gets
/// its own, so a late publication from a cancelled or replaced run never
/// reaches its replacement.
#[derive(Clone, Default)]
pub struct CaptureBoundary(Arc<(OnceLock<CaptureStart>, Notify)>);

impl CaptureBoundary {
    pub fn new() -> Self {
        Self::default()
    }

    /// Publish `start`; the first publication wins (later ones are
    /// ignored and return false).
    pub fn publish(&self, start: CaptureStart) -> bool {
        let first = self.0.0.set(start).is_ok();
        if first {
            self.0.1.notify_waiters();
        }
        first
    }

    pub fn get(&self) -> Option<&CaptureStart> {
        self.0.0.get()
    }

    /// Wait until published (returns at once if it already is).
    pub async fn wait(&self) -> CaptureStart {
        loop {
            let notified = self.0.1.notified();
            if let Some(start) = self.get() {
                return start.clone();
            }
            notified.await;
        }
    }

    /// Whether `self` and `other` are the same run's boundary.
    pub fn same_run(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mysql(pos: u64) -> CaptureStart {
        CaptureStart::Mysql {
            origin: CaptureOrigin::FirstStart,
            lineage: Some("l".into()),
            server_uuid: "u".into(),
            file: "mysql-bin.000001".into(),
            pos,
            gtid_set: None,
        }
    }

    #[tokio::test]
    async fn the_first_publication_wins_and_wakes_waiters() {
        let b = CaptureBoundary::new();
        assert!(b.get().is_none());
        let waiter = tokio::spawn({
            let b = b.clone();
            async move { b.wait().await }
        });
        tokio::task::yield_now().await;
        assert!(b.publish(mysql(4)));
        assert!(!b.publish(mysql(9)), "a second publication is ignored");
        assert_eq!(waiter.await.unwrap(), mysql(4));
        assert_eq!(b.get(), Some(&mysql(4)));
    }

    #[test]
    fn runs_are_told_apart_by_identity() {
        let a = CaptureBoundary::new();
        assert!(a.same_run(&a.clone()));
        assert!(!a.same_run(&CaptureBoundary::new()));
    }

    #[test]
    fn serializes_with_its_source_and_origin() {
        let v = serde_json::to_value(mysql(4)).unwrap();
        assert_eq!(v["source"], "mysql");
        assert_eq!(v["origin"], "first_start");
        assert_eq!(v["pos"], 4);
        let pg = CaptureStart::Postgres {
            origin: CaptureOrigin::Snapshot,
            lineage: None,
            system_identifier: 7,
            slot: "s".into(),
            lsn: "0/16B3780".into(),
        };
        let v = serde_json::to_value(&pg).unwrap();
        assert_eq!(v["source"], "postgres");
        assert_eq!(v["origin"], "snapshot");
        assert_eq!(serde_json::from_value::<CaptureStart>(v).unwrap(), pg);
    }
}

//! Snapshot connection admission (`docs/design/snapshot-durable-queue.md`,
//! section 9): one process-wide pool over every snapshot connection
//! (coordinator, lock, workers, intra-table readers) of every pipeline, with a
//! per-source share.
//!
//! A snapshot takes its base permits (the connections it cannot start
//! without) all at once, before it opens any catalog transaction, lock or
//! anchor; the wait is cancellable and never holds part of the base (a fair
//! semaphore wait would: it hands a front waiter permits as they free up).
//! Workers above the base take one permit each and release it when done.
//! Oversubscription is allowed: snapshots queue.

use std::sync::{Arc, OnceLock};

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

/// The process-wide cap when none is configured.
pub const DEFAULT_MAX_SNAPSHOT_CONNECTIONS: u32 = 64;

/// The process-wide permits, and the notification every release sends.
#[derive(Clone)]
struct Pool {
    permits: Arc<Semaphore>,
    released: Arc<Notify>,
}

impl Pool {
    fn new(cap: u32) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(cap as usize)),
            released: Arc::new(Notify::new()),
        }
    }

    /// Take `n` permits all at once, or none: wait (cancellably) for a
    /// release and try again.
    async fn take(
        &self,
        n: u32,
        cancel: &CancellationToken,
    ) -> Result<Released, String> {
        loop {
            let released = self.released.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            if let Ok(p) = self.permits.clone().try_acquire_many_owned(n) {
                return Ok(Released {
                    permit: Some(p),
                    released: self.released.clone(),
                });
            }
            tokio::select! {
                _ = &mut released => {}
                _ = cancel.cancelled() => {
                    return Err(
                        "cancelled while queued for snapshot connections".into()
                    );
                }
            }
        }
    }
}

/// Permits that wake the waiters when released.
struct Released {
    permit: Option<OwnedSemaphorePermit>,
    released: Arc<Notify>,
}

impl Drop for Released {
    fn drop(&mut self) {
        drop(self.permit.take());
        self.released.notify_waiters();
    }
}

static GLOBAL: OnceLock<(u32, Pool)> = OnceLock::new();

/// Set the process-wide cap once, at startup. `Err` on a zero cap, or on a
/// second call with another cap.
pub fn configure(cap: u32) -> Result<(), String> {
    if cap == 0 {
        return Err("max_snapshot_connections must be positive".into());
    }
    let set = GLOBAL.get_or_init(|| (cap, Pool::new(cap)));
    if set.0 == cap {
        Ok(())
    } else {
        Err(format!(
            "max_snapshot_connections is already {} for this process",
            set.0
        ))
    }
}

fn global() -> &'static (u32, Pool) {
    GLOBAL.get_or_init(|| {
        let cap = DEFAULT_MAX_SNAPSHOT_CONNECTIONS;
        (cap, Pool::new(cap))
    })
}

/// The process-wide cap in force.
pub fn global_cap() -> u32 {
    global().0
}

/// Validate one source's connection demand (design section 9): the global cap
/// is positive, its base fits its own share, and its base and maximum fit the
/// process-wide cap.
pub fn validate(
    global_cap: u32,
    source_cap: u32,
    base: u32,
    max: u32,
) -> Result<(), String> {
    if global_cap == 0 {
        return Err(
            "the process-wide snapshot connection cap must be positive".into(),
        );
    }
    if base > source_cap {
        return Err(format!(
            "the snapshot needs {base} connections to start, above its \
             max_snapshot_connections of {source_cap}"
        ));
    }
    let most = max.min(source_cap).max(base);
    if most > global_cap {
        return Err(format!(
            "the snapshot needs up to {most} connections, above the \
             process-wide cap of {global_cap}"
        ));
    }
    Ok(())
}

/// The permits one snapshot holds.
pub struct SnapshotPermits {
    pipeline: String,
    global: Pool,
    share: Arc<Semaphore>,
    _base: (Released, OwnedSemaphorePermit),
}

/// One extra connection's permits (released on drop).
pub struct ExtraPermit {
    _global: Released,
    _share: OwnedSemaphorePermit,
}

impl SnapshotPermits {
    /// Acquire the base all at once on the process-wide pool, within a share
    /// of `source_cap`. Cancellable; while it waits the snapshot is counted as
    /// queued (`deltaforge_snapshot_queued{pipeline}`).
    pub async fn acquire(
        pipeline: &str,
        source_cap: u32,
        base: u32,
        cancel: &CancellationToken,
    ) -> Result<Self, String> {
        Self::acquire_on(global().1.clone(), pipeline, source_cap, base, cancel)
            .await
    }

    async fn acquire_on(
        global: Pool,
        pipeline: &str,
        source_cap: u32,
        base: u32,
        cancel: &CancellationToken,
    ) -> Result<Self, String> {
        let share = Arc::new(Semaphore::new(source_cap as usize));
        let own = share.clone().try_acquire_many_owned(base).map_err(|_| {
            format!("base of {base} exceeds the share of {source_cap}")
        })?;
        let queued = metrics::gauge!(
            "deltaforge_snapshot_queued",
            "pipeline" => pipeline.to_string()
        );
        queued.set(1.0);
        let got = global.take(base, cancel).await;
        queued.set(0.0);
        let g = got?;
        record_permits(&global.permits);
        Ok(Self {
            pipeline: pipeline.to_string(),
            global,
            share,
            _base: (g, own),
        })
    }

    /// One more connection (a worker above the base, an intra-table reader),
    /// within the share. Cancellable.
    pub async fn extra(
        &self,
        cancel: &CancellationToken,
    ) -> Result<ExtraPermit, String> {
        let share = tokio::select! {
            p = self.share.clone().acquire_owned() => p.map_err(|e| e.to_string())?,
            _ = cancel.cancelled() => return Err("cancelled".into()),
        };
        let global = self.global.take(1, cancel).await?;
        record_permits(&self.global.permits);
        Ok(ExtraPermit {
            _global: global,
            _share: share,
        })
    }

    pub fn pipeline(&self) -> &str {
        &self.pipeline
    }
}

fn record_permits(global: &Semaphore) {
    metrics::gauge!(
        "deltaforge_snapshot_connection_permits",
        "state" => "available"
    )
    .set(global.available_permits() as f64);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_follows_the_ruling() {
        assert!(validate(0, 4, 2, 4).is_err(), "global cap positive");
        assert!(validate(64, 2, 3, 4).is_err(), "base fits the share");
        assert!(validate(2, 8, 3, 8).is_err(), "base fits the global cap");
        assert!(validate(4, 8, 2, 8).is_err(), "maximum fits the global cap");
        assert!(validate(64, 8, 3, 8).is_ok());
    }

    /// Two snapshots sharing a cap of 4, each with a base of 3: the second
    /// queues holding nothing of the cap until the first releases.
    #[tokio::test]
    async fn a_queued_snapshot_holds_no_partial_base() {
        let global = Pool::new(4);
        let cancel = CancellationToken::new();
        let first =
            SnapshotPermits::acquire_on(global.clone(), "a", 4, 3, &cancel)
                .await
                .unwrap();
        let (g2, c2) = (global.clone(), cancel.clone());
        let second = tokio::spawn(async move {
            SnapshotPermits::acquire_on(g2, "b", 4, 3, &c2).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!second.is_finished(), "queued");
        assert_eq!(
            global.permits.available_permits(),
            1,
            "no partial base held while queued"
        );
        drop(first);
        let second = second.await.unwrap().unwrap();
        assert_eq!(global.permits.available_permits(), 1);
        // An extra connection within the share and the cap.
        let extra = second.extra(&cancel).await.unwrap();
        assert_eq!(global.permits.available_permits(), 0);
        drop(extra);
        assert_eq!(global.permits.available_permits(), 1);
    }

    #[tokio::test]
    async fn a_queued_snapshot_is_cancellable() {
        let global = Pool::new(2);
        let cancel = CancellationToken::new();
        let _held =
            SnapshotPermits::acquire_on(global.clone(), "a", 2, 2, &cancel)
                .await
                .unwrap();
        let other = CancellationToken::new();
        let (o2, g2) = (other.clone(), global.clone());
        let waiting = tokio::spawn(async move {
            SnapshotPermits::acquire_on(g2, "b", 2, 2, &o2).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        other.cancel();
        assert!(waiting.await.unwrap().is_err());
        assert_eq!(
            global.permits.available_permits(),
            0,
            "the holder keeps its base"
        );
    }
}

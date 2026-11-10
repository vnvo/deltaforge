//! The backlog drain (scenario `D1`): every pipeline is stopped through the
//! REST API with a durable checkpoint (`Action::BacklogPause`), the writers
//! go on and build a backlog, and only after every writer has finished and
//! the committed count is frozen is the same pipeline resumed and its
//! delivery followed to the frozen count ([`after_writers`], then
//! [`drain`]). What it measures is backlog-drain capacity, not steady-state
//! latency or behaviour under concurrent writes.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

type Fut<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// What the drain needs from DeltaForge: its pipelines' status, per-sink
/// checkpoints and resume, and its metrics.
pub trait Ops: Sync {
    fn status<'a>(&'a self, name: &'a str) -> Fut<'a, String>;
    fn checkpoints<'a>(
        &'a self,
        name: &'a str,
    ) -> Fut<'a, BTreeMap<String, String>>;
    fn resume<'a>(&'a self, name: &'a str) -> Fut<'a, ()>;
    fn stop<'a>(&'a self, name: &'a str) -> Fut<'a, ()>;
    fn metrics(&self) -> Fut<'_, String>;
}

/// The pause: when, and every pipeline's per-sink checkpoints once stopped.
#[derive(Debug, Clone, Serialize)]
pub struct Paused {
    pub at: DateTime<Utc>,
    pub checkpoints: BTreeMap<String, BTreeMap<String, String>>,
}

/// One metrics scrape: the run's pipelines' delivered events and bytes.
#[derive(Debug, Clone, Serialize)]
pub struct Scrape {
    pub at: DateTime<Utc>,
    pub sink_events: f64,
    pub sink_bytes: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Report {
    pub paused: Paused,
    /// Every pipeline still `stopped`, with its checkpoints unchanged, just
    /// before the resume.
    pub status_before_resume: BTreeMap<String, String>,
    pub checkpoints_before_resume: BTreeMap<String, BTreeMap<String, String>>,
    pub checkpoint_preserved: bool,
    /// Row events the frozen committed counts produce (ops plus delete
    /// stamps), and the uncertain operations beside them.
    pub expected_row_events: u64,
    pub uncertain_ops: u64,
    /// Row events the verifier had received at the resume, and the backlog.
    pub delivered_at_resume: u64,
    pub backlog_events: u64,
    pub resume_requested_at: Option<DateTime<Utc>>,
    /// From the resume request: every pipeline `running` (startup and
    /// reload), the first backlog event, and the last expected one.
    pub running_after_secs: Option<f64>,
    pub first_delivery_after_secs: Option<f64>,
    pub complete_after_secs: Option<f64>,
    pub delivered_at_end: u64,
    /// `drained`, `drain incomplete` (the bound passed: not proven loss; the
    /// pipelines are stopped there and the ledger's missing counts stand
    /// beside it), `pipeline failed`,
    /// `resume failed` or `not resumed`.
    pub outcome: String,
    pub failure: Option<String>,
    /// The two scrapes bracketing the drain (events and bytes from the same
    /// two).
    pub metrics_at_resume: Option<Scrape>,
    pub metrics_at_end: Option<Scrape>,
    /// Delivery polls this often: the completion time's resolution.
    pub poll_secs: f64,
}

/// Stop the writers and join every one; `then` runs only after (the frozen
/// counts, and a paused pipeline's resume). Returns the writers' errors.
pub async fn after_writers<W, F, Fu, T>(
    stop: &watch::Sender<bool>,
    writers: Vec<JoinHandle<Result<W>>>,
    then: F,
) -> (Vec<String>, T)
where
    F: FnOnce() -> Fu,
    Fu: Future<Output = T>,
{
    stop.send(true).ok();
    let mut errors = Vec::new();
    for w in writers {
        match w.await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => errors.push(format!("{e:#}")),
            Err(e) => errors.push(format!("writer task: {e}")),
        }
    }
    (errors, then().await)
}

/// The sum over `pipelines` of a metric family in a Prometheus text body.
fn family(body: &str, metric: &str, pipelines: &[String]) -> f64 {
    body.lines()
        .filter(|l| l.starts_with(&format!("{metric}{{")))
        .filter(|l| {
            pipelines
                .iter()
                .any(|p| l.contains(&format!("pipeline=\"{p}\"")))
        })
        .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

async fn scrape(ops: &dyn Ops, pipelines: &[String]) -> Option<Scrape> {
    let body = ops.metrics().await.ok()?;
    Some(Scrape {
        at: Utc::now(),
        sink_events: family(&body, "deltaforge_sink_events_total", pipelines),
        sink_bytes: family(&body, "deltaforge_sink_bytes_total", pipelines),
    })
}

/// Resume the paused `pipelines` (after [`after_writers`]) and follow
/// delivery until `observed` (the verifier's row events) reaches
/// `expected`, a pipeline fails, or `max` passes from the resume request.
#[allow(clippy::too_many_arguments)]
pub async fn drain(
    ops: &dyn Ops,
    pipelines: &[String],
    paused: Paused,
    expected: u64,
    uncertain_ops: u64,
    observed: &AtomicU64,
    max: Duration,
    poll: Duration,
) -> Report {
    let mut status = BTreeMap::new();
    let mut now_cp = BTreeMap::new();
    for p in pipelines {
        let s = ops.status(p).await.unwrap_or_else(|e| format!("({e:#})"));
        status.insert(p.clone(), s);
        now_cp.insert(p.clone(), ops.checkpoints(p).await.unwrap_or_default());
    }
    let preserved = status.values().all(|s| s == "stopped")
        && now_cp == paused.checkpoints
        && paused.checkpoints.values().all(|c| !c.is_empty());
    let delivered = observed.load(Ordering::Relaxed);
    let mut r = Report {
        paused,
        status_before_resume: status,
        checkpoints_before_resume: now_cp,
        checkpoint_preserved: preserved,
        expected_row_events: expected,
        uncertain_ops,
        delivered_at_resume: delivered,
        backlog_events: expected.saturating_sub(delivered),
        resume_requested_at: None,
        running_after_secs: None,
        first_delivery_after_secs: None,
        complete_after_secs: None,
        delivered_at_end: delivered,
        outcome: "not resumed".into(),
        failure: None,
        metrics_at_resume: None,
        metrics_at_end: None,
        poll_secs: poll.as_secs_f64(),
    };
    if !preserved {
        r.failure = Some(
            "a pipeline was not stopped, or its checkpoints changed or were \
             missing, before the resume"
                .into(),
        );
        return r;
    }
    r.metrics_at_resume = scrape(ops, pipelines).await;
    r.resume_requested_at = Some(Utc::now());
    let t0 = Instant::now();
    for p in pipelines {
        if let Err(e) = ops.resume(p).await {
            r.outcome = "resume failed".into();
            r.failure = Some(format!("resume {p}: {e:#}"));
            return r;
        }
    }
    loop {
        let seen = observed.load(Ordering::Relaxed);
        if r.first_delivery_after_secs.is_none() && seen > delivered {
            r.first_delivery_after_secs = Some(t0.elapsed().as_secs_f64());
        }
        if seen >= expected {
            r.complete_after_secs = Some(t0.elapsed().as_secs_f64());
            r.delivered_at_end = seen;
            r.outcome = "drained".into();
            break;
        }
        let mut running = true;
        for p in pipelines {
            match ops.status(p).await.as_deref() {
                Ok("running") => {}
                Ok("failed") => {
                    r.outcome = "pipeline failed".into();
                    r.failure = Some(format!("pipeline {p} failed"));
                }
                _ => running = false,
            }
        }
        if r.failure.is_some() {
            r.delivered_at_end = seen;
            break;
        }
        if running && r.running_after_secs.is_none() {
            r.running_after_secs = Some(t0.elapsed().as_secs_f64());
        }
        if t0.elapsed() >= max {
            r.delivered_at_end = seen;
            r.outcome = "drain incomplete".into();
            // Stopped at the bound, so the verifier's completeness is of
            // what arrived within it (and the run ends in its deadline).
            for p in pipelines {
                if let Err(e) = ops.stop(p).await {
                    r.failure = Some(format!("stop {p} at the bound: {e:#}"));
                }
            }
            break;
        }
        tokio::time::sleep(poll).await;
    }
    r.metrics_at_end = scrape(ops, pipelines).await;
    r
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;

    /// DeltaForge as the drain sees it: stopped until resumed; once resumed,
    /// every poll delivers `step` more row events to `observed`.
    struct Fake {
        resumed: AtomicBool,
        observed: Arc<AtomicU64>,
        step: u64,
        /// At the resume: (writers finished, the committed count then).
        at_resume: Mutex<Option<(usize, u64)>>,
        finished: Arc<AtomicU64>,
        committed: Arc<AtomicU64>,
        writers: usize,
    }

    impl Ops for Fake {
        fn status<'a>(&'a self, _: &'a str) -> Fut<'a, String> {
            Box::pin(async move {
                if self.resumed.load(Ordering::SeqCst) {
                    self.observed.fetch_add(self.step, Ordering::SeqCst);
                    Ok("running".into())
                } else {
                    Ok("stopped".into())
                }
            })
        }
        fn checkpoints<'a>(
            &'a self,
            _: &'a str,
        ) -> Fut<'a, BTreeMap<String, String>> {
            Box::pin(async {
                Ok(BTreeMap::from([("kafka".into(), "cp-1".into())]))
            })
        }
        fn resume<'a>(&'a self, _: &'a str) -> Fut<'a, ()> {
            Box::pin(async move {
                *self.at_resume.lock().unwrap() = Some((
                    self.finished.load(Ordering::SeqCst) as usize,
                    self.committed.load(Ordering::SeqCst),
                ));
                self.resumed.store(true, Ordering::SeqCst);
                Ok(())
            })
        }
        fn stop<'a>(&'a self, _: &'a str) -> Fut<'a, ()> {
            Box::pin(async move {
                self.resumed.store(false, Ordering::SeqCst);
                Ok(())
            })
        }
        fn metrics(&self) -> Fut<'_, String> {
            let n = self.observed.load(Ordering::SeqCst);
            Box::pin(async move {
                Ok(format!(
                    "deltaforge_sink_events_total{{pipeline=\"p\",sink=\"kafka\"}} {n}\n\
                     deltaforge_sink_bytes_total{{pipeline=\"p\",sink=\"kafka\"}} {}\n",
                    n * 100
                ))
            })
        }
    }

    fn paused() -> Paused {
        Paused {
            at: Utc::now(),
            checkpoints: BTreeMap::from([(
                "p".to_string(),
                BTreeMap::from([("kafka".to_string(), "cp-1".to_string())]),
            )]),
        }
    }

    /// Writers still committing when the stop is sent: the resume is
    /// requested only after every one has finished, with the committed
    /// count already final; delivery is then followed to that count.
    #[tokio::test(start_paused = true)]
    async fn the_resume_waits_for_every_writer_then_drains_the_frozen_backlog()
    {
        let (stop_tx, stop_rx) = watch::channel(false);
        let committed = Arc::new(AtomicU64::new(0));
        let finished = Arc::new(AtomicU64::new(0));
        let writers: Vec<JoinHandle<Result<u64>>> = (0..4)
            .map(|w| {
                let (mut stop, committed, finished) =
                    (stop_rx.clone(), committed.clone(), finished.clone());
                tokio::spawn(async move {
                    // Each writer commits until told to stop, then needs a
                    // while (its open transaction) longer for writer `w`.
                    loop {
                        committed.fetch_add(10, Ordering::SeqCst);
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
                            _ = stop.changed() => break,
                        }
                    }
                    tokio::time::sleep(Duration::from_secs(w + 1)).await;
                    committed.fetch_add(10, Ordering::SeqCst);
                    finished.fetch_add(1, Ordering::SeqCst);
                    Ok(0)
                })
            })
            .collect();
        tokio::time::sleep(Duration::from_secs(1)).await;
        let observed = Arc::new(AtomicU64::new(0));
        let fake = Fake {
            resumed: AtomicBool::new(false),
            observed: observed.clone(),
            step: 7,
            at_resume: Mutex::new(None),
            finished: finished.clone(),
            committed: committed.clone(),
            writers: 4,
        };
        let pipelines = vec!["p".to_string()];
        let (errors, report) = after_writers(&stop_tx, writers, || async {
            let expected = committed.load(Ordering::SeqCst);
            drain(
                &fake,
                &pipelines,
                paused(),
                expected,
                0,
                &observed,
                Duration::from_secs(900),
                Duration::from_millis(250),
            )
            .await
        })
        .await;
        assert!(errors.is_empty(), "{errors:?}");
        let (done, at_resume) = fake.at_resume.lock().unwrap().unwrap();
        assert_eq!(
            done, fake.writers,
            "every writer finished before the resume"
        );
        assert_eq!(at_resume, committed.load(Ordering::SeqCst), "frozen");
        assert_eq!(report.expected_row_events, at_resume);
        assert_eq!(report.delivered_at_resume, 0);
        assert_eq!(report.backlog_events, at_resume);
        assert!(report.checkpoint_preserved);
        assert_eq!(report.outcome, "drained");
        assert!(report.delivered_at_end >= at_resume);
        assert!(report.running_after_secs.is_some());
        assert!(report.first_delivery_after_secs.is_some());
        let (a, b) = (
            report.metrics_at_resume.unwrap(),
            report.metrics_at_end.unwrap(),
        );
        assert_eq!(a.sink_events, 0.0);
        assert!(
            b.sink_events >= at_resume as f64
                && b.sink_bytes == b.sink_events * 100.0
        );
    }

    /// The bound passes first: `drain incomplete`, never a pass, with what
    /// arrived; a checkpoint that changed while stopped is not resumed.
    #[tokio::test(start_paused = true)]
    async fn a_drain_past_its_bound_is_incomplete_and_a_changed_checkpoint_is_not_resumed()
     {
        let observed = Arc::new(AtomicU64::new(0));
        let fake = Fake {
            resumed: AtomicBool::new(false),
            observed: observed.clone(),
            step: 1,
            at_resume: Mutex::new(None),
            finished: Arc::default(),
            committed: Arc::default(),
            writers: 0,
        };
        let pipelines = vec!["p".to_string()];
        let r = drain(
            &fake,
            &pipelines,
            paused(),
            1_000_000,
            0,
            &observed,
            Duration::from_secs(10),
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(r.outcome, "drain incomplete");
        assert!(r.complete_after_secs.is_none());
        assert!(!fake.resumed.load(Ordering::SeqCst), "stopped at the bound");
        assert!(r.delivered_at_end > 0 && r.delivered_at_end < 1_000_000);

        let mut changed = paused();
        changed
            .checkpoints
            .get_mut("p")
            .unwrap()
            .insert("kafka".into(), "cp-0".into());
        let fake = Fake {
            resumed: AtomicBool::new(false),
            observed: observed.clone(),
            step: 1,
            at_resume: Mutex::new(None),
            finished: Arc::default(),
            committed: Arc::default(),
            writers: 0,
        };
        let r = drain(
            &fake,
            &pipelines,
            changed,
            10,
            0,
            &observed,
            Duration::from_secs(10),
            Duration::from_secs(1),
        )
        .await;
        assert!(!r.checkpoint_preserved);
        assert_eq!(r.outcome, "not resumed");
        assert!(fake.at_resume.lock().unwrap().is_none(), "never resumed");
    }
}

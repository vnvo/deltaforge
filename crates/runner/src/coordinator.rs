use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use checkpoints::CheckpointStore;
use futures::future::BoxFuture;
use metrics::{counter, histogram};
use tokio::sync::watch;
use tokio::time::{Instant, interval};
use tokio_util::sync::CancellationToken;
use tracing::debug;
use uuid::Uuid;

use deltaforge_config::{BatchConfig, CommitPolicy};
use deltaforge_core::{ArcDynProcessor, ArcDynSink, CheckpointMeta, Event};

/// Persist/commit the token after the batch is successfully delivered to sinks.
pub type CommitCpFn<Tok> =
    Box<dyn Fn(Tok) -> BoxFuture<'static, Result<()>> + Send + Sync + 'static>;

pub struct ProcessedBatch<Tok> {
    pub events: Vec<Event>,
    pub last_checkpoint: Option<Tok>,
}

/// Process a *mutable* batch of events (can modify/duplicate/drop).
/// Takes ownership of the current batch Vec and returns the processed Vec.
pub type ProcessBatchFn<Tok> = Arc<
    dyn Fn(Vec<Event>) -> BoxFuture<'static, Result<ProcessedBatch<Tok>>>
        + Send
        + Sync
        + 'static,
>;

/// During accumulation, we own a mutable Vec<Event>.
struct BuildingBatch {
    started_at: Instant,
    raw: Vec<Event>,
    bytes: usize,
}

impl BuildingBatch {
    fn new() -> Self {
        Self {
            started_at: Instant::now(),
            raw: Vec::new(),
            bytes: 0,
        }
    }
}

/// After processing, freeze as Arc<[Event]> for zero-copy sharing.
#[derive(Debug)]
struct FrozenBatch {
    id: Uuid,
    //started_at: Instant,
    events: Arc<[Event]>,
    bytes: usize,
}

fn event_size_hint(ev: &Event) -> usize {
    ev.size_bytes
}

/// something to fix later in the `Event`
fn is_tx_boundary(_ev: &Event) -> bool {
    //ev.is_tx_boundary()
    true
}

fn policy_satisfied(
    policy: &Option<CommitPolicy>,
    required_total: usize,
    required_acks: usize,
    total_acks: usize,
) -> bool {
    match policy.as_ref().unwrap_or(&CommitPolicy::Required) {
        CommitPolicy::All => total_acks == required_total,
        CommitPolicy::Required => required_acks == required_total,
        CommitPolicy::Quorum { quorum } => total_acks >= *quorum,
    }
}

// For now, treat all sinks as required.
fn is_sink_required(_sink: &ArcDynSink) -> bool {
    true
}

pub struct Coordinator<Tok> {
    pipeline_name: Arc<str>,
    sinks: Vec<ArcDynSink>,
    /// Store the *effective* (defaults-applied) batch config.
    batch_cfg_eff: BatchConfig,
    commit_policy: Option<CommitPolicy>,
    commit_cp: CommitCpFn<Tok>,
    process_batch: ProcessBatchFn<Tok>,
}

impl<Tok: Send + 'static> Coordinator<Tok> {
    pub fn new(
        name: String,
        sinks: Vec<ArcDynSink>,
        batch_cfg_from_spec: Option<BatchConfig>,
        commit_policy: Option<CommitPolicy>,
        commit_cp: CommitCpFn<Tok>,
        process_batch: ProcessBatchFn<Tok>,
    ) -> Self {
        let batch_cfg_eff = Self::effective(&batch_cfg_from_spec);
        Self {
            pipeline_name: name.into(),
            sinks,
            batch_cfg_eff,
            commit_policy,
            commit_cp,
            process_batch,
        }
    }

    /// Build - process (mutable) - freeze (Arc<[Event]>) - deliver - maybe commit.
    pub async fn run(
        self,
        mut event_rx: tokio::sync::mpsc::Receiver<Event>,
        cancel: CancellationToken,
        mut pause_rx: watch::Receiver<bool>,
    ) -> Result<()> {
        let tick_ms = self.batch_cfg_eff.max_ms.unwrap_or(200);
        let mut ticker = interval(Duration::from_millis(tick_ms));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut building: Option<BuildingBatch> = None;

        loop {
            if *pause_rx.borrow() {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        break;
                    }
                    changed = pause_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        continue;
                    }
                }
            }
            tokio::select! {
                _ = cancel.cancelled() => {
                    if let Some(b) = building.take()
                        && !b.raw.is_empty() {
                            self.process_deliver_and_maybe_commit(b, "cancelled").await?;
                        }

                    break;
                }
                _ = ticker.tick() => {
                    if let Some(b) = building.take() {
                        if !b.raw.is_empty() && b.started_at.elapsed() >= Duration::from_millis(tick_ms) {
                            self.process_deliver_and_maybe_commit(b, "timer").await?;
                        } else {
                            building = Some(b);
                        }
                    }
                }

                changed = pause_rx.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    continue;
                }

                maybe_ev = event_rx.recv() => {
                    let Some(ev) = maybe_ev else {
                        // channel closed — final flush
                        if let Some(b) = building.take()
                            && !b.raw.is_empty() {
                                self.process_deliver_and_maybe_commit(b, "shutdown").await?;
                            }

                        break;
                    };

                    if building.is_none() {
                        building = Some(BuildingBatch::new());
                        debug!(pipeline=%self.pipeline_name, "building new batch");
                    }

                    let cfg = &self.batch_cfg_eff;
                    let mut b = building.take().unwrap();

                    let would_exceed_events = cfg.max_events.map(|m| b.raw.len() >= m).unwrap_or(false);
                    let would_exceed_bytes  = cfg.max_bytes.map(|m| b.bytes + event_size_hint(&ev) >= m).unwrap_or(false);
                    let limit_hit = would_exceed_events || would_exceed_bytes;

                    let _respect_tx = cfg.respect_source_tx.unwrap_or(true);
                    let boundary = is_tx_boundary(&ev);

                    // If limits hit, flush current (avoid splitting tx if configured).
                    //if (limit_hit && !boundary) || (limit_hit && boundary && !b.raw.is_empty()) {
                    if (!b.raw.is_empty() || !boundary) && limit_hit {
                        self.process_deliver_and_maybe_commit(b, "limits").await?;
                        b = BuildingBatch::new();
                    }

                    b.bytes += event_size_hint(&ev);
                    b.raw.push(ev);
                    building = Some(b);
                }
            }
        }

        Ok(())
    }

    async fn process_deliver_and_maybe_commit(
        &self,
        mut b: BuildingBatch,
        reason: &str,
    ) -> Result<()> {
        // 1) PROCESS (mutable): processors can modify/duplicate/drop events
        let proc_start = Instant::now();
        let processed = (self.process_batch)(std::mem::take(&mut b.raw))
            .await
            .context("process batch")?;
        histogram!(
            "deltaforge_stage_latency_seconds",
            "pipeline" => self.pipeline_name.to_string(),
            "stage" => "process",
            "trigger" => reason.to_string()
        )
        .record(proc_start.elapsed().as_secs_f64());
        debug!(
            pipeline=%self.pipeline_name,
            processed_count=%processed.events.len(),
            "receveid events from processors"
        );

        let last_cp = processed.last_checkpoint;
        // recompute size after processing
        let bytes = processed.events.iter().map(event_size_hint).sum();
        histogram!(
            "deltaforge_batch_events",
            "pipeline" => self.pipeline_name.to_string(),
        )
        .record(processed.events.len() as f64);
        histogram!(
            "deltaforge_batch_bytes",
            "pipeline" => self.pipeline_name.to_string(),
        )
        .record(bytes as f64);

        // 2) freeze
        let frozen = FrozenBatch {
            id: Uuid::new_v4(),
            //started_at: b.started_at,
            bytes,
            events: Arc::<[Event]>::from(processed.events),
        };

        // 3) deliver to sinks
        let mut required_total = 0usize;
        let mut required_acks = 0usize;
        let mut total_acks = 0usize;

        debug!(
            pipeline=%self.pipeline_name,
            sink_count=self.sinks.len(),
            event_count=frozen.events.len(),
            "sending batch to sink(s)");

        for sink in &self.sinks {
            let required = is_sink_required(sink);
            if required {
                required_total += 1;
            }

            let sink_start = Instant::now();

            // batch send
            match sink.send_batch(&frozen.events).await {
                Ok(()) => {
                    total_acks += 1;
                    if required {
                        required_acks += 1;
                    }

                    counter!(
                        "deltaforge_sink_events_total",
                        "pipeline"=>self.pipeline_name.to_string(),
                        "sink"=>sink.id().to_string(),
                    )
                    .increment(frozen.events.len() as u64);
                    counter!(
                        "deltaforge_sink_batch_total",
                        "pipeline"=>self.pipeline_name.to_string(),
                        "sink"=>sink.id().to_string(),
                    )
                    .increment(1);
                    histogram!(
                        "deltaforge_sink_latency_seconds",
                        "pipeline" => self.pipeline_name.to_string(),
                        "sink" => sink.id().to_string(),
                    )
                    .record(sink_start.elapsed().as_secs_f64());
                }
                Err(e) => {
                    tracing::warn!(
                        pipeline = %self.pipeline_name,
                        sink_id = %sink.id(),
                        batch_id = %frozen.id,
                        event_count = frozen.events.len(),
                        kind = %e.kind(),
                        details = %e.details(),
                        error = ?e,
                        "sink batch failed"
                    );

                    counter!(
                        "deltaforge_sink_failures_total",
                        "pipeline" => self.pipeline_name.to_string(),
                        "sink" => sink.id().to_string(),
                    )
                    .increment(1);
                }
            }
        }

        // 4) COMMIT checkpoint if policy satisfied
        if policy_satisfied(
            &self.commit_policy,
            required_total,
            required_acks,
            total_acks,
        ) {
            if let Some(cp) = last_cp {
                (self.commit_cp)(cp)
                    .await
                    .context("commit checkpoint failed")?;
                debug!(batch_id=%frozen.id, reason, events=frozen.events.len(), bytes=frozen.bytes, "batch committed");
            } else {
                debug!(batch_id=%frozen.id, reason, "no checkpoint-bearing events; checkpoint NOT advanced");
            }
        } else {
            tracing::warn!(
                batch_id=%frozen.id, reason, required_total, required_acks, total_acks,
                "commit policy not satisfied; checkpoint NOT advanced"
            );
        }

        Ok(())
    }

    /// compute the effective batch configuration by applying defaults.
    fn effective(cfg: &Option<BatchConfig>) -> BatchConfig {
        BatchConfig {
            max_events: Some(
                cfg.as_ref().and_then(|c| c.max_events).unwrap_or(1000),
            ),
            max_bytes: Some(
                cfg.as_ref()
                    .and_then(|c| c.max_bytes)
                    .unwrap_or(8 * 1024 * 1024),
            ),
            max_ms: Some(cfg.as_ref().and_then(|c| c.max_ms).unwrap_or(200)),
            respect_source_tx: Some(
                cfg.as_ref()
                    .and_then(|c| c.respect_source_tx)
                    .unwrap_or(true),
            ),
            max_inflight: Some(
                cfg.as_ref().and_then(|c| c.max_inflight).unwrap_or(1),
            ),
        }
    }
}

pub fn build_commit_fn(
    ckpt_store: Arc<dyn CheckpointStore>,
    checkpoint_key: String,
) -> CommitCpFn<CheckpointMeta> {
    use futures::FutureExt;
    Box::new(move |cp: CheckpointMeta| {
        let ckpt_store = ckpt_store.clone();
        let key = checkpoint_key.clone();

        async move {
            ckpt_store
                .put_raw(&key, cp.as_bytes())
                .await
                .context("save checkpoint")?;
            debug!(checkpoint_key=%key, bytes=cp.as_bytes().len(), "checkpoint saved");
            Ok(())
        }.boxed()
    })
}

pub fn build_batch_processor(
    processors: Arc<[ArcDynProcessor]>,
    pipeline: String,
) -> ProcessBatchFn<CheckpointMeta> {
    use futures::FutureExt;

    let pipeline_name: Arc<str> = pipeline.into();

    Arc::new(move |events: Vec<Event>| {
        let procs = Arc::clone(&processors);
        let pipeline = Arc::clone(&pipeline_name);

        async move {
            if procs.is_empty() {
                let last_cp = events
                    .iter()
                    .rev()
                    .find_map(|e| e.checkpoint.as_ref())
                    .cloned();
                return Ok(ProcessedBatch {
                    events,
                    last_checkpoint: last_cp,
                });
            }

            //let mut out = Vec::with_capacity(events.len());
            let mut batch = events;

            for p in procs.iter() {
                let pid = p.id();
                let start = Instant::now();
                batch = p
                    .process(batch)
                    .await
                    .with_context(|| format!("processor {pid} failed"))?;
                histogram!(
                    "deltaforge_processor_latency_seconds",
                    "pipeline" => pipeline.to_string(),
                    "processor" => pid.to_string(),
                )
                .record(start.elapsed().as_secs_f64());
            }

            let last_cp = batch
                .iter()
                .rev()
                .find_map(|e| e.checkpoint.as_ref())
                .cloned();

            Ok(ProcessedBatch {
                events: batch,
                last_checkpoint: last_cp,
            })
        }
        .boxed()
    })
}

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use deltaforge_checkpoints::CheckpointStore;
use futures::future::BoxFuture;
use metrics::counter;
use tokio::time::{Instant, interval};
use tracing::{debug, warn};
use uuid::Uuid;

use deltaforge_config::{BatchConfig, CommitPolicy};
use deltaforge_core::{ArcDynProcessor, ArcDynSink, CheckpointMeta, Event, SinkError};

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
struct FrozenBatch {
    id: Uuid,
    started_at: Instant,
    events: Arc<[Event]>,
    bytes: usize,
}

fn event_size_hint(ev: &Event) -> usize {
    // Prefer exact if Event carries raw bytes; JSON fallback is fine.
    serde_json::to_vec(ev).map(|v| v.len()).unwrap_or(256)
}

/// something to fix later in the `Event`
fn is_tx_boundary(ev: &Event) -> bool {
    //ev.is_tx_boundary()
    true
}

fn policy_satisfied(
    policy: &Option<CommitPolicy>,
    required_total: usize,
    required_acks: usize,
    total_acks: usize,
) -> bool {
    match policy.clone().unwrap_or(CommitPolicy::Required) {
        CommitPolicy::All => total_acks == required_total,
        CommitPolicy::Required => required_acks == required_total,
        CommitPolicy::Quorum { quorum } => total_acks >= quorum,
    }
}

// For now, treat all sinks as required.
fn is_sink_required(_sink: &ArcDynSink) -> bool {
    true
}

pub struct Coordinator<Tok> {
    pipeline_name: String,
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
            pipeline_name: name,
            sinks,
            batch_cfg_eff,
            commit_policy,
            commit_cp,
            process_batch,
        }
    }

    /// Build - process (mutable) - freeze (Arc<[Event]>) - deliver - maybe commit.
    pub async fn run(
        mut self,
        mut event_rx: tokio::sync::mpsc::Receiver<Event>,
    ) -> Result<()> {
        let tick_ms = self.batch_cfg_eff.max_ms.unwrap_or(200);
        let mut ticker = interval(Duration::from_millis(tick_ms));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut building: Option<BuildingBatch> = None;

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Some(b) = building.take() {
                        if !b.raw.is_empty() && b.started_at.elapsed() >= Duration::from_millis(tick_ms) {
                            self.process_deliver_and_maybe_commit(b, "timer").await?;
                        } else {
                            building = Some(b);
                        }
                    }
                }

                maybe_ev = event_rx.recv() => {
                    let Some(ev) = maybe_ev else {
                        // channel closed — final flush
                        if let Some(b) = building.take() {
                            if !b.raw.is_empty() {
                                self.process_deliver_and_maybe_commit(b, "shutdown").await?;
                            }
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

                    let respect_tx = cfg.respect_source_tx.unwrap_or(true);
                    let boundary = is_tx_boundary(&ev);

                    // If limits hit, flush current (avoid splitting tx if configured).
                    if (limit_hit && !boundary) || (limit_hit && boundary && !b.raw.is_empty()) {
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
        let processed = (self.process_batch)(std::mem::take(&mut b.raw))
            .await
            .context("process batch")?;
        debug!(pipeline=%self.pipeline_name, processed_count=%processed.events.len(), "receveid events from processors");

        let last_cp = processed.last_checkpoint;
        // recompute size after processing
        let bytes = processed.events.iter().map(event_size_hint).sum();

        // 2) freeze
        let frozen = FrozenBatch {
            id: Uuid::new_v4(),
            started_at: b.started_at,
            bytes,
            events: Arc::<[Event]>::from(processed.events),
        };

        // 3) deliver to sinks
        let mut required_total = 0usize;
        let mut required_acks = 0usize;
        let mut total_acks = 0usize;

        debug!(pipeline=%self.pipeline_name, sink_count=self.sinks.len(), event_count=frozen.events.len(), "sending batch to sink(s)");
        for sink in &self.sinks {
            let required = is_sink_required(sink);
            if required {
                required_total += 1;
            }

            let mut ok = true;
            for ev in frozen.events.iter() {
                counter!("deltaforge_sink_events_total", "pipeline"=>self.pipeline_name.clone()).increment(1);

                debug!(pipeline=%self.pipeline_name, eid=%ev.event_id, "sending to sink");
                if let Err(e) = sink.send(ev.clone()).await {
                    tracing::warn!(
                        pipeline = %self.pipeline_name,
                        sink = %std::any::type_name::<ArcDynSink>(),
                        batch_id = %frozen.id,
                        kind = %e.kind(),
                        details = %e.details(),
                        error = ?e,
                        "sink failed"
                    );

                    counter!(
                        "deltaforge_sink_failures_total",
                        "pipeline" => self.pipeline_name.clone()
                    )
                    .increment(1);

                    ok = false;
                    break;
                }
            }

            if ok {
                total_acks += 1;
                if required {
                    required_acks += 1;
                }
            }
        }

        // 4) COMMIT checkpoint if policy satisfied (token from *processed* events)
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

    /// Compute the effective batch configuration by applying defaults.
    fn effective(cfg: &Option<BatchConfig>) -> BatchConfig {
        let mut out = BatchConfig::default();
        out.max_events =
            Some(cfg.as_ref().and_then(|c| c.max_events).unwrap_or(1000));
        out.max_bytes = Some(
            cfg.as_ref()
                .and_then(|c| c.max_bytes)
                .unwrap_or(8 * 1024 * 1024),
        );
        out.max_ms = Some(cfg.as_ref().and_then(|c| c.max_ms).unwrap_or(200));
        out.respect_source_tx = Some(
            cfg.as_ref()
                .and_then(|c| c.respect_source_tx)
                .unwrap_or(true),
        );
        out.max_inflight =
            Some(cfg.as_ref().and_then(|c| c.max_inflight).unwrap_or(1));
        out
    }
}

pub(crate) fn build_commit_fn(
    ckpt_store: Arc<dyn CheckpointStore>,
    pipeline_name: String,
) -> CommitCpFn<CheckpointMeta> {
    use futures::FutureExt;
    Box::new(move |cp: CheckpointMeta| {
        let ckpt_store = ckpt_store.clone();
        let pipeline_name = pipeline_name.clone();
        async move {
            let key = format!("pipeline/{pipeline_name}");
            let bytes = serde_json::to_vec(&cp)
                .context("serialize checkpoint to store")?;
            ckpt_store
                .put_raw(&key, &bytes)
                .await
                .context("save checkpoint")?;
            
            debug!(pipeline_id=%pipeline_name, checkpoint=?cp, "checkpoint saved");
            Ok(())
        }.boxed()
    })
}

pub(crate) fn build_batch_processor(
    processors: Arc<[ArcDynProcessor]>,
) -> ProcessBatchFn<CheckpointMeta> {
    use futures::FutureExt;

    let procs = Arc::clone(&processors);

    Arc::new(move |mut events: Vec<Event>| {
        let procs = Arc::clone(&procs);

        async move {
            if procs.is_empty() {
                let last_cp = events.iter().filter_map(|e| e.checkpoint.clone()).last();
                return Ok(ProcessedBatch { events, last_checkpoint: last_cp });
            }

            let mut out = Vec::with_capacity(events.len());
            let mut last_cp: Option<CheckpointMeta> = None;

            for mut ev in events.drain(..) {
                let mut dropped = false;
                let mut extras: Vec<Event> = Vec::new();

                for p in procs.iter() {
                    match p.process(&mut ev).await {
                        Ok(mut produced) => {
                            // append ALL extras; do not treat as replacement
                            for e2 in produced.drain(..) {
                                if let Some(cp) = e2.checkpoint.clone() { last_cp = Some(cp); }
                                extras.push(e2);
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error=?e, "processor failed; dropping original event");
                            dropped = true;
                            break;
                        }
                    }
                }

                if !dropped {
                    if let Some(cp) = ev.checkpoint.clone() { last_cp = Some(cp); }
                    out.push(ev);          // keep mutated original
                    out.extend(extras);    // plus all extras
                }
            }

            Ok(ProcessedBatch { events: out, last_checkpoint: last_cp })
        }
        .boxed()
    })
}

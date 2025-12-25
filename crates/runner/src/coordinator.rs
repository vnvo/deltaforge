//! Pipeline coordinator with batching, delivery, and schema sensing.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use checkpoints::CheckpointStore;
use futures::future::BoxFuture;
use metrics::{counter, histogram};
use parking_lot::Mutex;
use tokio::sync::watch;
use tokio::time::{Instant, interval};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid::Uuid;

use deltaforge_config::{BatchConfig, CommitPolicy, SchemaSensingConfig};
use deltaforge_core::{ArcDynProcessor, ArcDynSink, CheckpointMeta, Event};
use schema_sensing::{ObserveResult, SchemaSensor};

use crate::drift_detector::{DriftDetector, DriftSummary};
use crate::schema_provider::{ArcSchemaProvider, TableSchemaInfo};

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
    events: Arc<[Event]>,
    bytes: usize,
}

fn event_size_hint(ev: &Event) -> usize {
    ev.size_bytes
}

fn is_tx_boundary(ev: &Event) -> bool {
    ev.tx_end
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

fn is_sink_required(sink: &ArcDynSink) -> bool {
    sink.required()
}

/// Schema sensing state, wrapped for interior mutability.
pub struct SchemaSensorState {
    sensor: Mutex<SchemaSensor>,
    drift_detector: Mutex<DriftDetector>,
    config: SchemaSensingConfig,
}

impl SchemaSensorState {
    pub fn new(config: SchemaSensingConfig) -> Self {
        Self {
            sensor: Mutex::new(SchemaSensor::new(config.clone())),
            drift_detector: Mutex::new(DriftDetector::new()),
            config,
        }
    }

    /// Register a table schema for drift detection.
    pub fn register_table_schema(&self, schema: TableSchemaInfo) {
        self.drift_detector.lock().register_table(schema);
    }

    /// Observe a batch of events with guided JSON-only sensing.
    ///
    /// When `db_schemas` is provided, only JSON columns are deep-inspected,
    /// and drift detection runs against expected types.
    ///
    /// When `db_schemas` is None (schemaless sources), observes entire payload.
    ///
    /// Returns the number of schema evolutions detected.
    pub fn observe_and_enrich(
        &self,
        events: &mut [Event],
        db_schemas: Option<&HashMap<String, TableSchemaInfo>>,
    ) -> usize {
        if !self.config.enabled {
            return 0;
        }

        let mut evolutions = 0;
        let mut sensor = self.sensor.lock();
        let mut drift = self.drift_detector.lock();

        for event in events.iter_mut() {
            let Some(after) = &event.after else { continue };

            // Run drift detection if we have DB schema
            if let Some(schemas) = db_schemas {
                if let Some(_schema) = schemas.get(&event.table) {
                    drift.observe(&event.table, after);
                }
            }

            // Guided vs full sensing
            if let Some(schemas) = db_schemas {
                if let Some(schema) = schemas.get(&event.table) {
                    // Guided: only observe JSON columns
                    evolutions += self.observe_json_columns(
                        &mut sensor,
                        &event.table,
                        after,
                        schema,
                    );
                } else {
                    // No schema for this table - observe full payload
                    evolutions += self.observe_full_payload(
                        &mut sensor,
                        &event.table,
                        after,
                    );
                }
            } else {
                // No DB schemas at all - observe full payload
                evolutions +=
                    self.observe_full_payload(&mut sensor, &event.table, after);
            }

            // Enrich event with schema version info
            if let Some(version) = sensor.get_version(&event.table) {
                event.schema_version = Some(version.fingerprint.clone());
                event.schema_sequence = Some(version.sequence);
            }
        }

        evolutions
    }

    /// Observe only JSON columns in a row.
    fn observe_json_columns(
        &self,
        sensor: &mut SchemaSensor,
        table: &str,
        row: &serde_json::Value,
        schema: &TableSchemaInfo,
    ) -> usize {
        let Some(obj) = row.as_object() else {
            return 0;
        };

        let mut evolutions = 0;

        for col in schema.json_columns() {
            if let Some(json_value) = obj.get(&col.name) {
                // Skip null JSON columns
                if json_value.is_null() {
                    continue;
                }

                // Create a unique key for this table:column
                let key = format!("{}:{}", table, col.name);

                match sensor.observe_value(&key, json_value) {
                    Ok(result) => match &result {
                        ObserveResult::Evolved {
                            new_fingerprint,
                            new_sequence,
                            ..
                        } => {
                            evolutions += 1;
                            info!(
                                table = %table,
                                column = %col.name,
                                fingerprint = %new_fingerprint,
                                sequence = %new_sequence,
                                "JSON column schema evolved"
                            );
                        }
                        ObserveResult::NewSchema {
                            fingerprint,
                            sequence,
                        } => {
                            info!(
                                table = %table,
                                column = %col.name,
                                fingerprint = %fingerprint,
                                sequence = %sequence,
                                "new JSON column schema discovered"
                            );
                        }
                        _ => {}
                    },
                    Err(e) => {
                        warn!(
                            table = %table,
                            column = %col.name,
                            error = %e,
                            "JSON column schema sensing failed"
                        );
                    }
                }
                
            }
        }

        evolutions
    }

    /// Observe entire payload (for schemaless sources).
    fn observe_full_payload(
        &self,
        sensor: &mut SchemaSensor,
        table: &str,
        row: &serde_json::Value,
    ) -> usize {
        match sensor.observe_value(table, row) {
            Ok(result) => match &result {
                ObserveResult::Evolved {
                    new_fingerprint,
                    new_sequence,
                    ..
                } => {
                    info!(
                        table = %table,
                        fingerprint = %new_fingerprint,
                        sequence = %new_sequence,
                        "schema evolved"
                    );
                    1
                }
                ObserveResult::NewSchema {
                    fingerprint,
                    sequence,
                } => {
                    info!(
                        table = %table,
                        fingerprint = %fingerprint,
                        sequence = %sequence,
                        "new schema discovered"
                    );
                    0
                }
                _ => 0,
            },
            Err(e) => {
                warn!(table = %table, error = %e, "schema sensing failed");
                0
            }
        }
    }

    /// Get the underlying sensor for API access.
    pub fn sensor(&self) -> &Mutex<SchemaSensor> {
        &self.sensor
    }

    /// Get drift detector for API access.
    pub fn drift_detector(&self) -> &Mutex<DriftDetector> {
        &self.drift_detector
    }

    /// Check if drift has been detected.
    pub fn has_drift(&self) -> bool {
        self.drift_detector.lock().has_drift()
    }

    /// Get all drift summaries.
    pub fn drift_summaries(&self) -> Vec<DriftSummary> {
        self.drift_detector.lock().all_summaries()
    }
}

pub struct Coordinator<Tok> {
    pipeline_name: Arc<str>,
    sinks: Vec<ArcDynSink>,
    batch_cfg_eff: BatchConfig,
    commit_policy: Option<CommitPolicy>,
    commit_cp: CommitCpFn<Tok>,
    process_batch: ProcessBatchFn<Tok>,
    /// Optional schema sensing
    schema_sensor: Option<Arc<SchemaSensorState>>,
    /// Optional schema provider for guided sensing
    schema_provider: Option<ArcSchemaProvider>,
    /// Cached DB schemas for fast lookup during batch processing
    db_schema_cache: Mutex<HashMap<String, TableSchemaInfo>>,
}

pub struct CoordinatorBuilder<Tok> {
    pipeline_name: String,
    sinks: Vec<ArcDynSink>,
    batch_config: Option<BatchConfig>,
    commit_policy: Option<CommitPolicy>,
    commit_fn: Option<CommitCpFn<Tok>>,
    process_fn: Option<ProcessBatchFn<Tok>>,
    schema_sensor: Option<Arc<SchemaSensorState>>,
    schema_provider: Option<ArcSchemaProvider>,
}

impl<Tok: Send + 'static> CoordinatorBuilder<Tok> {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            pipeline_name: name.into(),
            sinks: Vec::new(),
            batch_config: None,
            commit_policy: None,
            commit_fn: None,
            process_fn: None,
            schema_sensor: None,
            schema_provider: None,
        }
    }

    pub fn sinks(mut self, sinks: Vec<ArcDynSink>) -> Self {
        self.sinks = sinks;
        self
    }

    pub fn batch_config(mut self, config: Option<BatchConfig>) -> Self {
        self.batch_config = config;
        self
    }

    pub fn commit_policy(mut self, policy: Option<CommitPolicy>) -> Self {
        self.commit_policy = policy;
        self
    }

    pub fn commit_fn(mut self, f: CommitCpFn<Tok>) -> Self {
        self.commit_fn = Some(f);
        self
    }

    pub fn process_fn(mut self, f: ProcessBatchFn<Tok>) -> Self {
        self.process_fn = Some(f);
        self
    }

    pub fn schema_sensor(mut self, sensor: Arc<SchemaSensorState>) -> Self {
        self.schema_sensor = Some(sensor);
        self
    }

    pub fn schema_provider(mut self, provider: ArcSchemaProvider) -> Self {
        self.schema_provider = Some(provider);
        self
    }

    pub fn build(self) -> Coordinator<Tok> {
        let batch_cfg_eff = Coordinator::<Tok>::effective(&self.batch_config);

        Coordinator {
            pipeline_name: self.pipeline_name.into(),
            sinks: self.sinks,
            batch_cfg_eff,
            commit_policy: self.commit_policy,
            commit_cp: self.commit_fn.expect("commit_fn is required"),
            process_batch: self.process_fn.expect("process_fn is required"),
            schema_sensor: self.schema_sensor,
            schema_provider: self.schema_provider,
            db_schema_cache: Mutex::new(HashMap::new()),
        }
    }
}

impl<Tok: Send + 'static> Coordinator<Tok> {
    pub fn builder(name: impl Into<String>) -> CoordinatorBuilder<Tok> {
        CoordinatorBuilder::new(name)
    }

    /// Get access to schema sensor state (for API exposure).
    pub fn schema_sensor(&self) -> Option<&Arc<SchemaSensorState>> {
        self.schema_sensor.as_ref()
    }

    fn effective(cfg: &Option<BatchConfig>) -> BatchConfig {
        let defaults = BatchConfig::default();
        match cfg {
            Some(c) => BatchConfig {
                max_events: c.max_events.or(defaults.max_events),
                max_bytes: c.max_bytes.or(defaults.max_bytes),
                max_ms: c.max_ms.or(defaults.max_ms),
                respect_source_tx: c
                    .respect_source_tx
                    .or(defaults.respect_source_tx),
                max_inflight: c.max_inflight.or(defaults.max_inflight),
            },
            None => defaults,
        }
    }

    /// Fetch and cache schema for a table from the provider.
    async fn get_or_fetch_schema(
        &self,
        table: &str,
    ) -> Option<TableSchemaInfo> {
        // Check cache first
        {
            let cache = self.db_schema_cache.lock();
            if let Some(schema) = cache.get(table) {
                return Some(schema.clone());
            }
        }

        // Fetch from provider
        let provider = self.schema_provider.as_ref()?;
        let schema = provider.get_table_schema(table).await?;

        // Register with drift detector
        if let Some(ref sensor) = self.schema_sensor {
            sensor.register_table_schema(schema.clone());
        }

        // Cache it
        {
            let mut cache = self.db_schema_cache.lock();
            cache.insert(table.to_string(), schema.clone());
        }

        Some(schema)
    }

    /// Build - process (mutable) - sense - freeze (Arc<[Event]>) - deliver - maybe commit.
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
                    if let Some(b) = building.take() {
                        if !b.raw.is_empty() {
                            self.process_deliver_and_maybe_commit(b, "cancelled").await?;
                        }
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
                        // Channel closed — final flush
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
                    let would_exceed_bytes = cfg.max_bytes.map(|m| b.bytes + event_size_hint(&ev) >= m).unwrap_or(false);
                    let limit_hit = would_exceed_events || would_exceed_bytes;

                    let boundary = is_tx_boundary(&ev);

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
        // 1) PROCESS: processors can modify/duplicate/drop events
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
            "received events from processors"
        );

        let last_cp = processed.last_checkpoint;

        // 2) SCHEMA SENSING: observe and enrich events
        let mut events = processed.events;
        if let Some(ref sensor_state) = self.schema_sensor {
            let sense_start = Instant::now();

            // Build schema map for guided sensing (if we have a provider)
            let db_schemas = if self.schema_provider.is_some() {
                // Collect unique table names in this batch
                let tables: std::collections::HashSet<&str> =
                    events.iter().map(|e| e.table.as_str()).collect();

                // Fetch schemas for each table
                let mut schema_map = HashMap::new();
                for table in tables {
                    if let Some(schema) = self.get_or_fetch_schema(table).await
                    {
                        schema_map.insert(table.to_string(), schema);
                    }
                }

                if schema_map.is_empty() {
                    None
                } else {
                    Some(schema_map)
                }
            } else {
                None
            };

            // Run sensing (guided if we have schemas, full if not)
            let evolutions = sensor_state
                .observe_and_enrich(&mut events, db_schemas.as_ref());

            histogram!(
                "deltaforge_stage_latency_seconds",
                "pipeline" => self.pipeline_name.to_string(),
                "stage" => "schema_sensing",
                "trigger" => reason.to_string()
            )
            .record(sense_start.elapsed().as_secs_f64());

            if evolutions > 0 {
                counter!(
                    "deltaforge_schema_evolutions_total",
                    "pipeline" => self.pipeline_name.to_string()
                )
                .increment(evolutions as u64);
            }

            // Check for drift and emit warning metric
            if sensor_state.has_drift() {
                counter!(
                    "deltaforge_schema_drift_detected",
                    "pipeline" => self.pipeline_name.to_string()
                )
                .increment(1);
            }
        }

        // Recompute size after processing
        let bytes: usize = events.iter().map(event_size_hint).sum();

        histogram!(
            "deltaforge_batch_events",
            "pipeline" => self.pipeline_name.to_string(),
        )
        .record(events.len() as f64);

        histogram!(
            "deltaforge_batch_bytes",
            "pipeline" => self.pipeline_name.to_string(),
        )
        .record(bytes as f64);

        // 3) FREEZE for zero-copy sharing
        let frozen = FrozenBatch {
            id: Uuid::new_v4(),
            bytes,
            events: Arc::<[Event]>::from(events),
        };

        // 4) DELIVER to sinks
        let mut required_total = 0usize;
        let mut required_acks = 0usize;
        let mut total_acks = 0usize;

        debug!(
            pipeline=%self.pipeline_name,
            sink_count=self.sinks.len(),
            event_count=frozen.events.len(),
            "sending batch to sink(s)"
        );

        for sink in &self.sinks {
            let required = is_sink_required(sink);
            if required {
                required_total += 1;
            }

            let sink_start = Instant::now();

            match sink.send_batch(&frozen.events).await {
                Ok(()) => {
                    total_acks += 1;
                    if required {
                        required_acks += 1;
                    }

                    counter!(
                        "deltaforge_sink_events_total",
                        "pipeline" => self.pipeline_name.to_string(),
                        "sink" => sink.id().to_string()
                    )
                    .increment(frozen.events.len() as u64);

                    histogram!(
                        "deltaforge_sink_latency_seconds",
                        "pipeline" => self.pipeline_name.to_string(),
                        "sink" => sink.id().to_string()
                    )
                    .record(sink_start.elapsed().as_secs_f64());
                }
                Err(e) => {
                    counter!(
                        "deltaforge_sink_errors_total",
                        "pipeline" => self.pipeline_name.to_string(),
                        "sink" => sink.id().to_string()
                    )
                    .increment(1);

                    warn!(
                        pipeline=%self.pipeline_name,
                        sink=%sink.id(),
                        error=%e,
                        "sink delivery failed"
                    );
                }
            }
        }

        // 5) COMMIT checkpoint if policy satisfied
        if policy_satisfied(
            &self.commit_policy,
            required_total,
            required_acks,
            total_acks,
        ) {
            if let Some(cp) = last_cp {
                let commit_start = Instant::now();
                (self.commit_cp)(cp).await.context("commit checkpoint")?;

                histogram!(
                    "deltaforge_stage_latency_seconds",
                    "pipeline" => self.pipeline_name.to_string(),
                    "stage" => "commit",
                    "trigger" => reason.to_string()
                )
                .record(commit_start.elapsed().as_secs_f64());

                counter!(
                    "deltaforge_checkpoints_total",
                    "pipeline" => self.pipeline_name.to_string()
                )
                .increment(1);
            }
        } else {
            warn!(
                pipeline=%self.pipeline_name,
                required_total=%required_total,
                required_acks=%required_acks,
                total_acks=%total_acks,
                "commit policy not satisfied, checkpoint not saved"
            );
        }

        Ok(())
    }
}

/// Build the commit function for checkpoints.
pub fn build_commit_fn(
    store: Arc<dyn CheckpointStore>,
    key: String,
) -> CommitCpFn<CheckpointMeta> {
    use futures::FutureExt;

    Box::new(move |cp: CheckpointMeta| {
        let store = Arc::clone(&store);
        let key = key.clone();
        async move {
            store.put_raw(&key, cp.as_bytes()).await?;
            debug!(checkpoint_key=%key, bytes=cp.as_bytes().len(), "checkpoint saved");
            Ok(())
        }
        .boxed()
    })
}

/// Build the batch processor function.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_effective_batch_config_defaults() {
        let eff = Coordinator::<CheckpointMeta>::effective(&None);
        assert!(eff.max_events.is_some());
        assert!(eff.max_bytes.is_some());
        assert!(eff.max_ms.is_some());
    }

    #[test]
    fn test_effective_batch_config_override() {
        let cfg = Some(BatchConfig {
            max_events: Some(500),
            max_bytes: None,
            max_ms: Some(100),
            respect_source_tx: None,
            max_inflight: None,
        });

        let eff = Coordinator::<CheckpointMeta>::effective(&cfg);
        assert_eq!(eff.max_events, Some(500));
        assert_eq!(eff.max_ms, Some(100));
        // Defaults should fill in
        assert!(eff.max_bytes.is_some());
    }
}

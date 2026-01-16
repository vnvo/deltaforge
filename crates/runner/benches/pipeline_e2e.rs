//! End-to-end pipeline benchmarks using actual Coordinator
//!
//! Run with: cargo bench -p runner --bench pipeline_e2e
//!
//! # Benchmark Groups
//!
//! - `coordinator_throughput`: Core coordinator overhead with various sink configs
//! - `batch_size_impact`: How batch sizing affects throughput
//! - `multi_sink_scaling`: Performance with multiple parallel sinks
//! - `event_size_impact`: Small/medium/large event payload overhead
//! - `envelope_overhead`: Serialization cost of Native vs Debezium vs CloudEvents
//! - `schema_sensing_*`: Schema inference overhead benchmarks

use criterion::{
    BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use futures::FutureExt;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use deltaforge_config::{BatchConfig, SamplingConfig, SchemaSensingConfig};
use deltaforge_core::{
    ArcDynSink, CheckpointMeta, Event, Op, Sink, SinkResult, SourceInfo,
    SourcePosition,
    envelope::{Envelope, EnvelopeType},
};
use serde_json::json;

use runner::{
    CommitCpFn, Coordinator, ProcessBatchFn, ProcessedBatch, SchemaSensorState,
};

// =============================================================================
// Test Sinks
// =============================================================================

/// Sink that just counts events (measures pure coordinator overhead).
struct CountingSink {
    id: String,
    required: bool,
    event_count: Arc<AtomicU64>,
    batch_count: Arc<AtomicU64>,
}

impl CountingSink {
    fn new(id: &str) -> (Arc<Self>, Arc<AtomicU64>, Arc<AtomicU64>) {
        let event_count = Arc::new(AtomicU64::new(0));
        let batch_count = Arc::new(AtomicU64::new(0));
        (
            Arc::new(Self {
                id: id.to_string(),
                required: true,
                event_count: event_count.clone(),
                batch_count: batch_count.clone(),
            }),
            event_count,
            batch_count,
        )
    }
}

#[async_trait]
impl Sink for CountingSink {
    fn id(&self) -> &str {
        &self.id
    }

    fn required(&self) -> bool {
        self.required
    }

    async fn send(&self, _event: &Event) -> SinkResult<()> {
        self.event_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn send_batch(&self, events: &[Event]) -> SinkResult<()> {
        self.event_count
            .fetch_add(events.len() as u64, Ordering::Relaxed);
        self.batch_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

/// Sink that serializes events using direct JSON (baseline).
struct SerializingSink {
    id: String,
    required: bool,
    bytes_written: Arc<AtomicU64>,
}

impl SerializingSink {
    fn new(id: &str) -> (Arc<Self>, Arc<AtomicU64>) {
        let bytes_written = Arc::new(AtomicU64::new(0));
        (
            Arc::new(Self {
                id: id.to_string(),
                required: true,
                bytes_written: bytes_written.clone(),
            }),
            bytes_written,
        )
    }
}

#[async_trait]
impl Sink for SerializingSink {
    fn id(&self) -> &str {
        &self.id
    }

    fn required(&self) -> bool {
        self.required
    }

    async fn send(&self, event: &Event) -> SinkResult<()> {
        let data = serde_json::to_vec(event)
            .map_err(|e| deltaforge_core::SinkError::Other(e.into()))?;
        self.bytes_written
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    async fn send_batch(&self, events: &[Event]) -> SinkResult<()> {
        let mut total = 0u64;
        for event in events {
            let data = serde_json::to_vec(event)
                .map_err(|e| deltaforge_core::SinkError::Other(e.into()))?;
            total += data.len() as u64;
        }
        self.bytes_written.fetch_add(total, Ordering::Relaxed);
        Ok(())
    }
}

/// Sink that serializes events using configurable envelope format.
///
/// This provides realistic benchmarks for different envelope configurations
/// that sinks would use in production.
struct EnvelopeSink {
    id: String,
    required: bool,
    envelope: Box<dyn Envelope>,
    bytes_written: Arc<AtomicU64>,
    event_count: Arc<AtomicU64>,
}

impl EnvelopeSink {
    fn new(
        id: &str,
        envelope_type: EnvelopeType,
    ) -> (Arc<Self>, Arc<AtomicU64>, Arc<AtomicU64>) {
        let bytes_written = Arc::new(AtomicU64::new(0));
        let event_count = Arc::new(AtomicU64::new(0));
        (
            Arc::new(Self {
                id: id.to_string(),
                required: true,
                envelope: envelope_type.build(),
                bytes_written: bytes_written.clone(),
                event_count: event_count.clone(),
            }),
            bytes_written,
            event_count,
        )
    }

    fn native(id: &str) -> (Arc<Self>, Arc<AtomicU64>, Arc<AtomicU64>) {
        Self::new(id, EnvelopeType::Native)
    }

    fn debezium(id: &str) -> (Arc<Self>, Arc<AtomicU64>, Arc<AtomicU64>) {
        Self::new(id, EnvelopeType::Debezium)
    }

    fn cloudevents(
        id: &str,
        type_prefix: &str,
    ) -> (Arc<Self>, Arc<AtomicU64>, Arc<AtomicU64>) {
        Self::new(
            id,
            EnvelopeType::CloudEvents {
                type_prefix: type_prefix.to_string(),
            },
        )
    }
}

#[async_trait]
impl Sink for EnvelopeSink {
    fn id(&self) -> &str {
        &self.id
    }

    fn required(&self) -> bool {
        self.required
    }

    async fn send(&self, event: &Event) -> SinkResult<()> {
        let data = self
            .envelope
            .serialize(event)
            .map_err(|e| deltaforge_core::SinkError::Other(e.into()))?;
        self.bytes_written
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        self.event_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn send_batch(&self, events: &[Event]) -> SinkResult<()> {
        let mut total_bytes = 0u64;
        for event in events {
            let data = self
                .envelope
                .serialize(event)
                .map_err(|e| deltaforge_core::SinkError::Other(e.into()))?;
            total_bytes += data.len() as u64;
        }
        self.bytes_written.fetch_add(total_bytes, Ordering::Relaxed);
        self.event_count
            .fetch_add(events.len() as u64, Ordering::Relaxed);
        Ok(())
    }
}

// =============================================================================
// Event Factories (Debezium-compatible format)
// =============================================================================

/// Create a SourceInfo for benchmark events.
fn make_source_info(db: &str, table: &str, connector: &str) -> SourceInfo {
    SourceInfo {
        version: "1.0.0".into(),
        connector: connector.into(),
        name: "bench-source".into(),
        ts_ms: 1_700_000_000_000,
        db: db.into(),
        schema: None,
        table: table.into(),
        snapshot: None,
        position: SourcePosition::default(),
    }
}

/// Generate standard benchmark events.
fn make_events(count: usize) -> Vec<Event> {
    (0..count)
        .map(|i| {
            let mut ev = Event::new_row(
                make_source_info("benchdb", "events", "bench"),
                Op::Create,
                None,
                Some(json!({
                    "id": i,
                    "name": format!("event-{}", i),
                    "data": { "field1": "value1", "field2": 12345 }
                })),
                1_700_000_000_000,
                256,
            );
            ev.checkpoint = Some(CheckpointMeta::from_vec(
                format!("pos-{}", i).into_bytes(),
            ));
            ev
        })
        .collect()
}

/// Events with JSON columns for schema sensing benchmarks.
/// Uses homogeneous structure for cache hit testing.
fn make_events_with_json(count: usize) -> Vec<Event> {
    (0..count)
        .map(|i| {
            let mut ev = Event::new_row(
                make_source_info("shop", "orders", "mysql"),
                Op::Create,
                None,
                Some(json!({
                    "id": i,
                    "customer_id": i % 1000,
                    "status": "pending",
                    "amount": 99.99 + (i as f64 * 0.01),
                    "metadata": {
                        "source": "web",
                        "campaign": format!("campaign-{}", i % 10),
                        "tags": ["promo", "featured"],
                        "tracking": {
                            "click_id": format!("click-{}", i),
                            "session_id": format!("sess-{}", i % 100)
                        }
                    },
                    "line_items": [
                        {"sku": "SKU-001", "qty": 1, "price": 49.99},
                        {"sku": "SKU-002", "qty": 2, "price": 25.00}
                    ]
                })),
                1_700_000_000_000,
                512,
            );
            ev.checkpoint = Some(CheckpointMeta::from_vec(
                format!("pos-{}", i).into_bytes(),
            ));
            ev
        })
        .collect()
}

/// Events with varying JSON structures (for cache miss testing).
fn make_events_heterogeneous(count: usize) -> Vec<Event> {
    (0..count)
        .map(|i| {
            // Vary structure based on i to prevent cache hits
            let payload = match i % 3 {
                0 => json!({
                    "id": i,
                    "type": "order",
                    "total": 99.99
                }),
                1 => json!({
                    "id": i,
                    "type": "refund",
                    "amount": 50.00,
                    "reason": "damaged"
                }),
                _ => json!({
                    "id": i,
                    "type": "adjustment",
                    "delta": -10.00,
                    "applied_by": "system",
                    "timestamp": "2024-01-15T10:30:00Z"
                }),
            };

            let mut ev = Event::new_row(
                make_source_info("shop", "transactions", "mysql"),
                Op::Create,
                None,
                Some(payload),
                1_700_000_000_000,
                256,
            );
            ev.checkpoint = Some(CheckpointMeta::from_vec(
                format!("pos-{}", i).into_bytes(),
            ));
            ev
        })
        .collect()
}

/// Small events (minimal payload).
fn make_small_events(count: usize) -> Vec<Event> {
    (0..count)
        .map(|i| {
            Event::new_row(
                make_source_info("d", "t", "b"),
                Op::Create,
                None,
                Some(json!({"id": i})),
                1_700_000_000_000,
                32,
            )
        })
        .collect()
}

/// Medium events (typical CRUD payload).
fn make_medium_events(count: usize) -> Vec<Event> {
    (0..count)
        .map(|i| {
            Event::new_row(
                make_source_info("shop", "orders", "mysql"),
                Op::Update,
                Some(json!({"id": i, "status": "pending", "amount": 99.99})),
                Some(json!({"id": i, "status": "shipped", "amount": 99.99, "shipped_at": "2024-01-15"})),
                1_700_000_000_000,
                256,
            )
        })
        .collect()
}

/// Large events (nested arrays, complex payloads).
fn make_large_events(count: usize) -> Vec<Event> {
    (0..count)
        .map(|i| {
            let big_data: Vec<_> = (0..20)
                .map(|j| json!({"idx": j, "data": "x".repeat(50)}))
                .collect();
            Event::new_row(
                make_source_info("warehouse", "inventory", "mysql"),
                Op::Create,
                None,
                Some(json!({"id": i, "items": big_data})),
                1_700_000_000_000,
                2048,
            )
        })
        .collect()
}

// =============================================================================
// Coordinator Helpers
// =============================================================================

fn noop_commit_fn() -> CommitCpFn<CheckpointMeta> {
    Box::new(|_cp: CheckpointMeta| async { Ok(()) }.boxed())
}

fn noop_process_fn() -> ProcessBatchFn<CheckpointMeta> {
    Arc::new(|events: Vec<Event>| {
        async move {
            let last_cp = events
                .iter()
                .rev()
                .find_map(|e| e.checkpoint.as_ref())
                .cloned();
            Ok(ProcessedBatch {
                events,
                last_checkpoint: last_cp,
            })
        }
        .boxed()
    })
}

fn make_batch_config(max_events: usize, max_ms: u64) -> Option<BatchConfig> {
    Some(BatchConfig {
        max_events: Some(max_events),
        max_bytes: Some(64 * 1024 * 1024),
        max_ms: Some(max_ms),
        respect_source_tx: Some(false),
        max_inflight: Some(1),
    })
}

fn make_sensing_config_with_cache(cache_enabled: bool) -> SchemaSensingConfig {
    SchemaSensingConfig {
        enabled: true,
        sampling: SamplingConfig {
            structure_cache: cache_enabled,
            structure_cache_size: 100,
            warmup_events: 100,
            sample_rate: 1, // No sampling, just cache
        },
        ..Default::default()
    }
}

fn make_sensing_config_with_sampling(
    warmup: usize,
    rate: usize,
) -> SchemaSensingConfig {
    SchemaSensingConfig {
        enabled: true,
        sampling: SamplingConfig {
            structure_cache: true,
            structure_cache_size: 100,
            warmup_events: warmup,
            sample_rate: rate,
        },
        ..Default::default()
    }
}

async fn run_coordinator_bench(
    events: Vec<Event>,
    sinks: Vec<ArcDynSink>,
    batch_config: Option<BatchConfig>,
) -> Result<()> {
    let (tx, rx) = mpsc::channel::<Event>(events.len() + 100);
    let cancel = CancellationToken::new();
    let (_pause_tx, pause_rx) = watch::channel(false);

    let coord = Coordinator::builder("bench-pipeline")
        .sinks(sinks)
        .batch_config(batch_config)
        .commit_fn(noop_commit_fn())
        .process_fn(noop_process_fn())
        .build();

    for ev in events {
        tx.send(ev).await?;
    }
    drop(tx);

    coord.run(rx, cancel, pause_rx).await?;

    Ok(())
}

async fn run_coordinator_bench_with_sensing(
    events: Vec<Event>,
    sinks: Vec<ArcDynSink>,
    batch_config: Option<BatchConfig>,
    sensing_config: SchemaSensingConfig,
) -> Result<()> {
    let (tx, rx) = mpsc::channel::<Event>(events.len() + 100);
    let cancel = CancellationToken::new();
    let (_pause_tx, pause_rx) = watch::channel(false);

    let sensor = Arc::new(SchemaSensorState::new(sensing_config));

    let coord = Coordinator::builder("bench-pipeline")
        .sinks(sinks)
        .batch_config(batch_config)
        .commit_fn(noop_commit_fn())
        .process_fn(noop_process_fn())
        .schema_sensor(sensor)
        .build();

    for ev in events {
        tx.send(ev).await?;
    }
    drop(tx);

    coord.run(rx, cancel, pause_rx).await?;

    Ok(())
}

// =============================================================================
// Benchmarks
// =============================================================================

fn bench_coordinator_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("coordinator_throughput");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(8));

    for event_count in [1_000, 10_000, 50_000] {
        let events = make_events(event_count);
        group.throughput(Throughput::Elements(event_count as u64));

        group.bench_with_input(
            BenchmarkId::new("1_sink_counting", event_count),
            &events,
            |b, evs| {
                b.to_async(&rt).iter(|| async {
                    let (sink, event_count, _) = CountingSink::new("sink-1");
                    run_coordinator_bench(
                        evs.clone(),
                        vec![sink],
                        make_batch_config(100, 10),
                    )
                    .await
                    .unwrap();
                    assert_eq!(
                        event_count.load(Ordering::Relaxed),
                        evs.len() as u64
                    );
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("2_sinks_counting", event_count),
            &events,
            |b, evs| {
                b.to_async(&rt).iter(|| async {
                    let (sink1, count1, _) = CountingSink::new("sink-1");
                    let (sink2, count2, _) = CountingSink::new("sink-2");
                    run_coordinator_bench(
                        evs.clone(),
                        vec![sink1, sink2],
                        make_batch_config(100, 10),
                    )
                    .await
                    .unwrap();
                    assert_eq!(
                        count1.load(Ordering::Relaxed),
                        evs.len() as u64
                    );
                    assert_eq!(
                        count2.load(Ordering::Relaxed),
                        evs.len() as u64
                    );
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("1_sink_serializing", event_count),
            &events,
            |b, evs| {
                b.to_async(&rt).iter(|| async {
                    let (sink, bytes) = SerializingSink::new("sink-ser");
                    run_coordinator_bench(
                        evs.clone(),
                        vec![sink],
                        make_batch_config(100, 10),
                    )
                    .await
                    .unwrap();
                    assert!(bytes.load(Ordering::Relaxed) > 0);
                })
            },
        );
    }

    group.finish();
}

fn bench_batch_sizes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("batch_size_impact");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(6));

    let events = make_events(10_000);
    group.throughput(Throughput::Elements(10_000));

    for batch_size in [10, 50, 100, 500, 1000] {
        group.bench_with_input(
            BenchmarkId::new("batch_size", batch_size),
            &events,
            |b, evs| {
                b.to_async(&rt).iter(|| async {
                    let (sink, _, batch_count) = CountingSink::new("sink");
                    run_coordinator_bench(
                        evs.clone(),
                        vec![sink],
                        make_batch_config(batch_size, 1000),
                    )
                    .await
                    .unwrap();
                    let batches = batch_count.load(Ordering::Relaxed);
                    assert!(batches > 0);
                })
            },
        );
    }

    group.finish();
}

fn bench_multi_sink_scaling(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("multi_sink_scaling");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(6));

    let events = make_events(10_000);
    group.throughput(Throughput::Elements(10_000));

    for sink_count in [1, 2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::new("sink_count", sink_count),
            &events,
            |b, evs| {
                b.to_async(&rt).iter(|| async {
                    let sinks: Vec<ArcDynSink> = (0..sink_count)
                        .map(|i| {
                            let (sink, _, _) =
                                CountingSink::new(&format!("sink-{}", i));
                            sink as ArcDynSink
                        })
                        .collect();

                    run_coordinator_bench(
                        evs.clone(),
                        sinks,
                        make_batch_config(100, 10),
                    )
                    .await
                    .unwrap();
                })
            },
        );
    }

    group.finish();
}

fn bench_event_sizes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("event_size_impact");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(6));

    let small_events = make_small_events(5_000);
    let medium_events = make_medium_events(5_000);
    let large_events = make_large_events(5_000);

    group.throughput(Throughput::Elements(5_000));

    group.bench_function("small_events", |b| {
        b.to_async(&rt).iter(|| async {
            let (sink, _, _) = CountingSink::new("sink");
            run_coordinator_bench(
                small_events.clone(),
                vec![sink],
                make_batch_config(100, 10),
            )
            .await
            .unwrap();
        })
    });

    group.bench_function("medium_events", |b| {
        b.to_async(&rt).iter(|| async {
            let (sink, _, _) = CountingSink::new("sink");
            run_coordinator_bench(
                medium_events.clone(),
                vec![sink],
                make_batch_config(100, 10),
            )
            .await
            .unwrap();
        })
    });

    group.bench_function("large_events", |b| {
        b.to_async(&rt).iter(|| async {
            let (sink, _, _) = CountingSink::new("sink");
            run_coordinator_bench(
                large_events.clone(),
                vec![sink],
                make_batch_config(100, 10),
            )
            .await
            .unwrap();
        })
    });

    group.finish();
}

/// Benchmark envelope serialization overhead.
///
/// Compares Native (direct), Debezium (payload wrapper), and CloudEvents formats.
fn bench_envelope_overhead(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("envelope_overhead");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(8));

    let events = make_events(10_000);
    group.throughput(Throughput::Elements(10_000));

    // Native envelope (baseline - direct Event serialization)
    group.bench_with_input(
        BenchmarkId::new("envelope", "native"),
        &events,
        |b, evs| {
            b.to_async(&rt).iter(|| async {
                let (sink, bytes, count) = EnvelopeSink::native("sink-native");
                run_coordinator_bench(
                    evs.clone(),
                    vec![sink],
                    make_batch_config(100, 10),
                )
                .await
                .unwrap();
                assert_eq!(count.load(Ordering::Relaxed), evs.len() as u64);
                assert!(bytes.load(Ordering::Relaxed) > 0);
            })
        },
    );

    // Debezium envelope (payload wrapper)
    group.bench_with_input(
        BenchmarkId::new("envelope", "debezium"),
        &events,
        |b, evs| {
            b.to_async(&rt).iter(|| async {
                let (sink, bytes, count) =
                    EnvelopeSink::debezium("sink-debezium");
                run_coordinator_bench(
                    evs.clone(),
                    vec![sink],
                    make_batch_config(100, 10),
                )
                .await
                .unwrap();
                assert_eq!(count.load(Ordering::Relaxed), evs.len() as u64);
                assert!(bytes.load(Ordering::Relaxed) > 0);
            })
        },
    );

    // CloudEvents envelope (restructured format)
    group.bench_with_input(
        BenchmarkId::new("envelope", "cloudevents"),
        &events,
        |b, evs| {
            b.to_async(&rt).iter(|| async {
                let (sink, bytes, count) =
                    EnvelopeSink::cloudevents("sink-ce", "com.example.cdc");
                run_coordinator_bench(
                    evs.clone(),
                    vec![sink],
                    make_batch_config(100, 10),
                )
                .await
                .unwrap();
                assert_eq!(count.load(Ordering::Relaxed), evs.len() as u64);
                assert!(bytes.load(Ordering::Relaxed) > 0);
            })
        },
    );

    group.finish();
}

/// Benchmark envelope serialization with different event sizes.
fn bench_envelope_with_event_sizes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("envelope_event_sizes");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(6));

    let small = make_small_events(5_000);
    let medium = make_medium_events(5_000);
    let large = make_large_events(5_000);

    group.throughput(Throughput::Elements(5_000));

    // Test each envelope with different payload sizes
    for (name, events) in
        [("small", &small), ("medium", &medium), ("large", &large)]
    {
        group.bench_with_input(
            BenchmarkId::new("native", name),
            events,
            |b, evs| {
                b.to_async(&rt).iter(|| async {
                    let (sink, _, _) = EnvelopeSink::native("sink");
                    run_coordinator_bench(
                        evs.clone(),
                        vec![sink],
                        make_batch_config(100, 10),
                    )
                    .await
                    .unwrap();
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("debezium", name),
            events,
            |b, evs| {
                b.to_async(&rt).iter(|| async {
                    let (sink, _, _) = EnvelopeSink::debezium("sink");
                    run_coordinator_bench(
                        evs.clone(),
                        vec![sink],
                        make_batch_config(100, 10),
                    )
                    .await
                    .unwrap();
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("cloudevents", name),
            events,
            |b, evs| {
                b.to_async(&rt).iter(|| async {
                    let (sink, _, _) =
                        EnvelopeSink::cloudevents("sink", "com.example");
                    run_coordinator_bench(
                        evs.clone(),
                        vec![sink],
                        make_batch_config(100, 10),
                    )
                    .await
                    .unwrap();
                })
            },
        );
    }

    group.finish();
}

/// Benchmark multi-sink with different envelope configurations.
///
/// Real-world scenario: same events going to Kafka (Debezium) and
/// webhook (CloudEvents) simultaneously.
fn bench_multi_sink_mixed_envelopes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("multi_sink_mixed_envelopes");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(6));

    let events = make_events(10_000);
    group.throughput(Throughput::Elements(10_000));

    // Single sink baselines
    group.bench_with_input(
        BenchmarkId::new("single", "native"),
        &events,
        |b, evs| {
            b.to_async(&rt).iter(|| async {
                let (sink, _, _) = EnvelopeSink::native("sink");
                run_coordinator_bench(
                    evs.clone(),
                    vec![sink],
                    make_batch_config(100, 10),
                )
                .await
                .unwrap();
            })
        },
    );

    // Dual sink: same envelope
    group.bench_with_input(
        BenchmarkId::new("dual", "both_native"),
        &events,
        |b, evs| {
            b.to_async(&rt).iter(|| async {
                let (sink1, _, _) = EnvelopeSink::native("sink1");
                let (sink2, _, _) = EnvelopeSink::native("sink2");
                run_coordinator_bench(
                    evs.clone(),
                    vec![sink1, sink2],
                    make_batch_config(100, 10),
                )
                .await
                .unwrap();
            })
        },
    );

    // Dual sink: mixed envelopes (Kafka + webhook scenario)
    group.bench_with_input(
        BenchmarkId::new("dual", "debezium_plus_cloudevents"),
        &events,
        |b, evs| {
            b.to_async(&rt).iter(|| async {
                let (kafka_sink, _, _) = EnvelopeSink::debezium("kafka");
                let (webhook_sink, _, _) =
                    EnvelopeSink::cloudevents("webhook", "com.example.cdc");
                run_coordinator_bench(
                    evs.clone(),
                    vec![kafka_sink, webhook_sink],
                    make_batch_config(100, 10),
                )
                .await
                .unwrap();
            })
        },
    );

    // Triple sink: all three envelope types
    group.bench_with_input(
        BenchmarkId::new("triple", "all_envelope_types"),
        &events,
        |b, evs| {
            b.to_async(&rt).iter(|| async {
                let (native, _, _) = EnvelopeSink::native("native");
                let (debezium, _, _) = EnvelopeSink::debezium("debezium");
                let (cloudevents, _, _) =
                    EnvelopeSink::cloudevents("cloudevents", "com.example");
                run_coordinator_bench(
                    evs.clone(),
                    vec![native, debezium, cloudevents],
                    make_batch_config(100, 10),
                )
                .await
                .unwrap();
            })
        },
    );

    group.finish();
}

fn bench_schema_sensing(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("schema_sensing_overhead");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(8));

    let events = make_events_with_json(10_000);
    group.throughput(Throughput::Elements(10_000));

    // Baseline: no schema sensing
    group.bench_with_input(
        BenchmarkId::new("sensing", "disabled"),
        &events,
        |b, evs| {
            b.to_async(&rt).iter(|| async {
                let (sink, _, _) = CountingSink::new("sink");
                run_coordinator_bench(
                    evs.clone(),
                    vec![sink],
                    make_batch_config(100, 10),
                )
                .await
                .unwrap();
            })
        },
    );

    // With schema sensing enabled (no optimizations)
    group.bench_with_input(
        BenchmarkId::new("sensing", "enabled_no_cache"),
        &events,
        |b, evs| {
            b.to_async(&rt).iter(|| async {
                let (sink, _, _) = CountingSink::new("sink");
                run_coordinator_bench_with_sensing(
                    evs.clone(),
                    vec![sink],
                    make_batch_config(100, 10),
                    make_sensing_config_with_cache(false),
                )
                .await
                .unwrap();
            })
        },
    );

    // With structure cache (homogeneous data - high cache hit rate)
    group.bench_with_input(
        BenchmarkId::new("sensing", "enabled_with_cache"),
        &events,
        |b, evs| {
            b.to_async(&rt).iter(|| async {
                let (sink, _, _) = CountingSink::new("sink");
                run_coordinator_bench_with_sensing(
                    evs.clone(),
                    vec![sink],
                    make_batch_config(100, 10),
                    make_sensing_config_with_cache(true),
                )
                .await
                .unwrap();
            })
        },
    );

    // With sampling (10% after warmup)
    group.bench_with_input(
        BenchmarkId::new("sensing", "enabled_with_sampling"),
        &events,
        |b, evs| {
            b.to_async(&rt).iter(|| async {
                let (sink, _, _) = CountingSink::new("sink");
                run_coordinator_bench_with_sensing(
                    evs.clone(),
                    vec![sink],
                    make_batch_config(100, 10),
                    make_sensing_config_with_sampling(100, 10),
                )
                .await
                .unwrap();
            })
        },
    );

    group.finish();
}

fn bench_schema_sensing_heterogeneous(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("schema_sensing_heterogeneous");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(8));

    let events = make_events_heterogeneous(10_000);
    group.throughput(Throughput::Elements(10_000));

    // Cache with heterogeneous data (low cache hit rate)
    group.bench_with_input(
        BenchmarkId::new("heterogeneous", "with_cache"),
        &events,
        |b, evs| {
            b.to_async(&rt).iter(|| async {
                let (sink, _, _) = CountingSink::new("sink");
                run_coordinator_bench_with_sensing(
                    evs.clone(),
                    vec![sink],
                    make_batch_config(100, 10),
                    make_sensing_config_with_cache(true),
                )
                .await
                .unwrap();
            })
        },
    );

    // Sampling helps more with heterogeneous data
    group.bench_with_input(
        BenchmarkId::new("heterogeneous", "with_sampling"),
        &events,
        |b, evs| {
            b.to_async(&rt).iter(|| async {
                let (sink, _, _) = CountingSink::new("sink");
                run_coordinator_bench_with_sensing(
                    evs.clone(),
                    vec![sink],
                    make_batch_config(100, 10),
                    make_sensing_config_with_sampling(100, 10),
                )
                .await
                .unwrap();
            })
        },
    );

    group.finish();
}

fn bench_schema_sensing_scaling(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("schema_sensing_scaling");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(8));

    for event_count in [1_000, 5_000, 10_000, 25_000] {
        let events = make_events_with_json(event_count);
        group.throughput(Throughput::Elements(event_count as u64));

        group.bench_with_input(
            BenchmarkId::new("events_with_sensing", event_count),
            &events,
            |b, evs| {
                b.to_async(&rt).iter(|| async {
                    let (sink, _, _) = CountingSink::new("sink");
                    run_coordinator_bench_with_sensing(
                        evs.clone(),
                        vec![sink],
                        make_batch_config(100, 10),
                        make_sensing_config_with_sampling(100, 10),
                    )
                    .await
                    .unwrap();
                })
            },
        );
    }

    group.finish();
}

// =============================================================================
// Criterion Configuration
// =============================================================================

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(30)
        .measurement_time(Duration::from_secs(12))
        .warm_up_time(Duration::from_secs(5));
    targets =
        bench_coordinator_throughput,
        bench_batch_sizes,
        bench_multi_sink_scaling,
        bench_event_sizes,
        bench_envelope_overhead,
        bench_envelope_with_event_sizes,
        bench_multi_sink_mixed_envelopes,
        bench_schema_sensing,
        bench_schema_sensing_heterogeneous,
        bench_schema_sensing_scaling
}

criterion_main!(benches);

// =============================================================================
// Throughput Summary (printed at end of benchmarks)
// =============================================================================

#[cfg(test)]
mod summary {
    //! Run `cargo test --release -p runner --test pipeline_e2e -- --nocapture summary`
    //! to see the throughput summary.

    #[test]
    #[ignore]
    fn print_throughput_summary() {
        println!("\n");
        println!("Benchmark Groups:");
        println!("  - coordinator_throughput: Core coordinator overhead");
        println!("  - batch_size_impact: Batch sizing effects");
        println!("  - multi_sink_scaling: Parallel sink performance");
        println!("  - event_size_impact: Payload size overhead");
        println!("  - envelope_overhead: Native vs Debezium vs CloudEvents");
        println!("  - envelope_event_sizes: Envelope cost by payload size");
        println!(
            "  - multi_sink_mixed_envelopes: Real-world multi-format scenarios"
        );
        println!("  - schema_sensing_*: Schema inference overhead");
        println!();
        println!("Notes:");
        println!(
            "  - Benchmarks measure coordinator throughput (in-memory → sinks)"
        );
        println!("  - Real-world includes binlog parsing, network I/O");
        println!(
            "  - Native envelope is baseline; Debezium adds ~12 bytes wrapper"
        );
        println!(
            "  - CloudEvents restructures event (higher overhead, serverless compat)"
        );
        println!(
            "  - Structure cache uses top-level keys only (O(k) not O(nodes))"
        );
        println!(
            "  - Sampling recommended for production: warmup=1000, rate=10"
        );
        println!(
            "  - Cache effective for homogeneous data (same column structure)"
        );
        println!("\n");
    }
}

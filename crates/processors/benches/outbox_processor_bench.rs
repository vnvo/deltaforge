//! Outbox Processor benchmarks.
//!
//! Measures outbox transform overhead: topic/key resolution, header extraction,
//! payload rewriting, and mixed CDC+outbox passthrough.
//!
//! Run with: cargo bench -p processors --bench outbox_processor_bench

use criterion::{
    Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use deltaforge_config::{
    OUTBOX_SCHEMA_SENTINEL, OutboxColumns, OutboxProcessorCfg,
};
use deltaforge_core::{Event, Op, Processor, SourceInfo, SourcePosition};
use once_cell::sync::Lazy;
use serde_json::json;
use std::collections::HashMap;
use std::time::Instant;
use tokio::runtime::Runtime;

use processors::OutboxProcessor;

static RT: Lazy<Runtime> = Lazy::new(|| Runtime::new().unwrap());

// =============================================================================
// Event Builders
// =============================================================================

fn outbox_source_info(table: &str) -> SourceInfo {
    SourceInfo {
        version: "1.0.0".into(),
        connector: "mysql".into(),
        name: "bench-db".into(),
        ts_ms: 1_700_000_000_000,
        db: "shop".into(),
        schema: Some(OUTBOX_SCHEMA_SENTINEL.into()),
        table: table.into(),
        snapshot: None,
        position: SourcePosition::default(),
    }
}

fn cdc_source_info() -> SourceInfo {
    SourceInfo {
        version: "1.0.0".into(),
        connector: "mysql".into(),
        name: "bench-db".into(),
        ts_ms: 1_700_000_000_000,
        db: "shop".into(),
        schema: None,
        table: "orders".into(),
        snapshot: None,
        position: SourcePosition::default(),
    }
}

fn make_outbox_event(i: usize) -> Event {
    Event::new_row(
        outbox_source_info("outbox"),
        Op::Create,
        None,
        Some(json!({
            "id": format!("evt-{i}"),
            "aggregate_type": "Order",
            "aggregate_id": format!("{i}"),
            "event_type": "OrderCreated",
            "payload": {"order_id": i, "total": 99.99, "currency": "USD"}
        })),
        1_700_000_000_000,
        256,
    )
}

fn make_large_outbox_event(payload_keys: usize) -> Event {
    let mut payload = serde_json::Map::new();
    for k in 0..payload_keys {
        payload.insert(format!("field_{k}"), json!(format!("value_{k}")));
    }
    Event::new_row(
        outbox_source_info("outbox"),
        Op::Create,
        None,
        Some(json!({
            "id": "evt-large",
            "aggregate_type": "Order",
            "aggregate_id": "1",
            "event_type": "OrderCreated",
            "payload": payload
        })),
        1_700_000_000_000,
        payload_keys * 30,
    )
}

fn make_cdc_event(i: usize) -> Event {
    Event::new_row(
        cdc_source_info(),
        Op::Create,
        None,
        Some(json!({"id": i, "status": "active", "total": 42.0})),
        1_700_000_000_000,
        64,
    )
}

fn default_cfg() -> OutboxProcessorCfg {
    OutboxProcessorCfg {
        id: "outbox".into(),
        tables: vec![],
        columns: OutboxColumns::default(),
        topic: Some("${aggregate_type}.${event_type}".into()),
        default_topic: Some("events.default".into()),
        key: None,
        additional_headers: HashMap::new(),
        raw_payload: false,
    }
}

fn cfg_with_headers() -> OutboxProcessorCfg {
    OutboxProcessorCfg {
        additional_headers: HashMap::from([
            ("x-trace-id".into(), "trace_id".into()),
            ("x-tenant".into(), "tenant".into()),
            ("x-region".into(), "region".into()),
        ]),
        ..default_cfg()
    }
}

fn cfg_with_key() -> OutboxProcessorCfg {
    OutboxProcessorCfg {
        key: Some("${aggregate_type}:${aggregate_id}".into()),
        ..default_cfg()
    }
}

fn make_outbox_event_with_extra_headers(i: usize) -> Event {
    Event::new_row(
        outbox_source_info("outbox"),
        Op::Create,
        None,
        Some(json!({
            "id": format!("evt-{i}"),
            "aggregate_type": "Order",
            "aggregate_id": format!("{i}"),
            "event_type": "OrderCreated",
            "trace_id": format!("trace-{i}"),
            "tenant": "acme",
            "region": "us-east-1",
            "payload": {"order_id": i, "total": 99.99}
        })),
        1_700_000_000_000,
        256,
    )
}

// =============================================================================
// Benchmarks
// =============================================================================

fn bench_outbox_single_event(c: &mut Criterion) {
    let mut group = c.benchmark_group("outbox_single");
    group.throughput(Throughput::Elements(1));

    let proc = OutboxProcessor::new(default_cfg()).unwrap();
    let ev = make_outbox_event(1);

    group.bench_function("transform", |b| {
        b.iter_custom(|iters| {
            RT.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    let _ = black_box(proc.process(vec![ev.clone()]).await)
                        .unwrap();
                }
                start.elapsed()
            })
        })
    });

    group.finish();
}

fn bench_outbox_with_key_template(c: &mut Criterion) {
    let mut group = c.benchmark_group("outbox_key_template");
    group.throughput(Throughput::Elements(1));

    let proc = OutboxProcessor::new(cfg_with_key()).unwrap();
    let ev = make_outbox_event(1);

    group.bench_function("topic_and_key", |b| {
        b.iter_custom(|iters| {
            RT.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    let _ = black_box(proc.process(vec![ev.clone()]).await)
                        .unwrap();
                }
                start.elapsed()
            })
        })
    });

    group.finish();
}

fn bench_outbox_additional_headers(c: &mut Criterion) {
    let mut group = c.benchmark_group("outbox_additional_headers");
    group.throughput(Throughput::Elements(1));

    let proc = OutboxProcessor::new(cfg_with_headers()).unwrap();
    let ev = make_outbox_event_with_extra_headers(1);

    group.bench_function("3_extra_headers", |b| {
        b.iter_custom(|iters| {
            RT.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    let _ = black_box(proc.process(vec![ev.clone()]).await)
                        .unwrap();
                }
                start.elapsed()
            })
        })
    });

    group.finish();
}

fn bench_outbox_batch_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("outbox_batch_sizes");

    let proc = OutboxProcessor::new(default_cfg()).unwrap();

    for size in [1, 10, 50, 100, 500] {
        let batch: Vec<Event> = (0..size).map(make_outbox_event).collect();
        group.throughput(Throughput::Elements(size as u64));

        group.bench_function(format!("batch_{size}"), |b| {
            b.iter_custom(|iters| {
                RT.block_on(async {
                    let start = Instant::now();
                    for _ in 0..iters {
                        let _ = black_box(proc.process(batch.clone()).await)
                            .unwrap();
                    }
                    start.elapsed()
                })
            })
        });
    }

    group.finish();
}

fn bench_outbox_large_payload(c: &mut Criterion) {
    let mut group = c.benchmark_group("outbox_large_payload");

    let proc = OutboxProcessor::new(default_cfg()).unwrap();

    for keys in [10, 50, 200] {
        let ev = make_large_outbox_event(keys);
        group.throughput(Throughput::Elements(1));

        group.bench_function(format!("{keys}_payload_fields"), |b| {
            b.iter_custom(|iters| {
                RT.block_on(async {
                    let start = Instant::now();
                    for _ in 0..iters {
                        let _ = black_box(proc.process(vec![ev.clone()]).await)
                            .unwrap();
                    }
                    start.elapsed()
                })
            })
        });
    }

    group.finish();
}

fn bench_outbox_mixed_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("outbox_mixed_pipeline");

    let proc = OutboxProcessor::new(default_cfg()).unwrap();

    // 20% outbox, 80% CDC passthrough — realistic mixed pipeline
    let batch: Vec<Event> = (0..100)
        .map(|i| {
            if i % 5 == 0 {
                make_outbox_event(i)
            } else {
                make_cdc_event(i)
            }
        })
        .collect();
    group.throughput(Throughput::Elements(100));

    group.bench_function("20pct_outbox_80pct_cdc", |b| {
        b.iter_custom(|iters| {
            RT.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    let _ =
                        black_box(proc.process(batch.clone()).await).unwrap();
                }
                start.elapsed()
            })
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_outbox_single_event,
    bench_outbox_with_key_template,
    bench_outbox_additional_headers,
    bench_outbox_batch_sizes,
    bench_outbox_large_payload,
    bench_outbox_mixed_pipeline,
);
criterion_main!(benches);

//! Flatten Processor benchmarks.
//!
//! Measures flatten overhead across payload shapes and config variants.
//! Native Rust processor - expected to be significantly faster than JS
//! and in the same ballpark as the outbox processor.
//!
//! Run with: cargo bench -p processors --bench flatten_processor_bench

use criterion::{
    Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use deltaforge_config::{
    EmptyListPolicy, EmptyObjectPolicy, FlattenProcessorCfg, ListPolicy,
};
use deltaforge_core::{Event, Op, Processor, SourceInfo, SourcePosition};
use once_cell::sync::Lazy;
use serde_json::json;
use std::time::Instant;
use tokio::runtime::Runtime;

use processors::FlattenProcessor;

static RT: Lazy<Runtime> = Lazy::new(|| Runtime::new().unwrap());

// =============================================================================
// Event Builders
// =============================================================================

fn source_info() -> SourceInfo {
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

/// Shallow payload — 1 level of nesting, representative of most CDC events.
fn make_shallow_event() -> Event {
    Event::new_row(
        source_info(),
        Op::Create,
        None,
        Some(json!({
            "id": 1,
            "customer": { "id": 42, "name": "Alice" },
            "status": "pending",
            "total": 99.99
        })),
        1_700_000_000_000,
        128,
    )
}

/// Deep payload — 4 levels of nesting, stress-tests recursion.
fn make_deep_event() -> Event {
    Event::new_row(
        source_info(),
        Op::Create,
        None,
        Some(json!({
            "order": {
                "customer": {
                    "address": {
                        "geo": { "lat": 52.5, "lng": 13.4 }
                    }
                }
            }
        })),
        1_700_000_000_000,
        128,
    )
}

/// Wide payload — many top-level keys, each with one nested object.
fn make_wide_event(fields: usize) -> Event {
    let mut map = serde_json::Map::new();
    for i in 0..fields {
        map.insert(
            format!("field_{i}"),
            json!({ "value": i, "label": format!("label_{i}") }),
        );
    }
    Event::new_row(
        source_info(),
        Op::Create,
        None,
        Some(serde_json::Value::Object(map)),
        1_700_000_000_000,
        fields * 40,
    )
}

/// Flat payload — already flat, tests idempotency overhead.
fn make_flat_event() -> Event {
    Event::new_row(
        source_info(),
        Op::Create,
        None,
        Some(json!({
            "id": 1, "name": "Alice", "status": "active",
            "total": 99.99, "currency": "USD"
        })),
        1_700_000_000_000,
        64,
    )
}

fn make_batch(size: usize) -> Vec<Event> {
    (0..size).map(|_| make_shallow_event()).collect()
}

// =============================================================================
// Config Helpers
// =============================================================================

fn default_cfg() -> FlattenProcessorCfg {
    FlattenProcessorCfg::default()
}

fn cfg_with_max_depth(depth: usize) -> FlattenProcessorCfg {
    FlattenProcessorCfg {
        max_depth: Some(depth),
        ..Default::default()
    }
}

fn cfg_analytics() -> FlattenProcessorCfg {
    FlattenProcessorCfg {
        lists: ListPolicy::Index,
        empty_object: EmptyObjectPolicy::Drop,
        empty_list: EmptyListPolicy::Drop,
        ..Default::default()
    }
}

// =============================================================================
// Benchmarks
// =============================================================================

fn bench_flatten_payload_shapes(c: &mut Criterion) {
    let mut group = c.benchmark_group("flatten_payload_shapes");
    group.throughput(Throughput::Elements(1));

    let proc = FlattenProcessor::new(default_cfg()).unwrap();

    group.bench_function("shallow_1_level", |b| {
        let ev = make_shallow_event();
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

    group.bench_function("deep_4_levels", |b| {
        let ev = make_deep_event();
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

    group.bench_function("flat_already_flat", |b| {
        let ev = make_flat_event();
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

fn bench_flatten_wide_payloads(c: &mut Criterion) {
    let mut group = c.benchmark_group("flatten_wide_payloads");

    let proc = FlattenProcessor::new(default_cfg()).unwrap();

    for fields in [10, 50, 200] {
        let ev = make_wide_event(fields);
        group.throughput(Throughput::Elements(fields as u64));

        group.bench_function(format!("{fields}_fields"), |b| {
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

fn bench_flatten_batch_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("flatten_batch_sizes");

    let proc = FlattenProcessor::new(default_cfg()).unwrap();

    for size in [1, 10, 100, 500] {
        let batch = make_batch(size);
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

fn bench_flatten_max_depth(c: &mut Criterion) {
    let mut group = c.benchmark_group("flatten_max_depth");
    group.throughput(Throughput::Elements(1));

    let ev = make_deep_event();

    for depth in [1, 2, 4] {
        let proc = FlattenProcessor::new(cfg_with_max_depth(depth)).unwrap();
        group.bench_function(format!("max_depth_{depth}"), |b| {
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

fn bench_flatten_analytics_config(c: &mut Criterion) {
    let mut group = c.benchmark_group("flatten_analytics_config");
    group.throughput(Throughput::Elements(1));

    // Analytics config does more work per field (policy checks on every node)
    let proc_default = FlattenProcessor::new(default_cfg()).unwrap();
    let proc_analytics = FlattenProcessor::new(cfg_analytics()).unwrap();
    let ev = make_shallow_event();

    group.bench_function("default_policies", |b| {
        b.iter_custom(|iters| {
            RT.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    let _ =
                        black_box(proc_default.process(vec![ev.clone()]).await)
                            .unwrap();
                }
                start.elapsed()
            })
        })
    });

    group.bench_function("analytics_policies", |b| {
        b.iter_custom(|iters| {
            RT.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    let _ = black_box(
                        proc_analytics.process(vec![ev.clone()]).await,
                    )
                    .unwrap();
                }
                start.elapsed()
            })
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_flatten_payload_shapes,
    bench_flatten_wide_payloads,
    bench_flatten_batch_sizes,
    bench_flatten_max_depth,
    bench_flatten_analytics_config,
);
criterion_main!(benches);

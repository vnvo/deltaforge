//! JS Processor benchmarks.
//!
//! Measures JS processor overhead compared to Rust baseline and different
//! processing patterns (passthrough, mutation, expansion).
//!
//! Run with: cargo bench -p processors --bench js_processor_bench

use anyhow::Result;
use criterion::{
    Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use deltaforge_core::{
    BatchContext, Event, Op, Processor, SourceInfo, SourcePosition,
};
use once_cell::sync::Lazy;
use processors::JsProcessor;
use serde_json::json;
use std::time::Instant;
use tokio::runtime::Runtime;

static RT: Lazy<Runtime> = Lazy::new(|| Runtime::new().unwrap());

fn make_source_info() -> SourceInfo {
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

fn make_small_event() -> Event {
    Event::new_row(
        make_source_info(),
        Op::Create,
        None,
        Some(json!({"id": 1, "sku": "ABC-1", "qty": 2})),
        1_700_000_000_000,
        64,
    )
}

fn make_large_event(bytes: usize) -> Event {
    let big = "x".repeat(bytes);
    Event::new_row(
        make_source_info(),
        Op::Create,
        None,
        Some(json!({
            "id": 1,
            "desc": big,
            "tags": ["a", "b", "c"],
            "nested": {"k": [1, 2, 3, 4, 5]}
        })),
        1_700_000_000_000,
        bytes,
    )
}

fn make_batch(size: usize) -> Vec<Event> {
    (0..size)
        .map(|i| {
            Event::new_row(
                make_source_info(),
                Op::Create,
                None,
                Some(json!({"id": i, "sku": format!("SKU-{}", i), "qty": i % 10})),
                1_700_000_000_000,
                64,
            )
        })
        .collect()
}

// ============================================================================
// Rust Baseline Processor
// ============================================================================

struct RustNoop;

#[async_trait::async_trait]
impl Processor for RustNoop {
    fn id(&self) -> &str {
        "noop"
    }
    async fn process(
        &self,
        events: Vec<Event>,
        _ctx: &BatchContext,
    ) -> Result<Vec<Event>> {
        Ok(events)
    }
}

// ============================================================================
// Benchmarks
// ============================================================================

fn bench_rust_noop_baseline(c: &mut Criterion) {
    let mut group = c.benchmark_group("rust_noop_baseline");
    group.throughput(Throughput::Elements(1));

    let proc = RustNoop;
    let ev = make_small_event();

    group.bench_function("rust_noop", |b| {
        b.iter_custom(|iters| {
            RT.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    let events = vec![ev.clone()];
                    let ctx = BatchContext::from_batch(&events);
                    let _ =
                        black_box(proc.process(events, &ctx).await).unwrap();
                }
                start.elapsed()
            })
        })
    });

    group.finish();
}

fn bench_js_passthrough(c: &mut Criterion) {
    let mut group = c.benchmark_group("js_passthrough");
    group.throughput(Throughput::Elements(1));

    let js = r#"
        function processBatch(events) {
            return events;
        }
    "#;

    let proc =
        JsProcessor::new("passthrough".into(), js.into()).expect("init ok");
    let ev = make_small_event();

    group.bench_function("passthrough", |b| {
        b.iter_custom(|iters| {
            RT.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    let events = vec![ev.clone()];
                    let ctx = BatchContext::from_batch(&events);
                    let _ =
                        black_box(proc.process(events, &ctx).await).unwrap();
                }
                start.elapsed()
            })
        })
    });

    group.finish();
}

fn bench_js_mutation(c: &mut Criterion) {
    let mut group = c.benchmark_group("js_mutation");
    group.throughput(Throughput::Elements(1));

    let js = r#"
        function processBatch(events) {
            for (const ev of events) {
                if (ev.after) {
                    ev.after.qty = (ev.after.qty || 0) + 1;
                    ev.after.processed = true;
                }
            }
            return null; // use mutated input
        }
    "#;

    let proc = JsProcessor::new("mutation".into(), js.into()).expect("init ok");
    let ev = make_small_event();

    group.bench_function("mutation", |b| {
        b.iter_custom(|iters| {
            RT.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    let events = vec![ev.clone()];
                    let ctx = BatchContext::from_batch(&events);
                    let _ =
                        black_box(proc.process(events, &ctx).await).unwrap();
                }
                start.elapsed()
            })
        })
    });

    group.finish();
}

fn bench_js_expansion(c: &mut Criterion) {
    let mut group = c.benchmark_group("js_expansion");
    group.throughput(Throughput::Elements(2)); // 1 in, 2 out

    let js = r#"
        function processBatch(events) {
            const out = [];
            for (const ev of events) {
                out.push(ev);
                const clone = JSON.parse(JSON.stringify(ev));
                clone.after = clone.after || {};
                clone.after.is_audit = true;
                out.push(clone);
            }
            return out;
        }
    "#;

    let proc =
        JsProcessor::new("expansion".into(), js.into()).expect("init ok");
    let ev = make_small_event();

    group.bench_function("expansion_1to2", |b| {
        b.iter_custom(|iters| {
            RT.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    let events = vec![ev.clone()];
                    let ctx = BatchContext::from_batch(&events);
                    let v =
                        black_box(proc.process(events, &ctx).await).unwrap();

                    assert_eq!(v.len(), 2);
                }
                start.elapsed()
            })
        })
    });

    group.finish();
}

fn bench_js_filtering(c: &mut Criterion) {
    let mut group = c.benchmark_group("js_filtering");

    let js = r#"
        function processBatch(events) {
            return events.filter(ev => ev.after && ev.after.qty > 5);
        }
    "#;

    let proc =
        JsProcessor::new("filtering".into(), js.into()).expect("init ok");
    let batch = make_batch(100);
    group.throughput(Throughput::Elements(100));

    group.bench_function("filter_100_events", |b| {
        b.iter_custom(|iters| {
            RT.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    let events = batch.clone();
                    let ctx = BatchContext::from_batch(&events);
                    let _ =
                        black_box(proc.process(events, &ctx).await).unwrap();
                }
                start.elapsed()
            })
        })
    });

    group.finish();
}

fn bench_js_large_payload(c: &mut Criterion) {
    let mut group = c.benchmark_group("js_large_payload");
    group.throughput(Throughput::Bytes(64 * 1024));

    let js = r#"
        function processBatch(events) {
            return events;
        }
    "#;

    let proc = JsProcessor::new("large".into(), js.into()).expect("init ok");
    let ev = make_large_event(64 * 1024);

    group.bench_function("64kb_passthrough", |b| {
        b.iter_custom(|iters| {
            RT.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    let events = vec![ev.clone()];
                    let ctx = BatchContext::from_batch(&events);
                    let _ =
                        black_box(proc.process(events, &ctx).await).unwrap();
                }
                start.elapsed()
            })
        })
    });

    group.finish();
}

fn bench_js_batch_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("js_batch_sizes");

    let js = r#"
        function processBatch(events) {
            for (const ev of events) {
                if (ev.after) ev.after.processed = true;
            }
            return null;
        }
    "#;

    let proc = JsProcessor::new("batch".into(), js.into()).expect("init ok");

    for size in [1, 10, 50, 100, 500] {
        let batch = make_batch(size);
        group.throughput(Throughput::Elements(size as u64));

        group.bench_function(format!("batch_{}", size), |b| {
            b.iter_custom(|iters| {
                RT.block_on(async {
                    let start = Instant::now();
                    for _ in 0..iters {
                        let events = batch.clone();
                        let ctx = BatchContext::from_batch(&events);
                        let _ = black_box(proc.process(events, &ctx).await)
                            .unwrap();
                    }
                    start.elapsed()
                })
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_rust_noop_baseline,
    bench_js_passthrough,
    bench_js_mutation,
    bench_js_expansion,
    bench_js_filtering,
    bench_js_large_payload,
    bench_js_batch_sizes,
);
criterion_main!(benches);

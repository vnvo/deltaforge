//! Benchmarks for schema sensing with and without high-cardinality handling.
//!
//! Run with: cargo bench -p schema-sensing
//!
//! Key metrics:
//! - stable_events: Overhead of HC for pure struct data (should be minimal)
//! - dynamic_nested: Realistic nested map pattern (sessions.sess_*)
//! - dynamic_toplevel: Top-level dynamic keys (worst case for original hash)

use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use schema_sensing::{HighCardinalityConfig, ObserveResult, SchemaSensor};

use deltaforge_config::{SamplingConfig, SchemaSensingConfig};
use serde_json::json;

/// Pure struct - no dynamic keys at any level
fn make_stable_event(i: u64) -> serde_json::Value {
    json!({
        "id": i,
        "user_id": i % 100,
        "action": "click",
        "timestamp": 1700000000 + i,
        "metadata": {
            "browser": "chrome",
            "os": "linux",
            "version": "1.0"
        }
    })
}

/// Nested dynamic keys (realistic pattern)
/// Top-level is stable, but sessions/metrics contain dynamic keys
fn make_nested_dynamic_event(i: u64) -> serde_json::Value {
    json!({
        "id": i,
        "type": "event",
        "timestamp": 1700000000 + i,
        "sessions": {
            format!("sess_{:08x}", i): {
                "user_id": i % 100,
                "started_at": 1700000000 + i
            }
        },
        "metrics": {
            format!("metric_{}", i % 10): i as f64 * 0.1
        }
    })
}

/// Top-level dynamic keys (stress test for structure hash)
fn make_toplevel_dynamic_event(i: u64) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert("id".to_string(), json!(i));
    obj.insert("type".to_string(), json!("event"));
    obj.insert(format!("sess_{:08x}", i), json!({"value": i}));
    obj.insert(format!("trace_{:08x}", i), json!({"value": i + 1}));
    serde_json::Value::Object(obj)
}

fn create_sensor(hc_enabled: bool, cache_enabled: bool) -> SchemaSensor {
    let config = SchemaSensingConfig {
        enabled: true,
        sampling: SamplingConfig {
            structure_cache: cache_enabled,
            structure_cache_size: 100,
            warmup_events: 100000, // High to avoid sampling
            sample_rate: 1,
        },
        ..Default::default()
    };

    let hc_config = HighCardinalityConfig {
        enabled: hc_enabled,
        min_events: 20,
        confidence_threshold: 0.5,
        min_dynamic_fields: 2,
        ..Default::default()
    };

    SchemaSensor::with_hc_config(config, hc_config)
}

/// Benchmark stable events - measures HC overhead for pure structs
fn bench_stable_events(c: &mut Criterion) {
    let mut group = c.benchmark_group("stable_events");
    group.throughput(Throughput::Elements(1000));

    for hc_enabled in [false, true] {
        let label = if hc_enabled { "hc_on" } else { "hc_off" };

        group.bench_with_input(BenchmarkId::new("observe", label), &hc_enabled, |b, &hc| {
            b.iter_batched(
                || {
                    let sensor = create_sensor(hc, true);
                    let events: Vec<_> = (0..1000).map(make_stable_event).collect();
                    (sensor, events)
                },
                |(mut sensor, events)| {
                    for event in events {
                        black_box(sensor.observe_value("events", &event).unwrap());
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

/// Benchmark nested dynamic keys (realistic CDC pattern)
fn bench_nested_dynamic(c: &mut Criterion) {
    let mut group = c.benchmark_group("nested_dynamic");
    group.throughput(Throughput::Elements(1000));

    for hc_enabled in [false, true] {
        let label = if hc_enabled { "hc_on" } else { "hc_off" };

        group.bench_with_input(BenchmarkId::new("cold", label), &hc_enabled, |b, &hc| {
            b.iter_batched(
                || {
                    let sensor = create_sensor(hc, true);
                    let events: Vec<_> = (0..1000).map(make_nested_dynamic_event).collect();
                    (sensor, events)
                },
                |(mut sensor, events)| {
                    for event in events {
                        black_box(sensor.observe_value("events", &event).unwrap());
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // Warmed up - classifications established
        group.bench_with_input(BenchmarkId::new("warm", label), &hc_enabled, |b, &hc| {
            b.iter_batched(
                || {
                    let mut sensor = create_sensor(hc, true);
                    // Warmup to establish classifications
                    for i in 0..100 {
                        sensor
                            .observe_value("events", &make_nested_dynamic_event(i))
                            .unwrap();
                    }
                    let events: Vec<_> = (1000..2000).map(make_nested_dynamic_event).collect();
                    (sensor, events)
                },
                |(mut sensor, events)| {
                    for event in events {
                        black_box(sensor.observe_value("events", &event).unwrap());
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

/// Benchmark top-level dynamic keys (stress test)
fn bench_toplevel_dynamic(c: &mut Criterion) {
    let mut group = c.benchmark_group("toplevel_dynamic");
    group.throughput(Throughput::Elements(1000));

    for hc_enabled in [false, true] {
        let label = if hc_enabled { "hc_on" } else { "hc_off" };

        group.bench_with_input(BenchmarkId::new("warm", label), &hc_enabled, |b, &hc| {
            b.iter_batched(
                || {
                    let mut sensor = create_sensor(hc, true);
                    for i in 0..100 {
                        sensor
                            .observe_value("events", &make_toplevel_dynamic_event(i))
                            .unwrap();
                    }
                    let events: Vec<_> = (1000..2000).map(make_toplevel_dynamic_event).collect();
                    (sensor, events)
                },
                |(mut sensor, events)| {
                    for event in events {
                        black_box(sensor.observe_value("events", &event).unwrap());
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

/// Measure actual cache effectiveness (not just timing)
fn bench_cache_effectiveness(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_effectiveness");

    for (name, make_event) in [
        ("stable", make_stable_event as fn(u64) -> serde_json::Value),
        ("nested_dyn", make_nested_dynamic_event),
        ("toplevel_dyn", make_toplevel_dynamic_event),
    ] {
        for hc_enabled in [false, true] {
            let label = format!("{}_{}", name, if hc_enabled { "hc_on" } else { "hc_off" });

            group.bench_function(&label, |b| {
                b.iter_batched(
                    || {
                        let mut sensor = create_sensor(hc_enabled, true);
                        // Warmup
                        for i in 0..100 {
                            sensor.observe_value("events", &make_event(i)).unwrap();
                        }
                        sensor
                    },
                    |mut sensor| {
                        let mut cache_hits = 0u64;
                        let mut evolutions = 0u64;
                        let mut other = 0u64;

                        for i in 1000..2000 {
                            match sensor.observe_value("events", &make_event(i)).unwrap() {
                                ObserveResult::CacheHit { .. } => cache_hits += 1,
                                ObserveResult::Evolved { .. } => evolutions += 1,
                                _ => other += 1,
                            }
                        }

                        black_box((cache_hits, evolutions, other))
                    },
                    criterion::BatchSize::SmallInput,
                );
            });
        }
    }

    group.finish();
}

/// Benchmark without structure cache (isolates schema fingerprint benefit)
fn bench_no_cache(c: &mut Criterion) {
    let mut group = c.benchmark_group("no_cache");
    group.throughput(Throughput::Elements(500)); // Fewer events since no cache

    for hc_enabled in [false, true] {
        let label = if hc_enabled { "hc_on" } else { "hc_off" };

        group.bench_with_input(
            BenchmarkId::new("nested_dynamic", label),
            &hc_enabled,
            |b, &hc| {
                b.iter_batched(
                    || {
                        let mut sensor = create_sensor(hc, false); // cache disabled
                        for i in 0..100 {
                            sensor
                                .observe_value("events", &make_nested_dynamic_event(i))
                                .unwrap();
                        }
                        let events: Vec<_> = (1000..1500).map(make_nested_dynamic_event).collect();
                        (sensor, events)
                    },
                    |(mut sensor, events)| {
                        for event in events {
                            black_box(sensor.observe_value("events", &event).unwrap());
                        }
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_stable_events,
    bench_nested_dynamic,
    bench_toplevel_dynamic,
    bench_cache_effectiveness,
    bench_no_cache,
);
criterion_main!(benches);
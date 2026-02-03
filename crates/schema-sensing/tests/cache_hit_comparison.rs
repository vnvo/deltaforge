//! Cache hit rate comparison test.
//!
//! Run with: cargo test -p schema-sensing --test cache_hit_comparison -- --nocapture

use schema_sensing::{HighCardinalityConfig, ObserveResult, SchemaSensor};

use deltaforge_config::{SamplingConfig, SchemaSensingConfig};
use serde_json::json;

/// Generate event with NESTED dynamic keys (realistic pattern)
///
/// Real-world examples:
/// - Session stores: `sessions.{session_id}`
/// - Feature flags: `flags.{flag_name}`
/// - User attributes: `attributes.{attr_key}`
/// - Metrics: `metrics.{metric_name}`
fn make_dynamic_event(i: u64) -> serde_json::Value {
    json!({
        "id": i,
        "type": "event",
        "timestamp": 1700000000 + i,
        "sessions": {
            format!("sess_{:08x}", i): {
                "user_id": i % 100,
                "started_at": 1700000000 + i,
                "page_views": i % 50
            },
            format!("sess_{:08x}", i + 1000000): {
                "user_id": (i + 1) % 100,
                "started_at": 1700000000 + i + 1,
                "page_views": (i + 1) % 50
            }
        },
        "metrics": {
            format!("metric_{}", i % 10): i as f64 * 0.1,
            format!("metric_{}", (i + 5) % 10): i as f64 * 0.2
        }
    })
}

fn create_sensor(hc_enabled: bool) -> SchemaSensor {
    let config = SchemaSensingConfig {
        enabled: true,
        sampling: SamplingConfig {
            // DISABLE structure cache to force full schema analysis every time
            // This isolates the benefit of schema fingerprint normalization
            structure_cache: false,
            structure_cache_size: 0,
            warmup_events: 100000,
            sample_rate: 1,
        },
        ..Default::default()
    };

    let hc_config = HighCardinalityConfig {
        enabled: hc_enabled,
        min_events: 50,
        confidence_threshold: 0.5,
        min_dynamic_fields: 3,
        ..Default::default()
    };

    SchemaSensor::with_hc_config(config, hc_config)
}

#[derive(Default)]
struct Stats {
    new_schema: u64,
    evolved: u64,
    unchanged: u64,
    cache_hit: u64,
    total: u64,
}

impl Stats {
    fn record(&mut self, result: &ObserveResult) {
        self.total += 1;
        match result {
            ObserveResult::NewSchema { .. } => self.new_schema += 1,
            ObserveResult::Evolved { .. } => self.evolved += 1,
            ObserveResult::Unchanged { .. } => self.unchanged += 1,
            ObserveResult::CacheHit { .. } => self.cache_hit += 1,
            _ => {}
        }
    }

    fn cache_hit_rate(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            self.cache_hit as f64 / self.total as f64 * 100.0
        }
    }

    fn evolution_rate(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            self.evolved as f64 / self.total as f64 * 100.0
        }
    }
}

#[test]
fn compare_cache_hit_rates() {
    const WARMUP: u64 = 200;
    const TEST_EVENTS: u64 = 5000;

    println!("\n============================================================");
    println!("Schema Sensing: High-Cardinality Detection Benefit");
    println!("============================================================");
    println!("Warmup events: {WARMUP}");
    println!("Test events: {TEST_EVENTS}");
    println!();
    println!("Structure cache: DISABLED (forces full schema analysis)");
    println!(
        "Pattern: Nested dynamic keys (sessions.sess_*, metrics.metric_*)"
    );
    println!();
    println!(
        "Key metric: Evolution rate (false schema changes from dynamic keys)"
    );
    println!("  - HC OFF: Every unique dynamic key → schema 'evolves'");
    println!("  - HC ON:  Dynamic keys normalized → stable fingerprint");
    println!();

    for hc_enabled in [false, true] {
        let label = if hc_enabled {
            "HIGH-CARDINALITY ON"
        } else {
            "HIGH-CARDINALITY OFF"
        };

        let mut sensor = create_sensor(hc_enabled);

        // Warmup phase
        let mut warmup_stats = Stats::default();
        for i in 0..WARMUP {
            let result = sensor
                .observe_value("events", &make_dynamic_event(i))
                .unwrap();
            warmup_stats.record(&result);
        }

        // Test phase
        let mut test_stats = Stats::default();
        for i in WARMUP..(WARMUP + TEST_EVENTS) {
            let result = sensor
                .observe_value("events", &make_dynamic_event(i))
                .unwrap();
            test_stats.record(&result);
        }

        // Get cache stats
        let cache_stats = sensor.cache_stats("events");
        let cached_structures = cache_stats
            .as_ref()
            .map(|s| s.cached_structures)
            .unwrap_or(0);

        println!("--- {label} ---");
        println!("Warmup phase:");
        println!("  New schemas:    {:>6}", warmup_stats.new_schema);
        println!("  Evolutions:     {:>6}", warmup_stats.evolved);
        println!("  Cache hits:     {:>6}", warmup_stats.cache_hit);
        println!("  Hit rate:       {:>5.1}%", warmup_stats.cache_hit_rate());
        println!();
        println!("Test phase:");
        println!("  Evolutions:     {:>6}", test_stats.evolved);
        println!("  Cache hits:     {:>6}", test_stats.cache_hit);
        println!("  Unchanged:      {:>6}", test_stats.unchanged);
        println!("  Hit rate:       {:>5.1}%", test_stats.cache_hit_rate());
        println!("  Evolution rate: {:>5.1}%", test_stats.evolution_rate());
        println!();
        println!("Cache state:");
        println!("  Cached structures: {}", cached_structures);

        if hc_enabled {
            let maps = sensor.detected_maps("events");
            println!("  Detected maps: {:?}", maps);

            // Assertions for HC ON
            assert!(
                test_stats.evolution_rate() < 10.0,
                "HC ON should have <10% evolution rate, got {:.1}%",
                test_stats.evolution_rate()
            );
            assert!(
                maps.contains(&"sessions"),
                "Should detect 'sessions' as map"
            );
            assert!(
                maps.contains(&"metrics"),
                "Should detect 'metrics' as map"
            );
        } else {
            // Assertions for HC OFF
            assert!(
                test_stats.evolution_rate() > 90.0,
                "HC OFF should have >90% evolution rate, got {:.1}%",
                test_stats.evolution_rate()
            );
        }

        println!();
    }

    println!("✓ High-cardinality detection reduced false evolutions by >90%");
}

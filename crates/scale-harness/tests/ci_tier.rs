//! CI tier of the dense-catalog harness: structural assertions only, never
//! timing. Two equivalent fresh stores (1K and 2K tables per source, same
//! seed and shape) are generated through the real registration path; counters
//! are reset after generation; every scenario must then produce the SAME
//! complete per-primitive operation vector (calls and bytes) at both sizes,
//! with no enumeration, so no cost grows with the catalog.

use std::sync::Arc;

use scale_harness::counting::{CountingBackend, OpVector};
use scale_harness::fixture::{self, Fixture, FixtureSpec, PayloadConfig};
use scale_harness::scenarios::{
    self, MigrationRun, RegistryReport, ScenarioConfig,
};
use storage::ArcStorageBackend;

const ACTIVE: u64 = 100;
const LEGACY_TABLES: u64 = 20;
const CONCURRENCY: usize = 16;

struct Tier {
    fixture: Fixture,
    registry: RegistryReport,
    migration: MigrationRun,
    _dir: tempfile::TempDir,
}

async fn tier(tables: u64) -> Tier {
    let dir = tempfile::tempdir().unwrap();
    let store: ArcStorageBackend =
        storage::SqliteStorageBackend::open(dir.path().join("reg.db")).unwrap();
    let spec = FixtureSpec {
        seed: 42,
        tables,
        versions: 2,
        sources: 2,
        payload: PayloadConfig { columns: 8 },
        legacy_tables: LEGACY_TABLES,
        legacy_versions: vec![3],
    };
    let fixture =
        fixture::open_or_generate(&store, "sqlite", &spec, false, false)
            .await
            .unwrap();
    assert!(!fixture.reused);

    // Counting starts only after generation.
    let cb = Arc::new(CountingBackend::new(store));
    cb.normalize_timestamps(true);
    let cfg = ScenarioConfig {
        active: ACTIVE,
        cache_max_bytes: 64 * 1024 * 1024,
        cache_max_entries: 50_000,
        concurrency: CONCURRENCY,
        history_page: 1,
    };
    let registry = scenarios::run_registry(&cb, &fixture, &cfg).await.unwrap();
    let migration = scenarios::run_migration(&cb, &fixture, 0, 2, "mig-ci")
        .await
        .unwrap();
    Tier {
        fixture,
        registry,
        migration,
        _dir: dir,
    }
}

/// Per primitive: calls, bytes returned (normalized for the registration
/// timestamp, see `OpStat::bytes_returned_normalized`), bytes written.
fn comparable(ops: &OpVector) -> Vec<(&'static str, u64, u64, u64)> {
    ops.iter()
        .map(|(p, s)| {
            (*p, s.calls, s.bytes_returned_normalized, s.bytes_written)
        })
        .collect()
}

fn no_enumeration(what: &str, ops: &OpVector) {
    assert_eq!(
        CountingBackend::enumeration_calls(ops),
        0,
        "{what} enumerated a namespace or read a whole stream: {ops:?}"
    );
}

fn assert_structure(size: u64, t: &Tier) {
    let r = &t.registry;
    let at = |s: &str| format!("{s} ({size} tables)");

    no_enumeration(&at("startup"), &r.startup.ops);
    no_enumeration(&at("cold lookup"), &r.lookup_cold.ops);
    no_enumeration(&at("small cache"), &r.small_cache.phase.ops);
    no_enumeration(&at("single-flight"), &r.single_flight.phase.ops);
    no_enumeration(&at("history"), &r.history.phase.ops);
    no_enumeration(&at("isolation"), &r.isolation.phase.ops);
    no_enumeration(&at("migration plan"), &t.migration.plan.ops);
    no_enumeration(&at("migration apply"), &t.migration.apply.ops);

    assert!(
        r.lookup_cold_identity_ok,
        "{}",
        at("cold lookups returned the wrong schema")
    );
    assert!(
        r.lookup_hot.ops.is_empty(),
        "{}: {:?}",
        at("hot lookups hit the backend"),
        r.lookup_hot.ops
    );

    assert!(
        r.small_cache.resident_within_budget,
        "{}",
        at("cache exceeded its byte budget")
    );
    assert!(
        r.small_cache.phase.registry.unwrap().evictions > 0,
        "{}",
        at("a working set 4x the cache evicted nothing")
    );

    assert_eq!(r.single_flight.backend_loads, 1, "{}", at("single-flight"));
    assert_eq!(
        r.single_flight.served_without_load,
        CONCURRENCY as u64 - 1,
        "{}",
        at("single-flight")
    );

    assert_eq!(r.history.versions_read, 2, "{}", at("history"));

    let iso = &r.isolation;
    assert!(iso.reads > 0);
    assert_eq!(
        iso.foreign_reads,
        0,
        "{}: {:?}",
        at("source 0 read another source's keys"),
        iso.foreign_examples
    );
    assert!(
        iso.identity_ok,
        "{}",
        at("a colliding table name resolved to another source")
    );
    assert!(
        iso.residency_only_own,
        "{}",
        at("cache residency of another source")
    );

    let m = &t.migration;
    assert_eq!(
        (m.migrated, m.rejected, m.ambiguous),
        (LEGACY_TABLES, 0, 0),
        "{}",
        at("migration")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn registry_costs_do_not_grow_with_the_catalog() {
    let small = tier(1_000).await;
    let large = tier(2_000).await;
    assert_structure(1_000, &small);
    assert_structure(2_000, &large);

    // The fixtures are equivalent apart from their size.
    assert_eq!(small.fixture.sources, large.fixture.sources);

    let (a, b) = (&small.registry, &large.registry);
    for (name, x, y) in [
        ("startup", &a.startup.ops, &b.startup.ops),
        ("cold lookup", &a.lookup_cold.ops, &b.lookup_cold.ops),
        ("hot lookup", &a.lookup_hot.ops, &b.lookup_hot.ops),
        (
            "small cache",
            &a.small_cache.phase.ops,
            &b.small_cache.phase.ops,
        ),
        (
            "single-flight",
            &a.single_flight.phase.ops,
            &b.single_flight.phase.ops,
        ),
        ("history", &a.history.phase.ops, &b.history.phase.ops),
        ("isolation", &a.isolation.phase.ops, &b.isolation.phase.ops),
        (
            "migration plan",
            &small.migration.plan.ops,
            &large.migration.plan.ops,
        ),
        (
            "migration apply",
            &small.migration.apply.ops,
            &large.migration.apply.ops,
        ),
    ] {
        assert_eq!(
            comparable(x),
            comparable(y),
            "{name}: the complete operation vector (calls and bytes) differs \
             between 1K and 2K tables\n  1K: {x:?}\n  2K: {y:?}"
        );
    }
    assert_eq!(a.small_cache.budget_bytes, b.small_cache.budget_bytes);
}

#[tokio::test]
async fn reused_stores_must_match_their_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reg.db");
    let spec = FixtureSpec {
        seed: 7,
        tables: 10,
        versions: 1,
        sources: 2,
        payload: PayloadConfig { columns: 4 },
        legacy_tables: 2,
        legacy_versions: vec![1],
    };
    let store: ArcStorageBackend =
        storage::SqliteStorageBackend::open(&path).unwrap();
    fixture::open_or_generate(&store, "sqlite", &spec, false, false)
        .await
        .unwrap();

    // Same spec, backend and revision: reused.
    let again =
        fixture::open_or_generate(&store, "sqlite", &spec, false, false)
            .await
            .unwrap();
    assert!(again.reused);

    // Anything else is refused.
    for changed in [
        FixtureSpec {
            seed: 8,
            ..spec.clone()
        },
        FixtureSpec {
            tables: 11,
            ..spec.clone()
        },
        FixtureSpec {
            versions: 2,
            ..spec.clone()
        },
        FixtureSpec {
            sources: 3,
            ..spec.clone()
        },
        FixtureSpec {
            payload: PayloadConfig { columns: 5 },
            ..spec.clone()
        },
        FixtureSpec {
            legacy_versions: vec![2],
            ..spec.clone()
        },
    ] {
        let err =
            fixture::open_or_generate(&store, "sqlite", &changed, false, false)
                .await
                .unwrap_err();
        assert!(err.to_string().contains("does not match"), "{err:#}");
    }
    assert!(
        fixture::open_or_generate(&store, "postgres", &spec, false, false)
            .await
            .is_err()
    );

    // A store with schema data but no manifest is never generated into.
    let other = dir.path().join("other.db");
    let raw: ArcStorageBackend =
        storage::SqliteStorageBackend::open(&other).unwrap();
    raw.log_append("schemas.v1", "x", b"{}").await.unwrap();
    let err = fixture::open_or_generate(&raw, "sqlite", &spec, false, false)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("no fixture manifest"), "{err:#}");
}

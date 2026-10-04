//! Scenario 8 against live databases (Docker). Not part of normal CI; run
//! with the container matrix (`--include-ignored`). Checks the measurement's
//! boundaries: exactly the known event is awaited after a CDC-only restart,
//! with no enumeration of the registry during startup, and the restart's work
//! does not grow with the live catalog.

use scale_harness::live::{self, Engine, LiveConfig};
use storage::ArcStorageBackend;

async fn check(engine: Engine) {
    let dir = tempfile::tempdir().unwrap();
    let store: ArcStorageBackend =
        storage::SqliteStorageBackend::open(dir.path().join("live.db"))
            .unwrap();
    let r = live::run(
        &LiveConfig {
            engine,
            tables: 1_000,
            catalog_tables: 0,
            matched_tables: 0,
            versions: 2,
            columns: 8,
            seed: 42,
        },
        store,
    )
    .await
    .unwrap();
    println!("{engine:?}: {r:#?}");
    assert_eq!(
        r.events_before_known, 0,
        "the restart re-delivered events before the known one"
    );
    assert_eq!(
        r.registry_enumeration_calls, 0,
        "startup enumerated the schema registry: {:?}",
        r.enumerations
    );
    assert!(r.time_to_first_event_ms > 0.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn postgres_time_to_first_event() {
    check(Engine::Postgres).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn mysql_time_to_first_event() {
    check(Engine::Mysql).await;
}

/// A CDC-only restart does the same work whatever part of the live catalog
/// its table pattern matches: the same storage operations (calls, returned
/// and written bytes per primitive), no registry enumeration and the same number of
/// statements on the server, whether 20 or all 1,000 catalog tables match (no
/// enumeration, no per-table schema load at startup).
async fn startup_ignores_catalog_size(engine: Engine) {
    let mut runs = Vec::new();
    for matched_tables in [20, 1_000] {
        let dir = tempfile::tempdir().unwrap();
        let store: ArcStorageBackend =
            storage::SqliteStorageBackend::open(dir.path().join("live.db"))
                .unwrap();
        let r = live::run(
            &LiveConfig {
                engine,
                tables: 100,
                catalog_tables: 1_000,
                matched_tables,
                versions: 1,
                columns: 4,
                seed: 7,
            },
            store,
        )
        .await
        .unwrap();
        println!(
            "{engine:?} {matched_tables} matched tables: {:.0} ms, {} server \
             statements, ops {:?}",
            r.time_to_first_event_ms, r.server_statements, r.ops
        );
        assert_eq!(r.events_before_known, 0);
        assert_eq!(r.registry_enumeration_calls, 0, "{:?}", r.enumerations);
        runs.push(r);
    }
    // Calls, normalized returned bytes and written bytes per primitive, up
    // to the moment the known event arrived: one call reading the whole
    // catalog would differ in bytes.
    let work = |r: &live::LiveReport| {
        r.ops
            .iter()
            .map(|(p, s)| {
                (*p, s.calls, s.bytes_returned_normalized, s.bytes_written)
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(work(&runs[0]), work(&runs[1]), "startup work grew");
    assert_eq!(
        runs[0].server_statements, runs[1].server_statements,
        "server statements grew with the catalog"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn postgres_restart_work_ignores_catalog_size() {
    startup_ignores_catalog_size(Engine::Postgres).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn mysql_restart_work_ignores_catalog_size() {
    startup_ignores_catalog_size(Engine::Mysql).await;
}

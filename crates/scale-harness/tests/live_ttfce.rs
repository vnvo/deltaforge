//! Scenario 8 against live databases (Docker). Not part of normal CI; run
//! with the container matrix (`--include-ignored`). Checks the measurement's
//! boundaries: exactly the known event is awaited after a CDC-only restart,
//! with no enumeration of the registry during startup.

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

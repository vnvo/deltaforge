//! End-to-end smoke test of the PostgreSQL source qualification (`pg-qualify`,
//! release checklist G4) at toy scale: a real PostgreSQL 17, Kafka and schema
//! registry, and a DeltaForge pipeline manager on a SQLite state store served
//! in-process behind its real REST API and metrics. It proves the harness's
//! own machinery (fixture and registration, snapshot, paced writers, the stop
//! and resume, ADD COLUMN, TOAST under both replica identities, the
//! deliberately unsupported cases, health, counters and reconciliation), not
//! any capacity.
//!
//! ```bash
//! cargo test -p fleet-harness --test pg_qualify_smoke -- --include-ignored
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use fleet_harness::pgqual::{
    AddColumnCfg, CdcCfg, PgQualConfig, StopCfg, TablesCfg, UnsupportedCfg, run,
};
use gate_ownership::GateOwned;
use rdkafka::ClientConfig;
use rest_api::AppState;
use runner::pipeline_manager::PipelineManager;
use serde_json::Value;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const NETWORK: &str = "pgqual-smoke";
const KAFKA_NAME: &str = "pgqual-smoke-kafka";
const KAFKA_INTERNAL_PORT: u16 = 29092;
const KAFKA_HOST_PORT: u16 = 9398;

async fn start_kafka() -> (ContainerAsync<GenericImage>, String) {
    std::process::Command::new("docker")
        .args(["rm", "-f", "-v", KAFKA_NAME])
        .output()
        .ok();
    let c = GenericImage::new("confluentinc/cp-kafka", "7.5.0")
        .with_wait_for(WaitFor::Duration { length: Duration::from_secs(15) })
        .with_container_name(KAFKA_NAME)
        .with_network(NETWORK)
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:29093")
        .with_env_var(
            "KAFKA_LISTENERS",
            format!("PLAINTEXT://0.0.0.0:{KAFKA_INTERNAL_PORT},CONTROLLER://0.0.0.0:29093,EXTERNAL://0.0.0.0:{KAFKA_HOST_PORT}"),
        )
        .with_env_var(
            "KAFKA_ADVERTISED_LISTENERS",
            format!("PLAINTEXT://{KAFKA_NAME}:{KAFKA_INTERNAL_PORT},EXTERNAL://localhost:{KAFKA_HOST_PORT}"),
        )
        .with_env_var(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT",
        )
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env_var("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env_var("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .with_env_var("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qg")
        .with_mapped_port(KAFKA_HOST_PORT, KAFKA_HOST_PORT.tcp())
        .gate_owned()
        .start()
        .await
        .expect("start kafka");
    let brokers = format!("localhost:{KAFKA_HOST_PORT}");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        use rdkafka::consumer::{BaseConsumer, Consumer};
        let ok = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create::<BaseConsumer>()
            .ok()
            .and_then(|c| c.fetch_metadata(None, Duration::from_secs(2)).ok())
            .is_some();
        if ok {
            break;
        }
        assert!(Instant::now() < deadline, "kafka never became ready");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    (c, brokers)
}

/// The schema registry (the Avro sink of the unsupported cases), reached
/// from this process through its mapped port.
async fn start_registry() -> (ContainerAsync<GenericImage>, String) {
    let c = GenericImage::new("confluentinc/cp-schema-registry", "7.5.0")
        .with_exposed_port(8081.tcp())
        .with_wait_for(WaitFor::Duration {
            length: Duration::from_secs(5),
        })
        .with_network(NETWORK)
        .with_env_var("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
        .with_env_var(
            "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
            format!("PLAINTEXT://{KAFKA_NAME}:{KAFKA_INTERNAL_PORT}"),
        )
        .with_env_var("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
        .gate_owned()
        .start()
        .await
        .expect("start schema registry");
    let url = format!(
        "http://127.0.0.1:{}",
        c.get_host_port_ipv4(8081).await.expect("registry port")
    );
    let deadline = Instant::now() + Duration::from_secs(120);
    while reqwest::get(format!("{url}/subjects"))
        .await
        .map(|r| r.status().is_success())
        .ok()
        != Some(true)
    {
        assert!(
            Instant::now() < deadline,
            "schema registry never became ready"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    (c, url)
}

async fn start_postgres() -> (ContainerAsync<GenericImage>, String) {
    let c = GenericImage::new("postgres", "17")
        .with_wait_for(WaitFor::message_on_stderr("database system is ready"))
        .with_env_var("POSTGRES_PASSWORD", "password")
        .with_cmd(vec![
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=10",
            "-c",
            "max_wal_senders=10",
        ])
        .gate_owned()
        .start()
        .await
        .expect("start postgres");
    let port = c.get_host_port_ipv4(5432).await.expect("postgres port");
    let dsn = format!("postgres://postgres:password@127.0.0.1:{port}/postgres");
    let deadline = Instant::now() + Duration::from_secs(60);
    while tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .is_err()
    {
        assert!(
            Instant::now() < deadline,
            "postgres never accepted connections"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    (c, dsn)
}

/// The instance under test: a pipeline manager on a SQLite state store
/// behind the real REST API, with the real metrics route.
async fn serve_deltaforge(store: &std::path::Path) -> String {
    o11y::df_metrics::init(&o11y::df_metrics::Config {
        enable: true,
        http_listener: None,
    })
    .expect("metrics");
    let backend: storage::ArcStorageBackend =
        storage::SqliteStorageBackend::open(store).expect("state store");
    let mgr = Arc::new(
        PipelineManager::with_backend(backend)
            .await
            .expect("manager"),
    );
    let app = rest_api::router(AppState { controller: mgr })
        .merge(o11y::df_metrics::router_with_metrics());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    format!("http://{addr}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "docker: PostgreSQL, Kafka and a schema registry"]
async fn the_postgres_qualification_runs_end_to_end() {
    std::process::Command::new("docker")
        .args(["network", "create", NETWORK])
        .output()
        .ok();
    let (_kafka, brokers) = start_kafka().await;
    let (_registry, registry) = start_registry().await;
    let (_pg, dsn) = start_postgres().await;
    let dir = tempfile::tempdir().unwrap();
    let api = serve_deltaforge(&dir.path().join("deltaforge.db")).await;
    let out = dir.path().join("results");
    let cfg = PgQualConfig {
        output_dir: out.display().to_string(),
        api_url: api.clone(),
        metrics_url: format!("{api}/metrics"),
        state_store: "sqlite".into(),
        kafka_brokers: brokers.clone(),
        verifier_kafka_brokers: brokers,
        admin_dsn: dsn.clone(),
        cdc_dsn: dsn,
        prefix: "g4".into(),
        tenant: "g4".into(),
        tables: TablesCfg {
            count: 12,
            toast_full: 2,
            toast_default: 2,
            initial_rows: 4,
            big_bytes: 8192,
        },
        cdc: CdcCfg {
            duration_secs: 40,
            ops_per_sec: 30.0,
            writers: 3,
        },
        stop: StopCfg {
            at_secs: 12,
            for_secs: 4,
        },
        add_column: AddColumnCfg {
            at_secs: 24,
            table: 6,
        },
        unsupported: Some(UnsupportedCfg {
            schema_registry_url: registry,
            timeout_secs: 60,
        }),
        status_poll_ms: 250,
        snapshot_timeout_secs: 120,
        drain_idle_secs: 10,
        drain_max_secs: 120,
        spec_overrides: None,
        seed: 7,
    };
    let run_dir = run(&cfg).await.expect("the run completes");
    let r: Value = serde_json::from_slice(
        &std::fs::read(run_dir.join("result.json")).unwrap(),
    )
    .unwrap();
    eprintln!(
        "result: ok {} | report {} | toast counters {} | health {} | timings {} | unsupported {}",
        r["ok"],
        r["report"],
        r["toast_counters"],
        r["health"],
        r["timings_ms"],
        serde_json::json!({
            "u1": r["unsupported_cases"]["u1_refused_at_start"]["ok"],
            "u1_messages": r["unsupported_cases"]["u1_refused_at_start"]["messages"],
            "u2": r["unsupported_cases"]["u2_stopped_when_changed"]["ok"],
            "u2_before": r["unsupported_cases"]["u2_stopped_when_changed"]["messages_before_change"],
            "u2_after": r["unsupported_cases"]["u2_stopped_when_changed"]["messages_after"],
        })
    );
    let pretty = serde_json::to_string_pretty(&r["reasons"]).unwrap();
    assert_eq!(
        r["ok"], true,
        "reasons: {pretty}\nreport: {}\nunsupported: {}",
        r["report"], r["unsupported_cases"]
    );
    let rep = &r["report"];
    assert!(rep["committed"].as_u64().unwrap() > 1000, "{rep}");
    assert!(
        rep["toast_reconstructed"].as_u64().unwrap() > 0,
        "FULL unchanged values exercised"
    );
    assert!(
        rep["toast_unavailable"].as_u64().unwrap() > 0,
        "DEFAULT unchanged values exercised"
    );
    assert_eq!(rep["duplicates_unexplained"], 0);
    assert_eq!(r["state_store"], "sqlite");
    assert_eq!(r["pg_state_store_qualified"], false);
    for case in ["u1_refused_at_start", "u2_stopped_when_changed"] {
        assert_eq!(
            r["unsupported_cases"][case]["ok"], true,
            "{case}: {}",
            r["unsupported_cases"][case]
        );
    }
    for f in [
        "ledger.jsonl",
        "delivered.jsonl",
        "statuses.json",
        "pipeline.json",
    ] {
        assert!(run_dir.join(f).exists(), "{f} kept");
    }
}

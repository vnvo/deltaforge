//! End-to-end smoke test of the fleet harness at toy scale: a real MySQL
//! 8.4 and Kafka, and a DeltaForge pipeline manager served in-process
//! behind its real REST API (the instance under test). It proves the
//! harness's own machinery, not any capacity: fixture build, pipeline
//! creation, paced writers with stamped deletes, a migration with probes,
//! a pipeline restart with recovery measurement, the verifying consumer,
//! the offline completeness and ordering check, and the result files.
//!
//! It also records the current product limit the owner's primary-key
//! requirement depends on: with `key: ${after.id}`, every insert and
//! update carries its primary key, and deletes do not.
//!
//! ```bash
//! cargo test -p fleet-harness --test harness_smoke -- --include-ignored --test-threads=1
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use fleet_harness::config::RunConfig;
use fleet_harness::fixture;
use fleet_harness::run::{RunOptions, sweep};
use gate_ownership::GateOwned;
use rdkafka::ClientConfig;
use rest_api::AppState;
use runner::pipeline_manager::PipelineManager;
use serde_json::Value;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const KAFKA_INTERNAL_PORT: u16 = 29092;

fn kafka_port() -> u16 {
    std::env::var("FLEET_HARNESS_KAFKA_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(9394)
}

async fn start_kafka() -> (ContainerAsync<GenericImage>, String) {
    let kp = kafka_port();
    let container = GenericImage::new("confluentinc/cp-kafka", "7.5.0")
        .with_wait_for(WaitFor::Duration { length: Duration::from_secs(15) })
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:29093")
        .with_env_var(
            "KAFKA_LISTENERS",
            format!("PLAINTEXT://0.0.0.0:{KAFKA_INTERNAL_PORT},CONTROLLER://0.0.0.0:29093,EXTERNAL://0.0.0.0:{kp}"),
        )
        .with_env_var(
            "KAFKA_ADVERTISED_LISTENERS",
            format!("PLAINTEXT://localhost:{KAFKA_INTERNAL_PORT},EXTERNAL://localhost:{kp}"),
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
        .with_env_var("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
        .with_env_var("KAFKA_NUM_PARTITIONS", "3")
        .with_env_var("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qg")
        .with_mapped_port(kp, kp.tcp())
        .gate_owned()
        .start()
        .await
        .expect("start kafka");
    let brokers = format!("localhost:{kp}");
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
    (container, brokers)
}

async fn start_mysql() -> (ContainerAsync<GenericImage>, u16) {
    let c = GenericImage::new("mysql", "8.4")
        .with_wait_for(WaitFor::message_on_stderr(
            "ready for connections. Version: '8.4",
        ))
        .with_env_var("MYSQL_ROOT_PASSWORD", "rootpw")
        .with_cmd(vec![
            "--server-id=999",
            "--log-bin=/var/lib/mysql/mysql-bin.log",
            "--binlog-format=ROW",
            "--binlog-row-image=FULL",
            "--binlog-row-metadata=FULL",
            "--gtid-mode=ON",
            "--enforce-gtid-consistency=ON",
            "--binlog-checksum=NONE",
        ])
        .gate_owned()
        .start()
        .await
        .expect("start mysql");
    let port = c.get_host_port_ipv4(3306).await.expect("mysql port");
    // Accepts connections a moment after the banner.
    let opts = mysql_async::Opts::from_url(&format!(
        "mysql://root:rootpw@127.0.0.1:{port}/"
    ))
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    while mysql_async::Conn::new(opts.clone()).await.is_err() {
        assert!(
            Instant::now() < deadline,
            "mysql never accepted connections"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    (c, port)
}

/// The instance under test: a pipeline manager behind the real REST API,
/// plus a metrics route.
async fn serve_deltaforge() -> String {
    let backend: storage::ArcStorageBackend =
        Arc::new(storage::MemoryStorageBackend::new());
    let mgr = Arc::new(
        PipelineManager::with_backend(backend)
            .await
            .expect("manager"),
    );
    let app = rest_api::router(AppState { controller: mgr }).route(
        "/metrics",
        axum::routing::get(|| async { "deltaforge_pipelines_total 1\n" }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    format!("http://{addr}")
}

fn config(
    scenario: &str,
    port: u16,
    brokers: &str,
    api: &str,
    out: &str,
    work: &str,
    expected_key: &str,
) -> RunConfig {
    let yaml = format!(
        r#"
class: exploratory
profile: cdc
output_dir: {out}
topology:
  servers: [{{name: c01, host: 127.0.0.1, port: {port}}}]
  admin_user: root
  admin_password: rootpw
  databases_per_server: 3
  tables_per_database: 4
  seed: 3
deltaforge:
  api_url: {api}
  metrics_url: {api}/metrics
  process: {{kind: pid, pid: {pid}}}
  state_store: memory
  cdc_user: df
  cdc_password: dfpw
  kafka_brokers: {brokers}
  key: {{mode: template, template: "${{after.id}}"}}
verifier:
  kafka_brokers: {brokers}
  expected_key: {expected_key}
  work_dir: {work}
traffic:
  changes_per_sec_avg: 40
  changes_per_sec_peak: 40
  peak_fraction: 0
  op_mix: {{insert: 0.6, update: 0.3, delete: 0.1}}
  row_bytes: 64
  rows_per_txn: 2
  zipf_exponent: 0
  active_set: {{pattern: fixed, fraction: 1.0}}
  writers_per_server: 2
ddl:
  migration_statements: 2
  migration_pace: 10
  migration_rollout: {{kind: sequential}}
measure:
  interval_secs: 1
  cdc_users: [df]
run:
  scenario: {scenario}
  duration_secs: 20
  warmup_secs: 3
  sources: [1]
"#,
        pid = std::process::id()
    );
    let cfg: RunConfig = serde_yaml::from_str(&yaml).expect("config");
    cfg.validate().unwrap();
    cfg
}

fn result(out: &str, run_id: &str) -> Value {
    let path = std::path::Path::new(out)
        .join("exploratory")
        .join(run_id)
        .join("result.json");
    serde_json::from_slice(&std::fs::read(&path).expect("result.json"))
        .expect("json")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "docker: MySQL and Kafka"]
async fn the_harness_runs_scenarios_end_to_end() {
    let (_kafka, brokers) = start_kafka().await;
    let (_mysql, port) = start_mysql().await;
    let api = serve_deltaforge().await;
    let out = tempfile::tempdir().unwrap();
    let work = tempfile::tempdir().unwrap();
    let (out_s, work_s) =
        (out.path().to_str().unwrap(), work.path().to_str().unwrap());
    let opts = RunOptions {
        drain_idle: Duration::from_secs(8),
        drain_max: Duration::from_secs(120),
        recovery_timeout: Duration::from_secs(60),
        start_timeout: Duration::from_secs(120),
        ..RunOptions::default()
    };

    // S3: a migration with probes, keys checked as primary keys.
    let cfg = config("S3", port, &brokers, &api, out_s, work_s, "primary_key");
    let built = fixture::build(&cfg, &cfg.topology.servers[0], 2, false)
        .await
        .unwrap();
    assert_eq!(built.tables, 12);
    let sweep_s3 = sweep(cfg, opts.clone()).await.unwrap();
    assert_eq!(
        sweep_s3.largest_passing_sources, None,
        "placeholder budgets declare nothing"
    );
    let r = result(out_s, &sweep_s3.points[0].runs[0]);
    assert_eq!(r["claims_allowed"], false);
    let c = &r["completeness"];
    assert!(c["written"].as_u64().unwrap() > 100, "{c}");
    for k in [
        "missing",
        "unexpected",
        "final_state_mismatches",
        "partition_order_violations",
    ] {
        assert_eq!(c[k], 0, "{k}: {c}");
    }
    let s = &r["stream"];
    assert!(
        s["probes_ok"].as_u64().unwrap() >= 6,
        "2 statements x 3 databases: {s}"
    );
    assert_eq!(s["probes_failed"], 0);
    // ${after.id} keys every insert and update, and no delete.
    let deletes = r["driver"]["c01"]["deletes"].as_u64().unwrap();
    assert!(deletes > 0);
    assert_eq!(s["key_mismatches"], s["key_mismatches_on_deletes"], "{s}");
    assert!(
        s["key_mismatches_on_deletes"].as_u64().unwrap() >= deletes,
        "{s}"
    );
    assert_eq!(
        r["verdict"]["correctness_ok"], false,
        "keys on deletes fail the primary-key check"
    );
    assert!(r["resources"]["samples"].as_u64().unwrap() > 0);
    assert!(r["resources"]["connections_max"]["c01"].as_u64().unwrap() >= 1);

    // S5: restart the pipeline mid-run; recovery is measured.
    let cfg = config("S5", port, &brokers, &api, out_s, work_s, "none");
    let sweep_s5 = sweep(cfg, opts).await.unwrap();
    let r = result(out_s, &sweep_s5.points[0].runs[0]);
    assert!(
        r["stream"]["foreign_events"].as_u64().unwrap() > 0,
        "S3's events on the same topic are skipped, not counted"
    );
    assert_eq!(
        r["verdict"]["correctness_ok"], true,
        "{}",
        r["completeness"]
    );
    let step = &r["steps"][0];
    assert_eq!(step["action"]["action"], "restart_pipelines");
    assert_eq!(step["error"], Value::Null, "{step}");
    assert_eq!(step["recovery"]["recovered"], 1, "{step}");
    assert!(sweep_s5.points[0].correctness_ok);
}

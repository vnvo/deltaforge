//! End-to-end smoke test of the fleet harness at toy scale: a real MySQL
//! 8.4 and Kafka, and a DeltaForge pipeline manager served in-process
//! behind its real REST API (the instance under test). It proves the
//! harness's own machinery, not any capacity: fixture build, pipeline
//! creation, paced writers with stamped deletes, a migration with probes,
//! a pipeline restart with recovery measurement, the verifying consumer,
//! the offline completeness and ordering check, and the result files.
//!
//! The instance keeps its checkpoints in a real PostgreSQL state store (the
//! harness's progress check reads them) and writes its proof trace to a
//! file the harness collects and reports. The broker does not auto-create
//! topics: the harness creates the run's topics and waits for the verifier
//! to hold them before any write. Besides the completed runs it covers an
//! interrupted run, a run failing its redo budget, a run failing after its
//! window, and a pipeline that fails closed mid-run: each writes a
//! `result.json` with its reason and partial counters.
//!
//! It also records the current product limit the owner's primary-key
//! requirement depends on: with `key: ${after.id}`, every insert and
//! update carries its primary key, and deletes do not.
//!
//! ```bash
//! cargo test -p fleet-harness --test harness_smoke -- --include-ignored --test-threads=1
//! ```

use std::collections::BTreeSet;
use std::io::Write as _;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use fleet_harness::config::RunConfig;
use fleet_harness::deltaforge::{Api, pipeline_name};
use fleet_harness::fixture;
use fleet_harness::run::{RunOptions, sweep};
use fleet_harness::topology::Naming;
use gate_ownership::GateOwned;
use mysql_async::prelude::Queryable;
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
        .with_env_var("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
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

async fn start_postgres() -> (ContainerAsync<GenericImage>, String) {
    let c = GenericImage::new("postgres", "17")
        .with_wait_for(WaitFor::message_on_stderr("database system is ready"))
        .with_env_var("POSTGRES_PASSWORD", "password")
        .gate_owned()
        .start()
        .await
        .expect("start postgres");
    let port = c.get_host_port_ipv4(5432).await.expect("postgres port");
    let dsn = format!(
        "host=127.0.0.1 port={port} user=postgres password=password dbname=postgres"
    );
    // The first banner is the init server's; wait for the real one.
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

/// Where this process's `deltaforge::proof_trace` records go: one JSON
/// record per line, as the instance logs them.
static TRACE: OnceLock<Mutex<std::fs::File>> = OnceLock::new();

struct ProofTraceFile;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for ProofTraceFile {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if event.metadata().target() != "deltaforge::proof_trace" {
            return;
        }
        struct Msg(String);
        impl tracing::field::Visit for Msg {
            fn record_debug(
                &mut self,
                f: &tracing::field::Field,
                v: &dyn std::fmt::Debug,
            ) {
                if f.name() == "message" {
                    self.0 = format!("{v:?}");
                }
            }
        }
        let mut m = Msg(String::new());
        event.record(&mut m);
        if let Some(f) = TRACE.get() {
            let _ = writeln!(f.lock().unwrap(), "{}", m.0);
        }
    }
}

fn install_proof_trace(path: &Path) {
    use tracing_subscriber::Layer as _;
    use tracing_subscriber::layer::SubscriberExt;
    TRACE.get_or_init(|| {
        Mutex::new(std::fs::File::create(path).expect("trace file"))
    });
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry().with(ProofTraceFile).with(
            tracing_subscriber::fmt::layer()
                .with_test_writer()
                .with_filter(tracing_subscriber::EnvFilter::new("warn")),
        ),
    )
    .expect("subscriber");
}

/// The instance under test: a pipeline manager with a PostgreSQL state
/// store behind the real REST API, plus a metrics route.
async fn serve_deltaforge(store_dsn: &str) -> String {
    let backend: storage::ArcStorageBackend =
        storage::PostgresStorageBackend::connect(store_dsn)
            .await
            .expect("state store");
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

struct Env<'a> {
    port: u16,
    brokers: &'a str,
    api: &'a str,
    store: &'a str,
    trace: &'a str,
    out: &'a str,
    work: &'a str,
}

fn config(scenario: &str, env: &Env<'_>, expected_key: &str) -> RunConfig {
    let Env {
        port,
        brokers,
        api,
        store,
        trace,
        out,
        work,
    } = env;
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
  state_store: postgres
  cdc_user: df
  cdc_password: dfpw
  kafka_brokers: {brokers}
  key: {{mode: template, template: "${{after.id}}"}}
  topic_partitions: 2
  proof_trace: {{kind: file, path: "{trace}"}}
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
  state_store_dsn: "{store}"
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
    let path = Path::new(out)
        .join("exploratory")
        .join(run_id)
        .join("result.json");
    serde_json::from_slice(&std::fs::read(&path).expect("result.json"))
        .expect("json")
}

/// The runs with a `result.json` so far.
fn run_ids(out: &str) -> BTreeSet<String> {
    std::fs::read_dir(Path::new(out).join("exploratory"))
        .map(|d| {
            d.filter_map(|e| e.ok())
                .filter(|e| e.path().join("result.json").exists())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default()
}

/// The one run that wrote a `result.json` since `before`.
fn new_result(out: &str, before: &BTreeSet<String>) -> Value {
    let new: Vec<String> = run_ids(out).difference(before).cloned().collect();
    assert_eq!(new.len(), 1, "{new:?}");
    result(out, &new[0])
}

async fn until_status(api: &Api, name: &str, want: &str) {
    let deadline = Instant::now() + Duration::from_secs(120);
    while api.status(name).await.ok().as_deref() != Some(want) {
        assert!(Instant::now() < deadline, "{name} never reached {want}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// A run that stopped early still accounts for what it wrote.
fn assert_partial_counters(r: &Value) {
    let d = &r["driver"]["c01"];
    assert!(d["ops"].as_u64().unwrap() > 0, "{r}");
    assert_eq!(r["verdict"]["completed"], false, "{r}");
    assert_eq!(r["verdict"]["repetition_ok"], false, "{r}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "docker: MySQL, Kafka and PostgreSQL"]
async fn the_harness_runs_scenarios_end_to_end() {
    let out = tempfile::tempdir().unwrap();
    let work = tempfile::tempdir().unwrap();
    let trace = out.path().join("deltaforge-proof-trace.log");
    install_proof_trace(&trace);
    let (_kafka, brokers) = start_kafka().await;
    let (_mysql, port) = start_mysql().await;
    let (_pg, store) = start_postgres().await;
    let api = serve_deltaforge(&store).await;
    let env = Env {
        port,
        brokers: &brokers,
        api: &api,
        store: &store,
        trace: trace.to_str().unwrap(),
        out: out.path().to_str().unwrap(),
        work: work.path().to_str().unwrap(),
    };
    let out_s = env.out;
    let opts = RunOptions {
        drain_idle: Duration::from_secs(8),
        drain_max: Duration::from_secs(120),
        recovery_timeout: Duration::from_secs(60),
        start_timeout: Duration::from_secs(120),
        status_poll: Duration::from_secs(1),
        ..RunOptions::default()
    };

    // S3: a migration with probes, keys checked as primary keys.
    let cfg = config("S3", &env, "primary_key");
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
    assert_eq!(r["outcome"], "completed");
    assert_eq!(r["claims_allowed"], false);
    // The harness created the topic (the broker would not) and the verifier
    // held its partitions before the first write.
    let ready = &r["readiness"];
    assert_eq!(ready["topics"]["fleet.c01"], 2, "{ready}");
    assert!(
        ready["verifier_assigned_secs"].as_f64().unwrap()
            >= ready["topics_ready_secs"].as_f64().unwrap(),
        "{ready}"
    );
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
    // ${after.id} keys every insert and update, and no delete (the known
    // limitation the primary-key requirement depends on).
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
    let cfg = config("S5", &env, "none");
    let sweep_s5 = sweep(cfg, opts.clone()).await.unwrap();
    let run_s5 = &sweep_s5.points[0].runs[0];
    let r = result(out_s, run_s5);
    assert!(
        r["stream"]["foreign_events"].as_u64().unwrap() > 0,
        "S3's events on the same topic are skipped, not counted"
    );
    assert_eq!(
        r["verdict"]["correctness_ok"], true,
        "{}",
        r["completeness"]
    );
    // The source's checkpoints (in the state store, under its own key
    // prefix) advanced during the window.
    let cp = &r["resources"]["checkpoints"]["c01"];
    assert!(cp["distinct"].as_u64().unwrap() >= 2, "{cp}");
    assert_eq!(r["verdict"]["progress_ok"], true, "{}", r["verdict"]);
    assert_eq!(r["verdict"]["repetition_ok"], true, "{}", r["verdict"]);
    assert_eq!(r["verdict"]["plan_executed"], true, "{}", r["verdict"]);
    assert_eq!(r["driver"]["c01"]["uncertain_txns"], 0);
    let w = &r["workload"]["c01"];
    assert!(
        w["ratio"].as_f64().unwrap() > 0.5,
        "the writers kept up with the target: {w}"
    );
    for side in ["written", "consumed"] {
        assert!(
            r["sort"][side]["max_open_runs"].as_u64().unwrap() <= 64,
            "{}",
            r["sort"]
        );
    }
    let step = &r["steps"][0];
    assert_eq!(step["action"]["action"], "restart_pipelines");
    assert_eq!(step["error"], Value::Null, "{step}");
    assert_eq!(step["recovery"]["recovered"], 1, "{step}");
    assert!(sweep_s5.points[0].correctness_ok);
    // The restart's resume proof is in the collected trace, with the
    // server's binlog inventory and the generated report.
    let pt = &r["proof_trace"];
    assert!(pt["records"].as_u64().unwrap() > 0, "{pt}");
    assert_eq!(pt["errors"], serde_json::json!([]), "{pt}");
    let dir = Path::new(out_s).join("exploratory").join(run_s5);
    let meta: Value = serde_json::from_slice(
        &std::fs::read(dir.join("proof-trace-c01-meta.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(meta["mode"], "gtid");
    assert!(!meta["binlogs"].as_array().unwrap().is_empty(), "{meta}");
    let report =
        std::fs::read_to_string(dir.join("proof-trace-c01-report.md")).unwrap();
    assert!(
        report.starts_with("# Proof-trace evidence: gtid mode"),
        "{report}"
    );

    // An interrupted run stops its writers, drains and still checks what it
    // wrote.
    let mut cfg = config("S5", &env, "none");
    cfg.run.duration_secs = 30;
    let (abort_tx, abort_rx) = tokio::sync::watch::channel(None);
    let name = pipeline_name(&cfg, &cfg.topology.servers[0]);
    let client = Api::new(&api).unwrap();
    let before = run_ids(out_s);
    let run = tokio::spawn(sweep(
        cfg,
        RunOptions {
            abort: Some(abort_rx),
            ..opts.clone()
        },
    ));
    until_status(&client, &name, "running").await;
    tokio::time::sleep(Duration::from_secs(8)).await;
    abort_tx.send(Some("interrupted (test)".into())).unwrap();
    let interrupted = run.await.unwrap().unwrap();
    assert!(!interrupted.points[0].correctness_ok);
    let r = new_result(out_s, &before);
    assert_eq!(r["outcome"], "aborted", "{r}");
    assert_eq!(r["reason"], "interrupted (test)");
    assert!(r["measured_secs"].as_f64().unwrap() < 30.0);
    assert_eq!(
        r["steps"],
        serde_json::json!([]),
        "aborted before the restart"
    );
    assert_partial_counters(&r);
    assert_eq!(r["completeness"]["missing"], 0, "{}", r["completeness"]);

    // A server over the redo budget fails the run before any pipeline.
    let mut cfg = config("S5", &env, "none");
    cfg.topology.redo_log_capacity_max_bytes = Some(1 << 20);
    let before = run_ids(out_s);
    let failed = sweep(cfg, opts.clone()).await.unwrap();
    assert!(failed.points[0].runs.is_empty());
    let r = new_result(out_s, &before);
    assert_eq!(r["outcome"], "failed", "{r}");
    assert_eq!(r["stage"], "servers", "{r}");
    let reason = r["reason"].as_str().unwrap();
    assert!(reason.contains("innodb_redo_log_capacity"), "{reason}");
    assert_eq!(r["pipelines"], serde_json::json!([]), "{r}");
    // Run ids have a resolution of one second.
    tokio::time::sleep(Duration::from_millis(1_100)).await;

    // A run failing after its window (its ledger lost) still writes its
    // partial counters and readiness.
    let mut cfg = config("S5", &env, "none");
    cfg.run.duration_secs = 10;
    let before_dirs: BTreeSet<_> = std::fs::read_dir(env.work)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    let before = run_ids(out_s);
    let run = tokio::spawn(sweep(cfg, opts.clone()));
    until_status(&client, &name, "running").await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    for e in std::fs::read_dir(env.work).unwrap() {
        let p = e.unwrap().path();
        if !before_dirs.contains(&p) {
            std::fs::remove_dir_all(&p).unwrap();
        }
    }
    let failed = run.await.unwrap().unwrap();
    assert!(failed.points[0].runs.is_empty());
    let r = new_result(out_s, &before);
    assert_eq!(r["outcome"], "failed", "{r}");
    assert!(
        ["drain", "completeness"].contains(&r["stage"].as_str().unwrap()),
        "{r}"
    );
    assert_eq!(r["pipelines"], serde_json::json!([name]), "{r}");
    assert_eq!(r["readiness"]["topics"]["fleet.c01"], 2, "{r}");
    assert_partial_counters(&r);

    // A pipeline that fails closed mid-run aborts the run with its
    // incidents: a table created, written and changed while the pipeline
    // was stopped leaves its row with no recorded shape and a live shape
    // that does not match it. Last: the source's checkpoint stays before
    // that row.
    let mut cfg = config("S5", &env, "none");
    cfg.run.duration_secs = 60;
    let db = Naming {
        database_prefix: cfg.topology.database_prefix.clone(),
    }
    .database(0);
    let mut admin = mysql_async::Conn::new(
        mysql_async::Opts::from_url(&format!(
            "mysql://root:rootpw@127.0.0.1:{port}/{db}"
        ))
        .unwrap(),
    )
    .await
    .unwrap();
    let before = run_ids(out_s);
    let run = tokio::spawn(sweep(cfg, opts.clone()));
    until_status(&client, &name, "running").await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    client.stop(&name).await.unwrap();
    admin
        .query_drop("CREATE TABLE drifted (id BIGINT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    admin
        .query_drop("INSERT INTO drifted VALUES (1, 1)")
        .await
        .unwrap();
    admin
        .query_drop("ALTER TABLE drifted ADD COLUMN w INT")
        .await
        .unwrap();
    client.resume(&name).await.unwrap();
    let closed = run.await.unwrap().unwrap();
    assert!(!closed.points[0].correctness_ok);
    let r = new_result(out_s, &before);
    assert_eq!(r["outcome"], "aborted", "{r}");
    let reason = r["reason"].as_str().unwrap();
    assert!(
        reason.starts_with(&format!("pipeline {name} failed: ")),
        "{reason}"
    );
    let incidents: Value = serde_json::from_str(
        reason
            .strip_prefix(&format!("pipeline {name} failed: "))
            .unwrap(),
    )
    .unwrap();
    let incident = &incidents["incidents"][0];
    assert_eq!(incident["component"]["id"], "src-c01", "{incident}");
    assert_eq!(incident["cause_code"], "source_schema", "{incident}");
    assert_eq!(incident["safety_state"], "halted_uncertain", "{incident}");
    assert!(r["measured_secs"].as_f64().unwrap() < 60.0);
    assert_partial_counters(&r);
}

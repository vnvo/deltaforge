//! Evidence harness (not a gate suite): how much binlog the activation
//! proofs read when many previously unseen tables become active while the
//! source lags behind sustained writes.
//!
//! The source starts at the tail and is paused; meanwhile each of N tables
//! gets its first row, separated by GAP single-row transactions of an
//! unrelated table; then the source resumes while writes continue. Every
//! first row must arrive, correctly decoded. The proof trace
//! (`deltaforge::proof_trace`) is written as JSONL with the server's binlog
//! inventory and executed set, captured once at the end, for the offline
//! report (`scripts/proof-trace-report.py`).
//!
//! Run with:
//! ```bash
//! cargo test -p sources --test mysql_proof_amplification -- --ignored --test-threads=1 gtid
//! ```
//! Environment: `DF_EVIDENCE_TABLES` (100), `DF_EVIDENCE_GAP` (1000),
//! `DF_EVIDENCE_RATE` (sustained transactions per second, 200),
//! `DF_EVIDENCE_OUT` (directory, the workspace's `target/proof-evidence`).

use std::collections::BTreeMap;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use checkpoints::{CheckpointStore, MemCheckpointStore};
use deltaforge_config::{OnSchemaDrift, SnapshotCfg, SnapshotMode};
use deltaforge_core::{Source, SourceItem};
use gate_ownership::GateOwned;
use mysql_async::prelude::Queryable;
use sources::MySqlSource;
use storage::{DurableSchemaRegistry, MemoryStorageBackend};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner,
};
use tokio::time::{Duration, sleep};

mod test_common;

const ROOT_PW: &str = "pw";
const DB: &str = "amp";

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// The trace records of this process, appended to the current output file.
static OUT: OnceLock<Mutex<Option<std::fs::File>>> = OnceLock::new();

struct ToFile;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for ToFile {
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
        if let Some(out) = OUT.get() {
            if let Some(f) = out.lock().unwrap().as_mut() {
                let _ = writeln!(f, "{}", m.0);
            }
        }
    }
}

fn install_trace() {
    use tracing_subscriber::Layer as _;
    use tracing_subscriber::layer::SubscriberExt;
    OUT.get_or_init(|| Mutex::new(None));
    let _ = tracing::subscriber::set_global_default(
        tracing_subscriber::registry().with(ToFile).with(
            tracing_subscriber::fmt::layer()
                .with_test_writer()
                .with_filter(tracing_subscriber::EnvFilter::new("warn")),
        ),
    );
}

async fn start(gtid: bool) -> (ContainerAsync<GenericImage>, u16) {
    let mut cmd = vec![
        "--server-id=61".to_string(),
        "--log-bin=mysql-bin".into(),
        "--binlog-format=ROW".into(),
        "--binlog-row-image=FULL".into(),
    ];
    if gtid {
        cmd.push("--gtid-mode=ON".into());
        cmd.push("--enforce-gtid-consistency=ON".into());
    }
    let c = GenericImage::new("mysql", "8.4")
        .with_wait_for(WaitFor::message_on_stderr(
            "ready for connections. Version: '8.4",
        ))
        .with_env_var("MYSQL_ROOT_PASSWORD", ROOT_PW)
        .with_cmd(cmd)
        .gate_owned()
        .start()
        .await
        .expect("start mysql");
    let port = c.get_host_port_ipv4(3306).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(90);
    while mysql_async::Conn::from_url(dsn(port, "")).await.is_err() {
        assert!(Instant::now() < deadline, "mysql on {port} not ready");
        sleep(Duration::from_millis(500)).await;
    }
    (c, port)
}

fn dsn(port: u16, db: &str) -> String {
    format!("mysql://root:{ROOT_PW}@127.0.0.1:{port}/{db}")
}

async fn conn(port: u16) -> mysql_async::Conn {
    mysql_async::Conn::from_url(dsn(port, DB)).await.unwrap()
}

/// `n` single-row transactions of the noise table, in batches.
async fn noise(c: &mut mysql_async::Conn, n: u64) {
    let mut left = n;
    while left > 0 {
        let k = left.min(200);
        let batch = "INSERT INTO noise (v) VALUES (1);".repeat(k as usize);
        c.query_drop(batch).await.unwrap();
        left -= k;
    }
}

async fn amplification(gtid: bool) {
    install_trace();
    let (tables, gap, rate) = (
        env("DF_EVIDENCE_TABLES", 100),
        env("DF_EVIDENCE_GAP", 1000),
        env("DF_EVIDENCE_RATE", 200),
    );
    let mode = if gtid { "gtid" } else { "file" };
    let out = std::path::PathBuf::from(
        std::env::var("DF_EVIDENCE_OUT").unwrap_or_else(|_| {
            concat!(env!("CARGO_MANIFEST_DIR"), "/../../target/proof-evidence")
                .into()
        }),
    );
    std::fs::create_dir_all(&out).unwrap();
    let stem = format!("{mode}-{tables}");
    *OUT.get().unwrap().lock().unwrap() =
        Some(std::fs::File::create(out.join(format!("{stem}.jsonl"))).unwrap());

    let (container, port) = start(gtid).await;
    let mut c = mysql_async::Conn::from_url(dsn(port, "")).await.unwrap();
    c.query_drop(format!("CREATE DATABASE {DB}")).await.unwrap();
    let mut c = conn(port).await;
    c.query_drop(
        "CREATE TABLE noise (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT)",
    )
    .await
    .unwrap();
    for i in 0..tables {
        c.query_drop(format!(
            "CREATE TABLE t{i:04} (id INT PRIMARY KEY, v INT NOT NULL)"
        ))
        .await
        .unwrap();
    }

    let backend: storage::ArcStorageBackend =
        Arc::new(MemoryStorageBackend::new());
    let src = MySqlSource {
        id: format!("amp-{mode}"),
        dsn: dsn(port, DB).into(),
        tables: vec![format!("{DB}.*")],
        tenant: "acme".into(),
        pipeline: "evidence".into(),
        registry: DurableSchemaRegistry::new(backend.clone()).await.unwrap(),
        registry_scope: sources::registry_scope::SharedRegistryScope::default(),
        outbox_tables: Default::default(),
        snapshot_cfg: SnapshotCfg {
            mode: SnapshotMode::Never,
            ..Default::default()
        },
        backend,
        on_schema_drift: OnSchemaDrift::Adapt,
        table_options: Default::default(),
        rotation: None,
        snapshot_cohort: Default::default(),
    };
    let ckpt: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new().unwrap());
    let (tx, mut rx) = test_common::acked_channel(&src, &ckpt, &src.id, 4096);
    let handle = src.run(tx, Arc::clone(&ckpt)).await;
    let deadline = Instant::now() + Duration::from_secs(60);
    while !handle.ready.is_ready() {
        assert!(!handle.join.is_finished(), "the source ended");
        assert!(Instant::now() < deadline, "the source never streamed");
        sleep(Duration::from_millis(50)).await;
    }

    // First rows of every table, as delivered: table -> (id, v).
    let firsts: Arc<Mutex<BTreeMap<String, (i64, i64)>>> = Default::default();
    let seen = firsts.clone();
    let drain = tokio::spawn(async move {
        while let Some(item) = rx.recv().await {
            if let SourceItem::Event(e) = item
                && e.source.table.starts_with('t')
                && let Some(a) = e.after.as_ref()
            {
                seen.lock().unwrap().insert(
                    e.source.table.clone(),
                    (a["id"].as_i64().unwrap(), a["v"].as_i64().unwrap()),
                );
            }
        }
    });

    // The backlog, behind a paused source.
    handle.pause();
    for i in 0..tables {
        c.query_drop(format!("INSERT INTO t{i:04} VALUES (1, {i})"))
            .await
            .unwrap();
        noise(&mut c, gap).await;
    }
    // Sustained writes while the source catches up.
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let stop = stop.clone();
        tokio::spawn(async move {
            let mut c = conn(port).await;
            let tick = Duration::from_millis(100);
            while !stop.load(Ordering::Relaxed) {
                noise(&mut c, (rate / 10).max(1)).await;
                sleep(tick).await;
            }
        })
    };
    let resumed = Instant::now();
    handle.resume();
    let deadline = Instant::now() + Duration::from_secs(3600);
    while firsts.lock().unwrap().len() < tables as usize {
        assert!(!handle.join.is_finished(), "the source ended");
        assert!(
            Instant::now() < deadline,
            "only {} of {tables} tables delivered",
            firsts.lock().unwrap().len()
        );
        sleep(Duration::from_millis(200)).await;
    }
    let catch_up = resumed.elapsed();
    stop.store(true, Ordering::Relaxed);
    writer.await.unwrap();
    handle.stop();
    let _ = tokio::time::timeout(Duration::from_secs(60), handle.join).await;
    drain.abort();

    // Every first row arrived, correctly decoded.
    let got = firsts.lock().unwrap().clone();
    for i in 0..tables as i64 {
        assert_eq!(got.get(&format!("t{i:04}")), Some(&(1, i)), "t{i:04}");
    }

    // The binlog inventory and executed set, once, for the offline report.
    let mut c = conn(port).await;
    let logs: Vec<(String, u64)> = c
        .query_map("SHOW BINARY LOGS", |row: mysql_async::Row| {
            let mut row = row;
            (
                row.take::<String, _>(0).unwrap(),
                row.take::<u64, _>(1).unwrap(),
            )
        })
        .await
        .unwrap();
    let uuid: String = c
        .query_first("SELECT @@GLOBAL.server_uuid")
        .await
        .unwrap()
        .unwrap();
    let executed: String = c
        .query_first("SELECT @@GLOBAL.gtid_executed")
        .await
        .unwrap()
        .unwrap();
    let meta = serde_json::json!({
        "mode": mode,
        "tables": tables,
        "gap": gap,
        "rate": rate,
        "catch_up_s": catch_up.as_secs_f64(),
        "server_uuid": uuid,
        "gtid_executed": executed,
        "binlogs": logs,
    });
    std::fs::write(out.join(format!("{stem}-meta.json")), meta.to_string())
        .unwrap();
    *OUT.get().unwrap().lock().unwrap() = None;
    container.rm().await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "evidence harness"]
async fn gtid() {
    amplification(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "evidence harness"]
async fn file_position() {
    amplification(false).await;
}

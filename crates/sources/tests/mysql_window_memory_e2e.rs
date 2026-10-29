//! A window's working memory, measured with jemalloc
//! (`docs/design/mysql-mass-invalidation.md`, "Bounded memory from capture
//! to commit"): one burst of many newly active tables with large schemas
//! holds at most the plan budget, one table's aggregate and a fixed
//! overhead at once, whatever the burst's size. Scaled down with test caps
//! (a 4 MiB plan) so the gate can run it; a run with the default caps is
//! the control that shows the measurement sees the difference.
//!
//! The source runs on a single-thread runtime, so the thread's allocation
//! peak (`thread.peak`) is everything the source, the driver and the
//! store allocate meanwhile.

// Reading jemalloc's statistics is a foreign call.
#![allow(unsafe_code)]

#[path = "test_common.rs"]
mod test_common;

use std::sync::Arc;
use std::time::Instant;

use checkpoints::{CheckpointStore, MemCheckpointStore};
use deltaforge_config::{
    MysqlProofCfg, OnSchemaDrift, SnapshotCfg, SnapshotMode,
};
use deltaforge_core::{Source, SourceItem};
use gate_ownership::GateOwned;
use mysql_async::prelude::Queryable;
use sources::MySqlSource;
use storage::{ArcStorageBackend, DurableSchemaRegistry, MemoryStorageBackend};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner,
};
use tokio::time::{Duration, sleep};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const ROOT_PW: &str = "pw";
const DB: &str = "mem";
const TABLES: usize = 120;
const MIB: u64 = 1024 * 1024;

/// Reset this thread's allocation peak.
fn reset_peak() {
    // SAFETY: a documented write-only mallctl with no argument.
    let rc = unsafe {
        tikv_jemalloc_sys::mallctl(
            c"thread.peak.reset".as_ptr(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0,
        )
    };
    assert_eq!(rc, 0, "thread.peak.reset");
}

/// A u64 statistic of this thread.
fn thread_stat(name: &std::ffi::CStr) -> u64 {
    let mut value: u64 = 0;
    let mut len = std::mem::size_of::<u64>();
    // SAFETY: the named statistic is one u64 written into `value`.
    let rc = unsafe {
        tikv_jemalloc_sys::mallctl(
            name.as_ptr(),
            (&mut value as *mut u64).cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    assert_eq!(rc, 0, "{name:?}");
    value
}

/// This thread's bytes held now (allocated - deallocated, since it began).
fn held() -> i64 {
    thread_stat(c"thread.allocated") as i64
        - thread_stat(c"thread.deallocated") as i64
}

/// This thread's highest (allocated - deallocated) since the last reset.
fn peak() -> u64 {
    let mut value: u64 = 0;
    let mut len = std::mem::size_of::<u64>();
    // SAFETY: `thread.peak.read` writes one u64 into `value`.
    let rc = unsafe {
        tikv_jemalloc_sys::mallctl(
            c"thread.peak.read".as_ptr(),
            (&mut value as *mut u64).cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    assert_eq!(rc, 0, "thread.peak.read");
    value
}

fn dsn(port: u16, db: &str) -> String {
    format!("mysql://root:{ROOT_PW}@127.0.0.1:{port}/{db}")
}

async fn start() -> (ContainerAsync<GenericImage>, u16) {
    let c = GenericImage::new("mysql", "8.4")
        .with_wait_for(WaitFor::message_on_stderr(
            "ready for connections. Version: '8.4",
        ))
        .with_env_var("MYSQL_ROOT_PASSWORD", ROOT_PW)
        .with_cmd([
            "--server-id=81",
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            "--binlog-row-image=FULL",
            "--gtid-mode=ON",
            "--enforce-gtid-consistency=ON",
        ])
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
    let mut c0 = mysql_async::Conn::from_url(dsn(port, "")).await.unwrap();
    c0.query_drop(format!("CREATE DATABASE {DB}"))
        .await
        .unwrap();
    let comment = "x".repeat(1000);
    let cols: String = (1..=48)
        .map(|k| format!(", c{k} INT COMMENT '{comment}'"))
        .collect();
    for i in 0..TABLES {
        c0.query_drop(format!(
            "CREATE TABLE {DB}.t{i:04} (id INT PRIMARY KEY{cols})"
        ))
        .await
        .unwrap();
    }
    (c, port)
}

/// Run a source over `DB.*`, write one row to every table in one burst and
/// return the thread's working peak from just before the burst until every
/// row was delivered: its allocation peak less what it still holds at the
/// end (the state the burst leaves, such as its tables' cached schemas and
/// timelines, bounded by the caches' own limits).
async fn burst_peak(port: u16, id: &str, row: i64) -> u64 {
    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let src = MySqlSource {
        id: id.into(),
        dsn: dsn(port, DB).into(),
        tables: vec![format!("{DB}.*")],
        tenant: "acme".into(),
        pipeline: "mem".into(),
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
        proofs: MysqlProofCfg::default(),
        type_mapping: deltaforge_core::TypeMapping::V2,
    };
    let ckpt: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new().unwrap());
    let (tx, mut rx) = test_common::acked_channel(&src, &ckpt, &src.id, 8192);
    let handle = src.run(tx, Arc::clone(&ckpt)).await;
    let deadline = Instant::now() + Duration::from_secs(60);
    while !handle.ready.is_ready() {
        assert!(!handle.join.is_finished(), "the source ended");
        assert!(Instant::now() < deadline, "the source never streamed");
        sleep(Duration::from_millis(50)).await;
    }
    handle.pause();
    let mut c = mysql_async::Conn::from_url(dsn(port, DB)).await.unwrap();
    for i in 0..TABLES {
        c.query_drop(format!("INSERT INTO t{i:04} (id) VALUES ({row})"))
            .await
            .unwrap();
    }
    c.disconnect().await.ok();
    reset_peak();
    let held_before = held();
    handle.resume();
    let mut rows = 0;
    let deadline = Instant::now() + Duration::from_secs(300);
    while rows < TABLES {
        let left = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(left, rx.recv()).await {
            Ok(Some(SourceItem::Event(e)))
                if e.source.table.starts_with('t') =>
            {
                rows += 1
            }
            Ok(Some(_)) => {}
            Ok(None) => panic!("the source ended after {rows} rows"),
            Err(_) => panic!("only {rows} of {TABLES} rows delivered"),
        }
    }
    let p = peak();
    let retained = (held() - held_before).max(0) as u64;
    eprintln!("{id}: peak {p} bytes, retained {retained} bytes");
    let p = p.saturating_sub(retained);
    handle.stop();
    let _ = tokio::time::timeout(Duration::from_secs(60), handle.join).await;
    p
}

/// A burst of 120 tables with 48 KB schemas: under a 4 MiB plan the peak
/// stays within the plan, one table's aggregate and a fixed overhead; with
/// the default (64 MiB) plan it does not, so the bound is what holds it.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn a_windows_memory_is_bounded_by_its_plan() {
    let (container, port) = start().await;
    let plan = 4 * MIB;
    let overhead = 8 * MIB;
    sources::stream_probe::set_window_caps(Some((plan, 16_384, plan)));
    let bounded = burst_peak(port, "bounded", 1).await;
    sources::stream_probe::set_window_caps(None);
    let unbounded = burst_peak(port, "unbounded", 2).await;
    eprintln!(
        "peak: {bounded} bytes under a {plan}-byte plan, {unbounded} bytes under the default"
    );
    assert!(
        bounded <= plan + plan + overhead,
        "the bounded burst peaked at {bounded} bytes"
    );
    assert!(
        unbounded > plan + plan + overhead,
        "the control did not exceed the bound ({unbounded} bytes): the measurement shows nothing"
    );
    container.rm().await.ok();
}

/// The selection cache's byte estimate (what its 256 MiB bound counts)
/// covers the memory its timelines hold, for timelines of 1, 100 and
/// 10,000 records per table (`docs/design/mysql-mass-invalidation.md`,
/// section 6): resident memory never exceeds the estimate, and the estimate
/// stays within twice resident memory.
#[test]
fn the_cache_estimate_covers_resident_memory() {
    for (tables, records) in [(4096, 1), (1000, 100), (10, 10_000)] {
        let before = held();
        let (estimate, cache) =
            sources::mysql::cache_memory_probe(tables, records);
        let resident = (held() - before) as u64;
        drop(cache);
        let ratio = resident as f64 / estimate as f64;
        eprintln!(
            "{tables} tables of {records} records: estimate {estimate} bytes, \
             resident {resident} bytes, ratio {ratio:.2}"
        );
        assert!(resident <= estimate, "{resident} > {estimate}");
        assert!(estimate <= 2 * resident, "{estimate} > 2 x {resident}");
    }
}

//! Scenario 8 (scale tier only): time to first CDC event on a CDC-only restart
//! of a source whose durable registry holds a large synthetic catalog.
//!
//! Boundaries (Round 14):
//! - the database is already running and ready (provisioning is excluded);
//! - the synthetic registry is generated for the live source's verified
//!   lineage before anything is timed (generation is excluded);
//! - a first run establishes the replication position and commits a
//!   checkpoint, then stops;
//! - a known event is committed while the pipeline is stopped;
//! - timing starts immediately before source startup and stops when an
//!   instrumented sink receives exactly that event.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use checkpoints::CheckpointStore;
use deltaforge_config::{
    BatchConfig, OnSchemaDrift, SnapshotCfg, SnapshotMode,
};
use deltaforge_core::{
    ArcDynProcessor, ArcDynSink, BatchResult, Event, Sink, SinkResult, Source,
    SourceItem,
};
use runner::coordinator::{
    Coordinator, PauseState, build_batch_processor, build_commit_fn,
};
use runner::pipeline_manager::PerSinkCheckpointProxy;
use serde::Serialize;
use storage::adapters::{LineageDescriptor, source_lineage};
use storage::{ArcStorageBackend, DurableSchemaRegistry};
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::sync::{Notify, mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::counting::{CountingBackend, ENUMERATION, KeyRead, OpVector};
use crate::fixture::{self, FixtureSpec, PayloadConfig, TENANT};

const SOURCE: &str = "live-src";
const SINK: &str = "probe";
const WARMUP_ID: i64 = 1;
const KNOWN_ID: i64 = 424_242;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Engine {
    Postgres,
    Mysql,
}

#[derive(Debug, Clone)]
pub struct LiveConfig {
    pub engine: Engine,
    /// Synthetic tables registered for the live source (not in the database).
    pub tables: u64,
    pub versions: u32,
    pub columns: u32,
    pub seed: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct LiveReport {
    pub engine: Engine,
    pub synthetic_tables: u64,
    pub versions: u32,
    pub lineage_hash: String,
    pub generation_secs: f64,
    /// From immediately before source startup to the sink receiving the
    /// known event.
    pub time_to_first_event_ms: f64,
    /// Events delivered in the timed run before the known event.
    pub events_before_known: usize,
    /// Storage operations during the timed window (registry, lineage and
    /// checkpoint namespaces together), and reads per namespace.
    pub ops: OpVector,
    pub enumeration_calls: u64,
    /// Enumeration calls on schema-registry namespaces (`schemas*`,
    /// `schema_lineage`); must be zero.
    pub registry_enumeration_calls: usize,
    /// Every enumeration call in the window, with its namespace and prefix
    /// (an empty key means the whole namespace).
    pub enumerations: Vec<KeyRead>,
    pub reads_by_namespace: Vec<(String, usize)>,
}

/// Records delivered row ids and wakes waiters.
struct ProbeSink {
    ids: Mutex<Vec<i64>>,
    delivered: Notify,
}

#[async_trait]
impl Sink for ProbeSink {
    fn id(&self) -> &str {
        SINK
    }
    async fn send(&self, e: &Event) -> SinkResult<()> {
        self.send_batch(std::slice::from_ref(e)).await.map(|_| ())
    }
    async fn send_batch(&self, events: &[Event]) -> SinkResult<BatchResult> {
        {
            let mut ids = self.ids.lock().unwrap();
            for e in events {
                if let Some(id) = e.after.as_ref().and_then(|a| a.get("id")) {
                    ids.push(id.as_i64().unwrap_or(-1));
                }
            }
        }
        self.delivered.notify_one();
        Ok(BatchResult::ok())
    }
}

impl ProbeSink {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            ids: Mutex::new(Vec::new()),
            delivered: Notify::new(),
        })
    }

    async fn wait_for(&self, id: i64, within: Duration) -> Result<()> {
        let deadline = Instant::now() + within;
        loop {
            if self.ids.lock().unwrap().contains(&id) {
                return Ok(());
            }
            let left = deadline.saturating_duration_since(Instant::now());
            ensure!(
                !left.is_zero(),
                "event {id} was not delivered within {within:?}"
            );
            let _ = tokio::time::timeout(left, self.delivered.notified()).await;
        }
    }
}

struct Running {
    handle: deltaforge_core::SourceHandle,
    coord: tokio::task::JoinHandle<Result<()>>,
    cancel: CancellationToken,
    _pause: watch::Sender<PauseState>,
}

impl Running {
    async fn stop(self) {
        self.cancel.cancel();
        self.handle.stop();
        let _ = self.handle.join().await;
        let _ = self.coord.await;
    }
}

async fn start(
    src: Arc<dyn Source>,
    store: Arc<dyn CheckpointStore>,
    sink: Arc<ProbeSink>,
) -> Running {
    let commit = Arc::new(Notify::new());
    let proxy: Arc<dyn CheckpointStore> = Arc::new(
        PerSinkCheckpointProxy::for_source(
            store.clone(),
            SOURCE.to_string(),
            &src,
        )
        .with_commit_signal(commit.clone()),
    );
    let (tx, rx) = mpsc::channel::<SourceItem>(8192);
    let handle = src.run(tx, proxy).await;
    let cancel = CancellationToken::new();
    let processors: Arc<[ArcDynProcessor]> = Arc::from(vec![]);
    let coord = Coordinator::builder(SOURCE)
        .sinks(vec![sink as ArcDynSink])
        .batch_config(Some(BatchConfig {
            max_events: Some(1000),
            max_ms: Some(10),
            ..BatchConfig::default()
        }))
        .commit_fn(
            SINK,
            build_commit_fn(store, format!("{SOURCE}::sink::{SINK}")),
        )
        .commit_notify(commit)
        .process_fn(build_batch_processor(processors, "scale".to_string()))
        .build();
    let (pause_tx, pause_rx) = watch::channel(PauseState::default());
    let c = cancel.clone();
    let coord = tokio::spawn(async move { coord.run(rx, c, pause_rx).await });
    Running {
        handle,
        coord,
        cancel,
        _pause: pause_tx,
    }
}

// ── databases ────────────────────────────────────────────────────────────────

struct Db {
    _container: ContainerAsync<GenericImage>,
    port: u16,
    engine: Engine,
}

async fn start_db(engine: Engine) -> Result<Db> {
    let (image, inner) = match engine {
        Engine::Postgres => (
            GenericImage::new("postgres", "17")
                .with_wait_for(WaitFor::message_on_stderr(
                    "database system is ready to accept connections",
                ))
                .with_env_var("POSTGRES_PASSWORD", "pw")
                .with_cmd(vec!["postgres", "-c", "wal_level=logical"]),
            5432,
        ),
        Engine::Mysql => (
            GenericImage::new("mysql", "8.4")
                .with_wait_for(WaitFor::message_on_stderr(
                    "ready for connections. Version: '8.4",
                ))
                .with_env_var("MYSQL_ROOT_PASSWORD", "rootpw")
                .with_cmd(vec![
                    "--server-id=999",
                    "--log-bin=/var/lib/mysql/mysql-bin.log",
                    "--binlog-format=ROW",
                    "--binlog-row-image=FULL",
                    "--gtid-mode=ON",
                    "--enforce-gtid-consistency=ON",
                    "--binlog-checksum=NONE",
                ]),
            3306,
        ),
    };
    let container = image.start().await.context("start database container")?;
    let port = container.get_host_port_ipv4(inner).await?;
    Ok(Db {
        _container: container,
        port,
        engine,
    })
}

fn pg_dsn(port: u16) -> String {
    format!(
        "host=127.0.0.1 port={port} user=postgres password=pw dbname=postgres"
    )
}

async fn pg(port: u16) -> Result<tokio_postgres::Client> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match tokio_postgres::connect(&pg_dsn(port), tokio_postgres::NoTls)
            .await
        {
            Ok((c, conn)) => {
                tokio::spawn(conn);
                return Ok(c);
            }
            Err(e) if Instant::now() < deadline => {
                let _ = e;
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

async fn my(dsn: &str) -> Result<mysql_async::Conn> {
    let opts = mysql_async::Opts::from_url(dsn)?;
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match mysql_async::Conn::new(opts.clone()).await {
            Ok(c) => return Ok(c),
            Err(e) if Instant::now() < deadline => {
                let _ = e;
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

fn my_root(port: u16) -> String {
    format!("mysql://root:rootpw@127.0.0.1:{port}/")
}

fn my_cdc(port: u16) -> String {
    format!("mysql://df:dfpw@127.0.0.1:{port}/app")
}

/// Create the captured table and return the database's verified lineage.
async fn prepare(db: &Db) -> Result<LineageDescriptor> {
    use mysql_async::prelude::Queryable;
    match db.engine {
        Engine::Postgres => {
            let c = pg(db.port).await?;
            c.batch_execute(
                "CREATE TABLE app_events (id BIGINT PRIMARY KEY, v TEXT);
                 CREATE PUBLICATION scale_pub FOR TABLE app_events;",
            )
            .await?;
            // A slot cannot be created in a transaction that has written.
            c.batch_execute(
                "SELECT pg_create_logical_replication_slot('scale_slot', 'pgoutput')",
            )
            .await?;
            let sysid: i64 = c
                .query_one(
                    "SELECT system_identifier FROM pg_control_system()",
                    &[],
                )
                .await?
                .get(0);
            let dboid: u32 = c
                .query_one(
                    "SELECT oid FROM pg_database WHERE datname = current_database()",
                    &[],
                )
                .await?
                .get(0);
            LineageDescriptor::postgres(sysid as u64, u64::from(dboid))
        }
        Engine::Mysql => {
            let mut root = my(&my_root(db.port)).await?;
            for q in [
                "CREATE DATABASE app",
                "CREATE USER 'df'@'%' IDENTIFIED BY 'dfpw'",
                "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'df'@'%'",
                "GRANT SELECT ON app.* TO 'df'@'%'",
                "FLUSH PRIVILEGES",
                "CREATE TABLE app.app_events (id BIGINT PRIMARY KEY, v VARCHAR(64))",
            ] {
                root.query_drop(q).await?;
            }
            // Prime caching_sha2_password for the binlog connector.
            my(&my_cdc(db.port)).await?.query_drop("SELECT 1").await?;
            let uuid: String = root
                .query_first("SELECT @@server_uuid")
                .await?
                .context("no server_uuid")?;
            LineageDescriptor::mysql(&uuid)
        }
    }
}

async fn insert(db: &Db, id: i64) -> Result<()> {
    use mysql_async::prelude::Queryable;
    match db.engine {
        Engine::Postgres => {
            pg(db.port)
                .await?
                .execute(
                    "INSERT INTO app_events (id, v) VALUES ($1, 'x')",
                    &[&id],
                )
                .await?;
        }
        Engine::Mysql => {
            my(&my_root(db.port))
                .await?
                .exec_drop(
                    "INSERT INTO app.app_events (id, v) VALUES (?, 'x')",
                    (id,),
                )
                .await?;
        }
    }
    Ok(())
}

fn source(
    db: &Db,
    registry: Arc<DurableSchemaRegistry>,
    backend: ArcStorageBackend,
) -> Arc<dyn Source> {
    let snapshot_cfg = SnapshotCfg {
        mode: SnapshotMode::Never,
        ..Default::default()
    };
    match db.engine {
        Engine::Postgres => Arc::new(sources::postgres::PostgresSource {
            id: SOURCE.into(),
            dsn: pg_dsn(db.port).into(),
            slot: "scale_slot".into(),
            publication: "scale_pub".into(),
            tables: vec!["public.app_events".into()],
            tenant: TENANT.into(),
            pipeline: "scale".into(),
            registry,
            registry_scope:
                sources::registry_scope::SharedRegistryScope::default(),
            outbox_prefixes: Default::default(),
            snapshot_cfg,
            backend,
            on_schema_drift: OnSchemaDrift::Adapt,
            table_options: Default::default(),
            rotation: None,
        }),
        Engine::Mysql => Arc::new(sources::mysql::MySqlSource {
            id: SOURCE.into(),
            dsn: my_cdc(db.port).into(),
            tables: vec!["app.app_events".into()],
            tenant: TENANT.into(),
            pipeline: "scale".into(),
            registry,
            registry_scope:
                sources::registry_scope::SharedRegistryScope::default(),
            backend,
            outbox_tables: Default::default(),
            snapshot_cfg,
            on_schema_drift: OnSchemaDrift::Adapt,
            table_options: Default::default(),
            rotation: None,
        }),
    }
}

/// Run scenario 8 once. `store` must be an empty durable store (the scale
/// tool opens a fresh SQLite file).
pub async fn run(
    cfg: &LiveConfig,
    store: ArcStorageBackend,
) -> Result<LiveReport> {
    let db = start_db(cfg.engine).await?;
    let descriptor = prepare(&db).await?;

    // Synthetic catalog for the live source's own lineage (untimed).
    let spec = FixtureSpec {
        seed: cfg.seed,
        tables: cfg.tables,
        versions: cfg.versions,
        sources: 1,
        payload: PayloadConfig {
            columns: cfg.columns,
        },
        legacy_tables: 0,
        legacy_versions: vec![],
    };
    let t = Instant::now();
    let lineage_hash =
        fixture::populate_source(&store, 0, SOURCE, descriptor, &spec, true)
            .await?;
    let generation_secs = t.elapsed().as_secs_f64();

    let cb = Arc::new(CountingBackend::new(store.clone()));
    let backend: ArcStorageBackend = cb.clone();
    let checkpoints: Arc<dyn CheckpointStore> =
        Arc::new(storage::BackendCheckpointStore::new(backend.clone()));
    let cp_key = format!("{SOURCE}::sink::{SINK}");

    // Run 1: establish the position and commit a checkpoint.
    let registry = DurableSchemaRegistry::new(backend.clone()).await?;
    let sink = ProbeSink::new();
    let run1 = start(
        source(&db, registry, backend.clone()),
        checkpoints.clone(),
        sink.clone(),
    )
    .await;
    if db.engine == Engine::Mysql {
        // The MySQL source starts at the binlog tail; let it get there first.
        tokio::time::sleep(Duration::from_secs(8)).await;
    }
    insert(&db, WARMUP_ID).await?;
    sink.wait_for(WARMUP_ID, Duration::from_secs(120)).await?;
    let deadline = Instant::now() + Duration::from_secs(60);
    while checkpoints.get_raw(&cp_key).await?.is_none() {
        ensure!(
            Instant::now() < deadline,
            "run 1 never committed a checkpoint"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    run1.stop().await;
    let recorded = source_lineage::load(&store, TENANT, SOURCE)
        .await?
        .context("the source recorded no lineage")?;
    if recorded.current.lineage_hash != lineage_hash {
        bail!(
            "the source verified lineage {} but the catalog was generated for {}",
            recorded.current.lineage_hash,
            lineage_hash
        );
    }

    // The known event, committed while the pipeline is stopped.
    insert(&db, KNOWN_ID).await?;

    // Run 2 (timed): a fresh registry, as after a process restart.
    let registry = DurableSchemaRegistry::new(backend.clone()).await?;
    let src = source(&db, registry, backend.clone());
    let sink = ProbeSink::new();
    cb.reset();
    cb.record_keys(true);
    let t0 = Instant::now();
    let run2 = start(src, checkpoints.clone(), sink.clone()).await;
    sink.wait_for(KNOWN_ID, Duration::from_secs(300)).await?;
    let elapsed = t0.elapsed();
    let ops = cb.ops();
    let reads = cb.reads();
    cb.record_keys(false);
    run2.stop().await;

    let events_before_known = {
        let ids = sink.ids.lock().unwrap();
        ids.iter().position(|&i| i == KNOWN_ID).unwrap_or(ids.len())
    };
    let enumerations: Vec<KeyRead> = reads
        .iter()
        .filter(|r| ENUMERATION.contains(&r.primitive))
        .cloned()
        .collect();
    let registry_enumeration_calls = enumerations
        .iter()
        .filter(|r| r.ns.starts_with("schemas") || r.ns == "schema_lineage")
        .count();
    let mut by_ns = std::collections::BTreeMap::<String, usize>::new();
    for r in &reads {
        *by_ns.entry(r.ns.clone()).or_default() += 1;
    }
    Ok(LiveReport {
        engine: cfg.engine,
        synthetic_tables: cfg.tables,
        versions: cfg.versions,
        lineage_hash,
        generation_secs,
        time_to_first_event_ms: elapsed.as_secs_f64() * 1e3,
        events_before_known,
        enumeration_calls: CountingBackend::enumeration_calls(&ops),
        registry_enumeration_calls,
        enumerations,
        ops,
        reads_by_namespace: by_ns.into_iter().collect(),
    })
}

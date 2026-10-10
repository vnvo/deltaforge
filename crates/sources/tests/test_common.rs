#![allow(dead_code)]

//! Shared test infrastructure for sources integration tests.
//!
//! Generic utilities (tracing, random suffixes) are unprefixed.
//! PostgreSQL-specific helpers are prefixed with `pg_`.
//! MySQL tests keep their own inline infrastructure.

use std::sync::{Arc, Once};

use anyhow::Result;
use gate_ownership::GateOwned;
use storage::{ArcStorageBackend, DurableSchemaRegistry, MemoryStorageBackend};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner,
};
use tokio::sync::OnceCell;
use tokio::time::{Duration, sleep};
use tokio_postgres::NoTls;
use tracing_subscriber::{EnvFilter, fmt};

use mysql_async::{Opts, Pool as MySQLPool, prelude::Queryable};
use sources::postgres::PostgresSchemaLoader;

// ============================================================================
// Tracing - shared
// ============================================================================

static INIT: Once = Once::new();

pub fn init_test_tracing() {
    INIT.call_once(|| {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            EnvFilter::new("debug,serial_test=off,hyper=warn,rustls=warn,bollard=off,testcontainers=off")
        });
        let _ = fmt()
            .with_env_filter(filter)
            .with_test_writer()
            .compact()
            .try_init();
    });
}

// ============================================================================
// Snapshot barriers - shared
// ============================================================================

/// The single sink a source run outside a pipeline delivers to.
pub const TEST_SINK: &str = "test";

/// One required sink, [`TEST_SINK`].
pub fn test_cohort() -> deltaforge_core::SnapshotCohort {
    deltaforge_core::SnapshotCohort {
        policy: deltaforge_core::CohortPolicy::Required,
        sinks: vec![deltaforge_core::CohortSink {
            id: TEST_SINK.into(),
            required: true,
        }],
    }
}

/// The channel to run `source` with outside a pipeline: sets its sink
/// cohort ([`TEST_SINK`]) and returns the sender to run it with and the
/// receiver the test reads. Every barrier is committed to the sink's
/// per-sink checkpoint key by the runner's own barrier commit
/// (`build_barrier_fn`: a generation start after the source's local check,
/// a terminal with its checkpoint) and not forwarded; a refused barrier
/// closes the channel, as a failed delivery would. Everything else is
/// forwarded unchanged.
pub fn acked_channel<S>(
    source: &S,
    ckpt: &std::sync::Arc<dyn checkpoints::CheckpointStore>,
    source_id: &str,
    cap: usize,
) -> (
    tokio::sync::mpsc::Sender<deltaforge_core::SourceItem>,
    tokio::sync::mpsc::Receiver<deltaforge_core::SourceItem>,
)
where
    S: deltaforge_core::Source + Clone + 'static,
{
    let source: deltaforge_core::ArcDynSource =
        std::sync::Arc::new(source.clone());
    acked_channel_dyn(&source, ckpt, source_id, cap)
}

/// [`acked_channel`] for a source already built behind `Arc<dyn Source>`
/// (as `build_source` returns it).
#[allow(dead_code)]
pub fn acked_channel_dyn(
    source: &deltaforge_core::ArcDynSource,
    ckpt: &std::sync::Arc<dyn checkpoints::CheckpointStore>,
    source_id: &str,
    cap: usize,
) -> (
    tokio::sync::mpsc::Sender<deltaforge_core::SourceItem>,
    tokio::sync::mpsc::Receiver<deltaforge_core::SourceItem>,
) {
    use deltaforge_core::{BarrierKind, SourceItem};
    use runner::coordinator::{BarrierCommit, build_barrier_fn};
    source.set_snapshot_cohort(test_cohort());
    let commit = build_barrier_fn(
        std::sync::Arc::clone(ckpt),
        format!("{source_id}::sink::{TEST_SINK}"),
        std::sync::Arc::clone(source),
    );
    let (tx_in, mut rx_in) = tokio::sync::mpsc::channel(cap);
    let (tx_out, rx_out) = tokio::sync::mpsc::channel(cap);
    tokio::spawn(async move {
        while let Some(item) = rx_in.recv().await {
            match item {
                SourceItem::Barrier { barrier } => {
                    let c = match barrier.kind {
                        BarrierKind::GenerationStart(s) => {
                            BarrierCommit::Start(s)
                        }
                        BarrierKind::Terminal => BarrierCommit::Checkpoint(
                            barrier.boundary.checkpoint,
                        ),
                    };
                    if let Err(e) = commit(c).await {
                        tracing::error!(error = %e, "test sink refused a barrier");
                        return;
                    }
                }
                other => {
                    if tx_out.send(other).await.is_err() {
                        return;
                    }
                }
            }
        }
    });
    (tx_in, rx_out)
}

/// The stored snapshot control record of `source_id`: `(state, generation)`.
pub async fn snapshot_state(
    backend: &ArcStorageBackend,
    source_id: &str,
) -> Option<(String, u64)> {
    match sources::snapshot_queue::QueueStore::new(backend.clone(), source_id)
        .read()
        .await
        .ok()
        .flatten()?
    {
        sources::snapshot_queue::Stored::Current { control, .. } => Some((
            serde_json::to_value(control.state)
                .ok()?
                .as_str()?
                .to_string(),
            control.generation,
        )),
        sources::snapshot_queue::Stored::Legacy { .. } => None,
    }
}

// ============================================================================
// Random suffix - shared
// ============================================================================

pub fn rand_suffix() -> u32 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::time::SystemTime;

    let mut h = DefaultHasher::new();
    SystemTime::now().hash(&mut h);
    std::thread::current().id().hash(&mut h);
    (h.finish() % 100_000) as u32
}

// ============================================================================
// PostgreSQL container - pg_
// ============================================================================

pub const PG_USER: &str = "postgres";
pub const PG_PASS: &str = "password";
pub const PG_CDC_USER: &str = "df";
pub const PG_CDC_PASS: &str = "dfpw";

// Stores (container, host_port) — dynamic port avoids conflicts when multiple
// test binaries (e.g. postgres_cdc_e2e and postgres_snapshot_e2e) run together.
pub static PG_CONTAINER: OnceCell<(ContainerAsync<GenericImage>, u16)> =
    OnceCell::const_new();

async fn pg_container_and_port() -> &'static (ContainerAsync<GenericImage>, u16)
{
    PG_CONTAINER
        .get_or_init(|| async {
            let img = GenericImage::new("postgres", "17")
                .with_wait_for(WaitFor::message_on_stderr(
                    "database system is ready",
                ))
                .with_env_var("POSTGRES_USER", PG_USER)
                .with_env_var("POSTGRES_PASSWORD", PG_PASS)
                .with_cmd(vec![
                    "postgres",
                    "-c",
                    "wal_level=logical",
                    "-c",
                    "max_replication_slots=10",
                    "-c",
                    "max_wal_senders=10",
                ]);
            // No fixed port — let Docker assign one to avoid conflicts.
            let c = img.gate_owned().start().await.expect("start postgres");
            let port = c.get_host_port_ipv4(5432).await.expect("get pg port");
            sleep(Duration::from_secs(5)).await;
            pg_setup_cdc_user(port).await.expect("setup pg cdc user");
            (c, port)
        })
        .await
}

pub async fn pg_get_container() -> &'static ContainerAsync<GenericImage> {
    &pg_container_and_port().await.0
}

pub async fn pg_port() -> u16 {
    pg_container_and_port().await.1
}

async fn pg_setup_cdc_user(port: u16) -> Result<()> {
    let (client, conn) =
        tokio_postgres::connect(&pg_admin_dsn_on("postgres", port), NoTls)
            .await?;
    tokio::spawn(async move {
        conn.await.ok();
    });
    client
        .execute(
            &format!(
                "CREATE USER {PG_CDC_USER} WITH REPLICATION PASSWORD '{PG_CDC_PASS}'"
            ),
            &[],
        )
        .await
        .ok(); // idempotent
    Ok(())
}

// ============================================================================
// PostgreSQL DSN helpers - pg_
// ============================================================================

/// DSN helpers — async variants resolve the dynamic port from the container.
fn pg_admin_dsn_on(dbname: &str, port: u16) -> String {
    format!(
        "host=127.0.0.1 port={port} user={PG_USER} password={PG_PASS} dbname={dbname}"
    )
}

fn pg_cdc_dsn_on(dbname: &str, port: u16) -> String {
    format!(
        "host=127.0.0.1 port={port} user={PG_CDC_USER} password={PG_CDC_PASS} dbname={dbname}"
    )
}

pub async fn pg_admin_dsn(dbname: &str) -> String {
    pg_admin_dsn_on(dbname, pg_port().await)
}

pub async fn pg_cdc_dsn(dbname: &str) -> String {
    pg_cdc_dsn_on(dbname, pg_port().await)
}

// ============================================================================
// PostgreSQL database lifecycle - pg_
// ============================================================================

/// Create an isolated test database; returns (db_name, superuser client).
pub async fn pg_create_db(
    prefix: &str,
) -> Result<(String, tokio_postgres::Client)> {
    pg_get_container().await;

    let db = format!("df_test_{prefix}_{}", rand_suffix());

    let (admin, conn) =
        tokio_postgres::connect(&pg_admin_dsn("postgres").await, NoTls).await?;
    tokio::spawn(async move {
        conn.await.ok();
    });

    admin
        .execute(&format!("CREATE DATABASE \"{db}\""), &[])
        .await?;
    admin
        .execute(
            &format!("GRANT CONNECT ON DATABASE \"{db}\" TO {PG_CDC_USER}"),
            &[],
        )
        .await?;

    let (client, conn) =
        tokio_postgres::connect(&pg_admin_dsn(&db).await, NoTls).await?;
    tokio::spawn(async move {
        conn.await.ok();
    });

    client
        .execute(
            &format!("GRANT USAGE ON SCHEMA public TO {PG_CDC_USER}"),
            &[],
        )
        .await?;

    Ok((db, client))
}

/// Init tracing + create isolated database. Used at the top of every PG test.
pub async fn pg_setup(
    suffix: &str,
) -> Result<(String, tokio_postgres::Client)> {
    init_test_tracing();
    pg_create_db(suffix).await
}

/// Drop a test database (best-effort, WITH FORCE).
pub async fn pg_drop_db(db: &str) {
    if let Ok((admin, conn)) =
        tokio_postgres::connect(&pg_admin_dsn("postgres").await, NoTls).await
    {
        tokio::spawn(async move {
            conn.await.ok();
        });
        admin
            .execute(
                &format!("DROP DATABASE IF EXISTS \"{db}\" WITH (FORCE)"),
                &[],
            )
            .await
            .ok();
    }
}

// ============================================================================
// PostgreSQL schema / registry helpers - pg_
// ============================================================================

pub async fn make_registry() -> Arc<DurableSchemaRegistry> {
    DurableSchemaRegistry::new(Arc::new(MemoryStorageBackend::new()))
        .await
        .expect("registry")
}

pub async fn make_storage_backend() -> ArcStorageBackend {
    Arc::new(storage::MemoryStorageBackend::new()) as storage::ArcStorageBackend
}

/// Build a schema loader whose registry scope is established through the real
/// PostgreSQL startup path (verified live lineage, durably recorded).
pub async fn pg_make_schema_loader(dsn: &str) -> Result<PostgresSchemaLoader> {
    let backend = make_storage_backend().await;
    let registry = DurableSchemaRegistry::new(Arc::clone(&backend))
        .await
        .expect("registry");
    let scope = sources::registry_scope::SharedRegistryScope::new("test");
    sources::postgres::establish_registry_scope(
        dsn, &backend, &scope, "test", "test",
    )
    .await?;
    Ok(PostgresSchemaLoader::new(dsn, registry, "test", scope))
}

/// Build a schema loader over an existing registry, with its registry scope
/// established through the real PostgreSQL startup path. Returns the scope so
/// a test can build the same qualified keys the loader uses.
pub async fn pg_scoped_loader(
    dsn: &str,
    registry: Arc<DurableSchemaRegistry>,
    tenant: &str,
) -> Result<(
    PostgresSchemaLoader,
    sources::registry_scope::SharedRegistryScope,
)> {
    let backend = make_storage_backend().await;
    let scope = sources::registry_scope::SharedRegistryScope::new("test");
    sources::postgres::establish_registry_scope(
        dsn, &backend, &scope, tenant, "test",
    )
    .await?;
    Ok((
        PostgresSchemaLoader::new(dsn, registry, tenant, scope.clone()),
        scope,
    ))
}

/// Every version of one qualified table, read in bounded pages (test-only
/// convenience over the production `history_page`).
pub async fn registry_history(
    registry: &DurableSchemaRegistry,
    key: &storage::adapters::SchemaKey,
) -> Vec<schema_registry::SchemaVersion> {
    let mut out = Vec::new();
    let mut cursor = None;
    loop {
        let page = registry
            .history_page(key, cursor, 256)
            .await
            .expect("registry history page");
        out.extend(page.versions);
        match page.next {
            Some(next) => cursor = Some(next),
            None => return out,
        }
    }
}

/// Convenience: build a schema loader using the admin DSN for a given db.
// pg_make_schema_loader_for uses make_registry
pub async fn pg_make_schema_loader_for(
    dbname: &str,
) -> Result<PostgresSchemaLoader> {
    pg_make_schema_loader(&pg_admin_dsn(dbname).await).await
}

// ============================================================================
// PostgreSQL replication helpers - pg_
// ============================================================================

pub async fn pg_create_pub_slot(
    client: &tokio_postgres::Client,
    pub_name: &str,
    slot_name: &str,
    tables: &[&str],
) -> Result<()> {
    let tables: Vec<String> =
        tables.iter().map(|t| format!("public.{t}")).collect();
    let tables: Vec<&str> = tables.iter().map(String::as_str).collect();
    sources::postgres::postgres_publication::fixtures::recreate_registered(
        client, pub_name, &tables,
    )
    .await?;

    client
        .execute(
            &format!(
                "SELECT pg_create_logical_replication_slot('{slot_name}', 'pgoutput')"
            ),
            &[],
        )
        .await?;

    Ok(())
}

pub async fn pg_cleanup_repl(
    client: &tokio_postgres::Client,
    pub_name: &str,
    slot_name: &str,
) {
    sources::postgres::postgres_publication::fixtures::release_all(client)
        .await
        .ok();
    client
        .execute(&format!("DROP PUBLICATION IF EXISTS {pub_name}"), &[])
        .await
        .ok();
    client
        .execute(
            &format!(
                "SELECT pg_drop_replication_slot(slot_name) \
                 FROM pg_replication_slots WHERE slot_name = '{slot_name}'"
            ),
            &[],
        )
        .await
        .ok();
}

// ============================================================================
// MySQL container - mysql_
// ============================================================================

pub const MYSQL_ROOT_PASSWORD: &str = "rootpw";
pub const MYSQL_CDC_USER: &str = "df";
pub const MYSQL_CDC_PASSWORD: &str = "dfpw";

pub static MYSQL_CONTAINER: OnceCell<(ContainerAsync<GenericImage>, u16)> =
    OnceCell::const_new();

async fn mysql_container_and_port()
-> &'static (ContainerAsync<GenericImage>, u16) {
    MYSQL_CONTAINER
        .get_or_init(|| async {
            let image = GenericImage::new("mysql", "8.4")
                .with_wait_for(WaitFor::message_on_stderr(
                    "ready for connections",
                ))
                .with_env_var("MYSQL_ROOT_PASSWORD", MYSQL_ROOT_PASSWORD)
                .with_cmd(vec![
                    "--server-id=999",
                    "--log-bin=/var/lib/mysql/mysql-bin.log",
                    "--binlog-format=ROW",
                    "--binlog-row-image=FULL",
                    "--gtid-mode=ON",
                    "--enforce-gtid-consistency=ON",
                    "--binlog-checksum=NONE",
                ]);
            // Dynamic port — no fixed binding to avoid cross-binary conflicts.
            let c = image
                .gate_owned()
                .start()
                .await
                .expect("start mysql container");
            let port =
                c.get_host_port_ipv4(3306).await.expect("get mysql port");
            // The entrypoint restarts mysqld after initialising it: wait for
            // the real server with an authenticated query.
            if let Err(e) =
                until_ready(MYSQL_READY_DEADLINE, READY_STEP, || {
                    mysql_ready_attempt(port)
                })
                .await
            {
                let logs = container_logs(&c).await;
                let _ = c.rm().await;
                panic!("mysql test container never became ready: {e}\n{logs}");
            }
            mysql_provision_cdc_user(port)
                .await
                .expect("provision mysql cdc user");
            (c, port)
        })
        .await
}

pub async fn mysql_get_container() -> &'static ContainerAsync<GenericImage> {
    &mysql_container_and_port().await.0
}

pub async fn mysql_port() -> u16 {
    mysql_container_and_port().await.1
}

/// How long a fresh MySQL container may take to accept an authenticated
/// query (initialisation plus the restart into the real server).
pub const MYSQL_READY_DEADLINE: Duration = Duration::from_secs(120);
const READY_STEP: Duration = Duration::from_millis(500);

/// Whether a failed readiness attempt may be retried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptClass {
    /// The server is still starting (or restarting): connection refused,
    /// reset or closed, too many connections, shutdown in progress.
    Transient,
    /// A real failure - wrong credentials, a bad URL, any other server
    /// error: retrying cannot fix it.
    Fatal,
}

pub fn mysql_attempt_class(e: &mysql_async::Error) -> AttemptClass {
    use mysql_async::{DriverError, Error};
    match e {
        Error::Io(_) | Error::Driver(DriverError::ConnectionClosed) => {
            AttemptClass::Transient
        }
        // ER_CON_COUNT_ERROR, ER_SERVER_SHUTDOWN
        Error::Server(s) if matches!(s.code, 1040 | 1053) => {
            AttemptClass::Transient
        }
        _ => AttemptClass::Fatal,
    }
}

/// Why a server never became ready.
#[derive(Debug)]
pub enum NotReady {
    Fatal(mysql_async::Error),
    Deadline {
        attempts: u32,
        last: Option<mysql_async::Error>,
    },
}

impl std::fmt::Display for NotReady {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NotReady::Fatal(e) => write!(f, "non-transient error: {e}"),
            NotReady::Deadline { attempts, last } => write!(
                f,
                "not ready by the deadline after {attempts} attempts; last \
                 error: {}",
                last.as_ref()
                    .map_or("none (attempts timed out)".into(), |e| e
                        .to_string())
            ),
        }
    }
}

/// Repeat `attempt` every `step` until it succeeds, a non-transient error
/// stops it, or `deadline` passes (an attempt in flight at the deadline is
/// abandoned). Dropping the future stops it.
pub async fn until_ready<F, Fut>(
    deadline: Duration,
    step: Duration,
    mut attempt: F,
) -> Result<(), NotReady>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), mysql_async::Error>>,
{
    let end = tokio::time::Instant::now() + deadline;
    let (mut attempts, mut last) = (0, None);
    loop {
        let left = end.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            return Err(NotReady::Deadline { attempts, last });
        }
        attempts += 1;
        match tokio::time::timeout(left, attempt()).await {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(e)) => match mysql_attempt_class(&e) {
                AttemptClass::Fatal => return Err(NotReady::Fatal(e)),
                AttemptClass::Transient => last = Some(e),
            },
            Err(_) => {}
        }
        let left = end.saturating_duration_since(tokio::time::Instant::now());
        sleep(step.min(left)).await;
    }
}

/// One authenticated query as root.
async fn mysql_ready_attempt(port: u16) -> Result<(), mysql_async::Error> {
    let opts = Opts::from_url(&mysql_root_dsn_on(port))?;
    let mut conn = mysql_async::Conn::new(opts).await?;
    conn.query_drop("SELECT 1").await?;
    conn.disconnect().await
}

/// The container's output, for a failure report.
async fn container_logs(c: &ContainerAsync<GenericImage>) -> String {
    let out = c.stdout_to_vec().await.unwrap_or_default();
    let err = c.stderr_to_vec().await.unwrap_or_default();
    format!(
        "--- container stdout ---\n{}\n--- container stderr ---\n{}",
        String::from_utf8_lossy(&out),
        String::from_utf8_lossy(&err)
    )
}

async fn mysql_provision_cdc_user(port: u16) -> Result<()> {
    let pool = MySQLPool::new(Opts::from_url(&mysql_root_dsn_on(port))?);
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!(
        "CREATE USER IF NOT EXISTS '{}'@'%' IDENTIFIED BY '{}'",
        MYSQL_CDC_USER, MYSQL_CDC_PASSWORD
    ))
    .await?;
    // RELOAD is required for FLUSH TABLES WITH READ LOCK, which brackets the
    // consistent snapshot anchor (snapshot-anchor hardening / MY-1).
    conn.query_drop(format!(
        "GRANT RELOAD, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{}'@'%'",
        MYSQL_CDC_USER
    ))
    .await?;
    conn.query_drop("FLUSH PRIVILEGES").await?;
    Ok(())
}

// ============================================================================
// MySQL DSN helpers - mysql_
// ============================================================================

fn mysql_root_dsn_on(port: u16) -> String {
    format!("mysql://root:{}@127.0.0.1:{}/", MYSQL_ROOT_PASSWORD, port)
}

fn mysql_cdc_dsn_on(db: &str, port: u16) -> String {
    format!(
        "mysql://{}:{}@127.0.0.1:{}/{}",
        MYSQL_CDC_USER, MYSQL_CDC_PASSWORD, port, db
    )
}

pub async fn mysql_root_dsn() -> String {
    mysql_root_dsn_on(mysql_port().await)
}

pub async fn mysql_cdc_dsn(db: &str) -> String {
    mysql_cdc_dsn_on(db, mysql_port().await)
}

// ============================================================================
// MySQL database lifecycle - mysql_
// ============================================================================

pub async fn mysql_create_db(test_name: &str) -> Result<(String, MySQLPool)> {
    mysql_get_container().await;
    let db = format!("test_{}", test_name.replace('-', "_"));
    let pool = MySQLPool::new(Opts::from_url(&mysql_root_dsn().await)?);
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("DROP DATABASE IF EXISTS {db}"))
        .await?;
    conn.query_drop(format!("CREATE DATABASE {db}")).await?;
    conn.query_drop(format!(
        "GRANT SELECT, SHOW VIEW ON {db}.* TO '{}'@'%'",
        MYSQL_CDC_USER
    ))
    .await?;
    Ok((db, pool))
}

pub async fn mysql_setup(name: &str) -> Result<(String, MySQLPool, String)> {
    init_test_tracing();
    let (db, pool) = mysql_create_db(name).await?;
    let dsn = mysql_cdc_dsn(&db).await;
    Ok((db, pool, dsn))
}

pub async fn mysql_drop_db(pool: &MySQLPool, db: &str) {
    if let Ok(mut conn) = pool.get_conn().await {
        conn.query_drop(format!("DROP DATABASE IF EXISTS {db}"))
            .await
            .ok();
    }
}

/// Writers committing continuously, until finished, into `{db}.busy` (an
/// untracked table of the tracked database) and `{db}_other.busy` (another
/// database): the server position never stands still while a source proves
/// its tables.
#[allow(dead_code)]
pub struct Busy {
    stop: Arc<std::sync::atomic::AtomicBool>,
    tasks: Vec<tokio::task::JoinHandle<u64>>,
}

#[allow(dead_code)]
impl Busy {
    pub async fn start(dsn: &str, db: &str) -> Self {
        let busy = "busy (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT)";
        let mut c = mysql_async::Conn::from_url(dsn).await.unwrap();
        for s in [
            format!("CREATE TABLE IF NOT EXISTS {db}.{busy}"),
            format!("CREATE DATABASE IF NOT EXISTS {db}_other"),
            format!("CREATE TABLE IF NOT EXISTS {db}_other.{busy}"),
        ] {
            c.query_drop(&s).await.unwrap();
        }
        c.disconnect().await.ok();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut tasks = Vec::new();
        for target in [format!("{db}.busy"), format!("{db}_other.busy")] {
            for _ in 0..2 {
                let q = format!("INSERT INTO {target} (v) VALUES (1)");
                let (dsn, stop) = (dsn.to_string(), stop.clone());
                tasks.push(tokio::spawn(async move {
                    let mut c = mysql_async::Conn::from_url(dsn).await.unwrap();
                    let mut n = 0;
                    while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                        c.query_drop(&q).await.unwrap();
                        n += 1;
                    }
                    c.disconnect().await.ok();
                    n
                }));
            }
        }
        sleep(Duration::from_millis(300)).await;
        Self { stop, tasks }
    }

    /// Stops the writers; returns how many commits they made.
    pub async fn finish(self) -> u64 {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
        let mut total = 0;
        for t in self.tasks {
            total += t.await.unwrap();
        }
        total
    }
}

// ============================================================================
// Sink checkpoint initialization - shared
// ============================================================================

/// A checkpoint store that holds nothing and refuses every write.
pub struct UnwritableStore;

#[async_trait::async_trait]
impl checkpoints::CheckpointStore for UnwritableStore {
    async fn get_raw(
        &self,
        _key: &str,
    ) -> checkpoints::CheckpointResult<Option<Vec<u8>>> {
        Ok(None)
    }
    async fn put_raw(
        &self,
        _key: &str,
        _bytes: &[u8],
    ) -> checkpoints::CheckpointResult<()> {
        Err(checkpoints::CheckpointError::Database("unwritable".into()))
    }
    async fn delete(&self, _key: &str) -> checkpoints::CheckpointResult<bool> {
        Ok(false)
    }
    async fn list(&self) -> checkpoints::CheckpointResult<Vec<String>> {
        Ok(vec![])
    }
}

/// The production per-sink checkpoint proxy for `source` with one
/// configured sink, over a store that cannot persist its checkpoint.
pub fn unwritable_sink_checkpoints(
    source: deltaforge_core::ArcDynSource,
    source_id: &str,
) -> Arc<dyn checkpoints::CheckpointStore> {
    Arc::new(
        runner::pipeline_manager::PerSinkCheckpointProxy::for_source(
            Arc::new(UnwritableStore),
            source_id.to_string(),
            &source,
        )
        .with_sinks(vec![TEST_SINK.to_string()]),
    )
}

/// The source stops before it streams: its run fails with the
/// initialization error and nothing was emitted.
pub async fn assert_fails_closed_without_sink_checkpoints(
    handle: deltaforge_core::SourceHandle,
    rx: &mut tokio::sync::mpsc::Receiver<deltaforge_core::SourceItem>,
) {
    let result = tokio::time::timeout(Duration::from_secs(60), handle.join)
        .await
        .expect("the source stops")
        .expect("the source task joins");
    let err = result.expect_err("startup fails closed");
    assert!(
        err.to_string().contains("initialize the sink checkpoints"),
        "{err}"
    );
    while let Ok(item) = rx.try_recv() {
        assert!(
            !matches!(item, deltaforge_core::SourceItem::Event(_)),
            "an event was emitted"
        );
    }
}

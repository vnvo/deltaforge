#![allow(dead_code)]

//! Shared test infrastructure for sources integration tests.
//!
//! Generic utilities (tracing, random suffixes) are unprefixed.
//! PostgreSQL-specific helpers are prefixed with `pg_`.
//! MySQL tests keep their own inline infrastructure.

use std::sync::{Arc, Once};

use anyhow::Result;
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
            let c = img.start().await.expect("start postgres");
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

pub async fn pg_make_schema_loader(dsn: &str) -> Result<PostgresSchemaLoader> {
    Ok(PostgresSchemaLoader::new(
        dsn,
        make_registry().await,
        "test",
    ))
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
    client
        .execute(&format!("DROP PUBLICATION IF EXISTS {pub_name}"), &[])
        .await?;

    let table_list = tables
        .iter()
        .map(|t| format!("public.{t}"))
        .collect::<Vec<_>>()
        .join(", ");

    client
        .execute(
            &format!("CREATE PUBLICATION {pub_name} FOR TABLE {table_list}"),
            &[],
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
            let c = image.start().await.expect("start mysql container");
            let port =
                c.get_host_port_ipv4(3306).await.expect("get mysql port");
            sleep(Duration::from_secs(8)).await;
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

async fn mysql_provision_cdc_user(port: u16) -> Result<()> {
    let pool = MySQLPool::new(Opts::from_url(&mysql_root_dsn_on(port))?);
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!(
        "CREATE USER IF NOT EXISTS '{}'@'%' IDENTIFIED BY '{}'",
        MYSQL_CDC_USER, MYSQL_CDC_PASSWORD
    ))
    .await?;
    conn.query_drop(format!(
        "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{}'@'%'",
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

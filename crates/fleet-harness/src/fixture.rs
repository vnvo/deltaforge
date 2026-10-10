//! MySQL fixtures (design section 4.2).
//!
//! A fixture is built once per server and reused: creation is idempotent
//! (`IF NOT EXISTS`) and resumable, and `harness_meta.fixture` records the
//! spec it was built from, so a server holding a different fixture is never
//! silently reused. Tables start empty; the workload driver writes the rows.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use mysql_async::prelude::Queryable;
use mysql_async::{Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts};
use serde::Serialize;

use crate::config::{RunConfig, Server};
use crate::topology::{Naming, Template};

/// Template format version: bump when generated DDL changes.
const FORMAT: u32 = 1;
const META_DB: &str = "harness_meta";

/// What a fixture is built from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FixtureSpec {
    pub databases: u32,
    pub tables: u32,
    pub prefix: String,
    pub seed: u64,
    pub format: u32,
}

impl FixtureSpec {
    pub fn of(cfg: &RunConfig) -> Self {
        FixtureSpec {
            databases: cfg.topology.databases_per_server.value,
            tables: cfg.topology.tables_per_database.value,
            prefix: cfg.topology.database_prefix.clone(),
            seed: cfg.topology.seed,
            format: FORMAT,
        }
    }

    pub fn id(&self) -> String {
        format!(
            "{}x{}:{}:{}:v{}",
            self.databases, self.tables, self.prefix, self.seed, self.format
        )
    }
}

/// A pool to `server` as the harness admin.
pub fn admin_pool(
    cfg: &RunConfig,
    server: &Server,
    max: usize,
) -> Result<Pool> {
    let opts = OptsBuilder::default()
        .ip_or_hostname(server.host.clone())
        .tcp_port(server.port)
        .user(Some(cfg.topology.admin_user.clone()))
        .pass(Some(cfg.topology.admin_password.clone()))
        .pool_opts(PoolOpts::default().with_constraints(
            PoolConstraints::new(1, max.max(1)).expect("min <= max"),
        ));
    Ok(Pool::new(Opts::from(opts)))
}

#[derive(Debug, Clone, Serialize)]
pub struct FixtureReport {
    pub server: String,
    pub spec: String,
    pub reused: bool,
    pub tables: u64,
    pub build_secs: f64,
}

/// Build (or resume, or reuse) the fixture on `server`.
pub async fn build(
    cfg: &RunConfig,
    server: &Server,
    concurrency: usize,
    progress: bool,
) -> Result<FixtureReport> {
    let spec = FixtureSpec::of(cfg);
    let pool = admin_pool(cfg, server, concurrency + 1)?;
    let mut conn = pool.get_conn().await.context("connect for the fixture")?;
    conn.query_drop(format!("CREATE DATABASE IF NOT EXISTS `{META_DB}`"))
        .await?;
    conn.query_drop(format!(
        "CREATE TABLE IF NOT EXISTS `{META_DB}`.`fixture` (\
         id INT PRIMARY KEY, spec VARCHAR(255) NOT NULL, complete BOOL NOT NULL, \
         build_secs DOUBLE NOT NULL)"
    ))
    .await?;
    let existing: Option<(String, bool, f64)> = conn
        .query_first(format!(
            "SELECT spec, complete, build_secs FROM `{META_DB}`.`fixture` WHERE id = 1"
        ))
        .await?;
    match existing {
        Some((s, true, secs)) if s == spec.id() => {
            let tables = count_tables(&mut conn, &spec.prefix).await?;
            return Ok(FixtureReport {
                server: server.name.clone(),
                spec: spec.id(),
                reused: true,
                tables,
                build_secs: secs,
            });
        }
        Some((s, _, _)) if s != spec.id() => bail!(
            "server {} holds fixture {s}, not {}: use another server or drop it",
            server.name,
            spec.id()
        ),
        _ => {}
    }
    conn.query_drop(format!(
        "REPLACE INTO `{META_DB}`.`fixture` VALUES (1, '{}', FALSE, 0)",
        spec.id()
    ))
    .await?;
    drop(conn);

    let started = Instant::now();
    let naming = Naming {
        database_prefix: spec.prefix.clone(),
    };
    let templates: Arc<Vec<Template>> = Arc::new(
        (0..spec.tables)
            .map(|t| Template::generate(spec.seed, t as u16))
            .collect(),
    );
    let next = Arc::new(AtomicU32::new(0));
    let mut workers = Vec::new();
    for _ in 0..concurrency.max(1) {
        let (pool, next, templates, naming) = (
            pool.clone(),
            next.clone(),
            templates.clone(),
            naming.clone(),
        );
        let databases = spec.databases;
        workers.push(tokio::spawn(async move {
            let mut conn = pool.get_conn().await?;
            loop {
                let db = next.fetch_add(1, Ordering::Relaxed);
                if db >= databases {
                    return Ok::<_, anyhow::Error>(());
                }
                let name = naming.database(db);
                conn.query_drop(format!(
                    "CREATE DATABASE IF NOT EXISTS `{name}`"
                ))
                .await?;
                for t in templates.iter() {
                    conn.query_drop(
                        t.create_sql(&name, &naming.table(t.index)),
                    )
                    .await
                    .with_context(|| {
                        format!("create {name}.t{:03}", t.index)
                    })?;
                }
                if progress && db % 100 == 0 {
                    eprintln!("  fixture: database {db}/{databases}");
                }
            }
        }));
    }
    for w in workers {
        w.await??;
    }
    let build_secs = started.elapsed().as_secs_f64();
    let mut conn = pool.get_conn().await?;
    let tables = count_tables(&mut conn, &spec.prefix).await?;
    let want = u64::from(spec.databases) * u64::from(spec.tables);
    if tables < want {
        bail!(
            "fixture on {}: {tables} tables, expected {want}",
            server.name
        );
    }
    conn.query_drop(format!(
        "UPDATE `{META_DB}`.`fixture` SET complete = TRUE, build_secs = {build_secs} WHERE id = 1"
    ))
    .await?;
    drop(conn);
    pool.disconnect().await.ok();
    Ok(FixtureReport {
        server: server.name.clone(),
        spec: spec.id(),
        reused: false,
        tables,
        build_secs,
    })
}

async fn count_tables(
    conn: &mut mysql_async::Conn,
    prefix: &str,
) -> Result<u64> {
    let like = format!("{}%", prefix.replace('_', "\\_"));
    let n: Option<u64> = conn
        .exec_first(
            "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA LIKE ?",
            (like,),
        )
        .await?;
    Ok(n.unwrap_or(0))
}

/// Create the CDC user DeltaForge connects as (idempotent).
pub async fn ensure_cdc_user(cfg: &RunConfig, server: &Server) -> Result<()> {
    let pool = admin_pool(cfg, server, 1)?;
    let mut conn = pool.get_conn().await?;
    let (u, p) = (&cfg.deltaforge.cdc_user, &cfg.deltaforge.cdc_password);
    conn.query_drop(format!(
        "CREATE USER IF NOT EXISTS '{u}'@'%' IDENTIFIED BY '{p}'"
    ))
    .await?;
    conn.query_drop(format!(
        "GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{u}'@'%'"
    ))
    .await?;
    drop(conn);
    pool.disconnect().await.ok();
    Ok(())
}

/// Server settings that matter at this scale (design R11), recorded with
/// every run.
pub const SETTINGS: [&str; 14] = [
    "version",
    "binlog_format",
    "binlog_row_image",
    "binlog_row_metadata",
    "gtid_mode",
    "binlog_expire_logs_seconds",
    "table_open_cache",
    "table_definition_cache",
    "open_files_limit",
    "information_schema_stats_expiry",
    "innodb_file_per_table",
    "max_connections",
    "lower_case_table_names",
    "innodb_redo_log_capacity",
];

pub async fn settings(
    conn: &mut mysql_async::Conn,
) -> Result<BTreeMap<String, String>> {
    let rows: Vec<(String, String)> =
        conn.query("SHOW GLOBAL VARIABLES").await?;
    Ok(rows
        .into_iter()
        .filter(|(k, _)| SETTINGS.contains(&k.as_str()))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::tests::EXAMPLE;

    #[test]
    fn the_fixture_id_changes_with_any_input() {
        let cfg: RunConfig = serde_yaml::from_str(EXAMPLE).unwrap();
        let a = FixtureSpec::of(&cfg);
        assert_eq!(a.id(), "2000x200:cust_:1:v1");
        let mut b = a.clone();
        b.seed = 2;
        assert_ne!(a.id(), b.id());
    }
}

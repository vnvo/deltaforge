//! MySQL fixtures (design section 4.2).
//!
//! A fixture is built once per server and reused: creation is idempotent
//! (`IF NOT EXISTS`) and resumable, and `harness_meta.fixture` records the
//! spec it was built from, so a server holding a different fixture is never
//! silently reused. Tables start empty; the workload driver writes the rows.
//!
//! A complete fixture can grow to more tables per database when asked to
//! (`--grow`): only the missing tables are created, and the growth itself
//! is resumable. Tables live in one file each or, with the
//! `general_per_database` layout, in one general tablespace per database.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use mysql_async::prelude::Queryable;
use mysql_async::{Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts};
use serde::Serialize;

use crate::config::{RunConfig, Server, TablespaceLayout};
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
    pub layout: TablespaceLayout,
}

impl FixtureSpec {
    pub fn of(cfg: &RunConfig) -> Self {
        FixtureSpec {
            databases: cfg.topology.databases_per_server.value,
            tables: cfg.topology.tables_per_database.value,
            prefix: cfg.topology.database_prefix.clone(),
            seed: cfg.topology.seed,
            format: FORMAT,
            layout: cfg.topology.tablespace,
        }
    }

    /// `<databases>x<tables>:<prefix>:<seed>:v<format>`, plus `:gts` for
    /// the general-tablespace layout (file-per-table ids are unchanged, so
    /// fixtures built before layouts existed are still recognised).
    pub fn id(&self) -> String {
        let id = format!(
            "{}x{}:{}:{}:v{}",
            self.databases, self.tables, self.prefix, self.seed, self.format
        );
        match self.layout {
            TablespaceLayout::FilePerTable => id,
            TablespaceLayout::GeneralPerDatabase => format!("{id}:gts"),
        }
    }

    /// The spec an [`id`](Self::id) was made from, if it is one.
    pub fn parse(id: &str) -> Option<FixtureSpec> {
        let parts: Vec<&str> = id.split(':').collect();
        let layout = match parts.get(4) {
            None => TablespaceLayout::FilePerTable,
            Some(&"gts") if parts.len() == 5 => {
                TablespaceLayout::GeneralPerDatabase
            }
            Some(_) => return None,
        };
        if parts.len() < 4 {
            return None;
        }
        let (databases, tables) = parts[0].split_once('x')?;
        Some(FixtureSpec {
            databases: databases.parse().ok()?,
            tables: tables.parse().ok()?,
            prefix: parts[1].to_string(),
            seed: parts[2].parse().ok()?,
            format: parts[3].strip_prefix('v')?.parse().ok()?,
            layout,
        })
    }
}

/// What to do with the fixture a server holds (`harness_meta.fixture`).
#[derive(Debug, Clone, PartialEq, Eq)]
enum Decision {
    /// The wanted fixture, complete.
    Reuse,
    /// Build it, or resume an interrupted build or growth.
    Build,
    /// Grow a complete fixture of `from_tables` tables per database.
    Grow { from_tables: u32 },
}

fn decide(
    held: Option<(String, bool)>,
    want: &FixtureSpec,
    allow_grow: bool,
) -> Result<Decision> {
    let Some((id, complete)) = held else {
        return Ok(Decision::Build);
    };
    if id == want.id() {
        return Ok(if complete {
            Decision::Reuse
        } else {
            Decision::Build
        });
    }
    let growable = complete
        && FixtureSpec::parse(&id).is_some_and(|h| {
            h.tables < want.tables
                && FixtureSpec {
                    tables: want.tables,
                    ..h
                } == *want
        });
    match (growable, allow_grow) {
        (true, true) => Ok(Decision::Grow {
            from_tables: FixtureSpec::parse(&id).expect("parsed").tables,
        }),
        (true, false) => bail!(
            "holds fixture {id}, not {}: grow it (--grow), use another server or drop it",
            want.id()
        ),
        (false, _) => bail!(
            "holds fixture {id}, not {}: use another server or drop it",
            want.id()
        ),
    }
}

/// Create `name` (idempotent) and, with a general layout, its tablespace;
/// returns the tablespace its tables go in.
pub async fn create_database(
    conn: &mut mysql_async::Conn,
    name: &str,
    layout: TablespaceLayout,
) -> Result<Option<String>> {
    conn.query_drop(format!("CREATE DATABASE IF NOT EXISTS `{name}`"))
        .await?;
    let Some(ts) = layout.tablespace(name) else {
        return Ok(None);
    };
    let exists: Option<u64> = conn
        .exec_first(
            "SELECT COUNT(*) FROM information_schema.INNODB_TABLESPACES WHERE NAME = ?",
            (ts.as_str(),),
        )
        .await?;
    if exists.unwrap_or(0) == 0 {
        conn.query_drop(format!(
            "CREATE TABLESPACE `{ts}` ADD DATAFILE '{ts}.ibd' ENGINE=InnoDB"
        ))
        .await
        .with_context(|| format!("create tablespace {ts}"))?;
    }
    Ok(Some(ts))
}

/// Drop `name` and, with a general layout, its tablespace (idempotent).
pub async fn drop_database(
    conn: &mut mysql_async::Conn,
    name: &str,
    layout: TablespaceLayout,
) -> Result<()> {
    conn.query_drop(format!("DROP DATABASE IF EXISTS `{name}`"))
        .await?;
    if let Some(ts) = layout.tablespace(name) {
        let exists: Option<u64> = conn
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.INNODB_TABLESPACES WHERE NAME = ?",
                (ts.as_str(),),
            )
            .await?;
        if exists.unwrap_or(0) > 0 {
            conn.query_drop(format!("DROP TABLESPACE `{ts}`")).await?;
        }
    }
    Ok(())
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
    /// The fixture this build grew from, when it grew one.
    pub grown_from: Option<String>,
    pub tables: u64,
    pub build_secs: f64,
}

/// Build (or resume, or reuse) the fixture on `server`; with `allow_grow`,
/// a complete fixture with fewer tables per database grows to the spec.
pub async fn build(
    cfg: &RunConfig,
    server: &Server,
    concurrency: usize,
    progress: bool,
    allow_grow: bool,
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
    let decision = decide(
        existing.as_ref().map(|(s, c, _)| (s.clone(), *c)),
        &spec,
        allow_grow,
    )
    .with_context(|| format!("server {}", server.name))?;
    let first_table = match decision {
        Decision::Reuse => {
            let tables = count_tables(&mut conn, &spec.prefix).await?;
            return Ok(FixtureReport {
                server: server.name.clone(),
                spec: spec.id(),
                reused: true,
                grown_from: None,
                tables,
                build_secs: existing.map_or(0.0, |(_, _, s)| s),
            });
        }
        Decision::Build => 0,
        Decision::Grow { from_tables } => from_tables,
    };
    let grown_from = matches!(decision, Decision::Grow { .. })
        .then(|| existing.as_ref().map(|(s, _, _)| s.clone()))
        .flatten();
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
        (first_table..spec.tables)
            .map(|t| Template::generate(spec.seed, t as u16))
            .collect(),
    );
    let layout = spec.layout;
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
                let ts = create_database(&mut conn, &name, layout).await?;
                for t in templates.iter() {
                    conn.query_drop(t.create_sql(
                        &name,
                        &naming.table(t.index),
                        ts.as_deref(),
                    ))
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
        grown_from,
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
        "GRANT SELECT, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, BACKUP_ADMIN ON *.* TO '{u}'@'%'"
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
        let mut c = a.clone();
        c.layout = TablespaceLayout::GeneralPerDatabase;
        assert_eq!(c.id(), "2000x200:cust_:1:v1:gts");
    }

    #[test]
    fn a_fixture_id_parses_back_to_its_spec() {
        let cfg: RunConfig = serde_yaml::from_str(EXAMPLE).unwrap();
        let a = FixtureSpec::of(&cfg);
        assert_eq!(FixtureSpec::parse(&a.id()), Some(a.clone()));
        let mut g = a;
        g.layout = TablespaceLayout::GeneralPerDatabase;
        assert_eq!(FixtureSpec::parse(&g.id()), Some(g));
        assert_eq!(FixtureSpec::parse("2000x200:cust_:1"), None);
        assert_eq!(FixtureSpec::parse("2000x200:cust_:1:v1:xyz"), None);
    }

    fn spec(databases: u32, tables: u32) -> FixtureSpec {
        FixtureSpec {
            databases,
            tables,
            prefix: "cust_".into(),
            seed: 1,
            format: FORMAT,
            layout: TablespaceLayout::GeneralPerDatabase,
        }
    }

    #[test]
    fn a_complete_smaller_fixture_grows_only_when_allowed() {
        let want = spec(2000, 200);
        let held = Some((spec(2000, 50).id(), true));
        assert_eq!(
            decide(held.clone(), &want, true).unwrap(),
            Decision::Grow { from_tables: 50 }
        );
        let err = decide(held, &want, false).unwrap_err().to_string();
        assert!(err.contains("holds fixture 2000x50"), "{err}");
    }

    #[test]
    fn the_same_fixture_is_reused_or_resumed_and_nothing_else_grows() {
        let want = spec(2000, 200);
        assert_eq!(decide(None, &want, false).unwrap(), Decision::Build);
        assert_eq!(
            decide(Some((want.id(), true)), &want, false).unwrap(),
            Decision::Reuse
        );
        assert_eq!(
            decide(Some((want.id(), false)), &want, true).unwrap(),
            Decision::Build,
            "an interrupted build (or growth) resumes"
        );
        let refused = |held: FixtureSpec, complete: bool| {
            decide(Some((held.id(), complete)), &want, true).is_err()
        };
        assert!(refused(spec(2000, 300), true), "never shrinks");
        assert!(refused(spec(2000, 50), false), "an incomplete build");
        assert!(refused(spec(1000, 50), true), "other databases");
        let mut other = spec(2000, 50);
        other.layout = TablespaceLayout::FilePerTable;
        assert!(refused(other, true), "another layout");
        let mut other = spec(2000, 50);
        other.seed = 2;
        assert!(refused(other, true), "other templates");
        assert!(
            decide(Some(("legacy".into(), true)), &want, true).is_err(),
            "an id it cannot read"
        );
    }

    #[test]
    fn a_general_layout_names_one_tablespace_per_database() {
        assert_eq!(
            TablespaceLayout::GeneralPerDatabase.tablespace("cust_00007"),
            Some("cust_00007_ts".into())
        );
        assert_eq!(
            TablespaceLayout::FilePerTable.tablespace("cust_00007"),
            None
        );
    }
}

//! Physical state-store qualification (design section 4.6), one matrix cell
//! per invocation against an empty PostgreSQL store.
//!
//! Records are generated through DeltaForge's own registration path
//! (`scale-harness`): per source, the unchanged tables at one version and
//! the changed fraction at the cell's version count. Then, on the populated
//! store: relation and index sizes, cold and warm latency of the store's hot
//! queries (verbatim from `storage/src/postgres.rs`, with keys sampled from
//! the store), `EXPLAIN (ANALYZE, BUFFERS)` of each, autovacuum and analyze
//! statistics, and timed backup and restore hooks.
//!
//! Not covered in this tooling-only form, and recorded as such in every
//! result: activation histories and barrier streams, and byte-equivalent
//! bulk fixtures for 10M+ tables. Both need a test-support entry point in
//! the product crates (their encoders are internal), which is a separate
//! reviewed change.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use rand::SeedableRng;
use rand::seq::SliceRandom;
use serde::Serialize;
use serde_json::Value;
use tokio_postgres::types::ToSql;

use crate::config::RunClass;
use crate::stats::{Histogram, Summary};

/// A hot read of the store: its SQL (verbatim from the backend) and the
/// parameters it takes.
#[derive(Debug, Clone, Copy)]
pub struct HotQuery {
    pub name: &'static str,
    pub table: &'static str,
    pub sql: &'static str,
    pub params: Params,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Params {
    NsKey,
    NsKeySeq,
    NsKeySeqLimit,
}

pub const HOT_QUERIES: [HotQuery; 6] = [
    HotQuery {
        name: "kv_get",
        table: "df_kv",
        sql: "SELECT val, expires_at FROM df_kv WHERE ns=$1 AND key=$2",
        params: Params::NsKey,
    },
    HotQuery {
        name: "log_latest",
        table: "df_log",
        sql: "SELECT seq, val FROM df_log WHERE ns=$1 AND key=$2 ORDER BY seq DESC LIMIT 1",
        params: Params::NsKey,
    },
    HotQuery {
        name: "log_history",
        table: "df_log",
        sql: "SELECT seq, val FROM df_log WHERE ns=$1 AND key=$2 ORDER BY seq ASC",
        params: Params::NsKey,
    },
    HotQuery {
        name: "log_since",
        table: "df_log",
        sql: "SELECT seq, val FROM df_log WHERE ns=$1 AND key=$2 AND seq>$3 ORDER BY seq ASC",
        params: Params::NsKeySeq,
    },
    HotQuery {
        name: "log_page",
        table: "df_log",
        sql: "SELECT seq, COALESCE(ts_ms, ts*1000), capture_id, content_hash, val FROM df_log WHERE ns=$1 AND key=$2 AND seq>$3 ORDER BY seq ASC LIMIT $4",
        params: Params::NsKeySeqLimit,
    },
    HotQuery {
        name: "log_meta",
        table: "df_log_meta",
        sql: "SELECT min_valid_from_seq, head_seq FROM df_log_meta WHERE ns=$1 AND key=$2",
        params: Params::NsKey,
    },
];

pub const NOT_COVERED: [&str; 2] = [
    "activation histories and barrier streams (their records are built by internal encoders; needs a test-support entry point: separate reviewed change)",
    "byte-equivalent bulk fixtures for 10M+ tables (same reason); cells beyond what production-path generation reaches in time are not run",
];

#[derive(Debug, Clone, Serialize)]
pub struct Cell {
    pub sources: u32,
    pub tables_per_source: u64,
    pub versions_changed: u32,
    pub changed_fraction: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryResult {
    pub name: String,
    pub sql: String,
    pub cold_us: Summary,
    pub warm_us: Summary,
    pub explain: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct StoreResult {
    pub class: RunClass,
    pub evidence: &'static str,
    pub cell: Cell,
    pub generated_versions: u64,
    pub generation_secs: f64,
    pub registrations_per_sec: f64,
    pub database_bytes: u64,
    /// Relation -> (table bytes, index bytes, total bytes).
    pub relations: BTreeMap<String, (u64, u64, u64)>,
    pub queries: Vec<QueryResult>,
    pub vacuum: Value,
    pub backup_secs: Option<f64>,
    pub restore_secs: Option<f64>,
    pub not_covered: Vec<&'static str>,
    pub harness_revision: String,
}

async fn count(client: &tokio_postgres::Client, table: &str) -> Result<i64> {
    let exists: bool = client
        .query_one("SELECT to_regclass($1) IS NOT NULL", &[&table])
        .await?
        .get(0);
    if !exists {
        return Ok(0);
    }
    Ok(client
        .query_one(&format!("SELECT COUNT(*) FROM {table}"), &[])
        .await?
        .get(0))
}

/// Run one cell against the (empty) store at `dsn`.
pub async fn run_cell(
    dsn: &str,
    class: RunClass,
    cell: Cell,
    samples: usize,
    hooks: &BTreeMap<String, String>,
    output_dir: &str,
) -> Result<StoreResult> {
    let (client, conn) =
        tokio_postgres::connect(dsn, tokio_postgres::NoTls).await?;
    tokio::spawn(conn);
    for t in ["df_kv", "df_log"] {
        if count(&client, t).await? > 0 {
            bail!(
                "the store at the DSN is not empty ({t} has rows): one cell per empty store"
            );
        }
    }
    let backend: storage::ArcStorageBackend =
        storage::PostgresStorageBackend::connect(dsn).await?;

    // Generation through the production registration path.
    let started = Instant::now();
    let changed =
        (cell.tables_per_source as f64 * cell.changed_fraction).round() as u64;
    let unchanged = cell.tables_per_source - changed;
    let mut generated = 0u64;
    for i in 0..cell.sources {
        for (suffix, tables, versions) in [
            ("", unchanged, 1),
            ("-changed", changed, cell.versions_changed),
        ] {
            if tables == 0 {
                continue;
            }
            let spec = scale_harness::fixture::FixtureSpec {
                seed: 1,
                tables,
                versions,
                sources: 1,
                payload: scale_harness::fixture::PayloadConfig { columns: 12 },
                legacy_tables: 0,
                legacy_versions: vec![],
            };
            let id = format!("store-src-{i:03}{suffix}");
            let descriptor = storage::adapters::LineageDescriptor::postgres(
                2_000_000 + u64::from(i) * 2 + u64::from(!suffix.is_empty()),
                16_384,
            )?;
            scale_harness::fixture::populate_source(
                &backend, i, &id, descriptor, &spec, true,
            )
            .await
            .with_context(|| format!("populate {id}"))?;
            generated += tables * u64::from(versions);
        }
    }
    let generation_secs = started.elapsed().as_secs_f64();
    client.batch_execute("ANALYZE").await?;

    // Sizes.
    let database_bytes: i64 = client
        .query_one("SELECT pg_database_size(current_database())", &[])
        .await?
        .get(0);
    let mut relations = BTreeMap::new();
    for r in client
        .query(
            "SELECT relname::text, pg_relation_size(oid), pg_indexes_size(oid), pg_total_relation_size(oid) \
             FROM pg_class WHERE relname LIKE 'df\\_%' AND relkind = 'r'",
            &[],
        )
        .await?
    {
        relations.insert(r.get::<_, String>(0), (r.get::<_, i64>(1) as u64, r.get::<_, i64>(2) as u64, r.get::<_, i64>(3) as u64));
    }

    // Keys sampled from the populated tables.
    let mut rng = rand::rngs::StdRng::seed_from_u64(7);
    let mut keys: BTreeMap<&str, Vec<(String, String)>> = BTreeMap::new();
    for table in ["df_kv", "df_log", "df_log_meta"] {
        let rows = client
            .query(
                &format!(
                    "SELECT DISTINCT ns, key FROM {table} LIMIT {}",
                    samples * 20
                ),
                &[],
            )
            .await?;
        let mut v: Vec<(String, String)> =
            rows.into_iter().map(|r| (r.get(0), r.get(1))).collect();
        v.shuffle(&mut rng);
        v.truncate(samples);
        keys.insert(table, v);
    }

    let mut queries = Vec::new();
    for q in HOT_QUERIES {
        let sample = keys.get(q.table).cloned().unwrap_or_default();
        if sample.is_empty() {
            continue;
        }
        let stmt = client.prepare(q.sql).await?;
        let (zero, page): (i64, i64) = (0, 256);
        let mut cold = Histogram::default();
        let mut warm = Histogram::default();
        for round in 0..2 {
            for (ns, key) in &sample {
                let params: Vec<&(dyn ToSql + Sync)> = match q.params {
                    Params::NsKey => vec![ns, key],
                    Params::NsKeySeq => vec![ns, key, &zero],
                    Params::NsKeySeqLimit => vec![ns, key, &zero, &page],
                };
                let t = Instant::now();
                client.query(&stmt, &params).await?;
                let us = t.elapsed().as_micros() as u64;
                if round == 0 {
                    cold.record(us)
                } else {
                    warm.record(us)
                }
            }
        }
        let (ns, key) = &sample[0];
        let explain_sql =
            format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {}", q.sql);
        let params: Vec<&(dyn ToSql + Sync)> = match q.params {
            Params::NsKey => vec![ns, key],
            Params::NsKeySeq => vec![ns, key, &zero],
            Params::NsKeySeqLimit => vec![ns, key, &zero, &page],
        };
        let explain: Value = client
            .query_one(&explain_sql, &params)
            .await
            .map(|r| r.get::<_, Value>(0))
            .unwrap_or(Value::Null);
        queries.push(QueryResult {
            name: q.name.into(),
            sql: q.sql.into(),
            cold_us: cold.summary(),
            warm_us: warm.summary(),
            explain,
        });
    }

    let vacuum: Value = client
        .query_one(
            "SELECT COALESCE(json_agg(json_build_object('relation', relname, 'live', n_live_tup, \
             'dead', n_dead_tup, 'autovacuum_count', autovacuum_count, 'autoanalyze_count', \
             autoanalyze_count, 'last_autovacuum', last_autovacuum, 'last_autoanalyze', \
             last_autoanalyze)), '[]'::json) FROM pg_stat_user_tables WHERE relname LIKE 'df\\_%'",
            &[],
        )
        .await?
        .get(0);

    let timed = |name: &'static str| {
        let hooks = hooks.clone();
        async move {
            if hooks.contains_key(name) {
                crate::actions::hook(&hooks, name, &[])
                    .await
                    .map(|d| Some(d.as_secs_f64()))
            } else {
                Ok(None)
            }
        }
    };
    let backup_secs = timed("store_backup").await?;
    let restore_secs = timed("store_restore").await?;

    let result = StoreResult {
        class,
        evidence: "production_path",
        registrations_per_sec: generated as f64
            / generation_secs.max(f64::MIN_POSITIVE),
        cell,
        generated_versions: generated,
        generation_secs,
        database_bytes: database_bytes as u64,
        relations,
        queries,
        vacuum,
        backup_secs,
        restore_secs,
        not_covered: NOT_COVERED.to_vec(),
        harness_revision: crate::git_revision().to_string(),
    };
    let dir = Path::new(output_dir).join(class.dir()).join(format!(
        "store-{}-s{}-t{}-v{}-f{}",
        crate::evidence::unique_stem(Utc::now()),
        result.cell.sources,
        result.cell.tables_per_source,
        result.cell.versions_changed,
        result.cell.changed_fraction
    ));
    crate::evidence::create_new_dir(&dir)?;
    crate::evidence::write_new(
        &dir.join("result.json"),
        &serde_json::to_vec_pretty(&result)?,
    )?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn normalize(s: &str) -> String {
        s.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    /// The curated queries are the backend's own: a change to the store's
    /// SQL fails here until the list is updated.
    #[test]
    fn hot_queries_are_verbatim_from_the_postgres_backend() {
        let source = normalize(include_str!("../../storage/src/postgres.rs"));
        for q in HOT_QUERIES {
            assert!(
                source.contains(&normalize(q.sql)),
                "{} is not in storage/src/postgres.rs",
                q.name
            );
        }
    }
}

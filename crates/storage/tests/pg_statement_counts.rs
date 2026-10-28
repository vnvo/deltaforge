//! Statement counts of a window's registry planning and atomic commit on
//! PostgreSQL, measured with `pg_stat_statements` on a server of its own
//! (`docs/design/mysql-mass-invalidation.md`, "Exact statement bound per
//! atomic plan"): bounded by the window limits, never by its table count.

use std::sync::Arc;
use std::time::{Duration, Instant};

use deadpool_postgres::tokio_postgres::{self, NoTls};
use gate_ownership::GateOwned;
use serde_json::json;
use storage::adapters::schema_registry::{
    RegistrationPlanning, RegistrationRequest,
};
use storage::adapters::{LineageDescriptor, SchemaKey};
use storage::{
    ArcStorageBackend, AtomicBatch, BatchOp, CommitResult,
    DurableSchemaRegistry, PostgresStorageBackend, Precondition, commit_atomic,
};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner,
};

/// The exact bound of one atomic plan's statements: reads, preconditions
/// and writes, whatever the table count.
const PLAN_STATEMENT_BOUND: i64 = 54;

/// The activation stream a window appends to for each admitted table.
const ACTIVATION_NS: &str = "schemas.v1.activation";

async fn start() -> (ContainerAsync<GenericImage>, String) {
    let c = GenericImage::new("postgres", "17")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "pg")
        .with_cmd([
            "postgres",
            "-c",
            "shared_preload_libraries=pg_stat_statements",
            "-c",
            "pg_stat_statements.track_utility=on",
        ])
        .gate_owned()
        .start()
        .await
        .expect("start postgres");
    let port = c.get_host_port_ipv4(5432).await.unwrap();
    let dsn = format!(
        "host=127.0.0.1 port={port} user=postgres password=pg dbname=postgres"
    );
    // The image restarts its server once after initialization.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok((client, conn)) = tokio_postgres::connect(&dsn, NoTls).await {
            tokio::spawn(conn);
            if client
                .batch_execute(
                    "CREATE EXTENSION IF NOT EXISTS pg_stat_statements",
                )
                .await
                .is_ok()
            {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "postgres never accepted connections"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    (c, dsn)
}

/// Statements executed since the last reset, apart from this measurement's
/// own and the backend's periodic sweeps.
async fn statements(m: &tokio_postgres::Client) -> i64 {
    m.query_one(
        "SELECT COALESCE(SUM(calls), 0)::bigint FROM pg_stat_statements
         WHERE query NOT ILIKE '%pg_stat_statements%'
           AND query NOT ILIKE 'DELETE FROM df_kv WHERE expires_at%'
           AND query NOT ILIKE '%DELETE FROM df_batch_commit%'",
        &[],
    )
    .await
    .unwrap()
    .get(0)
}

/// A narrow table's captured shape (about 1.5 KB encoded, as measured).
fn shape(table: &str) -> serde_json::Value {
    json!({
        "table": table,
        "columns": (0..4).map(|i| json!({
            "name": format!("c{i}"),
            "type": "varchar(255)",
            "nullable": i > 0,
            "default": null,
            "comment": "x".repeat(300),
        })).collect::<Vec<_>>(),
        "primary_key": ["c0"],
    })
}

/// One window of `n` new tables: its registry planning and its atomic
/// commit (registrations and activation records), as `admit_batch` builds
/// it. Returns the statements it executed.
async fn window(
    be: &ArcStorageBackend,
    m: &tokio_postgres::Client,
    lineage: &str,
    n: usize,
) -> i64 {
    let registry = DurableSchemaRegistry::new(be.clone()).await.unwrap();
    let keys: Vec<SchemaKey> = (0..n)
        .map(|i| {
            SchemaKey::new(
                "acme",
                "src",
                lineage,
                "db",
                format!("w{n}_t{i:05}"),
            )
        })
        .collect();
    let reqs: Vec<RegistrationRequest> = keys
        .iter()
        .map(|k| RegistrationRequest {
            key: k.clone(),
            hash: format!("h-{}", k.table),
            schema_json: shape(&k.table),
            checkpoint: Some(b"cp".to_vec()),
        })
        .collect();
    m.execute("SELECT pg_stat_statements_reset()", &[])
        .await
        .unwrap();
    let RegistrationPlanning::Planned {
        preconditions,
        registrations,
    } = registry
        .plan_registrations(&reqs, chrono::Utc::now())
        .await
        .unwrap()
    else {
        panic!("needs repair");
    };
    let mut batch = AtomicBatch {
        preconditions,
        ops: Vec::new(),
    };
    for (k, r) in keys.iter().zip(registrations) {
        batch.push_group(r.ops);
        batch.preconditions.push(Precondition::LogHead {
            ns: ACTIVATION_NS.into(),
            key: k.backend_key(),
            seq: 0,
        });
        batch.ops.push(BatchOp::LogAppendIfAbsent {
            ns: ACTIVATION_NS.into(),
            key: k.backend_key(),
            capture_id: format!("act/{}", k.table),
            value: br#"{"kind":"baseline"}"#.to_vec(),
        });
    }
    match commit_atomic(be.as_ref(), &batch, 3).await.unwrap() {
        CommitResult::Committed(seqs) => {
            assert_eq!(seqs.iter().filter(|s| s.is_some()).count(), 2 * n);
        }
        CommitResult::Conflict(why) => panic!("conflict: {why}"),
    }
    statements(m).await
}

/// A window's planning and commit execute at most the plan bound, for 1, 64
/// and 2,048 new tables alike (today's per-item execution costs about 30
/// statements per table).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker"]
async fn a_windows_statements_are_bounded_whatever_its_table_count() {
    let (container, dsn) = start().await;
    let be: ArcStorageBackend =
        PostgresStorageBackend::connect(&dsn).await.unwrap();
    let (m, conn) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
    tokio::spawn(conn);
    // The backend's sweeps run once as it starts.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let lineage =
        LineageDescriptor::mysql("3E11FA47-71CA-11E1-9E33-C80AA9429562")
            .unwrap()
            .lineage_hash();
    let mut counts = Vec::new();
    for n in [1, 64, 2048] {
        counts.push((n, window(&be, &m, &lineage, n).await));
    }
    eprintln!("statements per window: {counts:?}");
    for (n, count) in &counts {
        assert!(
            *count <= PLAN_STATEMENT_BOUND,
            "{n} tables: {count} statements (bound {PLAN_STATEMENT_BOUND}); all: {counts:?}"
        );
    }
    drop(Arc::clone(&be));
    container.rm().await.ok();
}

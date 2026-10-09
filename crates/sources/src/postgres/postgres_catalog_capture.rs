//! Catalog reads bound to the stream (design `docs/design/postgres-catalog-capture.md`,
//! sections 6-8 of revision 4 and the approved capture protocol).
//!
//! Every catalog read is accepted only after a proof, in the read's own
//! snapshot, that the catalog session reaches the node and live walsender of
//! the authoritative stream (slot held by that walsender: pid, backend start,
//! the session's exact nonce in `application_name`, its database) at or past
//! the commit position of the transaction whose Relation triggered the read.
//! Nothing read before the proof holds is used.
//!
//! Two reads exist:
//! - the **stamp**: one statement (one snapshot) returning the proof and the
//!   canonical catalog inputs (the annotation attributes, which include the
//!   generated-column state). Its digest decides whether anything changed;
//! - the **capture**: the same statement as the first query of a REPEATABLE
//!   READ READ ONLY transaction that first takes the table's ACCESS SHARE
//!   lock (so the snapshot follows any in-flight table DDL, and none commits
//!   during the read). It yields the capture-time annotation.
//!
//! Annotations describe the catalog at `captured_lsn`, never the rows of any
//! event schema version, and are never referenced by events.

use std::time::{Duration, Instant};

use pgwire_replication::Lsn;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use storage::adapters::SchemaKey;
use storage::{ArcStorageBackend, LogTruncateRequest};
use tokio_postgres::NoTls;
use tracing::{debug, warn};

use super::postgres_helpers::StreamToken;
use deltaforge_core::{SourceError, SourceResult};

/// Log namespace of the capture-time annotations (one stream per table key).
pub(crate) const ANNOTATION_NS: &str = "schemas.v1.pg.catalog_annotation";
/// Annotation records kept per table.
pub(crate) const ANNOTATIONS_KEPT: u64 = 8;
/// How long a proof may wait for routing to settle (failover, lag).
#[cfg(not(test))]
const PROOF_WAIT: Duration = Duration::from_secs(30);
#[cfg(test)]
const PROOF_WAIT: Duration = Duration::from_secs(3);
/// The capture's lock and statement timeouts.
#[cfg(not(test))]
const LOCK_TIMEOUT: &str = "30s";
#[cfg(test)]
const LOCK_TIMEOUT: &str = "3s";
const STATEMENT_TIMEOUT: &str = "60s";
const PROOF_RETRY: Duration = Duration::from_millis(200);
/// Attempts of a locked capture before it fails closed.
const CAPTURE_ATTEMPTS: u32 = 3;

/// One column's catalog attributes at capture time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AnnotationColumn {
    pub name: String,
    pub not_null: bool,
    pub default: Option<String>,
    /// `pg_attribute.attidentity` (`a`, `d`) when an identity column.
    pub identity: Option<String>,
    /// `pg_attribute.attgenerated` (`s`) when a stored generated column.
    pub generated: Option<String>,
    /// `format_type(atttypid, atttypmod)`.
    pub type_text: String,
    pub type_schema: String,
    pub type_name: String,
    /// For a domain-typed column: the domain's NOT NULL.
    pub domain_not_null: Option<bool>,
}

/// A table's catalog attributes at capture time: the canonical inputs of
/// the stamp and the content of an annotation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CatalogAttributes {
    pub columns: Vec<AnnotationColumn>,
    pub primary_key: Vec<String>,
    /// `pg_class.relreplident`.
    pub replica_identity: String,
    pub replica_identity_index: Option<String>,
}

impl CatalogAttributes {
    /// SHA-256 of the canonical bytes (field order is the struct's).
    pub(crate) fn digest(&self) -> String {
        let bytes = serde_json::to_vec(self).unwrap_or_default();
        hex::encode(Sha256::digest(&bytes))
    }

    /// Stored generated columns (unsupported: design D4).
    pub(crate) fn generated_columns(&self) -> Vec<&str> {
        self.columns
            .iter()
            .filter(|c| c.generated.as_deref().is_some_and(|g| !g.is_empty()))
            .map(|c| c.name.as_str())
            .collect()
    }
}

/// A durable annotation record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AnnotationRecord {
    pub binding_digest: String,
    pub captured_lsn: String,
    pub captured_at: String,
    pub provenance: String,
    pub content_digest: String,
    pub attributes: CatalogAttributes,
}

/// What the proof statement found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProofFacts {
    pub in_recovery: bool,
    pub system_identifier: u64,
    pub database_oid: u64,
    pub active_pid: Option<i32>,
    pub backend_start: Option<String>,
    pub application_name: Option<String>,
    pub datid: Option<u64>,
    pub current_lsn: Lsn,
}

/// Whether `facts` prove the catalog session reaches `token`'s node and
/// walsender at or past `target`; otherwise the failing condition.
pub(crate) fn check_proof(
    facts: &ProofFacts,
    token: &StreamToken,
    target: Lsn,
) -> Result<(), &'static str> {
    if facts.in_recovery {
        return Err("server_in_recovery");
    }
    if facts.system_identifier != token.system_identifier {
        return Err("other_system");
    }
    if facts.database_oid != token.database_oid {
        return Err("other_database");
    }
    let w = &token.walsender;
    if facts.active_pid != Some(w.pid) {
        return Err("slot_not_held_by_stream");
    }
    if facts.backend_start.as_deref() != Some(w.backend_start.as_str()) {
        return Err("walsender_start_mismatch");
    }
    if facts.application_name.as_deref() != Some(w.application_name.as_str()) {
        return Err("walsender_nonce_mismatch");
    }
    if facts.datid != Some(token.database_oid) {
        return Err("walsender_database_mismatch");
    }
    if facts.current_lsn < target {
        return Err("behind_target");
    }
    Ok(())
}

/// The proof and the canonical inputs in ONE statement (one snapshot).
/// `$1` slot, `$2` schema, `$3` table.
const PROOF_SQL: &str = r#"
WITH rel AS (
    SELECT c.oid, c.relreplident::text AS relreplident
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = $2 AND c.relname = $3
), cols AS (
    SELECT coalesce(pg_catalog.json_agg(pg_catalog.json_build_object(
        'name', a.attname,
        'not_null', a.attnotnull,
        'default', pg_catalog.pg_get_expr(d.adbin, d.adrelid),
        'identity', NULLIF(a.attidentity::text, ''),
        'generated', NULLIF(a.attgenerated::text, ''),
        'type_text', pg_catalog.format_type(a.atttypid, a.atttypmod),
        'type_schema', tn.nspname,
        'type_name', t.typname,
        'domain_not_null', CASE WHEN t.typtype = 'd' THEN t.typnotnull END
    ) ORDER BY a.attnum), '[]'::json)::text AS columns
    FROM rel
    JOIN pg_catalog.pg_attribute a
        ON a.attrelid = rel.oid AND a.attnum > 0 AND NOT a.attisdropped
    JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
    JOIN pg_catalog.pg_namespace tn ON tn.oid = t.typnamespace
    LEFT JOIN pg_catalog.pg_attrdef d
        ON d.adrelid = a.attrelid AND d.adnum = a.attnum
), pk AS (
    SELECT coalesce(pg_catalog.json_agg(a.attname
        ORDER BY pg_catalog.array_position(i.indkey::int2[], a.attnum)),
        '[]'::json)::text AS pk
    FROM rel
    JOIN pg_catalog.pg_index i ON i.indrelid = rel.oid AND i.indisprimary
    JOIN pg_catalog.pg_attribute a
        ON a.attrelid = rel.oid AND a.attnum = ANY (i.indkey)
)
SELECT
    pg_catalog.pg_is_in_recovery(),
    (SELECT system_identifier::text FROM pg_catalog.pg_control_system()),
    (SELECT oid::int8 FROM pg_catalog.pg_database
     WHERE datname = pg_catalog.current_database()),
    s.active_pid,
    a.backend_start::text,
    a.application_name,
    a.datid::int8,
    CASE WHEN pg_catalog.pg_is_in_recovery() THEN NULL
         ELSE pg_catalog.pg_current_wal_lsn()::text END,
    (SELECT count(*) FROM rel),
    (SELECT columns FROM cols),
    (SELECT pk FROM pk),
    (SELECT relreplident FROM rel),
    (SELECT ic.relname::text FROM rel
     JOIN pg_catalog.pg_index i ON i.indrelid = rel.oid AND i.indisreplident
     JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid)
FROM (SELECT 1) one
LEFT JOIN pg_catalog.pg_replication_slots s ON s.slot_name = $1
LEFT JOIN pg_catalog.pg_stat_activity a ON a.pid = s.active_pid
"#;

/// The catalog session of a stream: one connection, reconnected after an
/// error. Every read on it carries its own proof.
pub(crate) struct CatalogSession {
    dsn: crate::credentials::ProtectedDsn,
    client: Option<tokio_postgres::Client>,
}

impl CatalogSession {
    pub(crate) fn new(dsn: crate::credentials::ProtectedDsn) -> Self {
        Self { dsn, client: None }
    }

    async fn client(&mut self) -> SourceResult<&tokio_postgres::Client> {
        if self.client.as_ref().is_none_or(|c| c.is_closed()) {
            let (client, conn) =
                tokio_postgres::connect(self.dsn.expose(), NoTls)
                    .await
                    .map_err(|e| SourceError::Connect {
                        details: format!("catalog session: {e}").into(),
                    })?;
            tokio::spawn(async move {
                let _ = conn.await;
            });
            self.client = Some(client);
        }
        Ok(self.client.as_ref().expect("connected"))
    }

    fn reset(&mut self) {
        self.client = None;
    }

    /// Use `dsn` from the next connection on (credential rotation); the
    /// current connection is dropped.
    pub(crate) fn set_dsn(&mut self, dsn: crate::credentials::ProtectedDsn) {
        self.dsn = dsn;
        self.client = None;
    }
}

/// What one proven read returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Read {
    Found {
        attributes: CatalogAttributes,
        captured_lsn: Lsn,
    },
    /// The table does not exist in the proven snapshot.
    Missing,
}

/// A server's answer (lock timeout, missing privilege) is an error of the
/// read; anything else (refused, reset, closed) is a routing condition that
/// the bounded wait retries.
fn query_error(what: &str, e: tokio_postgres::Error) -> SourceError {
    if e.as_db_error().is_some() {
        SourceError::Other(anyhow::anyhow!("catalog {what}: {e}"))
    } else {
        SourceError::Connect {
            details: format!("catalog {what}: {e}").into(),
        }
    }
}

/// Run [`PROOF_SQL`] once; `Ok(Err(class))` when the proof does not hold.
async fn proven_read(
    client: &tokio_postgres::Client,
    token: &StreamToken,
    target: Lsn,
    schema: &str,
    table: &str,
) -> SourceResult<Result<Read, &'static str>> {
    let row = client
        .query_one(PROOF_SQL, &[&token.slot, &schema, &table])
        .await
        .map_err(|e| query_error("proof", e))?;
    let facts = ProofFacts {
        in_recovery: row.get(0),
        system_identifier: row
            .get::<_, Option<String>>(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or_default(),
        database_oid: row.get::<_, Option<i64>>(2).unwrap_or_default() as u64,
        active_pid: row.get(3),
        backend_start: row.get(4),
        application_name: row.get(5),
        datid: row.get::<_, Option<i64>>(6).map(|d| d as u64),
        current_lsn: row
            .get::<_, Option<String>>(7)
            .and_then(|l| Lsn::parse(&l).ok())
            .unwrap_or(Lsn(0)),
    };
    if let Err(class) = check_proof(&facts, token, target) {
        return Ok(Err(class));
    }
    if row.get::<_, i64>(8) == 0 {
        return Ok(Ok(Read::Missing));
    }
    let columns: Vec<AnnotationColumn> = serde_json::from_str(
        &row.get::<_, Option<String>>(9).unwrap_or_default(),
    )
    .map_err(|e| SourceError::Other(e.into()))?;
    let primary_key: Vec<String> = serde_json::from_str(
        &row.get::<_, Option<String>>(10).unwrap_or_default(),
    )
    .map_err(|e| SourceError::Other(e.into()))?;
    Ok(Ok(Read::Found {
        attributes: CatalogAttributes {
            columns,
            primary_key,
            replica_identity: row
                .get::<_, Option<String>>(11)
                .unwrap_or_default(),
            replica_identity_index: row.get(12),
        },
        captured_lsn: facts.current_lsn,
    }))
}

/// The stamp: one proven statement. A failed proof or an unreachable
/// catalog session is retried every [`PROOF_RETRY`] for at most
/// [`PROOF_WAIT`] (routing may be settling after a failover); then it fails
/// closed with `pg_catalog_visibility_unproven`.
pub(crate) async fn stamp(
    session: &mut CatalogSession,
    source_id: &str,
    token: &StreamToken,
    target: Lsn,
    schema: &str,
    table: &str,
) -> SourceResult<Read> {
    crate::catalog_probe::record_stamp();
    let deadline = Instant::now() + PROOF_WAIT;
    loop {
        let class = match session.client().await {
            Ok(client) => {
                match proven_read(client, token, target, schema, table).await {
                    Ok(Ok(read)) => return Ok(read),
                    // Reconnect: the endpoint may route elsewhere by now.
                    Ok(Err(class)) => {
                        session.reset();
                        class
                    }
                    Err(SourceError::Connect { .. }) => {
                        session.reset();
                        "catalog_unreachable"
                    }
                    Err(e) => return Err(e),
                }
            }
            Err(_) => "catalog_unreachable",
        };
        if Instant::now() >= deadline {
            return Err(crate::incident_drafts::catalog_visibility_unproven(
                source_id,
                &token.slot,
                &target.to_string(),
                class,
            ));
        }
        debug!(source_id, class, "catalog proof not yet holding; retrying");
        tokio::time::sleep(PROOF_RETRY).await;
    }
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// The locked coherent capture: REPEATABLE READ READ ONLY, bounded lock
/// and statement timeouts, the table's ACCESS SHARE lock before the
/// snapshot, then the proven read as the transaction's first query.
pub(crate) async fn capture(
    session: &mut CatalogSession,
    source_id: &str,
    token: &StreamToken,
    target: Lsn,
    schema: &str,
    table: &str,
) -> SourceResult<Read> {
    crate::catalog_probe::record_capture();
    let mut last = None;
    let deadline = Instant::now() + PROOF_WAIT;
    let mut attempt = 0;
    while attempt < CAPTURE_ATTEMPTS {
        // A failed proof or an unreachable session is a routing condition:
        // reconnect through the endpoint and retry within the bound, without
        // spending an attempt; then fail closed with its class.
        let class = match capture_once(session, token, target, schema, table)
            .await
        {
            Ok(Ok(r)) => return Ok(r),
            Ok(Err(class)) => class,
            Err(SourceError::Connect { .. }) => "catalog_unreachable",
            Err(e) => {
                attempt += 1;
                warn!(source_id, %schema, %table, attempt, error = %e, "catalog capture failed");
                session.reset();
                last = Some(e);
                continue;
            }
        };
        session.reset();
        if Instant::now() >= deadline {
            return Err(crate::incident_drafts::catalog_visibility_unproven(
                source_id,
                &token.slot,
                &target.to_string(),
                class,
            ));
        }
        tokio::time::sleep(PROOF_RETRY).await;
    }
    Err(last.expect("at least one attempt"))
}

async fn capture_once(
    session: &mut CatalogSession,
    token: &StreamToken,
    target: Lsn,
    schema: &str,
    table: &str,
) -> SourceResult<Result<Read, &'static str>> {
    let client = session.client().await?;
    // Dropping this future (cancellation) while the server still waits for
    // the lock or runs the read sends a cancel request, so the backend
    // releases everything at once instead of at its timeout.
    let mut cancel = CancelOnDrop(Some(client.cancel_token()));
    let begin = format!(
        "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; \
         SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'; \
         SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'; \
         LOCK TABLE ONLY {}.{} IN ACCESS SHARE MODE",
        quote_ident(schema),
        quote_ident(table)
    );
    let r = capture_steps(client, &begin, token, target, schema, table).await;
    if r.is_ok() {
        cancel.0 = None;
    }
    r
}

/// Sends a cancel request when dropped armed.
struct CancelOnDrop(Option<tokio_postgres::CancelToken>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(t) = self.0.take()
            && let Ok(handle) = tokio::runtime::Handle::try_current()
        {
            handle.spawn(async move {
                let _ = t.cancel_query(NoTls).await;
            });
        }
    }
}

async fn capture_steps(
    client: &tokio_postgres::Client,
    begin: &str,
    token: &StreamToken,
    target: Lsn,
    schema: &str,
    table: &str,
) -> SourceResult<Result<Read, &'static str>> {
    if let Err(e) = client.batch_execute(begin).await {
        let missing =
            e.code() == Some(&tokio_postgres::error::SqlState::UNDEFINED_TABLE);
        let _ = client.batch_execute("ROLLBACK").await;
        if missing {
            return Ok(Ok(Read::Missing));
        }
        return Err(query_error("lock", e));
    }
    #[cfg(test)]
    capture_hook::after(table, 0).await;
    // The first query takes the snapshot, after the lock.
    match proven_read(client, token, target, schema, table).await {
        Ok(Ok(read)) => {
            #[cfg(test)]
            capture_hook::after(table, 1).await;
            client
                .batch_execute("COMMIT")
                .await
                .map_err(|e| query_error("commit", e))?;
            Ok(Ok(read))
        }
        Ok(Err(class)) => {
            let _ = client.batch_execute("ROLLBACK").await;
            Ok(Err(class))
        }
        Err(e) => {
            let _ = client.batch_execute("ROLLBACK").await;
            Err(e)
        }
    }
}

/// Append an annotation unless the table's latest one has the same binding
/// and content; keep the latest [`ANNOTATIONS_KEPT`]. Returns whether one was
/// appended.
pub(crate) async fn record_annotation(
    backend: &ArcStorageBackend,
    key: &SchemaKey,
    binding_digest: &str,
    attributes: &CatalogAttributes,
    captured_lsn: Lsn,
) -> anyhow::Result<bool> {
    let stream = key.backend_key();
    let content_digest = attributes.digest();
    if let Some((_, bytes)) = backend.log_latest(ANNOTATION_NS, &stream).await?
    {
        let latest: AnnotationRecord = serde_json::from_slice(&bytes)?;
        if latest.binding_digest == binding_digest
            && latest.content_digest == content_digest
        {
            return Ok(false);
        }
    }
    let record = AnnotationRecord {
        binding_digest: binding_digest.to_string(),
        captured_lsn: captured_lsn.to_string(),
        captured_at: chrono::Utc::now().to_rfc3339(),
        provenance: "catalog_at_capture".into(),
        content_digest,
        attributes: attributes.clone(),
    };
    backend
        .log_append(ANNOTATION_NS, &stream, &serde_json::to_vec(&record)?)
        .await?;
    // Retention: a failure leaves extra records, retried at the next append.
    if let Err(e) = backend
        .log_truncate(
            ANNOTATION_NS,
            &stream,
            LogTruncateRequest {
                older_than_ms: None,
                pin_seq: u64::MAX,
                max_entries: Some(ANNOTATIONS_KEPT),
                max_bytes: None,
            },
        )
        .await
    {
        warn!(error = %e, "annotation retention failed; retried at the next append");
    }
    Ok(true)
}

/// Test hook between the capture's statements (after the lock: 0; after the
/// proven read, before COMMIT: 1), keyed by table.
#[cfg(test)]
pub(crate) mod capture_hook {
    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex, OnceLock};

    pub(crate) type Hook = Arc<
        dyn Fn(u8) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync,
    >;

    fn hooks() -> &'static Mutex<HashMap<String, Hook>> {
        static HOOKS: OnceLock<Mutex<HashMap<String, Hook>>> = OnceLock::new();
        HOOKS.get_or_init(Default::default)
    }

    pub(crate) fn set(table: &str, hook: Hook) {
        hooks().lock().unwrap().insert(table.to_string(), hook);
    }

    pub(crate) fn clear(table: &str) {
        hooks().lock().unwrap().remove(table);
    }

    pub(crate) async fn after(table: &str, step: u8) {
        let hook = hooks().lock().unwrap().get(table).cloned();
        if let Some(h) = hook {
            h(step).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::postgres_continuity::Walsender;

    fn token() -> StreamToken {
        StreamToken {
            slot: "s".into(),
            walsender: Walsender {
                pid: 42,
                backend_start: "2026-10-08 10:00:00.123456+00".into(),
                application_name: "deltaforge:abc".into(),
            },
            system_identifier: 7,
            database_oid: 5,
        }
    }

    fn facts() -> ProofFacts {
        ProofFacts {
            in_recovery: false,
            system_identifier: 7,
            database_oid: 5,
            active_pid: Some(42),
            backend_start: Some("2026-10-08 10:00:00.123456+00".into()),
            application_name: Some("deltaforge:abc".into()),
            datid: Some(5),
            current_lsn: Lsn(100),
        }
    }

    #[test]
    fn the_proof_requires_every_binding_fact() {
        assert_eq!(check_proof(&facts(), &token(), Lsn(100)), Ok(()));
        type Change = Box<dyn Fn(&mut ProofFacts)>;
        let cases: Vec<(Change, &str)> = vec![
            (Box::new(|f| f.in_recovery = true), "server_in_recovery"),
            (Box::new(|f| f.system_identifier = 8), "other_system"),
            (Box::new(|f| f.database_oid = 6), "other_database"),
            (Box::new(|f| f.active_pid = None), "slot_not_held_by_stream"),
            (
                Box::new(|f| f.active_pid = Some(43)),
                "slot_not_held_by_stream",
            ),
            (
                Box::new(|f| f.backend_start = Some("other".into())),
                "walsender_start_mismatch",
            ),
            (
                Box::new(|f| {
                    f.application_name = Some("deltaforge:other".into())
                }),
                "walsender_nonce_mismatch",
            ),
            (
                Box::new(|f| f.datid = Some(6)),
                "walsender_database_mismatch",
            ),
            (Box::new(|f| f.current_lsn = Lsn(99)), "behind_target"),
        ];
        for (change, class) in cases {
            let mut f = facts();
            change(&mut f);
            assert_eq!(check_proof(&f, &token(), Lsn(100)), Err(class));
        }
    }

    #[tokio::test]
    async fn annotations_are_deduplicated_and_retained_to_eight() {
        let backend: ArcStorageBackend =
            std::sync::Arc::new(storage::MemoryStorageBackend::new());
        let key = SchemaKey::new("t", "s", "lineage", "public", "orders");
        let attrs = |n: usize| CatalogAttributes {
            columns: vec![],
            primary_key: vec![format!("k{n}")],
            replica_identity: "d".into(),
            replica_identity_index: None,
        };
        assert!(
            record_annotation(&backend, &key, "b", &attrs(0), Lsn(1))
                .await
                .unwrap()
        );
        assert!(
            !record_annotation(&backend, &key, "b", &attrs(0), Lsn(2))
                .await
                .unwrap()
        );
        for n in 1..12 {
            record_annotation(
                &backend,
                &key,
                "b",
                &attrs(n),
                Lsn(10 + n as u64),
            )
            .await
            .unwrap();
        }
        let kept = backend
            .log_list(ANNOTATION_NS, &key.backend_key())
            .await
            .unwrap();
        assert_eq!(kept.len() as u64, ANNOTATIONS_KEPT);
        let last: AnnotationRecord =
            serde_json::from_slice(&kept.last().unwrap().1).unwrap();
        assert_eq!(last.attributes.primary_key, ["k11"]);
    }

    #[test]
    fn annotation_digest_tracks_content_and_generated_columns() {
        let col = |g: Option<&str>| AnnotationColumn {
            name: "c".into(),
            not_null: false,
            default: None,
            identity: None,
            generated: g.map(Into::into),
            type_text: "integer".into(),
            type_schema: "pg_catalog".into(),
            type_name: "int4".into(),
            domain_not_null: None,
        };
        let a = CatalogAttributes {
            columns: vec![col(None)],
            primary_key: vec![],
            replica_identity: "d".into(),
            replica_identity_index: None,
        };
        let mut b = a.clone();
        b.columns[0].generated = Some("s".into());
        assert_ne!(a.digest(), b.digest());
        assert!(a.generated_columns().is_empty());
        assert_eq!(b.generated_columns(), ["c"]);
    }
}

/// Live PostgreSQL 17 (Docker): the approved capture protocol against a real
/// walsender holding the slot with its nonce.
#[cfg(test)]
mod live {
    use super::*;
    use crate::postgres::postgres_continuity::Walsender;
    use gate_ownership::GateOwned;
    use pgwire_replication::{ReplicationClient, ReplicationConfig, TlsConfig};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use testcontainers::core::WaitFor;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{ContainerAsync, GenericImage, ImageExt};
    use tokio::sync::OnceCell;

    static PG: OnceCell<(ContainerAsync<GenericImage>, u16)> =
        OnceCell::const_new();

    async fn port() -> u16 {
        PG.get_or_init(|| async {
            let c = GenericImage::new("postgres", "17")
                .with_wait_for(WaitFor::message_on_stderr(
                    "database system is ready",
                ))
                .with_env_var("POSTGRES_PASSWORD", "pw")
                .with_cmd(vec!["postgres", "-c", "wal_level=logical"])
                .gate_owned()
                .start()
                .await
                .expect("start postgres");
            let port = c.get_host_port_ipv4(5432).await.unwrap();
            for _ in 0..60 {
                if tokio_postgres::connect(&dsn(port, "postgres"), NoTls)
                    .await
                    .is_ok()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
            (c, port)
        })
        .await
        .1
    }

    fn dsn(port: u16, db: &str) -> String {
        format!(
            "host=127.0.0.1 port={port} user=postgres password=pw dbname={db}"
        )
    }

    async fn connect(port: u16, db: &str) -> tokio_postgres::Client {
        let (c, conn) = tokio_postgres::connect(&dsn(port, db), NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        c
    }

    struct Fx {
        port: u16,
        db: String,
        admin: tokio_postgres::Client,
        token: StreamToken,
        _repl: ReplicationClient,
        session: CatalogSession,
    }

    impl Fx {
        /// A database with table `t`, a publication, a slot, and a live
        /// walsender holding the slot under a fresh nonce.
        async fn new(db: &str) -> Self {
            let port = port().await;
            let root = connect(port, "postgres").await;
            root.batch_execute(&format!(
                "DROP DATABASE IF EXISTS {db} WITH (FORCE)"
            ))
            .await
            .ok();
            root.batch_execute(&format!("CREATE DATABASE {db}"))
                .await
                .unwrap();
            let admin = connect(port, db).await;
            admin
                .batch_execute(
                    "CREATE TABLE t (id INT PRIMARY KEY, v INT); \
                     CREATE DOMAIN d_int AS INT; \
                     CREATE TABLE u (id INT PRIMARY KEY, w d_int); \
                     CREATE PUBLICATION p FOR ALL TABLES;",
                )
                .await
                .unwrap();
            admin
                .batch_execute(
                    &format!(
                        "SELECT pg_create_logical_replication_slot('s_{db}', 'pgoutput')"
                    ),
                )
                .await
                .unwrap();
            let nonce = format!("deltaforge:{}", uuid::Uuid::new_v4().simple());
            let cfg = ReplicationConfig::new(
                "127.0.0.1",
                "postgres",
                "pw",
                db,
                format!("s_{db}"),
                "p",
            )
            .with_port(port)
            .with_tls(TlsConfig::disabled())
            .with_application_name(nonce.clone());
            let repl = ReplicationClient::connect(cfg).await.unwrap();
            let mut token = None;
            for _ in 0..50 {
                let rows = admin
                    .query(
                        "SELECT a.pid, a.backend_start::text, \
                         (SELECT system_identifier::text FROM pg_control_system()), \
                         a.datid::int8 \
                         FROM pg_replication_slots s \
                         JOIN pg_stat_activity a ON a.pid = s.active_pid \
                         WHERE s.slot_name = $2 AND a.application_name = $1",
                        &[&nonce, &format!("s_{db}")],
                    )
                    .await
                    .unwrap();
                if let Some(r) = rows.first() {
                    token = Some(StreamToken {
                        slot: format!("s_{db}"),
                        walsender: Walsender {
                            pid: r.get(0),
                            backend_start: r.get(1),
                            application_name: nonce.clone(),
                        },
                        system_identifier: r
                            .get::<_, String>(2)
                            .parse()
                            .unwrap(),
                        database_oid: r.get::<_, i64>(3) as u64,
                    });
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Self {
                port,
                db: db.into(),
                admin,
                token: token.expect("walsender holds the slot"),
                _repl: repl,
                session: CatalogSession::new(dsn(port, db).as_str().into()),
            }
        }

        async fn lsn(&self) -> Lsn {
            let l: String = self
                .admin
                .query_one("SELECT pg_current_wal_lsn()::text", &[])
                .await
                .unwrap()
                .get(0);
            Lsn::parse(&l).unwrap()
        }

        async fn capture(&mut self, table: &str) -> SourceResult<Read> {
            let (token, target) = (self.token.clone(), self.lsn().await);
            capture(&mut self.session, "src", &token, target, "public", table)
                .await
        }

        async fn other(&self) -> tokio_postgres::Client {
            connect(self.port, &self.db).await
        }
    }

    type PendingDdl = Arc<
        std::sync::Mutex<
            Option<tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>>,
        >,
    >;

    fn names(r: &Read) -> Vec<String> {
        match r {
            Read::Found { attributes, .. } => {
                attributes.columns.iter().map(|c| c.name.clone()).collect()
            }
            Read::Missing => vec![],
        }
    }

    #[tokio::test]
    #[ignore = "requires docker"]
    async fn a_proven_capture_reads_the_table() {
        let mut fx = Fx::new("cc_basic").await;
        let r = fx.capture("t").await.unwrap();
        assert_eq!(names(&r), ["id", "v"]);
        let Read::Found {
            attributes,
            captured_lsn,
        } = r
        else {
            panic!()
        };
        assert_eq!(attributes.primary_key, ["id"]);
        assert!(captured_lsn >= Lsn(1));
        assert_eq!(fx.capture("missing").await.unwrap(), Read::Missing);
        let (token, target) = (fx.token.clone(), fx.lsn().await);
        let s = stamp(&mut fx.session, "src", &token, target, "public", "t")
            .await
            .unwrap();
        assert_eq!(names(&s), ["id", "v"]);
    }

    /// T1: a DDL issued at each step of the capture (after the lock, after
    /// the proven read) waits for the capture; the capture returns the
    /// coherent pre-DDL shape; the next capture sees the DDL.
    #[tokio::test]
    #[ignore = "requires docker"]
    async fn a_ddl_at_each_capture_step_waits() {
        let mut fx = Fx::new("cc_steps").await;
        for step in [0u8, 1] {
            let other = fx.other().await;
            let observer = fx.other().await;
            let ddl = format!("ALTER TABLE t ADD COLUMN c{step} INT");
            let slot: PendingDdl = Default::default();
            let (slot2, other) = (slot.clone(), Arc::new(other));
            let observer = Arc::new(observer);
            capture_hook::set(
                "t",
                Arc::new(move |s| {
                    let (slot, other, observer, ddl) = (
                        slot2.clone(),
                        other.clone(),
                        observer.clone(),
                        ddl.clone(),
                    );
                    Box::pin(async move {
                        if s != step {
                            return;
                        }
                        let task = tokio::spawn(async move {
                            other.batch_execute(&ddl).await
                        });
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        assert!(!task.is_finished(), "the DDL must wait");
                        let waiting: i64 = observer
                            .query_one(
                                "SELECT count(*) FROM pg_locks l JOIN pg_class c \
                                 ON c.oid = l.relation WHERE c.relname = 't' \
                                 AND l.mode = 'AccessExclusiveLock' AND NOT l.granted",
                                &[],
                            )
                            .await
                            .unwrap()
                            .get(0);
                        assert_eq!(
                            waiting, 1,
                            "the DDL waits for the capture's lock"
                        );
                        *slot.lock().unwrap() = Some(task);
                    })
                }),
            );
            let before = names(&fx.capture("t").await.unwrap());
            capture_hook::clear("t");
            assert!(
                !before.contains(&format!("c{step}")),
                "step {step}: {before:?}"
            );
            let task = slot.lock().unwrap().take().unwrap();
            tokio::time::timeout(Duration::from_secs(10), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(
                names(&fx.capture("t").await.unwrap())
                    .contains(&format!("c{step}"))
            );
        }
    }

    /// T2: a DDL already holding the table is waited for; the capture's
    /// snapshot follows its commit.
    #[tokio::test]
    #[ignore = "requires docker"]
    async fn an_in_flight_ddl_is_waited_for() {
        let mut fx = Fx::new("cc_inflight").await;
        let other = fx.other().await;
        other
            .batch_execute("BEGIN; ALTER TABLE t ADD COLUMN z INT")
            .await
            .unwrap();
        let mut session =
            CatalogSession::new(dsn(fx.port, &fx.db).as_str().into());
        let (token, target) = (fx.token.clone(), fx.lsn().await);
        let cap = tokio::spawn(async move {
            capture(&mut session, "src", &token, target, "public", "t").await
        });
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert!(!cap.is_finished(), "the capture waits for the DDL's lock");
        other.batch_execute("COMMIT").await.unwrap();
        let r = cap.await.unwrap().unwrap();
        assert!(names(&r).contains(&"z".to_string()));
        let _ = &mut fx;
    }

    /// T3, T4: statements the lock does not conflict with, and DML on the
    /// table, proceed while a capture holds its lock.
    #[tokio::test]
    #[ignore = "requires docker"]
    async fn dml_and_non_conflicting_ddl_proceed_under_the_lock() {
        let mut fx = Fx::new("cc_dml").await;
        let other = Arc::new(fx.other().await);
        let o = other.clone();
        capture_hook::set(
            "t",
            Arc::new(move |s| {
                let o = o.clone();
                Box::pin(async move {
                    if s != 0 {
                        return;
                    }
                    for sql in [
                        "INSERT INTO t VALUES (1, 1)",
                        "UPDATE t SET v = 2 WHERE id = 1",
                        "DELETE FROM t WHERE id = 1",
                        "CREATE INDEX t_v ON t (v)",
                        "ALTER TABLE t ALTER COLUMN v SET STATISTICS 100",
                        "ALTER DOMAIN d_int SET NOT NULL",
                    ] {
                        tokio::time::timeout(
                            Duration::from_secs(5),
                            o.batch_execute(sql),
                        )
                        .await
                        .unwrap_or_else(|_| panic!("{sql} must not wait"))
                        .unwrap();
                    }
                })
            }),
        );
        let r = fx.capture("t").await;
        capture_hook::clear("t");
        assert_eq!(names(&r.unwrap()), ["id", "v"]);
    }

    /// T9: a lock that is never granted fails closed after the bounded
    /// attempts; a terminated catalog backend is retried.
    #[tokio::test]
    #[ignore = "requires docker"]
    async fn lock_timeout_fails_closed_and_a_terminated_backend_is_retried() {
        let mut fx = Fx::new("cc_fail").await;
        let holder = fx.other().await;
        holder
            .batch_execute("BEGIN; LOCK TABLE t IN ACCESS EXCLUSIVE MODE")
            .await
            .unwrap();
        let r = fx.capture("t").await;
        assert!(r.is_err(), "{r:?}");
        holder.batch_execute("ROLLBACK").await.unwrap();

        let killer = Arc::new(fx.other().await);
        let done = Arc::new(AtomicU32::new(0));
        let (k, d) = (killer.clone(), done.clone());
        capture_hook::set(
            "t",
            Arc::new(move |s| {
                let (k, d) = (k.clone(), d.clone());
                Box::pin(async move {
                    if s == 1 && d.fetch_add(1, Ordering::SeqCst) == 0 {
                        k.batch_execute(
                            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                             WHERE datname = current_database() \
                             AND state LIKE 'idle in transaction%' \
                             AND pid <> pg_backend_pid()",
                        )
                        .await
                        .unwrap();
                    }
                })
            }),
        );
        let r = fx.capture("t").await;
        capture_hook::clear("t");
        assert_eq!(names(&r.unwrap()), ["id", "v"]);
        assert!(done.load(Ordering::SeqCst) >= 2, "a second attempt ran");
    }

    async fn waiting(c: &tokio_postgres::Client) -> i64 {
        c.query_one(
            "SELECT count(*) FROM pg_locks l JOIN pg_class c ON \
             c.oid = l.relation WHERE c.relname = 't' AND NOT l.granted",
            &[],
        )
        .await
        .unwrap()
        .get(0)
    }

    /// Cancellation: dropping a capture that waits for the lock sends a
    /// cancel request, so its lock request disappears at once.
    #[tokio::test]
    #[ignore = "requires docker"]
    async fn a_cancelled_capture_releases_its_lock_request() {
        let fx = Fx::new("cc_cancel").await;
        let holder = fx.other().await;
        holder
            .batch_execute("BEGIN; LOCK TABLE t IN ACCESS EXCLUSIVE MODE")
            .await
            .unwrap();
        let mut session =
            CatalogSession::new(dsn(fx.port, &fx.db).as_str().into());
        let (token, target) = (fx.token.clone(), fx.lsn().await);
        let cap = tokio::spawn(async move {
            capture(&mut session, "src", &token, target, "public", "t").await
        });
        tokio::time::sleep(Duration::from_millis(500)).await;
        let observer = fx.other().await;
        assert_eq!(waiting(&observer).await, 1, "the capture waits");
        cap.abort();
        let _ = cap.await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            waiting(&observer).await,
            0,
            "cancelled at once, not at the timeout"
        );
        holder.batch_execute("ROLLBACK").await.unwrap();
    }

    /// The proof refuses another walsender's nonce and a target beyond the
    /// node's position, failing closed with the visibility incident.
    #[tokio::test]
    #[ignore = "requires docker"]
    async fn the_proof_refuses_a_stale_nonce_or_an_unreached_target() {
        let mut fx = Fx::new("cc_proof").await;
        let mut stale = fx.token.clone();
        stale.walsender.application_name = "deltaforge:stale".into();
        let target = fx.lsn().await;
        let r = capture(&mut fx.session, "src", &stale, target, "public", "t")
            .await;
        let msg = format!("{:#}", r.unwrap_err());
        assert!(msg.contains("walsender_nonce_mismatch"), "{msg}");
        let token = fx.token.clone();
        let far = Lsn(target.0 + (1 << 40));
        let r = stamp(&mut fx.session, "src", &token, far, "public", "t").await;
        let msg = format!("{:#}", r.unwrap_err());
        assert!(msg.contains("behind_target"), "{msg}");
    }
}

//! `ClickHouseSink` — the `Sink` implementation.
//!
//! `send_batch`: for each event, resolve the source table's columns (cached)
//! and claim the target table once per session before it is written (see
//! [`super::ownership`]): created by the sink with its marker, fence column
//! and fence constraint, or proven to be the sink's and its fence rotated.
//! Each event is projected into a RowBinary row carrying the fence, per-row
//! encode failures go to the DLQ, and the good rows are inserted as one
//! synchronous `INSERT … FORMAT RowBinary` with a dedup token. An insert the
//! fence constraint rejects fails the whole batch (`sink_destination_conflict`,
//! class `fenced`, never dead-lettered).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use deltaforge_config::{ChMode, ChVersionSource, ClickHouseSinkCfg};
use deltaforge_core::ownership::{
    DestinationMarker, EncodingMarker, Expected, OwnershipClass,
    RelationMarker, ownership_refusal,
};
use deltaforge_core::{
    BatchResult, Event, Sink, SinkError, SinkResult, SourceRelationIdentity,
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::ClickHouseSchemaResolver;
use super::client::{ChTransport, ClickHouseClient, is_fence_rejection};
use super::ownership::{ChDestination, mode_name};
use super::project::{TableProjection, project_row};
use super::types::map_column;
use super::version::derive_version;
use crate::ownership_guard::OwnershipGuard;

/// The marker's encoding layout.
const LAYOUT: &str = "clickhouse.row.v1";

pub struct ClickHouseSink {
    id: String,
    pipeline: String,
    required: bool,
    database: String,
    table: String,
    mode: ChMode,
    version_source: ChVersionSource,
    extensions_column: String,
    transport: Arc<dyn ChTransport>,
    resolver: Option<ClickHouseSchemaResolver>,
    projections: RwLock<HashMap<SourceRelationIdentity, Arc<Projected>>>,
    /// The target table this session verified it owns, and its fence.
    guard: OwnershipGuard,
}

/// A source table's projection and the key the target table is created
/// with.
struct Projected {
    projection: TableProjection,
    primary_key: Vec<String>,
}

impl ClickHouseSink {
    /// Source table key: `namespace.table`, the qualification the schema
    /// registry uses (see `SourceInfo::namespace`).
    fn source_key(ev: &Event) -> String {
        ev.source.full_table_name()
    }

    /// Resolve (and cache) the projection for an event's source table.
    fn projection_for(&self, ev: &Event) -> Result<Arc<Projected>, SinkError> {
        let relation = SourceRelationIdentity::of(&ev.source);
        if let Some(p) = self.projections.read().unwrap().get(&relation) {
            return Ok(p.clone());
        }
        let key = Self::source_key(ev);
        let resolver = self.resolver.as_ref().ok_or_else(|| SinkError::Other(
            anyhow::anyhow!("clickhouse sink has no schema resolver (source schema unavailable)"),
        ))?;
        let resolved =
            resolver(&key).map_err(SinkError::Other)?.ok_or_else(|| {
                // Transient during startup (before the snapshot loads the schema) —
                // return a retryable error so the batch replays later.
                SinkError::Other(anyhow::anyhow!(
                    "no schema yet for source table '{key}'"
                ))
            })?;

        // A source column named like a meta, fence or extension column would
        // collide in the target table: refused before any row is delivered.
        if let Some(c) = resolved.columns.iter().find(|c| {
            c.name == self.extensions_column
                || super::project::META_COLUMNS.contains(&c.name.as_str())
        }) {
            return Err(SinkError::Fatal {
                details: format!(
                    "clickhouse sink {}: source table '{key}' has a column \
                     `{}`, which collides with the sink's meta, fence or \
                     extension column (configure `extensions_column`)",
                    self.id, c.name
                )
                .into(),
            });
        }
        let p = Arc::new(Projected {
            projection: TableProjection {
                columns: resolved
                    .columns
                    .iter()
                    .map(|c| (c.clone(), map_column(c)))
                    .collect(),
                version_source: self.version_source.clone(),
                extensions_column: self.extensions_column.clone(),
            },
            primary_key: resolved.primary_key.clone(),
        });
        self.projections
            .write()
            .unwrap()
            .insert(relation, p.clone());
        Ok(p)
    }

    /// What the sink expects its target table to be for `ev`.
    fn expected(&self, ev: &Event) -> Expected {
        Expected {
            owner: self.guard.owner().clone(),
            relation: RelationMarker::from(&SourceRelationIdentity::of(
                &ev.source,
            )),
            destination: DestinationMarker::clickhouse(
                &self.database,
                &self.table,
            ),
            encoding: EncodingMarker {
                layout: LAYOUT.into(),
                type_mapping: ev.type_mapping.as_str().into(),
                mode: Some(mode_name(&self.mode).into()),
                extensions_column: Some(self.extensions_column.clone()),
            },
        }
    }

    /// A transport failure may be a reconnect: the table is claimed again
    /// before the next write.
    fn forget_on_transport_error(&self, e: &SinkError) {
        if matches!(
            e,
            SinkError::Connect { .. }
                | SinkError::Io(_)
                | SinkError::Backpressure { .. }
        ) {
            self.guard.invalidate_all();
        }
    }
}

/// The table claimed within one batch: its fence and the expectation it was
/// claimed for.
struct Claim {
    fence: String,
    expected: Expected,
}

impl Claim {
    /// Whether `ev` is of the relation (and type mapping) it was claimed
    /// for.
    fn holds(&self, ev: &Event) -> bool {
        let r = &self.expected.relation;
        let s = &ev.source;
        r.connector == s.connector
            && r.source == s.name
            && r.database == s.db
            && r.namespace == s.namespace()
            && r.table == s.table
            && self.expected.encoding.type_mapping == ev.type_mapping.as_str()
    }
}

#[async_trait]
impl Sink for ClickHouseSink {
    /// A ClickHouse row has a value for every column.
    fn requires_complete_rows(&self) -> bool {
        true
    }

    /// Nothing is buffered after an acknowledged batch: every INSERT of an
    /// acknowledged batch returned 2xx synchronously (`async_insert=0`, so
    /// the rows are in the table and passed its fence constraint) before
    /// `send_batch` returned, and a barrier is delivered only after every earlier batch was
    /// acknowledged.
    /// This sink keeps no watermark of its own.
    async fn barrier(
        &self,
        _kind: &deltaforge_core::BarrierKind,
        _ctx: &deltaforge_core::SinkBatchContext,
    ) -> SinkResult<()> {
        Ok(())
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn required(&self) -> bool {
        self.required
    }

    async fn send(&self, event: &Event) -> SinkResult<()> {
        self.send_batch(std::slice::from_ref(event))
            .await
            .map(|_| ())
    }

    async fn send_batch(&self, events: &[Event]) -> SinkResult<BatchResult> {
        if events.is_empty() {
            return Ok(BatchResult::default());
        }
        let mut body = Vec::with_capacity(events.len() * 64);
        let mut dlq: Vec<(usize, SinkError)> = Vec::new();
        let mut first_version: Option<u64> = None;
        let mut rows: u64 = 0;

        // Preflight: every source table's projection (and its collision
        // checks) and the target table's claim, before any row of the batch
        // is encoded or delivered. The claim (and its cache) is consulted
        // once per relation in the batch, not per event.
        let mut projections = Vec::with_capacity(events.len());
        let mut claims: Vec<Claim> = Vec::new();
        let mut fences = Vec::with_capacity(events.len());
        for ev in events {
            let p = self.projection_for(ev)?;
            let at = match claims.iter().position(|c| c.holds(ev)) {
                Some(at) => at,
                None => {
                    let expected = self.expected(ev);
                    let claimed = self
                        .guard
                        .fence(&expected, || ChDestination {
                            transport: self.transport.as_ref(),
                            database: &self.database,
                            table: &self.table,
                            mode: &self.mode,
                            extensions_column: &self.extensions_column,
                            columns: &p.projection.columns,
                            primary_key: &p.primary_key,
                        })
                        .await;
                    let claimed = match claimed {
                        Ok(c) => c,
                        Err(e) => {
                            self.forget_on_transport_error(&e);
                            return Err(e);
                        }
                    };
                    claims.push(Claim {
                        fence: claimed.fence,
                        expected,
                    });
                    claims.len() - 1
                }
            };
            projections.push(p);
            fences.push(at);
        }
        for (i, ev) in events.iter().enumerate() {
            let proj = &projections[i].projection;
            match project_row(proj, ev, &claims[fences[i]].fence) {
                Ok(bytes) => {
                    first_version.get_or_insert_with(|| {
                        derive_version(ev, self.version_source.clone())
                    });
                    body.extend_from_slice(&bytes);
                    rows += 1;
                }
                Err(e) => dlq.push((
                    i,
                    SinkError::Serialization {
                        details: e.to_string().into(),
                    },
                )),
            }
        }

        if rows > 0 {
            let token = format!(
                "{}:{}:{}:{}",
                self.pipeline,
                self.id,
                first_version.unwrap_or(0),
                rows
            );
            let start = std::time::Instant::now();
            if let Err(e) = self
                .transport
                .insert_rowbinary(&self.table, &token, body)
                .await
            {
                if is_fence_rejection(&e) {
                    // This session no longer holds the fence: the batch
                    // fails whole (never dead-lettered), the checkpoint
                    // stays, and the next claim decides.
                    let c = &claims[0];
                    self.guard.invalidate(&c.expected);
                    return Err(ownership_refusal(
                        &self.id,
                        &c.expected.destination,
                        &c.expected.relation.identity(),
                        OwnershipClass::Fenced,
                        None,
                        None,
                    ));
                }
                self.forget_on_transport_error(&e);
                return Err(e);
            }
            metrics::histogram!(
                "deltaforge_sink_clickhouse_insert_seconds",
                "pipeline" => self.pipeline.clone(),
                "sink" => self.id.clone(),
            )
            .record(start.elapsed().as_secs_f64());
            metrics::counter!(
                "deltaforge_sink_clickhouse_rows_total",
                "pipeline" => self.pipeline.clone(),
                "sink" => self.id.clone(),
            )
            .increment(rows);
        }

        Ok(BatchResult { dlq_failures: dlq })
    }
}

/// Build a `ClickHouseSink` from config. `resolver` supplies source-table
/// columns + PK (built by the runner); without it the sink can't project rows.
pub fn build_clickhouse_sink(
    cfg: &ClickHouseSinkCfg,
    _cancel: CancellationToken,
    pipeline: &str,
    resolver: Option<ClickHouseSchemaResolver>,
    ownership: &crate::SinkOwnership,
    creds: &crate::ResolvedSinkCreds,
) -> anyhow::Result<ClickHouseSink> {
    validate_clickhouse_cfg(cfg)?;
    // Effective credential: a resolved reference (protected runtime value) wins; otherwise
    // fall back to the inline (deprecated) field with `${ENV}` expansion. Resolved values
    // are never written back into the config.
    let effective = |field: &str,
                     inline: &Option<String>|
     -> anyhow::Result<Option<String>> {
        if let Some(v) = creds.get(field) {
            return Ok(Some(v.to_string()));
        }
        match inline {
            Some(s) => Ok(Some(shellexpand::env(s)?.into_owned())),
            None => Ok(None),
        }
    };
    let user = effective("user", &cfg.user)?;
    let password = effective("password", &cfg.password)?;

    let client = ClickHouseClient::new(cfg, user, password)?;
    if resolver.is_none() {
        warn!(
            sink = %cfg.id,
            "clickhouse sink built without a schema resolver — projection will fail until one is provided"
        );
    }
    Ok(ClickHouseSink {
        id: cfg.id.clone(),
        pipeline: pipeline.to_string(),
        required: cfg.required.unwrap_or(true),
        database: cfg.database.clone(),
        table: cfg.table.clone(),
        mode: cfg.mode.clone(),
        version_source: cfg.version_source.clone(),
        extensions_column: cfg.extensions_column.clone(),
        transport: Arc::new(client),
        resolver,
        projections: RwLock::new(HashMap::new()),
        guard: ownership.guard(pipeline, &cfg.id),
    })
}

/// The sink writes only to a table it created and owns: a table it did not
/// create can never be accepted, so `auto_create: false` is refused.
pub fn validate_clickhouse_cfg(cfg: &ClickHouseSinkCfg) -> anyhow::Result<()> {
    anyhow::ensure!(
        cfg.auto_create,
        "clickhouse sink {}: auto_create: false is not supported: the sink \
         writes only to a table it creates (with its ownership marker and \
         fence constraint); remove the setting",
        cfg.id
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clickhouse::TableColumns;
    use crate::clickhouse::ddl::{FENCE_COLUMN, fence_constraint_clause};
    use crate::clickhouse::ownership::element_list;
    use crate::clickhouse::types::ColDesc;
    use deltaforge_core::canonical::fixture;
    use deltaforge_core::{Op, SourceInfo, SourcePosition, TypeMapping};
    use serde_json::{Value, json};
    use std::sync::Mutex;

    /// One table of [`MiniCh`].
    #[derive(Clone)]
    struct Table {
        engine: String,
        comment: String,
        /// `(name, type)` in position order.
        columns: Vec<(String, String)>,
        /// The element-list items that are not columns.
        constraints: Vec<String>,
    }

    impl Table {
        fn create_table_query(&self, db: &str, name: &str) -> String {
            let mut items: Vec<String> = self
                .columns
                .iter()
                .map(|(n, t)| format!("`{n}` {t}"))
                .collect();
            items.extend(self.constraints.iter().cloned());
            format!(
                "CREATE TABLE {db}.{name} ({}) ENGINE = {} ORDER BY id",
                items.join(", "),
                self.engine
            )
        }

        fn fence(&self) -> Option<String> {
            self.constraints.iter().find_map(|c| {
                c.strip_prefix("CONSTRAINT df_fence_")
                    .map(|r| r[..32].to_string())
            })
        }
    }

    /// An in-memory ClickHouse server: the claim's queries, atomic
    /// creation, the constraint swap as a compare-and-swap, and inserts
    /// checked against the fence constraint.
    struct MiniCh {
        server: Mutex<String>,
        async_insert: Mutex<&'static str>,
        tables: Mutex<HashMap<String, Table>>,
        ddls: Mutex<Vec<String>>,
        inserts: Mutex<Vec<Vec<u8>>>,
        calls: Mutex<usize>,
        insert_attempts: Mutex<usize>,
    }

    impl MiniCh {
        fn new() -> Arc<Self> {
            Arc::new(MiniCh {
                server: Mutex::new("srv-1".into()),
                async_insert: Mutex::new("0"),
                tables: Mutex::new(HashMap::new()),
                ddls: Mutex::new(vec![]),
                inserts: Mutex::new(vec![]),
                calls: Mutex::new(0),
                insert_attempts: Mutex::new(0),
            })
        }

        fn only(&self) -> Table {
            self.tables.lock().unwrap().values().next().unwrap().clone()
        }

        /// Another claimer (a copy of the state store) rotated the fence.
        fn steal(&self) {
            let mut ts = self.tables.lock().unwrap();
            let t = ts.values_mut().next().unwrap();
            t.constraints = vec![fence_constraint_clause(
                &deltaforge_core::ownership::random_token(),
            )];
        }
    }

    fn unquote(s: &str) -> String {
        s.trim_matches('`')
            .replace("\\`", "`")
            .replace("\\\\", "\\")
    }

    fn literal(s: &str) -> String {
        s.trim_matches('\'')
            .replace("\\'", "'")
            .replace("\\\\", "\\")
    }

    #[async_trait]
    impl ChTransport for MiniCh {
        async fn insert_rowbinary(
            &self,
            _table: &str,
            _token: &str,
            body: Vec<u8>,
        ) -> Result<(), SinkError> {
            *self.calls.lock().unwrap() += 1;
            *self.insert_attempts.lock().unwrap() += 1;
            let fence = self.only().fence().unwrap();
            // Every row ends with the writer's fence: the constraint holds
            // only when the body carries the current one.
            if !body.windows(32).any(|w| w == fence.as_bytes()) {
                return Err(SinkError::Fatal {
                    details: format!(
                        "{}: Code: 469. Constraint `df_fence_{fence}` is violated",
                        crate::clickhouse::client::FENCE_REJECTED
                    )
                    .into(),
                });
            }
            self.inserts.lock().unwrap().push(body);
            Ok(())
        }
        async fn execute_ddl(&self, _sql: &str) -> Result<(), SinkError> {
            unreachable!("DDL goes through query_status")
        }
        async fn query_text(&self, _sql: &str) -> Result<String, SinkError> {
            unreachable!("queries go through query_status")
        }
        async fn query_status(
            &self,
            sql: &str,
        ) -> Result<(u16, String), SinkError> {
            // Lets concurrent claims interleave between their statements.
            tokio::task::yield_now().await;
            *self.calls.lock().unwrap() += 1;
            let ok = |t: String| Ok((200, t));
            if sql.starts_with("SELECT serverUUID()") {
                return ok(format!("{}\n", self.server.lock().unwrap()));
            }
            if sql.starts_with("SELECT getSetting('async_insert')") {
                return ok(format!("{}\n", self.async_insert.lock().unwrap()));
            }
            if sql.contains("FROM system.tables")
                || sql.contains("FROM system.columns")
            {
                let lits: Vec<String> = sql
                    .split('\'')
                    .skip(1)
                    .step_by(2)
                    .map(str::to_string)
                    .collect();
                let key = format!("{}.{}", lits[0], lits[1]);
                let Some(t) = self.tables.lock().unwrap().get(&key).cloned()
                else {
                    return ok(String::new());
                };
                if sql.contains("system.tables") {
                    return ok(json!({
                        "engine": t.engine,
                        "comment": t.comment,
                        "create_table_query": t.create_table_query(&lits[0], &lits[1]),
                    })
                    .to_string());
                }
                return ok(t
                    .columns
                    .iter()
                    .map(|(n, ty)| json!({"name": n, "type": ty}).to_string())
                    .collect::<Vec<_>>()
                    .join("\n"));
            }
            if let Some(rest) = sql.strip_prefix("CREATE TABLE ") {
                self.ddls.lock().unwrap().push(sql.into());
                let name_end = rest.find(" (").unwrap();
                let (db, table) = rest[..name_end].split_once("`.`").unwrap();
                let key = format!("{}.{}", unquote(db), unquote(table));
                let mut ts = self.tables.lock().unwrap();
                if ts.contains_key(&key) {
                    return Ok((
                        500,
                        "Code: 57. DB::Exception: Table already exists.".into(),
                    ));
                }
                let items = element_list(sql).unwrap();
                let (cons, cols): (Vec<_>, Vec<_>) = items
                    .into_iter()
                    .partition(|i| i.starts_with("CONSTRAINT"));
                let engine = if sql.contains("ENGINE = ReplacingMergeTree") {
                    "ReplacingMergeTree"
                } else {
                    "MergeTree"
                };
                let comment =
                    literal(&sql[sql.rfind(" COMMENT ").unwrap() + 9..]);
                ts.insert(
                    key,
                    Table {
                        engine: engine.into(),
                        comment,
                        columns: cols
                            .iter()
                            .map(|c| {
                                let close =
                                    if let Some(rest) = c.strip_prefix('`') {
                                        rest.find('`').unwrap() + 1
                                    } else {
                                        c.find(' ').unwrap() - 1
                                    };
                                (
                                    unquote(&c[..=close]),
                                    c[close + 2..].to_string(),
                                )
                            })
                            .collect(),
                        constraints: cons,
                    },
                );
                return ok(String::new());
            }
            if sql.starts_with("ALTER TABLE ") {
                let from = &sql
                    [sql.find("DROP CONSTRAINT df_fence_").unwrap() + 25..]
                    [..32];
                let mut ts = self.tables.lock().unwrap();
                let t = ts.values_mut().next().unwrap();
                if t.fence().as_deref() != Some(from) {
                    return Ok((
                        500,
                        "Code: 36. DB::Exception: Wrong constraint name."
                            .into(),
                    ));
                }
                let add = &sql[sql.find(", ADD ").unwrap() + 6..];
                t.constraints = vec![add.to_string()];
                return ok(String::new());
            }
            panic!("unexpected statement: {sql}");
        }
    }

    fn id_col() -> ColDesc {
        ColDesc {
            name: "id".into(),
            data_type: "bigint".into(),
            full_type: "bigint".into(),
            nullable: false,
            unsigned: false,
            precision: None,
            scale: None,
        }
    }

    fn sink_over(
        ch: Arc<MiniCh>,
        columns: Vec<ColDesc>,
        ownership: &crate::SinkOwnership,
    ) -> ClickHouseSink {
        let resolver: ClickHouseSchemaResolver = Arc::new(move |_key: &str| {
            Ok(Some(TableColumns {
                columns: columns.clone(),
                primary_key: vec!["id".into()],
            }))
        });
        ClickHouseSink {
            id: "c".into(),
            pipeline: "p".into(),
            required: true,
            database: "d".into(),
            table: "t".into(),
            mode: ChMode::Upsert,
            version_source: ChVersionSource::TsMs,
            extensions_column: "_extensions".into(),
            transport: ch,
            resolver: Some(resolver),
            projections: RwLock::new(HashMap::new()),
            guard: ownership.guard("p", "c"),
        }
    }

    fn sink_with(ch: Arc<MiniCh>) -> ClickHouseSink {
        sink_over(ch, vec![id_col()], &crate::SinkOwnership::ephemeral())
    }

    fn ev(after: Value) -> Event {
        Event {
            before: None,
            after: Some(fixture::row(after)),
            source: SourceInfo {
                version: "1".into(),
                connector: "mysql".into(),
                name: "t".into(),
                ts_ms: 5,
                db: "d".into(),
                schema: None,
                table: "orders".into(),
                snapshot: None,
                position: SourcePosition::default(),
            },
            op: Op::Create,
            ts_ms: 5,
            transaction: None,
            type_mapping: TypeMapping::V2,
            event_id: None,
            tenant_id: None,
            schema_version: None,
            schema_sequence: None,
            ddl: None,
            trace_id: None,
            tags: None,
            synthetic: None,
            routing: None,
            tx_end: false,
            boundary: None,
            size_bytes: 0,

            received_at_ms: 0,
        }
    }

    fn class(e: &SinkError) -> Option<String> {
        e.draft()
            .and_then(|d| d.discriminator.get("class").cloned())
    }

    #[tokio::test]
    async fn creates_the_table_with_marker_fence_column_and_constraint() {
        let ch = MiniCh::new();
        let sink = sink_with(ch.clone());
        let res = sink
            .send_batch(&[ev(json!({"id": 1})), ev(json!({"id": "abc"}))])
            .await
            .unwrap();
        assert_eq!(res.dlq_failures.len(), 1, "the bad row is isolated");
        let ddl = ch.ddls.lock().unwrap()[0].clone();
        assert!(ddl.starts_with("CREATE TABLE `d`.`t` ("), "{ddl}");
        assert!(!ddl.contains("IF NOT EXISTS"));
        assert!(ddl.contains("ReplacingMergeTree"));
        let t = ch.only();
        assert_eq!(
            t.columns.last().unwrap(),
            &(
                FENCE_COLUMN.to_string(),
                "LowCardinality(String)".to_string()
            )
        );
        let marker: Value = serde_json::from_str(&t.comment).unwrap();
        assert_eq!(
            marker["destination"],
            json!({"kind": "clickhouse", "database": "d", "table": "t"})
        );
        assert_eq!(marker["encoding"]["mode"], "upsert");
        assert!(marker.get("fence").is_none());
        // The inserted row ends with the current fence.
        let fence = t.fence().unwrap();
        let body = ch.inserts.lock().unwrap()[0].clone();
        assert!(body.ends_with(fence.as_bytes()));
        assert_eq!(body[body.len() - 33], 32, "varuint length of the fence");
    }

    #[tokio::test]
    async fn the_steady_state_makes_no_ownership_request() {
        let ch = MiniCh::new();
        let sink = sink_with(ch.clone());
        sink.send_batch(&[ev(json!({"id": 1}))]).await.unwrap();
        let after_claim = *ch.calls.lock().unwrap();
        for i in 2..22 {
            sink.send_batch(&[ev(json!({"id": i}))]).await.unwrap();
        }
        assert_eq!(*ch.calls.lock().unwrap() - after_claim, 20, "inserts only");
    }

    #[tokio::test]
    async fn the_target_table_holds_one_relation_across_restarts() {
        let ch = MiniCh::new();
        let ownership = crate::SinkOwnership::ephemeral();
        let of = |db: &str| {
            let mut e = ev(json!({"id": 1}));
            e.source.db = db.into();
            e
        };
        sink_over(ch.clone(), vec![id_col()], &ownership)
            .send_batch(&[of("d1")])
            .await
            .unwrap();
        let restarted = sink_over(ch.clone(), vec![id_col()], &ownership);
        let e = restarted.send_batch(&[of("d2")]).await.unwrap_err();
        assert_eq!(class(&e).as_deref(), Some("bound_to_another_relation"));
        assert_eq!(
            ch.inserts.lock().unwrap().len(),
            1,
            "nothing more inserted"
        );
        restarted.send_batch(&[of("d1")]).await.unwrap();
    }

    #[tokio::test]
    async fn a_fenced_insert_fails_whole_and_never_reaches_the_dlq() {
        let ch = MiniCh::new();
        let sink = sink_with(ch.clone());
        sink.send_batch(&[ev(json!({"id": 1}))]).await.unwrap();
        ch.steal();
        let e = sink
            .send_batch(&[ev(json!({"id": 2})), ev(json!({"id": "bad"}))])
            .await
            .unwrap_err();
        assert_eq!(class(&e).as_deref(), Some("fenced"), "{e:?}");
        let e = sink.send_batch(&[ev(json!({"id": 2}))]).await.unwrap_err();
        assert_eq!(
            class(&e).as_deref(),
            Some("fenced"),
            "the claim is refused too"
        );
        assert_eq!(ch.inserts.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_transport_failure_reclaims_before_the_next_insert() {
        let ch = MiniCh::new();
        let sink = sink_with(ch.clone());
        sink.send_batch(&[ev(json!({"id": 1}))]).await.unwrap();
        let fence = ch.only().fence().unwrap();
        // A broken connection while the next claim reads.
        *ch.server.lock().unwrap() = String::new();
        sink.guard.invalidate_all();
        let e = sink.send_batch(&[ev(json!({"id": 2}))]).await.unwrap_err();
        assert!(matches!(e, SinkError::Backpressure { .. }), "{e:?}");
        *ch.server.lock().unwrap() = "srv-1".into();
        sink.send_batch(&[ev(json!({"id": 2}))]).await.unwrap();
        assert_ne!(ch.only().fence().unwrap(), fence, "claimed again");
    }

    #[tokio::test]
    async fn an_insert_transport_error_forgets_the_verification() {
        struct Flaky(Arc<MiniCh>, Mutex<bool>);
        #[async_trait]
        impl ChTransport for Flaky {
            async fn insert_rowbinary(
                &self,
                t: &str,
                k: &str,
                b: Vec<u8>,
            ) -> Result<(), SinkError> {
                if std::mem::take(&mut *self.1.lock().unwrap()) {
                    return Err(SinkError::Connect {
                        details: "down".into(),
                    });
                }
                self.0.insert_rowbinary(t, k, b).await
            }
            async fn execute_ddl(&self, s: &str) -> Result<(), SinkError> {
                self.0.execute_ddl(s).await
            }
            async fn query_text(&self, s: &str) -> Result<String, SinkError> {
                self.0.query_text(s).await
            }
            async fn query_status(
                &self,
                s: &str,
            ) -> Result<(u16, String), SinkError> {
                self.0.query_status(s).await
            }
        }
        let ch = MiniCh::new();
        let flaky = Arc::new(Flaky(ch.clone(), Mutex::new(false)));
        let mut sink = sink_with(ch.clone());
        sink.transport = flaky.clone();
        sink.send_batch(&[ev(json!({"id": 1}))]).await.unwrap();
        let fence = ch.only().fence().unwrap();
        *flaky.1.lock().unwrap() = true;
        sink.send_batch(&[ev(json!({"id": 2}))]).await.unwrap_err();
        sink.send_batch(&[ev(json!({"id": 2}))]).await.unwrap();
        assert_ne!(
            ch.only().fence().unwrap(),
            fence,
            "a reconnect claims again"
        );
    }

    /// Two copies of one state store claiming at once: the loser of the
    /// constraint compare-and-swap is refused at its claim and never even
    /// attempts an insert.
    #[tokio::test]
    async fn the_loser_of_a_concurrent_claim_never_inserts() {
        for _ in 0..20 {
            let ch = MiniCh::new();
            let store = Arc::new(
                deltaforge_core::ownership::MemoryOwnershipStore::default(),
            );
            let original = crate::SinkOwnership {
                deployment: deltaforge_core::ownership::random_token(),
                store: store.clone(),
            };
            sink_over(ch.clone(), vec![id_col()], &original)
                .send_batch(&[ev(json!({"id": 1}))])
                .await
                .unwrap();
            let copy = crate::SinkOwnership {
                deployment: original.deployment.clone(),
                store: Arc::new(store.copy()),
            };
            let a = sink_over(ch.clone(), vec![id_col()], &original);
            let b = sink_over(ch.clone(), vec![id_col()], &copy);
            let before = *ch.insert_attempts.lock().unwrap();
            let batch = [ev(json!({"id": 2}))];
            let (ra, rb) =
                tokio::join!(a.send_batch(&batch), b.send_batch(&batch));
            let ok = [&ra, &rb].iter().filter(|r| r.is_ok()).count();
            let attempts = *ch.insert_attempts.lock().unwrap() - before;
            assert!(ok <= 1, "both claimed");
            assert_eq!(attempts, ok, "a refused claimer attempted an insert");
        }
    }

    #[tokio::test]
    async fn unsupported_engines_and_async_inserts_are_refused() {
        for engine in
            ["ReplicatedReplacingMergeTree", "Distributed", "MergeTree"]
        {
            let ch = MiniCh::new();
            let ownership = crate::SinkOwnership::ephemeral();
            sink_over(ch.clone(), vec![id_col()], &ownership)
                .send_batch(&[ev(json!({"id": 1}))])
                .await
                .unwrap();
            ch.tables
                .lock()
                .unwrap()
                .values_mut()
                .next()
                .unwrap()
                .engine = engine.into();
            let e = sink_over(ch.clone(), vec![id_col()], &ownership)
                .send_batch(&[ev(json!({"id": 1}))])
                .await
                .unwrap_err();
            assert_eq!(
                class(&e).as_deref(),
                Some("unsupported_topology"),
                "{engine}"
            );
        }
        let ch = MiniCh::new();
        *ch.async_insert.lock().unwrap() = "1";
        let e = sink_with(ch.clone())
            .send_batch(&[ev(json!({"id": 1}))])
            .await
            .unwrap_err();
        assert_eq!(class(&e).as_deref(), Some("fencing_unenforced"));
        assert!(ch.tables.lock().unwrap().is_empty(), "nothing created");
    }

    #[tokio::test]
    async fn another_server_behind_the_endpoint_is_refused() {
        let ch = MiniCh::new();
        let ownership = crate::SinkOwnership::ephemeral();
        sink_over(ch.clone(), vec![id_col()], &ownership)
            .send_batch(&[ev(json!({"id": 1}))])
            .await
            .unwrap();
        *ch.server.lock().unwrap() = "srv-2".into();
        let e = sink_over(ch.clone(), vec![id_col()], &ownership)
            .send_batch(&[ev(json!({"id": 1}))])
            .await
            .unwrap_err();
        assert_eq!(class(&e).as_deref(), Some("server_changed"));
    }

    #[tokio::test]
    async fn a_malformed_fence_column_or_constraint_is_refused() {
        type Damage = fn(&mut Table);
        let damages: [(&str, Damage); 4] = [
            ("no constraint", |t| t.constraints.clear()),
            ("two constraints", |t| {
                let extra = fence_constraint_clause(
                    &deltaforge_core::ownership::random_token(),
                );
                t.constraints.push(extra)
            }),
            ("weakened", |t| {
                let c = t.constraints[0].clone() + " OR 1";
                t.constraints = vec![c]
            }),
            ("column type", |t| {
                t.columns.last_mut().unwrap().1 = "String".into()
            }),
        ];
        for (what, damage) in damages {
            let ch = MiniCh::new();
            let ownership = crate::SinkOwnership::ephemeral();
            sink_over(ch.clone(), vec![id_col()], &ownership)
                .send_batch(&[ev(json!({"id": 1}))])
                .await
                .unwrap();
            damage(ch.tables.lock().unwrap().values_mut().next().unwrap());
            let e = sink_over(ch.clone(), vec![id_col()], &ownership)
                .send_batch(&[ev(json!({"id": 1}))])
                .await
                .unwrap_err();
            assert_eq!(class(&e).as_deref(), Some("fence_malformed"), "{what}");
        }
    }

    #[tokio::test]
    async fn an_unmarked_or_foreign_table_is_refused() {
        let ch = MiniCh::new();
        sink_with(ch.clone())
            .send_batch(&[ev(json!({"id": 1}))])
            .await
            .unwrap();
        // Another deployment.
        let e = sink_with(ch.clone())
            .send_batch(&[ev(json!({"id": 1}))])
            .await
            .unwrap_err();
        assert_eq!(class(&e).as_deref(), Some("owned_by_other"));
        ch.tables
            .lock()
            .unwrap()
            .values_mut()
            .next()
            .unwrap()
            .comment
            .clear();
        let e = sink_with(ch.clone())
            .send_batch(&[ev(json!({"id": 1}))])
            .await
            .unwrap_err();
        assert_eq!(class(&e).as_deref(), Some("marker_missing"));
    }

    #[tokio::test]
    async fn reserved_column_collisions_are_refused_before_anything() {
        for name in ["_extensions", "_version", FENCE_COLUMN] {
            let ch = MiniCh::new();
            let mut clash = id_col();
            clash.name = name.into();
            let sink = sink_over(
                ch.clone(),
                vec![id_col(), clash],
                &crate::SinkOwnership::ephemeral(),
            );
            let err =
                sink.send_batch(&[ev(json!({"id": 1}))]).await.unwrap_err();
            assert!(matches!(err, SinkError::Fatal { .. }), "{name}: {err:?}");
            assert!(ch.tables.lock().unwrap().is_empty(), "{name}: no table");
            assert!(ch.inserts.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn inserted_row_ends_with_the_extension_column_then_the_fence() {
        let ch = MiniCh::new();
        let sink = sink_with(ch.clone());
        let mut e = ev(json!({"id": 1}));
        e.after
            .as_mut()
            .unwrap()
            .set_extension(
                "enriched",
                deltaforge_core::canonical::Document::new(json!({"k": [1, 2]})),
            )
            .unwrap();
        let res = sink.send_batch(&[e, ev(json!({"id": 2}))]).await.unwrap();
        assert!(res.dlq_failures.is_empty());
        let fence = ch.only().fence().unwrap();
        let body = ch.inserts.lock().unwrap()[0].clone();
        // Row 1: id(8) _op(2) _version(8) _deleted(1) _source_ts(8),
        // Nullable(String) = 0, varuint len, canonical JSON, then the fence.
        let ext = br#"{"enriched":{"k":[1,2]}}"#;
        let mut row1 = Vec::new();
        row1.extend_from_slice(&1i64.to_le_bytes());
        row1.extend_from_slice(&[1, b'c']);
        row1.extend_from_slice(&5u64.to_le_bytes());
        row1.push(0);
        row1.extend_from_slice(&5i64.to_le_bytes());
        row1.push(0);
        row1.push(ext.len() as u8);
        row1.extend_from_slice(ext);
        row1.push(32);
        row1.extend_from_slice(fence.as_bytes());
        assert_eq!(&body[..row1.len()], &row1[..]);
        // Row 2: no extensions (NULL, one byte 1), then the fence.
        assert_eq!(body.len(), row1.len() + 27 + 1 + 33);
    }

    #[test]
    fn auto_create_false_is_rejected_at_configuration() {
        let mut cfg: ClickHouseSinkCfg = serde_json::from_value(json!({
            "id": "c", "url": "http://ch:8123", "database": "d", "table": "t"
        }))
        .unwrap();
        assert!(validate_clickhouse_cfg(&cfg).is_ok());
        cfg.auto_create = false;
        let e = validate_clickhouse_cfg(&cfg).unwrap_err().to_string();
        assert!(e.contains("auto_create: false is not supported"), "{e}");
    }
}

//! `ClickHouseSink` — the `Sink` implementation.
//!
//! `send_batch`: for each event, resolve the source table's columns (cached),
//! ensure the target table exists (auto-create once), project the event into a
//! RowBinary row, isolate per-row encode failures into the DLQ, then insert the
//! good rows as one `INSERT … FORMAT RowBinary` with a dedup token.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use deltaforge_config::{ChMode, ChVersionSource, ClickHouseSinkCfg};
use deltaforge_core::{
    BatchResult, Event, Sink, SinkError, SinkResult, SourceRelationIdentity,
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::ClickHouseSchemaResolver;
use super::client::{ChTransport, ClickHouseClient};
use super::ddl::create_table_ddl;
use super::project::{TableProjection, project_row};
use super::types::map_column;
use super::version::derive_version;

pub struct ClickHouseSink {
    id: String,
    pipeline: String,
    required: bool,
    database: String,
    table: String,
    mode: ChMode,
    version_source: ChVersionSource,
    auto_create: bool,
    extensions_column: String,
    transport: Arc<dyn ChTransport>,
    resolver: Option<ClickHouseSchemaResolver>,
    projections: RwLock<HashMap<SourceRelationIdentity, Arc<TableProjection>>>,
    /// The target table holds one source relation, durably bound.
    guard: crate::destination::DestinationGuard,
    /// The server's UUID (read once per session): part of the destination
    /// name, so the same table name on two servers is two destinations.
    server: tokio::sync::OnceCell<String>,
    table_ensured: AtomicBool,
}

impl ClickHouseSink {
    /// Source table key: `namespace.table`, the qualification the schema
    /// registry uses (see `SourceInfo::namespace`).
    fn source_key(ev: &Event) -> String {
        ev.source.full_table_name()
    }

    /// Resolve (and cache) the projection for an event's source table, ensuring
    /// the target table exists on first use.
    async fn projection_for(
        &self,
        ev: &Event,
    ) -> Result<Arc<TableProjection>, SinkError> {
        let relation = SourceRelationIdentity::of(&ev.source);
        let cached = self.projections.read().unwrap().get(&relation).cloned();
        let proj = match cached {
            Some(p) => p,
            None => self.build_projection(ev, &relation).await?,
        };
        // The target table holds one source relation, durably bound.
        self.verify_destination(&relation).await?;
        Ok(proj)
    }

    /// The table's durable binding admits `relation` (verified once per
    /// session, before anything is written).
    async fn verify_destination(
        &self,
        relation: &SourceRelationIdentity,
    ) -> Result<(), SinkError> {
        let server = self
            .server
            .get_or_try_init(|| async {
                let text =
                    self.transport.query_text("SELECT serverUUID()").await?;
                let uuid = text.trim().to_string();
                if uuid.is_empty() {
                    return Err(SinkError::Io(std::io::Error::other(
                        "clickhouse: the server reports no serverUUID()",
                    )));
                }
                Ok(uuid)
            })
            .await?;
        let destination =
            format!("clickhouse/{server}/{}.{}", self.database, self.table);
        let transport = Arc::clone(&self.transport);
        let (database, table) = (self.database.clone(), self.table.clone());
        self.guard
            .verify(&destination, relation, || async move {
                table_state(transport.as_ref(), &database, &table).await
            })
            .await
    }

    /// Resolve the projection of `relation` (ensuring the target table
    /// exists on first use) and cache it.
    async fn build_projection(
        &self,
        ev: &Event,
        relation: &SourceRelationIdentity,
    ) -> Result<Arc<TableProjection>, SinkError> {
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

        // A source column named like a meta or the extension column would
        // collide in the target table: refused before any row is delivered.
        if let Some(c) = resolved.columns.iter().find(|c| {
            c.name == self.extensions_column
                || super::project::META_COLUMNS.contains(&c.name.as_str())
        }) {
            return Err(SinkError::Fatal {
                details: format!(
                    "clickhouse sink {}: source table '{key}' has a column \
                     `{}`, which collides with the sink's meta or extension \
                     column (configure `extensions_column`)",
                    self.id, c.name
                )
                .into(),
            });
        }
        let typed: Vec<_> = resolved
            .columns
            .iter()
            .map(|c| (c.clone(), map_column(c)))
            .collect();

        // Auto-create the target table once (CREATE TABLE IF NOT EXISTS).
        if self.auto_create && !self.table_ensured.load(Ordering::Acquire) {
            let ddl = create_table_ddl(
                &self.database,
                &self.table,
                &typed,
                &resolved.primary_key,
                self.mode.clone(),
                &self.extensions_column,
            );
            self.transport.execute_ddl(&ddl).await?;
            self.table_ensured.store(true, Ordering::Release);
        }

        let proj = Arc::new(TableProjection {
            columns: typed,
            version_source: self.version_source.clone(),
            extensions_column: self.extensions_column.clone(),
        });
        self.projections
            .write()
            .unwrap()
            .insert(relation.clone(), proj.clone());
        Ok(proj)
    }
}

/// A ClickHouse string literal.
fn literal(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// The table's own identity (its UUID) and whether it holds rows.
async fn table_state(
    transport: &dyn ChTransport,
    database: &str,
    table: &str,
) -> Result<crate::destination::TargetState, SinkError> {
    let uid = transport
        .query_text(&format!(
            "SELECT toString(uuid) FROM system.tables WHERE database = {} \
             AND name = {} FORMAT TabSeparated",
            literal(database),
            literal(table)
        ))
        .await?
        .trim()
        .to_string();
    if uid.is_empty() {
        return Err(SinkError::Io(std::io::Error::other(format!(
            "clickhouse: table {database}.{table} does not exist"
        ))));
    }
    let rows = transport
        .query_text(&format!(
            "SELECT count() > 0 FROM `{}`.`{}` FORMAT TabSeparated",
            database.replace('`', "``"),
            table.replace('`', "``")
        ))
        .await?;
    Ok(crate::destination::TargetState {
        uid,
        has_rows: rows.trim() != "0",
    })
}

#[async_trait]
impl Sink for ClickHouseSink {
    /// Nothing is buffered after an acknowledged batch: every INSERT of an
    /// acknowledged batch returned 2xx with `wait_for_async_insert=1` (so the
    /// rows are in the table even under `async_insert`) before `send_batch`
    /// returned, and a barrier is delivered only after every earlier batch was
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
        // checks) before any row of the batch is encoded or delivered.
        let mut projections = Vec::with_capacity(events.len());
        for ev in events {
            projections.push(self.projection_for(ev).await?);
        }
        for (i, ev) in events.iter().enumerate() {
            let proj = &projections[i];
            match project_row(proj, ev) {
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
            self.transport
                .insert_rowbinary(&self.table, &token, body)
                .await?;
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
    bindings: Arc<dyn deltaforge_core::destination::DestinationBindingStore>,
    creds: &crate::ResolvedSinkCreds,
) -> anyhow::Result<ClickHouseSink> {
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
        auto_create: cfg.auto_create,
        extensions_column: cfg.extensions_column.clone(),
        transport: Arc::new(client),
        resolver,
        projections: RwLock::new(HashMap::new()),
        guard: crate::destination::DestinationGuard::new(&cfg.id, bindings),
        server: Default::default(),
        table_ensured: AtomicBool::new(false),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clickhouse::TableColumns;
    use crate::clickhouse::types::ColDesc;
    use deltaforge_core::canonical::fixture;
    use deltaforge_core::{Op, SourceInfo, SourcePosition, TypeMapping};
    use serde_json::{Value, json};
    use std::sync::Mutex;

    #[derive(Default)]
    struct Captured {
        inserts: Mutex<Vec<Vec<u8>>>,
        ddls: Mutex<Vec<String>>,
    }

    struct FakeTransport {
        cap: Arc<Captured>,
    }
    #[async_trait]
    impl ChTransport for FakeTransport {
        async fn insert_rowbinary(
            &self,
            _table: &str,
            _token: &str,
            body: Vec<u8>,
        ) -> Result<(), SinkError> {
            self.cap.inserts.lock().unwrap().push(body);
            Ok(())
        }
        async fn execute_ddl(&self, sql: &str) -> Result<(), SinkError> {
            self.cap.ddls.lock().unwrap().push(sql.to_string());
            Ok(())
        }
        /// One server; the table's UUID; rows after any insert.
        async fn query_text(&self, sql: &str) -> Result<String, SinkError> {
            Ok(if sql.contains("serverUUID") {
                "srv\n".into()
            } else if sql.contains("system.tables") {
                "tbl-uuid\n".into()
            } else if self.cap.inserts.lock().unwrap().is_empty() {
                "0\n".into()
            } else {
                "1\n".into()
            })
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

    fn sink_with(cap: Arc<Captured>) -> ClickHouseSink {
        sink_with_columns(cap, vec![id_col()])
    }

    fn sink_with_columns(
        cap: Arc<Captured>,
        columns: Vec<ColDesc>,
    ) -> ClickHouseSink {
        sink_over(
            cap,
            columns,
            Arc::new(deltaforge_core::destination::MemoryDestinationBindings::default()),
        )
    }

    fn sink_over(
        cap: Arc<Captured>,
        columns: Vec<ColDesc>,
        store: Arc<dyn deltaforge_core::destination::DestinationBindingStore>,
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
            auto_create: true,
            extensions_column: "_extensions".into(),
            transport: Arc::new(FakeTransport { cap }),
            resolver: Some(resolver),
            projections: RwLock::new(HashMap::new()),
            guard: crate::destination::DestinationGuard::new("c", store),
            server: Default::default(),
            table_ensured: AtomicBool::new(false),
        }
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

    /// A second relation into the bound table is refused before any insert,
    /// also when a restart sees it first; the bound one still writes.
    #[tokio::test]
    async fn the_target_table_holds_one_relation_across_restarts() {
        let store: Arc<
            dyn deltaforge_core::destination::DestinationBindingStore,
        > = Arc::new(
            deltaforge_core::destination::MemoryDestinationBindings::default(),
        );
        let of = |db: &str| {
            let mut e = ev(json!({"id": 1}));
            e.source.db = db.into();
            e
        };
        let cap = Arc::new(Captured::default());
        let sink = sink_over(cap.clone(), vec![id_col()], Arc::clone(&store));
        sink.send_batch(&[of("d1")]).await.unwrap();

        let cap2 = Arc::new(Captured::default());
        let restarted =
            sink_over(cap2.clone(), vec![id_col()], Arc::clone(&store));
        let e = restarted.send_batch(&[of("d2")]).await.unwrap_err();
        assert_eq!(
            e.draft().map(|d| d.reason_code),
            Some(
                deltaforge_core::incident::ReasonCode::SinkDestinationConflict
            )
        );
        assert!(cap2.inserts.lock().unwrap().is_empty(), "nothing inserted");
        restarted.send_batch(&[of("d1")]).await.unwrap();
    }

    #[tokio::test]
    async fn projects_batch_isolates_bad_row_and_auto_creates() {
        let cap = Arc::new(Captured::default());
        let sink = sink_with(cap.clone());
        // one good row, one with a non-integer id → DLQ
        let events = vec![ev(json!({"id": 1})), ev(json!({"id": "abc"}))];
        let res = sink.send_batch(&events).await.unwrap();

        assert_eq!(res.dlq_failures.len(), 1);
        assert_eq!(res.dlq_failures[0].0, 1, "second event failed");
        assert_eq!(
            cap.inserts.lock().unwrap().len(),
            1,
            "one insert for the good row"
        );
        assert_eq!(
            cap.ddls.lock().unwrap().len(),
            1,
            "auto-created the table once"
        );
        assert!(cap.ddls.lock().unwrap()[0].contains("ReplacingMergeTree"));
    }

    #[tokio::test]
    async fn extension_column_collision_is_refused_before_any_insert() {
        let cap = Arc::new(Captured::default());
        let mut clash = id_col();
        clash.name = "_extensions".into();
        clash.data_type = "varchar".into();
        clash.full_type = "varchar(10)".into();
        let sink = sink_with_columns(cap.clone(), vec![id_col(), clash]);

        let err = sink
            .send_batch(&[ev(json!({"id": 1, "_extensions": "x"}))])
            .await
            .unwrap_err();
        assert!(matches!(err, SinkError::Fatal { .. }), "got: {err:?}");
        assert!(err.to_string().contains("_extensions"), "got: {err}");
        assert!(cap.inserts.lock().unwrap().is_empty(), "nothing inserted");
        assert!(cap.ddls.lock().unwrap().is_empty(), "no table created");
    }

    #[tokio::test]
    async fn meta_column_collision_is_refused_before_any_insert() {
        let cap = Arc::new(Captured::default());
        let mut clash = id_col();
        clash.name = "_version".into();
        let sink = sink_with_columns(cap.clone(), vec![id_col(), clash]);

        let err = sink
            .send_batch(&[ev(json!({"id": 1, "_version": 2}))])
            .await
            .unwrap_err();
        assert!(matches!(err, SinkError::Fatal { .. }), "got: {err:?}");
        assert!(cap.inserts.lock().unwrap().is_empty(), "nothing inserted");
    }

    #[tokio::test]
    async fn inserted_row_ends_with_the_extension_column() {
        let cap = Arc::new(Captured::default());
        let sink = sink_with(cap.clone());
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
        let inserts = cap.inserts.lock().unwrap();
        assert_eq!(inserts.len(), 1);
        let body = &inserts[0];
        // Row 1: id(8) _op(2) _version(8) _deleted(1) _source_ts(8), then
        // Nullable(String) = 0, varuint len, canonical JSON.
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
        assert_eq!(&body[..row1.len()], &row1[..]);
        // Row 2 has no extensions: the column is NULL (one byte 1).
        assert_eq!(body.len(), row1.len() + 27 + 1);
        assert_eq!(*body.last().unwrap(), 1);
    }
}

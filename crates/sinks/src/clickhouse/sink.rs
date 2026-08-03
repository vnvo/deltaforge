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
use deltaforge_core::{BatchResult, Event, Sink, SinkError, SinkResult};
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
    transport: Arc<dyn ChTransport>,
    resolver: Option<ClickHouseSchemaResolver>,
    projections: RwLock<HashMap<String, Arc<TableProjection>>>,
    table_ensured: AtomicBool,
}

impl ClickHouseSink {
    /// Source table key: `"{namespace}.{table}"` where namespace is the PG schema
    /// (if present) else the db — the same qualification the schema registry uses.
    fn source_key(ev: &Event) -> String {
        let ns = ev
            .source
            .schema
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or(&ev.source.db);
        format!("{ns}.{}", ev.source.table)
    }

    /// Resolve (and cache) the projection for an event's source table, ensuring
    /// the target table exists on first use.
    async fn projection_for(
        &self,
        ev: &Event,
    ) -> Result<Arc<TableProjection>, SinkError> {
        let key = Self::source_key(ev);
        if let Some(p) = self.projections.read().unwrap().get(&key) {
            return Ok(p.clone());
        }

        let resolver = self.resolver.as_ref().ok_or_else(|| SinkError::Other(
            anyhow::anyhow!("clickhouse sink has no schema resolver (source schema unavailable)"),
        ))?;
        let resolved = resolver(&key).ok_or_else(|| {
            // Transient during startup (before the snapshot loads the schema) —
            // return a retryable error so the batch replays later.
            SinkError::Other(anyhow::anyhow!(
                "no schema yet for source table '{key}'"
            ))
        })?;

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
            );
            self.transport.execute_ddl(&ddl).await?;
            self.table_ensured.store(true, Ordering::Release);
        }

        let proj = Arc::new(TableProjection {
            columns: typed,
            version_source: self.version_source.clone(),
        });
        self.projections.write().unwrap().insert(key, proj.clone());
        Ok(proj)
    }
}

#[async_trait]
impl Sink for ClickHouseSink {
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

        for (i, ev) in events.iter().enumerate() {
            let proj = self.projection_for(ev).await?;
            match project_row(&proj, ev) {
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
) -> anyhow::Result<ClickHouseSink> {
    // Expand ${ENV} in user/password like other DSNs.
    let expand = |o: &Option<String>| -> anyhow::Result<Option<String>> {
        match o {
            Some(s) => Ok(Some(shellexpand::env(s)?.into_owned())),
            None => Ok(None),
        }
    };
    let mut expanded = cfg.clone();
    expanded.user = expand(&cfg.user)?;
    expanded.password = expand(&cfg.password)?;

    let client = ClickHouseClient::new(&expanded)?;
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
        transport: Arc::new(client),
        resolver,
        projections: RwLock::new(HashMap::new()),
        table_ensured: AtomicBool::new(false),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clickhouse::TableColumns;
    use crate::clickhouse::types::ColDesc;
    use deltaforge_core::{Op, SourceInfo, SourcePosition};
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
        let resolver: ClickHouseSchemaResolver = Arc::new(|_key: &str| {
            Some(TableColumns {
                columns: vec![id_col()],
                primary_key: vec!["id".into()],
            })
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
            transport: Arc::new(FakeTransport { cap }),
            resolver: Some(resolver),
            projections: RwLock::new(HashMap::new()),
            table_ensured: AtomicBool::new(false),
        }
    }

    fn ev(after: Value) -> Event {
        Event {
            before: None,
            after: Some(after),
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
            checkpoint: None,
            size_bytes: 0,
            received_at_ms: 0,
        }
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
}

//! `ElasticsearchSink` — the `Sink` implementation.
//!
//! `send_batch`: for each event resolve the source PK (+ types), ensure the
//! target index exists once (with a generated mapping), derive `_id` + external
//! `version`, assemble one `_bulk` request, then map the per-item response —
//! `409 version_conflict` is success, other per-doc errors go to the DLQ.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use deltaforge_config::{ElasticsearchSinkCfg, EsAuth, EsVersionSource};
use deltaforge_core::{BatchResult, Event, Op, Sink, SinkError, SinkResult};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::EsSchemaResolver;
use super::TableColumns;
use super::bulk::{
    BulkAction, ItemOutcome, build_bulk_body, parse_bulk_response,
};
use super::client::{ElasticsearchClient, EsTransport};
use super::id::derive_id;
use super::index::render_index;
use super::mapping::build_mapping;
use super::version::derive_es_version;
use crate::clickhouse::types::ColDesc;

pub struct ElasticsearchSink {
    id: String,
    pipeline: String,
    required: bool,
    index_template: String,
    auto_create_index: bool,
    id_fields: Vec<String>,
    id_separator: String,
    version_source: EsVersionSource,
    transport: Arc<dyn EsTransport>,
    resolver: Option<EsSchemaResolver>,
    resolved: RwLock<HashMap<String, Arc<TableColumns>>>,
    ensured: RwLock<HashSet<String>>,
}

impl ElasticsearchSink {
    /// Source table key: `"{namespace}.{table}"` (PG schema if present else db) —
    /// the same qualification the schema registry uses.
    fn source_key(ev: &Event) -> String {
        let ns = ev
            .source
            .schema
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or(&ev.source.db);
        format!("{ns}.{}", ev.source.table)
    }

    /// True when the sink must know the source schema: for the PK (when no
    /// `id_fields`) or for the generated mapping (when auto-creating indices).
    fn needs_schema(&self) -> bool {
        self.id_fields.is_empty() || self.auto_create_index
    }

    /// Resolve (and cache) the source columns + PK for an event's table.
    fn schema_for(&self, ev: &Event) -> Result<Arc<TableColumns>, SinkError> {
        let key = Self::source_key(ev);
        if let Some(tc) = self.resolved.read().unwrap().get(&key) {
            return Ok(tc.clone());
        }
        let resolver = self.resolver.as_ref().ok_or_else(|| {
            SinkError::Other(anyhow::anyhow!(
                "elasticsearch sink has no schema resolver (source schema unavailable)"
            ))
        })?;
        let tc = resolver(&key).ok_or_else(|| {
            // Transient during startup (before the schema is loadable) — retry.
            SinkError::Other(anyhow::anyhow!(
                "no schema yet for source table '{key}'"
            ))
        })?;
        let tc = Arc::new(tc);
        self.resolved.write().unwrap().insert(key, tc.clone());
        Ok(tc)
    }

    /// Ensure the index exists (once), creating it with a generated mapping.
    async fn ensure_index(
        &self,
        index: &str,
        cols: &[ColDesc],
    ) -> Result<(), SinkError> {
        if self.ensured.read().unwrap().contains(index) {
            return Ok(());
        }
        let mapping = build_mapping(cols);
        self.transport.ensure_index(index, &mapping).await?;
        self.ensured.write().unwrap().insert(index.to_string());
        Ok(())
    }
}

#[async_trait]
impl Sink for ElasticsearchSink {
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
        let needs_schema = self.needs_schema();
        let mut actions: Vec<BulkAction> = Vec::with_capacity(events.len());
        let mut action_event_idx: Vec<usize> = Vec::with_capacity(events.len());
        let mut dlq: Vec<(usize, SinkError)> = Vec::new();

        for (i, ev) in events.iter().enumerate() {
            // Truncate carries no row — not representable as a per-doc op in v1.
            if ev.op == Op::Truncate {
                continue;
            }

            let tc = if needs_schema {
                // A missing/late schema is transient → replay the whole batch.
                self.schema_for(ev)?
            } else {
                Arc::new(TableColumns {
                    columns: vec![],
                    primary_key: vec![],
                })
            };

            let index = render_index(&self.index_template, ev);
            if self.auto_create_index {
                self.ensure_index(&index, &tc.columns).await?;
            }

            let id = match derive_id(
                ev,
                &self.id_fields,
                &tc.primary_key,
                &self.id_separator,
            ) {
                Ok(id) => id,
                Err(e) => {
                    dlq.push((i, e));
                    continue;
                }
            };

            let doc = if ev.op == Op::Delete {
                None
            } else {
                match ev.after.clone() {
                    Some(d) => Some(d),
                    None => {
                        dlq.push((
                            i,
                            SinkError::Serialization {
                                details:
                                    "elasticsearch: non-delete event has no `after` body"
                                        .into(),
                            },
                        ));
                        continue;
                    }
                }
            };

            let version = derive_es_version(ev, self.version_source.clone());
            actions.push(BulkAction {
                index,
                id,
                version,
                doc,
            });
            action_event_idx.push(i);
        }

        if actions.is_empty() {
            return Ok(BatchResult { dlq_failures: dlq });
        }

        let body = build_bulk_body(&actions);
        let start = std::time::Instant::now();
        let resp = self.transport.bulk(body).await?; // retryable → replay
        let outcomes = parse_bulk_response(&resp)?;

        let mut applied: u64 = 0;
        for (pos, outcome) in outcomes.into_iter().enumerate() {
            match outcome {
                ItemOutcome::Ok | ItemOutcome::Superseded => applied += 1,
                ItemOutcome::Failed(msg) => {
                    let orig =
                        action_event_idx.get(pos).copied().unwrap_or(pos);
                    dlq.push((
                        orig,
                        SinkError::Serialization {
                            details: msg.into(),
                        },
                    ));
                }
            }
        }

        metrics::histogram!(
            "deltaforge_sink_elasticsearch_bulk_seconds",
            "pipeline" => self.pipeline.clone(),
            "sink" => self.id.clone(),
        )
        .record(start.elapsed().as_secs_f64());
        metrics::counter!(
            "deltaforge_sink_elasticsearch_docs_total",
            "pipeline" => self.pipeline.clone(),
            "sink" => self.id.clone(),
        )
        .increment(applied);

        Ok(BatchResult { dlq_failures: dlq })
    }
}

/// Build an `ElasticsearchSink` from config. `resolver` supplies source-table
/// columns + PK (built by the runner).
pub fn build_elasticsearch_sink(
    cfg: &ElasticsearchSinkCfg,
    _cancel: CancellationToken,
    pipeline: &str,
    resolver: Option<EsSchemaResolver>,
) -> anyhow::Result<ElasticsearchSink> {
    // Expand ${ENV} in the URL and auth secrets, like the other sinks.
    let mut expanded = cfg.clone();
    expanded.url = shellexpand::env(&cfg.url)?.into_owned();
    expanded.auth = match &cfg.auth {
        Some(EsAuth::Basic { username, password }) => Some(EsAuth::Basic {
            username: shellexpand::env(username)?.into_owned(),
            password: shellexpand::env(password)?.into_owned(),
        }),
        Some(EsAuth::ApiKey { api_key }) => Some(EsAuth::ApiKey {
            api_key: shellexpand::env(api_key)?.into_owned(),
        }),
        other => other.clone(),
    };

    let needs_schema = cfg.id_fields.is_empty() || cfg.auto_create_index;
    if resolver.is_none() && needs_schema {
        warn!(
            sink = %cfg.id,
            "elasticsearch sink built without a schema resolver — projection \
             will fail until one is provided (set id_fields and auto_create_index: \
             false to run without a resolver)"
        );
    }

    let client = ElasticsearchClient::new(&expanded)?;
    Ok(ElasticsearchSink {
        id: cfg.id.clone(),
        pipeline: pipeline.to_string(),
        required: cfg.required.unwrap_or(true),
        index_template: cfg.index.clone(),
        auto_create_index: cfg.auto_create_index,
        id_fields: cfg.id_fields.clone(),
        id_separator: cfg.id_separator.clone(),
        version_source: cfg.version_source.clone(),
        transport: Arc::new(client),
        resolver,
        resolved: RwLock::new(HashMap::new()),
        ensured: RwLock::new(HashSet::new()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clickhouse::types::ColDesc;
    use crate::elasticsearch::test_support::mk_event;
    use serde_json::{Value, json};
    use std::sync::Mutex;

    /// Capturing fake transport.
    struct FakeTransport {
        ensure_calls: Mutex<Vec<(String, Value)>>,
        bulk_bodies: Mutex<Vec<Vec<u8>>>,
        response: Value,
        bulk_err: bool,
    }

    impl FakeTransport {
        fn new(response: Value) -> Self {
            Self {
                ensure_calls: Mutex::new(vec![]),
                bulk_bodies: Mutex::new(vec![]),
                response,
                bulk_err: false,
            }
        }
    }

    #[async_trait]
    impl EsTransport for FakeTransport {
        async fn bulk(&self, body: Vec<u8>) -> Result<Vec<u8>, SinkError> {
            self.bulk_bodies.lock().unwrap().push(body);
            if self.bulk_err {
                return Err(SinkError::Backpressure {
                    details: "down".into(),
                });
            }
            Ok(self.response.to_string().into_bytes())
        }
        async fn ensure_index(
            &self,
            index: &str,
            mapping: &Value,
        ) -> Result<(), SinkError> {
            self.ensure_calls
                .lock()
                .unwrap()
                .push((index.to_string(), mapping.clone()));
            Ok(())
        }
    }

    fn resolver() -> EsSchemaResolver {
        Arc::new(|_key: &str| {
            Some(TableColumns {
                columns: vec![
                    ColDesc {
                        name: "id".into(),
                        data_type: "bigint".into(),
                        full_type: "bigint".into(),
                        nullable: false,
                        unsigned: false,
                        precision: None,
                        scale: None,
                    },
                    ColDesc {
                        name: "amount".into(),
                        data_type: "decimal".into(),
                        full_type: "decimal(12,2)".into(),
                        nullable: true,
                        unsigned: false,
                        precision: None,
                        scale: Some(2),
                    },
                ],
                primary_key: vec!["id".into()],
            })
        })
    }

    fn sink_with(fake: Arc<FakeTransport>) -> ElasticsearchSink {
        ElasticsearchSink {
            id: "es".into(),
            pipeline: "p".into(),
            required: true,
            index_template: "cdc-{db}.{table}".into(),
            auto_create_index: true,
            id_fields: vec![],
            id_separator: "_".into(),
            version_source: EsVersionSource::TsMs,
            transport: fake,
            resolver: Some(resolver()),
            resolved: RwLock::new(HashMap::new()),
            ensured: RwLock::new(HashSet::new()),
        }
    }

    fn ev(op: Op, after: Value, before: Value, ts: i64) -> Event {
        mk_event(op, after, before, "orders", None, "customers", ts, None)
    }

    /// Count `_bulk` action metas by type in a recorded body.
    fn count_actions(body: &[u8]) -> (usize, usize) {
        let s = String::from_utf8(body.to_vec()).unwrap();
        let mut idx = 0;
        let mut del = 0;
        for line in s.lines() {
            let v: Value = serde_json::from_str(line).unwrap();
            if v.get("index").and_then(|a| a.get("_id")).is_some() {
                idx += 1;
            } else if v.get("delete").and_then(|a| a.get("_id")).is_some() {
                del += 1;
            }
        }
        (idx, del)
    }

    #[tokio::test]
    async fn builds_actions_and_ensures_index_once() {
        let fake = Arc::new(FakeTransport::new(json!({
            "errors": false,
            "items": [{"index": {"status": 200}}, {"index": {"status": 200}}, {"delete": {"status": 200}}]
        })));
        let sink = sink_with(fake.clone());
        let batch = vec![
            ev(
                Op::Create,
                json!({"id": 1, "amount": "10.00"}),
                json!(null),
                1,
            ),
            ev(
                Op::Update,
                json!({"id": 1, "amount": "20.50"}),
                json!(null),
                2,
            ),
            ev(Op::Delete, json!(null), json!({"id": 2}), 3),
        ];
        let res = sink.send_batch(&batch).await.unwrap();
        assert!(res.dlq_failures.is_empty(), "{:?}", res.dlq_failures);

        // ensure_index called exactly once, with a typed mapping (scaled_float).
        let calls = fake.ensure_calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "cdc-orders.customers");
        assert_eq!(calls[0].1["properties"]["amount"]["type"], "scaled_float");
        assert_eq!(calls[0].1["properties"]["id"]["type"], "long");

        // one bulk request: 2 index actions + 1 delete action.
        let bodies = fake.bulk_bodies.lock().unwrap();
        assert_eq!(bodies.len(), 1);
        assert_eq!(count_actions(&bodies[0]), (2, 1));
    }

    #[tokio::test]
    async fn version_conflict_is_success_and_mapper_error_dlqs() {
        let fake = Arc::new(FakeTransport::new(json!({
            "errors": true,
            "items": [
                {"index": {"status": 200}},
                {"index": {"status": 409, "error": {"type": "version_conflict_engine_exception"}}},
                {"index": {"status": 400, "error": {"type": "mapper_parsing_exception", "reason": "bad"}}}
            ]
        })));
        let sink = sink_with(fake);
        let batch = vec![
            ev(Op::Create, json!({"id": 1}), json!(null), 1),
            ev(Op::Update, json!({"id": 2}), json!(null), 2),
            ev(Op::Update, json!({"id": 3}), json!(null), 3),
        ];
        let res = sink.send_batch(&batch).await.unwrap();
        assert_eq!(res.dlq_failures.len(), 1, "only the 400 item fails");
        assert_eq!(res.dlq_failures[0].0, 2, "third event (index 2) failed");
    }

    #[tokio::test]
    async fn retryable_bulk_error_returns_err_for_replay() {
        let mut ft = FakeTransport::new(json!({"items": []}));
        ft.bulk_err = true;
        let fake = Arc::new(ft);
        let sink = sink_with(fake);
        let batch = vec![ev(Op::Create, json!({"id": 1}), json!(null), 1)];
        let err = sink.send_batch(&batch).await.unwrap_err();
        assert!(matches!(err, SinkError::Backpressure { .. }));
    }

    #[tokio::test]
    async fn bad_id_row_goes_to_dlq_without_failing_batch() {
        let fake = Arc::new(FakeTransport::new(json!({
            "errors": false,
            "items": [{"index": {"status": 200}}]
        })));
        let sink = sink_with(fake);
        // Second event's row is missing the `id` PK field → DLQ, first still sent.
        let batch = vec![
            ev(Op::Create, json!({"id": 1}), json!(null), 1),
            ev(Op::Create, json!({"amount": "1.00"}), json!(null), 2),
        ];
        let res = sink.send_batch(&batch).await.unwrap();
        assert_eq!(res.dlq_failures.len(), 1);
        assert_eq!(res.dlq_failures[0].0, 1);
    }
}

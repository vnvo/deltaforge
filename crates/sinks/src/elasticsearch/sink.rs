//! `ElasticsearchSink` — the `Sink` implementation.
//!
//! `send_batch`: for each event resolve the source PK (+ types) and the index
//! it renders to. Each index is claimed once per session before it is
//! written (see [`super::ownership`]): created by the sink with its marker
//! and fenced write alias, or proven to be the sink's and its fence rotated.
//! Every bulk action then targets the fenced write alias. Per item, `409
//! version_conflict` is success, a removed alias fails the whole batch
//! (`sink_destination_conflict`, class `fenced`, never dead-lettered), and
//! other per-document errors go to the DLQ.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use deltaforge_config::{ElasticsearchSinkCfg, EsVersionSource};
use deltaforge_core::ownership::{
    DestinationMarker, EncodingMarker, Expected, OwnershipClass,
    RelationMarker, ownership_refusal,
};
use deltaforge_core::{BatchResult, Event, Op, Sink, SinkError, SinkResult};
use deltaforge_core::{SourceInfo, SourceRelationIdentity};
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
use super::normalize::normalize_doc;
use super::ownership::{EsDestination, write_alias};
use super::version::derive_es_version;
use crate::ownership_guard::OwnershipGuard;

/// The marker's encoding layout.
const LAYOUT: &str = "elasticsearch.document.v1";

pub struct ElasticsearchSink {
    id: String,
    pipeline: String,
    required: bool,
    index_template: String,
    id_fields: Vec<String>,
    id_separator: String,
    version_source: EsVersionSource,
    transport: Arc<dyn EsTransport>,
    resolver: Option<EsSchemaResolver>,
    resolved: RwLock<HashMap<SourceRelationIdentity, Arc<TableColumns>>>,
    /// The indices this session verified it owns, and their fences.
    guard: OwnershipGuard,
    /// The cluster's UUID (read once per session, forgotten on a
    /// reconnect): part of each index's identity.
    cluster: Mutex<Option<String>>,
}

/// An index claimed within one batch: the alias its actions target and the
/// expectation it was claimed for.
struct Target {
    alias: String,
    expected: Expected,
}

impl Target {
    /// Whether `source` is the relation this index was claimed for.
    fn holds(&self, source: &SourceInfo, ev: &Event) -> bool {
        let r = &self.expected.relation;
        r.connector == source.connector
            && r.source == source.name
            && r.database == source.db
            && r.namespace == source.namespace()
            && r.table == source.table
            && self.expected.encoding.type_mapping == ev.type_mapping.as_str()
    }
}

impl ElasticsearchSink {
    /// The source table as the schema resolver names it: `namespace.table`
    /// within the pipeline's database.
    fn source_key(ev: &Event) -> String {
        ev.source.full_table_name()
    }

    /// Resolve (and cache) the source columns + PK for an event's table.
    fn schema_for(&self, ev: &Event) -> Result<Arc<TableColumns>, SinkError> {
        let relation = SourceRelationIdentity::of(&ev.source);
        if let Some(tc) = self.resolved.read().unwrap().get(&relation) {
            return Ok(tc.clone());
        }
        let key = Self::source_key(ev);
        let resolver = self.resolver.as_ref().ok_or_else(|| {
            SinkError::Other(anyhow::anyhow!(
                "elasticsearch sink has no schema resolver (source schema unavailable)"
            ))
        })?;
        let tc =
            resolver(&key).map_err(SinkError::Other)?.ok_or_else(|| {
                // Transient during startup (before the schema is loadable) — retry.
                SinkError::Other(anyhow::anyhow!(
                    "no schema yet for source table '{key}'"
                ))
            })?;
        let tc = Arc::new(tc);
        self.resolved.write().unwrap().insert(relation, tc.clone());
        Ok(tc)
    }

    /// The cluster's UUID, read once per session.
    async fn cluster(&self) -> Result<String, SinkError> {
        if let Some(c) = self.cluster.lock().unwrap().clone() {
            return Ok(c);
        }
        let v = self.transport.get_json("").await?;
        let c = v["cluster_uuid"].as_str().map(str::to_string).ok_or_else(
            || {
                SinkError::Io(std::io::Error::other(
                    "elasticsearch: the cluster reports no cluster_uuid",
                ))
            },
        )?;
        *self.cluster.lock().unwrap() = Some(c.clone());
        Ok(c)
    }

    /// What the sink expects `index` to be for `ev`.
    fn expected(&self, cluster: &str, index: &str, ev: &Event) -> Expected {
        Expected {
            owner: self.guard.owner().clone(),
            relation: RelationMarker::from(&SourceRelationIdentity::of(
                &ev.source,
            )),
            destination: DestinationMarker::elasticsearch(cluster, index),
            encoding: EncodingMarker {
                layout: LAYOUT.into(),
                type_mapping: ev.type_mapping.as_str().into(),
                mode: None,
                extensions_column: None,
            },
        }
    }

    /// The write alias for `ev` into `index`, claiming the index when this
    /// session has not verified it for exactly this expectation.
    async fn target(
        &self,
        cluster: &str,
        index: &str,
        ev: &Event,
        cols: &TableColumns,
    ) -> Result<Target, SinkError> {
        let expected = self.expected(cluster, index, ev);
        let claimed = self
            .guard
            .fence(&expected, || EsDestination {
                transport: self.transport.as_ref(),
                index,
                mapping: build_mapping(&cols.columns),
            })
            .await?;
        Ok(Target {
            alias: write_alias(&claimed.instance, &claimed.fence),
            expected,
        })
    }

    /// A transport failure may be a reconnect: every verification is
    /// claimed again before the next write.
    fn forget_on_transport_error(&self, e: &SinkError) {
        if matches!(
            e,
            SinkError::Connect { .. }
                | SinkError::Io(_)
                | SinkError::Backpressure { .. }
        ) {
            self.guard.invalidate_all();
            *self.cluster.lock().unwrap() = None;
        }
    }
}

#[async_trait]
impl Sink for ElasticsearchSink {
    /// An Elasticsearch document replaces the stored one whole.
    fn requires_complete_rows(&self) -> bool {
        true
    }

    /// Nothing is buffered after an acknowledged batch: every bulk request of an acknowledged batch was answered before `send_batch` returned, and a
    /// barrier is delivered only after every earlier batch was acknowledged.
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
        let mut actions: Vec<BulkAction> = Vec::with_capacity(events.len());
        let mut action_event_idx: Vec<usize> = Vec::with_capacity(events.len());
        let mut dlq: Vec<(usize, SinkError)> = Vec::new();

        let cluster = match self.cluster().await {
            Ok(c) => c,
            Err(e) => {
                self.forget_on_transport_error(&e);
                return Err(e);
            }
        };
        // Indices claimed for this batch, by name: the claim itself (and
        // its cache) is consulted once per index, not per event.
        let mut targets: HashMap<String, Target> = HashMap::new();
        let mut action_target: Vec<String> = Vec::with_capacity(events.len());

        for (i, ev) in events.iter().enumerate() {
            // Truncate carries no row — not representable as a per-doc op in v1.
            if ev.op == Op::Truncate {
                continue;
            }

            // A missing/late schema is transient → replay the whole batch.
            let tc = self.schema_for(ev)?;

            let index = render_index(&self.index_template, ev);
            let alias = match targets.get(&index) {
                Some(t) if t.holds(&ev.source, ev) => t.alias.clone(),
                _ => {
                    let t = match self.target(&cluster, &index, ev, &tc).await {
                        Ok(t) => t,
                        Err(e) => {
                            self.forget_on_transport_error(&e);
                            return Err(e);
                        }
                    };
                    let alias = t.alias.clone();
                    targets.insert(index.clone(), t);
                    alias
                }
            };

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
                match ev.after.as_ref() {
                    // The document from the row's canonical values, by type.
                    Some(d) => match normalize_doc(&tc.columns, d) {
                        Ok(doc) => Some(doc),
                        Err(e) => {
                            dlq.push((i, e));
                            continue;
                        }
                    },
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
                index: alias,
                id,
                version,
                doc,
            });
            action_event_idx.push(i);
            action_target.push(index);
        }

        if actions.is_empty() {
            return Ok(BatchResult { dlq_failures: dlq });
        }

        let body = build_bulk_body(&actions);
        let start = std::time::Instant::now();
        let resp = match self.transport.bulk(body).await {
            Ok(r) => r,
            Err(e) => {
                self.forget_on_transport_error(&e);
                return Err(e); // retryable → replay
            }
        };
        let outcomes = parse_bulk_response(&resp)?;

        // A removed write alias: this session no longer holds the fence.
        // The whole batch fails (never dead-lettered) and the checkpoint
        // stays; the next claim decides.
        if let Some(pos) =
            outcomes.iter().position(|o| *o == ItemOutcome::Fenced)
        {
            let mut refusal = None;
            for (o, index) in outcomes.iter().zip(&action_target) {
                if *o == ItemOutcome::Fenced
                    && let Some(t) = targets.get(index)
                {
                    self.guard.invalidate(&t.expected);
                    refusal.get_or_insert_with(|| {
                        ownership_refusal(
                            &self.id,
                            &t.expected.destination,
                            &t.expected.relation.identity(),
                            OwnershipClass::Fenced,
                            None,
                            None,
                        )
                    });
                }
            }
            return Err(refusal.unwrap_or_else(|| SinkError::Fatal {
                details: format!("elasticsearch: bulk item {pos} was fenced")
                    .into(),
            }));
        }

        let mut applied: u64 = 0;
        for (pos, outcome) in outcomes.into_iter().enumerate() {
            match outcome {
                ItemOutcome::Ok | ItemOutcome::Superseded => applied += 1,
                // Refused above, before anything is dead-lettered.
                ItemOutcome::Fenced => {}
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
    ownership: &crate::SinkOwnership,
    creds: &crate::ResolvedSinkCreds,
) -> anyhow::Result<ElasticsearchSink> {
    validate_elasticsearch_cfg(cfg)?;
    // Expand ${ENV} in the URL. Auth is resolved (references win over inline).
    let mut expanded = cfg.clone();
    expanded.url = shellexpand::env(&cfg.url)?.into_owned();
    let auth = super::client::resolve_es_auth(&cfg.auth, creds)?;

    if resolver.is_none() {
        warn!(
            sink = %cfg.id,
            "elasticsearch sink built without a schema resolver — index \
             creation and projection will fail until one is provided"
        );
    }

    let client = ElasticsearchClient::new(&expanded, auth)?;
    Ok(ElasticsearchSink {
        id: cfg.id.clone(),
        pipeline: pipeline.to_string(),
        required: cfg.required.unwrap_or(true),
        index_template: cfg.index.clone(),
        id_fields: cfg.id_fields.clone(),
        id_separator: cfg.id_separator.clone(),
        version_source: cfg.version_source.clone(),
        transport: Arc::new(client),
        resolver,
        resolved: RwLock::new(HashMap::new()),
        guard: ownership.guard(pipeline, &cfg.id),
        cluster: Mutex::new(None),
    })
}

/// The sink writes only to indices it created and owns: an index it did
/// not create can never be accepted, so `auto_create_index: false` is
/// refused.
pub fn validate_elasticsearch_cfg(
    cfg: &ElasticsearchSinkCfg,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        cfg.auto_create_index,
        "elasticsearch sink {}: auto_create_index: false is not supported: the \
         sink writes only to indices it creates (with its ownership marker \
         and fenced write alias); remove the setting",
        cfg.id
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clickhouse::types::ColDesc;
    use crate::elasticsearch::ownership::{ALIAS_PREFIX, AUTO_CREATE_SETTINGS};
    use crate::elasticsearch::test_support::mk_event;
    use serde_json::{Value, json};
    use std::collections::BTreeSet;

    /// One index of [`MiniEs`].
    #[derive(Clone, Default)]
    struct Index {
        meta: Value,
        aliases: BTreeSet<String>,
    }

    /// An in-memory Elasticsearch enforcing what fencing relies on: atomic
    /// creation, alias compare-and-swap (`must_exist`), `require_alias` for
    /// index actions and the auto-create rule for deletes.
    struct MiniEs {
        auto_create: Mutex<String>,
        settings_status: Mutex<u16>,
        indices: Mutex<HashMap<String, Index>>,
        /// Every remote call, as `METHOD path`.
        calls: Mutex<Vec<String>>,
        bulk_bodies: Mutex<Vec<Vec<u8>>>,
        /// Replaces the per-item answers of the next bulk request.
        items: Mutex<Option<Value>>,
        bulk_err: Mutex<bool>,
    }

    impl MiniEs {
        fn new() -> Arc<Self> {
            Arc::new(MiniEs {
                auto_create: Mutex::new("-df-w-*,+*".into()),
                settings_status: Mutex::new(200),
                indices: Mutex::new(HashMap::new()),
                calls: Mutex::new(vec![]),
                bulk_bodies: Mutex::new(vec![]),
                items: Mutex::new(None),
                bulk_err: Mutex::new(false),
            })
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }

        fn concrete(&self, name: &str) -> Option<String> {
            let ix = self.indices.lock().unwrap();
            if ix.contains_key(name) {
                return Some(name.into());
            }
            ix.iter()
                .find(|(_, i)| i.aliases.contains(name))
                .map(|(n, _)| n.clone())
        }

        /// Rotate the internal alias of `index` to another fence (another
        /// claimer, for example a clone of the state store).
        fn steal(&self, index: &str) {
            let mut ix = self.indices.lock().unwrap();
            let i = ix.get_mut(index).unwrap();
            let old: Vec<_> = i
                .aliases
                .iter()
                .filter(|a| a.starts_with(ALIAS_PREFIX))
                .cloned()
                .collect();
            let instance = i.meta["deltaforge"]["instance"].as_str().unwrap();
            for a in old {
                i.aliases.remove(&a);
            }
            i.aliases.insert(write_alias(
                instance,
                &deltaforge_core::ownership::random_token(),
            ));
        }
    }

    #[async_trait]
    impl EsTransport for MiniEs {
        async fn bulk(&self, body: Vec<u8>) -> Result<Vec<u8>, SinkError> {
            self.calls.lock().unwrap().push("POST _bulk".into());
            self.bulk_bodies.lock().unwrap().push(body.clone());
            if *self.bulk_err.lock().unwrap() {
                return Err(SinkError::Connect {
                    details: "down".into(),
                });
            }
            if let Some(items) = self.items.lock().unwrap().take() {
                return Ok(json!({"errors": true, "items": items})
                    .to_string()
                    .into_bytes());
            }
            let text = String::from_utf8(body).unwrap();
            let mut lines = text.lines();
            let mut items = vec![];
            while let Some(line) = lines.next() {
                let meta: Value = serde_json::from_str(line).unwrap();
                let (op, a) = meta.as_object().unwrap().iter().next().unwrap();
                if op == "index" {
                    lines.next();
                }
                let target = a["_index"].as_str().unwrap();
                let item = match self.concrete(target) {
                    // require_alias: the target must be an alias.
                    Some(c) if c != target => {
                        json!({op: {"_index": c, "status": 200}})
                    }
                    _ => {
                        let forbidden = crate::elasticsearch::ownership::rules_forbid_prefix(
                            &self.auto_create.lock().unwrap(),
                            ALIAS_PREFIX,
                        )
                        .is_ok();
                        if op == "delete" && !forbidden {
                            // Auto-created, as a real cluster would.
                            self.indices
                                .lock()
                                .unwrap()
                                .insert(target.into(), Index::default());
                            json!({op: {"_index": target, "status": 404, "result": "not_found"}})
                        } else {
                            json!({op: {"_index": target, "status": 404,
                                "error": {"type": "index_not_found_exception"}}})
                        }
                    }
                };
                items.push(item);
            }
            Ok(json!({"errors": false, "items": items})
                .to_string()
                .into_bytes())
        }

        async fn send_json(
            &self,
            method: reqwest::Method,
            path: &str,
            body: Option<&Value>,
        ) -> Result<(u16, Value), SinkError> {
            // Lets concurrent claims interleave between their requests.
            tokio::task::yield_now().await;
            self.calls.lock().unwrap().push(format!("{method} {path}"));
            if path == AUTO_CREATE_SETTINGS {
                let status = *self.settings_status.lock().unwrap();
                return Ok((
                    status,
                    json!({"persistent": {"action": {"auto_create_index": *self.auto_create.lock().unwrap()}}}),
                ));
            }
            if method == reqwest::Method::POST && path == "_aliases" {
                let actions = body.unwrap()["actions"].as_array().unwrap();
                let mut ix = self.indices.lock().unwrap();
                let remove = &actions[0]["remove"];
                let add = &actions[1]["add"];
                let index =
                    ix.get_mut(remove["index"].as_str().unwrap()).unwrap();
                let old = remove["alias"].as_str().unwrap();
                // Like the cluster: a missing alias fails the request only
                // with `must_exist`.
                if !index.aliases.contains(old) && remove["must_exist"] == true
                {
                    return Ok((
                        404,
                        json!({"error": {"type": "aliases_not_found_exception"}}),
                    ));
                }
                index.aliases.remove(old);
                index.aliases.insert(add["alias"].as_str().unwrap().into());
                return Ok((200, json!({"acknowledged": true})));
            }
            let name = path.split('?').next().unwrap();
            if method == reqwest::Method::PUT {
                let mut ix = self.indices.lock().unwrap();
                if ix.contains_key(name) {
                    return Ok((
                        400,
                        json!({"error": {"type": "resource_already_exists_exception"}}),
                    ));
                }
                let b = body.unwrap();
                ix.insert(
                    name.into(),
                    Index {
                        meta: b["mappings"]["_meta"].clone(),
                        aliases: b["aliases"]
                            .as_object()
                            .unwrap()
                            .keys()
                            .cloned()
                            .collect(),
                    },
                );
                return Ok((200, json!({"acknowledged": true})));
            }
            // GET <name>: keyed by the concrete index.
            match self.concrete(name) {
                None => Ok((
                    404,
                    json!({"error": {"type": "index_not_found_exception"}}),
                )),
                Some(c) => {
                    let i = self.indices.lock().unwrap()[&c].clone();
                    let aliases: serde_json::Map<String, Value> = i
                        .aliases
                        .iter()
                        .map(|a| (a.clone(), json!({})))
                        .collect();
                    Ok((
                        200,
                        json!({ c: {"aliases": aliases, "mappings": {"_meta": i.meta},
                            "settings": {"index": {"uuid": "u"}}}}),
                    ))
                }
            }
        }

        async fn get_json(&self, path: &str) -> Result<Value, SinkError> {
            self.calls.lock().unwrap().push(format!("GET {path}"));
            assert!(path.is_empty(), "only the cluster root is read: {path}");
            Ok(json!({"cluster_uuid": "c1"}))
        }
    }

    fn resolver() -> EsSchemaResolver {
        Arc::new(|_key: &str| Ok(fixture_columns()))
    }

    fn fixture_columns() -> Option<TableColumns> {
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
    }

    fn sink_over(
        es: Arc<MiniEs>,
        ownership: &crate::SinkOwnership,
    ) -> ElasticsearchSink {
        ElasticsearchSink {
            id: "es".into(),
            pipeline: "p".into(),
            required: true,
            index_template: "cdc-{db}.{table}".into(),
            id_fields: vec![],
            id_separator: "_".into(),
            version_source: EsVersionSource::TsMs,
            transport: es,
            resolver: Some(resolver()),
            resolved: RwLock::new(HashMap::new()),
            guard: ownership.guard("p", "es"),
            cluster: Mutex::new(None),
        }
    }

    fn sink_with(es: Arc<MiniEs>) -> ElasticsearchSink {
        sink_over(es, &crate::SinkOwnership::ephemeral())
    }

    fn ev(op: Op, after: Value, before: Value, ts: i64) -> Event {
        mk_event(op, after, before, "orders", None, "customers", ts, None)
    }

    fn class(e: &SinkError) -> Option<String> {
        e.draft()
            .and_then(|d| d.discriminator.get("class").cloned())
    }

    /// The `_index` of every action in a recorded bulk body.
    fn targets(body: &[u8]) -> Vec<String> {
        String::from_utf8(body.to_vec())
            .unwrap()
            .lines()
            .filter_map(|l| {
                let v: Value = serde_json::from_str(l).unwrap();
                ["index", "delete"]
                    .iter()
                    .find_map(|op| v[op]["_index"].as_str().map(str::to_string))
            })
            .collect()
    }

    #[tokio::test]
    async fn creates_the_index_with_its_marker_and_writes_through_the_alias() {
        let es = MiniEs::new();
        let sink = sink_with(es.clone());
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

        let ix = es.indices.lock().unwrap()["cdc-orders.customers"].clone();
        let marker = &ix.meta["deltaforge"];
        assert_eq!(marker["format"], "deltaforge.destination-owner");
        assert_eq!(marker["version"], 1);
        assert_eq!(marker["owner"]["sink"], "es");
        assert_eq!(marker["relation"]["table"], "customers");
        assert_eq!(marker["destination"]["cluster"], "c1");
        assert!(marker.get("fence").is_none(), "no fence in the marker");
        let alias = ix.aliases.iter().next().unwrap().clone();
        assert!(alias.starts_with("df-w-"));
        // Every action (deletes too) targets the fenced alias.
        let bodies = es.bulk_bodies.lock().unwrap();
        assert_eq!(targets(&bodies[0]), vec![alias.clone(); 3]);
    }

    #[tokio::test]
    async fn the_steady_state_makes_no_ownership_request() {
        let es = MiniEs::new();
        let sink = sink_with(es.clone());
        for ts in 1..=20 {
            sink.send_batch(&[ev(
                Op::Create,
                json!({"id": ts}),
                json!(null),
                ts,
            )])
            .await
            .unwrap();
        }
        let calls = es.calls();
        let bulks = calls.iter().filter(|c| *c == "POST _bulk").count();
        assert_eq!(bulks, 20);
        assert_eq!(calls.len() - bulks, 4, "one claim only: {calls:?}");
    }

    #[tokio::test]
    async fn an_index_holds_one_relation_across_restarts() {
        let es = MiniEs::new();
        let ownership = crate::SinkOwnership::ephemeral();
        let row = |schema: &str| {
            mk_event(
                Op::Create,
                json!({"id": 1, "amount": "1.00"}),
                json!(null),
                "shop",
                Some(schema),
                "orders",
                1,
                None,
            )
        };
        let mut sink = sink_over(es.clone(), &ownership);
        sink.index_template = "cdc-{table}".into();
        sink.send_batch(&[row("a")]).await.unwrap();

        let mut restarted = sink_over(es.clone(), &ownership);
        restarted.index_template = "cdc-{table}".into();
        let before = es.bulk_bodies.lock().unwrap().len();
        let e = restarted.send_batch(&[row("b")]).await.unwrap_err();
        assert_eq!(class(&e).as_deref(), Some("bound_to_another_relation"));
        let text = format!("{e:?}");
        assert!(text.contains("shop/b/orders"), "{text}");
        assert!(!text.contains("1.00"), "no row data: {text}");
        assert_eq!(
            es.bulk_bodies.lock().unwrap().len(),
            before,
            "nothing delivered"
        );
        // The bound relation still writes after the restart.
        restarted.send_batch(&[row("a")]).await.unwrap();
    }

    #[tokio::test]
    async fn a_fenced_batch_fails_whole_and_never_reaches_the_dlq() {
        let es = MiniEs::new();
        let ownership = crate::SinkOwnership::ephemeral();
        let sink = sink_over(es.clone(), &ownership);
        sink.send_batch(&[ev(Op::Create, json!({"id": 1}), json!(null), 1)])
            .await
            .unwrap();
        // Another claimer (a clone of the store) rotated the fence.
        es.steal("cdc-orders.customers");
        let batch = vec![
            ev(Op::Create, json!({"id": 2}), json!(null), 2),
            ev(Op::Delete, json!(null), json!({"id": 1}), 3),
        ];
        let e = sink.send_batch(&batch).await.unwrap_err();
        assert_eq!(class(&e).as_deref(), Some("fenced"), "{e:?}");
        // The next attempt claims again and is refused: the store's fence is
        // not the destination's.
        let e = sink.send_batch(&batch).await.unwrap_err();
        assert_eq!(class(&e).as_deref(), Some("fenced"));
        assert!(
            !es.indices
                .lock()
                .unwrap()
                .keys()
                .any(|k| k.starts_with("df-w-")),
            "no index was auto-created"
        );
    }

    /// Two copies of one state store claiming at once: whichever loses
    /// the alias compare-and-swap is refused, and at most one of them can
    /// write afterwards.
    #[tokio::test]
    async fn concurrently_claiming_copies_never_both_write() {
        for _ in 0..20 {
            let es = MiniEs::new();
            let store = Arc::new(
                deltaforge_core::ownership::MemoryOwnershipStore::default(),
            );
            let original = crate::SinkOwnership {
                deployment: deltaforge_core::ownership::random_token(),
                store: store.clone(),
            };
            sink_over(es.clone(), &original)
                .send_batch(&[ev(Op::Create, json!({"id": 1}), json!(null), 1)])
                .await
                .unwrap();
            let copy = crate::SinkOwnership {
                deployment: original.deployment.clone(),
                store: Arc::new(store.copy()),
            };
            let (a, b) = (
                sink_over(es.clone(), &original),
                sink_over(es.clone(), &copy),
            );
            let batch = [ev(Op::Create, json!({"id": 2}), json!(null), 2)];
            let _ = tokio::join!(a.send_batch(&batch), b.send_batch(&batch));
            let (ra, rb) =
                tokio::join!(a.send_batch(&batch), b.send_batch(&batch));
            assert!(
                !(ra.is_ok() && rb.is_ok()),
                "both copies kept writing; aliases: {:?}",
                es.indices.lock().unwrap()["cdc-orders.customers"].aliases
            );
        }
    }

    #[tokio::test]
    async fn unenforced_or_unreadable_delete_fencing_is_refused() {
        for (rule, status) in [("+*", 200), ("true", 200), ("-df-w-*,+*", 403)]
        {
            let es = MiniEs::new();
            *es.auto_create.lock().unwrap() = rule.into();
            *es.settings_status.lock().unwrap() = status;
            let e = sink_with(es.clone())
                .send_batch(&[ev(Op::Create, json!({"id": 1}), json!(null), 1)])
                .await
                .unwrap_err();
            assert_eq!(
                class(&e).as_deref(),
                Some("fencing_unenforced"),
                "{rule} {status}"
            );
            assert!(es.indices.lock().unwrap().is_empty(), "nothing created");
        }
    }

    #[tokio::test]
    async fn an_alias_in_place_of_an_index_is_refused() {
        let es = MiniEs::new();
        let mut ix = Index::default();
        ix.aliases.insert("cdc-orders.customers".into());
        es.indices.lock().unwrap().insert("real-index".into(), ix);
        let e = sink_with(es)
            .send_batch(&[ev(Op::Create, json!({"id": 1}), json!(null), 1)])
            .await
            .unwrap_err();
        assert_eq!(class(&e).as_deref(), Some("alias_not_supported"));
    }

    #[tokio::test]
    async fn an_unmarked_or_foreign_index_is_refused() {
        let es = MiniEs::new();
        es.indices
            .lock()
            .unwrap()
            .insert("cdc-orders.customers".into(), Index::default());
        let e = sink_with(es.clone())
            .send_batch(&[ev(Op::Create, json!({"id": 1}), json!(null), 1)])
            .await
            .unwrap_err();
        assert_eq!(class(&e).as_deref(), Some("marker_missing"));
        // Created by another deployment.
        es.indices.lock().unwrap().clear();
        sink_with(es.clone())
            .send_batch(&[ev(Op::Create, json!({"id": 1}), json!(null), 1)])
            .await
            .unwrap();
        let e = sink_with(es.clone())
            .send_batch(&[ev(Op::Create, json!({"id": 1}), json!(null), 1)])
            .await
            .unwrap_err();
        assert_eq!(class(&e).as_deref(), Some("owned_by_other"));
        assert!(!format!("{e:?}").contains(r#""deployment""#));
    }

    #[tokio::test]
    async fn zero_or_several_internal_aliases_are_refused() {
        let es = MiniEs::new();
        let ownership = crate::SinkOwnership::ephemeral();
        sink_over(es.clone(), &ownership)
            .send_batch(&[ev(Op::Create, json!({"id": 1}), json!(null), 1)])
            .await
            .unwrap();
        let name = "cdc-orders.customers";
        let alias = es.indices.lock().unwrap()[name]
            .aliases
            .iter()
            .next()
            .unwrap()
            .clone();
        let extra = write_alias(
            es.indices.lock().unwrap()[name].meta["deltaforge"]["instance"]
                .as_str()
                .unwrap(),
            &deltaforge_core::ownership::random_token(),
        );
        es.indices
            .lock()
            .unwrap()
            .get_mut(name)
            .unwrap()
            .aliases
            .insert(extra);
        let e = sink_over(es.clone(), &ownership)
            .send_batch(&[ev(Op::Create, json!({"id": 1}), json!(null), 1)])
            .await
            .unwrap_err();
        assert_eq!(class(&e).as_deref(), Some("fence_malformed"));
        es.indices
            .lock()
            .unwrap()
            .get_mut(name)
            .unwrap()
            .aliases
            .clear();
        let e = sink_over(es.clone(), &ownership)
            .send_batch(&[ev(Op::Create, json!({"id": 1}), json!(null), 1)])
            .await
            .unwrap_err();
        assert_eq!(class(&e).as_deref(), Some("fence_malformed"));
        let _ = alias;
    }

    #[tokio::test]
    async fn user_aliases_and_metadata_survive_claims() {
        let es = MiniEs::new();
        let ownership = crate::SinkOwnership::ephemeral();
        sink_over(es.clone(), &ownership)
            .send_batch(&[ev(Op::Create, json!({"id": 1}), json!(null), 1)])
            .await
            .unwrap();
        let name = "cdc-orders.customers";
        {
            let mut ix = es.indices.lock().unwrap();
            let i = ix.get_mut(name).unwrap();
            i.aliases.insert("orders-read".into());
            i.meta["user_tool"] = json!({"owner": "team-x"});
        }
        for _ in 0..3 {
            sink_over(es.clone(), &ownership)
                .send_batch(&[ev(Op::Create, json!({"id": 1}), json!(null), 1)])
                .await
                .unwrap();
        }
        let i = es.indices.lock().unwrap()[name].clone();
        assert!(i.aliases.contains("orders-read"));
        assert_eq!(i.meta["user_tool"]["owner"], "team-x");
        assert_eq!(
            i.aliases.iter().filter(|a| a.starts_with("df-w-")).count(),
            1
        );
    }

    #[tokio::test]
    async fn a_deleted_index_is_neither_recreated_nor_auto_created() {
        let es = MiniEs::new();
        let ownership = crate::SinkOwnership::ephemeral();
        let sink = sink_over(es.clone(), &ownership);
        sink.send_batch(&[ev(Op::Create, json!({"id": 1}), json!(null), 1)])
            .await
            .unwrap();
        es.indices.lock().unwrap().clear();
        // The verified session writes through its alias: refused, nothing
        // auto-created.
        let e = sink
            .send_batch(&[ev(Op::Delete, json!(null), json!({"id": 1}), 2)])
            .await
            .unwrap_err();
        assert_eq!(class(&e).as_deref(), Some("fenced"));
        assert!(es.indices.lock().unwrap().is_empty());
        // A restart claims: the destination it created is gone.
        let e = sink_over(es.clone(), &ownership)
            .send_batch(&[ev(Op::Create, json!({"id": 1}), json!(null), 3)])
            .await
            .unwrap_err();
        assert_eq!(class(&e).as_deref(), Some("destination_missing"));
        assert!(es.indices.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_transport_failure_reclaims_before_the_next_write() {
        let es = MiniEs::new();
        let sink = sink_with(es.clone());
        sink.send_batch(&[ev(Op::Create, json!({"id": 1}), json!(null), 1)])
            .await
            .unwrap();
        *es.bulk_err.lock().unwrap() = true;
        let e = sink
            .send_batch(&[ev(Op::Create, json!({"id": 2}), json!(null), 2)])
            .await
            .unwrap_err();
        assert!(matches!(e, SinkError::Connect { .. }));
        *es.bulk_err.lock().unwrap() = false;
        let n = es.calls().len();
        sink.send_batch(&[ev(Op::Create, json!({"id": 2}), json!(null), 3)])
            .await
            .unwrap();
        let calls = es.calls()[n..].to_vec();
        assert!(
            calls.iter().any(|c| c == "POST _aliases"),
            "rotated: {calls:?}"
        );
    }

    #[tokio::test]
    async fn version_conflict_is_success_and_mapper_error_dlqs() {
        let es = MiniEs::new();
        let sink = sink_with(es.clone());
        let batch = vec![
            ev(Op::Create, json!({"id": 1}), json!(null), 1),
            ev(Op::Update, json!({"id": 2}), json!(null), 2),
            ev(Op::Update, json!({"id": 3}), json!(null), 3),
        ];
        *es.items.lock().unwrap() = Some(json!([
            {"index": {"status": 200}},
            {"index": {"status": 409, "error": {"type": "version_conflict_engine_exception"}}},
            {"index": {"status": 400, "error": {"type": "mapper_parsing_exception", "reason": "bad"}}}
        ]));
        let res = sink.send_batch(&batch).await.unwrap();
        assert_eq!(res.dlq_failures.len(), 1, "only the 400 item fails");
        assert_eq!(res.dlq_failures[0].0, 2, "third event (index 2) failed");
    }

    #[tokio::test]
    async fn bad_id_row_goes_to_dlq_without_failing_batch() {
        let sink = sink_with(MiniEs::new());
        // Second event's row is missing the `id` PK field → DLQ, first still sent.
        let batch = vec![
            ev(Op::Create, json!({"id": 1}), json!(null), 1),
            ev(Op::Create, json!({"amount": "1.00"}), json!(null), 2),
        ];
        let res = sink.send_batch(&batch).await.unwrap();
        assert_eq!(res.dlq_failures.len(), 1);
        assert_eq!(res.dlq_failures[0].0, 1);
    }

    #[test]
    fn auto_create_index_false_is_rejected_at_configuration() {
        let mut cfg: ElasticsearchSinkCfg = serde_json::from_value(json!({
            "id": "es", "url": "http://es:9200", "index": "t"
        }))
        .unwrap();
        assert!(validate_elasticsearch_cfg(&cfg).is_ok());
        cfg.auto_create_index = false;
        let e = validate_elasticsearch_cfg(&cfg).unwrap_err().to_string();
        assert!(
            e.contains("auto_create_index: false is not supported"),
            "{e}"
        );
    }
}

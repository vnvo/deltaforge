//! The retained-state binding (`docs/design/retained-state-compatibility.md`).
//!
//! A pipeline's durable state outlives its registration and is adopted by
//! the next start with the same source id. The binding is the canonical
//! form of every spec field that state depends on, recorded once and
//! compared on every later start and PATCH: a difference refuses.
//!
//! The comparison is an allowlist. Every field is recorded (compared)
//! unless it is classified operational, credential or a fenced endpoint
//! component; an unclassified field is compared, so it refuses until it is
//! classified. Endpoints, query parameters, HTTP headers and Kafka
//! `client_conf` keys are classified by built-in tables only: credential
//! components are removed, destination and delivery components are kept,
//! and an unclassified name is rejected. A value supplied through a secret
//! is accepted only where its name is a built-in credential; anything else
//! a secret supplies could select a destination, so it is rejected.
//!
//! Order is kept everywhere except where a field is a map (sorted by key):
//! no list is sorted, because no list here is proven to be a set. A
//! reordered list therefore refuses, which errs on the safe side.
//!
//! Credential values never enter the binding, a difference or a rejection.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::{PipelineSpec, SinkCfg, SourceCfg};

/// The binding format this release writes and reads.
pub const FORMAT_VERSION: u32 = 1;

/// The recorded binding of one source id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateBinding {
    pub format_version: u32,
    pub source_id: String,
    pub pipeline: String,
    pub tenant: String,
    pub engine: String,
    /// The recorded fields, canonical (sorted maps, credentials removed).
    pub canonical: Value,
    /// SHA-256 over the canonical serialization of every field above.
    pub digest: String,
}

/// Values a spec takes from secrets that the binding needs a component of
/// (never a credential component): the PostgreSQL DSN's database name, a
/// Redis URI's host and database. Resolved by the caller.
#[derive(Debug, Clone, Default)]
pub struct ResolvedEndpoints {
    /// The resolved source DSN, when the spec gives it through a secret.
    pub source_dsn: Option<String>,
    /// Resolved sink URIs by sink id, when given through a secret.
    pub sink_uris: BTreeMap<String, String>,
}

/// A spec field or name the binding cannot classify.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejection {
    pub field: String,
    pub reason: String,
}

impl std::fmt::Display for Rejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.field, self.reason)
    }
}

/// One recorded field that differs: its stored and new canonical values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Difference {
    pub field: String,
    pub stored: String,
    pub new: String,
}

impl std::fmt::Display for Difference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} (recorded {}, now {})",
            self.field, self.stored, self.new
        )
    }
}

/// How a spec field is treated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    /// Compared.
    Recorded,
    /// Ignored: changes nothing the retained state depends on.
    Operational,
    /// Removed: authenticates; never stored or shown.
    Credential,
    /// Removed: a source or sink endpoint whose fence establishes
    /// compatibility (see the design's "Fences").
    Fenced,
    /// Canonicalized by its own handler.
    Special(Special),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Special {
    KafkaBrokers,
    KafkaClientConf,
    KafkaSecretRefs,
    RedisUri,
    NatsUrl,
    HttpUrl,
    HttpHeaders,
    HttpSecretRefs,
    UrlWithoutQuery,
    ClickHouseUrl,
}

use Class::{Credential, Fenced, Operational, Recorded};

/// The classification of every spec field, by path. `*` matches one
/// segment; the longest matching pattern wins. Paths: `source.<engine>.*`,
/// `sinks.<type>.*` (sink ids are map keys of the canonical form, not path
/// segments), `spec.*` (the rest of the spec).
const RULES: &[(&str, Class)] = &[
    // Source.
    ("source.*.dsn", Fenced),
    ("source.*.dsn_secret", Credential),
    ("source.*.credentials", Credential),
    ("source.*.rotation", Credential),
    ("source.*.tables", Recorded),
    ("source.*.table_options", Recorded),
    ("source.*.outbox", Recorded),
    ("source.*.on_schema_drift", Recorded),
    ("source.*.type_mapping", Recorded),
    ("source.*.snapshot", Operational),
    ("source.postgres.publication", Recorded),
    ("source.postgres.slot", Recorded),
    ("source.postgres.database", Recorded),
    ("source.postgres.start_position", Operational),
    ("source.postgres.max_message_bytes", Operational),
    ("source.mysql.proofs", Operational),
    // The rest of the spec.
    ("spec.sharding", Recorded),
    ("spec.processors", Recorded),
    ("spec.commit_policy", Recorded),
    ("spec.connection_policy", Operational),
    ("spec.batch", Operational),
    ("spec.sink_batch_deadline_secs", Operational),
    ("spec.schema_sensing", Operational),
    ("spec.schema_sensing.output.emit_events", Recorded),
    ("spec.journal", Operational),
    ("spec.journal.enabled", Recorded),
    ("spec.secrets", Credential),
    ("spec.metrics", Operational),
    // Every sink.
    ("sinks.*.type", Recorded),
    ("sinks.*.required", Recorded),
    ("sinks.*.filter", Recorded),
    ("sinks.*.key", Recorded),
    ("sinks.*.envelope", Recorded),
    ("sinks.*.encoding", Recorded),
    (
        "sinks.*.encoding.schema_registry_url",
        Class::Special(Special::UrlWithoutQuery),
    ),
    ("sinks.*.encoding.username", Credential),
    ("sinks.*.encoding.password", Credential),
    ("sinks.*.encoding.username_ref", Credential),
    ("sinks.*.encoding.password_ref", Credential),
    ("sinks.*.json_wide_integers", Recorded),
    ("sinks.*.send_timeout_secs", Operational),
    ("sinks.*.batch_timeout_secs", Operational),
    ("sinks.*.connect_timeout_secs", Operational),
    ("sinks.*.tls", Operational),
    // Kafka.
    ("sinks.kafka.brokers", Class::Special(Special::KafkaBrokers)),
    ("sinks.kafka.topic", Recorded),
    ("sinks.kafka.exactly_once", Recorded),
    (
        "sinks.kafka.client_conf",
        Class::Special(Special::KafkaClientConf),
    ),
    (
        "sinks.kafka.secret_refs",
        Class::Special(Special::KafkaSecretRefs),
    ),
    ("sinks.kafka.encode_workers", Operational),
    // Redis.
    ("sinks.redis.uri", Class::Special(Special::RedisUri)),
    ("sinks.redis.uri_secret", Credential),
    ("sinks.redis.credentials", Credential),
    ("sinks.redis.stream", Recorded),
    // NATS.
    ("sinks.nats.url", Class::Special(Special::NatsUrl)),
    ("sinks.nats.subject", Recorded),
    ("sinks.nats.stream", Recorded),
    ("sinks.nats.credentials_file", Credential),
    ("sinks.nats.username", Credential),
    ("sinks.nats.password", Credential),
    ("sinks.nats.token", Credential),
    ("sinks.nats.username_ref", Credential),
    ("sinks.nats.password_ref", Credential),
    ("sinks.nats.token_ref", Credential),
    // HTTP.
    ("sinks.http.url", Class::Special(Special::HttpUrl)),
    ("sinks.http.method", Recorded),
    ("sinks.http.headers", Class::Special(Special::HttpHeaders)),
    (
        "sinks.http.secret_refs",
        Class::Special(Special::HttpSecretRefs),
    ),
    ("sinks.http.batch_mode", Recorded),
    // S3.
    ("sinks.s3.bucket", Recorded),
    ("sinks.s3.prefix", Recorded),
    ("sinks.s3.region", Recorded),
    (
        "sinks.s3.endpoint",
        Class::Special(Special::UrlWithoutQuery),
    ),
    ("sinks.s3.local", Recorded),
    ("sinks.s3.format", Recorded),
    ("sinks.s3.compression", Recorded),
    ("sinks.s3.durability", Recorded),
    ("sinks.s3.access_key_id", Credential),
    ("sinks.s3.secret_access_key", Credential),
    ("sinks.s3.session_token", Credential),
    ("sinks.s3.access_key_id_ref", Credential),
    ("sinks.s3.secret_access_key_ref", Credential),
    ("sinks.s3.session_token_ref", Credential),
    ("sinks.s3.virtual_hosted_style", Operational),
    ("sinks.s3.file_roll", Operational),
    // ClickHouse.
    (
        "sinks.clickhouse.url",
        Class::Special(Special::ClickHouseUrl),
    ),
    ("sinks.clickhouse.database", Recorded),
    ("sinks.clickhouse.table", Recorded),
    ("sinks.clickhouse.mode", Recorded),
    ("sinks.clickhouse.version_source", Recorded),
    ("sinks.clickhouse.auto_create", Recorded),
    ("sinks.clickhouse.extensions_column", Recorded),
    ("sinks.clickhouse.user", Credential),
    ("sinks.clickhouse.password", Credential),
    ("sinks.clickhouse.user_ref", Credential),
    ("sinks.clickhouse.password_ref", Credential),
    // Elasticsearch.
    (
        "sinks.elasticsearch.url",
        Class::Special(Special::UrlWithoutQuery),
    ),
    ("sinks.elasticsearch.index", Recorded),
    ("sinks.elasticsearch.auto_create_index", Recorded),
    ("sinks.elasticsearch.id_fields", Recorded),
    ("sinks.elasticsearch.id_separator", Recorded),
    ("sinks.elasticsearch.version_source", Recorded),
    ("sinks.elasticsearch.auth", Credential),
];

/// The class of `path`, and whether a rule names it (an unnamed path is
/// compared).
fn class_of(path: &str) -> (Class, bool) {
    let segs: Vec<&str> = path.split('.').collect();
    let mut best: Option<(usize, Class)> = None;
    for (pattern, class) in RULES {
        let p: Vec<&str> = pattern.split('.').collect();
        if p.len() > segs.len() {
            continue;
        }
        let matches = p.iter().zip(&segs).all(|(a, b)| *a == "*" || a == b);
        if matches && best.is_none_or(|(n, _)| p.len() > n) {
            best = Some((p.len(), *class));
        }
    }
    match best {
        Some((_, c)) => (c, true),
        None => (Recorded, false),
    }
}

/// Whether a rule records a field inside `path` (which itself is ignored).
fn has_recorded_within(path: &str) -> bool {
    let segs: Vec<&str> = path.split('.').collect();
    RULES.iter().any(|(pattern, class)| {
        let p: Vec<&str> = pattern.split('.').collect();
        p.len() > segs.len()
            && !matches!(class, Operational | Credential | Fenced)
            && p.iter().zip(&segs).all(|(a, b)| *a == "*" || a == b)
    })
}

// ── Built-in name classifications ──────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NameClass {
    Credential,
    Destination,
    Delivery,
    Operational,
}

/// librdkafka producer keys, classified explicitly. Any other key is
/// rejected.
fn kafka_key(key: &str) -> Option<NameClass> {
    use NameClass::*;
    Some(match key {
        "sasl.username"
        | "sasl.password"
        | "sasl.mechanism"
        | "sasl.mechanisms"
        | "sasl.oauthbearer.client.id"
        | "sasl.oauthbearer.client.secret"
        | "sasl.oauthbearer.config"
        | "ssl.key.password"
        | "ssl.key.pem"
        | "ssl.keystore.password"
        | "ssl.keystore.location" => Credential,
        "bootstrap.servers" | "metadata.broker.list" => Destination,
        "acks"
        | "request.required.acks"
        | "enable.idempotence"
        | "enable.gapless.guarantee"
        | "transactional.id"
        | "compression.type"
        | "compression.codec"
        | "compression.level"
        | "partitioner"
        | "message.max.bytes"
        | "max.in.flight.requests.per.connection"
        | "max.in.flight" => Delivery,
        "client.id"
        | "linger.ms"
        | "queue.buffering.max.ms"
        | "queue.buffering.max.messages"
        | "queue.buffering.max.kbytes"
        | "batch.num.messages"
        | "batch.size"
        | "socket.timeout.ms"
        | "socket.keepalive.enable"
        | "request.timeout.ms"
        | "message.timeout.ms"
        | "delivery.timeout.ms"
        | "transaction.timeout.ms"
        | "retries"
        | "message.send.max.retries"
        | "retry.backoff.ms"
        | "retry.backoff.max.ms"
        | "reconnect.backoff.ms"
        | "reconnect.backoff.max.ms"
        | "statistics.interval.ms"
        | "metadata.max.age.ms"
        | "topic.metadata.refresh.interval.ms"
        | "connections.max.idle.ms"
        | "security.protocol"
        | "ssl.ca.location"
        | "ssl.certificate.location"
        | "ssl.key.location"
        | "ssl.endpoint.identification.algorithm"
        | "enable.ssl.certificate.verification"
        | "debug"
        | "log_level" => Operational,
        _ => return None,
    })
}

/// HTTP header names (lowercased), classified explicitly.
fn http_header(name: &str) -> Option<NameClass> {
    use NameClass::*;
    Some(match name {
        "authorization" | "proxy-authorization" | "cookie" => Credential,
        "content-type" | "content-encoding" => Delivery,
        _ => return None,
    })
}

/// Redis URI query parameters.
fn redis_param(name: &str) -> Option<NameClass> {
    use NameClass::*;
    Some(match name {
        "db" => Destination,
        "protocol" => Delivery,
        _ => return None,
    })
}

/// ClickHouse URL query parameters (`None`: rejected).
fn clickhouse_param(name: &str) -> Result<Option<NameClass>, &'static str> {
    use NameClass::*;
    match name {
        "user" | "password" => Ok(Some(Credential)),
        "database" => Ok(Some(Destination)),
        "query"
        | "insert_deduplication_token"
        | "async_insert"
        | "wait_for_async_insert" => {
            Err("is set by DeltaForge and cannot be configured")
        }
        _ => Ok(None),
    }
}

// ── Canonicalization ───────────────────────────────────────────────────────

struct Ctx {
    rejections: Vec<Rejection>,
}

impl Ctx {
    fn reject(&mut self, field: &str, reason: impl Into<String>) {
        self.rejections.push(Rejection {
            field: field.to_string(),
            reason: reason.into(),
        });
    }
}

/// The binding of `spec`, or every reason it cannot be classified.
pub fn binding(
    spec: &PipelineSpec,
    resolved: &ResolvedEndpoints,
) -> Result<StateBinding, Vec<Rejection>> {
    let mut ctx = Ctx {
        rejections: Vec::new(),
    };
    let (engine, source_cfg) = match &spec.spec.source {
        SourceCfg::Postgres(c) => ("postgres", to_value(c)),
        SourceCfg::Mysql(c) => ("mysql", to_value(c)),
    };
    let source_id = spec.spec.source.source_id().to_string();

    // Source: the configuration without its id (an owner field), plus the
    // PostgreSQL database name (from the DSN, inline or resolved).
    let mut source = strip(source_cfg, &["id"]);
    if let SourceCfg::Postgres(c) = &spec.spec.source {
        let dsn = c.dsn.clone().or_else(|| resolved.source_dsn.clone());
        match dsn.as_deref().map(pg_database) {
            Some(Ok(db)) => {
                source.insert("database".into(), Value::String(db));
            }
            Some(Err(why)) => ctx.reject("source.dsn", why),
            None => ctx.reject(
                "source.dsn_secret",
                "the database name the DSN selects cannot be determined \
                 (the secret was not resolved)",
            ),
        }
    }
    let source =
        walk(&format!("source.{engine}"), Value::Object(source), &mut ctx);

    // The rest of the spec.
    let rest = strip(to_value(&spec.spec), &["source", "sinks"]);
    let rest = walk("spec", Value::Object(rest), &mut ctx);

    // Sinks by id.
    let mut sinks = BTreeMap::new();
    for sink in &spec.spec.sinks {
        let id = sink.sink_id().to_string();
        let mut cfg = strip(sink_config(sink), &["id"]);
        let kind = sink_type(sink);
        cfg.insert("type".into(), Value::String(kind.into()));
        apply_defaults(&mut cfg);
        if let SinkCfg::Redis(r) = sink
            && r.uri_secret.is_some()
        {
            match resolved.sink_uris.get(&id) {
                Some(uri) => {
                    cfg.insert("uri".into(), Value::String(uri.clone()));
                }
                None => ctx.reject(
                    &format!("sinks.{id}.uri_secret"),
                    "the Redis URI selects the destination and comes from \
                     a secret that was not resolved",
                ),
            }
        }
        let walked = walk_sink(
            &id,
            &format!("sinks.{kind}"),
            Value::Object(cfg),
            &mut ctx,
        );
        sinks.insert(id, walked);
    }
    if let Err(why) = clickhouse_database_agrees(spec) {
        ctx.rejections.push(why);
    }

    if !ctx.rejections.is_empty() {
        return Err(ctx.rejections);
    }

    let mut canonical = Map::new();
    canonical.insert("source".into(), source.unwrap_or(Value::Null));
    canonical.insert("spec".into(), rest.unwrap_or(Value::Null));
    canonical.insert(
        "sinks".into(),
        Value::Object(
            sinks
                .into_iter()
                .map(|(k, v)| (k, v.unwrap_or(Value::Null)))
                .collect(),
        ),
    );
    let canonical = sorted(Value::Object(canonical));
    let mut b = StateBinding {
        format_version: FORMAT_VERSION,
        source_id,
        pipeline: spec.metadata.name.clone(),
        tenant: spec.metadata.tenant.clone(),
        engine: engine.to_string(),
        canonical,
        digest: String::new(),
    };
    b.digest = digest_of(&b);
    Ok(b)
}

/// Every reason `spec` cannot be classified, ignoring what needs a
/// resolved secret (create and PATCH validation, before anything is
/// resolved).
pub fn validate(spec: &PipelineSpec) -> Result<(), Vec<Rejection>> {
    let mut resolved = ResolvedEndpoints::default();
    if let SourceCfg::Postgres(c) = &spec.spec.source
        && c.dsn.is_none()
    {
        resolved.source_dsn = Some("postgres://placeholder/validate".into());
    }
    for sink in &spec.spec.sinks {
        if let SinkCfg::Redis(r) = sink
            && r.uri_secret.is_some()
        {
            resolved
                .sink_uris
                .insert(r.id.clone(), "redis://placeholder/0".into());
        }
    }
    binding(spec, &resolved).map(|_| ())
}

/// The recorded fields of `new` that differ from `stored` (owner fields
/// included). Empty when they match.
pub fn diff(stored: &StateBinding, new: &StateBinding) -> Vec<Difference> {
    let mut out = Vec::new();
    for (field, a, b) in [
        ("metadata.tenant", &stored.tenant, &new.tenant),
        ("metadata.name", &stored.pipeline, &new.pipeline),
        ("source.id", &stored.source_id, &new.source_id),
        ("source.type", &stored.engine, &new.engine),
    ] {
        if a != b {
            out.push(Difference {
                field: field.into(),
                stored: quoted(a),
                new: quoted(b),
            });
        }
    }
    if stored.format_version != new.format_version {
        out.push(Difference {
            field: "format_version".into(),
            stored: stored.format_version.to_string(),
            new: new.format_version.to_string(),
        });
        return out;
    }
    diff_values("", &stored.canonical, &new.canonical, &mut out);
    out
}

fn diff_values(path: &str, a: &Value, b: &Value, out: &mut Vec<Difference>) {
    match (a, b) {
        (Value::Object(x), Value::Object(y)) => {
            let keys: std::collections::BTreeSet<&String> =
                x.keys().chain(y.keys()).collect();
            for k in keys {
                let p = if path.is_empty() {
                    k.clone()
                } else {
                    format!("{path}.{k}")
                };
                diff_values(
                    &p,
                    x.get(k).unwrap_or(&Value::Null),
                    y.get(k).unwrap_or(&Value::Null),
                    out,
                );
            }
        }
        _ if a != b => out.push(Difference {
            field: path.to_string(),
            stored: shown(a),
            new: shown(b),
        }),
        _ => {}
    }
}

/// A canonical value as shown in a refusal. Canonical values exclude
/// credentials by construction; the configuration sanitizer runs over it
/// as a backstop.
fn shown(v: &Value) -> String {
    if v.is_null() {
        return "(absent)".into();
    }
    let mut v = v.clone();
    crate::sanitize::redact_secrets(&mut v);
    v.to_string()
}

fn quoted(s: &str) -> String {
    Value::String(s.to_string()).to_string()
}

/// The digest of a binding (its `digest` field excluded).
pub fn digest_of(b: &StateBinding) -> String {
    let v = sorted(serde_json::json!({
        "format_version": b.format_version,
        "source_id": b.source_id,
        "pipeline": b.pipeline,
        "tenant": b.tenant,
        "engine": b.engine,
        "canonical": b.canonical,
    }));
    hex(&Sha256::digest(v.to_string().as_bytes()))
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

fn to_value<T: Serialize>(v: &T) -> Value {
    serde_json::to_value(v).expect("configuration serializes")
}

fn strip(v: Value, drop: &[&str]) -> Map<String, Value> {
    match v {
        Value::Object(mut m) => {
            for k in drop {
                m.remove(*k);
            }
            m
        }
        _ => Map::new(),
    }
}

fn sink_config(sink: &SinkCfg) -> Value {
    match to_value(sink) {
        Value::Object(mut m) => m.remove("config").unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

fn sink_type(sink: &SinkCfg) -> &'static str {
    match sink {
        SinkCfg::Kafka(_) => "kafka",
        SinkCfg::Redis(_) => "redis",
        SinkCfg::Nats(_) => "nats",
        SinkCfg::Http(_) => "http",
        SinkCfg::S3(_) => "s3",
        SinkCfg::ClickHouse(_) => "clickhouse",
        SinkCfg::Elasticsearch(_) => "elasticsearch",
    }
}

/// Defaults made explicit: an omitted field and the same field set to its
/// default compare equal.
fn apply_defaults(cfg: &mut Map<String, Value>) {
    for (k, default) in [("required", true), ("exactly_once", false)] {
        if cfg.get(k).is_some_and(Value::is_null) {
            cfg.insert(k.into(), Value::Bool(default));
        }
    }
}

/// Recursively sorted object keys (the workspace may preserve insertion
/// order); arrays keep their order.
fn sorted(v: Value) -> Value {
    match v {
        Value::Object(m) => {
            let b: BTreeMap<String, Value> =
                m.into_iter().map(|(k, v)| (k, sorted(v))).collect();
            Value::Object(b.into_iter().collect())
        }
        Value::Array(a) => Value::Array(a.into_iter().map(sorted).collect()),
        v => v,
    }
}

/// Walk `v` at `path`: keep recorded leaves, drop the others, canonicalize
/// special fields. `None` when nothing is kept.
fn walk(path: &str, v: Value, ctx: &mut Ctx) -> Option<Value> {
    walk_in(path, path, v, ctx)
}

/// As [`walk`] for a sink: rejections name the sink id, rules use its type.
fn walk_sink(id: &str, path: &str, v: Value, ctx: &mut Ctx) -> Option<Value> {
    walk_in(path, &format!("sinks.{id}"), v, ctx)
}

fn walk_in(
    rule_path: &str,
    shown_path: &str,
    v: Value,
    ctx: &mut Ctx,
) -> Option<Value> {
    if v.is_null() {
        return None;
    }
    let (class, _) = class_of(rule_path);
    match class {
        // A recorded field inside an ignored one (`journal.enabled`).
        Class::Operational | Class::Credential | Class::Fenced
            if v.is_object() && has_recorded_within(rule_path) =>
        {
            let mut out = Map::new();
            for (k, child) in v.as_object().cloned().unwrap_or_default() {
                if let Some(c) = walk_in(
                    &format!("{rule_path}.{k}"),
                    &format!("{shown_path}.{k}"),
                    child,
                    ctx,
                ) {
                    out.insert(k, c);
                }
            }
            (!out.is_empty()).then_some(Value::Object(out))
        }
        Class::Operational | Class::Credential | Class::Fenced => None,
        Class::Special(s) => special(s, shown_path, v, ctx),
        Class::Recorded => match v {
            Value::Object(m) => {
                let mut out = Map::new();
                for (k, child) in m {
                    if let Some(c) = walk_in(
                        &format!("{rule_path}.{k}"),
                        &format!("{shown_path}.{k}"),
                        child,
                        ctx,
                    ) {
                        out.insert(k, c);
                    }
                }
                (!out.is_empty()).then_some(Value::Object(out))
            }
            v => Some(v),
        },
    }
}

fn special(s: Special, path: &str, v: Value, ctx: &mut Ctx) -> Option<Value> {
    let text = |v: &Value| v.as_str().map(str::to_string);
    match s {
        Special::KafkaBrokers => {
            let list = text(&v)?;
            let brokers: Vec<Value> = list
                .split(',')
                .map(str::trim)
                .filter(|b| !b.is_empty())
                .map(|b| Value::String(b.to_ascii_lowercase()))
                .collect();
            Some(Value::Array(brokers))
        }
        Special::KafkaClientConf => {
            let mut kept = Map::new();
            for (k, val) in v.as_object().cloned().unwrap_or_default() {
                match kafka_key(&k) {
                    Some(NameClass::Destination | NameClass::Delivery) => {
                        kept.insert(k, val);
                    }
                    Some(NameClass::Credential | NameClass::Operational) => {}
                    None => ctx.reject(
                        &format!("{path}.{k}"),
                        "is not a classified Kafka setting; it may change \
                         the destination or delivery, so it is refused",
                    ),
                }
            }
            (!kept.is_empty()).then_some(Value::Object(kept))
        }
        Special::KafkaSecretRefs => {
            for k in v
                .as_object()
                .map(|m| m.keys().cloned().collect::<Vec<_>>())
                .unwrap_or_default()
            {
                if kafka_key(&k) != Some(NameClass::Credential) {
                    ctx.reject(
                        &format!("{path}.{k}"),
                        "only a credential setting may come from a secret; \
                         this one can select the destination or change \
                         delivery, so its value must be compared",
                    );
                }
            }
            None
        }
        Special::HttpHeaders => {
            let mut kept = Map::new();
            for (k, val) in v.as_object().cloned().unwrap_or_default() {
                let name = k.to_ascii_lowercase();
                match http_header(&name) {
                    Some(NameClass::Destination | NameClass::Delivery) => {
                        kept.insert(name, val);
                    }
                    Some(NameClass::Credential | NameClass::Operational) => {}
                    None => ctx.reject(
                        &format!("{path}.{k}"),
                        "is not a classified HTTP header; it may select a \
                         tenant or destination, so it is refused",
                    ),
                }
            }
            (!kept.is_empty()).then_some(Value::Object(kept))
        }
        Special::HttpSecretRefs => {
            for k in v
                .as_object()
                .map(|m| m.keys().cloned().collect::<Vec<_>>())
                .unwrap_or_default()
            {
                if http_header(&k.to_ascii_lowercase())
                    != Some(NameClass::Credential)
                {
                    ctx.reject(
                        &format!("{path}.{k}"),
                        "only a credential header (Authorization, \
                         Proxy-Authorization, Cookie) may come from a \
                         secret; this one can select a tenant or \
                         destination",
                    );
                }
            }
            None
        }
        Special::HttpUrl | Special::UrlWithoutQuery | Special::NatsUrl => {
            let raw = text(&v)?;
            let urls: Vec<&str> = if s == Special::NatsUrl {
                raw.split(',')
                    .map(str::trim)
                    .filter(|u| !u.is_empty())
                    .collect()
            } else {
                vec![raw.as_str()]
            };
            let mut out = Vec::new();
            for u in urls {
                match endpoint(u) {
                    Ok(e) => {
                        if !e.query.is_empty() {
                            for (name, _) in &e.query {
                                ctx.reject(
                                    &format!("{path}?{name}"),
                                    "query parameters of this endpoint are \
                                     not classified; one may select a tenant \
                                     or destination, so it is refused",
                                );
                            }
                        }
                        out.push(Value::String(e.base()));
                    }
                    Err(why) => ctx.reject(path, why),
                }
            }
            match out.len() {
                0 => None,
                1 if s != Special::NatsUrl => out.pop(),
                _ => Some(Value::Array(out)),
            }
        }
        Special::RedisUri => {
            let raw = text(&v)?;
            match endpoint(&raw) {
                Ok(e) => {
                    let mut m = Map::new();
                    m.insert("endpoint".into(), Value::String(e.base()));
                    let mut q = Map::new();
                    for (name, val) in &e.query {
                        match redis_param(name) {
                            Some(_) => {
                                q.insert(
                                    name.clone(),
                                    Value::String(val.clone()),
                                );
                            }
                            None => ctx.reject(
                                &format!("{path}?{name}"),
                                "is not a classified Redis URI parameter; \
                                 it may select the destination, so it is \
                                 refused",
                            ),
                        }
                    }
                    if !q.is_empty() {
                        m.insert("query".into(), Value::Object(q));
                    }
                    Some(Value::Object(m))
                }
                Err(why) => {
                    ctx.reject(path, why);
                    None
                }
            }
        }
        Special::ClickHouseUrl => {
            // Host and port are fenced (the ownership claim checks the
            // server); kept: the scheme, the path and the destination
            // parameters.
            let raw = text(&v)?;
            match endpoint(&raw) {
                Ok(e) => {
                    let mut m = Map::new();
                    m.insert("scheme".into(), Value::String(e.scheme.clone()));
                    if e.path != "/" && !e.path.is_empty() {
                        m.insert("path".into(), Value::String(e.path.clone()));
                    }
                    let mut q = Map::new();
                    for (name, val) in &e.query {
                        match clickhouse_param(name) {
                            Ok(Some(
                                NameClass::Destination | NameClass::Delivery,
                            )) => {
                                q.insert(
                                    name.clone(),
                                    Value::String(val.clone()),
                                );
                            }
                            Ok(Some(_)) => {}
                            Ok(None) => ctx.reject(
                                &format!("{path}?{name}"),
                                "is not a classified ClickHouse URL \
                                 parameter; it may change the destination, \
                                 so it is refused",
                            ),
                            Err(why) => {
                                ctx.reject(&format!("{path}?{name}"), why)
                            }
                        }
                    }
                    if !q.is_empty() {
                        m.insert("query".into(), Value::Object(q));
                    }
                    Some(Value::Object(m))
                }
                Err(why) => {
                    ctx.reject(path, why);
                    None
                }
            }
        }
    }
}

/// A ClickHouse URL `database` parameter that disagrees with the sink's
/// `database` field is a validation error.
fn clickhouse_database_agrees(spec: &PipelineSpec) -> Result<(), Rejection> {
    for sink in &spec.spec.sinks {
        if let SinkCfg::ClickHouse(c) = sink
            && let Ok(e) = endpoint(&c.url)
            && let Some((_, db)) = e.query.iter().find(|(k, _)| k == "database")
            && db != &c.database
        {
            return Err(Rejection {
                field: format!("sinks.{}.url?database", c.id),
                reason: format!(
                    "the URL selects database {db:?}, the sink's database \
                     field {:?}",
                    c.database
                ),
            });
        }
    }
    Ok(())
}

/// A credential-free endpoint: scheme, lowercased host, port (the scheme's
/// default filled in), path, and the query parameters (decoded, in order).
struct Endpoint {
    scheme: String,
    host: String,
    port: Option<u16>,
    path: String,
    query: Vec<(String, String)>,
}

impl Endpoint {
    fn base(&self) -> String {
        let port = self.port.map(|p| format!(":{p}")).unwrap_or_default();
        format!("{}://{}{}{}", self.scheme, self.host, port, self.path)
    }
}

fn endpoint(raw: &str) -> Result<Endpoint, String> {
    let u = url::Url::parse(raw.trim()).map_err(|e| {
        // Never echo the value: it may carry credentials.
        format!("is not a valid URL ({e})")
    })?;
    let host = u
        .host_str()
        .map(str::to_ascii_lowercase)
        .unwrap_or_default();
    let path = match u.path() {
        "" => "/".to_string(),
        p => p.to_string(),
    };
    Ok(Endpoint {
        scheme: u.scheme().to_ascii_lowercase(),
        host,
        port: u.port_or_known_default(),
        path,
        query: u
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect(),
    })
}

/// The database a PostgreSQL DSN selects: the URL path, or the `dbname`
/// key of a key-value DSN.
fn pg_database(dsn: &str) -> Result<String, String> {
    let dsn = dsn.trim();
    if dsn.starts_with("postgres://") || dsn.starts_with("postgresql://") {
        let e = endpoint(dsn)?;
        let db = e.path.trim_start_matches('/').to_string();
        if let Some((_, v)) = e.query.iter().find(|(k, _)| k == "dbname") {
            return Ok(v.clone());
        }
        return Ok(db);
    }
    for part in dsn.split_whitespace() {
        if let Some(v) = part.strip_prefix("dbname=") {
            return Ok(v.trim_matches('\'').to_string());
        }
    }
    Ok(String::new())
}

#[cfg(test)]
mod tests;

//! Avro encoding with Confluent Schema Registry support.
//!
//! Produces the [Confluent wire format]:
//! `[0x00][4-byte schema ID (big-endian)][Avro binary payload]`
//!
//! [Confluent wire format]: https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format
//!
//! # Usage
//!
//! ```ignore
//! let encoder = AvroEncoder::new(
//!     "http://localhost:8081",
//!     SubjectStrategy::TopicName,
//!     None, // auth
//! ).await?;
//!
//! let bytes = encoder.encode("my-topic", &envelope_data).await?;
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use apache_avro::Schema as AvroSchema;
use apache_avro::types::Value as AvroValue;
use bytes::Bytes;
use metrics::counter;
use parking_lot::RwLock;
use serde::Serialize;
use tracing::debug;

use super::EncodingError;
use super::avro_infer::Inferred;
use crate::canonical::CanonicalRow;
use crate::envelope::EnvelopeData;

#[path = "avro_canonical.rs"]
mod canonical_rows;

// =============================================================================
// Source schema provider trait
// =============================================================================

/// The registered source schema version an event was produced under: the
/// schema registry's content hash of the table schema and, when the event
/// records it, the registry sequence of that version.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SchemaIdentity {
    pub fingerprint: String,
    pub sequence: Option<u64>,
}

impl SchemaIdentity {
    /// The identity an event carries (`schema_version`, `schema_sequence`),
    /// or `None` when it carries no schema version.
    pub fn of(
        fingerprint: Option<&str>,
        sequence: Option<u64>,
    ) -> Option<Self> {
        fingerprint.map(|f| Self {
            fingerprint: f.to_string(),
            sequence,
        })
    }
}

/// Provides DDL-derived Avro schemas for CDC events (Path A).
///
/// Implementors look up source table schemas and convert them to Avro
/// field definitions using the type converters in [`super::avro_types`].
///
/// The returned schema string is a complete Avro envelope schema JSON
/// (built by [`super::avro_schema::build_envelope_schema`]).
pub trait SourceSchemaProvider: Send + Sync {
    /// The Avro envelope schema of table `table` in `namespace` (see
    /// [`crate::SourceInfo::namespace`]) of `database` at exactly the
    /// registered version `identity` names, never the table's latest
    /// schema.
    fn get_envelope_schema(
        &self,
        connector: &str,
        database: &str,
        namespace: &str,
        table: &str,
        identity: &SchemaIdentity,
    ) -> EnvelopeSchemaLookup;

    /// Identifies the column-to-Avro type mapping (its version and
    /// options): the same source schema maps to a different Avro schema
    /// under another mapping.
    fn mapping_id(&self) -> &str;
}

/// Outcome of a DDL-derived Avro schema lookup.
#[derive(Debug, Clone)]
pub enum EnvelopeSchemaLookup {
    /// DDL is available: `(schema_json, parsed_schema)` (Path A).
    Found(String, Arc<AvroSchema>),
    /// The provider does not serve this source: fall back to JSON
    /// inference (Path C).
    Absent,
    /// The source schema cannot be read right now (for example the source has
    /// not yet verified its server). Encoding must fail retryably: inferring
    /// and registering a schema here would fix a wrong schema for the subject.
    Unavailable(String),
    /// The event's schema version is not the registered one: its
    /// fingerprint is unknown, or registered under another sequence.
    /// Encoding must stop: no schema can be trusted for the event.
    Conflict(String),
}

// =============================================================================
// Subject naming strategy
// =============================================================================

/// Subject naming strategy for Schema Registry (mirrors config type).
#[derive(Debug, Clone, Default)]
pub enum SubjectStrategy {
    /// `{topic}-value`
    #[default]
    TopicName,
    /// `{record_name}`
    RecordName,
    /// `{topic}-{record_name}`
    TopicRecordName,
}

impl SubjectStrategy {
    /// Resolve the subject name for a given topic and optional record name.
    pub fn resolve(&self, topic: &str, record_name: Option<&str>) -> String {
        match self {
            SubjectStrategy::TopicName => format!("{topic}-value"),
            SubjectStrategy::RecordName => {
                record_name.unwrap_or("deltaforge.Event").to_string()
            }
            SubjectStrategy::TopicRecordName => {
                let rn = record_name.unwrap_or("deltaforge.Event");
                format!("{topic}-{rn}")
            }
        }
    }
}

// =============================================================================
// Schema Registry client
// =============================================================================

/// The canonical hash of an Avro schema JSON: SHA-256 of its JSON with
/// object keys sorted and no whitespace. Two texts of the same schema share
/// it; schemas differing in any type, name, logical type or attribute do not.
pub fn canonical_schema_hash(
    schema_json: &str,
) -> Result<String, EncodingError> {
    use sha2::{Digest, Sha256};
    fn write(v: &serde_json::Value, out: &mut String) {
        match v {
            serde_json::Value::Object(m) => {
                let mut keys: Vec<&String> = m.keys().collect();
                keys.sort();
                out.push('{');
                for (i, k) in keys.into_iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push_str(
                        &serde_json::Value::from(k.as_str()).to_string(),
                    );
                    out.push(':');
                    write(&m[k], out);
                }
                out.push('}');
            }
            serde_json::Value::Array(a) => {
                out.push('[');
                for (i, x) in a.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write(x, out);
                }
                out.push(']');
            }
            scalar => out.push_str(&scalar.to_string()),
        }
    }
    let v: serde_json::Value = serde_json::from_str(schema_json)
        .map_err(|e| EncodingError::Avro(format!("schema is not JSON: {e}")))?;
    let mut canonical = String::with_capacity(schema_json.len());
    write(&v, &mut canonical);
    Ok(hex::encode(Sha256::digest(canonical.as_bytes())))
}

/// Confluent Schema Registry HTTP client.
///
/// Registered schema IDs are cached per `(subject, canonical schema hash)`:
/// a cached ID is only ever returned for the exact schema it was
/// registered for, never for another version of the subject.
pub struct SchemaRegistryClient {
    client: reqwest::Client,
    base_url: String,
    cache: Arc<RwLock<HashMap<(String, String), CachedSchema>>>,
}

#[derive(Clone)]
struct CachedSchema {
    id: u32,
    schema: Arc<AvroSchema>,
}

/// Response from POST /subjects/{subject}/versions
#[derive(serde::Deserialize)]
struct RegisterResponse {
    id: u32,
}

/// Response from GET /subjects/{subject}/versions/latest
#[derive(serde::Deserialize)]
struct LatestResponse {
    schema: String,
}

/// Error response from Schema Registry
#[derive(serde::Deserialize)]
struct SrErrorResponse {
    message: Option<String>,
    error_code: Option<i32>,
}

impl SchemaRegistryClient {
    /// Create a new Schema Registry client.
    pub fn new(
        base_url: &str,
        _username: Option<&str>,
        _password: Option<&str>,
    ) -> Result<Self, EncodingError> {
        // Credentials are sent per request (see `AvroEncoder::auth`).
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .connect_timeout(std::time::Duration::from_secs(5))
            .build()
            .map_err(|e| EncodingError::SchemaRegistry(e.to_string()))?;

        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Register `schema_json` under `subject`, or return its existing ID.
    ///
    /// Uses the Schema Registry's idempotent registration endpoint:
    /// POST /subjects/{subject}/versions
    pub async fn register_schema(
        &self,
        subject: &str,
        schema_json: &str,
        auth: Option<(&str, &str)>,
    ) -> Result<(u32, Arc<AvroSchema>), EncodingError> {
        let hash = canonical_schema_hash(schema_json)?;
        self.register_hashed(subject, schema_json, &hash, auth)
            .await
    }

    async fn register_hashed(
        &self,
        subject: &str,
        schema_json: &str,
        hash: &str,
        auth: Option<(&str, &str)>,
    ) -> Result<(u32, Arc<AvroSchema>), EncodingError> {
        if let Some(cached) = self.get_cached(subject, hash) {
            return Ok(cached);
        }
        let schema = AvroSchema::parse_str(schema_json).map_err(|e| {
            EncodingError::Avro(format!("failed to parse schema: {e}"))
        })?;

        let url = format!("{}/subjects/{}/versions", self.base_url, subject);
        let body = serde_json::json!({
            "schema": schema_json,
            "schemaType": "AVRO"
        });
        let mut req = self
            .client
            .post(&url)
            .header("Content-Type", "application/vnd.schemaregistry.v1+json")
            .json(&body);
        if let Some((u, p)) = auth {
            req = req.basic_auth(u, Some(p));
        }

        let resp = req.send().await.map_err(|e| {
            EncodingError::SchemaRegistryUnavailable(format!(
                "failed to register schema for subject {subject}: {e}"
            ))
        })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            let details = match serde_json::from_str::<SrErrorResponse>(&body) {
                Ok(sr_err) => format!(
                    "{} (code: {})",
                    sr_err.message.unwrap_or_default(),
                    sr_err.error_code.unwrap_or(0),
                ),
                Err(_) => format!("HTTP {status}: {body}"),
            };
            return Err(registry_error(
                status,
                subject,
                format!("schema registration failed for {subject}: {details}"),
            ));
        }

        let register_resp: RegisterResponse =
            resp.json().await.map_err(|e| {
                EncodingError::SchemaRegistryUnavailable(format!(
                    "invalid response from schema registry: {e}"
                ))
            })?;
        let schema = Arc::new(schema);

        debug!(
            subject,
            schema_id = register_resp.id,
            "registered Avro schema"
        );

        self.cache.write().insert(
            (subject.to_string(), hash.to_string()),
            CachedSchema {
                id: register_resp.id,
                schema: schema.clone(),
            },
        );

        Ok((register_resp.id, schema))
    }

    /// The latest schema registered under `subject`, or `None` when the
    /// subject has none.
    pub async fn latest_schema(
        &self,
        subject: &str,
        auth: Option<(&str, &str)>,
    ) -> Result<Option<String>, EncodingError> {
        let url =
            format!("{}/subjects/{}/versions/latest", self.base_url, subject);
        let mut req = self.client.get(&url);
        if let Some((u, p)) = auth {
            req = req.basic_auth(u, Some(p));
        }
        let resp = req.send().await.map_err(|e| {
            EncodingError::SchemaRegistryUnavailable(format!(
                "latest schema lookup failed for {subject}: {e}"
            ))
        })?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(registry_error(
                status,
                subject,
                format!(
                    "latest schema lookup failed for {subject}: HTTP {status}: {body}"
                ),
            ));
        }
        let latest: LatestResponse = resp.json().await.map_err(|e| {
            EncodingError::SchemaRegistryUnavailable(format!(
                "invalid latest schema response: {e}"
            ))
        })?;
        Ok(Some(latest.schema))
    }

    /// The cached ID of exactly the schema with canonical hash `hash`
    /// under `subject`.
    pub fn get_cached(
        &self,
        subject: &str,
        hash: &str,
    ) -> Option<(u32, Arc<AvroSchema>)> {
        self.cache
            .read()
            .get(&(subject.to_string(), hash.to_string()))
            .map(|c| (c.id, c.schema.clone()))
    }

    /// Clear the schema cache; the next encode of each schema registers
    /// it again (idempotent in the registry).
    pub fn clear_cache(&self) {
        self.cache.write().clear();
    }
}

/// The error for a Schema Registry error response: an outage (5xx, 408,
/// 429) is retryable, 409 is a compatibility rejection, anything else a
/// registry error. None is a property of the event.
fn registry_error(
    status: reqwest::StatusCode,
    subject: &str,
    details: String,
) -> EncodingError {
    use reqwest::StatusCode as S;
    match status {
        S::CONFLICT => EncodingError::SchemaRejected {
            subject: subject.to_string(),
            details,
        },
        s if s.is_server_error()
            || s == S::REQUEST_TIMEOUT
            || s == S::TOO_MANY_REQUESTS =>
        {
            EncodingError::SchemaRegistryUnavailable(details)
        }
        _ => EncodingError::SchemaRegistry(details),
    }
}

// =============================================================================
// Avro Encoder
// =============================================================================

/// What a DDL-derived (Path A) schema is resolved for: the topic (its
/// subject), the source relation, the exact source schema version the
/// event was produced under, and the type mapping, written into `out` as
/// one string with every part length-prefixed (injective), so a cached
/// event builds no identity.
fn resolved_key(
    out: &mut String,
    topic: &str,
    event: &crate::Event,
    fingerprint: &str,
    mapping_id: &str,
) {
    use std::fmt::Write;
    let source = &event.source;
    let mut part = |p: &str| {
        let _ = write!(out, "{}:", p.len());
        out.push_str(p);
    };
    for p in [
        topic,
        &source.connector,
        &source.name,
        &source.db,
        source.namespace(),
        &source.table,
        fingerprint,
        mapping_id,
        event.type_mapping.as_str(),
    ] {
        part(p);
    }
    // The registry sequence: `s<digits>;`, or `n;` without one.
    let _ = match event.schema_sequence {
        Some(n) => write!(out, "s{n};"),
        None => write!(out, "n;"),
    };
}

/// A schema registered for a subject, prepared for encoding.
type Registered = Arc<Prepared>;

/// A registered schema prepared once for encoding: its ID, its named types
/// (resolved once, not per event) and writers that validate and encode
/// values with them (several, so concurrent encoders do not wait for one).
struct Prepared {
    id: u32,
    schema: Arc<AvroSchema>,
    names: Names,
    writers: Vec<parking_lot::Mutex<apache_avro::GenericSingleObjectWriter>>,
}

/// Writers per prepared schema: concurrent encoders beyond this share them.
const WRITERS: usize = 8;

/// A schema's named types by name.
type Names = HashMap<apache_avro::schema::Name, AvroSchema>;

/// The length of the single-object header the writer puts before each
/// datum (`C3 01` and the schema's 8-byte fingerprint).
const SINGLE_OBJECT_HEADER: usize = 10;

impl Prepared {
    fn new(
        id: u32,
        schema: Arc<AvroSchema>,
    ) -> Result<Registered, EncodingError> {
        let names = resolve_names(&schema)?;
        let writers = (0..WRITERS)
            .map(|_| {
                apache_avro::GenericSingleObjectWriter::new_with_capacity(
                    &schema, 1024,
                )
                .map(parking_lot::Mutex::new)
                .map_err(|e| {
                    EncodingError::Avro(format!("unresolvable schema: {e}"))
                })
            })
            .collect::<Result<_, _>>()?;
        Ok(Arc::new(Prepared {
            id,
            schema,
            names,
            writers,
        }))
    }
}

fn resolve_names(schema: &AvroSchema) -> Result<Names, EncodingError> {
    let resolved = apache_avro::schema::ResolvedSchema::try_from(schema)
        .map_err(|e| {
            EncodingError::Avro(format!("unresolvable schema: {e}"))
        })?;
    Ok(resolved
        .get_names()
        .iter()
        .map(|(n, s)| (n.clone(), (*s).clone()))
        .collect())
}

/// The Schema Registry subject of a topic's DDL events: stable, separate from
/// every row subject, so neither's compatibility checks affect the other.
pub fn ddl_subject(topic: &str) -> String {
    format!(
        "{topic}-deltaforge.DdlEvent-v{}",
        super::avro_schema::DDL_MAPPING_VERSION
    )
}

/// What a DDL schema is registered for: the subject, the canonical hash of
/// the explicit DDL schema and its mapping version.
type DdlKey = (String, String, u32);

/// The current widened inferred schema of a subject and record name.
struct InferredState {
    shape: Inferred,
    schema_json: String,
    hash: String,
}

/// Avro encoder that produces Confluent wire format.
///
/// Serializes events as:
/// - Byte 0: Magic byte (0x00)
/// - Bytes 1-4: Schema ID (big-endian u32)
/// - Bytes 5+: Avro binary-encoded payload
///
/// Each event is encoded with the schema it requires. A DDL-derived event
/// uses exactly the source schema version it carries (cached per subject,
/// table, version and type mapping), so events of different versions, in
/// any order, each get their own schema and ID. An event without a source
/// schema version uses the subject's inferred schema, widened to hold it.
pub struct AvroEncoder {
    registry: SchemaRegistryClient,
    strategy: SubjectStrategy,
    auth: Option<(String, String)>,
    /// Optional DDL-derived schema provider (Path A).
    source_schemas: Option<Arc<dyn SourceSchemaProvider>>,
    /// Cache: topic → resolved subject name (avoids format! per event)
    subject_cache: RwLock<HashMap<String, String>>,
    /// Path A: the registered schema of each resolved version.
    resolved: RwLock<HashMap<String, Registered>>,
    /// Path C: (subject, record name) → widened inferred schema.
    inferred: RwLock<HashMap<(String, String), InferredState>>,
    /// DDL schemas by connector: (schema JSON, canonical hash).
    ddl_schemas: RwLock<HashMap<String, (String, String)>>,
    /// DDL: the registered schema of each (subject, hash, mapping version).
    ddl: RwLock<HashMap<DdlKey, Registered>>,
    /// Every registered schema, prepared, by (subject, canonical hash).
    prepared: RwLock<HashMap<(String, String), Registered>>,
}

impl AvroEncoder {
    /// Create a new Avro encoder (Path C only — JSON inference fallback).
    pub fn new(
        schema_registry_url: &str,
        strategy: SubjectStrategy,
        username: Option<&str>,
        password: Option<&str>,
    ) -> Result<Self, EncodingError> {
        Self::with_source_schemas(
            schema_registry_url,
            strategy,
            username,
            password,
            None,
        )
    }

    /// Create a new Avro encoder with an optional DDL schema provider.
    ///
    /// When `source_schemas` is provided, the encoder uses DDL-derived
    /// Avro schemas (Path A) for events that carry their source schema
    /// version. Other events use JSON inference (Path C).
    pub fn with_source_schemas(
        schema_registry_url: &str,
        strategy: SubjectStrategy,
        username: Option<&str>,
        password: Option<&str>,
        source_schemas: Option<Arc<dyn SourceSchemaProvider>>,
    ) -> Result<Self, EncodingError> {
        let registry =
            SchemaRegistryClient::new(schema_registry_url, username, password)?;

        let auth = match (username, password) {
            (Some(u), Some(p)) => Some((u.to_string(), p.to_string())),
            _ => None,
        };

        Ok(Self {
            registry,
            strategy,
            auth,
            source_schemas,
            subject_cache: RwLock::new(HashMap::new()),
            resolved: RwLock::new(HashMap::new()),
            inferred: RwLock::new(HashMap::new()),
            ddl_schemas: RwLock::new(HashMap::new()),
            ddl: RwLock::new(HashMap::new()),
            prepared: RwLock::new(HashMap::new()),
        })
    }

    /// Set the source schema provider after construction.
    pub fn set_source_schemas(
        &mut self,
        provider: Arc<dyn SourceSchemaProvider>,
    ) {
        self.source_schemas = Some(provider);
        self.resolved.write().clear();
    }

    /// Drop every cached schema and ID. Encoding afterwards resolves and
    /// registers each schema again; inferred schemas resume from the
    /// subject's latest registered schema.
    pub fn clear_caches(&self) {
        self.registry.clear_cache();
        self.resolved.write().clear();
        self.inferred.write().clear();
        self.ddl.write().clear();
        self.prepared.write().clear();
    }

    fn auth(&self) -> Option<(&str, &str)> {
        self.auth.as_ref().map(|(u, p)| (u.as_str(), p.as_str()))
    }

    /// Resolve subject name with caching (avoids format! allocation per event).
    /// The subject of `topic`'s records of mapping version `mapping`.
    fn subject(&self, topic: &str, mapping: crate::TypeMapping) -> String {
        format!("{}-{}", self.resolve_subject(topic), mapping.as_str())
    }

    fn resolve_subject(&self, topic: &str) -> String {
        {
            let cache = self.subject_cache.read();
            if let Some(cached) = cache.get(topic) {
                return cached.clone();
            }
        }
        let subject = self.strategy.resolve(topic, None);
        {
            let mut cache = self.subject_cache.write();
            cache.insert(topic.to_string(), subject.clone());
        }
        subject
    }

    /// Encode a serializable value to Confluent Avro wire format with an
    /// inferred schema (Path C).
    ///
    /// The subject's schema is widened to hold the value (see
    /// [`super::avro_infer`]), registered with the Schema Registry, and the
    /// value is encoded as Avro binary with the Confluent wire format prefix.
    ///
    /// # Arguments
    ///
    /// * `topic` - Kafka topic (used for subject naming)
    /// * `value` - Any serializable value (typically EnvelopeData)
    /// * `record_name` - Optional record name for RecordName/TopicRecordName strategies
    pub async fn encode<T: Serialize>(
        &self,
        topic: &str,
        value: &T,
        record_name: Option<&str>,
    ) -> Result<Bytes, EncodingError> {
        let json_value = serde_json::to_value(value)
            .map_err(|e| EncodingError::Avro(e.to_string()))?;
        let subject = self.strategy.resolve(topic, record_name);
        let name = record_name.unwrap_or("deltaforge.Event");
        self.encode_inferred(&subject, name, &json_value).await
    }

    /// Encode a CDC event with the schema it requires, under the subject
    /// of its mapping version (`<subject>-v1` / `<subject>-v2`: records of
    /// different versions never share a subject or a cached schema).
    ///
    /// An event carrying its source schema version is encoded with the
    /// DDL-derived schema of exactly that version (Path A), its row images
    /// converted from their canonical values; an event without one, or from
    /// a source the provider does not serve, with the subject's inferred
    /// schema (Path C).
    ///
    /// # Arguments
    ///
    /// * `topic` - Destination topic/stream/subject (used for SR subject naming)
    /// * `event` - The event (its rows, mapping version and schema version)
    /// * `envelope` - The event's envelope (its non-row fields are encoded
    ///   from it)
    /// The envelope as the JSON value the schema's converter reads: the
    /// row images as placeholders (converted from their canonical values),
    /// their extensions in their sibling slots.
    fn envelope_value(
        event: &crate::Event,
        envelope: &EnvelopeData<'_>,
    ) -> Result<serde_json::Value, EncodingError> {
        let mut json_value = serde_json::to_value(envelope.row_placeholders())
            .map_err(|e| EncodingError::Avro(e.to_string()))?;
        // The row images' extensions fill their stable sibling slots (null
        // when a row has none); the rows' own records hold declared columns
        // only.
        if let serde_json::Value::Object(o) = &mut json_value {
            for (key, row) in [
                ("before_extensions", &event.before),
                ("after_extensions", &event.after),
            ] {
                let ext = row
                    .as_ref()
                    .filter(|r| !r.extensions().is_empty())
                    .map(|r| {
                        serde_json::Value::Object(
                            r.extensions()
                                .iter()
                                .map(|(n, d)| {
                                    (
                                        n.clone(),
                                        serde_json::Value::String(
                                            d.canonical_text(),
                                        ),
                                    )
                                })
                                .collect(),
                        )
                    })
                    .unwrap_or(serde_json::Value::Null);
                o.insert(key.to_string(), ext);
            }
        }

        Ok(json_value)
    }

    /// Encode an event whose DDL-derived schema (Path A) is already
    /// registered, without waiting: `None` when it is not (the event is
    /// encoded by [`Self::encode_event`], which registers or infers it).
    pub fn encode_cached(
        &self,
        topic: &str,
        event: &crate::Event,
        envelope: &EnvelopeData<'_>,
    ) -> Option<Result<Bytes, EncodingError>> {
        let provider = self.source_schemas.as_ref()?;
        let fingerprint = event.schema_version.as_deref()?;
        let mut key = String::with_capacity(160);
        resolved_key(
            &mut key,
            topic,
            event,
            fingerprint,
            provider.mapping_id(),
        );
        let prepared = self.resolved.read().get(key.as_str()).cloned()?;
        Some(
            Self::envelope_value(event, envelope)
                .and_then(|v| encode_with(&prepared, &v, &row_paths(event))),
        )
    }

    pub async fn encode_event(
        &self,
        topic: &str,
        event: &crate::Event,
        envelope: &EnvelopeData<'_>,
    ) -> Result<Bytes, EncodingError> {
        let mapping = event.type_mapping;
        // The row images are converted from their canonical values (Path A)
        // or rebuilt from them (Path C): the envelope holds `{}` in their
        // places, never a rendering to discard.
        let mut json_value = Self::envelope_value(event, envelope)?;
        let (connector, database, namespace, table) = (
            event.source.connector.as_str(),
            event.source.db.as_str(),
            event.source.namespace(),
            event.source.table.as_str(),
        );
        let rows = row_paths(event);
        if let (Some(provider), Some(fingerprint)) = (
            self.source_schemas.as_ref(),
            event.schema_version.as_deref(),
        ) {
            let mut key = String::with_capacity(160);
            resolved_key(
                &mut key,
                topic,
                event,
                fingerprint,
                provider.mapping_id(),
            );
            let cached = self.resolved.read().get(key.as_str()).cloned();
            if let Some(prepared) = cached {
                return encode_with(&prepared, &json_value, &rows);
            }
            let identity = SchemaIdentity {
                fingerprint: fingerprint.to_string(),
                sequence: event.schema_sequence,
            };
            match provider.get_envelope_schema(
                connector, database, namespace, table, &identity,
            ) {
                EnvelopeSchemaLookup::Found(schema_json, _) => {
                    debug!(
                        relation = %crate::SourceRelationIdentity::of(&event.source).canonical(),
                        fingerprint = %identity.fingerprint,
                        "using DDL-derived Avro schema (Path A)"
                    );
                    counter!("deltaforge_avro_encode_total", "path" => "ddl")
                        .increment(1);
                    let (schema_json, _) =
                        with_mapping_attr(&schema_json, mapping)?;
                    let prepared = self
                        .register(
                            &self.subject(topic, mapping),
                            &schema_json,
                            None,
                        )
                        .await?;
                    self.resolved.write().insert(key, prepared.clone());
                    return encode_with(&prepared, &json_value, &rows);
                }
                EnvelopeSchemaLookup::Unavailable(reason) => {
                    return Err(EncodingError::SchemaUnavailable(format!(
                        "{connector} {database}.{namespace}.{table}: {reason}"
                    )));
                }
                EnvelopeSchemaLookup::Conflict(reason) => {
                    return Err(EncodingError::SchemaConflict(format!(
                        "{connector} {database}.{namespace}.{table}: {reason}"
                    )));
                }
                EnvelopeSchemaLookup::Absent => {}
            }
        }

        // The relation's full identity: Avro names carry its escaped parts,
        // so same-named tables of other databases or schemas never share a
        // name.
        let relation = crate::SourceRelationIdentity::of(&event.source);
        let (avro_ns, avro_table) = relation.avro_path();
        debug!(
            relation = %relation.canonical(),
            "no DDL schema version for the event: JSON inference (Path C)"
        );
        counter!("deltaforge_avro_encode_total", "path" => "inferred")
            .increment(1);
        let record_name =
            format!("deltaforge.{connector}.{avro_ns}.{avro_table}.Value");
        if let serde_json::Value::Object(o) = &mut json_value {
            // An inferred schema carries extensions as row fields.
            o.remove("before_extensions");
            o.remove("after_extensions");
        }
        let json_value = inference_json(json_value, event)?;
        self.encode_inferred(
            &self.subject(topic, mapping),
            &record_name,
            &json_value,
        )
        .await
    }

    /// Encode a DDL event as the explicit DDL record (see
    /// [`super::avro_schema::build_ddl_schema`]) under the topic's DDL
    /// subject ([`ddl_subject`]), whatever the sink's envelope. A DDL event
    /// never enters row-schema inference and is never encoded with a row
    /// schema; the record stays on `topic`, identified by its schema ID.
    pub async fn encode_ddl(
        &self,
        topic: &str,
        event: &crate::Event,
    ) -> Result<Bytes, EncodingError> {
        use super::avro_schema::{DDL_MAPPING_VERSION, build_ddl_schema};
        let Some(ddl) = event.ddl.as_ref() else {
            return Err(EncodingError::Avro("not a DDL event".into()));
        };
        let connector = event.source.connector.as_str();
        let cached = self.ddl_schemas.read().get(connector).cloned();
        let (schema_json, hash) = match cached {
            Some(c) => c,
            None => {
                let (json, _) = build_ddl_schema(connector)?;
                let hash = canonical_schema_hash(&json)?;
                self.ddl_schemas.write().insert(
                    connector.to_string(),
                    (json.clone(), hash.clone()),
                );
                (json, hash)
            }
        };
        let record = serde_json::json!({
            "mapping_version": DDL_MAPPING_VERSION,
            "ddl": ddl,
            "source": event.source,
            "op": event.op.as_str(),
            "ts_ms": event.ts_ms,
            "event_id": event.event_id.map(|id| id.to_string()),
            "tenant_id": event.tenant_id,
            "transaction": event.transaction,
        });
        let subject = ddl_subject(topic);
        // The mapping version is also bound by the versioned subject, the
        // record's `mapping_version` field and the canonical schema hash, so
        // while it is a compile-time constant this component is defensive
        // domain separation (dropping it is an equivalent mutant). Should
        // several DDL versions become selectable in one process, add a
        // mixed-version regression that fails without it.
        let key = (subject, hash, DDL_MAPPING_VERSION);
        let cached = self.ddl.read().get(&key).cloned();
        let prepared = match cached {
            Some(r) => r,
            None => {
                let r =
                    self.register(&key.0, &schema_json, Some(&key.1)).await?;
                self.ddl.write().insert(key, r.clone());
                r
            }
        };
        counter!("deltaforge_avro_encode_total", "path" => "ddl_event")
            .increment(1);
        encode_with(&prepared, &record, &[])
    }

    /// Path C: widen the inferred schema of `(subject, record_name)` to
    /// hold `json_value`, register it, and encode the value with it.
    ///
    /// Inferred events carry no schema identity: an event replayed after the
    /// schema widened is encoded, exactly, with the current (newer,
    /// backward-compatible) schema and its ID, not the one it was first
    /// encoded with. Exact schema-identity replay for inferred events is a
    /// canonical-typing follow-up.
    async fn encode_inferred(
        &self,
        subject: &str,
        record_name: &str,
        json_value: &serde_json::Value,
    ) -> Result<Bytes, EncodingError> {
        let serde_json::Value::Object(map) = json_value else {
            return Err(EncodingError::Avro(
                "an inferred Avro schema needs a JSON object".into(),
            ));
        };
        let key = (subject.to_string(), record_name.to_string());
        if !self.inferred.read().contains_key(&key) {
            // Resume from the subject's latest schema so a restart widens it
            // instead of registering a narrower one.
            let seed =
                match self.registry.latest_schema(subject, self.auth()).await {
                    Ok(Some(latest)) => Inferred::seeded(record_name, &latest),
                    Ok(None) => None,
                    Err(e) => {
                        counter!(
                            "deltaforge_avro_encode_failure_total",
                            "reason" => "sr_unavailable"
                        )
                        .increment(1);
                        return Err(e);
                    }
                };
            let shape = seed.unwrap_or_else(|| Inferred::new(record_name));
            let state = inferred_state(shape)?;
            self.inferred.write().entry(key.clone()).or_insert(state);
        }
        let (schema_json, hash) = {
            let mut states = self.inferred.write();
            let state = states.get_mut(&key).expect("inserted above");
            if !state.shape.covers(map) {
                let mut shape = state.shape.clone();
                shape.absorb(map);
                *state = inferred_state(shape)?;
            }
            (state.schema_json.clone(), state.hash.clone())
        };
        let prepared =
            self.register(subject, &schema_json, Some(&hash)).await?;
        encode_with(&prepared, json_value, &[])
    }

    /// Register a schema (cached per subject and canonical hash).
    async fn register(
        &self,
        subject: &str,
        schema_json: &str,
        hash: Option<&str>,
    ) -> Result<Registered, EncodingError> {
        let hash = match hash {
            Some(h) => h.to_string(),
            None => canonical_schema_hash(schema_json)?,
        };
        let key = (subject.to_string(), hash);
        if let Some(prepared) = self.prepared.read().get(&key) {
            return Ok(prepared.clone());
        }
        let (id, schema) = match self.registry.get_cached(subject, &key.1) {
            Some(cached) => cached,
            None => match self
                .registry
                .register_hashed(subject, schema_json, &key.1, self.auth())
                .await
            {
                Ok(result) => {
                    counter!("deltaforge_avro_schema_registrations_total")
                        .increment(1);
                    result
                }
                Err(e) => {
                    counter!(
                        "deltaforge_avro_encode_failure_total",
                        "reason" => "sr_unavailable"
                    )
                    .increment(1);
                    return Err(e);
                }
            },
        };
        let prepared = Prepared::new(id, schema)?;
        self.prepared.write().insert(key, prepared.clone());
        Ok(prepared)
    }
}

fn inferred_state(shape: Inferred) -> Result<InferredState, EncodingError> {
    let schema_json = shape.schema_json();
    let hash = canonical_schema_hash(&schema_json)?;
    Ok(InferredState {
        shape,
        schema_json,
        hash,
    })
}

/// Convert `json_value` exactly to the prepared schema and frame it in the
/// Confluent wire format: `[0x00][schema_id:4][avro_payload]`.
fn encode_with(
    prepared: &Prepared,
    json_value: &serde_json::Value,
    rows: &[(&str, &CanonicalRow)],
) -> Result<Bytes, EncodingError> {
    let avro_value =
        convert_named(json_value, &prepared.schema, &prepared.names, rows)
            .inspect_err(|_| {
                counter!(
                    "deltaforge_avro_encode_failure_total",
                    "reason" => "schema_mismatch"
                )
                .increment(1);
            })?;
    let mut out = Vec::with_capacity(256);
    // A free writer, or the first one when every writer is busy.
    prepared
        .writers
        .iter()
        .find_map(|w| w.try_lock())
        .unwrap_or_else(|| prepared.writers[0].lock())
        .write_value_ref(&avro_value, &mut out)
        .map_err(|e| {
            // A failed validation names the reason only: never the value.
            let e = match e {
                apache_avro::Error::ValidationWithReason { .. } => {
                    apache_avro::Error::Validation
                }
                e => e,
            };
            EncodingError::Avro(format!("Avro encoding failed: {e}"))
        })?;
    // The writer's single-object header becomes the wire-format prefix.
    out.drain(..SINGLE_OBJECT_HEADER - 5);
    out[0] = 0x00;
    out[1..5].copy_from_slice(&prepared.id.to_be_bytes());
    Ok(Bytes::from(out))
}

// =============================================================================
// JSON → Avro value conversion
// =============================================================================

/// Convert a JSON value to an Avro value of `schema`.
///
/// Every conversion is exact. A value the target type cannot represent
/// exactly (out of range, the wrong kind, a lost fraction, an undecodable
/// text, an epoch number of undeclared unit, an unsupported target type)
/// fails before any record exists, with a [`ConversionFailure`] naming the
/// field, its source type (`deltaforge.source_type` on the field, else the
/// JSON kind) and the target type, never the value. Nothing is substituted,
/// rounded, truncated or defaulted.
#[cfg(test)]
fn json_to_avro(
    json: &serde_json::Value,
    schema: &AvroSchema,
) -> Result<AvroValue, EncodingError> {
    json_to_avro_rows(json, schema, &[])
}

/// [`json_to_avro`], with the event's row images (by envelope path)
/// converted from their canonical values wherever the schema has a row
/// image record.
#[cfg(test)]
fn json_to_avro_rows(
    json: &serde_json::Value,
    schema: &AvroSchema,
    rows: &[(&str, &CanonicalRow)],
) -> Result<AvroValue, EncodingError> {
    convert_named(json, schema, &resolve_names(schema)?, rows)
}

/// [`json_to_avro_rows`] with the schema's named types resolved.
fn convert_named(
    json: &serde_json::Value,
    schema: &AvroSchema,
    names: &Names,
    rows: &[(&str, &CanonicalRow)],
) -> Result<AvroValue, EncodingError> {
    convert(json, schema, &Cx::root(names, rows))
}

/// The envelope paths of an event's row images, for every envelope.
fn row_paths(event: &crate::Event) -> Vec<(&'static str, &CanonicalRow)> {
    let mut rows = Vec::new();
    if let Some(b) = &event.before {
        rows.extend([("before", b), ("payload.before", b), ("data.before", b)]);
    }
    if let Some(a) = &event.after {
        rows.extend([("after", a), ("payload.after", a), ("data.after", a)]);
    }
    rows
}

/// The JSON an inferred (Path C) schema is built from and encoded with:
/// the envelope with each row image as exact JSON (integers as numbers,
/// JSON columns and documents as their canonical text). An unchanged TOAST
/// value has no value to infer from: a conversion failure.
fn inference_json(
    mut envelope: serde_json::Value,
    event: &crate::Event,
) -> Result<serde_json::Value, EncodingError> {
    use crate::canonical::view::value_view;
    use crate::canonical::{CanonicalValue, DbValue};
    let row_json = |image: &str,
                    row: &CanonicalRow|
     -> Result<serde_json::Value, EncodingError> {
        let mut m = serde_json::Map::new();
        for (name, v) in row.iter() {
            let j = match v {
                CanonicalValue::Unchanged => {
                    return Err(EncodingError::Conversion(
                        super::ConversionFailure {
                            column: format!("{image}.{name}"),
                            source_type: "unchanged TOAST".into(),
                            target_type: "Avro (inferred)".into(),
                            reason: "PostgreSQL did not send the value",
                        },
                    ));
                }
                CanonicalValue::Db(DbValue::Json(j)) => {
                    serde_json::Value::String(j.as_str().to_string())
                }
                CanonicalValue::Document(d) => {
                    serde_json::Value::String(d.canonical_text())
                }
                other => value_view(other, event.type_mapping)
                    .expect("not unchanged"),
            };
            m.insert(name.to_string(), j);
        }
        for (name, d) in row.extensions() {
            if m.contains_key(name) {
                return Err(EncodingError::Conversion(
                    super::ConversionFailure {
                        column: format!("{image}.{name}"),
                        source_type: "processor extension".into(),
                        target_type: "Avro (inferred)".into(),
                        reason: "an extension has a column's name",
                    },
                ));
            }
            m.insert(
                name.clone(),
                serde_json::Value::String(d.canonical_text()),
            );
        }
        Ok(serde_json::Value::Object(m))
    };
    for (path, row) in row_paths(event) {
        let (parent, image) = match path.split_once('.') {
            Some((p, i)) => (Some(p), i),
            None => (None, path),
        };
        let target = match parent {
            Some(p) => envelope.get_mut(p),
            None => Some(&mut envelope),
        };
        if let Some(serde_json::Value::Object(obj)) = target
            && obj.contains_key(image)
        {
            obj.insert(image.to_string(), row_json(image, row)?);
        }
    }
    Ok(envelope)
}

/// `schema_json` with `deltaforge.type_mapping` on its top-level record.
fn with_mapping_attr(
    schema_json: &str,
    mapping: crate::TypeMapping,
) -> Result<(String, Arc<AvroSchema>), EncodingError> {
    let mut v: serde_json::Value = serde_json::from_str(schema_json)
        .map_err(|e| EncodingError::Avro(format!("schema JSON: {e}")))?;
    if let serde_json::Value::Object(o) = &mut v {
        o.insert("deltaforge.type_mapping".into(), mapping.as_str().into());
    }
    let json = v.to_string();
    let schema = AvroSchema::parse_str(&json)
        .map_err(|e| EncodingError::Avro(format!("schema: {e}")))?;
    Ok((json, Arc::new(schema)))
}

/// Where a conversion is: the named types, the field path and the field's
/// declared source type. The path is a chain of borrowed steps, written
/// out only for a failure (or to find a row image).
struct Cx<'a> {
    names: &'a Names,
    /// The enclosing context; `None` at the root.
    parent: Option<&'a Cx<'a>>,
    step: Step<'a>,
    source: Option<&'a str>,
    /// The event's canonical row images by envelope path (`after`,
    /// `payload.after`, ...): converted from their values, never from JSON.
    rows: &'a [(&'a str, &'a CanonicalRow)],
}

/// One step of a field path.
#[derive(Clone, Copy)]
enum Step<'a> {
    Root,
    Field(&'a str),
    Item,
}

impl<'a> Cx<'a> {
    fn root(names: &'a Names, rows: &'a [(&'a str, &'a CanonicalRow)]) -> Self {
        Cx {
            names,
            parent: None,
            step: Step::Root,
            source: None,
            rows,
        }
    }

    /// The field path (`after.id`, `tags[]`).
    fn path(&self) -> String {
        let mut out = String::new();
        self.write_path(&mut out);
        out
    }

    fn write_path(&self, out: &mut String) {
        if let Some(p) = self.parent {
            p.write_path(out);
        }
        match self.step {
            Step::Root => {}
            Step::Field(name) => {
                if !out.is_empty() {
                    out.push('.');
                }
                out.push_str(name);
            }
            Step::Item => out.push_str("[]"),
        }
    }

    /// Whether the field path is exactly `path` (written out by
    /// [`Self::path`]), without writing it.
    fn path_is(&self, path: &str) -> bool {
        match (self.step, self.parent) {
            (Step::Root, _) | (_, None) => path.is_empty(),
            (Step::Item, Some(p)) => {
                path.strip_suffix("[]").is_some_and(|rest| p.path_is(rest))
            }
            (Step::Field(name), Some(p)) => match path.strip_suffix(name) {
                None => false,
                Some("") => p.path_is(""),
                Some(rest) => rest.strip_suffix('.').is_some_and(|r| {
                    // An empty parent path writes no separator.
                    !r.is_empty() && p.path_is(r)
                }),
            },
        }
    }

    fn row(&self) -> Option<&CanonicalRow> {
        self.rows
            .iter()
            .find(|(p, _)| self.path_is(p))
            .map(|(_, r)| *r)
    }

    fn field<'b>(&'b self, name: &'b str, source: Option<&'b str>) -> Cx<'b> {
        Cx {
            rows: self.rows,
            names: self.names,
            parent: Some(self),
            step: Step::Field(name),
            source,
        }
    }

    fn item(&self) -> Cx<'_> {
        Cx {
            rows: self.rows,
            names: self.names,
            parent: Some(self),
            step: Step::Item,
            source: self.source,
        }
    }

    fn fail(
        &self,
        json: &serde_json::Value,
        target: &str,
        reason: &'static str,
    ) -> EncodingError {
        EncodingError::Conversion(super::ConversionFailure {
            column: self.path(),
            source_type: self
                .source
                .map(str::to_string)
                .unwrap_or_else(|| format!("JSON {}", json_kind(json))),
            target_type: format!("Avro {target}"),
            reason,
        })
    }

    /// Whether the field's source type starts with `prefix` (ASCII case
    /// insensitive).
    fn source_is(&self, prefixes: &[&str]) -> bool {
        self.source.is_some_and(|s| {
            prefixes.iter().any(|p| {
                s.len() >= p.len()
                    && s.as_bytes()[..p.len()]
                        .eq_ignore_ascii_case(p.as_bytes())
            })
        })
    }
}

/// The `deltaforge.source_type` a DDL-derived schema records on a field.
const SOURCE_TYPE_ATTR: &str = "deltaforge.source_type";

/// A record whose every input field must be defined by the schema.
const STRICT_ATTR: &str = "deltaforge.strict";

const MICROS_PER_DAY: i64 = 86_400_000_000;

fn convert(
    json: &serde_json::Value,
    schema: &AvroSchema,
    cx: &Cx<'_>,
) -> Result<AvroValue, EncodingError> {
    use serde_json::Value as J;
    match (schema, json) {
        (AvroSchema::Null, J::Null) => Ok(AvroValue::Null),
        (AvroSchema::Union(u), _) => convert_union(json, u, cx),
        (AvroSchema::Ref { name }, _) => {
            let target = cx
                .names
                .get(name)
                .or_else(|| {
                    cx.names
                        .iter()
                        .find(|(n, _)| n.name == name.name)
                        .map(|(_, s)| s)
                })
                .ok_or_else(|| {
                    cx.fail(json, "named type", "unresolved named reference")
                })?;
            convert(json, target, cx)
        }
        // A row image the event carries canonically.
        (AvroSchema::Record(r), _)
            if r.fields.iter().any(|f| {
                f.custom_attributes.contains_key(SOURCE_TYPE_ATTR)
            }) && cx.row().is_some() =>
        {
            canonical_rows::convert_row(cx.row().expect("checked"), r, cx)
        }
        (_, J::Null) => {
            Err(cx.fail(json, kind_of(schema), "null for a non-nullable field"))
        }

        (AvroSchema::Boolean, J::Bool(b)) => Ok(AvroValue::Boolean(*b)),
        (AvroSchema::Int, J::Number(n)) => n
            .as_i64()
            .and_then(|v| i32::try_from(v).ok())
            .map(AvroValue::Int)
            .ok_or_else(|| {
                cx.fail(
                    json,
                    "int",
                    "not an integer in the signed 32-bit range",
                )
            }),
        (AvroSchema::Long, J::Number(n)) => {
            n.as_i64().map(AvroValue::Long).ok_or_else(|| {
                cx.fail(
                    json,
                    "long",
                    "not an integer in the signed 64-bit range",
                )
            })
        }
        (AvroSchema::Float, J::Number(n)) => exact_f64(n)
            .filter(|f| (*f as f32) as f64 == *f)
            .map(|f| AvroValue::Float(f as f32))
            .ok_or_else(|| {
                cx.fail(
                    json,
                    "float",
                    "not exactly representable as a 32-bit float",
                )
            }),
        (AvroSchema::Double, J::Number(n)) => {
            exact_f64(n).map(AvroValue::Double).ok_or_else(|| {
                cx.fail(
                    json,
                    "double",
                    "not exactly representable as a 64-bit float",
                )
            })
        }

        (AvroSchema::String, _) => {
            convert_string(json, cx).map(AvroValue::String)
        }
        (AvroSchema::Uuid, J::String(s)) => Ok(AvroValue::String(s.clone())),
        (AvroSchema::Bytes, _) => {
            convert_bytes(json, cx, "bytes").map(AvroValue::Bytes)
        }
        (AvroSchema::Fixed(f), _) => {
            let b = convert_bytes(json, cx, "fixed")?;
            if b.len() == f.size {
                Ok(AvroValue::Fixed(f.size, b))
            } else {
                Err(cx.fail(
                    json,
                    "fixed",
                    "byte length differs from the fixed size",
                ))
            }
        }
        (AvroSchema::Enum(e), J::String(s)) => e
            .symbols
            .iter()
            .position(|x| x == s)
            .map(|i| AvroValue::Enum(i as u32, s.clone()))
            .ok_or_else(|| cx.fail(json, "enum", "not a symbol of the enum")),

        (AvroSchema::Date, J::Number(n)) => n
            .as_i64()
            .and_then(|v| i32::try_from(v).ok())
            .map(AvroValue::Date)
            .ok_or_else(|| {
                cx.fail(json, "date", "not a day count in the 32-bit range")
            }),
        (AvroSchema::Date, J::String(s)) => parse_date(s)
            .and_then(|d| i32::try_from(d).ok())
            .map(AvroValue::Date)
            .ok_or_else(|| {
                cx.fail(json, "date", "not a calendar date (YYYY-MM-DD)")
            }),
        (AvroSchema::TimeMillis, J::String(s)) => parse_time_of_day(s)
            .filter(|us| us % 1_000 == 0)
            .map(|us| AvroValue::TimeMillis((us / 1_000) as i32))
            .ok_or_else(|| {
                cx.fail(
                    json,
                    "time-millis",
                    "not a time of day with at most millisecond precision",
                )
            }),
        (AvroSchema::TimeMicros, J::String(s)) => parse_time_of_day(s)
            .map(AvroValue::TimeMicros)
            .ok_or_else(|| {
                cx.fail(
                    json,
                    "time-micros",
                    "not a time of day with at most microsecond precision",
                )
            }),
        (AvroSchema::TimeMillis, J::Number(n)) => n
            .as_i64()
            .filter(|v| (0..86_400_000).contains(v))
            .map(|v| AvroValue::TimeMillis(v as i32))
            .ok_or_else(|| {
                cx.fail(
                    json,
                    "time-millis",
                    "not a time of day in milliseconds",
                )
            }),
        (AvroSchema::TimeMicros, J::Number(n)) => n
            .as_i64()
            .filter(|v| (0..MICROS_PER_DAY).contains(v))
            .map(AvroValue::TimeMicros)
            .ok_or_else(|| {
                cx.fail(
                    json,
                    "time-micros",
                    "not a time of day in microseconds",
                )
            }),
        // An epoch number is accepted only where the source declares its
        // unit: MySQL TIMESTAMP values are epoch microseconds.
        (AvroSchema::TimestampMicros, J::Number(n))
            if cx.source_is(&["timestamp"]) =>
        {
            n.as_i64().map(AvroValue::TimestampMicros).ok_or_else(|| {
                cx.fail(
                    json,
                    "timestamp-micros",
                    "not an integer in the signed 64-bit range",
                )
            })
        }
        (
            AvroSchema::TimestampMillis | AvroSchema::TimestampMicros,
            J::Number(_),
        ) => Err(cx.fail(
            json,
            kind_of(schema),
            "an epoch number whose unit the source does not declare",
        )),
        (AvroSchema::TimestampMillis, J::String(s)) => parse_instant_micros(s)
            .filter(|us| us % 1_000 == 0)
            .map(|us| AvroValue::TimestampMillis(us / 1_000))
            .ok_or_else(|| {
                cx.fail(
                    json,
                    "timestamp-millis",
                    "not a date-time with at most millisecond precision",
                )
            }),
        (AvroSchema::TimestampMicros, J::String(s)) => parse_instant_micros(s)
            .map(AvroValue::TimestampMicros)
            .ok_or_else(|| {
                cx.fail(
                    json,
                    "timestamp-micros",
                    "not a date-time with at most microsecond precision",
                )
            }),
        (AvroSchema::LocalTimestampMillis, J::String(s)) => {
            parse_local_micros(s)
                .filter(|us| us % 1_000 == 0)
                .map(|us| AvroValue::LocalTimestampMillis(us / 1_000))
                .ok_or_else(|| {
                    cx.fail(
                        json,
                        "local-timestamp-millis",
                        "not a date-time with at most millisecond precision",
                    )
                })
        }
        (AvroSchema::LocalTimestampMicros, J::String(s)) => {
            parse_local_micros(s)
                .map(AvroValue::LocalTimestampMicros)
                .ok_or_else(|| {
                    cx.fail(
                        json,
                        "local-timestamp-micros",
                        "not a date-time with at most microsecond precision",
                    )
                })
        }

        (AvroSchema::Record(r), J::Object(map)) => {
            // A row image (fields carrying their source column type) or a
            // strict record (a DDL payload) must represent every input
            // field: an unknown one fails before any record exists, never
            // dropped.
            let strict = r
                .attributes
                .get(STRICT_ATTR)
                .is_some_and(|v| v.as_bool() == Some(true))
                || r.fields.iter().any(|f| {
                    f.custom_attributes.contains_key(SOURCE_TYPE_ATTR)
                });
            if strict
                && let Some((k, v)) =
                    map.iter().find(|(k, _)| !r.lookup.contains_key(*k))
            {
                return Err(cx.field(k, None).fail(
                    v,
                    "record",
                    "a column the event's schema version does not define",
                ));
            }
            let mut fields = Vec::with_capacity(r.fields.len());
            for field in &r.fields {
                let source = field
                    .custom_attributes
                    .get(SOURCE_TYPE_ATTR)
                    .and_then(|v| v.as_str());
                let value = map.get(&field.name).unwrap_or(&J::Null);
                let v = convert(
                    value,
                    &field.schema,
                    &cx.field(&field.name, source),
                )?;
                fields.push((field.name.clone(), v));
            }
            Ok(AvroValue::Record(fields))
        }
        (AvroSchema::Array(a), J::Array(items)) => {
            let item_cx = cx.item();
            items
                .iter()
                .map(|v| convert(v, &a.items, &item_cx))
                .collect::<Result<Vec<_>, _>>()
                .map(AvroValue::Array)
        }
        (AvroSchema::Map(m), J::Object(map)) => {
            let item_cx = cx.item();
            map.iter()
                .map(|(k, v)| {
                    convert(v, &m.types, &item_cx).map(|av| (k.clone(), av))
                })
                .collect::<Result<HashMap<_, _>, _>>()
                .map(AvroValue::Map)
        }
        (
            AvroSchema::Decimal(_)
            | AvroSchema::BigDecimal
            | AvroSchema::Duration,
            _,
        ) => Err(cx.fail(
            json,
            kind_of(schema),
            "target type not supported by the converter",
        )),
        _ => Err(cx.fail(
            json,
            kind_of(schema),
            "the value's kind does not match the target type",
        )),
    }
}

fn convert_union(
    json: &serde_json::Value,
    union: &apache_avro::schema::UnionSchema,
    cx: &Cx<'_>,
) -> Result<AvroValue, EncodingError> {
    let variants = union.variants();
    // A row image the event carries canonically is present whatever its
    // JSON rendering.
    if json.is_null() && cx.row().is_none() {
        return variants
            .iter()
            .position(|v| matches!(v, AvroSchema::Null))
            .map(|i| AvroValue::Union(i as u32, Box::new(AvroValue::Null)))
            .ok_or_else(|| {
                cx.fail(json, "union", "null for a non-nullable field")
            });
    }
    let mut last = None;
    for (i, variant) in variants.iter().enumerate() {
        if matches!(variant, AvroSchema::Null) {
            continue;
        }
        match convert(json, variant, cx) {
            Ok(v) => return Ok(AvroValue::Union(i as u32, Box::new(v))),
            Err(e) => last = Some(e),
        }
    }
    Err(last.unwrap_or_else(|| {
        cx.fail(json, "union", "no branch accepts the value")
    }))
}

/// Text for a `string` field: a string as is; an integer or decimal number
/// as its exact decimal text (BIGINT UNSIGNED in string mode, numbers of
/// inferred schemas); `{"_base64"}` text that is valid UTF-8; a JSON
/// column's structured value as its JSON text. An ENUM or SET index is
/// never accepted for its label, and text bytes are never replaced.
fn convert_string(
    json: &serde_json::Value,
    cx: &Cx<'_>,
) -> Result<String, EncodingError> {
    use serde_json::Value as J;
    let json_source = cx.source.is_none() || cx.source_is(&["json"]);
    match json {
        J::String(s) => Ok(s.clone()),
        J::Number(_) if cx.source_is(&["enum", "set"]) => {
            Err(cx.fail(json, "string", "an index cannot stand for its label"))
        }
        J::Number(n) => Ok(n.to_string()),
        J::Object(m) if m.len() == 1 && m.contains_key("_base64") => {
            let bytes = m["_base64"]
                .as_str()
                .and_then(base64_decode)
                .ok_or_else(|| cx.fail(json, "string", "not valid base64"))?;
            String::from_utf8(bytes).map_err(|_| {
                cx.fail(json, "string", "bytes are not valid UTF-8 text")
            })
        }
        J::Bool(_) | J::Array(_) | J::Object(_) if json_source => {
            Ok(json.to_string())
        }
        _ => {
            Err(cx.fail(json, "string", "a structured value for a text column"))
        }
    }
}

/// Bytes for a `bytes` or `fixed` field: a string's exact UTF-8 bytes or a
/// strictly decoded `{"_base64"}`. Numbers are never rendered as text.
fn convert_bytes(
    json: &serde_json::Value,
    cx: &Cx<'_>,
    target: &str,
) -> Result<Vec<u8>, EncodingError> {
    use serde_json::Value as J;
    match json {
        J::String(s) => Ok(s.as_bytes().to_vec()),
        J::Object(m) if m.len() == 1 && m.contains_key("_base64") => {
            m["_base64"]
                .as_str()
                .and_then(base64_decode)
                .ok_or_else(|| cx.fail(json, target, "not valid base64"))
        }
        _ => Err(cx.fail(json, target, "not bytes")),
    }
}

/// `n` as an `f64` when the conversion is exact.
fn exact_f64(n: &serde_json::Number) -> Option<f64> {
    if let Some(i) = n.as_i64() {
        let f = i as f64;
        return (f as i128 == i as i128).then_some(f);
    }
    if let Some(u) = n.as_u64() {
        let f = u as f64;
        return (f as u128 == u as u128 && f < 18_446_744_073_709_551_616.0)
            .then_some(f);
    }
    n.as_f64().filter(|f| f.is_finite())
}

/// Days since 1970-01-01 of a `YYYY-MM-DD` calendar date (zero and invalid
/// dates are not dates).
fn parse_date(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    let shape = b.len() == 10
        && b[4] == b'-'
        && b[7] == b'-'
        && b.iter()
            .enumerate()
            .all(|(i, c)| i == 4 || i == 7 || c.is_ascii_digit());
    if !shape {
        return None;
    }
    let d = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
    Some((d - epoch).num_days())
}

/// Microseconds since midnight of `HH:MM:SS[.f{1,6}]`, a time of day
/// (`00:00:00` to `23:59:59.999999`).
fn parse_time_of_day(s: &str) -> Option<i64> {
    let (hms, frac) = match s.split_once('.') {
        Some((a, b)) => (a, Some(b)),
        None => (s, None),
    };
    let mut parts = hms.split(':');
    let (h, m, sec) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() || h.len() != 2 || m.len() != 2 || sec.len() != 2
    {
        return None;
    }
    let digits = |x: &str| x.bytes().all(|b| b.is_ascii_digit());
    if !(digits(h) && digits(m) && digits(sec)) {
        return None;
    }
    let (h, m, sec): (i64, i64, i64) =
        (h.parse().ok()?, m.parse().ok()?, sec.parse().ok()?);
    if h > 23 || m > 59 || sec > 59 {
        return None;
    }
    let micros = match frac {
        None => 0,
        Some(f) if !f.is_empty() && f.len() <= 6 && digits(f) => {
            f.parse::<i64>().ok()? * 10_i64.pow(6 - f.len() as u32)
        }
        Some(_) => return None,
    };
    Some(((h * 60 + m) * 60 + sec) * 1_000_000 + micros)
}

/// A date-time `YYYY-MM-DD[ T]HH:MM:SS[.f{1,6}]`, split from an optional
/// UTC offset (`Z`, `+HH`, `+HH:MM`, `+HHMM`): (naive micros since the
/// epoch, offset seconds).
fn parse_date_time(s: &str) -> Option<(i64, Option<i64>)> {
    let sep = s.find([' ', 'T'])?;
    let (date, rest) = (&s[..sep], &s[sep + 1..]);
    let (time, offset) = if let Some(t) = rest.strip_suffix('Z') {
        (t, Some(0))
    } else if let Some(i) = rest.rfind(['+', '-']) {
        let (t, o) = rest.split_at(i);
        let sign = if o.starts_with('-') { -1 } else { 1 };
        let o = o[1..].replace(':', "");
        let secs = match o.len() {
            2 => o.parse::<i64>().ok()? * 3_600,
            4 => {
                o[..2].parse::<i64>().ok()? * 3_600
                    + o[2..].parse::<i64>().ok()? * 60
            }
            _ => return None,
        };
        (t, Some(sign * secs))
    } else {
        (rest, None)
    };
    let days = parse_date(date)?;
    let tod = parse_time_of_day(time)?;
    Some((days * MICROS_PER_DAY + tod, offset))
}

/// An instant in epoch microseconds. A date-time without an offset is read
/// as UTC (the `timestamp` mode of naive date-times).
fn parse_instant_micros(s: &str) -> Option<i64> {
    let (naive, offset) = parse_date_time(s)?;
    naive.checked_sub(offset.unwrap_or(0) * 1_000_000)
}

/// A local (zone-free) date-time in microseconds; an offset is refused.
fn parse_local_micros(s: &str) -> Option<i64> {
    match parse_date_time(s)? {
        (naive, None) => Some(naive),
        _ => None,
    }
}

/// The JSON kind of a value, for failures without a declared source type.
fn json_kind(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(n) if n.is_i64() || n.is_u64() => "integer",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

/// The Avro type name of `schema` for failures.
fn kind_of(schema: &AvroSchema) -> &'static str {
    match schema {
        AvroSchema::Null => "null",
        AvroSchema::Boolean => "boolean",
        AvroSchema::Int => "int",
        AvroSchema::Long => "long",
        AvroSchema::Float => "float",
        AvroSchema::Double => "double",
        AvroSchema::Bytes => "bytes",
        AvroSchema::String => "string",
        AvroSchema::Array(_) => "array",
        AvroSchema::Map(_) => "map",
        AvroSchema::Union(_) => "union",
        AvroSchema::Record(_) => "record",
        AvroSchema::Enum(_) => "enum",
        AvroSchema::Fixed(_) => "fixed",
        AvroSchema::Decimal(_) | AvroSchema::BigDecimal => "decimal",
        AvroSchema::Uuid => "uuid",
        AvroSchema::Date => "date",
        AvroSchema::TimeMillis => "time-millis",
        AvroSchema::TimeMicros => "time-micros",
        AvroSchema::TimestampMillis => "timestamp-millis",
        AvroSchema::TimestampMicros => "timestamp-micros",
        AvroSchema::TimestampNanos => "timestamp-nanos",
        AvroSchema::LocalTimestampMillis => "local-timestamp-millis",
        AvroSchema::LocalTimestampMicros => "local-timestamp-micros",
        AvroSchema::LocalTimestampNanos => "local-timestamp-nanos",
        AvroSchema::Duration => "duration",
        AvroSchema::Ref { .. } => "named type",
    }
}

/// Decode a base64 string. Returns None if decoding fails.
fn base64_decode(input: &str) -> Option<Vec<u8>> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.decode(input).ok()
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct UnavailableSchemas;

    impl SourceSchemaProvider for UnavailableSchemas {
        fn get_envelope_schema(
            &self,
            _connector: &str,
            _database: &str,
            _namespace: &str,
            _table: &str,
            _identity: &SchemaIdentity,
        ) -> EnvelopeSchemaLookup {
            EnvelopeSchemaLookup::Unavailable(
                "schema lineage not established".into(),
            )
        }

        fn mapping_id(&self) -> &str {
            "test"
        }
    }

    /// The schema the inference derives from `value` alone.
    fn derive_avro_schema(
        value: &serde_json::Value,
        _record_name: Option<&str>,
    ) -> String {
        let mut s = Inferred::new("deltaforge.Event");
        s.absorb(value.as_object().expect("an object"));
        s.schema_json()
    }

    #[tokio::test]
    async fn unavailable_source_schema_fails_retryably_without_inferring() {
        // The registry URL is unreachable: falling back to JSON inference would
        // try to register an inferred schema and fail with a registry error.
        // An unavailable source schema must instead stop before any registry
        // call, with the dedicated retryable error.
        let encoder = AvroEncoder::with_source_schemas(
            "http://127.0.0.1:1",
            SubjectStrategy::TopicName,
            None,
            None,
            Some(Arc::new(UnavailableSchemas)),
        )
        .unwrap();
        let mut event = crate::Event::new_row(
            crate::EventId::mysql_row_server(1, "t", 1, 0),
            crate::SourceInfo {
                version: "t".into(),
                connector: "mysql".into(),
                name: "p".into(),
                ts_ms: 1,
                db: "shop".into(),
                schema: None,
                table: "orders".into(),
                snapshot: None,
                position: Default::default(),
            },
            crate::Op::Create,
            None,
            Some(crate::canonical::fixture::row(json!({"id": 1}))),
            crate::TypeMapping::V2,
            1,
            1,
        );
        event.schema_version = Some("h1".into());
        event.schema_sequence = Some(1);
        let native = crate::envelope::EnvelopeType::Native.build();
        let envelope =
            native.wrap(&event, crate::NativeOpts::default()).unwrap();
        let err = encoder
            .encode_event("orders", &event, &envelope)
            .await
            .unwrap_err();
        assert!(
            matches!(err, EncodingError::SchemaUnavailable(ref m) if m.contains("lineage")),
            "{err:?}"
        );
        assert!(matches!(
            err.into_sink_error(),
            crate::SinkError::Backpressure { .. }
        ));
    }

    #[test]
    fn test_subject_strategy_topic_name() {
        let s = SubjectStrategy::TopicName;
        assert_eq!(s.resolve("my-topic", None), "my-topic-value");
    }

    #[test]
    fn test_subject_strategy_record_name() {
        let s = SubjectStrategy::RecordName;
        assert_eq!(
            s.resolve("my-topic", Some("com.acme.Order")),
            "com.acme.Order"
        );
        assert_eq!(s.resolve("my-topic", None), "deltaforge.Event");
    }

    #[test]
    fn test_subject_strategy_topic_record_name() {
        let s = SubjectStrategy::TopicRecordName;
        assert_eq!(
            s.resolve("my-topic", Some("com.acme.Order")),
            "my-topic-com.acme.Order"
        );
    }

    #[test]
    fn test_derive_schema_simple_object() {
        let value = json!({
            "id": 42,
            "name": "Alice",
            "active": true,
            "score": 3.15
        });
        let schema_json = derive_avro_schema(&value, None);
        // Should parse as valid Avro schema
        let schema = AvroSchema::parse_str(&schema_json);
        assert!(
            schema.is_ok(),
            "derived schema should be valid Avro: {:?}",
            schema.err()
        );
    }

    #[test]
    fn test_derive_schema_with_nested_object() {
        let value = json!({
            "id": 1,
            "source": {
                "db": "mydb",
                "table": "orders"
            }
        });
        let schema_json = derive_avro_schema(&value, None);
        let schema = AvroSchema::parse_str(&schema_json);
        assert!(
            schema.is_ok(),
            "nested schema should be valid: {:?}",
            schema.err()
        );
    }

    #[test]
    fn test_derive_schema_with_null_fields() {
        let value = json!({
            "id": 1,
            "deleted_at": null
        });
        let schema_json = derive_avro_schema(&value, None);
        let schema = AvroSchema::parse_str(&schema_json);
        assert!(
            schema.is_ok(),
            "schema with null fields should be valid: {:?}",
            schema.err()
        );
    }

    #[test]
    fn resolved_keys_differ_whenever_a_part_differs() {
        let event = |db: &str, table: &str, seq: Option<u64>| {
            let mut e = crate::Event::new_row(
                crate::EventId::mysql_row_server(1, "f", 1, 0),
                crate::SourceInfo {
                    version: "v".into(),
                    connector: "mysql".into(),
                    name: "src".into(),
                    ts_ms: 0,
                    db: db.into(),
                    schema: None,
                    table: table.into(),
                    snapshot: None,
                    position: Default::default(),
                },
                crate::Op::Create,
                None,
                Some(crate::canonical::fixture::row(json!({"id": 1}))),
                crate::TypeMapping::V2,
                0,
                1,
            );
            e.schema_sequence = seq;
            e
        };
        let key = |topic: &str, e: &crate::Event, fp: &str, m: &str| {
            let mut k = String::new();
            resolved_key(&mut k, topic, e, fp, m);
            k
        };
        let base = event("a", "b", Some(1));
        let k = key("t", &base, "fp", "m");
        assert_eq!(k, key("t", &event("a", "b", Some(1)), "fp", "m"));
        // A separator moved between parts, or any one part changed.
        for other in [
            key("t", &event("a1", "b", Some(1)), "fp", "m"),
            key("t", &event("a", "1b", Some(1)), "fp", "m"),
            key("t", &event("a:", "b", Some(1)), "fp", "m"),
            key("t", &event("a", "b", Some(11)), "fp", "m"),
            key("t", &event("a", "b", None), "fp", "m"),
            key("t2", &base, "fp", "m"),
            key("t", &base, "fp2", "m"),
            key("t", &base, "fp", "m2"),
            key("t", &event("a", "b", Some(1)), "f", "pm"),
        ] {
            assert_ne!(k, other);
        }
        let mut v1 = event("a", "b", Some(1));
        v1.type_mapping = crate::TypeMapping::V1;
        assert_ne!(k, key("t", &v1, "fp", "m"));
    }

    #[test]
    fn field_paths_match_exactly_without_being_written() {
        let names = Names::new();
        let root = Cx::root(&names, &[]);
        let payload = root.field("payload", None);
        let after = payload.field("after", None);
        assert_eq!(after.path(), "payload.after");
        assert!(after.path_is("payload.after"));
        for other in [
            "after",
            "xpayload.after",
            "payload.xafter",
            "payload..after",
            ".after",
            "",
        ] {
            assert!(!after.path_is(other), "{other}");
        }
        let top = root.field("after", None);
        assert!(top.path_is("after"));
        assert!(!top.path_is("payload.after"));
        let tags = after.field("tags", Some("set('a')"));
        let item = tags.item();
        assert_eq!(item.path(), "payload.after.tags[]");
        assert!(item.path_is("payload.after.tags[]"));
        assert!(!item.path_is("payload.after.tags"));
        assert!(item.source_is(&["SET"]));
        assert!(root.path_is(""));
    }

    #[test]
    fn the_wire_format_is_the_magic_byte_the_schema_id_then_the_datum() {
        let schema = Arc::new(
            AvroSchema::parse_str(
                r#"{"type": "record", "name": "R", "fields": [
                    {"name": "a", "type": "long"},
                    {"name": "s", "type": ["null", "string"]}
                ]}"#,
            )
            .unwrap(),
        );
        let prepared = Prepared::new(42, schema.clone()).unwrap();
        // Twice: the writer's buffer is reused.
        for _ in 0..2 {
            let bytes = encode_with(&prepared, &json!({"a": 7, "s": "x"}), &[])
                .unwrap();
            assert_eq!(bytes[0], 0x00);
            assert_eq!(bytes[1..5], 42u32.to_be_bytes());
            let datum = apache_avro::to_avro_datum(
                &schema,
                AvroValue::Record(vec![
                    ("a".into(), AvroValue::Long(7)),
                    (
                        "s".into(),
                        AvroValue::Union(
                            1,
                            Box::new(AvroValue::String("x".into())),
                        ),
                    ),
                ]),
            )
            .unwrap();
            assert_eq!(bytes[5..], datum[..]);
        }
    }

    // ── json_to_avro tests ──────────────────────────────────────────────

    /// Helper: derive schema from JSON, parse it, convert value, encode to datum.
    /// This exercises the full path that was failing in production.
    fn roundtrip_json_to_avro(value: &serde_json::Value) {
        let schema_json = derive_avro_schema(value, None);
        let schema = AvroSchema::parse_str(&schema_json)
            .expect("derived schema should parse");
        let avro_value =
            json_to_avro(value, &schema).expect("json_to_avro should succeed");
        let encoded = apache_avro::to_avro_datum(&schema, avro_value);
        assert!(
            encoded.is_ok(),
            "to_avro_datum should succeed: {:?}",
            encoded.err()
        );
    }

    #[test]
    fn json_to_avro_simple_object() {
        roundtrip_json_to_avro(&json!({
            "id": 42,
            "name": "Alice",
            "active": true
        }));
    }

    #[test]
    fn json_to_avro_with_nulls() {
        roundtrip_json_to_avro(&json!({
            "id": 1,
            "name": "Bob",
            "deleted_at": null,
            "notes": null
        }));
    }

    #[test]
    fn json_to_avro_nested_object_in_union() {
        // This is the exact pattern that was failing: nested objects
        // inside nullable unions (CDC events have before/after/source as
        // nullable records).
        roundtrip_json_to_avro(&json!({
            "before": null,
            "after": {"id": 1, "name": "Alice", "email": "alice@test.com"},
            "source": {
                "connector": "mysql",
                "db": "orders",
                "table": "customers",
                "ts_ms": 1700000000000_i64
            },
            "op": "c",
            "ts_ms": 1700000000000_i64
        }));
    }

    #[test]
    fn json_to_avro_all_null_nested() {
        // DELETE event: after is null, before has data
        roundtrip_json_to_avro(&json!({
            "before": {"id": 1, "name": "Alice"},
            "after": null,
            "op": "d",
            "ts_ms": 1700000000000_i64
        }));
    }

    #[test]
    fn json_to_avro_mixed_types() {
        roundtrip_json_to_avro(&json!({
            "id": 42,
            "score": 3.15,
            "active": true,
            "name": "test",
            "tags": ["a", "b", "c"],
            "meta": null
        }));
    }

    #[test]
    fn json_to_avro_with_integer_types() {
        // Verify int vs long handling in unions
        roundtrip_json_to_avro(&json!({
            "small_int": 42,
            "big_int": 1700000000000_i64,
            "float_val": 3.15,
            "flag": true
        }));
    }

    // ── Edge case: union null handling ──────────────────────────────────

    #[test]
    fn json_to_avro_null_in_nullable_string() {
        let schema_json = r#"["null", "string"]"#;
        let schema = AvroSchema::parse_str(schema_json).unwrap();
        let val = json_to_avro(&json!(null), &schema).unwrap();
        assert!(
            matches!(val, AvroValue::Union(0, _)),
            "null should map to union branch 0: {val:?}"
        );
    }

    #[test]
    fn json_to_avro_value_in_nullable_string() {
        let schema_json = r#"["null", "string"]"#;
        let schema = AvroSchema::parse_str(schema_json).unwrap();
        let val = json_to_avro(&json!("hello"), &schema).unwrap();
        match &val {
            AvroValue::Union(idx, inner) => {
                assert_eq!(*idx, 1);
                assert!(matches!(inner.as_ref(), AvroValue::String(_)));
            }
            _ => panic!("expected Union, got {val:?}"),
        }
    }

    // ---- per-family conversion (slice 1: fail closed) ----------------------

    /// One record `After` with a single field `c` of `avro_type`, recording
    /// `source` as its `deltaforge.source_type` (as DDL-derived schemas do).
    fn field_schema(
        avro_type: serde_json::Value,
        source: Option<&str>,
    ) -> AvroSchema {
        let mut field =
            json!({"name": "c", "type": ["null", avro_type], "default": null});
        if let Some(src) = source {
            field["deltaforge.source_type"] = json!(src);
        }
        AvroSchema::parse(
            &json!({"type": "record", "name": "After", "fields": [field]}),
        )
        .unwrap()
    }

    fn one(
        v: serde_json::Value,
        ty: serde_json::Value,
        src: Option<&str>,
    ) -> Result<AvroValue, EncodingError> {
        json_to_avro(&json!({"c": v}), &field_schema(ty, src))
    }

    fn ok(
        v: serde_json::Value,
        ty: serde_json::Value,
        src: Option<&str>,
    ) -> AvroValue {
        match one(v.clone(), ty.clone(), src)
            .unwrap_or_else(|e| panic!("{v} as {ty}: {e}"))
        {
            AvroValue::Record(mut f) => match f.remove(0).1 {
                AvroValue::Union(_, inner) => *inner,
                other => other,
            },
            other => panic!("{other:?}"),
        }
    }

    /// The typed failure for `v`; it never contains the value's text.
    fn refused(
        v: serde_json::Value,
        ty: serde_json::Value,
        src: Option<&str>,
    ) -> super::super::ConversionFailure {
        match one(v.clone(), ty.clone(), src) {
            Err(EncodingError::Conversion(f)) => {
                let text = f.to_string();
                let shown = match &v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                if shown.len() > 3 {
                    assert!(!text.contains(&shown), "the value leaked: {text}");
                }
                assert_eq!(f.column, "c");
                f
            }
            other => panic!(
                "{v} as {ty}: expected a conversion failure, got {other:?}"
            ),
        }
    }

    #[test]
    fn unsigned_bigint_is_exact_or_refused() {
        let src = Some("bigint unsigned");
        assert_eq!(
            ok(json!(i64::MAX), json!("long"), src),
            AvroValue::Long(i64::MAX)
        );
        for v in [json!(i64::MAX as u64 + 1), json!(u64::MAX)] {
            let f = refused(v, json!("long"), src);
            assert_eq!(f.source_type, "bigint unsigned");
            assert_eq!(f.target_type, "Avro long");
        }
        for v in [i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX] {
            assert_eq!(
                ok(json!(v), json!("string"), src),
                AvroValue::String(v.to_string())
            );
        }
        // INT UNSIGNED is a long; an int field refuses values above i32.
        assert_eq!(
            ok(
                json!(4_294_967_295_u64),
                json!("long"),
                Some("int unsigned")
            ),
            AvroValue::Long(4_294_967_295)
        );
        refused(json!(2_147_483_648_u64), json!("int"), Some("int"));
        refused(json!(1.5), json!("long"), None);
        refused(json!("12"), json!("long"), None);
    }

    #[test]
    fn floats_are_exact_or_refused() {
        // A FLOAT column's value is an f32 widened to f64: exact.
        assert_eq!(
            ok(json!(1.100000023841858_f64), json!("float"), Some("float")),
            AvroValue::Float(1.1)
        );
        refused(json!(0.1), json!("float"), Some("float"));
        assert_eq!(
            ok(json!(1e308), json!("double"), None),
            AvroValue::Double(1e308)
        );
        // 2^53 + 1 has no exact double.
        refused(json!(9_007_199_254_740_993_i64), json!("double"), None);
    }

    #[test]
    fn dates_convert_exactly_and_zero_dates_are_refused() {
        let date = json!({"type": "int", "logicalType": "date"});
        assert_eq!(
            ok(json!("1970-01-01"), date.clone(), Some("date")),
            AvroValue::Date(0)
        );
        assert_eq!(
            ok(json!("2024-02-29"), date.clone(), Some("date")),
            AvroValue::Date(19_782)
        );
        assert_eq!(
            ok(json!("1000-01-01"), date.clone(), Some("date")),
            AvroValue::Date(-354_285)
        );
        for v in [
            "0000-00-00",
            "0-00-00",
            "2023-02-29",
            "2024-1-5",
            "2024-01-02 BC",
        ] {
            let f = refused(json!(v), date.clone(), Some("date"));
            assert_eq!(f.target_type, "Avro date");
        }
    }

    #[test]
    fn timestamp_units_are_never_mixed() {
        let micros = json!({"type": "long", "logicalType": "timestamp-micros"});
        let millis = json!({"type": "long", "logicalType": "timestamp-millis"});
        // MySQL TIMESTAMP: epoch microseconds, declared by its source type.
        assert_eq!(
            ok(json!(1_000_000), micros.clone(), Some("timestamp")),
            AvroValue::TimestampMicros(1_000_000)
        );
        assert_eq!(
            ok(json!(1_000_000), micros.clone(), Some("timestamp(6)")),
            AvroValue::TimestampMicros(1_000_000)
        );
        // A number of undeclared unit is never passed through.
        refused(json!(1_000_000), micros.clone(), None);
        refused(json!(1_000_000), millis.clone(), Some("timestamp"));
        // Date-time text with an offset is an instant; precision is kept.
        assert_eq!(
            ok(
                json!("2024-01-02 03:04:05.123456+00"),
                micros.clone(),
                Some("timestamp with time zone")
            ),
            AvroValue::TimestampMicros(1_704_164_645_123_456)
        );
        assert_eq!(
            ok(json!("2024-01-02T05:04:05.123+02:00"), millis.clone(), None),
            AvroValue::TimestampMillis(1_704_164_645_123)
        );
        // Sub-millisecond digits are not dropped.
        refused(json!("2024-01-02 03:04:05.123456+00"), millis, None);
        for v in ["infinity", "0000-00-00 00:00:00", "2024-01-02 25:00:00"] {
            refused(json!(v), micros.clone(), Some("timestamp"));
        }
    }

    #[test]
    fn times_of_day_are_exact_and_durations_refused() {
        let micros = json!({"type": "long", "logicalType": "time-micros"});
        let millis = json!({"type": "int", "logicalType": "time-millis"});
        assert_eq!(
            ok(json!("12:34:56.5"), micros.clone(), None),
            AvroValue::TimeMicros(45_296_500_000)
        );
        assert_eq!(
            ok(json!("12:34:56.123000"), millis.clone(), None),
            AvroValue::TimeMillis(45_296_123)
        );
        refused(json!("12:34:56.123456"), millis, None);
        // MySQL TIME is a signed duration: not a time of day.
        for v in ["-00:00:00.500000", "838:59:59", "24:00:00"] {
            refused(json!(v), micros.clone(), Some("time(6)"));
        }
    }

    #[test]
    fn bits_enums_and_sets_are_never_rendered_from_numbers() {
        // BIT: a number is not its bytes (never its ASCII digits).
        let f = refused(json!(255), json!("bytes"), Some("bit(8)"));
        assert_eq!(f.target_type, "Avro bytes");
        refused(json!(1), json!("boolean"), Some("bit(1)"));
        assert_eq!(
            ok(json!({"_base64": "/w=="}), json!("bytes"), Some("bit(8)")),
            AvroValue::Bytes(vec![0xff])
        );
        // ENUM / SET: an index is not its label.
        let f = refused(json!(1), json!("string"), Some("enum('a','b','c')"));
        assert_eq!(f.reason, "an index cannot stand for its label");
        refused(json!(5), json!("string"), Some("set('x','y','z')"));
        assert_eq!(
            ok(json!("b"), json!("string"), Some("enum('a','b','c')")),
            AvroValue::String("b".into())
        );
        let avro_enum =
            json!({"type": "enum", "name": "c_enum", "symbols": ["a", "b"]});
        assert_eq!(
            ok(json!("b"), avro_enum.clone(), None),
            AvroValue::Enum(1, "b".into())
        );
        refused(json!("zz"), avro_enum, None);
    }

    #[test]
    fn text_is_exact_and_never_replaced() {
        use base64::Engine;
        let b64 = |b: &[u8]| json!({"_base64": base64::engine::general_purpose::STANDARD.encode(b)});
        // TEXT arrives base64-wrapped from the binlog: valid UTF-8 is exact.
        assert_eq!(
            ok(
                b64("multi\nline 😀".as_bytes()),
                json!("string"),
                Some("text")
            ),
            AvroValue::String("multi\nline 😀".into())
        );
        // latin1 bytes (é = 0xE9) are not UTF-8: refused, never U+FFFD.
        let f = refused(b64(b"d\xe9j\xe0"), json!("string"), Some("text"));
        assert_eq!(f.reason, "bytes are not valid UTF-8 text");
        refused(
            json!({"_base64": "not base64!"}),
            json!("string"),
            Some("text"),
        );
        // A JSON column's structured value is its JSON text; a structured
        // value for a text column is refused.
        assert_eq!(
            ok(json!({"a": 1}), json!("string"), Some("json")),
            AvroValue::String(r#"{"a":1}"#.into())
        );
        refused(json!({"a": 1}), json!("string"), Some("varchar(10)"));
        // Bytes: a string's exact bytes or a strict base64 decode.
        assert_eq!(
            ok(json!("ab"), json!("bytes"), Some("varbinary(4)")),
            AvroValue::Bytes(b"ab".to_vec())
        );
        refused(json!({"_base64": "@@"}), json!("bytes"), Some("blob"));
    }

    #[test]
    fn nulls_and_unsupported_targets_are_refused() {
        let schema = AvroSchema::parse(&json!({"type": "record", "name": "R", "fields": [
            {"name": "c", "type": "long", "deltaforge.source_type": "bigint"}]})).unwrap();
        match json_to_avro(&json!({"c": null}), &schema) {
            Err(EncodingError::Conversion(f)) => {
                assert_eq!(
                    (f.column.as_str(), f.source_type.as_str()),
                    ("c", "bigint")
                );
                assert_eq!(f.reason, "null for a non-nullable field");
            }
            other => panic!("{other:?}"),
        }
        let decimal = json!({"type": "bytes", "logicalType": "decimal", "precision": 5, "scale": 2});
        refused(json!("1.50"), decimal, Some("decimal(5,2)"));
    }

    /// A named type used twice is resolved, not converted best-effort.
    #[test]
    fn named_references_are_resolved_exactly() {
        let schema = AvroSchema::parse(&json!({"type": "record", "name": "E", "fields": [
            {"name": "before", "type": ["null", {"type": "record", "name": "V", "fields": [
                {"name": "b", "type": "long", "deltaforge.source_type": "bigint unsigned"}]}], "default": null},
            {"name": "after", "type": ["null", "V"], "default": null}]})).unwrap();
        let v =
            json_to_avro(&json!({"before": null, "after": {"b": 7}}), &schema)
                .unwrap();
        assert!(format!("{v:?}").contains("Long(7)"), "{v:?}");
        match json_to_avro(
            &json!({"before": null, "after": {"b": u64::MAX}}),
            &schema,
        ) {
            Err(EncodingError::Conversion(f)) => {
                assert_eq!(f.column, "after.b")
            }
            other => panic!("never a double or a wrapped long: {other:?}"),
        }
    }

    /// A conversion failure becomes the `sink_conversion_failed` incident:
    /// column, source and target type as evidence, never the value; still a
    /// DLQ-eligible serialization error.
    #[test]
    fn conversion_failures_raise_sink_conversion_failed() {
        let f =
            refused(json!(u64::MAX), json!("long"), Some("bigint unsigned"));
        let e = EncodingError::Conversion(f).into_sink_error_for("kafka");
        assert!(e.is_dlq_eligible());
        let draft = e.draft().expect("an incident draft");
        assert_eq!(
            draft.reason_code,
            crate::incident::ReasonCode::SinkConversionFailed
        );
        let json = serde_json::to_string(draft).unwrap();
        for want in ["\"column\"", "bigint_unsigned", "avro_long", "kafka"] {
            assert!(json.contains(want), "{want}: {json}");
        }
        assert!(!json.contains("18446744073709551615"), "{json}");
        assert!(!e.to_string().contains("18446744073709551615"));
    }

    /// Integers reach Avro `long`/`int` exactly or fail explicitly: an
    /// unsigned value above `i64::MAX` (MySQL BIGINT UNSIGNED in `long`
    /// mode) is never wrapped, zeroed or turned into a double.
    #[test]
    fn integers_encode_exactly_or_fail_explicitly() {
        let long = AvroSchema::parse_str(r#""long""#).unwrap();
        let nullable_long =
            AvroSchema::parse_str(r#"["null","long"]"#).unwrap();
        let int = AvroSchema::parse_str(r#""int""#).unwrap();
        for (v, want) in [
            (json!(4_294_967_295_u64), 4_294_967_295_i64),
            (json!(i64::MAX), i64::MAX),
            (json!(i64::MIN), i64::MIN),
        ] {
            assert_eq!(json_to_avro(&v, &long).unwrap(), AvroValue::Long(want));
            assert_eq!(
                json_to_avro(&v, &nullable_long).unwrap(),
                AvroValue::Union(1, Box::new(AvroValue::Long(want)))
            );
        }
        for v in [json!(i64::MAX as u64 + 1), json!(u64::MAX), json!(1.5)] {
            let e = json_to_avro(&v, &long).unwrap_err().to_string();
            assert!(
                e.contains("cannot be represented exactly as Avro long"),
                "{v}: {e}"
            );
            let e = json_to_avro(&v, &nullable_long).unwrap_err().to_string();
            assert!(
                e.contains("cannot be represented exactly as Avro long"),
                "{v}: {e}"
            );
        }
        assert_eq!(
            json_to_avro(&json!(i32::MAX), &int).unwrap(),
            AvroValue::Int(i32::MAX)
        );
        for v in [json!(i32::MAX as i64 + 1), json!(i32::MIN as i64 - 1)] {
            let e = json_to_avro(&v, &int).unwrap_err().to_string();
            assert!(
                e.contains("cannot be represented exactly as Avro int"),
                "{v}: {e}"
            );
        }
    }

    /// BIGINT UNSIGNED in `string` mode keeps the exact decimal value.
    #[test]
    fn unsigned_values_encode_as_exact_decimal_strings() {
        let schema = AvroSchema::parse_str(r#"["null","string"]"#).unwrap();
        for v in [i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX] {
            assert_eq!(
                json_to_avro(&json!(v), &schema).unwrap(),
                AvroValue::Union(1, Box::new(AvroValue::String(v.to_string())))
            );
        }
    }

    #[test]
    fn json_to_avro_number_coerced_to_string() {
        let schema_json = r#"["null", "string"]"#;
        let schema = AvroSchema::parse_str(schema_json).unwrap();
        // Number should be coerced to string when schema expects string
        let val = json_to_avro(&json!(42), &schema).unwrap();
        match &val {
            AvroValue::Union(_, inner) => {
                assert!(
                    matches!(inner.as_ref(), AvroValue::String(_)),
                    "number should coerce to string: {inner:?}"
                );
            }
            _ => panic!("expected Union, got {val:?}"),
        }
    }

    #[test]
    fn json_to_avro_null_in_nullable_long() {
        let schema_json = r#"["null", "long"]"#;
        let schema = AvroSchema::parse_str(schema_json).unwrap();
        let val = json_to_avro(&json!(null), &schema).unwrap();
        assert!(matches!(val, AvroValue::Union(0, _)));
    }

    #[test]
    fn json_to_avro_int_in_nullable_long() {
        let schema_json = r#"["null", "long"]"#;
        let schema = AvroSchema::parse_str(schema_json).unwrap();
        let val = json_to_avro(&json!(12345), &schema).unwrap();
        match &val {
            AvroValue::Union(1, inner) => {
                assert!(matches!(inner.as_ref(), AvroValue::Long(12345)));
            }
            _ => panic!("expected Union(1, Long), got {val:?}"),
        }
    }

    // ── Edge case: nullable records ─────────────────────────────────────

    #[test]
    fn json_to_avro_null_in_nullable_record() {
        let schema_json = r#"["null", {"type":"record","name":"R","fields":[{"name":"id","type":"long"}]}]"#;
        let schema = AvroSchema::parse_str(schema_json).unwrap();
        let val = json_to_avro(&json!(null), &schema).unwrap();
        assert!(matches!(val, AvroValue::Union(0, _)));
    }

    #[test]
    fn json_to_avro_object_in_nullable_record() {
        let schema_json = r#"["null", {"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"name","type":["null","string"],"default":null}]}]"#;
        let schema = AvroSchema::parse_str(schema_json).unwrap();
        let val =
            json_to_avro(&json!({"id": 42, "name": "Alice"}), &schema).unwrap();
        match &val {
            AvroValue::Union(1, inner) => {
                assert!(matches!(inner.as_ref(), AvroValue::Record(_)));
            }
            _ => panic!("expected Union(1, Record), got {val:?}"),
        }
        // Should encode successfully
        let encoded = apache_avro::to_avro_datum(&schema, val);
        assert!(encoded.is_ok(), "encoding failed: {:?}", encoded.err());
    }

    // ── Edge case: record with missing/extra fields ─────────────────────

    #[test]
    fn json_to_avro_record_missing_nullable_field() {
        // JSON is missing "name" field — should default to null
        let schema_json = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"name","type":["null","string"],"default":null}]}"#;
        let schema = AvroSchema::parse_str(schema_json).unwrap();
        let val = json_to_avro(&json!({"id": 1}), &schema).unwrap();
        if let AvroValue::Record(fields) = &val {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[1].0, "name");
            // Missing field should be null (wrapped in union)
        } else {
            panic!("expected Record, got {val:?}");
        }
        let encoded = apache_avro::to_avro_datum(&schema, val);
        assert!(encoded.is_ok(), "encoding failed: {:?}", encoded.err());
    }

    #[test]
    fn json_to_avro_record_extra_fields_ignored() {
        // JSON has "extra_field" not in schema — should be ignored
        let schema_json = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"}]}"#;
        let schema = AvroSchema::parse_str(schema_json).unwrap();
        let val =
            json_to_avro(&json!({"id": 1, "extra_field": "ignored"}), &schema)
                .unwrap();
        if let AvroValue::Record(fields) = &val {
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].0, "id");
        } else {
            panic!("expected Record, got {val:?}");
        }
        let encoded = apache_avro::to_avro_datum(&schema, val);
        assert!(encoded.is_ok(), "encoding failed: {:?}", encoded.err());
    }

    // ── Edge case: arrays ───────────────────────────────────────────────

    #[test]
    fn json_to_avro_empty_array() {
        let schema_json = r#"{"type":"array","items":"string"}"#;
        let schema = AvroSchema::parse_str(schema_json).unwrap();
        let val = json_to_avro(&json!([]), &schema).unwrap();
        assert!(matches!(val, AvroValue::Array(ref a) if a.is_empty()));
        let encoded = apache_avro::to_avro_datum(&schema, val);
        assert!(encoded.is_ok());
    }

    #[test]
    fn json_to_avro_array_of_records() {
        let schema_json = r#"{"type":"array","items":{"type":"record","name":"Item","fields":[{"name":"sku","type":"string"},{"name":"qty","type":"int"}]}}"#;
        let schema = AvroSchema::parse_str(schema_json).unwrap();
        let val = json_to_avro(
            &json!([{"sku": "A1", "qty": 5}, {"sku": "B2", "qty": 10}]),
            &schema,
        )
        .unwrap();
        if let AvroValue::Array(items) = &val {
            assert_eq!(items.len(), 2);
            assert!(matches!(&items[0], AvroValue::Record(_)));
        } else {
            panic!("expected Array, got {val:?}");
        }
        let encoded = apache_avro::to_avro_datum(&schema, val);
        assert!(encoded.is_ok(), "encoding failed: {:?}", encoded.err());
    }

    // ── Edge case: type coercion ────────────────────────────────────────

    #[test]
    fn json_to_avro_large_int_as_long() {
        // Number > i32 range should work with Long schema
        let schema_json = r#""long""#;
        let schema = AvroSchema::parse_str(schema_json).unwrap();
        let val = json_to_avro(&json!(1700000000000_i64), &schema).unwrap();
        assert!(matches!(val, AvroValue::Long(1700000000000)));
    }

    #[test]
    fn json_to_avro_float_as_double() {
        let schema_json = r#""double""#;
        let schema = AvroSchema::parse_str(schema_json).unwrap();
        let val = json_to_avro(&json!(3.15), &schema).unwrap();
        assert!(matches!(val, AvroValue::Double(_)));
    }

    #[test]
    fn json_to_avro_bool_coercion() {
        let schema_json = r#""boolean""#;
        let schema = AvroSchema::parse_str(schema_json).unwrap();
        let val = json_to_avro(&json!(true), &schema).unwrap();
        assert!(matches!(val, AvroValue::Boolean(true)));
    }

    // ── Edge case: map ──────────────────────────────────────────────────

    #[test]
    fn json_to_avro_map_string_values() {
        let schema_json = r#"{"type":"map","values":"string"}"#;
        let schema = AvroSchema::parse_str(schema_json).unwrap();
        let val =
            json_to_avro(&json!({"key1": "val1", "key2": "val2"}), &schema)
                .unwrap();
        assert!(matches!(val, AvroValue::Map(_)));
        let encoded = apache_avro::to_avro_datum(&schema, val);
        assert!(encoded.is_ok());
    }

    // ── Full CDC roundtrip tests ────────────────────────────────────────

    #[test]
    fn json_to_avro_cdc_insert_roundtrip() {
        // INSERT: before=null, after=row
        roundtrip_json_to_avro(&json!({
            "before": null,
            "after": {"id": 1, "name": "Alice"},
            "op": "c",
            "ts_ms": 1700000000000_i64,
            "source": {
                "connector": "mysql",
                "db": "shop",
                "table": "users"
            }
        }));
    }

    #[test]
    fn json_to_avro_cdc_update_roundtrip() {
        // UPDATE: before=old row, after=new row
        roundtrip_json_to_avro(&json!({
            "before": {"id": 1, "name": "Alice"},
            "after": {"id": 1, "name": "Alicia"},
            "op": "u",
            "ts_ms": 1700000000001_i64,
            "source": {
                "connector": "mysql",
                "db": "shop",
                "table": "users"
            }
        }));
    }

    #[test]
    fn json_to_avro_cdc_delete_roundtrip() {
        // DELETE: before=row, after=null
        roundtrip_json_to_avro(&json!({
            "before": {"id": 1, "name": "Alice"},
            "after": null,
            "op": "d",
            "ts_ms": 1700000000002_i64,
            "source": {
                "connector": "mysql",
                "db": "shop",
                "table": "users"
            }
        }));
    }

    // ── Envelope schema tests ───────────────────────────────────────────

    #[test]
    fn json_to_avro_envelope_schema() {
        // Test with the actual CDC envelope schema structure
        // (built by avro_schema module)
        use super::super::avro_schema::{
            build_envelope_schema, build_value_schema,
        };

        let value_schema = build_value_schema(
            "mysql",
            "orders",
            "customers",
            vec![
                json!({"name": "id", "type": "long"}),
                json!({"name": "name", "type": ["null", "string"], "default": null}),
                json!({"name": "email", "type": ["null", "string"], "default": null}),
            ],
        );

        let (schema_json, schema) =
            build_envelope_schema("mysql", "orders", "customers", value_schema)
                .expect("envelope schema should build");

        // Simulate a CDC INSERT event
        let event = json!({
            "before": null,
            "after": {"id": 1, "name": "Alice", "email": "alice@test.com"},
            "source": {
                "version": "deltaforge-test",
                "connector": "mysql",
                "name": "test-pipeline",
                "ts_ms": 1700000000000_i64,
                "db": "orders",
                "schema": null,
                "table": "customers",
                "snapshot": null,
                "position": {
                    "server_id": 1,
                    "file": "mysql-bin.000001",
                    "pos": 12345,
                    "gtid": null,
                    "row": null
                }
            },
            "op": "c",
            "ts_ms": 1700000000000_i64,
            "event_id": null,
            "schema_version": null,
            "transaction": null
        });

        let avro_value = json_to_avro(&event, &schema);
        assert!(
            avro_value.is_ok(),
            "CDC envelope should convert: schema={schema_json}, error={:?}",
            avro_value.err()
        );

        let encoded = apache_avro::to_avro_datum(&schema, avro_value.unwrap());
        assert!(
            encoded.is_ok(),
            "CDC envelope should encode to Avro datum: {:?}",
            encoded.err()
        );
    }

    // ── _base64 wrapper tests (MySQL TEXT/BLOB columns) ─────────────

    #[test]
    fn json_to_avro_base64_wrapper_as_string() {
        // MySQL TEXT column → JSON {"_base64": "aGVsbG8="} → Avro String
        let schema_json = r#"["null", "string"]"#;
        let schema = AvroSchema::parse_str(schema_json).unwrap();
        let val = json_to_avro(
            &json!({"_base64": "aGVsbG8="}), // "hello" in base64
            &schema,
        )
        .unwrap();
        match &val {
            AvroValue::Union(_, inner) => {
                if let AvroValue::String(s) = inner.as_ref() {
                    assert_eq!(s, "hello");
                } else {
                    panic!("expected String, got {inner:?}");
                }
            }
            _ => panic!("expected Union, got {val:?}"),
        }
    }

    #[test]
    fn json_to_avro_base64_wrapper_as_bytes() {
        // MySQL BLOB column → JSON {"_base64": "AQID"} → Avro Bytes
        let schema_json = r#"["null", "bytes"]"#;
        let schema = AvroSchema::parse_str(schema_json).unwrap();
        let val = json_to_avro(
            &json!({"_base64": "AQID"}), // [1, 2, 3] in base64
            &schema,
        )
        .unwrap();
        match &val {
            AvroValue::Union(_, inner) => {
                if let AvroValue::Bytes(b) = inner.as_ref() {
                    assert_eq!(b, &[1, 2, 3]);
                } else {
                    panic!("expected Bytes, got {inner:?}");
                }
            }
            _ => panic!("expected Union, got {val:?}"),
        }
    }

    #[test]
    fn json_to_avro_base64_roundtrip() {
        // Full roundtrip: record with _base64 field → encode → datum
        let schema_json = r#"{"type":"record","name":"R","fields":[
            {"name":"id","type":"int"},
            {"name":"data","type":["null","string"],"default":null},
            {"name":"blob_col","type":["null","bytes"],"default":null}
        ]}"#;
        let schema = AvroSchema::parse_str(schema_json).unwrap();
        let val = json_to_avro(
            &json!({
                "id": 1,
                "data": {"_base64": "aGVsbG8gd29ybGQ="},
                "blob_col": {"_base64": "AQIDBA=="}
            }),
            &schema,
        )
        .unwrap();
        let encoded = apache_avro::to_avro_datum(&schema, val);
        assert!(
            encoded.is_ok(),
            "base64 record should encode: {:?}",
            encoded.err()
        );
    }
}

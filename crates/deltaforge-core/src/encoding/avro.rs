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
use bytes::{BufMut, Bytes, BytesMut};
use metrics::counter;
use parking_lot::RwLock;
use serde::Serialize;
use tracing::{debug, warn};

use super::EncodingError;

// =============================================================================
// Source schema provider trait
// =============================================================================

/// Provides DDL-derived Avro schemas for CDC events (Path A).
///
/// Implementors look up source table schemas and convert them to Avro
/// field definitions using the type converters in [`super::avro_types`].
///
/// The returned schema string is a complete Avro envelope schema JSON
/// (built by [`super::avro_schema::build_envelope_schema`]).
pub trait SourceSchemaProvider: Send + Sync {
    /// Look up the Avro envelope schema for a given source table.
    ///
    /// Returns `Some((schema_json, parsed_schema))` if DDL is available
    /// for the given connector/db/table, or `None` to fall back to
    /// JSON inference (Path C).
    fn get_envelope_schema(
        &self,
        connector: &str,
        db: &str,
        table: &str,
    ) -> Option<(String, Arc<AvroSchema>)>;
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

/// Confluent Schema Registry HTTP client.
///
/// Supports schema registration, lookup, and caching.
pub struct SchemaRegistryClient {
    client: reqwest::Client,
    base_url: String,
    /// Cache: subject → (schema_id, avro_schema)
    cache: Arc<RwLock<HashMap<String, CachedSchema>>>,
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

/// Response from POST /subjects/{subject} (lookup)
#[derive(serde::Deserialize)]
struct LookupResponse {
    id: u32,
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
        username: Option<&str>,
        password: Option<&str>,
    ) -> Result<Self, EncodingError> {
        let mut builder = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .connect_timeout(std::time::Duration::from_secs(5));

        if let (Some(u), Some(p)) = (username, password) {
            // reqwest doesn't have default basic auth on the client,
            // we'll add it per-request
            let _ = (u, p); // suppress unused warning, handled in request methods
            let _ = &mut builder;
        }

        let client = builder
            .build()
            .map_err(|e| EncodingError::SchemaRegistry(e.to_string()))?;

        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Register a schema for a subject, or return the existing ID if already registered.
    ///
    /// Uses the Schema Registry's idempotent registration endpoint:
    /// POST /subjects/{subject}/versions
    pub async fn register_schema(
        &self,
        subject: &str,
        schema_json: &str,
        auth: Option<(&str, &str)>,
    ) -> Result<(u32, Arc<AvroSchema>), EncodingError> {
        // Check cache first
        {
            let cache = self.cache.read();
            if let Some(cached) = cache.get(subject) {
                return Ok((cached.id, cached.schema.clone()));
            }
        }

        // Register with Schema Registry
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
            EncodingError::SchemaRegistry(format!(
                "failed to register schema for subject {subject}: {e}"
            ))
        })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            // Try to parse structured error
            if let Ok(sr_err) = serde_json::from_str::<SrErrorResponse>(&body) {
                return Err(EncodingError::SchemaRegistry(format!(
                    "schema registration failed for {subject}: {} (code: {})",
                    sr_err.message.unwrap_or_default(),
                    sr_err.error_code.unwrap_or(0),
                )));
            }
            return Err(EncodingError::SchemaRegistry(format!(
                "schema registration failed for {subject}: HTTP {status}: {body}"
            )));
        }

        let register_resp: RegisterResponse =
            resp.json().await.map_err(|e| {
                EncodingError::SchemaRegistry(format!(
                    "invalid response from schema registry: {e}"
                ))
            })?;

        let schema = AvroSchema::parse_str(schema_json).map_err(|e| {
            EncodingError::Avro(format!(
                "failed to parse registered schema: {e}"
            ))
        })?;
        let schema = Arc::new(schema);

        debug!(
            subject,
            schema_id = register_resp.id,
            "registered Avro schema"
        );

        // Cache it
        {
            let mut cache = self.cache.write();
            cache.insert(
                subject.to_string(),
                CachedSchema {
                    id: register_resp.id,
                    schema: schema.clone(),
                },
            );
        }

        Ok((register_resp.id, schema))
    }

    /// Look up an existing schema by subject (without registering).
    #[allow(dead_code)]
    pub async fn lookup_schema(
        &self,
        subject: &str,
        schema_json: &str,
        auth: Option<(&str, &str)>,
    ) -> Result<Option<(u32, Arc<AvroSchema>)>, EncodingError> {
        // Check cache first
        {
            let cache = self.cache.read();
            if let Some(cached) = cache.get(subject) {
                return Ok(Some((cached.id, cached.schema.clone())));
            }
        }

        let url = format!("{}/subjects/{}", self.base_url, subject);
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
            EncodingError::SchemaRegistry(format!(
                "schema lookup failed for {subject}: {e}"
            ))
        })?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(EncodingError::SchemaRegistry(format!(
                "schema lookup failed for {subject}: HTTP {status}: {body}"
            )));
        }

        let lookup: LookupResponse = resp.json().await.map_err(|e| {
            EncodingError::SchemaRegistry(format!(
                "invalid lookup response: {e}"
            ))
        })?;

        let schema = AvroSchema::parse_str(&lookup.schema).map_err(|e| {
            EncodingError::Avro(format!(
                "failed to parse schema from registry: {e}"
            ))
        })?;
        let schema = Arc::new(schema);

        // Cache it
        {
            let mut cache = self.cache.write();
            cache.insert(
                subject.to_string(),
                CachedSchema {
                    id: lookup.id,
                    schema: schema.clone(),
                },
            );
        }

        Ok(Some((lookup.id, schema)))
    }

    /// Get cached schema for a subject (if previously registered).
    pub fn get_cached(
        &self,
        subject: &str,
    ) -> Option<(u32, Arc<AvroSchema>)> {
        let cache = self.cache.read();
        cache
            .get(subject)
            .map(|c| (c.id, c.schema.clone()))
    }

    /// Clear the schema cache (useful for testing or schema evolution).
    #[allow(dead_code)]
    pub fn clear_cache(&self) {
        self.cache.write().clear();
    }
}

// =============================================================================
// Avro Encoder
// =============================================================================

/// Avro encoder that produces Confluent wire format.
///
/// Serializes events as:
/// - Byte 0: Magic byte (0x00)
/// - Bytes 1-4: Schema ID (big-endian u32)
/// - Bytes 5+: Avro binary-encoded payload
pub struct AvroEncoder {
    registry: SchemaRegistryClient,
    strategy: SubjectStrategy,
    auth: Option<(String, String)>,
    /// Optional DDL-derived schema provider (Path A).
    /// When set, the encoder uses precise DDL-derived Avro schemas
    /// instead of inferring from JSON (Path C).
    source_schemas: Option<Arc<dyn SourceSchemaProvider>>,
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
    /// Avro schemas (Path A) for events whose source table is known.
    /// Falls back to JSON inference (Path C) for unknown tables.
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
        })
    }

    /// Set the source schema provider after construction.
    pub fn set_source_schemas(
        &mut self,
        provider: Arc<dyn SourceSchemaProvider>,
    ) {
        self.source_schemas = Some(provider);
    }

    /// Encode a serializable value to Confluent Avro wire format.
    ///
    /// The value is first serialized to JSON, then an Avro schema is derived
    /// from its structure, registered with the Schema Registry, and the value
    /// is encoded as Avro binary with the Confluent wire format prefix.
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
        // 1. Serialize to JSON first to derive the schema
        let json_value = serde_json::to_value(value)
            .map_err(|e| EncodingError::Avro(e.to_string()))?;

        // 2. Derive Avro schema from the JSON structure
        let schema_json = derive_avro_schema(&json_value, record_name);

        // 3. Resolve subject name
        let subject = self.strategy.resolve(topic, record_name);

        // 4. Register schema (idempotent — returns cached ID if unchanged)
        let auth = self.auth.as_ref().map(|(u, p)| (u.as_str(), p.as_str()));
        let (schema_id, schema) = self
            .registry
            .register_schema(&subject, &schema_json, auth)
            .await?;

        // 5. Convert JSON to Avro value
        let avro_value = json_to_avro(&json_value, &schema)?;

        // 6. Encode as Avro binary
        let avro_bytes = apache_avro::to_avro_datum(&schema, avro_value)
            .map_err(|e| {
                EncodingError::Avro(format!("Avro encoding failed: {e}"))
            })?;

        // 7. Build Confluent wire format: [0x00][schema_id:4][avro_payload]
        let mut buf = BytesMut::with_capacity(5 + avro_bytes.len());
        buf.put_u8(0x00); // magic byte
        buf.put_u32(schema_id); // schema ID (big-endian)
        buf.extend_from_slice(&avro_bytes);

        Ok(buf.freeze())
    }

    /// Encode a CDC event using DDL-derived schema when available (Path A),
    /// falling back to JSON inference (Path C).
    ///
    /// This is the preferred entry point for CDC events where the source
    /// metadata (connector, db, table) is known.
    ///
    /// # Arguments
    ///
    /// * `topic` - Destination topic/stream/subject (used for SR subject naming)
    /// * `value` - Serializable envelope data
    /// * `connector` - Source connector type ("mysql", "postgresql", etc.)
    /// * `db` - Source database name
    /// * `table` - Source table name
    pub async fn encode_event<T: Serialize>(
        &self,
        topic: &str,
        value: &T,
        connector: &str,
        db: &str,
        table: &str,
    ) -> Result<Bytes, EncodingError> {
        // 1. Try Path A: DDL-derived schema
        if let Some(ref provider) = self.source_schemas {
            if let Some((schema_json, schema)) =
                provider.get_envelope_schema(connector, db, table)
            {
                debug!(
                    connector,
                    db,
                    table,
                    "using DDL-derived Avro schema (Path A)"
                );
                counter!("deltaforge_avro_encode_total", "path" => "ddl")
                    .increment(1);
                return self
                    .encode_with_schema(topic, value, &schema_json, &schema)
                    .await;
            }
        }

        // 2. Fall back to Path C: JSON inference
        debug!(
            connector,
            db,
            table,
            "no DDL schema available — falling back to JSON inference (Path C)"
        );
        counter!("deltaforge_avro_encode_total", "path" => "inferred")
            .increment(1);
        let record_name = Some(
            format!("deltaforge.{connector}.{db}.{table}.Value").leak() as &str
        );
        self.encode(topic, value, record_name).await
    }

    /// Encode a value using a pre-built Avro schema.
    ///
    /// Shared encoding logic for both Path A (DDL-derived) and Path C (inferred).
    ///
    /// If the Schema Registry is unavailable but a schema is already cached
    /// for this subject, encoding continues with the cached schema ID.
    /// If encoding fails under the cached schema (e.g., DDL changed), the
    /// error is propagated for DLQ routing.
    async fn encode_with_schema<T: Serialize>(
        &self,
        topic: &str,
        value: &T,
        schema_json: &str,
        schema: &AvroSchema,
    ) -> Result<Bytes, EncodingError> {
        // 1. Resolve subject name
        let subject = self.strategy.resolve(topic, None);

        // 2. Register schema with SR (idempotent, cached).
        //    On SR failure, fall back to cached schema if available.
        let auth =
            self.auth.as_ref().map(|(u, p)| (u.as_str(), p.as_str()));
        let (schema_id, registered_schema) = match self
            .registry
            .register_schema(&subject, schema_json, auth)
            .await
        {
            Ok(result) => {
                counter!("deltaforge_avro_schema_registrations_total")
                    .increment(1);
                result
            }
            Err(e) => {
                // SR unavailable — try cached schema
                if let Some(cached) = self.registry.get_cached(&subject) {
                    warn!(
                        subject = %subject,
                        error = %e,
                        cached_schema_id = cached.0,
                        "Schema Registry unavailable — using cached schema"
                    );
                    counter!(
                        "deltaforge_avro_sr_cache_fallback_total"
                    )
                    .increment(1);
                    (cached.0, cached.1)
                } else {
                    // No cache — cannot encode
                    counter!(
                        "deltaforge_avro_encode_failure_total",
                        "reason" => "sr_unavailable"
                    )
                    .increment(1);
                    return Err(e);
                }
            }
        };

        // 3. Serialize value to JSON, then convert to Avro
        let json_value = serde_json::to_value(value)
            .map_err(|e| EncodingError::Avro(e.to_string()))?;
        let avro_value = match json_to_avro(&json_value, &registered_schema) {
            Ok(v) => v,
            Err(e) => {
                counter!(
                    "deltaforge_avro_encode_failure_total",
                    "reason" => "schema_mismatch"
                )
                .increment(1);
                return Err(e);
            }
        };

        // 4. Encode as Avro binary
        let avro_bytes =
            apache_avro::to_avro_datum(schema, avro_value).map_err(|e| {
                EncodingError::Avro(format!("Avro encoding failed: {e}"))
            })?;

        // 5. Build Confluent wire format
        let mut buf = BytesMut::with_capacity(5 + avro_bytes.len());
        buf.put_u8(0x00);
        buf.put_u32(schema_id);
        buf.extend_from_slice(&avro_bytes);

        Ok(buf.freeze())
    }
}

// =============================================================================
// Schema derivation from JSON
// =============================================================================

/// Derive an Avro schema from a JSON value.
///
/// CDC events have a consistent structure, so the schema is derived from
/// the first event and cached. Fields are mapped as:
/// - JSON string → Avro string
/// - JSON number (integer) → Avro long
/// - JSON number (float) → Avro double
/// - JSON boolean → Avro boolean
/// - JSON null → Avro null
/// - JSON array → Avro array (union of element types)
/// - JSON object → Avro record (recursive)
///
/// All fields are wrapped in a union with null for optionality, since
/// CDC events may have nullable columns.
fn derive_avro_schema(
    value: &serde_json::Value,
    record_name: Option<&str>,
) -> String {
    let name = record_name.unwrap_or("deltaforge.Event");

    match value {
        serde_json::Value::Object(map) => {
            let fields: Vec<String> = map
                .iter()
                .map(|(key, val)| {
                    let avro_type = json_type_to_avro(val, key);
                    format!(
                        r#"{{"name":"{}","type":{},"default":null}}"#,
                        escape_json_string(key),
                        avro_type
                    )
                })
                .collect();

            format!(
                r#"{{"type":"record","name":"{}","fields":[{}]}}"#,
                name,
                fields.join(",")
            )
        }
        _ => {
            // Non-object top-level: wrap in a record with a single "value" field
            warn!(
                "non-object value for Avro schema derivation, wrapping in record"
            );
            let avro_type = json_type_to_avro(value, "value");
            format!(
                r#"{{"type":"record","name":"{}","fields":[{{"name":"value","type":{}}}]}}"#,
                name, avro_type
            )
        }
    }
}

/// Map a JSON value to its Avro type representation.
/// All types are wrapped in ["null", <type>] for optionality.
fn json_type_to_avro(value: &serde_json::Value, field_name: &str) -> String {
    match value {
        serde_json::Value::Null => r#""null""#.to_string(),
        serde_json::Value::Bool(_) => r#"["null","boolean"]"#.to_string(),
        serde_json::Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                r#"["null","long"]"#.to_string()
            } else {
                r#"["null","double"]"#.to_string()
            }
        }
        serde_json::Value::String(_) => r#"["null","string"]"#.to_string(),
        serde_json::Value::Array(arr) => {
            if arr.is_empty() {
                // Empty array: default to array of strings
                r#"["null",{"type":"array","items":"string"}]"#.to_string()
            } else {
                let item_type = json_type_to_avro_primitive(&arr[0]);
                format!(r#"["null",{{"type":"array","items":{}}}]"#, item_type)
            }
        }
        serde_json::Value::Object(map) => {
            // Nested record: use field_name as record name
            let nested_name = format!("deltaforge.{}", capitalize(field_name));
            let fields: Vec<String> = map
                .iter()
                .map(|(key, val)| {
                    let avro_type = json_type_to_avro(val, key);
                    format!(
                        r#"{{"name":"{}","type":{},"default":null}}"#,
                        escape_json_string(key),
                        avro_type
                    )
                })
                .collect();

            format!(
                r#"["null",{{"type":"record","name":"{}","fields":[{}]}}]"#,
                nested_name,
                fields.join(",")
            )
        }
    }
}

/// Map a JSON value to a primitive Avro type (without null union wrapper).
fn json_type_to_avro_primitive(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => r#""null""#.to_string(),
        serde_json::Value::Bool(_) => r#""boolean""#.to_string(),
        serde_json::Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                r#""long""#.to_string()
            } else {
                r#""double""#.to_string()
            }
        }
        serde_json::Value::String(_) => r#""string""#.to_string(),
        serde_json::Value::Array(_) => r#""string""#.to_string(),
        serde_json::Value::Object(_) => r#""string""#.to_string(),
    }
}

fn capitalize(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
    }
}

fn escape_json_string(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

// =============================================================================
// JSON → Avro value conversion
// =============================================================================

/// Convert a JSON value to an Avro value using the given schema.
fn json_to_avro(
    json: &serde_json::Value,
    schema: &AvroSchema,
) -> Result<AvroValue, EncodingError> {
    // Use apache-avro's built-in JSON → Avro conversion
    let avro_value = AvroValue::from(json.clone());

    // Resolve against the schema to ensure correct types
    let resolved = avro_value.resolve(schema).map_err(|e| {
        EncodingError::Avro(format!("Avro schema resolution failed: {e}"))
    })?;

    Ok(resolved)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
    fn test_confluent_wire_format_prefix() {
        // Verify the wire format structure: [0x00][4-byte ID][payload]
        let mut buf = BytesMut::with_capacity(9);
        buf.put_u8(0x00);
        buf.put_u32(42);
        buf.extend_from_slice(&[1, 2, 3, 4]);
        let bytes = buf.freeze();

        assert_eq!(bytes[0], 0x00); // magic byte
        assert_eq!(
            u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]),
            42
        ); // schema ID
        assert_eq!(&bytes[5..], &[1, 2, 3, 4]); // payload
    }
}

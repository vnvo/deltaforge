//! Database schema provider abstraction.
//!
//! Provides a unified interface for accessing database schema information
//! from various sources (MySQL, Postgres, etc.).

use async_trait::async_trait;
use sources::ArcSchemaLoader;
use std::sync::Arc;

/// Information about a single column.
#[derive(Debug, Clone, Default)]
pub struct ColumnSchemaInfo {
    /// Column name
    pub name: String,
    /// SQL data type (e.g., "int", "varchar", "json", "jsonb")
    pub data_type: String,
    /// Full column type with modifiers (e.g., "varchar(255)", "bigint unsigned")
    pub full_type: String,
    /// Whether the column allows NULL
    pub nullable: bool,
    /// Whether this is a JSON-like column that should be deep-inspected
    pub is_json_like: bool,
    /// Numeric precision (for DECIMAL/NUMERIC)
    pub numeric_precision: Option<i64>,
    /// Numeric scale (for DECIMAL/NUMERIC)
    pub numeric_scale: Option<i64>,
    /// Whether the type is unsigned (MySQL)
    pub unsigned: bool,
    /// Whether the type is an array (PostgreSQL)
    pub is_array: bool,
    /// Array element type (PostgreSQL)
    pub element_type: Option<String>,
}

/// Schema information for a table.
#[derive(Debug, Clone)]
pub struct TableSchemaInfo {
    /// Database/schema name
    pub database: String,
    /// Table name
    pub table: String,
    /// Column definitions
    pub columns: Vec<ColumnSchemaInfo>,
    /// Primary key column names
    pub primary_key: Vec<String>,
}

impl TableSchemaInfo {
    /// Get columns that are JSON-like and should be deep-inspected.
    pub fn json_columns(&self) -> impl Iterator<Item = &ColumnSchemaInfo> {
        self.columns.iter().filter(|c| c.is_json_like)
    }

    /// Get column by name.
    pub fn column(&self, name: &str) -> Option<&ColumnSchemaInfo> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Check if a column is JSON-like.
    pub fn is_json_column(&self, name: &str) -> bool {
        self.column(name).map(|c| c.is_json_like).unwrap_or(false)
    }
}

/// Why a schema lookup could not be answered. Distinct from "no schema"
/// (`Ok(None)`): consumers must retry later and must never cache a fallback
/// built from this outcome.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaLookupError {
    #[error(
        "source schema unavailable for {table}: the pipeline's source has not \
         yet verified its database server identity, so its schema registry \
         scope does not exist yet; retry once the source is running"
    )]
    LineageNotEstablished { table: String },

    /// The requested schema version is not the registered one: its
    /// fingerprint is not registered for the table, or is registered under
    /// another sequence. Not retryable.
    #[error("schema version conflict for {table}: {reason}")]
    VersionConflict { table: String, reason: String },

    /// The schema store could not be read; retry later.
    #[error("source schema for {table} could not be read: {reason}")]
    Unreadable { table: String, reason: String },
}

/// Trait for providing database schema information.
#[async_trait]
pub trait SchemaProvider: Send + Sync {
    /// Get schema for a table: `Ok(Some)` when known, `Ok(None)` when no schema
    /// is known, `Err` when it cannot be determined right now.
    ///
    /// The `table` parameter may be in various formats:
    /// - "table_name" (table only)
    /// - "db.table" (database.table)
    /// - "schema.table" (for Postgres)
    async fn get_table_schema(
        &self,
        table: &str,
    ) -> Result<Option<TableSchemaInfo>, SchemaLookupError>;

    /// Get the schema of a table at exactly the registered version with
    /// content hash `fingerprint`, checked against its registry `sequence`:
    /// `Ok(None)` when no such version is registered.
    async fn get_table_schema_version(
        &self,
        table: &str,
        fingerprint: &str,
        sequence: Option<u64>,
    ) -> Result<Option<TableSchemaInfo>, SchemaLookupError> {
        let _ = (table, fingerprint, sequence);
        Ok(None)
    }

    /// Get all cached schemas.
    async fn list_schemas(&self) -> Vec<TableSchemaInfo>;
}

/// Arc wrapper for schema providers.
pub type ArcSchemaProvider = Arc<dyn SchemaProvider>;

/// Determines if a SQL type is JSON-like and should be deep-inspected.
pub fn is_json_type(data_type: &str) -> bool {
    let lower = data_type.to_lowercase();
    matches!(
        lower.as_str(),
        "json" | "jsonb" | "variant" | "object" | "map"
    )
}

/// Determines if a SQL type might contain JSON (needs heuristic detection).
pub fn might_be_json(data_type: &str) -> bool {
    let lower = data_type.to_lowercase();
    // TEXT and similar types might contain JSON
    matches!(
        lower.as_str(),
        "text" | "mediumtext" | "longtext" | "clob" | "nclob"
    )
}

pub struct SchemaLoaderAdapter {
    loader: ArcSchemaLoader,
}

impl SchemaLoaderAdapter {
    pub fn new(loader: ArcSchemaLoader) -> Self {
        Self { loader }
    }
}

#[async_trait]
impl SchemaProvider for SchemaLoaderAdapter {
    async fn get_table_schema(
        &self,
        table: &str,
    ) -> Result<Option<TableSchemaInfo>, SchemaLookupError> {
        // Parse "db.table" format
        let (db, tbl) = match table.split_once('.') {
            Some((d, t)) => (d, t),
            None => ("", table),
        };

        let loaded = match self.loader.load(db, tbl).await {
            Ok(loaded) => loaded,
            Err(e) if sources::registry_scope::is_lineage_unavailable(&e) => {
                return Err(SchemaLookupError::LineageNotEstablished {
                    table: table.to_string(),
                });
            }
            Err(_) => return Ok(None),
        };

        // Convert LoadedSchema to TableSchemaInfo
        let columns = extract_column_infos(&loaded.schema_json);

        Ok(Some(TableSchemaInfo {
            database: loaded.database,
            table: loaded.table,
            columns,
            primary_key: loaded.primary_key,
        }))
    }

    async fn get_table_schema_version(
        &self,
        table: &str,
        fingerprint: &str,
        sequence: Option<u64>,
    ) -> Result<Option<TableSchemaInfo>, SchemaLookupError> {
        let (db, tbl) = match table.split_once('.') {
            Some((d, t)) => (d, t),
            None => ("", table),
        };
        let loaded = match self
            .loader
            .load_version(db, tbl, fingerprint, sequence)
            .await
        {
            Ok(Some(loaded)) => loaded,
            Ok(None) => return Ok(None),
            Err(e) if sources::registry_scope::is_lineage_unavailable(&e) => {
                return Err(SchemaLookupError::LineageNotEstablished {
                    table: table.to_string(),
                });
            }
            Err(e) => {
                return Err(
                    match e.downcast_ref::<sources::schema_loader::SchemaVersionConflict>() {
                        Some(c) => SchemaLookupError::VersionConflict {
                            table: table.to_string(),
                            reason: c.to_string(),
                        },
                        None => SchemaLookupError::Unreadable {
                            table: table.to_string(),
                            reason: format!("{e:#}"),
                        },
                    },
                );
            }
        };
        Ok(Some(TableSchemaInfo {
            database: loaded.database,
            table: loaded.table,
            columns: extract_column_infos(&loaded.schema_json),
            primary_key: loaded.primary_key,
        }))
    }

    async fn list_schemas(&self) -> Vec<TableSchemaInfo> {
        self.loader
            .list_cached()
            .await
            .into_iter()
            .filter_map(|_entry| {
                // Convert each cached entry
                // This is a simplified version - may need async load for full info
                None // TODO: implement if needed
            })
            .collect()
    }
}

/// Extract ColumnSchemaInfo from source-specific schema JSON.
fn extract_column_infos(
    schema_json: &serde_json::Value,
) -> Vec<ColumnSchemaInfo> {
    // PostgreSQL registers two shapes (the CDC event schema and the
    // snapshot's catalog schema); both describe the same DDL identically.
    // An unreadable PostgreSQL schema has no columns (the encoders refuse
    // rows with undeclared columns, never guess).
    if let Some(pg) =
        sources::declared_types::postgres_encoder_columns(schema_json)
    {
        return pg
            .unwrap_or_default()
            .into_iter()
            .map(|c| ColumnSchemaInfo {
                is_json_like: is_json_type(&c.type_name)
                    || might_be_json(&c.type_name),
                data_type: c.type_name.clone(),
                full_type: c.type_name,
                name: c.name,
                nullable: true,
                numeric_precision: c.precision,
                numeric_scale: c.scale,
                unsigned: false,
                is_array: c.is_array,
                element_type: c.element_type,
            })
            .collect();
    }
    let Some(cols) = schema_json.get("columns").and_then(|v| v.as_array())
    else {
        return vec![];
    };

    cols.iter()
        .filter_map(|c| {
            let name = c.get("name")?.as_str()?.to_string();
            let data_type = c
                .get("data_type")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let full_type = c
                .get("column_type")
                .or(c.get("declared_type"))
                .and_then(|v| v.as_str())
                .unwrap_or(&data_type)
                .to_string();
            let nullable =
                c.get("nullable").and_then(|v| v.as_bool()).unwrap_or(true);
            let numeric_precision =
                c.get("numeric_precision").and_then(|v| v.as_i64());
            let numeric_scale = c.get("numeric_scale").and_then(|v| v.as_i64());

            // MySQL: detect unsigned from column_type string
            let unsigned = full_type.to_lowercase().contains("unsigned");

            // PostgreSQL: detect arrays
            let is_array =
                c.get("is_array").and_then(|v| v.as_bool()).unwrap_or(false);
            let element_type = c
                .get("element_type")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            Some(ColumnSchemaInfo {
                name,
                data_type: data_type.clone(),
                full_type,
                nullable,
                is_json_like: is_json_type(&data_type)
                    || might_be_json(&data_type),
                numeric_precision,
                numeric_scale,
                unsigned,
                is_array,
                element_type,
            })
        })
        .collect()
}

// =============================================================================
// Avro schema provider (bridges SchemaProvider → SourceSchemaProvider)
// =============================================================================

use std::collections::HashMap;

use apache_avro::Schema as AvroSchema;
use deltaforge_core::encoding::avro::{
    EnvelopeSchemaLookup, SchemaIdentity, SourceSchemaProvider,
};
use deltaforge_core::encoding::avro_schema::{
    build_envelope_schema, build_value_schema,
};
use deltaforge_core::encoding::avro_types::{
    AVRO_MAPPING_VERSION, ColumnDesc, TypeConversionOpts, mysql_column_to_avro,
    postgres_column_to_avro,
};
use parking_lot::RwLock;
use tracing::{debug, warn};

/// A cached envelope schema: the registry sequence of the version it was
/// built from, its JSON and the parsed schema.
type CachedAvroSchema = (Option<u64>, String, Arc<AvroSchema>);

/// Implements `SourceSchemaProvider` by looking up table schemas from
/// the `SchemaProvider` (which reads from the internal schema registry)
/// and converting column types to Avro using the type converters.
///
/// Each event's schema is built from exactly the registered table schema
/// version the event carries, never the table's latest schema. Envelope
/// schemas are cached per `(db, table, fingerprint)` with the version's
/// registry sequence: a later request for the same fingerprint under
/// another sequence is a conflict.
pub struct AvroSchemaProviderImpl {
    /// The underlying schema provider (reads from internal registry).
    schema_provider: ArcSchemaProvider,
    /// Source connector type ("mysql", "postgresql", etc.)
    connector: String,
    /// Type conversion options.
    opts: TypeConversionOpts,
    /// The type mapping version and options (see `mapping_id`).
    mapping_id: String,
    /// Cache: (database, namespace, table, fingerprint) → (sequence,
    /// schema_json, schema)
    cache: RwLock<HashMap<(String, String, String, String), CachedAvroSchema>>,
}

impl AvroSchemaProviderImpl {
    /// Create a new Avro schema provider.
    pub fn new(
        schema_provider: ArcSchemaProvider,
        connector: &str,
        opts: TypeConversionOpts,
    ) -> Self {
        Self {
            schema_provider,
            connector: connector.to_string(),
            mapping_id: format!("v{AVRO_MAPPING_VERSION}:{opts:?}"),
            opts,
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// Invalidate all cached schemas; each is rebuilt from its registered
    /// version on next use.
    pub fn invalidate_all(&self) {
        self.cache.write().clear();
    }

    /// Build the Avro envelope schema for a table from its column info.
    fn build_for_table(
        &self,
        db: &str,
        table: &str,
        table_schema: &TableSchemaInfo,
    ) -> Option<(String, Arc<AvroSchema>)> {
        let fields: Vec<serde_json::Value> = table_schema
            .columns
            .iter()
            .map(|col| {
                let col_desc = column_info_to_desc(col);
                match self.connector.as_str() {
                    "mysql" => mysql_column_to_avro(&col_desc, &self.opts),
                    "postgresql" | "postgres" => {
                        postgres_column_to_avro(&col_desc, &self.opts)
                    }
                    _ => {
                        // Generic: treat as MySQL-ish
                        mysql_column_to_avro(&col_desc, &self.opts)
                    }
                }
            })
            .collect();

        let value_schema =
            build_value_schema(&self.connector, db, table, fields);

        match build_envelope_schema(&self.connector, db, table, value_schema) {
            Ok((schema_json, schema)) => {
                debug!(
                    connector = %self.connector,
                    db,
                    table,
                    "built DDL-derived Avro envelope schema"
                );
                Some((schema_json, Arc::new(schema)))
            }
            Err(e) => {
                warn!(
                    connector = %self.connector,
                    db,
                    table,
                    error = %e,
                    "failed to build Avro schema from DDL — will fall back to JSON inference"
                );
                None
            }
        }
    }
}

impl SourceSchemaProvider for AvroSchemaProviderImpl {
    fn get_envelope_schema(
        &self,
        connector: &str,
        database: &str,
        namespace: &str,
        table: &str,
        identity: &SchemaIdentity,
    ) -> EnvelopeSchemaLookup {
        if connector != self.connector {
            return EnvelopeSchemaLookup::Absent;
        }
        let key = (
            database.to_string(),
            namespace.to_string(),
            table.to_string(),
            identity.fingerprint.clone(),
        );
        if let Some((sequence, json, schema)) = self.cache.read().get(&key) {
            return match (sequence, identity.sequence) {
                (Some(registered), Some(stamped)) if *registered != stamped => {
                    EnvelopeSchemaLookup::Conflict(format!(
                        "schema {} is registered with sequence {registered}, \
                         but the event was stamped with sequence {stamped}",
                        identity.fingerprint
                    ))
                }
                _ => EnvelopeSchemaLookup::Found(json.clone(), schema.clone()),
            };
        }

        // The lookup is async but this path is sync (called from encode):
        // block in place. It runs once per (table, schema version).
        // Registries key a table by namespace within the source's database.
        let qualified = format!("{namespace}.{table}");
        let table_schema = match tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(
                self.schema_provider.get_table_schema_version(
                    &qualified,
                    &identity.fingerprint,
                    identity.sequence,
                ),
            )
        }) {
            Ok(Some(ts)) => ts,
            Ok(None) => {
                return EnvelopeSchemaLookup::Conflict(format!(
                    "schema version {} is not registered for the table",
                    identity.fingerprint
                ));
            }
            Err(e @ SchemaLookupError::VersionConflict { .. }) => {
                return EnvelopeSchemaLookup::Conflict(e.to_string());
            }
            // Not cached: the next event retries the lookup.
            Err(e) => return EnvelopeSchemaLookup::Unavailable(e.to_string()),
        };

        // Avro names carry the relation's escaped database, schema
        // (PostgreSQL) and table: same-named tables never share a name.
        let (avro_ns, avro_table) =
            deltaforge_core::relation::avro_path(database, namespace, table);
        let Some((json, schema)) =
            self.build_for_table(&avro_ns, &avro_table, &table_schema)
        else {
            return EnvelopeSchemaLookup::Absent;
        };
        self.cache
            .write()
            .insert(key, (identity.sequence, json.clone(), schema.clone()));
        EnvelopeSchemaLookup::Found(json, schema)
    }

    fn mapping_id(&self) -> &str {
        &self.mapping_id
    }
}

/// Convert `ColumnSchemaInfo` to `ColumnDesc` (the avro_types input).
fn column_info_to_desc(col: &ColumnSchemaInfo) -> ColumnDesc {
    ColumnDesc {
        name: col.name.clone(),
        data_type: col.data_type.to_lowercase(),
        column_type: col.full_type.clone(),
        nullable: col.nullable,
        precision: col.numeric_precision,
        scale: col.numeric_scale,
        unsigned: col.unsigned,
        is_array: col.is_array,
        element_type: col.element_type.clone(),
    }
}

// ============================================================================
// Arrow schema resolver (for S3 / Parquet sink)
// ============================================================================

/// Build a `SchemaResolver` closure for the S3 sink that maps a partition's
/// table name to a DDL-derived Arrow envelope schema.
///
/// Behavior:
/// - Looks up `TableSchemaInfo` for `(connector, source.table)` from the
///   provided `SchemaProvider`.
/// - Converts each column via the same `column_info_to_desc` used by the
///   Avro path (one source of truth for type policy).
/// - Builds the Arrow envelope schema with the configured `TypeConversionOpts`.
/// - Caches results per `(connector, table)` so repeated lookups are O(1).
///
/// Fallback: if no `TableSchemaInfo` is registered for a table (e.g.,
/// snapshot has not yet completed, schema sensing not yet warmed up), an
/// envelope-only schema (meta columns only) is returned. The fallback is
/// logged once per table so operators notice.
pub fn build_arrow_schema_resolver(
    schema_provider: ArcSchemaProvider,
    connector: &str,
    opts: deltaforge_core::encoding::avro_types::TypeConversionOpts,
) -> sinks::s3::SchemaResolver {
    use deltaforge_core::encoding::arrow_schema::{
        Connector as ArrowConnector, build_envelope_arrow_schema_arc,
    };
    use parking_lot::RwLock;
    use sinks::s3::PartitionKey;
    use std::sync::Arc;

    let arrow_connector = match connector {
        "mysql" => ArrowConnector::Mysql,
        "postgresql" | "postgres" => ArrowConnector::Postgres,
        _ => ArrowConnector::Mysql, // generic fallback
    };

    // Use the re-exported arrow_schema types from the parquet/sinks crates'
    // dependency closure — we rely on arrow_schema being on the same major
    // version (58) as the sinks crate that consumes the returned closure.
    use ::arrow_schema::Schema as ArrowSchema;
    // Keyed by the relation's parts (database, namespace, table), never a
    // joined string.
    type RelationKey = (String, String, String);
    let cache: Arc<RwLock<HashMap<RelationKey, Arc<ArrowSchema>>>> =
        Arc::new(RwLock::new(HashMap::new()));
    let fallback_logged: Arc<RwLock<HashMap<String, ()>>> =
        Arc::new(RwLock::new(HashMap::new()));

    Arc::new(move |partition: &PartitionKey| {
        // Resolve by fully-qualified name. The schema registry is keyed by
        // "db.table", but the partition carries the bare table name — without
        // the db qualifier the lookup misses and the sink silently falls back
        // to an envelope-only schema (meta columns, no row data). Fall back to
        // the bare name when the source has no database concept (db empty).
        let lookup_key = if partition.namespace.is_empty() {
            partition.table.clone()
        } else {
            format!("{}.{}", partition.namespace, partition.table)
        };

        let relation_key = (
            partition.database.clone(),
            partition.namespace.clone(),
            partition.table.clone(),
        );
        if let Some(cached) = cache.read().get(&relation_key) {
            return Ok(cached.clone());
        }

        // The provider lookup is async; we block on the current runtime —
        // same pattern as `AvroSchemaProviderImpl`.
        let provider_clone = schema_provider.clone();
        let key_for_lookup = lookup_key.clone();
        let table_schema = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(provider_clone.get_table_schema(&key_for_lookup))
        });

        let schema = match table_schema {
            // Never fall back to (and cache) an envelope-only schema when the
            // source schema cannot be read yet: fail this writer so the batch
            // is retried.
            Err(e) => return Err(anyhow::Error::new(e)),
            Ok(Some(ts)) => {
                let cols: Vec<ColumnDesc> =
                    ts.columns.iter().map(column_info_to_desc).collect();
                build_envelope_arrow_schema_arc(arrow_connector, &cols, &opts)
                    .map_err(|e| anyhow::anyhow!("{lookup_key}: {e}"))?
            }
            Ok(None) => {
                // Log the fallback once per table.
                let mut logged = fallback_logged.write();
                if logged.insert(lookup_key.clone(), ()).is_none() {
                    warn!(
                        table = %lookup_key,
                        "no schema registered for this table: its rows cannot \
                         be encoded (they fail, never dropped) until the source \
                         has loaded the table schema"
                    );
                }
                // Not cached: the table's schema is used once it exists.
                return build_envelope_arrow_schema_arc(
                    arrow_connector,
                    &[],
                    &opts,
                )
                .map_err(|e| anyhow::anyhow!("{lookup_key}: {e}"));
            }
        };

        cache.write().insert(relation_key, schema.clone());
        Ok(schema)
    })
}

/// Build a `ClickHouseSchemaResolver` for the ClickHouse sink: maps a source
/// table (`"namespace.table"`) to its columns + primary key from the schema
/// registry. Mirrors `build_arrow_schema_resolver`'s inversion so the `sinks`
/// crate never depends on `runner`.
pub fn build_clickhouse_schema_resolver(
    schema_provider: ArcSchemaProvider,
) -> sinks::clickhouse::ClickHouseSchemaResolver {
    use sinks::clickhouse::TableColumns;
    use sinks::clickhouse::types::ColDesc;
    use std::sync::Arc;

    Arc::new(move |key: &str| {
        let provider = schema_provider.clone();
        let key_owned = key.to_string();
        let Some(ts) = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(provider.get_table_schema(&key_owned))
        })?
        else {
            return Ok(None);
        };
        let columns = ts
            .columns
            .iter()
            .map(|c| ColDesc {
                name: c.name.clone(),
                data_type: c.data_type.clone(),
                full_type: c.full_type.clone(),
                nullable: c.nullable,
                unsigned: c.unsigned,
                precision: c.numeric_precision,
                scale: c.numeric_scale,
            })
            .collect();
        Ok(Some(TableColumns {
            columns,
            primary_key: ts.primary_key.clone(),
        }))
    })
}

/// Build an `EsSchemaResolver` for the Elasticsearch sink. It needs the same
/// source columns + primary key (types drive the generated index mapping, the
/// PK drives `_id`), so it reuses the ClickHouse resolver — both alias the same
/// `TableColumns` closure type.
pub fn build_elasticsearch_schema_resolver(
    schema_provider: ArcSchemaProvider,
) -> sinks::elasticsearch::EsSchemaResolver {
    build_clickhouse_schema_resolver(schema_provider)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_json_type() {
        assert!(is_json_type("json"));
        assert!(is_json_type("JSON"));
        assert!(is_json_type("jsonb"));
        assert!(!is_json_type("varchar"));
        assert!(!is_json_type("int"));
    }

    #[test]
    fn test_might_be_json() {
        assert!(might_be_json("text"));
        assert!(might_be_json("TEXT"));
        assert!(might_be_json("mediumtext"));
        assert!(!might_be_json("varchar"));
        assert!(!might_be_json("json")); // definite JSON, not "might be"
    }

    #[test]
    fn test_table_schema_info() {
        let schema = TableSchemaInfo {
            database: "shop".into(),
            table: "orders".into(),
            columns: vec![
                ColumnSchemaInfo {
                    name: "id".into(),
                    data_type: "int".into(),
                    full_type: "int".into(),
                    nullable: false,
                    ..Default::default()
                },
                ColumnSchemaInfo {
                    name: "metadata".into(),
                    data_type: "json".into(),
                    full_type: "json".into(),
                    nullable: true,
                    is_json_like: true,
                    ..Default::default()
                },
            ],
            primary_key: vec!["id".into()],
        };

        assert_eq!(schema.json_columns().count(), 1);
        assert!(schema.is_json_column("metadata"));
        assert!(!schema.is_json_column("id"));
    }

    // -----------------------------------------------------------------------
    // build_arrow_schema_resolver
    // -----------------------------------------------------------------------

    use async_trait::async_trait;
    use sinks::s3::PartitionKey;

    struct FakeProvider {
        tables: HashMap<String, TableSchemaInfo>,
    }

    #[async_trait]
    impl SchemaProvider for FakeProvider {
        async fn get_table_schema(
            &self,
            table: &str,
        ) -> Result<Option<TableSchemaInfo>, SchemaLookupError> {
            Ok(self.tables.get(table).cloned())
        }
        async fn list_schemas(&self) -> Vec<TableSchemaInfo> {
            self.tables.values().cloned().collect()
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn arrow_resolver_returns_schema_with_user_columns() {
        let mut tables = HashMap::new();
        tables.insert(
            "orders".to_string(),
            TableSchemaInfo {
                database: "shop".into(),
                table: "orders".into(),
                columns: vec![
                    ColumnSchemaInfo {
                        name: "id".into(),
                        data_type: "bigint".into(),
                        full_type: "bigint".into(),
                        nullable: false,
                        is_json_like: false,
                        unsigned: false,
                        is_array: false,
                        numeric_precision: None,
                        numeric_scale: None,
                        element_type: None,
                    },
                    ColumnSchemaInfo {
                        name: "amount".into(),
                        data_type: "decimal".into(),
                        full_type: "decimal(10,2)".into(),
                        nullable: true,
                        is_json_like: false,
                        unsigned: false,
                        is_array: false,
                        numeric_precision: Some(10),
                        numeric_scale: Some(2),
                        element_type: None,
                    },
                ],
                primary_key: vec!["id".into()],
            },
        );

        let provider: ArcSchemaProvider = Arc::new(FakeProvider { tables });
        let resolver = build_arrow_schema_resolver(
            provider,
            "mysql",
            deltaforge_core::encoding::avro_types::TypeConversionOpts::default(
            ),
        );

        let key = PartitionKey {
            database: String::new(),
            namespace: String::new(),
            table: "orders".into(),
            year: 2026,
            month: 5,
            day: 19,
            type_mapping: deltaforge_core::TypeMapping::V2,
        };
        let schema = resolver(&key).unwrap();
        let names: Vec<&str> =
            schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(names.contains(&"op"));
        assert!(names.contains(&"after_id"));
        assert!(names.contains(&"after_amount"));
        // Decimal128 round-trip: schema should declare it natively, not Utf8.
        let amount = schema
            .field_with_name("after_amount")
            .unwrap()
            .data_type()
            .clone();
        assert!(matches!(amount, arrow_schema::DataType::Decimal128(10, 2)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn arrow_resolver_looks_up_db_qualified_name() {
        // Regression: the S3 partition carries the bare table name, but the
        // registry is keyed by "db.table". The resolver must query the
        // qualified name — otherwise it silently returns an envelope-only
        // schema (meta columns only, no row data).
        let mut tables = HashMap::new();
        tables.insert(
            "shop.orders".to_string(),
            TableSchemaInfo {
                database: "shop".into(),
                table: "orders".into(),
                columns: vec![ColumnSchemaInfo {
                    name: "id".into(),
                    data_type: "bigint".into(),
                    full_type: "bigint".into(),
                    nullable: false,
                    is_json_like: false,
                    unsigned: false,
                    is_array: false,
                    numeric_precision: None,
                    numeric_scale: None,
                    element_type: None,
                }],
                primary_key: vec!["id".into()],
            },
        );

        let provider: ArcSchemaProvider = Arc::new(FakeProvider { tables });
        let resolver = build_arrow_schema_resolver(
            provider,
            "mysql",
            deltaforge_core::encoding::avro_types::TypeConversionOpts::default(
            ),
        );

        // Bare "orders" alone would miss the "shop.orders" registry key.
        let key = PartitionKey {
            database: "shop".into(),
            namespace: "shop".into(),
            table: "orders".into(),
            year: 2026,
            month: 5,
            day: 19,
            type_mapping: deltaforge_core::TypeMapping::V2,
        };
        let schema = resolver(&key).unwrap();
        let names: Vec<&str> =
            schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            names.contains(&"after_id"),
            "resolver must resolve user columns via db.table, got {names:?}"
        );
    }

    /// Reports the source schema as unavailable on its first lookup, then
    /// serves it - the shape of a request racing the source's startup.
    struct UnavailableThenReady {
        calls: std::sync::atomic::AtomicUsize,
        schema: TableSchemaInfo,
    }

    #[async_trait]
    impl SchemaProvider for UnavailableThenReady {
        async fn get_table_schema(
            &self,
            table: &str,
        ) -> Result<Option<TableSchemaInfo>, SchemaLookupError> {
            if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0
            {
                return Err(SchemaLookupError::LineageNotEstablished {
                    table: table.to_string(),
                });
            }
            Ok(Some(self.schema.clone()))
        }
        async fn get_table_schema_version(
            &self,
            table: &str,
            _fingerprint: &str,
            _sequence: Option<u64>,
        ) -> Result<Option<TableSchemaInfo>, SchemaLookupError> {
            self.get_table_schema(table).await
        }
        async fn list_schemas(&self) -> Vec<TableSchemaInfo> {
            vec![]
        }
    }

    fn orders_schema() -> TableSchemaInfo {
        TableSchemaInfo {
            database: "shop".into(),
            table: "orders".into(),
            columns: vec![ColumnSchemaInfo {
                name: "id".into(),
                data_type: "bigint".into(),
                full_type: "bigint".into(),
                nullable: false,
                is_json_like: false,
                unsigned: false,
                is_array: false,
                numeric_precision: None,
                numeric_scale: None,
                element_type: None,
            }],
            primary_key: vec!["id".into()],
        }
    }

    fn flaky() -> ArcSchemaProvider {
        Arc::new(UnavailableThenReady {
            calls: Default::default(),
            schema: orders_schema(),
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn arrow_resolver_never_caches_an_unavailable_schema() {
        let resolver = build_arrow_schema_resolver(
            flaky(),
            "mysql",
            deltaforge_core::encoding::avro_types::TypeConversionOpts::default(
            ),
        );
        let key = PartitionKey {
            database: "shop".into(),
            namespace: "shop".into(),
            table: "orders".into(),
            year: 2026,
            month: 5,
            day: 19,
            type_mapping: deltaforge_core::TypeMapping::V2,
        };
        // Unavailable: the writer fails (retried) instead of writing an
        // envelope-only file...
        let err = resolver(&key).unwrap_err();
        assert!(
            err.downcast_ref::<SchemaLookupError>().is_some(),
            "typed reason must be preserved: {err:#}"
        );
        // ...and nothing was cached: the next attempt gets the real columns.
        let schema = resolver(&key).unwrap();
        assert!(schema.field_with_name("after_id").is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn clickhouse_resolver_surfaces_unavailable_as_an_error() {
        let resolver = build_clickhouse_schema_resolver(flaky());
        let Err(err) = resolver("shop.orders") else {
            panic!("an unavailable schema must be an error");
        };
        assert!(err.downcast_ref::<SchemaLookupError>().is_some(), "{err:#}");
        let cols = resolver("shop.orders").unwrap().unwrap();
        assert_eq!(cols.primary_key, vec!["id".to_string()]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn avro_provider_reports_unavailable_and_does_not_cache_it() {
        let avro = AvroSchemaProviderImpl::new(
            flaky(),
            "mysql",
            TypeConversionOpts::default(),
        );
        let v1 = SchemaIdentity::of(Some("h1"), Some(1)).unwrap();
        assert!(matches!(
            avro.get_envelope_schema("mysql", "shop", "shop", "orders", &v1),
            EnvelopeSchemaLookup::Unavailable(_)
        ));
        assert!(matches!(
            avro.get_envelope_schema("mysql", "shop", "shop", "orders", &v1),
            EnvelopeSchemaLookup::Found(..)
        ));
    }

    /// Serves registered versions of `shop.orders` by fingerprint, checking
    /// the sequence as the durable registry does, and counts lookups.
    struct Versions {
        versions: HashMap<String, (u64, TableSchemaInfo)>,
        lookups: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl SchemaProvider for Versions {
        async fn get_table_schema(
            &self,
            _table: &str,
        ) -> Result<Option<TableSchemaInfo>, SchemaLookupError> {
            panic!("the latest schema must never be used for an event");
        }
        async fn get_table_schema_version(
            &self,
            table: &str,
            fingerprint: &str,
            sequence: Option<u64>,
        ) -> Result<Option<TableSchemaInfo>, SchemaLookupError> {
            self.lookups
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let Some((registered, ts)) = self.versions.get(fingerprint) else {
                return Ok(None);
            };
            match sequence {
                Some(s) if s != *registered => {
                    Err(SchemaLookupError::VersionConflict {
                        table: table.to_string(),
                        reason: format!("registered {registered}, stamped {s}"),
                    })
                }
                _ => Ok(Some(ts.clone())),
            }
        }
        async fn list_schemas(&self) -> Vec<TableSchemaInfo> {
            vec![]
        }
    }

    fn two_versions() -> Arc<Versions> {
        let v1 = orders_schema();
        let mut v2 = orders_schema();
        v2.columns.push(ColumnSchemaInfo {
            name: "extra".into(),
            data_type: "int".into(),
            full_type: "int".into(),
            nullable: true,
            ..Default::default()
        });
        Arc::new(Versions {
            versions: HashMap::from([
                ("h1".to_string(), (1, v1)),
                ("h2".to_string(), (2, v2)),
            ]),
            lookups: Default::default(),
        })
    }

    fn columns_of(lookup: EnvelopeSchemaLookup) -> Vec<String> {
        let EnvelopeSchemaLookup::Found(json, _) = lookup else {
            panic!("expected a schema, got {lookup:?}");
        };
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        let after = v["fields"]
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["name"] == "after")
            .unwrap();
        after["type"][1]["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["name"].as_str().unwrap().to_string())
            .collect()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn avro_provider_builds_each_events_own_version() {
        let versions = two_versions();
        let avro = AvroSchemaProviderImpl::new(
            versions.clone(),
            "mysql",
            TypeConversionOpts::default(),
        );
        let v1 = SchemaIdentity::of(Some("h1"), Some(1)).unwrap();
        let v2 = SchemaIdentity::of(Some("h2"), Some(2)).unwrap();
        // Old, new, old: each request gets exactly its version.
        for (id, cols) in [
            (&v1, vec!["id"]),
            (&v2, vec!["id", "extra"]),
            (&v1, vec!["id"]),
        ] {
            assert_eq!(
                columns_of(avro.get_envelope_schema(
                    "mysql", "shop", "shop", "orders", id
                )),
                cols
            );
        }
        assert_eq!(
            versions.lookups.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "one lookup per version"
        );
        // Evicted and reloaded: still exactly the requested version.
        avro.invalidate_all();
        assert_eq!(
            columns_of(
                avro.get_envelope_schema(
                    "mysql", "shop", "shop", "orders", &v1
                )
            ),
            vec!["id"]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn avro_provider_fails_closed_on_a_conflicting_or_unknown_version() {
        let avro = AvroSchemaProviderImpl::new(
            two_versions(),
            "mysql",
            TypeConversionOpts::default(),
        );
        // The registry's own sequence check.
        let stale = SchemaIdentity::of(Some("h1"), Some(7)).unwrap();
        assert!(matches!(
            avro.get_envelope_schema("mysql", "shop", "shop", "orders", &stale),
            EnvelopeSchemaLookup::Conflict(_)
        ));
        // A cached fingerprint requested under another sequence.
        let v1 = SchemaIdentity::of(Some("h1"), Some(1)).unwrap();
        assert!(matches!(
            avro.get_envelope_schema("mysql", "shop", "shop", "orders", &v1),
            EnvelopeSchemaLookup::Found(..)
        ));
        assert!(matches!(
            avro.get_envelope_schema("mysql", "shop", "shop", "orders", &stale),
            EnvelopeSchemaLookup::Conflict(m) if m.contains("sequence 1")
        ));
        // A fingerprint the registry does not know.
        let unknown = SchemaIdentity::of(Some("h9"), Some(9)).unwrap();
        assert!(matches!(
            avro.get_envelope_schema(
                "mysql", "shop", "shop", "orders", &unknown
            ),
            EnvelopeSchemaLookup::Conflict(_)
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn arrow_resolver_caches_lookups() {
        // Run the resolver twice for the same table; second call must not
        // hit the underlying provider (the fake panics on second call).
        struct OneShotProvider {
            schema: parking_lot::Mutex<Option<TableSchemaInfo>>,
        }
        #[async_trait]
        impl SchemaProvider for OneShotProvider {
            async fn get_table_schema(
                &self,
                _table: &str,
            ) -> Result<Option<TableSchemaInfo>, SchemaLookupError>
            {
                Ok(self.schema.lock().take())
            }
            async fn list_schemas(&self) -> Vec<TableSchemaInfo> {
                vec![]
            }
        }

        let schema_info = TableSchemaInfo {
            database: "shop".into(),
            table: "orders".into(),
            columns: vec![ColumnSchemaInfo {
                name: "id".into(),
                data_type: "bigint".into(),
                full_type: "bigint".into(),
                nullable: false,
                is_json_like: false,
                unsigned: false,
                is_array: false,
                numeric_precision: None,
                numeric_scale: None,
                element_type: None,
            }],
            primary_key: vec!["id".into()],
        };
        let provider: ArcSchemaProvider = Arc::new(OneShotProvider {
            schema: parking_lot::Mutex::new(Some(schema_info)),
        });
        let resolver = build_arrow_schema_resolver(
            provider,
            "mysql",
            deltaforge_core::encoding::avro_types::TypeConversionOpts::default(
            ),
        );

        let key = PartitionKey {
            database: String::new(),
            namespace: String::new(),
            table: "orders".into(),
            year: 2026,
            month: 5,
            day: 19,
            type_mapping: deltaforge_core::TypeMapping::V2,
        };
        let s1 = resolver(&key).unwrap();
        let s2 = resolver(&key).unwrap(); // would unwrap None from OneShotProvider without cache
        assert_eq!(Arc::as_ptr(&s1), Arc::as_ptr(&s2), "cached schema reused");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn arrow_resolver_falls_back_to_envelope_only_when_no_schema() {
        let provider: ArcSchemaProvider = Arc::new(FakeProvider {
            tables: HashMap::new(),
        });
        let resolver = build_arrow_schema_resolver(
            provider,
            "mysql",
            deltaforge_core::encoding::avro_types::TypeConversionOpts::default(
            ),
        );

        let key = PartitionKey {
            database: String::new(),
            namespace: String::new(),
            table: "missing".into(),
            year: 2026,
            month: 5,
            day: 19,
            type_mapping: deltaforge_core::TypeMapping::V2,
        };
        let schema = resolver(&key).unwrap();
        let names: Vec<&str> =
            schema.fields().iter().map(|f| f.name().as_str()).collect();
        // Envelope meta and the stable extension slots only; no
        // before_*/after_* user columns.
        assert!(names.contains(&"op"));
        let user = |n: &&str| {
            (n.starts_with("before_") || n.starts_with("after_"))
                && !n.ends_with("_extensions")
        };
        assert!(!names.iter().any(user));
        assert!(names.contains(&"before_extensions"));
        assert!(names.contains(&"after_extensions"));
    }
}

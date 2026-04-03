//! Database schema provider abstraction.
//!
//! Provides a unified interface for accessing database schema information
//! from various sources (MySQL, Postgres, Turso, etc.).

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

/// Trait for providing database schema information.
#[async_trait]
pub trait SchemaProvider: Send + Sync {
    /// Get schema for a table.
    ///
    /// The `table` parameter may be in various formats:
    /// - "table_name" (table only)
    /// - "db.table" (database.table)
    /// - "schema.table" (for Postgres)
    async fn get_table_schema(&self, table: &str) -> Option<TableSchemaInfo>;

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
    async fn get_table_schema(&self, table: &str) -> Option<TableSchemaInfo> {
        // Parse "db.table" format
        let (db, tbl) = match table.split_once('.') {
            Some((d, t)) => (d, t),
            None => ("", table),
        };

        let loaded = self.loader.load(db, tbl).await.ok()?;

        // Convert LoadedSchema to TableSchemaInfo
        let columns = extract_column_infos(&loaded.schema_json);

        Some(TableSchemaInfo {
            database: loaded.database,
            table: loaded.table,
            columns,
            primary_key: loaded.primary_key,
        })
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
use deltaforge_core::encoding::avro::SourceSchemaProvider;
use deltaforge_core::encoding::avro_schema::{
    build_envelope_schema, build_value_schema,
};
use deltaforge_core::encoding::avro_types::{
    ColumnDesc, TypeConversionOpts, mysql_column_to_avro,
    postgres_column_to_avro,
};
use parking_lot::RwLock;
use tracing::{debug, warn};

/// Cached Avro envelope schema entry.
type CachedAvroSchema = (String, Arc<AvroSchema>);

/// Implements `SourceSchemaProvider` by looking up table schemas from
/// the `SchemaProvider` (which reads from the internal schema registry)
/// and converting column types to Avro using the type converters.
///
/// Caches envelope schemas per `(db, table)` to avoid re-deriving on
/// every event.
pub struct AvroSchemaProviderImpl {
    /// The underlying schema provider (reads from internal registry).
    schema_provider: ArcSchemaProvider,
    /// Source connector type ("mysql", "postgresql", etc.)
    connector: String,
    /// Type conversion options.
    opts: TypeConversionOpts,
    /// Cache: (db, table) → (schema_json, parsed AvroSchema)
    cache: RwLock<HashMap<(String, String), CachedAvroSchema>>,
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
            opts,
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// Invalidate cached schema for a table (called on DDL change).
    pub fn invalidate(&self, db: &str, table: &str) {
        let mut cache = self.cache.write();
        if cache.remove(&(db.to_string(), table.to_string())).is_some() {
            debug!(db, table, "invalidated cached Avro schema");
        }
    }

    /// Invalidate all cached schemas.
    #[allow(dead_code)]
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
        db: &str,
        table: &str,
    ) -> Option<(String, Arc<AvroSchema>)> {
        // Ignore if connector doesn't match
        if connector != self.connector {
            return None;
        }

        // Check cache
        {
            let cache = self.cache.read();
            if let Some(cached) =
                cache.get(&(db.to_string(), table.to_string()))
            {
                return Some(cached.clone());
            }
        }

        // Look up table schema from the underlying provider (blocking).
        // SchemaProvider::get_table_schema is async, but SourceSchemaProvider
        // is sync (called from the encode path). We use the cached/loaded
        // schemas that are already populated by the coordinator at startup.
        //
        // For the initial implementation, we use tokio::task::block_in_place
        // to call the async method. This is acceptable because:
        // 1. Schema lookups are rare (only on first event per table + DDL changes)
        // 2. The underlying SchemaLoaderAdapter typically hits an in-memory cache
        let table_key = format!("{db}.{table}");
        let table_schema = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(self.schema_provider.get_table_schema(&table_key))
        })?;

        // Build the envelope schema
        let result = self.build_for_table(db, table, &table_schema)?;

        // Cache it
        {
            let mut cache = self.cache.write();
            cache.insert((db.to_string(), table.to_string()), result.clone());
        }

        Some(result)
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
}

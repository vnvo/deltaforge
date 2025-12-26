//! Database schema provider abstraction.
//!
//! Provides a unified interface for accessing database schema information
//! from various sources (MySQL, Postgres, Turso, etc.).

use async_trait::async_trait;
use sources::ArcSchemaLoader;
use std::sync::Arc;

/// Information about a single column.
#[derive(Debug, Clone)]
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
            .filter_map(|entry| {
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

            Some(ColumnSchemaInfo {
                name,
                data_type: data_type.clone(),
                full_type,
                nullable,
                is_json_like: is_json_type(&data_type)
                    || might_be_json(&data_type),
            })
        })
        .collect()
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
                    is_json_like: false,
                },
                ColumnSchemaInfo {
                    name: "metadata".into(),
                    data_type: "json".into(),
                    full_type: "json".into(),
                    nullable: true,
                    is_json_like: true,
                },
            ],
            primary_key: vec!["id".into()],
        };

        assert_eq!(schema.json_columns().count(), 1);
        assert!(schema.is_json_column("metadata"));
        assert!(!schema.is_json_column("id"));
    }
}

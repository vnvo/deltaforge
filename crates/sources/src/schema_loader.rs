//! Unified schema loader trait for database sources.
//!
//! This trait provides a common interface for loading schemas while
//! preserving source-owned semantics. Schemas are returned as JSON
//! with metadata, allowing each source to maintain its native structure.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Loaded schema with metadata.
///
/// The schema itself is JSON to preserve source-specific structure.
/// Metadata provides common fields needed by the API layer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadedSchema {
    /// Database/schema name ("main" for SQLite)
    pub database: String,
    /// Table name
    pub table: String,
    /// Schema as JSON (source-specific structure)
    pub schema_json: serde_json::Value,
    /// Column names in ordinal order
    pub columns: Vec<String>,
    /// Primary key columns
    pub primary_key: Vec<String>,
    /// Content fingerprint (SHA-256)
    pub fingerprint: String,
    /// Version in schema registry
    pub registry_version: i32,
    /// When loaded
    pub loaded_at: DateTime<Utc>,
}

/// Summary for listing schemas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaListEntry {
    pub database: String,
    pub table: String,
    pub column_count: usize,
    pub primary_key: Vec<String>,
    pub fingerprint: String,
    pub registry_version: i32,
}

impl From<&LoadedSchema> for SchemaListEntry {
    fn from(s: &LoadedSchema) -> Self {
        Self {
            database: s.database.clone(),
            table: s.table.clone(),
            column_count: s.columns.len(),
            primary_key: s.primary_key.clone(),
            fingerprint: s.fingerprint.clone(),
            registry_version: s.registry_version,
        }
    }
}

/// Trait for loading and caching database schemas.
#[async_trait]
pub trait SourceSchemaLoader: Send + Sync {
    /// Source type identifier ("mysql", "postgres").
    fn source_type(&self) -> &'static str;

    /// Load schema for a table (cached if available).
    async fn load(&self, db: &str, table: &str) -> Result<LoadedSchema>;

    /// Force reload from database.
    async fn reload(&self, db: &str, table: &str) -> Result<LoadedSchema>;

    /// Reload all schemas matching patterns.
    /// Returns (database, table) pairs that were reloaded.
    async fn reload_all(
        &self,
        patterns: &[String],
    ) -> Result<Vec<(String, String)>>;

    /// List cached schemas.
    async fn list_cached(&self) -> Vec<SchemaListEntry>;

    /// Whether the source has established the verified lineage its schema
    /// registry access is scoped by. Until it has, `load`/`reload`/`reload_all`
    /// fail with a typed
    /// [`RegistryError::NotEstablished`](crate::registry_scope::RegistryError)
    /// (detect it with
    /// [`is_lineage_unavailable`](crate::registry_scope::is_lineage_unavailable)).
    fn lineage_established(&self) -> bool {
        true
    }

    /// The registered version of `db.table` whose fingerprint is
    /// `fingerprint`: the schema an event stamped with that fingerprint (and,
    /// when given, registry `sequence`) was decoded with, never the table's
    /// latest. `None` when no such version is registered (or the source keeps
    /// no versions); a version whose sequence differs from `sequence` is a
    /// [`SchemaVersionConflict`].
    async fn load_version(
        &self,
        db: &str,
        table: &str,
        fingerprint: &str,
        sequence: Option<u64>,
    ) -> Result<Option<LoadedSchema>> {
        let _ = (db, table, fingerprint, sequence);
        Ok(None)
    }
}

/// A registered fingerprint whose registry sequence differs from the one an
/// event was stamped with: the event's schema cannot be identified.
#[derive(Debug, thiserror::Error)]
#[error(
    "schema of {table}: fingerprint {fingerprint} is registered at sequence \
     {registered}, but the event was stamped with sequence {stamped}"
)]
pub struct SchemaVersionConflict {
    pub table: String,
    pub fingerprint: String,
    pub registered: u64,
    pub stamped: u64,
}

/// [`SourceSchemaLoader::load_version`] over a durable registry: the version
/// of `key` with content hash `fingerprint`, checked against `sequence`.
pub(crate) async fn load_registered_version(
    registry: &storage::DurableSchemaRegistry,
    key: &storage::adapters::SchemaKey,
    db: &str,
    table: &str,
    fingerprint: &str,
    sequence: Option<u64>,
) -> Result<Option<LoadedSchema>> {
    let Some(v) = registry.get_by_hash(key, fingerprint).await? else {
        return Ok(None);
    };
    if let Some(stamped) = sequence
        && stamped != v.sequence
    {
        return Err(SchemaVersionConflict {
            table: format!("{db}.{table}"),
            fingerprint: fingerprint.to_string(),
            registered: v.sequence,
            stamped,
        }
        .into());
    }
    let names = |field: &str| -> Vec<String> {
        v.schema_json[field]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|c| {
                        c.as_str()
                            .or_else(|| c["name"].as_str())
                            .map(str::to_string)
                    })
                    .collect()
            })
            .unwrap_or_default()
    };
    Ok(Some(LoadedSchema {
        database: db.to_string(),
        table: table.to_string(),
        columns: names("columns"),
        primary_key: names("primary_key"),
        schema_json: v.schema_json.clone(),
        fingerprint: v.hash.clone(),
        registry_version: v.version,
        loaded_at: Utc::now(),
    }))
}

/// Arc-wrapped schema loader.
pub type ArcSchemaLoader = Arc<dyn SourceSchemaLoader>;

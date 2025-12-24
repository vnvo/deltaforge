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
    /// Source type identifier ("mysql", "turso", "postgres").
    fn source_type(&self) -> &'static str;

    /// Load schema for a table (cached if available).
    async fn load(&self, db: &str, table: &str) -> Result<LoadedSchema>;

    /// Force reload from database.
    async fn reload(&self, db: &str, table: &str) -> Result<LoadedSchema>;

    /// Reload all schemas matching patterns.
    /// Returns (database, table) pairs that were reloaded.
    async fn reload_all(&self, patterns: &[String]) -> Result<Vec<(String, String)>>;

    /// List cached schemas.
    async fn list_cached(&self) -> Vec<SchemaListEntry>;
}

/// Arc-wrapped schema loader.
pub type ArcSchemaLoader = Arc<dyn SourceSchemaLoader>;
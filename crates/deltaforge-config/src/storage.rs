use serde::{Deserialize, Serialize};

/// Top-level storage configuration.
///
/// ```yaml
/// storage:
///   backend: sqlite
///   path: ./data/deltaforge.db
///
/// # or for PostgreSQL:
/// storage:
///   backend: postgres
///   dsn: "host=localhost dbname=deltaforge user=df password=secret"
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct StorageConfig {
    #[serde(default)]
    pub backend: StorageBackendKind,

    /// Path for SQLite database file (sqlite backend only).
    #[serde(default = "default_sqlite_path")]
    pub path: String,

    /// PostgreSQL connection string (postgres backend only).
    pub dsn: Option<String>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: StorageBackendKind::Sqlite,
            path: default_sqlite_path(),
            dsn: None,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackendKind {
    #[default]
    Sqlite,
    Memory,
    Postgres,
}

fn default_sqlite_path() -> String {
    "./data/deltaforge.db".to_string()
}

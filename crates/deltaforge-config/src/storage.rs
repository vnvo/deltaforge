use serde::{Deserialize, Serialize};

/// Top-level storage configuration.
///
/// Controls where all operational runtime state is persisted:
/// checkpoints, schema registry, FSM state, leases, dedup, quarantine, DLQ.
///
/// ```yaml
/// storage:
///   backend: sqlite
///   path: ./data/deltaforge.db
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct StorageConfig {
    #[serde(default)]
    pub backend: StorageBackendKind,

    /// Path for SQLite database file (only used when backend = sqlite).
    #[serde(default = "default_sqlite_path")]
    pub path: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: StorageBackendKind::Sqlite,
            path: default_sqlite_path(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackendKind {
    #[default]
    Sqlite,
    Memory, // for testing / ephemeral deployments
}

fn default_sqlite_path() -> String {
    "./data/deltaforge.db".to_string()
}

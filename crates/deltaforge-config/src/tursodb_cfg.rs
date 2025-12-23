use serde::{Deserialize, Serialize};

/// Configuration for TursoDB/SQLite CDC source.
///
/// Turso's SQLite rewrite includes native CDC support via the `turso_cdc` table.
/// For standard SQLite without native CDC, we fall back to triggers or polling.
///
/// CDC Modes:
/// - **Native** (default): Uses Turso's built-in CDC via `PRAGMA unstable_capture_data_changes_conn`
/// - **Triggers**: Shadow tables populated by triggers (for standard SQLite)
/// - **Polling**: Track changes via rowid/timestamp columns (inserts only)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TursoSrcCfg {
    /// Unique identifier for this source instance
    pub id: String,

    /// Connection URL for Turso/LibSQL
    /// Formats:
    /// - `libsql://your-db.turso.io` (Turso cloud - use auth_token)
    /// - `file:///path/to/local.db` (local SQLite)
    /// - `http://localhost:8080` (local sqld server)
    pub url: String,

    /// Auth token for Turso cloud connections (optional for local)
    #[serde(default)]
    pub auth_token: Option<String>,

    /// Tables to capture changes from.
    /// Format: `table_name` (no schema prefix for SQLite)
    /// Supports wildcards: `orders%`, `*`
    pub tables: Vec<String>,

    /// CDC mode configuration
    #[serde(default)]
    pub cdc_mode: TursoCdcMode,

    /// Native CDC capture level (only for native mode)
    /// Options: id, before, after, full (default)
    #[serde(default = "default_native_cdc_level")]
    pub native_cdc_level: NativeCdcLevel,

    /// Custom CDC table name (only for native mode)
    /// Default: turso_cdc
    #[serde(default)]
    pub cdc_table_name: Option<String>,

    /// Polling interval in milliseconds (for polling/hybrid modes)
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,

    /// Column to use for change tracking in polling mode.
    /// Common patterns: `_rowid_`, `updated_at`, `version`
    #[serde(default = "default_tracking_column")]
    pub tracking_column: String,

    /// Whether to create CDC triggers automatically (for triggers mode)
    #[serde(default)]
    pub auto_create_triggers: bool,

    /// Batch size for queries
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_poll_interval_ms() -> u64 {
    1000
}

fn default_tracking_column() -> String {
    "_rowid_".to_string()
}

fn default_batch_size() -> usize {
    1000
}

fn default_native_cdc_level() -> NativeCdcLevel {
    NativeCdcLevel::Full
}

/// CDC mode for Turso/SQLite sources
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TursoCdcMode {
    /// Native CDC using Turso's built-in `turso_cdc` table.
    /// Requires Turso's SQLite rewrite (v0.1.2+).
    /// This is the recommended mode for Turso Cloud.
    #[default]
    Native,

    /// Use triggers to capture changes to shadow tables.
    /// Works with standard SQLite. Requires write access.
    Triggers,

    /// Poll tables for changes using rowid/timestamp columns.
    /// Simple but only captures inserts, not updates/deletes.
    Polling,

    /// Try native CDC first, fall back to triggers, then polling.
    Auto,
}

/// Native CDC capture level - controls what data is captured
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NativeCdcLevel {
    /// Capture only the rowid of changed rows (minimal overhead)
    Id,

    /// Capture row state before changes (for updates/deletes)
    Before,

    /// Capture row state after changes (for inserts/updates)
    After,

    /// Capture both before and after states (full CDC)
    #[default]
    Full,
}

impl NativeCdcLevel {
    /// Returns the PRAGMA value for this CDC level
    pub fn pragma_value(&self) -> &'static str {
        match self {
            NativeCdcLevel::Id => "id",
            NativeCdcLevel::Before => "before",
            NativeCdcLevel::After => "after",
            NativeCdcLevel::Full => "full",
        }
    }
}

impl TursoSrcCfg {
    /// Returns true if this is a Turso cloud connection
    pub fn is_turso_cloud(&self) -> bool {
        self.url.starts_with("libsql://") && self.url.contains(".turso.io")
    }

    /// Returns true if this is a local SQLite file
    pub fn is_local_file(&self) -> bool {
        self.url.starts_with("file://")
    }

    /// Returns the CDC table name to use
    pub fn cdc_table(&self) -> &str {
        self.cdc_table_name.as_deref().unwrap_or("turso_cdc")
    }

    /// Returns the PRAGMA statement to enable native CDC
    pub fn native_cdc_pragma(&self) -> String {
        match &self.cdc_table_name {
            Some(table) => format!(
                "PRAGMA unstable_capture_data_changes_conn('{},{}');",
                self.native_cdc_level.pragma_value(),
                table
            ),
            None => format!(
                "PRAGMA unstable_capture_data_changes_conn('{}');",
                self.native_cdc_level.pragma_value()
            ),
        }
    }
}

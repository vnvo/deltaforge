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
///
/// Schema Loading Strategy:
/// - **Native mode**: JSON provided by Turso's helper functions - no schema loader needed
/// - **Triggers mode**: JSON embedded via json_object() in triggers - no schema loader needed
/// - **Polling mode**: Requires schema loader to map raw SELECT results to JSON
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
    /// JSON is provided directly by Turso - no schema loader needed.
    #[default]
    Native,

    /// Use triggers to capture changes to shadow tables.
    /// Works with standard SQLite. Requires write access.
    /// JSON is embedded in trigger via json_object() - no schema loader needed.
    Triggers,

    /// Poll tables for changes using rowid/timestamp columns.
    /// Simple but only captures inserts, not updates/deletes.
    /// Requires schema loader to build JSON from raw SELECT results.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cdc_mode_default() {
        let mode = TursoCdcMode::default();
        assert_eq!(mode, TursoCdcMode::Native);
    }

    #[test]
    fn test_native_cdc_level_default() {
        let level = NativeCdcLevel::default();
        assert_eq!(level, NativeCdcLevel::Full);
    }

    #[test]
    fn test_pragma_values() {
        assert_eq!(NativeCdcLevel::Id.pragma_value(), "id");
        assert_eq!(NativeCdcLevel::Before.pragma_value(), "before");
        assert_eq!(NativeCdcLevel::After.pragma_value(), "after");
        assert_eq!(NativeCdcLevel::Full.pragma_value(), "full");
    }

    #[test]
    fn test_is_turso_cloud() {
        let cfg = TursoSrcCfg {
            id: "test".to_string(),
            url: "libsql://my-db.turso.io".to_string(),
            auth_token: None,
            tables: vec![],
            cdc_mode: TursoCdcMode::Native,
            native_cdc_level: NativeCdcLevel::Full,
            cdc_table_name: None,
            poll_interval_ms: 1000,
            tracking_column: "_rowid_".to_string(),
            auto_create_triggers: false,
            batch_size: 1000,
        };
        assert!(cfg.is_turso_cloud());
    }

    #[test]
    fn test_is_local_file() {
        let cfg = TursoSrcCfg {
            id: "test".to_string(),
            url: "file:///tmp/test.db".to_string(),
            auth_token: None,
            tables: vec![],
            cdc_mode: TursoCdcMode::Polling,
            native_cdc_level: NativeCdcLevel::Full,
            cdc_table_name: None,
            poll_interval_ms: 1000,
            tracking_column: "_rowid_".to_string(),
            auto_create_triggers: false,
            batch_size: 1000,
        };
        assert!(cfg.is_local_file());
    }

    #[test]
    fn test_native_cdc_pragma() {
        let cfg = TursoSrcCfg {
            id: "test".to_string(),
            url: "libsql://test.turso.io".to_string(),
            auth_token: None,
            tables: vec![],
            cdc_mode: TursoCdcMode::Native,
            native_cdc_level: NativeCdcLevel::Full,
            cdc_table_name: None,
            poll_interval_ms: 1000,
            tracking_column: "_rowid_".to_string(),
            auto_create_triggers: false,
            batch_size: 1000,
        };
        assert_eq!(
            cfg.native_cdc_pragma(),
            "PRAGMA unstable_capture_data_changes_conn('full');"
        );
    }

    #[test]
    fn test_native_cdc_pragma_custom_table() {
        let cfg = TursoSrcCfg {
            id: "test".to_string(),
            url: "libsql://test.turso.io".to_string(),
            auth_token: None,
            tables: vec![],
            cdc_mode: TursoCdcMode::Native,
            native_cdc_level: NativeCdcLevel::Before,
            cdc_table_name: Some("my_cdc".to_string()),
            poll_interval_ms: 1000,
            tracking_column: "_rowid_".to_string(),
            auto_create_triggers: false,
            batch_size: 1000,
        };
        assert_eq!(
            cfg.native_cdc_pragma(),
            "PRAGMA unstable_capture_data_changes_conn('before,my_cdc');"
        );
    }
}
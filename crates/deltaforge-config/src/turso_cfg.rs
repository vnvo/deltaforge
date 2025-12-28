//! Configuration for TursoDB CDC source.
//!
//! **STATUS: EXPERIMENTAL / PAUSED**
//!
//! See turso/mod.rs for details on why this is paused.
//!
//! ---
//!
//! The application must enable CDC on its connection with:
//! ```sql
//! PRAGMA unstable_capture_data_changes_conn('full');
//! ```
//!
//! DeltaForge then reads from the resulting `turso_cdc` table.

use serde::{Deserialize, Serialize};

/// Configuration for TursoDB CDC source.
///
/// **The application must enable CDC** - DeltaForge only reads from the CDC table.
///
/// CDC is per-connection: only changes made by connections that run the CDC
/// pragma are captured. Use `native_cdc_pragma()` to get the PRAGMA statement
/// to run in your application.
///
/// # Supported URLs
///
/// - Local file: `/path/to/database.db`
/// - Remote HTTP: `http://localhost:8080`
/// - Turso cloud: `libsql://your-db.turso.io` (requires auth_token)
///
/// # Example
///
/// ```yaml
/// source:
///   type: turso
///   config:
///     id: turso-main
///     url: "/tmp/myapp.db"
///     tables: ["users", "orders"]
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TursoSrcCfg {
    /// Unique identifier for this source instance
    pub id: String,

    /// Connection URL for Turso/LibSQL
    ///
    /// Formats:
    /// - `/path/to/database.db` (local file)
    /// - `http://localhost:8080` (local sqld server)
    /// - `libsql://your-db.turso.io` (Turso cloud - requires auth_token)
    pub url: String,

    /// Auth token for Turso cloud connections
    #[serde(default)]
    pub auth_token: Option<String>,

    /// Tables to capture changes from.
    ///
    /// Format: `table_name` (no schema prefix for SQLite)
    /// Supports wildcards: `orders%`, `*`
    pub tables: Vec<String>,

    /// Recommended CDC capture level for the application.
    ///
    /// This tells the application what PRAGMA to use. DeltaForge doesn't
    /// enable CDC - the app must do it on its own connection.
    ///
    /// Options:
    /// - `id`: Capture only the rowid (minimal overhead)
    /// - `before`: Capture row state before changes
    /// - `after`: Capture row state after changes
    /// - `full` (default): Capture both before and after states
    #[serde(default)]
    pub native_cdc_level: NativeCdcLevel,

    /// Custom CDC table name (default: `turso_cdc`)
    #[serde(default)]
    pub cdc_table_name: Option<String>,

    /// Polling interval in milliseconds
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,

    /// Batch size for CDC queries
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_poll_interval_ms() -> u64 {
    1000
}

fn default_batch_size() -> usize {
    1000
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
    /// Returns true if this is a local file path
    pub fn is_local_file(&self) -> bool {
        !self.url.starts_with("libsql://")
            && !self.url.starts_with("http://")
            && !self.url.starts_with("https://")
    }

    /// Returns true if this is a Turso cloud connection
    pub fn is_turso_cloud(&self) -> bool {
        self.url.starts_with("libsql://") && self.url.contains(".turso.io")
    }

    /// Returns the CDC table name to use
    pub fn cdc_table(&self) -> &str {
        self.cdc_table_name.as_deref().unwrap_or("turso_cdc")
    }

    /// Returns the PRAGMA statement the application should run to enable CDC
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
            native_cdc_level: NativeCdcLevel::Full,
            cdc_table_name: None,
            poll_interval_ms: 1000,
            batch_size: 1000,
        };
        assert!(cfg.is_turso_cloud());
    }

    #[test]
    fn test_native_cdc_pragma() {
        let cfg = TursoSrcCfg {
            id: "test".to_string(),
            url: "libsql://test.turso.io".to_string(),
            auth_token: None,
            tables: vec![],
            native_cdc_level: NativeCdcLevel::Full,
            cdc_table_name: None,
            poll_interval_ms: 1000,
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
            native_cdc_level: NativeCdcLevel::Before,
            cdc_table_name: Some("my_cdc".to_string()),
            poll_interval_ms: 1000,
            batch_size: 1000,
        };
        assert_eq!(
            cfg.native_cdc_pragma(),
            "PRAGMA unstable_capture_data_changes_conn('before,my_cdc');"
        );
    }

    #[test]
    fn test_deserialize_minimal() {
        let yaml = r#"
id: turso-main
url: libsql://test.turso.io
tables: ["users", "orders"]
"#;
        let cfg: TursoSrcCfg = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.id, "turso-main");
        assert_eq!(cfg.native_cdc_level, NativeCdcLevel::Full);
        assert_eq!(cfg.poll_interval_ms, 1000);
        assert_eq!(cfg.batch_size, 1000);
    }
}

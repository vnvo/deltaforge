use schema_registry::{SourceSchema, compute_fingerprint};
use serde::{Deserialize, Serialize};

/// MySQL table schema with full type information.
///
/// this captures full type information for:
/// - Schema fingerprinting and change detection
/// - Type-aware CDC event processing
/// - Schema registry integration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MySqlTableSchema {
    /// Columns in ordinal order.
    pub columns: Vec<MySqlColumn>,

    /// Primary key column names.
    #[serde(default)]
    pub primary_key: Vec<String>,

    /// Storage engine (InnoDB, MyISAM, etc.)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine: Option<String>,

    /// Default charset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub charset: Option<String>,

    /// Default collation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
}

/// MySQL column definition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MySqlColumn {
    /// Column name.
    pub name: String,

    /// Full column type (e.g., "bigint(20) unsigned").
    pub column_type: String,

    /// Base data type (e.g., "bigint").
    pub data_type: String,

    /// Whether NULL is allowed.
    pub nullable: bool,

    /// Ordinal position (1-indexed).
    pub ordinal_position: u32,

    /// Default value expression.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_value: Option<String>,

    /// Extra attributes (e.g., "auto_increment").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extra: Option<String>,

    /// Column comment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,

    /// Character maximum length.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub char_max_length: Option<i64>,

    /// Numeric precision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub numeric_precision: Option<i64>,

    /// Numeric scale.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub numeric_scale: Option<i64>,
}

impl MySqlTableSchema {
    /// Create new schema with columns.
    pub fn new(columns: Vec<MySqlColumn>) -> Self {
        Self {
            columns,
            primary_key: vec![],
            engine: None,
            charset: None,
            collation: None,
        }
    }

    /// Set primary key.
    pub fn with_primary_key(mut self, pk: Vec<String>) -> Self {
        self.primary_key = pk;
        self
    }

    /// Set engine.
    pub fn with_engine(mut self, engine: impl Into<String>) -> Self {
        self.engine = Some(engine.into());
        self
    }

    /// Get column by name.
    pub fn column(&self, name: &str) -> Option<&MySqlColumn> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Get column index by name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Check if a column is part of the primary key.
    pub fn is_primary_key(&self, name: &str) -> bool {
        self.primary_key.iter().any(|pk| pk == name)
    }
}

impl MySqlColumn {
    /// Create a column.
    pub fn new(
        name: impl Into<String>,
        column_type: impl Into<String>,
        data_type: impl Into<String>,
        nullable: bool,
        ordinal: u32,
    ) -> Self {
        Self {
            name: name.into(),
            column_type: column_type.into(),
            data_type: data_type.into(),
            nullable,
            ordinal_position: ordinal,
            default_value: None,
            extra: None,
            comment: None,
            char_max_length: None,
            numeric_precision: None,
            numeric_scale: None,
        }
    }

    /// Builder: set default value.
    pub fn with_default(mut self, default: impl Into<String>) -> Self {
        self.default_value = Some(default.into());
        self
    }

    /// Builder: set extra attributes.
    pub fn with_extra(mut self, extra: impl Into<String>) -> Self {
        self.extra = Some(extra.into());
        self
    }

    /// Check if column is auto-increment.
    pub fn is_auto_increment(&self) -> bool {
        self.extra
            .as_ref()
            .map(|e| e.contains("auto_increment"))
            .unwrap_or(false)
    }

    /// Check if column is unsigned.
    pub fn is_unsigned(&self) -> bool {
        self.column_type.contains("unsigned")
    }
}

impl SourceSchema for MySqlTableSchema {
    fn source_kind(&self) -> &'static str {
        "mysql"
    }

    fn fingerprint(&self) -> String {
        // Only columns and PK affect fingerprint (not engine/charset)
        #[derive(Serialize)]
        struct FingerprintData<'a> {
            columns: &'a [MySqlColumn],
            primary_key: &'a [String],
        }
        compute_fingerprint(&FingerprintData {
            columns: &self.columns,
            primary_key: &self.primary_key,
        })
    }

    fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    fn primary_key(&self) -> Vec<&str> {
        self.primary_key.iter().map(|s| s.as_str()).collect()
    }

    fn describe(&self) -> String {
        let engine = self.engine.as_deref().unwrap_or("unknown");
        format!(
            "mysql({} cols, engine={}, pk=[{}])",
            self.columns.len(),
            engine,
            self.primary_key.join(",")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema() -> MySqlTableSchema {
        MySqlTableSchema::new(vec![
            MySqlColumn::new("id", "bigint(20) unsigned", "bigint", false, 1)
                .with_extra("auto_increment"),
            MySqlColumn::new("name", "varchar(255)", "varchar", true, 2),
            MySqlColumn::new("created_at", "datetime", "datetime", false, 3),
        ])
        .with_primary_key(vec!["id".into()])
        .with_engine("InnoDB")
    }

    #[test]
    fn test_source_schema() {
        let s = test_schema();
        assert_eq!(s.source_kind(), "mysql");
        assert_eq!(s.column_names(), vec!["id", "name", "created_at"]);
        assert_eq!(s.primary_key(), vec!["id"]);
    }

    #[test]
    fn test_fingerprint_stable() {
        let s = test_schema();
        assert_eq!(s.fingerprint(), s.fingerprint());
    }

    #[test]
    fn test_fingerprint_ignores_engine() {
        let s1 = test_schema();
        let mut s2 = test_schema();
        s2.engine = Some("MyISAM".into());

        // Engine doesn't affect fingerprint
        assert_eq!(s1.fingerprint(), s2.fingerprint());
    }

    #[test]
    fn test_fingerprint_changes_with_columns() {
        let s1 = test_schema();
        let mut s2 = test_schema();
        s2.columns.push(MySqlColumn::new(
            "email",
            "varchar(255)",
            "varchar",
            true,
            4,
        ));

        assert_ne!(s1.fingerprint(), s2.fingerprint());
    }

    #[test]
    fn test_column_helpers() {
        let s = test_schema();

        assert!(s.column("id").unwrap().is_auto_increment());
        assert!(s.column("id").unwrap().is_unsigned());
        assert!(!s.column("name").unwrap().is_auto_increment());

        assert!(s.is_primary_key("id"));
        assert!(!s.is_primary_key("name"));

        assert_eq!(s.column_index("name"), Some(1));
        assert_eq!(s.column_index("nonexistent"), None);
    }

    #[test]
    fn test_serde() {
        let s = test_schema();
        let json = serde_json::to_string(&s).unwrap();
        let parsed: MySqlTableSchema = serde_json::from_str(&json).unwrap();
        assert_eq!(s, parsed);
    }
}

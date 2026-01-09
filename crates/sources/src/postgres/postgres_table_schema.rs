//! PostgreSQL table schema with full type information.
//!
//! Following DeltaForge's source-owned schema philosophy, this captures
//! PostgreSQL-specific type semantics including:
//! - Array types
//! - Custom enum types
//! - Numeric precision/scale
//! - Character varying lengths
//! - PostgreSQL-specific attributes (identity, generated columns)

use schema_registry::{SourceSchema, compute_fingerprint};
use serde::{Deserialize, Serialize};

/// PostgreSQL table schema with full type information.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PostgresTableSchema {
    /// Columns in ordinal order.
    pub columns: Vec<PostgresColumn>,

    /// Primary key column names.
    #[serde(default)]
    pub primary_key: Vec<String>,

    /// Replica identity setting (default, full, index, nothing).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica_identity: Option<String>,

    /// Table OID (useful for pgoutput relation messages).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oid: Option<u32>,

    /// Schema namespace (e.g., "public").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_name: Option<String>,
}

/// PostgreSQL column definition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PostgresColumn {
    /// Column name.
    pub name: String,

    /// Full data type (e.g., "character varying(255)", "integer[]").
    pub data_type: String,

    /// PostgreSQL type OID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub type_oid: Option<u32>,

    /// Whether NULL is allowed.
    pub nullable: bool,

    /// Ordinal position (1-indexed).
    pub ordinal_position: i32,

    /// Default value expression.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_value: Option<String>,

    /// Whether this is an array type.
    #[serde(default)]
    pub is_array: bool,

    /// Element type for arrays.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub element_type: Option<String>,

    /// Character maximum length (for varchar, char, text).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub char_max_length: Option<i32>,

    /// Numeric precision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub numeric_precision: Option<i32>,

    /// Numeric scale.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub numeric_scale: Option<i32>,

    /// Whether column is part of replica identity.
    #[serde(default)]
    pub is_identity: bool,

    /// Identity generation type (ALWAYS, BY DEFAULT).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_generation: Option<String>,

    /// Column is generated (STORED).
    #[serde(default)]
    pub is_generated: bool,

    /// UDT (user-defined type) name for custom types.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub udt_name: Option<String>,
}

impl PostgresTableSchema {
    /// Create new schema with columns.
    pub fn new(columns: Vec<PostgresColumn>) -> Self {
        Self {
            columns,
            primary_key: vec![],
            replica_identity: None,
            oid: None,
            schema_name: None,
        }
    }

    #[must_use]
    pub fn with_primary_key(mut self, pk: Vec<String>) -> Self {
        self.primary_key = pk;
        self
    }

    #[must_use]
    pub fn with_replica_identity(
        mut self,
        identity: impl Into<String>,
    ) -> Self {
        self.replica_identity = Some(identity.into());
        self
    }

    #[must_use]
    pub fn with_schema_name(mut self, schema: impl Into<String>) -> Self {
        self.schema_name = Some(schema.into());
        self
    }

    #[must_use]
    pub fn with_oid(mut self, oid: u32) -> Self {
        self.oid = Some(oid);
        self
    }

    /// Get column by name.
    pub fn column(&self, name: &str) -> Option<&PostgresColumn> {
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

impl PostgresColumn {
    /// Create a column.
    pub fn new(
        name: impl Into<String>,
        data_type: impl Into<String>,
        nullable: bool,
        ordinal: i32,
    ) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
            type_oid: None,
            nullable,
            ordinal_position: ordinal,
            default_value: None,
            is_array: false,
            element_type: None,
            char_max_length: None,
            numeric_precision: None,
            numeric_scale: None,
            is_identity: false,
            identity_generation: None,
            is_generated: false,
            udt_name: None,
        }
    }

    #[must_use]
    pub fn with_type_oid(mut self, oid: u32) -> Self {
        self.type_oid = Some(oid);
        self
    }

    #[must_use]
    pub fn with_default(mut self, default: impl Into<String>) -> Self {
        self.default_value = Some(default.into());
        self
    }

    #[must_use]
    pub fn as_array(mut self, element_type: impl Into<String>) -> Self {
        self.is_array = true;
        self.element_type = Some(element_type.into());
        self
    }

    #[must_use]
    pub fn with_char_max_length(mut self, len: i32) -> Self {
        self.char_max_length = Some(len);
        self
    }

    #[must_use]
    pub fn with_numeric(mut self, precision: i32, scale: i32) -> Self {
        self.numeric_precision = Some(precision);
        self.numeric_scale = Some(scale);
        self
    }

    #[must_use]
    pub fn with_identity(mut self, generation: impl Into<String>) -> Self {
        self.is_identity = true;
        self.identity_generation = Some(generation.into());
        self
    }

    /// Check if this is a serial/bigserial type.
    pub fn is_serial(&self) -> bool {
        matches!(
            self.data_type.to_lowercase().as_str(),
            "serial" | "bigserial" | "smallserial"
        ) || (self.is_identity
            && self.identity_generation.as_deref() == Some("BY DEFAULT"))
    }

    /// Check if this is a numeric type with precision.
    pub fn is_exact_numeric(&self) -> bool {
        matches!(
            self.data_type.to_lowercase().as_str(),
            "numeric" | "decimal"
        )
    }

    /// Get base type name (without array brackets or length modifiers).
    pub fn base_type(&self) -> &str {
        let t = self.data_type.strip_suffix("[]").unwrap_or(&self.data_type);
        t.find('(').map(|idx| &t[..idx]).unwrap_or(t)
    }
}

impl SourceSchema for PostgresTableSchema {
    fn source_kind(&self) -> &'static str {
        "postgres"
    }

    fn fingerprint(&self) -> String {
        #[derive(Serialize)]
        struct FingerprintData<'a> {
            columns: &'a [PostgresColumn],
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
        format!(
            "postgres({} cols, replica_identity={}, pk=[{}])",
            self.columns.len(),
            self.replica_identity.as_deref().unwrap_or("default"),
            self.primary_key.join(",")
        )
    }
}

// ============================================================================
// Well-known PostgreSQL type OIDs
// ============================================================================

/// Common PostgreSQL type OIDs for reference.
pub mod type_oids {
    pub const BOOL: u32 = 16;
    pub const BYTEA: u32 = 17;
    #[allow(dead_code)]
    pub const CHAR: u32 = 18;
    pub const INT8: u32 = 20;
    pub const INT2: u32 = 21;
    pub const INT4: u32 = 23;
    #[allow(dead_code)]
    pub const TEXT: u32 = 25;
    pub const OID: u32 = 26;
    pub const JSON: u32 = 114;
    pub const FLOAT4: u32 = 700;
    pub const FLOAT8: u32 = 701;
    #[allow(dead_code)]
    pub const VARCHAR: u32 = 1043;
    pub const DATE: u32 = 1082;
    pub const TIME: u32 = 1083;
    pub const TIMESTAMP: u32 = 1114;
    pub const TIMESTAMPTZ: u32 = 1184;
    #[allow(dead_code)]
    pub const INTERVAL: u32 = 1186;
    pub const NUMERIC: u32 = 1700;
    pub const UUID: u32 = 2950;
    pub const JSONB: u32 = 3802;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema() -> PostgresTableSchema {
        PostgresTableSchema::new(vec![
            PostgresColumn::new("id", "bigint", false, 1)
                .with_type_oid(type_oids::INT8)
                .with_identity("ALWAYS"),
            PostgresColumn::new("name", "character varying(255)", true, 2)
                .with_type_oid(type_oids::VARCHAR)
                .with_char_max_length(255),
            PostgresColumn::new("tags", "text[]", true, 3).as_array("text"),
            PostgresColumn::new(
                "created_at",
                "timestamp with time zone",
                false,
                4,
            )
            .with_type_oid(type_oids::TIMESTAMPTZ),
        ])
        .with_primary_key(vec!["id".into()])
        .with_replica_identity("full")
        .with_schema_name("public")
    }

    #[test]
    fn test_source_schema() {
        let s = test_schema();
        assert_eq!(s.source_kind(), "postgres");
        assert_eq!(s.column_names(), vec!["id", "name", "tags", "created_at"]);
        assert_eq!(s.primary_key(), vec!["id"]);
    }

    #[test]
    fn test_fingerprint_stable() {
        let s = test_schema();
        assert_eq!(s.fingerprint(), s.fingerprint());
    }

    #[test]
    fn test_fingerprint_ignores_replica_identity() {
        let s1 = test_schema();
        let mut s2 = test_schema();
        s2.replica_identity = Some("index".into());
        assert_eq!(s1.fingerprint(), s2.fingerprint());
    }

    #[test]
    fn test_fingerprint_changes_with_columns() {
        let s1 = test_schema();
        let mut s2 = test_schema();
        s2.columns
            .push(PostgresColumn::new("email", "text", true, 5));
        assert_ne!(s1.fingerprint(), s2.fingerprint());
    }

    #[test]
    fn test_column_helpers() {
        let s = test_schema();

        assert!(s.column("id").unwrap().is_identity);
        assert!(s.column("tags").unwrap().is_array);
        assert_eq!(s.column("name").unwrap().char_max_length, Some(255));

        assert!(s.is_primary_key("id"));
        assert!(!s.is_primary_key("name"));

        assert_eq!(s.column_index("name"), Some(1));
        assert_eq!(s.column_index("nonexistent"), None);
    }

    #[test]
    fn test_base_type() {
        assert_eq!(
            PostgresColumn::new("v", "character varying(100)", true, 1)
                .base_type(),
            "character varying"
        );
        assert_eq!(
            PostgresColumn::new("a", "integer[]", true, 2).base_type(),
            "integer"
        );
        assert_eq!(
            PostgresColumn::new("n", "numeric(10,2)", true, 3).base_type(),
            "numeric"
        );
    }

    #[test]
    fn test_serde() {
        let s = test_schema();
        let json = serde_json::to_string(&s).unwrap();
        let parsed: PostgresTableSchema = serde_json::from_str(&json).unwrap();
        assert_eq!(s, parsed);
    }
}

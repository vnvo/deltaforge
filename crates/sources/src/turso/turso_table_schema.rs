//! Turso/SQLite table schema representation.
//!
//! Captures SQLite-native schema semantics including:
//! - SQLite affinity types (INTEGER, TEXT, REAL, BLOB, NUMERIC)
//! - Primary key columns (including WITHOUT ROWID tables)
//! - Column constraints and defaults
//!
//! # SQLite Type Affinity
//!
//! SQLite uses type affinity rather than strict types. This module preserves
//! that semantic rather than forcing normalization to a universal type system.
//!
//! See: <https://www.sqlite.org/datatype3.html>

use schema_registry::SourceSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// ============================================================================
// Schema Types
// ============================================================================

/// SQLite/Turso table schema representation.
///
/// Preserves SQLite's native type affinity system rather than
/// forcing normalization to a universal type system.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TursoTableSchema {
    /// Columns in ordinal order
    pub columns: Vec<TursoColumn>,

    /// Primary key column names
    pub primary_key: Vec<String>,

    /// Whether this is a WITHOUT ROWID table
    #[serde(default)]
    pub without_rowid: bool,

    /// Whether this table has any triggers installed for CDC
    #[serde(default)]
    pub has_cdc_triggers: bool,

    /// SQL statement used to create the table (from sqlite_master)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create_sql: Option<String>,
}

/// SQLite column definition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TursoColumn {
    /// Column name
    pub name: String,

    /// Declared type (e.g., "INTEGER", "VARCHAR(255)", "TEXT")
    pub declared_type: String,

    /// SQLite type affinity (INTEGER, TEXT, REAL, BLOB, NUMERIC)
    pub affinity: SqliteAffinity,

    /// Whether the column can contain NULL
    pub nullable: bool,

    /// Column index (0-based)
    pub column_index: usize,

    /// Default value expression
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_value: Option<String>,

    /// Whether this column is part of the primary key
    #[serde(default)]
    pub is_primary_key: bool,

    /// Whether this column auto-increments
    #[serde(default)]
    pub is_autoincrement: bool,
}

/// SQLite type affinity.
///
/// SQLite uses a dynamic type system with type affinity. The affinity
/// of a column is the recommended type for data stored in that column,
/// but any column can store any type of data.
///
/// See: <https://www.sqlite.org/datatype3.html#type_affinity>
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "UPPERCASE")]
pub enum SqliteAffinity {
    /// Prefers INTEGER storage (INT, INTEGER, TINYINT, etc.)
    Integer,
    /// Prefers TEXT storage (TEXT, VARCHAR, CHAR, CLOB, etc.)
    Text,
    /// Prefers REAL storage (REAL, DOUBLE, FLOAT, etc.)
    Real,
    /// No type preference (BLOB, or no type specified)
    Blob,
    /// Prefers NUMERIC storage (NUMERIC, DECIMAL, BOOLEAN, DATE, etc.)
    Numeric,
}

// ============================================================================
// TursoColumn Implementation
// ============================================================================

impl TursoColumn {
    /// Create a new column definition.
    ///
    /// The affinity is automatically determined from the declared type
    /// according to SQLite's affinity rules.
    pub fn new(
        name: impl Into<String>,
        declared_type: impl Into<String>,
        nullable: bool,
        column_index: usize,
    ) -> Self {
        let declared = declared_type.into();
        let affinity = Self::determine_affinity(&declared);

        Self {
            name: name.into(),
            declared_type: declared,
            affinity,
            nullable,
            column_index,
            default_value: None,
            is_primary_key: false,
            is_autoincrement: false,
        }
    }

    /// Set this column as a primary key.
    pub fn with_primary_key(mut self) -> Self {
        self.is_primary_key = true;
        self
    }

    /// Set this column as auto-increment.
    pub fn with_autoincrement(mut self) -> Self {
        self.is_autoincrement = true;
        self
    }

    /// Set the default value.
    pub fn with_default(mut self, default: impl Into<String>) -> Self {
        self.default_value = Some(default.into());
        self
    }

    /// Determine SQLite type affinity from declared type.
    ///
    /// Follows SQLite's affinity determination rules:
    /// 1. If the declared type contains "INT" → INTEGER affinity
    /// 2. If it contains "CHAR", "CLOB", or "TEXT" → TEXT affinity
    /// 3. If it contains "BLOB" or is empty → BLOB affinity
    /// 4. If it contains "REAL", "FLOA", or "DOUB" → REAL affinity
    /// 5. Otherwise → NUMERIC affinity
    pub fn determine_affinity(declared_type: &str) -> SqliteAffinity {
        let upper = declared_type.to_uppercase();

        // Rule 1: INT in name -> INTEGER affinity
        if upper.contains("INT") {
            return SqliteAffinity::Integer;
        }

        // Rule 2: CHAR, CLOB, or TEXT -> TEXT affinity
        if upper.contains("CHAR")
            || upper.contains("CLOB")
            || upper.contains("TEXT")
        {
            return SqliteAffinity::Text;
        }

        // Rule 3: BLOB or no type -> BLOB affinity (none)
        if upper.contains("BLOB") || upper.is_empty() {
            return SqliteAffinity::Blob;
        }

        // Rule 4: REAL, FLOA, or DOUB -> REAL affinity
        if upper.contains("REAL")
            || upper.contains("FLOA")
            || upper.contains("DOUB")
        {
            return SqliteAffinity::Real;
        }

        // Rule 5: Everything else -> NUMERIC affinity
        SqliteAffinity::Numeric
    }

    /// Check if this column can be used for rowid aliasing.
    ///
    /// In SQLite, an INTEGER PRIMARY KEY column becomes an alias for the rowid.
    pub fn is_rowid_alias(&self) -> bool {
        self.is_primary_key && self.affinity == SqliteAffinity::Integer
    }
}

// ============================================================================
// TursoTableSchema Implementation
// ============================================================================

impl TursoTableSchema {
    /// Create a new schema with the given columns.
    ///
    /// Primary key is automatically extracted from columns marked as primary key.
    pub fn new(columns: Vec<TursoColumn>) -> Self {
        let primary_key: Vec<String> = columns
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| c.name.clone())
            .collect();

        Self {
            columns,
            primary_key,
            without_rowid: false,
            has_cdc_triggers: false,
            create_sql: None,
        }
    }

    /// Set the primary key explicitly.
    pub fn with_primary_key(mut self, pk: Vec<String>) -> Self {
        self.primary_key = pk;
        self
    }

    /// Mark as WITHOUT ROWID table.
    pub fn with_without_rowid(mut self) -> Self {
        self.without_rowid = true;
        self
    }

    /// Set the CREATE SQL statement.
    pub fn with_create_sql(mut self, sql: impl Into<String>) -> Self {
        self.create_sql = Some(sql.into());
        self
    }

    /// Get column by name.
    pub fn column(&self, name: &str) -> Option<&TursoColumn> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Get column by index.
    pub fn column_at(&self, index: usize) -> Option<&TursoColumn> {
        self.columns.get(index)
    }

    /// Get column index by name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Check if this table has a rowid (most SQLite tables do).
    ///
    /// Tables created with WITHOUT ROWID do not have an implicit rowid.
    pub fn has_rowid(&self) -> bool {
        !self.without_rowid
    }

    /// Check if a column is part of the primary key.
    pub fn is_primary_key(&self, name: &str) -> bool {
        self.primary_key.iter().any(|pk| pk == name)
    }

    /// Get the number of columns.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}

// ============================================================================
// SourceSchema Implementation
// ============================================================================

impl SourceSchema for TursoTableSchema {
    fn source_kind(&self) -> &'static str {
        "turso"
    }

    fn fingerprint(&self) -> String {
        // Create a stable fingerprint from structurally significant fields
        // Note: create_sql and has_cdc_triggers are excluded as they don't
        // affect the logical schema
        #[derive(Serialize)]
        struct FingerprintData<'a> {
            columns: &'a [TursoColumn],
            primary_key: &'a [String],
            without_rowid: bool,
        }

        let data = FingerprintData {
            columns: &self.columns,
            primary_key: &self.primary_key,
            without_rowid: self.without_rowid,
        };

        let json = serde_json::to_vec(&data).unwrap_or_default();
        let hash = Sha256::digest(&json);
        format!("sha256:{}", hex::encode(hash))
    }

    fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    fn primary_key(&self) -> Vec<&str> {
        self.primary_key.iter().map(|s| s.as_str()).collect()
    }

    fn describe(&self) -> String {
        let cols: Vec<String> = self
            .columns
            .iter()
            .map(|c| {
                let pk = if c.is_primary_key { " PK" } else { "" };
                let null = if c.nullable { "" } else { " NOT NULL" };
                let auto = if c.is_autoincrement { " AUTO" } else { "" };
                format!("{} {:?}{}{}{}", c.name, c.affinity, pk, null, auto)
            })
            .collect();

        let rowid = if self.without_rowid {
            " WITHOUT ROWID"
        } else {
            ""
        };

        format!(
            "TursoTable({} columns{}: [{}])",
            self.columns.len(),
            rowid,
            cols.join(", ")
        )
    }
}

// ============================================================================
// Display Implementation
// ============================================================================

impl std::fmt::Display for SqliteAffinity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqliteAffinity::Integer => write!(f, "INTEGER"),
            SqliteAffinity::Text => write!(f, "TEXT"),
            SqliteAffinity::Real => write!(f, "REAL"),
            SqliteAffinity::Blob => write!(f, "BLOB"),
            SqliteAffinity::Numeric => write!(f, "NUMERIC"),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_affinity_determination() {
        // INTEGER affinity
        assert_eq!(
            TursoColumn::determine_affinity("INTEGER"),
            SqliteAffinity::Integer
        );
        assert_eq!(
            TursoColumn::determine_affinity("INT"),
            SqliteAffinity::Integer
        );
        assert_eq!(
            TursoColumn::determine_affinity("BIGINT"),
            SqliteAffinity::Integer
        );
        assert_eq!(
            TursoColumn::determine_affinity("TINYINT"),
            SqliteAffinity::Integer
        );
        assert_eq!(
            TursoColumn::determine_affinity("SMALLINT"),
            SqliteAffinity::Integer
        );
        assert_eq!(
            TursoColumn::determine_affinity("MEDIUMINT"),
            SqliteAffinity::Integer
        );
        assert_eq!(
            TursoColumn::determine_affinity("INT8"),
            SqliteAffinity::Integer
        );

        // TEXT affinity
        assert_eq!(
            TursoColumn::determine_affinity("TEXT"),
            SqliteAffinity::Text
        );
        assert_eq!(
            TursoColumn::determine_affinity("VARCHAR(255)"),
            SqliteAffinity::Text
        );
        assert_eq!(
            TursoColumn::determine_affinity("CHAR(10)"),
            SqliteAffinity::Text
        );
        assert_eq!(
            TursoColumn::determine_affinity("CLOB"),
            SqliteAffinity::Text
        );
        assert_eq!(
            TursoColumn::determine_affinity("NVARCHAR(100)"),
            SqliteAffinity::Text
        );

        // REAL affinity
        assert_eq!(
            TursoColumn::determine_affinity("REAL"),
            SqliteAffinity::Real
        );
        assert_eq!(
            TursoColumn::determine_affinity("DOUBLE"),
            SqliteAffinity::Real
        );
        assert_eq!(
            TursoColumn::determine_affinity("DOUBLE PRECISION"),
            SqliteAffinity::Real
        );
        assert_eq!(
            TursoColumn::determine_affinity("FLOAT"),
            SqliteAffinity::Real
        );

        // BLOB affinity
        assert_eq!(
            TursoColumn::determine_affinity("BLOB"),
            SqliteAffinity::Blob
        );
        assert_eq!(TursoColumn::determine_affinity(""), SqliteAffinity::Blob);

        // NUMERIC affinity (default)
        assert_eq!(
            TursoColumn::determine_affinity("NUMERIC"),
            SqliteAffinity::Numeric
        );
        assert_eq!(
            TursoColumn::determine_affinity("DECIMAL(10,2)"),
            SqliteAffinity::Numeric
        );
        assert_eq!(
            TursoColumn::determine_affinity("BOOLEAN"),
            SqliteAffinity::Numeric
        );
        assert_eq!(
            TursoColumn::determine_affinity("DATE"),
            SqliteAffinity::Numeric
        );
        assert_eq!(
            TursoColumn::determine_affinity("DATETIME"),
            SqliteAffinity::Numeric
        );
    }

    #[test]
    fn test_fingerprint_stability() {
        let col1 = TursoColumn::new("id", "INTEGER", false, 0);
        let col2 = TursoColumn::new("name", "TEXT", true, 1);

        let schema1 = TursoTableSchema::new(vec![col1.clone(), col2.clone()]);
        let schema2 = TursoTableSchema::new(vec![col1, col2]);

        assert_eq!(
            schema1.fingerprint(),
            schema2.fingerprint(),
            "Same schema should produce same fingerprint"
        );
    }

    #[test]
    fn test_fingerprint_changes_with_columns() {
        let col1 = TursoColumn::new("id", "INTEGER", false, 0);
        let col2 = TursoColumn::new("name", "TEXT", true, 1);
        let col3 = TursoColumn::new("email", "TEXT", true, 2);

        let schema1 = TursoTableSchema::new(vec![col1.clone(), col2.clone()]);
        let schema2 = TursoTableSchema::new(vec![col1, col2, col3]);

        assert_ne!(
            schema1.fingerprint(),
            schema2.fingerprint(),
            "Different columns should produce different fingerprint"
        );
    }

    #[test]
    fn test_fingerprint_ignores_create_sql() {
        let col = TursoColumn::new("id", "INTEGER", false, 0);

        let schema1 = TursoTableSchema::new(vec![col.clone()]);
        let schema2 = TursoTableSchema::new(vec![col])
            .with_create_sql("CREATE TABLE t (id INT)");

        assert_eq!(
            schema1.fingerprint(),
            schema2.fingerprint(),
            "create_sql should not affect fingerprint"
        );
    }

    #[test]
    fn test_source_schema_trait() {
        let col1 =
            TursoColumn::new("id", "INTEGER", false, 0).with_primary_key();
        let col2 = TursoColumn::new("name", "TEXT", true, 1);

        let schema = TursoTableSchema::new(vec![col1, col2])
            .with_primary_key(vec!["id".into()]);

        assert_eq!(schema.source_kind(), "turso");
        assert_eq!(schema.column_names(), vec!["id", "name"]);
        assert_eq!(schema.primary_key(), vec!["id"]);
        assert!(schema.fingerprint().starts_with("sha256:"));
    }

    #[test]
    fn test_column_builders() {
        let col = TursoColumn::new("id", "INTEGER", false, 0)
            .with_primary_key()
            .with_autoincrement()
            .with_default("0");

        assert!(col.is_primary_key);
        assert!(col.is_autoincrement);
        assert_eq!(col.default_value, Some("0".to_string()));
    }

    #[test]
    fn test_rowid_alias() {
        let int_pk =
            TursoColumn::new("id", "INTEGER", false, 0).with_primary_key();
        let text_pk =
            TursoColumn::new("uuid", "TEXT", false, 0).with_primary_key();
        let int_non_pk = TursoColumn::new("count", "INTEGER", false, 1);

        assert!(int_pk.is_rowid_alias());
        assert!(!text_pk.is_rowid_alias());
        assert!(!int_non_pk.is_rowid_alias());
    }

    #[test]
    fn test_schema_helpers() {
        let col1 =
            TursoColumn::new("id", "INTEGER", false, 0).with_primary_key();
        let col2 = TursoColumn::new("name", "TEXT", true, 1);

        let schema = TursoTableSchema::new(vec![col1, col2])
            .with_primary_key(vec!["id".into()]);

        assert_eq!(schema.column_count(), 2);
        assert!(schema.column("id").is_some());
        assert!(schema.column("nonexistent").is_none());
        assert_eq!(schema.column_index("name"), Some(1));
        assert!(schema.is_primary_key("id"));
        assert!(!schema.is_primary_key("name"));
        assert!(schema.has_rowid());
    }

    #[test]
    fn test_without_rowid() {
        let col = TursoColumn::new("uuid", "TEXT", false, 0).with_primary_key();
        let schema = TursoTableSchema::new(vec![col]).with_without_rowid();

        assert!(!schema.has_rowid());
        assert!(schema.describe().contains("WITHOUT ROWID"));
    }

    #[test]
    fn test_serde_roundtrip() {
        let col = TursoColumn::new("id", "INTEGER", false, 0)
            .with_primary_key()
            .with_autoincrement();

        let schema = TursoTableSchema::new(vec![col])
            .with_primary_key(vec!["id".into()])
            .with_create_sql(
                "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT)",
            );

        let json = serde_json::to_string(&schema).unwrap();
        let parsed: TursoTableSchema = serde_json::from_str(&json).unwrap();

        assert_eq!(schema, parsed);
    }

    #[test]
    fn test_affinity_display() {
        assert_eq!(format!("{}", SqliteAffinity::Integer), "INTEGER");
        assert_eq!(format!("{}", SqliteAffinity::Text), "TEXT");
        assert_eq!(format!("{}", SqliteAffinity::Real), "REAL");
        assert_eq!(format!("{}", SqliteAffinity::Blob), "BLOB");
        assert_eq!(format!("{}", SqliteAffinity::Numeric), "NUMERIC");
    }
}

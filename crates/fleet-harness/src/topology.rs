//! Fixture naming, table templates and generated SQL.
//!
//! A server holds `databases` customer databases named
//! `<prefix><5-digit index>`, each with `tables` tables `t<3-digit index>`.
//! Table `t<k>` has the same definition in every database (template `k`,
//! derived from the seed), as customer schemas are copies of one product
//! schema. Every table has the columns the verifier relies on: `id` (primary
//! key), `version` (the driver's operation sequence) and `committed_at`.

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

/// A table in the fleet (server, database and table indexes).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
)]
pub struct TableRef {
    pub server: u16,
    pub db: u32,
    pub table: u16,
}

/// One row's logical identity: never derived from a Kafka message key.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
)]
pub struct RowKey {
    pub table: TableRef,
    pub id: u64,
}

impl RowKey {
    pub const BYTES: usize = 16;

    pub fn to_bytes(self) -> [u8; Self::BYTES] {
        let mut b = [0u8; Self::BYTES];
        b[0..2].copy_from_slice(&self.table.server.to_be_bytes());
        b[2..6].copy_from_slice(&self.table.db.to_be_bytes());
        b[6..8].copy_from_slice(&self.table.table.to_be_bytes());
        b[8..16].copy_from_slice(&self.id.to_be_bytes());
        b
    }

    pub fn from_bytes(b: &[u8]) -> Self {
        RowKey {
            table: TableRef {
                server: u16::from_be_bytes([b[0], b[1]]),
                db: u32::from_be_bytes([b[2], b[3], b[4], b[5]]),
                table: u16::from_be_bytes([b[6], b[7]]),
            },
            id: u64::from_be_bytes(b[8..16].try_into().expect("8 bytes")),
        }
    }
}

/// Names for one topology.
#[derive(Debug, Clone)]
pub struct Naming {
    pub database_prefix: String,
}

impl Naming {
    pub fn database(&self, db: u32) -> String {
        format!("{}{db:05}", self.database_prefix)
    }

    pub fn table(&self, table: u16) -> String {
        format!("t{table:03}")
    }

    /// Parse a database name back to its index (`None` if not a customer
    /// database of this topology).
    pub fn parse_database(&self, name: &str) -> Option<u32> {
        name.strip_prefix(&self.database_prefix)?.parse().ok()
    }

    pub fn parse_table(&self, name: &str) -> Option<u16> {
        name.strip_prefix('t')?.parse().ok()
    }

    /// The DeltaForge table pattern covering every customer database.
    pub fn table_pattern(&self) -> String {
        format!("{}*.*", self.database_prefix)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ColumnKind {
    Int,
    BigInt,
    Decimal,
    Double,
    Varchar,
    Text,
    Json,
    Datetime,
}

impl ColumnKind {
    const ALL: [ColumnKind; 8] = [
        ColumnKind::Int,
        ColumnKind::BigInt,
        ColumnKind::Decimal,
        ColumnKind::Double,
        ColumnKind::Varchar,
        ColumnKind::Text,
        ColumnKind::Json,
        ColumnKind::Datetime,
    ];

    fn sql(self) -> &'static str {
        match self {
            ColumnKind::Int => "INT NULL",
            ColumnKind::BigInt => "BIGINT NULL",
            ColumnKind::Decimal => "DECIMAL(14,2) NULL",
            ColumnKind::Double => "DOUBLE NULL",
            ColumnKind::Varchar => "VARCHAR(64) NULL",
            ColumnKind::Text => "TEXT NULL",
            ColumnKind::Json => "JSON NULL",
            ColumnKind::Datetime => "DATETIME(6) NULL",
        }
    }

    /// A literal for a written row (deterministic in `n`).
    pub fn literal(self, n: u64) -> String {
        match self {
            ColumnKind::Int => format!("{}", n % 1_000_000),
            ColumnKind::BigInt => format!("{n}"),
            ColumnKind::Decimal => format!("{}.{:02}", n % 1_000_000, n % 100),
            ColumnKind::Double => format!("{}.5", n % 1_000_000),
            ColumnKind::Varchar => format!("'v{n}'"),
            ColumnKind::Text => format!("'text {n}'"),
            ColumnKind::Json => format!("'{{\"n\":{n}}}'"),
            ColumnKind::Datetime => "'2026-01-01 00:00:00.000000'".into(),
        }
    }
}

/// Table template `k`: its extra columns and secondary indexes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Template {
    pub index: u16,
    pub columns: Vec<(String, ColumnKind)>,
    pub indexed: Vec<String>,
}

impl Template {
    /// Deterministic in `(seed, index)`: 4 to 16 extra columns of mixed
    /// kinds, and up to 2 secondary indexes on indexable ones.
    pub fn generate(seed: u64, index: u16) -> Template {
        let mut rng =
            StdRng::seed_from_u64(seed ^ (u64::from(index) << 32) ^ 0x9e37);
        let n = rng.random_range(4..=16);
        let columns: Vec<(String, ColumnKind)> = (0..n)
            .map(|i| {
                let kind =
                    ColumnKind::ALL[rng.random_range(0..ColumnKind::ALL.len())];
                (format!("c{i:02}"), kind)
            })
            .collect();
        let indexable: Vec<&String> = columns
            .iter()
            .filter(|(_, k)| !matches!(k, ColumnKind::Text | ColumnKind::Json))
            .map(|(c, _)| c)
            .collect();
        let indexed = indexable
            .into_iter()
            .take(rng.random_range(0..=2))
            .cloned()
            .collect();
        Template {
            index,
            columns,
            indexed,
        }
    }

    pub fn create_sql(&self, database: &str, table: &str) -> String {
        let mut cols = vec![
            "`id` BIGINT NOT NULL".to_string(),
            "`version` BIGINT NOT NULL".to_string(),
            "`committed_at` DATETIME(6) NOT NULL".to_string(),
            "`payload` VARCHAR(4096) NULL".to_string(),
        ];
        cols.extend(
            self.columns
                .iter()
                .map(|(c, k)| format!("`{c}` {}", k.sql())),
        );
        cols.push("PRIMARY KEY (`id`)".into());
        cols.extend(
            self.indexed.iter().map(|c| format!("KEY `ix_{c}` (`{c}`)")),
        );
        format!(
            "CREATE TABLE IF NOT EXISTS `{database}`.`{table}` ({}) ENGINE=InnoDB",
            cols.join(", ")
        )
    }
}

/// The column a migration statement adds, and its `ALTER TABLE`.
pub fn migration_column(generation: u32, statement: u32) -> String {
    format!("m{generation:04}_{statement:02}")
}

pub fn migration_sql(database: &str, table: &str, column: &str) -> String {
    format!("ALTER TABLE `{database}`.`{table}` ADD COLUMN `{column}` INT NULL")
}

/// The template a migration statement alters in each database.
pub fn migration_table(statement: u32, tables: u16) -> u16 {
    (statement % u32::from(tables)) as u16
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_keys_round_trip_and_sort_by_identity() {
        let a = RowKey {
            table: TableRef {
                server: 3,
                db: 1_999,
                table: 199,
            },
            id: 42,
        };
        assert_eq!(RowKey::from_bytes(&a.to_bytes()), a);
        let b = RowKey { id: 43, ..a };
        assert!(
            a.to_bytes() < b.to_bytes(),
            "byte order matches identity order"
        );
    }

    #[test]
    fn names_parse_back() {
        let n = Naming {
            database_prefix: "cust_".into(),
        };
        assert_eq!(n.database(7), "cust_00007");
        assert_eq!(n.parse_database("cust_00007"), Some(7));
        assert_eq!(n.parse_database("other"), None);
        assert_eq!(n.parse_table(&n.table(12)), Some(12));
        assert_eq!(n.table_pattern(), "cust_*.*");
    }

    #[test]
    fn templates_are_deterministic_and_varied() {
        let a = Template::generate(1, 5);
        assert_eq!(a, Template::generate(1, 5));
        assert_ne!(a, Template::generate(2, 5));
        let sql = a.create_sql("cust_00001", "t005");
        assert!(
            sql.contains("`id` BIGINT NOT NULL")
                && sql.contains("PRIMARY KEY (`id`)")
        );
        let kinds: std::collections::HashSet<_> = (0..200)
            .flat_map(|i| {
                Template::generate(1, i).columns.into_iter().map(|(_, k)| k)
            })
            .collect();
        assert_eq!(kinds.len(), ColumnKind::ALL.len(), "every kind appears");
    }
}

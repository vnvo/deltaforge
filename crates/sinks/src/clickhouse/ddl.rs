//! Generate `CREATE TABLE` DDL for auto table creation.
//!
//! The inserted row shape is: user columns (in declared order) then the meta
//! columns `_op` / `_version` / `_deleted` / `_source_ts`. The target-table
//! engine determines change-log vs current-state behaviour:
//! - `upsert`    → `ReplacingMergeTree(_version, _deleted) ORDER BY (pk…)`
//! - `changelog` → `MergeTree ORDER BY (pk…, _version)`

use super::types::{ChType, ColDesc};
use deltaforge_config::ChMode;

/// Build a `CREATE TABLE IF NOT EXISTS` statement.
///
/// `cols` is the user columns in declared order (with their mapped ClickHouse
/// type). `pk` is the source primary key (used for `ORDER BY`).
pub fn create_table_ddl(
    db: &str,
    table: &str,
    cols: &[(ColDesc, ChType)],
    pk: &[String],
    mode: ChMode,
) -> String {
    let mut lines: Vec<String> = Vec::with_capacity(cols.len() + 4);
    for (c, ty) in cols {
        let t = if c.nullable {
            format!("Nullable({})", ty.ddl_name())
        } else {
            ty.ddl_name()
        };
        lines.push(format!("  `{}` {}", c.name, t));
    }
    lines.push("  `_op` LowCardinality(String)".into());
    lines.push("  `_version` UInt64".into());
    lines.push("  `_deleted` UInt8".into());
    lines.push("  `_source_ts` DateTime64(3)".into());

    let (engine, order_by) = match mode {
        ChMode::Upsert => (
            "ReplacingMergeTree(_version, _deleted)".to_string(),
            order_by_clause(pk, false),
        ),
        ChMode::Changelog => ("MergeTree".to_string(), order_by_clause(pk, true)),
    };

    format!(
        "CREATE TABLE IF NOT EXISTS `{db}`.`{table}` (\n{}\n) ENGINE = {engine} ORDER BY {order_by}",
        lines.join(",\n"),
    )
}

/// `ORDER BY` tuple. For change-log we append `_version` so repeated keys keep a
/// stable, version-ordered layout. Empty PK → `tuple()`.
fn order_by_clause(pk: &[String], append_version: bool) -> String {
    let mut keys: Vec<String> = pk.iter().map(|k| format!("`{k}`")).collect();
    if append_version {
        keys.push("`_version`".into());
    }
    if keys.is_empty() {
        "tuple()".into()
    } else {
        format!("({})", keys.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols() -> Vec<(ColDesc, ChType)> {
        vec![
            (
                ColDesc {
                    name: "id".into(),
                    data_type: "bigint".into(),
                    full_type: "bigint".into(),
                    nullable: false,
                    unsigned: false,
                    precision: None,
                    scale: None,
                },
                ChType::Int64,
            ),
            (
                ColDesc {
                    name: "email".into(),
                    data_type: "varchar".into(),
                    full_type: "varchar(255)".into(),
                    nullable: true,
                    unsigned: false,
                    precision: None,
                    scale: None,
                },
                ChType::String,
            ),
        ]
    }

    #[test]
    fn upsert_uses_replacingmergetree_and_pk_order() {
        let sql = create_table_ddl("analytics", "orders", &cols(), &["id".into()], ChMode::Upsert);
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS `analytics`.`orders`"));
        assert!(sql.contains("`id` Int64"));
        assert!(sql.contains("`email` Nullable(String)"));
        assert!(sql.contains("`_version` UInt64"));
        assert!(sql.contains("ENGINE = ReplacingMergeTree(_version, _deleted)"));
        assert!(sql.contains("ORDER BY (`id`)"), "got: {sql}");
    }

    #[test]
    fn changelog_uses_mergetree_and_appends_version_to_order() {
        let sql =
            create_table_ddl("d", "t", &cols(), &["id".into()], ChMode::Changelog);
        assert!(sql.contains("ENGINE = MergeTree"));
        assert!(sql.contains("ORDER BY (`id`, `_version`)"), "got: {sql}");
    }

    #[test]
    fn empty_pk_orders_by_tuple() {
        let sql = create_table_ddl("d", "t", &cols(), &[], ChMode::Upsert);
        assert!(sql.contains("ORDER BY tuple()"), "got: {sql}");
    }
}

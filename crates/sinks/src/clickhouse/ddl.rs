//! Generate `CREATE TABLE` DDL for auto table creation.
//!
//! The inserted row shape is: user columns (in declared order) then the meta
//! columns `_op` / `_version` / `_deleted` / `_source_ts`, then the
//! configured extension column (the row's processor-added document
//! extensions as a canonical JSON object, NULL when it has none), then the
//! reserved fence column `_df_fence`, checked by the table's fence
//! constraint (see [`super::ownership`]). The table comment is its
//! immutable ownership marker. The target-table
//! engine determines change-log vs current-state behaviour:
//! - `upsert`    → `ReplacingMergeTree(_version, _deleted) ORDER BY (pk…)`
//! - `changelog` → `MergeTree ORDER BY (pk…, _version)`

use super::types::{ChType, ColDesc};
use deltaforge_config::ChMode;

/// The reserved column every row carries its writer's fence in.
pub const FENCE_COLUMN: &str = "_df_fence";
/// The reserved column's type.
pub const FENCE_COLUMN_TYPE: &str = "LowCardinality(String)";

/// A ClickHouse identifier, always backtick-quoted (`\` and `` ` ``
/// escaped).
pub fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
}

/// A ClickHouse string literal (`\` and `'` escaped).
pub fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// The fence constraint's name for `fence`.
pub fn fence_constraint(fence: &str) -> String {
    format!("df_fence_{fence}")
}

/// The fence constraint's clause.
pub fn fence_constraint_clause(fence: &str) -> String {
    format!(
        "CONSTRAINT {} CHECK {} = {}",
        fence_constraint(fence),
        FENCE_COLUMN,
        quote_literal(fence)
    )
}

/// Build the `CREATE TABLE` statement (no `IF NOT EXISTS`: a table that
/// exists already makes it fail, and the claim reads that table instead).
///
/// `cols` is the user columns in declared order (with their mapped ClickHouse
/// type). `pk` is the source primary key (used for `ORDER BY`). `fence` is
/// the initial fence and `marker` the ownership marker (the table comment).
#[allow(clippy::too_many_arguments)]
pub fn create_table_ddl(
    db: &str,
    table: &str,
    cols: &[(ColDesc, ChType)],
    pk: &[String],
    mode: ChMode,
    extensions_column: &str,
    fence: &str,
    marker: &str,
) -> String {
    let mut lines: Vec<String> = Vec::with_capacity(cols.len() + 4);
    for (c, ty) in cols {
        let t = if c.nullable {
            format!("Nullable({})", ty.ddl_name())
        } else {
            ty.ddl_name()
        };
        lines.push(format!("  {} {}", quote_ident(&c.name), t));
    }
    lines.push("  `_op` LowCardinality(String)".into());
    lines.push("  `_version` UInt64".into());
    lines.push("  `_deleted` UInt8".into());
    lines.push("  `_source_ts` DateTime64(3)".into());
    lines.push(format!(
        "  {} Nullable(String)",
        quote_ident(extensions_column)
    ));
    lines.push(format!("  {FENCE_COLUMN} {FENCE_COLUMN_TYPE}"));
    lines.push(format!("  {}", fence_constraint_clause(fence)));

    let (engine, order_by) = match mode {
        ChMode::Upsert => (
            "ReplacingMergeTree(_version, _deleted)".to_string(),
            order_by_clause(pk, false),
        ),
        ChMode::Changelog => {
            ("MergeTree".to_string(), order_by_clause(pk, true))
        }
    };

    format!(
        "CREATE TABLE {}.{} (\n{}\n) ENGINE = {engine} ORDER BY {order_by} COMMENT {}",
        quote_ident(db),
        quote_ident(table),
        lines.join(",\n"),
        quote_literal(marker),
    )
}

/// `ORDER BY` tuple. For change-log we append `_version` so repeated keys keep a
/// stable, version-ordered layout. Empty PK → `tuple()`.
fn order_by_clause(pk: &[String], append_version: bool) -> String {
    let mut keys: Vec<String> = pk.iter().map(|k| quote_ident(k)).collect();
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

    const F: &str = "0123456789abcdef0123456789abcdef";

    #[test]
    fn upsert_uses_replacingmergetree_and_pk_order() {
        let sql = create_table_ddl(
            "analytics",
            "orders",
            &cols(),
            &["id".into()],
            ChMode::Upsert,
            "_extensions",
            F,
            "{}",
        );
        assert!(sql.starts_with("CREATE TABLE `analytics`.`orders` ("));
        assert!(!sql.contains("IF NOT EXISTS"));
        assert!(sql.contains("`id` Int64"));
        assert!(sql.contains("`email` Nullable(String)"));
        assert!(sql.contains("`_version` UInt64"));
        assert!(
            sql.contains("ENGINE = ReplacingMergeTree(_version, _deleted)")
        );
        assert!(sql.contains("ORDER BY (`id`)"), "got: {sql}");
        // The extension column follows the meta columns; the fence column
        // is last, then its constraint, and the marker is the comment.
        assert!(
            sql.contains(
                "`_source_ts` DateTime64(3),\n  `_extensions` Nullable(String),\n  \
                 _df_fence LowCardinality(String),\n  \
                 CONSTRAINT df_fence_0123456789abcdef0123456789abcdef CHECK \
                 _df_fence = '0123456789abcdef0123456789abcdef'\n)"
            ),
            "got: {sql}"
        );
        assert!(sql.ends_with(" COMMENT '{}'"), "got: {sql}");
    }

    #[test]
    fn names_and_the_marker_are_quoted() {
        let sql = create_table_ddl(
            "my db",
            "we`ird\\t",
            &cols(),
            &["id".into()],
            ChMode::Upsert,
            "_extensions",
            F,
            r#"{"a":"it's"}"#,
        );
        assert!(
            sql.starts_with(r"CREATE TABLE `my db`.`we\`ird\\t` ("),
            "{sql}"
        );
        assert!(sql.ends_with(r#" COMMENT '{"a":"it\'s"}'"#), "{sql}");
    }

    #[test]
    fn extension_column_name_is_configurable() {
        let sql = create_table_ddl(
            "d",
            "t",
            &cols(),
            &["id".into()],
            ChMode::Upsert,
            "proc_ext",
            F,
            "{}",
        );
        assert!(sql.contains("`proc_ext` Nullable(String)"), "got: {sql}");
        assert!(!sql.contains("_extensions"), "got: {sql}");
    }

    #[test]
    fn changelog_uses_mergetree_and_appends_version_to_order() {
        let sql = create_table_ddl(
            "d",
            "t",
            &cols(),
            &["id".into()],
            ChMode::Changelog,
            "_extensions",
            F,
            "{}",
        );
        assert!(sql.contains("ENGINE = MergeTree"));
        assert!(sql.contains("ORDER BY (`id`, `_version`)"), "got: {sql}");
    }

    #[test]
    fn empty_pk_orders_by_tuple() {
        let sql = create_table_ddl(
            "d",
            "t",
            &cols(),
            &[],
            ChMode::Upsert,
            "_extensions",
            F,
            "{}",
        );
        assert!(sql.contains("ORDER BY tuple()"), "got: {sql}");
    }
}

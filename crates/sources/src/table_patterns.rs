//! Table-pattern expansion that agrees with CDC filtering.
//!
//! A snapshot must copy exactly the tables whose changes CDC captures. CDC
//! filters rows with [`AllowList`]: a pattern part is `*` / `%` (anything), a
//! prefix ending in `*` or `%`, or an exact name; a pattern without a
//! qualifier matches any schema/database. Expansion therefore narrows the
//! catalog query to a superset with these rules (an escaped `LIKE` prefix, so
//! `_` and `%` in names are literal) and keeps only the rows the same
//! [`AllowList`] matches, whatever the server's name comparison.

use common::AllowList;

/// A SQL condition on `column` selecting at least every name `part` matches.
pub(crate) fn superset_clause(column: &str, part: &str) -> String {
    match part {
        "*" | "%" => "1=1".to_string(),
        p if p.ends_with('*') || p.ends_with('%') => format!(
            "{column} LIKE '{}%' ESCAPE '|'",
            escape_like_literal(&p[..p.len() - 1])
        ),
        p => format!("{column} = '{}'", p.replace('\'', "''")),
    }
}

/// `pattern` split like [`AllowList`] does: `(Some(qualifier), name)` or
/// `(None, name)` for any qualifier.
pub(crate) fn split(pattern: &str) -> (Option<&str>, &str) {
    match pattern.split_once('.') {
        Some((q, n)) => (Some(q), n),
        None => (None, pattern),
    }
}

/// Whether CDC captures `qualifier.name` under `pattern`.
pub(crate) fn captures(pattern: &str, qualifier: &str, name: &str) -> bool {
    AllowList::new(&[pattern.to_string()]).matches(qualifier, name)
}

/// A literal for a `LIKE ... ESCAPE '|'` prefix: quotes doubled, `|`, `%`
/// and `_` escaped.
fn escape_like_literal(s: &str) -> String {
    s.replace('\'', "''")
        .replace('|', "||")
        .replace('%', "|%")
        .replace('_', "|_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clauses_select_a_superset_with_literal_metacharacters() {
        assert_eq!(superset_clause("t", "*"), "1=1");
        assert_eq!(superset_clause("t", "%"), "1=1");
        assert_eq!(
            superset_clause("t", "order_*"),
            "t LIKE 'order|_%' ESCAPE '|'"
        );
        assert_eq!(
            superset_clause("t", "order_%"),
            "t LIKE 'order|_%' ESCAPE '|'"
        );
        assert_eq!(superset_clause("t", "a|b%"), "t LIKE 'a||b%' ESCAPE '|'");
        assert_eq!(superset_clause("t", "o'r"), "t = 'o''r'");
        assert_eq!(superset_clause("t", "order_a"), "t = 'order_a'");
    }

    #[test]
    fn captures_follows_the_cdc_allow_list() {
        assert!(captures("db.order_*", "db", "order_a"));
        assert!(captures("db.order_%", "db", "order_a"));
        assert!(!captures("db.order_*", "db", "orderXa"));
        assert!(!captures("db.order_*", "other", "order_a"));
        assert!(captures("order_a", "any_schema", "order_a"));
        assert_eq!(split("order_a"), (None, "order_a"));
        assert_eq!(split("db.t"), (Some("db"), "t"));
    }
}

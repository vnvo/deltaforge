//! Render an index name from a template using an event's source identity.
//!
//! Placeholders: `{db}`, `{schema}` (PG schema, falls back to db), `{table}`.
//! A template with no placeholders is a static index name. The result is
//! lowercased — Elasticsearch index names must be lowercase.

use deltaforge_core::Event;

pub fn render_index(template: &str, ev: &Event) -> String {
    let schema = ev
        .source
        .schema
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or(&ev.source.db);
    template
        .replace("{db}", &ev.source.db)
        .replace("{schema}", schema)
        .replace("{table}", &ev.source.table)
        .to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::elasticsearch::test_support::mk_event;
    use deltaforge_core::Op;
    use serde_json::json;

    fn ev(db: &str, schema: Option<&str>, table: &str) -> Event {
        mk_event(Op::Create, json!({}), json!(null), db, schema, table, 1, None)
    }

    #[test]
    fn substitutes_db_and_table() {
        let e = ev("orders", None, "customers");
        assert_eq!(render_index("cdc-{db}.{table}", &e), "cdc-orders.customers");
    }

    #[test]
    fn static_template_unchanged() {
        let e = ev("orders", None, "customers");
        assert_eq!(render_index("customers", &e), "customers");
    }

    #[test]
    fn schema_placeholder_for_pg() {
        let e = ev("db", Some("public"), "t");
        assert_eq!(render_index("{schema}.{table}", &e), "public.t");
    }

    #[test]
    fn schema_falls_back_to_db_when_absent() {
        let e = ev("shop", None, "t");
        assert_eq!(render_index("{schema}.{table}", &e), "shop.t");
    }

    #[test]
    fn result_is_lowercased() {
        let e = ev("Orders", None, "Customers");
        assert_eq!(render_index("CDC-{db}.{table}", &e), "cdc-orders.customers");
    }
}

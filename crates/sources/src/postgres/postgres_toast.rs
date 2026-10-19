//! Complete rows for sinks that need them (`Sink::requires_complete_rows`).
//!
//! PostgreSQL leaves an unchanged out-of-line (TOASTed) value out of an
//! update's new row. Under REPLICA IDENTITY FULL the source fills it from the
//! full old row (`PgRowDecoder::reconstruct_unchanged`); otherwise it cannot.
//! A pipeline with a sink that needs complete rows therefore requires FULL on
//! every captured table that has a TOAST-capable column: checked in bulk
//! before the source streams, and again for each Relation message (stream
//! start, after DDL, after reconnect or failover). A pipeline whose sinks
//! accept partial rows needs neither.
//!
//! TOAST capability comes from the catalog, never from type names: a column
//! of variable length (`attlen = -1`) stored with a strategy other than plain
//! (`attstorage <> 'p'`). Domains, arrays and every varlena type are covered
//! by their stored attributes.

use common::AllowList;
use deltaforge_core::{SourceError, SourceResult};
use tokio_postgres::NoTls;

/// The incident class of a table that cannot send complete rows.
pub(crate) const CLASS: &str = "toast_without_full_identity";

/// Every table of the publications with its replica identity and its
/// TOAST-capable columns, in one statement.
const PREFLIGHT_SQL: &str = r#"
SELECT pt.schemaname::text, pt.tablename::text, c.relreplident::text,
       coalesce(array_agg(a.attname::text ORDER BY a.attnum)
           FILTER (WHERE a.attlen = -1 AND a.attstorage <> 'p'), '{}')
FROM pg_catalog.pg_publication_tables pt
JOIN pg_catalog.pg_namespace n ON n.nspname = pt.schemaname
JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename
JOIN pg_catalog.pg_attribute a
    ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
WHERE pt.pubname = ANY ($1)
GROUP BY 1, 2, 3
"#;

/// The TOAST-capable columns of one relation.
pub(crate) const RELATION_SQL: &str = r#"
SELECT coalesce(array_agg(a.attname::text ORDER BY a.attnum), '{}')
FROM pg_catalog.pg_attribute a
WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
  AND a.attlen = -1 AND a.attstorage <> 'p'
"#;

/// A captured table that has TOAST-capable columns and no FULL identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Incomplete {
    pub schema: String,
    pub table: String,
    /// `relreplident`: `d` default, `n` nothing, `i` index.
    pub identity: String,
    pub columns: Vec<String>,
}

impl Incomplete {
    pub(crate) fn qualified(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }

    /// The statement that makes the table send complete rows.
    pub(crate) fn alter_statement(&self) -> String {
        format!(
            "ALTER TABLE {}.{} REPLICA IDENTITY FULL;",
            quote_ident(&self.schema),
            quote_ident(&self.table)
        )
    }
}

/// A PostgreSQL identifier, always quoted.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// `relreplident` as PostgreSQL names it.
fn identity_name(code: &str) -> &str {
    match code {
        "d" => "default",
        "n" => "nothing",
        "i" => "index",
        "f" => "full",
        other => other,
    }
}

/// From the preflight rows (schema, table, relreplident, TOAST-capable
/// columns): the captured tables that cannot send complete rows.
pub(crate) fn incomplete(
    rows: impl IntoIterator<Item = (String, String, String, Vec<String>)>,
    allow: &AllowList,
) -> Vec<Incomplete> {
    let mut out: Vec<Incomplete> = rows
        .into_iter()
        .filter(|(schema, table, identity, columns)| {
            allow.matches(schema, table)
                && identity != "f"
                && !columns.is_empty()
        })
        .map(|(schema, table, identity, columns)| Incomplete {
            schema,
            table,
            identity,
            columns,
        })
        .collect();
    out.sort_by(|a, b| (&a.schema, &a.table).cmp(&(&b.schema, &b.table)));
    out
}

/// The refusal for tables that cannot send complete rows: every table, its
/// TOAST-capable columns and identity, and the statement that fixes it.
/// Names only; never a row value.
pub(crate) fn refusal(source_id: &str, tables: &[Incomplete]) -> SourceError {
    let statements: Vec<String> = tables
        .iter()
        .map(|t| {
            format!(
                "{} -- {} (identity {}; TOAST-capable: {})",
                t.alter_statement(),
                t.qualified(),
                identity_name(&t.identity),
                t.columns.join(", ")
            )
        })
        .collect();
    for statement in &statements {
        tracing::error!(
            source_id,
            "needs REPLICA IDENTITY FULL for complete rows: {statement}"
        );
    }
    crate::incident_drafts::toast_without_full_identity(
        source_id,
        CLASS,
        &tables[0].qualified(),
        tables.len(),
        format!(
            "a sink of this pipeline needs complete rows, but PostgreSQL does \
             not send an unchanged TOASTed value unless the table has REPLICA \
             IDENTITY FULL; {} table(s) with TOAST-capable columns lack it. \
             Run: {} Or deliver these tables only to sinks that accept \
             partial rows (native JSON).",
            tables.len(),
            statements.join(" "),
        ),
    )
}

/// Before the source streams: every captured table of the publications can
/// send complete rows. One catalog statement for all tables.
pub(crate) async fn preflight(
    dsn: &str,
    source_id: &str,
    publications: &[String],
    allow: &AllowList,
) -> SourceResult<()> {
    let (client, conn) =
        tokio_postgres::connect(dsn, NoTls).await.map_err(|e| {
            SourceError::Connect {
                details: format!("complete-rows preflight: {e}").into(),
            }
        })?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let rows = client
        .query(PREFLIGHT_SQL, &[&publications])
        .await
        .map_err(|e| {
            SourceError::Other(anyhow::anyhow!("complete-rows preflight: {e}"))
        })?;
    let tables = incomplete(
        rows.iter()
            .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3))),
        allow,
    );
    if tables.is_empty() {
        Ok(())
    } else {
        Err(refusal(source_id, &tables))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(
        s: &str,
        t: &str,
        id: &str,
        cols: &[&str],
    ) -> (String, String, String, Vec<String>) {
        (
            s.into(),
            t.into(),
            id.into(),
            cols.iter().map(|c| c.to_string()).collect(),
        )
    }

    #[test]
    fn only_captured_toastable_tables_without_full_identity_are_incomplete() {
        let allow = AllowList::new(&["public.*".to_string()]);
        let got = incomplete(
            [
                row("public", "docs", "d", &["body"]), // incomplete
                row("public", "full", "f", &["body"]), // FULL
                row("public", "ints", "d", &[]),       // nothing TOASTable
                row("other", "docs", "d", &["body"]),  // not captured
                row("public", "idx", "i", &["body", "meta"]), // index identity
            ],
            &allow,
        );
        assert_eq!(
            got.iter().map(Incomplete::qualified).collect::<Vec<_>>(),
            ["public.docs", "public.idx"]
        );
        assert_eq!(got[1].columns, ["body", "meta"]);
    }

    #[test]
    fn the_refusal_names_every_table_and_its_statement() {
        let tables: Vec<Incomplete> = (0..25)
            .map(|i| Incomplete {
                schema: "public".into(),
                table: format!("t{i:02}"),
                identity: "d".into(),
                columns: vec!["body".into()],
            })
            .chain([Incomplete {
                schema: "odd\"schema".into(),
                table: "Mixed Case".into(),
                identity: "n".into(),
                columns: vec!["doc".into(), "blob".into()],
            }])
            .collect();
        let err = refusal("pg1", &tables);
        let text = err.root().to_string();
        for t in &tables[..25] {
            assert!(
                text.contains(&format!(
                    "ALTER TABLE \"public\".\"{}\" REPLICA IDENTITY FULL;",
                    t.table
                )),
                "{} missing: {text}",
                t.table
            );
        }
        assert!(
            text.contains(
                "ALTER TABLE \"odd\"\"schema\".\"Mixed Case\" REPLICA IDENTITY FULL;"
            ),
            "{text}"
        );
        assert!(text.contains("identity nothing; TOAST-capable: doc, blob"));
        assert!(text.contains("26 table(s)"));
        let draft = err.draft().expect("an incident");
        use deltaforge_core::incident::{EvidenceKey as K, EvidenceValue as V};
        assert_eq!(draft.evidence.text_of(K::Table), Some("public.t00"));
        assert_eq!(
            draft.evidence.get(K::BlockingCount),
            Some(&V::Count { value: 26 })
        );
        assert_eq!(
            draft.evidence.text_of(K::ReasonClass),
            Some("toast_without_full_identity")
        );
        let explained = err.to_string();
        assert!(explained.contains("26 captured table(s)"), "{explained}");
        assert!(explained.contains("first: public.t00"), "{explained}");
    }
}

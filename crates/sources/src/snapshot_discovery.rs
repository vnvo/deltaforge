//! Paged discovery of the tables a snapshot copies.
//!
//! The catalog is read in keyset pages ordered bytewise by `(qualifier,
//! name)` (PostgreSQL `COLLATE "C"`, MySQL `BINARY`): each query asks for at
//! most `page_size` rows strictly after the last row of the previous page, so
//! no page repeats a table and at most one page is resident. All configured
//! patterns are combined into one superset condition per query; the rows are
//! then filtered with the production CDC matcher ([`AllowList`]), so a table
//! matched by several patterns appears once and the snapshot copies exactly
//! the tables CDC captures.
//!
//! The engine runs the queries; [`Discovery`] owns the cursor and checks that
//! the catalog really returns rows in strictly increasing byte order (a
//! repeated or reordered row fails closed: it would copy a table twice or skip
//! one). Queries per discovery: `floor(rows / page_size) + 1`, where `rows`
//! counts the catalog rows the combined condition selects (the last, short or
//! empty, page ends it).

use common::AllowList;
use deltaforge_core::{SourceError, SourceResult};

/// A table as `(schema or database, table)`.
pub(crate) type TableRef = (String, String);

/// The cursor and checks of one paged discovery.
pub(crate) struct Discovery {
    matcher: AllowList,
    page_size: usize,
    after: Option<TableRef>,
    done: bool,
    /// A re-discovery that verifies a plan (counted apart from discovery).
    verification: bool,
}

impl Discovery {
    pub(crate) fn new(patterns: &[String], page_size: usize) -> Self {
        Self {
            matcher: AllowList::new(patterns),
            page_size: page_size.clamp(
                deltaforge_config::DISCOVERY_PAGE_SIZE_MIN,
                deltaforge_config::DISCOVERY_PAGE_SIZE_MAX,
            ),
            after: None,
            done: false,
            verification: false,
        }
    }

    /// A discovery that re-reads the catalog to verify an existing plan.
    pub(crate) fn for_verification(
        patterns: &[String],
        page_size: usize,
    ) -> Self {
        Self {
            verification: true,
            ..Self::new(patterns, page_size)
        }
    }

    /// The exclusive cursor of the next page (`None`: the first page).
    pub(crate) fn after(&self) -> Option<&TableRef> {
        self.after.as_ref()
    }

    pub(crate) fn page_size(&self) -> usize {
        self.page_size
    }

    /// Whether the last page has been read.
    pub(crate) fn is_done(&self) -> bool {
        self.done
    }

    /// Take one page as the catalog returned it: check its order against the
    /// cursor, advance the cursor, and return the tables CDC captures. A page
    /// shorter than the page size is the last.
    pub(crate) fn accept(
        &mut self,
        rows: Vec<TableRef>,
    ) -> SourceResult<Vec<TableRef>> {
        if rows.len() > self.page_size {
            return Err(out_of_contract(format!(
                "a discovery page returned {} rows (page size {})",
                rows.len(),
                self.page_size
            )));
        }
        let mut last = self.after.take();
        for row in &rows {
            if let Some(prev) = &last
                && bytes(row) <= bytes(prev)
            {
                return Err(out_of_contract(format!(
                    "the catalog returned {}.{} after {}.{}: not in strictly \
                     increasing byte order",
                    row.0, row.1, prev.0, prev.1
                )));
            }
            last = Some(row.clone());
        }
        self.done = rows.len() < self.page_size;
        self.after = last;
        let total = rows.len();
        let kept: Vec<TableRef> = rows
            .into_iter()
            .filter(|(q, n)| self.matcher.matches(q, n))
            .collect();
        if self.verification {
            crate::snapshot_probe::record_verification_page();
        } else {
            crate::snapshot_probe::record_discovery_page(total, kept.len());
        }
        Ok(kept)
    }
}

fn bytes(t: &TableRef) -> (&[u8], &[u8]) {
    (t.0.as_bytes(), t.1.as_bytes())
}

fn out_of_contract(details: String) -> SourceError {
    SourceError::Other(anyhow::anyhow!("table discovery: {details}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(q: &str, n: &str) -> TableRef {
        (q.into(), n.into())
    }

    #[test]
    fn pages_advance_an_exclusive_cursor_and_end_on_a_short_page() {
        let mut d = Discovery::new(&[], 2);
        assert_eq!(d.after(), None);
        let p = d.accept(vec![t("a", "x"), t("a", "y")]).unwrap();
        assert_eq!(p.len(), 2);
        assert!(!d.is_done());
        assert_eq!(d.after(), Some(&t("a", "y")));
        let p = d.accept(vec![t("b", "x")]).unwrap();
        assert_eq!(p, vec![t("b", "x")]);
        assert!(d.is_done(), "a short page is the last");
        // An exactly full last page needs one more (empty) query.
        let mut d = Discovery::new(&[], 1);
        d.accept(vec![t("a", "x")]).unwrap();
        assert!(!d.is_done());
        assert!(d.accept(vec![]).unwrap().is_empty());
        assert!(d.is_done());
    }

    #[test]
    fn repeated_or_unordered_rows_fail_closed() {
        let mut d = Discovery::new(&[], 3);
        assert!(d.accept(vec![t("a", "y"), t("a", "x")]).is_err());
        let mut d = Discovery::new(&[], 3);
        assert!(d.accept(vec![t("a", "x"), t("a", "x")]).is_err());
        let mut d = Discovery::new(&[], 1);
        d.accept(vec![t("b", "x")]).unwrap();
        assert!(d.accept(vec![t("a", "z")]).is_err(), "across pages too");
        let mut d = Discovery::new(&[], 1);
        assert!(
            d.accept(vec![t("a", "x"), t("a", "y")]).is_err(),
            "page size"
        );
    }

    #[test]
    fn order_is_bytewise() {
        // Uppercase sorts before lowercase, and a prefix before its
        // extensions, exactly as COLLATE "C" / BINARY order them.
        let mut d = Discovery::new(&[], 10);
        let rows = vec![
            t("A", "x"),
            t("a", "x"),
            t("ab", "x"),
            t("b", "X"),
            t("b", "x"),
        ];
        assert_eq!(d.accept(rows.clone()).unwrap(), rows);
    }

    #[test]
    fn rows_are_filtered_with_the_cdc_matcher() {
        let mut d = Discovery::new(
            &["public.order_*".into(), "public.order_a".into()],
            10,
        );
        let page = d
            .accept(vec![
                t("public", "order_a"),
                t("public", "order_b"),
                t("public", "orderxa"),
            ])
            .unwrap();
        // `_` is literal; overlapping patterns produce the table once.
        assert_eq!(page, vec![t("public", "order_a"), t("public", "order_b")]);
    }
}

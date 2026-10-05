//! The compact per-table plan a snapshot run works from.
//!
//! The preparation pass resolves each discovered table's schema exactly once
//! and keeps only what the run needs: its identity and cursor kind (the
//! schema version is bound into the generation fingerprint). Workers read the full schema again from the loader cache or the
//! durable registry (never the catalog). The plan is still one entry per
//! table: snapshot execution keeps compact per-table plan and frontier state
//! until the durable work queue replaces it.

use crate::durable_checkpoint::CursorKind;

/// One table of the snapshot plan; `I` is the engine's identity description.
#[derive(Debug, Clone)]
pub struct PlannedTable<I> {
    /// Schema (PostgreSQL) or database (MySQL).
    pub qualifier: String,
    pub table: String,
    pub identity: I,
    pub cursor_kind: CursorKind,
    /// The schema shape the plan was prepared from (engine-specific
    /// signature); verified against the catalog at the snapshot anchor.
    pub signature: String,
}

impl<I> PlannedTable<I> {
    /// `qualifier.table`.
    pub fn key(&self) -> String {
        format!("{}.{}", self.qualifier, self.table)
    }
}

/// Approximate resident bytes of plan entries with string identities.
pub(crate) fn plan_bytes<I>(
    tables: &[PlannedTable<I>],
    identity_bytes: impl Fn(&I) -> usize,
) -> usize {
    tables
        .iter()
        .map(|t| {
            std::mem::size_of::<PlannedTable<I>>()
                + t.qualifier.len()
                + t.table.len()
                + identity_bytes(&t.identity)
        })
        .sum()
}

/// A canonical, length-delimited SHA-256 over a table's schema shape. Each
/// engine feeds the same fields from its loaded schema and from the catalog
/// rows it reads at the anchor, so equal signatures mean the plan describes
/// the catalog the rows are read from.
pub(crate) struct ShapeSignature(sha2::Sha256);

impl ShapeSignature {
    pub(crate) fn new(engine: &str) -> Self {
        use sha2::Digest;
        let mut s = Self(sha2::Sha256::new());
        s.text(engine);
        s
    }

    pub(crate) fn text(&mut self, v: &str) -> &mut Self {
        use sha2::Digest;
        self.0.update((v.len() as u64).to_be_bytes());
        self.0.update(v.as_bytes());
        self
    }

    pub(crate) fn num(&mut self, v: i64) -> &mut Self {
        use sha2::Digest;
        self.0.update(v.to_be_bytes());
        self
    }

    pub(crate) fn finish(self) -> String {
        use sha2::Digest;
        hex::encode(self.0.finalize())
    }
}

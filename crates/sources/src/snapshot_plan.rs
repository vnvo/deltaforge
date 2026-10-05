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
    /// [`schema_signature`] of the schema the plan was prepared from;
    /// verified against the catalog at the snapshot anchor.
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

/// A plan entry's schema signature: a SHA-256 over the engine's whole
/// registered schema model (its serialized form, every field). Preparation
/// takes it from the schema it registered and the anchor from the model the
/// same fetch builds there, so the two never compare different schema
/// semantics.
pub(crate) fn schema_signature<S: serde::Serialize>(model: &S) -> String {
    use sha2::Digest;
    let bytes =
        serde_json::to_vec(model).expect("a schema model always serializes");
    hex::encode(sha2::Sha256::digest(bytes))
}

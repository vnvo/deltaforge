//! Durable per-source physical-lineage record.
//!
//! Established by a source after it verifies its live physical lineage and
//! before it first touches the scoped schema registry. Read by the REST schema
//! API and the one-shot migration command so they can reconstruct the same
//! qualified registry key the source writes under, and by the registry itself to
//! keep version numbering monotonic across a lineage change.
//!
//! # Namespace
//! `ns = "schema_lineage"`, `key = "{tenant}/{source_id}"` (segments encoded).

use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use super::schema_key::{LineageDescriptor, encode_segment};
use crate::ArcStorageBackend;

const NS: &str = "schema_lineage";
const RECORD_VERSION: u32 = 1;

/// A lineage descriptor together with its derived hash.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LineageRef {
    pub descriptor: LineageDescriptor,
    pub lineage_hash: String,
}

impl LineageRef {
    pub fn new(descriptor: LineageDescriptor) -> Self {
        let lineage_hash = descriptor.lineage_hash();
        Self {
            descriptor,
            lineage_hash,
        }
    }
}

/// The verified physical lineage a source currently runs under, its immediate
/// predecessor, and every lineage hash it has ever used.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceLineageRecord {
    pub record_version: u32,
    pub current: LineageRef,
    /// The lineage this source ran under immediately before `current`, if the
    /// lineage has ever changed (failover or replacement).
    #[serde(default)]
    pub previous: Option<LineageRef>,
    /// Every earlier lineage hash (oldest first, no duplicates, never
    /// `current`). Used only to keep version numbers from being reused.
    #[serde(default)]
    pub prior_lineage_hashes: Vec<String>,
    pub established_at_ms: i64,
}

impl SourceLineageRecord {
    /// Current and prior lineage hashes.
    pub fn all_lineage_hashes(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.current.lineage_hash.as_str())
            .chain(self.prior_lineage_hashes.iter().map(String::as_str))
    }
}

/// Result of [`establish`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Established {
    pub record: SourceLineageRecord,
    /// `Some(prior)` when this call moved the source to a new lineage.
    pub changed_from: Option<LineageRef>,
}

fn record_key(tenant: &str, source_id: &str) -> String {
    format!("{}/{}", encode_segment(tenant), encode_segment(source_id))
}

/// Make `descriptor` the current lineage of `(tenant, source_id)`, failing
/// closed on any storage error.
///
/// - Same lineage as recorded: no write; the existing record is returned.
/// - No record, or a different lineage: the record is (re)written durably
///   before returning. On a change the old current lineage becomes `previous`
///   and joins `prior_lineage_hashes`, so the new lineage gets a fresh, empty
///   schema namespace while version numbering stays monotonic.
///
/// A caller must not access the scoped registry unless this returns `Ok`.
pub async fn establish(
    backend: &ArcStorageBackend,
    tenant: &str,
    source_id: &str,
    descriptor: LineageDescriptor,
) -> Result<Established> {
    let current = LineageRef::new(descriptor);
    let existing = load(backend, tenant, source_id).await?;
    if let Some(rec) = &existing
        && rec.current == current
    {
        return Ok(Established {
            record: rec.clone(),
            changed_from: None,
        });
    }

    let (previous, mut prior) = match existing {
        Some(rec) => {
            let mut prior = rec.prior_lineage_hashes;
            prior.push(rec.current.lineage_hash.clone());
            (Some(rec.current), prior)
        }
        None => (None, Vec::new()),
    };
    // Never list the (new) current lineage as prior, and keep entries unique.
    prior.retain(|h| *h != current.lineage_hash);
    let mut seen = std::collections::HashSet::new();
    prior.retain(|h| seen.insert(h.clone()));

    let record = SourceLineageRecord {
        record_version: RECORD_VERSION,
        current,
        previous: previous.clone(),
        prior_lineage_hashes: prior,
        established_at_ms: Utc::now().timestamp_millis(),
    };
    backend
        .kv_put(
            NS,
            &record_key(tenant, source_id),
            &serde_json::to_vec(&record)?,
        )
        .await
        .context("source lineage: failed to persist the current lineage")?;
    Ok(Established {
        record,
        changed_from: previous,
    })
}

/// Read the lineage record for `(tenant, source_id)`. `Ok(None)` means no
/// lineage has been established; an error is a storage failure and must be
/// treated as fail-closed by callers (never as "no lineage").
pub async fn load(
    backend: &ArcStorageBackend,
    tenant: &str,
    source_id: &str,
) -> Result<Option<SourceLineageRecord>> {
    match backend
        .kv_get(NS, &record_key(tenant, source_id))
        .await
        .context("source lineage: failed to read lineage record")?
    {
        Some(bytes) => Ok(Some(
            serde_json::from_slice(&bytes)
                .context("source lineage: corrupt lineage record")?,
        )),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStorageBackend;
    use std::sync::Arc;

    fn backend() -> ArcStorageBackend {
        Arc::new(MemoryStorageBackend::new())
    }

    fn pg(sysid: u64, dboid: u64) -> LineageDescriptor {
        LineageDescriptor::postgres(sysid, dboid).unwrap()
    }

    #[tokio::test]
    async fn first_establish_persists_without_predecessor() {
        let b = backend();
        let e = establish(&b, "acme", "src1", pg(1, 2)).await.unwrap();
        assert_eq!(e.changed_from, None);
        let loaded = load(&b, "acme", "src1").await.unwrap().unwrap();
        assert_eq!(loaded, e.record);
        assert_eq!(loaded.current.lineage_hash, pg(1, 2).lineage_hash());
        assert!(loaded.previous.is_none());
    }

    #[tokio::test]
    async fn same_lineage_is_a_no_op() {
        let b = backend();
        let first = establish(&b, "acme", "src1", pg(1, 2)).await.unwrap();
        let again = establish(&b, "acme", "src1", pg(1, 2)).await.unwrap();
        assert_eq!(again.changed_from, None);
        assert_eq!(again.record, first.record);
    }

    #[tokio::test]
    async fn change_records_predecessor_and_prior_hashes() {
        let b = backend();
        establish(&b, "acme", "src1", pg(1, 2)).await.unwrap();
        let second = establish(&b, "acme", "src1", pg(1, 3)).await.unwrap();
        assert_eq!(second.changed_from, Some(LineageRef::new(pg(1, 2))));
        let third = establish(&b, "acme", "src1", pg(9, 9)).await.unwrap();
        assert_eq!(third.record.previous, Some(LineageRef::new(pg(1, 3))));
        assert_eq!(
            third.record.prior_lineage_hashes,
            vec![pg(1, 2).lineage_hash(), pg(1, 3).lineage_hash()]
        );
        // Returning to an earlier lineage never lists it as its own prior.
        let back = establish(&b, "acme", "src1", pg(1, 2)).await.unwrap();
        assert!(
            !back
                .record
                .prior_lineage_hashes
                .contains(&pg(1, 2).lineage_hash())
        );
        assert_eq!(back.record.prior_lineage_hashes.len(), 2);
    }

    #[tokio::test]
    async fn establish_fails_closed_on_storage_errors() {
        use crate::adapters::test_util::FaultBackend;
        use std::sync::atomic::Ordering;
        let f = Arc::new(FaultBackend::new());
        let b: ArcStorageBackend = f.clone();
        establish(&b, "acme", "src1", pg(1, 2)).await.unwrap();

        // Unreadable record: never treated as "no lineage".
        f.fail_kv_get.store(true, Ordering::SeqCst);
        assert!(establish(&b, "acme", "src1", pg(1, 2)).await.is_err());
        f.fail_kv_get.store(false, Ordering::SeqCst);

        // A detected replacement that cannot be persisted must fail, and the
        // durable record must still name the old lineage (not advanced).
        f.fail_kv_put.store(true, Ordering::SeqCst);
        assert!(establish(&b, "acme", "src1", pg(1, 3)).await.is_err());
        f.fail_kv_put.store(false, Ordering::SeqCst);
        let rec = load(&b, "acme", "src1").await.unwrap().unwrap();
        assert_eq!(rec.current, LineageRef::new(pg(1, 2)));
    }

    #[tokio::test]
    async fn distinct_sources_do_not_alias() {
        let b = backend();
        establish(
            &b,
            "acme",
            "src1",
            LineageDescriptor::mysql("3e11fa47-71ca-11e1-9e33-c80aa9429562")
                .unwrap(),
        )
        .await
        .unwrap();
        assert!(load(&b, "acme", "src2").await.unwrap().is_none());
    }
}

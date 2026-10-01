//! One-time adoption of pre-lineage per-sink checkpoints.
//!
//! Checkpoints now carry the verified server lineage, and checkpoints of
//! different lineages - or a lineage-bearing and a pre-lineage one - are never
//! ordered (see `compare_mysql_checkpoints`). Per-sink checkpoints are written
//! independently (a lagging or optional sink keeps its old checkpoint, and a
//! crash can fall between two sinks' writes), so an upgrade could otherwise
//! leave a source with a permanently mixed set.
//!
//! At startup, after the server lineage is verified and recorded and before
//! the resume position is read, pre-lineage checkpoints are stamped with that
//! lineage only when that is provable from durable state:
//! - the source's durable lineage record has ONLY the verified lineage (no
//!   `previous`, no prior lineages): no lineage change was ever recorded, so
//!   every checkpoint this source wrote belongs to that server;
//! - every checkpoint that already carries a lineage carries the same one.
//!
//! Stamping rewrites each pre-lineage checkpoint in place and is idempotent: a
//! crash midway leaves a set that still satisfies both conditions, and the
//! next start completes it. Otherwise nothing is changed and the resume fold
//! fails closed on the mixed set.

use checkpoints::CheckpointStore;
use storage::ArcStorageBackend;
use storage::adapters::source_lineage;
use tracing::{info, warn};

use deltaforge_core::{SourceError, SourceResult};

use super::MySqlCheckpoint;

/// What startup adoption did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Adoption {
    /// No pre-lineage checkpoint.
    NothingToDo,
    /// This many pre-lineage checkpoints now carry the verified lineage.
    Stamped(usize),
    /// Left unchanged; the resume fold will fail closed on the mixed set.
    Refused(String),
}

/// Whether `s` is a canonical lineage hash: exactly 32 lowercase hex digits.
pub(crate) fn is_canonical_lineage(s: &str) -> bool {
    s.len() == 32 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Stamp `source_id`'s pre-lineage checkpoints with `lineage_hash` when
/// provable (module docs). Only the source's per-sink keys
/// (`{source_id}::sink::*`) are considered, and its exact aggregate key
/// (`{source_id}`) when no per-sink key exists (then it is the resume
/// checkpoint); no other record is read as a checkpoint. Every candidate is
/// classified before anything is rewritten, so a foreign-lineage or malformed
/// entry prevents every rewrite. A rewrite inserts only the `lineage` field
/// into the stored JSON object; every other field stays byte-for-byte.
pub(crate) async fn adopt_legacy_checkpoints(
    chkpt: &dyn CheckpointStore,
    backend: &ArcStorageBackend,
    tenant: &str,
    source_id: &str,
    lineage_hash: &str,
) -> SourceResult<Adoption> {
    let other = |e: &dyn std::fmt::Display| {
        SourceError::Other(anyhow::anyhow!(
            "checkpoint lineage adoption for {source_id}: {e}"
        ))
    };
    if !is_canonical_lineage(lineage_hash) {
        return Err(other(&format!(
            "verified lineage {lineage_hash:?} is not a canonical lineage hash"
        )));
    }
    let prefix = format!("{source_id}::sink::");
    let mut keys = chkpt
        .list_with_prefix(&prefix)
        .await
        .map_err(|e| other(&e))?;
    keys.retain(|k| k.starts_with(&prefix));
    if keys.is_empty() {
        // No per-sink checkpoint: the aggregate key is the resume checkpoint
        // (read through the per-sink proxy it is returned as stored).
        keys.push(source_id.to_string());
    }

    let mut legacy: Vec<(String, serde_json::Map<String, serde_json::Value>)> =
        Vec::new();
    for key in keys {
        let Some(raw) = chkpt.get_raw(&key).await.map_err(|e| other(&e))?
        else {
            continue;
        };
        let refuse = |reason: String| {
            warn!(source_id, %reason, "pre-lineage checkpoints not adopted");
            Ok(Adoption::Refused(reason))
        };
        let (Ok(cp), Ok(serde_json::Value::Object(obj))) = (
            serde_json::from_slice::<MySqlCheckpoint>(&raw),
            serde_json::from_slice::<serde_json::Value>(&raw),
        ) else {
            return refuse(format!(
                "checkpoint {key} is not a MySQL checkpoint"
            ));
        };
        match &cp.lineage {
            None => legacy.push((key, obj)),
            Some(l) if l == lineage_hash => {}
            Some(l) => {
                return refuse(format!(
                    "checkpoint {key} belongs to lineage {l}, not the verified \
                     lineage {lineage_hash}"
                ));
            }
        }
    }
    if legacy.is_empty() {
        return Ok(Adoption::NothingToDo);
    }

    let record = source_lineage::load(backend, tenant, source_id)
        .await
        .map_err(|e| other(&e))?;
    let reason = match &record {
        None => Some("no durable lineage record".to_string()),
        Some(r) if r.current.lineage_hash != lineage_hash => Some(format!(
            "the durable lineage record is {}, not the verified lineage {lineage_hash}",
            r.current.lineage_hash
        )),
        Some(r)
            if r.previous.is_some() || !r.prior_lineage_hashes.is_empty() =>
        {
            Some(
                "the source's lineage has changed before (failover or \
                 replacement), so pre-lineage checkpoints cannot be attributed"
                    .to_string(),
            )
        }
        Some(_) => None,
    };
    if let Some(reason) = reason {
        warn!(source_id, %reason, "pre-lineage checkpoints not adopted");
        return Ok(Adoption::Refused(reason));
    }

    let n = legacy.len();
    for (key, mut obj) in legacy {
        obj.insert(
            "lineage".to_string(),
            serde_json::Value::String(lineage_hash.to_string()),
        );
        let bytes = serde_json::to_vec(&obj).map_err(|e| other(&e))?;
        chkpt.put_raw(&key, &bytes).await.map_err(|e| other(&e))?;
    }
    info!(
        source_id,
        lineage = lineage_hash,
        checkpoints = n,
        "pre-lineage checkpoints adopted into the verified lineage"
    );
    Ok(Adoption::Stamped(n))
}

#[cfg(test)]
mod tests {
    use super::*;
    use checkpoints::MemCheckpointStore;
    use std::sync::Arc;
    use storage::MemoryStorageBackend;
    use storage::adapters::LineageDescriptor;

    const T: &str = "acme";
    const S: &str = "orders";
    const UUID_A: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
    const UUID_B: &str = "4f2a0b1c-71ca-11e1-9e33-c80aa9429562";
    const OTHER: &str = "0123456789abcdef0123456789abcdef";

    fn cp(file: &str, pos: u64, lineage: Option<&str>) -> Vec<u8> {
        serde_json::to_vec(&MySqlCheckpoint {
            file: file.into(),
            pos,
            gtid_set: None,
            lineage: lineage.map(str::to_string),
        })
        .unwrap()
    }

    async fn setup(
        uuids: &[&str],
    ) -> (Arc<MemCheckpointStore>, ArcStorageBackend, String) {
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let mut hash = String::new();
        for u in uuids {
            hash = source_lineage::establish(
                &backend,
                T,
                S,
                LineageDescriptor::mysql(u).unwrap(),
            )
            .await
            .unwrap()
            .record
            .current
            .lineage_hash;
        }
        (Arc::new(MemCheckpointStore::new().unwrap()), backend, hash)
    }

    async fn lineage_of(
        store: &MemCheckpointStore,
        key: &str,
    ) -> Option<String> {
        let raw = store.get_raw(key).await.unwrap().unwrap();
        serde_json::from_slice::<MySqlCheckpoint>(&raw)
            .unwrap()
            .lineage
    }

    #[tokio::test]
    async fn legacy_checkpoints_of_an_unchanged_lineage_are_adopted_once() {
        let (store, backend, l) = setup(&[UUID_A]).await;
        store
            .put_raw("orders::sink::kafka", &cp("bin.000002", 9, None))
            .await
            .unwrap();
        store
            .put_raw("orders::sink::s3", &cp("bin.000001", 4, None))
            .await
            .unwrap();
        // Another source's checkpoint is never touched.
        store
            .put_raw("orders-archive::sink::kafka", &cp("bin.000001", 4, None))
            .await
            .unwrap();

        let out = adopt_legacy_checkpoints(store.as_ref(), &backend, T, S, &l)
            .await
            .unwrap();
        assert_eq!(out, Adoption::Stamped(2));
        for k in ["orders::sink::kafka", "orders::sink::s3"] {
            assert_eq!(
                lineage_of(&store, k).await.as_deref(),
                Some(l.as_str())
            );
        }
        assert_eq!(
            lineage_of(&store, "orders-archive::sink::kafka").await,
            None
        );
        // Positions unchanged, and now comparable under one lineage.
        let a = store.get_raw("orders::sink::s3").await.unwrap().unwrap();
        let b = store.get_raw("orders::sink::kafka").await.unwrap().unwrap();
        assert_eq!(
            super::super::compare_mysql_checkpoints(&a, &b),
            deltaforge_core::CheckpointOrder::Before
        );
        // Idempotent.
        assert_eq!(
            adopt_legacy_checkpoints(store.as_ref(), &backend, T, S, &l)
                .await
                .unwrap(),
            Adoption::NothingToDo
        );
    }

    /// A crash midway (or a sink that committed under the new version while
    /// another kept its old checkpoint) leaves a mix that is completed.
    #[tokio::test]
    async fn a_partially_adopted_set_is_completed() {
        let (store, backend, l) = setup(&[UUID_A]).await;
        store
            .put_raw("orders::sink::kafka", &cp("bin.000002", 9, Some(&l)))
            .await
            .unwrap();
        store
            .put_raw("orders::sink::s3", &cp("bin.000001", 4, None))
            .await
            .unwrap();
        assert_eq!(
            adopt_legacy_checkpoints(store.as_ref(), &backend, T, S, &l)
                .await
                .unwrap(),
            Adoption::Stamped(1)
        );
        assert_eq!(
            lineage_of(&store, "orders::sink::s3").await.as_deref(),
            Some(l.as_str())
        );
    }

    /// After a recorded lineage change, pre-lineage checkpoints may belong to
    /// the earlier server: never attributed.
    #[tokio::test]
    async fn legacy_checkpoints_after_a_lineage_change_are_left() {
        let (store, backend, l) = setup(&[UUID_A, UUID_B]).await;
        store
            .put_raw("orders::sink::kafka", &cp("bin.000002", 9, Some(&l)))
            .await
            .unwrap();
        store
            .put_raw("orders::sink::s3", &cp("bin.000002", 9, None))
            .await
            .unwrap();
        let out = adopt_legacy_checkpoints(store.as_ref(), &backend, T, S, &l)
            .await
            .unwrap();
        assert!(
            matches!(out, Adoption::Refused(r) if r.contains("changed before"))
        );
        assert_eq!(lineage_of(&store, "orders::sink::s3").await, None);
        // The fold therefore sees a mixed set and fails closed.
        let a = store.get_raw("orders::sink::kafka").await.unwrap().unwrap();
        let b = store.get_raw("orders::sink::s3").await.unwrap().unwrap();
        assert_eq!(
            super::super::compare_mysql_checkpoints(&a, &b),
            deltaforge_core::CheckpointOrder::Incomparable
        );
    }

    #[tokio::test]
    async fn a_checkpoint_of_another_lineage_blocks_adoption() {
        let (store, backend, l) = setup(&[UUID_A]).await;
        store
            .put_raw("orders::sink::kafka", &cp("bin.000002", 9, Some(OTHER)))
            .await
            .unwrap();
        store
            .put_raw("orders::sink::s3", &cp("bin.000001", 4, None))
            .await
            .unwrap();
        assert!(matches!(
            adopt_legacy_checkpoints(store.as_ref(), &backend, T, S, &l).await.unwrap(),
            Adoption::Refused(r) if r.contains("belongs to lineage")
        ));
        assert_eq!(lineage_of(&store, "orders::sink::s3").await, None);
    }

    #[tokio::test]
    async fn a_verified_lineage_other_than_the_record_blocks_adoption() {
        let (store, backend, _) = setup(&[UUID_A]).await;
        store
            .put_raw("orders::sink::s3", &cp("bin.000001", 4, None))
            .await
            .unwrap();
        assert!(matches!(
            adopt_legacy_checkpoints(store.as_ref(), &backend, T, S, OTHER)
                .await
                .unwrap(),
            Adoption::Refused(_)
        ));
        assert_eq!(lineage_of(&store, "orders::sink::s3").await, None);
    }

    /// With no per-sink checkpoint, the exact aggregate key is the resume
    /// checkpoint and is adopted; snapshot progress and other records under
    /// similar names are never read as checkpoints.
    #[tokio::test]
    async fn the_aggregate_key_is_adopted_and_control_records_are_untouched() {
        let (store, backend, l) = setup(&[UUID_A]).await;
        store.put_raw(S, &cp("bin.000001", 4, None)).await.unwrap();
        let progress = br#"{"finished":false,"start_position":"x"}"#;
        store
            .put_raw("mysql_snapshot_progress:orders", progress)
            .await
            .unwrap();
        store
            .put_raw("orders::other", b"not a checkpoint")
            .await
            .unwrap();
        assert_eq!(
            adopt_legacy_checkpoints(store.as_ref(), &backend, T, S, &l)
                .await
                .unwrap(),
            Adoption::Stamped(1)
        );
        assert_eq!(lineage_of(&store, S).await.as_deref(), Some(l.as_str()));
        assert_eq!(
            store
                .get_raw("mysql_snapshot_progress:orders")
                .await
                .unwrap()
                .unwrap(),
            progress
        );
        assert_eq!(
            store.get_raw("orders::other").await.unwrap().unwrap(),
            b"not a checkpoint"
        );
    }

    /// Only the `lineage` field is added; every position field keeps its exact
    /// value, including an empty GTID set (`Some("")`).
    #[tokio::test]
    async fn adoption_preserves_every_position_field() {
        let (store, backend, l) = setup(&[UUID_A]).await;
        let empty_gtid = br#"{"file":"bin.000003","pos":4,"gtid_set":""}"#;
        let gtid = br#"{"file":"bin.000004","pos":157,"gtid_set":"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-9"}"#;
        store.put_raw("orders::sink::a", empty_gtid).await.unwrap();
        store.put_raw("orders::sink::b", gtid).await.unwrap();
        adopt_legacy_checkpoints(store.as_ref(), &backend, T, S, &l)
            .await
            .unwrap();
        for (key, before) in [
            ("orders::sink::a", &empty_gtid[..]),
            ("orders::sink::b", &gtid[..]),
        ] {
            let mut want: serde_json::Value =
                serde_json::from_slice(before).unwrap();
            want["lineage"] = serde_json::Value::String(l.clone());
            let got: serde_json::Value = serde_json::from_slice(
                &store.get_raw(key).await.unwrap().unwrap(),
            )
            .unwrap();
            assert_eq!(got, want, "{key}");
        }
        let a: MySqlCheckpoint = serde_json::from_slice(
            &store.get_raw("orders::sink::a").await.unwrap().unwrap(),
        )
        .unwrap();
        assert_eq!(a.gtid_set.as_deref(), Some(""));
    }

    /// A malformed entry, like a foreign one, prevents every rewrite.
    #[tokio::test]
    async fn a_malformed_entry_prevents_every_rewrite() {
        let (store, backend, l) = setup(&[UUID_A]).await;
        store
            .put_raw("orders::sink::a", &cp("bin.000001", 4, None))
            .await
            .unwrap();
        store.put_raw("orders::sink::b", b"{garbage").await.unwrap();
        assert!(matches!(
            adopt_legacy_checkpoints(store.as_ref(), &backend, T, S, &l)
                .await
                .unwrap(),
            Adoption::Refused(_)
        ));
        assert_eq!(lineage_of(&store, "orders::sink::a").await, None);
    }

    /// A store whose writes fail after a budget: a crash part-way through.
    struct CrashingStore {
        inner: MemCheckpointStore,
        writes_left: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl CheckpointStore for CrashingStore {
        async fn get_raw(
            &self,
            k: &str,
        ) -> checkpoints::CheckpointResult<Option<Vec<u8>>> {
            self.inner.get_raw(k).await
        }
        async fn put_raw(
            &self,
            k: &str,
            v: &[u8],
        ) -> checkpoints::CheckpointResult<()> {
            use std::sync::atomic::Ordering::SeqCst;
            if self.writes_left.load(SeqCst) == 0 {
                return Err(checkpoints::CheckpointError::Data(
                    "injected crash".into(),
                ));
            }
            self.writes_left.fetch_sub(1, SeqCst);
            self.inner.put_raw(k, v).await
        }
        async fn delete(&self, k: &str) -> checkpoints::CheckpointResult<bool> {
            self.inner.delete(k).await
        }
        async fn list(&self) -> checkpoints::CheckpointResult<Vec<String>> {
            self.inner.list().await
        }
    }

    /// A crash after rewriting one of several sink checkpoints: the restart
    /// finds a partial set (matching + legacy), completes it, and every
    /// position is unchanged.
    #[tokio::test]
    async fn a_restart_after_a_crash_mid_adoption_completes_it() {
        let (_, backend, l) = setup(&[UUID_A]).await;
        let store = CrashingStore {
            inner: MemCheckpointStore::new().unwrap(),
            writes_left: std::sync::atomic::AtomicUsize::new(usize::MAX),
        };
        let originals = [
            ("orders::sink::a", cp("bin.000001", 4, None)),
            ("orders::sink::b", cp("bin.000002", 9, None)),
            ("orders::sink::c", cp("bin.000003", 1, None)),
        ];
        for (k, v) in &originals {
            store.put_raw(k, v).await.unwrap();
        }
        store
            .writes_left
            .store(1, std::sync::atomic::Ordering::SeqCst);
        assert!(
            adopt_legacy_checkpoints(&store, &backend, T, S, &l)
                .await
                .is_err(),
            "the crash interrupts adoption"
        );
        let mut stamped = Vec::new();
        for (k, _) in &originals {
            stamped.push(lineage_of(&store.inner, k).await);
        }
        assert_eq!(stamped.iter().filter(|s| s.is_some()).count(), 1);

        // Restart.
        store
            .writes_left
            .store(usize::MAX, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            adopt_legacy_checkpoints(&store, &backend, T, S, &l)
                .await
                .unwrap(),
            Adoption::Stamped(2)
        );
        for (k, v) in &originals {
            let got: MySqlCheckpoint = serde_json::from_slice(
                &store.inner.get_raw(k).await.unwrap().unwrap(),
            )
            .unwrap();
            let want: MySqlCheckpoint = serde_json::from_slice(v).unwrap();
            assert_eq!(got.lineage.as_deref(), Some(l.as_str()));
            assert_eq!(
                (got.file, got.pos, got.gtid_set),
                (want.file, want.pos, want.gtid_set)
            );
        }
    }

    #[test]
    fn canonical_lineage_is_32_lowercase_hex() {
        assert!(is_canonical_lineage("0123456789abcdef0123456789abcdef"));
        for bad in [
            "",
            "0123456789ABCDEF0123456789ABCDEF",
            "0123456789abcdef0123456789abcde",
            "0123456789abcdef0123456789abcdef0",
            "0123456789abcdef0123456789abcdeg",
            "lineage-a",
        ] {
            assert!(!is_canonical_lineage(bad), "{bad}");
        }
    }
}

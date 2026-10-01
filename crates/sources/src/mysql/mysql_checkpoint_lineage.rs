//! Startup reconciliation of stored MySQL checkpoints with the verified
//! server lineage.
//!
//! Checkpoints carry the verified server lineage, and checkpoints of different
//! lineages - or a lineage-bearing and a pre-lineage one - are never ordered
//! (see `compare_mysql_checkpoints`). Per-sink checkpoints are written
//! independently, so a source can hold checkpoints written before lineage
//! existed, or written on the previous server of a failover.
//!
//! At startup, after the server lineage is verified and durably recorded and
//! BEFORE snapshot progress or the resume position is read, every candidate
//! checkpoint (the `{source_id}::sink::*` keys, or the exact aggregate key when
//! there is none) is classified against the durable lineage edge
//! `previous -> current`, then:
//! - all current: nothing to do;
//! - pre-lineage checkpoints with NO recorded lineage change: adopted into the
//!   current lineage (the source has only ever run on this server);
//! - checkpoints of the IMMEDIATE predecessor lineage (a failover): carried
//!   over only if every one carries a GTID set and every set was executed by
//!   the verified current server (checked on one connection that re-verifies
//!   the server's identity first);
//! - anything else stops the source with a typed checkpoint error and rewrites
//!   nothing: a pre-lineage checkpoint once a lineage change exists, a
//!   malformed checkpoint or lineage, any other lineage (not transitive to
//!   older predecessors), a predecessor file/pos checkpoint, an unavailable or
//!   unverifiable GTID set.
//!
//! All validation completes before the first rewrite. A rewrite inserts only
//! the `lineage` field into the stored JSON object, so every position field is
//! preserved. Rewriting is idempotent; a crash part-way leaves checkpoints of
//! the current lineage plus the remaining legacy / predecessor ones, which the
//! next start classifies and finishes the same way.

use async_trait::async_trait;
use checkpoints::CheckpointStore;
use storage::ArcStorageBackend;
use storage::adapters::source_lineage;
use tracing::info;

use deltaforge_core::{SourceError, SourceResult};

use super::MySqlCheckpoint;

/// What startup reconciliation did (refusals are errors).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Reconciled {
    /// Every candidate already carries the current lineage (or none exists).
    NothingToDo,
    /// Pre-lineage checkpoints adopted into the current lineage (no recorded
    /// lineage change).
    Adopted(usize),
    /// Checkpoints of the immediate predecessor lineage carried over to the
    /// current lineage after GTID verification (failover).
    CarriedOver(usize),
}

/// Whether `s` is a canonical lineage hash: exactly 32 lowercase hex digits.
pub(crate) fn is_canonical_lineage(s: &str) -> bool {
    s.len() == 32 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Verifies that GTID sets were executed by the verified current server.
#[async_trait]
pub(crate) trait GtidAvailability: Send + Sync {
    /// `Ok` only if, on one connection whose server identity is re-verified
    /// first, every set is a subset of the server's `gtid_executed`; any
    /// failure or doubt is `Err` with the reason.
    async fn verify_executed(&self, sets: &[String]) -> Result<(), String>;
}

/// The live check against the source server.
pub(crate) struct LiveGtidAvailability<'a> {
    pub dsn: &'a str,
    /// The `server_uuid` of the verified current lineage.
    pub server_uuid: String,
}

#[async_trait]
impl GtidAvailability for LiveGtidAvailability<'_> {
    async fn verify_executed(&self, sets: &[String]) -> Result<(), String> {
        use mysql_async::prelude::Queryable;
        let pool = mysql_async::Pool::new(self.dsn);
        let result = async {
            let mut conn = pool
                .get_conn()
                .await
                .map_err(|e| format!("connect failed: {e}"))?;
            let uuid: Option<String> = conn
                .query_first("SELECT @@GLOBAL.server_uuid")
                .await
                .map_err(|e| format!("server_uuid query failed: {e}"))?;
            if uuid.as_deref() != Some(self.server_uuid.as_str()) {
                return Err(format!(
                    "the server answering is {uuid:?}, not the verified {}",
                    self.server_uuid
                ));
            }
            for set in sets {
                let executed: Option<i64> = conn
                    .exec_first(
                        "SELECT GTID_SUBSET(?, @@GLOBAL.gtid_executed)",
                        (set.as_str(),),
                    )
                    .await
                    .map_err(|e| format!("GTID_SUBSET failed for {set:?}: {e}"))?;
                match executed {
                    Some(1) => {}
                    Some(0) => {
                        return Err(format!(
                            "GTID set {set:?} was not executed by the current server"
                        ));
                    }
                    other => {
                        return Err(format!(
                            "GTID_SUBSET returned {other:?} for {set:?}"
                        ));
                    }
                }
            }
            conn.disconnect().await.ok();
            Ok(())
        }
        .await;
        pool.disconnect().await.ok();
        result
    }
}

fn refuse(source_id: &str, reason: String) -> SourceError {
    SourceError::Checkpoint {
        details: format!(
            "stored checkpoints of source {source_id} cannot be attributed to the \
             verified server lineage: {reason}. Nothing was rewritten and the \
             source did not start. Re-snapshot, or deliberately move the affected \
             checkpoints after assessing the data they represent."
        )
        .into(),
    }
}

enum Class {
    Current,
    Legacy,
    Predecessor,
}

/// Reconcile `source_id`'s stored checkpoints with `current_lineage` (module
/// docs). Must run after the lineage is verified and durably recorded and
/// before anything reads snapshot progress or the resume position.
pub(crate) async fn reconcile_checkpoint_lineage(
    chkpt: &dyn CheckpointStore,
    backend: &ArcStorageBackend,
    tenant: &str,
    source_id: &str,
    current_lineage: &str,
    gtid: &dyn GtidAvailability,
) -> SourceResult<Reconciled> {
    let other = |e: &dyn std::fmt::Display| {
        SourceError::Other(anyhow::anyhow!(
            "checkpoint lineage reconciliation for {source_id}: {e}"
        ))
    };
    if !is_canonical_lineage(current_lineage) {
        return Err(refuse(
            source_id,
            format!(
                "the verified lineage {current_lineage:?} is not canonical"
            ),
        ));
    }
    let record = source_lineage::load(backend, tenant, source_id)
        .await
        .map_err(|e| other(&e))?
        .ok_or_else(|| refuse(source_id, "no durable lineage record".into()))?;
    if record.current.lineage_hash != current_lineage {
        return Err(refuse(
            source_id,
            format!(
                "the durable lineage record is {}, not the verified {current_lineage}",
                record.current.lineage_hash
            ),
        ));
    }
    let predecessor = record.previous.as_ref().map(|p| p.lineage_hash.clone());
    let lineage_changed =
        predecessor.is_some() || !record.prior_lineage_hashes.is_empty();

    // Candidates: per-sink keys, or the exact aggregate key without them.
    let prefix = format!("{source_id}::sink::");
    let mut keys = chkpt
        .list_with_prefix(&prefix)
        .await
        .map_err(|e| other(&e))?;
    keys.retain(|k| k.starts_with(&prefix));
    if keys.is_empty() {
        keys.push(source_id.to_string());
    }

    // 1. Classify every candidate before anything else.
    let mut to_rewrite: Vec<(
        String,
        serde_json::Map<String, serde_json::Value>,
    )> = Vec::new();
    let (mut legacy, mut carried, mut gtid_sets) = (0usize, 0usize, Vec::new());
    for key in keys {
        let Some(raw) = chkpt.get_raw(&key).await.map_err(|e| other(&e))?
        else {
            continue;
        };
        let (Ok(cp), Ok(serde_json::Value::Object(obj))) = (
            serde_json::from_slice::<MySqlCheckpoint>(&raw),
            serde_json::from_slice::<serde_json::Value>(&raw),
        ) else {
            return Err(refuse(
                source_id,
                format!(
                    "checkpoint {key} is malformed (not a MySQL checkpoint)"
                ),
            ));
        };
        let class = match cp.lineage.as_deref() {
            None => Class::Legacy,
            Some(l) if !is_canonical_lineage(l) => {
                return Err(refuse(
                    source_id,
                    format!("checkpoint {key} has a malformed lineage {l:?}"),
                ));
            }
            Some(l) if l == current_lineage => Class::Current,
            Some(l) if Some(l) == predecessor.as_deref() => Class::Predecessor,
            Some(l) => {
                return Err(refuse(
                    source_id,
                    format!(
                        "checkpoint {key} belongs to lineage {l}, which is neither \
                         the current lineage nor its immediate predecessor"
                    ),
                ));
            }
        };
        match class {
            Class::Current => {}
            Class::Legacy => {
                if lineage_changed {
                    return Err(refuse(
                        source_id,
                        format!(
                            "checkpoint {key} predates lineage tracking and the \
                             source's lineage has changed since, so the server it \
                             belongs to is unknown"
                        ),
                    ));
                }
                legacy += 1;
                to_rewrite.push((key, obj));
            }
            Class::Predecessor => {
                let Some(set) = cp.gtid_set.clone() else {
                    return Err(refuse(
                        source_id,
                        format!(
                            "checkpoint {key} is a binlog file/position on the \
                             previous server, which does not exist on the current one"
                        ),
                    ));
                };
                if crate::durable_checkpoint::mysql_checkpoint_position(
                    &cp.file,
                    cp.pos,
                    Some(&set),
                )
                .is_none_or(|p| {
                    crate::durable_checkpoint::order_positions(&p, &p)
                        != deltaforge_core::CheckpointOrder::Equal
                }) {
                    return Err(refuse(
                        source_id,
                        format!("checkpoint {key} has an unparseable GTID set"),
                    ));
                }
                carried += 1;
                gtid_sets.push(set);
                to_rewrite.push((key, obj));
            }
        }
    }
    if to_rewrite.is_empty() {
        return Ok(Reconciled::NothingToDo);
    }

    // 2. Validate before any rewrite (legacy and predecessor never coexist:
    //    legacy is refused once a lineage change exists).
    if carried > 0 {
        gtid.verify_executed(&gtid_sets).await.map_err(|reason| {
            refuse(source_id, format!("failover carry-over: {reason}"))
        })?;
    }

    // 3. Rewrite: insert only the lineage field.
    for (key, mut obj) in to_rewrite {
        obj.insert(
            "lineage".to_string(),
            serde_json::Value::String(current_lineage.to_string()),
        );
        let bytes = serde_json::to_vec(&obj).map_err(|e| other(&e))?;
        chkpt.put_raw(&key, &bytes).await.map_err(|e| other(&e))?;
    }
    if carried > 0 {
        info!(
            source_id,
            lineage = current_lineage,
            checkpoints = carried,
            "failover: GTID checkpoints of the previous server carried over to the current lineage"
        );
        Ok(Reconciled::CarriedOver(carried))
    } else {
        info!(
            source_id,
            lineage = current_lineage,
            checkpoints = legacy,
            "pre-lineage checkpoints adopted into the verified lineage"
        );
        Ok(Reconciled::Adopted(legacy))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use checkpoints::MemCheckpointStore;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
    use storage::MemoryStorageBackend;
    use storage::adapters::LineageDescriptor;

    const T: &str = "acme";
    const S: &str = "orders";
    const UUID_A: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
    const UUID_B: &str = "4f2a0b1c-71ca-11e1-9e33-c80aa9429562";
    const UUID_C: &str = "5a3b1c2d-71ca-11e1-9e33-c80aa9429562";
    const OTHER: &str = "0123456789abcdef0123456789abcdef";
    const GTID_A: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-9";

    /// A GTID check that answers as told and counts its calls.
    struct FakeGtid {
        ok: bool,
        calls: AtomicUsize,
    }

    impl FakeGtid {
        fn ok() -> Self {
            Self {
                ok: true,
                calls: AtomicUsize::new(0),
            }
        }
        fn unavailable() -> Self {
            Self {
                ok: false,
                calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl GtidAvailability for FakeGtid {
        async fn verify_executed(
            &self,
            _sets: &[String],
        ) -> Result<(), String> {
            self.calls.fetch_add(1, SeqCst);
            if self.ok {
                Ok(())
            } else {
                Err("GTID set was not executed by the current server".into())
            }
        }
    }

    fn cp(
        file: &str,
        pos: u64,
        gtid: Option<&str>,
        lineage: Option<&str>,
    ) -> Vec<u8> {
        serde_json::to_vec(&MySqlCheckpoint {
            file: file.into(),
            pos,
            gtid_set: gtid.map(str::to_string),
            lineage: lineage.map(str::to_string),
        })
        .unwrap()
    }

    /// Establish the given lineages in order; returns their hashes.
    async fn setup(
        uuids: &[&str],
    ) -> (Arc<MemCheckpointStore>, ArcStorageBackend, Vec<String>) {
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let mut hashes = Vec::new();
        for u in uuids {
            hashes.push(
                source_lineage::establish(
                    &backend,
                    T,
                    S,
                    LineageDescriptor::mysql(u).unwrap(),
                )
                .await
                .unwrap()
                .record
                .current
                .lineage_hash,
            );
        }
        (
            Arc::new(MemCheckpointStore::new().unwrap()),
            backend,
            hashes,
        )
    }

    async fn run(
        store: &dyn CheckpointStore,
        backend: &ArcStorageBackend,
        current: &str,
        gtid: &FakeGtid,
    ) -> SourceResult<Reconciled> {
        reconcile_checkpoint_lineage(store, backend, T, S, current, gtid).await
    }

    async fn snapshot(store: &MemCheckpointStore) -> Vec<(String, Vec<u8>)> {
        let mut out = Vec::new();
        for k in store.list().await.unwrap() {
            out.push((k.clone(), store.get_raw(&k).await.unwrap().unwrap()));
        }
        out.sort();
        out
    }

    async fn lineage_of(
        store: &dyn CheckpointStore,
        key: &str,
    ) -> Option<String> {
        let raw = store.get_raw(key).await.unwrap().unwrap();
        serde_json::from_slice::<MySqlCheckpoint>(&raw)
            .unwrap()
            .lineage
    }

    fn refused(r: SourceResult<Reconciled>, why: &str) {
        match r {
            Err(SourceError::Checkpoint { details }) => {
                assert!(details.contains(why), "{details}")
            }
            other => {
                panic!("expected a checkpoint refusal ({why}), got {other:?}")
            }
        }
    }

    // ---- no lineage change: adoption --------------------------------------

    #[tokio::test]
    async fn legacy_checkpoints_of_an_unchanged_lineage_are_adopted_once() {
        let (store, backend, h) = setup(&[UUID_A]).await;
        let l = &h[0];
        store
            .put_raw("orders::sink::kafka", &cp("bin.000002", 9, None, None))
            .await
            .unwrap();
        store
            .put_raw("orders::sink::s3", &cp("bin.000001", 4, None, None))
            .await
            .unwrap();
        store
            .put_raw(
                "orders-archive::sink::kafka",
                &cp("bin.000001", 4, None, None),
            )
            .await
            .unwrap();
        let g = FakeGtid::ok();
        assert_eq!(
            run(store.as_ref(), &backend, l, &g).await.unwrap(),
            Reconciled::Adopted(2)
        );
        assert_eq!(g.calls.load(SeqCst), 0);
        for k in ["orders::sink::kafka", "orders::sink::s3"] {
            assert_eq!(
                lineage_of(store.as_ref(), k).await.as_deref(),
                Some(l.as_str())
            );
        }
        assert_eq!(
            lineage_of(store.as_ref(), "orders-archive::sink::kafka").await,
            None
        );
        let a = store.get_raw("orders::sink::s3").await.unwrap().unwrap();
        let b = store.get_raw("orders::sink::kafka").await.unwrap().unwrap();
        assert_eq!(
            super::super::compare_mysql_checkpoints(&a, &b),
            deltaforge_core::CheckpointOrder::Before
        );
        assert_eq!(
            run(store.as_ref(), &backend, l, &g).await.unwrap(),
            Reconciled::NothingToDo
        );
    }

    #[tokio::test]
    async fn matching_lineage_and_no_checkpoint_need_nothing() {
        let (store, backend, h) = setup(&[UUID_A]).await;
        let g = FakeGtid::ok();
        assert_eq!(
            run(store.as_ref(), &backend, &h[0], &g).await.unwrap(),
            Reconciled::NothingToDo
        );
        store
            .put_raw(
                "orders::sink::kafka",
                &cp("bin.000002", 9, None, Some(&h[0])),
            )
            .await
            .unwrap();
        assert_eq!(
            run(store.as_ref(), &backend, &h[0], &g).await.unwrap(),
            Reconciled::NothingToDo
        );
    }

    #[tokio::test]
    async fn a_partially_adopted_set_is_completed() {
        let (store, backend, h) = setup(&[UUID_A]).await;
        let l = &h[0];
        store
            .put_raw("orders::sink::kafka", &cp("bin.000002", 9, None, Some(l)))
            .await
            .unwrap();
        store
            .put_raw("orders::sink::s3", &cp("bin.000001", 4, None, None))
            .await
            .unwrap();
        assert_eq!(
            run(store.as_ref(), &backend, l, &FakeGtid::ok())
                .await
                .unwrap(),
            Reconciled::Adopted(1)
        );
        assert_eq!(
            lineage_of(store.as_ref(), "orders::sink::s3")
                .await
                .as_deref(),
            Some(l.as_str())
        );
    }

    #[tokio::test]
    async fn the_aggregate_key_is_adopted_and_control_records_are_untouched() {
        let (store, backend, h) = setup(&[UUID_A]).await;
        store
            .put_raw(S, &cp("bin.000001", 4, None, None))
            .await
            .unwrap();
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
            run(store.as_ref(), &backend, &h[0], &FakeGtid::ok())
                .await
                .unwrap(),
            Reconciled::Adopted(1)
        );
        assert_eq!(
            lineage_of(store.as_ref(), S).await.as_deref(),
            Some(h[0].as_str())
        );
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

    #[tokio::test]
    async fn adoption_preserves_every_position_field() {
        let (store, backend, h) = setup(&[UUID_A]).await;
        let empty_gtid = br#"{"file":"bin.000003","pos":4,"gtid_set":""}"#;
        let gtid = br#"{"file":"bin.000004","pos":157,"gtid_set":"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-9"}"#;
        store.put_raw("orders::sink::a", empty_gtid).await.unwrap();
        store.put_raw("orders::sink::b", gtid).await.unwrap();
        run(store.as_ref(), &backend, &h[0], &FakeGtid::ok())
            .await
            .unwrap();
        for (key, before) in [
            ("orders::sink::a", &empty_gtid[..]),
            ("orders::sink::b", &gtid[..]),
        ] {
            let mut want: serde_json::Value =
                serde_json::from_slice(before).unwrap();
            want["lineage"] = serde_json::Value::String(h[0].clone());
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

    // ---- failover: carry-over of the immediate predecessor ---------------

    #[tokio::test]
    async fn predecessor_gtid_checkpoints_are_carried_over_after_verification()
    {
        let (store, backend, h) = setup(&[UUID_A, UUID_B]).await;
        let (a, b) = (&h[0], &h[1]);
        store
            .put_raw(
                "orders::sink::kafka",
                &cp("bin.000009", 900, Some(GTID_A), Some(a)),
            )
            .await
            .unwrap();
        store
            .put_raw(
                "orders::sink::s3",
                &cp("bin.000009", 500, Some(GTID_A), Some(a)),
            )
            .await
            .unwrap();
        let g = FakeGtid::ok();
        assert_eq!(
            run(store.as_ref(), &backend, b, &g).await.unwrap(),
            Reconciled::CarriedOver(2)
        );
        assert_eq!(
            g.calls.load(SeqCst),
            1,
            "verified once, before any rewrite"
        );
        for k in ["orders::sink::kafka", "orders::sink::s3"] {
            assert_eq!(
                lineage_of(store.as_ref(), k).await.as_deref(),
                Some(b.as_str())
            );
        }
        assert_eq!(
            run(store.as_ref(), &backend, b, &g).await.unwrap(),
            Reconciled::NothingToDo
        );
    }

    #[tokio::test]
    async fn unavailable_predecessor_gtids_refuse_and_rewrite_nothing() {
        let (store, backend, h) = setup(&[UUID_A, UUID_B]).await;
        store
            .put_raw(
                "orders::sink::kafka",
                &cp("bin.000009", 900, Some(GTID_A), Some(&h[0])),
            )
            .await
            .unwrap();
        store
            .put_raw(
                "orders::sink::s3",
                &cp("bin.000009", 500, Some(GTID_A), Some(&h[1])),
            )
            .await
            .unwrap();
        let before = snapshot(&store).await;
        refused(
            run(store.as_ref(), &backend, &h[1], &FakeGtid::unavailable())
                .await,
            "not executed",
        );
        assert_eq!(snapshot(&store).await, before);
    }

    #[tokio::test]
    async fn a_predecessor_file_position_checkpoint_is_fatal() {
        let (store, backend, h) = setup(&[UUID_A, UUID_B]).await;
        store
            .put_raw(
                "orders::sink::kafka",
                &cp("bin.000009", 900, None, Some(&h[0])),
            )
            .await
            .unwrap();
        let before = snapshot(&store).await;
        let g = FakeGtid::ok();
        refused(
            run(store.as_ref(), &backend, &h[1], &g).await,
            "file/position",
        );
        assert_eq!(g.calls.load(SeqCst), 0);
        assert_eq!(snapshot(&store).await, before);
    }

    #[tokio::test]
    async fn an_unparseable_predecessor_gtid_set_is_fatal() {
        let (store, backend, h) = setup(&[UUID_A, UUID_B]).await;
        store
            .put_raw(
                "orders::sink::kafka",
                &cp("bin.000009", 900, Some("nonsense"), Some(&h[0])),
            )
            .await
            .unwrap();
        let before = snapshot(&store).await;
        refused(
            run(store.as_ref(), &backend, &h[1], &FakeGtid::ok()).await,
            "unparseable GTID",
        );
        assert_eq!(snapshot(&store).await, before);
    }

    /// Carry-over is not transitive: A -> B -> C makes A's checkpoints foreign.
    #[tokio::test]
    async fn an_older_predecessor_is_fatal() {
        let (store, backend, h) = setup(&[UUID_A, UUID_B, UUID_C]).await;
        store
            .put_raw(
                "orders::sink::kafka",
                &cp("bin.000009", 900, Some(GTID_A), Some(&h[0])),
            )
            .await
            .unwrap();
        let before = snapshot(&store).await;
        refused(
            run(store.as_ref(), &backend, &h[2], &FakeGtid::ok()).await,
            "neither",
        );
        assert_eq!(snapshot(&store).await, before);
    }

    /// A crash part-way through a carry-over leaves A and B checkpoints
    /// mixed; the next start recognises the same durable A -> B edge and
    /// finishes.
    #[tokio::test]
    async fn a_restart_after_a_crash_mid_carry_over_finishes_it() {
        let (_, backend, h) = setup(&[UUID_A, UUID_B]).await;
        let (a, b) = (&h[0], &h[1]);
        let store = CrashingStore {
            inner: MemCheckpointStore::new().unwrap(),
            writes_left: AtomicUsize::new(usize::MAX),
        };
        let originals = [
            (
                "orders::sink::a",
                cp("bin.000009", 100, Some(GTID_A), Some(a)),
            ),
            (
                "orders::sink::b",
                cp("bin.000009", 200, Some(GTID_A), Some(a)),
            ),
            (
                "orders::sink::c",
                cp("bin.000009", 300, Some(GTID_A), Some(a)),
            ),
        ];
        for (k, v) in &originals {
            store.put_raw(k, v).await.unwrap();
        }
        store.writes_left.store(1, SeqCst);
        assert!(run(&store, &backend, b, &FakeGtid::ok()).await.is_err());
        let mut on_b = 0;
        for (k, _) in &originals {
            if lineage_of(&store, k).await.as_deref() == Some(b.as_str()) {
                on_b += 1;
            }
        }
        assert_eq!(on_b, 1, "mixed A/B after the crash");
        store.writes_left.store(usize::MAX, SeqCst);
        assert_eq!(
            run(&store, &backend, b, &FakeGtid::ok()).await.unwrap(),
            Reconciled::CarriedOver(2)
        );
        for (k, v) in &originals {
            let got: MySqlCheckpoint = serde_json::from_slice(
                &store.get_raw(k).await.unwrap().unwrap(),
            )
            .unwrap();
            let want: MySqlCheckpoint = serde_json::from_slice(v).unwrap();
            assert_eq!(got.lineage.as_deref(), Some(b.as_str()));
            assert_eq!(
                (got.file, got.pos, got.gtid_set),
                (want.file, want.pos, want.gtid_set)
            );
        }
    }

    // ---- refusals: nothing rewritten ---------------------------------------

    #[tokio::test]
    async fn legacy_checkpoints_after_a_lineage_change_are_fatal() {
        for keys in [
            &["orders"][..],
            &["orders::sink::kafka", "orders::sink::s3"][..],
        ] {
            let (store, backend, h) = setup(&[UUID_A, UUID_B]).await;
            for (i, k) in keys.iter().enumerate() {
                store
                    .put_raw(k, &cp("bin.000002", 9 + i as u64, None, None))
                    .await
                    .unwrap();
            }
            let before = snapshot(&store).await;
            refused(
                run(store.as_ref(), &backend, &h[1], &FakeGtid::ok()).await,
                "predates lineage",
            );
            assert_eq!(snapshot(&store).await, before, "{keys:?}");
        }
    }

    #[tokio::test]
    async fn a_single_foreign_lineage_checkpoint_is_fatal() {
        let (store, backend, h) = setup(&[UUID_A]).await;
        store
            .put_raw("orders", &cp("bin.000002", 9, None, Some(OTHER)))
            .await
            .unwrap();
        let before = snapshot(&store).await;
        refused(
            run(store.as_ref(), &backend, &h[0], &FakeGtid::ok()).await,
            "neither",
        );
        assert_eq!(snapshot(&store).await, before);
    }

    #[tokio::test]
    async fn malformed_checkpoints_or_lineages_are_fatal() {
        for bytes in [
            b"{garbage".to_vec(),
            cp("bin.000002", 9, None, Some("NOT-CANONICAL")),
        ] {
            let (store, backend, h) = setup(&[UUID_A]).await;
            store
                .put_raw("orders::sink::a", &cp("bin.000001", 4, None, None))
                .await
                .unwrap();
            store.put_raw("orders::sink::b", &bytes).await.unwrap();
            let before = snapshot(&store).await;
            refused(
                run(store.as_ref(), &backend, &h[0], &FakeGtid::ok()).await,
                "malformed",
            );
            assert_eq!(snapshot(&store).await, before);
        }
    }

    #[tokio::test]
    async fn a_verified_lineage_other_than_the_record_is_fatal() {
        let (store, backend, _) = setup(&[UUID_A]).await;
        store
            .put_raw("orders::sink::s3", &cp("bin.000001", 4, None, None))
            .await
            .unwrap();
        let before = snapshot(&store).await;
        refused(
            run(store.as_ref(), &backend, OTHER, &FakeGtid::ok()).await,
            "durable lineage record",
        );
        assert_eq!(snapshot(&store).await, before);
    }

    /// A store whose writes fail after a budget: a crash part-way through.
    struct CrashingStore {
        inner: MemCheckpointStore,
        writes_left: AtomicUsize,
    }

    #[async_trait]
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

    #[tokio::test]
    async fn a_restart_after_a_crash_mid_adoption_completes_it() {
        let (_, backend, h) = setup(&[UUID_A]).await;
        let l = &h[0];
        let store = CrashingStore {
            inner: MemCheckpointStore::new().unwrap(),
            writes_left: AtomicUsize::new(usize::MAX),
        };
        let originals = [
            ("orders::sink::a", cp("bin.000001", 4, None, None)),
            ("orders::sink::b", cp("bin.000002", 9, None, None)),
            ("orders::sink::c", cp("bin.000003", 1, None, None)),
        ];
        for (k, v) in &originals {
            store.put_raw(k, v).await.unwrap();
        }
        store.writes_left.store(1, SeqCst);
        assert!(run(&store, &backend, l, &FakeGtid::ok()).await.is_err());
        store.writes_left.store(usize::MAX, SeqCst);
        assert_eq!(
            run(&store, &backend, l, &FakeGtid::ok()).await.unwrap(),
            Reconciled::Adopted(2)
        );
        for (k, v) in &originals {
            let got: MySqlCheckpoint = serde_json::from_slice(
                &store.get_raw(k).await.unwrap().unwrap(),
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

//! Crash-boundary fault-injection matrix for the durable S3 ack path (P0.4).
//!
//! Deterministic, operation-counted failpoints (never timing sleeps) at every
//! boundary of the publish/recover sequence, asserting the durability contract:
//! acknowledgement is returned ONLY after the HEAD compare-and-swap; anything
//! before it leaves at most harmless orphans (unreferenced data/manifest objects)
//! and never advances HEAD; retries are idempotent (content-addressed, no
//! conflicting durable content); a lost response reconciles to success ONLY when
//! HEAD selects the exact entry; and a fenced writer stays fenced.
//!
//! Both returned errors AND abrupt task cancellation (future dropped mid-op) are
//! exercised, because cleanup runs on ordinary errors but not on process death.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::path::Path;
use tokio::sync::{Mutex, Notify};

use deltaforge_core::{CheckpointComparator, CheckpointOrder};

use super::batch_upload::TableObject;
use super::compaction::{
    CompactionRecord, EQUIVALENCE_ALGO, EQUIVALENCE_VERSION,
    write_record as write_compaction_record,
};
use super::gc::{
    GC_MARK_VERSION, GcConfig, GcMark, entry_seq_from_key, mark_key,
};
use super::head::{DurableWriter, Head, HeadError, head_key_for};
use super::keys::EncodingDomain;
use super::manifest::{ManifestEntry, ManifestObject};
use super::rollup::{
    INVENTORY_INDEX_VERSION, InventoryIndex, ROLLUP_RECORD_VERSION,
    RollupRecord, load_inventory, load_record as load_rollup_rec,
    object_digest, write_inventory, write_record,
};
use super::store_cond::{
    CondError, CondResult, ConditionalStore, ObjectStoreConditional, PutOutcome,
};

const PFX: &str = "pfx";
const PIPE: &str = "pipe";

// ── Fault classification + actions ──────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
enum Op {
    /// Create-only write of a per-table data object.
    DataPut,
    /// Create-only write of a manifest entry.
    ManifestPut,
    /// Create-only write of the genesis HEAD.
    HeadPut,
    /// Compare-and-swap of HEAD.
    HeadCas,
    /// Create-only write of a compacted data object.
    CompactedPut,
    /// Create-only write of a compaction record.
    CompactionRecordPut,
    /// Create-only write of a rollup record.
    RollupRecordPut,
    /// Create-only write of a rollup inventory index object.
    InventoryPut,
    /// Read of an original data object (compaction "read originals" step).
    GetData,
    /// Create-only write of a manifest-entry GC eligibility mark (9B.2).
    MarkPut,
    /// Delete of a manifest entry (9B.2 expiry).
    EntryDelete,
    /// Delete of a compacted original data object (9B.2 data GC).
    OriginalDelete,
    /// A `list_meta` call during reconciliation (Commit 10).
    ListObjects,
}

fn classify_put(key: &Path) -> Op {
    let k = key.to_string();
    if k.contains("_manifest/HEAD") {
        Op::HeadPut
    } else if k.contains("_manifest/gc") {
        Op::MarkPut
    } else if k.contains("_manifest/entries") {
        Op::ManifestPut
    } else if k.contains("_manifest/compactions") {
        Op::CompactionRecordPut
    } else if k.contains("_manifest/rollups/inventory") {
        Op::InventoryPut
    } else if k.contains("_manifest/rollups") {
        Op::RollupRecordPut
    } else if k.contains("/compacted/") {
        Op::CompactedPut
    } else {
        Op::DataPut
    }
}

/// Deletes of a manifest entry (expiry) or a compacted original (data GC) are
/// failpoint-able in 9B.2; other deletes pass.
fn classify_delete(key: &Path) -> Option<Op> {
    let k = key.to_string();
    if k.contains("_manifest/entries") {
        Some(Op::EntryDelete)
    } else if k.contains("/wm-") && !k.contains("/compacted/") {
        Some(Op::OriginalDelete)
    } else {
        None
    }
}

/// A data-object read is the compaction "read originals" step; other reads pass.
fn classify_get(key: &Path) -> Option<Op> {
    let k = key.to_string();
    if k.contains("/wm-") && !k.contains("/compacted/") {
        Some(Op::GetData)
    } else {
        None
    }
}

#[derive(Clone, Copy, Debug)]
enum Action {
    /// Return an error WITHOUT applying (op never reaches the store).
    ErrBefore,
    /// Apply to the store, THEN return an error (lost/ambiguous response).
    ApplyThenErr,
    /// Hang WITHOUT applying (models process death before the op).
    HangBefore,
    /// Apply, THEN hang (models process death after the op, before the caller
    /// observes success).
    HangAfterApply,
}

#[derive(Clone, Copy, Debug)]
struct Trigger {
    op: Op,
    /// 0-based occurrence of `op` at which to fire.
    nth: usize,
    action: Action,
}

/// A `ConditionalStore` decorator with deterministic, operation-counted
/// failpoints over an inner `ObjectStoreConditional`.
struct FaultStore {
    inner: Arc<ObjectStoreConditional>,
    triggers: Vec<Trigger>,
    counts: Mutex<HashMap<Op, usize>>,
    /// Fired when a hang action begins, so a test can observe the boundary and
    /// then drop the in-flight future (abrupt cancellation).
    hang_started: Arc<Notify>,
}

impl FaultStore {
    fn new(inner: Arc<ObjectStoreConditional>, triggers: Vec<Trigger>) -> Self {
        Self {
            inner,
            triggers,
            counts: Mutex::new(HashMap::new()),
            hang_started: Arc::new(Notify::new()),
        }
    }

    async fn fire(&self, op: Op) -> Option<Action> {
        let mut c = self.counts.lock().await;
        let n = c.entry(op).or_insert(0);
        let cur = *n;
        *n += 1;
        self.triggers
            .iter()
            .find(|t| t.op == op && t.nth == cur)
            .map(|t| t.action)
    }

    async fn hang(&self) -> ! {
        self.hang_started.notify_one();
        std::future::pending::<()>().await;
        unreachable!()
    }
}

#[async_trait]
impl ConditionalStore for FaultStore {
    async fn put_if_absent(
        &self,
        key: &Path,
        bytes: Bytes,
    ) -> CondResult<PutOutcome> {
        let op = classify_put(key);
        match self.fire(op).await {
            Some(Action::ErrBefore) => {
                Err(CondError::Store(format!("injected err before {op:?}")))
            }
            Some(Action::ApplyThenErr) => {
                let _ = self.inner.put_if_absent(key, bytes).await?;
                Err(CondError::Store(format!("injected lost {op:?} response")))
            }
            Some(Action::HangBefore) => self.hang().await,
            Some(Action::HangAfterApply) => {
                let _ = self.inner.put_if_absent(key, bytes).await?;
                self.hang().await
            }
            None => self.inner.put_if_absent(key, bytes).await,
        }
    }

    async fn cas_put(
        &self,
        key: &Path,
        bytes: Bytes,
        expected: &str,
    ) -> CondResult<PutOutcome> {
        match self.fire(Op::HeadCas).await {
            Some(Action::ErrBefore) => {
                Err(CondError::Store("injected err before HeadCas".into()))
            }
            Some(Action::ApplyThenErr) => {
                let _ = self.inner.cas_put(key, bytes, expected).await?;
                Err(CondError::Store("injected lost HeadCas response".into()))
            }
            Some(Action::HangBefore) => self.hang().await,
            Some(Action::HangAfterApply) => {
                let _ = self.inner.cas_put(key, bytes, expected).await?;
                self.hang().await
            }
            None => self.inner.cas_put(key, bytes, expected).await,
        }
    }

    async fn get_with_etag(
        &self,
        key: &Path,
    ) -> CondResult<Option<(Bytes, Option<String>)>> {
        if let Some(op) = classify_get(key) {
            if let Some(Action::ErrBefore) = self.fire(op).await {
                return Err(CondError::Store(format!(
                    "injected err reading {op:?}"
                )));
            }
        }
        self.inner.get_with_etag(key).await
    }
    async fn delete(&self, key: &Path) -> CondResult<()> {
        if let Some(op) = classify_delete(key) {
            match self.fire(op).await {
                Some(Action::ErrBefore) => {
                    return Err(CondError::Store(format!(
                        "injected err before {op:?}"
                    )));
                }
                Some(Action::ApplyThenErr) => {
                    self.inner.delete(key).await?;
                    return Err(CondError::Store(format!(
                        "injected lost {op:?} response"
                    )));
                }
                Some(Action::HangBefore) => self.hang().await,
                Some(Action::HangAfterApply) => {
                    self.inner.delete(key).await?;
                    self.hang().await
                }
                None => {}
            }
        }
        self.inner.delete(key).await
    }
    async fn list(&self, prefix: &Path) -> CondResult<Vec<Path>> {
        self.inner.list(prefix).await
    }
    async fn list_meta(
        &self,
        prefix: &Path,
    ) -> CondResult<Vec<super::store_cond::ObjectListing>> {
        // A listing failpoint models provider "listing uncertainty".
        if let Some(Action::ErrBefore) = self.fire(Op::ListObjects).await {
            return Err(CondError::Store(
                "injected listing uncertainty".into(),
            ));
        }
        self.inner.list_meta(prefix).await
    }
}

// ── Comparator + fixtures ───────────────────────────────────────────────────

/// Watermark = `[lineage, 8-byte BE pos]`; different lineage is Incomparable.
struct MonoCmp;
impl CheckpointComparator for MonoCmp {
    fn order(&self, a: &[u8], b: &[u8]) -> CheckpointOrder {
        fn parse(x: &[u8]) -> Option<(u8, u64)> {
            if x.len() != 9 {
                return None;
            }
            let mut p = [0u8; 8];
            p.copy_from_slice(&x[1..9]);
            Some((x[0], u64::from_be_bytes(p)))
        }
        match (parse(a), parse(b)) {
            (Some((la, xa)), Some((lb, xb))) if la == lb => {
                use std::cmp::Ordering::*;
                match xa.cmp(&xb) {
                    Less => CheckpointOrder::Before,
                    Equal => CheckpointOrder::Equal,
                    Greater => CheckpointOrder::After,
                }
            }
            _ => CheckpointOrder::Incomparable,
        }
    }
}

fn cmp() -> Arc<dyn CheckpointComparator> {
    Arc::new(MonoCmp)
}

fn wm(pos: u64) -> Vec<u8> {
    let mut v = vec![0u8];
    v.extend_from_slice(&pos.to_be_bytes());
    v
}

fn tobj(table: &str, bytes: &'static [u8]) -> TableObject {
    TableObject {
        table: table.to_string(),
        bytes: Bytes::from_static(bytes),
        domain: EncodingDomain::new("jsonl", 1, "s1", "none", "table", 1),
        ext: "jsonl",
    }
}

fn inmem() -> Arc<ObjectStoreConditional> {
    Arc::new(ObjectStoreConditional::new(Arc::new(InMemory::new())))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Extract the error without requiring the Ok type to be `Debug`.
fn expect_err<T>(r: Result<T, HeadError>) -> HeadError {
    match r {
        Err(e) => e,
        Ok(_) => panic!("expected Err"),
    }
}

/// Read the authoritative HEAD straight from the store (no live writer).
async fn read_head(inner: &ObjectStoreConditional) -> Option<Head> {
    inner
        .get_with_etag(&head_key_for(PFX, PIPE))
        .await
        .unwrap()
        .map(|(raw, _)| serde_json::from_slice(&raw).unwrap())
}

async fn entry_count(inner: &ObjectStoreConditional) -> usize {
    inner
        .list(&Path::from(format!("{PFX}/{PIPE}/_manifest/entries")))
        .await
        .unwrap()
        .len()
}

/// Count durable data objects (keys under the pipeline that are not manifest
/// state) - i.e. the per-table content objects, referenced or orphan.
async fn data_count(inner: &ObjectStoreConditional) -> usize {
    inner
        .list(&Path::from(format!("{PFX}/{PIPE}")))
        .await
        .unwrap()
        .iter()
        .filter(|p| p.to_string().contains("/wm-"))
        .count()
}

async fn acquire(
    store: Arc<FaultStore>,
) -> Result<DurableWriter<FaultStore>, HeadError> {
    DurableWriter::acquire(store, PFX, PIPE, "src", "sink", cmp()).await
}

/// A genesis writer over a fault store with the given triggers, sharing `inner`.
async fn genesis(
    inner: &Arc<ObjectStoreConditional>,
    triggers: Vec<Trigger>,
) -> (Arc<FaultStore>, DurableWriter<FaultStore>) {
    let fs = Arc::new(FaultStore::new(Arc::clone(inner), triggers));
    let w = acquire(Arc::clone(&fs)).await.unwrap();
    (fs, w)
}

/// Poll `publish` until its fault store starts a hang, then drop it (abrupt
/// cancellation at that boundary). Returns after the boundary is reached.
async fn cancel_at_hang(
    store: &FaultStore,
    fut: impl std::future::Future<Output = Result<(), HeadError>>,
) {
    tokio::pin!(fut);
    tokio::select! {
        r = &mut fut => panic!("expected a hang, publish returned {r:?}"),
        _ = store.hang_started.notified() => {}
    }
    // `fut` is dropped here: no further await steps run (process-death model).
}

// ── Boundary matrix (returned-error failpoints) ─────────────────────────────

#[tokio::test]
async fn before_first_data_write_denies_ack_then_retry_idempotent() {
    let inner = inmem();
    let (_fs, w) =
        genesis(&inner, vec![tr(Op::DataPut, 0, Action::ErrBefore)]).await;

    // First attempt fails before writing any data object.
    assert!(
        w.publish(&wm(1), vec![tobj("orders", b"a")], 1)
            .await
            .is_err()
    );
    assert_eq!(data_count(&inner).await, 0, "no data object written");
    assert_eq!(entry_count(&inner).await, 0);
    assert_eq!(
        read_head(&inner).await.unwrap().seq,
        0,
        "HEAD still genesis"
    );

    // Retry (trigger is one-shot) acknowledges; exactly one entry + object.
    w.publish(&wm(1), vec![tobj("orders", b"a")], 1)
        .await
        .unwrap();
    assert_eq!(read_head(&inner).await.unwrap().seq, 1);
    assert_eq!(entry_count(&inner).await, 1);
    assert_eq!(data_count(&inner).await, 1);
}

#[tokio::test]
async fn between_table_writes_leaves_harmless_orphan_then_reconciles() {
    let inner = inmem();
    // Fail the SECOND data-object write in a two-table batch.
    let (_fs, w) =
        genesis(&inner, vec![tr(Op::DataPut, 1, Action::ErrBefore)]).await;

    let batch = vec![tobj("orders", b"a"), tobj("users", b"b")];
    assert!(w.publish(&wm(1), batch, 1).await.is_err());
    assert_eq!(data_count(&inner).await, 1, "first object is an orphan");
    assert_eq!(entry_count(&inner).await, 0, "no entry, no ack");
    assert_eq!(read_head(&inner).await.unwrap().seq, 0);

    // Retry: the first object's create-only put is idempotent; both objects end
    // up durable and referenced by exactly one entry.
    let batch = vec![tobj("orders", b"a"), tobj("users", b"b")];
    w.publish(&wm(1), batch, 2).await.unwrap();
    assert_eq!(data_count(&inner).await, 2);
    assert_eq!(entry_count(&inner).await, 1);
    assert_eq!(read_head(&inner).await.unwrap().seq, 1);
}

#[tokio::test]
async fn after_data_before_manifest_denies_ack_orphans_harmless() {
    let inner = inmem();
    let (_fs, w) =
        genesis(&inner, vec![tr(Op::ManifestPut, 0, Action::ErrBefore)]).await;

    assert!(
        w.publish(&wm(1), vec![tobj("orders", b"a")], 1)
            .await
            .is_err()
    );
    assert_eq!(data_count(&inner).await, 1, "data object is an orphan");
    assert_eq!(entry_count(&inner).await, 0);
    assert_eq!(read_head(&inner).await.unwrap().seq, 0);

    w.publish(&wm(1), vec![tobj("orders", b"a")], 1)
        .await
        .unwrap();
    assert_eq!(entry_count(&inner).await, 1);
    assert_eq!(read_head(&inner).await.unwrap().seq, 1);
}

#[tokio::test]
async fn manifest_lost_response_reconciles_without_duplicate_entry() {
    let inner = inmem();
    // The manifest entry is written but the response is lost.
    let (_fs, w) =
        genesis(&inner, vec![tr(Op::ManifestPut, 0, Action::ApplyThenErr)])
            .await;

    // First attempt returns Err (lost response), but the entry landed.
    assert!(
        w.publish(&wm(1), vec![tobj("orders", b"a")], 1)
            .await
            .is_err()
    );
    assert_eq!(
        entry_count(&inner).await,
        1,
        "entry written (orphan for now)"
    );
    assert_eq!(
        read_head(&inner).await.unwrap().seq,
        0,
        "not acked, no HEAD"
    );

    // Retry: create-only write_entry sees AlreadyExists (byte-identical) and
    // reconciles - still exactly one entry, now referenced by HEAD.
    w.publish(&wm(1), vec![tobj("orders", b"a")], 1)
        .await
        .unwrap();
    assert_eq!(
        entry_count(&inner).await,
        1,
        "no duplicate/conflicting entry"
    );
    assert_eq!(read_head(&inner).await.unwrap().seq, 1);
}

#[tokio::test]
async fn head_cas_rejected_reconciles_within_publish() {
    let inner = inmem();
    // The first HEAD CAS errors before applying; publish's reconcile loop retries.
    let (_fs, w) =
        genesis(&inner, vec![tr(Op::HeadCas, 0, Action::ErrBefore)]).await;

    // Data + manifest were written before the failed CAS; the reconcile retry
    // reuses the same entry and acknowledges. One entry, HEAD at seq 1.
    w.publish(&wm(1), vec![tobj("orders", b"a")], 1)
        .await
        .unwrap();
    assert_eq!(entry_count(&inner).await, 1);
    assert_eq!(read_head(&inner).await.unwrap().seq, 1);
}

#[tokio::test]
async fn head_cas_lost_response_acks_because_head_references_entry() {
    let inner = inmem();
    let (_fs, w) =
        genesis(&inner, vec![tr(Op::HeadCas, 0, Action::ApplyThenErr)]).await;

    // The CAS applied (HEAD moved); the lost response reconciles to success
    // ONLY because HEAD now references our exact entry.
    w.publish(&wm(5), vec![tobj("orders", b"a")], 1)
        .await
        .unwrap();
    let h = read_head(&inner).await.unwrap();
    assert_eq!(h.seq, 1);
    assert!(h.head_entry_key.is_some());
}

// ── Abrupt cancellation (process death), not returned errors ────────────────

#[tokio::test]
async fn head_cas_applied_then_crash_before_return_is_replayable() {
    let inner = inmem();
    let (fs, w) =
        genesis(&inner, vec![tr(Op::HeadCas, 0, Action::HangAfterApply)]).await;

    // The CAS applies (HEAD -> seq 1, pos 5), then the process "dies" before the
    // sink returns: the batch is durable but NEVER acknowledged.
    cancel_at_hang(&fs, w.publish(&wm(5), vec![tobj("orders", b"a")], 1)).await;
    drop(w);
    let h = read_head(&inner).await.unwrap();
    assert_eq!(h.seq, 1, "HEAD moved (data is durable)");
    assert_eq!(entry_count(&inner).await, 1);

    // Restart: a new epoch acquires and the source replays the same (un-acked)
    // watermark. It is Equal to HEAD -> acknowledged without moving HEAD or
    // writing a new entry. No data loss, no duplicate.
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let w2 = acquire(Arc::clone(&clean)).await.unwrap();
    assert_eq!(w2.epoch().await, 2);
    w2.publish(&wm(5), vec![tobj("orders", b"a")], 1)
        .await
        .unwrap();
    assert_eq!(read_head(&inner).await.unwrap().seq, 1, "HEAD unchanged");
    assert_eq!(entry_count(&inner).await, 1, "no duplicate entry on replay");
}

#[tokio::test]
async fn crash_before_manifest_via_cancellation_leaves_only_orphans() {
    let inner = inmem();
    let (fs, w) =
        genesis(&inner, vec![tr(Op::ManifestPut, 0, Action::HangBefore)]).await;

    // Data uploaded, then death before the manifest write: no entry, no HEAD.
    cancel_at_hang(&fs, w.publish(&wm(1), vec![tobj("orders", b"a")], 1)).await;
    drop(w);
    assert_eq!(data_count(&inner).await, 1, "orphan data object");
    assert_eq!(entry_count(&inner).await, 0);
    assert_eq!(read_head(&inner).await.unwrap().seq, 0);

    // Restart recovers cleanly (HEAD present at genesis, orphan ignored) and can
    // publish, referencing the (idempotently re-used) object.
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let w2 = acquire(Arc::clone(&clean)).await.unwrap();
    w2.publish(&wm(1), vec![tobj("orders", b"a")], 1)
        .await
        .unwrap();
    assert_eq!(read_head(&inner).await.unwrap().seq, 1);
    assert_eq!(data_count(&inner).await, 1, "orphan reused, not duplicated");
}

// ── After-ack / coordinator-checkpoint boundary ─────────────────────────────

#[tokio::test]
async fn crash_after_ack_before_checkpoint_replays_idempotently() {
    let inner = inmem();
    let (_fs, w) = genesis(&inner, vec![]).await;
    w.publish(&wm(5), vec![tobj("orders", b"a")], 1)
        .await
        .unwrap();
    assert_eq!(read_head(&inner).await.unwrap().seq, 1);

    // Sink acked, but the coordinator "crashes" before committing its checkpoint.
    // On restart the source replays the same watermark: Equal -> ack, HEAD and
    // entry count unchanged.
    drop(w);
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let w2 = acquire(Arc::clone(&clean)).await.unwrap();
    w2.publish(&wm(5), vec![tobj("orders", b"a")], 1)
        .await
        .unwrap();
    assert_eq!(read_head(&inner).await.unwrap().seq, 1);
    assert_eq!(entry_count(&inner).await, 1);
}

// ── Zero-object completion publication ──────────────────────────────────────

#[tokio::test]
async fn zero_object_completion_advances_head_and_survives_cas_reject() {
    let inner = inmem();
    // Reject the first HEAD CAS of the zero-object publish; reconcile acks.
    let (_fs, w) =
        genesis(&inner, vec![tr(Op::HeadCas, 0, Action::ErrBefore)]).await;

    w.publish(&wm(9), vec![], 0).await.unwrap();
    assert_eq!(data_count(&inner).await, 0, "zero data objects");
    assert_eq!(entry_count(&inner).await, 1, "zero-object manifest entry");
    let h = read_head(&inner).await.unwrap();
    assert_eq!(h.seq, 1);
    assert_eq!(h.watermark_hex.as_deref(), Some(hex(&wm(9)).as_str()));
}

// ── Restart recovery with orphans ───────────────────────────────────────────

#[tokio::test]
async fn recovery_required_when_head_missing_but_entries_exist() {
    let inner = inmem();
    // Crash BEFORE the HEAD CAS so a manifest entry lands but HEAD stays genesis.
    let (fs, w) =
        genesis(&inner, vec![tr(Op::HeadCas, 0, Action::HangBefore)]).await;
    cancel_at_hang(&fs, w.publish(&wm(1), vec![tobj("orders", b"a")], 1)).await;
    drop(w);
    // Then delete the genesis HEAD to model "HEAD missing but entries exist".
    inner.delete(&head_key_for(PFX, PIPE)).await.unwrap();
    assert_eq!(entry_count(&inner).await, 1);
    assert!(read_head(&inner).await.is_none());

    // Acquire must refuse to empty-initialize: recovery is required (fatal).
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let err = expect_err(acquire(clean).await);
    assert!(matches!(err, HeadError::RecoveryRequired(_)));
    assert!(err.is_fatal());
}

#[tokio::test]
async fn recovery_with_head_present_ignores_orphans() {
    let inner = inmem();
    let (_fs, w) = genesis(&inner, vec![]).await;
    w.publish(&wm(5), vec![tobj("orders", b"a")], 1)
        .await
        .unwrap();

    // A prior failed attempt left an orphan data object (unreferenced by HEAD).
    inner
        .put_if_absent(
            &Path::from(format!(
                "{PFX}/{PIPE}/orders/wm-deadbeef/orphan.jsonl"
            )),
            Bytes::from_static(b"orphan"),
        )
        .await
        .unwrap();
    assert_eq!(data_count(&inner).await, 2);

    // Restart: acquire verifies HEAD's chain (ignoring the orphan) and advances
    // the epoch; a further publish still acks. The orphan remains, harmless.
    drop(w);
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let w2 = acquire(Arc::clone(&clean)).await.unwrap();
    assert_eq!(w2.epoch().await, 2);
    w2.publish(&wm(6), vec![tobj("orders", b"b")], 1)
        .await
        .unwrap();
    assert_eq!(read_head(&inner).await.unwrap().seq, 2);
    assert_eq!(
        data_count(&inner).await,
        3,
        "orphan still present, harmless"
    );
}

// ── Competing writers / fencing ─────────────────────────────────────────────

#[tokio::test]
async fn fenced_writer_publish_denied_orphans_harmless() {
    let inner = inmem();
    let a = acquire(Arc::new(FaultStore::new(Arc::clone(&inner), vec![])))
        .await
        .unwrap(); // epoch 1
    let _b = acquire(Arc::new(FaultStore::new(Arc::clone(&inner), vec![])))
        .await
        .unwrap(); // epoch 2 - fences A
    assert_eq!(a.epoch().await, 1);

    // A publishes: it uploads (orphans), writes an entry, then its HEAD CAS finds
    // a higher epoch and A is permanently fenced - never acknowledged.
    let err = a
        .publish(&wm(1), vec![tobj("orders", b"a")], 1)
        .await
        .expect_err("stale writer must be fenced");
    assert!(matches!(err, HeadError::Fenced { .. }));
    let h = read_head(&inner).await.unwrap();
    assert_eq!(h.epoch, 2, "HEAD is B's epoch");
    assert_eq!(h.seq, 0, "A never moved HEAD");

    // Permanently fenced: a second attempt is denied immediately.
    let err2 = a
        .publish(&wm(2), vec![tobj("orders", b"c")], 1)
        .await
        .expect_err("");
    assert!(matches!(err2, HeadError::Fenced { .. }));
}

#[tokio::test]
async fn fenced_after_object_creation_before_head_cas() {
    // A uploads its data object, THEN B acquires (fences A), THEN A proceeds to
    // manifest + HEAD CAS and is fenced. Coordinate via a hang on A's DataPut.
    let inner = inmem();
    let fs_a = Arc::new(FaultStore::new(
        Arc::clone(&inner),
        vec![tr(Op::DataPut, 0, Action::HangAfterApply)],
    ));
    let a = acquire(Arc::clone(&fs_a)).await.unwrap(); // epoch 1

    // Start A's publish; it uploads the data object then hangs.
    let fs_a_hang = Arc::clone(&fs_a);
    let a_ref = &a;
    let hang = async move {
        cancel_at_hang(
            &fs_a_hang,
            a_ref.publish(&wm(1), vec![tobj("orders", b"a")], 1),
        )
        .await;
    };
    hang.await;
    assert_eq!(data_count(&inner).await, 1, "A's object uploaded");

    // B acquires (fences A) and publishes successfully.
    let b = acquire(Arc::new(FaultStore::new(Arc::clone(&inner), vec![])))
        .await
        .unwrap();
    assert_eq!(b.epoch().await, 2);
    b.publish(&wm(3), vec![tobj("orders", b"b")], 1)
        .await
        .unwrap();

    // A resumes (fresh publish call): its HEAD CAS now conflicts with B's epoch
    // and A is fenced. B's HEAD stands.
    let err = a
        .publish(&wm(1), vec![tobj("orders", b"a")], 1)
        .await
        .expect_err("A fenced");
    assert!(matches!(err, HeadError::Fenced { .. }));
    let h = read_head(&inner).await.unwrap();
    assert_eq!(h.epoch, 2);
    assert_eq!(
        h.watermark_hex.as_deref(),
        Some(hex(&wm(3)).as_str()),
        "B's watermark is authoritative"
    );
}

fn tr(op: Op, nth: usize, action: Action) -> Trigger {
    Trigger { op, nth, action }
}

// ── Compaction crash matrix ─────────────────────────────────────────────────
//
// Setup publishes two batches for one table (consuming DataPut 0/1, ManifestPut
// 0/1, HeadCas 0/1), then compacts them. Compaction-only ops (CompactedPut,
// CompactionRecordPut, GetData) start at nth 0 during compact; the compact HEAD
// CAS is HeadCas nth 2 (after the two publishes).

async fn all_manifest_objects(
    inner: &ObjectStoreConditional,
) -> Vec<ManifestObject> {
    let mut out = Vec::new();
    for k in inner
        .list(&Path::from(format!("{PFX}/{PIPE}/_manifest/entries")))
        .await
        .unwrap()
    {
        let (raw, _) = inner.get_with_etag(&k).await.unwrap().unwrap();
        let e: ManifestEntry = serde_json::from_slice(&raw).unwrap();
        out.extend(e.objects);
    }
    out
}

async fn compaction_record_count(inner: &ObjectStoreConditional) -> usize {
    inner
        .list(&Path::from(format!("{PFX}/{PIPE}/_manifest/compactions")))
        .await
        .unwrap()
        .len()
}

async fn compacted_object_count(inner: &ObjectStoreConditional) -> usize {
    inner
        .list(&Path::from(format!("{PFX}/{PIPE}")))
        .await
        .unwrap()
        .iter()
        .filter(|p| p.to_string().contains("/compacted/"))
        .count()
}

/// Genesis writer + two published batches for table "orders" (objects a, b),
/// returning the writer, the shared fault store, and the two originals.
async fn setup_two_batches(
    inner: &Arc<ObjectStoreConditional>,
    triggers: Vec<Trigger>,
) -> (
    Arc<FaultStore>,
    DurableWriter<FaultStore>,
    Vec<ManifestObject>,
) {
    let (fs, w) = genesis(inner, triggers).await;
    w.publish(&wm(1), vec![tobj_jsonl("orders", &[1])], 1)
        .await
        .unwrap();
    w.publish(&wm(2), vec![tobj_jsonl("orders", &[2])], 1)
        .await
        .unwrap();
    let originals = all_manifest_objects(inner).await;
    assert_eq!(originals.len(), 2);
    (fs, w, originals)
}

/// The equivalence-preserving replacement for the two `setup_two_batches` originals
/// (rows 1 and 2 concatenated in ack order).
fn replacement() -> TableObject {
    tobj_jsonl("orders", &[1, 2])
}

#[tokio::test]
async fn compaction_preserves_ack_state_and_sets_reference() {
    let inner = inmem();
    let (_fs, w, originals) = setup_two_batches(&inner, vec![]).await;
    let before = read_head(&inner).await.unwrap();
    assert_eq!(before.seq, 2);
    let wm2_hex = before.watermark_hex.clone();

    w.compact_with_replacement(replacement(), originals)
        .await
        .unwrap();

    let h = read_head(&inner).await.unwrap();
    // Ack state is untouched; only the compaction reference is set.
    assert_eq!(h.seq, 2, "seq preserved");
    assert_eq!(h.watermark_hex, wm2_hex, "watermark preserved");
    assert_eq!(
        h.head_entry_key, before.head_entry_key,
        "entry ref preserved"
    );
    assert!(h.compaction_key.is_some(), "compaction reference set");
    assert_eq!(compaction_record_count(&inner).await, 1);
    assert_eq!(compacted_object_count(&inner).await, 1);
    assert_eq!(data_count(&inner).await, 2, "originals NOT deleted");

    // A later ordinary publish preserves the compaction reference.
    w.publish(&wm(3), vec![tobj("orders", b"c")], 1)
        .await
        .unwrap();
    let h2 = read_head(&inner).await.unwrap();
    assert_eq!(h2.seq, 3);
    assert_eq!(h2.compaction_key, h.compaction_key, "compaction ref kept");
}

#[tokio::test]
async fn compaction_read_originals_failure_no_record_ack_intact() {
    let inner = inmem();
    let (_fs, w, originals) =
        setup_two_batches(&inner, vec![tr(Op::GetData, 0, Action::ErrBefore)])
            .await;
    let before = read_head(&inner).await.unwrap();

    assert!(
        w.compact_with_replacement(replacement(), originals.clone())
            .await
            .is_err()
    );
    assert_eq!(compaction_record_count(&inner).await, 0);
    assert_eq!(compacted_object_count(&inner).await, 0);
    assert_eq!(read_head(&inner).await.unwrap(), before, "HEAD unchanged");

    // Ack path independent: normal publication still works after the failure.
    w.publish(&wm(3), vec![tobj("orders", b"c")], 1)
        .await
        .unwrap();
    assert_eq!(read_head(&inner).await.unwrap().seq, 3);
    // And a retry of the compaction now succeeds (one-shot fault cleared).
    w.compact_with_replacement(replacement(), originals)
        .await
        .unwrap();
    assert!(read_head(&inner).await.unwrap().compaction_key.is_some());
}

#[tokio::test]
async fn compaction_replacement_upload_failure_no_record() {
    let inner = inmem();
    let (_fs, w, originals) = setup_two_batches(
        &inner,
        vec![tr(Op::CompactedPut, 0, Action::ErrBefore)],
    )
    .await;
    let before = read_head(&inner).await.unwrap();
    assert!(
        w.compact_with_replacement(replacement(), originals)
            .await
            .is_err()
    );
    assert_eq!(compacted_object_count(&inner).await, 0);
    assert_eq!(compaction_record_count(&inner).await, 0);
    assert_eq!(read_head(&inner).await.unwrap(), before);
}

#[tokio::test]
async fn crash_after_replacement_before_record_leaves_orphan() {
    let inner = inmem();
    let (fs, w, originals) = setup_two_batches(
        &inner,
        vec![tr(Op::CompactionRecordPut, 0, Action::HangBefore)],
    )
    .await;
    let before = read_head(&inner).await.unwrap();
    cancel_at_hang(&fs, w.compact_with_replacement(replacement(), originals))
        .await;
    drop(w);
    assert_eq!(
        compacted_object_count(&inner).await,
        1,
        "orphan compacted obj"
    );
    assert_eq!(compaction_record_count(&inner).await, 0);
    assert_eq!(read_head(&inner).await.unwrap(), before, "HEAD unchanged");

    // Restart ignores the orphan (HEAD has no compaction ref) and works.
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let w2 = acquire(Arc::clone(&clean)).await.unwrap();
    w2.publish(&wm(3), vec![tobj("orders", b"c")], 1)
        .await
        .unwrap();
    assert_eq!(read_head(&inner).await.unwrap().seq, 3);
}

#[tokio::test]
async fn crash_after_record_before_head_cas_leaves_orphans() {
    let inner = inmem();
    // The compact HEAD CAS is HeadCas nth 2 (after the two publish CASes).
    let (fs, w, originals) =
        setup_two_batches(&inner, vec![tr(Op::HeadCas, 2, Action::HangBefore)])
            .await;
    let before = read_head(&inner).await.unwrap();
    cancel_at_hang(&fs, w.compact_with_replacement(replacement(), originals))
        .await;
    drop(w);
    assert_eq!(compaction_record_count(&inner).await, 1, "orphan record");
    assert_eq!(compacted_object_count(&inner).await, 1, "orphan object");
    assert_eq!(
        read_head(&inner).await.unwrap(),
        before,
        "HEAD never referenced the orphan record"
    );

    // Restart: verify_compaction_chain sees HEAD has no compaction ref, so the
    // orphan record is ignored; recovery succeeds.
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let w2 = acquire(Arc::clone(&clean)).await.unwrap();
    assert_eq!(w2.epoch().await, 2);
}

#[tokio::test]
async fn compaction_head_cas_rejected_then_lost_response() {
    // Rejected-before-apply: reconcile retries and succeeds.
    let inner = inmem();
    let (_fs, w, originals) =
        setup_two_batches(&inner, vec![tr(Op::HeadCas, 2, Action::ErrBefore)])
            .await;
    w.compact_with_replacement(replacement(), originals)
        .await
        .unwrap();
    assert!(read_head(&inner).await.unwrap().compaction_key.is_some());

    // Applied-then-lost: reconcile sees HEAD already references the record.
    let inner2 = inmem();
    let (_fs2, w2, orig2) = setup_two_batches(
        &inner2,
        vec![tr(Op::HeadCas, 2, Action::ApplyThenErr)],
    )
    .await;
    w2.compact_with_replacement(replacement(), orig2)
        .await
        .unwrap();
    assert!(read_head(&inner2).await.unwrap().compaction_key.is_some());
    assert_eq!(compaction_record_count(&inner2).await, 1);
}

#[tokio::test]
async fn fencing_during_compaction_denies_and_leaves_orphans() {
    let inner = inmem();
    let (_fs, a, originals) = setup_two_batches(&inner, vec![]).await; // epoch 1
    // B acquires, fencing A.
    let b = acquire(Arc::new(FaultStore::new(Arc::clone(&inner), vec![])))
        .await
        .unwrap();
    assert_eq!(b.epoch().await, 2);

    // A's compaction uploads object + record, but its HEAD CAS is fenced.
    let err =
        expect_err(a.compact_with_replacement(replacement(), originals).await);
    assert!(matches!(err, HeadError::Fenced { .. }));
    let h = read_head(&inner).await.unwrap();
    assert_eq!(h.epoch, 2, "B's HEAD stands");
    assert!(h.compaction_key.is_none(), "A never set the compaction ref");
    // Orphan object + record are harmless; B recovers fine on restart.
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let _ = acquire(clean).await.unwrap();
}

#[tokio::test]
async fn restart_with_published_compaction_verifies_and_continues() {
    let inner = inmem();
    let (_fs, w, originals) = setup_two_batches(&inner, vec![]).await;
    w.compact_with_replacement(replacement(), originals)
        .await
        .unwrap();
    drop(w);

    // Restart: acquire verifies the compaction chain and bumps the epoch.
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let w2 = acquire(Arc::clone(&clean)).await.unwrap();
    assert_eq!(w2.epoch().await, 2);
    let h = read_head(&inner).await.unwrap();
    assert!(h.compaction_key.is_some());
    // Publishing still works and keeps the compaction reference.
    w2.publish(&wm(3), vec![tobj("orders", b"c")], 1)
        .await
        .unwrap();
    assert_eq!(read_head(&inner).await.unwrap().seq, 3);
    assert!(read_head(&inner).await.unwrap().compaction_key.is_some());
}

#[tokio::test]
async fn recovery_with_corrupt_replacement_fails_closed() {
    let inner = inmem();
    let (_fs, w, originals) = setup_two_batches(&inner, vec![]).await;
    w.compact_with_replacement(replacement(), originals)
        .await
        .unwrap();
    drop(w);

    // Corrupt (delete) the compacted replacement object.
    let compacted: Vec<Path> = inner
        .list(&Path::from(format!("{PFX}/{PIPE}")))
        .await
        .unwrap()
        .into_iter()
        .filter(|p| p.to_string().contains("/compacted/"))
        .collect();
    inner.delete(&compacted[0]).await.unwrap();

    // Recovery must fail closed: the referenced replacement is missing.
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let err = expect_err(acquire(clean).await);
    assert!(
        err.is_fatal(),
        "corrupt compaction replacement is fatal: {err:?}"
    );
}

#[tokio::test]
async fn recompaction_of_superseded_original_detected_at_recovery() {
    let inner = inmem();
    let (_fs, w, originals) = setup_two_batches(&inner, vec![]).await;
    // First compaction supersedes both originals.
    w.compact_with_replacement(replacement(), originals.clone())
        .await
        .unwrap();
    // A buggy second compaction re-lists one already-superseded original, with an
    // equivalence-preserving replacement (row 1) so it publishes.
    let dup = vec![originals[0].clone()];
    w.compact_with_replacement(tobj_jsonl("orders", &[1]), dup)
        .await
        .unwrap();
    drop(w);

    // Recovery detects the same original in two active compactions.
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let err = expect_err(acquire(clean).await);
    assert!(
        matches!(err, HeadError::Integrity(ref m) if m.contains("conflicting")),
        "expected conflicting-compaction integrity error, got {err:?}"
    );
}

// ── Production compaction API (replacement built from verified originals) ────

/// Read the single compacted replacement object's bytes.
async fn compacted_bytes(inner: &ObjectStoreConditional) -> Bytes {
    let key = inner
        .list(&Path::from(format!("{PFX}/{PIPE}")))
        .await
        .unwrap()
        .into_iter()
        .find(|p| p.to_string().contains("/compacted/"))
        .expect("a compacted object");
    inner.get_with_etag(&key).await.unwrap().unwrap().0
}

#[tokio::test]
async fn production_compact_builds_replacement_from_verified_originals() {
    let inner = inmem();
    let (_fs, w, originals) = setup_two_batches(&inner, vec![]).await;
    let before = read_head(&inner).await.unwrap();

    // Production API: no caller bytes, just the authoritative originals.
    w.compact(originals).await.unwrap();

    let h = read_head(&inner).await.unwrap();
    assert_eq!(h.seq, before.seq, "ack state preserved");
    assert_eq!(h.watermark_hex, before.watermark_hex);
    assert!(h.compaction_key.is_some(), "compaction reference set");
    assert_eq!(compaction_record_count(&inner).await, 1);
    assert_eq!(data_count(&inner).await, 2, "originals NOT deleted");
    // The replacement is the originals' exact bytes concatenated in ack order (the
    // two JSONL rows), which is exactly the equivalent [1,2] object.
    assert_eq!(
        compacted_bytes(&inner).await,
        tobj_jsonl("orders", &[1, 2]).bytes
    );
}

#[tokio::test]
async fn production_compact_orders_originals_by_ack_sequence() {
    let inner = inmem();
    let (_fs, w, mut originals) = setup_two_batches(&inner, vec![]).await;
    // Hand the API the originals in the WRONG order; it must re-order by ack seq.
    originals.reverse();
    w.compact(originals).await.unwrap();
    assert_eq!(
        compacted_bytes(&inner).await,
        tobj_jsonl("orders", &[1, 2]).bytes,
        "replacement respects ack order, not caller order"
    );
}

#[tokio::test]
async fn production_compact_rejects_non_authoritative_original() {
    let inner = inmem();
    let (_fs, w, _originals) = setup_two_batches(&inner, vec![]).await;
    // A fabricated original that is not in the acknowledgement chain.
    let fake = ManifestObject {
        key: format!("{PFX}/{PIPE}/orders/wm-fake/x.jsonl"),
        table: "orders".into(),
        content_hash: "h-fake".into(),
        byte_len: 1,
        format: "jsonl".into(),
        format_version: 1,
        schema_id: "s1".into(),
        compression: "none".into(),
        partition_spec: "table".into(),
        partition_version: 1,
    };
    let err = expect_err(w.compact(vec![fake]).await);
    assert!(
        matches!(err, HeadError::Integrity(ref m) if m.contains("authoritative")),
        "expected non-authoritative rejection, got {err:?}"
    );
    assert_eq!(compaction_record_count(&inner).await, 0);
    assert_eq!(compacted_object_count(&inner).await, 0);
}

#[tokio::test]
async fn production_compact_rejects_tampered_original_hash() {
    let inner = inmem();
    let (_fs, w, mut originals) = setup_two_batches(&inner, vec![]).await;
    // Same key as a real original, but a content hash that no longer matches.
    originals[0].content_hash = "h-tampered".into();
    let err = expect_err(w.compact(originals).await);
    assert!(
        matches!(err, HeadError::Integrity(ref m) if m.contains("hash")),
        "expected content-hash mismatch, got {err:?}"
    );
    assert_eq!(compaction_record_count(&inner).await, 0);
}

// ── Rollup publication + recovery ────────────────────────────────────────────

async fn rollup_record_count(inner: &ObjectStoreConditional) -> usize {
    inner
        .list(&Path::from(format!("{PFX}/{PIPE}/_manifest/rollups")))
        .await
        .unwrap()
        .iter()
        .filter(|p| !p.to_string().contains("/inventory/"))
        .count()
}

async fn inventory_count(inner: &ObjectStoreConditional) -> usize {
    inner
        .list(&Path::from(format!(
            "{PFX}/{PIPE}/_manifest/rollups/inventory"
        )))
        .await
        .unwrap()
        .len()
}

/// Delete the rollup record a HEAD field currently references.
async fn delete_rollup(inner: &ObjectStoreConditional, key: &str) {
    inner.delete(&Path::from(key.to_string())).await.unwrap();
}

#[tokio::test]
async fn rollup_happy_path_sets_refs_and_preserves_ack_state() {
    let inner = inmem();
    let (_fs, w, _o) = setup_two_batches(&inner, vec![]).await;
    let before = read_head(&inner).await.unwrap();

    w.rollup().await.unwrap();

    let h = read_head(&inner).await.unwrap();
    assert_eq!(h.seq, before.seq, "ack state untouched");
    assert_eq!(h.watermark_hex, before.watermark_hex);
    assert_eq!(h.head_entry_key, before.head_entry_key);
    assert!(h.rollup_key.is_some(), "current rollup reference set");
    assert!(h.prev_rollup_key.is_none(), "no previous rollup yet");
    assert_eq!(rollup_record_count(&inner).await, 1);
}

#[tokio::test]
async fn second_rollup_retains_current_and_previous_refs_and_recovers() {
    let inner = inmem();
    let (_fs, w, _o) = setup_two_batches(&inner, vec![]).await;
    w.rollup().await.unwrap(); // rollup 1 covers seq 1..=2
    let r1 = read_head(&inner).await.unwrap().rollup_key.unwrap();

    // Publish a third batch, then roll up again: rollup 2 covers seq 3.
    w.publish(&wm(3), vec![tobj("orders", b"c")], 1)
        .await
        .unwrap();
    w.rollup().await.unwrap();

    let h = read_head(&inner).await.unwrap();
    assert!(h.rollup_key.is_some());
    assert_eq!(
        h.prev_rollup_key.as_deref(),
        Some(r1.as_str()),
        "previous rollup reference retained"
    );
    assert_ne!(h.rollup_key, h.prev_rollup_key, "two distinct rollups");
    assert_eq!(rollup_record_count(&inner).await, 2);

    // Recovery verifies the rollup chain and continues.
    drop(w);
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let w2 = acquire(Arc::clone(&clean)).await.unwrap();
    assert_eq!(w2.epoch().await, 2);
}

#[tokio::test]
async fn ordinary_publish_and_compaction_preserve_both_rollup_refs() {
    let inner = inmem();
    let (_fs, w, _o) = setup_two_batches(&inner, vec![]).await;
    w.rollup().await.unwrap();
    w.publish(&wm(3), vec![tobj_jsonl("orders", &[3])], 1)
        .await
        .unwrap();
    w.rollup().await.unwrap();
    let h = read_head(&inner).await.unwrap();
    let (cur, prev) = (h.rollup_key.clone(), h.prev_rollup_key.clone());
    assert!(cur.is_some() && prev.is_some());

    // An ordinary publish keeps both rollup references.
    w.publish(&wm(4), vec![tobj_jsonl("orders", &[4])], 1)
        .await
        .unwrap();
    let h2 = read_head(&inner).await.unwrap();
    assert_eq!(h2.rollup_key, cur, "publish keeps current rollup ref");
    assert_eq!(
        h2.prev_rollup_key, prev,
        "publish keeps previous rollup ref"
    );

    // A compaction also keeps both rollup references.
    let originals = all_manifest_objects(&inner).await;
    w.compact(originals).await.unwrap();
    let h3 = read_head(&inner).await.unwrap();
    assert_eq!(h3.rollup_key, cur, "compaction keeps current rollup ref");
    assert_eq!(
        h3.prev_rollup_key, prev,
        "compaction keeps previous rollup ref"
    );
    assert!(h3.compaction_key.is_some());
}

#[tokio::test]
async fn rollup_record_upload_failure_leaves_head_unchanged() {
    let inner = inmem();
    let (_fs, w, _o) = setup_two_batches(
        &inner,
        vec![tr(Op::RollupRecordPut, 0, Action::ErrBefore)],
    )
    .await;
    let before = read_head(&inner).await.unwrap();
    assert!(w.rollup().await.is_err());
    assert_eq!(rollup_record_count(&inner).await, 0);
    assert_eq!(read_head(&inner).await.unwrap(), before, "HEAD unchanged");
    // Retry (one-shot fault cleared) succeeds.
    w.rollup().await.unwrap();
    assert!(read_head(&inner).await.unwrap().rollup_key.is_some());
}

#[tokio::test]
async fn rollup_head_cas_rejected_reconciles() {
    let inner = inmem();
    // The rollup HEAD CAS is HeadCas nth 2 (after the two publish CASes).
    let (_fs, w, _o) =
        setup_two_batches(&inner, vec![tr(Op::HeadCas, 2, Action::ErrBefore)])
            .await;
    w.rollup().await.unwrap();
    assert!(read_head(&inner).await.unwrap().rollup_key.is_some());
    assert_eq!(rollup_record_count(&inner).await, 1);
}

#[tokio::test]
async fn rollup_head_cas_lost_response_acks_because_head_references_rollup() {
    let inner = inmem();
    let (_fs, w, _o) = setup_two_batches(
        &inner,
        vec![tr(Op::HeadCas, 2, Action::ApplyThenErr)],
    )
    .await;
    w.rollup().await.unwrap();
    assert!(read_head(&inner).await.unwrap().rollup_key.is_some());
    assert_eq!(rollup_record_count(&inner).await, 1);
}

#[tokio::test]
async fn crash_after_rollup_record_before_head_cas_leaves_orphan() {
    let inner = inmem();
    let (fs, w, _o) =
        setup_two_batches(&inner, vec![tr(Op::HeadCas, 2, Action::HangBefore)])
            .await;
    let before = read_head(&inner).await.unwrap();
    cancel_at_hang(&fs, w.rollup()).await;
    drop(w);
    assert_eq!(rollup_record_count(&inner).await, 1, "orphan rollup record");
    assert_eq!(
        read_head(&inner).await.unwrap(),
        before,
        "HEAD never referenced the orphan rollup"
    );

    // Restart: HEAD has no rollup ref, so the orphan is ignored; recovery works.
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let w2 = acquire(Arc::clone(&clean)).await.unwrap();
    assert_eq!(w2.epoch().await, 2);
    assert!(read_head(&inner).await.unwrap().rollup_key.is_none());
}

#[tokio::test]
async fn fencing_during_rollup_denies_and_leaves_orphan() {
    let inner = inmem();
    let (_fs, a, _o) = setup_two_batches(&inner, vec![]).await; // epoch 1
    let b = acquire(Arc::new(FaultStore::new(Arc::clone(&inner), vec![])))
        .await
        .unwrap();
    assert_eq!(b.epoch().await, 2);

    // A's rollup writes its record, but the HEAD CAS finds B's higher epoch.
    let err = expect_err(a.rollup().await);
    assert!(matches!(err, HeadError::Fenced { .. }));
    let h = read_head(&inner).await.unwrap();
    assert_eq!(h.epoch, 2, "B's HEAD stands");
    assert!(h.rollup_key.is_none(), "A never set the rollup ref");
    // Orphan rollup record is harmless; B recovers on restart.
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let _ = acquire(clean).await.unwrap();
}

#[tokio::test]
async fn corrupt_current_rollup_falls_back_and_recovers() {
    // A damaged CURRENT rollup is an alarm, not a failure: recovery falls back to
    // the previous rollup / retained entries (ground truth) and succeeds.
    let inner = inmem();
    let (_fs, w, _o) = setup_two_batches(&inner, vec![]).await;
    w.rollup().await.unwrap();
    w.publish(&wm(3), vec![tobj("orders", b"c")], 1)
        .await
        .unwrap();
    w.rollup().await.unwrap();
    drop(w);
    let cur = read_head(&inner).await.unwrap().rollup_key.unwrap();
    delete_rollup(&inner, &cur).await;

    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let w2 = acquire(Arc::clone(&clean)).await.unwrap();
    assert_eq!(w2.epoch().await, 2, "recovery succeeded via fallback");
    // Publishing still works after fall-back recovery.
    w2.publish(&wm(4), vec![tobj("orders", b"d")], 1)
        .await
        .unwrap();
    assert_eq!(read_head(&inner).await.unwrap().seq, 4);
}

#[tokio::test]
async fn corrupt_previous_rollup_falls_back_to_entries_and_recovers() {
    let inner = inmem();
    let (_fs, w, _o) = setup_two_batches(&inner, vec![]).await;
    w.rollup().await.unwrap();
    w.publish(&wm(3), vec![tobj("orders", b"c")], 1)
        .await
        .unwrap();
    w.rollup().await.unwrap();
    drop(w);
    // Corrupt the PREVIOUS rollup: the current one still verifies its own range,
    // but its prev chain is broken; recovery falls back to the entry chain.
    let prev = read_head(&inner).await.unwrap().prev_rollup_key.unwrap();
    delete_rollup(&inner, &prev).await;

    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let w2 = acquire(Arc::clone(&clean)).await.unwrap();
    assert_eq!(w2.epoch().await, 2, "recovery succeeded via entry chain");
}

#[tokio::test]
async fn rollup_before_any_publish_is_rejected() {
    let inner = inmem();
    let (_fs, w) = genesis(&inner, vec![]).await;
    // Genesis HEAD (seq 0) has nothing to summarize.
    let err = expect_err(w.rollup().await);
    assert!(matches!(err, HeadError::Integrity(_)), "got {err:?}");
    assert_eq!(rollup_record_count(&inner).await, 0);
}

// ── 9B.1: cumulative rollups, recovery-from-rollup + inventory, dry-run GC ────

/// Read HEAD and its ETag straight from the store.
async fn head_and_etag(inner: &ObjectStoreConditional) -> (Head, String) {
    let (raw, etag) = inner
        .get_with_etag(&head_key_for(PFX, PIPE))
        .await
        .unwrap()
        .unwrap();
    (serde_json::from_slice(&raw).unwrap(), etag.unwrap())
}

/// Load the rollup record HEAD currently references.
async fn current_rollup(inner: &ObjectStoreConditional) -> RollupRecord {
    let h = read_head(inner).await.unwrap();
    load_rollup_rec(inner, &h.rollup_key.unwrap(), &h.rollup_hash.unwrap())
        .await
        .unwrap()
}

/// Load the inventory index bound by a rollup record.
async fn inventory_of(
    inner: &ObjectStoreConditional,
    rec: &RollupRecord,
) -> InventoryIndex {
    load_inventory(inner, &rec.inventory_key, &rec.inventory_record_hash)
        .await
        .unwrap()
}

/// Delete every manifest entry with seq <= `seq_max` (simulate authorized GC).
async fn delete_entries_le(inner: &ObjectStoreConditional, seq_max: u64) {
    for k in inner
        .list(&Path::from(format!("{PFX}/{PIPE}/_manifest/entries")))
        .await
        .unwrap()
    {
        let ks = k.to_string();
        if let Some(seq) = entry_seq_from_key(&ks) {
            if seq <= seq_max {
                inner.delete(&k).await.unwrap();
            }
        }
    }
}

/// Delete the rollup record + its bound inventory index (simulate damage).
async fn delete_rollup_and_inventory(
    inner: &ObjectStoreConditional,
    key: &str,
    hash: &str,
) {
    let rec = load_rollup_rec(inner, key, hash).await.unwrap();
    inner.delete(&Path::from(rec.inventory_key)).await.unwrap();
    inner.delete(&Path::from(key.to_string())).await.unwrap();
}

/// Publish `n` single-object batches ("orders", value per seq) and roll up,
/// producing one cumulative generation. Returns the writer + shared fault store.
async fn setup_generations(
    inner: &Arc<ObjectStoreConditional>,
) -> (Arc<FaultStore>, DurableWriter<FaultStore>) {
    // gen1 over [1,2]; publish seq 3 then gen2 over [1,3]. HEAD: current=gen2
    // (end 3), prev=gen1 (end 2), horizon = 2.
    let (fs, w) = genesis(inner, vec![]).await;
    w.publish(&wm(1), vec![tobj_jsonl("orders", &[1])], 1)
        .await
        .unwrap();
    w.publish(&wm(2), vec![tobj_jsonl("orders", &[2])], 1)
        .await
        .unwrap();
    w.rollup().await.unwrap(); // gen1 [1,2]
    w.publish(&wm(3), vec![tobj_jsonl("orders", &[3])], 1)
        .await
        .unwrap();
    w.rollup().await.unwrap(); // gen2 [1,3]
    (fs, w)
}

#[tokio::test]
async fn cumulative_rollups_are_nested_from_genesis() {
    let inner = inmem();
    let (_fs, w) = setup_generations(&inner).await;
    let cur = current_rollup(&inner).await;
    assert_eq!(
        cur.start_seq, 1,
        "current rollup is cumulative from genesis"
    );
    assert_eq!(cur.end_seq, 3);
    let h = read_head(&inner).await.unwrap();
    let prev = load_rollup_rec(
        inner.as_ref(),
        &h.prev_rollup_key.unwrap(),
        &h.prev_rollup_hash.unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(prev.start_seq, 1, "previous rollup also cumulative");
    assert_eq!(prev.end_seq, 2);
    assert!(
        prev.end_seq < cur.end_seq,
        "generations strictly increasing"
    );
    // Each generation wrote its own inventory index.
    assert_eq!(inventory_count(&inner).await, 2);
    assert_eq!(cur.inventory_count, 3, "current inventory covers [1,3]");
    drop(w);
    // Intact recovery still works.
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    acquire(clean).await.unwrap();
}

#[tokio::test]
async fn remove_entries_through_horizon_recovers_via_current_rollup() {
    let inner = inmem();
    let (_fs, w) = setup_generations(&inner).await;
    drop(w);
    // Authorized GC removes entries at/below the horizon (prev.end = 2).
    delete_entries_le(&inner, 2).await;

    // Recovery reconstructs [1,3] from the current cumulative rollup + inventory
    // plus the retained tail (seq 3), and continues.
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let w2 = acquire(Arc::clone(&clean)).await.unwrap();
    assert_eq!(w2.epoch().await, 2, "recovered-current");
    w2.publish(&wm(4), vec![tobj("orders", b"d")], 1)
        .await
        .unwrap();
    assert_eq!(read_head(&inner).await.unwrap().seq, 4);
}

#[tokio::test]
async fn damaged_current_falls_back_to_previous_generation() {
    let inner = inmem();
    let (_fs, w) = setup_generations(&inner).await;
    let h = read_head(&inner).await.unwrap();
    drop(w);
    delete_entries_le(&inner, 2).await;
    // Damage the current generation entirely.
    delete_rollup_and_inventory(
        &inner,
        h.rollup_key.as_ref().unwrap(),
        h.rollup_hash.as_ref().unwrap(),
    )
    .await;

    // Recovery falls back to the previous rollup [1,2] + the retained tail (seq 3).
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let w2 = acquire(Arc::clone(&clean)).await.unwrap();
    assert_eq!(w2.epoch().await, 2, "recovered-previous");
}

#[tokio::test]
async fn removing_older_than_previous_generation_does_not_break_recovery() {
    let inner = inmem();
    let (fs, w) = genesis(&inner, vec![]).await;
    w.publish(&wm(1), vec![tobj("orders", b"a")], 1)
        .await
        .unwrap();
    w.publish(&wm(2), vec![tobj("orders", b"b")], 1)
        .await
        .unwrap();
    w.rollup().await.unwrap(); // gen1 [1,2]
    let gen1 = read_head(&inner).await.unwrap();
    w.publish(&wm(3), vec![tobj("orders", b"c")], 1)
        .await
        .unwrap();
    w.rollup().await.unwrap(); // gen2 [1,3]
    w.publish(&wm(4), vec![tobj("orders", b"d")], 1)
        .await
        .unwrap();
    w.rollup().await.unwrap(); // gen3 [1,4]; HEAD current=gen3, prev=gen2
    drop((fs, w));

    // Remove entries through the horizon (prev = gen2, end 3) and delete the
    // older-than-previous generation (gen1): a dangling prev.prev is expected.
    delete_entries_le(&inner, 3).await;
    delete_rollup_and_inventory(
        &inner,
        gen1.rollup_key.as_ref().unwrap(),
        gen1.rollup_hash.as_ref().unwrap(),
    )
    .await;

    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    acquire(clean)
        .await
        .expect("recovery ignores the removed older generation");
}

#[tokio::test]
async fn losing_previous_generation_with_damaged_current_halts() {
    let inner = inmem();
    let (_fs, w) = setup_generations(&inner).await;
    let h = read_head(&inner).await.unwrap();
    drop(w);
    delete_entries_le(&inner, 2).await;
    // Damage BOTH retained generations: beyond the one-generation horizon.
    delete_rollup_and_inventory(
        &inner,
        h.rollup_key.as_ref().unwrap(),
        h.rollup_hash.as_ref().unwrap(),
    )
    .await;
    delete_rollup_and_inventory(
        &inner,
        h.prev_rollup_key.as_ref().unwrap(),
        h.prev_rollup_hash.as_ref().unwrap(),
    )
    .await;

    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let err = expect_err(acquire(clean).await);
    assert!(
        err.is_fatal(),
        "corruption beyond the horizon halts: {err:?}"
    );
}

#[tokio::test]
async fn publish_and_compaction_preserve_cumulative_refs_and_times() {
    let inner = inmem();
    let (_fs, w) = setup_generations(&inner).await;
    let before = read_head(&inner).await.unwrap();
    assert!(before.rollup_published_at_ms.is_some());
    assert!(before.prev_rollup_published_at_ms.is_some());

    // Ordinary publish preserves both generations' refs + publication times.
    w.publish(&wm(4), vec![tobj_jsonl("orders", &[4])], 1)
        .await
        .unwrap();
    let h = read_head(&inner).await.unwrap();
    assert_eq!(h.rollup_key, before.rollup_key);
    assert_eq!(h.prev_rollup_key, before.prev_rollup_key);
    assert_eq!(h.rollup_published_at_ms, before.rollup_published_at_ms);
    assert_eq!(
        h.prev_rollup_published_at_ms,
        before.prev_rollup_published_at_ms
    );

    // Compaction also preserves them.
    let originals = all_manifest_objects(&inner).await;
    w.compact(originals).await.unwrap();
    let h2 = read_head(&inner).await.unwrap();
    assert_eq!(h2.rollup_key, before.rollup_key);
    assert_eq!(h2.prev_rollup_key, before.prev_rollup_key);
    assert_eq!(h2.rollup_published_at_ms, before.rollup_published_at_ms);
}

#[tokio::test]
async fn cumulative_inventory_is_unchanged_by_compaction() {
    let inner = inmem();
    let (_fs, w) = setup_generations(&inner).await;
    let inv_before = inventory_of(&inner, &current_rollup(&inner).await).await;

    // Compact the originals; the rollup + its inventory are untouched.
    let originals = all_manifest_objects(&inner).await;
    w.compact(originals).await.unwrap();

    let inv_after = inventory_of(&inner, &current_rollup(&inner).await).await;
    assert_eq!(
        inv_before, inv_after,
        "the rollup inventory still lists the original acknowledged objects"
    );
    // None of the inventory objects is the compacted replacement.
    assert!(
        inv_after
            .objects
            .iter()
            .all(|o| !o.key.contains("/compacted/")),
        "inventory holds originals, not replacements"
    );
}

// ── Dry-run GcPlan ───────────────────────────────────────────────────────────

const NO_WINDOW: GcConfig = GcConfig {
    safety_window_ms: 0,
};

/// A `TableObject` of real canonical JSONL rows, each with a valid `event_id`, so
/// the internal compactor + equivalence validator can round-trip it.
fn tobj_jsonl(table: &str, rows: &[u32]) -> TableObject {
    let mut buf = Vec::new();
    for r in rows {
        let id = deltaforge_core::EventId::mysql_row_server(
            1, "orders", *r as u64, 0,
        )
        .to_string();
        buf.extend_from_slice(
            format!("{{\"event_id\":\"{id}\",\"v\":{r}}}").as_bytes(),
        );
        buf.push(b'\n');
    }
    TableObject {
        table: table.to_string(),
        bytes: Bytes::from(buf),
        domain: EncodingDomain::new("jsonl", 1, "s1", "none", "table", 1),
        ext: "jsonl",
    }
}

#[tokio::test]
async fn plan_gc_lists_eligible_entries_and_data_originals() {
    let inner = inmem();
    // Two cumulative generations over real JSONL objects (so compaction produces an
    // equivalence-provable replacement).
    let (_fs, w) = genesis(&inner, vec![]).await;
    w.publish(&wm(1), vec![tobj_jsonl("orders", &[1])], 1)
        .await
        .unwrap();
    w.publish(&wm(2), vec![tobj_jsonl("orders", &[2])], 1)
        .await
        .unwrap();
    w.rollup().await.unwrap(); // gen1 [1,2]
    w.publish(&wm(3), vec![tobj_jsonl("orders", &[3])], 1)
        .await
        .unwrap();
    w.rollup().await.unwrap(); // gen2 [1,3]
    let originals = all_manifest_objects(&inner).await;
    w.compact(originals).await.unwrap();

    let now = now_ms_test() + 1_000_000;
    let plan = w.plan_gc(now, NO_WINDOW).await.unwrap();

    // Horizon = prev.end = 2; entries seq 1,2 are eligible (seq 3 is the tail).
    assert_eq!(plan.horizon_seq, 2);
    let seqs: Vec<u64> = plan
        .manifest_entries_eligible
        .iter()
        .map(|e| e.seq)
        .collect();
    assert_eq!(seqs, vec![1, 2], "only entries at/below the horizon");
    // All three originals were compacted -> data-eligible, each with a replacement.
    assert_eq!(plan.data_originals_eligible.len(), 3);
    assert!(
        plan.data_originals_eligible
            .iter()
            .all(|o| o.replacement_key.contains("/compacted/"))
    );
    assert!(
        plan.alarms.is_empty(),
        "no alarms on a clean plan: {:?}",
        plan.alarms
    );
}

#[tokio::test]
async fn plan_gc_premature_window_lists_no_entries() {
    let inner = inmem();
    let (_fs, w) = setup_generations(&inner).await;
    let pub_at = read_head(&inner)
        .await
        .unwrap()
        .rollup_published_at_ms
        .unwrap();
    // now == publish time => age 0; a large window => not yet eligible.
    let plan = w
        .plan_gc(
            pub_at,
            GcConfig {
                safety_window_ms: u64::MAX,
            },
        )
        .await
        .unwrap();
    assert!(
        plan.manifest_entries_eligible.is_empty(),
        "window not elapsed => no manifest entries"
    );
}

#[tokio::test]
async fn plan_gc_future_timestamp_alarms_and_lists_no_entries() {
    let inner = inmem();
    let (_fs, w) = setup_generations(&inner).await;
    let pub_at = read_head(&inner)
        .await
        .unwrap()
        .rollup_published_at_ms
        .unwrap();
    // now strictly before the publication time => suspicious clock.
    let plan = w.plan_gc(pub_at - 1, NO_WINDOW).await.unwrap();
    assert!(plan.manifest_entries_eligible.is_empty());
    assert!(
        plan.alarms.iter().any(|a| a.message.contains("future")),
        "future-timestamp alarm expected: {:?}",
        plan.alarms
    );
}

#[tokio::test]
async fn plan_gc_still_valid_until_head_changes() {
    let inner = inmem();
    let (_fs, w) = setup_generations(&inner).await;
    let now = now_ms_test() + 1_000_000;
    let plan = w.plan_gc(now, NO_WINDOW).await.unwrap();
    let (h1, e1) = head_and_etag(&inner).await;
    assert!(
        plan.still_valid(&h1, &e1),
        "valid against the HEAD it was built on"
    );

    // Any HEAD write invalidates the plan.
    w.publish(&wm(4), vec![tobj("orders", b"d")], 1)
        .await
        .unwrap();
    let (h2, e2) = head_and_etag(&inner).await;
    assert!(!plan.still_valid(&h2, &e2), "stale after a new batch");
}

#[tokio::test]
async fn plan_gc_fenced_writer_returns_fenced() {
    let inner = inmem();
    let (_fs, a) = setup_generations(&inner).await; // epoch 1
    let _b = acquire(Arc::new(FaultStore::new(Arc::clone(&inner), vec![])))
        .await
        .unwrap(); // epoch 2 fences A
    let now = now_ms_test() + 1_000_000;
    let err = expect_err(a.plan_gc(now, NO_WINDOW).await);
    assert!(matches!(err, HeadError::Fenced { .. }), "got {err:?}");
}

#[tokio::test]
async fn non_equivalent_compaction_is_not_published() {
    let inner = inmem();
    let (_fs, w) = setup_generations(&inner).await;
    let before = read_head(&inner).await.unwrap();
    // A compaction whose replacement is NOT equivalent to its originals is refused at
    // publication: HEAD is unchanged, no compaction reference, and the uploaded
    // replacement is a harmless orphan.
    let originals = all_manifest_objects(&inner).await;
    let err = expect_err(
        w.compact_with_replacement(
            tobj("orders", b"not-equivalent"),
            originals,
        )
        .await,
    );
    assert!(matches!(err, HeadError::Integrity(_)), "got {err:?}");
    let after = read_head(&inner).await.unwrap();
    assert_eq!(after, before, "HEAD unchanged");
    assert!(
        after.compaction_key.is_none(),
        "no compaction reference set"
    );
    assert_eq!(
        compaction_record_count(&inner).await,
        0,
        "no compaction record published"
    );
}

fn now_ms_test() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

// ── 9B.1 follow-up: deletion-gate regression tests ───────────────────────────

#[tokio::test]
async fn cumulative_rollup_can_be_created_after_entry_gc() {
    // Blocker 1: a later cumulative rollup must be buildable from the current
    // cumulative inventory + retained tail, without walking to a GC'd genesis.
    let inner = inmem();
    let (_fs, w) = setup_generations(&inner).await; // gen2 [1,3], prev gen1 [1,2]
    drop(w);
    delete_entries_le(&inner, 2).await; // authorized GC through the horizon

    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let w2 = acquire(Arc::clone(&clean)).await.unwrap(); // recovered-current
    w2.publish(&wm(4), vec![tobj("orders", b"d")], 1)
        .await
        .unwrap();
    w2.publish(&wm(5), vec![tobj("orders", b"e")], 1)
        .await
        .unwrap();
    // The new rollup builds on gen2's inventory ([1,3]) + the retained tail (4,5].
    w2.rollup().await.unwrap();
    let gen3 = current_rollup(&inner).await;
    assert_eq!(gen3.start_seq, 1, "still cumulative from genesis");
    assert_eq!(gen3.end_seq, 5);
    assert_eq!(gen3.inventory_count, 5, "covers all five objects");
    drop(w2);

    // And recovery from the new generation still works.
    let clean2 = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let w3 = acquire(Arc::clone(&clean2)).await.unwrap();
    w3.publish(&wm(6), vec![tobj("orders", b"f")], 1)
        .await
        .unwrap();
    assert_eq!(read_head(&inner).await.unwrap().seq, 6);
}

#[tokio::test]
async fn missing_entry_at_horizon_plus_one_halts() {
    // Blocker 2: a gap ABOVE the fallback horizon is not authorized GC.
    let inner = inmem();
    let (_fs, w) = setup_generations(&inner).await; // horizon = prev.end = 2
    w.publish(&wm(4), vec![tobj("orders", b"d")], 1)
        .await
        .unwrap(); // HEAD seq 4
    drop(w);
    // Delete seq 3 (= horizon + 1), which must remain in the retained tail.
    delete_entries_le_range(&inner, 3, 3).await;

    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let err = expect_err(acquire(clean).await);
    assert!(err.is_fatal(), "gap above the horizon must halt: {err:?}");
}

#[tokio::test]
async fn missing_entry_elsewhere_in_retained_tail_halts() {
    // Blocker 2: a gap further up the retained tail (still above horizon) halts.
    let inner = inmem();
    let (_fs, w) = setup_generations(&inner).await; // horizon 2
    w.publish(&wm(4), vec![tobj("orders", b"d")], 1)
        .await
        .unwrap();
    w.publish(&wm(5), vec![tobj("orders", b"e")], 1)
        .await
        .unwrap(); // HEAD seq 5
    drop(w);
    delete_entries_le_range(&inner, 4, 4).await; // interior tail entry, above horizon

    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let err = expect_err(acquire(clean).await);
    assert!(
        err.is_fatal(),
        "tail gap above the horizon must halt: {err:?}"
    );
}

#[tokio::test]
async fn rollup_lost_response_does_not_adopt_higher_epoch_rollup() {
    // Blocker 4: a stale writer whose rollup CAS conflicts must be fenced, even if a
    // higher-epoch writer published the byte-identical (content-addressed) rollup.
    let inner = inmem();
    let (_fsa, a) = genesis(&inner, vec![]).await; // epoch 1
    a.publish(&wm(1), vec![tobj("orders", b"a")], 1)
        .await
        .unwrap();
    a.publish(&wm(2), vec![tobj("orders", b"b")], 1)
        .await
        .unwrap();
    // B acquires (epoch 2, fences A) and rolls up the same [1,2] range.
    let b = acquire(Arc::new(FaultStore::new(Arc::clone(&inner), vec![])))
        .await
        .unwrap();
    assert_eq!(b.epoch().await, 2);
    b.rollup().await.unwrap();

    // A rolls up the identical range; its HEAD CAS conflicts with B's higher epoch.
    // A must be fenced, NOT treat B's identical rollup as its own lost response.
    let err = expect_err(a.rollup().await);
    assert!(matches!(err, HeadError::Fenced { .. }), "got {err:?}");
    let h = read_head(&inner).await.unwrap();
    assert_eq!(h.epoch, 2, "B's HEAD stands");
    assert!(h.rollup_key.is_some());
}

/// Delete every manifest entry with `lo <= seq <= hi`.
async fn delete_entries_le_range(
    inner: &ObjectStoreConditional,
    lo: u64,
    hi: u64,
) {
    for k in inner
        .list(&Path::from(format!("{PFX}/{PIPE}/_manifest/entries")))
        .await
        .unwrap()
    {
        let ks = k.to_string();
        if let Some(seq) = entry_seq_from_key(&ks) {
            if seq >= lo && seq <= hi {
                inner.delete(&k).await.unwrap();
            }
        }
    }
}

// ── 9B.1 review round 2: cross-epoch reconciliation + version validation ──────

#[tokio::test]
async fn higher_epoch_preserving_entry_ref_fences_stale_publisher() {
    // A's publish CAS applies (HEAD -> entry E1 at epoch 1), then the process dies
    // before returning. B acquires (epoch 2), whose epoch bump PRESERVES E1 as the
    // head entry. A retries the identical publish: its CAS conflicts and the re-read
    // HEAD references its exact entry E1 - but under epoch 2. A must be fenced, not
    // report idempotent success.
    let inner = inmem();
    let (fs, a) =
        genesis(&inner, vec![tr(Op::HeadCas, 0, Action::HangAfterApply)]).await;
    cancel_at_hang(&fs, a.publish(&wm(5), vec![tobj("orders", b"a")], 1)).await;
    assert_eq!(read_head(&inner).await.unwrap().seq, 1, "E1 applied");

    let b = acquire(Arc::new(FaultStore::new(Arc::clone(&inner), vec![])))
        .await
        .unwrap();
    assert_eq!(b.epoch().await, 2);
    let h = read_head(&inner).await.unwrap();
    assert_eq!(h.epoch, 2, "epoch bumped, entry ref preserved");

    let err =
        expect_err(a.publish(&wm(5), vec![tobj("orders", b"a")], 1).await);
    assert!(matches!(err, HeadError::Fenced { .. }), "got {err:?}");
}

#[tokio::test]
async fn higher_epoch_preserving_compaction_hash_fences_stale_compactor() {
    // A's compaction CAS applies (HEAD gains the compaction ref at epoch 1), then
    // dies. B acquires (epoch 2), preserving the same compaction ref. A retries the
    // identical (content-addressed) compaction: its CAS conflicts and HEAD carries
    // the same compaction hash under epoch 2. A must be fenced, not ack.
    let inner = inmem();
    let (fs, a, originals) = setup_two_batches(
        &inner,
        vec![tr(Op::HeadCas, 2, Action::HangAfterApply)],
    )
    .await;
    cancel_at_hang(
        &fs,
        a.compact_with_replacement(replacement(), originals.clone()),
    )
    .await;
    assert_eq!(compaction_record_count(&inner).await, 1, "record applied");

    let b = acquire(Arc::new(FaultStore::new(Arc::clone(&inner), vec![])))
        .await
        .unwrap();
    assert_eq!(b.epoch().await, 2);
    assert!(read_head(&inner).await.unwrap().compaction_key.is_some());

    let err =
        expect_err(a.compact_with_replacement(replacement(), originals).await);
    assert!(matches!(err, HeadError::Fenced { .. }), "got {err:?}");
}

/// Replace HEAD's current rollup reference with a freshly-planted rollup + inventory
/// carrying the given versions (keeping the previous generation + publication times),
/// to exercise version rejection. The rollup covers the same range as the current one.
async fn plant_current_rollup(
    inner: &ObjectStoreConditional,
    rollup_version: u16,
    inv_version: u16,
) {
    let head = read_head(inner).await.unwrap();
    let cur = current_rollup(inner).await; // valid v2 rollup (gen2)
    let inv = inventory_of(inner, &cur).await;
    let inventory = InventoryIndex {
        version: inv_version,
        pipeline: PIPE.into(),
        source_id: "src".into(),
        sink_id: "sink".into(),
        start_seq: 1,
        end_seq: cur.end_seq,
        objects: inv.objects.clone(),
    };
    let wi = write_inventory(inner, PFX, &inventory).await.unwrap();
    let rec = RollupRecord {
        version: rollup_version,
        pipeline: PIPE.into(),
        source_id: "src".into(),
        sink_id: "sink".into(),
        start_seq: 1,
        end_seq: cur.end_seq,
        start_entry_hash: cur.start_entry_hash.clone(),
        end_entry_hash: cur.end_entry_hash.clone(),
        watermark_hex: cur.watermark_hex.clone(),
        object_count: inv.objects.len() as u64,
        object_digest: object_digest(&inv.objects),
        inventory_key: wi.key.clone(),
        inventory_record_hash: wi.record_hash.clone(),
        inventory_count: wi.count,
        prev: cur.prev.clone(),
    };
    let wr = write_record(inner, PFX, &rec).await.unwrap();

    let hkey = head_key_for(PFX, PIPE);
    let (_raw, etag) = inner.get_with_etag(&hkey).await.unwrap().unwrap();
    let mut h2 = head;
    h2.rollup_key = Some(wr.key);
    h2.rollup_hash = Some(wr.hash);
    inner
        .cas_put(
            &hkey,
            Bytes::from(serde_json::to_vec(&h2).unwrap()),
            etag.as_deref().unwrap(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn unsupported_version_rollup_intact_chain_recovers_via_entries() {
    // A HEAD referencing an unsupported-version current rollup on an INTACT chain:
    // rollup verification is advisory, so the bad-version rollup is not trusted and
    // recovery falls back to the retained entries and succeeds.
    let inner = inmem();
    let (_fs, w) = setup_generations(&inner).await;
    drop(w);
    plant_current_rollup(
        &inner,
        ROLLUP_RECORD_VERSION + 7,
        INVENTORY_INDEX_VERSION,
    )
    .await;

    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    acquire(clean)
        .await
        .expect("intact recovery tolerates a bad-version rollup");
}

#[tokio::test]
async fn unsupported_version_rollup_post_gc_falls_back_to_previous() {
    // Post-GC authoritative recovery: an unsupported-version current rollup is
    // rejected at load, so recovery falls back to the valid previous generation.
    let inner = inmem();
    let (_fs, w) = setup_generations(&inner).await;
    drop(w);
    plant_current_rollup(
        &inner,
        ROLLUP_RECORD_VERSION + 7,
        INVENTORY_INDEX_VERSION,
    )
    .await;
    delete_entries_le(&inner, 2).await; // GC through the horizon (prev.end = 2)

    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let w2 = acquire(Arc::clone(&clean)).await.unwrap();
    assert_eq!(w2.epoch().await, 2, "recovered via the previous generation");
}

#[tokio::test]
async fn unsupported_version_inventory_post_gc_falls_back_to_previous() {
    // The current rollup record is a supported version but its inventory index is a
    // future version: rejected at load, driving the fallback.
    let inner = inmem();
    let (_fs, w) = setup_generations(&inner).await;
    drop(w);
    plant_current_rollup(
        &inner,
        ROLLUP_RECORD_VERSION,
        INVENTORY_INDEX_VERSION + 7,
    )
    .await;
    delete_entries_le(&inner, 2).await;

    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let w2 = acquire(Arc::clone(&clean)).await.unwrap();
    assert_eq!(w2.epoch().await, 2, "recovered via the previous generation");
}

// ── 9B.2: destructive GC actors (deletion) fault matrix ───────────────────────
//
// Authorization is durable: manifest entries get immutable self-authenticating
// marks; compacted originals get an immutable content-addressed data-GC
// authorization written BEFORE any deletion. The destructive actors consume those
// durable authorizations (never an in-memory plan), re-read HEAD and fence per unit,
// re-verify the live objects, and stop on any unsafe condition. Every failure case
// proves recovery within the fallback horizon and no lost last durable copy.

fn no_window() -> GcConfig {
    GcConfig {
        safety_window_ms: 0,
    }
}

fn far_future() -> u64 {
    now_ms_test() + 1_000_000
}

async fn mark_count(inner: &ObjectStoreConditional) -> usize {
    inner
        .list(&Path::from(format!("{PFX}/{PIPE}/_manifest/gc/marks")))
        .await
        .unwrap()
        .len()
}

/// Publish 3 JSONL batches + 2 rollups but NO compaction (so there is nothing data-
/// GC-eligible unless a compaction is added).
async fn setup_no_compaction(
    inner: &Arc<ObjectStoreConditional>,
) -> (Arc<FaultStore>, DurableWriter<FaultStore>) {
    let (fs, w) = genesis(inner, vec![]).await;
    w.publish(&wm(1), vec![tobj_jsonl("orders", &[1])], 1)
        .await
        .unwrap();
    w.publish(&wm(2), vec![tobj_jsonl("orders", &[2])], 1)
        .await
        .unwrap();
    w.rollup().await.unwrap();
    w.publish(&wm(3), vec![tobj_jsonl("orders", &[3])], 1)
        .await
        .unwrap();
    w.rollup().await.unwrap();
    (fs, w)
}

/// Two cumulative generations over real JSONL objects (gen1 [1,2], gen2 [1,3]) plus a
/// compaction of all three originals, so both manifest entries (seq 1,2 <= horizon 2)
/// and data originals (all 3) are GC-eligible.
async fn setup_gc_ready(
    inner: &Arc<ObjectStoreConditional>,
    triggers: Vec<Trigger>,
) -> (Arc<FaultStore>, DurableWriter<FaultStore>) {
    let (fs, w) = genesis(inner, triggers).await;
    w.publish(&wm(1), vec![tobj_jsonl("orders", &[1])], 1)
        .await
        .unwrap();
    w.publish(&wm(2), vec![tobj_jsonl("orders", &[2])], 1)
        .await
        .unwrap();
    w.rollup().await.unwrap();
    w.publish(&wm(3), vec![tobj_jsonl("orders", &[3])], 1)
        .await
        .unwrap();
    w.rollup().await.unwrap();
    let originals = all_manifest_objects(inner).await;
    w.compact(originals).await.unwrap();
    (fs, w)
}

// ── Data-side: the HEAD-reachable equivalence-proven compaction record is the
//    authorization (no separate object). ───────────────────────────────────────

#[tokio::test]
async fn data_gc_deletes_originals_preserves_replacement_and_recovers() {
    let inner = inmem();
    let (_fs, w) = setup_gc_ready(&inner, vec![]).await;
    let plan = w.plan_gc(far_future(), no_window()).await.unwrap();
    assert_eq!(plan.data_originals_eligible().len(), 3);

    let run = w.gc_delete_originals().await.unwrap();
    assert_eq!(run.deleted, 3);
    assert!(run.stopped.is_none(), "{:?}", run.stopped);
    assert_eq!(data_count(&inner).await, 0, "originals removed");
    assert_eq!(compacted_object_count(&inner).await, 1, "replacement kept");

    drop(w);
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    acquire(clean)
        .await
        .expect("recovery succeeds after data GC");
}

#[tokio::test]
async fn data_gc_keeps_original_when_replacement_missing() {
    let inner = inmem();
    let (_fs, w) = setup_gc_ready(&inner, vec![]).await;
    let compacted: Vec<Path> = inner
        .list(&Path::from(format!("{PFX}/{PIPE}")))
        .await
        .unwrap()
        .into_iter()
        .filter(|p| p.to_string().contains("/compacted/"))
        .collect();
    inner.delete(&compacted[0]).await.unwrap();

    // With the replacement (surviving copy) gone, verification fails closed and NO
    // original is deleted - the last durable copy is never lost.
    let err = expect_err(w.gc_delete_originals().await);
    assert!(err.is_fatal(), "missing replacement fails closed: {err:?}");
    assert_eq!(data_count(&inner).await, 3, "no original deleted");
}

#[tokio::test]
async fn data_gc_crash_before_delete_then_resume() {
    let inner = inmem();
    let (fs, w) = setup_gc_ready(
        &inner,
        vec![tr(Op::OriginalDelete, 0, Action::HangBefore)],
    )
    .await;
    cancel_at_hang(&fs, async { w.gc_delete_originals().await.map(|_| ()) })
        .await;
    assert_eq!(
        data_count(&inner).await,
        3,
        "nothing deleted before the crash"
    );

    let run = w.gc_delete_originals().await.unwrap();
    assert_eq!(run.deleted, 3);
    assert_eq!(data_count(&inner).await, 0);
    drop(w);
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    acquire(clean)
        .await
        .expect("recovery after resumed data GC");
}

#[tokio::test]
async fn data_gc_survives_process_restart_after_partial_deletion() {
    // A real crash loses any in-memory state. The HEAD-reachable, equivalence-proven
    // compaction record IS the durable authorization, so a new owner (higher epoch)
    // resumes from it - no re-run of equivalence.
    let inner = inmem();
    let (_fs, w) = setup_gc_ready(
        &inner,
        vec![tr(Op::OriginalDelete, 0, Action::ApplyThenErr)],
    )
    .await;
    let run = w.gc_delete_originals().await.unwrap();
    assert!(run.stopped.is_some(), "lost response stops the first pass");

    drop(w);
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let w2 = acquire(Arc::clone(&clean)).await.unwrap();
    assert!(w2.epoch().await >= 2, "new owner bumped the epoch");
    let run2 = w2.gc_delete_originals().await.unwrap();
    assert!(run2.stopped.is_none());
    assert_eq!(
        data_count(&inner).await,
        0,
        "resumed to completion after restart"
    );
    assert_eq!(compacted_object_count(&inner).await, 1);
    drop(w2);
    let clean2 = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    acquire(clean2)
        .await
        .expect("recovery after restart-resumed data GC");
}

#[tokio::test]
async fn data_gc_fenced_before_deleting() {
    let inner = inmem();
    let (_fs, a) = setup_gc_ready(&inner, vec![]).await;
    let _b = acquire(Arc::new(FaultStore::new(Arc::clone(&inner), vec![])))
        .await
        .unwrap(); // epoch 2 fences A
    let err = expect_err(a.gc_delete_originals().await);
    assert!(matches!(err, HeadError::Fenced { .. }), "got {err:?}");
    assert_eq!(
        data_count(&inner).await,
        3,
        "a fenced writer deletes nothing"
    );
}

#[tokio::test]
async fn non_equivalent_compaction_is_refused_and_originals_retained() {
    // A compaction whose replacement is NOT row-equivalent to its originals is refused
    // at publication (never recorded as active), so its originals stay and are never
    // GC-eligible.
    let inner = inmem();
    let (_fs, w) = genesis(&inner, vec![]).await;
    w.publish(&wm(1), vec![tobj_jsonl("orders", &[1])], 1)
        .await
        .unwrap();
    w.publish(&wm(2), vec![tobj_jsonl("orders", &[2])], 1)
        .await
        .unwrap();
    w.publish(&wm(3), vec![tobj_jsonl("orders", &[3])], 1)
        .await
        .unwrap();
    let originals = all_manifest_objects(&inner).await;
    // Replacement drops a row -> not equivalent -> refused.
    let err = expect_err(
        w.compact_with_replacement(tobj_jsonl("orders", &[1, 2]), originals)
            .await,
    );
    assert!(matches!(err, HeadError::Integrity(_)), "got {err:?}");
    assert!(read_head(&inner).await.unwrap().compaction_key.is_none());
    assert_eq!(compaction_record_count(&inner).await, 0);

    let plan = w.plan_gc(far_future(), no_window()).await.unwrap();
    assert!(plan.data_originals_eligible().is_empty());
    let run = w.gc_delete_originals().await.unwrap();
    assert_eq!(run.deleted, 0);
    assert_eq!(data_count(&inner).await, 3, "originals retained");
}

#[tokio::test]
async fn forged_compaction_record_not_head_reachable_is_ignored() {
    // A canonical, correctly content-addressed compaction record (equivalence_result
    // true) that is NOT referenced from HEAD is a harmless orphan: data GC only ever
    // consumes HEAD-reachable records, so nothing is deleted.
    let inner = inmem();
    let (_fs, w) = setup_no_compaction(&inner).await;
    let originals = all_manifest_objects(&inner).await;
    let forged = CompactionRecord {
        version: super::compaction::COMPACTION_RECORD_VERSION,
        pipeline: PIPE.into(),
        source_id: "src".into(),
        sink_id: "sink".into(),
        table: "orders".into(),
        replacement: originals[0].clone(),
        originals: originals.clone(),
        equivalence_algo: EQUIVALENCE_ALGO.into(),
        equivalence_version: EQUIVALENCE_VERSION,
        equivalence_result: true,
        prev: None,
    };
    // Write it durably but NEVER reference it from HEAD.
    write_compaction_record(&*inner, PFX, &forged)
        .await
        .unwrap();

    let run = w.gc_delete_originals().await.unwrap();
    assert_eq!(run.deleted, 0, "unreferenced forged record is ignored");
    assert_eq!(data_count(&inner).await, 3, "no original deleted");
}

// ── Manifest-side (mark + expire) ─────────────────────────────────────────────

#[tokio::test]
async fn manifest_gc_mark_then_expire_deletes_entries_and_recovers() {
    let inner = inmem();
    let (_fs, w) = setup_gc_ready(&inner, vec![]).await;
    let plan = w.plan_gc(far_future(), no_window()).await.unwrap();
    let seqs: Vec<u64> = plan
        .manifest_entries_eligible()
        .iter()
        .map(|e| e.seq)
        .collect();
    assert_eq!(seqs, vec![1, 2]);

    let m = w.gc_mark_entries(far_future(), no_window()).await.unwrap();
    assert_eq!(m.marked, 2);
    assert_eq!(mark_count(&inner).await, 2);

    let e = w.gc_expire_entries().await.unwrap();
    assert_eq!(e.deleted, 2);
    assert_eq!(entry_count(&inner).await, 1, "only the tail entry remains");
    assert_eq!(rollup_record_count(&inner).await, 2);
    assert_eq!(inventory_count(&inner).await, 2);

    drop(w);
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let w2 = acquire(Arc::clone(&clean)).await.unwrap();
    assert_eq!(
        w2.epoch().await,
        2,
        "recovery via rollup after entry expiry"
    );
}

#[tokio::test]
async fn manifest_gc_expire_without_marks_deletes_nothing() {
    let inner = inmem();
    let (_fs, w) = setup_gc_ready(&inner, vec![]).await;
    let e = w.gc_expire_entries().await.unwrap();
    assert_eq!(e.deleted, 0);
    assert_eq!(entry_count(&inner).await, 3);
}

#[tokio::test]
async fn manifest_gc_crash_before_expire_then_resume() {
    let inner = inmem();
    let (fs, w) = setup_gc_ready(
        &inner,
        vec![tr(Op::EntryDelete, 0, Action::HangBefore)],
    )
    .await;
    w.gc_mark_entries(far_future(), no_window()).await.unwrap();
    cancel_at_hang(&fs, async { w.gc_expire_entries().await.map(|_| ()) })
        .await;
    assert_eq!(
        entry_count(&inner).await,
        3,
        "no entry expired before the crash"
    );

    let e = w.gc_expire_entries().await.unwrap();
    assert_eq!(e.deleted, 2);
    assert_eq!(entry_count(&inner).await, 1);
    drop(w);
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    acquire(clean)
        .await
        .expect("recovery after resumed entry expiry");
}

#[tokio::test]
async fn manifest_gc_expiry_survives_restart_and_epoch_bump() {
    // Marks are durable authorizations; a new owner (higher epoch) expires from them.
    let inner = inmem();
    let (_fs, w) = setup_gc_ready(&inner, vec![]).await;
    w.gc_mark_entries(far_future(), no_window()).await.unwrap();
    drop(w);
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let w2 = acquire(Arc::clone(&clean)).await.unwrap();
    assert!(w2.epoch().await >= 2);
    let e = w2.gc_expire_entries().await.unwrap();
    assert_eq!(e.deleted, 2, "monotonic eligibility across the epoch bump");
    assert_eq!(entry_count(&inner).await, 1);
}

#[tokio::test]
async fn manifest_gc_fenced_between_marking_and_expiry() {
    let inner = inmem();
    let (_fs, a) = setup_gc_ready(&inner, vec![]).await;
    a.gc_mark_entries(far_future(), no_window()).await.unwrap();
    let _b = acquire(Arc::new(FaultStore::new(Arc::clone(&inner), vec![])))
        .await
        .unwrap(); // epoch 2 fences A
    let err = expect_err(a.gc_expire_entries().await);
    assert!(matches!(err, HeadError::Fenced { .. }), "got {err:?}");
    assert_eq!(
        entry_count(&inner).await,
        3,
        "a fenced writer expires nothing"
    );
}

#[tokio::test]
async fn manifest_gc_malformed_mark_halts() {
    // Blocker 4: a tampered/garbage mark object must halt expiry without deleting.
    let inner = inmem();
    let (_fs, w) = setup_gc_ready(&inner, vec![]).await;
    w.gc_mark_entries(far_future(), no_window()).await.unwrap();
    // Overwrite one mark with garbage.
    let mkeys = inner
        .list(&Path::from(format!("{PFX}/{PIPE}/_manifest/gc/marks")))
        .await
        .unwrap();
    inner.delete(&mkeys[0]).await.unwrap();
    inner
        .put_if_absent(&mkeys[0], Bytes::from_static(b"not json"))
        .await
        .unwrap();

    let e = w.gc_expire_entries().await.unwrap();
    assert!(e.stopped.is_some(), "malformed mark halts");
}

#[tokio::test]
async fn manifest_gc_forged_mark_with_non_canonical_key_halts() {
    // A forged mark whose entry_key does not equal the canonical key derived from
    // (seq, entry_hash) - e.g. pointing at a rollup - must halt; nothing deleted.
    let inner = inmem();
    let (_fs, w) = setup_gc_ready(&inner, vec![]).await;
    let head = read_head(&inner).await.unwrap();
    let forged = GcMark {
        version: GC_MARK_VERSION,
        pipeline: PIPE.into(),
        source_id: "src".into(),
        sink_id: "sink".into(),
        seq: 1,
        entry_key: head.rollup_key.clone().unwrap(), // NOT the canonical entry key
        entry_hash: "eh".into(),
    };
    let key = mark_key(PFX, PIPE, &forged.entry_hash);
    inner
        .put_if_absent(&key, Bytes::from(forged.canonical_bytes()))
        .await
        .unwrap();

    let e = w.gc_expire_entries().await.unwrap();
    assert!(e.stopped.is_some(), "non-canonical mark halts");
    // The rollup the forged key pointed at is untouched.
    assert!(
        inner
            .get_with_etag(&Path::from(head.rollup_key.unwrap()))
            .await
            .unwrap()
            .is_some()
    );
}

// ── Blocker 1: exact compaction-original metadata ─────────────────────────────

#[tokio::test]
async fn compaction_original_metadata_mismatch_fails_recovery() {
    // A compaction record naming an authoritative key but a substituted content hash
    // must fail closed at recovery (and thus never authorize GC of the real object).
    let inner = inmem();
    let (_fs, w) = genesis(&inner, vec![]).await;
    w.publish(&wm(1), vec![tobj_jsonl("orders", &[1])], 1)
        .await
        .unwrap();
    w.publish(&wm(2), vec![tobj_jsonl("orders", &[2])], 1)
        .await
        .unwrap();
    let originals = all_manifest_objects(&inner).await;
    let head = read_head(&inner).await.unwrap();
    drop(w);

    // Forge a compaction record: same authoritative key, tampered content hash.
    let mut tampered = originals.clone();
    tampered[0].content_hash = "tampered".into();
    let rec = CompactionRecord {
        version: super::compaction::COMPACTION_RECORD_VERSION,
        pipeline: PIPE.into(),
        source_id: "src".into(),
        sink_id: "sink".into(),
        table: "orders".into(),
        replacement: originals[0].clone(),
        originals: tampered,
        equivalence_algo: EQUIVALENCE_ALGO.into(),
        equivalence_version: EQUIVALENCE_VERSION,
        equivalence_result: true,
        prev: None,
    };
    let wr = write_compaction_record(&*inner, PFX, &rec).await.unwrap();
    let hkey = head_key_for(PFX, PIPE);
    let (_raw, etag) = inner.get_with_etag(&hkey).await.unwrap().unwrap();
    let mut h2 = head;
    h2.compaction_key = Some(wr.key);
    h2.compaction_hash = Some(wr.hash);
    inner
        .cas_put(
            &hkey,
            Bytes::from(serde_json::to_vec(&h2).unwrap()),
            etag.as_deref().unwrap(),
        )
        .await
        .unwrap();

    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let err = expect_err(acquire(clean).await);
    assert!(
        err.is_fatal(),
        "tampered original metadata is fatal: {err:?}"
    );
}

/// Publish two JSONL batches (no compaction) and return the writer + the two
/// originals - a base for planting a crafted compaction record at HEAD.
async fn two_batches_for_compaction(
    inner: &Arc<ObjectStoreConditional>,
) -> (
    Arc<FaultStore>,
    DurableWriter<FaultStore>,
    Vec<ManifestObject>,
) {
    let (fs, w) = genesis(inner, vec![]).await;
    w.publish(&wm(1), vec![tobj_jsonl("orders", &[1])], 1)
        .await
        .unwrap();
    w.publish(&wm(2), vec![tobj_jsonl("orders", &[2])], 1)
        .await
        .unwrap();
    let originals = all_manifest_objects(inner).await;
    (fs, w, originals)
}

/// Write `rec` and point HEAD's compaction reference at it (bypassing publication),
/// to plant a crafted compaction record for recovery tests.
async fn plant_compaction_at_head(
    inner: &ObjectStoreConditional,
    rec: &CompactionRecord,
) {
    let wr = write_compaction_record(inner, PFX, rec).await.unwrap();
    let hkey = head_key_for(PFX, PIPE);
    let (raw, etag) = inner.get_with_etag(&hkey).await.unwrap().unwrap();
    let mut h: Head = serde_json::from_slice(&raw).unwrap();
    h.compaction_key = Some(wr.key);
    h.compaction_hash = Some(wr.hash);
    inner
        .cas_put(
            &hkey,
            Bytes::from(serde_json::to_vec(&h).unwrap()),
            etag.as_deref().unwrap(),
        )
        .await
        .unwrap();
}

fn crafted_record(
    originals: &[ManifestObject],
    version: u16,
    algo: &str,
    equiv_version: u16,
    result: bool,
) -> CompactionRecord {
    CompactionRecord {
        version,
        pipeline: PIPE.into(),
        source_id: "src".into(),
        sink_id: "sink".into(),
        table: "orders".into(),
        replacement: originals[0].clone(),
        originals: originals.to_vec(),
        equivalence_algo: algo.into(),
        equivalence_version: equiv_version,
        equivalence_result: result,
        prev: None,
    }
}

#[tokio::test]
async fn compaction_publish_fails_on_store_error_during_validation() {
    // A transient read failure DURING equivalence validation aborts publication with
    // HEAD unchanged and only an orphan replacement. (GetData 0,1 are the read-originals
    // step; GetData 2 is the first re-read inside the equivalence validator.)
    let inner = inmem();
    let (_fs, w, originals) =
        setup_two_batches(&inner, vec![tr(Op::GetData, 2, Action::ErrBefore)])
            .await;
    let before = read_head(&inner).await.unwrap();
    let err =
        expect_err(w.compact_with_replacement(replacement(), originals).await);
    assert!(matches!(err, HeadError::Store(_) | HeadError::Integrity(_)));
    assert_eq!(read_head(&inner).await.unwrap(), before, "HEAD unchanged");
    assert_eq!(compaction_record_count(&inner).await, 0);
}

#[tokio::test]
async fn head_reachable_non_equivalent_record_halts_recovery() {
    let inner = inmem();
    let (_fs, w, originals) = two_batches_for_compaction(&inner).await;
    drop(w);
    // A HEAD-reachable v3 record with equivalence_result = false is corruption.
    let rec = crafted_record(
        &originals,
        super::compaction::COMPACTION_RECORD_VERSION,
        EQUIVALENCE_ALGO,
        EQUIVALENCE_VERSION,
        false,
    );
    plant_compaction_at_head(&inner, &rec).await;
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let err = expect_err(acquire(clean).await);
    assert!(err.is_fatal(), "non-equivalent HEAD record halts: {err:?}");
}

#[tokio::test]
async fn unsupported_compaction_version_halts_recovery() {
    let inner = inmem();
    let (_fs, w, originals) = two_batches_for_compaction(&inner).await;
    drop(w);
    // A future record version is rejected fail-closed.
    let rec = crafted_record(
        &originals,
        super::compaction::COMPACTION_RECORD_VERSION + 5,
        EQUIVALENCE_ALGO,
        EQUIVALENCE_VERSION,
        true,
    );
    plant_compaction_at_head(&inner, &rec).await;
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let err = expect_err(acquire(clean).await);
    assert!(err.is_fatal(), "unsupported version halts: {err:?}");
}

#[tokio::test]
async fn unknown_equivalence_algorithm_halts_recovery() {
    let inner = inmem();
    let (_fs, w, originals) = two_batches_for_compaction(&inner).await;
    drop(w);
    let rec = crafted_record(
        &originals,
        super::compaction::COMPACTION_RECORD_VERSION,
        "bogus-algo",
        EQUIVALENCE_VERSION,
        true,
    );
    plant_compaction_at_head(&inner, &rec).await;
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let err = expect_err(acquire(clean).await);
    assert!(
        err.is_fatal(),
        "unknown equivalence algorithm halts: {err:?}"
    );
}

// ── Combined ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn combined_data_and_entry_gc_then_recovery_preserves_events() {
    let inner = inmem();
    let (_fs, w) = setup_gc_ready(&inner, vec![]).await;
    let d = w.gc_delete_originals().await.unwrap();
    assert_eq!(d.deleted, 3);
    w.gc_mark_entries(far_future(), no_window()).await.unwrap();
    let e = w.gc_expire_entries().await.unwrap();
    assert_eq!(e.deleted, 2);

    assert_eq!(data_count(&inner).await, 0, "originals gone");
    assert_eq!(compacted_object_count(&inner).await, 1, "replacement kept");
    assert_eq!(entry_count(&inner).await, 1, "only the tail entry remains");

    drop(w);
    let clean = Arc::new(FaultStore::new(Arc::clone(&inner), vec![]));
    let w2 = acquire(Arc::clone(&clean)).await.unwrap();
    w2.publish(&wm(5), vec![tobj_jsonl("orders", &[5])], 1)
        .await
        .unwrap();
    assert_eq!(read_head(&inner).await.unwrap().seq, 4);
}

// ── Commit 10: orphan reconciliation + metrics ────────────────────────────────

use super::reconcile::ReconcileConfig;

/// Write an unreferenced object under the pipeline (an orphan candidate).
async fn plant_orphan(inner: &ObjectStoreConditional, key: &str) {
    inner
        .put_if_absent(
            &Path::from(key.to_string()),
            Bytes::from_static(b"orphan"),
        )
        .await
        .unwrap();
}

fn recon(now_ms: u64, grace_period_ms: u64) -> ReconcileConfig {
    ReconcileConfig {
        now_ms,
        grace_period_ms,
    }
}

#[tokio::test]
async fn reconcile_deletes_orphan_past_grace_keeps_referenced() {
    let inner = inmem();
    let (_fs, w) = setup_generations(&inner).await;
    let entries_before = entry_count(&inner).await;
    let data_before = data_count(&inner).await;
    plant_orphan(&inner, &format!("{PFX}/{PIPE}/orders/wm-9999/orphan.jsonl"))
        .await;

    // now far in the future, grace 0 -> the orphan is past grace.
    let report = w
        .reconcile(recon(now_ms_test() + 1_000_000, 0))
        .await
        .unwrap();
    assert!(report.stopped.is_none(), "{:?}", report.stopped);
    assert_eq!(report.orphans_deleted, 1);
    assert!(report.missing_referenced.is_empty());
    assert_eq!(report.mpu_aborts, 0, "single-PUT path has no MPUs");
    // Referenced state is untouched.
    assert_eq!(entry_count(&inner).await, entries_before);
    assert_eq!(
        data_count(&inner).await,
        data_before,
        "referenced data kept"
    );
    assert_eq!(rollup_record_count(&inner).await, 2);
    assert_eq!(inventory_count(&inner).await, 2);
}

#[tokio::test]
async fn reconcile_keeps_orphan_within_grace() {
    let inner = inmem();
    let (_fs, w) = setup_generations(&inner).await;
    let okey = format!("{PFX}/{PIPE}/orders/wm-9999/orphan.jsonl");
    plant_orphan(&inner, &okey).await;

    // A huge grace window: a freshly-created object is never mistaken for an orphan
    // (the orphan-vs-referenced race is closed by grace).
    let report = w.reconcile(recon(now_ms_test(), u64::MAX)).await.unwrap();
    assert_eq!(report.orphans_deleted, 0);
    assert!(report.orphans_within_grace >= 1);
    assert!(
        inner
            .get_with_etag(&Path::from(okey))
            .await
            .unwrap()
            .is_some(),
        "orphan within grace is kept"
    );
}

#[tokio::test]
async fn reconcile_never_deletes_gc_marks() {
    let inner = inmem();
    let (_fs, w) = setup_gc_ready(&inner, vec![]).await;
    w.gc_mark_entries(far_future(), no_window()).await.unwrap();
    let marks_before = mark_count(&inner).await;
    assert!(marks_before > 0);
    plant_orphan(&inner, &format!("{PFX}/{PIPE}/orders/wm-9999/orphan.jsonl"))
        .await;

    let report = w
        .reconcile(recon(now_ms_test() + 1_000_000, 0))
        .await
        .unwrap();
    assert_eq!(report.orphans_deleted, 1, "only the orphan is deleted");
    assert_eq!(mark_count(&inner).await, marks_before, "GC marks kept");
}

#[tokio::test]
async fn reconcile_missing_referenced_object_is_a_hard_alarm() {
    let inner = inmem();
    let (_fs, w) = setup_generations(&inner).await;
    plant_orphan(&inner, &format!("{PFX}/{PIPE}/orders/wm-9999/orphan.jsonl"))
        .await;
    // Delete a REFERENCED (non-superseded) data object out of band.
    let referenced_obj = inner
        .list(&Path::from(format!("{PFX}/{PIPE}/orders")))
        .await
        .unwrap()
        .into_iter()
        .find(|p| {
            p.to_string().contains("/wm-") && !p.to_string().contains("9999")
        })
        .unwrap();
    inner.delete(&referenced_obj).await.unwrap();

    // Reconciliation fails closed (verify_state) and deletes nothing.
    let err =
        expect_err(w.reconcile(recon(now_ms_test() + 1_000_000, 0)).await);
    assert!(err.is_fatal(), "missing referenced object halts: {err:?}");
    assert!(
        inner
            .get_with_etag(&Path::from(format!(
                "{PFX}/{PIPE}/orders/wm-9999/orphan.jsonl"
            )))
            .await
            .unwrap()
            .is_some(),
        "no cleanup on a missing-referenced alarm"
    );
}

#[tokio::test]
async fn reconcile_fenced_stops() {
    let inner = inmem();
    let (_fs, a) = setup_generations(&inner).await;
    plant_orphan(&inner, &format!("{PFX}/{PIPE}/orders/wm-9999/orphan.jsonl"))
        .await;
    let _b = acquire(Arc::new(FaultStore::new(Arc::clone(&inner), vec![])))
        .await
        .unwrap(); // epoch 2 fences A
    let err =
        expect_err(a.reconcile(recon(now_ms_test() + 1_000_000, 0)).await);
    assert!(matches!(err, HeadError::Fenced { .. }), "got {err:?}");
}

#[tokio::test]
async fn reconcile_listing_uncertainty_stops() {
    let inner = inmem();
    let (fs, w) =
        genesis(&inner, vec![tr(Op::ListObjects, 0, Action::ErrBefore)]).await;
    // Build a couple of generations on the same fault store.
    w.publish(&wm(1), vec![tobj_jsonl("orders", &[1])], 1)
        .await
        .unwrap();
    w.publish(&wm(2), vec![tobj_jsonl("orders", &[2])], 1)
        .await
        .unwrap();
    w.rollup().await.unwrap();
    drop(fs);
    plant_orphan(&inner, &format!("{PFX}/{PIPE}/orders/wm-9999/orphan.jsonl"))
        .await;

    // The listing failpoint makes reconciliation stop rather than act on an uncertain
    // listing.
    let err =
        expect_err(w.reconcile(recon(now_ms_test() + 1_000_000, 0)).await);
    assert!(matches!(err, HeadError::Store(_)), "got {err:?}");
    assert!(
        inner
            .get_with_etag(&Path::from(format!(
                "{PFX}/{PIPE}/orders/wm-9999/orphan.jsonl"
            )))
            .await
            .unwrap()
            .is_some(),
        "no deletion under listing uncertainty"
    );
}

#[tokio::test]
async fn reconcile_lost_delete_response_is_resumable() {
    let inner = inmem();
    let (_fs, w) = setup_generations(&inner).await;
    let okey = format!("{PFX}/{PIPE}/orders/wm-9999/orphan.jsonl");
    plant_orphan(&inner, &okey).await;
    // A lost delete response (the delete applied) stops the pass.
    let (fs2, w2) = {
        let fs = Arc::new(FaultStore::new(
            Arc::clone(&inner),
            vec![tr(Op::OriginalDelete, 0, Action::ApplyThenErr)],
        ));
        let w = acquire(Arc::clone(&fs)).await.unwrap();
        (fs, w)
    };
    let _ = fs2;
    let report = w2
        .reconcile(recon(now_ms_test() + 1_000_000, 0))
        .await
        .unwrap();
    assert!(
        report.stopped.is_some(),
        "lost delete response stops the pass"
    );
    // The orphan was actually removed; a resumed pass is clean + idempotent.
    let report2 = w2
        .reconcile(recon(now_ms_test() + 1_000_000, 0))
        .await
        .unwrap();
    assert!(report2.stopped.is_none());
    assert_eq!(report2.orphans_deleted, 0, "already gone");
    assert!(
        inner
            .get_with_etag(&Path::from(okey))
            .await
            .unwrap()
            .is_none()
    );
    let _ = w; // original writer is now fenced by w2
}

#[tokio::test]
async fn metrics_snapshot_reports_durable_state() {
    let inner = inmem();
    let (_fs, w) = setup_gc_ready(&inner, vec![]).await; // 3 batches, 2 rollups, 1 compaction
    let m = w.metrics().await.unwrap();
    assert_eq!(m.seq, 3);
    assert_eq!(m.rollup_end_seq, Some(3));
    assert_eq!(m.prev_rollup_end_seq, Some(2));
    assert_eq!(m.horizon_seq, 2);
    assert_eq!(m.compaction_records, 1);
    assert_eq!(m.compaction_lag, 0, "all originals superseded");
}

#[tokio::test]
async fn metrics_counts_cas_conflicts() {
    let inner = inmem();
    // A rejected HEAD CAS on the first publish forces a reconcile retry (one conflict).
    let (_fs, w) =
        genesis(&inner, vec![tr(Op::HeadCas, 0, Action::ErrBefore)]).await;
    w.publish(&wm(1), vec![tobj_jsonl("orders", &[1])], 1)
        .await
        .unwrap();
    let m = w.metrics().await.unwrap();
    assert!(
        m.cas_conflicts >= 1,
        "cas conflict counted: {}",
        m.cas_conflicts
    );
}

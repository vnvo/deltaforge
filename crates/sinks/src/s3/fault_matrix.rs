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
use super::head::{DurableWriter, Head, HeadError, head_key_for};
use super::keys::EncodingDomain;
use super::manifest::{ManifestEntry, ManifestObject};
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
    /// Read of an original data object (compaction "read originals" step).
    GetData,
}

fn classify_put(key: &Path) -> Op {
    let k = key.to_string();
    if k.contains("_manifest/HEAD") {
        Op::HeadPut
    } else if k.contains("_manifest/entries") {
        Op::ManifestPut
    } else if k.contains("_manifest/compactions") {
        Op::CompactionRecordPut
    } else if k.contains("_manifest/rollups") {
        Op::RollupRecordPut
    } else if k.contains("/compacted/") {
        Op::CompactedPut
    } else {
        Op::DataPut
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
        self.inner.delete(key).await
    }
    async fn list(&self, prefix: &Path) -> CondResult<Vec<Path>> {
        self.inner.list(prefix).await
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
    match inner.get_with_etag(&head_key_for(PFX, PIPE)).await.unwrap() {
        Some((raw, _)) => Some(serde_json::from_slice(&raw).unwrap()),
        None => None,
    }
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
    w.publish(&wm(1), vec![tobj("orders", b"a")], 1)
        .await
        .unwrap();
    w.publish(&wm(2), vec![tobj("orders", b"b")], 1)
        .await
        .unwrap();
    let originals = all_manifest_objects(inner).await;
    assert_eq!(originals.len(), 2);
    (fs, w, originals)
}

fn replacement() -> TableObject {
    tobj("orders", b"compacted-ab")
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
    // A buggy second compaction re-lists one already-superseded original.
    let dup = vec![originals[0].clone()];
    w.compact_with_replacement(tobj("orders", b"compacted-again"), dup)
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
    // The replacement is the originals' exact bytes, concatenated in ack order.
    assert_eq!(compacted_bytes(&inner).await.as_ref(), b"ab");
}

#[tokio::test]
async fn production_compact_orders_originals_by_ack_sequence() {
    let inner = inmem();
    let (_fs, w, mut originals) = setup_two_batches(&inner, vec![]).await;
    // Hand the API the originals in the WRONG order; it must re-order by ack seq.
    originals.reverse();
    w.compact(originals).await.unwrap();
    assert_eq!(
        compacted_bytes(&inner).await.as_ref(),
        b"ab",
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
    w.publish(&wm(3), vec![tobj("orders", b"c")], 1)
        .await
        .unwrap();
    w.rollup().await.unwrap();
    let h = read_head(&inner).await.unwrap();
    let (cur, prev) = (h.rollup_key.clone(), h.prev_rollup_key.clone());
    assert!(cur.is_some() && prev.is_some());

    // An ordinary publish keeps both rollup references.
    w.publish(&wm(4), vec![tobj("orders", b"d")], 1)
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

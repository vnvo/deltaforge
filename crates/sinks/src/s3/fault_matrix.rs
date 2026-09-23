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
}

fn classify_put(key: &Path) -> Op {
    let k = key.to_string();
    if k.contains("_manifest/HEAD") {
        Op::HeadPut
    } else if k.contains("_manifest/entries") {
        Op::ManifestPut
    } else {
        Op::DataPut
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
        domain: EncodingDomain::new("jsonl", 1, "s1"),
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

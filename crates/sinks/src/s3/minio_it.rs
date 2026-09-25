//! Live MinIO / S3 integration tests for the durable S3 path (P0.4, Commit 11).
//!
//! These are `#[ignore]`d, so they never run in a normal `cargo test`. When run
//! EXPLICITLY (the command below), they REQUIRE a real S3-compatible backend and
//! fail loudly if it is not configured: a missing env var or a store that will not
//! construct panics rather than passing vacuously. A selected live test therefore
//! either exercises the provider or fails - it can never report a green run while
//! testing nothing, and it cannot hide invalid credentials or a malformed endpoint.
//!
//! They exercise ACTUAL provider behavior - conditional writes, ETag CAS, listing
//! timestamps, deletes - which the in-memory `FaultStore` suite cannot prove:
//! capability probing, publish/ack + restart recovery, concurrent-writer fencing, CAS
//! conflicts, cumulative rollups + fallback, compaction, both GC domains, orphan
//! reconciliation, and end-to-end recoverability after combined compaction + entry
//! expiry + original deletion + restart.
//!
//! Run (with MinIO up and a bucket created):
//! ```text
//! export DELTAFORGE_IT_S3_ENDPOINT=http://localhost:9000
//! export DELTAFORGE_IT_S3_BUCKET=deltaforge-it
//! export DELTAFORGE_IT_S3_ACCESS_KEY=minioadmin
//! export DELTAFORGE_IT_S3_SECRET_KEY=minioadmin
//! cargo test -p sinks --lib -- --ignored minio
//! ```
//! Lost/ambiguous provider responses cannot be forced deterministically against a real
//! backend; that boundary is covered by the injected in-memory fault matrix.

#![cfg(test)]

use std::sync::Arc;

use bytes::Bytes;
use deltaforge_core::{CheckpointComparator, CheckpointOrder};
use object_store::path::Path;

use super::batch_upload::TableObject;
use super::gc::GcConfig;
use super::head::{DurableWriter, HeadError};
use super::keys::EncodingDomain;
use super::manifest::{ManifestEntry, ManifestObject};
use super::object_writer::{ObjectStoreParams, build_object_store};
use super::reconcile::ReconcileConfig;
use super::store_cond::{
    ConditionalStore, ObjectStoreConditional, probe_conditional_writes,
};

const PIPE: &str = "pipe";

struct MonoCmp;
impl CheckpointComparator for MonoCmp {
    fn order(&self, a: &[u8], b: &[u8]) -> CheckpointOrder {
        fn parse(x: &[u8]) -> Option<u64> {
            (x.len() == 9).then(|| {
                let mut p = [0u8; 8];
                p.copy_from_slice(&x[1..9]);
                u64::from_be_bytes(p)
            })
        }
        match (parse(a), parse(b)) {
            (Some(x), Some(y)) => match x.cmp(&y) {
                std::cmp::Ordering::Less => CheckpointOrder::Before,
                std::cmp::Ordering::Equal => CheckpointOrder::Equal,
                std::cmp::Ordering::Greater => CheckpointOrder::After,
            },
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

/// A required env var, or a loud panic naming it. Because these tests are
/// `#[ignore]`d, reaching here means the operator asked to run them explicitly, so a
/// missing variable is a configuration error, never a reason to pass vacuously.
fn require_env(name: &str) -> String {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => v,
        _ => panic!(
            "live MinIO/S3 test requires env var {name} to be set to a non-empty \
             value. These tests are #[ignore]d; running them explicitly must \
             exercise a real backend. See this module's docs for the full set."
        ),
    }
}

/// Build a `ConditionalStore` over the configured MinIO/S3 backend. Panics loudly if
/// any required variable is missing or the store cannot be constructed - a selected
/// live test must exercise the provider or fail, never pass without testing anything.
fn it_store() -> Arc<ObjectStoreConditional> {
    let params = ObjectStoreParams {
        bucket: require_env("DELTAFORGE_IT_S3_BUCKET"),
        endpoint: Some(require_env("DELTAFORGE_IT_S3_ENDPOINT")),
        region: Some(
            std::env::var("DELTAFORGE_IT_S3_REGION")
                .unwrap_or_else(|_| "us-east-1".into()),
        ),
        access_key_id: Some(require_env("DELTAFORGE_IT_S3_ACCESS_KEY")),
        secret_access_key: Some(require_env("DELTAFORGE_IT_S3_SECRET_KEY")),
        virtual_hosted_style: false,
        local: false,
    };
    let store = build_object_store(&params).expect(
        "construct live S3/MinIO object store from DELTAFORGE_IT_S3_* env vars",
    );
    Arc::new(ObjectStoreConditional::new(store))
}

/// A fresh unique prefix per test, so runs never collide on a shared bucket.
fn unique_prefix() -> String {
    static N: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("it/{}-{n}", now_ms())
}

async fn acquire(
    store: Arc<ObjectStoreConditional>,
    prefix: &str,
) -> Result<DurableWriter<ObjectStoreConditional>, HeadError> {
    DurableWriter::acquire(store, prefix, PIPE, "src", "sink", cmp()).await
}

/// Every `ManifestObject` referenced by the retained entries, read straight from the
/// store (for driving compaction in the integration lifecycle).
async fn all_objects(
    store: &ObjectStoreConditional,
    prefix: &str,
) -> Vec<ManifestObject> {
    let mut out = Vec::new();
    for k in store
        .list(&Path::from(format!("{prefix}/{PIPE}/_manifest/entries")))
        .await
        .unwrap()
    {
        let (raw, _) = store.get_with_etag(&k).await.unwrap().unwrap();
        let e: ManifestEntry = serde_json::from_slice(&raw).unwrap();
        out.extend(e.objects);
    }
    out
}

async fn data_count(store: &ObjectStoreConditional, prefix: &str) -> usize {
    store
        .list(&Path::from(format!("{prefix}/{PIPE}")))
        .await
        .unwrap()
        .iter()
        .filter(|p| p.to_string().contains("/wm-"))
        .count()
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[tokio::test]
#[ignore = "requires a live MinIO/S3 backend (DELTAFORGE_IT_S3_*)"]
async fn minio_conditional_write_probe_passes() {
    let store = it_store();
    let prefix = unique_prefix();
    // Real conditional-write + ETag capability parity on the live backend.
    probe_conditional_writes(store.as_ref(), &prefix)
        .await
        .expect("MinIO/S3 honors create-only + CAS conditions");
}

#[tokio::test]
#[ignore = "requires a live MinIO/S3 backend (DELTAFORGE_IT_S3_*)"]
async fn minio_publish_ack_and_recover() {
    let store = it_store();
    let prefix = unique_prefix();
    let w = acquire(Arc::clone(&store), &prefix).await.unwrap();
    w.publish(&wm(1), vec![tobj_jsonl("orders", &[1])], 1)
        .await
        .unwrap();
    w.publish(&wm(2), vec![tobj_jsonl("orders", &[2])], 1)
        .await
        .unwrap();
    drop(w);
    // Restart: recovery verifies the real chain + ETag'd HEAD and bumps the epoch.
    let w2 = acquire(Arc::clone(&store), &prefix).await.unwrap();
    assert_eq!(w2.epoch().await, 2);
    // Replaying the last watermark is Equal -> acknowledged without a new entry.
    w2.publish(&wm(2), vec![tobj_jsonl("orders", &[2])], 1)
        .await
        .unwrap();
    assert_eq!(w2.metrics().await.unwrap().seq, 2);
}

#[tokio::test]
#[ignore = "requires a live MinIO/S3 backend (DELTAFORGE_IT_S3_*)"]
async fn minio_concurrent_writers_fence() {
    let store = it_store();
    let prefix = unique_prefix();
    let a = acquire(Arc::clone(&store), &prefix).await.unwrap();
    let b = acquire(Arc::clone(&store), &prefix).await.unwrap();
    assert_eq!(b.epoch().await, 2);
    // A's HEAD CAS finds B's higher epoch and A is permanently fenced.
    let err = a
        .publish(&wm(1), vec![tobj_jsonl("orders", &[1])], 1)
        .await
        .expect_err("stale writer fenced");
    assert!(matches!(err, HeadError::Fenced { .. }), "got {err:?}");
    b.publish(&wm(1), vec![tobj_jsonl("orders", &[1])], 1)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires a live MinIO/S3 backend (DELTAFORGE_IT_S3_*)"]
async fn minio_rollup_fallback_after_gc() {
    let store = it_store();
    let prefix = unique_prefix();
    let w = acquire(Arc::clone(&store), &prefix).await.unwrap();
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
    drop(w);
    // Simulate authorized entry GC (delete entries at/below the horizon = 2) and
    // confirm recovery reconstructs from the current cumulative rollup + inventory.
    for k in store
        .list(&Path::from(format!("{prefix}/{PIPE}/_manifest/entries")))
        .await
        .unwrap()
    {
        let (raw, _) = store.get_with_etag(&k).await.unwrap().unwrap();
        let e: ManifestEntry = serde_json::from_slice(&raw).unwrap();
        if e.seq <= 2 {
            store.delete(&k).await.unwrap();
        }
    }
    let w2 = acquire(Arc::clone(&store), &prefix).await.unwrap();
    assert!(w2.epoch().await >= 2, "recovered from rollup after GC");
}

#[tokio::test]
#[ignore = "requires a live MinIO/S3 backend (DELTAFORGE_IT_S3_*)"]
async fn minio_full_lifecycle_recoverable_after_combined_gc() {
    let store = it_store();
    let prefix = unique_prefix();
    let w = acquire(Arc::clone(&store), &prefix).await.unwrap();
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
    // Compaction (real equivalence over real objects).
    w.compact(all_objects(&store, &prefix).await).await.unwrap();
    // Data GC: delete the superseded originals; the replacement stays.
    let del = w.gc_delete_originals().await.unwrap();
    assert!(del.stopped.is_none(), "{:?}", del.stopped);
    assert!(del.deleted >= 1, "superseded originals removed");
    // Manifest GC: mark + expire the entries at/below the horizon.
    let cfg = GcConfig {
        safety_window_ms: 0,
    };
    w.gc_mark_entries(now_ms() + 1_000_000, cfg).await.unwrap();
    w.gc_expire_entries().await.unwrap();
    drop(w);

    // Restart: acknowledged events remain recoverable after combined compaction,
    // entry expiry, and original deletion.
    let w2 = acquire(Arc::clone(&store), &prefix).await.unwrap();
    w2.publish(&wm(4), vec![tobj_jsonl("orders", &[4])], 1)
        .await
        .unwrap();
    assert_eq!(w2.metrics().await.unwrap().seq, 4);
}

#[tokio::test]
#[ignore = "requires a live MinIO/S3 backend (DELTAFORGE_IT_S3_*)"]
async fn minio_reconcile_deletes_orphan_after_grace() {
    let store = it_store();
    let prefix = unique_prefix();
    let w = acquire(Arc::clone(&store), &prefix).await.unwrap();
    w.publish(&wm(1), vec![tobj_jsonl("orders", &[1])], 1)
        .await
        .unwrap();
    w.rollup().await.unwrap();
    // Plant an unreferenced object (uses the real listing timestamp for grace).
    let orphan = Path::from(format!("{prefix}/{PIPE}/orders/wm-9999/x.jsonl"));
    store
        .put_if_absent(&orphan, Bytes::from_static(b"orphan"))
        .await
        .unwrap();
    let before = data_count(&store, &prefix).await;
    let report = w
        .reconcile(ReconcileConfig {
            now_ms: now_ms() + 1_000_000,
            grace_period_ms: 0,
        })
        .await
        .unwrap();
    assert!(report.stopped.is_none(), "{:?}", report.stopped);
    assert_eq!(report.orphans_deleted, 1);
    assert_eq!(data_count(&store, &prefix).await, before - 1);
}

//! Live S3-server integration tests for the durable S3 path.
//!
//! These are `#[ignore]`d, so they never run in a normal `cargo test`. When run
//! EXPLICITLY they always exercise a real S3 server: with no `DELTAFORGE_IT_S3_*`
//! variable set they start the pinned S3 test server (`s3-test-server`, the core
//! gate lane `sinks/lib-s3-server-it`); with the full set they use that external
//! server. A partial set, or a store that will not construct, panics rather than
//! passing vacuously or silently switching servers.
//!
//! They exercise ACTUAL provider behavior - conditional writes, ETag CAS, listing
//! timestamps, deletes - which the in-memory `FaultStore` suite cannot prove:
//! capability probing, publish/ack + restart recovery, concurrent-writer fencing, CAS
//! conflicts, cumulative rollups + fallback, compaction, both GC domains, orphan
//! reconciliation, and end-to-end recoverability after combined compaction + entry
//! expiry + original deletion + restart.
//!
//! Run against the pinned server:
//! ```text
//! cargo test -p sinks --lib -- --include-ignored --test-threads=1 s3_server_it
//! ```
//! or against an external server with a bucket created (see
//! `docs/src/sinks/s3-test-backends.md`):
//! ```text
//! export DELTAFORGE_IT_S3_ENDPOINT=<endpoint>
//! export DELTAFORGE_IT_S3_BUCKET=<bucket>
//! export DELTAFORGE_IT_S3_ACCESS_KEY=<access key>
//! export DELTAFORGE_IT_S3_SECRET_KEY=<secret key>
//! export DELTAFORGE_IT_S3_REGION=<region>   # optional, default us-east-1
//! cargo test -p sinks --lib -- --include-ignored --test-threads=1 s3_server_it
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

/// The external-server variables. All four required ones set: run against
/// that server. None set: start the pinned S3 test server. Anything in
/// between is refused, so a typo can never silently redirect the run.
const IT_REQUIRED: [&str; 4] = [
    "DELTAFORGE_IT_S3_ENDPOINT",
    "DELTAFORGE_IT_S3_BUCKET",
    "DELTAFORGE_IT_S3_ACCESS_KEY",
    "DELTAFORGE_IT_S3_SECRET_KEY",
];
const IT_REGION: &str = "DELTAFORGE_IT_S3_REGION";
const PINNED_BUCKET: &str = "deltaforge-it";

#[ctor::dtor]
fn cleanup_server() {
    s3_test_server::remove_shared();
}

/// Where the live tests run, from the `DELTAFORGE_IT_S3_*` variables
/// (`None` = the pinned server).
fn it_target(
    var: impl Fn(&str) -> Option<String>,
) -> Result<Option<ObjectStoreParams>, String> {
    let var = |name: &str| var(name).filter(|v| !v.is_empty());
    let set: Vec<_> = IT_REQUIRED.iter().filter(|n| var(n).is_some()).collect();
    if set.is_empty() && var(IT_REGION).is_none() {
        return Ok(None);
    }
    if set.len() < IT_REQUIRED.len() {
        let missing: Vec<_> =
            IT_REQUIRED.iter().filter(|n| var(n).is_none()).collect();
        return Err(format!(
            "partial DELTAFORGE_IT_S3_* configuration: missing {missing:?}. Set \
             all of {IT_REQUIRED:?} for an external server, or none to use the \
             pinned S3 test server"
        ));
    }
    let get = |name: &str| var(name).expect("checked above");
    Ok(Some(ObjectStoreParams {
        bucket: get("DELTAFORGE_IT_S3_BUCKET"),
        endpoint: Some(get("DELTAFORGE_IT_S3_ENDPOINT")),
        region: Some(var(IT_REGION).unwrap_or_else(|| "us-east-1".into())),
        access_key_id: Some(get("DELTAFORGE_IT_S3_ACCESS_KEY")),
        secret_access_key: Some(get("DELTAFORGE_IT_S3_SECRET_KEY")),
        session_token: None,
        virtual_hosted_style: false,
        local: false,
    }))
}

/// A `ConditionalStore` over the configured external server, or over the
/// pinned S3 test server when none is configured. Panics on a partial
/// configuration or a store that cannot be built: a selected live test must
/// exercise a real server or fail, never pass without testing anything.
async fn it_store() -> Arc<ObjectStoreConditional> {
    let params = match it_target(|n| std::env::var(n).ok()) {
        Ok(Some(params)) => params,
        Ok(None) => {
            static BUCKET_READY: tokio::sync::OnceCell<()> =
                tokio::sync::OnceCell::const_new();
            let server = s3_test_server::shared().await;
            BUCKET_READY
                .get_or_init(|| async {
                    server.create_bucket(PINNED_BUCKET).await.expect(
                        "create the bucket on the pinned S3 test server",
                    )
                })
                .await;
            ObjectStoreParams::s3_compatible(
                PINNED_BUCKET,
                server.endpoint.clone(),
                s3_test_server::ACCESS_KEY,
                s3_test_server::SECRET_KEY,
            )
        }
        Err(e) => panic!("{e}"),
    };
    let store = build_object_store(&params)
        .expect("construct the live S3 object store");
    Arc::new(ObjectStoreConditional::new(store))
}

#[test]
fn it_target_is_all_or_nothing() {
    use std::collections::HashMap;
    let env = |pairs: &[(&str, &str)]| {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name: &str| map.get(name).cloned()
    };
    assert!(matches!(it_target(env(&[])), Ok(None)));
    let full = [
        ("DELTAFORGE_IT_S3_ENDPOINT", "http://s3:9000"),
        ("DELTAFORGE_IT_S3_BUCKET", "b"),
        ("DELTAFORGE_IT_S3_ACCESS_KEY", "k"),
        ("DELTAFORGE_IT_S3_SECRET_KEY", "s"),
    ];
    let params = it_target(env(&full)).unwrap().expect("external server");
    assert_eq!(params.endpoint.as_deref(), Some("http://s3:9000"));
    assert_eq!(params.bucket, "b");
    for skip in 0..full.len() {
        let partial: Vec<_> = full
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != skip)
            .map(|(_, kv)| *kv)
            .collect();
        let err = it_target(env(&partial)).unwrap_err();
        assert!(err.contains(full[skip].0), "{err}");
    }
    let mut blank = full.to_vec();
    blank[1].1 = "";
    assert!(
        it_target(env(&blank)).is_err(),
        "an empty value counts as unset"
    );
    assert!(
        it_target(env(&[(IT_REGION, "eu-west-1")])).is_err(),
        "a region alone is a partial configuration"
    );
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
#[ignore = "live S3 server: the pinned one, or DELTAFORGE_IT_S3_*"]
async fn s3_server_conditional_write_probe_passes() {
    let store = it_store().await;
    let prefix = unique_prefix();
    // Real conditional-write + ETag capability parity on the live backend.
    probe_conditional_writes(store.as_ref(), &prefix)
        .await
        .expect("the S3 server honors create-only + CAS conditions");
}

#[tokio::test]
#[ignore = "live S3 server: the pinned one, or DELTAFORGE_IT_S3_*"]
async fn s3_server_publish_ack_and_recover() {
    let store = it_store().await;
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
#[ignore = "live S3 server: the pinned one, or DELTAFORGE_IT_S3_*"]
async fn s3_server_concurrent_writers_fence() {
    let store = it_store().await;
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
#[ignore = "live S3 server: the pinned one, or DELTAFORGE_IT_S3_*"]
async fn s3_server_rollup_fallback_after_gc() {
    let store = it_store().await;
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
#[ignore = "live S3 server: the pinned one, or DELTAFORGE_IT_S3_*"]
async fn s3_server_full_lifecycle_recoverable_after_combined_gc() {
    let store = it_store().await;
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
#[ignore = "live S3 server: the pinned one, or DELTAFORGE_IT_S3_*"]
async fn s3_server_reconcile_deletes_orphan_after_grace() {
    let store = it_store().await;
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

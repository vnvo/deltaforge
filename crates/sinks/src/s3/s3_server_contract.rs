//! The S3 behavior DeltaForge depends on, checked against the pinned S3 test
//! server (`s3-test-server`; see `docs/src/sinks/s3-test-backends.md`).
//!
//! A server is adopted as a test backend only while every check here passes:
//! conditional PUT (`If-None-Match: *`, `If-Match`) with the exact 412/404 wire
//! statuses AWS returns, stable ETags for the durable_v2 HEAD CAS, PUT/GET/
//! HEAD/LIST/DELETE, multipart complete and abort, zero-byte objects,
//! read-after-write visibility, persistence across a server restart, and
//! exactly one winner among concurrent conditional writers. The conditional
//! checks go through the production [`ObjectStoreConditional`]; the wire
//! checks send signed requests so the raw status is asserted, not a client
//! library's reading of it.
//!
//! `#[ignore]`d (each run starts a container):
//! `cargo test -p sinks --lib -- --include-ignored s3_server_contract`.

#![cfg(test)]

use std::sync::Arc;

use bytes::Bytes;
use ctor::dtor;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use reqwest::Method;
use s3_test_server::S3Server;
use tokio::sync::{Barrier, OnceCell};

use super::object_writer::{ObjectStoreParams, build_object_store};
use super::store_cond::{
    ConditionalStore, ObjectStoreConditional, PutOutcome,
    probe_conditional_writes,
};

const BUCKET: &str = "deltaforge-contract";
const WRITERS: usize = 16;
const ROUNDS: usize = 20;

#[dtor]
fn cleanup_server() {
    s3_test_server::remove_shared();
}

async fn server() -> &'static S3Server {
    static BUCKET_READY: OnceCell<()> = OnceCell::const_new();
    let server = s3_test_server::shared().await;
    BUCKET_READY
        .get_or_init(|| async {
            server
                .create_bucket(BUCKET)
                .await
                .expect("create the bucket")
        })
        .await;
    server
}

fn store(endpoint: &str) -> Arc<dyn ObjectStore> {
    build_object_store(&ObjectStoreParams::s3_compatible(
        BUCKET,
        endpoint,
        s3_test_server::ACCESS_KEY,
        s3_test_server::SECRET_KEY,
    ))
    .expect("build the object store")
}

fn cond(endpoint: &str) -> ObjectStoreConditional {
    ObjectStoreConditional::new(store(endpoint))
}

fn prefix(name: &str) -> String {
    format!("contract/{name}/{}", uuid::Uuid::new_v4())
}

/// One signed request for `key` in the bucket: (status, ETag header, body).
async fn wire(
    endpoint: &str,
    method: Method,
    key: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> (u16, Option<String>, String) {
    let resp = s3_test_server::signed_request(
        endpoint,
        method,
        &format!("/{BUCKET}/{key}"),
        &[],
        headers,
        body.to_vec(),
    )
    .await
    .expect("send the request");
    let status = resp.status().as_u16();
    let etag = resp
        .headers()
        .get("etag")
        .map(|v| v.to_str().expect("ascii etag").to_string());
    (status, etag, resp.text().await.expect("read the body"))
}

async fn get_bytes(store: &Arc<dyn ObjectStore>, key: &Path) -> Bytes {
    store
        .get(key)
        .await
        .expect("get")
        .bytes()
        .await
        .expect("read")
}

async fn listed(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
) -> Vec<object_store::ObjectMeta> {
    let mut out: Vec<_> = store
        .list(Some(&Path::from(prefix)))
        .try_collect()
        .await
        .expect("list");
    out.sort_by(|a, b| a.location.cmp(&b.location));
    out
}

fn written_etag(outcome: PutOutcome) -> String {
    match outcome {
        PutOutcome::Written { etag: Some(etag) } => etag,
        other => panic!("expected a write with an ETag, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts the pinned S3 test server (docker)"]
async fn s3_server_contract_wire_conditional_put_statuses() {
    let ep = &server().await.endpoint;
    let key = format!("{}/obj", prefix("wire"));

    let (status, e1, _) = wire(ep, Method::PUT, &key, &[], b"a").await;
    assert_eq!(status, 200, "unconditional PUT");
    let e1 = e1.expect("PUT returns an ETag");

    let (status, _, body) =
        wire(ep, Method::PUT, &key, &[("if-none-match", "*")], b"x").await;
    assert_eq!(status, 412, "If-None-Match: * on an existing key: {body}");
    assert!(body.contains("PreconditionFailed"), "error code: {body}");

    let (status, _, body) = wire(
        ep,
        Method::PUT,
        &key,
        &[("if-match", "\"0123456789abcdef0123456789abcdef\"")],
        b"x",
    )
    .await;
    assert_eq!(status, 412, "If-Match with a wrong ETag: {body}");

    let (status, _, body) = wire(ep, Method::GET, &key, &[], b"").await;
    assert_eq!(
        (status, body.as_str()),
        (200, "a"),
        "rejected PUTs wrote nothing"
    );

    let (status, e2, _) =
        wire(ep, Method::PUT, &key, &[("if-match", &e1)], b"b").await;
    assert_eq!(status, 200, "If-Match with the current ETag");
    let e2 = e2.expect("PUT returns an ETag");
    assert_ne!(e1, e2, "a replaced object has a new ETag");

    let (status, _, body) =
        wire(ep, Method::PUT, &key, &[("if-match", &e1)], b"c").await;
    assert_eq!(status, 412, "If-Match with a stale ETag: {body}");

    let missing = format!("{}/missing", prefix("wire"));
    let (status, _, body) =
        wire(ep, Method::PUT, &missing, &[("if-match", &e1)], b"c").await;
    assert_eq!(status, 404, "If-Match on a missing key: {body}");
    assert!(body.contains("NoSuchKey"), "error code: {body}");

    let (status, _, _) =
        wire(ep, Method::PUT, &missing, &[("if-none-match", "*")], b"d").await;
    assert_eq!(status, 200, "If-None-Match: * on a missing key");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts the pinned S3 test server (docker)"]
async fn s3_server_contract_production_conditional_path() {
    let ep = &server().await.endpoint;
    let p = prefix("cond");
    let c = cond(ep);
    let key = Path::from(format!("{p}/head"));

    let e1 = written_etag(
        c.put_if_absent(&key, Bytes::from_static(b"one"))
            .await
            .unwrap(),
    );
    assert_eq!(
        c.put_if_absent(&key, Bytes::from_static(b"two"))
            .await
            .unwrap(),
        PutOutcome::AlreadyExists
    );
    let (bytes, etag) = c.get_with_etag(&key).await.unwrap().unwrap();
    assert_eq!((bytes.as_ref(), etag.as_deref()), (&b"one"[..], Some(&*e1)));

    let e2 = written_etag(
        c.cas_put(&key, Bytes::from_static(b"two"), &e1)
            .await
            .unwrap(),
    );
    assert_ne!(e1, e2);
    assert_eq!(
        c.cas_put(&key, Bytes::from_static(b"three"), &e1)
            .await
            .unwrap(),
        PutOutcome::Conflict,
        "a stale ETag loses"
    );
    let missing = Path::from(format!("{p}/missing"));
    assert_eq!(
        c.cas_put(&missing, Bytes::from_static(b"x"), &e2)
            .await
            .unwrap(),
        PutOutcome::Conflict,
        "CAS on a missing key loses"
    );
    let (bytes, _) = c.get_with_etag(&key).await.unwrap().unwrap();
    assert_eq!(bytes.as_ref(), b"two");

    let probe = format!("{p}/probe");
    probe_conditional_writes(&c, &probe)
        .await
        .expect("the probe passes");
    assert!(
        c.list(&Path::from(probe)).await.unwrap().is_empty(),
        "the probe leaves no trace"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts the pinned S3 test server (docker)"]
async fn s3_server_contract_etags_are_stable() {
    let ep = &server().await.endpoint;
    let p = prefix("etag");
    let s = store(ep);
    let key = Path::from(format!("{p}/head"));

    let put = s.put(&key, PutPayload::from_static(b"head")).await.unwrap();
    let etag = put.e_tag.expect("PUT returns an ETag");
    for _ in 0..5 {
        assert_eq!(s.head(&key).await.unwrap().e_tag.as_deref(), Some(&*etag));
        let got = s.get(&key).await.unwrap();
        assert_eq!(got.meta.e_tag.as_deref(), Some(&*etag));
        let list = listed(&s, &p).await;
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].e_tag.as_deref(), Some(&*etag));
        let (status, wire_etag, _) =
            wire(ep, Method::HEAD, key.as_ref(), &[], b"").await;
        assert_eq!((status, wire_etag.as_deref()), (200, Some(&*etag)));
    }
    let c = cond(ep);
    written_etag(
        c.cas_put(&key, Bytes::from_static(b"next"), &etag)
            .await
            .unwrap(),
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts the pinned S3 test server (docker)"]
async fn s3_server_contract_put_get_head_list_delete() {
    let ep = &server().await.endpoint;
    let p = prefix("crud");
    let s = store(ep);
    let a = Path::from(format!("{p}/a"));
    let b = Path::from(format!("{p}/sub/b"));

    s.put(&a, PutPayload::from_static(b"alpha")).await.unwrap();
    s.put(&b, PutPayload::from_static(b"bravo!")).await.unwrap();
    assert_eq!(get_bytes(&s, &a).await.as_ref(), b"alpha");
    assert_eq!(s.head(&b).await.unwrap().size, 6);
    let list = listed(&s, &p).await;
    let keys: Vec<_> = list.iter().map(|m| m.location.clone()).collect();
    assert_eq!(keys, vec![a.clone(), b.clone()]);
    assert_eq!((list[0].size, list[1].size), (5, 6));

    s.delete(&a).await.unwrap();
    assert!(matches!(
        s.get(&a).await,
        Err(object_store::Error::NotFound { .. })
    ));
    assert!(matches!(
        s.head(&a).await,
        Err(object_store::Error::NotFound { .. })
    ));
    let keys: Vec<_> = listed(&s, &p)
        .await
        .into_iter()
        .map(|m| m.location)
        .collect();
    assert_eq!(keys, vec![b]);

    let (status, _, body) =
        wire(ep, Method::DELETE, a.as_ref(), &[], b"").await;
    assert_eq!(status, 204, "DELETE of a missing key: {body}");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts the pinned S3 test server (docker)"]
async fn s3_server_contract_multipart_complete_and_abort() {
    let ep = &server().await.endpoint;
    let p = prefix("multipart");
    let s = store(ep);

    let key = Path::from(format!("{p}/complete"));
    let part1 = vec![b'x'; 5 * 1024 * 1024];
    let part2 = b"tail".to_vec();
    let mut upload = s.put_multipart(&key).await.unwrap();
    upload
        .put_part(PutPayload::from(part1.clone()))
        .await
        .unwrap();
    upload
        .put_part(PutPayload::from(part2.clone()))
        .await
        .unwrap();
    upload.complete().await.unwrap();
    let got = get_bytes(&s, &key).await;
    assert_eq!(got.len(), part1.len() + part2.len());
    assert!(got.starts_with(&part1) && got.ends_with(&part2));

    let aborted = Path::from(format!("{p}/aborted"));
    let mut upload = s.put_multipart(&aborted).await.unwrap();
    upload.put_part(PutPayload::from(part1)).await.unwrap();
    upload.abort().await.unwrap();
    assert!(matches!(
        s.head(&aborted).await,
        Err(object_store::Error::NotFound { .. })
    ));
    let keys: Vec<_> = listed(&s, &p)
        .await
        .into_iter()
        .map(|m| m.location)
        .collect();
    assert_eq!(keys, vec![key], "an aborted upload is never visible");

    let resp = s3_test_server::signed_request(
        ep,
        Method::GET,
        &format!("/{BUCKET}"),
        &[("uploads", ""), ("prefix", &p)],
        &[],
        Vec::new(),
    )
    .await
    .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let body = resp.text().await.unwrap();
    assert!(
        !body.contains("<UploadId>"),
        "no upload remains open: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts the pinned S3 test server (docker)"]
async fn s3_server_contract_zero_byte_objects() {
    let ep = &server().await.endpoint;
    let p = prefix("empty");
    let s = store(ep);
    let key = Path::from(format!("{p}/plain"));

    s.put(&key, PutPayload::default()).await.unwrap();
    assert_eq!(s.head(&key).await.unwrap().size, 0);
    assert!(get_bytes(&s, &key).await.is_empty());
    let created = Path::from(format!("{p}/created"));
    written_etag(
        cond(ep)
            .put_if_absent(&created, Bytes::new())
            .await
            .unwrap(),
    );
    let list = listed(&s, &p).await;
    assert_eq!(list.len(), 2);
    assert!(list.iter().all(|m| m.size == 0));
    let (status, _, body) =
        wire(ep, Method::GET, created.as_ref(), &[], b"").await;
    assert_eq!((status, body.as_str()), (200, ""));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts the pinned S3 test server (docker)"]
async fn s3_server_contract_read_after_write() {
    let ep = &server().await.endpoint;
    let p = prefix("raw");
    let s = store(ep);
    let key = Path::from(format!("{p}/obj"));

    for i in 0..50u32 {
        let body = format!("version-{i}");
        let put = s.put(&key, PutPayload::from(body.clone())).await.unwrap();
        assert_eq!(get_bytes(&s, &key).await.as_ref(), body.as_bytes());
        assert_eq!(s.head(&key).await.unwrap().e_tag, put.e_tag);
        assert_eq!(listed(&s, &p).await.len(), 1);
    }
    s.delete(&key).await.unwrap();
    assert!(matches!(
        s.get(&key).await,
        Err(object_store::Error::NotFound { .. })
    ));
    assert!(listed(&s, &p).await.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts the pinned S3 test server (docker)"]
async fn s3_server_contract_persists_across_restart() {
    let mut own = S3Server::start().await.unwrap();
    own.create_bucket(BUCKET).await.unwrap();
    let p = prefix("restart");
    let head = Path::from(format!("{p}/head"));
    let empty = Path::from(format!("{p}/empty"));

    let c = cond(&own.endpoint);
    let etag = written_etag(
        c.put_if_absent(&head, Bytes::from_static(b"durable"))
            .await
            .unwrap(),
    );
    written_etag(c.put_if_absent(&empty, Bytes::new()).await.unwrap());

    own.restart().await.unwrap();

    let c = cond(&own.endpoint);
    let (bytes, after) = c.get_with_etag(&head).await.unwrap().unwrap();
    assert_eq!(bytes.as_ref(), b"durable");
    assert_eq!(
        after.as_deref(),
        Some(&*etag),
        "the ETag survives a restart"
    );
    let (bytes, _) = c.get_with_etag(&empty).await.unwrap().unwrap();
    assert!(bytes.is_empty());
    assert_eq!(
        c.put_if_absent(&head, Bytes::from_static(b"x"))
            .await
            .unwrap(),
        PutOutcome::AlreadyExists
    );
    written_etag(
        c.cas_put(&head, Bytes::from_static(b"next"), &etag)
            .await
            .unwrap(),
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts the pinned S3 test server (docker)"]
async fn s3_server_contract_concurrent_creates_have_one_winner() {
    let ep = &server().await.endpoint;
    let p = prefix("race-create");
    let c = Arc::new(cond(ep));

    for round in 0..ROUNDS {
        let key = Path::from(format!("{p}/{round}"));
        let barrier = Arc::new(Barrier::new(WRITERS));
        let tasks: Vec<_> = (0..WRITERS)
            .map(|w| {
                let (c, key, barrier) =
                    (c.clone(), key.clone(), barrier.clone());
                tokio::spawn(async move {
                    barrier.wait().await;
                    let body = Bytes::from(format!("writer-{w}"));
                    (w, c.put_if_absent(&key, body).await.unwrap())
                })
            })
            .collect();
        let mut winners = Vec::new();
        for task in tasks {
            match task.await.unwrap() {
                (w, PutOutcome::Written { etag }) => winners.push((w, etag)),
                (_, PutOutcome::AlreadyExists) => {}
                (w, other) => panic!("round {round} writer {w}: {other:?}"),
            }
        }
        assert_eq!(winners.len(), 1, "round {round}: {winners:?}");
        let (w, etag) = winners.pop().unwrap();
        let (bytes, now) = c.get_with_etag(&key).await.unwrap().unwrap();
        assert_eq!(bytes, Bytes::from(format!("writer-{w}")), "round {round}");
        assert_eq!(now, etag, "round {round}");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts the pinned S3 test server (docker)"]
async fn s3_server_contract_concurrent_cas_has_one_winner() {
    let ep = &server().await.endpoint;
    let key = Path::from(format!("{}/head", prefix("race-cas")));
    let c = Arc::new(cond(ep));
    let mut current = written_etag(
        c.put_if_absent(&key, Bytes::from_static(b"seed"))
            .await
            .unwrap(),
    );

    for round in 0..ROUNDS {
        let barrier = Arc::new(Barrier::new(WRITERS));
        let tasks: Vec<_> = (0..WRITERS)
            .map(|w| {
                let (c, key, barrier) =
                    (c.clone(), key.clone(), barrier.clone());
                let expected = current.clone();
                tokio::spawn(async move {
                    barrier.wait().await;
                    let body = Bytes::from(format!("round-{round}-writer-{w}"));
                    (w, c.cas_put(&key, body, &expected).await.unwrap())
                })
            })
            .collect();
        let mut winners = Vec::new();
        for task in tasks {
            match task.await.unwrap() {
                (w, PutOutcome::Written { etag }) => winners.push((w, etag)),
                (_, PutOutcome::Conflict) => {}
                (w, other) => panic!("round {round} writer {w}: {other:?}"),
            }
        }
        assert_eq!(winners.len(), 1, "round {round}: {winners:?}");
        let (w, etag) = winners.pop().unwrap();
        let (bytes, now) = c.get_with_etag(&key).await.unwrap().unwrap();
        assert_eq!(
            bytes,
            Bytes::from(format!("round-{round}-writer-{w}")),
            "round {round}"
        );
        assert_eq!(now, etag, "round {round}");
        current = etag.expect("a CAS write returns an ETag");
    }
}

use super::*;
use std::sync::Mutex;
use storage::MemoryStorageBackend;

const BASE: &str = r#"
metadata: {name: orders, tenant: acme}
spec:
  source:
    type: mysql
    config:
      id: src
      dsn: "mysql://df:PLANTED-SRC-PW@db.example:3306/shop"
      tables: ["shop.orders"]
  processors: []
  sinks:
    - type: kafka
      config:
        id: k
        brokers: "kafka:9092"
        topic: orders
        client_conf: {sasl.password: PLANTED-SASL-PW}
"#;

fn spec(yaml: &str) -> PipelineSpec {
    serde_yaml::from_str(yaml).expect("spec parses")
}

/// The base spec with another topic (a recorded field).
fn other_topic() -> PipelineSpec {
    spec(&BASE.replace("topic: orders", "topic: elsewhere"))
}

fn backend() -> ArcStorageBackend {
    Arc::new(MemoryStorageBackend::new())
}

/// External probes answering what the test sets.
#[derive(Default)]
struct Probe {
    found: Mutex<Vec<Found>>,
    fail: Mutex<bool>,
}

#[async_trait]
impl ExternalState for Probe {
    async fn protected(&self, _spec: &PipelineSpec) -> Result<Vec<Found>> {
        if *self.fail.lock().unwrap() {
            anyhow::bail!(
                "connect postgres://df:PLANTED-PROBE-PW@pg/app refused"
            );
        }
        Ok(self.found.lock().unwrap().clone())
    }
}

fn slot_found() -> Found {
    found("postgresql_replication_slot", "slot1", b"slot1")
}

async fn stored(b: &ArcStorageBackend) -> Option<StateBinding> {
    read(b, "src").await.unwrap().map(|(_, s)| s)
}

const PLANTED: &[&str] =
    &["PLANTED-SRC-PW", "PLANTED-SASL-PW", "PLANTED-PROBE-PW"];

fn assert_no_planted(text: &str) {
    for p in PLANTED {
        assert!(!text.contains(p), "{p} leaked: {text}");
    }
}

#[tokio::test]
async fn a_first_start_binds_and_the_same_spec_matches() {
    let b = backend();
    let probe = Probe::default();
    let s = spec(BASE);
    assert_eq!(
        check_and_bind(&b, &probe, &s).await.unwrap(),
        Outcome::Created
    );
    let first = stored(&b).await.unwrap();
    assert_no_planted(&serde_json::to_string(&first).unwrap());
    assert_eq!(
        check_and_bind(&b, &probe, &s).await.unwrap(),
        Outcome::Matched
    );
    assert_eq!(stored(&b).await.unwrap(), first, "unchanged");
}

/// Writes one piece of protected state for source `src` / pipeline
/// `orders`.
async fn plant(b: &ArcStorageBackend, kind: &str) {
    match kind {
        "sink_checkpoint" => b
            .kv_put(CHECKPOINTS_NS, "src::sink::k", b"cp")
            .await
            .unwrap(),
        "legacy_checkpoint" => {
            b.kv_put(CHECKPOINTS_NS, "src", b"cp").await.unwrap()
        }
        "snapshot_progress" => b
            .kv_put(CHECKPOINTS_NS, "mysql_snapshot_progress:src", b"{}")
            .await
            .unwrap(),
        "postgresql_slot_owner" => b
            .kv_put(CHECKPOINTS_NS, "slot_owner:src", b"{}")
            .await
            .unwrap(),
        "snapshot_generation" => {
            b.slot_create(
                sources::snapshot_queue::CONTROL_NS,
                "snapshot_generation:src",
                b"{}",
            )
            .await
            .unwrap();
        }
        "recovery_authorization" => {
            b.slot_create(
                sources::snapshot_recovery::AUTHORIZATION_NS,
                "pg_slot_recreation:src",
                b"{}",
            )
            .await
            .unwrap();
        }
        "destination_ownership" => {
            b.slot_create(
                crate::destination_ownership::NAMESPACE,
                &format!(
                    "{}k/elasticsearch/c1/orders",
                    sinks::ownership_prefix("orders")
                ),
                b"{}",
            )
            .await
            .unwrap();
        }
        "dlq_entries" => {
            b.queue_push(crate::dlq::JOURNAL_NS, "orders:dlq", b"entry")
                .await
                .unwrap();
        }
        other => panic!("{other}"),
    }
}

const KINDS: &[&str] = &[
    "sink_checkpoint",
    "legacy_checkpoint",
    "snapshot_progress",
    "postgresql_slot_owner",
    "snapshot_generation",
    "recovery_authorization",
    "destination_ownership",
    "dlq_entries",
];

#[tokio::test]
async fn each_kind_of_retained_state_keeps_the_binding_authoritative() {
    for kind in KINDS {
        let b = backend();
        let probe = Probe::default();
        check_and_bind(&b, &probe, &spec(BASE)).await.unwrap();
        let before = stored(&b).await.unwrap();
        plant(&b, kind).await;
        let err = check_and_bind(&b, &probe, &other_topic())
            .await
            .unwrap_err();
        match &err {
            BindingError::Incompatible { differences, .. } => assert!(
                differences.iter().any(|d| d.field == "sinks.k.topic"),
                "{kind}: {differences:?}"
            ),
            e => panic!("{kind}: {e}"),
        }
        assert_no_planted(&err.to_string());
        assert_eq!(
            stored(&b).await.unwrap(),
            before,
            "{kind}: binding unchanged"
        );
        // DELETE keeps it too.
        assert!(
            !release_if_unprotected(&b, &probe, &spec(BASE))
                .await
                .unwrap(),
            "{kind}"
        );
        assert!(stored(&b).await.is_some());
    }
}

#[tokio::test]
async fn with_no_retained_state_left_a_different_spec_binds() {
    let b = backend();
    let probe = Probe::default();
    check_and_bind(&b, &probe, &spec(BASE)).await.unwrap();
    assert_eq!(
        check_and_bind(&b, &probe, &other_topic()).await.unwrap(),
        Outcome::Replaced
    );
    assert_eq!(
        stored(&b).await.unwrap().canonical["sinks"]["k"]["topic"],
        "elsewhere"
    );
    // And DELETE removes a binding nothing depends on.
    assert!(
        release_if_unprotected(&b, &probe, &other_topic())
            .await
            .unwrap()
    );
    assert!(stored(&b).await.is_none());
}

#[tokio::test]
async fn retained_state_without_a_binding_is_unbound() {
    for kind in KINDS {
        let b = backend();
        plant(&b, kind).await;
        let err = check_and_bind(&b, &Probe::default(), &spec(BASE))
            .await
            .unwrap_err();
        assert!(matches!(err, BindingError::Unbound { .. }), "{kind}: {err}");
        assert!(err.to_string().contains("state-binding adopt"));
        assert!(stored(&b).await.is_none(), "{kind}: nothing written");
    }
}

#[tokio::test]
async fn a_missing_local_record_never_proves_a_fresh_start() {
    // An existing PostgreSQL slot (or S3 HEAD) with an empty store: unbound.
    let b = backend();
    let probe = Probe::default();
    probe.found.lock().unwrap().push(slot_found());
    let err = check_and_bind(&b, &probe, &spec(BASE)).await.unwrap_err();
    assert!(matches!(err, BindingError::Unbound { .. }), "{err}");
    assert!(stored(&b).await.is_none());

    // It keeps an existing binding authoritative as well.
    let b = backend();
    let probe = Probe::default();
    check_and_bind(&b, &probe, &spec(BASE)).await.unwrap();
    probe.found.lock().unwrap().push(slot_found());
    let err = check_and_bind(&b, &probe, &other_topic())
        .await
        .unwrap_err();
    assert!(matches!(err, BindingError::Incompatible { .. }), "{err}");
    assert!(
        !release_if_unprotected(&b, &probe, &spec(BASE))
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn protected_state_that_cannot_be_determined_fails_closed() {
    let b = backend();
    let probe = Probe::default();
    *probe.fail.lock().unwrap() = true;
    let err = check_and_bind(&b, &probe, &spec(BASE)).await.unwrap_err();
    assert!(matches!(err, BindingError::Undetermined(_)), "{err}");
    assert_no_planted(&err.to_string());
    assert!(stored(&b).await.is_none(), "nothing written");
    // Nor is a binding released when it cannot be told.
    *probe.fail.lock().unwrap() = false;
    check_and_bind(&b, &probe, &spec(BASE)).await.unwrap();
    *probe.fail.lock().unwrap() = true;
    assert!(
        release_if_unprotected(&b, &probe, &spec(BASE))
            .await
            .is_err()
    );
    assert!(stored(&b).await.is_some());

    // A state store that cannot be read: likewise.
    let fault = Arc::new(storage::adapters::test_util::FaultBackend::new());
    let fb: ArcStorageBackend = fault.clone();
    fault
        .fail_kv_get
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let err = check_and_bind(&fb, &Probe::default(), &spec(BASE))
        .await
        .unwrap_err();
    assert!(matches!(err, BindingError::Undetermined(_)), "{err}");
}

#[tokio::test]
async fn crash_points_leave_a_binding_that_is_stale_or_matching() {
    // A crash after the binding is created, before the claim or spawn:
    // nothing it protects was written, so it is stale, and the next start
    // binds whichever spec it brings.
    let b = backend();
    let probe = Probe::default();
    check_and_bind(&b, &probe, &spec(BASE)).await.unwrap();
    assert_eq!(
        check_and_bind(&b, &probe, &spec(BASE)).await.unwrap(),
        Outcome::Matched
    );
    assert_eq!(
        check_and_bind(&b, &probe, &other_topic()).await.unwrap(),
        Outcome::Replaced
    );

    // A crash during DELETE after the claim release, before the binding is
    // removed: the per-sink checkpoints are already gone; with nothing else
    // left the binding is stale and replaced at the next first start.
    let b = backend();
    check_and_bind(&b, &probe, &spec(BASE)).await.unwrap();
    plant(&b, "sink_checkpoint").await;
    b.kv_delete(CHECKPOINTS_NS, "src::sink::k").await.unwrap();
    assert_eq!(
        check_and_bind(&b, &probe, &other_topic()).await.unwrap(),
        Outcome::Replaced
    );

    // A crash during that DELETE with state that DELETE leaves behind:
    // still authoritative.
    let b = backend();
    check_and_bind(&b, &probe, &spec(BASE)).await.unwrap();
    plant(&b, "legacy_checkpoint").await;
    assert!(check_and_bind(&b, &probe, &other_topic()).await.is_err());

    // A write that fails while binding: nothing is bound and the start is
    // refused; the next attempt binds.
    let fault = Arc::new(storage::adapters::test_util::FaultBackend::new());
    let fb: ArcStorageBackend = fault.clone();
    fault.fail_one_write_after(0);
    assert!(check_and_bind(&fb, &probe, &spec(BASE)).await.is_err());
    assert!(read(&fb, "src").await.unwrap().is_none());
    assert_eq!(
        check_and_bind(&fb, &probe, &spec(BASE)).await.unwrap(),
        Outcome::Created
    );
}

#[tokio::test]
async fn patch_compares_with_the_binding_and_never_writes() {
    let b = backend();
    let probe = Probe::default();
    check_and_bind(&b, &probe, &spec(BASE)).await.unwrap();
    let before = stored(&b).await.unwrap();
    let operational = BASE.replace(
        "tables: [\"shop.orders\"]",
        "tables: [\"shop.orders\"]\n      snapshot: {mode: never}",
    );
    check_patch(&b, &spec(BASE), &spec(&operational))
        .await
        .unwrap();
    let err = check_patch(&b, &spec(BASE), &other_topic())
        .await
        .unwrap_err();
    assert!(matches!(err, BindingError::Incompatible { .. }), "{err}");
    assert_eq!(stored(&b).await.unwrap(), before);
    // An unclassified name is a validation error.
    let err = check_patch(
        &b,
        &spec(BASE),
        &spec(&BASE.replace(
            "{sasl.password: PLANTED-SASL-PW}",
            "{some.unknown.key: x}",
        )),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, BindingError::Unclassified(_)), "{err}");
}

// ── Adoption ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn adoption_binds_unbound_state_after_a_matching_dry_run() {
    let b = backend();
    let probe = Probe::default();
    plant(&b, "sink_checkpoint").await;
    plant(&b, "dlq_entries").await;
    b.slot_create("active_sources", "src", b"orders")
        .await
        .unwrap();
    let plan = adopt_plan(&b, &probe, &spec(BASE)).await.unwrap();
    assert!(plan.verified.iter().any(|v| v.contains("checkpoint keys")));
    assert!(plan.verified.iter().any(|v| v.contains("claim")));
    assert_no_planted(&serde_json::to_string(&plan).unwrap());
    let applied = adopt_apply(&b, &probe, &spec(BASE), &plan.proof, "tester")
        .await
        .unwrap();
    assert_eq!(applied.proof, plan.proof);
    assert!(stored(&b).await.is_some());
    assert!(
        !b.kv_list(AUDIT_NAMESPACE, None).await.unwrap().is_empty(),
        "audited"
    );
    // The adopted binding now governs starts.
    assert_eq!(
        check_and_bind(&b, &probe, &spec(BASE)).await.unwrap(),
        Outcome::Matched
    );
    assert!(check_and_bind(&b, &probe, &other_topic()).await.is_err());
    // And a second adoption is refused.
    assert!(adopt_plan(&b, &probe, &spec(BASE)).await.is_err());
}

#[tokio::test]
async fn adoption_refuses_a_store_changed_since_the_dry_run() {
    let b = backend();
    let probe = Probe::default();
    plant(&b, "sink_checkpoint").await;
    let plan = adopt_plan(&b, &probe, &spec(BASE)).await.unwrap();
    plant(&b, "legacy_checkpoint").await;
    let err = adopt_apply(&b, &probe, &spec(BASE), &plan.proof, "tester")
        .await
        .unwrap_err();
    assert!(err.downcast_ref::<AdoptRefused>().is_some(), "{err:#}");
    assert!(stored(&b).await.is_none());
}

#[tokio::test]
async fn adoption_refuses_state_that_does_not_match_the_spec() {
    async fn refused(b: ArcStorageBackend) -> String {
        adopt_plan(&b, &Probe::default(), &spec(BASE))
            .await
            .expect_err("refused")
            .to_string()
    }
    // A checkpoint of a sink the spec does not have.
    let b = backend();
    b.kv_put(CHECKPOINTS_NS, "src::sink::other", b"cp")
        .await
        .unwrap();
    assert!(refused(b).await.contains("sink 'other'"));
    // A claim by another pipeline.
    let b = backend();
    plant(&b, "sink_checkpoint").await;
    b.slot_create("active_sources", "src", b"someone-else")
        .await
        .unwrap();
    assert!(refused(b).await.contains("someone-else"));
    // A snapshot generation of other table patterns.
    let b = backend();
    plant(&b, "sink_checkpoint").await;
    let control = serde_json::json!({
        "fingerprint_format": 3,
        "config_fingerprint": sources::snapshot_recovery::config_fingerprint(
            sources::snapshot_recovery::Engine::Mysql,
            &["shop.other".to_string()],
        ),
    });
    b.slot_create(
        sources::snapshot_queue::CONTROL_NS,
        "snapshot_generation:src",
        &serde_json::to_vec(&control).unwrap(),
    )
    .await
    .unwrap();
    assert!(refused(b).await.contains("table patterns"));
    // A PostgreSQL slot owner record for a MySQL spec.
    let b = backend();
    plant(&b, "postgresql_slot_owner").await;
    assert!(refused(b).await.contains("slot owner"));
    // Nothing to adopt.
    assert!(refused(backend()).await.contains("no retained state"));
}

#[tokio::test]
async fn adoption_matches_a_snapshot_generation_of_the_same_tables() {
    let b = backend();
    let control = serde_json::json!({
        "fingerprint_format": 3,
        "config_fingerprint": sources::snapshot_recovery::config_fingerprint(
            sources::snapshot_recovery::Engine::Mysql,
            &["shop.orders".to_string()],
        ),
    });
    b.slot_create(
        sources::snapshot_queue::CONTROL_NS,
        "snapshot_generation:src",
        &serde_json::to_vec(&control).unwrap(),
    )
    .await
    .unwrap();
    let plan = adopt_plan(&b, &Probe::default(), &spec(BASE))
        .await
        .unwrap();
    assert!(
        plan.verified
            .iter()
            .any(|v| v.contains("snapshot generation"))
    );
}

// ── A mismatching binding is replaced only when its state is verifiable ──

const PG: &str = r#"
metadata: {name: orders, tenant: acme}
spec:
  source:
    type: postgres
    config:
      id: src
      dsn: "postgres://df:PLANTED-SRC-PW@pg.example:5432/app"
      publication: pub1
      slot: slot1
      tables: ["public.orders"]
  processors: []
  sinks:
    - type: kafka
      config: {id: k, brokers: "kafka:9092", topic: orders}
"#;

const S3: &str = r#"
metadata: {name: orders, tenant: acme}
spec:
  source:
    type: mysql
    config:
      id: src
      dsn: "mysql://df:PLANTED-SRC-PW@db.example:3306/shop"
      tables: ["shop.orders"]
  processors: []
  sinks:
    - type: kafka
      config: {id: k, brokers: "kafka:9092", topic: orders}
    - type: s3
      config: {id: s, bucket: lake, prefix: cdc, region: eu-west-1}
"#;

/// A probe that records whether it was asked.
#[derive(Default)]
struct Watched {
    asked: Mutex<u32>,
}

#[async_trait]
impl ExternalState for Watched {
    async fn protected(&self, _spec: &PipelineSpec) -> Result<Vec<Found>> {
        *self.asked.lock().unwrap() += 1;
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn an_empty_new_destination_never_makes_a_binding_stale() {
    // PostgreSQL: the recorded slot's server is not recorded, so a new spec
    // cannot check it; the mismatching binding is never replaced.
    let b = backend();
    let probe = Watched::default();
    check_and_bind(&b, &probe, &spec(PG)).await.unwrap();
    let before = stored(&b).await.unwrap();
    let moved = spec(&PG.replace("slot: slot1", "slot: slot2"));
    let err = check_and_bind(&b, &probe, &moved).await.unwrap_err();
    assert!(
        matches!(
            err,
            BindingError::Incompatible {
                unverifiable: true,
                ..
            }
        ),
        "{err}"
    );
    assert!(err.to_string().contains("cannot be verified absent"));
    assert_eq!(stored(&b).await.unwrap(), before, "unchanged");

    // S3 durable: a moved bucket cannot be checked through the new spec.
    let b = backend();
    let probe = Watched::default();
    check_and_bind(&b, &probe, &spec(S3)).await.unwrap();
    let before = stored(&b).await.unwrap();
    let err = check_and_bind(
        &b,
        &probe,
        &spec(&S3.replace("bucket: lake", "bucket: other")),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            err,
            BindingError::Incompatible {
                unverifiable: true,
                ..
            }
        ),
        "{err}"
    );
    assert_eq!(stored(&b).await.unwrap(), before, "unchanged");
    let asked = *probe.asked.lock().unwrap();

    // The same bucket, prefix and pipeline: the new spec probes the recorded
    // state itself, and with none found the stale binding is replaced.
    let same_place = spec(&S3.replace("topic: orders", "topic: elsewhere"));
    assert_eq!(
        check_and_bind(&b, &probe, &same_place).await.unwrap(),
        Outcome::Replaced
    );
    assert!(
        *probe.asked.lock().unwrap() > asked,
        "the probe was consulted"
    );
}

// ── The adoption proof covers state contents, not only keys ──────────────

async fn proof(b: &ArcStorageBackend, probe: &Probe) -> String {
    adopt_plan(b, probe, &spec(BASE)).await.unwrap().proof
}

#[tokio::test]
async fn same_key_changes_change_the_adoption_proof() {
    let probe = Probe::default();

    // An ownership record rewritten under its key.
    let b = backend();
    plant(&b, "destination_ownership").await;
    let key = format!(
        "{}k/elasticsearch/c1/orders",
        sinks::ownership_prefix("orders")
    );
    let before = proof(&b, &probe).await;
    let (v, _) = b
        .slot_get(crate::destination_ownership::NAMESPACE, &key)
        .await
        .unwrap()
        .unwrap();
    b.slot_cas(
        crate::destination_ownership::NAMESPACE,
        &key,
        v,
        b"{\"fence\":\"x\"}",
    )
    .await
    .unwrap();
    assert_ne!(proof(&b, &probe).await, before, "ownership content");

    // A snapshot plan item rewritten under its key.
    let b = backend();
    plant(&b, "sink_checkpoint").await;
    let item = format!("{}/0000000000000001/aa/bb", hex(b"src"));
    b.slot_create(sources::snapshot_queue::PLAN_NS, &item, b"pending")
        .await
        .unwrap();
    let before = proof(&b, &probe).await;
    b.slot_cas(sources::snapshot_queue::PLAN_NS, &item, 1, b"done")
        .await
        .unwrap();
    assert_ne!(proof(&b, &probe).await, before, "plan item content");

    // A DLQ entry replaced: the same number of entries, other content.
    let b = backend();
    plant(&b, "dlq_entries").await;
    let before = proof(&b, &probe).await;
    let entries = b
        .queue_peek(crate::dlq::JOURNAL_NS, "orders:dlq", 10)
        .await
        .unwrap();
    b.queue_ack(crate::dlq::JOURNAL_NS, "orders:dlq", entries[0].0)
        .await
        .unwrap();
    b.queue_push(crate::dlq::JOURNAL_NS, "orders:dlq", b"other")
        .await
        .unwrap();
    assert_ne!(proof(&b, &probe).await, before, "DLQ content");

    // An external slot or HEAD of the same name in another state.
    let b = backend();
    let probe = Probe::default();
    probe.found.lock().unwrap().push(found(
        "postgresql_replication_slot",
        "slot1",
        b"confirmed_flush_lsn=0/10",
    ));
    let plan = adopt_plan(&b, &probe, &spec(BASE)).await.unwrap();
    *probe.found.lock().unwrap() = vec![found(
        "postgresql_replication_slot",
        "slot1",
        b"confirmed_flush_lsn=0/20",
    )];
    assert_ne!(proof(&b, &probe).await, plan.proof, "external state");
    let err = adopt_apply(&b, &probe, &spec(BASE), &plan.proof, "tester")
        .await
        .unwrap_err();
    assert!(err.downcast_ref::<AdoptRefused>().is_some(), "{err:#}");
    assert!(stored(&b).await.is_none());
}

//! Operator incidents through real pipelines (PipelineManager, PostgreSQL in a
//! container behind a TCP proxy, an HTTP sink served by the test).
//!
//! - A pipeline that fails gets one stable incident; failed restarts update
//!   it (occurrences grow) without resolving or reopening it, also after an
//!   acknowledgement, which never permits recovery.
//! - Verified running (the source reports its startup checks passed and its
//!   stream open while the coordinator runs and nothing failed) resolves it as
//!   `pipeline_recovered`; a failure right after that is a separate incident,
//!   and the same failure again later is a new occurrence (new identity).
//! - A different PostgreSQL cluster is a `pg_different_cluster` incident with
//!   bounded evidence and recommendations, kept across refused restarts and
//!   resolved (`lineage_verified`) once the source verifies its own server.
//!
//! Run with:
//! ```bash
//! cargo test -p runner --test incidents_e2e -- --include-ignored --test-threads=1
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use gate_ownership::GateOwned;
use rest_api::PipelineController;
use rest_api::pipelines::AcknowledgeRequest;
use runner::pipeline_manager::PipelineManager;
use serde_json::Value;
use storage::adapters::incidents::{IncidentStore, Transition};
use storage::{ArcStorageBackend, MemoryStorageBackend};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner,
};
use tokio::time::sleep;
use tokio_postgres::NoTls;

const DB: &str = "shop";
const PASS: &str = "pg";

async fn start_postgres() -> (ContainerAsync<GenericImage>, u16) {
    let c = GenericImage::new("postgres", "17")
        .with_wait_for(WaitFor::message_on_stderr("database system is ready"))
        .with_env_var("POSTGRES_PASSWORD", PASS)
        .with_cmd(["postgres", "-c", "wal_level=logical"])
        .gate_owned()
        .start()
        .await
        .expect("start postgres");
    let port = c.get_host_port_ipv4(5432).await.expect("pg port");
    sleep(Duration::from_secs(3)).await;
    let admin = client(port, "postgres").await;
    admin
        .execute(&format!("CREATE DATABASE {DB}"), &[])
        .await
        .unwrap();
    let c2 = client(port, DB).await;
    c2.batch_execute(
        "CREATE TABLE orders (id INT PRIMARY KEY, sku TEXT);
         ALTER TABLE orders REPLICA IDENTITY FULL;
         CREATE PUBLICATION inc_pub FOR TABLE orders;",
    )
    .await
    .unwrap();
    sources::postgres::postgres_publication::register(
        &c2,
        &["inc_pub".to_string()],
    )
    .await
    .unwrap();
    (c, port)
}

async fn client(port: u16, db: &str) -> tokio_postgres::Client {
    let dsn = format!(
        "host=127.0.0.1 port={port} user=postgres password={PASS} dbname={db}"
    );
    let (c, conn) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
    tokio::spawn(async move {
        conn.await.ok();
    });
    c
}

/// A TCP proxy standing in for a failover endpoint: new connections go to
/// the current target; switching drops every open one.
struct Proxy {
    port: u16,
    to: Arc<AtomicU16>,
    live: Arc<std::sync::Mutex<Vec<tokio::task::AbortHandle>>>,
}

impl Proxy {
    async fn start(to: u16) -> Self {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let target = Arc::new(AtomicU16::new(to));
        let live = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (t, lv) = (target.clone(), live.clone());
        tokio::spawn(async move {
            while let Ok((mut client, _)) = l.accept().await {
                let to = t.load(Ordering::SeqCst);
                let task = tokio::spawn(async move {
                    if let Ok(mut server) =
                        tokio::net::TcpStream::connect(("127.0.0.1", to)).await
                    {
                        let _ = tokio::io::copy_bidirectional(
                            &mut client,
                            &mut server,
                        )
                        .await;
                    }
                });
                lv.lock().unwrap().push(task.abort_handle());
            }
        });
        Self {
            port,
            to: target,
            live,
        }
    }

    fn switch_to(&self, to: u16) {
        self.to.store(to, Ordering::SeqCst);
        for t in self.live.lock().unwrap().drain(..) {
            t.abort();
        }
    }
}

async fn dead_port() -> u16 {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    l.local_addr().unwrap().port()
}

/// An HTTP endpoint for the sink: answers with `status`, counts requests.
struct Hook {
    url: String,
    status: Arc<AtomicU16>,
    hits: Arc<AtomicUsize>,
}

async fn hook() -> Hook {
    let status = Arc::new(AtomicU16::new(200));
    let hits = Arc::new(AtomicUsize::new(0));
    let (s, h) = (status.clone(), hits.clone());
    let app = axum::Router::new().route(
        "/",
        axum::routing::post(move || {
            let (s, h) = (s.clone(), h.clone());
            async move {
                h.fetch_add(1, Ordering::SeqCst);
                axum::http::StatusCode::from_u16(s.load(Ordering::SeqCst))
                    .unwrap()
            }
        }),
    );
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", l.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(l, app).await.ok();
    });
    Hook { url, status, hits }
}

fn spec(
    name: &str,
    pg_port: u16,
    url: &str,
) -> deltaforge_config::PipelineSpec {
    let yaml = format!(
        r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: {name}
  tenant: acme
spec:
  source:
    type: postgres
    config:
      id: pg-src
      dsn: "host=127.0.0.1 port={pg_port} user=postgres password={PASS} dbname={DB}"
      slot: inc_slot
      publication: inc_pub
      tables: [public.orders]
  processors: []
  sinks:
    - type: http
      config:
        id: hook
        url: "{url}"
        required: true
        send_timeout_secs: 2
        batch_timeout_secs: 2
        connect_timeout_secs: 2
  batch:
    max_events: 10
    max_ms: 50
    respect_source_tx: true
"#
    );
    serde_yaml::from_str(&yaml).expect("pipeline spec")
}

async fn manager() -> (PipelineManager, ArcStorageBackend) {
    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let mgr = PipelineManager::with_backend(Arc::clone(&backend))
        .await
        .unwrap();
    (mgr, backend)
}

/// Poll the pipeline until `pred` holds on its status (with incidents).
async fn until(
    mgr: &PipelineManager,
    name: &str,
    what: &str,
    pred: impl Fn(&rest_api::PipeInfo) -> bool,
) -> rest_api::PipeInfo {
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        let info = PipelineController::get(mgr, name).await.unwrap();
        if pred(&info) {
            return info;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        sleep(Duration::from_millis(200)).await;
    }
}

fn primary(info: &rest_api::PipeInfo) -> Option<String> {
    info.incidents.as_ref()?.primary.clone()
}

async fn failed_with_primary(mgr: &PipelineManager, name: &str) -> String {
    let info = until(mgr, name, "a failed pipeline with an incident", |i| {
        i.status == "failed"
            && i.incidents.as_ref().is_some_and(|inc| {
                inc.primary_final
                    && inc.primary.as_ref().is_some_and(|p| {
                        inc.blocking.iter().any(|b| b["incident_id"] == *p)
                    })
            })
    })
    .await;
    primary(&info).unwrap()
}

async fn incident(mgr: &PipelineManager, name: &str, id: &str) -> Value {
    mgr.incident(name, id).await.unwrap()
}

async fn until_incident(
    mgr: &PipelineManager,
    name: &str,
    id: &str,
    what: &str,
    pred: impl Fn(&Value) -> bool,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        let v = incident(mgr, name, id).await;
        if pred(&v) {
            return v;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}: {v}"
        );
        sleep(Duration::from_millis(200)).await;
    }
}

fn transitions(
    store_trail: &[storage::adapters::incidents::AuditEntry],
    id: &str,
) -> Vec<&'static str> {
    store_trail
        .iter()
        .filter(|e| e.incident_id.0 == id)
        .map(|e| match e.transition {
            Transition::Opened => "opened",
            Transition::Reopened => "reopened",
            Transition::Acknowledged { .. } => "acknowledged",
            Transition::Resolved { .. } => "resolved",
            Transition::ResolvedByRecovery { .. } => "resolved_by_recovery",
            Transition::Displaced => "displaced",
            Transition::Reclassified { .. } => "reclassified",
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn a_failure_keeps_one_incident_until_a_verified_recovery() -> Result<()>
{
    let (_pg, pg_port) = start_postgres().await;
    let dead = dead_port().await;
    let proxy = Proxy::start(dead).await;
    let hook = hook().await;
    let (mgr, backend) = manager().await;
    let store = IncidentStore::new(Arc::clone(&backend), "inc");

    // The endpoint is down: the pipeline fails with one incident.
    mgr.start_pipeline(spec("inc", proxy.port, &hook.url))
        .await?;
    let x = failed_with_primary(&mgr, "inc").await;
    let v = incident(&mgr, "inc", &x).await;
    assert_eq!(v["reason_code"], "unclassified_failure");
    assert_eq!(v["occurrences"], 1);
    assert_eq!(v["durable"], true);

    // Acknowledged: still open and blocking; it permits nothing.
    let acked = mgr
        .acknowledge_incident(
            "inc",
            &x,
            AcknowledgeRequest {
                asserted_actor: "alice".into(),
                reason: "looking at the endpoint".into(),
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(acked["status"]["state"], "acknowledged");
    assert_eq!(acked["blocking"], true);

    // A failed restart is the same incident, counted, never resolved.
    mgr.resume("inc").await.unwrap();
    until_incident(&mgr, "inc", &x, "a second occurrence", |v| {
        v["occurrences"] == 2
    })
    .await;
    assert_eq!(failed_with_primary(&mgr, "inc").await, x);
    let v = incident(&mgr, "inc", &x).await;
    assert_eq!(v["status"]["state"], "acknowledged");
    assert_eq!(
        transitions(&store.audit_trail(1000).await?, &x),
        ["opened", "acknowledged"],
        "no resolve/reopen flap"
    );

    // The endpoint is back: verified running resolves it.
    proxy.switch_to(pg_port);
    mgr.resume("inc").await.unwrap();
    let v = until_incident(&mgr, "inc", &x, "pipeline_recovered", |v| {
        v["status"]["state"] == "resolved"
    })
    .await;
    assert_eq!(v["status"]["by"]["check"], "pipeline_recovered");
    let info = until(&mgr, "inc", "a running, unblocked pipeline", |i| {
        i.status == "running"
            && !i.incidents.as_ref().unwrap().blocks_readiness()
    })
    .await;
    assert_eq!(info.status, "running");

    // A failure right after the recovery (the sink starts refusing) is a
    // separate incident; the recovered one stays resolved.
    hook.status.store(500, Ordering::SeqCst);
    client(pg_port, DB)
        .await
        .execute("INSERT INTO orders VALUES (1, 'a')", &[])
        .await?;
    let z = failed_with_primary(&mgr, "inc").await;
    assert_ne!(z, x);
    let vz = incident(&mgr, "inc", &z).await;
    assert_eq!(vz["component"]["kind"], "sink");
    assert!(hook.hits.load(Ordering::SeqCst) >= 1);
    assert_eq!(
        transitions(&store.audit_trail(1000).await?, &x),
        ["opened", "acknowledged", "resolved"]
    );

    // The source failure again, after that genuine recovery, is a new
    // occurrence (new identity), not a reopen.
    mgr.stop("inc").await.unwrap();
    hook.status.store(200, Ordering::SeqCst);
    proxy.switch_to(dead);
    mgr.resume("inc").await.unwrap();
    let y = failed_with_primary(&mgr, "inc").await;
    assert_ne!(y, x, "a new occurrence after a genuine recovery");
    assert_eq!(
        incident(&mgr, "inc", &y).await["reason_code"],
        "unclassified_failure"
    );
    assert_eq!(
        incident(&mgr, "inc", &x).await["status"]["state"],
        "resolved"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn a_different_cluster_is_one_incident_until_the_source_verifies_its_server()
-> Result<()> {
    let (_a, a_port) = start_postgres().await;
    let (_b, b_port) = start_postgres().await;
    let proxy = Proxy::start(a_port).await;
    let hook = hook().await;
    let (mgr, backend) = manager().await;
    let store = IncidentStore::new(Arc::clone(&backend), "dc");

    // Running on A (streams a row).
    mgr.start_pipeline(spec("dc", proxy.port, &hook.url))
        .await?;
    sleep(Duration::from_secs(4)).await;
    client(a_port, DB)
        .await
        .execute("INSERT INTO orders VALUES (1, 'a')", &[])
        .await?;
    let deadline = Instant::now() + Duration::from_secs(60);
    while hook.hits.load(Ordering::SeqCst) == 0 {
        assert!(Instant::now() < deadline, "never streamed from A");
        sleep(Duration::from_millis(200)).await;
    }

    // The endpoint now reaches another cluster: refused, one incident.
    mgr.stop("dc").await.unwrap();
    proxy.switch_to(b_port);
    mgr.resume("dc").await.unwrap();
    let d = failed_with_primary(&mgr, "dc").await;
    let v = incident(&mgr, "dc", &d).await;
    assert_eq!(v["reason_code"], "pg_different_cluster");
    assert_eq!(v["safety_state"], "halted_safe");
    assert_eq!(v["retryability"], "operator_action");
    let actions = v["recommended_actions"].as_array().unwrap();
    assert!(actions.contains(&Value::from("verify_endpoint")));
    assert!(actions.contains(&Value::from("use_new_source_id")));
    let ev = v["evidence"].as_object().unwrap();
    assert!(ev.contains_key("expected_system_identifier"));
    assert!(ev.contains_key("live_system_identifier"));
    assert!(ev.len() <= deltaforge_core::incident::MAX_EVIDENCE_ENTRIES);

    // Refused again: the same incident, counted.
    mgr.resume("dc").await.unwrap();
    until_incident(&mgr, "dc", &d, "a second refusal", |v| {
        v["occurrences"] == 2
    })
    .await;
    assert_eq!(failed_with_primary(&mgr, "dc").await, d);

    // Routed back to A: the source verifies its server; resolved.
    proxy.switch_to(a_port);
    mgr.resume("dc").await.unwrap();
    let v = until_incident(&mgr, "dc", &d, "lineage_verified", |v| {
        v["status"]["state"] == "resolved"
    })
    .await;
    assert_eq!(v["status"]["by"]["check"], "lineage_verified");
    assert_eq!(
        transitions(&store.audit_trail(1000).await?, &d),
        ["opened", "resolved"]
    );
    Ok(())
}

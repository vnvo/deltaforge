//! Live coverage for the `deltaforge preflight` command (operational pass item 1).
//!
//! - A valid FRESH deployment (no slot yet) must PASS: DeltaForge creates and owns
//!   the slot, so an absent slot is informational, not a hard error.
//! - An existing FOREIGN slot (no matching ownership record) must FAIL closed.
//! - An existing OWNED slot (matching ownership record) must PASS.
//! - A missing sink credential must FAIL closed (no container needed).
//!
//! Sink endpoint reachability is intentionally not probed.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use checkpoints::{
    CheckpointError, CheckpointResult, CheckpointStore, MemCheckpointStore,
};
use deltaforge_config::load_cfg;
use runner::preflight::check_all;
use sources::postgres::postgres_slot_owner::{
    SLOT_OWNER_RECORD_VERSION, SlotLifecycle, SlotOwnership,
};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner,
};
use tokio_postgres::NoTls;

const SOURCE_ID: &str = "orders-pg";
const SLOT: &str = "df_pf_slot";

async fn start_pg() -> (ContainerAsync<GenericImage>, u16) {
    let image = GenericImage::new("postgres", "17")
        .with_wait_for(WaitFor::message_on_stderr("database system is ready"))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "password")
        .with_cmd(vec![
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=10",
            "-c",
            "max_wal_senders=10",
        ]);
    let c = image.start().await.expect("start postgres");
    let port = c.get_host_port_ipv4(5432).await.expect("pg port");
    tokio::time::sleep(Duration::from_secs(5)).await;
    (c, port)
}

fn pg_dsn(port: u16) -> String {
    format!(
        "host=127.0.0.1 port={port} user=postgres password=password dbname=postgres"
    )
}

async fn pg_client(port: u16) -> tokio_postgres::Client {
    let (client, conn) =
        tokio_postgres::connect(&pg_dsn(port), NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

fn config_yaml(port: u16) -> String {
    format!(
        r#"apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: pf-e2e
  tenant: acme
spec:
  source:
    type: postgres
    config:
      id: {SOURCE_ID}
      dsn: "host=127.0.0.1 port={port} user=postgres password=password dbname=postgres"
      slot: {SLOT}
      publication: df_pf_pub
      tables:
        - public.orders
  processors: []
  sinks:
    - type: redis
      config:
        id: r1
        uri: "redis://127.0.0.1:6379"
        stream: df.events
        envelope:
          type: native
        encoding: json
        required: false
  batch:
    max_events: 100
    max_bytes: 8388608
    max_ms: 200
"#
    )
}

/// A checkpoint store whose reads fail, to simulate the ownership authority being
/// unavailable. Writes/list/delete are no-ops.
#[derive(Debug)]
struct FailingReadStore;

#[async_trait]
impl CheckpointStore for FailingReadStore {
    async fn get_raw(&self, _k: &str) -> CheckpointResult<Option<Vec<u8>>> {
        Err(CheckpointError::Data("injected read failure".into()))
    }
    async fn put_raw(&self, _k: &str, _b: &[u8]) -> CheckpointResult<()> {
        Ok(())
    }
    async fn delete(&self, _k: &str) -> CheckpointResult<bool> {
        Ok(false)
    }
    async fn list(&self) -> CheckpointResult<Vec<String>> {
        Ok(vec![])
    }
}

fn write_config(yaml: &str) -> tempfile::NamedTempFile {
    use std::io::Write;
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(yaml.as_bytes()).unwrap();
    f.flush().unwrap();
    f
}

#[tokio::test]
#[ignore = "requires docker"]
async fn preflight_slot_ownership_lifecycle() {
    let (_c, port) = start_pg().await;
    let client = pg_client(port).await;

    // Table + publication, but NO slot (fresh deployment).
    client
        .batch_execute("CREATE TABLE orders (id INT PRIMARY KEY, sku TEXT)")
        .await
        .unwrap();
    client
        .execute("ALTER TABLE orders REPLICA IDENTITY FULL", &[])
        .await
        .unwrap();
    client
        .execute("CREATE PUBLICATION df_pf_pub FOR TABLE orders", &[])
        .await
        .unwrap();

    let cfg = write_config(&config_yaml(port));
    let specs = load_cfg(cfg.path().to_str().unwrap()).expect("load config");
    let chkpt: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new().unwrap());

    // 1. Fresh deployment: absent slot is OK (DeltaForge will create+own it).
    let report = check_all(&specs, &chkpt).await;
    assert!(
        report.ok,
        "fresh deployment must pass preflight; got: {:?}",
        report.pipelines
    );

    // 2. Foreign slot: create it out of band with no ownership record -> FAIL.
    client
        .execute(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&SLOT],
        )
        .await
        .unwrap();
    let report = check_all(&specs, &chkpt).await;
    assert!(!report.ok, "a foreign existing slot must fail preflight");
    assert!(
        report.pipelines[0]
            .hard_errors
            .iter()
            .any(|e| e.contains("not owned by this pipeline")),
        "foreign slot must be reported; got: {:?}",
        report.pipelines[0].hard_errors
    );

    // 2b. Ownership store unreadable while the slot exists -> FAIL closed
    //     (must not PASS by skipping the ownership check).
    let failing: Arc<dyn CheckpointStore> = Arc::new(FailingReadStore);
    let report = check_all(&specs, &failing).await;
    assert!(
        !report.ok,
        "an unreadable ownership store must fail preflight for an existing slot"
    );
    assert!(
        report.pipelines[0]
            .hard_errors
            .iter()
            .any(|e| e.contains("state store is unavailable")),
        "ownership-store-unavailable must be a hard error; got: {:?}",
        report.pipelines[0].hard_errors
    );

    // 3. Owned slot: write a matching ownership record -> PASS.
    let sid: String = client
        .query_one(
            "SELECT system_identifier::text FROM pg_control_system()",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let db_row = client
        .query_one(
            "SELECT current_database()::text, \
             (SELECT oid::int8 FROM pg_database WHERE datname = current_database())",
            &[],
        )
        .await
        .unwrap();
    let record = SlotOwnership {
        record_version: SLOT_OWNER_RECORD_VERSION,
        source_id: SOURCE_ID.into(),
        pipeline: "pf-e2e".into(),
        system_identifier: sid,
        database: db_row.get(0),
        database_oid: db_row.get(1),
        slot: SLOT.into(),
        plugin: "pgoutput".into(),
        lifecycle: SlotLifecycle::Created,
        consistent_lsn: Some("0/0".into()),
        created_at_ms: 0,
    };
    chkpt
        .put_raw(
            &format!("slot_owner:{SOURCE_ID}"),
            &serde_json::to_vec(&record).unwrap(),
        )
        .await
        .unwrap();
    let report = check_all(&specs, &chkpt).await;
    assert!(
        report.ok,
        "an owned existing slot must pass preflight; got: {:?}",
        report.pipelines
    );
}

/// A missing sink credential must fail closed. No container: the source points at
/// a dead port (its own hard error), and we assert the sink credential error is
/// also reported - proving sink secret references are validated during preflight.
#[tokio::test]
async fn preflight_fails_on_missing_sink_secret() {
    let yaml = r#"apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: pf-sink-secret
  tenant: acme
spec:
  source:
    type: postgres
    config:
      id: s
      dsn: "host=127.0.0.1 port=59999 user=none dbname=none connect_timeout=1"
      slot: s_slot
      publication: s_pub
      tables:
        - public.orders
  processors: []
  sinks:
    - type: redis
      config:
        id: r1
        uri: "redis://127.0.0.1:6379"
        uri_secret:
          provider: env
          location: DEFINITELY_MISSING_REDIS_URI_SECRET_XYZ
        stream: df.events
        envelope:
          type: native
        encoding: json
        required: true
  batch:
    max_events: 100
    max_bytes: 8388608
    max_ms: 200
"#;
    let cfg = write_config(yaml);
    let specs = load_cfg(cfg.path().to_str().unwrap()).expect("load config");
    let chkpt: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new().unwrap());

    let report = check_all(&specs, &chkpt).await;
    assert!(!report.ok);
    assert!(
        report.pipelines[0]
            .hard_errors
            .iter()
            .any(|e| e.contains("sink credentials")),
        "a missing sink credential must be reported; got: {:?}",
        report.pipelines[0].hard_errors
    );
}

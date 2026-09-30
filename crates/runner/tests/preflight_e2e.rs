//! Live coverage for the `deltaforge preflight` command (item 1 of the
//! operational pass): it must PASS a correctly provisioned PostgreSQL source and
//! FAIL (fail closed) when a required object is missing - here, the replication
//! slot. Sink reachability is intentionally not probed.

use std::time::Duration;

use deltaforge_config::load_cfg;
use runner::preflight::check_all;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner,
};
use tokio_postgres::NoTls;

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

async fn pg_exec(port: u16, sql: &str) {
    let (client, conn) =
        tokio_postgres::connect(&pg_dsn(port), NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    // One statement per simple query: pg_create_logical_replication_slot cannot
    // run inside the implicit transaction that batching several statements forms.
    client.execute(sql, &[]).await.unwrap();
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
      id: orders-pg
      dsn: "host=127.0.0.1 port={port} user=postgres password=password dbname=postgres"
      slot: df_pf_slot
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

fn write_config(port: u16) -> tempfile::NamedTempFile {
    use std::io::Write;
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(config_yaml(port).as_bytes()).unwrap();
    f.flush().unwrap();
    f
}

#[tokio::test]
#[ignore = "requires docker"]
async fn preflight_passes_on_healthy_source_and_fails_when_slot_missing() {
    let (_c, port) = start_pg().await;

    // Provision a correct source: table, publication, logical slot.
    pg_exec(port, "CREATE TABLE orders (id INT PRIMARY KEY, sku TEXT)").await;
    pg_exec(port, "ALTER TABLE orders REPLICA IDENTITY FULL").await;
    pg_exec(port, "CREATE PUBLICATION df_pf_pub FOR TABLE orders").await;
    pg_exec(
        port,
        "SELECT pg_create_logical_replication_slot('df_pf_slot', 'pgoutput')",
    )
    .await;

    let cfg = write_config(port);
    let path = cfg.path().to_str().unwrap();

    // Healthy: preflight must pass.
    let specs = load_cfg(path).expect("load config");
    let report = check_all(&specs).await;
    assert!(
        report.ok,
        "preflight must pass on a correctly provisioned source; got: {:?}",
        report.pipelines
    );

    // Drop the slot -> preflight must fail closed and name the slot.
    pg_exec(port, "SELECT pg_drop_replication_slot('df_pf_slot');").await;
    let report = check_all(&specs).await;
    assert!(
        !report.ok,
        "preflight must fail when the replication slot is missing"
    );
    let errs = report.pipelines[0].hard_errors.join(" | ");
    assert!(
        errs.contains("df_pf_slot") || errs.to_lowercase().contains("slot"),
        "hard errors should name the missing slot, got: {errs}"
    );
}

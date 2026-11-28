//! The neighbour workload against a real MySQL 8.4: it offers its rate on
//! schedule, records every interval, and shows someone else's row locks as
//! latency from the scheduled start and in the server's lock-wait counter.
//!
//! ```bash
//! cargo test -p fleet-harness --test neighbour_live -- --include-ignored
//! ```

use std::time::{Duration, Instant};

use fleet_harness::config::{Neighbour, NeighbourMix, RunConfig};
use fleet_harness::neighbour;
use gate_ownership::GateOwned;
use mysql_async::prelude::Queryable;
use serde_json::Value;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const EXAMPLE: &str = include_str!("../configs/t1-exploratory.yaml");
const ROWS: u64 = 200;

async fn start_mysql() -> (ContainerAsync<GenericImage>, u16) {
    let c = GenericImage::new("mysql", "8.4")
        .with_wait_for(WaitFor::message_on_stderr(
            "ready for connections. Version: '8.4",
        ))
        .with_env_var("MYSQL_ROOT_PASSWORD", "rootpw")
        .gate_owned()
        .start()
        .await
        .expect("start mysql");
    let port = c.get_host_port_ipv4(3306).await.expect("mysql port");
    let deadline = Instant::now() + Duration::from_secs(60);
    while mysql_async::Conn::new(opts(port)).await.is_err() {
        assert!(
            Instant::now() < deadline,
            "mysql never accepted connections"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    (c, port)
}

fn opts(port: u16) -> mysql_async::Opts {
    mysql_async::Opts::from_url(&format!(
        "mysql://root:rootpw@127.0.0.1:{port}/"
    ))
    .unwrap()
}

async fn build(port: u16, tables: u32) {
    let mut c = mysql_async::Conn::new(opts(port)).await.unwrap();
    c.query_drop("CREATE DATABASE oltp").await.unwrap();
    for t in 0..tables {
        c.query_drop(format!(
            "CREATE TABLE oltp.t{t} (id BIGINT PRIMARY KEY, k INT NOT NULL, \
             c CHAR(120) NOT NULL, pad CHAR(60) NOT NULL)"
        ))
        .await
        .unwrap();
        c.query_drop(format!(
            "INSERT INTO oltp.t{t} WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL \
             SELECT n + 1 FROM s WHERE n < {ROWS}) SELECT n, 0, 'c', 'p' FROM s"
        ))
        .await
        .unwrap();
    }
}

fn config(port: u16, tables: u32, ops_per_sec: f64) -> RunConfig {
    let mut cfg: RunConfig = serde_yaml::from_str(EXAMPLE).unwrap();
    cfg.topology.servers.truncate(1);
    cfg.topology.servers[0].host = "127.0.0.1".into();
    cfg.topology.servers[0].port = port;
    cfg.topology.admin_user = "root".into();
    cfg.topology.admin_password = "rootpw".into();
    cfg.neighbour = Some(Neighbour {
        database: "oltp".into(),
        tables,
        rows_per_table: ROWS,
        connections: 4,
        ops_per_sec,
        mix: NeighbourMix {
            point_select: 0.5,
            update: 0.4,
            insert: 0.1,
        },
        lead_secs: 0,
        interval_secs: 1,
    });
    cfg.validate().unwrap();
    cfg
}

fn records(dir: &std::path::Path, server: &str) -> Vec<Value> {
    std::fs::read_to_string(dir.join(format!("neighbour-{server}.jsonl")))
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

#[tokio::test]
#[ignore = "needs docker"]
async fn the_neighbour_offers_its_rate_and_counts_others_row_locks() {
    let (_mysql, port) = start_mysql().await;
    build(port, 1).await;
    let cfg = config(port, 1, 200.0);
    let server = cfg.topology.servers[0].clone();
    let dir = tempfile::tempdir().unwrap();

    // Alone: on schedule, no errors, no lock waits.
    let running = neighbour::start(&cfg, &server, dir.path()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let alone = running.finish().await.unwrap();
    assert_eq!(alone.errors, 0, "{:?}", alone.first_error);
    let done = alone.done["total"].as_u64().unwrap();
    assert_eq!(done, alone.started["total"].as_u64().unwrap());
    assert!(
        (540..=660).contains(&done),
        "about 3 s at 200/s, got {done}"
    );
    let recs = records(dir.path(), &server.name);
    assert!(recs.len() >= 3, "{} records", recs.len());
    let first = &recs[0]["locks"];
    let waits =
        |r: &Value| r["locks"]["innodb_row_lock_waits"].as_u64().unwrap();
    assert!(first["mdl_waits"].as_u64().is_some(), "{first}");
    assert_eq!(
        waits(recs.last().unwrap()),
        waits(&recs[0]),
        "alone, no row lock waits"
    );
    assert!(
        recs.iter()
            .all(|r| r["lat_us"]["p99"].as_u64().unwrap() < 200_000)
    );

    // Someone else holds every row for 1.5 s: the neighbour's updates wait,
    // the wait is latency from the scheduled start, and the server counts it.
    let dir2 = tempfile::tempdir().unwrap();
    let running = neighbour::start(&cfg, &server, dir2.path()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let mut holder = mysql_async::Conn::new(opts(port)).await.unwrap();
    holder.query_drop("BEGIN").await.unwrap();
    holder
        .query_drop("SELECT id FROM oltp.t0 WHERE id <= 200 FOR UPDATE")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    holder.query_drop("COMMIT").await.unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let held = running.finish().await.unwrap();
    assert_eq!(held.errors, 0, "{:?}", held.first_error);
    let recs = records(dir2.path(), &server.name);
    assert!(
        waits(recs.last().unwrap()) > waits(&recs[0]),
        "the waits are counted"
    );
    let max = recs
        .iter()
        .filter_map(|r| r["lat_us"]["max"].as_u64())
        .max()
        .unwrap();
    assert!(
        max >= 1_000_000,
        "a held row shows as latency, max {max} us"
    );
}

/// The neighbour refuses to start on tables that are not filled.
#[tokio::test]
#[ignore = "needs docker"]
async fn the_neighbour_refuses_unbuilt_tables() {
    let (_mysql, port) = start_mysql().await;
    build(port, 1).await;
    let cfg = config(port, 2, 100.0);
    let server = cfg.topology.servers[0].clone();
    let dir = tempfile::tempdir().unwrap();
    let e = neighbour::start(&cfg, &server, dir.path())
        .await
        .err()
        .unwrap();
    assert!(format!("{e:#}").contains("oltp`.`t1"), "{e:#}");
}

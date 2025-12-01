use anyhow::Result;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use deltaforge_core::{Event, Op, Source};
use deltaforge_sources::mysql::MySqlSource;
use mysql_async::{prelude::Queryable, Pool as MySQLPool};
use std::collections::HashSet;
use std::io::BufRead;
use std::sync::Arc;
use std::time::Instant;
use testcontainers::runners::AsyncRunner;
use testcontainers::{
    core::IntoContainerPort, core::WaitFor, GenericImage, ImageExt,
};
use tokio::{
    io::AsyncBufReadExt,
    sync::mpsc,
    task,
    time::{sleep, timeout, Duration},
};
use tracing::{debug, error, info};

mod common;
use common::init_test_tracing;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_cdc_end_to_end() -> Result<()> {
    init_test_tracing();

    // Boot MySQL 8 with binlog/GTID
    let image = GenericImage::new("mysql", "8.4")
        //.with_entrypoint("mysqld")
        //.with_exposed_port(3306.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_env_var("MYSQL_ROOT_PASSWORD", "password")
        .with_cmd(vec![
            "--server-id=999",
            "--log-bin=/var/lib/mysql/mysql-bin.log",
            "--binlog-format=ROW",
            "--binlog-row-image=FULL",
            "--gtid-mode=ON",
            "--enforce-gtid-consistency=ON",
            "--binlog-checksum=NONE",
        ])
        .with_mapped_port(3307, 3306.tcp());

    let container = image.start().await.expect("start mysql");
    info!("Container ID: {}", container.id());

    // container log followers
    {
        let mut out = container.stdout(true); // follow=true
        task::spawn(async move {
            let mut line = String::new();
            loop {
                line.clear();
                match out.read_line(&mut line).await {
                    Ok(0) => break, // EOF, container stopped
                    Ok(_) => print!("STDOUT: {}", line),
                    Err(e) => {
                        eprint!("stdout read error: {e}");
                        break;
                    }
                }
            }
        });
    }
    {
        let mut err = container.stderr(true);
        task::spawn(async move {
            let mut line = String::new();
            loop {
                line.clear();
                match err.read_line(&mut line).await {
                    Ok(0) => break,
                    Ok(_) => print!("STDERR: {}", line),
                    Err(e) => {
                        eprint!("stderr read error: {e}");
                        break;
                    }
                }
            }
        });
    }

    sleep(Duration::from_secs(8)).await;
    info!("starting .............");
    //let host = container.get_host().await?.to_string();
    let port = 3307; // container.get_host_port_ipv4(3306).await?;
    let root_dsn = format!("mysql://root:password@127.0.0.1:{}/", port);
    let dsn = "mysql://df:dfpw@127.0.0.1:3307/shop";

    // Provision schema + user
    let pool = MySQLPool::new(root_dsn.as_str());
    let mut conn = pool.get_conn().await?;

    let v: (String, String) = conn
        .query_first("SHOW VARIABLES LIKE 'log_bin'")
        .await?
        .unwrap();
    assert_eq!(v.1.to_uppercase(), "ON");

    let gtid: (String, String) = conn
        .query_first("SHOW VARIABLES LIKE 'gtid_mode'")
        .await?
        .unwrap();
    assert!(gtid.1.eq_ignore_ascii_case("ON"));

    conn.query_drop("CREATE DATABASE IF NOT EXISTS shop")
        .await?;

    debug!("database `shop` created.");

    conn.query_drop("USE shop").await?;
    conn.query_drop(
        r#"
        CREATE TABLE IF NOT EXISTS orders(
            id INT PRIMARY KEY,
            sku VARCHAR(64),
            payload JSON,
            blobz BLOB
        )"#,
    )
    .await?;

    debug!("table `orders` created.");

    conn.query_drop(
        r#"CREATE USER IF NOT EXISTS 'df'@'%' IDENTIFIED BY 'dfpw'"#,
    )
    .await?;
    debug!("user `df` created");

    conn.query_drop(
        r#"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'df'@'%'"#,
    )
    .await?;
    conn.query_drop(r#"GRANT SELECT, SHOW VIEW ON shop.* TO 'df'@'%'"#)
        .await?;
    conn.query_drop("FLUSH PRIVILEGES").await?;

    // Start source with df
    let src = MySqlSource {
        id: "it-mysql".into(),
        dsn: dsn.to_string(),
        tables: vec!["shop.orders".into()],
        tenant: "acme".into(),
    };

    let ckpt_store: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new()?);

    let (tx, mut rx) = mpsc::channel::<Event>(128);
    let handle = src.run(tx, ckpt_store).await;
    info!("source is running ...");
    //tokio::spawn(async move {
    //    info!("source is running ...");
    //    _ = src.run(tx, ckpt_store).await
    //});

    sleep(Duration::from_secs(3)).await;
    conn.query_drop(
        r#"INSERT INTO orders VALUES (1,'sku-1','{"a":1}', X'DEADBEEF')"#,
    )
    .await?;
    conn.query_drop("UPDATE orders SET sku='sku-1b' WHERE id=1")
        .await?;
    conn.query_drop("DELETE FROM orders WHERE id=1").await?;

    debug!("insert, update and delete performed on `orders` - expecting 3 events ...");

    // Expect 3 events, but there might be extra stuff as well.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut got = Vec::new();
    let mut seen: HashSet<&str> = HashSet::new();

    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, rx.recv()).await {
            Ok(Some(e)) => {
                debug!(?e);

                match e.op {
                    Op::Insert => {
                        seen.insert("insert");
                    }
                    Op::Update => {
                        seen.insert("update");
                    }
                    Op::Delete => {
                        seen.insert("delete");
                    }
                    _ => {}
                }
                got.push(e);
                debug!(?seen);

                if seen.contains("insert")
                    && seen.contains("update")
                    && seen.contains("delete")
                {
                    info!("all expected events have arrived.");
                    break;
                }
            }
            Ok(None) => {
                break; // source ended: stop waiting
            } 
            Err(_elapsed) => {
                break; // deadline hit: stop waiting
            } 
        }
    }

    assert!(got.iter().any(|e| matches!(e.op, Op::Insert)));
    assert!(got.iter().any(|e| matches!(e.op, Op::Update)));
    assert!(got.iter().any(|e| matches!(e.op, Op::Delete)));

    // Check JSON/BLOB on insert
    let ins = got
        .into_iter()
        .find(|e| matches!(e.op, Op::Insert))
        .unwrap();
    let after = ins.after.as_ref().unwrap();
    assert_eq!(after["id"], 1);
    assert_eq!(after["sku"], "sku-1");
    assert_eq!(after["payload"]["a"], 1);
    assert!(after["blobz"]["_base64"].is_string());
    info!("all asserts are successful!");


    // clean shutdown for source
    handle.stop();
    let _  = handle.join().await;

    conn.disconnect().await?;
    pool.disconnect().await?;

    Ok(())
}

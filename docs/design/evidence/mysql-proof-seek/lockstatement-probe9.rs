//! Probe 9: one statement taking a window's metadata locks
//! (`docs/design/mysql-proof-seek.md`, revision 5 follow-up).
//!
//! `SELECT 1 FROM t1 WHERE FALSE UNION ALL SELECT 1 FROM t2 WHERE FALSE ...`
//! in a read-only transaction, against today's one `SELECT 1 FROM t LIMIT 0`
//! per table. Observed through `performance_schema.metadata_locks`: which
//! locks are granted or pending at each moment, their type and duration.
//!
//! Run (throwaway MySQL 8.4 container):
//! ```bash
//! cargo test -p sources --test mysql_lock_statement_probe -- --ignored --nocapture
//! ```

use std::time::{Duration, Instant};

use gate_ownership::GateOwned;
use mysql_async::prelude::Queryable;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner,
};
use tokio::time::sleep;

const DB: &str = "p9";
const MAX: usize = 2048;

async fn start() -> (ContainerAsync<GenericImage>, u16) {
    let c = GenericImage::new("mysql", "8.4")
        .with_wait_for(WaitFor::message_on_stderr(
            "ready for connections. Version: '8.4",
        ))
        .with_env_var("MYSQL_ROOT_PASSWORD", "pw")
        .with_cmd(vec![
            "--server-id=91".to_string(),
            "--log-bin=mysql-bin".into(),
            "--gtid-mode=ON".into(),
            "--enforce-gtid-consistency=ON".into(),
        ])
        .gate_owned()
        .start()
        .await
        .expect("start mysql");
    let port = c.get_host_port_ipv4(3306).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(90);
    while mysql_async::Conn::from_url(url(port)).await.is_err() {
        assert!(Instant::now() < deadline);
        sleep(Duration::from_millis(500)).await;
    }
    (c, port)
}

fn url(port: u16) -> String {
    format!("mysql://root:pw@127.0.0.1:{port}/")
}

async fn conn(port: u16) -> mysql_async::Conn {
    mysql_async::Conn::from_url(url(port)).await.unwrap()
}

fn name(i: usize) -> String {
    format!("t{i:04}")
}

fn union(tables: &[String]) -> String {
    tables
        .iter()
        .map(|t| format!("SELECT 1 FROM `{DB}`.`{t}` WHERE FALSE"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ")
}

fn per_table(tables: &[String]) -> String {
    tables
        .iter()
        .map(|t| format!("SELECT 1 FROM `{DB}`.`{t}` LIMIT 0;"))
        .collect::<String>()
}

/// The session's table metadata locks in `DB`: (table, type, duration,
/// status).
async fn locks(mon: &mut mysql_async::Conn, id: u32) -> Vec<(String, String, String, String)> {
    mon.exec(
        "SELECT ml.OBJECT_NAME, ml.LOCK_TYPE, ml.LOCK_DURATION, ml.LOCK_STATUS \
         FROM performance_schema.metadata_locks ml \
         JOIN performance_schema.threads th ON ml.OWNER_THREAD_ID = th.THREAD_ID \
         WHERE th.PROCESSLIST_ID = ? AND ml.OBJECT_TYPE = 'TABLE' AND ml.OBJECT_SCHEMA = ? \
         ORDER BY ml.OBJECT_NAME",
        (id, DB),
    )
    .await
    .unwrap()
}

fn granted(l: &[(String, String, String, String)]) -> Vec<String> {
    l.iter()
        .filter(|(_, _, _, s)| s == "GRANTED")
        .map(|(t, ..)| t.clone())
        .collect()
}

fn pending(l: &[(String, String, String, String)]) -> Vec<String> {
    l.iter()
        .filter(|(_, _, _, s)| s == "PENDING")
        .map(|(t, ..)| t.clone())
        .collect()
}

/// Wait until session `id` has a pending lock; its locks then.
async fn when_waiting(
    mon: &mut mysql_async::Conn,
    id: u32,
) -> Vec<(String, String, String, String)> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let l = locks(mon, id).await;
        if !pending(&l).is_empty() {
            return l;
        }
        assert!(Instant::now() < deadline, "session {id} never waited");
        sleep(Duration::from_millis(20)).await;
    }
}

fn summary(names: &[String]) -> String {
    match names {
        [] => "none".into(),
        [one] => one.clone(),
        [first, .., last] => format!("{} ({first}..{last})", names.len()),
    }
}

/// A capture session: `START TRANSACTION READ ONLY`, then `sql`, in a task.
async fn capture(
    port: u16,
    sql: String,
    wait_secs: u32,
) -> (u32, tokio::task::JoinHandle<(mysql_async::Conn, Result<(), String>)>) {
    let mut c = conn(port).await;
    c.query_drop(format!("SET SESSION lock_wait_timeout = {wait_secs}"))
        .await
        .unwrap();
    c.query_drop("START TRANSACTION READ ONLY").await.unwrap();
    let id = c.id();
    let h = tokio::spawn(async move {
        let r = c.query_drop(&sql).await.map_err(|e| e.to_string());
        (c, r)
    });
    (id, h)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "protocol probe: requires docker"]
async fn probe9_single_lock_statement() {
    let (container, port) = start().await;
    let mut c = conn(port).await;
    c.query_drop(format!("CREATE DATABASE {DB}")).await.unwrap();
    let t = Instant::now();
    let mut setup = Vec::new();
    for w in 0..8 {
        let mut c = conn(port).await;
        setup.push(tokio::spawn(async move {
            for i in (w..MAX + 8).step_by(8) {
                c.query_drop(format!(
                    "CREATE TABLE {DB}.{} (id INT PRIMARY KEY, v INT)",
                    name(i)
                ))
                .await
                .unwrap();
            }
        }));
    }
    for s in setup {
        s.await.unwrap();
    }
    println!("setup: {} tables in {:?}", MAX + 8, t.elapsed());
    let v: Option<String> = c.query_first("SELECT VERSION()").await.unwrap();
    let packet: Option<u64> = c.query_first("SELECT @@max_allowed_packet").await.unwrap();
    println!("server {v:?}, max_allowed_packet {packet:?}");
    let mut mon = conn(port).await;

    // A. Coverage, type, duration and cost for N tables.
    println!("\n== A. coverage and cost");
    for n in [1usize, 10, 200, MAX] {
        let tables: Vec<String> = (0..n).map(name).collect();
        let sql = union(&tables);
        for rep in 0..3 {
            let mut s = conn(port).await;
            s.query_drop("START TRANSACTION READ ONLY").await.unwrap();
            let t = Instant::now();
            s.query_drop(&sql).await.unwrap();
            let one = t.elapsed();
            let l = locks(&mut mon, s.id()).await;
            let all_read = l
                .iter()
                .all(|(_, ty, d, st)| ty == "SHARED_READ" && d == "TRANSACTION" && st == "GRANTED");
            s.query_drop("COMMIT").await.unwrap();
            let after = locks(&mut mon, s.id()).await.len();
            let mut m = conn(port).await;
            m.query_drop("START TRANSACTION READ ONLY").await.unwrap();
            let t = Instant::now();
            let mut r = m.query_iter(per_table(&tables)).await.unwrap();
            let mut results = 0;
            for _ in 0..n {
                r.for_each(|_| {}).await.unwrap();
                results += 1;
            }
            r.drop_result().await.unwrap();
            let multi = t.elapsed();
            m.query_drop("COMMIT").await.unwrap();
            println!(
                "N={n} rep {rep}: union {one:?} ({} bytes), held {} SHARED_READ/TRANSACTION/GRANTED={all_read}, \
                 after COMMIT {after}; per-table statements {multi:?} ({results} results)",
                sql.len(),
                l.len()
            );
            assert_eq!(l.len(), n);
            assert!(all_read);
            assert_eq!(after, 0);
        }
    }

    // B. Acquisition order: a middle table is held exclusively by another
    // session; what does the capture hold while it waits?
    println!("\n== B. acquisition order");
    for n in [10usize, 200, MAX] {
        let k = n / 2;
        let sorted: Vec<String> = (0..n).map(name).collect();
        let reversed: Vec<String> = sorted.iter().rev().cloned().collect();
        for (label, order) in [("sorted", &sorted), ("reversed", &reversed)] {
            for rep in 0..3 {
                let mut blocker = conn(port).await;
                blocker
                    .query_drop(format!("LOCK TABLES {DB}.{} WRITE", name(k)))
                    .await
                    .unwrap();
                let (id, h) = capture(port, union(order), 30).await;
                let l = when_waiting(&mut mon, id).await;
                let (g, p) = (granted(&l), pending(&l));
                let below = g.iter().filter(|t| **t < name(k)).count();
                let above = g.iter().filter(|t| **t > name(k)).count();
                blocker.query_drop("UNLOCK TABLES").await.unwrap();
                let (mut s, r) = h.await.unwrap();
                let held = locks(&mut mon, id).await.len();
                s.query_drop("COMMIT").await.unwrap();
                println!(
                    "N={n} {label} rep {rep}: waiting on {}, holding {} below and {} above {}; \
                     then {r:?}, holding {held}",
                    summary(&p),
                    below,
                    above,
                    name(k)
                );
            }
        }
    }

    // C. A multi-table RENAME spanning the set.
    println!("\n== C. RENAME spanning one capture's set");
    for (label, order) in [("sorted", false), ("reversed", true)] {
        for rep in 0..5 {
            let n = 200;
            let mut tables: Vec<String> = (0..n).map(name).collect();
            if order {
                tables.reverse();
            }
            // The capture holds the low half and waits on t0100; the
            // RENAME names t0010 (held) and t0150 (not yet).
            let mut blocker = conn(port).await;
            blocker
                .query_drop(format!("LOCK TABLES {DB}.{} WRITE", name(100)))
                .await
                .unwrap();
            let (id, h) = capture(port, union(&tables), 30).await;
            when_waiting(&mut mon, id).await;
            let mut ren = conn(port).await;
            ren.query_drop("SET SESSION lock_wait_timeout = 30").await.unwrap();
            let rename = tokio::spawn(async move {
                let r = ren
                    .query_drop(format!(
                        "RENAME TABLE {DB}.t0010 TO {DB}.x0010, {DB}.t0150 TO {DB}.x0150"
                    ))
                    .await
                    .map_err(|e| e.to_string());
                (ren, r)
            });
            sleep(Duration::from_millis(200)).await;
            blocker.query_drop("UNLOCK TABLES").await.unwrap();
            let (mut s, r) = h.await.unwrap();
            s.query_drop("COMMIT").await.ok();
            let (mut ren, rr) = rename.await.unwrap();
            println!("{label} rep {rep}: capture {r:?}; RENAME {rr:?}");
            if rr.is_ok() {
                ren.query_drop(format!(
                    "RENAME TABLE {DB}.x0010 TO {DB}.t0010, {DB}.x0150 TO {DB}.t0150"
                ))
                .await
                .unwrap();
            }
        }
    }

    // D. Two concurrent captures, disjoint (odd and even tables), each
    // waiting on one of its own tables, and a RENAME spanning both.
    println!("\n== D. two captures and a RENAME spanning both");
    for rep in 0..10 {
        let odd: Vec<String> = (0..200).filter(|i| i % 2 == 1).map(name).collect();
        let even: Vec<String> = (0..200).filter(|i| i % 2 == 0).map(name).collect();
        let mut b1 = conn(port).await;
        b1.query_drop(format!("LOCK TABLES {DB}.t0101 WRITE")).await.unwrap();
        let mut b2 = conn(port).await;
        b2.query_drop(format!("LOCK TABLES {DB}.t0120 WRITE")).await.unwrap();
        let (id1, h1) = capture(port, union(&odd), 30).await;
        let (id2, h2) = capture(port, union(&even), 30).await;
        when_waiting(&mut mon, id1).await;
        when_waiting(&mut mon, id2).await;
        let mut ren = conn(port).await;
        ren.query_drop("SET SESSION lock_wait_timeout = 30").await.unwrap();
        let rename = tokio::spawn(async move {
            let r = ren
                .query_drop(format!(
                    "RENAME TABLE {DB}.t0011 TO {DB}.x0011, {DB}.t0012 TO {DB}.x0012"
                ))
                .await
                .map_err(|e| e.to_string());
            (ren, r)
        });
        sleep(Duration::from_millis(100 + 20 * rep)).await;
        if rep % 2 == 0 {
            b1.query_drop("UNLOCK TABLES").await.unwrap();
            sleep(Duration::from_millis(50)).await;
            b2.query_drop("UNLOCK TABLES").await.unwrap();
        } else {
            b2.query_drop("UNLOCK TABLES").await.unwrap();
            sleep(Duration::from_millis(50)).await;
            b1.query_drop("UNLOCK TABLES").await.unwrap();
        }
        let (mut s1, r1) = h1.await.unwrap();
        s1.query_drop("COMMIT").await.ok();
        let (mut s2, r2) = h2.await.unwrap();
        s2.query_drop("COMMIT").await.ok();
        let (mut ren, rr) = tokio::time::timeout(Duration::from_secs(40), rename)
            .await
            .expect("the RENAME completes")
            .unwrap();
        println!("rep {rep}: captures {r1:?} {r2:?}; RENAME {rr:?}");
        if rr.is_ok() {
            ren.query_drop(format!(
                "RENAME TABLE {DB}.x0011 TO {DB}.t0011, {DB}.x0012 TO {DB}.t0012"
            ))
            .await
            .unwrap();
        }
    }

    // E. A missing table.
    println!("\n== E. missing table");
    {
        let mut tables: Vec<String> = (0..10).map(name).collect();
        tables.insert(5, "t9999".into());
        let mut s = conn(port).await;
        s.query_drop("START TRANSACTION READ ONLY").await.unwrap();
        let r = s.query_drop(union(&tables)).await;
        let held = locks(&mut mon, s.id()).await;
        println!("missing t9999: {:?}; holding after the error: {}", r.map_err(|e| e.to_string()), held.len());
        s.query_drop("ROLLBACK").await.unwrap();
    }

    // F. Lock timeout and deadlock: what remains held after each.
    println!("\n== F. timeout and deadlock cleanup");
    {
        let tables: Vec<String> = (0..200).map(name).collect();
        let mut blocker = conn(port).await;
        blocker.query_drop(format!("LOCK TABLES {DB}.t0100 WRITE")).await.unwrap();
        let (id, h) = capture(port, union(&tables), 1).await;
        let (mut s, r) = h.await.unwrap();
        let held = locks(&mut mon, id).await;
        println!("timeout: {r:?}; holding after the error: {}", held.len());
        s.query_drop("ROLLBACK").await.unwrap();
        blocker.query_drop("UNLOCK TABLES").await.unwrap();

        // A deadlock needs an unsorted statement: the capture names t0150
        // before t0010; a RENAME of t0010 and t0150 holds t0010 and waits
        // on t0150, which the capture holds while it waits on t0010.
        let mut b = conn(port).await;
        b.query_drop(format!("LOCK TABLES {DB}.t0150 WRITE")).await.unwrap();
        let mut ren = conn(port).await;
        ren.query_drop("SET SESSION lock_wait_timeout = 30").await.unwrap();
        let ren_id = ren.id();
        let rename = tokio::spawn(async move {
            let r = ren
                .query_drop(format!(
                    "RENAME TABLE {DB}.t0010 TO {DB}.y0010, {DB}.t0150 TO {DB}.y0150"
                ))
                .await
                .map_err(|e| e.to_string());
            (ren, r)
        });
        when_waiting(&mut mon, ren_id).await;
        b.query_drop("UNLOCK TABLES").await.unwrap();
        let mut c2 = conn(port).await;
        c2.query_drop("START TRANSACTION READ ONLY").await.unwrap();
        let r = c2
            .query_drop(union(&["t0150".to_string(), "t0010".to_string()]))
            .await
            .map_err(|e| e.to_string());
        let held = locks(&mut mon, c2.id()).await;
        c2.query_drop("ROLLBACK").await.ok();
        let (mut ren, rr) = rename.await.unwrap();
        println!("unsorted against RENAME: capture {r:?}, holding {} after; RENAME {rr:?}", held.len());
        if rr.is_ok() {
            ren.query_drop(format!(
                "RENAME TABLE {DB}.y0010 TO {DB}.t0010, {DB}.y0150 TO {DB}.t0150"
            ))
            .await
            .unwrap();
        }
    }

    // G. A DDL on a locked table waits until the capture commits.
    println!("\n== G. DDL waits for the capture's commit");
    {
        let tables: Vec<String> = (0..200).map(name).collect();
        let mut s = conn(port).await;
        s.query_drop("START TRANSACTION READ ONLY").await.unwrap();
        s.query_drop(union(&tables)).await.unwrap();
        let mut d = conn(port).await;
        let d_id = d.id();
        let t = Instant::now();
        let ddl = tokio::spawn(async move {
            d.query_drop(format!("ALTER TABLE {DB}.t0050 ADD COLUMN w INT"))
                .await
                .map_err(|e| e.to_string())
        });
        let l = when_waiting(&mut mon, d_id).await;
        sleep(Duration::from_secs(1)).await;
        let still = !ddl.is_finished();
        s.query_drop("COMMIT").await.unwrap();
        let committed = t.elapsed();
        let r = ddl.await.unwrap();
        println!(
            "ALTER waiting on {:?} ({}), still waiting after 1 s: {still}; capture committed at {committed:?}, \
             ALTER done at {:?}: {r:?}",
            pending(&l),
            l.iter().map(|(_, ty, ..)| ty.clone()).collect::<Vec<_>>().join(","),
            t.elapsed()
        );
    }

    // H. Statement size and server limits beyond the window's bounds.
    println!("\n== H. size and limits");
    for refs in [MAX, 4 * MAX, 16 * MAX] {
        let tables: Vec<String> = (0..refs).map(|i| name(i % MAX)).collect();
        let sql = union(&tables);
        let mut s = conn(port).await;
        s.query_drop("START TRANSACTION READ ONLY").await.unwrap();
        let t = Instant::now();
        let r = s.query_drop(&sql).await.map_err(|e| e.to_string());
        let held = locks(&mut mon, s.id()).await.len();
        s.query_drop("ROLLBACK").await.ok();
        println!(
            "{refs} references ({} bytes): {r:?} in {:?}, holding {held}",
            sql.len(),
            t.elapsed()
        );
    }
    let long: Vec<String> = (0..MAX).map(|i| format!("{}{:04}", "l".repeat(60), i)).collect();
    for t in long.iter().take(4) {
        c.query_drop(format!("CREATE TABLE {DB}.{t} (id INT PRIMARY KEY)")).await.unwrap();
    }
    let sql = union(&long);
    println!(
        "{MAX} 64-character names: {} bytes ({:.0}% of the 1 MiB request bound)",
        sql.len(),
        100.0 * sql.len() as f64 / (1 << 20) as f64
    );

    container.rm().await.ok();
}

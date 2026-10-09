//! Live PostgreSQL (Docker): the immutable-publication contract enforced by
//! the database itself, with no DeltaForge process running.

use std::time::Duration;

use gate_ownership::GateOwned;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::sync::OnceCell;
use tokio_postgres::{Client, NoTls};

use super::*;

static PG17: OnceCell<(ContainerAsync<GenericImage>, u16)> =
    OnceCell::const_new();

async fn port() -> u16 {
    PG17.get_or_init(|| start("17")).await.1
}

async fn start(version: &str) -> (ContainerAsync<GenericImage>, u16) {
    let c = GenericImage::new("postgres", version)
        .with_wait_for(WaitFor::message_on_stderr("database system is ready"))
        .with_env_var("POSTGRES_PASSWORD", "pw")
        .with_cmd(vec!["postgres", "-c", "wal_level=logical"])
        .gate_owned()
        .start()
        .await
        .expect("start postgres");
    let port = c.get_host_port_ipv4(5432).await.unwrap();
    for _ in 0..60 {
        if tokio_postgres::connect(&dsn(port, "postgres", "postgres"), NoTls)
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    (c, port)
}

fn dsn(port: u16, db: &str, user: &str) -> String {
    format!("host=127.0.0.1 port={port} user={user} password=pw dbname={db}")
}

async fn connect(port: u16, db: &str, user: &str) -> Client {
    let (c, conn) = tokio_postgres::connect(&dsn(port, db, user), NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    c
}

/// A fresh database: role `app` owns schema `s` with tables `s.a`, `s.b`,
/// `public.c`, `public.free` and publication `p` FOR TABLE s.a, s.b, c.
async fn fixture(port: u16, db: &str) -> Client {
    let root = connect(port, "postgres", "postgres").await;
    root.batch_execute(&format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)"))
        .await
        .unwrap();
    root.batch_execute(&format!("CREATE DATABASE {db}"))
        .await
        .unwrap();
    root.batch_execute(
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app') THEN \
         CREATE ROLE app LOGIN PASSWORD 'pw'; END IF; END $$",
    )
    .await
    .unwrap();
    let c = connect(port, db, "postgres").await;
    c.batch_execute(&format!(
        "GRANT CREATE ON DATABASE {db} TO app; GRANT CREATE ON SCHEMA public TO app;"
    ))
    .await
    .unwrap();
    let app = connect(port, db, "app").await;
    app.batch_execute(
        "CREATE SCHEMA s; CREATE TABLE s.a (id int PRIMARY KEY); \
         CREATE TABLE s.b (id int PRIMARY KEY); CREATE TABLE c (id int PRIMARY KEY); \
         CREATE TABLE free (id int PRIMARY KEY); \
         CREATE PUBLICATION p FOR TABLE s.a, s.b, c",
    )
    .await
    .unwrap();
    c
}

fn names(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

async fn digest(c: &Client, pubname: &str) -> String {
    live_state(c, pubname).await.unwrap().unwrap().digest
}

fn refused(r: Result<(), tokio_postgres::Error>, what: &str) {
    let e = r.expect_err(what);
    assert!(
        e.as_db_error()
            .is_some_and(|d| d.message().starts_with("deltaforge:")
                || d.message().contains("must be owner")
                || d.message().starts_with("permission denied")),
        "{what}: {e:?}"
    );
}

/// Registration refuses shapes whose membership or rows change without
/// publication DDL, shows the database-wide impact, transfers ownership and
/// records the digest and position.
#[tokio::test]
#[ignore = "requires docker"]
async fn registration_checks_shape_and_records_the_digest() {
    let port = port().await;
    let root = fixture(port, "pr_register").await;
    root.batch_execute(
        "CREATE PUBLICATION everything FOR ALL TABLES; \
         CREATE PUBLICATION schemas FOR TABLES IN SCHEMA s; \
         CREATE PUBLICATION filtered FOR TABLE c WHERE (id > 0); \
         CREATE PUBLICATION columns FOR TABLE c (id); \
         CREATE PUBLICATION partial FOR TABLE c WITH (publish = 'insert'); \
         CREATE PUBLICATION viaroot FOR TABLE c WITH (publish_via_partition_root = true); \
         CREATE TABLE parted (id int) PARTITION BY RANGE (id); \
         CREATE PUBLICATION partitioned FOR TABLE parted",
    )
    .await
    .unwrap();
    for bad in [
        "everything",
        "schemas",
        "filtered",
        "columns",
        "partial",
        "viaroot",
        "partitioned",
    ] {
        let e = register(&root, &names(&[bad])).await.unwrap_err();
        assert!(
            format!("{e:#}").contains("cannot be registered"),
            "{bad}: {e:#}"
        );
    }
    // Refused registrations left nothing behind.
    assert!(registrations(&root).await.unwrap().is_empty());
    let text = impact(&root).await.unwrap().describe("pr_register");
    assert!(text.contains("every role, superusers included"), "{text}");
    assert!(text.contains("p (owner app)"), "{text}");
    let before = digest(&root, "p").await;
    let regs = register(&root, &names(&["p"])).await.unwrap();
    assert_eq!(regs.len(), 1);
    let v = verify(&root, "p").await.unwrap().unwrap();
    assert_eq!(v.live.owner, PUB_OWNER);
    assert_eq!(v.registration.digest, v.live.digest);
    // The owner is part of the state.
    assert_ne!(v.live.digest, before);
    assert_eq!(
        enforcement_violations(&root).await.unwrap(),
        Vec::<String>::new()
    );
    let text = impact(&root).await.unwrap().describe("pr_register");
    assert!(text.contains("(registered)"), "{text}");
}

/// With no DeltaForge process, the database refuses every publication
/// ALTER and DROP (any publication, any role, superusers included) and any
/// drop removing a registered member, inside the transaction.
#[tokio::test]
#[ignore = "requires docker"]
async fn the_database_refuses_changes_while_registered() {
    let port = port().await;
    let root = fixture(port, "pr_refuse").await;
    root.batch_execute("CREATE PUBLICATION other FOR TABLE free")
        .await
        .unwrap();
    register(&root, &names(&["p"])).await.unwrap();
    let app = connect(port, "pr_refuse", "app").await;
    let before = digest(&root, "p").await;
    for sql in [
        "ALTER PUBLICATION p ADD TABLE free",
        "ALTER PUBLICATION p DROP TABLE c",
        "ALTER PUBLICATION p SET TABLE c",
        "ALTER PUBLICATION p SET (publish = 'insert')",
        "ALTER PUBLICATION p RENAME TO q",
        "ALTER PUBLICATION p OWNER TO postgres",
        "DROP PUBLICATION p",
        // Unregistered publications too: no ALTER reaches PostgreSQL's
        // per-relation collection while enforcement is active.
        "ALTER PUBLICATION other ADD TABLE c",
        "DROP PUBLICATION other",
        "DROP TABLE c",
        "DROP TABLE s.a, free",
        "DROP SCHEMA s CASCADE",
        "DROP OWNED BY app",
        "DROP OWNED BY deltaforge_publication_owner",
    ] {
        let e = root.batch_execute(sql).await.expect_err(sql);
        assert!(
            e.as_db_error()
                .is_some_and(|d| d.message().starts_with("deltaforge:")),
            "superuser {sql}: {e:?}"
        );
        refused(app.batch_execute(sql).await, sql);
    }
    assert_eq!(digest(&root, "p").await, before);
    // Unrelated drops and creations are unaffected.
    app.batch_execute("DROP TABLE free; CREATE TABLE free2 (id int); CREATE PUBLICATION other2 FOR TABLE free2")
        .await
        .unwrap();
}

/// A refused drop inside a savepoint is rolled back with it; the rest of the
/// transaction commits; a failed transaction changes nothing.
#[tokio::test]
#[ignore = "requires docker"]
async fn refusals_are_transactional() {
    let port = port().await;
    let root = fixture(port, "pr_txn").await;
    register(&root, &names(&["p"])).await.unwrap();
    let before = digest(&root, "p").await;
    root.batch_execute("BEGIN; CREATE TABLE kept (id int); SAVEPOINT x")
        .await
        .unwrap();
    refused(
        root.batch_execute("DROP TABLE c").await,
        "drop in savepoint",
    );
    root.batch_execute("ROLLBACK TO SAVEPOINT x; COMMIT")
        .await
        .unwrap();
    let kept: i64 = root
        .query_one(
            "SELECT count(*) FROM pg_class WHERE relname IN ('kept', 'c')",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(kept, 2);
    refused(
        root.batch_execute(
            "BEGIN; CREATE TABLE gone (id int); DROP SCHEMA s CASCADE; COMMIT",
        )
        .await,
        "drop in transaction",
    );
    root.batch_execute("ROLLBACK").await.ok();
    let gone: i64 = root
        .query_one("SELECT count(*) FROM pg_class WHERE relname = 'gone'", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(gone, 0);
    assert_eq!(digest(&root, "p").await, before);
}

/// Uninstall is refused while any registration exists (naming it); a failed
/// or interrupted unregister or registration leaves enforcement intact;
/// unregistering restores the previous owner and then changes are allowed.
#[tokio::test]
#[ignore = "requires docker"]
async fn maintenance_never_leaves_a_registration_unenforced() {
    let port = port().await;
    let root = fixture(port, "pr_maint").await;
    root.batch_execute("CREATE PUBLICATION p2 FOR TABLE free")
        .await
        .unwrap();
    register(&root, &names(&["p", "p2"])).await.unwrap();
    let e = uninstall(&root).await.unwrap_err();
    assert!(format!("{e:#}").contains("p, p2"), "{e:#}");
    // Unregister with one unknown name: nothing changes.
    assert!(unregister(&root, &names(&["p", "nope"])).await.is_err());
    assert_eq!(registrations(&root).await.unwrap().len(), 2);
    assert_eq!(
        enforcement_violations(&root).await.unwrap(),
        Vec::<String>::new()
    );
    // Interrupted mid-transaction (backend terminated before commit).
    let victim = connect(port, "pr_maint", "postgres").await;
    let pid: i32 = victim
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    victim
        .batch_execute(&format!(
            "BEGIN; ALTER EVENT TRIGGER {DDL_GUARD} DISABLE; \
             DELETE FROM deltaforge.registration WHERE pubname = 'p'"
        ))
        .await
        .unwrap();
    root.execute("SELECT pg_terminate_backend($1)", &[&pid])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(registrations(&root).await.unwrap().len(), 2);
    assert_eq!(
        enforcement_violations(&root).await.unwrap(),
        Vec::<String>::new()
    );
    // One left: still refused, still enforced.
    unregister(&root, &names(&["p2"])).await.unwrap();
    assert!(uninstall(&root).await.is_err());
    refused(
        root.batch_execute("ALTER PUBLICATION p2 ADD TABLE c").await,
        "p2 alter",
    );
    unregister(&root, &names(&["p"])).await.unwrap();
    let owner: String = root
        .query_one("SELECT pg_get_userbyid(pubowner)::text FROM pg_publication WHERE pubname = 'p'", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(owner, "app");
    uninstall(&root).await.unwrap();
    root.batch_execute("ALTER PUBLICATION p ADD TABLE free; DROP TABLE c")
        .await
        .unwrap();
    // Re-registration records the new digest.
    let r = register(&root, &names(&["p"])).await.unwrap();
    assert_eq!(
        verify(&root, "p").await.unwrap().unwrap().registration,
        r[0]
    );
}

/// Tampering (superuser) is outside the guarantee but always detected.
#[tokio::test]
#[ignore = "requires docker"]
async fn tampering_is_detected() {
    let port = port().await;
    for (n, (tamper, check_changed)) in [
        (format!("ALTER EVENT TRIGGER {DDL_GUARD} DISABLE"), false),
        (format!("ALTER EVENT TRIGGER {DROP_GUARD} ENABLE"), false),
        (format!("DROP EVENT TRIGGER {DROP_GUARD}"), false),
        ("CREATE OR REPLACE FUNCTION deltaforge.guard_drop() RETURNS event_trigger \
          LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp \
          AS $f$ BEGIN END $f$".to_string(), false),
        ("ALTER ROLE deltaforge_publication_owner LOGIN".to_string(), false),
        ("CREATE ROLE intruder; GRANT deltaforge_publication_owner TO intruder".to_string(), false),
        ("CREATE FUNCTION deltaforge.end_hook() RETURNS event_trigger LANGUAGE plpgsql \
          AS $f$ BEGIN END $f$; CREATE EVENT TRIGGER sneaky ON ddl_command_end \
          EXECUTE FUNCTION deltaforge.end_hook()".to_string(), false),
        (format!(
            "BEGIN; ALTER EVENT TRIGGER {DDL_GUARD} DISABLE; \
             ALTER PUBLICATION p ADD TABLE free; \
             ALTER EVENT TRIGGER {DDL_GUARD} ENABLE ALWAYS; COMMIT"
        ), true),
    ]
    .into_iter()
    .enumerate()
    {
        let root = fixture(port, &format!("pr_tamper{n}")).await;
        register(&root, &names(&["p"])).await.unwrap();
        assert!(verify(&root, "p").await.unwrap().is_ok());
        root.batch_execute(&tamper).await.unwrap();
        let got = verify(&root, "p").await.unwrap();
        // Roles are cluster-wide: undo before the next case.
        root.batch_execute(
            "ALTER ROLE deltaforge_publication_owner NOLOGIN; DROP ROLE IF EXISTS intruder",
        )
        .await
        .unwrap();
        if check_changed {
            assert!(matches!(got, Err(VerifyError::Changed { .. })), "{tamper}: {got:?}");
        } else {
            assert!(matches!(got, Err(VerifyError::Enforcement(_))), "{tamper}: {got:?}");
        }
    }
}

/// Memory proof, PostgreSQL 14-18: with enforcement installed (so the
/// sql_drop trigger enables PostgreSQL's command collection), refused
/// ALTER PUBLICATION SET TABLE statements of 1K, 4K and 10K relations stay
/// small, and the publication is unchanged.
#[tokio::test]
#[ignore = "requires docker; 14-18 lane"]
async fn refused_large_alters_stay_small() {
    for version in ["14", "15", "16", "17", "18"] {
        let (_c, port) = start(version).await;
        let root = fixture(port, "pr_mem").await;
        for from in (0..10_000).step_by(500) {
            let sql: String = (from..from + 500)
                .map(|i| format!("CREATE TABLE m{i} (id int);"))
                .collect();
            root.batch_execute(&format!("BEGIN; {sql} COMMIT"))
                .await
                .unwrap();
        }
        register(&root, &names(&["p"])).await.unwrap();
        let before = digest(&root, "p").await;
        for n in [1_000usize, 4_000, 10_000] {
            let c = connect(port, "pr_mem", "postgres").await;
            let hwm = || async {
                c.query_one(
                    "SELECT substring(pg_read_file('/proc/self/status') \
                     FROM 'VmHWM:\\s+(\\d+) kB')::int8 / 1024",
                    &[],
                )
                .await
                .unwrap()
                .get::<_, i64>(0)
            };
            let start = hwm().await;
            let list = (0..n)
                .map(|i| format!("m{i}"))
                .collect::<Vec<_>>()
                .join(", ");
            refused(
                c.batch_execute(&format!(
                    "ALTER PUBLICATION p SET TABLE {list}"
                ))
                .await,
                "large alter",
            );
            let growth = hwm().await - start;
            println!(
                "MEASURE pg={version} n={n} refused_alter_growth_mib={growth}"
            );
            assert!(growth < 64, "pg {version}, {n} relations: {growth} MiB");
        }
        assert_eq!(digest(&root, "p").await, before);
    }
}

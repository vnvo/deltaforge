//! PostgreSQL failover topologies: a primary and a physical standby (with
//! PostgreSQL 17 logical slot synchronization), promoted while a source is
//! stopped. The source resumes only when the promoted server provably
//! continues its durable checkpoint (design spec 7.24); otherwise it stops
//! with a `pg_continuity_unproven` incident before START_REPLICATION.
//!
//! The source reaches the servers through a TCP proxy standing in for the
//! failover endpoint (VIP, DNS). Slots are synchronized with
//! `pg_sync_replication_slots()` (deterministic) rather than the slot sync
//! worker.

use anyhow::Result;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use common::AllowList;
use deltaforge_config::{SnapshotCfg, SnapshotMode};
use deltaforge_core::incident::{
    EvidenceKey, EvidenceValue, ReasonCode, SafetyState,
};
use deltaforge_core::{Event, Source, SourceError, SourceItem};
use gate_ownership::GateOwned;
use sources::postgres::PostgresSource;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};
use std::time::Instant;
use storage::adapters::incidents::IncidentStore;
use storage::{ArcStorageBackend, MemoryStorageBackend};
use testcontainers::core::{CmdWaitFor, ExecCommand, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::sync::mpsc;
use tokio::time::{Duration, sleep, timeout};
use tokio_postgres::NoTls;

mod test_common;
use test_common::{PG_PASS, PG_USER, init_test_tracing};

const DB: &str = "shop";
const SLOT: &str = "df_slot";
const PUBLICATION: &str = "df_pub";
const SOURCE: &str = "pg-topology";
static UNIQ: AtomicUsize = AtomicUsize::new(0);

// ============================================================================
// Topology
// ============================================================================

struct Topology {
    primary: ContainerAsync<GenericImage>,
    standby: ContainerAsync<GenericImage>,
    primary_port: u16,
    standby_port: u16,
}

fn server_args() -> Vec<String> {
    [
        "wal_level=logical",
        "max_replication_slots=10",
        "max_wal_senders=10",
        "hot_standby_feedback=on",
        // No background transactions: a new synced slot is kept only once the
        // primary's slot has caught up with the standby's transaction horizon.
        "autovacuum=off",
    ]
    .iter()
    .flat_map(|a| ["-c".to_string(), a.to_string()])
    .collect()
}

/// A primary and a streaming physical standby (physical slot `standby_slot`)
/// on a private network.
async fn start_topology(version: &str) -> Topology {
    let n = UNIQ.fetch_add(1, Ordering::Relaxed);
    let tag = format!("{}-{n}", std::process::id());
    let network = format!("df-pgtopo-{tag}");
    let primary_name = format!("df-pgtopo-primary-{tag}");

    let mut cmd = vec!["postgres".to_string()];
    cmd.extend(server_args());
    let primary = GenericImage::new("postgres", version)
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", PG_USER)
        .with_env_var("POSTGRES_PASSWORD", PG_PASS)
        .with_network(network.clone())
        .with_container_name(primary_name.clone())
        .with_cmd(cmd)
        .gate_owned()
        .start()
        .await
        .expect("start the primary");
    let primary_port = primary.get_host_port_ipv4(5432).await.unwrap();
    // The image's entrypoint restarts the server once after init: wait for
    // the final one.
    wait_ready(primary_port).await;
    // `exec` returns once the command starts unless told to wait: the reload
    // below must not run before the rule is in the file.
    primary
        .exec(
            ExecCommand::new([
                "sh",
                "-c",
                "echo 'host replication all all scram-sha-256' \
                 >> /var/lib/postgresql/data/pg_hba.conf",
            ])
            .with_cmd_ready_condition(CmdWaitFor::exit_code(0)),
        )
        .await
        .expect("allow physical replication");
    let root = admin(primary_port, "postgres").await;
    let rules: i64 = root
        .query_one(
            "SELECT count(*) FROM pg_hba_file_rules \
             WHERE type = 'host' AND 'replication' = ANY(database) \
             AND address = 'all' AND error IS NULL",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        rules, 1,
        "phase primary-hba: the replication rule is not in pg_hba.conf"
    );
    root.execute("SELECT pg_reload_conf()", &[]).await.unwrap();
    // The standby's physical slot, created once here: a pg_basebackup retry
    // that also created it would fail forever on "already exists".
    root.execute(
        "SELECT pg_create_physical_replication_slot('standby_slot')",
        &[],
    )
    .await
    .unwrap();

    // The standby clones the primary with pg_basebackup (its recovery
    // configuration uses the physical slot and a dbname in primary_conninfo
    // for slot synchronization), then runs as the postgres user.
    let conninfo = format!(
        "host={primary_name} user={PG_USER} password={PG_PASS} dbname=postgres"
    );
    let mut args = server_args().join(" ");
    if version.parse::<u32>().unwrap_or(0) >= 17 {
        args.push_str(" -c sync_replication_slots=on");
    }
    // Bounded clone attempts, each marked: a refusal ends the container with
    // its reason in the log instead of looping until the startup timeout.
    let script = format!(
        "set -e; D=/tmp/standby; mkdir -p $D; chown postgres $D; chmod 700 $D; \
         n=0; until gosu postgres pg_basebackup -d '{conninfo}' -D $D -R \
           -X stream -S standby_slot; do n=$((n+1)); \
           echo \"phase standby-basebackup: attempt $n failed\" >&2; \
           [ $n -lt 60 ] || {{ echo 'phase standby-basebackup: gave up' >&2; \
             exit 1; }}; rm -rf $D/*; sleep 1; done; \
         echo 'phase standby-start' >&2; \
         exec gosu postgres postgres -D $D {args}"
    );
    let standby_name = format!("df-pgtopo-standby-{tag}");
    let standby = GenericImage::new("postgres", version)
        .with_wait_for(WaitFor::message_on_stderr(
            "ready to accept read-only connections",
        ))
        .with_entrypoint("bash")
        .with_network(network)
        .with_container_name(standby_name.clone())
        .with_cmd(["-c".to_string(), script])
        .with_startup_timeout(Duration::from_secs(180))
        .gate_owned()
        .start()
        .await
        .unwrap_or_else(|e| {
            panic!(
                "start the standby: {e:?}\n--- primary log tail ---\n{}\n\
                 --- standby log tail ---\n{}",
                log_tail(&primary_name),
                log_tail(&standby_name)
            )
        });
    let standby_port = standby.get_host_port_ipv4(5432).await.unwrap();
    wait_ready(standby_port).await;
    Topology {
        primary,
        standby,
        primary_port,
        standby_port,
    }
}

/// The last lines of a container's log, for a setup failure's message.
fn log_tail(container: &str) -> String {
    match std::process::Command::new("docker")
        .args(["logs", "--tail", "40", container])
        .output()
    {
        Ok(out) => format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        ),
        Err(e) => format!("(docker logs failed: {e})"),
    }
}

async fn wait_ready(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok((c, conn)) =
            tokio_postgres::connect(&dsn(port, "postgres"), NoTls).await
        {
            tokio::spawn(conn);
            if c.simple_query("SELECT 1").await.is_ok() {
                // Past the entrypoint's init restart.
                sleep(Duration::from_secs(2)).await;
                if c.simple_query("SELECT 1").await.is_ok() {
                    return;
                }
            }
        }
        assert!(Instant::now() < deadline, "server on {port} never ready");
        sleep(Duration::from_millis(500)).await;
    }
}

fn dsn(port: u16, db: &str) -> String {
    format!(
        "host=127.0.0.1 port={port} user={PG_USER} password={PG_PASS} dbname={db}"
    )
}

async fn admin(port: u16, db: &str) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(&dsn(port, db), NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        conn.await.ok();
    });
    client
}

/// The database, table and publication (no slot: the source creates it, as
/// a failover slot on PostgreSQL 17).
async fn create_schema(port: u16) {
    admin(port, "postgres")
        .await
        .execute(&format!("CREATE DATABASE {DB}"), &[])
        .await
        .unwrap();
    let c = admin(port, DB).await;
    c.batch_execute(&format!(
        "CREATE TABLE orders (id INT PRIMARY KEY, sku TEXT);
         CREATE PUBLICATION {PUBLICATION} FOR TABLE orders;"
    ))
    .await
    .unwrap();
    sources::postgres::postgres_publication::register(
        &c,
        &[PUBLICATION.to_string()],
    )
    .await
    .unwrap();
}

async fn insert(port: u16, id: i64) {
    admin(port, DB)
        .await
        .execute(&format!("INSERT INTO orders VALUES ({id}, 'x')"), &[])
        .await
        .unwrap();
}

async fn lsn_query(port: u16, db: &str, sql: &str) -> Option<String> {
    admin(port, db)
        .await
        .query_opt(sql, &[])
        .await
        .unwrap()
        .and_then(|r| r.get::<_, Option<String>>(0))
}

/// Wait until the standby replayed everything the primary has written.
async fn wait_replayed(t: &Topology) {
    let target = lsn_query(
        t.primary_port,
        "postgres",
        "SELECT pg_current_wal_lsn()::text",
    )
    .await
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let caught_up = admin(t.standby_port, "postgres")
            .await
            .query_one(
                "SELECT pg_last_wal_replay_lsn() >= $1::text::pg_lsn",
                &[&target],
            )
            .await
            .unwrap()
            .get::<_, bool>(0);
        if caught_up {
            return;
        }
        assert!(Instant::now() < deadline, "standby never caught up");
        sleep(Duration::from_millis(300)).await;
    }
}

/// Get the slot synchronized to the standby (the PostgreSQL 17 slot sync
/// worker) while nothing is written: the source streams while its durable
/// checkpoint is moved over idle WAL (as sinks acknowledging idle WAL would).
/// The worker keeps a new synced slot only once it can decode from where it
/// first saw the slot to the confirmed position and the slot's catalog xmin
/// has caught up with the standby, which needs the slot to confirm past
/// running-xacts records written after the last transaction. Ends with the
/// source stopped, the durable checkpoint at the idle position and the
/// standby's slot persistent, synced and at the primary's position.
async fn sync_idle(t: &Topology, proxy: &Proxy, d: &Durable) {
    let src_run = source(proxy, d).await;
    let (tx, _rx) =
        test_common::acked_channel(&src_run, &d.ckpt, &src_run.id, 256);
    let handle = src_run.run(tx, Arc::clone(&d.ckpt)).await;
    let slot_row = format!(
        "SELECT concat_ws(' ', confirmed_flush_lsn, synced, temporary) \
         FROM pg_replication_slots WHERE slot_name = '{SLOT}'"
    );
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut checkpoint = d.checkpoint().await;
    loop {
        let c = admin(t.primary_port, DB).await;
        c.execute("SELECT pg_log_standby_snapshot()", &[])
            .await
            .unwrap();
        c.execute(
            "SELECT pg_logical_emit_message(false, 'df-test', 'idle', true)",
            &[],
        )
        .await
        .unwrap();
        // Flushed WAL only: a checkpoint is never ahead of it.
        let idle = lsn_query(
            t.primary_port,
            DB,
            "SELECT pg_current_wal_flush_lsn()::text",
        )
        .await
        .unwrap();
        checkpoint["lsn"] = idle.clone().into();
        d.ckpt
            .put_raw(SOURCE, &serde_json::to_vec(&checkpoint).unwrap())
            .await
            .unwrap();
        sleep(Duration::from_secs(2)).await;
        wait_replayed(t).await;
        let primary = lsn_query(t.primary_port, DB, &slot_row).await;
        let standby = lsn_query(t.standby_port, DB, &slot_row).await;
        let confirmed = |row: &Option<String>| {
            row.as_deref()
                .and_then(|r| r.split(' ').next())
                .map(String::from)
        };
        let kept = standby.as_deref().is_some_and(|r| r.ends_with(" t f"));
        if kept && confirmed(&standby) == confirmed(&primary) {
            break;
        }
        if Instant::now() >= deadline {
            let log = t.standby.stderr_to_vec().await.unwrap_or_default();
            let log = String::from_utf8_lossy(&log);
            let tail: Vec<_> = log.lines().rev().take(20).collect();
            panic!(
                "the slot never synchronized (primary {primary:?}, standby \
                 {standby:?}); standby log:\n{}",
                tail.into_iter().rev().collect::<Vec<_>>().join("\n")
            );
        }
    }
    handle.stop();
    handle.join().await.ok();
    // The stop persists the read position, which may be before the last
    // idle position the slot confirmed: keep the idle one (nothing between).
    d.ckpt
        .put_raw(SOURCE, &serde_json::to_vec(&checkpoint).unwrap())
        .await
        .unwrap();
}

async fn promote(port: u16) {
    admin(port, "postgres")
        .await
        .execute("SELECT pg_promote(true, 60)", &[])
        .await
        .unwrap();
}

// ============================================================================
// Source
// ============================================================================

/// A TCP proxy standing in for the failover endpoint: every new connection
/// goes to the current target.
struct Proxy {
    port: u16,
    to: Arc<AtomicU16>,
}

impl Proxy {
    async fn start(to: u16) -> Self {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let target = Arc::new(AtomicU16::new(to));
        let t = target.clone();
        tokio::spawn(async move {
            while let Ok((mut client, _)) = l.accept().await {
                let to = t.load(Ordering::SeqCst);
                tokio::spawn(async move {
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
            }
        });
        Self { port, to: target }
    }

    fn switch_to(&self, to: u16) {
        self.to.store(to, Ordering::SeqCst);
    }
}

struct Durable {
    backend: ArcStorageBackend,
    ckpt: Arc<dyn CheckpointStore>,
}

impl Durable {
    fn new() -> Self {
        Self {
            backend: Arc::new(MemoryStorageBackend::new()),
            ckpt: Arc::new(MemCheckpointStore::new().unwrap()),
        }
    }

    async fn checkpoint(&self) -> serde_json::Value {
        serde_json::from_slice(
            &self
                .ckpt
                .get_raw(SOURCE)
                .await
                .unwrap()
                .expect("a checkpoint"),
        )
        .unwrap()
    }

    async fn record(&self) -> Option<serde_json::Value> {
        self.backend
            .kv_get("failover", &format!("pg_continuity:{SOURCE}"))
            .await
            .unwrap()
            .map(|b| serde_json::from_slice(&b).unwrap())
    }
}

async fn source(proxy: &Proxy, d: &Durable) -> PostgresSource {
    PostgresSource {
        id: SOURCE.into(),
        dsn: dsn(proxy.port, DB).into(),
        slot: SLOT.into(),
        publication: PUBLICATION.into(),
        tables: vec!["public.orders".into()],
        tenant: "acme".into(),
        pipeline: "test".into(),
        // The registry lives in the source's durable store, as in a
        // pipeline: Relation bindings reference its versions.
        registry: storage::DurableSchemaRegistry::new(Arc::clone(&d.backend))
            .await
            .unwrap(),
        registry_scope: sources::registry_scope::SharedRegistryScope::default(),
        backend: Arc::clone(&d.backend),
        outbox_prefixes: AllowList::default(),
        snapshot_cfg: SnapshotCfg {
            mode: SnapshotMode::Initial,
            ..Default::default()
        },
        on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
        table_options: Default::default(),
        rotation: None,
        snapshot_cohort: Default::default(),
    }
}

fn ids(events: &[Event]) -> Vec<i64> {
    events
        .iter()
        .filter_map(|e| e.after.as_ref()?.get("id")?.as_i64())
        .collect()
}

async fn collect(
    rx: &mut mpsc::Receiver<SourceItem>,
    dur: Duration,
    until: impl Fn(&[i64]) -> bool,
) -> Vec<i64> {
    let deadline = Instant::now() + dur;
    let mut events = Vec::new();
    while Instant::now() < deadline {
        let left = deadline.saturating_duration_since(Instant::now());
        match timeout(left, rx.recv()).await {
            Ok(Some(SourceItem::Event(e))) => {
                events.push(e);
                if until(&ids(&events)) {
                    break;
                }
            }
            Ok(Some(_)) => continue,
            _ => break,
        }
    }
    ids(&events)
}

/// Run the source until it delivered `id` (inserted once it streams), then
/// stop it: its durable checkpoint is then just after `id`.
async fn run_until(proxy: &Proxy, port: u16, d: &Durable, id: i64) {
    let src_run = source(proxy, d).await;
    let (tx, mut rx) =
        test_common::acked_channel(&src_run, &d.ckpt, &src_run.id, 256);
    let handle = src_run.run(tx, Arc::clone(&d.ckpt)).await;
    sleep(Duration::from_secs(4)).await;
    insert(port, id).await;
    let got =
        collect(&mut rx, Duration::from_secs(30), |ids| ids.contains(&id))
            .await;
    assert!(got.contains(&id), "row {id} streamed: {got:?}");
    handle.stop();
    handle.join().await.ok();
}

/// The class of the `pg_continuity_unproven` incident a run stopped with,
/// after delivering nothing.
async fn refused_with(proxy: &Proxy, d: &Durable) -> String {
    let src_run = source(proxy, d).await;
    let (tx, mut rx) =
        test_common::acked_channel(&src_run, &d.ckpt, &src_run.id, 256);
    let handle = src_run.run(tx, Arc::clone(&d.ckpt)).await;
    let err = match timeout(Duration::from_secs(90), handle.join()).await {
        Ok(Err(e)) => e,
        Ok(Ok(())) => panic!("the source must not continue"),
        Err(_) => panic!("the source did not stop"),
    };
    let err = err.downcast_ref::<SourceError>().expect("a source error");
    let draft = err
        .draft()
        .unwrap_or_else(|| panic!("an incident: {err:?}"));
    assert_eq!(draft.reason_code, ReasonCode::PgContinuityUnproven);
    let delivered = collect(&mut rx, Duration::from_secs(1), |_| false).await;
    assert!(delivered.is_empty(), "nothing delivered: {delivered:?}");
    match draft.evidence.get(EvidenceKey::ReasonClass) {
        Some(EvidenceValue::Text { value }) => value.clone(),
        other => panic!("{other:?}"),
    }
}

// ============================================================================
// Tests
// ============================================================================

/// A standby promoted with a synchronized failover slot continues exactly at
/// the durable checkpoint: the transaction committed on the old primary after
/// the checkpoint and the first transaction on the new primary are both
/// delivered, nothing before the checkpoint again. Before the new timeline is
/// recorded, a source whose record is missing is refused (it cannot tell this
/// history from another), and the refusal changes nothing.
#[tokio::test]
#[ignore = "requires docker"]
async fn a_promoted_standby_with_a_synced_slot_continues_at_the_checkpoint()
-> Result<()> {
    init_test_tracing();
    let t = start_topology("17").await;
    create_schema(t.primary_port).await;
    let root = admin(t.primary_port, "postgres").await;
    root.batch_execute(
        "ALTER SYSTEM SET synchronized_standby_slots = 'standby_slot'",
    )
    .await?;
    root.batch_execute("SELECT pg_reload_conf()").await?;
    let proxy = Proxy::start(t.primary_port).await;
    let d = Durable::new();

    run_until(&proxy, t.primary_port, &d, 1).await;
    sync_idle(&t, &proxy, &d).await;
    let f = d.checkpoint().await;
    let chain = d.record().await.unwrap()["chain_id"].clone();
    assert_eq!(d.record().await.unwrap()["timeline"], 1);
    // Checkpoints carry their continuity stamp.
    assert_eq!(f["timeline"], 1);
    assert_eq!(f["chain"], chain);
    assert_eq!(f["transition"], 0);

    // Committed after the checkpoint, before the failover; the synced slot
    // stays at the checkpoint.
    insert(t.primary_port, 2).await;
    wait_replayed(&t).await;
    t.primary.stop().await?;
    promote(t.standby_port).await;
    insert(t.standby_port, 3).await;
    proxy.switch_to(t.standby_port);

    // Without the record, the switch cannot be told from another history.
    let record = d
        .backend
        .kv_get("failover", &format!("pg_continuity:{SOURCE}"))
        .await?
        .unwrap();
    d.backend
        .kv_delete("failover", &format!("pg_continuity:{SOURCE}"))
        .await?;
    assert_eq!(refused_with(&proxy, &d).await, "timeline_unrecorded");
    assert_eq!(d.checkpoint().await, f, "the refusal moved nothing");
    d.backend
        .kv_put("failover", &format!("pg_continuity:{SOURCE}"), &record)
        .await?;

    let src_run = source(&proxy, &d).await;
    let (tx, mut rx) =
        test_common::acked_channel(&src_run, &d.ckpt, &src_run.id, 256);
    let handle = src_run.run(tx, Arc::clone(&d.ckpt)).await;
    let got =
        collect(&mut rx, Duration::from_secs(60), |ids| ids.contains(&3)).await;
    assert_eq!(got, vec![2, 3], "exactly the changes after the checkpoint");
    let record = d.record().await.unwrap();
    assert_eq!(record["timeline"], 2);
    assert_eq!(record["transition_id"], 1);
    assert_eq!(
        record["chain_id"], chain,
        "the same chain, one transition on"
    );
    handle.stop();
    handle.join().await.ok();
    let after = d.checkpoint().await;
    assert_eq!(after["timeline"], 2);
    assert_eq!(after["chain"], chain);
    assert_eq!(after["transition"], 1);
    drop(t.standby);
    Ok(())
}

/// A standby promoted before the checkpoint forked the history: the old
/// primary went on past the switch point. The fork has the same system
/// identifier, a synchronized slot and more WAL than the checkpoint, and is
/// still refused (`switch_before_checkpoint`); nothing is delivered and the
/// checkpoint and record stay.
#[tokio::test]
#[ignore = "requires docker"]
async fn a_standby_promoted_before_the_checkpoint_is_refused() -> Result<()> {
    init_test_tracing();
    let t = start_topology("17").await;
    create_schema(t.primary_port).await;
    let proxy = Proxy::start(t.primary_port).await;
    let d = Durable::new();

    run_until(&proxy, t.primary_port, &d, 1).await;
    sync_idle(&t, &proxy, &d).await;
    promote(t.standby_port).await;
    // The old primary goes on: the checkpoint moves past the fork.
    run_until(&proxy, t.primary_port, &d, 2).await;
    let f = d.checkpoint().await;
    // The fork has more WAL than the checkpoint.
    for id in 100..400 {
        insert(t.standby_port, id).await;
    }

    proxy.switch_to(t.standby_port);
    assert_eq!(refused_with(&proxy, &d).await, "switch_before_checkpoint");
    assert_eq!(d.checkpoint().await, f);
    assert_eq!(d.record().await.unwrap()["timeline"], 1);
    drop(t.primary);
    Ok(())
}

/// Before PostgreSQL 17 a timeline switch cannot be continued (no failover
/// slots): the source stops explicitly, even when a slot of the same name
/// exists on the promoted server.
#[tokio::test]
#[ignore = "requires docker"]
async fn a_timeline_switch_before_postgresql_17_stops_explicitly() -> Result<()>
{
    init_test_tracing();
    let t = start_topology("16").await;
    create_schema(t.primary_port).await;
    let proxy = Proxy::start(t.primary_port).await;
    let d = Durable::new();

    run_until(&proxy, t.primary_port, &d, 1).await;
    wait_replayed(&t).await;
    t.primary.stop().await?;
    promote(t.standby_port).await;
    admin(t.standby_port, DB)
        .await
        .execute(
            &format!(
                "SELECT pg_create_logical_replication_slot('{SLOT}', 'pgoutput')"
            ),
            &[],
        )
        .await?;
    proxy.switch_to(t.standby_port);
    assert_eq!(
        refused_with(&proxy, &d).await,
        "failover_unsupported_version"
    );
    drop(t.standby);
    Ok(())
}

/// On PostgreSQL 17 a slot that is not a failover slot is reported as a
/// running-degraded incident (the source runs); a later stream on a failover
/// slot resolves it.
#[tokio::test]
#[ignore = "requires docker"]
async fn a_slot_without_failover_is_a_running_degraded_incident() -> Result<()>
{
    init_test_tracing();
    let t = start_topology("17").await;
    create_schema(t.primary_port).await;
    // Pre-created without failover; the source adopts it.
    admin(t.primary_port, DB)
        .await
        .execute(
            &format!(
                "SELECT pg_create_logical_replication_slot('{SLOT}', 'pgoutput')"
            ),
            &[],
        )
        .await?;
    let proxy = Proxy::start(t.primary_port).await;
    let d = Durable::new();
    let mut src = source(&proxy, &d).await;
    src.snapshot_cfg.mode = SnapshotMode::Never;
    let (tx, mut rx) = test_common::acked_channel(&src, &d.ckpt, &src.id, 256);
    let handle = src.run(tx, Arc::clone(&d.ckpt)).await;
    sleep(Duration::from_secs(4)).await;
    insert(t.primary_port, 1).await;
    let got =
        collect(&mut rx, Duration::from_secs(30), |ids| ids.contains(&1)).await;
    assert_eq!(got, vec![1], "the source runs");

    let incidents = IncidentStore::new(Arc::clone(&d.backend), "test");
    let open = incidents.list().await?;
    let degraded: Vec<_> = open
        .iter()
        .filter(|r| r.reason_code == ReasonCode::PgFailoverSlotUnavailable)
        .collect();
    assert_eq!(degraded.len(), 1, "{open:?}");
    assert_eq!(degraded[0].safety_state, SafetyState::RunningDegraded);
    assert!(!degraded[0].status.is_resolved());
    handle.stop();
    handle.join().await.ok();

    // Make it a failover slot; the next stream resolves the incident.
    admin(t.primary_port, DB)
        .await
        .execute(&format!("SELECT pg_drop_replication_slot('{SLOT}')"), &[])
        .await?;
    admin(t.primary_port, DB)
        .await
        .execute(
            &format!(
                "SELECT pg_create_logical_replication_slot('{SLOT}', \
                 'pgoutput', false, false, true)"
            ),
            &[],
        )
        .await?;
    // The checkpoint is behind the new slot: forget it (the stream starts
    // from the slot), keep the incident store.
    d.ckpt.delete(SOURCE).await?;
    let mut src = source(&proxy, &d).await;
    src.snapshot_cfg.mode = SnapshotMode::Never;
    let (tx, _rx) = test_common::acked_channel(&src, &d.ckpt, &src.id, 256);
    let handle = src.run(tx, Arc::clone(&d.ckpt)).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let resolved = incidents.list().await?.iter().any(|r| {
            r.reason_code == ReasonCode::PgFailoverSlotUnavailable
                && r.status.is_resolved()
        });
        if resolved {
            break;
        }
        assert!(Instant::now() < deadline, "the incident was not resolved");
        sleep(Duration::from_millis(500)).await;
    }
    handle.stop();
    handle.join().await.ok();
    drop(t.standby);
    Ok(())
}

/// An endpoint that reaches a hot standby (for example mid-failover) is not
/// streamed from: the session is in recovery, so the source retries like a
/// connection failure with an open `auto_retry` incident
/// (`server_in_recovery`, recommending the writable primary) and no stream;
/// stopping the pipeline withdraws it.
#[tokio::test]
#[ignore = "requires docker"]
async fn a_standby_endpoint_is_retried_without_streaming() -> Result<()> {
    init_test_tracing();
    let t = start_topology("17").await;
    create_schema(t.primary_port).await;
    let proxy = Proxy::start(t.primary_port).await;
    let d = Durable::new();
    run_until(&proxy, t.primary_port, &d, 1).await;
    // The standby holds the synchronized slot, so only the session proof
    // can tell it is not a primary.
    sync_idle(&t, &proxy, &d).await;
    let f = d.checkpoint().await;

    proxy.switch_to(t.standby_port);
    let src_run = source(&proxy, &d).await;
    let (tx, mut rx) =
        test_common::acked_channel(&src_run, &d.ckpt, &src_run.id, 256);
    let handle = src_run.run(tx, Arc::clone(&d.ckpt)).await;
    let incidents = IncidentStore::new(Arc::clone(&d.backend), "test");
    let deadline = Instant::now() + Duration::from_secs(60);
    let retrying = loop {
        let found = incidents.list().await?.into_iter().find(|r| {
            r.reason_code == ReasonCode::PgContinuityUnproven
                && r.evidence.get(EvidenceKey::ReasonClass)
                    == Some(&EvidenceValue::Text {
                        value: "server_in_recovery".into(),
                    })
        });
        if let Some(r) = found {
            break r;
        }
        assert!(Instant::now() < deadline, "no server_in_recovery incident");
        sleep(Duration::from_millis(300)).await;
    };
    assert_eq!(retrying.retryability.as_str(), "auto_retry");
    assert_eq!(
        retrying.actions,
        vec![
            deltaforge_core::incident::ActionCode::VerifyEndpoint,
            deltaforge_core::incident::ActionCode::InspectLogs
        ]
    );
    let delivered = collect(&mut rx, Duration::from_secs(2), |_| false).await;
    assert!(delivered.is_empty(), "nothing streamed: {delivered:?}");

    handle.stop();
    handle.join().await.ok();
    let r = incidents
        .list()
        .await?
        .into_iter()
        .find(|r| r.incident_id == retrying.incident_id)
        .unwrap();
    assert!(
        matches!(
            r.status,
            storage::adapters::incidents::IncidentStatus::Resolved {
                by:
                    storage::adapters::incidents::Resolution::OperationCancelled,
                ..
            }
        ),
        "{:?}",
        r.status
    );
    assert_eq!(d.checkpoint().await, f, "the checkpoint did not move");
    drop(t.primary);
    Ok(())
}

// ---- Recovery: pg-adopt-timeline (docs/design/recovery-cli.md, 5.2) -------

/// Every checkpoint key of the source with its bytes.
async fn all_checkpoints(d: &Durable) -> Vec<(String, Option<Vec<u8>>)> {
    let mut keys = d
        .ckpt
        .list_with_prefix(&format!("{SOURCE}::sink::"))
        .await
        .unwrap();
    keys.push(SOURCE.into());
    keys.sort();
    let mut out = Vec::new();
    for k in keys {
        let v = d.ckpt.get_raw(&k).await.unwrap();
        out.push((k, v));
    }
    out
}

async fn put_json(d: &Durable, key: &str, v: &serde_json::Value) {
    d.ckpt
        .put_raw(key, &serde_json::to_vec(v).unwrap())
        .await
        .unwrap();
}

fn adoption_input<'a>(
    dsn_: &'a str,
    d: &'a Durable,
    sinks: &'a [String],
) -> sources::postgres::postgres_adoption::AdoptionInput<'a> {
    sources::postgres::postgres_adoption::AdoptionInput {
        dsn: dsn_,
        slot: SLOT,
        publication: PUBLICATION,
        source_id: SOURCE,
        tenant: "acme",
        backend: &d.backend,
        checkpoints: &d.ckpt,
        sinks,
        hook: None,
    }
}

/// `pg-adopt-timeline` on a promoted PostgreSQL 17 primary whose source's
/// checkpoints predate continuity records: every refusal writes nothing;
/// adoption creates exactly transition 0 at F and moves no checkpoint; a
/// re-apply is idempotent; an explicit resume runs the ordinary continuity
/// proof and stamps every checkpoint (same chain and transition, same
/// positions) before the first event, then streams exactly the changes
/// after F.
#[tokio::test]
#[ignore = "requires docker"]
async fn a_timeline_is_adopted_for_checkpoints_that_predate_continuity()
-> Result<()> {
    use sources::postgres::postgres_adoption::{
        AdoptionRefusal, adopted_record, apply_adoption, plan_adoption,
    };
    init_test_tracing();
    let t = start_topology("17").await;
    create_schema(t.primary_port).await;
    let root = admin(t.primary_port, "postgres").await;
    root.batch_execute(
        "ALTER SYSTEM SET synchronized_standby_slots = 'standby_slot'",
    )
    .await?;
    root.batch_execute("SELECT pg_reload_conf()").await?;
    let proxy = Proxy::start(t.primary_port).await;
    let d = Durable::new();
    run_until(&proxy, t.primary_port, &d, 1).await;
    sync_idle(&t, &proxy, &d).await;

    // Checkpoints written before continuity records: unstamped, two sinks
    // at F, and no record.
    let mut f = d.checkpoint().await;
    for k in ["timeline", "chain", "transition"] {
        f.as_object_mut().unwrap().remove(k);
    }
    let f_lsn = f["lsn"].as_str().unwrap().to_string();
    for (k, _) in all_checkpoints(&d).await {
        d.ckpt.delete(&k).await?;
    }
    let (sink_a, sink_b) =
        (format!("{SOURCE}::sink::a"), format!("{SOURCE}::sink::b"));
    put_json(&d, SOURCE, &f).await;
    put_json(&d, &sink_a, &f).await;
    put_json(&d, &sink_b, &f).await;
    d.backend
        .kv_delete("failover", &format!("pg_continuity:{SOURCE}"))
        .await?;
    let sinks = vec!["a".to_string(), "b".to_string()];
    let before = all_checkpoints(&d).await;
    let zero_writes = |what: &str| {
        let what = what.to_string();
        let before = before.clone();
        let d = &d;
        async move {
            assert!(d.record().await.is_none(), "{what}: no record");
            assert_eq!(all_checkpoints(d).await, before, "{what}: no move");
        }
    };

    // Committed after F, before the failover.
    insert(t.primary_port, 2).await;
    wait_replayed(&t).await;
    // A standby endpoint is refused.
    let standby_dsn = dsn(t.standby_port, DB);
    assert!(matches!(
        plan_adoption(&adoption_input(&standby_dsn, &d, &sinks)).await,
        Err(AdoptionRefusal::Precondition(m)) if m.contains("standby")
    ));
    zero_writes("standby").await;

    t.primary.stop().await?;
    promote(t.standby_port).await;
    insert(t.standby_port, 3).await;
    proxy.switch_to(t.standby_port);
    assert_eq!(refused_with(&proxy, &d).await, "timeline_unrecorded");
    zero_writes("the refused start").await;
    // The promoted endpoint is a primary and its synchronized slot is idle.
    let promoted = admin(t.standby_port, DB).await;
    let row = promoted
        .query_one(
            &format!(
                "SELECT pg_is_in_recovery(), active, synced \
                 FROM pg_replication_slots WHERE slot_name = '{SLOT}'"
            ),
            &[],
        )
        .await?;
    assert!(!row.get::<_, bool>(0), "primary");
    assert!(!row.get::<_, bool>(1), "inactive");
    assert!(row.get::<_, bool>(2), "synchronized");

    // Every other refusal writes nothing.
    let input = adoption_input(&standby_dsn, &d, &sinks);
    // An active slot.
    t.standby
        .exec(ExecCommand::new([
            "sh",
            "-c",
            &format!(
                "setsid gosu postgres pg_recvlogical -h /var/run/postgresql \
                 -d {DB} --slot {SLOT} --start -o proto_version=1 \
                 -o publication_names={PUBLICATION} -f /dev/null \
                 >/tmp/recv.log 2>&1 &"
            ),
        ]))
        .await?;
    let active = format!(
        "SELECT active FROM pg_replication_slots WHERE slot_name = '{SLOT}'"
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    while !promoted.query_one(&active, &[]).await?.get::<_, bool>(0) {
        assert!(Instant::now() < deadline, "the slot never became active");
        sleep(Duration::from_millis(200)).await;
    }
    assert!(matches!(
        plan_adoption(&input).await,
        Err(AdoptionRefusal::Precondition(m)) if m.contains("in use")
    ));
    zero_writes("active slot").await;
    t.standby
        .exec(
            ExecCommand::new(["sh", "-c", "pkill pg_recvlogical; true"])
                .with_cmd_ready_condition(CmdWaitFor::exit()),
        )
        .await?;
    let deadline = Instant::now() + Duration::from_secs(30);
    while promoted.query_one(&active, &[]).await?.get::<_, bool>(0) {
        assert!(Instant::now() < deadline, "the slot stayed active");
        sleep(Duration::from_millis(200)).await;
    }
    // F past the WAL flush, a slot bound past F, a snapshot position, a
    // malformed position, a partial set: each refused, nothing written.
    let at = |lsn: &str| {
        let mut v = f.clone();
        v["lsn"] = lsn.into();
        v
    };
    let adopted =
        serde_json::to_value(serde_json::from_slice::<serde_json::Value>(
            &sources::snapshot_position::encode_adopted("c", 2, "d"),
        )?)?;
    // (what, checkpoint writes (None: delete), expected refusal text)
    type Case<'a> =
        (&'a str, Vec<(String, Option<serde_json::Value>)>, &'a str);
    let cases: Vec<Case<'_>> = vec![
        (
            "flush below F",
            vec![
                (sink_a.clone(), Some(at("FF/0"))),
                (sink_b.clone(), Some(at("FF/0"))),
            ],
            "WAL flush",
        ),
        (
            "bound past F",
            vec![(sink_a.clone(), Some(at("0/1")))],
            "past the checkpoint",
        ),
        (
            "snapshot position",
            vec![(sink_b.clone(), Some(adopted))],
            "snapshot position",
        ),
        (
            "malformed",
            vec![(sink_b.clone(), Some(serde_json::json!({"lsn": "garbage"})))],
            "cannot be interpreted",
        ),
        ("partial", vec![(sink_b.clone(), None)], "partial"),
        (
            "stamped",
            vec![(
                sink_b.clone(),
                Some({
                    let mut v = f.clone();
                    v["timeline"] = 1.into();
                    v["chain"] = "c".into();
                    v["transition"] = 0.into();
                    v
                }),
            )],
            "continuity stamp",
        ),
    ];
    for (what, writes, expect) in cases {
        for (k, v) in &writes {
            match v {
                Some(v) => put_json(&d, k, v).await,
                None => {
                    d.ckpt.delete(k).await?;
                }
            }
        }
        let r = plan_adoption(&input).await;
        let msg = match &r {
            Err(
                AdoptionRefusal::Precondition(m) | AdoptionRefusal::Manual(m),
            ) => m.clone(),
            other => panic!("{what}: {other:?}"),
        };
        assert!(msg.contains(expect), "{what}: {msg}");
        assert!(d.record().await.is_none(), "{what}: no record");
        for (k, v) in &before {
            match v {
                Some(b) => d.ckpt.put_raw(k, b).await?,
                None => {
                    d.ckpt.delete(k).await?;
                }
            }
        }
        zero_writes(what).await;
    }

    // Adoption: transition 0 at F, nothing moved; the same apply again is
    // idempotent. First, an apply after the slot became active since the
    // plan is refused at apply time, nothing written.
    let planned = plan_adoption(&input).await.unwrap();
    let chain = "0123456789abcdef0123456789abcdef";
    let record = adopted_record(chain, &planned.facts, &planned.f);
    t.standby
        .exec(ExecCommand::new([
            "sh",
            "-c",
            &format!(
                "setsid gosu postgres pg_recvlogical -h /var/run/postgresql \
                 -d {DB} --slot {SLOT} --start -o proto_version=1 \
                 -o publication_names={PUBLICATION} -f /dev/null \
                 >/tmp/recv.log 2>&1 &"
            ),
        ]))
        .await?;
    let deadline = Instant::now() + Duration::from_secs(30);
    while !promoted.query_one(&active, &[]).await?.get::<_, bool>(0) {
        assert!(Instant::now() < deadline, "the slot never became active");
        sleep(Duration::from_millis(200)).await;
    }
    assert!(matches!(
        apply_adoption(&input, &planned, &record).await,
        Err(AdoptionRefusal::Precondition(_))
    ));
    zero_writes("apply with an active slot").await;
    t.standby
        .exec(
            ExecCommand::new(["sh", "-c", "pkill pg_recvlogical; true"])
                .with_cmd_ready_condition(CmdWaitFor::exit()),
        )
        .await?;
    let deadline = Instant::now() + Duration::from_secs(30);
    while promoted.query_one(&active, &[]).await?.get::<_, bool>(0) {
        assert!(Instant::now() < deadline, "the slot stayed active");
        sleep(Duration::from_millis(200)).await;
    }
    let planned = plan_adoption(&input).await.unwrap();
    assert_eq!(planned.f, f_lsn);
    assert_eq!(planned.facts.timeline, 2);
    assert!(!planned.facts.in_recovery);
    let record = adopted_record(chain, &planned.facts, &planned.f);
    apply_adoption(&input, &planned, &record).await.unwrap();
    let rec = d.record().await.unwrap();
    assert_eq!(rec["chain_id"], chain);
    assert_eq!(rec["transition_id"], 0);
    assert_eq!(rec["timeline"], 2);
    assert_eq!(rec["proven_at"], f_lsn.as_str());
    assert_eq!(
        all_checkpoints(&d).await,
        before,
        "apply moves no checkpoint"
    );
    apply_adoption(&input, &planned, &record).await.unwrap();
    assert_eq!(d.record().await.unwrap(), rec);
    // With the record there is nothing left to adopt.
    assert!(matches!(
        plan_adoption(&input).await,
        Err(AdoptionRefusal::NotApplicable(m)) if m.contains("continuity record")
    ));

    // The explicit resume: the ordinary proof against the record, the
    // checkpoints stamped before the first event, then exactly the changes
    // after F.
    let src_run = source(&proxy, &d).await;
    let (tx, mut rx) =
        test_common::acked_channel(&src_run, &d.ckpt, &src_run.id, 256);
    let handle = src_run.run(tx, Arc::clone(&d.ckpt)).await;
    let first =
        collect(&mut rx, Duration::from_secs(60), |ids| !ids.is_empty()).await;
    assert!(!first.is_empty(), "the stream started");
    for key in [&sink_a, &sink_b] {
        let cp: serde_json::Value =
            serde_json::from_slice(&d.ckpt.get_raw(key).await?.unwrap())?;
        assert_eq!(cp["chain"], chain, "{key}");
        assert_eq!(cp["transition"], 0, "{key}");
        assert_eq!(cp["timeline"], 2, "{key}");
        assert_eq!(cp["lsn"], f_lsn.as_str(), "{key}: the same position");
    }
    let mut got = first;
    got.extend(
        collect(&mut rx, Duration::from_secs(60), |ids| ids.contains(&3)).await,
    );
    assert_eq!(got, vec![2, 3], "exactly the changes after F");
    handle.stop();
    handle.join().await.ok();
    let after = d.record().await.unwrap();
    assert_eq!(after["chain_id"], chain);
    assert_eq!(after["transition_id"], 0, "no further transition");
    Ok(())
}

// ============================================================================
// Catalog visibility (design section 8): the catalog session must reach the
// node and walsender that produce the stream
// ============================================================================

/// A failover endpoint that sends replication sessions and ordinary
/// sessions (the source's catalog connection) to separately switchable
/// servers, by the startup packet's `replication` parameter.
struct SplitProxy {
    port: u16,
    other: Arc<AtomicU16>,
}

impl SplitProxy {
    async fn start(replication_to: u16, other_to: u16) -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let other = Arc::new(AtomicU16::new(other_to));
        let o = other.clone();
        tokio::spawn(async move {
            while let Ok((mut client, _)) = l.accept().await {
                let o = o.clone();
                tokio::spawn(async move {
                    let mut head = [0u8; 8];
                    if client.read_exact(&mut head).await.is_err() {
                        return;
                    }
                    // SSLRequest: decline, read the real startup packet.
                    if u32::from_be_bytes(head[4..8].try_into().unwrap())
                        == 80_877_103
                    {
                        if client.write_all(b"N").await.is_err()
                            || client.read_exact(&mut head).await.is_err()
                        {
                            return;
                        }
                    }
                    let len = u32::from_be_bytes(head[0..4].try_into().unwrap())
                        as usize;
                    let mut rest = vec![0u8; len.saturating_sub(8)];
                    if client.read_exact(&mut rest).await.is_err() {
                        return;
                    }
                    let parts: Vec<&[u8]> = rest.split(|b| *b == 0).collect();
                    let replication = parts
                        .windows(2)
                        .any(|w| w[0] == b"replication" && w[1] != b"false");
                    let to = if replication {
                        replication_to
                    } else {
                        o.load(Ordering::SeqCst)
                    };
                    let Ok(mut server) =
                        tokio::net::TcpStream::connect(("127.0.0.1", to)).await
                    else {
                        return;
                    };
                    if server.write_all(&head).await.is_err()
                        || server.write_all(&rest).await.is_err()
                    {
                        return;
                    }
                    let _ =
                        tokio::io::copy_bidirectional(&mut client, &mut server)
                            .await;
                });
            }
        });
        Self { port, other }
    }

    fn route_catalog_to(&self, to: u16) {
        self.other.store(to, Ordering::SeqCst);
    }
}

async fn cdc_source(port: u16, d: &Durable) -> PostgresSource {
    let mut s = source(
        &Proxy {
            port,
            to: Arc::new(AtomicU16::new(0)),
        },
        d,
    )
    .await;
    s.snapshot_cfg = SnapshotCfg {
        mode: SnapshotMode::Never,
        ..Default::default()
    };
    s
}

/// V1: with the stream on the primary and the catalog connection routed to
/// the physical standby (in recovery, behind), no catalog fact is used: the
/// source fails closed before the row.
#[tokio::test]
#[ignore = "requires docker"]
async fn a_catalog_session_on_a_standby_is_refused() -> Result<()> {
    init_test_tracing();
    let t = start_topology("17").await;
    create_schema(t.primary_port).await;
    wait_replayed(&t).await;
    let proxy = SplitProxy::start(t.primary_port, t.primary_port).await;
    let d = Durable::new();
    let src = cdc_source(proxy.port, &d).await;
    let (tx, mut rx) = test_common::acked_channel(&src, &d.ckpt, &src.id, 256);
    let handle = src.run(tx, Arc::clone(&d.ckpt)).await;
    sleep(Duration::from_secs(4)).await;
    proxy.route_catalog_to(t.standby_port);
    insert(t.primary_port, 1).await;
    let got =
        collect(&mut rx, Duration::from_secs(60), |ids| ids.contains(&1)).await;
    assert!(got.is_empty(), "no row without a proven catalog: {got:?}");
    let ended = timeout(Duration::from_secs(60), handle.join)
        .await
        .expect("the source stops")
        .expect("task");
    let msg = format!("{:#}", ended.expect_err("fails closed"));
    assert!(
        msg.contains("server_in_recovery") || msg.contains("slot_not_held"),
        "{msg}"
    );
    drop(t.standby);
    Ok(())
}

/// V3: a promoted node of the same cluster (same system identifier and
/// database) that does not hold the stream's slot is refused; once the
/// catalog connection is routed to the streaming node, the row flows.
#[tokio::test]
#[ignore = "requires docker"]
async fn a_catalog_endpoint_on_another_node_is_refused_until_routed()
-> Result<()> {
    init_test_tracing();
    let t = start_topology("17").await;
    create_schema(t.primary_port).await;
    wait_replayed(&t).await;
    promote(t.standby_port).await;
    let proxy = SplitProxy::start(t.primary_port, t.primary_port).await;
    let d = Durable::new();
    let src = cdc_source(proxy.port, &d).await;
    let (tx, mut rx) = test_common::acked_channel(&src, &d.ckpt, &src.id, 256);
    let handle = src.run(tx, Arc::clone(&d.ckpt)).await;
    sleep(Duration::from_secs(4)).await;
    proxy.route_catalog_to(t.standby_port);
    insert(t.primary_port, 1).await;
    let early =
        collect(&mut rx, Duration::from_secs(6), |ids| ids.contains(&1)).await;
    assert!(
        early.is_empty(),
        "refused while routed elsewhere: {early:?}"
    );
    proxy.route_catalog_to(t.primary_port);
    let got =
        collect(&mut rx, Duration::from_secs(40), |ids| ids.contains(&1)).await;
    assert_eq!(got, vec![1], "the row flows once the proof holds");
    handle.stop();
    handle.join().await.ok();
    drop(t.standby);
    Ok(())
}

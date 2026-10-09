//! Metric cardinality through real pipelines (PipelineManager, PostgreSQL in
//! a container, an HTTP sink served by the test), against the metrics the
//! process actually records.
//!
//! - Default: a pipeline over 10 tables and one over 1,000 tables record
//!   identical series sets (pipeline, source and sink values aside), and no
//!   series of either carries a `table` or `table_scope` label.
//! - `metrics.per_table` enabled with `max_tables: 5` over the 1,000 tables:
//!   every table-labelled metric holds at most 5 exact series plus one
//!   overflow series per remaining label set; the admitted, overflow and
//!   overflowed-table metrics report the truncation; the per-table lag holds
//!   at most 6 series, expires after `lag_idle_secs` without events, comes
//!   back with new events, and is gone with the pipeline's policy once the
//!   pipeline is deleted.
//!
//! Run with:
//! ```bash
//! cargo test -p runner --test metric_cardinality_e2e -- --include-ignored --test-threads=1
//! ```

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use anyhow::Result;
use deltaforge_core::table_metrics;
use gate_ownership::GateOwned;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use rest_api::PipelineController;
use runner::pipeline_manager::PipelineManager;
use storage::{ArcStorageBackend, MemoryStorageBackend};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner,
};
use tokio::time::sleep;
use tokio_postgres::NoTls;

const DB: &str = "card";
const PASS: &str = "pg";
const SMALL: usize = 10;
const LARGE: usize = 1_000;
const CAP: usize = 5;
const LAG_IDLE_SECS: u64 = 10;
/// The coordinator's closed set of batch flush causes.
const TRIGGERS: [&str; 7] = [
    "barrier",
    "boundary",
    "cancelled",
    "limits",
    "shutdown",
    "timer",
    "tx_commit",
];

/// Every metric this process records, installed before anything records.
fn snapshotter() -> &'static Snapshotter {
    static SNAPSHOTTER: OnceLock<Snapshotter> = OnceLock::new();
    SNAPSHOTTER.get_or_init(|| {
        let rec = DebuggingRecorder::new();
        let snapshotter = rec.snapshotter();
        metrics::set_global_recorder(rec).expect("install the recorder");
        snapshotter
    })
}

/// A metric name and its sorted labels.
type Shape = (String, Vec<(String, String)>);

/// One recorded series: metric name, sorted labels, value.
#[derive(Debug, Clone)]
struct Series {
    name: String,
    labels: Vec<(String, String)>,
    value: f64,
}

fn series() -> Vec<Series> {
    snapshotter()
        .snapshot()
        .into_vec()
        .into_iter()
        .map(|(k, _, _, v)| {
            let mut labels: Vec<_> = k
                .key()
                .labels()
                .map(|l| (l.key().to_string(), l.value().to_string()))
                .collect();
            labels.sort();
            let value = match v {
                DebugValue::Counter(n) => n as f64,
                DebugValue::Gauge(g) => g.into_inner(),
                DebugValue::Histogram(h) => h.len() as f64,
            };
            Series {
                name: k.key().name().to_string(),
                labels,
                value,
            }
        })
        .collect()
}

fn label<'a>(s: &'a Series, key: &str) -> Option<&'a str> {
    s.labels
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

/// `pipeline`'s series shapes, its identifying label values normalized.
fn shapes(pipeline: &str) -> BTreeSet<Shape> {
    series()
        .into_iter()
        .filter(|s| label(s, "pipeline") == Some(pipeline))
        .map(|s| {
            let labels = s
                .labels
                .into_iter()
                .map(|(k, v)| match k.as_str() {
                    "pipeline" | "source" => (k, "*".into()),
                    _ => (k, v),
                })
                .collect();
            (s.name, labels)
        })
        .collect()
}

fn delivered(pipeline: &str) -> f64 {
    series()
        .iter()
        .filter(|s| {
            s.name == "deltaforge_sink_events_total"
                && label(s, "pipeline") == Some(pipeline)
        })
        .map(|s| s.value)
        .sum()
}

fn events_seen(pipeline: &str) -> f64 {
    series()
        .iter()
        .filter(|s| {
            s.name == "deltaforge_source_events_total"
                && label(s, "pipeline") == Some(pipeline)
        })
        .map(|s| s.value)
        .sum()
}

async fn client(port: u16) -> tokio_postgres::Client {
    let dsn = format!(
        "host=127.0.0.1 port={port} user=postgres password={PASS} dbname={DB}"
    );
    let (c, conn) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
    tokio::spawn(async move {
        conn.await.ok();
    });
    c
}

async fn start_postgres() -> (ContainerAsync<GenericImage>, u16) {
    let c = GenericImage::new("postgres", "17")
        .with_wait_for(WaitFor::message_on_stderr("database system is ready"))
        .with_env_var("POSTGRES_PASSWORD", PASS)
        .with_cmd([
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=10",
            "-c",
            "max_wal_senders=10",
        ])
        .gate_owned()
        .start()
        .await
        .expect("start postgres");
    let port = c.get_host_port_ipv4(5432).await.expect("pg port");
    sleep(Duration::from_secs(3)).await;
    let dsn = format!(
        "host=127.0.0.1 port={port} user=postgres password={PASS} dbname=postgres"
    );
    let (admin, conn) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
    tokio::spawn(async move {
        conn.await.ok();
    });
    admin
        .execute(&format!("CREATE DATABASE {DB}"), &[])
        .await
        .unwrap();
    let db = client(port).await;
    for (schema, n) in [("s10", SMALL), ("s1k", LARGE)] {
        db.batch_execute(&format!(
            "CREATE SCHEMA {schema};
             DO $$ BEGIN
               FOR i IN 0..{last} LOOP
                 EXECUTE format('CREATE TABLE {schema}.t%s (id INT PRIMARY KEY, v TEXT)', i);
               END LOOP;
             END $$;
",
            last = n - 1
        ))
        .await
        .unwrap();
    }
    // The large publication is built (in batches) before any registration.
    for (schema, n) in [("s1k", LARGE), ("s10", SMALL)] {
        let tables: Vec<String> =
            (0..n).map(|i| format!("{schema}.t{i}")).collect();
        let tables: Vec<&str> = tables.iter().map(String::as_str).collect();
        sources::postgres::postgres_publication::fixtures::recreate_registered(
            &db,
            &format!("pub_{schema}"),
            &tables,
        )
        .await
        .unwrap();
    }
    (c, port)
}

/// One row in each of the schema's tables, in one transaction.
async fn touch(db: &tokio_postgres::Client, schema: &str, n: usize, id: i32) {
    db.batch_execute(&format!(
        "DO $$ BEGIN
           FOR i IN 0..{last} LOOP
             EXECUTE format('INSERT INTO {schema}.t%s VALUES ({id}, ''x'')', i);
           END LOOP;
         END $$;",
        last = n - 1
    ))
    .await
    .unwrap();
}

async fn sink() -> String {
    let app =
        axum::Router::new().route("/", axum::routing::post(|| async { "ok" }));
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", l.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(l, app).await.ok();
    });
    url
}

fn spec(
    name: &str,
    port: u16,
    schema: &str,
    url: &str,
    per_table: Option<usize>,
) -> deltaforge_config::PipelineSpec {
    let metrics = per_table.map_or(String::new(), |max| {
        format!(
            "  metrics:\n    per_table:\n      enabled: true\n      \
             max_tables: {max}\n      lag_idle_secs: {LAG_IDLE_SECS}\n"
        )
    });
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
      id: src-{name}
      dsn: "host=127.0.0.1 port={port} user=postgres password={PASS} dbname={DB}"
      slot: slot_{name}
      publication: pub_{schema}
      tables: ["{schema}.*"]
      snapshot:
        mode: initial
  processors: []
  sinks:
    - type: http
      config:
        id: hook
        url: "{url}"
        batch_mode: true
        required: true
        send_timeout_secs: 5
        batch_timeout_secs: 5
        connect_timeout_secs: 2
  batch:
    max_events: 100000
    max_ms: 200
    respect_source_tx: true
{metrics}"#
    );
    serde_yaml::from_str(&yaml).expect("pipeline spec")
}

async fn until(what: &str, mut pred: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(300);
    while !pred() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what} (events: small {}, large {}, capped {}; \
             recorded: {:?})",
            events_seen("small"),
            events_seen("large"),
            events_seen("capped"),
            series()
                .iter()
                .filter(|s| label(s, "pipeline") == Some("small"))
                .map(|s| (s.name.clone(), s.labels.clone(), s.value))
                .collect::<Vec<_>>()
        );
        sleep(Duration::from_millis(200)).await;
    }
}

async fn until_running(mgr: &PipelineManager, name: &str) {
    let deadline = Instant::now() + Duration::from_secs(300);
    loop {
        let info = PipelineController::get(mgr, name).await.unwrap();
        if info.status == "running" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{name} never ran: {}",
            info.status
        );
        sleep(Duration::from_millis(200)).await;
    }
}

fn lag_series(pipeline: &str) -> Vec<String> {
    o11y::table_lag::global()
        .render(Instant::now())
        .lines()
        .filter(|l| l.contains(&format!("pipeline=\"{pipeline}\"")))
        .map(str::to_string)
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "PostgreSQL container (docker)"]
async fn default_cardinality_ignores_table_count_and_opt_in_is_bounded()
-> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "warn".into()),
        )
        .try_init()
        .ok();
    snapshotter();
    let (_pg, port) = start_postgres().await;
    let db = client(port).await;
    let url = sink().await;
    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let mgr = PipelineManager::with_backend(backend).await?;

    // A row per table before the start, so the initial snapshots copy rows.
    touch(&db, "s10", SMALL, 0).await;
    touch(&db, "s1k", LARGE, 0).await;
    mgr.start_pipeline(spec("small", port, "s10", &url, None))
        .await?;
    mgr.start_pipeline(spec("large", port, "s1k", &url, None))
        .await?;
    mgr.start_pipeline(spec("capped", port, "s1k", &url, Some(CAP)))
        .await?;
    for name in ["small", "large", "capped"] {
        until_running(&mgr, name).await;
    }
    // A pipeline reports running before its slot exists; changes committed
    // before then are not streamed.
    let deadline = Instant::now() + Duration::from_secs(300);
    loop {
        let active: i64 = db
            .query_one(
                "SELECT count(*) FROM pg_replication_slots WHERE active",
                &[],
            )
            .await?
            .get(0);
        if active == 3 {
            break;
        }
        assert!(Instant::now() < deadline, "slots never became active");
        sleep(Duration::from_millis(200)).await;
    }
    touch(&db, "s10", SMALL, 1).await;
    touch(&db, "s1k", LARGE, 1).await;
    until("every event counted", || {
        events_seen("small") >= SMALL as f64
            && events_seen("large") >= LARGE as f64
            && events_seen("capped") >= LARGE as f64
    })
    .await;
    // Let every batch settle into its per-batch metrics.
    sleep(Duration::from_secs(2)).await;

    // Default: identical series sets, no table labels.
    let (small, large) = (shapes("small"), shapes("large"));
    for inventoried in [
        "deltaforge_source_events_total",
        "deltaforge_snapshot_rows_total",
    ] {
        assert!(
            small.iter().any(|(n, _)| n == inventoried),
            "{inventoried} was recorded"
        );
    }
    for (name, labels) in small.iter().chain(&large) {
        assert!(
            labels
                .iter()
                .all(|(k, _)| k != "table" && k != "table_scope"),
            "{name} carries a table label by default: {labels:?}"
        );
    }
    // Identical, except where a batch's flush cause differs with timing
    // (a snapshot's rows still pending at its end barrier, or already
    // flushed by the timer): `trigger` is a closed set, never a table name.
    let only_small: Vec<_> = small.difference(&large).collect();
    let only_large: Vec<_> = large.difference(&small).collect();
    for (name, labels) in only_small.iter().chain(&only_large) {
        let trigger = labels.iter().find(|(k, _)| k == "trigger");
        assert!(
            name == "deltaforge_stage_latency_seconds"
                && trigger.is_some_and(|(_, v)| TRIGGERS.contains(&v.as_str())),
            "series sets differ beyond flush timing: only at 10 tables \
             {only_small:#?}, only at 1,000 tables {only_large:#?}"
        );
    }
    let without_trigger = |set: &BTreeSet<Shape>| {
        set.iter()
            .map(|(n, l)| {
                let l: Vec<_> =
                    l.iter().filter(|(k, _)| k != "trigger").cloned().collect();
                (n.clone(), l)
            })
            .collect::<BTreeSet<_>>()
    };
    assert_eq!(without_trigger(&small), without_trigger(&large));
    assert!(lag_series("small").is_empty() && lag_series("large").is_empty());

    // Opt-in: bounded per table-labelled metric and label set.
    let capped: Vec<_> = series()
        .into_iter()
        .filter(|s| label(s, "pipeline") == Some("capped"))
        .collect();
    let mut tables_per_set: HashMap<Shape, BTreeSet<(String, String)>> =
        HashMap::new();
    for s in capped.iter().filter(|s| label(s, "table_scope").is_some()) {
        let rest: Vec<_> = s
            .labels
            .iter()
            .filter(|(k, _)| k != "table" && k != "table_scope")
            .cloned()
            .collect();
        tables_per_set
            .entry((s.name.clone(), rest))
            .or_default()
            .insert((
                label(s, "table").unwrap().to_string(),
                label(s, "table_scope").unwrap().to_string(),
            ));
    }
    assert!(
        tables_per_set
            .keys()
            .any(|(n, _)| n == "deltaforge_source_events_total"),
        "per-table detail was recorded"
    );
    for ((name, rest), tables) in &tables_per_set {
        assert!(tables.len() <= CAP + 1, "{name} {rest:?}: {tables:?}");
        let exact = tables.iter().filter(|(_, s)| s == "exact").count();
        assert!(exact <= CAP, "{name} {rest:?}: {tables:?}");
    }
    let gauge = |name: &str| {
        capped
            .iter()
            .find(|s| s.name == name)
            .map(|s| s.value)
            .unwrap_or_else(|| panic!("{name} is reported"))
    };
    assert_eq!(gauge(table_metrics::METRIC_ADMITTED), CAP as f64);
    assert!(gauge(table_metrics::METRIC_OVERFLOW) > 0.0);
    let overflowed = gauge(table_metrics::METRIC_OVERFLOWED);
    let expected = (LARGE - CAP) as f64;
    assert!(
        (overflowed - expected).abs() / expected <= 0.10,
        "overflowed tables estimate {overflowed}, expected about {expected}"
    );

    // Per-table lag: bounded, expires when idle, returns with events.
    let lag = lag_series("capped");
    assert!(!lag.is_empty() && lag.len() <= CAP + 1, "{lag:#?}");
    sleep(Duration::from_secs(LAG_IDLE_SECS + 2)).await;
    assert!(lag_series("capped").is_empty(), "idle lag series expired");
    touch(&db, "s1k", LARGE, 2).await;
    until("lag after new events", || !lag_series("capped").is_empty()).await;
    assert!(lag_series("capped").len() <= CAP + 1);
    assert_eq!(
        table_metrics::for_pipeline("capped").admitted(),
        CAP,
        "idle expiry freed no admission"
    );

    // Deliveries drain before the deletes (a delete cancels any in flight).
    until("every event delivered", || {
        delivered("small") >= SMALL as f64
            && delivered("large") >= 2.0 * LARGE as f64
            && delivered("capped") >= 2.0 * LARGE as f64
    })
    .await;
    for name in ["small", "large", "capped"] {
        let info = PipelineController::get(&mgr, name).await?;
        assert_eq!(info.status, "running", "{name} kept running");
    }

    // Deleting the pipeline removes its policy and every lag series.
    PipelineController::delete(&mgr, "capped").await?;
    assert_eq!(o11y::table_lag::global().len("capped"), 0);
    assert!(lag_series("capped").is_empty());
    assert_eq!(table_metrics::for_pipeline("capped").policy(), None);
    assert!(
        table_metrics::is_registered("capped"),
        "its admissions outlive the pipeline (its series cannot be removed)"
    );
    assert_eq!(table_metrics::for_pipeline("capped").admitted(), CAP);

    for name in ["small", "large"] {
        PipelineController::delete(&mgr, name).await?;
    }
    Ok(())
}

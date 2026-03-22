use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use anyhow::Result;
use axum::{
    Json, Router,
    extract::{Query, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::Mutex;

use crate::toxiproxy::ToxiproxyClient;

const HTML: &str = include_str!("ui.html");

// ── Shared state ─────────────────────────────────────────────────────────────

pub struct UiState {
    child: Mutex<Option<tokio::process::Child>>,
    log: Mutex<VecDeque<String>>,
    running: AtomicBool,
    toxi: ToxiproxyClient,
}

impl UiState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            child: Mutex::new(None),
            log: Mutex::new(VecDeque::new()),
            running: AtomicBool::new(false),
            toxi: ToxiproxyClient::new(),
        })
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Walk up from the running binary to find the workspace root
/// (target/debug/chaos → target/debug → target → workspace/).
fn workspace_root() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|p| {
            p.parent()
                .and_then(|p| p.parent())
                .and_then(|p| p.parent())
                .map(|p| p.to_path_buf())
        })
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_default())
}

async fn inspect_container(name: &str) -> (String, String) {
    let out = Command::new("docker")
        .args([
            "inspect",
            "--format",
            "{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{else}}-{{end}}",
            name,
        ])
        .output()
        .await;

    match out {
        Ok(o) if o.status.success() => {
            let s = String::from_utf8_lossy(&o.stdout).trim().to_string();
            let mut it = s.splitn(2, ' ');
            let state = it.next().unwrap_or("absent").to_string();
            let health = it.next().unwrap_or("-").to_string();
            (state, health)
        }
        _ => ("absent".to_string(), "-".to_string()),
    }
}

async fn check_http(url: &str) -> Option<bool> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(1))
        .build()
        .ok()?;
    Some(client.get(url).send().await.ok()?.status().is_success())
}

// ── Handlers ──────────────────────────────────────────────────────────────────

async fn serve_html() -> impl IntoResponse {
    Response::builder()
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .body(HTML.to_string())
        .unwrap()
}

#[derive(Serialize)]
struct ServiceInfo {
    label: String,
    state: String,
    health: String,
    http_ok: Option<bool>,
}

async fn api_status() -> Json<Vec<ServiceInfo>> {
    let defs: &[(&str, &str, Option<&str>)] = &[
        ("mysql",           "deltaforge-mysql-1",             None),
        ("mysql-b",         "deltaforge-mysql-b-1",           None),
        ("kafka",           "deltaforge-kafka-1",             None),
        ("zookeeper",       "deltaforge-zookeeper-1",         None),
        ("toxiproxy",       "deltaforge-toxiproxy-1",         Some("http://localhost:8474/proxies")),
        ("prometheus",      "deltaforge-prometheus-1",        Some("http://localhost:9090/-/ready")),
        ("grafana",         "deltaforge-grafana-1",           Some("http://localhost:3000/api/health")),
        ("cadvisor",        "deltaforge-cadvisor-1",          Some("http://localhost:8888/healthz")),
        ("deltaforge",      "deltaforge-deltaforge-1",        Some("http://localhost:8080/health")),
        ("deltaforge-pg",   "deltaforge-deltaforge-pg-1",     Some("http://localhost:8080/health")),
        ("deltaforge-soak", "deltaforge-deltaforge-soak-1",   Some("http://localhost:8081/health")),
        ("deltaforge-tpcc", "deltaforge-deltaforge-tpcc-1",   Some("http://localhost:8082/health")),
        ("postgres",        "deltaforge-postgres-1",          None),
        ("postgres-b",      "deltaforge-postgres-b-1",        None),
    ];

    let mut out = Vec::new();
    for (label, container, url) in defs {
        let (state, health) = inspect_container(container).await;
        let http_ok = if state == "running" {
            if let Some(u) = url { check_http(u).await } else { None }
        } else {
            None
        };
        out.push(ServiceInfo { label: label.to_string(), state, health, http_ok });
    }
    Json(out)
}

async fn api_proxies() -> impl IntoResponse {
    match reqwest::Client::new()
        .get("http://localhost:8474/proxies")
        .send()
        .await
    {
        Ok(r) => match r.text().await {
            Ok(b) => (StatusCode::OK, b),
            Err(_) => (StatusCode::BAD_GATEWAY, "{}".to_string()),
        },
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "{}".to_string()),
    }
}

#[derive(Deserialize)]
struct FaultRequest {
    preset: String,
}

async fn api_inject_fault(
    State(st): State<Arc<UiState>>,
    Json(req): Json<FaultRequest>,
) -> StatusCode {
    let r: anyhow::Result<()> = match req.preset.as_str() {
        "mysql-partition" => st.toxi.disable("mysql").await,
        "kafka-outage"    => st.toxi.disable("kafka").await,
        "pg-partition"    => st.toxi.disable("postgres").await,
        "mysql-latency"   => st.toxi.add_latency("mysql", 2000, 500).await,
        "kafka-latency"   => st.toxi.add_latency("kafka", 1000, 200).await,
        "mysql-bandwidth" => {
            st.toxi
                .add_toxic("mysql", "ui-bw", "bandwidth", serde_json::json!({"rate": 100}))
                .await
        }
        _ => Err(anyhow::anyhow!("unknown preset")),
    };
    if r.is_ok() { StatusCode::OK } else { StatusCode::BAD_GATEWAY }
}

async fn api_clear_faults(State(st): State<Arc<UiState>>) -> StatusCode {
    if st.toxi.reset_all().await.is_ok() { StatusCode::OK } else { StatusCode::BAD_GATEWAY }
}

#[derive(Deserialize)]
struct RunRequest {
    scenario: String,
    source: String,
    duration_mins: u64,
    #[serde(default)]
    writer_tasks: usize,
    #[serde(default)]
    write_delay_ms: u64,
}

#[derive(Serialize)]
struct ScenarioStatus {
    running: bool,
    lines: Vec<String>,
}

async fn api_scenario_start(
    State(st): State<Arc<UiState>>,
    Json(req): Json<RunRequest>,
) -> StatusCode {
    if st.running.load(Ordering::Relaxed) {
        return StatusCode::CONFLICT;
    }

    let binary = match std::env::current_exe() {
        Ok(p) => p,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };

    let mut cmd = Command::new(&binary);
    cmd.arg("--scenario").arg(&req.scenario);
    cmd.arg("--source").arg(&req.source);
    if req.duration_mins > 0 {
        cmd.arg("--duration-mins").arg(req.duration_mins.to_string());
    }
    if req.writer_tasks > 0 {
        cmd.arg("--writer-tasks").arg(req.writer_tasks.to_string());
    }
    if req.write_delay_ms > 0 {
        cmd.arg("--write-delay-ms").arg(req.write_delay_ms.to_string());
    }
    cmd.current_dir(workspace_root());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };

    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();

    // Clear previous log.
    st.log.lock().await.clear();
    st.running.store(true, Ordering::Relaxed);
    *st.child.lock().await = Some(child);

    // Stream stdout into log buffer.
    let st2 = Arc::clone(&st);
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let mut log = st2.log.lock().await;
            log.push_back(line);
            if log.len() > 500 { log.pop_front(); }
        }
    });

    // Stream stderr into log buffer.
    let st3 = Arc::clone(&st);
    tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let mut log = st3.log.lock().await;
            log.push_back(line);
            if log.len() > 500 { log.pop_front(); }
        }
    });

    // Monitor for exit.
    let st4 = Arc::clone(&st);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let mut lock = st4.child.lock().await;
            match lock.as_mut().map(|c| c.try_wait()) {
                Some(Ok(Some(_))) | None => {
                    *lock = None;
                    drop(lock);
                    st4.running.store(false, Ordering::Relaxed);
                    st4.log.lock().await.push_back("▶ scenario finished".to_string());
                    break;
                }
                Some(Err(_)) => {
                    *lock = None;
                    st4.running.store(false, Ordering::Relaxed);
                    break;
                }
                Some(Ok(None)) => {} // still running
            }
        }
    });

    StatusCode::OK
}

async fn api_scenario_stop(State(st): State<Arc<UiState>>) -> StatusCode {
    let mut lock = st.child.lock().await;
    if let Some(c) = lock.as_mut() {
        let _ = c.kill().await;
    }
    *lock = None;
    st.running.store(false, Ordering::Relaxed);
    st.log.lock().await.push_back("▶ stopped by user".to_string());
    StatusCode::OK
}

async fn api_scenario_status(State(st): State<Arc<UiState>>) -> Json<ScenarioStatus> {
    Json(ScenarioStatus {
        running: st.running.load(Ordering::Relaxed),
        lines: st.log.lock().await.iter().cloned().collect(),
    })
}

#[derive(Deserialize)]
struct InfraRequest {
    action: String,  // "up" or "stop"
    profile: String, // "", "app", "pg-app", "soak", "tpcc"
}

async fn api_infra(Json(req): Json<InfraRequest>) -> StatusCode {
    let root = workspace_root();
    let mut args = vec![
        "compose".to_string(),
        "-f".to_string(),
        "docker-compose.chaos.yml".to_string(),
    ];
    if !req.profile.is_empty() {
        args.push("--profile".to_string());
        args.push(req.profile.clone());
    }
    args.push(req.action.clone());
    if req.action == "up" {
        args.push("-d".to_string());
    }

    let ok = Command::new("docker")
        .args(&args)
        .current_dir(&root)
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false);

    if ok { StatusCode::OK } else { StatusCode::INTERNAL_SERVER_ERROR }
}

// ── DeltaForge API proxy ──────────────────────────────────────────────────────

#[derive(Deserialize)]
struct DfGetParams {
    port: u16,
    path: String,
}

/// Proxy a GET to any DeltaForge REST endpoint, e.g.
/// GET /api/df?port=8081&path=/pipelines/chaos-soak/sensing/stats
async fn api_df_get(Query(p): Query<DfGetParams>) -> impl IntoResponse {
    let url = format!("http://localhost:{}{}", p.port, p.path);
    match reqwest::Client::new().get(&url).send().await {
        Ok(r) => {
            let status = StatusCode::from_u16(r.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let body = r.text().await.unwrap_or_default();
            (status, [(header::CONTENT_TYPE, "application/json")], body)
        }
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, [(header::CONTENT_TYPE, "application/json")], "{}".to_string()),
    }
}

#[derive(Deserialize)]
struct DfPostReq {
    port: u16,
    path: String,
    #[serde(default)]
    body: Option<serde_json::Value>,
}

/// Proxy a POST to any DeltaForge REST endpoint, e.g.
/// POST /api/df { port: 8081, path: "/pipelines/chaos-soak/pause" }
async fn api_df_post(Json(req): Json<DfPostReq>) -> impl IntoResponse {
    let url = format!("http://localhost:{}{}", req.port, req.path);
    let client = reqwest::Client::new();
    let r = if let Some(b) = req.body {
        client.post(&url).json(&b).send().await
    } else {
        client.post(&url).send().await
    };
    match r {
        Ok(r) => {
            let status = StatusCode::from_u16(r.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let body = r.text().await.unwrap_or_default();
            (status, [(header::CONTENT_TYPE, "application/json")], body)
        }
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, [(header::CONTENT_TYPE, "application/json")], "{}".to_string()),
    }
}

/// Proxy a DELETE to any DeltaForge REST endpoint, e.g.
/// POST /api/df/delete { port: 8080, path: "/pipelines/my-pipeline" }
async fn api_df_delete(Json(req): Json<DfPostReq>) -> impl IntoResponse {
    let url = format!("http://localhost:{}{}", req.port, req.path);
    match reqwest::Client::new().delete(&url).send().await {
        Ok(r) => {
            let status = StatusCode::from_u16(r.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let body = r.text().await.unwrap_or_default();
            (status, [(header::CONTENT_TYPE, "application/json")], body)
        }
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, [(header::CONTENT_TYPE, "application/json")], "{}".to_string()),
    }
}

// ── Entry point ───────────────────────────────────────────────────────────────

pub async fn run(port: u16) -> Result<()> {
    let state = UiState::new();

    let app = Router::new()
        .route("/", get(serve_html))
        .route("/api/status", get(api_status))
        .route("/api/proxies", get(api_proxies))
        .route("/api/fault", post(api_inject_fault))
        .route("/api/fault/clear", post(api_clear_faults))
        .route("/api/scenario/start", post(api_scenario_start))
        .route("/api/scenario/stop", post(api_scenario_stop))
        .route("/api/scenario/status", get(api_scenario_status))
        .route("/api/infra", post(api_infra))
        .route("/api/df", get(api_df_get))
        .route("/api/df", post(api_df_post))
        .route("/api/df/delete", post(api_df_delete))
        .with_state(state);

    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("DeltaForge Playground → http://localhost:{port}");
    println!("DeltaForge Playground → http://localhost:{port}");
    axum::serve(listener, app).await?;
    Ok(())
}

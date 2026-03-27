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
    /// Last generated flamegraph SVG (populated after profiling completes).
    flamegraph: Mutex<Option<Vec<u8>>>,
    /// Profiling state: "idle", "recording", "generating".
    profile_status: Mutex<String>,
}

impl UiState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            child: Mutex::new(None),
            log: Mutex::new(VecDeque::new()),
            running: AtomicBool::new(false),
            toxi: ToxiproxyClient::new(),
            flamegraph: Mutex::new(None),
            profile_status: Mutex::new("idle".to_string()),
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

/// Container inspection result.
struct ContainerInfo {
    state: String,
    health: String,
    image: String,
    /// Published ports: list of (host_port, container_port, protocol).
    ports: Vec<(u16, u16, String)>,
}

async fn inspect_container(name: &str) -> ContainerInfo {
    let absent = ContainerInfo {
        state: "absent".into(),
        health: "-".into(),
        image: String::new(),
        ports: vec![],
    };

    // Get state, health, image in one call.
    let out = Command::new("docker")
        .args([
            "inspect",
            "--format",
            "{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{else}}-{{end}} {{.Config.Image}}",
            name,
        ])
        .output()
        .await;

    let (state, health, image) = match out {
        Ok(o) if o.status.success() => {
            let s = String::from_utf8_lossy(&o.stdout).trim().to_string();
            let mut it = s.splitn(3, ' ');
            let state = it.next().unwrap_or("absent").to_string();
            let health = it.next().unwrap_or("-").to_string();
            let image = it.next().unwrap_or("").to_string();
            (state, health, image)
        }
        _ => return absent,
    };

    // Get published port mappings via `docker port`.
    let ports_out = Command::new("docker").args(["port", name]).output().await;

    let mut ports = vec![];
    if let Ok(o) = ports_out {
        if o.status.success() {
            // Output format: "8080/tcp -> 0.0.0.0:8080\n9000/tcp -> 0.0.0.0:9000\n"
            let s = String::from_utf8_lossy(&o.stdout);
            for line in s.lines() {
                // "3306/tcp -> 0.0.0.0:3306"
                if let Some((left, right)) = line.split_once(" -> ") {
                    let (cport_str, proto) =
                        left.split_once('/').unwrap_or((left, "tcp"));
                    let cport: u16 = cport_str.parse().unwrap_or(0);
                    // right is "0.0.0.0:3306" or "[::]:3306"
                    let hport: u16 = right
                        .rsplit(':')
                        .next()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    if cport > 0 && hport > 0 {
                        ports.push((hport, cport, proto.to_string()));
                    }
                }
            }
        }
    }
    // Deduplicate (docker port can show both IPv4 and IPv6).
    ports.sort();
    ports.dedup();

    ContainerInfo {
        state,
        health,
        image,
        ports,
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
    image: String,
    /// Clickable links derived from published ports.
    links: Vec<ServiceLink>,
}

#[derive(Serialize, Clone)]
struct ServiceLink {
    /// Short label (e.g. "API", "metrics", "UI", or port number).
    label: String,
    /// Full URL (e.g. "http://localhost:8080").
    url: String,
}

/// Well-known container ports → human-readable link labels.
/// If not listed here, falls back to showing the port number.
fn port_label(host_port: u16) -> Option<(&'static str, String)> {
    match host_port {
        8080..=8083 => Some(("API", format!("http://localhost:{host_port}"))),
        9000..=9003 => {
            Some(("metrics", format!("http://localhost:{host_port}/metrics")))
        }
        3000 => Some(("UI", "http://localhost:3000".to_string())),
        9090 => Some(("UI", "http://localhost:9090".to_string())),
        8888 => Some(("UI", "http://localhost:8888".to_string())),
        8474 => Some(("API", "http://localhost:8474".to_string())),
        3306 | 3307 => Some(("mysql", format!("localhost:{host_port}"))),
        5432 | 5433 => Some(("psql", format!("localhost:{host_port}"))),
        9092 => Some(("broker", "localhost:9092".to_string())),
        2181 => Some(("zk", "localhost:2181".to_string())),
        _ => None,
    }
}

fn links_from_ports(ports: &[(u16, u16, String)]) -> Vec<ServiceLink> {
    let mut links = Vec::new();
    for &(host_port, _container_port, _) in ports {
        // Only include ports with known labels — skip internal/proxy ports.
        if let Some((lbl, url)) = port_label(host_port) {
            links.push(ServiceLink {
                label: lbl.to_string(),
                url,
            });
        }
    }
    links
}

async fn api_status() -> Json<Vec<ServiceInfo>> {
    let defs: &[(&str, &str, Option<&str>)] = &[
        ("mysql", "deltaforge-mysql-1", None),
        ("mysql-b", "deltaforge-mysql-b-1", None),
        ("kafka", "deltaforge-kafka-1", None),
        ("zookeeper", "deltaforge-zookeeper-1", None),
        (
            "toxiproxy",
            "deltaforge-toxiproxy-1",
            Some("http://localhost:8474/proxies"),
        ),
        (
            "prometheus",
            "deltaforge-prometheus-1",
            Some("http://localhost:9090/-/ready"),
        ),
        (
            "grafana",
            "deltaforge-grafana-1",
            Some("http://localhost:3000/api/health"),
        ),
        (
            "cadvisor",
            "deltaforge-cadvisor-1",
            Some("http://localhost:8888/healthz"),
        ),
        (
            "deltaforge",
            "deltaforge-deltaforge-1",
            Some("http://localhost:8080/health"),
        ),
        (
            "deltaforge-pg",
            "deltaforge-deltaforge-pg-1",
            Some("http://localhost:8080/health"),
        ),
        (
            "deltaforge-soak",
            "deltaforge-deltaforge-soak-1",
            Some("http://localhost:8081/health"),
        ),
        (
            "deltaforge-tpcc",
            "deltaforge-deltaforge-tpcc-1",
            Some("http://localhost:8082/health"),
        ),
        (
            "deltaforge-pg-soak",
            "deltaforge-deltaforge-pg-soak-1",
            Some("http://localhost:8083/health"),
        ),
        ("postgres", "deltaforge-postgres-1", None),
        ("postgres-b", "deltaforge-postgres-b-1", None),
    ];

    let mut out = Vec::new();
    for (label, container, url) in defs {
        let info = inspect_container(container).await;
        let http_ok = if info.state == "running" {
            if let Some(u) = url {
                check_http(u).await
            } else {
                None
            }
        } else {
            None
        };
        let links = links_from_ports(&info.ports);
        out.push(ServiceInfo {
            label: label.to_string(),
            state: info.state,
            health: info.health,
            http_ok,
            image: info.image,
            links,
        });
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
        "kafka-outage" => st.toxi.disable("kafka").await,
        "pg-partition" => st.toxi.disable("postgres").await,
        "mysql-latency" => st.toxi.add_latency("mysql", 2000, 500).await,
        "kafka-latency" => st.toxi.add_latency("kafka", 1000, 200).await,
        "mysql-bandwidth" => {
            st.toxi
                .add_toxic(
                    "mysql",
                    "ui-bw",
                    "bandwidth",
                    serde_json::json!({"rate": 100}),
                )
                .await
        }
        _ => Err(anyhow::anyhow!("unknown preset")),
    };
    if r.is_ok() {
        StatusCode::OK
    } else {
        StatusCode::BAD_GATEWAY
    }
}

async fn api_clear_faults(State(st): State<Arc<UiState>>) -> StatusCode {
    if st.toxi.reset_all().await.is_ok() {
        StatusCode::OK
    } else {
        StatusCode::BAD_GATEWAY
    }
}

#[derive(Deserialize)]
struct ResetRequest {
    scope: String, // "checkpoints" or "all"
}

async fn api_reset_volumes(Json(req): Json<ResetRequest>) -> StatusCode {
    let root = workspace_root();
    match req.scope.as_str() {
        "checkpoints" => {
            // Remove DeltaForge SQLite checkpoint DBs from each app volume.
            // Stop DeltaForge services first, then clear, then leave stopped.
            let profiles = &[
                ("app", "deltaforge"),
                ("pg-app", "deltaforge-pg"),
                ("soak", "deltaforge-soak"),
                ("pg-soak", "deltaforge-pg-soak"),
                ("tpcc", "deltaforge-tpcc"),
            ];
            for (profile, service) in profiles {
                // Best-effort stop — service may already be stopped.
                let _ = Command::new("docker")
                    .args([
                        "compose",
                        "-f",
                        "docker-compose.chaos.yml",
                        "--profile",
                        profile,
                        "stop",
                        service,
                    ])
                    .current_dir(&root)
                    .status()
                    .await;

                // Clear checkpoint files from the data volume.
                let _ = Command::new("docker")
                    .args([
                        "compose",
                        "-f",
                        "docker-compose.chaos.yml",
                        "--profile",
                        profile,
                        "run",
                        "--rm",
                        "--no-deps",
                        "--entrypoint",
                        "sh",
                        service,
                        "-c",
                        "rm -f /data/*.db /data/*.db-shm /data/*.db-wal",
                    ])
                    .current_dir(&root)
                    .status()
                    .await;
            }
            StatusCode::OK
        }
        "all" => {
            // Full teardown: stop everything and remove all named volumes.
            let ok = Command::new("docker")
                .args([
                    "compose",
                    "-f",
                    "docker-compose.chaos.yml",
                    "--profile",
                    "app",
                    "--profile",
                    "pg-app",
                    "--profile",
                    "soak",
                    "--profile",
                    "tpcc",
                    "down",
                    "-v",
                ])
                .current_dir(&root)
                .status()
                .await
                .map(|s| s.success())
                .unwrap_or(false);
            if ok {
                StatusCode::OK
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
        _ => StatusCode::BAD_REQUEST,
    }
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
    // Backlog-drain throughput settings
    #[serde(default)]
    drain_max_events: Option<u64>,
    #[serde(default)]
    drain_max_ms: Option<u64>,
    #[serde(default)]
    drain_commit_mode: Option<String>,
    #[serde(default)]
    drain_commit_interval_ms: Option<u64>,
    #[serde(default)]
    drain_schema_sensing: Option<bool>,
    /// rdkafka producer overrides as key=value strings.
    #[serde(default)]
    drain_kafka_conf: Vec<String>,
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
        cmd.arg("--duration-mins")
            .arg(req.duration_mins.to_string());
    }
    if req.writer_tasks > 0 {
        cmd.arg("--writer-tasks").arg(req.writer_tasks.to_string());
    }
    if req.write_delay_ms > 0 {
        cmd.arg("--write-delay-ms")
            .arg(req.write_delay_ms.to_string());
    }
    if let Some(v) = req.drain_max_events {
        cmd.arg("--drain-max-events").arg(v.to_string());
    }
    if let Some(v) = req.drain_max_ms {
        cmd.arg("--drain-max-ms").arg(v.to_string());
    }
    if let Some(v) = &req.drain_commit_mode {
        cmd.arg("--drain-commit-mode").arg(v);
    }
    if let Some(v) = req.drain_commit_interval_ms {
        cmd.arg("--drain-commit-interval-ms").arg(v.to_string());
    }
    if req.drain_schema_sensing == Some(true) {
        cmd.arg("--drain-schema-sensing");
    }
    for kv in &req.drain_kafka_conf {
        cmd.arg("--drain-kafka-conf").arg(kv);
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
            if log.len() > 500 {
                log.pop_front();
            }
        }
    });

    // Stream stderr into log buffer.
    let st3 = Arc::clone(&st);
    tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let mut log = st3.log.lock().await;
            log.push_back(line);
            if log.len() > 500 {
                log.pop_front();
            }
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
                    st4.log
                        .lock()
                        .await
                        .push_back("▶ scenario finished".to_string());
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
    st.log
        .lock()
        .await
        .push_back("▶ stopped by user".to_string());
    StatusCode::OK
}

async fn api_scenario_status(
    State(st): State<Arc<UiState>>,
) -> Json<ScenarioStatus> {
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

    if ok {
        StatusCode::OK
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

#[derive(Deserialize)]
struct RefreshServiceRequest {
    /// Compose service name (e.g. "deltaforge-pg-soak").
    service: String,
    /// Compose profile (e.g. "pg-soak").
    profile: String,
}

/// Recreate a single service with the latest image, without affecting
/// dependencies. Runs: docker compose up -d --no-deps --force-recreate <service>
async fn api_refresh_service(
    Json(req): Json<RefreshServiceRequest>,
) -> StatusCode {
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
    args.extend([
        "up".to_string(),
        "-d".to_string(),
        "--no-deps".to_string(),
        "--force-recreate".to_string(),
        req.service.clone(),
    ]);

    tracing::info!(service = %req.service, profile = %req.profile, "refreshing service");
    let ok = Command::new("docker")
        .args(&args)
        .current_dir(&root)
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false);

    if ok {
        StatusCode::OK
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

#[derive(Deserialize)]
struct ImageSwapRequest {
    /// Compose service name (e.g. "deltaforge-soak").
    service: String,
    /// Compose profile (e.g. "soak").
    profile: String,
    /// Target image (e.g. "deltaforge:dev-profile").
    image: String,
}

/// Swap the image for a DeltaForge service by stopping it, updating the
/// compose file, and recreating. Uses awk to patch only the target service.
async fn api_swap_image(Json(req): Json<ImageSwapRequest>) -> StatusCode {
    let root = workspace_root();
    let compose_file = root.join("docker-compose.chaos.yml");

    // Stop the service first.
    let _ = Command::new("docker")
        .args([
            "compose",
            "-f",
            "docker-compose.chaos.yml",
            "--profile",
            &req.profile,
            "stop",
            &req.service,
        ])
        .current_dir(&root)
        .status()
        .await;

    // Patch the image line for the target service using awk.
    let awk_prog = format!(
        "/^  {}:/ {{ in_svc=1 }} in_svc && /image:/ {{ sub(/image:.*/, \"image: {}\"); in_svc=0 }} {{ print }}",
        req.service, req.image
    );
    let tmp_file = compose_file.with_extension("yml.tmp");

    let awk_ok = Command::new("awk")
        .arg(&awk_prog)
        .arg(&compose_file)
        .stdout(std::process::Stdio::from(
            std::fs::File::create(&tmp_file).unwrap_or_else(|_| {
                std::fs::File::create("/dev/null").unwrap()
            }),
        ))
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false);

    if awk_ok {
        let _ = std::fs::rename(&tmp_file, &compose_file);
    } else {
        let _ = std::fs::remove_file(&tmp_file);
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    // Recreate the service with the new image.
    let ok = Command::new("docker")
        .args([
            "compose",
            "-f",
            "docker-compose.chaos.yml",
            "--profile",
            &req.profile,
            "up",
            "-d",
            "--force-recreate",
            &req.service,
        ])
        .current_dir(&root)
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false);

    if ok {
        StatusCode::OK
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
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
            let status = StatusCode::from_u16(r.status().as_u16())
                .unwrap_or(StatusCode::BAD_GATEWAY);
            let body = r.text().await.unwrap_or_default();
            (status, [(header::CONTENT_TYPE, "application/json")], body)
        }
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::CONTENT_TYPE, "application/json")],
            "{}".to_string(),
        ),
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
            let status = StatusCode::from_u16(r.status().as_u16())
                .unwrap_or(StatusCode::BAD_GATEWAY);
            let body = r.text().await.unwrap_or_default();
            (status, [(header::CONTENT_TYPE, "application/json")], body)
        }
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::CONTENT_TYPE, "application/json")],
            "{}".to_string(),
        ),
    }
}

/// Proxy a PATCH to any DeltaForge REST endpoint, e.g.
/// POST /api/df/patch { port: 8081, path: "/pipelines/chaos-soak", body: {...} }
async fn api_df_patch(Json(req): Json<DfPostReq>) -> impl IntoResponse {
    let url = format!("http://localhost:{}{}", req.port, req.path);
    let client = reqwest::Client::new();
    let r = if let Some(b) = req.body {
        client.patch(&url).json(&b).send().await
    } else {
        client.patch(&url).send().await
    };
    match r {
        Ok(r) => {
            let status = StatusCode::from_u16(r.status().as_u16())
                .unwrap_or(StatusCode::BAD_GATEWAY);
            let body = r.text().await.unwrap_or_default();
            (status, [(header::CONTENT_TYPE, "application/json")], body)
        }
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::CONTENT_TYPE, "application/json")],
            "{}".to_string(),
        ),
    }
}

/// Proxy a DELETE to any DeltaForge REST endpoint, e.g.
/// POST /api/df/delete { port: 8080, path: "/pipelines/my-pipeline" }
async fn api_df_delete(Json(req): Json<DfPostReq>) -> impl IntoResponse {
    let url = format!("http://localhost:{}{}", req.port, req.path);
    match reqwest::Client::new().delete(&url).send().await {
        Ok(r) => {
            let status = StatusCode::from_u16(r.status().as_u16())
                .unwrap_or(StatusCode::BAD_GATEWAY);
            let body = r.text().await.unwrap_or_default();
            (status, [(header::CONTENT_TYPE, "application/json")], body)
        }
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::CONTENT_TYPE, "application/json")],
            "{}".to_string(),
        ),
    }
}

// ── Profiling ─────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct ProfileRequest {
    /// Docker container name (e.g. "deltaforge-deltaforge-soak-1").
    container: String,
    /// Recording duration in seconds (default 30).
    #[serde(default = "default_profile_secs")]
    duration_secs: u64,
    /// Sampling frequency in Hz (default 99).
    #[serde(default = "default_profile_freq")]
    frequency: u32,
}

fn default_profile_secs() -> u64 {
    30
}
fn default_profile_freq() -> u32 {
    99
}

#[derive(Serialize)]
struct ProfileStatus {
    status: String, // "idle", "recording", "generating", "ready", "error"
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

async fn api_profile_start(
    State(st): State<Arc<UiState>>,
    Json(req): Json<ProfileRequest>,
) -> StatusCode {
    {
        let status = st.profile_status.lock().await;
        if *status == "recording" || *status == "generating" {
            return StatusCode::CONFLICT;
        }
    }

    *st.profile_status.lock().await = "recording".to_string();
    *st.flamegraph.lock().await = None;

    let st2 = Arc::clone(&st);
    tokio::spawn(async move {
        match run_profile(&req.container, req.duration_secs, req.frequency)
            .await
        {
            Ok(svg) => {
                *st2.flamegraph.lock().await = Some(svg);
                *st2.profile_status.lock().await = "ready".to_string();
            }
            Err(e) => {
                tracing::error!(error = %e, "profiling failed");
                *st2.profile_status.lock().await = format!("error: {e}");
            }
        }
    });

    StatusCode::OK
}

async fn api_profile_status(
    State(st): State<Arc<UiState>>,
) -> Json<ProfileStatus> {
    let status = st.profile_status.lock().await.clone();
    let (status_str, error) = if status.starts_with("error:") {
        (
            "error".to_string(),
            Some(
                status
                    .strip_prefix("error: ")
                    .unwrap_or(&status)
                    .to_string(),
            ),
        )
    } else {
        (status, None)
    };
    Json(ProfileStatus {
        status: status_str,
        error,
    })
}

async fn api_profile_flamegraph(
    State(st): State<Arc<UiState>>,
) -> impl IntoResponse {
    let svg = st.flamegraph.lock().await;
    match svg.as_ref() {
        Some(data) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "image/svg+xml"),
                (
                    header::CONTENT_DISPOSITION,
                    "inline; filename=\"flamegraph.svg\"",
                ),
            ],
            data.clone(),
        ),
        None => (
            StatusCode::NOT_FOUND,
            [
                (header::CONTENT_TYPE, "text/plain"),
                (header::CONTENT_DISPOSITION, ""),
            ],
            b"no flamegraph available".to_vec(),
        ),
    }
}

/// Record a CPU profile inside the container and generate a flamegraph SVG.
///
/// 1. `docker exec` to run `perf record` inside the container (symbols resolve correctly).
/// 2. `docker exec` to run `perf script` and capture the output.
/// 3. Collapse and render the flamegraph in-process via the `inferno` crate.
async fn run_profile(
    container: &str,
    duration_secs: u64,
    frequency: u32,
) -> anyhow::Result<Vec<u8>> {
    // Step 1: Record.
    tracing::info!(container, duration_secs, frequency, "starting perf record");
    let record_status = Command::new("docker")
        .args([
            "exec",
            container,
            "perf",
            "record",
            "-F",
            &frequency.to_string(),
            "-p",
            "1",
            "-g",
            "--call-graph",
            "dwarf",
            "-o",
            "/tmp/perf.data",
            "--",
            "sleep",
            &duration_secs.to_string(),
        ])
        .status()
        .await?;

    if !record_status.success() {
        anyhow::bail!(
            "perf record failed (exit {}). Is the container running the profiling image?",
            record_status.code().unwrap_or(-1)
        );
    }

    // Step 2: Script — run inside container so symbols resolve.
    // --no-inline avoids addr2line lookups that fail in the slim container
    // (no separate debuginfo packages installed).
    tracing::info!(container, "running perf script");
    let script_output = Command::new("docker")
        .args([
            "exec",
            container,
            "perf",
            "script",
            "--no-inline",
            "-i",
            "/tmp/perf.data",
        ])
        .output()
        .await?;

    // perf script may print addr2line warnings to stderr even on success.
    // Only fail if stdout is empty (no usable data).
    if script_output.stdout.is_empty() {
        let stderr = String::from_utf8_lossy(&script_output.stderr);
        anyhow::bail!("perf script produced no output: {stderr}");
    }

    // Step 3: Collapse via inferno.
    tracing::info!("collapsing perf script output");
    let mut collapsed = Vec::new();
    {
        let mut folder = inferno::collapse::perf::Folder::default();
        inferno::collapse::Collapse::collapse(
            &mut folder,
            &script_output.stdout[..],
            &mut collapsed,
        )?;
    }

    // Step 4: Generate flamegraph SVG.
    tracing::info!("generating flamegraph SVG");
    let mut svg = Vec::new();
    {
        let mut opts = inferno::flamegraph::Options::default();
        opts.title = format!("DeltaForge CPU Profile — {container}");
        opts.subtitle = Some(format!(
            "{duration_secs}s @ {frequency}Hz — {}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S")
        ));
        inferno::flamegraph::from_reader(&mut opts, &collapsed[..], &mut svg)?;
    }

    // Cleanup perf.data inside container (best-effort).
    let _ = Command::new("docker")
        .args(["exec", container, "rm", "-f", "/tmp/perf.data"])
        .status()
        .await;

    tracing::info!(svg_bytes = svg.len(), "flamegraph generated");
    Ok(svg)
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
        .route("/api/refresh-service", post(api_refresh_service))
        .route("/api/reset-volumes", post(api_reset_volumes))
        .route("/api/swap-image", post(api_swap_image))
        .route("/api/profile/start", post(api_profile_start))
        .route("/api/profile/status", get(api_profile_status))
        .route("/api/profile/flamegraph", get(api_profile_flamegraph))
        .route("/api/df", get(api_df_get))
        .route("/api/df", post(api_df_post))
        .route("/api/df/patch", post(api_df_patch))
        .route("/api/df/delete", post(api_df_delete))
        .with_state(state);

    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("DeltaForge Playground → http://localhost:{port}");
    println!("DeltaForge Playground → http://localhost:{port}");
    axum::serve(listener, app).await?;
    Ok(())
}

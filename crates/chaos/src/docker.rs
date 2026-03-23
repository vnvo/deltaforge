//! Shared Docker Compose helpers for chaos scenarios.
//!
//! All helpers target `docker-compose.chaos.yml` — no need to pass the file
//! path at every call site. Profile and service are passed as arguments so
//! each source backend can manage its own DeltaForge container.

use anyhow::{Result, bail};
use tokio::time::{Duration, sleep};

const COMPOSE_FILE: &str = "docker-compose.chaos.yml";

async fn compose(profile: &str, args: &[&str]) -> Result<()> {
    let mut full: Vec<&str> =
        vec!["compose", "-f", COMPOSE_FILE, "--profile", profile];
    full.extend_from_slice(args);
    let status = tokio::process::Command::new("docker")
        .args(&full)
        .status()
        .await?;
    if !status.success() {
        bail!(
            "docker compose {} failed (exit {:?})",
            args.join(" "),
            status.code()
        );
    }
    Ok(())
}

/// Graceful stop (SIGTERM). Checkpoint is flushed before exit.
pub async fn stop_service(profile: &str, service: &str) -> Result<()> {
    compose(profile, &["stop", service]).await
}

/// Start a stopped service.
pub async fn start_service(profile: &str, service: &str) -> Result<()> {
    compose(profile, &["start", service]).await
}

/// Stop then start — cleanest way to restart without relying on restart policy.
pub async fn restart_service(profile: &str, service: &str) -> Result<()> {
    stop_service(profile, service).await?;
    start_service(profile, service).await
}

/// SIGKILL a running service (no graceful shutdown, no checkpoint flush).
/// Used by crash_recovery to simulate a process crash.
pub async fn kill_service(profile: &str, service: &str) -> Result<()> {
    compose(profile, &["kill", "-s", "KILL", service]).await?;
    // Brief pause so the container is fully stopped before the caller starts it again.
    sleep(Duration::from_secs(2)).await;
    Ok(())
}

/// Resource usage snapshot for a running compose service.
#[derive(Debug, Default, Clone)]
pub struct ResourceStats {
    /// CPU utilisation percent (e.g. 2.5 means 2.5 %).
    pub cpu_percent: f64,
    /// Resident memory in bytes.
    pub mem_bytes: u64,
}

/// Sample CPU% and memory for a compose service via `docker compose stats`.
///
/// Returns `Ok(ResourceStats::default())` if the container is not running or
/// the output cannot be parsed — the caller should treat this as a gap sample.
pub async fn sample_stats(
    profile: &str,
    service: &str,
) -> Result<ResourceStats> {
    let output = tokio::process::Command::new("docker")
        .args([
            "compose",
            "-f",
            COMPOSE_FILE,
            "--profile",
            profile,
            "stats",
            "--no-stream",
            "--format",
            "{{.CPUPerc}}\t{{.MemUsage}}",
            service,
        ])
        .output()
        .await?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout.lines().next().unwrap_or("").trim();
    let parts: Vec<&str> = line.splitn(2, '\t').collect();
    if parts.len() < 2 {
        return Ok(ResourceStats::default());
    }

    let cpu = parts[0]
        .trim()
        .trim_end_matches('%')
        .parse::<f64>()
        .unwrap_or(0.0);
    let mem_str = parts[1].split('/').next().unwrap_or("").trim();
    let mem_bytes = parse_mem(mem_str);

    Ok(ResourceStats {
        cpu_percent: cpu,
        mem_bytes,
    })
}

fn parse_mem(s: &str) -> u64 {
    let s = s.trim();
    if let Some(n) = s.strip_suffix("GiB") {
        return (n.trim().parse::<f64>().unwrap_or(0.0)
            * 1024.0
            * 1024.0
            * 1024.0) as u64;
    }
    if let Some(n) = s.strip_suffix("MiB") {
        return (n.trim().parse::<f64>().unwrap_or(0.0) * 1024.0 * 1024.0)
            as u64;
    }
    if let Some(n) = s.strip_suffix("kB") {
        return (n.trim().parse::<f64>().unwrap_or(0.0) * 1024.0) as u64;
    }
    0
}

/// Delete the DeltaForge SQLite checkpoint DB from the service's data volume
/// while the container is stopped.
///
/// Uses `docker compose run` so we inherit the correct volume mount without
/// hard-coding the volume name. Assumes the checkpoint lives at `/data/`.
pub async fn clear_checkpoint(profile: &str, service: &str) -> Result<()> {
    // Best-effort — ignore exit code (files may not exist on a fresh volume).
    tokio::process::Command::new("docker")
        .args([
            "compose",
            "-f",
            COMPOSE_FILE,
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
        .status()
        .await?;
    Ok(())
}

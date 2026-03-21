//! Shared Docker Compose helpers for chaos scenarios.
//!
//! All helpers target `docker-compose.chaos.yml` — no need to pass the file
//! path at every call site. Profile and service are passed as arguments so
//! each source backend can manage its own DeltaForge container.

use anyhow::{Result, bail};
use tokio::time::{Duration, sleep};

const COMPOSE_FILE: &str = "docker-compose.chaos.yml";

async fn compose(profile: &str, args: &[&str]) -> Result<()> {
    let mut full: Vec<&str> = vec!["compose", "-f", COMPOSE_FILE, "--profile", profile];
    full.extend_from_slice(args);
    let status = tokio::process::Command::new("docker")
        .args(&full)
        .status()
        .await?;
    if !status.success() {
        bail!("docker compose {} failed (exit {:?})", args.join(" "), status.code());
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

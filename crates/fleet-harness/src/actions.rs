//! Disruptions and operator actions (design section 5.1).
//!
//! Built in: Toxiproxy endpoint outages, and pipeline lifecycle operations
//! through the REST API. Everything environment-specific (process
//! restart, sink or state-store outage, primary failover, credential
//! rotation) is a configured command hook, so the harness never assumes how
//! the owner's infrastructure works. A hook is a shell command template;
//! `{server}`, `{pipeline}` and `{pid}` are substituted.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use serde_json::json;

/// Run hook `name` with substitutions; fails if it is not configured.
pub async fn hook(
    hooks: &BTreeMap<String, String>,
    name: &str,
    vars: &[(&str, &str)],
) -> Result<Duration> {
    let template = hooks.get(name).ok_or_else(|| {
        anyhow!("action hook `{name}` is not configured (actions.{name})")
    })?;
    let mut cmd = template.clone();
    for (k, v) in vars {
        cmd = cmd.replace(&format!("{{{k}}}"), v);
    }
    let started = Instant::now();
    let out = tokio::process::Command::new("sh")
        .arg("-c")
        .arg(&cmd)
        .output()
        .await
        .with_context(|| format!("run hook {name}"))?;
    if !out.status.success() {
        bail!(
            "hook {name} failed ({}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(started.elapsed())
}

/// Enable or disable Toxiproxy proxies.
pub async fn toxiproxy(
    url: &str,
    proxies: &[String],
    enabled: bool,
) -> Result<()> {
    let http = reqwest::Client::new();
    for p in proxies {
        let resp = http
            .post(format!("{}/proxies/{p}", url.trim_end_matches('/')))
            .json(&json!({ "enabled": enabled }))
            .send()
            .await
            .with_context(|| format!("toxiproxy {p}"))?;
        if !resp.status().is_success() {
            bail!("toxiproxy {p}: {}", resp.status());
        }
    }
    Ok(())
}

/// Wait until `pid` no longer exists; returns how long it took.
pub async fn wait_exit(pid: u32, timeout: Duration) -> Result<Duration> {
    let started = Instant::now();
    let path = format!("/proc/{pid}");
    while std::path::Path::new(&path).exists() {
        if started.elapsed() > timeout {
            bail!("process {pid} still running after {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(started.elapsed())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn hooks_substitute_and_report_failures() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out");
        let hooks = BTreeMap::from([
            (
                "touch".to_string(),
                format!("echo {{server}}-{{pipeline}} > {}", out.display()),
            ),
            ("fail".to_string(), "exit 3".to_string()),
        ]);
        hook(
            &hooks,
            "touch",
            &[("server", "c01"), ("pipeline", "fleet-c01")],
        )
        .await
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(&out).unwrap().trim(),
            "c01-fleet-c01"
        );
        assert!(hook(&hooks, "fail", &[]).await.is_err());
        let err = hook(&hooks, "missing", &[]).await.unwrap_err().to_string();
        assert!(err.contains("actions.missing"));
    }

    #[tokio::test]
    async fn waiting_for_an_exited_process_returns_at_once() {
        assert!(
            wait_exit(u32::MAX - 1, Duration::from_secs(1))
                .await
                .unwrap()
                < Duration::from_secs(1)
        );
    }
}

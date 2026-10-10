//! The instance's proof trace: the `deltaforge::proof_trace` records
//! (`scan`, `request`, `proof`) it logged during the run, collected per
//! server with the server's binlog inventory (captured once, at the end)
//! and summarized offline by `scripts/proof-trace-report.py`. Collection
//! never fails a run: what could not be collected is recorded.

use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use mysql_async::prelude::Queryable;
use serde::Serialize;
use serde_json::{Value, json};

use crate::config::{RunConfig, Server, TraceSource};

/// Where the run's part of the log starts.
#[derive(Debug, Clone)]
pub enum Mark {
    Since(DateTime<Utc>),
    Offset(PathBuf, u64),
}

/// Remember where the run starts in `source`.
pub fn mark(source: &TraceSource, now: DateTime<Utc>) -> Mark {
    match source {
        TraceSource::DockerLogs { .. } => Mark::Since(now),
        TraceSource::File { path } => Mark::Offset(
            PathBuf::from(path),
            std::fs::metadata(path).map(|m| m.len()).unwrap_or(0),
        ),
    }
}

/// The proof-trace record in one log line: the JSON message itself (plain
/// text logs) or inside a JSON log line's `fields.message` / `message`.
pub fn record(line: &str) -> Option<Value> {
    let is_record = |v: &Value| {
        matches!(
            v.get("record").and_then(Value::as_str),
            Some("scan" | "request" | "proof")
        )
    };
    if let Ok(v) = serde_json::from_str::<Value>(line.trim()) {
        for msg in [&v["fields"]["message"], &v["message"]] {
            if let Some(m) = msg.as_str()
                && let Ok(inner) = serde_json::from_str::<Value>(m)
                && is_record(&inner)
            {
                return Some(inner);
            }
        }
        if is_record(&v) {
            return Some(v);
        }
    }
    let start = line.find("{\"record\":")?;
    let end = line.rfind('}')?;
    let v: Value = serde_json::from_str(line.get(start..=end)?).ok()?;
    is_record(&v).then_some(v)
}

/// The log text written since `mark`.
async fn read_since(source: &TraceSource, mark: &Mark) -> Result<String> {
    match (source, mark) {
        (TraceSource::DockerLogs { container }, Mark::Since(t)) => {
            let out = tokio::process::Command::new("docker")
                .args([
                    "logs",
                    "--since",
                    &t.to_rfc3339(),
                    "--until",
                    &Utc::now().to_rfc3339(),
                    container,
                ])
                .output()
                .await
                .context("docker logs")?;
            anyhow::ensure!(
                out.status.success(),
                "docker logs {container}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
            // The instance may log on either stream.
            let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
            text.push_str(&String::from_utf8_lossy(&out.stderr));
            Ok(text)
        }
        (TraceSource::File { .. }, Mark::Offset(path, at)) => {
            let mut f = std::fs::File::open(path)
                .with_context(|| format!("open {}", path.display()))?;
            f.seek(SeekFrom::Start(*at))?;
            let mut text = String::new();
            f.read_to_string(&mut text)?;
            Ok(text)
        }
        _ => anyhow::bail!("the trace mark does not match its source"),
    }
}

/// What was collected.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Collected {
    pub records: u64,
    pub scans: u64,
    pub requests: u64,
    pub proofs: u64,
    pub files: Vec<String>,
    /// Collection or report problems (the run's verdict is unaffected).
    pub errors: Vec<String>,
}

/// One server's binlog inventory, identity and executed set, for the
/// report's byte offsets and transaction counts.
async fn inventory(cfg: &RunConfig, server: &Server) -> Result<Value> {
    let pool = crate::fixture::admin_pool(cfg, server, 1)?;
    let mut c = pool.get_conn().await?;
    let logs: Vec<(String, u64)> = c
        .query_map("SHOW BINARY LOGS", |row: mysql_async::Row| {
            let mut row = row;
            (
                row.take::<String, _>(0).unwrap_or_default(),
                row.take::<u64, _>(1).unwrap_or_default(),
            )
        })
        .await?;
    let uuid: Option<String> =
        c.query_first("SELECT @@GLOBAL.server_uuid").await?;
    let gtid_mode: Option<String> =
        c.query_first("SELECT @@GLOBAL.gtid_mode").await?;
    let executed: Option<String> =
        c.query_first("SELECT @@GLOBAL.gtid_executed").await?;
    drop(c);
    pool.disconnect().await.ok();
    let gtid = gtid_mode.is_some_and(|m| m.eq_ignore_ascii_case("ON"));
    Ok(json!({
        "server": server.name,
        "mode": if gtid { "gtid" } else { "file" },
        "tables": u64::from(cfg.topology.databases_per_server.value)
            * u64::from(cfg.topology.tables_per_database.value),
        "server_uuid": uuid,
        "gtid_executed": executed,
        "binlogs": logs,
    }))
}

/// The report script: `FLEET_HARNESS_PROOF_REPORT`, else the repository's.
fn report_script() -> PathBuf {
    std::env::var_os("FLEET_HARNESS_PROOF_REPORT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../scripts/proof-trace-report.py")
        })
}

/// Collect the run's records per server into `dir`, with each server's
/// inventory, and generate each server's report.
pub async fn collect(
    cfg: &RunConfig,
    servers: &[Server],
    mark: &Mark,
    dir: &Path,
) -> Collected {
    let mut out = Collected::default();
    let Some(source) = &cfg.deltaforge.proof_trace else {
        return out;
    };
    let text = match read_since(source, mark).await {
        Ok(t) => t,
        Err(e) => {
            out.errors.push(format!("{e:#}"));
            return out;
        }
    };
    let records: Vec<Value> = text.lines().filter_map(record).collect();
    out.records = records.len() as u64;
    for r in &records {
        match r["record"].as_str() {
            Some("scan") => out.scans += 1,
            Some("request") => out.requests += 1,
            Some("proof") => out.proofs += 1,
            _ => {}
        }
    }
    for s in servers {
        let source_id = format!("src-{}", s.name);
        let jsonl = dir.join(format!("proof-trace-{}.jsonl", s.name));
        let meta = dir.join(format!("proof-trace-{}-meta.json", s.name));
        let mine: String = records
            .iter()
            .filter(|r| r["source"] == source_id.as_str())
            .map(|r| format!("{r}\n"))
            .collect();
        if let Err(e) = std::fs::write(&jsonl, mine) {
            out.errors.push(format!("{}: {e}", jsonl.display()));
            continue;
        }
        out.files.push(jsonl.display().to_string());
        match inventory(cfg, s).await {
            Ok(v) => {
                if let Err(e) = std::fs::write(&meta, v.to_string()) {
                    out.errors.push(format!("{}: {e}", meta.display()));
                    continue;
                }
                out.files.push(meta.display().to_string());
            }
            Err(e) => {
                out.errors.push(format!("inventory of {}: {e:#}", s.name));
                continue;
            }
        }
        let report = dir.join(format!("proof-trace-{}-report.md", s.name));
        match tokio::process::Command::new("python3")
            .arg(report_script())
            .arg(&jsonl)
            .arg(&meta)
            .output()
            .await
        {
            Ok(o) if o.status.success() => {
                if let Err(e) = std::fs::write(&report, &o.stdout) {
                    out.errors.push(format!("{}: {e}", report.display()));
                } else {
                    out.files.push(report.display().to_string());
                }
            }
            Ok(o) => out.errors.push(format!(
                "report for {}: {}",
                s.name,
                String::from_utf8_lossy(&o.stderr)
            )),
            Err(e) => out.errors.push(format!("report for {}: {e}", s.name)),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_are_found_in_plain_and_json_log_lines() {
        let rec = r#"{"record":"scan","scan_id":7,"source":"src-c01"}"#;
        let plain = format!(
            "2026-10-10T00:00:00Z  INFO deltaforge::proof_trace: {rec}"
        );
        assert_eq!(record(&plain).unwrap()["scan_id"], 7);
        let json_line = json!({
            "timestamp": "2026-10-10T00:00:00Z",
            "target": "deltaforge::proof_trace",
            "fields": {"message": rec},
        })
        .to_string();
        assert_eq!(record(&json_line).unwrap()["scan_id"], 7);
        let flat = json!({"message": rec}).to_string();
        assert_eq!(record(&flat).unwrap()["record"], "scan");
        assert_eq!(record(rec).unwrap()["record"], "scan");
        // Anything else is not a proof-trace record.
        for other in [
            "INFO sources::mysql: connected",
            r#"{"record":"other"}"#,
            r#"{"fields":{"message":"plain text"}}"#,
            "",
        ] {
            assert!(record(other).is_none(), "{other}");
        }
    }

    #[test]
    fn a_file_source_reads_only_what_the_run_wrote() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        std::fs::write(&path, "before {\"record\":\"scan\",\"n\":1}\n")
            .unwrap();
        let source = TraceSource::File {
            path: path.display().to_string(),
        };
        let m = mark(&source, Utc::now());
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        std::io::Write::write_all(
            &mut f,
            b"during {\"record\":\"scan\",\"n\":2}\n",
        )
        .unwrap();
        let text = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(read_since(&source, &m))
            .unwrap();
        let got: Vec<Value> = text.lines().filter_map(record).collect();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0]["n"], 2);
    }
}

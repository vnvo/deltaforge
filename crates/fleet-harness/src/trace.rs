//! The instance's proof trace: the `deltaforge::proof_trace` records
//! (`scan`, `request`, `proof`) it logged during the run, collected per
//! server with the server's binlog inventory (captured once, at the end)
//! and summarized offline by `scripts/proof-trace-report.py`.
//!
//! Collection is checked: every server's records, inventory and report
//! must exist and no record line may be malformed. Incomplete evidence
//! fails a run that requires it (`RunConfig::proof_trace_required`);
//! otherwise the run completes, records what is missing and supports no
//! proof-performance conclusion.

use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
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
    any_record(line).filter(|v| kind(v) == Kind::Proof)
}

/// The proof records, which the run's proof summary counts.
const PROOF_RECORDS: [&str; 3] = ["scan", "request", "proof"];
/// The activation window's measurement records: well-formed trace records
/// that no proof summary counts.
const MEASUREMENT_RECORDS: [&str; 4] =
    ["window", "release", "batch", "abandoned"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Proof,
    Measurement,
    Unknown,
}

fn kind(v: &Value) -> Kind {
    match v.get("record").and_then(Value::as_str) {
        Some(r) if PROOF_RECORDS.contains(&r) => Kind::Proof,
        Some(r) if MEASUREMENT_RECORDS.contains(&r) => Kind::Measurement,
        _ => Kind::Unknown,
    }
}

/// The trace record a line carries, of any type (a JSON object with a
/// string `record` field), whether the line is the record itself, a JSON
/// log line whose message is the record, or text around it.
fn any_record(line: &str) -> Option<Value> {
    let is_record =
        |v: &Value| v.get("record").and_then(Value::as_str).is_some();
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

/// Feed `each` the log lines written since `mark`, one at a time, so the
/// log is never held whole (a million active tables log about 1.2 GB).
/// Docker's two streams are spooled to files in `spool` (the instance may
/// log on either; stdout is read first, then stderr) and removed after.
async fn stream_since(
    source: &TraceSource,
    mark: &Mark,
    spool: &Path,
    mut each: impl FnMut(&str),
) -> Result<()> {
    match (source, mark) {
        (TraceSource::DockerLogs { container }, Mark::Since(t)) => {
            let out = spool.join(".proof-trace-stdout");
            let err = spool.join(".proof-trace-stderr");
            let status = async {
                tokio::process::Command::new("docker")
                    .args([
                        "logs",
                        "--since",
                        &t.to_rfc3339(),
                        "--until",
                        &Utc::now().to_rfc3339(),
                        container,
                    ])
                    .stdout(std::fs::File::create(&out)?)
                    .stderr(std::fs::File::create(&err)?)
                    .status()
                    .await
                    .context("docker logs")
            }
            .await;
            let read = match status {
                Ok(s) if s.success() => each_line(&out, 0, &mut each)
                    .and_then(|()| each_line(&err, 0, &mut each)),
                Ok(_) => {
                    let mut head = Vec::new();
                    if let Ok(f) = std::fs::File::open(&err) {
                        f.take(4096).read_to_end(&mut head).ok();
                    }
                    Err(anyhow::anyhow!(
                        "docker logs {container}: {}",
                        String::from_utf8_lossy(&head)
                    ))
                }
                Err(e) => Err(e),
            };
            std::fs::remove_file(&out).ok();
            std::fs::remove_file(&err).ok();
            read
        }
        (TraceSource::File { .. }, Mark::Offset(path, at)) => {
            each_line(path, *at, &mut each)
        }
        _ => anyhow::bail!("the trace mark does not match its source"),
    }
}

/// Feed `each` the lines of `path` from byte `at`, without their line
/// ending (`\n` or `\r\n`); invalid UTF-8 is replaced, not an error.
fn each_line(path: &Path, at: u64, each: &mut impl FnMut(&str)) -> Result<()> {
    let mut f = std::fs::File::open(path)
        .with_context(|| format!("open {}", path.display()))?;
    f.seek(SeekFrom::Start(at))?;
    let mut r = BufReader::with_capacity(1 << 20, f);
    let mut line = Vec::new();
    loop {
        line.clear();
        if r.read_until(b'\n', &mut line)? == 0 {
            return Ok(());
        }
        if line.last() == Some(&b'\n') {
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
        }
        each(&String::from_utf8_lossy(&line));
    }
}

/// The run's record counts over every source in the log.
#[derive(Debug, Clone, Copy, Default)]
struct Counts {
    records: u64,
    scans: u64,
    requests: u64,
    proofs: u64,
    malformed: u64,
}

/// One server's JSONL file of proof records.
struct Out {
    /// The records' `source`: `src-<server>`.
    source: String,
    path: PathBuf,
    file: Option<BufWriter<std::fs::File>>,
    written: u64,
    /// The first failure to create or write the file.
    error: Option<String>,
}

/// The log's proof records as it is read: counted, and each written at once
/// to its server's file, so no record is held in memory. A record of
/// another source is counted but written nowhere.
struct Router {
    outs: Vec<Out>,
    counts: Counts,
}

impl Router {
    /// One `proof-trace-<server>.jsonl` file in `dir` per server.
    fn create(dir: &Path, servers: &[String]) -> Router {
        let outs = servers
            .iter()
            .map(|name| {
                let path = dir.join(format!("proof-trace-{name}.jsonl"));
                let (file, error) = match std::fs::File::create(&path) {
                    Ok(f) => (Some(BufWriter::new(f)), None),
                    Err(e) => (None, Some(e.to_string())),
                };
                Out {
                    source: format!("src-{name}"),
                    path,
                    file,
                    written: 0,
                    error,
                }
            })
            .collect();
        Router {
            outs,
            counts: Counts::default(),
        }
    }

    /// One log line. Measurement records are recognized and left out; a
    /// line that claims a record but does not parse as one, or carries an
    /// unknown type, is malformed.
    fn line(&mut self, line: &str) {
        let r = match any_record(line) {
            Some(r) => match kind(&r) {
                Kind::Proof => r,
                Kind::Measurement => return,
                Kind::Unknown => {
                    self.counts.malformed += 1;
                    return;
                }
            },
            None => {
                if claims_record(line) {
                    self.counts.malformed += 1;
                }
                return;
            }
        };
        self.counts.records += 1;
        match r["record"].as_str() {
            Some("scan") => self.counts.scans += 1,
            Some("request") => self.counts.requests += 1,
            Some("proof") => self.counts.proofs += 1,
            _ => {}
        }
        let Some(out) = self.outs.iter_mut().find(|o| r["source"] == o.source)
        else {
            return;
        };
        out.written += 1;
        if let Some(f) = out.file.as_mut()
            && let Err(e) = writeln!(f, "{r}")
        {
            out.error.get_or_insert(e.to_string());
            out.file = None;
        }
    }

    /// Flush every file.
    fn finish(&mut self) {
        for out in &mut self.outs {
            if let Some(mut f) = out.file.take()
                && let Err(e) = f.flush()
            {
                out.error.get_or_insert(e.to_string());
            }
        }
    }
}

/// What was collected.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Collected {
    /// A proof trace was configured.
    pub requested: bool,
    /// It is required evidence: incomplete, it fails the repetition.
    pub required: bool,
    /// Every server's records, inventory and report exist and no record
    /// line is malformed.
    pub complete: bool,
    pub records: u64,
    pub scans: u64,
    pub requests: u64,
    pub proofs: u64,
    /// Lines that look like proof-trace records but do not parse as one.
    pub malformed: u64,
    pub files: Vec<String>,
    /// Why the evidence is incomplete (empty when complete).
    pub errors: Vec<String>,
}

/// A line that claims to carry a proof-trace record.
fn claims_record(line: &str) -> bool {
    line.contains("deltaforge::proof_trace") || line.contains("{\"record\":")
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
    let mut out = Collected {
        requested: cfg.deltaforge.proof_trace.is_some(),
        required: cfg.proof_trace_required(),
        ..Collected::default()
    };
    let Some(source) = &cfg.deltaforge.proof_trace else {
        return out;
    };
    let names: Vec<String> = servers.iter().map(|s| s.name.clone()).collect();
    let mut router = Router::create(dir, &names);
    if let Err(e) =
        stream_since(source, mark, dir, |line| router.line(line)).await
    {
        out.errors.push(format!("read the trace: {e:#}"));
        for o in &router.outs {
            std::fs::remove_file(&o.path).ok();
        }
        return out;
    }
    router.finish();
    let c = router.counts;
    (
        out.records,
        out.scans,
        out.requests,
        out.proofs,
        out.malformed,
    ) = (c.records, c.scans, c.requests, c.proofs, c.malformed);
    if out.malformed > 0 {
        out.errors
            .push(format!("{} malformed proof-trace lines", out.malformed));
    }
    for (s, o) in servers.iter().zip(router.outs) {
        let jsonl = o.path;
        let meta = dir.join(format!("proof-trace-{}-meta.json", s.name));
        if o.written == 0 {
            // Most often the instance's log filter drops the target.
            out.errors.push(format!(
                "no proof-trace records for {} (is deltaforge::proof_trace \
                 logged at info?)",
                s.name
            ));
        }
        if let Some(e) = o.error {
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
            Ok(o)
                if o.status.success()
                    && o.stdout.starts_with(b"# Proof-trace evidence") =>
            {
                if let Err(e) = std::fs::write(&report, &o.stdout) {
                    out.errors.push(format!("{}: {e}", report.display()));
                } else {
                    out.files.push(report.display().to_string());
                }
            }
            Ok(o) => out.errors.push(format!(
                "report for {}: {} {}",
                s.name,
                o.status,
                String::from_utf8_lossy(&o.stderr)
            )),
            Err(e) => out.errors.push(format!("report for {}: {e}", s.name)),
        }
    }
    out.complete = out.errors.is_empty();
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

    /// Route `text` line by line for `servers` into a temporary directory:
    /// the router and each server's written records, in file order.
    fn route(
        text: &str,
        servers: &[&str],
    ) -> (Router, Vec<Vec<Value>>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let names: Vec<String> =
            servers.iter().map(|s| s.to_string()).collect();
        let mut router = Router::create(dir.path(), &names);
        for line in text.lines() {
            router.line(line);
        }
        router.finish();
        let written = router
            .outs
            .iter()
            .map(|o| {
                std::fs::read_to_string(&o.path)
                    .unwrap()
                    .lines()
                    .map(|l| serde_json::from_str(l).unwrap())
                    .collect()
            })
            .collect();
        (router, written, dir)
    }

    /// Every accepted type parses: the proof records are kept, the
    /// measurement records recognized and left out of the summary; an
    /// unknown type is malformed, not ignored.
    #[test]
    fn record_types_are_explicit() {
        let text = [
            r#"{"record":"scan","scan_id":1,"source":"src-a"}"#,
            r#"INFO deltaforge::proof_trace: {"record":"request","served":"scanned","source":"src-a"}"#,
            r#"{"record":"proof","outcome":"proven","source":"src-a"}"#,
            r#"{"record":"window","tables":3,"source":"src-a"}"#,
            r#"{"record":"release","window":1,"rank":0,"source":"src-a"}"#,
            r#"{"record":"batch","seq":1,"tables":[],"source":"src-a"}"#,
            r#"{"record":"abandoned","id":2,"reason":"uncovered","source":"src-a"}"#,
            r#"{"record":"mystery","n":1,"source":"src-a"}"#,
        ]
        .join("\n");
        let (router, written, _dir) = route(&text, &["a"]);
        let kinds: Vec<&str> = written[0]
            .iter()
            .map(|r| r["record"].as_str().unwrap())
            .collect();
        assert_eq!(kinds, ["scan", "request", "proof"]);
        let c = &router.counts;
        assert_eq!((c.records, c.scans, c.requests, c.proofs), (3, 1, 1, 1));
        assert_eq!(c.malformed, 1, "the unknown type");
        for line in text.split('\n').skip(3).take(4) {
            assert!(record(line).is_none(), "{line} is not a proof record");
        }
        assert!(record(r#"{"record":"scan","scan_id":9}"#).is_some());
    }

    #[test]
    fn truncated_or_unparsable_record_lines_are_counted_as_malformed() {
        let text = [
            r#"{"record":"scan","scan_id":1}"#,
            r#"{"record":"scan","scan_id":2"#,
            "INFO deltaforge::proof_trace: not json",
            "INFO sources::mysql: connected",
        ]
        .join("\n");
        let (router, _, _dir) = route(&text, &["a"]);
        assert_eq!(router.counts.records, 1);
        assert_eq!(router.counts.malformed, 2);
    }

    /// Each record goes to its own server's file, in log order; a record of
    /// another source is counted but written nowhere, and a server without
    /// records gets an empty file.
    #[test]
    fn records_are_written_to_their_servers_files_in_log_order() {
        let text = [
            r#"{"record":"scan","scan_id":1,"source":"src-a"}"#,
            r#"{"record":"scan","scan_id":2,"source":"src-b"}"#,
            "INFO sources::mysql: connected",
            r#"{"record":"proof","n":3,"source":"src-a"}"#,
            r#"{"record":"proof","n":4,"source":"src-other"}"#,
        ]
        .join("\n");
        let (router, written, _dir) = route(&text, &["a", "b", "c"]);
        let ids = |rs: &Vec<Value>| -> Vec<String> {
            rs.iter().map(|r| r.to_string()).collect()
        };
        assert_eq!(
            ids(&written[0]),
            [
                r#"{"record":"scan","scan_id":1,"source":"src-a"}"#,
                r#"{"record":"proof","n":3,"source":"src-a"}"#,
            ]
        );
        assert_eq!(written[1].len(), 1);
        assert_eq!(written[1][0]["scan_id"], 2);
        assert!(written[2].is_empty());
        let written_counts: Vec<u64> =
            router.outs.iter().map(|o| o.written).collect();
        assert_eq!(written_counts, [2, 1, 0]);
        assert_eq!(router.counts.records, 4, "src-other is counted");
        assert!(router.outs.iter().all(|o| o.error.is_none()));
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
        std::io::Write::write_all(&mut f, b"no newline at the end").unwrap();
        let mut lines = Vec::new();
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(stream_since(&source, &m, dir.path(), |l| {
                lines.push(l.to_string())
            }))
            .unwrap();
        assert_eq!(
            lines,
            [
                "during {\"record\":\"scan\",\"n\":2}",
                "no newline at the end"
            ]
        );
    }
}

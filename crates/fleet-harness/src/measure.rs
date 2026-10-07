//! Measurements of the instance under test (design section 4.5).
//!
//! Sampled every `measure.interval_secs` into a JSON-lines time series and
//! folded into [`Aggregate`]: cgroup memory and CPU, threads and file
//! descriptors of the DeltaForge process (checking the resource inventory's
//! planning estimates), MySQL connections per server for the CDC users,
//! state-store size, and per-pipeline metrics from the Prometheus endpoint.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use mysql_async::prelude::Queryable;
use serde::Serialize;

use crate::config::ProcessLocator;

/// One Prometheus sample.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Sample {
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub value: f64,
}

/// Parse the Prometheus text exposition (comments and unparsable lines are
/// skipped).
pub fn parse_prometheus(text: &str) -> Vec<Sample> {
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (series, value) = match line.rsplit_once(' ') {
            Some(p) => p,
            None => continue,
        };
        let Ok(value) = value.trim().parse::<f64>() else {
            continue;
        };
        let (name, labels) = match series.split_once('{') {
            None => (series.to_string(), BTreeMap::new()),
            Some((name, rest)) => {
                let body = rest.strip_suffix('}').unwrap_or(rest);
                (name.to_string(), parse_labels(body))
            }
        };
        out.push(Sample {
            name,
            labels,
            value,
        });
    }
    out
}

fn parse_labels(body: &str) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    let mut chars = body.chars().peekable();
    loop {
        let key: String = chars.by_ref().take_while(|&c| c != '=').collect();
        if key.is_empty() {
            break;
        }
        if chars.next() != Some('"') {
            break;
        }
        let mut value = String::new();
        while let Some(c) = chars.next() {
            match c {
                '\\' => match chars.next() {
                    Some('n') => value.push('\n'),
                    Some(c) => value.push(c),
                    None => break,
                },
                '"' => break,
                c => value.push(c),
            }
        }
        labels.insert(key.trim_start_matches(',').trim().to_string(), value);
        if chars.peek() == Some(&',') {
            chars.next();
        }
    }
    labels
}

/// Sum of `name` per value of label `by`.
pub fn sum_by(
    samples: &[Sample],
    name: &str,
    by: &str,
) -> BTreeMap<String, f64> {
    let mut out = BTreeMap::new();
    for s in samples.iter().filter(|s| s.name == name) {
        if let Some(k) = s.labels.get(by) {
            *out.entry(k.clone()).or_insert(0.0) += s.value;
        }
    }
    out
}

/// The cgroup v2 directory of `pid` under `cgroup_root` (from
/// `/proc/<pid>/cgroup`'s `0::<path>` line).
pub fn cgroup_dir(
    proc_root: &Path,
    cgroup_root: &Path,
    pid: u32,
) -> Result<PathBuf> {
    let text =
        std::fs::read_to_string(proc_root.join(pid.to_string()).join("cgroup"))
            .context("read the process cgroup")?;
    let path = text
        .lines()
        .find_map(|l| l.strip_prefix("0::"))
        .ok_or_else(|| anyhow!("no cgroup v2 entry for pid {pid}"))?;
    Ok(cgroup_root.join(path.trim_start_matches('/')))
}

/// One reading of the process.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ProcessReading {
    pub memory_current: Option<u64>,
    pub memory_peak: Option<u64>,
    /// Cumulative CPU time (microseconds).
    pub cpu_usage_usec: Option<u64>,
    pub threads: Option<u64>,
    pub fds: Option<u64>,
}

pub fn read_process(
    proc_root: &Path,
    cgroup: Option<&Path>,
    pid: u32,
) -> ProcessReading {
    let num = |p: PathBuf| -> Option<u64> {
        std::fs::read_to_string(p).ok()?.trim().parse().ok()
    };
    let mut r = ProcessReading::default();
    if let Some(dir) = cgroup {
        r.memory_current = num(dir.join("memory.current"));
        r.memory_peak = num(dir.join("memory.peak"));
        r.cpu_usage_usec = std::fs::read_to_string(dir.join("cpu.stat"))
            .ok()
            .and_then(|t| {
                t.lines()
                    .find_map(|l| l.strip_prefix("usage_usec "))
                    .and_then(|v| v.trim().parse().ok())
            });
    }
    let proc_dir = proc_root.join(pid.to_string());
    r.threads = std::fs::read_to_string(proc_dir.join("status"))
        .ok()
        .and_then(|t| {
            t.lines()
                .find_map(|l| l.strip_prefix("Threads:"))
                .and_then(|v| v.trim().parse().ok())
        });
    r.fds = std::fs::read_dir(proc_dir.join("fd"))
        .ok()
        .map(|d| d.count() as u64);
    r
}

/// The PID of the instance under test.
pub fn resolve_pid(locator: &ProcessLocator) -> Result<Option<u32>> {
    match locator {
        ProcessLocator::None => Ok(None),
        ProcessLocator::Pid { pid } => Ok(Some(*pid)),
        ProcessLocator::DockerContainer { name } => {
            let out = std::process::Command::new("docker")
                .args(["inspect", "-f", "{{.State.Pid}}", name])
                .output()
                .context("docker inspect")?;
            if !out.status.success() {
                bail!(
                    "docker inspect {name}: {}",
                    String::from_utf8_lossy(&out.stderr)
                );
            }
            let pid: u32 =
                String::from_utf8_lossy(&out.stdout).trim().parse()?;
            Ok((pid != 0).then_some(pid))
        }
    }
}

/// Connections of `users` on one MySQL server.
pub async fn mysql_connections(
    conn: &mut mysql_async::Conn,
    users: &[String],
) -> Result<u64> {
    let rows: Vec<(String, u64)> = conn
        .query("SELECT USER, COUNT(*) FROM information_schema.PROCESSLIST GROUP BY USER")
        .await?;
    Ok(rows
        .into_iter()
        .filter(|(u, _)| users.contains(u))
        .map(|(_, n)| n)
        .sum())
}

/// State-store size: the database and each DeltaForge relation.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct StoreSize {
    pub database_bytes: u64,
    pub relations: BTreeMap<String, u64>,
}

pub async fn store_size(client: &tokio_postgres::Client) -> Result<StoreSize> {
    let db: i64 = client
        .query_one("SELECT pg_database_size(current_database())", &[])
        .await?
        .get(0);
    let rows = client
        .query(
            "SELECT relname::text, pg_total_relation_size(oid) FROM pg_class \
             WHERE relname LIKE 'df\\_%' AND relkind = 'r'",
            &[],
        )
        .await?;
    Ok(StoreSize {
        database_bytes: db as u64,
        relations: rows
            .into_iter()
            .map(|r| (r.get::<_, String>(0), r.get::<_, i64>(1) as u64))
            .collect(),
    })
}

/// Running aggregates over a measured window.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Aggregate {
    pub samples: u64,
    pub memory_peak_bytes: Option<u64>,
    pub memory_mean_bytes: Option<f64>,
    pub cpu_cores_mean: Option<f64>,
    pub cpu_cores_max: Option<f64>,
    pub threads_max: Option<u64>,
    pub fds_max: Option<u64>,
    /// Per server name.
    pub connections_max: BTreeMap<String, u64>,
    pub store_bytes_max: Option<u64>,
    pub scrape_ms_max: Option<f64>,
    pub scrape_bytes_max: Option<u64>,
    #[serde(skip)]
    memory_sum: f64,
    #[serde(skip)]
    memory_n: u64,
    #[serde(skip)]
    last_cpu: Option<(Instant, u64)>,
    #[serde(skip)]
    cpu_sum: f64,
    #[serde(skip)]
    cpu_n: u64,
}

fn max_opt<T: PartialOrd + Copy>(a: Option<T>, b: Option<T>) -> Option<T> {
    match (a, b) {
        (Some(a), Some(b)) => Some(if b > a { b } else { a }),
        (a, b) => a.or(b),
    }
}

impl Aggregate {
    pub fn process(&mut self, at: Instant, r: &ProcessReading) {
        self.samples += 1;
        self.memory_peak_bytes = max_opt(
            self.memory_peak_bytes,
            max_opt(r.memory_peak, r.memory_current),
        );
        if let Some(m) = r.memory_current {
            self.memory_sum += m as f64;
            self.memory_n += 1;
            self.memory_mean_bytes =
                Some(self.memory_sum / self.memory_n as f64);
        }
        if let Some(u) = r.cpu_usage_usec {
            if let Some((t0, u0)) = self.last_cpu {
                let secs = at.duration_since(t0).as_secs_f64();
                if secs > 0.0 {
                    let cores = u.saturating_sub(u0) as f64 / 1e6 / secs;
                    self.cpu_sum += cores;
                    self.cpu_n += 1;
                    self.cpu_cores_mean =
                        Some(self.cpu_sum / self.cpu_n as f64);
                    self.cpu_cores_max =
                        max_opt(self.cpu_cores_max, Some(cores));
                }
            }
            self.last_cpu = Some((at, u));
        }
        self.threads_max = max_opt(self.threads_max, r.threads);
        self.fds_max = max_opt(self.fds_max, r.fds);
    }

    pub fn connections(&mut self, per_server: &HashMap<String, u64>) {
        for (s, n) in per_server {
            let e = self.connections_max.entry(s.clone()).or_insert(0);
            *e = (*e).max(*n);
        }
    }

    pub fn store(&mut self, bytes: u64) {
        self.store_bytes_max = max_opt(self.store_bytes_max, Some(bytes));
    }

    pub fn scrape(&mut self, ms: f64, bytes: u64) {
        self.scrape_ms_max = max_opt(self.scrape_ms_max, Some(ms));
        self.scrape_bytes_max = max_opt(self.scrape_bytes_max, Some(bytes));
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn prometheus_text_is_parsed_with_escaped_labels() {
        let text = "# HELP x y\n# TYPE deltaforge_sink_events_total counter\n\
            deltaforge_sink_events_total{pipeline=\"fleet-c01\",sink=\"k\"} 12\n\
            deltaforge_sink_events_total{pipeline=\"fleet-c02\",sink=\"k\"} 3\n\
            deltaforge_source_lag_seconds{pipeline=\"a\\\"b\"} 0.5\n\
            deltaforge_pipelines_total 2\n";
        let s = parse_prometheus(text);
        assert_eq!(s.len(), 4);
        let sums = sum_by(&s, "deltaforge_sink_events_total", "pipeline");
        assert_eq!(sums["fleet-c01"], 12.0);
        assert_eq!(s[2].labels["pipeline"], "a\"b");
        assert_eq!(s[3].labels.len(), 0);
    }

    #[test]
    fn cgroup_and_proc_readings() {
        let dir = tempfile::tempdir().unwrap();
        let proc_root = dir.path().join("proc");
        let cg_root = dir.path().join("cg");
        let p = proc_root.join("42");
        std::fs::create_dir_all(p.join("fd")).unwrap();
        for i in 0..3 {
            std::fs::write(p.join("fd").join(i.to_string()), "").unwrap();
        }
        std::fs::write(p.join("cgroup"), "0::/system.slice/docker-abc.scope\n")
            .unwrap();
        std::fs::write(p.join("status"), "Name:\tdf\nThreads:\t17\n").unwrap();
        let cg = cgroup_dir(&proc_root, &cg_root, 42).unwrap();
        assert_eq!(cg, cg_root.join("system.slice/docker-abc.scope"));
        std::fs::create_dir_all(&cg).unwrap();
        std::fs::write(cg.join("memory.current"), "1000\n").unwrap();
        std::fs::write(cg.join("memory.peak"), "4000\n").unwrap();
        std::fs::write(
            cg.join("cpu.stat"),
            "usage_usec 2000000\nuser_usec 1\n",
        )
        .unwrap();
        let r = read_process(&proc_root, Some(&cg), 42);
        assert_eq!(
            r,
            ProcessReading {
                memory_current: Some(1000),
                memory_peak: Some(4000),
                cpu_usage_usec: Some(2_000_000),
                threads: Some(17),
                fds: Some(3),
            }
        );
    }

    #[test]
    fn aggregate_derives_cpu_cores_from_usage_deltas() {
        let mut a = Aggregate::default();
        let t0 = Instant::now();
        let reading = |mem, usec| ProcessReading {
            memory_current: Some(mem),
            cpu_usage_usec: Some(usec),
            ..Default::default()
        };
        a.process(t0, &reading(100, 0));
        a.process(t0 + Duration::from_secs(2), &reading(300, 4_000_000));
        assert_eq!(a.cpu_cores_max, Some(2.0));
        assert_eq!(a.memory_mean_bytes, Some(200.0));
        assert_eq!(a.memory_peak_bytes, Some(300));
        a.connections(&HashMap::from([("c01".to_string(), 4)]));
        a.connections(&HashMap::from([("c01".to_string(), 2)]));
        assert_eq!(a.connections_max["c01"], 4);
    }
}

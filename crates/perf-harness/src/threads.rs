//! Per-thread scheduling counters of a system under test, from `/proc`:
//! on-CPU and runqueue-wait time (`schedstat`: running, and runnable but
//! not running), voluntary and involuntary context switches (`status`) and
//! migrations between CPUs (`sched`, when readable). Threads are grouped
//! by name with trailing digits removed (`tokio-runtime-w`, `rdk:broker`,
//! a JVM's `kafka-producer-network-thread | ...`), and the window's delta
//! is the sum over the group's threads.

use std::collections::BTreeMap;

use serde_json::{Value, json};

#[derive(Clone, Copy, Default)]
struct Counters {
    cpu_ns: u64,
    wait_ns: u64,
    vcsw: u64,
    ivcsw: u64,
    migrations: u64,
}

/// A snapshot: every thread (by tid) of the process tree under `root`.
pub struct Snapshot(BTreeMap<u32, (String, Counters)>);

fn group(comm: &str) -> String {
    comm.trim()
        .trim_end_matches(|c: char| c.is_ascii_digit() || c == '-' || c == '#')
        .to_string()
}

fn field(text: &str, name: &str) -> u64 {
    text.lines()
        .find_map(|l| l.strip_prefix(name))
        .and_then(|v| {
            v.trim_start_matches([' ', ':', '\t']).trim().parse().ok()
        })
        .unwrap_or(0)
}

/// `root` and every process under it (by parent pid).
fn tree(root: u32) -> Vec<u32> {
    let parents: Vec<(u32, u32)> = std::fs::read_dir("/proc")
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| {
            let pid: u32 = e.file_name().to_str()?.parse().ok()?;
            let stat = std::fs::read_to_string(e.path().join("stat")).ok()?;
            let ppid = stat
                .rsplit_once(')')?
                .1
                .split_whitespace()
                .nth(1)?
                .parse()
                .ok()?;
            Some((pid, ppid))
        })
        .collect();
    let mut out = vec![root];
    let mut i = 0;
    while i < out.len() {
        let p = out[i];
        out.extend(parents.iter().filter(|(_, pp)| *pp == p).map(|(c, _)| *c));
        i += 1;
    }
    out
}

impl Snapshot {
    /// The threads of `root` and of every process under it.
    pub fn take(root: u32) -> Snapshot {
        let mut out = BTreeMap::new();
        for pid in tree(root) {
            let Ok(tasks) = std::fs::read_dir(format!("/proc/{pid}/task"))
            else {
                continue;
            };
            for t in tasks.flatten() {
                let Some(tid) =
                    t.file_name().to_str().and_then(|s| s.parse().ok())
                else {
                    continue;
                };
                let read = |f: &str| {
                    std::fs::read_to_string(t.path().join(f))
                        .unwrap_or_default()
                };
                let comm = read("comm");
                let sched: Vec<u64> = read("schedstat")
                    .split_whitespace()
                    .filter_map(|v| v.parse().ok())
                    .collect();
                let status = read("status");
                let c = Counters {
                    cpu_ns: sched.first().copied().unwrap_or(0),
                    wait_ns: sched.get(1).copied().unwrap_or(0),
                    vcsw: field(&status, "voluntary_ctxt_switches"),
                    ivcsw: field(&status, "nonvoluntary_ctxt_switches"),
                    migrations: field(&read("sched"), "se.nr_migrations"),
                };
                out.insert(tid, (group(&comm), c));
            }
        }
        Snapshot(out)
    }

    /// Per group, the counters' growth since `start` (a thread started in
    /// between counts from zero): `{group: {threads, cpu_s, runqueue_wait_s,
    /// vcsw, ivcsw, migrations}}`.
    pub fn since(&self, start: &Snapshot) -> Value {
        let mut groups: BTreeMap<String, (u64, Counters)> = BTreeMap::new();
        for (tid, (name, end)) in &self.0 {
            let begin = start.0.get(tid).map(|(_, c)| *c).unwrap_or_default();
            let g = groups.entry(name.clone()).or_default();
            g.0 += 1;
            g.1.cpu_ns += end.cpu_ns.saturating_sub(begin.cpu_ns);
            g.1.wait_ns += end.wait_ns.saturating_sub(begin.wait_ns);
            g.1.vcsw += end.vcsw.saturating_sub(begin.vcsw);
            g.1.ivcsw += end.ivcsw.saturating_sub(begin.ivcsw);
            g.1.migrations += end.migrations.saturating_sub(begin.migrations);
        }
        groups
            .into_iter()
            .filter(|(_, (_, c))| c.cpu_ns > 0 || c.vcsw > 0)
            .map(|(name, (n, c))| {
                (
                    name,
                    json!({
                        "threads": n,
                        "cpu_s": c.cpu_ns as f64 / 1e9,
                        "runqueue_wait_s": c.wait_ns as f64 / 1e9,
                        "vcsw": c.vcsw,
                        "ivcsw": c.ivcsw,
                        "migrations": c.migrations,
                    }),
                )
            })
            .collect::<serde_json::Map<String, Value>>()
            .into()
    }
}

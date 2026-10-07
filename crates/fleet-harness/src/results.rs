//! Run results (design sections 4.5 and 6).
//!
//! A result records what was achieved, not just pass or fail. Budgets are
//! evaluated only when they are owner inputs; a placeholder budget is
//! reported as not evaluated. `claims_allowed` is true only for a
//! qualification-class run whose inputs are all owner inputs; exploratory
//! results live under `<output>/exploratory/` and never support a capacity
//! claim.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::Serialize;

use crate::config::{Param, Provenance, RunClass, RunConfig};
use crate::driver::CountersSnapshot;
use crate::ledger::Completeness;
use crate::measure::Aggregate;
use crate::stats::{Recovery, Summary};
use crate::verify::StreamReport;

#[derive(Debug, Clone, Default, Serialize)]
pub struct Environment {
    pub hostname: Option<String>,
    pub kernel: Option<String>,
    pub cpu_model: Option<String>,
    pub cpus: usize,
    pub memory_bytes: Option<u64>,
    /// MySQL settings per server (design R11).
    pub mysql: BTreeMap<String, BTreeMap<String, String>>,
}

impl Environment {
    pub fn local() -> Environment {
        let read = |p: &str| {
            std::fs::read_to_string(p)
                .ok()
                .map(|s| s.trim().to_string())
        };
        let cpuinfo = read("/proc/cpuinfo").unwrap_or_default();
        let meminfo = read("/proc/meminfo").unwrap_or_default();
        Environment {
            hostname: read("/proc/sys/kernel/hostname"),
            kernel: read("/proc/sys/kernel/osrelease"),
            cpu_model: cpuinfo
                .lines()
                .find_map(|l| l.strip_prefix("model name"))
                .map(|v| v.trim_start_matches([' ', '\t', ':']).to_string()),
            cpus: std::thread::available_parallelism().map_or(0, |n| n.get()),
            memory_bytes: meminfo
                .lines()
                .find_map(|l| l.strip_prefix("MemTotal:"))
                .and_then(|v| {
                    v.trim().trim_end_matches(" kB").trim().parse::<u64>().ok()
                })
                .map(|kb| kb * 1024),
            mysql: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct StepOutcome {
    pub at_secs: u64,
    pub action: crate::scenario::Action,
    /// Why the action did not run (unconfigured hook or proxy).
    pub not_run: Option<String>,
    pub error: Option<String>,
    pub duration_secs: Option<f64>,
    /// Shutdown time, for process restarts.
    pub shutdown_secs: Option<f64>,
    pub recovery: Option<Recovery>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BudgetCheck {
    pub budget: String,
    pub limit: Option<f64>,
    pub achieved: Option<f64>,
    /// `None` when not evaluated (placeholder or nothing measured).
    pub passed: Option<bool>,
    pub note: Option<String>,
}

/// Compare `achieved` with an owner budget (at most `limit`).
pub fn check<T: Copy + Into<f64>>(
    name: &str,
    budget: &Option<Param<T>>,
    achieved: Option<f64>,
) -> BudgetCheck {
    let (limit, note) = match budget {
        None => (None, Some("no budget".to_string())),
        Some(p) if p.provenance != Provenance::Owner => (
            Some(p.value.into()),
            Some("placeholder budget: not evaluated".to_string()),
        ),
        Some(p) => (Some(p.value.into()), None),
    };
    let passed = match (budget, limit, achieved) {
        (Some(p), Some(l), Some(a)) if p.provenance == Provenance::Owner => {
            Some(a <= l)
        }
        _ => None,
    };
    BudgetCheck {
        budget: name.to_string(),
        limit,
        achieved,
        passed,
        note: note.or_else(|| {
            achieved
                .is_none()
                .then(|| "not measured in this run".into())
        }),
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Verdict {
    /// Completeness, ordering within partitions, schema probes and (when
    /// expected) primary-key keys.
    pub correctness_ok: bool,
    /// Every owner budget evaluated passed; `None` if none was evaluated.
    pub budgets_ok: Option<bool>,
    /// Actions of the plan that could not run.
    pub actions_not_run: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunResult {
    pub run_id: String,
    pub class: RunClass,
    pub claims_allowed: bool,
    pub scenario: String,
    pub sources: u32,
    pub repetition: u32,
    pub started_at: String,
    pub ended_at: String,
    pub harness_revision: String,
    pub placeholders: Vec<String>,
    pub config: RunConfig,
    pub environment: Environment,
    pub fixture_tables: BTreeMap<String, u64>,
    pub pipelines_running_secs: Option<f64>,
    pub measured_secs: f64,
    pub driver: BTreeMap<String, CountersSnapshot>,
    /// Committed operations per second per server over the measured window.
    pub achieved_ops_per_sec: BTreeMap<String, f64>,
    pub achieved_ops_per_sec_total: f64,
    pub stream: StreamReport,
    pub lag_ms: BTreeMap<String, Summary>,
    pub resources: Aggregate,
    pub steps: Vec<StepOutcome>,
    pub completeness: Completeness,
    pub budgets: Vec<BudgetCheck>,
    pub verdict: Verdict,
}

impl RunResult {
    pub fn evaluate(&mut self) {
        let b = &self.config.budgets;
        let lag_p99 = self
            .lag_ms
            .values()
            .filter_map(|s| s.p99)
            .max()
            .map(|ms| ms as f64 / 1_000.0);
        let worst = |f: fn(&Recovery) -> Option<f64>| -> Option<f64> {
            let all: Vec<Option<f64>> = self
                .steps
                .iter()
                .filter_map(|s| s.recovery.as_ref())
                .map(f)
                .collect();
            if all.is_empty() {
                None
            } else if all.iter().any(Option::is_none) {
                Some(f64::INFINITY) // a source never recovered
            } else {
                all.into_iter().flatten().reduce(f64::max)
            }
        };
        let shutdown = self
            .steps
            .iter()
            .filter_map(|s| s.shutdown_secs)
            .reduce(f64::max);
        let connections = self
            .resources
            .connections_max
            .values()
            .copied()
            .max()
            .map(|n| n as f64);
        self.budgets = vec![
            check("lag_p99_secs", &b.lag_p99_secs, lag_p99),
            check(
                "memory_bytes",
                &b.memory_bytes.as_ref().map(|p| Param {
                    value: p.value as f64,
                    provenance: p.provenance,
                }),
                self.resources.memory_peak_bytes.map(|v| v as f64),
            ),
            check("cpu_cores", &b.cpu_cores, self.resources.cpu_cores_mean),
            check(
                "recovery_p50_secs",
                &b.recovery_p50_secs,
                worst(|r| r.p50_secs),
            ),
            check(
                "recovery_p90_secs",
                &b.recovery_p90_secs,
                worst(|r| r.p90_secs),
            ),
            check(
                "recovery_p100_secs",
                &b.recovery_p100_secs,
                worst(|r| r.p100_secs),
            ),
            check("shutdown_secs", &b.shutdown_secs, shutdown),
            check(
                "connections_per_server",
                &b.connections_per_server.as_ref().map(|p| Param {
                    value: p.value as f64,
                    provenance: p.provenance,
                }),
                connections,
            ),
        ];
        let evaluated: Vec<bool> =
            self.budgets.iter().filter_map(|c| c.passed).collect();
        let s = &self.stream;
        self.verdict = Verdict {
            correctness_ok: self.completeness.ok()
                && s.probes_failed == 0
                && s.key_mismatches == 0
                && s.decode_errors == 0,
            budgets_ok: (!evaluated.is_empty())
                .then(|| evaluated.iter().all(|&p| p)),
            actions_not_run: self
                .steps
                .iter()
                .filter(|s| s.not_run.is_some())
                .count(),
        };
    }

    pub fn dir(output: &str, class: RunClass, run_id: &str) -> PathBuf {
        Path::new(output).join(class.dir()).join(run_id)
    }

    pub fn write(&self, dir: &Path) -> Result<()> {
        std::fs::create_dir_all(dir)?;
        std::fs::write(
            dir.join("result.json"),
            serde_json::to_vec_pretty(self)?,
        )?;
        Ok(())
    }
}

/// One source count of a sweep.
#[derive(Debug, Clone, Serialize)]
pub struct SweepPoint {
    pub sources: u32,
    pub runs: Vec<String>,
    pub correctness_ok: bool,
    pub budgets_ok: Option<bool>,
    pub achieved_ops_per_sec_total: Vec<f64>,
    pub lag_p99_ms_max: Vec<Option<u64>>,
    pub memory_peak_bytes: Vec<Option<u64>>,
    pub cpu_cores_mean: Vec<Option<f64>>,
    pub connections_max: Vec<Option<u64>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Sweep {
    pub class: RunClass,
    pub scenario: String,
    pub points: Vec<SweepPoint>,
    /// The largest source count whose every run kept correctness and met
    /// every owner budget; `None` while budgets are placeholders (the
    /// harness never declares a safe count from placeholders).
    pub largest_passing_sources: Option<u32>,
    pub note: String,
}

impl Sweep {
    pub fn summarize(
        class: RunClass,
        scenario: &str,
        points: Vec<SweepPoint>,
    ) -> Sweep {
        let all_budgets_owner = points.iter().all(|p| p.budgets_ok.is_some());
        let largest = if all_budgets_owner {
            points
                .iter()
                .take_while(|p| p.correctness_ok && p.budgets_ok == Some(true))
                .map(|p| p.sources)
                .last()
        } else {
            None
        };
        let note = if all_budgets_owner {
            "budgets are owner inputs: largest passing count evaluated".into()
        } else {
            "budgets are placeholders or not measured: no safe source count is declared; \
             compare the achieved values across points"
                .into()
        };
        Sweep {
            class,
            scenario: scenario.to_string(),
            points,
            largest_passing_sources: largest,
            note,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder_budgets_are_reported_not_evaluated() {
        let c = check("lag", &Some(Param::placeholder(5.0)), Some(9.0));
        assert_eq!(c.passed, None);
        assert!(c.note.unwrap().contains("placeholder"));
        let c = check("lag", &Some(Param::owner(5.0)), Some(9.0));
        assert_eq!(c.passed, Some(false));
        let c = check("lag", &Some(Param::owner(5.0)), None);
        assert_eq!(c.passed, None);
        let c = check::<f64>("lag", &None, Some(1.0));
        assert_eq!(c.passed, None);
    }

    fn point(sources: u32, correct: bool, budgets: Option<bool>) -> SweepPoint {
        SweepPoint {
            sources,
            runs: vec![],
            correctness_ok: correct,
            budgets_ok: budgets,
            achieved_ops_per_sec_total: vec![],
            lag_p99_ms_max: vec![],
            memory_peak_bytes: vec![],
            cpu_cores_mean: vec![],
            connections_max: vec![],
        }
    }

    #[test]
    fn a_safe_count_is_declared_only_from_owner_budgets() {
        let s = Sweep::summarize(
            RunClass::Qualification,
            "S1",
            vec![
                point(1, true, Some(true)),
                point(25, true, Some(true)),
                point(50, true, Some(false)),
                point(100, true, Some(true)),
            ],
        );
        assert_eq!(
            s.largest_passing_sources,
            Some(25),
            "stops at the first failing count"
        );
        let s = Sweep::summarize(
            RunClass::Exploratory,
            "S1",
            vec![point(1, true, None), point(25, true, None)],
        );
        assert_eq!(s.largest_passing_sources, None);
        assert!(s.note.contains("no safe source count"));
        let s = Sweep::summarize(
            RunClass::Exploratory,
            "S1",
            vec![point(1, false, Some(true))],
        );
        assert_eq!(s.largest_passing_sources, None, "correctness first");
    }
}

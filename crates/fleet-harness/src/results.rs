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
    /// Only a failure of the action: never a success note.
    pub error: Option<String>,
    /// What a successful action reported (background actions).
    pub note: Option<String>,
    pub duration_secs: Option<f64>,
    /// Shutdown time, for process restarts.
    pub shutdown_secs: Option<f64>,
    pub recovery: Option<Recovery>,
}

impl StepOutcome {
    /// Record a background action's end: `Ok` is a note, only `Err` is an
    /// error.
    pub fn settle(&mut self, end: std::result::Result<String, String>) {
        match end {
            Ok(note) if note.is_empty() => {}
            Ok(note) => self.note = Some(note),
            Err(e) => self.error = Some(e),
        }
    }
}

/// Whether `budget` applies to a run whose plan produced `steps`: recovery
/// budgets need a disrupting action, the shutdown budget a process restart;
/// the others apply to every run.
pub fn applicable(budget: &str, steps: &[StepOutcome]) -> bool {
    match budget {
        "recovery_p50_secs" | "recovery_p90_secs" | "recovery_p100_secs" => {
            steps.iter().any(|s| s.action.disrupts())
        }
        "shutdown_secs" => steps.iter().any(|s| {
            matches!(s.action, crate::scenario::Action::ProcessRestart)
        }),
        _ => true,
    }
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

/// Compare `achieved` with an owner budget (at most `limit`). In a strict
/// (qualification) evaluation, an applicable budget that is missing, or
/// whose value was not measured, fails: incomplete evidence never passes.
pub fn check<T: Copy + Into<f64>>(
    name: &str,
    budget: &Option<Param<T>>,
    achieved: Option<f64>,
    applicable: bool,
    strict: bool,
) -> BudgetCheck {
    let limit = budget.as_ref().map(|p| p.value.into());
    let owner = budget
        .as_ref()
        .is_some_and(|p| p.provenance == Provenance::Owner);
    let (passed, note) = if !applicable {
        (None, Some("not applicable to this scenario".to_string()))
    } else if budget.is_none() {
        (strict.then_some(false), Some("no budget".to_string()))
    } else if !owner {
        (
            strict.then_some(false),
            Some("placeholder budget: not evaluated".to_string()),
        )
    } else {
        match (limit, achieved) {
            (Some(l), Some(a)) => (Some(a <= l), None),
            _ => (
                strict.then_some(false),
                Some("applicable but not measured in this run".to_string()),
            ),
        }
    };
    BudgetCheck {
        budget: name.to_string(),
        limit,
        achieved,
        passed,
        note,
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Verdict {
    /// Completeness, ordering within partitions, schema probes and (when
    /// expected) primary-key keys.
    pub correctness_ok: bool,
    /// Every action of the plan ran without error.
    pub plan_executed: bool,
    pub actions_not_run: usize,
    pub action_errors: usize,
    /// Uncertain transactions within the owner's allowance (none by
    /// default), and no driver writer ended in error.
    pub uncertainty_ok: bool,
    /// Every server committed at least the owner's minimum share of the
    /// configured workload; `None` without an owner minimum.
    pub workload_ok: Option<bool>,
    /// Every owner budget evaluated passed; `None` if none was evaluated.
    pub budgets_ok: Option<bool>,
    /// What a sweep counts. Exploratory: correctness. Qualification:
    /// correctness, the whole plan executed, uncertainty within the
    /// allowance and the workload achieved (budgets are judged by the
    /// sweep).
    pub repetition_ok: bool,
}

/// The configured workload against what was committed, per server, over
/// the measured window.
#[derive(Debug, Clone, Serialize)]
pub struct Workload {
    pub target_ops: f64,
    pub committed_ops: u64,
    pub ratio: Option<f64>,
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
    pub workload: BTreeMap<String, Workload>,
    /// Writers that ended with an error (their remaining workload was not
    /// exercised).
    pub writer_errors: Vec<String>,
    pub sort: BTreeMap<String, crate::ledger::SortStats>,
    pub stream: StreamReport,
    pub lag_ms: BTreeMap<String, Summary>,
    pub resources: Aggregate,
    /// Steps in the scenario's plan; every one must have an outcome.
    pub planned_steps: usize,
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
        let strict = self.class == RunClass::Qualification;
        let steps = &self.steps;
        let applies = |name: &str| applicable(name, steps);
        self.budgets = vec![
            check(
                "lag_p99_secs",
                &b.lag_p99_secs,
                lag_p99,
                applies("lag_p99_secs"),
                strict,
            ),
            check(
                "memory_bytes",
                &b.memory_bytes.as_ref().map(|p| Param {
                    value: p.value as f64,
                    provenance: p.provenance,
                }),
                self.resources.memory_peak_bytes.map(|v| v as f64),
                applies("memory_bytes"),
                strict,
            ),
            check(
                "cpu_cores",
                &b.cpu_cores,
                self.resources.cpu_cores_mean,
                applies("cpu_cores"),
                strict,
            ),
            check(
                "recovery_p50_secs",
                &b.recovery_p50_secs,
                worst(|r| r.p50_secs),
                applies("recovery_p50_secs"),
                strict,
            ),
            check(
                "recovery_p90_secs",
                &b.recovery_p90_secs,
                worst(|r| r.p90_secs),
                applies("recovery_p90_secs"),
                strict,
            ),
            check(
                "recovery_p100_secs",
                &b.recovery_p100_secs,
                worst(|r| r.p100_secs),
                applies("recovery_p100_secs"),
                strict,
            ),
            check(
                "shutdown_secs",
                &b.shutdown_secs,
                shutdown,
                applies("shutdown_secs"),
                strict,
            ),
            check(
                "connections_per_server",
                &b.connections_per_server.as_ref().map(|p| Param {
                    value: p.value as f64,
                    provenance: p.provenance,
                }),
                connections,
                applies("connections_per_server"),
                strict,
            ),
        ];
        let evaluated: Vec<bool> =
            self.budgets.iter().filter_map(|c| c.passed).collect();
        let s = &self.stream;
        let correctness_ok = self.completeness.ok()
            && s.probes_failed == 0
            && s.key_mismatches == 0
            && s.decode_errors == 0;
        let actions_not_run =
            self.steps.iter().filter(|s| s.not_run.is_some()).count();
        let action_errors =
            self.steps.iter().filter(|s| s.error.is_some()).count();
        let plan_executed = actions_not_run == 0
            && action_errors == 0
            && self.steps.len() == self.planned_steps;
        let uncertain: u64 =
            self.driver.values().map(|c| c.uncertain_txns).sum();
        let uncertainty_ok = uncertain
            <= self.config.policy.uncertain_allowance()
            && self.writer_errors.is_empty();
        let workload_ok = match &self.config.policy.min_achieved_ratio {
            Some(p) if p.provenance == Provenance::Owner => Some(
                !self.workload.is_empty()
                    && self
                        .workload
                        .values()
                        .all(|w| w.ratio.is_some_and(|r| r >= p.value)),
            ),
            _ => None,
        };
        let repetition_ok = match self.class {
            RunClass::Exploratory => correctness_ok,
            RunClass::Qualification => {
                correctness_ok
                    && plan_executed
                    && uncertainty_ok
                    && workload_ok == Some(true)
            }
        };
        self.verdict = Verdict {
            correctness_ok,
            plan_executed,
            actions_not_run,
            action_errors,
            uncertainty_ok,
            workload_ok,
            budgets_ok: (!evaluated.is_empty())
                .then(|| evaluated.iter().all(|&p| p)),
            repetition_ok,
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
        let c = check(
            "lag",
            &Some(Param::placeholder(5.0)),
            Some(9.0),
            true,
            false,
        );
        assert_eq!(c.passed, None);
        assert!(c.note.unwrap().contains("placeholder"));
        let c = check("lag", &Some(Param::owner(5.0)), Some(9.0), true, false);
        assert_eq!(c.passed, Some(false));
        let c = check("lag", &Some(Param::owner(5.0)), None, true, false);
        assert_eq!(c.passed, None);
        let c = check::<f64>("lag", &None, Some(1.0), true, false);
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

    use crate::config::tests::EXAMPLE;
    use crate::scenario::Action;

    fn result(class: RunClass) -> RunResult {
        let mut config: RunConfig = serde_yaml::from_str(EXAMPLE).unwrap();
        config.class = class;
        RunResult {
            run_id: "r".into(),
            class,
            claims_allowed: false,
            scenario: "S7".into(),
            sources: 1,
            repetition: 1,
            started_at: String::new(),
            ended_at: String::new(),
            harness_revision: String::new(),
            placeholders: vec![],
            config,
            environment: Environment::default(),
            fixture_tables: BTreeMap::new(),
            pipelines_running_secs: None,
            measured_secs: 60.0,
            driver: BTreeMap::from([(
                "c01".to_string(),
                CountersSnapshot::default(),
            )]),
            achieved_ops_per_sec: BTreeMap::new(),
            achieved_ops_per_sec_total: 0.0,
            workload: BTreeMap::from([(
                "c01".to_string(),
                Workload {
                    target_ops: 1_000.0,
                    committed_ops: 1_000,
                    ratio: Some(1.0),
                },
            )]),
            writer_errors: vec![],
            sort: BTreeMap::new(),
            stream: StreamReport::default(),
            lag_ms: BTreeMap::new(),
            resources: Aggregate::default(),
            planned_steps: 0,
            steps: vec![],
            completeness: Completeness::default(),
            budgets: vec![],
            verdict: Verdict {
                correctness_ok: false,
                plan_executed: false,
                actions_not_run: 0,
                action_errors: 0,
                uncertainty_ok: false,
                workload_ok: None,
                budgets_ok: None,
                repetition_ok: false,
            },
        }
    }

    fn step(not_run: Option<&str>, error: Option<&str>) -> StepOutcome {
        StepOutcome {
            at_secs: 0,
            action: Action::Failover,
            not_run: not_run.map(String::from),
            error: error.map(String::from),
            note: None,
            duration_secs: None,
            shutdown_secs: None,
            recovery: None,
        }
    }

    fn qualified(mut r: RunResult) -> RunResult {
        r.config.policy.min_achieved_ratio = Some(Param::owner(0.95));
        r
    }

    #[test]
    fn a_qualification_repetition_fails_when_an_action_did_not_run_or_failed() {
        let mut r = qualified(result(RunClass::Qualification));
        r.evaluate();
        assert!(r.verdict.repetition_ok, "baseline: {:?}", r.verdict);
        for s in [
            step(Some("hooks not configured: failover"), None),
            step(None, Some("hook failover failed")),
        ] {
            let mut q = qualified(result(RunClass::Qualification));
            q.steps = vec![s.clone()];
            q.planned_steps = 1;
            q.evaluate();
            assert!(
                !q.verdict.plan_executed && !q.verdict.repetition_ok,
                "{:?}",
                q.verdict
            );
            let mut e = result(RunClass::Exploratory);
            e.steps = vec![s];
            e.planned_steps = 1;
            e.evaluate();
            assert!(
                e.verdict.repetition_ok,
                "exploratory runs record, not fail"
            );
            assert!(!e.verdict.plan_executed);
        }
    }

    #[test]
    fn uncertainty_fails_a_qualification_unless_the_owner_allows_it() {
        let mut r = qualified(result(RunClass::Qualification));
        r.driver.get_mut("c01").unwrap().uncertain_txns = 1;
        r.evaluate();
        assert!(!r.verdict.uncertainty_ok && !r.verdict.repetition_ok);
        r.config.policy.max_uncertain_transactions = Some(Param::owner(1));
        r.evaluate();
        assert!(r.verdict.uncertainty_ok && r.verdict.repetition_ok);
        r.writer_errors = vec!["writer connection to c01".into()];
        r.evaluate();
        assert!(
            !r.verdict.uncertainty_ok,
            "a writer that stopped early fails"
        );
    }

    #[test]
    fn a_workload_shortfall_fails_against_an_owner_minimum() {
        let mut r = qualified(result(RunClass::Qualification));
        r.workload.get_mut("c01").unwrap().ratio = Some(0.5);
        r.evaluate();
        assert_eq!(r.verdict.workload_ok, Some(false));
        assert!(!r.verdict.repetition_ok);
        let mut p = result(RunClass::Qualification);
        p.config.policy.min_achieved_ratio = Some(Param::placeholder(0.95));
        p.evaluate();
        assert_eq!(
            p.verdict.workload_ok, None,
            "a placeholder minimum is not evaluated"
        );
        assert!(
            !p.verdict.repetition_ok,
            "and a qualification cannot pass without one"
        );
    }

    #[test]
    fn a_successful_background_action_with_a_note_is_not_an_error() {
        let mut ok = step(None, None);
        ok.action = Action::Migration;
        ok.settle(Ok("migration statements with probes: 6".into()));
        assert_eq!(ok.error, None);
        assert!(ok.note.is_some());
        let mut r = qualified(result(RunClass::Qualification));
        r.steps = vec![ok];
        r.planned_steps = 1;
        r.evaluate();
        assert!(
            r.verdict.plan_executed && r.verdict.repetition_ok,
            "{:?}",
            r.verdict
        );
        let mut failed = step(None, None);
        failed.settle(Err("migrate cust_00001.t000".into()));
        assert!(failed.error.is_some() && failed.note.is_none());
    }

    #[test]
    fn a_planned_step_without_an_outcome_fails_the_plan() {
        let mut r = qualified(result(RunClass::Qualification));
        r.planned_steps = 1;
        r.evaluate();
        assert!(!r.verdict.plan_executed && !r.verdict.repetition_ok);
    }

    #[test]
    fn an_applicable_unmeasured_owner_budget_fails_a_qualification() {
        let owner_budgets = |r: &mut RunResult| {
            let b = &mut r.config.budgets;
            b.memory_bytes = Some(Param::owner(1 << 40));
            b.recovery_p100_secs = Some(Param::owner(60.0));
        };
        // Memory measured and within budget; a failover ran, but no
        // recovery was measured.
        let mut r = qualified(result(RunClass::Qualification));
        owner_budgets(&mut r);
        r.resources.memory_peak_bytes = Some(1 << 30);
        r.steps = vec![step(None, None)];
        r.planned_steps = 1;
        r.evaluate();
        let get = |r: &RunResult, n: &str| {
            r.budgets.iter().find(|c| c.budget == n).unwrap().clone()
        };
        assert_eq!(get(&r, "memory_bytes").passed, Some(true));
        assert_eq!(get(&r, "recovery_p100_secs").passed, Some(false));
        assert_eq!(r.verdict.budgets_ok, Some(false));
        // Without a disrupting action, recovery budgets do not apply.
        let mut quiet = qualified(result(RunClass::Qualification));
        owner_budgets(&mut quiet);
        quiet.resources.memory_peak_bytes = Some(1 << 30);
        quiet.evaluate();
        assert_eq!(get(&quiet, "recovery_p100_secs").passed, None);
        assert_eq!(
            get(&quiet, "shutdown_secs").passed,
            None,
            "no process restart"
        );
        // Exploratory: an unmeasured budget is reported, not failed.
        let mut e = result(RunClass::Exploratory);
        owner_budgets(&mut e);
        e.steps = vec![step(None, None)];
        e.planned_steps = 1;
        e.evaluate();
        assert_eq!(get(&e, "recovery_p100_secs").passed, None);
    }
}

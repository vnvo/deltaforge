//! Per-table metric labels, bounded (`docs/src/observability.md`).
//!
//! By default no metric carries a table label, so a pipeline's series count
//! does not depend on how many tables it captures. A pipeline that enables
//! `metrics.per_table` admits its first `max_tables` distinct tables to
//! exact series (`table="<name>", table_scope="exact"`) for the life of the
//! process; every other table is reported under one overflow series
//! (`table="__other__", table_scope="overflow"`). The closed `table_scope`
//! label keeps a real table named `__other__` apart from the overflow.
//!
//! Admission is never undone while the pipeline runs, so a table's counter
//! history never moves between its exact series and the overflow series.
//! Operators see truncation through table-free metrics: the admitted count,
//! observations routed to the overflow, and an estimate of how many distinct
//! tables overflowed.

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;

use metrics::{Label, counter, gauge};

/// The `table` value of the overflow series.
pub const OVERFLOW_TABLE: &str = "__other__";
pub const SCOPE_EXACT: &str = "exact";
pub const SCOPE_OVERFLOW: &str = "overflow";

/// Tables admitted to exact series (`{pipeline}`).
pub const METRIC_ADMITTED: &str = "deltaforge_metrics_tables_admitted";
/// Observations reported under the overflow series (`{pipeline}`).
pub const METRIC_OVERFLOW: &str = "deltaforge_metrics_table_overflow_total";
/// Estimated distinct tables reported under the overflow (`{pipeline}`).
pub const METRIC_OVERFLOWED: &str = "deltaforge_metrics_tables_overflowed";

/// A pipeline's per-table detail settings (present only when enabled).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PerTablePolicy {
    pub max_tables: usize,
    pub lag_idle: Duration,
}

/// The table labels of one observation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TableLabel {
    Exact(Arc<str>),
    Overflow,
}

impl TableLabel {
    pub fn table(&self) -> &str {
        match self {
            TableLabel::Exact(t) => t,
            TableLabel::Overflow => OVERFLOW_TABLE,
        }
    }

    pub fn scope(&self) -> &'static str {
        match self {
            TableLabel::Exact(_) => SCOPE_EXACT,
            TableLabel::Overflow => SCOPE_OVERFLOW,
        }
    }

    /// `table` and `table_scope`.
    pub fn labels(&self) -> [Label; 2] {
        [
            Label::new("table", self.table().to_string()),
            Label::new("table_scope", self.scope()),
        ]
    }
}

/// `base` plus the table labels of `table`, if any.
pub fn with_table(
    mut base: Vec<Label>,
    table: Option<&TableLabel>,
) -> Vec<Label> {
    if let Some(t) = table {
        base.extend(t.labels());
    }
    base
}

/// One pipeline's per-table label policy and admissions.
///
/// One instance per pipeline name lives for the process (see [`register`]):
/// the recorder can never remove a series, so the admissions that created
/// exact series are never discarded, and `max_tables` is fixed once any
/// table series exists. Enabling, disabling and the lag idle time can
/// change; they never create series beyond `max_tables + 1`.
#[derive(Debug)]
pub struct TableMetrics {
    pipeline: Arc<str>,
    enabled: AtomicBool,
    max_tables: AtomicUsize,
    lag_idle_ms: AtomicU64,
    admitted: RwLock<HashSet<Arc<str>>>,
    /// Allocated at the first overflow.
    overflowed: Mutex<Option<Box<DistinctEstimate>>>,
}

impl TableMetrics {
    pub fn new(pipeline: &str, policy: Option<PerTablePolicy>) -> Self {
        let tm = Self {
            pipeline: Arc::from(pipeline),
            enabled: AtomicBool::new(false),
            max_tables: AtomicUsize::new(0),
            lag_idle_ms: AtomicU64::new(0),
            admitted: RwLock::new(HashSet::new()),
            overflowed: Mutex::new(None),
        };
        tm.apply(policy);
        tm
    }

    /// Per-table detail off: every [`label`](Self::label) is `None`.
    pub fn disabled(pipeline: &str) -> Self {
        Self::new(pipeline, None)
    }

    pub fn pipeline(&self) -> &str {
        &self.pipeline
    }

    /// The current policy (`None` while per-table detail is off).
    pub fn policy(&self) -> Option<PerTablePolicy> {
        self.enabled
            .load(Ordering::Acquire)
            .then(|| PerTablePolicy {
                max_tables: self.max_tables.load(Ordering::Acquire),
                lag_idle: Duration::from_millis(
                    self.lag_idle_ms.load(Ordering::Acquire),
                ),
            })
    }

    /// Whether any table-labelled series was created under this name.
    pub fn has_table_series(&self) -> bool {
        !self.admitted.read().expect("admitted lock").is_empty()
            || self.overflowed.lock().expect("overflow lock").is_some()
    }

    /// Whether `policy` can apply without exceeding the series already
    /// created: once table series exist, `max_tables` is fixed.
    fn compatible(&self, policy: Option<PerTablePolicy>) -> Result<(), String> {
        let Some(p) = policy else { return Ok(()) };
        let fixed = self.max_tables.load(Ordering::Acquire);
        if p.max_tables != fixed && self.has_table_series() {
            return Err(format!(
                "metrics.per_table.max_tables is fixed at {fixed} for pipeline \
                 '{}' for the life of this DeltaForge process, because its \
                 per-table series already exist (they cannot be removed); keep \
                 {fixed}, or restart DeltaForge to change it",
                self.pipeline
            ));
        }
        Ok(())
    }

    /// Apply `policy` (callers check [`compatible`](Self::compatible)).
    fn apply(&self, policy: Option<PerTablePolicy>) {
        // Under the admissions lock, so no admission races a cap change.
        let _admitted = self.admitted.write().expect("admitted lock");
        if let Some(p) = policy {
            self.max_tables.store(p.max_tables, Ordering::Release);
            self.lag_idle_ms
                .store(p.lag_idle.as_millis() as u64, Ordering::Release);
        }
        self.enabled.store(policy.is_some(), Ordering::Release);
    }

    /// The table labels for an observation of `table`: `None` when per-table
    /// detail is off, the exact label for an admitted table (admitting it
    /// while there is room), and the overflow label otherwise.
    pub fn label(&self, table: &str) -> Option<TableLabel> {
        if !self.enabled.load(Ordering::Acquire) {
            return None;
        }
        {
            let admitted = self.admitted.read().expect("admitted lock");
            if let Some(t) = admitted.get(table) {
                return Some(TableLabel::Exact(t.clone()));
            }
            if admitted.len() >= self.max_tables.load(Ordering::Acquire) {
                drop(admitted);
                self.overflow(table);
                return Some(TableLabel::Overflow);
            }
        }
        let mut admitted = self.admitted.write().expect("admitted lock");
        if let Some(t) = admitted.get(table) {
            return Some(TableLabel::Exact(t.clone()));
        }
        if admitted.len() >= self.max_tables.load(Ordering::Acquire) {
            drop(admitted);
            self.overflow(table);
            return Some(TableLabel::Overflow);
        }
        let t: Arc<str> = Arc::from(table);
        admitted.insert(t.clone());
        gauge!(METRIC_ADMITTED, "pipeline" => self.pipeline.to_string())
            .set(admitted.len() as f64);
        Some(TableLabel::Exact(t))
    }

    /// Tables admitted to exact series.
    pub fn admitted(&self) -> usize {
        self.admitted.read().expect("admitted lock").len()
    }

    /// Estimated distinct tables reported under the overflow.
    pub fn overflowed_estimate(&self) -> u64 {
        self.overflowed
            .lock()
            .expect("overflow lock")
            .as_ref()
            .map_or(0, |e| e.estimate())
    }

    fn overflow(&self, table: &str) {
        let pipeline = self.pipeline.to_string();
        counter!(METRIC_OVERFLOW, "pipeline" => pipeline.clone()).increment(1);
        let mut est = self.overflowed.lock().expect("overflow lock");
        let est = est.get_or_insert_with(|| Box::new(DistinctEstimate::new()));
        if est.insert(table) {
            gauge!(METRIC_OVERFLOWED, "pipeline" => pipeline)
                .set(est.estimate() as f64);
        }
    }
}

/// HyperLogLog distinct-count estimate in a fixed 1 KiB (2^10 registers):
/// standard error about 3.3%, near-exact below a few thousand values
/// (linear counting). Counting overflowed tables exactly would need memory
/// per table, which is what per-table bounding exists to avoid.
#[derive(Debug)]
struct DistinctEstimate {
    registers: Box<[u8; Self::M]>,
}

impl DistinctEstimate {
    const P: u32 = 10;
    const M: usize = 1 << Self::P;

    fn new() -> Self {
        Self {
            registers: Box::new([0; Self::M]),
        }
    }

    /// Record `value`; `true` when the estimate may have changed.
    fn insert(&mut self, value: &str) -> bool {
        let mut h = std::hash::DefaultHasher::new();
        value.hash(&mut h);
        let hash = h.finish();
        let idx = (hash >> (64 - Self::P)) as usize;
        let rest = (hash << Self::P) | (1 << (Self::P - 1));
        let rank = rest.leading_zeros() as u8 + 1;
        if rank > self.registers[idx] {
            self.registers[idx] = rank;
            true
        } else {
            false
        }
    }

    fn estimate(&self) -> u64 {
        let m = Self::M as f64;
        let zeros = self.registers.iter().filter(|&&r| r == 0).count();
        let sum: f64 =
            self.registers.iter().map(|&r| 2f64.powi(-(r as i32))).sum();
        let raw = 0.7213 / (1.0 + 1.079 / m) * m * m / sum;
        let est = if raw <= 2.5 * m && zeros > 0 {
            m * (m / zeros as f64).ln()
        } else {
            raw
        };
        est.round() as u64
    }
}

fn registry() -> &'static RwLock<HashMap<String, Arc<TableMetrics>>> {
    static REGISTRY: OnceLock<RwLock<HashMap<String, Arc<TableMetrics>>>> =
        OnceLock::new();
    REGISTRY.get_or_init(Default::default)
}

/// Whether `pipeline` can take `policy` now (create and patch check this
/// before stopping or starting anything).
pub fn check(
    pipeline: &str,
    policy: Option<PerTablePolicy>,
) -> Result<(), String> {
    match registry()
        .read()
        .expect("table metrics registry")
        .get(pipeline)
    {
        Some(tm) => tm.compatible(policy),
        None => Ok(()),
    }
}

/// A policy registration made for a pipeline start; [`rollback`]
/// (Self::rollback) undoes it when the start fails.
#[must_use = "roll back the registration if the start fails"]
#[derive(Debug)]
pub struct Registration {
    tables: Arc<TableMetrics>,
    /// The policy before this registration; `None` when the entry is new.
    previous: Option<Option<PerTablePolicy>>,
}

impl Registration {
    pub fn tables(&self) -> &Arc<TableMetrics> {
        &self.tables
    }

    /// Undo the registration: restore the previous policy, or remove a new
    /// entry that created no table series (one that did is kept, disabled,
    /// so its admissions still bound the name).
    pub fn rollback(self) {
        let mut reg = registry().write().expect("table metrics registry");
        match self.previous {
            Some(previous) => self.tables.apply(previous),
            None if self.tables.has_table_series() => self.tables.apply(None),
            None => {
                if reg
                    .get(self.tables.pipeline())
                    .is_some_and(|tm| Arc::ptr_eq(tm, &self.tables))
                {
                    reg.remove(self.tables.pipeline());
                }
            }
        }
    }
}

/// Register `pipeline`'s policy for a start. The pipeline name keeps one
/// [`TableMetrics`] for the process: its admissions survive restarts,
/// policy changes and delete/recreate, so the exact series under the name
/// never exceed `max_tables`. Refused when it would change `max_tables`
/// after table series exist.
pub fn register(
    pipeline: &str,
    policy: Option<PerTablePolicy>,
) -> Result<Registration, String> {
    let mut reg = registry().write().expect("table metrics registry");
    if let Some(existing) = reg.get(pipeline) {
        existing.compatible(policy)?;
        let previous = existing.policy();
        existing.apply(policy);
        return Ok(Registration {
            tables: existing.clone(),
            previous: Some(previous),
        });
    }
    let tm = Arc::new(TableMetrics::new(pipeline, policy));
    reg.insert(pipeline.to_string(), tm.clone());
    Ok(Registration {
        tables: tm,
        previous: None,
    })
}

/// `pipeline`'s policy; an unregistered pipeline has per-table detail off.
pub fn for_pipeline(pipeline: &str) -> Arc<TableMetrics> {
    registry()
        .read()
        .expect("table metrics registry")
        .get(pipeline)
        .cloned()
        .unwrap_or_else(|| Arc::new(TableMetrics::disabled(pipeline)))
}

/// `pipeline` was deleted. A name that created no table series is
/// forgotten; one that did is kept, disabled, for the process: its series
/// remain in the recorder, and a recreated pipeline of the same name reuses
/// its admissions rather than creating new exact series. What is kept is
/// bounded by `max_tables` names and a 1 KiB estimate per such name.
pub fn unregister(pipeline: &str) {
    let mut reg = registry().write().expect("table metrics registry");
    if let Some(tm) = reg.get(pipeline) {
        if tm.has_table_series() {
            tm.apply(None);
        } else {
            reg.remove(pipeline);
        }
    }
}

/// Whether `pipeline` has a registry entry.
pub fn is_registered(pipeline: &str) -> bool {
    registry()
        .read()
        .expect("table metrics registry")
        .contains_key(pipeline)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(max_tables: usize) -> Option<PerTablePolicy> {
        Some(PerTablePolicy {
            max_tables,
            lag_idle: Duration::from_secs(300),
        })
    }

    #[test]
    fn disabled_emits_no_table_labels() {
        let tm = TableMetrics::disabled("p");
        assert_eq!(tm.label("db.t"), None);
        assert!(with_table(vec![Label::new("pipeline", "p")], None).len() == 1);
    }

    #[test]
    fn admission_is_capped_and_stable() {
        let tm = TableMetrics::new("p", policy(2));
        assert_eq!(tm.label("a").unwrap().scope(), SCOPE_EXACT);
        assert_eq!(tm.label("b").unwrap().scope(), SCOPE_EXACT);
        assert_eq!(tm.label("c"), Some(TableLabel::Overflow));
        assert_eq!(tm.label("a").unwrap().table(), "a", "still admitted");
        assert_eq!(tm.label("c"), Some(TableLabel::Overflow), "never promoted");
        assert_eq!(tm.admitted(), 2);
        let labels: HashSet<_> = ["a", "b", "c", "d", "e"]
            .iter()
            .map(|t| tm.label(t).unwrap())
            .collect();
        assert_eq!(labels.len(), 3, "max_tables + 1 distinct label sets");
    }

    #[test]
    fn a_real_overflow_named_table_stays_distinct() {
        let tm = TableMetrics::new("p", policy(1));
        let real = tm.label(OVERFLOW_TABLE).unwrap();
        let overflow = tm.label("later").unwrap();
        assert_eq!(real.table(), overflow.table());
        assert_ne!(real.labels(), overflow.labels(), "table_scope differs");
        assert_eq!(real.scope(), SCOPE_EXACT);
        assert_eq!(overflow.scope(), SCOPE_OVERFLOW);
    }

    #[test]
    fn concurrent_admission_never_exceeds_the_cap() {
        let tm = Arc::new(TableMetrics::new("p", policy(50)));
        let handles: Vec<_> = (0..8)
            .map(|w| {
                let tm = tm.clone();
                std::thread::spawn(move || {
                    for i in 0..500 {
                        tm.label(&format!("t{}", (i * 7 + w) % 400));
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(tm.admitted(), 50);
    }

    #[test]
    fn overflowed_tables_are_estimated_within_bounds() {
        for n in [1u64, 10, 100, 1_000, 10_000, 100_000] {
            let tm = TableMetrics::new("p", policy(1));
            tm.label("admitted");
            for i in 0..n {
                tm.label(&format!("db.table_{i}"));
                tm.label(&format!("db.table_{i}")); // repeats do not count
            }
            let est = tm.overflowed_estimate() as f64;
            let err = (est - n as f64).abs() / n as f64;
            assert!(err <= 0.10, "n={n} estimate={est}");
        }
    }

    fn names(prefix: &str) -> impl Iterator<Item = String> + '_ {
        (0..).map(move |i| format!("{prefix}{i}"))
    }

    #[test]
    fn a_restart_with_the_same_policy_keeps_admissions() {
        let a = register("reg-same", policy(3)).unwrap();
        a.tables().label("x");
        let b = register("reg-same", policy(3)).unwrap();
        assert!(Arc::ptr_eq(a.tables(), b.tables()));
        assert_eq!(for_pipeline("reg-same").admitted(), 1);
    }

    /// Disabling and re-enabling, with disjoint table sets each time, never
    /// creates more than `max_tables` exact tables under the name.
    #[test]
    fn policy_changes_with_disjoint_tables_stay_bounded() {
        let first = register("reg-toggle", policy(3)).unwrap();
        for t in names("a").take(10) {
            first.tables().label(&t);
        }
        let _ = register("reg-toggle", None).unwrap();
        assert_eq!(for_pipeline("reg-toggle").label("b0"), None);
        let again = register("reg-toggle", policy(3)).unwrap();
        let mut exact = HashSet::new();
        for t in names("a").take(10).chain(names("b").take(10)) {
            if let Some(TableLabel::Exact(t)) = again.tables().label(&t) {
                exact.insert(t);
            }
        }
        assert_eq!(exact.len(), 3, "the same three admissions: {exact:?}");
        assert!(exact.iter().all(|t| t.starts_with('a')));
    }

    #[test]
    fn max_tables_is_fixed_once_table_series_exist() {
        // Before any series, the cap can change freely.
        let _ = register("reg-cap", policy(3)).unwrap();
        let r = register("reg-cap", policy(5)).unwrap();
        r.tables().label("t");
        let err = register("reg-cap", policy(6)).unwrap_err();
        assert!(err.contains("fixed at 5"), "{err}");
        assert!(check("reg-cap", policy(6)).is_err());
        assert_eq!(for_pipeline("reg-cap").policy().unwrap().max_tables, 5);
        // Disabling, or changing only the lag idle time, is allowed.
        check("reg-cap", None).unwrap();
        let lag = Some(PerTablePolicy {
            max_tables: 5,
            lag_idle: Duration::from_secs(60),
        });
        let _ = register("reg-cap", lag).unwrap();
        assert_eq!(for_pipeline("reg-cap").policy(), lag);
    }

    /// Delete and recreate with a disjoint table set reuses the admissions.
    #[test]
    fn delete_and_recreate_with_disjoint_tables_stay_bounded() {
        let first = register("reg-recreate", policy(2)).unwrap();
        first.tables().label("old.a");
        first.tables().label("old.b");
        unregister("reg-recreate");
        assert!(is_registered("reg-recreate"), "kept: its series exist");
        assert_eq!(for_pipeline("reg-recreate").label("old.a"), None);
        let second = register("reg-recreate", policy(2)).unwrap();
        assert_eq!(second.tables().label("new.a"), Some(TableLabel::Overflow));
        assert_eq!(second.tables().admitted(), 2);
        assert!(register("reg-recreate", policy(4)).is_err());
    }

    #[test]
    fn names_without_table_series_are_forgotten() {
        for name in names("reg-unused-").take(1_000) {
            let _ = register(&name, policy(10)).unwrap();
            unregister(&name);
            assert!(!is_registered(&name));
        }
        let tm = TableMetrics::new("p", policy(1));
        tm.label("a");
        assert!(tm.overflowed.lock().unwrap().is_none(), "no estimator yet");
    }

    #[test]
    fn rollback_removes_a_new_entry_and_restores_an_existing_one() {
        let fresh = register("reg-rb-new", policy(3)).unwrap();
        fresh.rollback();
        assert!(!is_registered("reg-rb-new"));

        let running = register("reg-rb-old", policy(3)).unwrap();
        running.tables().label("t");
        let failed = register("reg-rb-old", None).unwrap();
        failed.rollback();
        assert_eq!(for_pipeline("reg-rb-old").policy(), policy(3));
        assert_eq!(for_pipeline("reg-rb-old").admitted(), 1);

        // A new entry that created series before failing is kept, disabled.
        let partial = register("reg-rb-partial", policy(3)).unwrap();
        partial.tables().label("t");
        partial.rollback();
        assert!(is_registered("reg-rb-partial"));
        assert_eq!(for_pipeline("reg-rb-partial").policy(), None);
        assert!(register("reg-rb-partial", policy(4)).is_err());
    }
}

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

/// One pipeline's per-table label policy.
#[derive(Debug)]
pub struct TableMetrics {
    pipeline: Arc<str>,
    policy: Option<PerTablePolicy>,
    admitted: RwLock<HashSet<Arc<str>>>,
    overflowed: Mutex<DistinctEstimate>,
}

impl TableMetrics {
    pub fn new(pipeline: &str, policy: Option<PerTablePolicy>) -> Self {
        Self {
            pipeline: Arc::from(pipeline),
            policy,
            admitted: RwLock::new(HashSet::new()),
            overflowed: Mutex::new(DistinctEstimate::new()),
        }
    }

    /// Per-table detail off: every [`label`](Self::label) is `None`.
    pub fn disabled(pipeline: &str) -> Self {
        Self::new(pipeline, None)
    }

    pub fn pipeline(&self) -> &str {
        &self.pipeline
    }

    pub fn policy(&self) -> Option<PerTablePolicy> {
        self.policy
    }

    /// The table labels for an observation of `table`: `None` when per-table
    /// detail is off, the exact label for an admitted table (admitting it
    /// while there is room), and the overflow label otherwise.
    pub fn label(&self, table: &str) -> Option<TableLabel> {
        let policy = self.policy?;
        {
            let admitted = self.admitted.read().expect("admitted lock");
            if let Some(t) = admitted.get(table) {
                return Some(TableLabel::Exact(t.clone()));
            }
            if admitted.len() >= policy.max_tables {
                drop(admitted);
                self.overflow(table);
                return Some(TableLabel::Overflow);
            }
        }
        let mut admitted = self.admitted.write().expect("admitted lock");
        if let Some(t) = admitted.get(table) {
            return Some(TableLabel::Exact(t.clone()));
        }
        if admitted.len() >= policy.max_tables {
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
        self.overflowed.lock().expect("overflow lock").estimate()
    }

    fn overflow(&self, table: &str) {
        let pipeline = self.pipeline.to_string();
        counter!(METRIC_OVERFLOW, "pipeline" => pipeline.clone()).increment(1);
        let mut est = self.overflowed.lock().expect("overflow lock");
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

/// Register `pipeline`'s policy. A restart with the same policy keeps the
/// existing admissions; a changed policy starts fresh.
pub fn register(
    pipeline: &str,
    policy: Option<PerTablePolicy>,
) -> Arc<TableMetrics> {
    let mut reg = registry().write().expect("table metrics registry");
    if let Some(existing) = reg.get(pipeline)
        && existing.policy == policy
    {
        return existing.clone();
    }
    let tm = Arc::new(TableMetrics::new(pipeline, policy));
    reg.insert(pipeline.to_string(), tm.clone());
    tm
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

/// Forget `pipeline` (on deletion).
pub fn unregister(pipeline: &str) {
    registry()
        .write()
        .expect("table metrics registry")
        .remove(pipeline);
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

    #[test]
    fn registry_keeps_admissions_for_an_unchanged_policy() {
        let a = register("reg-p", policy(3));
        a.label("x");
        assert!(Arc::ptr_eq(&a, &register("reg-p", policy(3))));
        assert_eq!(for_pipeline("reg-p").admitted(), 1);
        let b = register("reg-p", policy(4));
        assert_eq!(b.admitted(), 0, "a changed policy starts fresh");
        unregister("reg-p");
        assert_eq!(for_pipeline("reg-p").policy(), None);
    }
}

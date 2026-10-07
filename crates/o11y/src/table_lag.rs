//! `deltaforge_source_table_lag_seconds`: per-table replication lag, kept
//! out of the metrics recorder because a table's series must disappear
//! when the table goes quiet, and the Prometheus exporter cannot remove a
//! single series.
//!
//! Only pipelines that enable `metrics.per_table` report here. Each holds
//! at most `max_tables + 1` series (its admitted tables and the overflow),
//! keyed by label set. A series not updated for the pipeline's
//! `lag_idle_secs` (monotonic time) is pruned on the next scrape: a quiet
//! table's detailed lag disappears, which says nothing about whether the
//! table still exists. Deleting a pipeline removes all of its series.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use deltaforge_core::table_metrics::{PerTablePolicy, TableLabel};

pub const NAME: &str = "deltaforge_source_table_lag_seconds";
const HELP: &str = "Replication lag of the latest event per table in each batch \
                    (seconds). Present only with metrics.per_table enabled.";

#[derive(Debug)]
struct PipelineLag {
    policy: PerTablePolicy,
    series: HashMap<TableLabel, (f64, Instant)>,
}

/// The collector. One per process ([`global`]); tests make their own.
#[derive(Debug, Default)]
pub struct TableLagCollector {
    pipelines: Mutex<HashMap<String, PipelineLag>>,
}

impl TableLagCollector {
    /// Set `pipeline`'s lag series for `label` at monotonic time `now`.
    /// A new series beyond the pipeline's `max_tables + 1` bound is refused
    /// (the label policy never produces one).
    pub fn set(
        &self,
        pipeline: &str,
        policy: PerTablePolicy,
        label: TableLabel,
        lag_secs: f64,
        now: Instant,
    ) {
        let mut pipelines = self.pipelines.lock().expect("table lag lock");
        let entry =
            pipelines.entry(pipeline.to_string()).or_insert_with(|| {
                PipelineLag {
                    policy,
                    series: HashMap::new(),
                }
            });
        if entry.policy != policy {
            // The pipeline was respawned with a different policy: its old
            // admissions no longer apply.
            entry.policy = policy;
            entry.series.clear();
        }
        let bound = policy.max_tables + 1;
        if entry.series.len() >= bound && !entry.series.contains_key(&label) {
            return;
        }
        entry.series.insert(label, (lag_secs, now));
    }

    /// Remove every series of `pipeline`.
    pub fn remove_pipeline(&self, pipeline: &str) {
        self.pipelines
            .lock()
            .expect("table lag lock")
            .remove(pipeline);
    }

    /// Series currently held for `pipeline`.
    pub fn len(&self, pipeline: &str) -> usize {
        self.pipelines
            .lock()
            .expect("table lag lock")
            .get(pipeline)
            .map_or(0, |p| p.series.len())
    }

    /// Prune series idle at `now`, then render the rest as Prometheus text:
    /// one consistent snapshot (taken under one lock), HELP and TYPE once,
    /// each series once, sorted, label values escaped. Empty when there is
    /// no series.
    pub fn render(&self, now: Instant) -> String {
        let mut lines = BTreeMap::new();
        {
            let mut pipelines = self.pipelines.lock().expect("table lag lock");
            for (pipeline, p) in pipelines.iter_mut() {
                let idle = p.policy.lag_idle;
                p.series.retain(|_, (_, at)| {
                    now.saturating_duration_since(*at) < idle
                });
                for (label, (value, _)) in &p.series {
                    let key = format!(
                        "pipeline=\"{}\",table=\"{}\",table_scope=\"{}\"",
                        escape(pipeline),
                        escape(label.table()),
                        label.scope()
                    );
                    lines.insert(key, *value);
                }
            }
            pipelines.retain(|_, p| !p.series.is_empty());
        }
        if lines.is_empty() {
            return String::new();
        }
        let mut out = format!("# HELP {NAME} {HELP}\n# TYPE {NAME} gauge\n");
        for (labels, value) in lines {
            let _ = writeln!(out, "{NAME}{{{labels}}} {}", sample(value));
        }
        out
    }
}

/// Prometheus label-value escaping: backslash, double quote, newline.
fn escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out
}

fn sample(value: f64) -> String {
    if value.is_nan() {
        "NaN".into()
    } else if value.is_infinite() {
        if value > 0.0 { "+Inf" } else { "-Inf" }.into()
    } else {
        format!("{value}")
    }
}

/// The process-wide collector, rendered by the `/metrics` handler.
pub fn global() -> &'static TableLagCollector {
    static GLOBAL: OnceLock<TableLagCollector> = OnceLock::new();
    GLOBAL.get_or_init(TableLagCollector::default)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::Duration;

    use deltaforge_core::table_metrics::{OVERFLOW_TABLE, TableMetrics};

    use super::*;

    fn policy(max_tables: usize, idle_secs: u64) -> PerTablePolicy {
        PerTablePolicy {
            max_tables,
            lag_idle: Duration::from_secs(idle_secs),
        }
    }

    fn exact(t: &str) -> TableLabel {
        TableLabel::Exact(Arc::from(t))
    }

    /// A strict parse of the exposition: (HELP count, TYPE count, samples as
    /// (unescaped labels, value)); panics on any malformed line.
    type Sample = (Vec<(String, String)>, f64);
    fn parse(text: &str) -> (usize, usize, Vec<Sample>) {
        let (mut help, mut ty, mut samples) = (0, 0, Vec::new());
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix("# HELP ") {
                assert!(rest.starts_with(NAME));
                help += 1;
                continue;
            }
            if let Some(rest) = line.strip_prefix("# TYPE ") {
                assert_eq!(rest, format!("{NAME} gauge"));
                ty += 1;
                continue;
            }
            let rest = line.strip_prefix(NAME).expect("metric name");
            let rest = rest.strip_prefix('{').expect("label set");
            let mut chars = rest.chars();
            let mut labels = Vec::new();
            loop {
                let name: String =
                    chars.by_ref().take_while(|&c| c != '=').collect();
                assert_eq!(chars.next(), Some('"'), "quoted value in {line}");
                let mut value = String::new();
                loop {
                    match chars.next().expect("closing quote") {
                        '\\' => match chars.next().expect("escape") {
                            '\\' => value.push('\\'),
                            '"' => value.push('"'),
                            'n' => value.push('\n'),
                            c => panic!("bad escape \\{c} in {line}"),
                        },
                        '"' => break,
                        '\n' => panic!("raw newline in {line}"),
                        c => value.push(c),
                    }
                }
                labels.push((name, value));
                match chars.next() {
                    Some(',') => continue,
                    Some('}') => break,
                    other => panic!("unexpected {other:?} in {line}"),
                }
            }
            let value: String = chars.collect();
            let value = value.strip_prefix(' ').expect("space before value");
            samples.push((labels, value.parse().expect("numeric value")));
        }
        (help, ty, samples)
    }

    #[test]
    fn empty_renders_nothing() {
        assert_eq!(TableLagCollector::default().render(Instant::now()), "");
    }

    #[test]
    fn exposition_is_well_formed_escaped_and_sorted() {
        let c = TableLagCollector::default();
        let now = Instant::now();
        let odd = "db.we\"ird\\name\nx";
        c.set("p", policy(10, 300), exact(odd), 1.5, now);
        c.set("p", policy(10, 300), exact("db.a"), 0.0, now);
        c.set("p", policy(10, 300), TableLabel::Overflow, 7.25, now);
        let text = c.render(now);
        let (help, ty, samples) = parse(&text);
        assert_eq!((help, ty), (1, 1));
        assert_eq!(samples.len(), 3);
        let tables: Vec<_> =
            samples.iter().map(|(l, _)| l[1].1.clone()).collect();
        assert!(tables.contains(&odd.to_string()), "escaping round-trips");
        let again = c.render(now);
        assert_eq!(text, again, "stable output");
    }

    #[test]
    fn a_real_overflow_named_table_is_a_separate_series() {
        let c = TableLagCollector::default();
        let now = Instant::now();
        c.set("p", policy(10, 300), exact(OVERFLOW_TABLE), 1.0, now);
        c.set("p", policy(10, 300), TableLabel::Overflow, 2.0, now);
        let (_, _, samples) = parse(&c.render(now));
        assert_eq!(samples.len(), 2);
        let scopes: HashSet<_> = samples
            .iter()
            .map(|(l, v)| (l[1].1.clone(), l[2].1.clone(), *v as u64))
            .collect();
        assert!(scopes.contains(&(OVERFLOW_TABLE.into(), "exact".into(), 1)));
        assert!(scopes.contains(&(
            OVERFLOW_TABLE.into(),
            "overflow".into(),
            2
        )));
    }

    #[test]
    fn idle_series_expire_on_monotonic_time() {
        let c = TableLagCollector::default();
        let t0 = Instant::now();
        let p = policy(10, 60);
        c.set("p", p, exact("db.quiet"), 3.0, t0);
        c.set("p", p, exact("db.busy"), 1.0, t0);
        c.set("p", p, exact("db.busy"), 2.0, t0 + Duration::from_secs(50));
        assert_eq!(parse(&c.render(t0 + Duration::from_secs(59))).2.len(), 2);
        let (_, _, samples) = parse(&c.render(t0 + Duration::from_secs(60)));
        assert_eq!(samples.len(), 1, "the quiet table's series is gone");
        assert_eq!(samples[0].0[1].1, "db.busy");
        assert_eq!(c.len("p"), 1, "pruned, not just hidden");
        assert_eq!(c.render(t0 + Duration::from_secs(200)), "");
        assert_eq!(c.len("p"), 0);
    }

    #[test]
    fn idle_expiry_frees_no_admission() {
        let tables = TableMetrics::new("p", Some(policy(1, 10)));
        let c = TableLagCollector::default();
        let t0 = Instant::now();
        let first = tables.label("db.a").unwrap();
        c.set("p", policy(1, 10), first, 1.0, t0);
        assert_eq!(c.render(t0 + Duration::from_secs(11)), "");
        assert_eq!(tables.label("db.b"), Some(TableLabel::Overflow));
        assert_eq!(tables.label("db.a").unwrap().table(), "db.a");
    }

    #[test]
    fn deleting_a_pipeline_removes_all_its_series() {
        let c = TableLagCollector::default();
        let now = Instant::now();
        for t in ["db.a", "db.b"] {
            c.set("gone", policy(10, 300), exact(t), 1.0, now);
        }
        c.set("gone", policy(10, 300), TableLabel::Overflow, 1.0, now);
        c.set("kept", policy(10, 300), exact("db.a"), 1.0, now);
        c.remove_pipeline("gone");
        let (_, _, samples) = parse(&c.render(now));
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].0[0], ("pipeline".into(), "kept".into()));
    }

    #[test]
    fn storage_is_bounded_per_pipeline() {
        let c = TableLagCollector::default();
        let now = Instant::now();
        for i in 0..100 {
            c.set("p", policy(5, 300), exact(&format!("db.t{i}")), 1.0, now);
        }
        assert_eq!(c.len("p"), 6, "max_tables + 1");
    }

    #[test]
    fn concurrent_updates_removals_and_scrapes_stay_consistent() {
        let c = Arc::new(TableLagCollector::default());
        let start = Instant::now();
        let writers: Vec<_> = (0..4)
            .map(|w| {
                let c = c.clone();
                std::thread::spawn(move || {
                    for i in 0..2_000u64 {
                        let p = format!("p{}", w % 2);
                        let label = if i % 7 == 0 {
                            TableLabel::Overflow
                        } else {
                            exact(&format!("db.t{}", i % 20))
                        };
                        c.set(&p, policy(30, 300), label, i as f64, start);
                        if i % 500 == 0 {
                            c.remove_pipeline(&p);
                        }
                    }
                })
            })
            .collect();
        let scraper = {
            let c = c.clone();
            std::thread::spawn(move || {
                for _ in 0..500 {
                    let (help, ty, samples) = parse(&c.render(Instant::now()));
                    assert!(help <= 1 && help == ty);
                    let keys: HashSet<_> =
                        samples.iter().map(|(l, _)| l.clone()).collect();
                    assert_eq!(
                        keys.len(),
                        samples.len(),
                        "no duplicate series"
                    );
                    assert!(samples.len() <= 2 * 31);
                }
            })
        };
        for w in writers {
            w.join().unwrap();
        }
        scraper.join().unwrap();
    }
}

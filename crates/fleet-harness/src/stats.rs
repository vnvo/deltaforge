//! Latency histograms and recovery percentiles.

use serde::Serialize;

/// Log-linear histogram of non-negative values (milliseconds, say): 64
/// powers of two, each split into 32 linear sub-buckets, so any recorded
/// value is reported within about 3% in fixed memory.
#[derive(Debug, Clone)]
pub struct Histogram {
    counts: Vec<u64>,
    total: u64,
    max: u64,
}

const SUB: u64 = 32;

impl Default for Histogram {
    fn default() -> Self {
        Histogram {
            counts: vec![0; 64 * SUB as usize],
            total: 0,
            max: 0,
        }
    }
}

impl Histogram {
    fn bucket(v: u64) -> usize {
        if v < SUB {
            return v as usize;
        }
        let exp = 63 - v.leading_zeros() as u64; // >= 5
        let sub = (v >> (exp - 5)) & (SUB - 1);
        ((exp - 4) * SUB + sub) as usize
    }

    /// The lowest value of bucket `b`.
    fn value(b: usize) -> u64 {
        let b = b as u64;
        if b < SUB {
            return b;
        }
        let exp = b / SUB + 4;
        let sub = b % SUB;
        (SUB + sub) << (exp - 5)
    }

    pub fn record(&mut self, v: u64) {
        self.counts[Self::bucket(v)] += 1;
        self.total += 1;
        self.max = self.max.max(v);
    }

    pub fn merge(&mut self, other: &Histogram) {
        for (a, b) in self.counts.iter_mut().zip(&other.counts) {
            *a += b;
        }
        self.total += other.total;
        self.max = self.max.max(other.max);
    }

    pub fn count(&self) -> u64 {
        self.total
    }

    /// The value at quantile `q` (0..=1); `None` when empty.
    pub fn quantile(&self, q: f64) -> Option<u64> {
        if self.total == 0 {
            return None;
        }
        if q >= 1.0 {
            return Some(self.max);
        }
        let rank = ((q * self.total as f64).ceil() as u64).max(1);
        let mut seen = 0;
        for (b, c) in self.counts.iter().enumerate() {
            seen += c;
            if seen >= rank {
                return Some(Self::value(b).min(self.max));
            }
        }
        Some(self.max)
    }

    pub fn summary(&self) -> Summary {
        Summary {
            count: self.total,
            p50: self.quantile(0.5),
            p90: self.quantile(0.9),
            p99: self.quantile(0.99),
            max: (self.total > 0).then_some(self.max),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Summary {
    pub count: u64,
    pub p50: Option<u64>,
    pub p90: Option<u64>,
    pub p99: Option<u64>,
    pub max: Option<u64>,
}

/// Recovery after a disruption: how long until 50%, 90% and 100% of the
/// affected sources delivered a change again.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Recovery {
    pub sources: usize,
    pub recovered: usize,
    pub p50_secs: Option<f64>,
    pub p90_secs: Option<f64>,
    pub p100_secs: Option<f64>,
    /// The last source to recover (or the first never to).
    pub slowest: Option<String>,
}

/// `times`: per source, seconds from the end of the disruption to its first
/// delivered change (`None` = never recovered in the run).
pub fn recovery(times: &[(String, Option<f64>)]) -> Recovery {
    let mut done: Vec<(f64, &str)> = times
        .iter()
        .filter_map(|(s, t)| t.map(|t| (t, s.as_str())))
        .collect();
    done.sort_by(|a, b| a.0.total_cmp(&b.0));
    let n = times.len();
    // The time by which `q` of ALL sources recovered.
    let at = |q: f64| -> Option<f64> {
        let need = ((q * n as f64).ceil() as usize).max(1);
        done.get(need - 1).map(|(t, _)| *t)
    };
    let slowest = times
        .iter()
        .find(|(_, t)| t.is_none())
        .map(|(s, _)| s.clone())
        .or_else(|| done.last().map(|(_, s)| s.to_string()));
    Recovery {
        sources: n,
        recovered: done.len(),
        p50_secs: at(0.5),
        p90_secs: at(0.9),
        p100_secs: at(1.0),
        slowest,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantiles_are_within_three_percent() {
        let mut h = Histogram::default();
        for v in 1..=100_000u64 {
            h.record(v);
        }
        for (q, want) in [(0.5, 50_000.0), (0.9, 90_000.0), (0.99, 99_000.0)] {
            let got = h.quantile(q).unwrap() as f64;
            assert!((got - want).abs() / want <= 0.035, "q={q} got={got}");
        }
        assert_eq!(h.quantile(1.0), Some(100_000));
        assert_eq!(Histogram::default().quantile(0.5), None);
    }

    #[test]
    fn small_values_are_exact_and_merge_adds() {
        let mut a = Histogram::default();
        let mut b = Histogram::default();
        for v in [1, 2, 3] {
            a.record(v);
        }
        b.record(30);
        a.merge(&b);
        assert_eq!(a.count(), 4);
        assert_eq!(a.quantile(0.5), Some(2));
        assert_eq!(a.summary().max, Some(30));
    }

    #[test]
    fn recovery_percentiles_count_unrecovered_sources() {
        let times: Vec<(String, Option<f64>)> = (0..10)
            .map(|i| {
                (
                    format!("c{i:02}"),
                    if i == 9 { None } else { Some(f64::from(i)) },
                )
            })
            .collect();
        let r = recovery(&times);
        assert_eq!((r.sources, r.recovered), (10, 9));
        assert_eq!(r.p50_secs, Some(4.0));
        assert_eq!(r.p90_secs, Some(8.0));
        assert_eq!(r.p100_secs, None, "one source never recovered");
        assert_eq!(r.slowest.as_deref(), Some("c09"));
    }
}

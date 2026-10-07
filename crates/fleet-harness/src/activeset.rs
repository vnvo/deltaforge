//! Which tables receive changes (design section 5.2).
//!
//! A server's tables are numbered `0..total` (`db * tables_per_db + table`)
//! and visited through a fixed pseudo-random permutation, so the active set
//! of `k = ceil(fraction * total)` tables is spread over customers. A moving
//! set takes the next `k` positions of the permutation each period, so
//! consecutive sets are disjoint until the permutation wraps (reported by
//! [`ActiveSet::wraps_after`]). Within the set, a table is chosen with a
//! Zipf distribution over its rank (exponent 0 is uniform).

use rand::Rng;

use crate::config::ActiveSetPattern;
use crate::topology::TableRef;

/// A bijection on `0..n`: `i -> (a * i + b) mod n` with `gcd(a, n) = 1`.
#[derive(Debug, Clone, Copy)]
struct Permutation {
    n: u64,
    a: u64,
    b: u64,
}

impl Permutation {
    fn new(n: u64, seed: u64) -> Self {
        let mut a = (seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1) % n.max(1);
        while a == 0 || gcd(a, n) != 1 {
            a = (a + 1) % n.max(1);
            if n <= 1 {
                a = 1;
                break;
            }
        }
        Permutation {
            n,
            a,
            b: seed % n.max(1),
        }
    }

    fn apply(&self, i: u64) -> u64 {
        ((u128::from(self.a) * u128::from(i % self.n) + u128::from(self.b))
            % u128::from(self.n)) as u64
    }
}

fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

/// Zipf over ranks `0..k` by inverse CDF.
#[derive(Debug, Clone)]
struct Zipf {
    cdf: Vec<f64>,
}

impl Zipf {
    fn new(k: usize, exponent: f64) -> Self {
        let mut cdf = Vec::with_capacity(k);
        let mut sum = 0.0;
        for r in 1..=k {
            sum += 1.0 / (r as f64).powf(exponent);
            cdf.push(sum);
        }
        for c in &mut cdf {
            *c /= sum;
        }
        Zipf { cdf }
    }

    fn sample(&self, rng: &mut impl Rng) -> usize {
        let u: f64 = rng.random();
        self.cdf.partition_point(|&c| c < u).min(self.cdf.len() - 1)
    }
}

/// One server's active set.
#[derive(Debug, Clone)]
pub struct ActiveSet {
    server: u16,
    tables_per_db: u32,
    total: u64,
    k: u64,
    period_secs: Option<u64>,
    perm: Permutation,
    zipf: Zipf,
}

impl ActiveSet {
    pub fn new(
        server: u16,
        databases: u32,
        tables_per_db: u32,
        pattern: &ActiveSetPattern,
        zipf_exponent: f64,
        seed: u64,
    ) -> Self {
        let total = u64::from(databases) * u64::from(tables_per_db);
        let (fraction, period_secs) = match pattern {
            ActiveSetPattern::Fixed { fraction } => (*fraction, None),
            ActiveSetPattern::Moving {
                fraction,
                period_secs,
            } => (*fraction, Some(*period_secs)),
        };
        let k =
            ((fraction * total as f64).ceil() as u64).clamp(1, total.max(1));
        ActiveSet {
            server,
            tables_per_db,
            total,
            k,
            period_secs,
            perm: Permutation::new(total.max(1), seed ^ u64::from(server)),
            zipf: Zipf::new(k as usize, zipf_exponent),
        }
    }

    /// Tables in the set at a time.
    pub fn size(&self) -> u64 {
        self.k
    }

    /// The window (period index) at `elapsed_secs`; always 0 for a fixed set.
    pub fn window(&self, elapsed_secs: u64) -> u64 {
        self.period_secs.map_or(0, |p| elapsed_secs / p.max(1))
    }

    /// Windows until a moving set reuses tables (`None` for a fixed set).
    pub fn wraps_after(&self) -> Option<u64> {
        self.period_secs.map(|_| self.total / self.k)
    }

    /// The `rank`-th table of `window`'s set.
    pub fn table_at(&self, window: u64, rank: u64) -> TableRef {
        let position = window.wrapping_mul(self.k).wrapping_add(rank % self.k);
        let t = self.perm.apply(position);
        TableRef {
            server: self.server,
            db: (t / u64::from(self.tables_per_db)) as u32,
            table: (t % u64::from(self.tables_per_db)) as u16,
        }
    }

    /// A table to change now.
    pub fn pick(&self, elapsed_secs: u64, rng: &mut impl Rng) -> TableRef {
        let rank = self.zipf.sample(rng) as u64;
        self.table_at(self.window(elapsed_secs), rank)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rand::SeedableRng;
    use rand::rngs::StdRng;

    use super::*;

    fn set(fraction: f64, period: Option<u64>) -> ActiveSet {
        let pattern = match period {
            None => ActiveSetPattern::Fixed { fraction },
            Some(p) => ActiveSetPattern::Moving {
                fraction,
                period_secs: p,
            },
        };
        ActiveSet::new(0, 2_000, 200, &pattern, 1.0, 7)
    }

    #[test]
    fn the_permutation_is_a_bijection() {
        for n in [1u64, 2, 7, 400_000] {
            let p = Permutation::new(n, 99);
            let seen: HashSet<u64> =
                (0..n.min(50_000)).map(|i| p.apply(i)).collect();
            assert_eq!(seen.len() as u64, n.min(50_000), "n={n}");
            assert!(seen.iter().all(|&v| v < n));
        }
    }

    #[test]
    fn set_sizes_follow_the_fraction() {
        assert_eq!(set(0.0001, None).size(), 40);
        assert_eq!(set(0.01, None).size(), 4_000);
        assert_eq!(set(0.1, None).size(), 40_000);
    }

    #[test]
    fn a_fixed_set_is_spread_over_customers() {
        let s = set(0.001, None);
        let dbs: HashSet<u32> =
            (0..s.size()).map(|r| s.table_at(0, r).db).collect();
        assert!(dbs.len() > 300, "{} databases", dbs.len());
    }

    #[test]
    fn moving_sets_are_disjoint_until_they_wrap() {
        let s = set(0.01, Some(1_800));
        let window = |w: u64| -> HashSet<TableRef> {
            (0..s.size()).map(|r| s.table_at(w, r)).collect()
        };
        let (a, b) = (window(0), window(1));
        assert_eq!(a.len() as u64, s.size());
        assert!(a.is_disjoint(&b));
        assert_eq!(s.wraps_after(), Some(100));
        assert_eq!(s.window(3_599), 1);
        assert_eq!(s.window(3_600), 2);
    }

    #[test]
    fn picks_stay_in_the_current_set_and_are_skewed() {
        let s = set(0.01, None);
        let members: HashSet<TableRef> =
            (0..s.size()).map(|r| s.table_at(0, r)).collect();
        let mut rng = StdRng::seed_from_u64(1);
        let mut hot = 0;
        for _ in 0..20_000 {
            let t = s.pick(0, &mut rng);
            assert!(members.contains(&t));
            if t == s.table_at(0, 0) {
                hot += 1;
            }
        }
        assert!(hot > 1_000, "rank 0 is hot under Zipf(1): {hot}");
        let uniform = ActiveSet::new(
            0,
            2_000,
            200,
            &ActiveSetPattern::Fixed { fraction: 0.01 },
            0.0,
            7,
        );
        let mut rng = StdRng::seed_from_u64(1);
        let top = (0..20_000)
            .filter(|_| uniform.pick(0, &mut rng) == uniform.table_at(0, 0))
            .count();
        assert!(top < 50, "exponent 0 is uniform: {top}");
    }
}

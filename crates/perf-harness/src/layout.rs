//! Layout-randomized qualification: two versions, each linked once per
//! predeclared seed with its functions in a seeded order
//! (`layout-links.sh`), compared seed by seed with the same workload, and
//! aggregated over every seed.
//!
//! A single link of each version can differ by a few percent from another
//! link of the same source (code placement), which is larger than a 2%
//! budget; the distribution over several links is what a version costs.
//! Every seed counts: a seed is never left out because of its result, and a
//! run that failed is reported as an incomplete pair, not dropped.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

/// The links of one version (`manifest.json` of `layout-links.sh`).
pub struct Manifest {
    pub rev: String,
    pub links: Vec<(u64, PathBuf, String)>,
}

fn sha256(path: &Path) -> Result<String> {
    let out = std::process::Command::new("sha256sum")
        .arg(path)
        .output()
        .context("sha256sum")?;
    Ok(String::from_utf8(out.stdout)?
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_string())
}

/// Load `dir/manifest.json` and check every link's executable against its
/// recorded SHA-256.
pub fn load(dir: &Path) -> Result<Manifest> {
    let v: Value = serde_json::from_slice(
        &std::fs::read(dir.join("manifest.json"))
            .with_context(|| format!("{}/manifest.json", dir.display()))?,
    )?;
    let rev = v["rev"].as_str().context("manifest rev")?.to_string();
    let mut links = Vec::new();
    for l in v["links"].as_array().context("manifest links")? {
        let seed = l["seed"].as_u64().context("link seed")?;
        let path = dir.join(l["path"].as_str().context("link path")?);
        let want = l["sha256"].as_str().context("link sha256")?.to_string();
        let got = sha256(&path)?;
        if got != want {
            bail!("{}: SHA-256 {got}, manifest {want}", path.display());
        }
        links.push((seed, path, want));
    }
    if links.len() < 5 {
        bail!(
            "{}: {} links; qualification needs at least 5",
            dir.display(),
            links.len()
        );
    }
    Ok(Manifest { rev, links })
}

/// The seeds both manifests list, in order; they must list the same.
pub fn seeds(a: &Manifest, b: &Manifest) -> Result<Vec<u64>> {
    let sa: Vec<u64> = a.links.iter().map(|l| l.0).collect();
    let sb: Vec<u64> = b.links.iter().map(|l| l.0).collect();
    if sa != sb {
        bail!(
            "the versions were linked with different seeds: {sa:?} and {sb:?}"
        );
    }
    Ok(sa)
}

/// A deterministic generator for the bootstrap (SplitMix64).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

fn geomean(logs: &[f64]) -> f64 {
    (logs.iter().sum::<f64>() / logs.len() as f64).exp()
}

/// The geometric mean of paired ratios and its 95% bootstrap interval
/// (10,000 resamples of the pairs, a fixed generator seed).
fn ci(logs: &[f64]) -> (f64, f64, f64) {
    let mut rng = Rng(0x5eed);
    let mut means: Vec<f64> = (0..10_000)
        .map(|_| {
            let s: f64 = (0..logs.len())
                .map(|_| logs[(rng.next() % logs.len() as u64) as usize])
                .sum();
            (s / logs.len() as f64).exp()
        })
        .collect();
    means.sort_by(f64::total_cmp);
    (geomean(logs), means[250], means[9_749])
}

/// One cell's paired results over the seeds: `(seed, rep) -> label -> run`.
type Pairs = BTreeMap<(u64, u64), BTreeMap<String, Value>>;

/// Aggregate the seeds' `runs.jsonl` under `out/seed-<seed>/` into
/// `out/layout-summary.md` and `out/layout.json`.
pub fn aggregate(
    out: &Path,
    (a, rev_a): (&str, &str),
    (b, rev_b): (&str, &str),
    seeds: &[u64],
) -> Result<()> {
    let mut cells: BTreeMap<String, Pairs> = BTreeMap::new();
    let mut hashes: BTreeMap<(String, u64), Vec<String>> = BTreeMap::new();
    for seed in seeds {
        let path = out.join(format!("seed-{seed}")).join("runs.jsonl");
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("{}", path.display()))?;
        for line in text.lines() {
            let r: Value = serde_json::from_str(line)?;
            let cell = r["cell"].as_str().unwrap_or_default().to_string();
            let label = r["version"].as_str().unwrap_or_default().to_string();
            let rep = r["rep"].as_u64().unwrap_or_default();
            if let Some(h) = r["result"]["binary_sha256"].as_str() {
                let e = hashes.entry((label.clone(), *seed)).or_default();
                if !e.iter().any(|x| x == h) {
                    e.push(h.to_string());
                }
            }
            cells
                .entry(cell)
                .or_default()
                .entry((*seed, rep))
                .or_default()
                .insert(label, r["result"].clone());
        }
    }
    let ratio = |x: &BTreeMap<String, Value>, key: &str| -> Option<f64> {
        let (ra, rb) = (x.get(a)?, x.get(b)?);
        if ra["ok"] != json!(true) || rb["ok"] != json!(true) {
            return None;
        }
        Some((rb[key].as_f64()? / ra[key].as_f64()?).ln())
    };
    let mut md = format!(
        "Layout-randomized comparison, {b} (`{rev_b}`) against {a} (`{rev_a}`): {} seeds ({}), every seed included.\n\n\
         | cell | pairs | incomplete | {b}/{a} rows/s | 95% CI | per-seed geomeans | min-max over seeds | cpu/row {b}/{a} |\n|{}\n",
        seeds.len(),
        seeds
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(", "),
        "---|".repeat(8),
    );
    let mut report = serde_json::Map::new();
    for (cell, pairs) in &cells {
        let logs: Vec<f64> = pairs
            .values()
            .filter_map(|x| ratio(x, "rows_per_sec"))
            .collect();
        let incomplete: Vec<String> = pairs
            .iter()
            .filter(|(_, x)| ratio(x, "rows_per_sec").is_none())
            .map(|((seed, rep), x)| {
                let why = [a, b]
                    .iter()
                    .map(|l| match x.get(*l) {
                        None => format!("{l}: missing"),
                        Some(r) if r["ok"] != json!(true) => format!(
                            "{l}: {}",
                            r["error"].as_str().unwrap_or("failed")
                        ),
                        Some(_) => format!("{l}: ok"),
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("seed {seed} rep {rep} ({why})")
            })
            .collect();
        let per_seed: Vec<(u64, f64)> = seeds
            .iter()
            .filter_map(|s| {
                let l: Vec<f64> = pairs
                    .iter()
                    .filter(|((seed, _), _)| seed == s)
                    .filter_map(|(_, x)| ratio(x, "rows_per_sec"))
                    .collect();
                (!l.is_empty()).then(|| (*s, geomean(&l)))
            })
            .collect();
        let cpu: Vec<f64> = pairs
            .values()
            .filter_map(|x| {
                let (ra, rb) = (x.get(a)?, x.get(b)?);
                let per = |r: &Value| {
                    Some(r["cpu_secs"].as_f64()? / r["rows"].as_f64()?)
                };
                Some((per(rb)? / per(ra)?).ln())
            })
            .filter(|v| v.is_finite())
            .collect();
        let (g, lo, hi) = if logs.is_empty() {
            (f64::NAN, f64::NAN, f64::NAN)
        } else {
            ci(&logs)
        };
        let (smin, smax) = per_seed
            .iter()
            .fold((f64::INFINITY, f64::NEG_INFINITY), |(lo, hi), (_, v)| {
                (lo.min(*v), hi.max(*v))
            });
        md.push_str(&format!(
            "| {cell} | {} | {} | {g:.3} | {lo:.3}-{hi:.3} | {} | {smin:.3}-{smax:.3} | {} |\n",
            logs.len(),
            incomplete.len(),
            per_seed.iter().map(|(s, v)| format!("{s}: {v:.3}")).collect::<Vec<_>>().join(", "),
            if cpu.is_empty() { "-".into() } else { format!("{:.3}", geomean(&cpu)) },
        ));
        report.insert(
            cell.clone(),
            json!({
                "pairs": logs.len(), "geomean": g, "ci95": [lo, hi],
                "per_seed": per_seed.iter().map(|(s, v)| json!({"seed": s, "geomean": v})).collect::<Vec<_>>(),
                "cpu_per_row_ratio": (!cpu.is_empty()).then(|| geomean(&cpu)),
                "incomplete": incomplete,
            }),
        );
    }
    md.push_str("\nIncomplete pairs (a run that failed or is missing; never dropped silently):\n\n");
    let mut any = false;
    for (cell, v) in &report {
        for i in v["incomplete"].as_array().into_iter().flatten() {
            any = true;
            md.push_str(&format!(
                "- `{cell}`: {}\n",
                i.as_str().unwrap_or_default()
            ));
        }
    }
    if !any {
        md.push_str("- none\n");
    }
    md.push_str("\nExecutables run (SHA-256 per label and seed):\n\n");
    for ((label, seed), hs) in &hashes {
        md.push_str(&format!("- {label} seed {seed}: {}\n", hs.join(", ")));
    }
    md.push_str("\nRaw results: `seed-<seed>/runs.jsonl` and `seed-<seed>/runs/`; the links in each version's `manifest.json`.\n");
    std::fs::write(out.join("layout-summary.md"), md)?;
    std::fs::write(
        out.join("layout.json"),
        serde_json::to_vec_pretty(
            &json!({"a": a, "b": b, "seeds": seeds, "cells": report}),
        )?,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_bootstrap_interval_holds_the_geometric_mean() {
        let logs: Vec<f64> = [0.98_f64, 1.02, 1.0, 0.99, 1.01, 1.03, 0.97]
            .iter()
            .map(|r| r.ln())
            .collect();
        let (g, lo, hi) = ci(&logs);
        assert!((g - 1.0).abs() < 0.01, "{g}");
        assert!(lo < g && g < hi, "{lo} {g} {hi}");
        assert_eq!(ci(&logs), (g, lo, hi), "deterministic");
    }

    #[test]
    fn versions_linked_with_different_seeds_are_refused() {
        let m = |seeds: &[u64]| Manifest {
            rev: "r".into(),
            links: seeds
                .iter()
                .map(|s| (*s, PathBuf::new(), String::new()))
                .collect(),
        };
        assert!(seeds(&m(&[1, 2, 3, 4, 5]), &m(&[1, 2, 3, 4, 5])).is_ok());
        assert!(seeds(&m(&[1, 2, 3, 4, 5]), &m(&[1, 2, 3, 4, 6])).is_err());
    }
}

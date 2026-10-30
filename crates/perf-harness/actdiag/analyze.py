#!/usr/bin/env python3
"""Summarize an actdiag series and judge it by the variation rule fixed in
docs/design/mysql-ddl-proof-transactions.md, section 7, before any result
was seen.

    analyze.py <series-dir> --old LABEL --new LABEL [--also LABEL ...]
               [--max-diff 0.03]

With --max-diff (the instrumentation overhead check, section 6), a metric
also needs its medians within that fraction of each other, in either
direction, or within its absolute tolerance.

Writes <series-dir>/summary.md and verdicts.json. The rule applies only to
the end-to-end metrics; phase histograms are reported as distributions
(count, mean), never judged, and are not traces of individual DDLs.

Exit status: 0 when every judged metric is "no worse" and every run is
correct; 1 otherwise (a worse or inconclusive metric, a failed run, or a
correctness failure).
"""
import argparse
import json
import pathlib
import statistics
import sys

# Absolute tolerances (section 7), in each metric's unit.
TOLERANCE = {
    "phase_completion_s": 1.0,
    "per_ddl_ms": 2.0,
    "busy_lag_p99_ms": 50.0,
    "busy_lag_max_ms": 50.0,
    "activation_p50_ms": 50.0,
    "activation_p99_ms": 50.0,
    "drain_s": 1.0,
}
INCONCLUSIVE_SPREAD = 0.15


def metrics(run):
    """The comparable end-to-end metrics of a run (lower is better)."""
    out = {"drain_s": run.get("drain_s")}
    if "ddl" in run:
        d = run["ddl"]
        out["phase_completion_s"] = d["phase_completion_s"]
        out["per_ddl_ms"] = d["per_ddl_ms"]
        if run.get("busy", 0) and d.get("busy_lag_ms"):
            out["busy_lag_p99_ms"] = d["busy_lag_ms"]["p99"]
            out["busy_lag_max_ms"] = d["busy_lag_ms"]["max"]
    if "activation" in run:
        a = run["activation"]
        out["phase_completion_s"] = a["phase_completion_s"]
        if a.get("activation_ms"):
            out["activation_p50_ms"] = a["activation_ms"]["p50"]
            out["activation_p99_ms"] = a["activation_ms"]["p99"]
    return {k: v for k, v in out.items() if v is not None}


def spread(values):
    med = statistics.median(values)
    return (max(values) - min(values)) / med if med else 0.0


def judge(old, new, tol, max_diff=None):
    """The verdict on one metric from each build's repetitions."""
    so, sn = spread(old), spread(new)
    wide = [
        s for s, v in ((so, old), (sn, new))
        if s > INCONCLUSIVE_SPREAD and (max(v) - min(v)) > tol
    ]
    allowance = max(max(so, sn) * statistics.median(old), tol)
    no_worse = (
        statistics.median(new) <= statistics.median(old) + allowance
        and max(new) <= max(old) + tol
    )
    mo, mn = statistics.median(old), statistics.median(new)
    if max_diff is not None and abs(mn - mo) > max(max_diff * mo, tol):
        no_worse = False
    verdict = "inconclusive" if wide else ("no worse" if no_worse else "worse")
    return {
        "old_median": statistics.median(old),
        "new_median": statistics.median(new),
        "old": old,
        "new": new,
        "old_spread": so,
        "new_spread": sn,
        "allowance": allowance,
        "tolerance": tol,
        "verdict": verdict,
    }


def phase_table(runs):
    """Per phase: count per run and mean seconds, over a build's runs."""
    acc = {}
    for r in runs:
        for phase, h in (r.get("phases") or {}).items():
            a = acc.setdefault(phase, {"count": 0.0, "sum": 0.0, "runs": 0})
            a["count"] += h["count"]
            a["sum"] += h["sum_s"]
            a["runs"] += 1
    return {
        p: {
            "count_per_run": a["count"] / max(a["runs"], 1),
            "mean_ms": (a["sum"] / a["count"] * 1e3) if a["count"] else None,
            "total_s_per_run": a["sum"] / max(a["runs"], 1),
        }
        for p, a in sorted(acc.items())
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("series")
    ap.add_argument("--old", required=True)
    ap.add_argument("--new", required=True)
    ap.add_argument("--also", action="append", default=[])
    ap.add_argument("--max-diff", type=float)
    a = ap.parse_args()
    d = pathlib.Path(a.series)
    runs = [json.loads(l) for l in (d / "runs.jsonl").read_text().splitlines() if l]
    labels = [a.old, a.new, *a.also]
    failed = [r for r in runs if "failed" in r]
    incorrect = [
        r for r in runs
        if "failed" not in r and not r.get("drain", {}).get("ledger", {}).get("equal")
    ]
    verdicts = {}
    lines = [f"# actdiag summary: {d.name}", ""]
    lines += [
        f"Runs: {len(runs)}; failed: {len(failed)}; incorrect: {len(incorrect)}.",
        f"Judged: `{a.new}` against `{a.old}` (section 7 rule; tolerances "
        + ", ".join(f"{k} {v}" for k, v in TOLERANCE.items()) + ").",
        "",
    ]
    for cmp in sorted({r["comparison"] for r in runs}):
        ok = [r for r in runs if r["comparison"] == cmp and "failed" not in r]
        by = {l: [r for r in ok if r["build"] == l] for l in labels}
        lines += [f"## {cmp}", ""]
        names = sorted({k for r in ok for k in metrics(r)})
        lines.append("| metric | " + " | ".join(f"{l} median (runs)" for l in labels) + " | verdict |")
        lines.append("|---|" + "---|" * len(labels) + "---|")
        for m in names:
            vals = {l: [metrics(r)[m] for r in by[l] if m in metrics(r)] for l in labels}
            cells = [
                f"{statistics.median(v):.3f} ({', '.join(f'{x:.3f}' for x in v)})" if v else "-"
                for v in vals.values()
            ]
            verdict = "-"
            if vals[a.old] and vals[a.new]:
                j = judge(vals[a.old], vals[a.new], TOLERANCE[m], a.max_diff)
                verdicts.setdefault(cmp, {})[m] = j
                verdict = j["verdict"]
            lines.append(f"| {m} | " + " | ".join(cells) + f" | {verdict} |")
        lines += ["", "Phase distributions (diagnostic, not judged): count per run, mean ms.", ""]
        lines.append("| phase | " + " | ".join(labels) + " |")
        lines.append("|---|" + "---|" * len(labels))
        tables = {l: phase_table(by[l]) for l in labels}
        for p in sorted({p for t in tables.values() for p in t}):
            cells = []
            for l in labels:
                t = tables[l].get(p)
                cells.append(
                    f"{t['count_per_run']:.0f} x {t['mean_ms']:.2f}" if t and t["mean_ms"] is not None else "-"
                )
            lines.append(f"| {p} | " + " | ".join(cells) + " |")
        store = [
            (r["build"], r["ddls"], r["store_before"], r["store_after_phase"])
            for r in ok if r.get("store_after_phase") and r.get("store_before")
        ]
        if store:
            lines += ["", "State store during the DDL phase, per DDL (all store traffic, background included):", ""]
            for b, n, s0, s1 in store:
                per = {k: (s1[k] - s0[k]) / n for k in s1}
                lines.append(
                    f"- {b}: {per.get('commit_statements', float('nan')):.1f} explicit commits, "
                    f"{per['statements']:.1f} statements, "
                    f"{per['advisory_xact_locks']:.1f} store-lock calls"
                )
        lines.append("")
    if failed or incorrect:
        lines += ["## Failed or incorrect runs", ""]
        for r in failed:
            lines.append(f"- {r['comparison']} {r['build']} rep {r.get('rep')}: {r['failed']}")
        for r in incorrect:
            lines.append(f"- {r['comparison']} {r['build']} rep {r.get('rep')}: ledger not equal")
        lines.append("")
    (d / "summary.md").write_text("\n".join(lines) + "\n")
    (d / "verdicts.json").write_text(json.dumps(verdicts, indent=2) + "\n")
    bad = any(
        j["verdict"] != "no worse" for c in verdicts.values() for j in c.values()
    )
    print((d / "summary.md").read_text())
    sys.exit(1 if bad or failed or incorrect else 0)


if __name__ == "__main__":
    main()

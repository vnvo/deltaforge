#!/usr/bin/env python3
"""Synthetic checks that analyze.py fails closed on an incomplete series.

    python3 -I crates/perf-harness/actdiag/analyze_test.py
"""
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest

ANALYZE = pathlib.Path(__file__).resolve().parent / "analyze.py"


def ddl_run(comparison, build, rep, per_ddl_ms=10.0, rate=2000.0):
    run = {
        "comparison": comparison,
        "build": build,
        "rep": rep,
        "busy": 150 if rate else 1,
        "rate": rate,
        "drain_s": 0.5,
        "drain": {"ledger": {"equal": True}},
        "ddl": {
            "phase_completion_s": 3.0,
            "per_ddl_ms": per_ddl_ms,
            "busy_lag_ms": {"p99": 1000.0, "max": 1100.0} if rate else None,
        },
    }
    return run


def act_run(comparison, build, rep):
    return {
        "comparison": comparison,
        "build": build,
        "rep": rep,
        "busy": 0,
        "rate": 0.0,
        "drain_s": 0.3,
        "drain": {"ledger": {"equal": True}},
        "activation": {
            "phase_completion_s": 0.5,
            "activation_ms": {"p50": 70.0, "p99": 110.0},
        },
    }


def series(comparisons=("ddl-loaded", "act-fresh"), builds=("old", "change"), reps=3):
    runs = []
    for c in comparisons:
        for b in builds:
            for r in range(1, reps + 1):
                runs.append(act_run(c, b, r) if c.startswith("act") else ddl_run(c, b, r))
    return runs


class Analyze(unittest.TestCase):
    def analyze(self, runs, comparisons=("ddl-loaded", "act-fresh"), builds=("old", "change"),
                reps=3, invocation=True, extra=()):
        with tempfile.TemporaryDirectory() as t:
            d = pathlib.Path(t)
            (d / "runs.jsonl").write_text("".join(json.dumps(r) + "\n" for r in runs))
            if invocation:
                cs = ", ".join(f'"{c}"' for c in comparisons)
                (d / "invocation.json").write_text(json.dumps({
                    "args": f"Args {{ builds: [], comparisons: [{cs}], store: \"postgres\", reps: {reps}, seed: 1 }}",
                    "builds": [{"label": b} for b in builds],
                }))
            p = subprocess.run(
                [sys.executable, "-I", str(ANALYZE), str(d), "--old", "old", "--new", "change", *extra],
                capture_output=True, text=True,
            )
            verdicts = json.loads((d / "verdicts.json").read_text())
            return p.returncode, verdicts

    def assert_incomplete(self, rc, verdicts, needle):
        self.assertEqual(rc, 1)
        self.assertFalse(verdicts["complete"])
        self.assertTrue(any(needle in p for p in verdicts["problems"]), verdicts["problems"])

    def test_a_complete_series_passes(self):
        rc, v = self.analyze(series())
        self.assertEqual(rc, 0, v)
        self.assertTrue(v["complete"])
        self.assertEqual(set(v["verdicts"]), {"ddl-loaded", "act-fresh"})

    def test_a_missing_build_fails(self):
        runs = [r for r in series() if r["build"] != "change"]
        rc, v = self.analyze(runs)
        self.assert_incomplete(rc, v, "change: 0 successful runs")

    def test_a_build_not_in_the_series_fails(self):
        rc, v = self.analyze(series(), builds=("old",))
        self.assert_incomplete(rc, v, "build change is not in the series")

    def test_a_missing_comparison_fails(self):
        runs = [r for r in series() if r["comparison"] != "act-fresh"]
        rc, v = self.analyze(runs)
        self.assert_incomplete(rc, v, "act-fresh old: 0 successful runs")

    def test_a_missing_metric_fails(self):
        runs = series()
        runs[0]["ddl"]["busy_lag_ms"] = None
        rc, v = self.analyze(runs)
        self.assert_incomplete(rc, v, "missing busy_lag_max_ms, busy_lag_p99_ms")

    def test_an_empty_series_fails(self):
        rc, v = self.analyze([])
        self.assert_incomplete(rc, v, "no runs")
        self.assertIn("no metric was judged", v["problems"])

    def test_fewer_repetitions_than_started_fail(self):
        runs = [r for r in series() if r["rep"] < 3]
        rc, v = self.analyze(runs)
        self.assert_incomplete(rc, v, "2 successful runs, need 3")

    def test_fewer_than_three_repetitions_never_pass(self):
        rc, v = self.analyze(series(reps=2), reps=2, extra=("--min-reps", "1"))
        self.assert_incomplete(rc, v, "2 successful runs, need 3")

    def test_a_failed_run_does_not_count_as_a_repetition(self):
        runs = series()
        runs[0]["failed"] = "drain timeout"
        rc, v = self.analyze(runs)
        self.assert_incomplete(rc, v, "ddl-loaded old: 2 successful runs, need 3")

    def test_a_duplicate_repetition_fails(self):
        runs = series()
        runs.append(dict(runs[0]))
        rc, v = self.analyze(runs)
        self.assert_incomplete(rc, v, "ddl-loaded old rep 1 appears twice")

    def test_a_missing_invocation_fails(self):
        rc, v = self.analyze(series(), invocation=False)
        self.assert_incomplete(rc, v, "invocation.json unusable")

    def test_a_worse_metric_fails_a_complete_series(self):
        runs = series()
        for r in runs:
            if r["build"] == "change" and "ddl" in r:
                r["ddl"]["per_ddl_ms"] = 20.0
        rc, v = self.analyze(runs)
        self.assertEqual(rc, 1)
        self.assertTrue(v["complete"])
        self.assertEqual(v["verdicts"]["ddl-loaded"]["per_ddl_ms"]["verdict"], "worse")


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
"""Evaluator of the single-source capacity campaign (docs/design/
fleet-capacity-plan.md, "Execution recipe"). Frozen with the campaign: its
hash is in IDENTITY.sha256 and a changed copy is refused.

  frontier.py <A|B|C> <invocation dir> <active tables> [--live <harness pid>]

The invocation dir holds results/exploratory/<run>/{samples.jsonl,
result.json}, df.log (`docker logs -t cap-df`), resources.jsonl (watch.sh),
end-state.json and floor-watch.rc (run-one.sh), and any marker file.

Writes verdict.json and prints one line. Exit: 0 viable, 1 saturated,
2 correctness or health failure (ends the campaign), 3 inconclusive.

Live mode (Phase B only) follows the run and SIGINTs the harness after the
first step that is saturated (SATURATED-STOP) or generator-limited
(GENERATOR-STOP); the harness then drains and checks completeness as usual.
"""
import glob, json, math, os, re, signal, subprocess, sys, time
from datetime import datetime

STEPS = [2000, 4000, 8000, 16000, 32000, 64000]
STEP_SECS = 300
SETTLE = 120            # each step: 2 minutes settling, 3 measured
MIN_STEP_SPAN = 150     # a step's measured samples must span this much
A_MEASURED = 600        # Phase A: 10 measured minutes after activation
C_WARMUP, C_MEASURED = 900, 2700
DELIVERY_RATIO = 0.95   # criterion 1, and the generator check
ALLOC = {"cap-mysql": 3.0, "cap-kafka": 2.0, "cap-df": 4.0}  # CPUs
BUSY = 0.9              # a component at >= 90% of its CPUs is "at allocation"


def ts(s):
    s = re.sub(r"(\.\d{6})\d+", r"\1", s.replace("Z", "+00:00"))
    return datetime.fromisoformat(s).timestamp()


def one(d):
    return sum(d.values()) if isinstance(d, dict) and d else None


def load_samples(path):
    out = []
    for line in open(path):
        r = json.loads(line)
        off, com, dlv = one(r.get("offered_ops")), one(r.get("committed_ops")), one(r.get("sink_events"))
        if off is None or com is None:
            continue
        out.append({"t": ts(r["at"]), "off": off, "com": com, "dlv": dlv or 0.0,
                    "lag": one(r.get("lag_seconds")), "mem": (r.get("process") or {}).get("memory_current")})
    return out


def windows_from_log(lines):
    """(commit time, tables) of every activation window record, in order."""
    out = []
    for line in lines:
        if '"record":"window"' not in line:
            continue
        m = re.search(r"\{.*\}", line)
        try:
            r = json.loads(m.group(0))
        except Exception:
            continue
        if r.get("record") == "window":
            out.append((r["commit_unix_us"] / 1e6, int(r["tables"])))
    return out


def activation(wins, n):
    """Completion: the commit of the window whose running sum of activated
    tables (deferred tables excluded) reaches the active set. A final sum
    other than the active set leaves completion not established."""
    total, done = 0, None
    for t, k in wins:
        total += k
        if done is None and total >= n:
            done = t
    return {"windows": len(wins), "activated": total, "active": n,
            "complete_at": done if total == n else None,
            "established": total == n}


def writes_span(s):
    moving = [i for i in range(1, len(s)) if s[i]["off"] > s[i - 1]["off"]]
    if not moving:
        return None, None
    return s[moving[0] - 1]["t"], s[moving[-1]]["t"]


def transitions(s, start):
    """Actual start of steps 2..6, solved from offered_ops in the sample
    interval that straddles each rate change. Step 1 continues the warm-up
    rate (2,000), so its start is step 2's minus 300 s."""
    out, k = {}, 1
    for i in range(1, len(s)):
        if k >= len(STEPS) or s[i - 1]["t"] < start:
            continue
        a, b = s[i - 1], s[i]
        dt = b["t"] - a["t"]
        if dt <= 0:
            continue
        old, new = STEPS[k - 1], STEPS[k]
        if (b["off"] - a["off"]) / dt > old * 1.02:
            t = (b["off"] - a["off"] + old * a["t"] - new * b["t"]) / (old - new)
            out[k + 1] = min(max(t, a["t"]), b["t"])
            k += 1
    if 2 in out:
        out[1] = out[2] - STEP_SECS
    return out


def resources(path):
    rows = []
    if not os.path.exists(path):
        return rows
    for line in open(path):
        r = json.loads(line)
        cpu = {}
        for st in r.get("docker_stats") or []:
            try:
                cpu[st["Name"]] = float(st["CPUPerc"].rstrip("%")) / 100.0
            except Exception:
                pass
        rows.append({"t": ts(r["at"]), "cpu": cpu, "mem": r.get("mem_available"),
                     "harness": r.get("harness_cpu_ticks")})
    return rows


def slope(xs, ys):
    n = len(xs)
    if n < 2:
        return None
    mx, my = sum(xs) / n, sum(ys) / n
    den = sum((x - mx) ** 2 for x in xs)
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / den if den else None


def pct(v, p):
    v = sorted(x for x in v if x is not None)
    return v[int((len(v) - 1) * p)] if v else None


def measure(s, res, lo, hi):
    """A window [lo, hi]: rates, both ratios, backlog, lag and resources."""
    w = [x for x in s if lo <= x["t"] <= hi]
    if len(w) < 2:
        return None
    a, b = w[0], w[-1]
    dt = b["t"] - a["t"]
    off, com, dlv = (b["off"] - a["off"]) / dt, (b["com"] - a["com"]) / dt, (b["dlv"] - a["dlv"]) / dt
    backlog = [x["com"] - x["dlv"] for x in w]
    r = [x for x in res if lo <= x["t"] <= hi]
    cpu = {c: (sum(x["cpu"].get(c, 0) for x in r) / len(r) if r else None) for c in ALLOC}
    ticks = [x["harness"] for x in r if x["harness"] is not None]
    hz = os.sysconf("SC_CLK_TCK")
    return {
        "from": a["t"], "to": b["t"], "span_secs": dt,
        "offered_per_sec": off, "committed_per_sec": com, "delivered_per_sec": dlv,
        "offered_to_committed": com / off if off else None,
        "committed_to_delivered": dlv / com if com else None,
        "backlog_start": backlog[0], "backlog_end": backlog[-1],
        "backlog_slope_per_sec": slope([x["t"] for x in w], backlog),
        "lag_p50": pct([x["lag"] for x in w], 0.5), "lag_p99": pct([x["lag"] for x in w], 0.99),
        "lag_slope": slope([x["t"] for x in w if x["lag"] is not None], [x["lag"] for x in w if x["lag"] is not None]),
        "df_memory_max": max((x["mem"] or 0) for x in w),
        "cpu_used": cpu,
        "harness_cpu": ((ticks[-1] - ticks[0]) / hz / (r[-1]["t"] - r[0]["t"])) if len(ticks) > 1 and r[-1]["t"] > r[0]["t"] else None,
        "host_mem_available_min": min((x["mem"] for x in r if x["mem"] is not None), default=None),
    }


def judge(m):
    """Criteria 1 and 2 (committed to delivered only) and the generator check."""
    sat = []
    if m["committed_to_delivered"] is not None and m["committed_to_delivered"] < DELIVERY_RATIO:
        sat.append("1: delivered < 95% of committed")
    growth = m["backlog_end"] - m["backlog_start"]
    if growth > m["offered_per_sec"] * 1.0 and (m["backlog_slope_per_sec"] or 0) > 0:
        sat.append("2: backlog grew by more than one second of offered traffic")
    gen = m["offered_to_committed"] is not None and m["offered_to_committed"] < DELIVERY_RATIO
    return sat, gen


def attribute(m, sat, gen):
    busy = {c: (m["cpu_used"][c] or 0) >= BUSY * ALLOC[c] for c in ALLOC}
    if gen:
        return "MySQL limit" if busy["cap-mysql"] else "generator limit"
    if not sat:
        return None
    if (m["lag_slope"] or 0) > 0 and not busy["cap-kafka"]:
        return "DeltaForge limit"
    if busy["cap-kafka"] and (m["lag_slope"] or 0) <= 0:
        return "sink limit"
    return "unresolved environment limit"


def health(d, inv, phase):
    """Criterion 4 and the run's evidence: any failure ends the campaign."""
    bad = []
    for f in ("FAILCLOSED",):
        if os.path.exists(os.path.join(inv, f)):
            bad.append("pipeline failed (FAILCLOSED)")
    v, c, st = d["verdict"], d["completeness"], d["stream"]
    stopped = any(os.path.exists(os.path.join(inv, f)) for f in ("SATURATED-STOP", "GENERATOR-STOP", "MEMFLOOR", "OOM"))
    try:
        stopped = stopped or int(open(os.path.join(inv, "floor-watch.rc")).read()) in (4, 5)
    except Exception:
        pass
    if d.get("outcome") != "completed" and not stopped:
        bad.append(f"outcome {d.get('outcome')}: {d.get('reason')}")
    for k in ("correctness_ok", "evidence_ok"):
        if not v.get(k):
            bad.append(f"{k} false")
    for k in ("missing", "duplicates", "unexpected", "final_state_mismatches", "partition_order_violations"):
        if c.get(k, 0):
            bad.append(f"{k}={c[k]}")
    for k in ("decode_errors", "key_mismatches"):
        if st.get(k, 0):
            bad.append(f"{k}={st[k]}")
    try:
        e = json.load(open(os.path.join(inv, "end-state.json")))
        if e.get("incidents") != 0:
            bad.append(f"incidents {e.get('incidents')}")
        if e.get("restarted"):
            bad.append("the instance restarted or stopped")
    except Exception as ex:
        bad.append(f"end state unreadable: {ex}")
    return bad


def resource_stop(inv):
    out = []
    for f, why in (("MEMFLOOR", "3: host available memory under 4 GiB"), ("OOM", "3: a container OOM-killed or stopped")):
        if os.path.exists(os.path.join(inv, f)):
            out.append(why)
    try:
        rc = int(open(os.path.join(inv, "floor-watch.rc")).read())
        if rc == 4:
            out.append("3: disk floor (25 GiB) reached")
        if rc == 5:
            out.append("deadline reached")
    except Exception:
        pass
    return out


def evaluate(phase, inv, n):
    runs = sorted(glob.glob(os.path.join(inv, "results/exploratory/*/result.json")))
    out = {"phase": phase, "invocation": inv, "active_tables": n}
    if not runs:
        out.update(status="inconclusive", reasons=["no result.json"] + resource_stop(inv))
        return out, 3
    run = os.path.dirname(runs[-1])
    d = json.load(open(runs[-1]))
    s = load_samples(os.path.join(run, "samples.jsonl"))
    res = resources(os.path.join(inv, "resources.jsonl"))
    act = activation(windows_from_log(open(os.path.join(inv, "df.log"), errors="replace")), n)
    start, end = writes_span(s)
    out.update(run=run, activation=act, writes_from=start, writes_to=end)
    bad = health(d, inv, phase)
    stops = resource_stop(inv)
    if bad:
        out.update(status="failure", reasons=bad)
        return out, 2
    if start is None:
        out.update(status="inconclusive", reasons=["no writes recorded"] + stops)
        return out, 3
    if act["established"]:
        out["activation"]["secs"] = act["complete_at"] - start
        out["activation"]["tables_per_sec"] = n / (act["complete_at"] - start)
    if phase in ("A", "C"):
        lo = act["complete_at"] if act["established"] else None
        if phase == "C" and lo is not None:
            lo = max(lo, start + C_WARMUP)
        need = A_MEASURED if phase == "A" else C_MEASURED
        if lo is None or end - lo < need - 5:
            out.update(status="inconclusive", reasons=[
                "activation not established" if lo is None else
                f"measured span after activation {end - lo:.0f}s < {need}s"] + stops)
            if phase == "A":
                out["activation_lag"] = measure(s, res, start, end)
            return out, 3
        m = measure(s, res, lo, end)
        out["during_activation"] = measure(s, res, start, act["complete_at"])
        sat, gen = judge(m)
        sat += [x for x in stops if x.startswith("3:")]
        out.update(window=m, saturated=sat, generator_limited=gen, attribution=attribute(m, sat, gen))
        if gen and not sat:
            out.update(status="inconclusive", reasons=["generator or MySQL did not commit 95% of the offered rate"])
            return out, 3
        out["status"] = "saturated" if sat else "viable"
        return out, 1 if sat else 0
    # Phase B
    tr = transitions(s, start)
    out["transitions"] = tr
    steps, first_sat = [], None
    for k in range(1, len(STEPS) + 1):
        if k not in tr:
            steps.append({"step": k, "rate": STEPS[k - 1], "status": "not reached"})
            continue
        lo, hi = tr[k] + SETTLE, tr.get(k + 1, end)
        row = {"step": k, "rate": STEPS[k - 1], "started_at": tr[k], "window": [lo, hi]}
        if not act["established"] or act["complete_at"] > lo:
            row["status"] = "not measured: activation incomplete before its window"
        elif hi - lo < MIN_STEP_SPAN:
            row["status"] = "not measured: window too short (run stopped in the step)"
        else:
            m = measure(s, res, lo, hi)
            sat, gen = judge(m)
            row.update(metrics=m, saturated=sat, generator_limited=gen, attribution=attribute(m, sat, gen))
            row["status"] = "saturated" if sat else ("generator-limited" if gen else "viable")
        steps.append(row)
        if row["status"] in ("saturated", "generator-limited") and first_sat is None:
            first_sat = row
    out["steps"] = steps
    viable = [r["rate"] for r in steps if r["status"] == "viable" and (first_sat is None or r["step"] < first_sat["step"])]
    out["highest_viable"] = max(viable) if viable else None
    out["lowest_saturated"] = first_sat["rate"] if first_sat and first_sat["status"] == "saturated" else None
    if stops and first_sat is None:
        out["resource_stop"] = stops
    if first_sat is not None:
        out["status"] = first_sat["status"]
        return out, 1 if first_sat["status"] == "saturated" else 3
    if any(x.startswith("3:") for x in stops):
        out["status"] = "saturated"
        out["saturated"] = stops
        return out, 1
    out["status"] = "viable" if out["highest_viable"] == STEPS[-1] else "inconclusive"
    return out, 0 if out["status"] == "viable" else 3


def live(inv, n, pid):
    """Phase B: stop the harness after the first saturated or
    generator-limited step whose window followed completed activation."""
    lines, since, seen = [], None, 0
    while True:
        try:
            os.kill(pid, 0)
        except OSError:
            return
        cmd = ["docker", "logs", "-t", "cap-df"] + (["--since", since] if since else [])
        new = subprocess.run(cmd, capture_output=True, text=True, errors="replace")
        chunk = (new.stdout + new.stderr).splitlines()
        if chunk:
            since = chunk[-1].split(" ", 1)[0]
            # --since is inclusive: drop lines already held.
            chunk = [l for l in chunk if l not in set(lines[-200:])]
            lines += chunk
        act = activation(windows_from_log(lines), n)
        paths = glob.glob(os.path.join(inv, "results/exploratory/*/samples.jsonl"))
        if paths:
            s = load_samples(paths[-1])
            res = resources(os.path.join(inv, "resources.jsonl"))
            start, _ = writes_span(s)
            tr = transitions(s, start) if start else {}
            for k in range(1, len(STEPS)):
                if k + 1 not in tr or k <= seen:
                    continue
                seen = k
                lo, hi = tr[k] + SETTLE, tr[k + 1]
                if not act["established"] or act["complete_at"] > lo or hi - lo < MIN_STEP_SPAN:
                    continue
                m = measure(s, res, lo, hi)
                sat, gen = judge(m)
                if sat or gen:
                    marker = "SATURATED-STOP" if sat else "GENERATOR-STOP"
                    json.dump({"step": k, "rate": STEPS[k - 1], "saturated": sat, "generator_limited": gen,
                               "metrics": m, "at": time.time()}, open(os.path.join(inv, marker), "w"), indent=1)
                    os.kill(pid, signal.SIGINT)
                    return
        time.sleep(5)


if __name__ == "__main__":
    phase, inv, n = sys.argv[1], os.path.abspath(sys.argv[2]), int(sys.argv[3])
    if len(sys.argv) > 5 and sys.argv[4] == "--live":
        live(inv, n, int(sys.argv[5]))
        sys.exit(0)
    out, rc = evaluate(phase, inv, n)
    json.dump(out, open(os.path.join(inv, "verdict.json"), "w"), indent=1)
    keys = ("status", "highest_viable", "lowest_saturated", "attribution", "saturated", "reasons")
    print(phase, " ".join(f"{k}={out[k]}" for k in keys if out.get(k) is not None), f"verdict={inv}/verdict.json")
    sys.exit(rc)

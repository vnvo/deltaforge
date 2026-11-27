#!/usr/bin/env python3
"""Evaluator of the single-source capacity campaign (docs/design/
fleet-capacity-plan.md, "Execution recipe"). Frozen with the campaign: its
hash is in IDENTITY.sha256 and a changed copy is refused.

  frontier.py <A|B|C> <invocation dir> <active tables> [--live <harness pid> <topic> <kafka image>]
  frontier.py coverage <invocation dir> <active tables> <topic> <kafka image>

The invocation dir holds results/exploratory/<run>/{samples.jsonl,
result.json}, df.log (`docker logs -t cap-df`), resources.jsonl (watch.sh),
end-state.json and floor-watch.rc (campaign.sh), coverage.json, and any
marker file.

Activation completion is established from distinct table identities in
the run's retained Kafka events: `coverage` reads the whole topic (its
record count must equal the topic's end offsets) and records, per source
table, the earliest record timestamp. Completion is the latest of those
first appearances, and only when exactly <active tables> distinct tables
appear with no unreadable record. The product's activation window records
are reported, never used as the boundary.

Writes verdict.json and prints one line. Exit: 0 viable, 1 saturated,
2 correctness or health failure (ends the campaign), 3 inconclusive
(including evidence incomplete: verdict.json evidence_complete is false).

Calibration (`CAL`, run `cal100`) reads the generator only: per step, the
offered, committed and delivered rates, MySQL's CPU against its allocation,
its table-open rates and memory. Live, it applies R2 (MySQL to 5 CPUs) once,
after the first step whose committed traffic is under 95% of offered while
MySQL is at 90% or more of 3 CPUs (`R2-APPLIED`); a later such step, or one
where MySQL is not at its allocation, stops the run as an environment limit
(`GENERATOR-STOP`). It is never extended.

Live mode (Phase B only) follows the run and SIGINTs the harness after the
first step that is saturated (SATURATED-STOP) or generator-limited
(GENERATOR-STOP), or when step 2's window starts before coverage is
complete (ACTIVATION-STOP); the harness then drains and checks
completeness as usual.
"""
import glob, json, math, os, re, signal, subprocess, sys, threading, time
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
R2_CPUS = 5.0           # the calibration's R2: MySQL's CPUs
MAX_GAP = 30            # samples further apart than this leave a window incomplete
COVERAGE_IDLE_MS = 60000
COVERAGE_TIMEOUT = 900


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
        # A sample without delivery counters (failed scrape) stays unknown.
        out.append({"t": ts(r["at"]), "off": off, "com": com, "dlv": dlv,
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


def windows_summary(wins):
    """The product's activation windows, reported only: a sum of per-window
    table counts cannot exclude repeated activations or untouched tables."""
    return {"windows": len(wins), "tables_summed": sum(k for _, k in wins),
            "last_commit": wins[-1][0] if wins else None}


SOURCE = re.compile(r'"source":(\{[^{}]*\})')
DB, TABLE = re.compile(r"cust_\d{5}"), re.compile(r"t\d{3,}")


class Coverage:
    """Distinct source tables in `CreateTime:<ms>\\t<event>` records (the
    console consumer's print.timestamp format), each with the earliest
    record timestamp seen."""

    def __init__(self, n):
        self.n, self.first, self.records, self.rows, self.errors = n, {}, 0, 0, 0
        self.latest = None  # the latest record timestamp read
        self.lock = threading.Lock()

    def add(self, line):
        line = line.rstrip("\n")
        head, sep, value = line.partition("\t")
        with self.lock:
            self.records += 1
            try:
                if not sep or not head.startswith("CreateTime:"):
                    raise ValueError("no record timestamp")
                t = int(head[len("CreateTime:"):]) / 1000.0
                self.latest = t if self.latest is None else max(self.latest, t)
                m = SOURCE.search(value)
                src = json.loads(m.group(1)) if m else (json.loads(value).get("source") or {})
            except Exception:
                self.errors += 1
                return
            db, table = src.get("db"), src.get("table")
            if not (isinstance(db, str) and isinstance(table, str)
                    and DB.fullmatch(db) and TABLE.fullmatch(table)):
                return
            self.rows += 1
            k = (db, table)
            if k not in self.first or t < self.first[k]:
                self.first[k] = t

    def distinct(self):
        with self.lock:
            return len(self.first)

    def complete_at(self):
        """The latest first appearance, once exactly n tables appeared."""
        with self.lock:
            return max(self.first.values()) if len(self.first) == self.n else None


def end_offsets(topic, image):
    """Records in the topic: the sum of its partitions' end offsets."""
    p = subprocess.run(["docker", "run", "--rm", "--network", "cap", "--entrypoint", "kafka-get-offsets",
                        image, "--bootstrap-server", "cap-kafka:29092", "--topic", topic, "--time", "-1"],
                       capture_output=True, text=True, timeout=180)
    if p.returncode != 0:
        raise RuntimeError(f"kafka-get-offsets rc={p.returncode}: {p.stderr.strip()[-500:]}")
    rows = [l.rsplit(":", 2) for l in p.stdout.split() if l.startswith(topic + ":")]
    if not rows:
        raise RuntimeError(f"kafka-get-offsets: no partitions of {topic}: {p.stdout.strip()[-500:]}")
    return sum(int(r[2]) for r in rows), len(rows)


def consumer_cmd(name, topic, image, idle_ms=None):
    cmd = ["docker", "run", "--rm", "--name", name, "--network", "cap", "--cpus", "1", "--memory", "1g",
           "-e", "KAFKA_HEAP_OPTS=-Xmx512m", "--entrypoint", "kafka-console-consumer", image,
           "--bootstrap-server", "cap-kafka:29092", "--topic", topic, "--from-beginning",
           "--property", "print.timestamp=true"]
    return cmd + (["--timeout-ms", str(idle_ms)] if idle_ms else [])


def coverage(inv, n, topic, image):
    """Read the whole retained topic once; write coverage.json."""
    out = {"topic": topic, "active": n, "established": False, "complete_at": None}
    cov, reasons = Coverage(n), []
    try:
        expected, parts = end_offsets(topic, image)
        out.update(expected_records=expected, partitions=parts)
        p = subprocess.Popen(consumer_cmd("cap-cov", topic, image, COVERAGE_IDLE_MS),
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, errors="replace")
        timer = threading.Timer(COVERAGE_TIMEOUT, lambda: subprocess.run(["docker", "stop", "cap-cov"],
                                                                         capture_output=True))
        timer.start()
        for line in p.stdout:
            cov.add(line)
        err = p.stderr.read()
        rc = p.wait()
        timer.cancel()
        m = re.search(r"Processed a total of (\d+) messages", err)
        out.update(consumer_rc=rc, consumer_processed=int(m.group(1)) if m else None)
        if not m:
            reasons.append(f"consumer did not report its total (rc={rc}): {err.strip()[-300:]}")
        if cov.records != expected:
            reasons.append(f"read {cov.records} records of {expected}")
    except Exception as e:
        reasons.append(f"{type(e).__name__}: {e}")
    if cov.errors:
        reasons.append(f"{cov.errors} unreadable records")
    out.update(records=cov.records, row_events=cov.rows, unreadable=cov.errors,
               distinct_tables=cov.distinct(), read_complete=not reasons)
    if not reasons and cov.distinct() != n:
        reasons.append(f"{cov.distinct()} distinct tables, not the active set of {n}")
    out["reasons"] = reasons
    if not reasons:
        out.update(established=True, complete_at=cov.complete_at())
    json.dump(out, open(os.path.join(inv, "coverage.json"), "w"), indent=1)
    return out


def activation(inv, n):
    """The activation boundary, from coverage.json only."""
    try:
        c = json.load(open(os.path.join(inv, "coverage.json")))
    except Exception as e:
        return {"active": n, "established": False, "complete_at": None, "read_complete": False,
                "reasons": [f"coverage.json unreadable: {e}"]}
    ok = bool(c.get("established")) and c.get("distinct_tables") == n and c.get("complete_at") is not None
    return {"active": n, "established": ok, "complete_at": c.get("complete_at") if ok else None,
            "read_complete": bool(c.get("read_complete")), "distinct_tables": c.get("distinct_tables"),
            "reasons": c.get("reasons") or []}


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
    # Delivery is known only when every sample in the window carries it and
    # no gap between samples exceeds MAX_GAP; otherwise it stays unknown.
    incomplete = []
    if any(x["dlv"] is None for x in w):
        incomplete.append(f"{sum(x['dlv'] is None for x in w)} of {len(w)} samples without delivery counters")
    gap = max(w[i]["t"] - w[i - 1]["t"] for i in range(1, len(w)))
    if gap > MAX_GAP or a["t"] - lo > MAX_GAP or hi - b["t"] > MAX_GAP:
        incomplete.append(f"sample gap up to {max(gap, a['t'] - lo, hi - b['t']):.0f}s")
    off, com = (b["off"] - a["off"]) / dt, (b["com"] - a["com"]) / dt
    dlv = None if incomplete else (b["dlv"] - a["dlv"]) / dt
    backlog = None if incomplete else [x["com"] - x["dlv"] for x in w]
    r = [x for x in res if lo <= x["t"] <= hi]
    cpu = {c: (sum(x["cpu"].get(c, 0) for x in r) / len(r) if r else None) for c in ALLOC}
    ticks = [x["harness"] for x in r if x["harness"] is not None]
    hz = os.sysconf("SC_CLK_TCK")
    return {
        "from": a["t"], "to": b["t"], "span_secs": dt, "delivery_incomplete": incomplete,
        "offered_per_sec": off, "committed_per_sec": com, "delivered_per_sec": dlv,
        "offered_to_committed": com / off if off else None,
        "committed_to_delivered": dlv / com if com and dlv is not None else None,
        "backlog_start": backlog[0] if backlog else None, "backlog_end": backlog[-1] if backlog else None,
        "backlog_slope_per_sec": slope([x["t"] for x in w], backlog) if backlog else None,
        "lag_p50": pct([x["lag"] for x in w], 0.5), "lag_p99": pct([x["lag"] for x in w], 0.99),
        "lag_slope": slope([x["t"] for x in w if x["lag"] is not None], [x["lag"] for x in w if x["lag"] is not None]),
        "df_memory_max": max((x["mem"] or 0) for x in w),
        "cpu_used": cpu,
        "harness_cpu": ((ticks[-1] - ticks[0]) / hz / (r[-1]["t"] - r[0]["t"])) if len(ticks) > 1 and r[-1]["t"] > r[0]["t"] else None,
        "host_mem_available_min": min((x["mem"] for x in r if x["mem"] is not None), default=None),
    }


def judge(m):
    """Criteria 1 and 2 (committed to delivered only) and the generator check.
    Callers judge only windows whose delivery is known."""
    assert not m["delivery_incomplete"]
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
    """Criterion 4 and the run's evidence. Returns (failures, incomplete):
    any failure ends the campaign; incomplete evidence never becomes a
    pass, a zero or a failure, and leaves the run inconclusive."""
    bad, incomplete = [], []
    if os.path.exists(os.path.join(inv, "FAILCLOSED")):
        bad.append("pipeline failed (FAILCLOSED)")
    if os.path.exists(os.path.join(inv, "NOHARNESS")):
        bad.append("the harness process was not found, so the run was not observed (NOHARNESS)")
    markers = ("SATURATED-STOP", "GENERATOR-STOP", "ACTIVATION-STOP", "MEMFLOOR", "OOM")
    stopped = any(os.path.exists(os.path.join(inv, f)) for f in markers)
    try:
        stopped = stopped or int(open(os.path.join(inv, "floor-watch.rc")).read()) in (4, 5)
    except Exception:
        pass
    if d.get("outcome") != "completed" and not stopped:
        bad.append(f"outcome {d.get('outcome')}: {d.get('reason')}")
    v = d.get("verdict") or {}
    for k in ("correctness_ok", "evidence_ok"):
        if not v.get(k):
            bad.append(f"{k} false")
    c, st = d.get("completeness"), d.get("stream")
    if not isinstance(c, dict):
        incomplete.append("result.json has no completeness check")
    else:
        for k in ("missing", "duplicates", "unexpected", "final_state_mismatches", "partition_order_violations"):
            if k not in c:
                incomplete.append(f"completeness.{k} absent")
            elif c[k]:
                bad.append(f"{k}={c[k]}")
    if not isinstance(st, dict):
        incomplete.append("result.json has no stream summary")
    else:
        for k in ("decode_errors", "key_mismatches"):
            if k not in st:
                incomplete.append(f"stream.{k} absent")
            elif st[k]:
                bad.append(f"{k}={st[k]}")
    try:
        e = json.load(open(os.path.join(inv, "end-state.json")))
    except Exception as ex:
        incomplete.append(f"end state unreadable: {ex}")
        return bad, incomplete
    if e.get("incidents") is None:
        incomplete.append(f"incidents unknown ({e.get('incidents_read')})")
    elif e["incidents"] != 0:
        bad.append(f"incidents {e['incidents']}")
    if e.get("restarted") is None:
        incomplete.append(f"restart state unknown ({e.get('inspect_read')})")
    elif e["restarted"]:
        bad.append("the instance restarted or stopped")
    return bad, incomplete


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
    out = {"phase": phase, "invocation": inv, "active_tables": n, "evidence_complete": False}
    runs = sorted(glob.glob(os.path.join(inv, "results/exploratory/*/result.json")))
    if not runs:
        out.update(status="inconclusive", reasons=["no result.json"] + resource_stop(inv))
        return out, 3
    run = os.path.dirname(runs[-1])
    d = json.load(open(runs[-1]))
    if not os.path.exists(os.path.join(run, "samples.jsonl")):
        # The harness ended before sampling: nothing measured, never a crash.
        out.update(status="inconclusive", run=run, reasons=["no samples.jsonl (the run ended before sampling)"]
                   + resource_stop(inv))
        return out, 3
    s = load_samples(os.path.join(run, "samples.jsonl"))
    res = resources(os.path.join(inv, "resources.jsonl"))
    try:
        wins = windows_summary(windows_from_log(open(os.path.join(inv, "df.log"), errors="replace")))
    except Exception as e:
        wins = {"unreadable": str(e)}
    act = activation(inv, n)
    start, end = writes_span(s)
    out.update(run=run, activation=act, product_windows=wins, writes_from=start, writes_to=end)
    bad, incomplete = health(d, inv, phase)
    if not act["read_complete"]:
        incomplete += [f"coverage: {r}" for r in act["reasons"]]
    stops = resource_stop(inv)
    if bad:
        out.update(status="failure", reasons=bad + incomplete)
        return out, 2
    out["evidence_incomplete"] = incomplete
    if start is None:
        out.update(status="inconclusive", reasons=["no writes recorded"] + incomplete + stops)
        return out, 3
    if act["established"]:
        out["activation"]["secs"] = act["complete_at"] - start
        out["activation"]["tables_per_sec"] = n / max(act["complete_at"] - start, 1e-9)
    if incomplete:
        out.update(status="inconclusive", reasons=["evidence incomplete"] + incomplete + stops)
        return out, 3
    if phase in ("A", "C"):
        lo = act["complete_at"] if act["established"] else None
        if phase == "C" and lo is not None:
            lo = max(lo, start + C_WARMUP)
        need = A_MEASURED if phase == "A" else C_MEASURED
        if lo is None or end - lo < need - 5:
            out.update(status="inconclusive", evidence_complete=True, reasons=[
                "activation not established: " + "; ".join(act["reasons"]) if lo is None else
                f"measured span after activation {end - lo:.0f}s < {need}s"] + stops)
            if phase == "A":
                out["activation_lag"] = measure(s, res, start, end)
            return out, 3
        m = measure(s, res, lo, end)
        out["during_activation"] = measure(s, res, start, act["complete_at"])
        if m is None or m["delivery_incomplete"]:
            out.update(window=m, status="inconclusive",
                       reasons=["evidence incomplete: delivery unknown in the window"]
                       + (m["delivery_incomplete"] if m else ["fewer than two samples"]) + stops)
            return out, 3
        out["evidence_complete"] = True
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
    steps, first_stop = [], None
    for k in range(1, len(STEPS) + 1):
        if k not in tr:
            steps.append({"step": k, "rate": STEPS[k - 1], "status": "not reached"})
            continue
        lo, hi = tr[k] + SETTLE, tr.get(k + 1, end)
        row = {"step": k, "rate": STEPS[k - 1], "started_at": tr[k], "window": [lo, hi]}
        m = measure(s, res, lo, hi) if hi - lo >= MIN_STEP_SPAN else None
        if not act["established"] or act["complete_at"] > lo:
            row["status"] = "not measured: activation incomplete before its window"
        elif m is None:
            row["status"] = "not measured: window too short (run stopped in the step)"
        elif m["delivery_incomplete"]:
            row.update(metrics=m, status="evidence incomplete: delivery unknown in the window")
        else:
            sat, gen = judge(m)
            row.update(metrics=m, saturated=sat, generator_limited=gen, attribution=attribute(m, sat, gen))
            row["status"] = "saturated" if sat else ("generator-limited" if gen else "viable")
        steps.append(row)
        if first_stop is None and (row["status"] in ("saturated", "generator-limited")
                                   or row["status"].startswith("evidence incomplete")):
            first_stop = row
    out["steps"] = steps
    # Reported separately: the highest rate any measured step demonstrated
    # viable; the viable steps below the first step that saturated, was
    # generator-limited or had unknown delivery; and that step. A ceiling is
    # bracketed only by a saturated step; an incomplete step gives no
    # conclusion at its rate, so the rate below it is not a ceiling.
    demonstrated = [r["rate"] for r in steps if r["status"] == "viable"]
    out["highest_demonstrated_viable"] = max(demonstrated) if demonstrated else None
    viable = [r["rate"] for r in steps if r["status"] == "viable" and (first_stop is None or r["step"] < first_stop["step"])]
    out["highest_viable"] = max(viable) if viable else None
    out["lowest_saturated"] = first_stop["rate"] if first_stop and first_stop["status"] == "saturated" else None
    if first_stop is not None and first_stop["status"].startswith("evidence incomplete"):
        out["no_conclusion_at"] = first_stop["rate"]
    out["ceiling_bracketed"] = out["lowest_saturated"] is not None
    out["evidence_complete"] = not any(r["status"].startswith("evidence incomplete") for r in steps)
    if stops and first_stop is None:
        out["resource_stop"] = stops
    if first_stop is not None:
        out["status"] = "inconclusive" if first_stop["status"].startswith("evidence incomplete") else first_stop["status"]
        return out, 1 if first_stop["status"] == "saturated" else 3
    if any(x.startswith("3:") for x in stops):
        out["status"] = "saturated"
        out["saturated"] = stops
        return out, 1
    out["status"] = "viable" if out["highest_viable"] == STEPS[-1] else "inconclusive"
    return out, 0 if out["status"] == "viable" else 3


def container_memory(inv, lo, hi):
    """Each container's peak memory (bytes) over [lo, hi], from docker stats."""
    units = {"B": 1, "KiB": 2 ** 10, "MiB": 2 ** 20, "GiB": 2 ** 30}
    peak = {}
    path = os.path.join(inv, "resources.jsonl")
    if not os.path.exists(path):
        return peak
    for line in open(path):
        r = json.loads(line)
        if not lo <= ts(r["at"]) <= hi:
            continue
        for st in r.get("docker_stats") or []:
            m = re.match(r"([\d.]+)([KMG]?i?B)", st.get("MemUsage", ""))
            if m and m.group(2) in units:
                v = float(m.group(1)) * units[m.group(2)]
                peak[st["Name"]] = max(peak.get(st["Name"], 0), v)
    return peak


def drain_readout(inv, n):
    """The backlog drain (phase DRN, harness scenario D1): correctness and
    health over the whole run, activation before the pause, and the drain
    from the resume request to the last expected event (`backlog-drain.json`,
    written by the harness)."""
    out = {"phase": "DRN", "invocation": inv, "active_tables": n, "evidence_complete": False,
           "scheduled_pause": "expected: the pipelines are stopped by the scenario, not failed"}
    runs = sorted(glob.glob(os.path.join(inv, "results/exploratory/*/result.json")))
    if not runs:
        out.update(status="inconclusive", reasons=["no result.json"] + resource_stop(inv))
        return out, 3
    run = os.path.dirname(runs[-1])
    d = json.load(open(runs[-1]))
    try:
        rep = json.load(open(os.path.join(run, "backlog-drain.json")))
    except Exception as e:
        out.update(status="inconclusive", run=run, reasons=[f"backlog-drain.json unreadable: {e}"] + resource_stop(inv))
        return out, 3
    s = load_samples(os.path.join(run, "samples.jsonl")) if os.path.exists(os.path.join(run, "samples.jsonl")) else []
    res = resources(os.path.join(inv, "resources.jsonl"))
    act = activation(inv, n)
    start, _ = writes_span(s)
    out.update(run=run, activation=act, backlog=rep)
    bad, incomplete = health(d, inv, "DRN")
    if not act["read_complete"]:
        incomplete += [f"coverage: {r}" for r in act["reasons"]]
    # A drain stopped at its bound leaves rows undelivered: reported as an
    # incomplete drain with the ledger's counts, not as proven loss. Any
    # other finding (duplicates, unexpected rows, order, decoding, a failed
    # pipeline, an incident or a restart) still fails the run.
    c = d.get("completeness") or {}
    out["ledger"] = {k: c.get(k) for k in ("missing", "duplicates", "unexpected", "final_state_mismatches",
                                         "partition_order_violations")}
    if rep.get("outcome") == "drain incomplete":
        undelivered = ("missing=", "final_state_mismatches=", "correctness_ok false", "evidence_ok false")
        bad = [b for b in bad if not b.startswith(undelivered)]
    if not rep.get("checkpoint_preserved"):
        bad.append("the durable checkpoint was not preserved across the pause")
    if rep.get("outcome") in ("pipeline failed", "resume failed", "not resumed"):
        bad.append(f"drain {rep.get('outcome')}: {rep.get('failure')}")
    if bad:
        out.update(status="failure", reasons=bad + incomplete)
        return out, 2
    t_pause = ts(rep["paused"]["at"])
    t_resume = ts(rep["resume_requested_at"])
    secs = rep.get("complete_after_secs")
    t_end = t_resume + secs if secs else ts(rep["metrics_at_end"]["at"]) if rep.get("metrics_at_end") else None
    if act["established"] and start is not None:
        out["activation"]["secs"] = act["complete_at"] - start
        out["activation"]["before_pause"] = act["complete_at"] <= t_pause
    out["steady_before_pause"] = measure(s, res, act["complete_at"] or start or t_pause, t_pause) if s else None
    m = measure(s, res, t_resume, t_end) if s and t_end else None
    a, b = rep.get("metrics_at_resume"), rep.get("metrics_at_end")
    scr = None
    if a and b and ts(b["at"]) > ts(a["at"]):
        dt = ts(b["at"]) - ts(a["at"])
        scr = {"secs": dt, "sink_events": b["sink_events"] - a["sink_events"], "sink_bytes": b["sink_bytes"] - a["sink_bytes"],
               "events_per_sec": (b["sink_events"] - a["sink_events"]) / dt,
               "bytes_per_sec": (b["sink_bytes"] - a["sink_bytes"]) / dt}
    # The 5-second samples bracketing the drain: the window is known to
    # within one sample period at each end.
    samp = None
    inside = [x for x in s if x["dlv"] is not None and t_resume <= x["t"] <= (t_end or t_resume)]
    if secs and len(inside) >= 2:
        span = inside[-1]["t"] - inside[0]["t"]
        moved = inside[-1]["dlv"] - inside[0]["dlv"]
        samp = {"from": inside[0]["t"], "to": inside[-1]["t"], "span_secs": span, "delivered": moved,
                "events_per_sec": moved / span if span else None,
                "rate_bounds_events_per_sec": [rep["backlog_events"] / (secs + 5.0), rep["backlog_events"] / max(secs - 5.0, 1e-9)]}
    out["drain"] = {
        "outcome": rep["outcome"], "backlog_events": rep["backlog_events"], "expected_row_events": rep["expected_row_events"],
        "delivered_at_resume": rep["delivered_at_resume"], "delivered_at_end": rep["delivered_at_end"],
        "uncertain_ops": rep.get("uncertain_ops"),
        "startup_reload_secs": rep.get("running_after_secs"), "first_delivery_secs": rep.get("first_delivery_after_secs"),
        "complete_secs": secs, "completion_resolution_secs": rep.get("poll_secs"),
        "backlog_events_per_sec": rep["backlog_events"] / secs if secs else None,
        "scrapes": scr, "samples": samp, "window": m,
        "memory_peak_bytes": container_memory(inv, t_resume, t_end) if t_end else None,
    }
    out["evidence_complete"] = not incomplete and m is not None and scr is not None
    if incomplete:
        out.update(status="inconclusive", reasons=["evidence incomplete"] + incomplete + resource_stop(inv))
        return out, 3
    out["status"] = "drained" if rep["outcome"] == "drained" else "drain incomplete"
    return out, 0 if out["status"] == "drained" else 3


def mysql_rows(inv):
    """(t, MySQL counters, MySQL memory bytes) from the watcher's samples."""
    out = []
    path = os.path.join(inv, "resources.jsonl")
    if not os.path.exists(path):
        return out
    units = {"KiB": 2 ** 10, "MiB": 2 ** 20, "GiB": 2 ** 30, "B": 1}
    for line in open(path):
        r = json.loads(line)
        mem = None
        for st in r.get("docker_stats") or []:
            if st.get("Name") == "cap-mysql":
                m = re.match(r"([\d.]+)(KiB|MiB|GiB|B)", st.get("MemUsage", ""))
                mem = float(m.group(1)) * units[m.group(2)] if m else None
        out.append((ts(r["at"]), r.get("mysql_status"), mem))
    return out


def r2_applied(inv):
    try:
        return json.load(open(os.path.join(inv, "R2-APPLIED")))
    except Exception:
        return None


def calibration_step(s, res, mrows, lo, hi, r2):
    """One step's generator readout over [lo, hi]."""
    m = measure(s, res, lo, hi)
    if m is None:
        return None
    alloc = R2_CPUS if r2 and r2["at"] <= lo else ALLOC["cap-mysql"]
    rows = [(t, st, mem) for t, st, mem in mrows if lo <= t <= hi]
    known = [(t, st) for t, st, _ in rows if st]
    rates = {}
    if len(known) >= 2 and known[-1][0] > known[0][0]:
        (a, sa), (b, sb) = known[0], known[-1]
        rates = {k: (sb[k] - sa[k]) / (b - a) for k in sa if k in sb}
    mysql_cpu = m["cpu_used"].get("cap-mysql")
    ratio = m["offered_to_committed"]
    return {
        "from": lo, "to": hi, "offered_per_sec": m["offered_per_sec"], "committed_per_sec": m["committed_per_sec"],
        "delivered_per_sec": m["delivered_per_sec"], "committed_to_offered": ratio,
        "generator_limited": ratio is not None and ratio < DELIVERY_RATIO,
        "mysql_cpus": alloc, "mysql_cpu_used": mysql_cpu,
        "mysql_at_allocation": mysql_cpu is not None and mysql_cpu >= BUSY * alloc,
        "mysql_mixed_allocation": bool(r2 and lo < r2["at"] < hi),
        "mysql_rates_per_sec": rates,
        "mysql_memory_max": max((mem for *_, mem in rows if mem is not None), default=None),
        "df_memory_max": m["df_memory_max"], "lag_p99": m["lag_p99"], "host_mem_available_min": m["host_mem_available_min"],
    }


def calibrate(inv, n):
    """The calibration's readout (`calibration` in verdict.json)."""
    out = {"phase": "CAL", "invocation": inv, "active_tables": n, "evidence_complete": False}
    runs = sorted(glob.glob(os.path.join(inv, "results/exploratory/*/result.json")))
    if not runs:
        out.update(status="inconclusive", reasons=["no result.json"] + resource_stop(inv))
        return out, 3
    run = os.path.dirname(runs[-1])
    d = json.load(open(runs[-1]))
    if not os.path.exists(os.path.join(run, "samples.jsonl")):
        # The harness ended before sampling: nothing measured, never a crash.
        out.update(status="inconclusive", run=run, reasons=["no samples.jsonl (the run ended before sampling)"]
                   + resource_stop(inv))
        return out, 3
    s = load_samples(os.path.join(run, "samples.jsonl"))
    res = resources(os.path.join(inv, "resources.jsonl"))
    mrows = mysql_rows(inv)
    r2 = r2_applied(inv)
    bad, incomplete = health(d, inv, "CAL")
    act = activation(inv, n)
    start, end = writes_span(s)
    out.update(run=run, activation=act, writes_from=start, writes_to=end, r2=r2)
    if bad:
        out.update(status="failure", reasons=bad + incomplete)
        return out, 2
    if start is None:
        out.update(status="inconclusive", reasons=["no writes recorded"] + incomplete)
        return out, 3
    tr = transitions(s, start)
    if 2 in tr:
        tr[1] = tr[2] - STEP_SECS
    else:
        tr[1] = start
    steps = []
    for k in range(1, 4):
        if k not in tr:
            steps.append({"step": k, "rate": STEPS[k - 1], "status": "not reached"})
            continue
        lo, hi = tr[k] + SETTLE, tr.get(k + 1, end)
        row = calibration_step(s, res, mrows, lo, hi, r2) if hi - lo >= MIN_STEP_SPAN else None
        steps.append({"step": k, "rate": STEPS[k - 1], "started_at": tr[k], **(row or {"status": "window too short"})})
    out["steps"] = steps
    out["evidence_complete"] = not incomplete
    met = [r for r in steps if "committed_to_offered" in r and not r["mysql_mixed_allocation"]]
    kept_up = [r["rate"] for r in met if not r["generator_limited"]]
    out["highest_rate_kept_up"] = max(kept_up) if kept_up else None
    r1 = [r for r in met if r["mysql_cpus"] == ALLOC["cap-mysql"]]
    r1_limited = [r["rate"] for r in r1 if r["generator_limited"]]
    if r1_limited:
        out["r1_insufficient_at"] = min(r1_limited)

    def why(r):
        return "MySQL at its allocation" if r["mysql_at_allocation"] else "MySQL not at its allocation"

    if r2:
        after = [r for r in met if r["mysql_cpus"] == R2_CPUS]
        limited = [r for r in after if r["generator_limited"]]
        if limited:
            out.update(status="environment limit", remedy="R2", reasons=[
                f"with R2, committed under 95% of offered at {limited[0]['rate']}/s ({why(limited[0])})"])
            return out, 3
        if not after:
            out.update(status="inconclusive", remedy="R2", reasons=["no step measured after R2"])
            return out, 3
        out.update(status="generator sufficient", remedy="R2")
        return out, 0
    limited = [r for r in r1 if r["generator_limited"]]
    if not limited:
        out.update(status="generator sufficient", remedy="R1")
        return out, 0
    last = limited[0]
    if last["mysql_at_allocation"]:
        out.update(status="inconclusive", remedy="R1", reasons=[
            f"with R1, committed under 95% of offered at {last['rate']}/s (MySQL at its allocation); "
            "R2 not tested (no later step)"])
        return out, 3
    out.update(status="environment limit", remedy="R1", reasons=[
        f"committed under 95% of offered at {last['rate']}/s ({why(last)})"])
    return out, 3


def live_calibrate(inv, pid):
    """Apply R2 once after the first MySQL-limited step; stop on the next
    limited step (or one where MySQL is not at its allocation)."""
    seen = 0
    while True:
        try:
            os.kill(pid, 0)
        except OSError:
            return
        paths = glob.glob(os.path.join(inv, "results/exploratory/*/samples.jsonl"))
        if paths:
            s = load_samples(paths[-1])
            res = resources(os.path.join(inv, "resources.jsonl"))
            mrows = mysql_rows(inv)
            start, _ = writes_span(s)
            tr = transitions(s, start) if start else {}
            for k in range(1, 3):
                if k + 1 not in tr or k <= seen:
                    continue
                seen = k
                lo = (tr[k] if k > 1 else tr[2] - STEP_SECS) + SETTLE
                hi = tr[k + 1]
                r2 = r2_applied(inv)
                row = calibration_step(s, res, mrows, lo, hi, r2) if hi - lo >= MIN_STEP_SPAN else None
                if row is None or not row["generator_limited"]:
                    continue
                if row["mysql_at_allocation"] and r2 is None:
                    rc = subprocess.run(["docker", "update", "--cpus", str(R2_CPUS), "cap-mysql"],
                                        capture_output=True, text=True)
                    json.dump({"at": time.time(), "after_step": k, "rate": STEPS[k - 1], "rc": rc.returncode,
                               "stderr": rc.stderr.strip(), "step": row}, open(os.path.join(inv, "R2-APPLIED"), "w"), indent=1)
                    print(f"R2 applied after step {k} ({STEPS[k - 1]}/s): rc={rc.returncode}", flush=True)
                    continue
                json.dump({"step": k, "rate": STEPS[k - 1], "generator_limited": True, "metrics": row,
                           "at": time.time()}, open(os.path.join(inv, "GENERATOR-STOP"), "w"), indent=1)
                os.kill(pid, signal.SIGINT)
                return
        time.sleep(5)


def live(inv, n, pid, topic, image):
    """Phase B: stop the harness after the first saturated or
    generator-limited step whose window followed complete coverage, or when
    step 2's window starts with coverage incomplete. A consumer follows the
    topic only until coverage is complete, so it reads during warm-up and
    not during the measured steps."""
    cov, seen = Coverage(n), 0
    reader = {"p": None}

    def follow():
        while reader["p"] is not False:
            p = subprocess.Popen(consumer_cmd("cap-cov-live", topic, image), stdout=subprocess.PIPE,
                                 stderr=subprocess.DEVNULL, text=True, errors="replace")
            if reader["p"] is False:   # stopped while starting: stop this one too
                time.sleep(5)
                subprocess.run(["docker", "stop", "cap-cov-live"], capture_output=True)
                p.wait()
                return
            reader["p"] = p
            for line in p.stdout:
                cov.add(line)
            p.wait()
            time.sleep(5)

    def stop_reader():
        reader["p"] = False
        subprocess.run(["docker", "stop", "cap-cov-live"], capture_output=True)

    threading.Thread(target=follow, daemon=True).start()
    try:
        while True:
            try:
                os.kill(pid, 0)
            except OSError:
                return
            done = cov.complete_at()
            if done is not None and reader["p"] is not False:
                stop_reader()
                print(f"coverage complete: {cov.distinct()} distinct tables, complete_at={done}, "
                      f"records={cov.records}, unreadable={cov.errors}", flush=True)
            paths = glob.glob(os.path.join(inv, "results/exploratory/*/samples.jsonl"))
            if paths:
                s = load_samples(paths[-1])
                res = resources(os.path.join(inv, "resources.jsonl"))
                start, _ = writes_span(s)
                tr = transitions(s, start) if start else {}
                # Only once the consumer has read past step 2's window start,
                # so a lagging reader is never taken for missing coverage.
                if done is None and 2 in tr and (cov.latest or 0) > tr[2] + SETTLE:
                    json.dump({"distinct_tables": cov.distinct(), "active": n, "records": cov.records, "read_to": cov.latest,
                               "at": time.time()}, open(os.path.join(inv, "ACTIVATION-STOP"), "w"), indent=1)
                    os.kill(pid, signal.SIGINT)
                    return
                for k in range(1, len(STEPS)):
                    # A step stays undecided until coverage is known.
                    if k + 1 not in tr or k <= seen or done is None:
                        continue
                    seen = k
                    lo, hi = tr[k] + SETTLE, tr[k + 1]
                    if done > lo or hi - lo < MIN_STEP_SPAN:
                        continue
                    m = measure(s, res, lo, hi)
                    if m is None or m["delivery_incomplete"]:
                        continue
                    sat, gen = judge(m)
                    if sat or gen:
                        marker = "SATURATED-STOP" if sat else "GENERATOR-STOP"
                        json.dump({"step": k, "rate": STEPS[k - 1], "saturated": sat, "generator_limited": gen,
                                   "metrics": m, "at": time.time()}, open(os.path.join(inv, marker), "w"), indent=1)
                        os.kill(pid, signal.SIGINT)
                        return
            time.sleep(5)
    finally:
        if reader["p"] is not False:
            stop_reader()


if __name__ == "__main__":
    mode, inv, n = sys.argv[1], os.path.abspath(sys.argv[2]), int(sys.argv[3])
    if mode == "coverage":
        c = coverage(inv, n, sys.argv[4], sys.argv[5])
        print("coverage", " ".join(f"{k}={c.get(k)}" for k in ("established", "distinct_tables", "records",
                                                              "expected_records", "complete_at", "reasons")))
        sys.exit(0 if c["established"] else 3)
    if len(sys.argv) > 7 and sys.argv[4] == "--live":
        if mode == "CAL":
            live_calibrate(inv, int(sys.argv[5]))
        else:
            live(inv, n, int(sys.argv[5]), sys.argv[6], sys.argv[7])
        sys.exit(0)
    out, rc = (calibrate(inv, n) if mode == "CAL" else drain_readout(inv, n) if mode == "DRAIN"
               else evaluate(mode, inv, n))
    # The run's topic may be deleted only when the evaluation is complete:
    # no failure, every piece of evidence read, and coverage established.
    out["topic_deletable"] = (out.get("status") != "failure" and out.get("evidence_complete") is True
                              and out.get("activation", {}).get("established") is True)
    json.dump(out, open(os.path.join(inv, "verdict.json"), "w"), indent=1)
    keys = ("status", "remedy", "highest_rate_kept_up", "evidence_complete", "highest_demonstrated_viable", "highest_viable", "lowest_saturated",
            "no_conclusion_at", "attribution", "saturated", "reasons")
    print(mode, " ".join(f"{k}={out[k]}" for k in keys if out.get(k) is not None), f"verdict={inv}/verdict.json")
    sys.exit(rc)

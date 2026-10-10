#!/usr/bin/env python3
"""Offline report of a proof-trace evidence run.

Usage: proof-trace-report.py <run>.jsonl <run>-meta.json

The JSONL holds the `deltaforge::proof_trace` records (`scan` and `proof`);
the meta file the server's binlog inventory, UUID and executed set, captured
once after the run. Byte spans and overlap are computed here, never by the
source.

- requested interval (A, B]: as the proof asked for it;
- distinct bytes: the union of all requested intervals in global binlog
  byte offsets (file order from the inventory);
- distinct transactions (GTID mode): the union of the source server UUID's
  GNO intervals (other UUIDs ignored);
- scanned: what the scans received (events, bytes);
- amplification: scanned / distinct. Distinct events are an estimate
  (scanned events scaled by distinct / received bytes) and labelled so.
"""

import json
import sys
from collections import defaultdict


def load(path):
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def union(intervals):
    """Total length of the union of half-open (a, b] intervals."""
    total, cur_a, cur_b = 0, None, None
    for a, b in sorted(i for i in intervals if i[1] > i[0]):
        if cur_b is None or a > cur_b:
            if cur_b is not None:
                total += cur_b - cur_a
            cur_a, cur_b = a, b
        else:
            cur_b = max(cur_b, b)
    if cur_b is not None:
        total += cur_b - cur_a
    return total


def pct(values, q):
    if not values:
        return None
    v = sorted(values)
    return v[min(len(v) - 1, int(q * (len(v) - 1) + 0.5))]


def gno_max(gtid_set, uuid):
    """The source UUID's highest GNO, if its intervals are contiguous from 1."""
    if not gtid_set:
        return 0
    for entry in gtid_set.replace("\n", "").split(","):
        parts = entry.strip().split(":")
        if parts[0].lower() != uuid.lower():
            continue
        ivs = []
        for iv in parts[1:]:
            lo, _, hi = iv.partition("-")
            ivs.append((int(lo), int(hi or lo)))
        ivs.sort()
        if ivs[0][0] != 1 or any(b[0] != a[1] + 1 for a, b in zip(ivs, ivs[1:])):
            return None
        return ivs[-1][1]
    return 0


def main(jsonl, meta_path):
    recs = load(jsonl)
    meta = json.load(open(meta_path))
    base, acc = {}, 0
    for name, size in meta["binlogs"]:
        base[name] = acc
        acc += size

    def off(p):
        if not p or not p.get("file") or p["file"] not in base:
            return None
        return base[p["file"]] + p["pos"]

    uuid, gtid = meta["server_uuid"], meta["mode"] == "gtid"
    scans = [r for r in recs if r["record"] == "scan"]
    proofs = [r for r in recs if r["record"] == "proof"]
    by_kind = defaultdict(list)
    for s in scans:
        by_kind[s["kind"]].append(s)

    print(f"# Proof-trace evidence: {meta['mode']} mode, {meta['tables']} tables")
    print(
        f"gap {meta['gap']} txns, sustained {meta['rate']} txn/s, "
        f"catch-up {meta['catch_up_s']:.1f} s; {len(scans)} scans, {len(proofs)} proofs\n"
    )

    def summary(name, ss):
        ok = [s for s in ss if s["outcome"] == "ok"]
        req = [(off(s["from"]), off(s["to"])) for s in ok]
        req = [(a, b) for a, b in req if a is not None and b is not None]
        distinct_b = union(req)
        requested_b = sum(b - a for a, b in req)
        recv_b = sum(s["bytes"] for s in ok)
        recv_e = sum(s["events"] for s in ok)
        row = {
            "kind": name,
            "scans": len(ss),
            "failed": len(ss) - len(ok),
            "requested_bytes": requested_b,
            "distinct_bytes": distinct_b,
            "received_bytes": recv_b,
            "received_events": recv_e,
            "bytes_amplification": recv_b / distinct_b if distinct_b else None,
            "distinct_events_est": round(recv_e * distinct_b / recv_b)
            if recv_b
            else None,
        }
        if gtid:
            tx = []
            for s in ok:
                a = gno_max(s["from"]["gtid_set"], uuid)
                b = gno_max(s["to"]["gtid_set"], uuid)
                if a is not None and b is not None:
                    tx.append((a, b))
            row["requested_txns"] = sum(b - a for a, b in tx)
            row["distinct_txns"] = union(tx)
            row["txns_amplification"] = (
                row["requested_txns"] / row["distinct_txns"]
                if row["distinct_txns"]
                else None
            )
        before = [
            s["detail"]["before_start"] for s in ok if s.get("detail")
        ]
        row["before_start_events"] = sum(before)
        row["scans_with_before_start"] = sum(1 for b in before if b)
        if not gtid:
            row["terminal_not_at_B"] = sum(
                1
                for s in ok
                if s.get("terminal")
                and (s["terminal"]["file"], s["terminal"]["end_pos"])
                != (s["to"]["file"], s["to"]["pos"])
            )
        row["no_terminal"] = sum(
            1 for s in ok if s["events"] and not s.get("terminal")
        )
        for t in ("setup_ms", "seek_ms", "scan_ms"):
            vals = [s[t] for s in ok]
            row[t] = {
                "p50": pct(vals, 0.5),
                "p90": pct(vals, 0.9),
                "p99": pct(vals, 0.99),
                "max": max(vals) if vals else None,
                "sum": sum(vals),
            }
        return row

    rows = [summary(k, v) for k, v in sorted(by_kind.items())]
    rows.append(summary("all", scans))
    print("```json")
    print(json.dumps(rows, indent=1))
    print("```\n")

    reqs = [r for r in recs if r["record"] == "request"]
    if reqs:
        print("## Shared scanner requests")
        scan_by_id = {s["scan_id"]: s for s in scans}
        for kind in sorted({r["kind"] for r in reqs}) + ["all"]:
            rs = [r for r in reqs if kind == "all" or r["kind"] == kind]
            spans = [(off(r["from"]), off(r["to"])) for r in rs if r["served"] != "failed"]
            spans = [(a, b) for a, b in spans if a is not None and b is not None]
            distinct = union(spans)
            phys_b = sum(r["bytes"] for r in rs)
            phys_e = sum(r["events"] for r in rs)
            served = defaultdict(int)
            for r in rs:
                served[r["served"]] += 1
            ext = sum(len(r["scan_ids"]) for r in rs)
            row = {
                "kind": kind,
                "requests": len(rs),
                "served": dict(served),
                "extensions": ext,
                "requested_bytes": sum(b - a for a, b in spans),
                "distinct_bytes": distinct,
                "physical_bytes": phys_b,
                "physical_events": phys_e,
                "amplification": phys_b / distinct if distinct else None,
                "peak_record_statements": max((r["record_statements"] or 0) for r in rs),
                "peak_record_bytes": max((r["record_bytes"] or 0) for r in rs),
            }
            if gtid:
                tx = []
                for r in rs:
                    if r["served"] == "failed":
                        continue
                    a = gno_max(r["from"]["gtid_set"], uuid)
                    b = gno_max(r["to"]["gtid_set"], uuid)
                    if a is not None and b is not None:
                        tx.append((a, b))
                row["distinct_txns"] = union(tx)
                # GTID mode: positions' file offsets are diagnostic only (not
                # read atomically with the GTID set); amplification counts
                # the source server's transactions.
                phys_tx = sum(
                    (scan_by_id.get(i, {}).get("detail") or {}).get("source_txns", 0)
                    for r in rs
                    for i in r["scan_ids"]
                )
                row["physical_txns"] = phys_tx
                row["amplification"] = (
                    phys_tx / row["distinct_txns"] if row["distinct_txns"] else None
                )
                row["amplification_unit"] = "source transactions"
            else:
                row["amplification_unit"] = "bytes"
            print(json.dumps(row))
        print()

    outcomes = defaultdict(lambda: defaultdict(int))
    lock, schema = defaultdict(list), defaultdict(list)
    for p in proofs:
        outcomes[p["kind"]][p["outcome"]] += 1
        if p.get("capture"):
            lock[p["kind"]].append(p["capture"]["lock_wait_ms"])
            schema[p["kind"]].append(p["capture"]["schema_ms"])
    print("## Proof outcomes and capture timing (ms)")
    for k in sorted(outcomes):
        print(
            f"- {k}: {dict(outcomes[k])}; lock wait p50/p99/max "
            f"{pct(lock[k], .5)}/{pct(lock[k], .99)}/{max(lock[k], default=None)}; "
            f"schema p50/p99/max {pct(schema[k], .5)}/{pct(schema[k], .99)}/"
            f"{max(schema[k], default=None)}"
        )


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])

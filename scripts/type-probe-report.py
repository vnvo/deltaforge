#!/usr/bin/env python3
"""Report of a MySQL type-probe run (crates/runner/tests/mysql_type_probe.rs).

Usage: type-probe-report.py <probe output dir>  > report.md

For every probed value (table, column, row): the SQL literal and MySQL's
column metadata, then what each path emitted, as `type:value`:

- JSON snapshot: the snapshot row (`op r`, source snapshot `initial`).
- JSON CDC: the CDC insert's after image. Update after / before and delete
  before images of the same value are compared with it (`images` flag).
- Avro / Parquet: CDC insert after image from the `never` (CDC-only) runs
  (typed sinks halt on snapshot rows; see the pipeline table).

Flags: `snap=cdc` snapshot JSON equals CDC JSON (value and JSON type);
`min=full` CDC JSON is identical under binlog_row_metadata MINIMAL and FULL;
`images` every CDC image of the value (insert after, update before/after,
delete before) is identical. `-` means not observed.
"""

import glob
import json
import os
import sys
from collections import defaultdict

MARKER = 9999


def load_jsonl(path):
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def render(tagged):
    """A tagged value as `type:value`."""
    if tagged is None:
        return "-"
    t = tagged.get("type")
    if "hex" in tagged:
        v = "0x" + tagged["hex"]
    elif isinstance(tagged.get("value"), dict) and "hex" in tagged["value"]:
        v = "0x" + tagged["value"]["hex"]
    else:
        v = json.dumps(tagged.get("value"), ensure_ascii=False)
    if len(v) > 60:
        v = v[:57] + "..."
    return f"{t}:{v}".replace("|", "\\|")


def same(a, b):
    if a is None or b is None:
        return None
    return a == b


def flag(x):
    return "-" if x is None else ("yes" if x else "**NO**")


def main(out):
    env = json.load(open(os.path.join(out, "env.json")))
    schema = json.load(open(os.path.join(out, "schema-minimal_initial.json")))
    info = {(c["table"], c["column"]): c for c in schema["information_schema"]}
    tables = schema["rows"]

    # obs[(meta, snapshot, sink, table, column, label, path)] = tagged value
    obs = {}
    for path in glob.glob(os.path.join(out, "records-*.jsonl")):
        for line in load_jsonl(path):
            t = next(x for x in tables if x["table"] == line["table"])
            n = len(t["rows"])
            labels = [r["label"] for r in t["rows"]]
            rid = line["id"]
            if rid is None or rid == MARKER:
                continue
            rec = line["record"]
            op = rec.get("op")
            k = rid - 101 if rid >= 101 else rid - 1
            for image, cols in rec.get("images", []):
                if op == "r" and image == "after":
                    where, label = "snapshot", labels[k]
                elif op == "c" and image == "after":
                    where, label = "insert", labels[k]
                elif op == "u" and image == "before":
                    where, label = "update_before", labels[k]
                elif op == "u" and image == "after":
                    where, label = "update_after", labels[(k + 1) % n]
                elif op == "d" and image == "before":
                    where, label = "delete_before", labels[(k + 1) % n]
                else:
                    continue
                for col, tagged in cols.items():
                    key = (line["meta"], line["snapshot"], line["sink"],
                           line["table"], col, label, where)
                    obs[key] = tagged

    print("# MySQL type probe: raw comparison\n")
    print(f"Probe run: `{os.path.basename(os.path.normpath(out))}`\n")
    print("## Environment\n")
    print("| setting | value |\n|---|---|")
    for k, v in env.items():
        print(f"| `{k}` | `{v}` |")
    print()

    print("## Pipeline outcomes\n")
    print("| run | sink | table | status | records / expected | first error |")
    print("|---|---|---|---|---|---|")
    warnings = ""
    wpath = os.path.join(out, "deltaforge-warnings.log")
    if os.path.exists(wpath):
        warnings = open(wpath).read().splitlines()
    for path in sorted(glob.glob(os.path.join(out, "pipelines-*.json"))):
        run = os.path.basename(path)[len("pipelines-"):-len(".json")]
        for name, p in sorted(json.load(open(path)).items()):
            err = ""
            if p["status"] != "running":
                for w in warnings:
                    if (f"pipeline={name} " in w or f"pipeline={name}\n" in w) and (
                        "error=" in w):
                        err = w.split("error=", 1)[1][:110]
                        break
            print(f"| {run} | {p['sink']} | {p['table']} | {p['status']} | "
                  f"{p['records']} / {p['expected']} | {err.replace('|', '/')} |")
    print()

    print("## SQL statements refused or warned\n")
    for path in sorted(glob.glob(os.path.join(out, "sql-*.jsonl"))):
        if "minimal_initial" not in path:
            continue
        for s in load_jsonl(path):
            if not s["ok"] or s["warnings"]:
                what = s["error"] if not s["ok"] else "; ".join(
                    w[2] for w in s["warnings"])
                print(f"- `{s['sql'][:110]}` ({'refused' if not s['ok'] else 'warned'}"
                      f", sql_mode `{s['sql_mode'] or ''}`): {what}")
    print()

    for t in tables:
        name = t["table"]
        print(f"## {name}\n")
        print("| column | row | SQL | MySQL type | JSON snapshot | JSON CDC | snap=cdc"
              " | min=full | images | Avro CDC | Parquet CDC |")
        print("|---|---|---|---|---|---|---|---|---|---|---|")
        for ci, (col, _ty) in enumerate(t["columns"]):
            meta_col = info.get((name, col), {})
            mtype = meta_col.get("column_type", "")
            cs = meta_col.get("charset")
            if cs:
                mtype += f" {cs}/{meta_col.get('collation')}"
            for r in t["rows"]:
                label = r["label"]
                get = lambda meta, snap, sink, where: obs.get(
                    (meta, snap, sink, name, col, label, where))
                snap_v = get("MINIMAL", "initial", "json", "snapshot")
                cdc_v = get("MINIMAL", "never", "json", "insert") or get(
                    "MINIMAL", "initial", "json", "insert")
                cdc_full = get("FULL", "never", "json", "insert") or get(
                    "FULL", "initial", "json", "insert")
                images = [get(m, s, "json", w)
                          for m in ("MINIMAL", "FULL")
                          for s in ("initial", "never")
                          for w in ("insert", "update_before", "update_after",
                                    "delete_before")]
                images = [x for x in images if x is not None]
                img_ok = None if not images else all(x == images[0] for x in images)
                avro = get("MINIMAL", "never", "avro", "insert")
                parq = get("MINIMAL", "never", "parquet", "insert")
                sql = r["sql"][ci].replace("|", "\\|")
                if len(sql) > 40:
                    sql = sql[:37] + "..."
                print(f"| {col} | {label} | `{sql}` | {mtype} | {render(snap_v)} | "
                      f"{render(cdc_v)} | {flag(same(snap_v, cdc_v))} | "
                      f"{flag(same(cdc_v, cdc_full))} | {flag(img_ok)} | "
                      f"{render(avro)} | {render(parq)} |")
        print()

    # Typed-sink consistency across MINIMAL / FULL and images.
    print("## Typed sinks: CDC image and metadata consistency\n")
    for sink in ("avro", "parquet"):
        diffs = []
        for (meta, snap, s, table, col, label, where), v in obs.items():
            if s != sink or snap != "never" or where != "insert" or col == "id":
                continue
            for other_meta in ("MINIMAL", "FULL"):
                for w in ("update_before", "update_after", "delete_before"):
                    o = obs.get((other_meta, "never", sink, table, col, label, w))
                    if o is not None and o != v:
                        diffs.append(f"{table}.{col} {label}: {meta} insert {render(v)}"
                                     f" vs {other_meta} {w} {render(o)}")
        print(f"- {sink}: {len(diffs)} differences")
        for d in sorted(set(diffs))[:20]:
            print(f"  - {d}")
    print()


if __name__ == "__main__":
    main(sys.argv[1])

#!/usr/bin/env bash
# The harness's own check (outside the core gate): its unit tests, then a
# 10-DDL unloaded comparison of one build against itself on the PostgreSQL
# store, which must drain authoritatively with the ledger equal in every
# run. Run before every measured series and after any harness change.
#
#   smoke.sh <build-dir> <out-dir>
set -euo pipefail
BUILD=$1 OUT=$2
ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
cd "$ROOT"
cargo test -q -p perf-harness --bin actdiag
cargo build -q --release -p perf-harness --bin actdiag
target/release/actdiag --build a="$BUILD" --build b="$BUILD" \
  --comparisons ddl-unloaded --scale 0.034 --reps 1 --store postgres \
  --out "$OUT"
python3 - "$OUT" <<'PY'
import json, pathlib, sys
runs = [json.loads(l) for l in (pathlib.Path(sys.argv[1]) / "runs.jsonl").read_text().splitlines() if l]
assert len(runs) == 2, f"{len(runs)} runs"
for r in runs:
    assert "failed" not in r, r["failed"]
    led = r["drain"]["ledger"]
    assert led["equal"], led
    assert r["ddl"]["phase_completion_s"] > 0, r["ddl"]
    assert r["drain"]["checkpoints"], "no sink checkpoint"
print("smoke: 2 runs drained, ledgers equal")
PY
python3 crates/perf-harness/actdiag/analyze.py "$OUT" --old a --new b >/dev/null || true
test -s "$OUT/summary.md"
echo "smoke: ok ($OUT)"

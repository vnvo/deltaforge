#!/usr/bin/env bash
# Regression for the gate's container ownership (scripts/gate.sh). Runs the
# real gate twice on one probe suite (crates/gate-ownership/tests/
# gate_probe.rs), which starts an owned container and never removes it:
#
#   forced failure  the probe panics; the gate fails and removes the probe's
#                   container; a test container created during the run
#                   without the labels fails the gate and is left alone
#   SIGINT          the probe hangs; SIGINT to the gate stops the suite and
#                   removes the probe's container (exit 130)
#   SIGINT during   a step before the suites blocks (GATE_SELFTEST_BLOCK,
#   a step          through the gate's own step runner); SIGINT stops the gate
#                   within 10 s (130), the step is gone and the run's
#                   container is removed
#
# Around both runs it plants containers of other runs: a stale run (owner
# process gone) that the gate start removes, and a live run and another
# host's run that are never touched. Needs Docker and the postgres:17 image.
set -u
ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"
S=$(mktemp -d)
RUN_LABEL=deltaforge.gate.run
OWNER_LABEL=deltaforge.gate.owner
HOST=$(hostname)
BOOT=$(cat /proc/sys/kernel/random/boot_id)
start_time() { sed 's/.*) //' "/proc/$1/stat" 2>/dev/null | awk '{print $20}'; }
TAG="selftest-$$"
fails=0

cleanup() {
  docker ps -aq --filter "label=deltaforge.gate.selftest=$TAG" | xargs -r docker rm -f -v >/dev/null 2>&1
  rm -rf -- "$S"
}
trap cleanup EXIT

check() { # description, then a command that must succeed
  local what=$1
  shift
  if "$@"; then echo "ok    $what"; else echo "FAIL  $what"; fails=1; fi
}
exists() { [ -n "$(docker ps -aq --filter "id=$1")" ]; }
gone() { ! exists "$1"; }
run_gone() { [ -z "$(docker ps -aq --filter "label=$RUN_LABEL=$1")" ]; }

sentinel() { # run id, owner
  docker run -d --label "deltaforge.gate.selftest=$TAG" --label "$RUN_LABEL=$1" \
    --label "$OWNER_LABEL=$2" --entrypoint sleep postgres:17 3600
}
plant() {
  sleep 30 &
  local dead=$! dead_start
  dead_start=$(start_time $dead)
  kill $dead
  wait $dead 2>/dev/null
  STALE=$(sentinel "$TAG-stale" "$HOST:$BOOT:$dead:$dead_start")
  LIVE=$(sentinel "$TAG-live" "$HOST:$BOOT:$$:$(start_time $$)")
  FOREIGN=$(sentinel "$TAG-foreign" "another-host:$BOOT:1:1")
}
others_kept() {
  check "a stale run's container is removed at gate start" gone "$STALE"
  check "a live run's container is kept" exists "$LIVE"
  check "another host's run is kept" exists "$FOREIGN"
}

# One-suite manifest: every other entry excluded.
grep -Ev '^\s*(#|$)' scripts/gate-suites.txt | grep -v 'gate-ownership/gate_probe' |
  awk '{print "excluded", $2, "not part of the gate self-test"}' > "$S/manifest"
echo "parallel gate-ownership/gate_probe -p gate-ownership --test gate_probe" >> "$S/manifest"
# Run in the background; `exec` makes `$!` the gate itself, not a subshell.
gate() { GATE_MANIFEST="$S/manifest" GATE_ONLY_SUITES=1 GATE_DIR="$S/gate" exec scripts/gate.sh "$@"; }

echo "== forced failure"
plant
GATE_PROBE=fail gate "$TAG-fail" > "$S/fail.out" 2>&1 &
gate_pid=$!
# A container without the gate labels, created while the gate runs.
for _ in $(seq 120); do [ -f "$S/gate/$TAG-fail/run-id" ] && break; sleep 1; done
UNOWNED=$(docker run -d --label "deltaforge.gate.selftest=$TAG" \
  --label org.testcontainers.managed-by=testcontainers --entrypoint sleep postgres:17 3600)
wait $gate_pid
rc=$?
run_id=$(cat "$S/gate/$TAG-fail/run-id" 2>/dev/null)
check "the gate fails (rc=$rc)" [ "$rc" -ne 0 ]
check "the probe suite is reported failed" grep -q '^FAILED gate-ownership/gate_probe' "$S/gate/$TAG-fail/summary"
check "the run's containers are removed" run_gone "$run_id"
check "an unlabelled test container fails the gate" grep -q "^FAILED unowned-containers.*${UNOWNED:0:12}" "$S/gate/$TAG-fail/summary"
check "an unlabelled test container is not removed" exists "$UNOWNED"
others_kept
docker rm -f -v "$STALE" "$LIVE" "$FOREIGN" "$UNOWNED" >/dev/null 2>&1

echo "== SIGINT"
plant
set -m # the gate must receive SIGINT (a background job otherwise ignores it)
GATE_PROBE=hang GATE_PROBE_MARKER="$S/marker" gate "$TAG-int" > "$S/int.out" 2>&1 &
gate_pid=$!
set +m
for _ in $(seq 900); do [ -s "$S/marker" ] && break; sleep 1; done
probe=$(cat "$S/marker" 2>/dev/null)
run_id=$(cat "$S/gate/$TAG-int/run-id" 2>/dev/null)
probe_owned() {
  [ -n "$probe" ] && [ -n "$run_id" ] &&
    [ -n "$(docker ps -q --filter "id=$probe" --filter "label=$RUN_LABEL=$run_id")" ]
}
check "the probe's container runs, owned by the run" probe_owned
kill -INT $gate_pid
for _ in $(seq 60); do kill -0 $gate_pid 2>/dev/null || break; sleep 1; done
if kill -0 $gate_pid 2>/dev/null; then
  check "the gate stops within 60 s of SIGINT" false
  kill -KILL -- -$gate_pid 2>/dev/null
fi
wait $gate_pid
rc=$?
check "the interrupted gate exits 130 (rc=$rc)" [ "$rc" -eq 130 ]
check "the probe's container is removed" gone "$probe"
check "the run's containers are removed" run_gone "$run_id"
# (A bracket so the pattern does not match the command line holding it.)
check "no suite process is left" bash -c '! pgrep -f "deps/gate_probe-[0-9a-f]" >/dev/null'
others_kept

echo "== SIGINT during a step"
set -m
GATE_SELFTEST_BLOCK="$S/block" gate "$TAG-step" > "$S/step.out" 2>&1 &
gate_pid=$!
set +m
for _ in $(seq 120); do [ -s "$S/block" ] && break; sleep 1; done
blocker=$(cat "$S/block" 2>/dev/null)
run_id=$(cat "$S/gate/$TAG-step/run-id" 2>/dev/null)
blocking() { [ -n "$blocker" ] && kill -0 "$blocker" 2>/dev/null; }
check "the blocking step runs" blocking
OWNED=$(docker run -d --label "deltaforge.gate.selftest=$TAG" --label "$RUN_LABEL=$run_id" \
  --entrypoint sleep postgres:17 3600)
kill -INT $gate_pid
for _ in $(seq 10); do kill -0 $gate_pid 2>/dev/null || break; sleep 1; done
if kill -0 $gate_pid 2>/dev/null; then
  check "the gate stops within 10 s of SIGINT during a step" false
  kill -KILL -- -$gate_pid 2>/dev/null
else
  check "the gate stops within 10 s of SIGINT during a step" true
fi
wait $gate_pid
rc=$?
check "the interrupted gate exits 130 (rc=$rc)" [ "$rc" -eq 130 ]
check "the blocking step is gone" bash -c "! kill -0 $blocker 2>/dev/null"
check "the run's container is removed" gone "$OWNED"
check "the run's containers are removed" run_gone "$run_id"
kill -KILL "$blocker" 2>/dev/null

if [ $fails -ne 0 ]; then
  echo "gate self-test FAILED (logs: fail.out, int.out, step.out)"
  cat "$S/fail.out" "$S/int.out" "$S/step.out" | tail -40
  exit 1
fi
echo "gate self-test ok"

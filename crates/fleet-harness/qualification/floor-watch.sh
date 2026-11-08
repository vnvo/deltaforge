#!/usr/bin/env bash
# Run a qualification command under the release disk floors and an outer
# deadline: refuse to start below START_GIB free (default 50), stop the
# command if free disk falls below FLOOR_GIB (default 25) while it runs, and
# stop it once it has run DEADLINE_SECS (default 0: no deadline; set it to the
# invocation's expected length, its drain included, plus a margin). Checks
# the filesystem of the current directory and Docker's data root.
#
# A stop is graceful first: SIGINT (the harness aborts its run and still
# writes its result), then, after GRACE_SECS (default 120), SIGKILL. A hung
# command is never waited on without bound.
#
# Exits with the command's status, or 3 when refused, 4 when stopped at the
# floor, 5 when stopped at the deadline.
#   DEADLINE_SECS=3600 floor-watch.sh -- <command> [args...]
set -u
[ "${1:-}" = "--" ] && shift
START_GIB=${START_GIB:-50} FLOOR_GIB=${FLOOR_GIB:-25}
DEADLINE_SECS=${DEADLINE_SECS:-0} GRACE_SECS=${GRACE_SECS:-120}
POLL_SECS=${POLL_SECS:-10}
root=$(docker info --format '{{.DockerRootDir}}' 2>/dev/null || true)
free_gib() {
  df -B1G --output=avail . ${root:+"$root"} | tail -n +2 | tr -dc '0-9\n' | sort -n | head -1
}
# Stop $pid: SIGINT, then SIGKILL once GRACE_SECS have passed.
stop() {
  kill -INT "$pid" 2>/dev/null
  local waited=0
  while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt "$GRACE_SECS" ]; do
    sleep 1
    waited=$((waited + 1))
  done
  if kill -0 "$pid" 2>/dev/null; then
    echo "floor-watch: $(date -Is) still running ${GRACE_SECS}s after SIGINT; SIGKILL"
    kill -KILL "$pid" 2>/dev/null
  fi
  wait "$pid" 2>/dev/null
}
f=$(free_gib)
echo "floor-watch: start $(date -Is) free=${f}G (start ${START_GIB}G, floor ${FLOOR_GIB}G, deadline ${DEADLINE_SECS}s, grace ${GRACE_SECS}s)"
if [ "$f" -lt "$START_GIB" ]; then
  echo "floor-watch: REFUSED: ${f}G free < ${START_GIB}G"
  exit 3
fi
started=$(date +%s)
"$@" &
pid=$!
min=$f
while kill -0 "$pid" 2>/dev/null; do
  f=$(free_gib)
  [ "$f" -lt "$min" ] && min=$f
  if [ "$f" -lt "$FLOOR_GIB" ]; then
    echo "floor-watch: FLOOR $(date -Is) free=${f}G < ${FLOOR_GIB}G; stopping $pid"
    stop
    echo "floor-watch: stopped at the floor; min_free=${min}G"
    exit 4
  fi
  if [ "$DEADLINE_SECS" -gt 0 ] && [ $(($(date +%s) - started)) -ge "$DEADLINE_SECS" ]; then
    echo "floor-watch: DEADLINE $(date -Is) ${DEADLINE_SECS}s reached; stopping $pid"
    stop
    echo "floor-watch: stopped at the deadline; min_free=${min}G"
    exit 5
  fi
  sleep "$POLL_SECS"
done
wait "$pid"
rc=$?
echo "floor-watch: end $(date -Is) rc=$rc min_free=${min}G"
exit $rc

#!/usr/bin/env bash
# Run a qualification command under the release disk floors: refuse to
# start below START_GIB free (default 50), and stop the command if free disk
# falls below FLOOR_GIB (default 25) while it runs. Checks the filesystem of
# the current directory and Docker's data root. Exits with the command's
# status, or 3 when refused, or 4 when stopped at the floor.
#   floor-watch.sh -- <command> [args...]
set -u
[ "${1:-}" = "--" ] && shift
START_GIB=${START_GIB:-50} FLOOR_GIB=${FLOOR_GIB:-25}
root=$(docker info --format '{{.DockerRootDir}}' 2>/dev/null || true)
free_gib() {
  df -B1G --output=avail . ${root:+"$root"} | tail -n +2 | tr -dc '0-9\n' | sort -n | head -1
}
f=$(free_gib)
echo "floor-watch: start $(date -Is) free=${f}G (start ${START_GIB}G, floor ${FLOOR_GIB}G)"
if [ "$f" -lt "$START_GIB" ]; then
  echo "floor-watch: REFUSED: ${f}G free < ${START_GIB}G"
  exit 3
fi
"$@" &
pid=$!
min=$f
while kill -0 "$pid" 2>/dev/null; do
  f=$(free_gib)
  [ "$f" -lt "$min" ] && min=$f
  if [ "$f" -lt "$FLOOR_GIB" ]; then
    echo "floor-watch: FLOOR $(date -Is) free=${f}G < ${FLOOR_GIB}G; stopping $pid"
    kill -TERM "$pid"
    wait "$pid"
    echo "floor-watch: stopped at the floor; min_free=${min}G"
    exit 4
  fi
  sleep 10
done
wait "$pid"
rc=$?
echo "floor-watch: end $(date -Is) rc=$rc min_free=${min}G"
exit $rc

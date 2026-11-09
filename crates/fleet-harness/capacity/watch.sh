#!/usr/bin/env bash
# Observer for one capacity run, outside the pinned harness: every 5 s it
# appends one JSON line to $DIR/resources.jsonl (host available memory,
# docker stats of cap-mysql, cap-kafka and cap-df, the binlog head, MySQL's
# running threads, the harness's CPU time and the pipeline statuses).
#
# It stops the harness ($PID) - SIGINT, then SIGKILL after 120 s, as
# floor-watch.sh does - and leaves a marker file when:
#   MEMFLOOR    host MemAvailable falls below 4 GiB (4,294,967,296 bytes);
#   OOM         a campaign container was OOM-killed or stopped running;
#   FAILCLOSED  the run's pipeline failed (its incidents are saved).
# Unreadable values are logged as null, never as healthy.
#   watch.sh <run dir> <harness pid>
set -u
DIR=$1 PID=$2
FLOOR=4294967296
CONTAINERS="cap-mysql cap-kafka cap-df"
stop() {
  kill -INT "$PID" 2>/dev/null
  for _ in $(seq 120); do kill -0 "$PID" 2>/dev/null || return; sleep 1; done
  kill -KILL "$PID" 2>/dev/null
}
while kill -0 "$PID" 2>/dev/null; do
  now=$(date -Is)
  mem=$(awk '/^MemAvailable:/{print $2 * 1024}' /proc/meminfo)
  stats=$(docker stats --no-stream --format '{{json .}}' $CONTAINERS 2>/dev/null | paste -sd, -)
  state=$(docker inspect --format '{{.Name}} {{.State.Running}} {{.State.OOMKilled}}' $CONTAINERS 2>/dev/null | paste -sd, -)
  head=$(docker exec cap-mysql mysql -uroot -prootpw -N -e "SHOW BINARY LOG STATUS; SHOW GLOBAL STATUS LIKE 'Threads_running'" 2>/dev/null | awk '{printf "%s%s", (NR>1?" ":""), $0}')
  cpu=$(awk '{print $14 + $15}' /proc/"$PID"/stat 2>/dev/null)
  pl=$(curl -sf -m 5 http://127.0.0.1:18090/pipelines | python3 -c 'import json,sys
try: print(" ".join("%s=%s" % (p.get("name"), p.get("status")) for p in json.load(sys.stdin)))
except Exception: print("unreadable")')
  python3 - "$now" "${mem:-}" "[${stats}]" "$state" "$head" "${cpu:-}" "$pl" >> "$DIR/resources.jsonl" <<'PY'
import json, sys
at, mem, stats, state, head, cpu, pl = sys.argv[1:]
try: stats = json.loads(stats)
except Exception: stats = None
print(json.dumps({"at": at, "mem_available": int(mem) if mem else None, "docker_stats": stats,
                  "containers": state or None, "mysql": head or None,
                  "harness_cpu_ticks": int(cpu) if cpu else None, "pipelines": pl}))
PY
  if [ -n "$mem" ] && [ "$mem" -lt "$FLOOR" ]; then
    echo "$now MemAvailable=$mem < $FLOOR" > "$DIR/MEMFLOOR"; stop; break
  fi
  # Every campaign container must be running and not OOM-killed.
  bad=$(printf '%s\n' "$state" | tr ',' '\n' | awk 'NF && ($2 != "true" || $3 != "false")')
  if [ -z "$state" ] || [ -n "$bad" ]; then
    echo "$now containers: ${bad:-unreadable}" > "$DIR/OOM"; stop; break
  fi
  for p in $(printf '%s\n' "$pl" | tr ' ' '\n' | grep '=failed$' | cut -d= -f1); do
    { echo "$now pipeline $p failed"; curl -s -m 5 "http://127.0.0.1:18090/pipelines/$p/incidents"; echo; } > "$DIR/FAILCLOSED"
    stop; break 2
  done
  sleep 5
done

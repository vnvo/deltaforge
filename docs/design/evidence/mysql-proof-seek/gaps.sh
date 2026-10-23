#!/usr/bin/env bash
# Probe 6: a GTID gap that later fills, across a DeltaForge restart.
set -uo pipefail
P=$(cd "$(dirname "$0")" && pwd); RUNNER=${1:?usage: gaps.sh <runner>}
OUT=$P/gaps; rm -rf "$OUT"; mkdir -p "$OUT"
NAME=seekprobe6-mysql; PW=pw
docker rm -f -v $NAME >/dev/null 2>&1
docker run -d --name $NAME -e MYSQL_ROOT_PASSWORD=$PW -p 127.0.0.1::3306 mysql:8.4 --server-id=60 \
  --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL --gtid-mode=ON --enforce-gtid-consistency=ON >/dev/null
cleanup() { kill $RECV 2>/dev/null; [ -n "${RPID:-}" ] && kill $RPID 2>/dev/null; docker rm -f -v $NAME >/dev/null 2>&1; }
trap cleanup EXIT
# The image runs a temporary initialization server first: wait for the
# final one (port 3306) and a working query.
for i in $(seq 1 120); do
  docker logs $NAME 2>&1 | grep -q "port: 3306" && docker exec $NAME mysql -uroot -p$PW -N -B -e "SELECT 1" >/dev/null 2>&1 && break
  sleep 2
done
PORT=$(docker port $NAME 3306 | head -1 | cut -d: -f2)
sql() { docker exec -i $NAME mysql -uroot -p$PW -N -B -e "$1" 2>/dev/null; }
log() { echo "$*" | tee -a "$OUT/log"; }
python3 -I $P/receiver.py 18999 "$OUT/received.jsonl" & RECV=$!
sql "CREATE DATABASE g; CREATE TABLE g.t (id BIGINT PRIMARY KEY, a INT)"
start_runner() {
  "$RUNNER" --api-addr 127.0.0.1:18080 --metrics-addr 127.0.0.1:19000 --storage-path "$OUT/state.db" >"$OUT/runner-$1.out" 2>"$OUT/runner-$1.err" & RPID=$!
  for i in $(seq 1 60); do curl -sf http://127.0.0.1:18080/health >/dev/null && return; curl -sf http://127.0.0.1:18080/pipelines >/dev/null && return; sleep 0.5; done
  log "runner did not start"; exit 1
}
start_runner 1
SPEC='{"apiVersion":"deltaforge/v1","kind":"Pipeline","metadata":{"name":"gaps","tenant":"perf"},"spec":{"source":{"type":"mysql","config":{"id":"gaps","dsn":"mysql://root:'$PW'@127.0.0.1:'$PORT'/","tables":["g.t"],"snapshot":{"mode":"never"}}},"processors":[],"sinks":[{"type":"http","config":{"id":"http","url":"http://127.0.0.1:18999/","batch_mode":true,"required":true}}]}}'
curl -sf -X POST -H 'Content-Type: application/json' -d "$SPEC" http://127.0.0.1:18080/pipelines >/dev/null || { log "create failed"; exit 1; }
sleep 3
UUID=$(sql "SELECT @@server_uuid")
sql "INSERT INTO g.t VALUES (1,1)"
N=$(sql "SELECT @@gtid_executed" | sed 's/.*-//')
log "executed before the gap: $(sql 'SELECT @@gtid_executed')"
# A transaction with an explicit GTID, leaving a gap of 4.
sql "SET gtid_next='$UUID:$((N+5))'; INSERT INTO g.t VALUES (2,2); SET gtid_next='AUTOMATIC'"
log "executed with the gap: $(sql 'SELECT @@gtid_executed')"
sleep 5
log "received before the restart: $(grep -o '\"id\":[^,}]*' "$OUT/received.jsonl" | sort -u | tr '\n' ' ')"
log "stored checkpoint before the restart: $(sqlite3 "$OUT/state.db" "select val from df_kv where ns='checkpoints'")"
# Stop DeltaForge after it has checkpointed the gap transaction.
kill -TERM $RPID; wait $RPID 2>/dev/null; RPID=
# Four rows while it is down: automatic GTIDs N+1..N+4 fill the gap.
sql "INSERT INTO g.t VALUES (3,3); INSERT INTO g.t VALUES (4,4); INSERT INTO g.t VALUES (5,5); INSERT INTO g.t VALUES (6,6)"
log "executed after the fill: $(sql 'SELECT @@gtid_executed')"
start_runner 2
# Pipelines are not restored on start: re-create it (it resumes from its
# stored checkpoint).
curl -sf -X POST -H 'Content-Type: application/json' -d "$SPEC" http://127.0.0.1:18080/pipelines >/dev/null || { log "re-create failed"; exit 1; }
sleep 8
# One more row after the restart, to show the stream is live.
sql "INSERT INTO g.t VALUES (7,7)"
sleep 5
log "stored checkpoint at the end: $(sqlite3 "$OUT/state.db" "select val from df_kv where ns='checkpoints'")"
log "received in all: $(grep -o '\"id\":[^,}]*' "$OUT/received.jsonl" | sort -u | tr '\n' ' ')"
grep -ho '"gtid_set":"[^"]*"\|gtid[^,]*' "$OUT"/runner-*.err | head -5 >> "$OUT/log"
log done

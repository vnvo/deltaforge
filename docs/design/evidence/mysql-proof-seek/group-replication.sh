#!/usr/bin/env bash
# Group Replication probe (external supporting evidence for the exact-GTID
# fix): a 2-member multi-primary group where each member assigns GTIDs of
# the group's UUID from its own block (block size 100 here), so the
# executed set has gaps that fill later. One DeltaForge runner reads member
# 1: rows on both members, a stop, rows on member 1 that take numbers in
# the gap, a restart; then what arrived.
#   gr.sh <runner> <label>
set -uo pipefail
RUNNER=$1; LABEL=$2
P=$(cd "$(dirname "$0")" && pwd); OUT=$P/$LABEL; rm -rf "$OUT"; mkdir -p "$OUT"
NET=grprobe-net; PW=pw; GROUP=aaaaaaaa-1111-2222-3333-444444444444
log() { echo "$*" | tee -a "$OUT/log"; }
cleanup() {
  [ -n "${RPID:-}" ] && kill "$RPID" 2>/dev/null
  [ -n "${RECV:-}" ] && kill "$RECV" 2>/dev/null
  docker rm -f -v gr1 gr2 >/dev/null 2>&1; docker network rm $NET >/dev/null 2>&1
}
trap cleanup EXIT
cleanup; docker network create $NET >/dev/null
for i in 1 2; do
  docker run -d --name gr$i --hostname gr$i --network $NET -e MYSQL_ROOT_PASSWORD=$PW \
    $([ $i = 1 ] && echo "-p 127.0.0.1::3306") mysql:8.4 \
    --server-id=$i --log-bin=mysql-bin --binlog-format=ROW --gtid-mode=ON \
    --enforce-gtid-consistency=ON --report-host=gr$i \
    --disabled-storage-engines=MyISAM,BLACKHOLE,FEDERATED,ARCHIVE,MEMORY \
    --plugin-load-add=group_replication.so \
    --loose-group-replication-group-name=$GROUP \
    --loose-group-replication-local-address=gr$i:33061 \
    --loose-group-replication-group-seeds=gr1:33061,gr2:33061 \
    --loose-group-replication-start-on-boot=OFF \
    --loose-group-replication-bootstrap-group=OFF \
    --loose-group-replication-single-primary-mode=OFF \
    --loose-group-replication-enforce-update-everywhere-checks=ON \
    --loose-group-replication-gtid-assignment-block-size=100 >/dev/null
done
ready() { docker logs "$1" 2>&1 | grep -q "port: 3306" && docker exec "$1" mysql -uroot -p$PW -N -B -e "SELECT 1" >/dev/null 2>&1; }
for i in $(seq 1 120); do ready gr1 && ready gr2 && break; sleep 2; done
sql() { docker exec -i "$1" mysql -uroot -p$PW -N -B -e "$2" 2>&1 | grep -v "Using a password"; }
for i in 1 2; do
  # Each member's own initialization transactions must not stop it joining.
  sql gr$i "RESET BINARY LOGS AND GTIDS"
  sql gr$i "SET SQL_LOG_BIN=0; CREATE USER rpl@'%' IDENTIFIED BY 'rpl'; GRANT REPLICATION SLAVE, BACKUP_ADMIN ON *.* TO rpl@'%'; SET SQL_LOG_BIN=1;
    CHANGE REPLICATION SOURCE TO SOURCE_USER='rpl', SOURCE_PASSWORD='rpl', GET_SOURCE_PUBLIC_KEY=1 FOR CHANNEL 'group_replication_recovery';"
done
sql gr1 "SET GLOBAL group_replication_bootstrap_group=ON; START GROUP_REPLICATION; SET GLOBAL group_replication_bootstrap_group=OFF;"
sql gr2 "START GROUP_REPLICATION;"
for i in $(seq 1 60); do
  n=$(sql gr1 "SELECT COUNT(*) FROM performance_schema.replication_group_members WHERE MEMBER_STATE='ONLINE'")
  [ "$n" = 2 ] && break; sleep 2
done
log "members online: $n"
[ "$n" = 2 ] || {
  log "the group did not form"
  docker logs gr2 2>&1 | grep -iE "error|group_replication|\[GCS\]" | tail -15 >> "$OUT/log"
  exit 1
}
PORT=$(docker port gr1 3306 | head -1 | cut -d: -f2)
python3 -I "$P/../probe/recv/receiver.py" 18999 "$OUT/received.jsonl" & RECV=$!
sql gr1 "CREATE DATABASE g; CREATE TABLE g.t (id BIGINT PRIMARY KEY, a INT)"
sleep 2
start_runner() {
  "$RUNNER" --api-addr 127.0.0.1:18080 --metrics-addr 127.0.0.1:19000 --storage-path "$OUT/state.db" >"$OUT/runner-$1.out" 2>"$OUT/runner-$1.err" & RPID=$!
  for i in $(seq 1 60); do curl -sf http://127.0.0.1:18080/pipelines >/dev/null && break; sleep 0.5; done
  curl -sf -X POST -H 'Content-Type: application/json' -d "$SPEC" http://127.0.0.1:18080/pipelines >/dev/null || { log "create failed"; exit 1; }
}
SPEC='{"apiVersion":"deltaforge/v1","kind":"Pipeline","metadata":{"name":"gr","tenant":"perf"},"spec":{"source":{"type":"mysql","config":{"id":"gr","dsn":"mysql://root:'$PW'@127.0.0.1:'$PORT'/","tables":["g.t"],"snapshot":{"mode":"never"}}},"processors":[],"sinks":[{"type":"http","config":{"id":"http","url":"http://127.0.0.1:18999/","batch_mode":true,"required":true}}]}}'
start_runner 1
sleep 4
# Rows on both members: member 1 takes numbers from its block, member 2 from
# the next block, so the executed set has a gap after member 1's rows.
sql gr1 "INSERT INTO g.t VALUES (1,1)"
sql gr2 "INSERT INTO g.t VALUES (2,2)"
sleep 3
log "executed with a gap: $(sql gr1 'SELECT @@gtid_executed' | tr -d '\n')"
log "stored checkpoint before the stop: $(sqlite3 "$OUT/state.db" "select val from df_kv where ns='checkpoints'")"
kill -TERM "$RPID"; wait "$RPID" 2>/dev/null; RPID=
# While stopped: rows on member 1, taking numbers inside the gap.
for id in 3 4 5 6; do sql gr1 "INSERT INTO g.t VALUES ($id,$id)"; done
log "executed after the fill: $(sql gr1 'SELECT @@gtid_executed' | tr -d '\n')"
start_runner 2
sleep 8
sql gr1 "INSERT INTO g.t VALUES (7,7)"
sleep 6
log "stored checkpoint at the end: $(sqlite3 "$OUT/state.db" "select val from df_kv where ns='checkpoints'")"
ids=$(grep -o '"id":"\?[0-9]*' "$OUT/received.jsonl" | grep -o '[0-9]*$' | sort -n | uniq -c | awk '{print $2"x"$1}' | tr '\n' ' ')
log "rows received (id x count): $ids"
for id in 3 4 5 6; do
  c=$(grep -o "\"id\":\"\\?$id[\",}]" "$OUT/received.jsonl" | wc -l)
  log "row $id (in the gap) delivered $c time(s)"
done
log done

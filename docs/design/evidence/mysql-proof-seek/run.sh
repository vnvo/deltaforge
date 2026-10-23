#!/usr/bin/env bash
# Proof-seek protocol probes on a throwaway MySQL 8.4 (removed with its
# volumes at the end). Results: $OUT/*.json and $OUT/log.
set -uo pipefail
P=$(cd "$(dirname "$0")" && pwd)
OUT=$P/results; rm -rf "$OUT"; mkdir -p "$OUT"
NAME=seekprobe-mysql; PW=pw; PADMIB=${PADMIB:-960}
docker rm -f -v $NAME >/dev/null 2>&1
docker run -d --name $NAME -e MYSQL_ROOT_PASSWORD=$PW -p 127.0.0.1::3306 \
  mysql:8.4 --server-id=60 --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL \
  --gtid-mode=ON --enforce-gtid-consistency=ON --max-allowed-packet=256M >/dev/null
trap 'docker rm -f -v $NAME >/dev/null 2>&1' EXIT
# The image runs a temporary initialization server first: wait for the
# final one (port 3306) and a working query.
for i in $(seq 1 120); do
  docker logs $NAME 2>&1 | grep -q "port: 3306" && docker exec $NAME mysql -uroot -p$PW -N -B -e "SELECT 1" >/dev/null 2>&1 && break
  sleep 2
done
PORT=$(docker port $NAME 3306 | head -1 | cut -d: -f2)
URL="mysql://root:$PW@127.0.0.1:$PORT"
sql() { docker exec -i $NAME mysql -uroot -p$PW -N -B -e "$1" 2>/dev/null; }
status() { sql "SHOW BINARY LOG STATUS" ; }
log() { echo "$*" | tee -a "$OUT/log"; }
sql "SET GLOBAL sync_binlog=0; SET GLOBAL innodb_flush_log_at_trx_commit=2;
 CREATE DATABASE p; CREATE TABLE p.pad (id BIGINT AUTO_INCREMENT PRIMARY KEY, a INT, b VARCHAR(32), c DOUBLE);
 CREATE TABLE p.t (id BIGINT PRIMARY KEY, a INT); CREATE TABLE p.u (id BIGINT PRIMARY KEY, a INT);"
# The procedure through stdin with its own delimiter (`-e` splits on ';').
printf '%s
' 'DELIMITER //'   "CREATE PROCEDURE p.fill(n INT) BEGIN DECLARE i INT DEFAULT 0; WHILE i < n DO INSERT INTO p.pad (a,b,c) VALUES (i, CONCAT('r',i), i+0.5); SET i = i + 1; END WHILE; END//"   'DELIMITER ;' | docker exec -i $NAME mysql -uroot -p$PW 2>&1 | grep -v Warning
sql "SHOW PROCEDURE STATUS WHERE Db='p'" | grep -q fill || { log "the padding procedure was not created"; exit 1; }
sql "FLUSH BINARY LOGS"
FILE=$(status | cut -f1)
# Pad the fresh file with single-row transactions to about PADMIB MiB.
target=$((PADMIB*1048576))
while :; do
  pos=$(status | cut -f2)
  [ -n "$pos" ] || { log "no binary log status"; exit 1; }
  [ "$pos" -ge "$target" ] && break
  docker exec -i $NAME mysql -uroot -p$PW -e "CALL p.fill(50000)" 2>&1 | grep -v Warning
  after=$(status | cut -f2)
  [ "${after:-0}" -gt "$pos" ] || { log "padding made no progress at offset $pos"; exit 1; }
done
read -r FA PA SETA < <(status | awk -F'\t' '{print $1, $2, $5}')
log "boundary A: file $FA offset $PA set $SETA (padding file $FILE)"
[ "$FA" = "$FILE" ] || log "WARNING: the file rotated while padding"
# The interval (A,B]: 3 rows, a DDL on p.t, 3 rows.
sql "INSERT INTO p.t VALUES (1,1); INSERT INTO p.t VALUES (2,2); INSERT INTO p.u VALUES (1,1);"
read -r _ PDDL _ < <(status | awk -F'\t' '{print $1, $2, $5}')
sql "ALTER TABLE p.t ADD COLUMN b INT"
read -r _ PAFTER _ < <(status | awk -F'\t' '{print $1, $2, $5}')
sql "INSERT INTO p.t VALUES (3,3,3); INSERT INTO p.u VALUES (2,2); INSERT INTO p.u VALUES (3,3);"
read -r FB PB SETB < <(status | awk -F'\t' '{print $1, $2, $5}')
log "DDL starts at $PDDL (after it: $PAFTER); boundary B: file $FB offset $PB set $SETB"
echo "{\"file\":\"$FA\",\"a_offset\":$PA,\"a_set\":\"$SETA\",\"ddl_offset\":$PDDL,\"after_ddl\":$PAFTER,\"b_offset\":$PB,\"b_set\":\"$SETB\"}" > "$OUT/positions.json"
run() { local name=$1; shift; log "== $name"; "$P/target/release/seekprobe" "$URL" "$@" > "$OUT/$name.json"; python3 -c "
import json,sys; r=json.load(open('$OUT/$name.json')); r.pop('gtids',None); print(json.dumps(r)[:1500])" | tee -a "$OUT/log"; }
# 1. Today's GTID dump (file chosen by the server).
run p1-gtid gtid "$SETA" "" 4 7 60
# 1b. GTID set plus file and the boundary offset.
run p1-gtidpos gtidpos "$SETA" "$FA" "$PA" 7 60
# 2. Position dump at the boundary.
run p2-pos pos "" "$FA" "$PA" 7 60
# 3. Bad offsets: inside the GTID event, inside the transaction (the next
#    event after the GTID event), past the file's end.
run p3-inside-event pos "" "$FA" $((PA+5)) 7 15
run p3-gtidpos-inside-event gtidpos "$SETA" "$FA" $((PA+5)) 7 15
run p3-past-end pos "" "$FA" $((PB+100000)) 7 10
# An event boundary inside a transaction: just after its GTID event.
GLEN=$(python3 -I -c "import json;print([e for e in json.load(open('$OUT/p2-pos.json'))['first_events'] if e['what'].startswith('gtid')][0]['len'])")
run p3-inside-txn-pos pos "" "$FA" $((PA+GLEN)) 7 10
run p3-inside-txn-gtidpos gtidpos "$SETA" "$FA" $((PA+GLEN)) 7 10
# 4. A too-late anchor: after the DDL transaction, GTID set A.
run p4-late-gtidpos gtidpos "$SETA" "$FA" "$PAFTER" 7 15
run p4-late-pos pos "" "$FA" "$PAFTER" 7 15
# 5. A purged anchor file, and a scan across a rotation from an older file.
sql "FLUSH BINARY LOGS; INSERT INTO p.u VALUES (4,4);"
run p5-across-rotation pos "" "$FA" "$PA" 8 20
sql "FLUSH BINARY LOGS"; NEWF=$(status | cut -f1); sql "PURGE BINARY LOGS TO '$NEWF'"
run p5-purged-pos pos "" "$FA" "$PA" 7 10
run p5-purged-gtidpos gtidpos "$SETA" "$FA" "$PA" 7 10
log "done"

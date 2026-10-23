#!/usr/bin/env bash
# Probe 8: revision 2's assumptions. (a) A positioned start at a compressed
# transaction: its GTID event is outside the payload. (b) A record frontier:
# opened at B's end offset with set B, the first transaction read is the
# next one, not in B. (c) At the live end the server sends heartbeats.
set -uo pipefail
P=$(cd "$(dirname "$0")" && pwd); OUT=$P/frontier; rm -rf "$OUT"; mkdir -p "$OUT"
NAME=seekprobe8-mysql; PW=pw
log() { echo "$*" | tee -a "$OUT/log"; }
docker rm -f -v $NAME >/dev/null 2>&1
docker run -d --name $NAME -e MYSQL_ROOT_PASSWORD=$PW -p 127.0.0.1::3306 mysql:8.4 --server-id=60 --log-bin=mysql-bin --binlog-format=ROW --gtid-mode=ON --enforce-gtid-consistency=ON >/dev/null
trap 'docker rm -f -v $NAME >/dev/null 2>&1' EXIT
for i in $(seq 1 120); do docker logs $NAME 2>&1 | grep -q "port: 3306" && docker exec $NAME mysql -uroot -p$PW -N -B -e "SELECT 1" >/dev/null 2>&1 && break; sleep 2; done
PORT=$(docker port $NAME 3306 | head -1 | cut -d: -f2); URL="mysql://root:$PW@127.0.0.1:$PORT"
sql() { docker exec -i $NAME mysql -uroot -p$PW -N -B -e "$1" 2>/dev/null; }
status() { sql "SHOW BINARY LOG STATUS"; }
show() { python3 -I -c "import json,sys; r=json.load(open(sys.argv[1])); print(json.dumps({k:r[k] for k in ('error','first_txn_ms','txns','first_gtids')}), [e['what'][:48]+'@'+str(e['start']) for e in r['first_events']])" "$1"; }
sql "CREATE DATABASE p; CREATE TABLE p.t (id INT PRIMARY KEY, a INT)"
# (a) A compressed transaction at a positioned start.
read -r F A SETA < <(status | awk -F'\t' '{print $1, $2, $5}')
sql "SET SESSION binlog_transaction_compression = ON; BEGIN; INSERT INTO p.t VALUES (1,1),(2,2),(3,3); COMMIT;"
"$P/target/release/seekprobe" "$URL" gtidpos "$SETA" "$F" "$A" 1 10 > "$OUT/a.json"
log "(a) positioned at a compressed transaction: $(show "$OUT/a.json")"
# (b) A frontier: B's end offset with set B; then one more transaction.
read -r F B SETB < <(status | awk -F'\t' '{print $1, $2, $5}')
( "$P/target/release/seekprobe" "$URL" gtidpos "$SETB" "$F" "$B" 1 15 > "$OUT/b.json" ) &
sleep 2; sql "INSERT INTO p.t VALUES (4,4)"; wait
log "(b) frontier at B's end ($SETB): $(show "$OUT/b.json")"
# (c) The live end: no new transaction; what arrives within 4 s.
read -r F E SETE < <(status | awk -F'\t' '{print $1, $2, $5}')
"$P/target/release/seekprobe" "$URL" gtidpos "$SETE" "$F" "$E" 1 4 > "$OUT/c.json"
log "(c) at the live end: $(show "$OUT/c.json")"
log done

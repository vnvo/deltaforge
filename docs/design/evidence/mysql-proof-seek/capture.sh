#!/usr/bin/env bash
# Probe 7: batched capture. One transaction taking N tables' metadata locks,
# the binary-log position, one schema query for N tables, the position
# again; against N captures of one table each (a new connection each, as
# today). Then its behavior with concurrent DDL and lock order.
set -uo pipefail
P=$(cd "$(dirname "$0")" && pwd); OUT=$P/capture; rm -rf "$OUT"; mkdir -p "$OUT"
NAME=seekprobe7-mysql; PW=pw
log() { echo "$*" | tee -a "$OUT/log"; }
docker rm -f -v $NAME >/dev/null 2>&1
docker run -d --name $NAME -e MYSQL_ROOT_PASSWORD=$PW mysql:8.4 --log-bin=mysql-bin --gtid-mode=ON --enforce-gtid-consistency=ON >/dev/null
trap 'docker rm -f -v $NAME >/dev/null 2>&1' EXIT
for i in $(seq 1 120); do docker logs $NAME 2>&1 | grep -q "port: 3306" && docker exec $NAME mysql -uroot -p$PW -N -B -e "SELECT 1" >/dev/null 2>&1 && break; sleep 2; done
my() { docker exec -i $NAME mysql -uroot -p$PW -N -B "$@" 2>&1 | grep -v "Using a password"; }
{ echo "CREATE DATABASE c;"; for i in $(seq -w 0 199); do echo "CREATE TABLE c.t$i (id BIGINT PRIMARY KEY, a INT, b VARCHAR(32), c DOUBLE);"; done; } | my
COLS="TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION, COLUMN_DEFAULT, EXTRA, COLUMN_COMMENT, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_OCTET_LENGTH, DATETIME_PRECISION, (SELECT co.ID FROM INFORMATION_SCHEMA.COLLATIONS co WHERE co.COLLATION_NAME = c.COLLATION_NAME) AS COLLATION_ID"
one() { # one table's capture on its own connection
  echo "connect;"; echo "START TRANSACTION READ ONLY; SELECT 1 FROM c.t$1 LIMIT 0; SHOW BINARY LOG STATUS;"
  echo "SELECT $COLS FROM INFORMATION_SCHEMA.COLUMNS c WHERE (TABLE_SCHEMA, TABLE_NAME) IN (('c','t$1')) ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;"
  echo "SHOW BINARY LOG STATUS; COMMIT;"
}
batch() { # N tables in one transaction, locks in sorted order
  local n=$1 pairs="" i
  echo "START TRANSACTION READ ONLY;"
  for i in $(seq -w 0 199 | head -n "$n"); do echo "SELECT 1 FROM c.t$i LIMIT 0;"; pairs="$pairs,('c','t$i')"; done
  echo "SHOW BINARY LOG STATUS;"
  echo "SELECT $COLS FROM INFORMATION_SCHEMA.COLUMNS c WHERE (TABLE_SCHEMA, TABLE_NAME) IN (${pairs#,}) ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;"
  echo "SHOW BINARY LOG STATUS; COMMIT;"
}
ms() { local t0 t1; t0=$(date +%s%N); "$@" >/dev/null; t1=$(date +%s%N); echo $(( (t1 - t0) / 1000000 )); }
run_seq() { { for i in $(seq -w 0 199 | head -n "$1"); do one "$i"; done; } | my; }
run_batch() { batch "$1" | my; }
run_seq 1 >/dev/null; run_batch 1 >/dev/null  # warm up
for n in 1 10 200; do
  for rep in 1 2 3; do
    log "n=$n rep=$rep sequential_ms=$(ms run_seq $n) batched_ms=$(ms run_batch $n)"
  done
done
# The columns returned by a batch of 200: every table's every column.
log "batched schema rows for 200 tables: $(batch 200 | my | grep -c "^c	t")"
# (i) A DDL while the batch holds its locks: it waits for the commit.
( echo "START TRANSACTION READ ONLY; SELECT 1 FROM c.t005 LIMIT 0; SELECT SLEEP(3); COMMIT;" | my >/dev/null ) &
sleep 1
t0=$(date +%s%N); my -e "ALTER TABLE c.t005 ADD COLUMN z INT" >/dev/null; t1=$(date +%s%N)
wait
log "(i) ALTER on a locked table waited $(( (t1 - t0) / 1000000 )) ms (the batch held it about 2000 ms more)"
# (ii) A table already locked: with a short lock_wait_timeout the batch fails
#      cleanly and holds no lock afterwards.
( echo "LOCK TABLES c.t007 WRITE; SELECT SLEEP(4); UNLOCK TABLES;" | my >/dev/null ) &
sleep 1
r=$(echo "SET SESSION lock_wait_timeout = 1; START TRANSACTION READ ONLY; SELECT 1 FROM c.t006 LIMIT 0; SELECT 1 FROM c.t007 LIMIT 0; COMMIT;" | my)
held=$(my -e "SELECT COUNT(*) FROM performance_schema.metadata_locks WHERE OBJECT_SCHEMA='c' AND OBJECT_NAME='t006'")
wait
log "(ii) batch on a write-locked table: $(echo "$r" | grep -o 'ERROR [0-9]*.*' | head -1); locks on t006 held after: $held"
# (iii) Lock order against a two-table RENAME: the batch holding t010 and
#       asking for t011 (sorted), and the reverse order.
for order in "t010 t011" "t011 t010"; do
  set -- $order
  ( echo "SET SESSION lock_wait_timeout = 8; START TRANSACTION READ ONLY; SELECT 1 FROM c.$1 LIMIT 0; SELECT SLEEP(2); SELECT 1 FROM c.$2 LIMIT 0; COMMIT;" | my > "$OUT/batch-$1-$2.txt" ) &
  sleep 0.5
  rn=$(my -e "SET SESSION lock_wait_timeout = 8; RENAME TABLE c.t010 TO c.r010, c.t011 TO c.r011")
  wait
  log "(iii) batch order $1,$2: batch $(grep -o 'ERROR [0-9]*[^.]*' "$OUT/batch-$1-$2.txt" | head -1 || true) rename ${rn:-ok}"
  my -e "RENAME TABLE c.r010 TO c.t010, c.r011 TO c.t011" >/dev/null 2>&1
done
log done

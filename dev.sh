#!/usr/bin/env bash
set -euo pipefail

COMPOSE="${COMPOSE_FILE:-docker-compose.dev.yml}"
DC=(docker compose -f "$COMPOSE")
BOOTSTRAP="localhost:9092"
API_BASE="${API_BASE:-http://localhost:8080}"
STORAGE_DB="${STORAGE_DB:-./data/deltaforge.db}"
IMAGE_NAME="deltaforge"
IMAGE_TAG="dev"

CHAOS_DC=(docker compose -f docker-compose.chaos.yml)
CHAOS_IMAGE="deltaforge:dev-debug"

usage() {
  cat <<'EOF'
DeltaForge dev helper

Usage:
  ./dev.sh up                     # start local stack
  ./dev.sh down                   # stop & remove
  ./dev.sh ps                     # list services
  ./dev.sh status                 # check all services health

Kafka:
  ./dev.sh k-list
  ./dev.sh k-create <topic> [partitions=6] [replication=1]
  ./dev.sh k-consume <topic> [--from-beginning] [--keys]
  ./dev.sh k-produce <topic>
  ./dev.sh k-inspect <topic> [count=1]  # consume and pretty-print JSON

Redis:
  ./dev.sh redis-cli              # open redis-cli
  ./dev.sh redis-read <stream> [count=10] [id=0-0]

NATS:
  ./dev.sh nats-sub <subject>
  ./dev.sh nats-pub <subject> <msg>
  ./dev.sh nats-stream-add <n> <subjects>
  ./dev.sh nats-stream-ls
  ./dev.sh nats-stream-info <n>
  ./dev.sh nats-stream-rm <n>
  ./dev.sh nats-stream-view <n> [count=10]
  ./dev.sh nats-consumer-add <stream> <consumer>
  ./dev.sh nats-consumer-ls <stream>
  ./dev.sh nats-consumer-next <stream> <consumer>

PostgreSQL:
  ./dev.sh pg-sh                  # psql into orders DB
  ./dev.sh pg-status              # wal_level, slots, publications
  ./dev.sh pg-slots               # list replication slots
  ./dev.sh pg-pub                 # show publication tables
  ./dev.sh pg-insert / pg-update / pg-delete
  ./dev.sh pg-reset-slot [slot]   # drop slot (default: deltaforge_*)

MySQL:
  ./dev.sh mysql-sh               # orders DB
  ./dev.sh mysql-sh-inv           # inventory DB
  ./dev.sh mysql-sh-users         # users DB
  ./dev.sh mysql-status           # binlog / GTID status
  ./dev.sh mysql-insert / mysql-update / mysql-delete

Dev:
  ./dev.sh build / build-release
  ./dev.sh run [config]           # default: examples/dev.yaml
  ./dev.sh run-pg / run-mysql / run-nats / run-envelopes
  ./dev.sh run-pg-storage         # run with Postgres storage backend
  ./dev.sh fmt / lint / test / check / cov
  ./dev.sh test-storage           # storage crate tests only
  ./dev.sh test-envelopes

Storage (SQLite):
  ./dev.sh storage-status         # path, size, table counts
  ./dev.sh storage-checkpoints    # checkpoint keys + sizes
  ./dev.sh storage-schemas        # schema registry entries
  ./dev.sh storage-reset <id>     # delete checkpoint (force replay)
  ./dev.sh storage-wipe           # delete entire storage db
  ./dev.sh storage-sh             # sqlite3 shell

Storage (PostgreSQL):
  ./dev.sh storage-pg-setup       # create deltaforge_storage db + df user
  ./dev.sh storage-pg-status      # table counts, checkpoints, schemas
  ./dev.sh storage-pg-sh          # psql shell on storage db (uses STORAGE_DSN)

API:
  ./dev.sh api-health / api-ready / api-list
  ./dev.sh api-get <pipeline>
  ./dev.sh api-pause / api-resume / api-stop / api-delete <pipeline>
  ./dev.sh api-schemas / api-sensing / api-drift / api-stats <pipeline>

Docker:
  ./dev.sh docker / docker-debug / docker-test / docker-test-debug
  ./dev.sh docker-all             # build and test all variants
  ./dev.sh docker-multi-setup / docker-multi
  ./dev.sh docker-shell           # shell into debug container

Docs:
  ./dev.sh docs / docs-build

Release:
  ./dev.sh release-check

Chaos (resilience scenarios against a live Docker Compose stack):
  ./dev.sh chaos-status                   # overview of all chaos services and Kafka topics
  ./dev.sh chaos-ui                       # start the web playground UI at http://localhost:7474
  ./dev.sh chaos-build                    # build deltaforge:dev-debug image
  ./dev.sh chaos-up [mysql|postgres]      # start chaos stack (default: mysql)
  ./dev.sh chaos-down [mysql|postgres]    # stop & remove (default: mysql)
  ./dev.sh chaos-ps                       # list all running chaos services
  ./dev.sh chaos-logs [service]           # tail logs (all services or one)
  ./dev.sh chaos-health [port=8080]       # curl DeltaForge /health
  ./dev.sh chaos-metrics [port=9000]      # curl /metrics from DeltaForge
  ./dev.sh chaos-toxics                   # show active toxiproxy toxics
  ./dev.sh chaos-run [args...]            # cargo run -p chaos -- <args>
  ./dev.sh chaos-scenario <scenario> [--source mysql|postgres] [--duration-mins N]
  ./dev.sh chaos-mysql-sh                 # mysql shell into chaos orders DB
  ./dev.sh chaos-pg-sh                    # psql shell into chaos orders DB
  ./dev.sh chaos-mysql-status             # binlog / GTID status
  ./dev.sh chaos-pg-status                # replication slots + publications
  ./dev.sh chaos-kafka-consume [topic=chaos.cdc]  # consume from chaos Kafka
  ./dev.sh chaos-reset [mysql|postgres]   # full teardown with volume wipe

Chaos — Soak (long-running, 120-table workload with random fault injection):
  ./dev.sh chaos-soak-up                  # start infra + deltaforge-soak (port 8081/9001)
  ./dev.sh chaos-soak-down                # stop soak stack (infra stays up)
  ./dev.sh chaos-soak-run [mins=120]      # run soak scenario
  ./dev.sh chaos-soak-reset               # stop + wipe soak storage (MySQL tables preserved)
  ./dev.sh chaos-soak-reload-tables       # reload soak SQL into running MySQL

Chaos — TPC-C (9-table OLTP benchmark, New-Order/Payment/Delivery mix):
  ./dev.sh chaos-tpcc-up                  # start infra + deltaforge-tpcc (port 8082/9002)
  ./dev.sh chaos-tpcc-down                # stop tpcc stack (infra stays up)
  ./dev.sh chaos-tpcc-run [mins=120]      # run TPC-C scenario
  ./dev.sh chaos-tpcc-reset               # stop + wipe tpcc storage (MySQL tables preserved)
  ./dev.sh chaos-tpcc-reload-tables       # reload TPC-C SQL into running MySQL

Notes:
- Kafka: localhost:9092, NATS: localhost:4222 (JetStream enabled)
- API: http://localhost:8080 (override with API_BASE)
- PostgreSQL: wal_level=logical; MySQL: GTID + ROW binlog
- Storage db: ./data/deltaforge.db (override with STORAGE_DB)
- Storage commands are SQLite-only; for Postgres use psql directly.
- Chaos stack: build debug image first, then chaos-up [mysql|postgres].
- Soak/TPC-C: infra must be running first (chaos-up starts it).
- Soak health: http://localhost:8081/health  metrics: http://localhost:9001/metrics
- TPC-C health: http://localhost:8082/health  metrics: http://localhost:9002/metrics
- Toxiproxy API: http://localhost:8474 (check active toxics with chaos-toxics).
EOF
}

# ── Guards ────────────────────────────────────────────────────────────────────

require_svc() {
  local svc="$1"; local label="${2:-$1}"
  if ! "${DC[@]}" ps | grep -q "${svc}.*Up"; then
    echo "⚠️  $label not up. Start with: ./dev.sh up" >&2; exit 1
  fi
}

ensure_up() {
  local missing=()
  for svc in kafka redis nats; do
    "${DC[@]}" ps | grep -q "${svc}.*Up" || missing+=("$svc")
  done
  if [[ ${#missing[@]} -gt 0 ]]; then
    echo "⚠️  dev services not up: ${missing[*]}" >&2
    echo "   start them with: ./dev.sh up" >&2; exit 1
  fi
}

ensure_storage_sqlite() {
  [[ -f "$STORAGE_DB" ]] || { echo "⚠️  No storage db at $STORAGE_DB (set STORAGE_DB or start deltaforge first)" >&2; exit 1; }
}

# ── Helpers ───────────────────────────────────────────────────────────────────

pgq()  { "${DC[@]}" exec postgres psql -U postgres -d orders "$@"; }
myq()  { "${DC[@]}" exec mysql mysql -udf -pdfpw orders -e "$@"; }
myqr() { "${DC[@]}" exec mysql mysql -uroot -ppassword -e "$@"; }

api_curl() { curl -s "$@" | jq . 2>/dev/null || curl -s "$@"; }

api_pipeline() {
  local method="$1" path="$2" pipeline="${3:-}"
  local cmd="${method##* }"  # GET/POST/DELETE
  [[ -z "$pipeline" ]] && { echo "Usage: ./dev.sh $cmd <pipeline>"; exit 1; }
  local url="$API_BASE/pipelines/$pipeline/$path"
  echo "${method} ${url}"
  if [[ "$cmd" == "DELETE" ]]; then
    curl -s -X DELETE "$url" -w "\nHTTP %{http_code}\n"
  elif [[ "$method" == POST* ]]; then
    api_curl -X POST "$url"
  else
    api_curl "$url"
  fi
}

# ── Infrastructure ────────────────────────────────────────────────────────────

cmd_up()   { "${DC[@]}" up -d; }
cmd_down() { "${DC[@]}" down -v; }
cmd_ps()   { "${DC[@]}" ps; }

cmd_status() {
  echo "=== DeltaForge Dev Stack Status ==="
  "${DC[@]}" ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || "${DC[@]}" ps
  echo ""
  local svc_info=(
    "kafka:✅ Kafka: localhost:9092:❌ Kafka"
    "redis:✅ Redis: localhost:6379:❌ Redis"
    "nats:✅ NATS: localhost:4222:❌ NATS"
    "postgres:✅ PostgreSQL: localhost:5432:⚪ PostgreSQL (optional)"
    "mysql:✅ MySQL: localhost:3306:⚪ MySQL (optional)"
  )
  for entry in "${svc_info[@]}"; do
    IFS=':' read -r svc ok fail <<< "$entry"
    if "${DC[@]}" ps | grep -q "${svc}.*Up"; then
      echo "$ok"
      [[ "$svc" == "kafka" ]] && echo "   Topics: $("${DC[@]}" exec -T kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null | wc -l)"
      [[ "$svc" == "nats" ]] && echo "   Streams: $("${DC[@]}" exec -T nats nats stream list --json 2>/dev/null | grep -c '"name"' || echo 0)"
    else
      echo "$fail: not running"
    fi
  done
  echo ""
}

# ── Kafka ─────────────────────────────────────────────────────────────────────

cmd_k_list() {
  ensure_up
  "${DC[@]}" exec kafka kafka-topics --list --bootstrap-server "$BOOTSTRAP"
}

cmd_k_create() {
  ensure_up
  local topic="${1:-}"; shift || true
  local parts="${1:-6}"; shift || true
  local repl="${1:-1}"
  [[ -z "$topic" ]] && { echo "Usage: ./dev.sh k-create <topic> [partitions] [replication]"; exit 1; }
  "${DC[@]}" exec kafka kafka-topics --create --topic "$topic" \
    --bootstrap-server "$BOOTSTRAP" --replication-factor "$repl" --partitions "$parts" || true
  echo "✅ topic: $topic"; cmd_k_list
}

cmd_k_consume() {
  ensure_up
  local topic="${1:-}"; shift || true
  [[ -z "$topic" ]] && { echo "Usage: ./dev.sh k-consume <topic> [--from-beginning] [--keys]"; exit 1; }
  local from="" props=()
  while [[ "${1:-}" =~ ^-- ]]; do
    case "$1" in
      --from-beginning) from="--from-beginning" ;;
      --keys) props+=(--property print.key=true) ;;
      *) echo "Unknown flag: $1"; exit 1 ;;
    esac; shift
  done
  "${DC[@]}" exec -e KAFKA_OPTS="" kafka kafka-console-consumer \
    --bootstrap-server "$BOOTSTRAP" --topic "$topic" $from "${props[@]}"
}

cmd_k_inspect() {
  ensure_up
  local topic="${1:-}"; shift || true; local count="${1:-1}"
  [[ -z "$topic" ]] && { echo "Usage: ./dev.sh k-inspect <topic> [count=1]"; exit 1; }
  "${DC[@]}" exec -e KAFKA_OPTS="" kafka kafka-console-consumer \
    --bootstrap-server "$BOOTSTRAP" --topic "$topic" --from-beginning \
    --max-messages "$count" 2>/dev/null \
    | while read -r line; do echo "$line" | python3 -m json.tool 2>/dev/null || echo "$line"; done
}

cmd_k_produce() {
  ensure_up
  local topic="${1:-}"
  [[ -z "$topic" ]] && { echo "Usage: ./dev.sh k-produce <topic>"; exit 1; }
  echo "Type messages, Ctrl+C to exit."
  "${DC[@]}" exec -ti kafka kafka-console-producer --bootstrap-server "$BOOTSTRAP" --topic "$topic"
}

# ── Redis ─────────────────────────────────────────────────────────────────────

cmd_redis_cli()  { ensure_up; "${DC[@]}" exec -ti redis redis-cli; }

cmd_redis_read() {
  ensure_up
  local stream="${1:-}"; shift || true; local count="${1:-10}"; shift || true; local id="${1:-0-0}"
  [[ -z "$stream" ]] && { echo "Usage: ./dev.sh redis-read <stream> [count=10] [id=0-0]"; exit 1; }
  "${DC[@]}" exec redis redis-cli XREAD COUNT "$count" STREAMS "$stream" "$id"
}

# ── NATS ──────────────────────────────────────────────────────────────────────

cmd_nats_sub() {
  require_svc nats NATS
  local subject="${1:-}"; [[ -z "$subject" ]] && { echo "Usage: ./dev.sh nats-sub <subject>"; exit 1; }
  "${DC[@]}" exec nats nats sub "$subject"
}

cmd_nats_pub() {
  require_svc nats NATS
  local subject="${1:-}"; shift || true; local message="${1:-}"
  [[ -z "$subject" || -z "$message" ]] && { echo "Usage: ./dev.sh nats-pub <subject> <message>"; exit 1; }
  "${DC[@]}" exec nats nats pub "$subject" "$message"; echo "✅ Published to $subject"
}

cmd_nats_stream_add() {
  require_svc nats NATS
  local stream="${1:-}"; shift || true; local subjects="${1:-}"
  [[ -z "$stream" || -z "$subjects" ]] && { echo "Usage: ./dev.sh nats-stream-add <stream> <subjects>"; exit 1; }
  "${DC[@]}" exec nats nats stream add "$stream" \
    --subjects "$subjects" --retention limits --storage file \
    --replicas 1 --max-age 7d --discard old
  echo "✅ Stream created: $stream"
}

cmd_nats_stream_ls()   { require_svc nats NATS; "${DC[@]}" exec nats nats stream list; }

cmd_nats_stream_info() {
  require_svc nats NATS
  local s="${1:-}"; [[ -z "$s" ]] && { echo "Usage: ./dev.sh nats-stream-info <stream>"; exit 1; }
  "${DC[@]}" exec nats nats stream info "$s"
}

cmd_nats_stream_rm() {
  require_svc nats NATS
  local s="${1:-}"; [[ -z "$s" ]] && { echo "Usage: ./dev.sh nats-stream-rm <stream>"; exit 1; }
  "${DC[@]}" exec nats nats stream delete "$s" -f; echo "✅ Stream deleted: $s"
}

cmd_nats_stream_view() {
  require_svc nats NATS
  local s="${1:-}"; shift || true; local count="${1:-10}"
  [[ -z "$s" ]] && { echo "Usage: ./dev.sh nats-stream-view <stream> [count=10]"; exit 1; }
  "${DC[@]}" exec nats nats stream view "$s" --last "$count"
}

cmd_nats_consumer_add() {
  require_svc nats NATS
  local stream="${1:-}"; shift || true; local consumer="${1:-}"
  [[ -z "$stream" || -z "$consumer" ]] && { echo "Usage: ./dev.sh nats-consumer-add <stream> <consumer>"; exit 1; }
  "${DC[@]}" exec nats nats consumer add "$stream" "$consumer" \
    --ack explicit --deliver all --replay instant --filter "" --max-deliver 3
  echo "✅ Consumer $consumer on stream $stream"
}

cmd_nats_consumer_ls() {
  require_svc nats NATS
  local s="${1:-}"; [[ -z "$s" ]] && { echo "Usage: ./dev.sh nats-consumer-ls <stream>"; exit 1; }
  "${DC[@]}" exec nats nats consumer list "$s"
}

cmd_nats_consumer_next() {
  require_svc nats NATS
  local stream="${1:-}"; shift || true; local consumer="${1:-}"
  [[ -z "$stream" || -z "$consumer" ]] && { echo "Usage: ./dev.sh nats-consumer-next <stream> <consumer>"; exit 1; }
  "${DC[@]}" exec nats nats consumer next "$stream" "$consumer" --count 1
}

# ── PostgreSQL ────────────────────────────────────────────────────────────────

cmd_pg_sh() { require_svc postgres PostgreSQL; "${DC[@]}" exec -ti postgres psql -U postgres -d orders; }

cmd_pg_status() {
  require_svc postgres PostgreSQL
  echo "=== PostgreSQL CDC Status ==="
  echo "WAL Level:";          pgq -c "SHOW wal_level;"
  echo "Replication Slots:";  pgq -c "SELECT slot_name, plugin, active, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots;"
  echo "Publications:";       pgq -c "SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete FROM pg_publication;"
  echo "Publication Tables:"; pgq -c "SELECT * FROM pg_publication_tables WHERE pubname = 'deltaforge_pub';"
}

cmd_pg_slots() {
  require_svc postgres PostgreSQL
  pgq -c "SELECT slot_name, plugin, active, restart_lsn, confirmed_flush_lsn,
                 pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as lag
          FROM pg_replication_slots;"
}

cmd_pg_pub() {
  require_svc postgres PostgreSQL
  pgq -c "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = 'deltaforge_pub';"
}

cmd_pg_insert() {
  require_svc postgres PostgreSQL
  pgq -c "INSERT INTO orders (customer_id, status, total) VALUES (1, 'pending', $((RANDOM % 1000 + 1)).99) RETURNING *;"
}

cmd_pg_update() {
  require_svc postgres PostgreSQL
  pgq -c "UPDATE orders SET status='shipped', updated_at=NOW() WHERE id=(SELECT MAX(id) FROM orders) RETURNING *;"
}

cmd_pg_delete() {
  require_svc postgres PostgreSQL
  pgq -c "DELETE FROM orders WHERE id=(SELECT MAX(id) FROM orders) RETURNING *;"
}

cmd_pg_reset_slot() {
  require_svc postgres PostgreSQL
  local slot="${1:-}"
  if [[ -z "$slot" ]]; then
    pgq -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'deltaforge_%';"
  else
    pgq -c "SELECT pg_drop_replication_slot('$slot');"
  fi
  echo "✅ Slot(s) dropped"; cmd_pg_slots
}

# ── MySQL ─────────────────────────────────────────────────────────────────────

cmd_mysql_sh()       { require_svc mysql MySQL; "${DC[@]}" exec -ti mysql mysql -udf -pdfpw orders; }
cmd_mysql_sh_inv()   { require_svc mysql MySQL; "${DC[@]}" exec -ti mysql mysql -udf -pdfpw inventory; }
cmd_mysql_sh_users() { require_svc mysql MySQL; "${DC[@]}" exec -ti mysql mysql -udf -pdfpw users; }

cmd_mysql_status() {
  require_svc mysql MySQL
  echo "=== MySQL CDC Status ==="
  myqr "SHOW MASTER STATUS\G"
  myqr "SELECT @@gtid_mode, @@enforce_gtid_consistency, @@binlog_format, @@binlog_row_image;"
  myq  "SHOW DATABASES;"
}

cmd_mysql_insert() {
  require_svc mysql MySQL
  myq "INSERT INTO order_items (order_id, product_name, quantity, price) VALUES ('ORD-$RANDOM', 'Test Product', 1, $((RANDOM % 1000 + 1)).99); SELECT * FROM order_items ORDER BY id DESC LIMIT 1;"
}

cmd_mysql_update() {
  require_svc mysql MySQL
  myq "UPDATE order_items SET quantity=quantity+1 WHERE id=(SELECT id FROM (SELECT MAX(id) as id FROM order_items) t); SELECT * FROM order_items ORDER BY id DESC LIMIT 1;"
}

cmd_mysql_delete() {
  require_svc mysql MySQL
  myq "DELETE FROM order_items WHERE id=(SELECT id FROM (SELECT MAX(id) as id FROM order_items) t); SELECT 'Deleted' as status;"
}

# ── Dev ───────────────────────────────────────────────────────────────────────

cmd_build()         { cargo build --workspace; }
cmd_build_release() { cargo build --workspace --release; }
cmd_fmt()           { cargo fmt --all; }
cmd_lint()          { cargo clippy --workspace --all-features -- -D warnings; }
cmd_test()          { cargo test --workspace --all-features; }
cmd_test_storage()  { cargo test -p storage --all-features -- --nocapture; }
cmd_cov()           { cargo llvm-cov --all-features --workspace --lcov --output-path lcov.info; }

cmd_run()           { local c="${1:-examples/dev.yaml}"; echo "cargo run -p runner -- --config $c"; cargo run -p runner -- --config "$c"; }
cmd_run_pg()        { echo "cargo run -p runner -- --config examples/dev.postgres.yaml"; cargo run -p runner -- --config examples/dev.postgres.yaml; }
cmd_run_mysql()     { echo "cargo run -p runner -- --config examples/dev.mysql.yaml"; cargo run -p runner -- --config examples/dev.mysql.yaml; }
cmd_run_nats()      { echo "cargo run -p runner -- --config examples/dev.nats.yaml"; cargo run -p runner -- --config examples/dev.nats.yaml; }
cmd_run_envelopes() { echo "cargo run -p runner -- --config examples/dev.envelopes.yaml"; cargo run -p runner -- --config examples/dev.envelopes.yaml; }

cmd_run_pg_storage() {
  local dsn="${STORAGE_DSN:-host=localhost dbname=deltaforge_storage user=df password=dfpw}"
  echo "cargo run -p runner -- --config examples/dev.yaml --storage-backend postgres --storage-dsn '$dsn'"
  cargo run -p runner -- --config examples/dev.yaml --storage-backend postgres --storage-dsn "$dsn"
}

cmd_check() {
  cargo fmt --all -- --check
  cargo clippy --workspace --all-features -- -D warnings
  cargo test --workspace --all-features
}

cmd_test_envelopes() {
  cargo test -p sinks --test kafka_sink_tests -- --include-ignored --test-threads=1 \
    kafka_sink_native_envelope kafka_sink_debezium_envelope kafka_sink_cloudevents_envelope
  echo "✅ Envelope tests passed"
}

# ── Storage ───────────────────────────────────────────────────────────────────

cmd_storage_status() {
  ensure_storage_sqlite
  echo "=== DeltaForge Storage ==="
  echo "  Path: $STORAGE_DB  ($(du -sh "$STORAGE_DB" | cut -f1))"
  echo ""
  sqlite3 "$STORAGE_DB" \
    "SELECT 'df_kv' as tbl, COUNT(*) as rows FROM df_kv UNION ALL
     SELECT 'df_log',  COUNT(*) FROM df_log  UNION ALL
     SELECT 'df_slot', COUNT(*) FROM df_slot UNION ALL
     SELECT 'df_queue',COUNT(*) FROM df_queue;"
}

cmd_storage_checkpoints() {
  ensure_storage_sqlite
  sqlite3 -column -header "$STORAGE_DB" \
    "SELECT key, length(val) as bytes, datetime(updated_at,'unixepoch') as updated
     FROM df_kv WHERE ns='checkpoints' ORDER BY key;"
}

cmd_storage_schemas() {
  ensure_storage_sqlite
  sqlite3 -column -header "$STORAGE_DB" \
    "SELECT key, COUNT(*) as versions, MAX(seq) as latest_seq
     FROM df_log WHERE ns='schemas' GROUP BY key ORDER BY key;"
}

cmd_storage_reset() {
  local id="${1:-}"; [[ -z "$id" ]] && { echo "Usage: ./dev.sh storage-reset <source-id>"; exit 1; }
  ensure_storage_sqlite
  local n; n=$(sqlite3 "$STORAGE_DB" "DELETE FROM df_kv WHERE ns='checkpoints' AND key='$id'; SELECT changes();")
  if [[ "$n" -gt 0 ]]; then
    echo "✅ Checkpoint deleted: $id (pipeline will replay from beginning)"
  else
    echo "⚠️  No checkpoint found: $id"; cmd_storage_checkpoints
  fi
}

cmd_storage_wipe() {
  [[ ! -f "$STORAGE_DB" ]] && { echo "Nothing to wipe at $STORAGE_DB"; exit 0; }
  read -r -p "⚠️  Delete $STORAGE_DB and all state? [y/N] " confirm
  [[ "$confirm" =~ ^[Yy]$ ]] && rm -f "$STORAGE_DB" "${STORAGE_DB}-wal" "${STORAGE_DB}-shm" && echo "✅ Storage wiped" || echo "Aborted"
}

cmd_storage_pg_setup() {
  require_svc postgres PostgreSQL
  echo "Creating deltaforge_storage database and df user..."
  "${DC[@]}" exec -T postgres psql -U postgres -c "CREATE DATABASE deltaforge_storage;" 2>/dev/null || echo "(database already exists)"
  "${DC[@]}" exec -T postgres psql -U postgres -c "CREATE USER df WITH PASSWORD 'dfpw';" 2>/dev/null || echo "(user already exists)"
  "${DC[@]}" exec -T postgres psql -U postgres -c "GRANT ALL ON DATABASE deltaforge_storage TO df;"
  "${DC[@]}" exec -T postgres psql -U postgres -d deltaforge_storage -c "GRANT ALL ON SCHEMA public TO df;"
  echo "✅ Done. Run: ./dev.sh run-pg-storage"
}

cmd_storage_pg_status() {
  require_svc postgres PostgreSQL
  local dsn="${STORAGE_DSN:-host=localhost dbname=deltaforge_storage user=df password=dfpw}"
  psql "$dsn" <<'SQL'
SELECT 'df_kv'    as table, COUNT(*) as rows FROM df_kv    UNION ALL
SELECT 'df_log',             COUNT(*)         FROM df_log   UNION ALL
SELECT 'df_slot',            COUNT(*)         FROM df_slot  UNION ALL
SELECT 'df_queue',           COUNT(*)         FROM df_queue;

SELECT key, length(val) as bytes, to_timestamp(updated_at) as updated
FROM df_kv WHERE ns='checkpoints' ORDER BY key;

SELECT key, COUNT(*) as versions, MAX(seq) as latest_seq
FROM df_log WHERE ns='schemas' GROUP BY key ORDER BY key;
SQL
}

cmd_storage_pg_sh() {
  require_svc postgres PostgreSQL
  local dsn="${STORAGE_DSN:-host=localhost dbname=deltaforge_storage user=df password=dfpw}"
  echo "psql $dsn"
  echo "Useful queries:"
  echo "  SELECT * FROM df_kv WHERE ns='checkpoints';"
  echo "  SELECT key, COUNT(*), MAX(seq) FROM df_log WHERE ns='schemas' GROUP BY key;"
  echo ""
  psql "$dsn"
}

cmd_storage_sh() {
  ensure_storage_sqlite
  echo "SQLite shell: $STORAGE_DB"
  echo "  SELECT * FROM df_kv WHERE ns='checkpoints';"
  echo "  SELECT key, COUNT(*) FROM df_log WHERE ns='schemas' GROUP BY key;"
  sqlite3 "$STORAGE_DB"
}

# ── API ───────────────────────────────────────────────────────────────────────

cmd_api_health()  { echo "GET $API_BASE/health"; curl -s "$API_BASE/health"; echo; }
cmd_api_ready()   { echo "GET $API_BASE/ready"; api_curl "$API_BASE/ready"; }
cmd_api_list()    { echo "GET $API_BASE/pipelines"; api_curl "$API_BASE/pipelines"; }

cmd_api_get()     { api_pipeline "GET"    ""            "${1:-}"; }
cmd_api_pause()   { api_pipeline "POST"   "pause"       "${1:-}"; }
cmd_api_resume()  { api_pipeline "POST"   "resume"      "${1:-}"; }
cmd_api_stop()    { api_pipeline "POST"   "stop"        "${1:-}"; }
cmd_api_delete()  { api_pipeline "DELETE" ""            "${1:-}"; }
cmd_api_schemas() { api_pipeline "GET"    "schemas"     "${1:-}"; }
cmd_api_sensing() { api_pipeline "GET"    "sensing/schemas" "${1:-}"; }
cmd_api_drift()   { api_pipeline "GET"    "drift"       "${1:-}"; }
cmd_api_stats()   { api_pipeline "GET"    "sensing/stats"   "${1:-}"; }

# ── Docker ────────────────────────────────────────────────────────────────────

cmd_docker() {
  docker build -t "${IMAGE_NAME}:${IMAGE_TAG}" .
  docker image ls | grep "$IMAGE_NAME"
  echo "✅ Built ${IMAGE_NAME}:${IMAGE_TAG}"
}

cmd_docker_debug() {
  docker build -t "${IMAGE_NAME}:${IMAGE_TAG}-debug" -f Dockerfile.debug .
  docker image ls | grep "$IMAGE_NAME"
  echo "✅ Built ${IMAGE_NAME}:${IMAGE_TAG}-debug"
}

cmd_docker_test()       { docker run --rm "${IMAGE_NAME}:${IMAGE_TAG}" --help && echo "✅ image ok"; }
cmd_docker_test_debug() { docker run --rm "${IMAGE_NAME}:${IMAGE_TAG}-debug" --help && echo "✅ debug image ok"; }
cmd_docker_shell()      { docker run --rm -it --entrypoint /bin/bash "${IMAGE_NAME}:${IMAGE_TAG}-debug"; }

cmd_docker_multi_setup() {
  docker buildx create --name multiarch --driver docker-container --use 2>/dev/null || true
  docker buildx ls
}

cmd_docker_multi() {
  docker buildx create --name multiarch --driver docker-container --use 2>/dev/null || true
  docker buildx build --platform linux/amd64,linux/arm64 -t "${IMAGE_NAME}:${IMAGE_TAG}-multi" .
  docker buildx build --platform linux/amd64,linux/arm64 -t "${IMAGE_NAME}:${IMAGE_TAG}-multi-debug" -f Dockerfile.debug .
  echo "✅ Multi-arch build complete (use --push to push)"
}

cmd_docker_all() {
  cmd_docker; cmd_docker_test; cmd_docker_debug; cmd_docker_test_debug
  echo "✅ All Docker builds successful!"
}

# ── Chaos ─────────────────────────────────────────────────────────────────────

_chaos_profile() {
  local src="${1:-mysql}"
  case "$src" in
    mysql)    echo "app" ;;
    postgres) echo "pg-app" ;;
    *) echo "Unknown source: $src (use mysql or postgres)" >&2; exit 1 ;;
  esac
}

cmd_chaos_build() {
  docker build -t "$CHAOS_IMAGE" -f Dockerfile.debug .
  docker image ls | grep deltaforge
  echo "✅ Built $CHAOS_IMAGE"
}

cmd_chaos_up() {
  local src="${1:-mysql}"
  local profile; profile=$(_chaos_profile "$src")
  "${CHAOS_DC[@]}" up -d
  "${CHAOS_DC[@]}" --profile "$profile" up -d
  echo "✅ Chaos stack up ($src / profile=$profile)"
  echo "   Grafana:    http://localhost:3000"
  echo "   Prometheus: http://localhost:9090"
  echo "   DeltaForge: http://localhost:8080/health"
}

cmd_chaos_down() {
  local src="${1:-mysql}"
  local profile; profile=$(_chaos_profile "$src")
  "${CHAOS_DC[@]}" --profile "$profile" down
  "${CHAOS_DC[@]}" down
}

cmd_chaos_ps() {
  "${CHAOS_DC[@]}" --profile app --profile pg-app ps
}

cmd_chaos_logs() {
  local svc="${1:-}"
  if [[ -n "$svc" ]]; then
    "${CHAOS_DC[@]}" --profile app --profile pg-app logs -f "$svc"
  else
    "${CHAOS_DC[@]}" --profile app --profile pg-app logs -f
  fi
}

cmd_chaos_run() {
  cargo run -p chaos -- "$@"
}

cmd_chaos_mysql_sh() {
  docker compose -f docker-compose.chaos.yml exec -ti mysql mysql -uroot -ppassword orders
}

cmd_chaos_pg_sh() {
  docker compose -f docker-compose.chaos.yml --profile pg-app exec -ti postgres psql -U postgres -d orders
}

cmd_chaos_mysql_status() {
  echo "=== Chaos MySQL CDC Status ==="
  docker compose -f docker-compose.chaos.yml exec -T mysql mysql -uroot -ppassword \
    -e "SHOW MASTER STATUS\G" \
    -e "SELECT @@gtid_mode, @@enforce_gtid_consistency, @@binlog_format, @@server_uuid\G"
}

cmd_chaos_pg_status() {
  echo "=== Chaos PostgreSQL CDC Status ==="
  docker compose -f docker-compose.chaos.yml --profile pg-app exec -T postgres \
    psql -U postgres -d orders \
    -c "SELECT slot_name, plugin, active, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots;" \
    -c "SELECT pubname, puballtables FROM pg_publication;" \
    -c "SELECT system_identifier FROM pg_control_system();"
}

cmd_chaos_reset() {
  local src="${1:-mysql}"
  local profile; profile=$(_chaos_profile "$src")
  echo "⚠️  Tearing down chaos stack ($src) with volume wipe..."
  "${CHAOS_DC[@]}" --profile "$profile" down -v
  "${CHAOS_DC[@]}" down -v
  echo "✅ Chaos stack reset"
}

cmd_chaos_status() {
  # Helper: check if a container is running and print a status line
  _svc_line() {
    local label="$1" container="$2" url="${3:-}" extra="${4:-}"
    local state; state=$(docker inspect --format '{{.State.Status}}' "$container" 2>/dev/null || echo "absent")
    local health; health=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}-{{end}}' "$container" 2>/dev/null || echo "-")
    local icon
    case "$state" in
      running)
        [[ "$health" == "healthy" || "$health" == "-" ]] && icon="✅" || icon="⚠️ "
        ;;
      exited) icon="💀" ;;
      absent)  icon="⬜" ;;
      *)       icon="❓" ;;
    esac
    local suffix=""
    if [[ "$state" == "running" && -n "$url" ]]; then
      local http_status; http_status=$(curl -s -o /dev/null -w "%{http_code}" --max-time 1 "$url" 2>/dev/null || echo "---")
      suffix=" [HTTP ${http_status}]"
    fi
    [[ -n "$extra" ]] && suffix="${suffix}  ${extra}"
    printf "  %-28s %s  %-10s  health:%-10s%s\n" "$label" "$icon" "$state" "$health" "$suffix"
  }

  # Helper: count active toxics across all proxies
  _toxic_count() {
    curl -s --max-time 1 http://localhost:8474/proxies 2>/dev/null \
      | python3 -c "import sys,json; d=json.load(sys.stdin); print(sum(len(v.get('toxics',[])) for v in d.values()))" 2>/dev/null \
      || echo "?"
  }

  echo ""
  echo "══════════════════════════════════════════════════════"
  echo "  DeltaForge Chaos Stack Status"
  echo "══════════════════════════════════════════════════════"

  echo ""
  echo "── Infrastructure ────────────────────────────────────"
  _svc_line "mysql"       "deltaforge-mysql-1"
  _svc_line "mysql-b"     "deltaforge-mysql-b-1"
  _svc_line "zookeeper"   "deltaforge-zookeeper-1"
  _svc_line "kafka"       "deltaforge-kafka-1"
  _svc_line "toxiproxy"   "deltaforge-toxiproxy-1"   "http://localhost:8474/proxies" \
    "(active toxics: $(_toxic_count))"
  _svc_line "prometheus"  "deltaforge-prometheus-1"   "http://localhost:9090/-/ready"
  _svc_line "grafana"     "deltaforge-grafana-1"      "http://localhost:3000/api/health"
  _svc_line "cadvisor"    "deltaforge-cadvisor-1"     "http://localhost:8888/healthz"

  echo ""
  echo "── DeltaForge Instances ──────────────────────────────"
  _svc_line "deltaforge (mysql→kafka)"  "deltaforge-deltaforge-1"     "http://localhost:8080/health"
  _svc_line "deltaforge-pg (pg→kafka)"  "deltaforge-deltaforge-pg-1"  "http://localhost:8080/health"
  _svc_line "deltaforge-soak"           "deltaforge-deltaforge-soak-1" "http://localhost:8081/health"
  _svc_line "deltaforge-tpcc"           "deltaforge-deltaforge-tpcc-1" "http://localhost:8082/health"

  echo ""
  echo "── Postgres (pg / pg-app profiles) ──────────────────"
  _svc_line "postgres"    "deltaforge-postgres-1"
  _svc_line "postgres-b"  "deltaforge-postgres-b-1"

  # Kafka topic offsets if kafka is up
  local kafka_running; kafka_running=$(docker inspect --format '{{.State.Status}}' "deltaforge-kafka-1" 2>/dev/null || echo "absent")
  if [[ "$kafka_running" == "running" ]]; then
    echo ""
    echo "── Kafka Topics ──────────────────────────────────────"
    docker compose -f docker-compose.chaos.yml exec -T kafka \
      kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null \
      | grep -v '^__' \
      | while read -r topic; do
          local offset
          offset=$(docker compose -f docker-compose.chaos.yml exec -T kafka \
            kafka-run-class kafka.tools.GetOffsetShell \
            --broker-list localhost:9092 --topic "$topic" --time -1 2>/dev/null \
            | awk -F: '{sum+=$3} END{print sum+0}')
          printf "  %-35s  %s msgs total\n" "$topic" "$offset"
        done
  fi

  echo ""
  echo "── Observability ─────────────────────────────────────"
  echo "  Grafana:    http://localhost:3000"
  echo "  Prometheus: http://localhost:9090"
  echo "  Toxiproxy:  http://localhost:8474"
  echo "══════════════════════════════════════════════════════"
  echo ""
}

cmd_chaos_health() {
  local port="${1:-8080}"
  echo "GET http://localhost:${port}/health"
  curl -s "http://localhost:${port}/health"; echo
}

cmd_chaos_metrics() {
  local port="${1:-9000}"
  curl -s "http://localhost:${port}/metrics"
}

cmd_chaos_toxics() {
  echo "=== Toxiproxy active toxics ==="
  curl -s http://localhost:8474/proxies | python3 -c "
import sys, json
proxies = json.load(sys.stdin)
any_found = False
for name, proxy in proxies.items():
    toxics = proxy.get('toxics', [])
    if toxics:
        any_found = True
        print(f'  {name}: {json.dumps(toxics, indent=4)}')
if not any_found:
    print('  (none — all proxies clean)')
" 2>/dev/null || curl -s http://localhost:8474/proxies
}

cmd_chaos_scenario() {
  local scenario="${1:-}"; shift || true
  [[ -z "$scenario" ]] && { echo "Usage: ./dev.sh chaos-scenario <scenario> [--source mysql|postgres] [--duration-mins N]"; exit 1; }
  cargo run -p chaos -- --scenario "$scenario" "$@"
}

cmd_chaos_kafka_consume() {
  local topic="${1:-chaos.cdc}"
  echo "Consuming from chaos Kafka topic: $topic (Ctrl+C to stop)"
  docker compose -f docker-compose.chaos.yml exec -e KAFKA_OPTS="" kafka \
    kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic "$topic" --from-beginning
}

# ── Chaos: Soak ───────────────────────────────────────────────────────────────

cmd_chaos_soak_up() {
  "${CHAOS_DC[@]}" up -d
  "${CHAOS_DC[@]}" --profile soak up -d
  echo "✅ Soak stack up"
  echo "   DeltaForge: http://localhost:8081/health"
  echo "   Metrics:    http://localhost:9001/metrics"
  echo "   Grafana:    http://localhost:3000"
  echo ""
  echo "Run: ./dev.sh chaos-soak-run [mins=120]"
}

cmd_chaos_soak_down() {
  "${CHAOS_DC[@]}" --profile soak stop deltaforge-soak
  echo "✅ Soak deltaforge stopped (infra still up)"
}

cmd_chaos_soak_run() {
  local mins="${1:-120}"
  cargo run -p chaos -- --scenario soak --duration-mins "$mins"
}

cmd_chaos_soak_reset() {
  local project; project=$(basename "$(pwd)")
  echo "⚠️  Stopping soak deltaforge + wiping its storage volume..."
  "${CHAOS_DC[@]}" --profile soak rm -sf deltaforge-soak 2>/dev/null || true
  docker volume rm "${project}_deltaforge_soak_data" 2>/dev/null || true
  echo "✅ Soak reset (MySQL soak tables preserved)"
  echo "   To reload soak tables: ./dev.sh chaos-soak-reload-tables"
}

cmd_chaos_soak_reload_tables() {
  echo "Reloading soak tables into running MySQL..."
  docker compose -f docker-compose.chaos.yml exec -T mysql \
    mysql -uroot -ppassword < chaos/init/mysql-soak.sql
  echo "✅ Soak tables reloaded"
}

# ── Chaos: TPC-C ─────────────────────────────────────────────────────────────

cmd_chaos_tpcc_up() {
  "${CHAOS_DC[@]}" up -d
  "${CHAOS_DC[@]}" --profile tpcc up -d
  echo "✅ TPC-C stack up"
  echo "   DeltaForge: http://localhost:8082/health"
  echo "   Metrics:    http://localhost:9002/metrics"
  echo "   Grafana:    http://localhost:3000"
  echo ""
  echo "Run: ./dev.sh chaos-tpcc-run [mins=120]"
}

cmd_chaos_tpcc_down() {
  "${CHAOS_DC[@]}" --profile tpcc stop deltaforge-tpcc
  echo "✅ TPC-C deltaforge stopped (infra still up)"
}

cmd_chaos_tpcc_run() {
  local mins="${1:-120}"
  cargo run -p chaos -- --scenario tpcc --duration-mins "$mins"
}

cmd_chaos_tpcc_reset() {
  local project; project=$(basename "$(pwd)")
  echo "⚠️  Stopping TPC-C deltaforge + wiping its storage volume..."
  "${CHAOS_DC[@]}" --profile tpcc rm -sf deltaforge-tpcc 2>/dev/null || true
  docker volume rm "${project}_deltaforge_tpcc_data" 2>/dev/null || true
  echo "✅ TPC-C reset (MySQL TPC-C tables preserved)"
  echo "   To reload TPC-C tables: ./dev.sh chaos-tpcc-reload-tables"
}

cmd_chaos_tpcc_reload_tables() {
  echo "Reloading TPC-C tables into running MySQL..."
  docker compose -f docker-compose.chaos.yml exec -T mysql \
    mysql -uroot -ppassword < chaos/init/mysql-tpcc.sql
  echo "✅ TPC-C tables reloaded"
}

# ── Docs / Release ────────────────────────────────────────────────────────────

cmd_docs()          { cd docs && mdbook serve --open; }
cmd_docs_build()    { cd docs && mdbook build; }
cmd_release_check() { cmd_check; cmd_docker_all; echo "✅ All release checks passed!"; }

# ── Main ──────────────────────────────────────────────────────────────────────

case "${1:-}" in
  up) shift; cmd_up "$@";;
  down) shift; cmd_down "$@";;
  ps) shift; cmd_ps "$@";;
  status) shift; cmd_status "$@";;

  k-list) shift; cmd_k_list "$@";;
  k-create) shift; cmd_k_create "$@";;
  k-consume) shift; cmd_k_consume "$@";;
  k-produce) shift; cmd_k_produce "$@";;
  k-inspect) shift; cmd_k_inspect "$@";;

  redis-cli) shift; cmd_redis_cli "$@";;
  redis-read) shift; cmd_redis_read "$@";;

  nats-sub) shift; cmd_nats_sub "$@";;
  nats-pub) shift; cmd_nats_pub "$@";;
  nats-stream-add) shift; cmd_nats_stream_add "$@";;
  nats-stream-ls) shift; cmd_nats_stream_ls "$@";;
  nats-stream-info) shift; cmd_nats_stream_info "$@";;
  nats-stream-rm) shift; cmd_nats_stream_rm "$@";;
  nats-stream-view) shift; cmd_nats_stream_view "$@";;
  nats-consumer-add) shift; cmd_nats_consumer_add "$@";;
  nats-consumer-ls) shift; cmd_nats_consumer_ls "$@";;
  nats-consumer-next) shift; cmd_nats_consumer_next "$@";;

  pg-sh) shift; cmd_pg_sh "$@";;
  pg-status) shift; cmd_pg_status "$@";;
  pg-slots) shift; cmd_pg_slots "$@";;
  pg-pub) shift; cmd_pg_pub "$@";;
  pg-insert) shift; cmd_pg_insert "$@";;
  pg-update) shift; cmd_pg_update "$@";;
  pg-delete) shift; cmd_pg_delete "$@";;
  pg-reset-slot) shift; cmd_pg_reset_slot "$@";;

  mysql-sh) shift; cmd_mysql_sh "$@";;
  mysql-sh-inv) shift; cmd_mysql_sh_inv "$@";;
  mysql-sh-users) shift; cmd_mysql_sh_users "$@";;
  mysql-status) shift; cmd_mysql_status "$@";;
  mysql-insert) shift; cmd_mysql_insert "$@";;
  mysql-update) shift; cmd_mysql_update "$@";;
  mysql-delete) shift; cmd_mysql_delete "$@";;

  build) shift; cmd_build "$@";;
  build-release) shift; cmd_build_release "$@";;
  run) shift; cmd_run "$@";;
  run-pg) shift; cmd_run_pg "$@";;
  run-mysql) shift; cmd_run_mysql "$@";;
  run-nats) shift; cmd_run_nats "$@";;
  run-envelopes) shift; cmd_run_envelopes "$@";;
  run-pg-storage) shift; cmd_run_pg_storage "$@";;
  fmt) shift; cmd_fmt "$@";;
  lint) shift; cmd_lint "$@";;
  test) shift; cmd_test "$@";;
  test-storage) shift; cmd_test_storage "$@";;
  test-envelopes) shift; cmd_test_envelopes "$@";;
  check) shift; cmd_check "$@";;
  cov) shift; cmd_cov "$@";;

  storage-status) shift; cmd_storage_status "$@";;
  storage-checkpoints) shift; cmd_storage_checkpoints "$@";;
  storage-schemas) shift; cmd_storage_schemas "$@";;
  storage-reset) shift; cmd_storage_reset "$@";;
  storage-wipe) shift; cmd_storage_wipe "$@";;
  storage-sh) shift; cmd_storage_sh "$@";;
  storage-pg-setup) shift; cmd_storage_pg_setup "$@";;
  storage-pg-status) shift; cmd_storage_pg_status "$@";;
  storage-pg-sh) shift; cmd_storage_pg_sh "$@";;

  api-health) shift; cmd_api_health "$@";;
  api-ready) shift; cmd_api_ready "$@";;
  api-list) shift; cmd_api_list "$@";;
  api-get) shift; cmd_api_get "$@";;
  api-pause) shift; cmd_api_pause "$@";;
  api-resume) shift; cmd_api_resume "$@";;
  api-stop) shift; cmd_api_stop "$@";;
  api-delete) shift; cmd_api_delete "$@";;
  api-schemas) shift; cmd_api_schemas "$@";;
  api-sensing) shift; cmd_api_sensing "$@";;
  api-drift) shift; cmd_api_drift "$@";;
  api-stats) shift; cmd_api_stats "$@";;

  docker) shift; cmd_docker "$@";;
  docker-debug) shift; cmd_docker_debug "$@";;
  docker-test) shift; cmd_docker_test "$@";;
  docker-test-debug) shift; cmd_docker_test_debug "$@";;
  docker-all) shift; cmd_docker_all "$@";;
  docker-multi-setup) shift; cmd_docker_multi_setup "$@";;
  docker-multi) shift; cmd_docker_multi "$@";;
  docker-shell) shift; cmd_docker_shell "$@";;

  docs) shift; cmd_docs "$@";;
  docs-build) shift; cmd_docs_build "$@";;
  release-check) shift; cmd_release_check "$@";;

  chaos-status) shift; cmd_chaos_status "$@";;
  chaos-ui) shift; cargo run -p chaos -- --scenario ui "$@";;
  chaos-build) shift; cmd_chaos_build "$@";;
  chaos-up) shift; cmd_chaos_up "$@";;
  chaos-down) shift; cmd_chaos_down "$@";;
  chaos-ps) shift; cmd_chaos_ps "$@";;
  chaos-logs) shift; cmd_chaos_logs "$@";;
  chaos-health) shift; cmd_chaos_health "$@";;
  chaos-metrics) shift; cmd_chaos_metrics "$@";;
  chaos-toxics) shift; cmd_chaos_toxics "$@";;
  chaos-run) shift; cmd_chaos_run "$@";;
  chaos-scenario) shift; cmd_chaos_scenario "$@";;
  chaos-mysql-sh) shift; cmd_chaos_mysql_sh "$@";;
  chaos-pg-sh) shift; cmd_chaos_pg_sh "$@";;
  chaos-mysql-status) shift; cmd_chaos_mysql_status "$@";;
  chaos-pg-status) shift; cmd_chaos_pg_status "$@";;
  chaos-kafka-consume) shift; cmd_chaos_kafka_consume "$@";;
  chaos-reset) shift; cmd_chaos_reset "$@";;
  chaos-soak-up) shift; cmd_chaos_soak_up "$@";;
  chaos-soak-down) shift; cmd_chaos_soak_down "$@";;
  chaos-soak-run) shift; cmd_chaos_soak_run "$@";;
  chaos-soak-reset) shift; cmd_chaos_soak_reset "$@";;
  chaos-soak-reload-tables) shift; cmd_chaos_soak_reload_tables "$@";;
  chaos-tpcc-up) shift; cmd_chaos_tpcc_up "$@";;
  chaos-tpcc-down) shift; cmd_chaos_tpcc_down "$@";;
  chaos-tpcc-run) shift; cmd_chaos_tpcc_run "$@";;
  chaos-tpcc-reset) shift; cmd_chaos_tpcc_reset "$@";;
  chaos-tpcc-reload-tables) shift; cmd_chaos_tpcc_reload_tables "$@";;

  -h|--help|help|"") usage;;
  *) echo "Unknown command: $1"; echo; usage; exit 1;;
esac
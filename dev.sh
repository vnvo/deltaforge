#!/usr/bin/env bash
set -euo pipefail

# Compose file and shorthand
COMPOSE="${COMPOSE_FILE:-docker-compose.dev.yml}"
DC=(docker compose -f "$COMPOSE")

BOOTSTRAP="localhost:9092"
API_BASE="${API_BASE:-http://localhost:8080}"

# Docker image settings
IMAGE_NAME="deltaforge"
IMAGE_TAG="dev"

usage() {
  cat <<'EOF'
DeltaForge dev helper

Usage:
  ./dev.sh up                     # start local stack
  ./dev.sh down                   # stop & remove
  ./dev.sh ps                     # list services

Kafka:
  ./dev.sh k-list
  ./dev.sh k-create <topic> [partitions=6] [replication=1]
  ./dev.sh k-consume <topic> [--from-beginning] [--keys]
  ./dev.sh k-produce <topic>

Redis:
  ./dev.sh redis-cli              # open redis-cli
  ./dev.sh redis-read <stream> [count=10] [id=0-0]

NATS:
  ./dev.sh nats-sub <subject>     # subscribe to subject
  ./dev.sh nats-pub <subject> <msg>  # publish a message
  ./dev.sh nats-stream-add <n> <subjects>  # create JetStream stream
  ./dev.sh nats-stream-ls         # list all streams
  ./dev.sh nats-stream-info <n>   # show stream details
  ./dev.sh nats-stream-rm <n>     # delete a stream
  ./dev.sh nats-stream-view <n> [count=10]  # view messages

PostgreSQL:
  ./dev.sh pg-sh                  # psql into Postgres (orders DB)
  ./dev.sh pg-status              # check CDC status (wal_level, slots, publications)
  ./dev.sh pg-slots               # list replication slots
  ./dev.sh pg-pub                 # show publication tables
  ./dev.sh pg-insert              # insert test data
  ./dev.sh pg-update              # update test data
  ./dev.sh pg-delete              # delete test data
  ./dev.sh pg-reset-slot [slot]   # drop replication slot (default: deltaforge_*)

MySQL:
  ./dev.sh mysql-sh               # mysql into MySQL (orders DB)
  ./dev.sh mysql-sh-inv           # mysql into MySQL (inventory DB)
  ./dev.sh mysql-sh-users         # mysql into MySQL (users DB)
  ./dev.sh mysql-status           # check binlog status
  ./dev.sh mysql-insert           # insert test data
  ./dev.sh mysql-update           # update test data
  ./dev.sh mysql-delete           # delete test data

Dev:
  ./dev.sh build                  # build project (debug)
  ./dev.sh build-release          # build project (release)
  ./dev.sh run [config]           # run with config (default: examples/dev.yaml)
  ./dev.sh run-pg                 # run with Postgres example config
  ./dev.sh run-mysql              # run with MySQL example config
  ./dev.sh run-nats               # run with NATS example config
  ./dev.sh fmt                    # format Rust code (cargo fmt --all)
  ./dev.sh lint                   # run clippy with warnings as errors
  ./dev.sh test                   # run test suite
  ./dev.sh check                  # run fmt-check + clippy + tests (what CI does)
  ./dev.sh cov                    # run coverage (cargo llvm-cov)

API:
  ./dev.sh api-health             # check API health
  ./dev.sh api-ready              # check API readiness + pipeline status
  ./dev.sh api-list               # list all pipelines
  ./dev.sh api-get <pipeline>     # get pipeline details
  ./dev.sh api-pause <pipeline>   # pause a pipeline
  ./dev.sh api-resume <pipeline>  # resume a pipeline
  ./dev.sh api-stop <pipeline>    # stop a pipeline
  ./dev.sh api-delete <pipeline>  # delete a pipeline
  ./dev.sh api-schemas <pipeline> # list DB schemas for pipeline
  ./dev.sh api-sensing <pipeline> # list inferred schemas (sensing)
  ./dev.sh api-drift <pipeline>   # get drift detection results
  ./dev.sh api-stats <pipeline>   # get cache statistics

Docker:
  ./dev.sh docker                 # build minimal image (~57MB)
  ./dev.sh docker-debug           # build debug image (~140MB)
  ./dev.sh docker-test            # test minimal image
  ./dev.sh docker-test-debug      # test debug image
  ./dev.sh docker-all             # build and test all variants
  ./dev.sh docker-multi-setup     # setup multi-arch builder
  ./dev.sh docker-multi           # build multi-arch (amd64 + arm64)
  ./dev.sh docker-shell           # shell into debug container

Docs:
  ./dev.sh docs                   # serve docs locally
  ./dev.sh docs-build             # build docs

Release:
  ./dev.sh release-check          # run all checks before release

Notes:
- Uses service names from docker-compose.dev.yml (kafka, redis, postgres, mysql, nats).
- Kafka broker advertises localhost:9092 (works from host & inside kafka container).
- NATS server on localhost:4222 with JetStream enabled.
- API defaults to http://localhost:8080 (set API_BASE to override).
- PostgreSQL configured with wal_level=logical for CDC.
- MySQL configured with GTID + ROW binlog for CDC.
EOF
}

ensure_up() {
  # quick check: kafka + redis should be Up
  if ! "${DC[@]}" ps | grep -q "kafka.*Up"; then
    echo "⚠️  Services not up. Start them with: ./dev.sh up" >&2
    exit 1
  fi
}

ensure_pg() {
  if ! "${DC[@]}" ps | grep -q "postgres.*Up"; then
    echo "⚠️  PostgreSQL not up. Start with: ./dev.sh up" >&2
    exit 1
  fi
}

ensure_mysql() {
  if ! "${DC[@]}" ps | grep -q "mysql.*Up"; then
    echo "⚠️  MySQL not up. Start with: ./dev.sh up" >&2
    exit 1
  fi
}

ensure_nats() {
  if ! "${DC[@]}" ps | grep -q "nats.*Up"; then
    echo "⚠️  NATS not up. Start with: ./dev.sh up" >&2
    exit 1
  fi
}

# ============================================================
# Infrastructure commands
# ============================================================
cmd_up()    { "${DC[@]}" up -d; }
cmd_down()  { "${DC[@]}" down -v; }
cmd_ps()    { "${DC[@]}" ps; }

cmd_k_list() {
  ensure_up
  "${DC[@]}" exec kafka kafka-topics --list --bootstrap-server "$BOOTSTRAP"
}

cmd_k_create() {
  ensure_up
  local topic="${1:-}"; shift || true
  local parts="${1:-6}"; shift || true
  local repl="${1:-1}"
  if [[ -z "$topic" ]]; then echo "Usage: ./dev.sh k-create <topic> [partitions] [replication]"; exit 1; fi
  "${DC[@]}" exec kafka kafka-topics \
    --create --topic "$topic" \
    --bootstrap-server "$BOOTSTRAP" \
    --replication-factor "$repl" --partitions "$parts" || true
  echo "✅ topic: $topic"
  cmd_k_list
}

cmd_k_consume() {
  ensure_up
  local topic="${1:-}"; shift || true
  if [[ -z "$topic" ]]; then echo "Usage: ./dev.sh k-consume <topic> [--from-beginning] [--keys]"; exit 1; fi
  local from=""; local props=()
  while [[ "${1:-}" =~ ^-- ]]; do
    case "$1" in
      --from-beginning) from="--from-beginning" ;;
      --keys) props+=(--property print.key=true) ;;
      *) echo "Unknown flag: $1"; exit 1 ;;
    esac
    shift
  done
  "${DC[@]}" exec -e KAFKA_OPTS="" kafka kafka-console-consumer \
    --bootstrap-server "$BOOTSTRAP" --topic "$topic" $from "${props[@]}"
}

cmd_k_produce() {
  ensure_up
  local topic="${1:-}"; shift || true
  if [[ -z "$topic" ]]; then echo "Usage: ./dev.sh k-produce <topic>"; exit 1; fi
  echo "Type messages, Ctrl+C to exit."
  "${DC[@]}" exec -ti kafka kafka-console-producer \
    --bootstrap-server "$BOOTSTRAP" --topic "$topic"
}

cmd_redis_cli() {
  ensure_up
  "${DC[@]}" exec -ti redis redis-cli
}

cmd_redis_read() {
  ensure_up
  local stream="${1:-}"; shift || true
  local count="${1:-10}"; shift || true
  local id="${1:-0-0}"
  if [[ -z "$stream" ]]; then echo "Usage: ./dev.sh redis-read <stream> [count=10] [id=0-0]"; exit 1; fi
  "${DC[@]}" exec redis redis-cli XREAD COUNT "$count" STREAMS "$stream" "$id"
}

# ============================================================
# NATS commands
# ============================================================
cmd_nats_sub() {
  ensure_nats
  local subject="${1:-}"
  if [[ -z "$subject" ]]; then echo "Usage: ./dev.sh nats-sub <subject>"; exit 1; fi
  echo "Subscribing to $subject (Ctrl+C to exit)..."
  "${DC[@]}" exec nats nats sub "$subject"
}

cmd_nats_pub() {
  ensure_nats
  local subject="${1:-}"; shift || true
  local message="${1:-}"
  if [[ -z "$subject" || -z "$message" ]]; then
    echo "Usage: ./dev.sh nats-pub <subject> <message>"
    exit 1
  fi
  "${DC[@]}" exec nats nats pub "$subject" "$message"
  echo "✅ Published to $subject"
}

cmd_nats_stream_add() {
  ensure_nats
  local stream="${1:-}"; shift || true
  local subjects="${1:-}"
  if [[ -z "$stream" || -z "$subjects" ]]; then
    echo "Usage: ./dev.sh nats-stream-add <stream> <subjects>"
    echo "Example: ./dev.sh nats-stream-add ORDERS 'orders.>'"
    exit 1
  fi
  "${DC[@]}" exec nats nats stream add "$stream" \
    --subjects "$subjects" \
    --retention limits \
    --storage file \
    --replicas 1 \
    --max-age 7d \
    --discard old
  echo "✅ Stream created: $stream"
}

cmd_nats_stream_ls() {
  ensure_nats
  "${DC[@]}" exec nats nats stream list
}

cmd_nats_stream_info() {
  ensure_nats
  local stream="${1:-}"
  if [[ -z "$stream" ]]; then echo "Usage: ./dev.sh nats-stream-info <stream>"; exit 1; fi
  "${DC[@]}" exec nats nats stream info "$stream"
}

cmd_nats_stream_rm() {
  ensure_nats
  local stream="${1:-}"
  if [[ -z "$stream" ]]; then echo "Usage: ./dev.sh nats-stream-rm <stream>"; exit 1; fi
  "${DC[@]}" exec nats nats stream delete "$stream" -f
  echo "✅ Stream deleted: $stream"
}

cmd_nats_stream_view() {
  ensure_nats
  local stream="${1:-}"; shift || true
  local count="${1:-10}"
  if [[ -z "$stream" ]]; then echo "Usage: ./dev.sh nats-stream-view <stream> [count=10]"; exit 1; fi
  "${DC[@]}" exec nats nats stream view "$stream" --last "$count"
}

# ============================================================
# PostgreSQL commands
# ============================================================
cmd_pg_sh() {
  ensure_pg
  "${DC[@]}" exec -ti postgres psql -U postgres -d orders
}

cmd_pg_status() {
  ensure_pg
  echo "=== PostgreSQL CDC Status ==="
  echo ""
  echo "WAL Level:"
  "${DC[@]}" exec postgres psql -U postgres -d orders -c "SHOW wal_level;"
  echo ""
  echo "Replication Slots:"
  "${DC[@]}" exec postgres psql -U postgres -d orders -c \
    "SELECT slot_name, plugin, slot_type, active, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots;"
  echo ""
  echo "Publications:"
  "${DC[@]}" exec postgres psql -U postgres -d orders -c \
    "SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete FROM pg_publication;"
  echo ""
  echo "Publication Tables:"
  "${DC[@]}" exec postgres psql -U postgres -d orders -c \
    "SELECT * FROM pg_publication_tables WHERE pubname = 'deltaforge_pub';"
}

cmd_pg_slots() {
  ensure_pg
  "${DC[@]}" exec postgres psql -U postgres -d orders -c \
    "SELECT slot_name, plugin, slot_type, active, restart_lsn, confirmed_flush_lsn, 
            pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as lag
     FROM pg_replication_slots;"
}

cmd_pg_pub() {
  ensure_pg
  "${DC[@]}" exec postgres psql -U postgres -d orders -c \
    "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = 'deltaforge_pub';"
}

cmd_pg_insert() {
  ensure_pg
  local price=$((RANDOM % 1000 + 1))
  echo "Inserting test order..."
  "${DC[@]}" exec postgres psql -U postgres -d orders -c \
    "INSERT INTO orders (customer_id, status, total) VALUES (1, 'pending', $price.99) RETURNING *;"
}

cmd_pg_update() {
  ensure_pg
  echo "Updating latest order..."
  "${DC[@]}" exec postgres psql -U postgres -d orders -c \
    "UPDATE orders SET status = 'shipped', updated_at = NOW() WHERE id = (SELECT MAX(id) FROM orders) RETURNING *;"
}

cmd_pg_delete() {
  ensure_pg
  echo "Deleting latest order..."
  "${DC[@]}" exec postgres psql -U postgres -d orders -c \
    "DELETE FROM orders WHERE id = (SELECT MAX(id) FROM orders) RETURNING *;"
}

cmd_pg_reset_slot() {
  ensure_pg
  local slot="${1:-}"
  if [[ -z "$slot" ]]; then
    echo "Dropping all deltaforge slots..."
    "${DC[@]}" exec postgres psql -U postgres -d orders -c \
      "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'deltaforge_%';"
  else
    echo "Dropping slot: $slot"
    "${DC[@]}" exec postgres psql -U postgres -d orders -c \
      "SELECT pg_drop_replication_slot('$slot');"
  fi
  echo "✅ Slot(s) dropped"
  cmd_pg_slots
}

# ============================================================
# MySQL commands
# ============================================================
cmd_mysql_sh() {
  ensure_mysql
  "${DC[@]}" exec -ti mysql mysql -udf -pdfpw orders
}

cmd_mysql_sh_inv() {
  ensure_mysql
  "${DC[@]}" exec -ti mysql mysql -udf -pdfpw inventory
}

cmd_mysql_sh_users() {
  ensure_mysql
  "${DC[@]}" exec -ti mysql mysql -udf -pdfpw users
}

cmd_mysql_status() {
  ensure_mysql
  echo "=== MySQL CDC Status ==="
  echo ""
  echo "Binary Log Status:"
  "${DC[@]}" exec mysql mysql -uroot -ppassword -e "SHOW MASTER STATUS\G"
  echo ""
  echo "GTID Mode:"
  "${DC[@]}" exec mysql mysql -uroot -ppassword -e "SELECT @@gtid_mode, @@enforce_gtid_consistency;"
  echo ""
  echo "Binary Log Format:"
  "${DC[@]}" exec mysql mysql -uroot -ppassword -e "SELECT @@binlog_format, @@binlog_row_image;"
  echo ""
  echo "Databases:"
  "${DC[@]}" exec mysql mysql -udf -pdfpw -e "SHOW DATABASES;"
}

cmd_mysql_insert() {
  ensure_mysql
  local price=$((RANDOM % 1000 + 1))
  echo "Inserting test order_item..."
  "${DC[@]}" exec mysql mysql -udf -pdfpw orders -e \
    "INSERT INTO order_items (order_id, product_name, quantity, price) VALUES ('ORD-$RANDOM', 'Test Product', 1, $price.99); SELECT * FROM order_items ORDER BY id DESC LIMIT 1;"
}

cmd_mysql_update() {
  ensure_mysql
  echo "Updating latest order_item..."
  "${DC[@]}" exec mysql mysql -udf -pdfpw orders -e \
    "UPDATE order_items SET quantity = quantity + 1 WHERE id = (SELECT id FROM (SELECT MAX(id) as id FROM order_items) t); SELECT * FROM order_items ORDER BY id DESC LIMIT 1;"
}

cmd_mysql_delete() {
  ensure_mysql
  echo "Deleting latest order_item..."
  "${DC[@]}" exec mysql mysql -udf -pdfpw orders -e \
    "DELETE FROM order_items WHERE id = (SELECT id FROM (SELECT MAX(id) as id FROM order_items) t); SELECT 'Deleted' as status;"
}

# ============================================================
# Dev commands
# ============================================================
cmd_build() {
  cargo build --workspace
}

cmd_build_release() {
  cargo build --workspace --release
}

cmd_run() {
  local config="${1:-examples/dev.yaml}"
  cargo run -p runner -- --config "$config"
}

cmd_run_pg() {
  POSTGRES_DSN="${POSTGRES_DSN:-postgres://postgres:postgres@localhost:5432/orders}" \
    cargo run -p runner -- --config examples/dev.postgres.yaml
}

cmd_run_mysql() {
  cargo run -p runner -- --config examples/mysql.yaml
}

cmd_run_nats() {
  cargo run -p runner -- --config examples/dev.nats.yaml
}

cmd_fmt() {
  cargo fmt --all
}

cmd_lint() {
  cargo clippy --workspace --all-features -- -D warnings
}

cmd_test() {
  cargo test --workspace --all-features
}

cmd_check() {
  # Mirror what CI does: no auto-fix, fail on issues
  cargo fmt --all -- --check
  cargo clippy --workspace --all-features -- -D warnings
  cargo test --workspace --all-features
}

cmd_cov() {
  # Requires cargo-llvm-cov to be installed
  cargo llvm-cov --all-features --workspace --lcov --output-path lcov.info
}

# ============================================================
# Turso commands (local tursodb with native CDC)
# ============================================================

# Default database path
TURSO_DB_PATH="${TURSO_DB_PATH:-/tmp/deltaforge-test.db}"

cmd_turso_setup() {
  echo "═══════════════════════════════════════════════════════════════"
  echo "  DeltaForge Turso Setup Guide"
  echo "═══════════════════════════════════════════════════════════════"
  echo ""
  echo "  Turso CDC requires the tursodb CLI (not sqld Docker image)."
  echo "  CDC is per-connection: your app must enable it."
  echo ""
  
  local all_good=true
  
  # Check 1: tursodb CLI
  echo "1. Checking tursodb CLI..."
  if command -v tursodb &>/dev/null; then
    echo "   ✅ tursodb installed: $(tursodb --version 2>/dev/null | head -1)"
  else
    echo "   ❌ tursodb not found"
    echo "      Install: curl -sSL tur.so/install | sh"
    all_good=false
  fi
  echo ""
  
  # Check 2: Dev services running
  echo "2. Checking dev services (Kafka, Redis)..."
  if "${DC[@]}" ps 2>/dev/null | grep -q "kafka.*Up"; then
    echo "   ✅ Kafka is running"
  else
    echo "   ❌ Kafka is not running"
    echo "      Run: ./dev.sh up"
    all_good=false
  fi
  
  if "${DC[@]}" ps 2>/dev/null | grep -q "redis.*Up"; then
    echo "   ✅ Redis is running"
  else
    echo "   ❌ Redis is not running"
    echo "      Run: ./dev.sh up"
    all_good=false
  fi
  echo ""
  
  if $all_good; then
    echo "✅ All prerequisites met!"
    echo ""
    echo "Next steps:"
    echo "  1. ./dev.sh turso-init     # Create test database with sample data"
    echo "  2. ./dev.sh turso-run      # Run DeltaForge with Turso source"
    echo "  3. ./dev.sh turso-shell    # Open SQL shell to make changes"
  else
    echo "❌ Some prerequisites missing. Please install/start them first."
  fi
}

cmd_turso_init() {
  echo "Creating Turso test database at: $TURSO_DB_PATH"
  
  # Remove existing database
  rm -f "$TURSO_DB_PATH" "$TURSO_DB_PATH-wal" "$TURSO_DB_PATH-shm"
  
  # Create database with schema and sample data
  tursodb "$TURSO_DB_PATH" <<'SQL'
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    total REAL NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS order_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER NOT NULL REFERENCES orders(id),
    product_id INTEGER NOT NULL,
    sku TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    price REAL NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

INSERT INTO orders (customer_id, status, total) VALUES
    (1, 'completed', 150.00),
    (1, 'pending', 75.50),
    (2, 'shipped', 200.00);

INSERT INTO order_items (order_id, product_id, sku, quantity, price) VALUES
    (1, 101, 'WIDGET-001', 2, 50.00),
    (1, 102, 'GADGET-002', 1, 50.00),
    (2, 101, 'WIDGET-001', 1, 75.50),
    (3, 103, 'THING-003', 4, 50.00);

SELECT 'orders' as tbl, COUNT(*) as cnt FROM orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM order_items;
SQL

  echo ""
  echo "✅ Database created: $TURSO_DB_PATH"
  echo ""
  echo "To run DeltaForge: ./dev.sh turso-run"
  echo "To open SQL shell: ./dev.sh turso-shell"
}

cmd_turso_shell() {
  echo "Opening Turso shell (database: $TURSO_DB_PATH)"
  echo "Type SQL commands, .quit to exit"
  echo ""
  tursodb "$TURSO_DB_PATH"
}

cmd_turso_sql() {
  local sql="${1:-}"
  if [[ -z "$sql" ]]; then
    echo "Usage: ./dev.sh turso-sql 'SELECT * FROM orders'"
    exit 1
  fi
  tursodb "$TURSO_DB_PATH" "$sql"
}

cmd_turso_run() {
  ensure_up
  
  echo "Starting DeltaForge with Turso source..."
  echo "─────────────────────────────────────────────────────────────"
  echo "  Database: $TURSO_DB_PATH"
  echo "  Kafka:    localhost:9092"
  echo "  Redis:    localhost:6379"
  echo "─────────────────────────────────────────────────────────────"
  echo ""
  echo "To make changes, run in another terminal:"
  echo "  ./dev.sh turso-shell"
  echo ""
  
  TURSO_DB_PATH="$TURSO_DB_PATH" \
  KAFKA_BROKERS="localhost:9092" \
  REDIS_URI="redis://localhost:6379" \
    cargo run -p runner -- --config examples/dev.turso.yaml
}

# ============================================================
# API commands
# ============================================================
api_curl() {
  curl -s "$@" | jq . 2>/dev/null || curl -s "$@"
}

cmd_api_health() {
  echo "GET $API_BASE/healthz"
  curl -s "$API_BASE/healthz"
  echo
}

cmd_api_ready() {
  echo "GET $API_BASE/readyz"
  api_curl "$API_BASE/readyz"
}

cmd_api_list() {
  echo "GET $API_BASE/pipelines"
  api_curl "$API_BASE/pipelines"
}

cmd_api_get() {
  local pipeline="${1:-}"
  if [[ -z "$pipeline" ]]; then echo "Usage: ./dev.sh api-get <pipeline>"; exit 1; fi
  echo "GET $API_BASE/pipelines/$pipeline"
  api_curl "$API_BASE/pipelines/$pipeline"
}

cmd_api_pause() {
  local pipeline="${1:-}"
  if [[ -z "$pipeline" ]]; then echo "Usage: ./dev.sh api-pause <pipeline>"; exit 1; fi
  echo "POST $API_BASE/pipelines/$pipeline/pause"
  api_curl -X POST "$API_BASE/pipelines/$pipeline/pause"
}

cmd_api_resume() {
  local pipeline="${1:-}"
  if [[ -z "$pipeline" ]]; then echo "Usage: ./dev.sh api-resume <pipeline>"; exit 1; fi
  echo "POST $API_BASE/pipelines/$pipeline/resume"
  api_curl -X POST "$API_BASE/pipelines/$pipeline/resume"
}

cmd_api_stop() {
  local pipeline="${1:-}"
  if [[ -z "$pipeline" ]]; then echo "Usage: ./dev.sh api-stop <pipeline>"; exit 1; fi
  echo "POST $API_BASE/pipelines/$pipeline/stop"
  api_curl -X POST "$API_BASE/pipelines/$pipeline/stop"
}

cmd_api_delete() {
  local pipeline="${1:-}"
  if [[ -z "$pipeline" ]]; then echo "Usage: ./dev.sh api-delete <pipeline>"; exit 1; fi
  echo "DELETE $API_BASE/pipelines/$pipeline"
  curl -s -X DELETE "$API_BASE/pipelines/$pipeline" -w "\nHTTP %{http_code}\n"
}

cmd_api_schemas() {
  local pipeline="${1:-}"
  if [[ -z "$pipeline" ]]; then echo "Usage: ./dev.sh api-schemas <pipeline>"; exit 1; fi
  echo "GET $API_BASE/pipelines/$pipeline/schemas"
  api_curl "$API_BASE/pipelines/$pipeline/schemas"
}

cmd_api_sensing() {
  local pipeline="${1:-}"
  if [[ -z "$pipeline" ]]; then echo "Usage: ./dev.sh api-sensing <pipeline>"; exit 1; fi
  echo "GET $API_BASE/pipelines/$pipeline/sensing/schemas"
  api_curl "$API_BASE/pipelines/$pipeline/sensing/schemas"
}

cmd_api_drift() {
  local pipeline="${1:-}"
  if [[ -z "$pipeline" ]]; then echo "Usage: ./dev.sh api-drift <pipeline>"; exit 1; fi
  echo "GET $API_BASE/pipelines/$pipeline/drift"
  api_curl "$API_BASE/pipelines/$pipeline/drift"
}

cmd_api_stats() {
  local pipeline="${1:-}"
  if [[ -z "$pipeline" ]]; then echo "Usage: ./dev.sh api-stats <pipeline>"; exit 1; fi
  echo "GET $API_BASE/pipelines/$pipeline/sensing/stats"
  api_curl "$API_BASE/pipelines/$pipeline/sensing/stats"
}

# ============================================================
# Docker commands
# ============================================================
cmd_docker() {
  echo "Building Docker image (minimal)..."
  docker build -t ${IMAGE_NAME}:${IMAGE_TAG} .
  docker image ls | grep ${IMAGE_NAME}
  echo "✅ Built ${IMAGE_NAME}:${IMAGE_TAG}"
}

cmd_docker_debug() {
  echo "Building Docker image (debug)..."
  docker build -t ${IMAGE_NAME}:${IMAGE_TAG}-debug -f Dockerfile.debug .
  docker image ls | grep ${IMAGE_NAME}
  echo "✅ Built ${IMAGE_NAME}:${IMAGE_TAG}-debug"
}

cmd_docker_test() {
  echo "Testing Docker image..."
  docker run --rm ${IMAGE_NAME}:${IMAGE_TAG} --help
  echo "✅ Docker image works!"
}

cmd_docker_test_debug() {
  echo "Testing Docker debug image..."
  docker run --rm ${IMAGE_NAME}:${IMAGE_TAG}-debug --help
  echo "✅ Docker debug image works!"
}

cmd_docker_multi_setup() {
  echo "Setting up multi-arch builder..."
  docker buildx create --name multiarch --driver docker-container --use 2>/dev/null || true
  docker buildx ls
}

cmd_docker_multi() {
  echo "Building multi-arch images (amd64 + arm64)..."
  # Ensure builder exists
  docker buildx create --name multiarch --driver docker-container --use 2>/dev/null || true
  
  echo "Building minimal image..."
  docker buildx build --platform linux/amd64,linux/arm64 -t ${IMAGE_NAME}:${IMAGE_TAG}-multi .
  
  echo "Building debug image..."
  docker buildx build --platform linux/amd64,linux/arm64 -t ${IMAGE_NAME}:${IMAGE_TAG}-multi-debug -f Dockerfile.debug .
  
  echo "✅ Multi-arch build complete (not loaded locally - use --push to push to registry)"
}

cmd_docker_shell() {
  echo "Opening shell in debug container..."
  docker run --rm -it --entrypoint /bin/bash ${IMAGE_NAME}:${IMAGE_TAG}-debug
}

cmd_docker_all() {
  echo "Building and testing all Docker variants..."
  cmd_docker
  cmd_docker_test
  cmd_docker_debug
  cmd_docker_test_debug
  echo "✅ All Docker builds successful!"
}

# ============================================================
# Docs commands
# ============================================================
cmd_docs() {
  echo "Serving documentation..."
  cd docs && mdbook serve --open
}

cmd_docs_build() {
  echo "Building documentation..."
  cd docs && mdbook build
}

# ============================================================
# Release commands
# ============================================================
cmd_release_check() {
  echo "Running release checks..."
  cmd_check
  cmd_docker_all
  echo "✅ All release checks passed!"
}

# ============================================================
# Main
# ============================================================
case "${1:-}" in
  # Infrastructure
  up) shift; cmd_up "$@";;
  down) shift; cmd_down "$@";;
  ps) shift; cmd_ps "$@";;

  # Kafka
  k-list) shift; cmd_k_list "$@";;
  k-create) shift; cmd_k_create "$@";;
  k-consume) shift; cmd_k_consume "$@";;
  k-produce) shift; cmd_k_produce "$@";;

  # Redis
  redis-cli) shift; cmd_redis_cli "$@";;
  redis-read) shift; cmd_redis_read "$@";;

  # NATS
  nats-sub) shift; cmd_nats_sub "$@";;
  nats-pub) shift; cmd_nats_pub "$@";;
  nats-stream-add) shift; cmd_nats_stream_add "$@";;
  nats-stream-ls) shift; cmd_nats_stream_ls "$@";;
  nats-stream-info) shift; cmd_nats_stream_info "$@";;
  nats-stream-rm) shift; cmd_nats_stream_rm "$@";;
  nats-stream-view) shift; cmd_nats_stream_view "$@";;

  # PostgreSQL
  pg-sh) shift; cmd_pg_sh "$@";;
  pg-status) shift; cmd_pg_status "$@";;
  pg-slots) shift; cmd_pg_slots "$@";;
  pg-pub) shift; cmd_pg_pub "$@";;
  pg-insert) shift; cmd_pg_insert "$@";;
  pg-update) shift; cmd_pg_update "$@";;
  pg-delete) shift; cmd_pg_delete "$@";;
  pg-reset-slot) shift; cmd_pg_reset_slot "$@";;

  # MySQL
  mysql-sh) shift; cmd_mysql_sh "$@";;
  mysql-sh-inv) shift; cmd_mysql_sh_inv "$@";;
  mysql-sh-users) shift; cmd_mysql_sh_users "$@";;
  mysql-status) shift; cmd_mysql_status "$@";;
  mysql-insert) shift; cmd_mysql_insert "$@";;
  mysql-update) shift; cmd_mysql_update "$@";;
  mysql-delete) shift; cmd_mysql_delete "$@";;

  # Dev
  build) shift; cmd_build "$@";;
  build-release) shift; cmd_build_release "$@";;
  run) shift; cmd_run "$@";;
  run-pg) shift; cmd_run_pg "$@";;
  run-mysql) shift; cmd_run_mysql "$@";;
  run-nats) shift; cmd_run_nats "$@";;
  fmt) shift; cmd_fmt "$@";;
  lint) shift; cmd_lint "$@";;
  test) shift; cmd_test "$@";;
  check) shift; cmd_check "$@";;
  cov) shift; cmd_cov "$@";;

  # Turso (hidden - experimental, not documented)
  # Commands still work but not shown in help
  turso-setup) shift; cmd_turso_setup "$@";;
  turso-init) shift; cmd_turso_init "$@";;
  turso-shell) shift; cmd_turso_shell "$@";;
  turso-sql) shift; cmd_turso_sql "$@";;
  turso-run) shift; cmd_turso_run "$@";;
  run-turso) shift; cmd_turso_run "$@";;

  # API
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

  # Docker
  docker) shift; cmd_docker "$@";;
  docker-debug) shift; cmd_docker_debug "$@";;
  docker-test) shift; cmd_docker_test "$@";;
  docker-test-debug) shift; cmd_docker_test_debug "$@";;
  docker-all) shift; cmd_docker_all "$@";;
  docker-multi-setup) shift; cmd_docker_multi_setup "$@";;
  docker-multi) shift; cmd_docker_multi "$@";;
  docker-shell) shift; cmd_docker_shell "$@";;

  # Docs
  docs) shift; cmd_docs "$@";;
  docs-build) shift; cmd_docs_build "$@";;

  # Release
  release-check) shift; cmd_release_check "$@";;

  -h|--help|help|"") usage;;
  *) echo "Unknown command: $1"; echo; usage; exit 1;;
esac
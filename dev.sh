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

# Turso local settings
TURSO_LOCAL_PORT="${TURSO_LOCAL_PORT:-8079}"
TURSO_LOCAL_DATA="${TURSO_LOCAL_DATA:-/tmp/deltaforge-turso}"

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

DB shells:
  ./dev.sh pg-sh                  # psql into Postgres (orders DB)
  ./dev.sh mysql-sh               # mysql into MySQL (orders DB)

Dev:
  ./dev.sh build                  # build project (debug)
  ./dev.sh build-release          # build project (release)
  ./dev.sh run [config]           # run with config (default: examples/dev.yaml)
  ./dev.sh run-turso              # run with Turso cloud (see turso-setup first)
  ./dev.sh run-pg                 # run with Postgres example config
  ./dev.sh fmt                    # format Rust code (cargo fmt --all)
  ./dev.sh lint                   # run clippy with warnings as errors
  ./dev.sh test                   # run test suite
  ./dev.sh check                  # run fmt-check + clippy + tests (what CI does)
  ./dev.sh cov                    # run coverage (cargo llvm-cov)

Turso/SQLite:
  ./dev.sh turso-setup            # check requirements for Turso cloud testing
  ./dev.sh turso-cloud-run        # run DeltaForge with Turso cloud (needs env vars)
  
  # Local libSQL server (sqld in Docker):
  ./dev.sh turso-local-up         # start local libSQL server (sqld)
  ./dev.sh turso-local-down       # stop local libSQL server
  ./dev.sh turso-local-shell      # open SQL shell to local libSQL
  ./dev.sh turso-local-run        # run DeltaForge with local libSQL
  
  # SQLite file (simplest - no Docker needed):
  ./dev.sh sqlite-init            # create test SQLite DB with sample tables
  ./dev.sh sqlite-shell           # open sqlite3 shell to test DB
  ./dev.sh sqlite-run             # run DeltaForge with SQLite file

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
- Uses service names from docker-compose.dev.yml (kafka, redis, postgres, mysql).
- Kafka broker advertises localhost:9092 (works from host & inside kafka container).
- API defaults to http://localhost:8080 (set API_BASE to override).
EOF
}

ensure_up() {
  # quick check: kafka + redis should be Up
  if ! "${DC[@]}" ps | grep -q "kafka.*Up"; then
    echo "⚠️  Services not up. Start them with: ./dev.sh up" >&2
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

cmd_pg_sh() {
  ensure_up
  "${DC[@]}" exec -ti postgres psql -U postgres -d orders
}

cmd_mysql_sh() {
  ensure_up
  "${DC[@]}" exec -ti mysql mysql -uroot -ppassword orders
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

cmd_run_turso() {
  cmd_turso_cloud_run
}

cmd_run_pg() {
  cargo run -p runner -- --config examples/postgres.yaml
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
# Turso commands
# ============================================================
cmd_turso_setup() {
  echo "═══════════════════════════════════════════════════════════════"
  echo "  DeltaForge Turso Setup Guide"
  echo "═══════════════════════════════════════════════════════════════"
  echo ""
  
  local all_good=true
  
  # Check 1: Dev services running
  echo "1. Checking dev services (Kafka, Redis)..."
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
  
  # Check 2: Turso CLI (optional but helpful)
  echo "2. Checking Turso CLI (optional)..."
  if command -v turso &>/dev/null; then
    echo "   ✅ Turso CLI installed: $(turso --version 2>/dev/null | head -1)"
  else
    echo "   ⚠️  Turso CLI not installed (optional)"
    echo "      Install: curl -sSfL https://get.tur.so/install.sh | bash"
  fi
  echo ""
  
  # Check 3: Environment variables for cloud
  echo "3. Checking Turso cloud environment variables..."
  if [[ -n "${TURSO_URL:-}" ]]; then
    echo "   ✅ TURSO_URL is set: $TURSO_URL"
  else
    echo "   ⚠️  TURSO_URL not set (needed for Turso cloud)"
    echo "      Example: export TURSO_URL=\"libsql://your-db.turso.io\""
  fi
  
  if [[ -n "${TURSO_AUTH_TOKEN:-}" ]]; then
    echo "   ✅ TURSO_AUTH_TOKEN is set: ${TURSO_AUTH_TOKEN:0:20}..."
  else
    echo "   ⚠️  TURSO_AUTH_TOKEN not set (needed for Turso cloud)"
    echo "      Get token: turso db tokens create <db-name>"
  fi
  echo ""
  
  # Check 4: Kafka topic
  echo "4. Checking Kafka topic..."
  if "${DC[@]}" exec kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null | grep -q "turso.changes"; then
    echo "   ✅ Topic 'turso.changes' exists"
  else
    echo "   ⚠️  Topic 'turso.changes' not found"
    echo "      Create: ./dev.sh k-create turso.changes"
  fi
  echo ""
  
  # Summary
  echo "═══════════════════════════════════════════════════════════════"
  if $all_good; then
    echo "  ✅ Basic requirements met!"
  else
    echo "  ⚠️  Some requirements missing - see above"
  fi
  echo ""
  echo "  Quick Start Options (easiest first):"
  echo ""
  echo "  A) SQLITE FILE (simplest - just need sqlite3):"
  echo "     ./dev.sh sqlite-init         # create test DB"
  echo "     ./dev.sh k-create sqlite.changes"
  echo "     ./dev.sh sqlite-run          # run DeltaForge"
  echo "     ./dev.sh sqlite-shell        # insert data in another terminal"
  echo ""
  echo "  B) LOCAL LIBSQL SERVER (sqld in Docker):"
  echo "     ./dev.sh turso-local-up      # start local libSQL"
  echo "     ./dev.sh turso-local-shell   # create tables"
  echo "     ./dev.sh turso-local-run     # run DeltaForge"
  echo ""
  echo "  C) TURSO CLOUD:"
  echo "     export TURSO_URL=\"libsql://your-db.turso.io\""
  echo "     export TURSO_AUTH_TOKEN=\"your-token\""
  echo "     ./dev.sh turso-cloud-run"
  echo "═══════════════════════════════════════════════════════════════"
}

cmd_turso_local_up() {
  echo "Starting local libSQL server (sqld)..."
  
  # Create data directory
  mkdir -p "$TURSO_LOCAL_DATA"
  
  # Check if already running
  if docker ps | grep -q deltaforge-sqld; then
    echo "⚠️  Local libSQL already running"
    echo "   Stop it with: ./dev.sh turso-local-down"
    return 0
  fi
  
  # Start sqld container
  docker run -d \
    --name deltaforge-sqld \
    --network deltaforge_default \
    -p ${TURSO_LOCAL_PORT}:8080 \
    -v "${TURSO_LOCAL_DATA}:/var/lib/sqld" \
    ghcr.io/tursodatabase/libsql-server:latest \
    sqld --http-listen-addr 0.0.0.0:8080
  
  echo "✅ Local libSQL started on port ${TURSO_LOCAL_PORT}"
  echo ""
  echo "Next steps:"
  echo "  1. Create tables:    ./dev.sh turso-local-shell"
  echo "  2. Run DeltaForge:   ./dev.sh turso-local-run"
  echo ""
  echo "Sample SQL to create test tables:"
  echo "  CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);"
  echo "  CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL);"
  echo "  INSERT INTO users (name, email) VALUES ('Alice', 'alice@test.com');"
}

cmd_turso_local_down() {
  echo "Stopping local libSQL server..."
  docker rm -f deltaforge-sqld 2>/dev/null || true
  echo "✅ Local libSQL stopped"
}

cmd_turso_local_shell() {
  # Check if sqld is running
  if ! docker ps | grep -q deltaforge-sqld; then
    echo "❌ Local libSQL not running. Start it with: ./dev.sh turso-local-up"
    exit 1
  fi
  
  # Check if sqlite3 is available
  if command -v sqlite3 &>/dev/null; then
    echo "Connecting to local libSQL via HTTP..."
    echo "Type SQL commands, or use these to get started:"
    echo ""
    echo "  CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);"
    echo "  CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL, status TEXT);"
    echo "  INSERT INTO users (name, email) VALUES ('Alice', 'alice@test.com');"
    echo "  INSERT INTO orders (user_id, total, status) VALUES (1, 99.99, 'completed');"
    echo "  .tables"
    echo "  .quit"
    echo ""
    # Use docker exec to run sqlite3 inside the container
    docker exec -it deltaforge-sqld sqlite3 /var/lib/sqld/data/data
  else
    echo "sqlite3 not found. You can:"
    echo "  1. Install sqlite3"
    echo "  2. Use curl to send SQL:"
    echo ""
    echo "     curl -s http://localhost:${TURSO_LOCAL_PORT} -d '{\"statements\": [\"SELECT * FROM users\"]}'"
    echo ""
    echo "  3. Or exec into the container:"
    echo "     docker exec -it deltaforge-sqld sqlite3 /var/lib/sqld/data/data"
  fi
}

cmd_turso_local_run() {
  echo "Running DeltaForge with local libSQL..."
  
  # Check if sqld is running
  if ! docker ps | grep -q deltaforge-sqld; then
    echo "❌ Local libSQL not running."
    echo "   Start it with: ./dev.sh turso-local-up"
    exit 1
  fi
  
  # Check if dev services are up
  ensure_up
  
  # Check if config exists
  if [[ ! -f "examples/dev.turso-local.yaml" ]]; then
    echo "❌ Config file not found: examples/dev.turso-local.yaml"
    exit 1
  fi
  
  echo "✅ Local libSQL running on port ${TURSO_LOCAL_PORT}"
  echo "✅ Kafka and Redis running"
  echo ""
  echo "Starting DeltaForge..."
  echo "─────────────────────────────────────────────────────────────"
  
  TURSO_LOCAL_URL="http://localhost:${TURSO_LOCAL_PORT}" \
  KAFKA_BROKERS="localhost:9092" \
  REDIS_URI="redis://localhost:6379" \
    cargo run -p runner -- --config examples/dev.turso-local.yaml
}

cmd_turso_cloud_run() {
  echo "Running DeltaForge with Turso cloud..."
  
  # Check environment variables
  local missing=false
  
  if [[ -z "${TURSO_URL:-}" ]]; then
    echo "❌ TURSO_URL not set"
    echo "   export TURSO_URL=\"libsql://your-db.turso.io\""
    missing=true
  fi
  
  if [[ -z "${TURSO_AUTH_TOKEN:-}" ]]; then
    echo "❌ TURSO_AUTH_TOKEN not set"
    echo "   export TURSO_AUTH_TOKEN=\"your-token\""
    echo "   Get token: turso db tokens create <db-name>"
    missing=true
  fi
  
  if $missing; then
    echo ""
    echo "Run './dev.sh turso-setup' for full setup guide"
    exit 1
  fi
  
  # Check if dev services are up
  ensure_up
  
  echo "✅ TURSO_URL: $TURSO_URL"
  echo "✅ TURSO_AUTH_TOKEN: ${TURSO_AUTH_TOKEN:0:20}..."
  echo "✅ Kafka and Redis running"
  echo ""
  echo "Starting DeltaForge..."
  echo "─────────────────────────────────────────────────────────────"
  
  KAFKA_BROKERS="localhost:9092" \
  REDIS_URI="redis://localhost:6379" \
    cargo run -p runner -- --config examples/dev.turso.yaml
}

# ============================================================
# SQLite file commands (simplest - no Docker/sqld needed)
# ============================================================
SQLITE_DB_PATH="${SQLITE_DB_PATH:-/tmp/deltaforge-test.db}"

cmd_sqlite_init() {
  echo "Creating test SQLite database at: $SQLITE_DB_PATH"
  
  # Check if sqlite3 is available
  if ! command -v sqlite3 &>/dev/null; then
    echo "❌ sqlite3 not found. Install it:"
    echo "   macOS:  brew install sqlite"
    echo "   Ubuntu: sudo apt install sqlite3"
    exit 1
  fi
  
  # Create database with test tables
  sqlite3 "$SQLITE_DB_PATH" <<'SQL'
-- Users table
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER,
  total REAL NOT NULL,
  status TEXT DEFAULT 'pending',
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');
INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com');
INSERT INTO orders (user_id, total, status) VALUES (1, 99.99, 'completed');
INSERT INTO orders (user_id, total, status) VALUES (2, 149.50, 'pending');

-- Show what was created
.tables
SELECT 'Users:' as '';
SELECT * FROM users;
SELECT 'Orders:' as '';
SELECT * FROM orders;
SQL

  echo ""
  echo "✅ Database created at: $SQLITE_DB_PATH"
  echo ""
  echo "Next steps:"
  echo "  1. Create Kafka topic:  ./dev.sh k-create sqlite.changes"
  echo "  2. Run DeltaForge:      ./dev.sh sqlite-run"
  echo "  3. Insert more data:    ./dev.sh sqlite-shell"
  echo "     sqlite> INSERT INTO users (name, email) VALUES ('Charlie', 'charlie@test.com');"
  echo "  4. Watch events:        ./dev.sh k-consume sqlite.changes --from-beginning"
}

cmd_sqlite_shell() {
  if ! command -v sqlite3 &>/dev/null; then
    echo "❌ sqlite3 not found"
    exit 1
  fi
  
  if [[ ! -f "$SQLITE_DB_PATH" ]]; then
    echo "❌ Database not found: $SQLITE_DB_PATH"
    echo "   Create it with: ./dev.sh sqlite-init"
    exit 1
  fi
  
  echo "Opening SQLite shell..."
  echo "Try: INSERT INTO users (name, email) VALUES ('Test', 'test@test.com');"
  echo ""
  sqlite3 "$SQLITE_DB_PATH"
}

cmd_sqlite_run() {
  echo "Running DeltaForge with SQLite file..."
  
  # Check if database exists
  if [[ ! -f "$SQLITE_DB_PATH" ]]; then
    echo "❌ Database not found: $SQLITE_DB_PATH"
    echo "   Create it with: ./dev.sh sqlite-init"
    exit 1
  fi
  
  # Check if dev services are up
  ensure_up
  
  # Check if config exists
  if [[ ! -f "examples/dev.sqlite.yaml" ]]; then
    echo "❌ Config file not found: examples/dev.sqlite.yaml"
    exit 1
  fi
  
  echo "✅ SQLite database: $SQLITE_DB_PATH"
  echo "✅ Kafka and Redis running"
  echo ""
  echo "Starting DeltaForge..."
  echo "─────────────────────────────────────────────────────────────"
  echo "Tip: In another terminal, insert data with:"
  echo "     ./dev.sh sqlite-shell"
  echo "     sqlite> INSERT INTO users (name, email) VALUES ('New', 'new@test.com');"
  echo "─────────────────────────────────────────────────────────────"
  echo ""
  
  SQLITE_DB_PATH="$SQLITE_DB_PATH" \
  KAFKA_BROKERS="localhost:9092" \
  REDIS_URI="redis://localhost:6379" \
    cargo run -p runner -- --config examples/dev.sqlite.yaml
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

  # DB shells
  pg-sh) shift; cmd_pg_sh "$@";;
  mysql-sh) shift; cmd_mysql_sh "$@";;

  # Dev
  build) shift; cmd_build "$@";;
  build-release) shift; cmd_build_release "$@";;
  run) shift; cmd_run "$@";;
  run-turso) shift; cmd_run_turso "$@";;
  run-pg) shift; cmd_run_pg "$@";;
  fmt) shift; cmd_fmt "$@";;
  lint) shift; cmd_lint "$@";;
  test) shift; cmd_test "$@";;
  check) shift; cmd_check "$@";;
  cov) shift; cmd_cov "$@";;

  # Turso
  turso-setup) shift; cmd_turso_setup "$@";;
  turso-local-up) shift; cmd_turso_local_up "$@";;
  turso-local-down) shift; cmd_turso_local_down "$@";;
  turso-local-shell) shift; cmd_turso_local_shell "$@";;
  turso-local-run) shift; cmd_turso_local_run "$@";;
  turso-cloud-run) shift; cmd_turso_cloud_run "$@";;

  # SQLite (simplest)
  sqlite-init) shift; cmd_sqlite_init "$@";;
  sqlite-shell) shift; cmd_sqlite_shell "$@";;
  sqlite-run) shift; cmd_sqlite_run "$@";;

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
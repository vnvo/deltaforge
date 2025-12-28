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

DB shells:
  ./dev.sh pg-sh                  # psql into Postgres (orders DB)
  ./dev.sh mysql-sh               # mysql into MySQL (orders DB)

Dev:
  ./dev.sh build                  # build project (debug)
  ./dev.sh build-release          # build project (release)
  ./dev.sh run [config]           # run with config (default: examples/dev.yaml)
  ./dev.sh run-pg                 # run with Postgres example config
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
- Uses service names from docker-compose.dev.yml (kafka, redis, postgres, mysql).
- Kafka broker advertises localhost:9092 (works from host & inside kafka container).
- API defaults to http://localhost:8080 (set API_BASE to override).
- Turso source uses native CDC via local libSQL server.
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
  
  # Check 3: Test database
  echo "3. Checking test database..."
  if [[ -f "$TURSO_DB_PATH" ]]; then
    echo "   ✅ Database exists: $TURSO_DB_PATH"
  else
    echo "   ⚠️  Database not found: $TURSO_DB_PATH"
    echo "      Create: ./dev.sh turso-init"
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
    echo "  ✅ Requirements met!"
  else
    echo "  ⚠️  Some requirements missing - see above"
  fi
  echo ""
  echo "  Quick Start:"
  echo ""
  echo "    1. Install tursodb:       curl -sSL tur.so/install | sh"
  echo "    2. Start infrastructure:  ./dev.sh up"
  echo "    3. Create Kafka topic:    ./dev.sh k-create turso.changes"
  echo "    4. Create test database:  ./dev.sh turso-init"
  echo "    5. Make changes:          ./dev.sh turso-shell"
  echo "    6. Run DeltaForge:        ./dev.sh turso-run"
  echo "    7. Watch events:          ./dev.sh k-consume turso.changes --from-beginning"
  echo ""
  echo "  IMPORTANT: CDC is per-connection!"
  echo "  The turso-shell command enables CDC. Changes made there are captured."
  echo "  Your app must also run: PRAGMA unstable_capture_data_changes_conn('full');"
  echo "═══════════════════════════════════════════════════════════════"
}

cmd_turso_init() {
  echo "Creating test database with CDC table..."
  
  # Check tursodb is available
  if ! command -v tursodb &>/dev/null; then
    echo "❌ tursodb not found"
    echo "   Install: curl -sSL tur.so/install | sh"
    exit 1
  fi
  
  # Remove existing database if present
  if [[ -f "$TURSO_DB_PATH" ]]; then
    echo "Removing existing database: $TURSO_DB_PATH"
    rm -f "$TURSO_DB_PATH"
  fi
  
  # Create database with CDC enabled and sample tables
  tursodb "$TURSO_DB_PATH" <<'SQL'
-- Enable CDC for this connection
PRAGMA unstable_capture_data_changes_conn('full');

-- Create test tables
CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER,
  total REAL NOT NULL,
  status TEXT DEFAULT 'pending',
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data (these will be in turso_cdc)
INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');
INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com');
INSERT INTO orders (user_id, total, status) VALUES (1, 99.99, 'completed');
INSERT INTO orders (user_id, total, status) VALUES (2, 149.50, 'pending');

-- Show what was created
SELECT 'Users:' as info;
SELECT * FROM users;
SELECT 'Orders:' as info;
SELECT * FROM orders;
SELECT 'CDC entries:' as info;
SELECT change_id, change_type, table_name FROM turso_cdc;
SQL

  echo ""
  echo "✅ Database created: $TURSO_DB_PATH"
  echo ""
  echo "Next steps:"
  echo "  1. Make more changes:  ./dev.sh turso-shell"
  echo "  2. Run DeltaForge:     ./dev.sh turso-run"
}

cmd_turso_shell() {
  # Check tursodb is available
  if ! command -v tursodb &>/dev/null; then
    echo "❌ tursodb not found"
    echo "   Install: curl -sSL tur.so/install | sh"
    exit 1
  fi
  
  # Check database exists
  if [[ ! -f "$TURSO_DB_PATH" ]]; then
    echo "❌ Database not found: $TURSO_DB_PATH"
    echo "   Create it with: ./dev.sh turso-init"
    exit 1
  fi
  
  echo "Opening tursodb shell with CDC enabled..."
  echo "Database: $TURSO_DB_PATH"
  echo ""
  echo "CDC is enabled - all changes will be captured to turso_cdc table."
  echo ""
  echo "Example commands:"
  echo "  INSERT INTO users (name, email) VALUES ('Charlie', 'charlie@test.com');"
  echo "  UPDATE users SET email = 'alice.new@test.com' WHERE name = 'Alice';"
  echo "  DELETE FROM users WHERE name = 'Bob';"
  echo "  SELECT * FROM turso_cdc;  -- see captured changes"
  echo "─────────────────────────────────────────────────────────────"
  
  # Run tursodb with CDC enabled
  # Using -e to echo commands and piping the pragma first
  echo "PRAGMA unstable_capture_data_changes_conn('full');" | tursodb "$TURSO_DB_PATH" -q
  tursodb "$TURSO_DB_PATH"
}

cmd_turso_sql() {
  local sql="${1:-}"
  if [[ -z "$sql" ]]; then
    echo "Usage: ./dev.sh turso-sql \"SELECT * FROM users\""
    exit 1
  fi
  
  # Check tursodb is available
  if ! command -v tursodb &>/dev/null; then
    echo "❌ tursodb not found"
    echo "   Install: curl -sSL tur.so/install | sh"
    exit 1
  fi
  
  # Check database exists
  if [[ ! -f "$TURSO_DB_PATH" ]]; then
    echo "❌ Database not found: $TURSO_DB_PATH"
    echo "   Create it with: ./dev.sh turso-init"
    exit 1
  fi
  
  # Run SQL with CDC enabled
  echo -e "PRAGMA unstable_capture_data_changes_conn('full');\n$sql" | tursodb "$TURSO_DB_PATH" -q
}

cmd_turso_run() {
  echo "Running DeltaForge with local Turso..."
  
  # Check database exists
  if [[ ! -f "$TURSO_DB_PATH" ]]; then
    echo "❌ Database not found: $TURSO_DB_PATH"
    echo "   Create it with: ./dev.sh turso-init"
    exit 1
  fi
  
  # Check if dev services are up
  ensure_up
  
  # Check if config exists
  if [[ ! -f "examples/dev.turso.yaml" ]]; then
    echo "❌ Config file not found: examples/dev.turso.yaml"
    exit 1
  fi
  
  # Check if turso_cdc table exists
  if ! tursodb "$TURSO_DB_PATH" -q "SELECT 1 FROM turso_cdc LIMIT 1" 2>/dev/null; then
    echo "❌ CDC table 'turso_cdc' not found in database"
    echo "   The database may not have been initialized with CDC enabled."
    echo "   Recreate with: ./dev.sh turso-init"
    exit 1
  fi
  
  echo "✅ Database: $TURSO_DB_PATH"
  echo "✅ CDC table found"
  echo "✅ Kafka and Redis running"
  echo ""
  echo "Starting DeltaForge..."
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

  # DB shells
  pg-sh) shift; cmd_pg_sh "$@";;
  mysql-sh) shift; cmd_mysql_sh "$@";;

  # Dev
  build) shift; cmd_build "$@";;
  build-release) shift; cmd_build_release "$@";;
  run) shift; cmd_run "$@";;
  run-pg) shift; cmd_run_pg "$@";;
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
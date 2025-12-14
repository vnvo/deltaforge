#!/usr/bin/env bash
set -euo pipefail

# Compose file and shorthand
COMPOSE="${COMPOSE_FILE:-docker-compose.dev.yml}"
DC=(docker compose -f "$COMPOSE")

BOOTSTRAP="localhost:9092"

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
  ./dev.sh run                    # run with examples/dev.yaml
  ./dev.sh fmt                    # format Rust code (cargo fmt --all)
  ./dev.sh lint                   # run clippy with warnings as errors
  ./dev.sh test                   # run test suite
  ./dev.sh check                  # run fmt-check + clippy + tests (what CI does)
  ./dev.sh cov                    # run coverage (cargo llvm-cov)

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
  cargo run -p runner -- --config examples/dev.yaml
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
  fmt) shift; cmd_fmt "$@";;
  lint) shift; cmd_lint "$@";;
  test) shift; cmd_test "$@";;
  check) shift; cmd_check "$@";;
  cov) shift; cmd_cov "$@";;

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
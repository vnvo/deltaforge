#!/usr/bin/env bash
set -euo pipefail

# Compose file and shorthand
COMPOSE="${COMPOSE_FILE:-docker-compose.dev.yml}"
DC=(docker compose -f "$COMPOSE")

BOOTSTRAP="localhost:9092"

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
  ./dev.sh fmt                    # format Rust code (cargo fmt --all)
  ./dev.sh lint                   # run clippy with warnings as errors
  ./dev.sh test                   # run test suite
  ./dev.sh check                  # run fmt-check + clippy + tests (what CI does)
  ./dev.sh cov                    # run coverage (cargo llvm-cov)

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

case "${1:-}" in
  up) shift; cmd_up "$@";;
  down) shift; cmd_down "$@";;
  ps) shift; cmd_ps "$@";;

  k-list) shift; cmd_k_list "$@";;
  k-create) shift; cmd_k_create "$@";;
  k-consume) shift; cmd_k_consume "$@";;
  k-produce) shift; cmd_k_produce "$@";;

  redis-cli) shift; cmd_redis_cli "$@";;
  redis-read) shift; cmd_redis_read "$@";;

  pg-sh) shift; cmd_pg_sh "$@";;
  mysql-sh) shift; cmd_mysql_sh "$@";;

  fmt) shift; cmd_fmt "$@";;
  lint) shift; cmd_lint "$@";;
  test) shift; cmd_test "$@";;
  check) shift; cmd_check "$@";;
  cov) shift; cmd_cov "$@";;

  -h|--help|help|"") usage;;
  *) echo "Unknown command: $1"; echo; usage; exit 1;;
esac

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

  -h|--help|help|"") usage;;
  *) echo "Unknown command: $1"; echo; usage; exit 1;;
esac

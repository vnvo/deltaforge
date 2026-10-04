#!/usr/bin/env bash
# Exact-tree gate. The suites it runs are defined in scripts/gate-suites.txt.
#
#   scripts/gate.sh [label]    run the full gate
#   scripts/gate.sh --check    only check that the manifest covers every
#                              integration test file
#
# Steps: manifest check; fmt; clippy -D warnings (also with the `vault`
# feature); workspace tests; every test binary built once; then the
# `serial-pg` and `serial` suites one at a time in one lane while the
# `parallel` suites run GATE_PARALLEL (default 3) at a time. Needs Docker.
# Prints one line per step (exit code, passed / failed counts) and the git
# tree hash before and after: a gate result is valid only when they match.
# Results go to $GATE_DIR/<label> (default target/gate/<label>).
set -u
ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"
MANIFEST="$ROOT/scripts/gate-suites.txt"

# Every crates/<crate>/tests/<name>.rs listed exactly once; no stale entries.
check_manifest() {
  local ok=0 f name listed
  listed=$(grep -Ev '^\s*(#|$)' "$MANIFEST" | awk '{print $2}' | grep -Ev '/lib(-pg)?$' | sort)
  for f in crates/*/tests/*.rs; do
    name="$(basename "$(dirname "$(dirname "$f")")")/$(basename "$f" .rs)"
    case $(grep -cx "$name" <<<"$listed") in
      1) ;;
      0) echo "manifest: $name is not classified in scripts/gate-suites.txt"; ok=1 ;;
      *) echo "manifest: $name is listed more than once"; ok=1 ;;
    esac
  done
  for name in $listed; do
    [ -f "crates/${name%%/*}/tests/${name#*/}.rs" ] ||
      { echo "manifest: $name has no test file"; ok=1; }
  done
  return $ok
}

if [ "${1:-}" = "--check" ]; then
  check_manifest && echo "manifest: every integration test file is classified"
  exit $?
fi
check_manifest || exit 2

LABEL=${1:-gate}
OUT="${GATE_DIR:-$ROOT/target/gate}/$LABEL"
rm -rf "$OUT"
mkdir -p "$OUT"
export OUT

free_gb=$(df --output=avail -BG / | tail -1 | tr -dc 0-9)
if [ "$free_gb" -lt 15 ]; then cargo clean >/dev/null 2>&1; fi
docker ps -aq --filter ancestor=mysql:8.4 | xargs -r docker rm -f -v >/dev/null 2>&1
docker rm -f -v df-it-pg >/dev/null 2>&1

git add -A && echo "tree before $(git write-tree)" | tee "$OUT/summary"
start=$(date +%s)

summ() { # name exit-code log
  local c
  c=$(grep "^test result" "$3" 2>/dev/null | awk '{p+=$4; f+=$6} END {printf "%d passed, %d failed", p, f}')
  echo "$1 rc=$2 ($c)" >> "$OUT/summary"
}
run() { # name log cmd...
  local name=$1 log=$2
  shift 2
  "$@" > "$log" 2>&1
  summ "$name" "$?" "$log"
}
# One manifest suite: name, then cargo test arguments.
suite() {
  local name=$1
  shift
  local args=("$@")
  case " ${args[*]} " in
    *" -- "*) ;;
    *) args+=(-- --include-ignored --test-threads=1) ;;
  esac
  run "$name" "$OUT/$(tr / _ <<<"$name").log" cargo test "${args[@]}"
}
export -f run summ suite

lane() { grep -E "^$1[[:space:]]" "$MANIFEST" | awk '{$1=""; print substr($0, 2)}'; }

# ---- static checks and workspace tests
run fmt "$OUT/fmt.log" cargo fmt --all -- --check
run clippy "$OUT/clippy.log" cargo clippy --workspace --all-targets -- -D warnings
run clippy-vault "$OUT/clippy-vault.log" \
  cargo clippy -p sources -p runner -p secrets --features vault --all-targets -- -D warnings
run workspace "$OUT/workspace.log" cargo test --workspace --no-fail-fast

# ---- build every gated test binary once
lane serial-pg >  "$OUT/suites"
lane serial    >> "$OUT/suites"
lane parallel  >> "$OUT/suites"
while read -r _ args; do
  cargo test ${args%% -- *} --no-run >> "$OUT/build.log" 2>&1
done < "$OUT/suites"

serial_lane() {
  docker run -d --name df-it-pg -e POSTGRES_PASSWORD=pg -p 55432:5432 postgres:17 >/dev/null
  for _ in $(seq 60); do
    docker exec df-it-pg pg_isready -U postgres >/dev/null 2>&1 && break
    sleep 1
  done
  sleep 2
  while read -r name args; do
    DELTAFORGE_IT_PG_DSN="host=127.0.0.1 port=55432 user=postgres password=pg dbname=postgres" \
      suite "$name" $args
  done < <(lane serial-pg)
  docker rm -f -v df-it-pg >/dev/null
  while read -r name args; do suite "$name" $args; done < <(lane serial)
}

serial_lane &
serial_pid=$!
lane parallel | xargs -P "${GATE_PARALLEL:-3}" -L 1 bash -c 'suite "$@"' _
wait $serial_pid

docker ps -aq --filter ancestor=mysql:8.4 | xargs -r docker rm -f -v >/dev/null 2>&1
git add -A && echo "tree after $(git write-tree)" >> "$OUT/summary"
echo "elapsed $(( ($(date +%s) - start) / 60 )) min" >> "$OUT/summary"
echo "suites ok: $(grep -c 'rc=0' "$OUT/summary")" >> "$OUT/summary"
grep -v "rc=0" "$OUT/summary" | grep "rc=" | sed 's/^/FAILED /' >> "$OUT/summary"
grep -E "^excluded" "$MANIFEST" | awk '{print "not gated: " $2}' >> "$OUT/summary"
cat "$OUT/summary"
! grep -q "^FAILED" "$OUT/summary"

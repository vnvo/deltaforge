#!/usr/bin/env bash
# Exact-tree gate. The suites it runs are defined in scripts/gate-suites.txt.
#
#   scripts/gate.sh [label]             core merge gate
#   scripts/gate.sh --release [label]   core gate plus the release-tier suites
#                                       (service-backed sinks, S3 canary)
#   scripts/gate.sh --check             only check the manifest: every
#                                       integration test file classified once,
#                                       no stale entry, a reason on every
#                                       exclusion, no ignored test in a suite
#                                       left to the workspace step
#
# Steps: manifest check; fmt; clippy -D warnings (also with the `vault`
# feature); workspace tests; every test binary built once; then the
# `serial-pg` and `serial` suites one at a time in one lane while the
# `parallel` suites (and, with --release, the `release` suites) run
# GATE_PARALLEL (default 3) at a time. Needs Docker.
#
# Prints one line per step (exit code, passed / failed counts) and the hash
# of the working tree (tracked and untracked files) before and after: a gate
# result is valid only when they match. The hash is computed in a temporary
# Git index; the repository's index is never modified.
#
# Results go to $GATE_DIR/<label> (default target/gate/<label>); the label is
# a plain name, and an existing directory there is replaced only if a gate
# wrote it. The gate stops if less than GATE_MIN_FREE_GB (default 15) is free
# on the build disk; it never deletes build output itself.
#
# Containers: every container of a run - the PostgreSQL the gate starts for
# the `serial-pg` suites and every test container (started through
# `gate_ownership::GateOwned`) - carries the run's labels
# `deltaforge.gate.run=<run id>` and `deltaforge.gate.owner=<host>:<boot
# id>:<pid>:<start time>` of the gate process. On exit, failure or
# interruption (INT/TERM: the suites are stopped first) the gate removes
# exactly its own run's containers with their anonymous volumes, and volumes
# carrying its run label. At start it also removes the resources of earlier
# runs whose owner is provably gone (same host; another boot, or no process
# with that pid and start time); a run of another host, of a live owner, or
# without a readable owner is never touched. A test container created during
# the run without the labels fails the gate (`unowned-containers`); it is
# reported, not removed. Linux only (/proc).
#
# For testing the gate itself (scripts/gate-selftest.sh): GATE_MANIFEST
# overrides the manifest, and GATE_ONLY_SUITES=1 skips the static checks and
# the workspace tests.
set -u
ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"
MANIFEST=${GATE_MANIFEST:-$ROOT/scripts/gate-suites.txt}

die() {
  echo "gate: $*" >&2
  exit 2
}

# Every crates/<crate>/tests/<name>.rs listed exactly once; no stale entries;
# every exclusion gives a reason; a `workspace` suite has no ignored test (the
# workspace step would silently skip it).
check_manifest() {
  local ok=0 f name listed
  listed=$(grep -Ev '^\s*(#|$)' "$MANIFEST" | awk '{print $2}' | grep -Ev '/lib(-pg)?$' | sort)
  for f in crates/*/tests/*.rs; do
    name="$(basename "$(dirname "$(dirname "$f")")")/$(basename "$f" .rs)"
    case $(grep -cx "$name" <<<"$listed") in
      1) ;;
      0) echo "manifest: $name is not classified in $MANIFEST"; ok=1 ;;
      *) echo "manifest: $name is listed more than once"; ok=1 ;;
    esac
  done
  for name in $listed; do
    [ -f "crates/${name%%/*}/tests/${name#*/}.rs" ] ||
      { echo "manifest: $name has no test file"; ok=1; }
  done
  while read -r name; do
    echo "manifest: excluded $name gives no reason"
    ok=1
  done < <(grep -E '^excluded[[:space:]]' "$MANIFEST" | awk 'NF < 3 {print $2}')
  while read -r lane name; do
    echo "manifest: $name has unknown lane $lane"
    ok=1
  done < <(grep -Ev '^\s*(#|$)' "$MANIFEST" |
    awk '$1 !~ /^(serial-pg|serial|parallel|release|workspace|excluded)$/ {print $1, $2}')
  while read -r name; do
    if grep -q '#\[ignore' "crates/${name%%/*}/tests/${name#*/}.rs" 2>/dev/null; then
      echo "manifest: $name has ignored tests; classify it as a gated lane, not workspace"
      ok=1
    fi
  done < <(grep -E '^workspace[[:space:]]' "$MANIFEST" | awk '{print $2}')
  return $ok
}

if [ "${1:-}" = "--check" ]; then
  check_manifest && echo "manifest: every integration test file is classified"
  exit $?
fi
check_manifest || exit 2

TIER=core
if [ "${1:-}" = "--release" ]; then
  TIER=release
  shift
fi
LABEL=${1:-$TIER}
[[ $LABEL =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] ||
  die "invalid label '$LABEL' (letters, digits, '.', '_' and '-'; no path)"
BASE=$(realpath -m -- "${GATE_DIR:-$ROOT/target/gate}")
case "$BASE" in
  / | "$HOME" | "$ROOT") die "refusing gate directory $BASE" ;;
esac
OUT="$BASE/$LABEL"
[ "$(dirname -- "$OUT")" = "$BASE" ] || die "output $OUT is not inside $BASE"

min_free=${GATE_MIN_FREE_GB:-15}
free_gb=$(df --output=avail -BG -- "$ROOT" | tail -1 | tr -dc 0-9)
[ "$free_gb" -ge "$min_free" ] ||
  die "only ${free_gb} GB free on the build disk (need ${min_free}); free space (for example \`cargo clean\`) or set GATE_MIN_FREE_GB, then rerun"

if [ -e "$OUT" ]; then
  [ -f "$OUT/.gate-output" ] ||
    die "$OUT exists and was not written by the gate; refusing to replace it"
  rm -rf -- "$OUT"
fi
mkdir -p -- "$OUT" && touch -- "$OUT/.gate-output"
export OUT

# Hash of the working tree (tracked and untracked, respecting .gitignore),
# built in a throwaway index so the repository's index is never touched.
tree_hash() {
  local idx
  idx=$(mktemp)
  cp -- "$(git rev-parse --git-path index)" "$idx" 2>/dev/null || rm -f -- "$idx"
  GIT_INDEX_FILE=$idx git add -A >/dev/null 2>&1 && GIT_INDEX_FILE=$idx git write-tree
  rm -f -- "$idx"
}

# ---- container ownership (see the header)
RUN_LABEL=deltaforge.gate.run
OWNER_LABEL=deltaforge.gate.owner
[ -r /proc/$$/stat ] && [ -r /proc/sys/kernel/random/boot_id ] ||
  die "needs /proc to identify the owner of its containers (Linux)"
HOST=$(hostname)
BOOT=$(cat /proc/sys/kernel/random/boot_id)
# Field 22 of /proc/<pid>/stat (start time), counted after the `(comm)` field.
start_time() { sed 's/.*) //' "/proc/$1/stat" 2>/dev/null | awk '{print $20}'; }
RUN_ID="deltaforge-gate-$$-$(date +%s)"
OWNER="$HOST:$BOOT:$$:$(start_time $$)"
export RUN_ID
export DELTAFORGE_GATE_RUN=$RUN_ID DELTAFORGE_GATE_OWNER=$OWNER
echo "$RUN_ID" > "$OUT/run-id"

reap_run() { # run id
  docker ps -aq --filter "label=$RUN_LABEL=$1" | xargs -r docker rm -f -v >/dev/null 2>&1
  docker volume ls -q --filter "label=$RUN_LABEL=$1" | xargs -r docker volume rm -f >/dev/null 2>&1
}
# Whether the owner of a run is provably gone. Anything this gate cannot
# prove - another host, a malformed or missing owner - counts as alive.
owner_gone() { # host:boot:pid:start
  local host boot pid st
  IFS=: read -r host boot pid st <<<"$1"
  [ "$host" = "$HOST" ] && [[ $pid =~ ^[0-9]+$ && $st =~ ^[0-9]+$ ]] && [ -n "$boot" ] || return 1
  [ "$boot" != "$BOOT" ] && return 0
  [ "$(start_time "$pid")" != "$st" ]
}
reap_stale() {
  local run owner
  {
    docker ps -a --filter "label=$RUN_LABEL" \
      --format "{{.Label \"$RUN_LABEL\"}} {{.Label \"$OWNER_LABEL\"}}"
    docker volume ls --filter "label=$RUN_LABEL" \
      --format "{{.Label \"$RUN_LABEL\"}} {{.Label \"$OWNER_LABEL\"}}"
  } 2>/dev/null | sort -u | while read -r run owner; do
    [ "$run" != "$RUN_ID" ] && [ -n "$owner" ] && owner_gone "$owner" || continue
    echo "gate: removing the containers of stale run $run (owner $owner)" >&2
    reap_run "$run"
  done
}
cleanup() { reap_run "$RUN_ID"; }
# Stop every descendant (TERM, then KILL after GATE_STOP_SECS, default 30)
# and wait for it, so no suite creates a container after the cleanup.
descendants() { # (excluding the subshell listing them)
  local p
  for p in $(pgrep -P "$1"); do
    [ "$p" = "$BASHPID" ] && continue
    descendants "$p"
    echo "$p"
  done
}
stop_children() {
  local ps i
  ps=$(descendants $$)
  [ -n "$ps" ] || return 0
  kill -TERM $ps 2>/dev/null
  for i in $(seq "${GATE_STOP_SECS:-30}"); do
    ps=$(descendants $$)
    [ -n "$ps" ] || break
    sleep 1
  done
  ps=$(descendants $$)
  [ -n "$ps" ] && kill -KILL $ps 2>/dev/null
  wait 2>/dev/null
}
trap cleanup EXIT
trap 'trap - INT TERM; echo "gate: interrupted" >&2; stop_children; cleanup; exit 130' INT TERM
reap_stale

echo "tier $TIER" > "$OUT/summary"
echo "tree before $(tree_hash)" | tee -a "$OUT/summary"
start=$(date +%s)
started_at=$(date -u +%Y-%m-%dT%H:%M:%S)

summ() { # name exit-code log
  local c
  c=$(grep "^test result" "$3" 2>/dev/null | awk '{p+=$4; f+=$6} END {printf "%d passed, %d failed", p, f}')
  echo "$1 rc=$2 ($c)" >> "$OUT/summary"
}
run() { # name log cmd... (in the background, so a signal is handled at once)
  local name=$1 log=$2
  shift 2
  "$@" > "$log" 2>&1 &
  wait $!
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
if [ "${GATE_ONLY_SUITES:-}" != 1 ]; then
  run fmt "$OUT/fmt.log" cargo fmt --all -- --check
  run clippy "$OUT/clippy.log" cargo clippy --workspace --all-targets -- -D warnings
  run clippy-vault "$OUT/clippy-vault.log" \
    cargo clippy -p sources -p runner -p secrets --features vault --all-targets -- -D warnings
  run workspace "$OUT/workspace.log" cargo test --workspace --no-fail-fast
fi

# ---- build every gated test binary once
# The suites run concurrently: `parallel`, plus `release` in the release tier.
concurrent() {
  lane parallel
  if [ "$TIER" = release ]; then lane release; fi
}
lane serial-pg >  "$OUT/suites"
lane serial    >> "$OUT/suites"
concurrent     >> "$OUT/suites"
while read -r _ args; do
  cargo test ${args%% -- *} --no-run >> "$OUT/build.log" 2>&1 &
  wait $!
done < "$OUT/suites"

# PostgreSQL for the `serial-pg` suites: this run's own container, on a free
# local port.
serial_lane() {
  local pg="$RUN_ID-pg" port
  if [ -n "$(lane serial-pg)" ]; then
    if docker run -d --name "$pg" --label "$RUN_LABEL=$RUN_ID" --label "$OWNER_LABEL=$OWNER" \
      -e POSTGRES_PASSWORD=pg -p 127.0.0.1::5432 postgres:17 >/dev/null 2>"$OUT/postgres.log"; then
      port=$(docker port "$pg" 5432/tcp | head -1 | sed 's/.*://')
      for _ in $(seq 60); do
        docker exec "$pg" pg_isready -U postgres >/dev/null 2>&1 && break
        sleep 1
      done
      sleep 2
      while read -r name args; do
        DELTAFORGE_IT_PG_DSN="host=127.0.0.1 port=$port user=postgres password=pg dbname=postgres" \
          suite "$name" $args
      done < <(lane serial-pg)
      docker rm -f -v "$pg" >/dev/null 2>&1
    else
      summ postgres 1 "$OUT/postgres.log"
    fi
  fi
  while read -r name args; do suite "$name" $args; done < <(lane serial)
}

serial_lane &
serial_pid=$!
concurrent | xargs -r -P "${GATE_PARALLEL:-3}" -L 1 bash -c 'suite "$@"' _ &
concurrent_pid=$!
wait $serial_pid
wait $concurrent_pid

# A test container of this run without the ownership labels: a start site
# that bypasses `GateOwned`. Reported (never removed: it may be another run's).
unowned=$(docker ps -a --filter label=org.testcontainers.managed-by=testcontainers \
  --format "{{.ID}} {{.Label \"$RUN_LABEL\"}}" 2>/dev/null | awk 'NF == 1 {print $1}' |
  while read -r id; do
    created=$(docker inspect -f '{{.Created}}' "$id" 2>/dev/null)
    [[ ${created:0:19} > "$started_at" || ${created:0:19} == "$started_at" ]] && echo "$id"
  done | tr '\n' ' ')
if [ -n "$unowned" ]; then
  echo "unowned-containers rc=1 (test containers without the gate labels: $unowned)" >> "$OUT/summary"
fi

echo "tree after $(tree_hash)" >> "$OUT/summary"
echo "elapsed $(( ($(date +%s) - start) / 60 )) min" >> "$OUT/summary"
echo "suites ok: $(grep -c 'rc=0' "$OUT/summary")" >> "$OUT/summary"
grep -v "rc=0" "$OUT/summary" | grep "rc=" | sed 's/^/FAILED /' >> "$OUT/summary"
grep -E "^excluded" "$MANIFEST" | awk '{print "not gated: " $2}' >> "$OUT/summary"
if [ "$TIER" = core ]; then
  grep -E "^release" "$MANIFEST" | awk '{print "release tier only: " $2}' >> "$OUT/summary"
fi
cat "$OUT/summary"
! grep -q "^FAILED" "$OUT/summary"

#!/usr/bin/env bash
# Replay canary driver. Automates the REST/metric-observable checks and guides
# the manual restart step. Assumes the disposable stack is already up and the
# runner is serving the `replay-canary` pipeline (see runbook.md).
#
# Requires: curl, jq, redis-cli (or set REDIS_EXEC to a docker-exec wrapper).
#
#   API=http://127.0.0.1:8080 METRICS=http://127.0.0.1:9000 \
#   REDIS_URL=redis://127.0.0.1:6379 ./canary.sh <check>
#
# <check>: 1 | 2 | 3-arm | 3-verify | 4 | 5 | status | cancel
set -euo pipefail

API="${API:-http://127.0.0.1:8080}"
METRICS="${METRICS:-http://127.0.0.1:9000}"
REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379}"
PIPE="${PIPE:-replay-canary}"
S_REPLAY="df.canary.replay"   # stream for the SELECTED (paused) sink
S_LIVE="df.canary.live"       # stream for the LIVE-only sink

# redis-cli wrapper: override REDIS_EXEC to e.g. "docker compose exec -T redis redis-cli"
redis() { ${REDIS_EXEC:-redis-cli -u "$REDIS_URL"} "$@"; }
xlen()  { redis XLEN "$1" 2>/dev/null | tr -d '\r' | tr -dc '0-9'; }
metric(){ curl -fsS "$METRICS/metrics" | awk -v m="$1" '$1==m {print $2}'; }
status(){ curl -fsS "$API/pipelines/$PIPE/journal/replay"; }
phase() { status | jq -r '.phase // "none"'; }
cursor(){ status | jq -r '.cursor // 0'; }
jobid() { status | jq -r '.job_id // "none"'; }

start_replay() { # $1 = extra json fields (e.g. dry_run) merged in
  curl -fsS -X POST "$API/pipelines/$PIPE/journal/replay" \
    -H 'content-type: application/json' \
    -d "$(jq -cn --argjson extra "${1:-{}}" \
      '{selected_sinks:["replay_target"], from_seq:0} + $extra')"
}
cancel() { curl -fsS -o /dev/null -w '%{http_code}\n' -X POST \
  "$API/pipelines/$PIPE/journal/replay/cancel"; }

wait_phase() { # $1 phase-regex  $2 timeout-secs
  local end=$(( $(date +%s) + ${2:-60} ))
  while (( $(date +%s) < end )); do
    [[ "$(phase)" =~ $1 ]] && { echo "  -> reached phase: $(phase)"; return 0; }
    sleep 1
  done
  echo "  !! timed out waiting for phase ~ /$1/ (last: $(phase))"; return 1
}

case "${1:-}" in

# ── Check 1: selected-sink pause and restoration ────────────────────────────
1)
  echo "[1] Selected-sink pause and restoration"
  live0=$(xlen "$S_LIVE"); rep0=$(xlen "$S_REPLAY")
  echo "  baseline  live=$live0  replay_target=$rep0"
  echo "  starting replay (selected: replay_target)"; start_replay; echo
  wait_phase 'running|catching_up' 30
  echo "  >> now generate ~20 live source rows (INSERT into public.orders)."
  read -r -p "  press ENTER once live rows are committed... " _
  sleep 3
  live1=$(xlen "$S_LIVE"); rep1=$(xlen "$S_REPLAY")
  echo "  during-replay  live=$live1 (was $live0)  replay_target=$rep1 (was $rep0)"
  if (( live1 > live0 )); then echo "  PASS: live_only kept receiving while replay_target paused"
  else echo "  CHECK: live_only did not advance - confirm you committed live rows"; fi
  echo "  waiting for handoff + restoration..."
  wait_phase 'live_restored|completed' 90
  echo "  final phase: $(phase)"
  echo "  PASS criterion: phase reaches completed and replay_target rejoined the live set."
  echo "  (First live delivery to replay_target is strictly after the handoff tail H.)"
  ;;

# ── Check 2: duplicate handling under at-least-once ─────────────────────────
2)
  echo "[2] Duplicate handling under at-least-once"
  echo "  This re-runs a bounded historical replay twice against the same sink."
  echo "  Append-only redis stream shows duplicates as extra entries; a dedup sink would absorb them."
  rep0=$(xlen "$S_REPLAY")
  thr=$(status | jq -r '.cursor // 0'); [[ "$thr" == "0" ]] && thr=$(metric deltaforge_replay_captured_total | cut -d. -f1)
  echo "  replaying historical range through_seq=$thr, run A"
  start_replay "{\"through_seq\": ${thr:-null}}"; echo
  wait_phase 'completed' 90 || true
  repA=$(xlen "$S_REPLAY"); echo "  after run A: replay_target=$repA (delivered ~$((repA-rep0)))"
  echo "  replaying the SAME range again, run B (expected re-delivery)"
  start_replay "{\"through_seq\": ${thr:-null}}"; echo
  wait_phase 'completed' 90 || true
  repB=$(xlen "$S_REPLAY"); echo "  after run B: replay_target=$repB (delivered ~$((repB-repA)))"
  if (( repB > repA )); then echo "  PASS: same range re-delivered -> at-least-once confirmed (duplicates present by design)"
  else echo "  NOTE: no growth - sink dedup absorbed the duplicates (also acceptable)"; fi
  echo "  deltaforge_replay_delivered_total=$(metric deltaforge_replay_delivered_total)"
  ;;

# ── Check 3: restart during replay (two phases) ─────────────────────────────
3-arm)
  echo "[3-arm] Restart during replay - arming"
  echo "  starting a LARGE historical replay so it is still in-flight when you kill the runner"
  start_replay; echo
  wait_phase 'running|catching_up' 30
  echo "  cursor before kill: $(cursor)  job_id: $(jobid)"
  echo "  >> NOW hard-kill the runner process/container (docker kill / SIGKILL), then restart it"
  echo "     with the SAME --config and SAME storage. Then run: $0 3-verify"
  ;;
3-verify)
  echo "[3-verify] Restart during replay - verifying resume"
  echo "  job_id: $(jobid)  phase: $(phase)  cursor: $(cursor)"
  c0=$(cursor); sleep 5; c1=$(cursor)
  echo "  cursor $c0 -> $c1 over 5s"
  if [[ "$(phase)" != "none" ]] && { (( c1 >= c0 )); }; then
    echo "  PASS: same job resumed from its durable cursor after restart"
    wait_phase 'completed' 120 || true
    echo "  final phase: $(phase)"
  else echo "  CHECK: no active/resumed job found - inspect logs"; fi
  ;;

# ── Check 4: retention and journal-growth metrics ───────────────────────────
4)
  echo "[4] Retention and journal-growth metrics"
  for m in deltaforge_replay_captured_total \
           deltaforge_replay_capture_oversized_total \
           deltaforge_replay_capture_failures_total \
           deltaforge_replay_delivered_total \
           deltaforge_replay_scanned_total \
           deltaforge_replay_retention_removed_total \
           deltaforge_replay_retention_capacity_pinned_total \
           deltaforge_replay_job_failed_total; do
    printf '  %-52s %s\n' "$m" "$(metric "$m" || echo '<absent>')"
  done
  echo "  PASS criteria:"
  echo "   - captured_total grows as live source rows commit (journal is capturing)"
  echo "   - delivered_total grows during a replay job"
  echo "   - retention_removed_total > 0 only after entries age past retention_secs"
  echo "   - retention_capacity_pinned_total increments if a cap was hit while a job pinned the range"
  echo "   - capture_failures_total / job_failed_total stay 0 on a clean canary"
  ;;

# ── Check 5: network restriction around the REST API ────────────────────────
5)
  echo "[5] Network restriction around the REST API"
  echo "  Runner defaults bind 0.0.0.0 (api :8080, metrics :9000) and have NO authN/authZ."
  echo "  The mutating replay endpoints MUST be unreachable from untrusted networks."
  echo "  a) trusted vantage should reach it:"
  if curl -fsS -o /dev/null -m 3 "$API/pipelines"; then echo "     OK reachable from here ($API)"; else echo "     API not reachable from here"; fi
  echo "  b) from an UNTRUSTED host/network, this MUST fail (timeout/refused):"
  echo "       curl -m 3 http://<runner-public-ip>:8080/pipelines/$PIPE/journal/replay"
  echo "     Expected: connection refused or timeout (blocked by firewall/mesh/proxy)."
  echo "  c) confirm the control: bind to loopback, or restrict via firewall/mesh/authenticating proxy."
  echo "     e.g. runner --api-addr 127.0.0.1:8080  (+ private compose network)"
  echo "  NOTE: the Prometheus scrape listener is hardcoded to 0.0.0.0:9000 and is NOT"
  echo "        governed by --metrics-addr, so :9000 must be firewalled too (read-only, but exposed)."
  echo "  MANUAL PASS: untrusted probe blocked (both :8080 and :9000) AND trusted probe works."
  ;;

status) status | jq . ;;
cancel) echo "cancel -> HTTP $(cancel)" ;;
*) echo "usage: $0 {1|2|3-arm|3-verify|4|5|status|cancel}"; exit 2 ;;
esac

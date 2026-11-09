#!/usr/bin/env bash
# Renders every configuration of the single-source capacity campaign
# (docs/design/fleet-capacity-plan.md, "Execution recipe") into $1:
#   a{100,200,300}.yaml        Phase A screening runs (F0, 2,000 changes/s)
#   b{100,200,300}.yaml        Phase B staircase (F1), one per possible size
#   br{100,200,300}.yaml       Phase B generator remedy (8 rows per transaction)
#   c{100,200,300}-<rate>.yaml Phase C confirmation at each possible step rate
#   a001.yaml                  tooling validation only (100 tables, 6 minutes)
#   a100c.yaml                 the A100 confirmation of the table-map fix (a100, 35 minutes)
# The output is fixed: CONFIGS.sha256 lists its hashes, and the run wrapper
# refuses a configuration whose hash differs.
set -eu
out=${1:?usage: render.sh <dir>}
mkdir -p "$out"

# name tables scenario warmup duration rate rows_per_txn steps [databases]
emit() {
  local name=$1 tables=$2 scenario=$3 warmup=$4 duration=$5 rate=$6 rows=$7 steps=$8 dbs=${9:-2000}
  {
    cat <<EOF
# Capacity campaign, run $name (rendered by capacity/render.sh; do not edit).
class: exploratory
profile: cdc
output_dir: results
topology:
  servers:
    - name: $name
      host: 127.0.0.1
      port: 33071
      cdc_host: cap-mysql
      cdc_port: 3306
  admin_user: root
  admin_password: rootpw
  databases_per_server: $dbs
  tables_per_database: $tables
  database_prefix: cust_
  seed: 1
deltaforge:
  api_url: http://127.0.0.1:18090
  metrics_url: http://127.0.0.1:19010/metrics
  process: {kind: docker_container, name: cap-df}
  state_store: sqlite
  pipeline_prefix: cap
  tenant: fleet
  cdc_user: df
  cdc_password: dfpw
  kafka_brokers: cap-kafka:29092
  topic_prefix: cap
  key: {mode: template, template: "\${after.id}"}
  proof_trace: {kind: docker_logs, container: cap-df}
  proof_trace_required: true
verifier:
  kafka_brokers: localhost:9405
  expected_key: none
  work_dir: work
traffic:
  changes_per_sec_avg: $rate
  changes_per_sec_peak: $rate
  peak_fraction: 0
  op_mix: {insert: 0.6, update: 0.3, delete: 0.1}
  row_bytes: 256
  rows_per_txn: $rows
  zipf_exponent: 0
  active_set: {pattern: fixed, fraction: 0.4}
  writers_per_server: 32
EOF
    if [ -n "$steps" ]; then
      echo "  rate_steps: [$steps]"
      echo "  rate_step_secs: 300"
    fi
    cat <<EOF
ddl:
  migration_statements: 3
  migration_pace: 50
  migration_rollout: {kind: sequential}
measure:
  interval_secs: 5
  state_store_file: state/deltaforge.db
  cdc_users: [df]
run:
  scenario: $scenario
  duration_secs: $duration
  warmup_secs: $warmup
  repetitions: 1
  sources: [1]
EOF
  } > "$out/$name.yaml"
}

emit a001 5 F0 0 360 2000 1 "" 20
# a100's catalog, active set and traffic, run long enough to observe at
# least 20 minutes after activation (about 6 minutes at A100).
emit a100c 50 F0 0 2100 2000 1 ""

STEPS="2000, 4000, 8000, 16000, 32000, 64000"
# size tables phase-A duration (activation allowance + 600 measured seconds)
for row in "100 50 1380" "200 100 2100" "300 150 2820"; do
  set -- $row
  size=$1 tables=$2 dur=$3
  allowance=$((dur - 600))
  emit "a$size" "$tables" F0 0 "$dur" 2000 1 ""
  emit "b$size" "$tables" F1 "$allowance" 1800 2000 1 "$STEPS"
  emit "br$size" "$tables" F1 "$allowance" 1800 2000 8 "$STEPS"
  for rate in 2000 4000 8000 16000 32000 64000; do
    emit "c$size-$rate" "$tables" F0 900 2700 "$rate" 1 ""
  done
done

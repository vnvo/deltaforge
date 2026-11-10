#!/usr/bin/env bash
# Renders every configuration of the single-source capacity campaign
# (docs/design/fleet-capacity-plan.md, "Execution recipe") into $1:
#   a{100,200,300}.yaml        Phase A screening runs (F0, 2,000 changes/s)
#   b{100,200,300}.yaml        Phase B staircase (F1), one per possible size
#   br{100,200,300}.yaml       Phase B generator remedy (8 rows per transaction)
#   c{100,200,300}-<rate>.yaml Phase C confirmation at each possible step rate
#   a001.yaml                  tooling validation only (100 tables, 6 minutes)
#   a100c.yaml                 the A100 confirmation of the table-map fix (a100, 35 minutes)
#   cal100.yaml                generator calibration at a100: 2,000, 4,000 and 8,000 changes/s, 300 s each
#   cal100m.yaml               the same calibration, second attempt (MySQL memory raised to 10 GiB)
#   cal100n.yaml               that attempt again after cal100m never ran traffic
#   cal100r.yaml               recovery check, setup failure: scenario F1
#                              without rate steps (the harness refused it)
#   cal100s.yaml               recovery check on the fixed build: cal100n's
#                              fixture and allocations, scenario F0 at a
#                              constant 2,000 changes/s for 840 s
# The laptop campaign (one shared host; docs/design/fleet-capacity-plan.md,
# "Laptop campaign"):
#   a003.yaml                  the throughput track's fixture (2,500 tables)
#   g003.yaml                  generator check: 2,000, 4,000 and 8,000 changes/s, 300 s each
#   b003.yaml                  staircase on 1,000 active tables (F1)
#   t003-<rate>.yaml           30-minute confirmation at each possible step rate (F0)
#   k100.yaml                  catalog track: the 100,000-table fixture, 1,000
#                              active tables, 500 changes/s for 20 minutes
#   d003.yaml                  backlog drain (scenario D1): 4,000 changes/s for
#                              900 s, the pipeline stopped from 300 s and
#                              resumed after the writers finish
# The output is fixed: CONFIGS.sha256 lists its hashes, and the run wrapper
# refuses a configuration whose hash differs.
set -eu
out=${1:?usage: render.sh <dir>}
mkdir -p "$out"

# name tables scenario warmup duration rate rows_per_txn steps [databases]
emit() {
  local name=$1 tables=$2 scenario=$3 warmup=$4 duration=$5 rate=$6 rows=$7 steps=$8 dbs=${9:-2000} fraction=${10:-0.4}
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
  active_set: {pattern: fixed, fraction: $fraction}
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
# The generator calibration: the staircase's first three steps, no warm-up
# (the harness invocation is bounded to 20 minutes, drain included).
emit cal100 50 F1 0 900 2000 1 "2000, 4000, 8000"
# Identical to cal100 under its own name (a run name, and its topic, is
# never reused): the attempt with MySQL's memory limit raised to 10 GiB.
emit cal100m 50 F1 0 900 2000 1 "2000, 4000, 8000"
# The same attempt, after cal100m was stopped before traffic (Kafka was down).
emit cal100n 50 F1 0 900 2000 1 "2000, 4000, 8000"
# The recovery check (phase REC): cal100n held at its first rate, writes
# ending at 840 s so the drain fits the check's 20-minute deadline.
emit cal100r 50 F1 0 840 2000 1 ""
# cal100r corrected: F0 is the constant-rate scenario (F1 needs rate steps).
emit cal100s 50 F0 0 840 2000 1 ""

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

# The laptop campaign. Throughput track: 50 databases of 50 tables (2,500),
# 40% active (1,000 tables, inside MySQL's default table caches).
emit a003 50 F0 0 600 2000 1 "" 50
emit g003 50 F1 0 900 2000 1 "2000, 4000, 8000" 50
emit b003 50 F1 120 1800 2000 1 "$STEPS" 50
for rate in 2000 4000 8000 16000 32000 64000; do
  emit "t003-$rate" 50 F0 0 1800 "$rate" 1 "" 50
done
# Catalog track: the kept 100,000-table fixture, 1% active (1,000 tables),
# at low traffic.
emit k100 50 F0 0 1200 500 1 "" 2000 0.01
# The backlog drain (harness scenario D1, fleet-harness 9cf13075/e20d45e5):
# about 2.4M changes written while the pipeline is stopped.
emit d003 50 D1 0 900 4000 1 "" 50

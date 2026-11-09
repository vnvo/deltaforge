#!/usr/bin/env bash
# Driver of the single-source capacity campaign (docs/design/
# fleet-capacity-plan.md, "Execution recipe"). Every step refuses to start
# when an identity, a floor or a deadline would not hold.
#
#   campaign.sh setup <campaign dir> <harness binary>   once: pins, renders, network, Kafka, T0
#   campaign.sh mysql <size>                            fresh MySQL for a size (100, 200, 300)
#   campaign.sh fixture <size>                          build that size's fixture
#   campaign.sh drop <size>                             remove that size's MySQL container and volume
#   campaign.sh run <A|B|BR|C> <config name>            one run, evaluated (exit: frontier.py's)
#
# Exit 3: refused (disk start floor, identity, freshness); 6: deadline or cap.
set -u
KIT=$(cd "$(dirname "$0")" && pwd)
C=${CAMPAIGN_DIR:-$HOME/deltaforge-evidence/capacity-rc1}
IMAGE=ghcr.io/vnvo/deltaforge@sha256:63823feac0c30805072b1b3b4a87544d64b77af22a2a6bbf43ece5e7f919b0d9
MYSQL=mysql@sha256:c36050afdca850f23cef85703f84c7531a5ae155a11b5ee1c60acb09937c4084
KAFKA=confluentinc/cp-kafka@sha256:fbbb6fa11b258a88b83f54d4f0bddfcffbf2279f99d66a843486e3da7bdfbf41
NEED=53687091200          # 50 GiB, byte-exact, at the start of every invocation
MEMFLOOR=4294967296       # 4 GiB available memory
TOTAL_CAP=28800           # 8 hours from T0
REPORT=900                # the last 15 minutes are the report's
declare -A BUDGET=([F]=2700 [A]=10800 [B]=9000 [BR]=9000 [C]=5400)
declare -A ACTIVE=([100]=40000 [200]=80000 [300]=120000)
declare -A ALLOW=([100]=780 [200]=1500 [300]=2220)      # Phase B warm-up (Phase A activation allowance)
declare -A ADEAD=([100]=2880 [200]=3600 [300]=4320)     # Phase A: duration + 600 drain + 900

log() { echo "$(date -Is) $*" | tee -a "$C/campaign.log"; }
die() { log "REFUSED: $1"; exit "${2:-3}"; }
avail() { df -B1 --output=avail "$C" "$(docker info --format '{{.DockerRootDir}}')" | tail -n +2 | sort -n | head -1; }

identity() {
  (cd "$C" && sha256sum -c --quiet IDENTITY.sha256) || die "identity: a pinned file changed"
  [ "$(docker image inspect --format '{{index .RepoDigests 0}}' "$IMAGE" 2>/dev/null)" = "$IMAGE" ] \
    || die "identity: $IMAGE is not the local image"
}

# Time used by a phase (BR shares B's budget) and since T0.
budget() {
  local phase=$1 deadline=$2 key=$1 used t0 now
  [ "$phase" = BR ] && key=B
  t0=$(cat "$C/T0"); now=$(date +%s)
  used=$(awk -v k="$key" '$2 == "END" && ($3 == k || (k == "B" && $3 == "BR")) {s += $5} END {print s + 0}' "$C/clock.log")
  [ $((used + deadline)) -le "${BUDGET[$phase]}" ] \
    || die "phase $key: ${used}s used + deadline ${deadline}s > ${BUDGET[$phase]}s" 6
  [ $((now - t0 + deadline)) -le $((TOTAL_CAP - REPORT)) ] \
    || die "total cap: $((now - t0))s elapsed + deadline ${deadline}s > $((TOTAL_CAP - REPORT))s" 6
}

preflight() {
  pgrep -x cargo >/dev/null && die "a cargo process is running"
  local others
  others=$(docker ps --format '{{.Names}}' | grep -v -E '^(cap-mysql|cap-kafka|k3d-.*|buildx_buildkit_.*)$')
  [ -z "$others" ] || die "other containers running: $others"
  local a; a=$(avail)
  [ "$a" -ge "$NEED" ] || die "disk: $a bytes free < $NEED"
  local m; m=$(awk '/^MemAvailable:/{print $2 * 1024}' /proc/meminfo)
  [ "$m" -ge "$MEMFLOOR" ] || die "memory: $m bytes available < $MEMFLOOR"
  log "preflight ok: free=$a mem_available=$m"
}

finish() {
  local phase=$1 name=$2 s=$3 rc=$4
  echo "$(date +%s) END $phase $name $(( $(date +%s) - s )) rc=$rc" >> "$C/clock.log"
}

case "${1:-}" in
setup)
  C=$2 H=$3
  [ -e "$C" ] && { echo "exists: $C"; exit 3; }
  mkdir -p "$C"/{bin,kit,configs}
  cp -a "$H" "$C/bin/fleet-harness"
  cp -a "$KIT"/{campaign.sh,frontier.py,watch.sh,render.sh,CONFIGS.sha256} "$C/kit/"
  cp -a "$KIT/../qualification/floor-watch.sh" "$C/kit/"
  "$KIT/render.sh" "$C/configs"
  (cd "$C/configs" && sha256sum -c --quiet ../kit/CONFIGS.sha256) || { echo "rendered configs differ from CONFIGS.sha256"; exit 3; }
  docker pull "$IMAGE" >/dev/null && docker pull "$MYSQL" >/dev/null && docker pull "$KAFKA" >/dev/null
  (cd "$C" && sha256sum bin/fleet-harness kit/* configs/* > IDENTITY.sha256)
  {
    echo "product $IMAGE platform $(docker image inspect --format '{{.Os}}/{{.Architecture}} {{.Id}}' "$IMAGE")"
    echo "harness $(sha256sum "$C/bin/fleet-harness" | cut -d' ' -f1) from $(git -C "$KIT" rev-parse HEAD)"
    echo "mysql $MYSQL"; echo "kafka $KAFKA"
  } > "$C/identity.txt"
  docker network create cap >/dev/null
  docker run -d --name cap-kafka --network cap --cpus 2 --memory 3g -p 127.0.0.1:9405:9405 \
    -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller -e CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qg \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:29093 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:29093,EXTERNAL://0.0.0.0:9405 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://cap-kafka:29092,EXTERNAL://localhost:9405 \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    -v cap-kafka:/var/lib/kafka/data "$KAFKA" > "$C/cap-kafka.id"
  date +%s > "$C/T0"; : > "$C/clock.log"
  echo "setup $(date -Is) T0=$(cat "$C/T0")" >> "$C/campaign.log"
  ;;
mysql)
  size=$2; identity
  docker ps -a --format '{{.Names}}' | grep -qx cap-mysql && die "cap-mysql exists (remove the previous size first)"
  docker volume inspect "cap-mysql-$size" >/dev/null 2>&1 && die "volume cap-mysql-$size exists"
  # A larger size is built only if, at A100's measured bytes per table, its
  # runs could still start with 50 GiB free.
  if [ "$size" != 100 ]; then
    bpt=$(grep -o 'fixture a100 rc=0 .*bytes_per_table=[0-9]*' "$C/campaign.log" | tail -1 | sed 's/.*=//')
    [ -n "$bpt" ] || die "fit: no measured A100 bytes per table"
    projected=$(( $(avail) - bpt * ${ACTIVE[$size]} * 5 / 2 ))
    [ "$projected" -ge "$NEED" ] || die "fit: a$size leaves $projected bytes free < $NEED (host limit; not built)"
    log "fit a$size: projected free after the fixture $projected bytes"
  fi
  docker run -d --name cap-mysql --network cap --network-alias cap-mysql --cpus 3 --memory 6g \
    -p 127.0.0.1:33071:3306 -e MYSQL_ROOT_PASSWORD=rootpw -v "cap-mysql-$size:/var/lib/mysql" "$MYSQL" \
    --server-id=101 --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL \
    --binlog-row-metadata=FULL --gtid-mode=ON --enforce-gtid-consistency=ON --log-replica-updates=ON \
    --binlog-expire-logs-seconds=21600 --max-binlog-size=268435456 --innodb-buffer-pool-size=2G \
    --innodb-file-per-table=ON > "$C/cap-mysql-$size.id"
  for _ in $(seq 120); do docker exec cap-mysql mysqladmin -uroot -prootpw ping >/dev/null 2>&1 && break; sleep 2; done
  sleep 10   # the entrypoint restarts mysqld once after initialising
  for _ in $(seq 60); do docker exec cap-mysql mysql -uroot -prootpw -e 'select 1' >/dev/null 2>&1 && break; sleep 2; done
  docker exec cap-mysql mysql -uroot -prootpw -e "CREATE USER 'df'@'%' IDENTIFIED BY 'dfpw';
    GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'df'@'%'; GRANT SELECT, SHOW VIEW ON \`cust_%\`.* TO 'df'@'%';" \
    || die "mysql: df user not created"
  log "mysql $size up: container $(cat "$C/cap-mysql-$size.id") volume cap-mysql-$size"
  ;;
fixture)
  size=$2; identity
  used=$(awk '$2 == "END" && $3 == "F" {s += $5} END {print s + 0}' "$C/clock.log")
  deadline=$(( ${BUDGET[F]} - used ))
  [ "$deadline" -gt 0 ] || die "fixture budget spent" 6
  budget F "$deadline"; preflight
  before=$(avail); s=$(date +%s)
  echo "$s START F a$size $deadline" >> "$C/clock.log"
  (cd "$C" && DEADLINE_SECS=$deadline START_GIB=51 FLOOR_GIB=26 kit/floor-watch.sh -- \
     bin/fleet-harness fixture "configs/a$size.yaml" --concurrency 16) > "$C/fixture-a$size.log" 2>&1
  rc=$?; finish F "a$size" "$s" "$rc"
  log "fixture a$size rc=$rc free_before=$before free_after=$(avail) bytes_per_table=$(( (before - $(avail)) / (${ACTIVE[$size]} * 5 / 2) ))"
  exit $rc
  ;;
drop)
  size=$2
  id=$(cat "$C/cap-mysql-$size.id") || die "no container id recorded for size $size"
  [ "$(docker inspect --format '{{.Id}} {{range .Mounts}}{{.Name}}{{end}}' cap-mysql)" = "$id cap-mysql-$size" ] \
    || die "cap-mysql is not the size-$size container with volume cap-mysql-$size"
  before=$(avail)
  docker rm -f "$id" >/dev/null && docker volume rm "cap-mysql-$size" >/dev/null || die "drop failed"
  log "dropped size $size: container $id volume cap-mysql-$size free $before -> $(avail)"
  ;;
run)
  phase=$2 name=$3
  size=$(echo "$name" | sed -E 's/^[a-z]+([0-9]{3}).*/\1/')
  case $phase in
    A)    deadline=${ADEAD[$size]}; drain=600 ;;
    B|BR) deadline=$(( ${ALLOW[$size]} + 1800 + 1800 + 900 )); drain=1800 ;;
    C)    deadline=5400; drain=900 ;;
    *)    die "phase $phase" ;;
  esac
  identity; budget "$phase" "$deadline"; preflight
  inv=$C/runs/$name
  [ -e "$inv" ] && die "$inv exists (a run is never repeated under the same name)"
  docker ps -a --format '{{.Names}}' | grep -qx cap-df && die "cap-df exists"
  mkdir -p "$inv/state"
  docker run -d --name cap-df --network cap --cpus 4 --memory 6g \
    -e RUST_LOG=warn,deltaforge::proof_trace=info -v "$inv/state:/data" \
    -p 127.0.0.1:18090:8080 -p 127.0.0.1:19010:9000 \
    "$IMAGE" --storage-backend sqlite --storage-path /data/deltaforge.db > "$inv/cap-df.id"
  for _ in $(seq 60); do curl -sf -m 2 http://127.0.0.1:18090/pipelines >/dev/null && break; sleep 1; done
  started=$(docker inspect --format '{{.State.StartedAt}}' cap-df)
  log "run $phase $name: deadline=${deadline}s drain=${drain}s cap-df $(cat "$inv/cap-df.id") started $started"
  s=$(date +%s); echo "$s START $phase $name $deadline" >> "$C/clock.log"
  cd "$inv" || exit 1
  DEADLINE_SECS=$deadline START_GIB=51 FLOOR_GIB=26 "$C/kit/floor-watch.sh" -- \
    "$C/bin/fleet-harness" run "$C/configs/$name.yaml" --drain-idle-secs 60 --drain-max-secs "$drain" \
    > run.log 2>&1 &
  fw=$!
  for _ in $(seq 50); do hp=$(pgrep -P "$fw" -f fleet-harness) && break; sleep 0.2; done
  "$C/kit/watch.sh" "$inv" "$hp" &
  wp=$!
  case $phase in B|BR) python3 "$C/kit/frontier.py" B "$inv" "${ACTIVE[$size]}" --live "$hp" > live.log 2>&1 & ;; esac
  wait "$fw"; rc=$?
  echo "$rc" > floor-watch.rc
  wait
  finish "$phase" "$name" "$s" "$rc"
  inc=$(curl -sf -m 10 http://127.0.0.1:19010/metrics | awk '/^deltaforge_incidents_raised_total[{ ]/{s += $NF} END {print s + 0}') || inc=unreadable
  python3 - "$inc" "$started" > end-state.json <<PY
import json, subprocess, sys
inc, started = sys.argv[1], sys.argv[2]
i = json.loads(subprocess.check_output(["docker", "inspect", "cap-df"]))[0]
print(json.dumps({"incidents": float(inc) if inc != "unreadable" else None,
                  "restarted": i["RestartCount"] != 0 or i["State"]["StartedAt"] != started or not i["State"]["Running"],
                  "oom_killed": i["State"]["OOMKilled"]}))
PY
  docker logs -t cap-df > df.log 2>&1
  for c in cap-df cap-mysql cap-kafka; do docker inspect "$c" > "inspect-$c.json"; done
  docker stop cap-df >/dev/null && docker rename cap-df "cap-df-$name"
  # The verified run's topic: its data is not evidence and holds disk.
  docker exec cap-kafka kafka-topics --bootstrap-server localhost:29092 --delete --topic "cap.$name" >> run.log 2>&1
  python3 "$C/kit/frontier.py" "${phase/BR/B}" "$inv" "${ACTIVE[$size]}" | tee -a "$C/campaign.log"
  vrc=${PIPESTATUS[0]}
  find . -type f ! -name MANIFEST.sha256 ! -path './state/*' -print0 | sort -z | xargs -0 sha256sum > MANIFEST.sha256
  [ "$rc" = 3 ] && exit 3
  exit "$vrc"
  ;;
*)
  sed -n '2,12p' "$0"; exit 2 ;;
esac

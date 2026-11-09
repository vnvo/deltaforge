#!/usr/bin/env bash
# Driver of the single-source capacity campaign (docs/design/
# fleet-capacity-plan.md, "Execution recipe"). Every step refuses to start
# when an identity, a floor or a deadline would not hold.
#
#   campaign.sh setup <campaign dir> <harness binary>   once: pins, renders, network, Kafka, T0
#   campaign.sh mysql <size>                            fresh MySQL for a size (100, 200, 300)
#   campaign.sh fixture <size>                          build that size's fixture
#   campaign.sh drop <size>                             remove that size's MySQL container and volume
#   campaign.sh adopt <size> <container id>             start a kept, stopped fixture container (full id)
#   campaign.sh tune R1                                 MySQL table caches sized to the active set (50,000)
#   campaign.sh run <A|B|BR|C|V> <config name>          one run, evaluated (exit: frontier.py's)
#
# Size 001 (100 tables) and phase V are the tooling validation only: a short
# Phase A run, with the live coverage reader, in its own campaign directory.
# Phase CAL is the generator calibration (`cal100`): one invocation of at
# most 20 minutes, R1 applied before it and R2 (MySQL 5 CPUs) applied live
# only if a step is MySQL-limited (frontier.py).
#
# Exit 3: refused (disk start floor, identity, freshness); 6: deadline or cap.
set -u
KIT=$(cd "$(dirname "$0")" && pwd)
C=${CAMPAIGN_DIR:-$HOME/deltaforge-evidence/capacity-rc1}
# The product image: the published RC by digest, unless setup was given a
# local build (CAP_IMAGE), which is then pinned by its image id. Every later
# step uses what setup recorded.
IMAGE=ghcr.io/vnvo/deltaforge@sha256:63823feac0c30805072b1b3b4a87544d64b77af22a2a6bbf43ece5e7f919b0d9
[ -f "$C/image.ref" ] && IMAGE=$(cat "$C/image.ref")
MYSQL=mysql@sha256:c36050afdca850f23cef85703f84c7531a5ae155a11b5ee1c60acb09937c4084
KAFKA=confluentinc/cp-kafka@sha256:fbbb6fa11b258a88b83f54d4f0bddfcffbf2279f99d66a843486e3da7bdfbf41
NEED_FIXTURE=53687091200  # 50 GiB, byte-exact, before each fixture build
NEED_RUN=42949672960      # 40 GiB, byte-exact, before each measured run
PRIOR_BPT=146597          # bytes per empty table, measured at A100 (2026-11-09), until a campaign measures its own
MEMFLOOR=4294967296       # 4 GiB available memory
TOTAL_CAP=28800           # 8 hours from T0
REPORT=900                # the last 15 minutes are the report's
declare -A BUDGET=([F]=2700 [A]=10800 [B]=9000 [BR]=9000 [C]=5400 [V]=600 [CAL]=2400)  # CAL: two 20-minute invocations (cal100, cal100m)
declare -A ACTIVE=([001]=40 [100]=40000 [200]=80000 [300]=120000)
declare -A TABLES=([001]=100 [100]=100000 [200]=200000 [300]=300000)
declare -A ALLOW=([100]=780 [200]=1500 [300]=2220)      # Phase B warm-up (Phase A activation allowance)

log() { echo "$(date -Is) $*" | tee -a "$C/campaign.log"; }
die() { log "REFUSED: $1"; exit "${2:-3}"; }
avail() { df -B1 --output=avail "$C" "$(docker info --format '{{.DockerRootDir}}')" | tail -n +2 | sort -n | head -1; }

identity() {
  (cd "$C" && sha256sum -c --quiet IDENTITY.sha256) || die "identity: a pinned file changed"
  [ "$(docker image inspect --format '{{.Id}}' "$IMAGE" 2>/dev/null)" = "$(cat "$C/image.id")" ] \
    || die "identity: $IMAGE is not the pinned image $(cat "$C/image.id")"
  case $IMAGE in
    *@sha256:*)
      [ "$(docker image inspect --format '{{index .RepoDigests 0}}' "$IMAGE" 2>/dev/null)" = "$IMAGE" ] \
        || die "identity: $IMAGE is not the local image" ;;
  esac
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
  local need=$1
  pgrep -x cargo >/dev/null && die "a cargo process is running"
  local others
  others=$(docker ps --format '{{.Names}}' | grep -v -E '^(cap-mysql|cap-kafka|k3d-.*|buildx_buildkit_.*)$')
  [ -z "$others" ] || die "other containers running: $others"
  local a; a=$(avail)
  [ "$a" -ge "$need" ] || die "disk: $a bytes free < $need"
  local m; m=$(awk '/^MemAvailable:/{print $2 * 1024}' /proc/meminfo)
  [ "$m" -ge "$MEMFLOOR" ] || die "memory: $m bytes available < $MEMFLOOR"
  log "preflight ok: free=$a (start floor $need) mem_available=$m"
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
  IMAGE=${CAP_IMAGE:-$IMAGE}
  case $IMAGE in
    *@sha256:*) docker pull "$IMAGE" >/dev/null || { echo "pull failed: $IMAGE"; exit 3; } ;;
    *) docker image inspect "$IMAGE" >/dev/null 2>&1 || { echo "no local image $IMAGE"; exit 3; } ;;
  esac
  docker pull "$MYSQL" >/dev/null && docker pull "$KAFKA" >/dev/null
  echo "$IMAGE" > "$C/image.ref"
  docker image inspect --format '{{.Id}}' "$IMAGE" > "$C/image.id"
  # Each campaign's Kafka data in its own volume (an earlier campaign's,
  # with any topic it kept, is never reused).
  KVOL=cap-kafka-$(basename "$C")
  docker volume inspect "$KVOL" >/dev/null 2>&1 && { echo "volume $KVOL exists"; exit 3; }
  (cd "$C" && sha256sum bin/fleet-harness image.ref image.id kit/* configs/* > IDENTITY.sha256)
  {
    echo "product $IMAGE platform $(docker image inspect --format '{{.Os}}/{{.Architecture}} {{.Id}}' "$IMAGE")"
    echo "harness $(sha256sum "$C/bin/fleet-harness" | cut -d' ' -f1) from $(git -C "$KIT" rev-parse HEAD)"
    echo "mysql $MYSQL"; echo "kafka $KAFKA volume $KVOL"
  } > "$C/identity.txt"
  docker network inspect cap >/dev/null 2>&1 || docker network create cap >/dev/null \
    || { echo "network cap could not be created"; exit 3; }
  docker run -d --name cap-kafka --network cap --cpus 2 --memory 3g -p 127.0.0.1:9405:9405 \
    -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller -e CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qg \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:29093 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:29093,EXTERNAL://0.0.0.0:9405 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://cap-kafka:29092,EXTERNAL://localhost:9405 \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    -v "$KVOL:/var/lib/kafka/data" "$KAFKA" > "$C/cap-kafka.id" || { echo "cap-kafka could not be started"; exit 3; }
  date +%s > "$C/T0"; : > "$C/clock.log"
  echo "setup $(date -Is) T0=$(cat "$C/T0")" >> "$C/campaign.log"
  ;;
mysql)
  size=$2; identity
  docker ps -a --format '{{.Names}}' | grep -qx cap-mysql && die "cap-mysql exists (remove the previous size first)"
  docker volume inspect "cap-mysql-$size" >/dev/null 2>&1 && die "volume cap-mysql-$size exists"
  # A size is built only if, at the measured bytes per table (A100's, or the
  # prior estimate before A100 is measured), the fixture would leave room
  # for the following run to start: 40 GiB free.
  if [ "$size" != 001 ]; then
    bpt=$(grep -o 'fixture a100 rc=0 .*bytes_per_table=[0-9]*' "$C/campaign.log" | tail -1 | sed 's/.*=//')
    src="measured at a100"
    [ -n "$bpt" ] || { bpt=$PRIOR_BPT; src="prior estimate"; }
    projected=$(( $(avail) - bpt * ${TABLES[$size]} ))
    [ "$projected" -ge "$NEED_RUN" ] || die "fit: a$size leaves $projected bytes free < $NEED_RUN for its run ($bpt bytes per table, $src; host limit; not built)"
    log "fit a$size: projected free after the fixture $projected bytes ($bpt bytes per table, $src)"
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
  # The documented grants (docs/src/sources/mysql.md), including the fresh
  # CDC start's capture boundary: BACKUP_ADMIN and performance_schema.log_status.
  docker exec cap-mysql mysql -uroot -prootpw -e "CREATE USER 'df'@'%' IDENTIFIED BY 'dfpw';
    GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'df'@'%'; GRANT SELECT, SHOW VIEW ON \`cust_%\`.* TO 'df'@'%';
    GRANT BACKUP_ADMIN ON *.* TO 'df'@'%'; GRANT SELECT ON performance_schema.log_status TO 'df'@'%'; FLUSH PRIVILEGES;" \
    || die "mysql: df user not created"
  docker exec cap-mysql mysql -uroot -prootpw -N -e "SHOW GRANTS FOR 'df'@'%'" > "$C/cap-mysql-$size.grants" \
    && grep -q BACKUP_ADMIN "$C/cap-mysql-$size.grants" && grep -q 'performance_schema`.`log_status' "$C/cap-mysql-$size.grants" \
    || die "mysql: df grants incomplete (see cap-mysql-$size.grants)"
  log "mysql $size up: container $(cat "$C/cap-mysql-$size.id") volume cap-mysql-$size"
  ;;
fixture)
  size=$2; identity
  used=$(awk '$2 == "END" && $3 == "F" {s += $5} END {print s + 0}' "$C/clock.log")
  deadline=$(( ${BUDGET[F]} - used ))
  [ "$deadline" -gt 0 ] || die "fixture budget spent" 6
  budget F "$deadline"; preflight "$NEED_FIXTURE"
  before=$(avail); s=$(date +%s)
  echo "$s START F a$size $deadline" >> "$C/clock.log"
  (cd "$C" && DEADLINE_SECS=$deadline START_GIB=51 FLOOR_GIB=26 kit/floor-watch.sh -- \
     bin/fleet-harness fixture "configs/a$size.yaml" --concurrency 16) > "$C/fixture-a$size.log" 2>&1
  rc=$?; finish F "a$size" "$s" "$rc"
  log "fixture a$size rc=$rc free_before=$before free_after=$(avail) bytes_per_table=$(( (before - $(avail)) / ${TABLES[$size]} ))"
  exit $rc
  ;;
adopt)
  # A kept fixture container, matched by its full id, image and only mount.
  size=$2 want=$3; identity
  got=$(docker inspect --format '{{.Id}} {{.Name}} {{.State.Status}} {{.Config.Image}} {{range .Mounts}}{{.Type}}:{{.Name}}{{end}}' "$want" 2>/dev/null)
  [ "$got" = "$want /cap-mysql exited $MYSQL volume:cap-mysql-$size" ] \
    || die "adopt: $want is not the stopped size-$size fixture container ($got)"
  docker start "$want" >/dev/null || die "adopt: start failed"
  for _ in $(seq 120); do docker exec cap-mysql mysql -uroot -prootpw -e 'select 1' >/dev/null 2>&1 && break; sleep 2; done
  echo "$want" > "$C/cap-mysql-$size.id"
  docker exec cap-mysql mysql -uroot -prootpw -N -e "SHOW GRANTS FOR 'df'@'%'" > "$C/cap-mysql-$size.grants" \
    && grep -q BACKUP_ADMIN "$C/cap-mysql-$size.grants" || die "adopt: df grants incomplete"
  log "adopted size $size: container $want volume cap-mysql-$size free=$(avail)"
  ;;
tune)
  # R1 at runtime (not persisted): the table caches sized to the active
  # set. The settings in effect are recorded.
  [ "${2:-}" = R1 ] || die "tune: only R1 (R2 is applied live by the calibration)"
  identity
  docker exec cap-mysql mysql -uroot -prootpw -e "SET GLOBAL table_definition_cache = 50000; SET GLOBAL table_open_cache = 50000;" \
    || die "tune: R1 not applied"
  { date -Is; docker exec cap-mysql mysql -uroot -prootpw -N -e "SHOW GLOBAL VARIABLES WHERE Variable_name IN ('table_definition_cache', 'table_open_cache', 'table_open_cache_instances', 'open_files_limit', 'innodb_buffer_pool_size', 'max_connections')"; } \
    | tee -a "$C/mysql-settings.log"
  log "tune R1 applied: $(tr '\n\t' '  ' < "$C/mysql-settings.log" | tail -c 400)"
  ;;
drop)
  size=$2
  id=$(cat "$C/cap-mysql-$size.id") || die "no container id recorded for size $size"
  [ "$(docker inspect --format '{{.Id}} {{range .Mounts}}{{.Name}}{{end}}' cap-mysql)" = "$id cap-mysql-$size" ] \
    || die "cap-mysql is not the size-$size container with volume cap-mysql-$size"
  # Only after every run of this size has its evidence verified.
  [ -s "$C/fixture-a$size.log" ] || die "drop: no fixture log for size $size"
  for r in "$C"/runs/[a-z]*"$size"/ "$C"/runs/[a-z]*"$size"-*/; do
    [ -d "$r" ] || continue
    (cd "$r" && [ -s MANIFEST.sha256 ] && sha256sum -c --quiet MANIFEST.sha256) \
      || die "drop: evidence of $(basename "$r") does not verify"
    log "drop: evidence of $(basename "$r") verified"
  done
  before=$(avail)
  docker rm -f "$id" >/dev/null && docker volume rm "cap-mysql-$size" >/dev/null || die "drop failed"
  log "dropped size $size: container $id volume cap-mysql-$size free $before -> $(avail)"
  ;;
run)
  phase=$2 name=$3
  size=$(echo "$name" | sed -E 's/^[a-z]+([0-9]{3}).*/\1/')
  case $phase in
    # Phase A: the configuration's duration, 600 s of drain and 900 s.
    A)    dur=$(awk '$1 == "duration_secs:" {print $2}' "$C/configs/$name.yaml")
          [ -n "$dur" ] || die "no duration in $name.yaml"
          deadline=$(( dur + 600 + 900 )); drain=600 ;;
    B|BR) deadline=$(( ${ALLOW[$size]} + 1800 + 1800 + 900 )); drain=1800 ;;
    C)    deadline=5400; drain=900 ;;
    V)    deadline=600; drain=120 ;;
    # The calibration: one invocation of at most 20 minutes, drain included.
    CAL)  deadline=1200; drain=240 ;;
    *)    die "phase $phase" ;;
  esac
  identity; budget "$phase" "$deadline"; preflight "$NEED_RUN"
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
  DEADLINE_SECS=$deadline START_GIB=41 FLOOR_GIB=26 "$C/kit/floor-watch.sh" -- \
    "$C/bin/fleet-harness" run "$C/configs/$name.yaml" --drain-idle-secs 60 --drain-max-secs "$drain" \
    > run.log 2>&1 &
  fw=$!
  # The harness is floor-watch's child running the pinned binary (matched by
  # process name and executable, never by command line: floor-watch's own
  # subshells carry the binary's path in theirs).
  hp= bin=$(readlink -f "$C/bin/fleet-harness")
  for _ in $(seq 100); do
    for p in $(pgrep -P "$fw" -x fleet-harness); do
      [ "$(readlink -f "/proc/$p/exe")" = "$bin" ] && hp=$p
    done
    [ -n "$hp" ] && break; sleep 0.2
  done
  topic=cap.$name
  if [ -z "$hp" ]; then
    echo "$(date -Is) harness not found under floor-watch $fw; stopped unobserved" > NOHARNESS
    pkill -INT -P "$fw" -x fleet-harness
  else
    log "run $name: harness pid $hp observed"
    # CPU and memory allocations at the start (the calibration may raise
    # MySQL's; the end-of-run inspections record the final ones).
    for c in cap-mysql cap-kafka cap-df; do
      docker inspect --format "$c {{.HostConfig.NanoCpus}} {{.HostConfig.Memory}}" "$c"
    done > allocations-start.txt
    "$C/kit/watch.sh" "$inv" "$hp" &
    case $phase in
      B|BR|V) python3 "$C/kit/frontier.py" B "$inv" "${ACTIVE[$size]}" --live "$hp" "$topic" "$KAFKA" > live.log 2>&1 & ;;
      CAL)    python3 "$C/kit/frontier.py" CAL "$inv" "${ACTIVE[$size]}" --live "$hp" "$topic" "$KAFKA" > live.log 2>&1 & ;;
    esac
  fi
  wait "$fw"; rc=$?
  echo "$rc" > floor-watch.rc
  wait
  docker stop cap-cov-live >/dev/null 2>&1   # never left behind (auto-removed)
  # End state. A failed HTTP request, read or parse leaves the value unknown
  # (null, with the error), never zero; the scraped body is kept as evidence.
  python3 - "$started" > end-state.json <<'PY'
import json, re, subprocess, sys, urllib.request
started, out = sys.argv[1], {}
try:
    with urllib.request.urlopen("http://127.0.0.1:19010/metrics", timeout=10) as r:
        if r.status != 200:
            raise RuntimeError(f"HTTP {r.status}")
        body = r.read().decode()
    open("metrics-end.txt", "w").write(body)
    inc, families = 0.0, set()
    for line in body.splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        m = re.fullmatch(r'([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)(\s+-?\d+)?\s*', line)
        if not m:
            raise ValueError(f"unparseable metrics line: {line[:200]!r}")
        families.add(m.group(1))
        if m.group(1) == "deltaforge_incidents_raised_total":
            inc += float(m.group(3))
    # The incident counter exists only once raised; its absence is zero only
    # in a complete scrape of this run's instance, shown by its sink counter.
    if "deltaforge_sink_events_total" not in families:
        raise ValueError("deltaforge_sink_events_total absent: not a complete scrape of this run")
    out.update(incidents=inc, incidents_read="ok")
except Exception as e:
    out.update(incidents=None, incidents_read=f"{type(e).__name__}: {e}")
try:
    i = json.loads(subprocess.check_output(["docker", "inspect", "cap-df"]))[0]
    out.update(restarted=i["RestartCount"] != 0 or i["State"]["StartedAt"] != started or not i["State"]["Running"],
               oom_killed=i["State"]["OOMKilled"], inspect_read="ok")
except Exception as e:
    out.update(restarted=None, oom_killed=None, inspect_read=f"{type(e).__name__}: {e}")
print(json.dumps(out))
PY
  docker logs -t cap-df > df.log 2>&1
  for c in cap-df cap-mysql cap-kafka; do docker inspect "$c" > "inspect-$c.json"; done
  docker stop cap-df >/dev/null && docker rename cap-df "cap-df-$name"
  # Coverage from the retained topic, then the evaluation; the topic stays.
  python3 "$C/kit/frontier.py" coverage "$inv" "${ACTIVE[$size]}" "$topic" "$KAFKA" | tee -a "$C/campaign.log"
  ev=$phase; [ "$phase" = BR ] && ev=B; [ "$phase" = V ] && ev=A
  [ "$phase" = CAL ] && docker inspect --format '{{.HostConfig.NanoCpus}}' cap-mysql > mysql-cpus-end.txt
  python3 "$C/kit/frontier.py" "$ev" "$inv" "${ACTIVE[$size]}" | tee -a "$C/campaign.log"
  vrc=${PIPESTATUS[0]}
  find . -type f ! -name MANIFEST.sha256 ! -path './state/*' -print0 | sort -z | xargs -0 sha256sum > MANIFEST.sha256
  finish "$phase" "$name" "$s" "$rc"
  # The topic is deleted only after the completeness evaluation and a
  # verified manifest, and only when the verdict marks it deletable (no
  # failure, evidence complete, coverage established). Otherwise it stays.
  if sha256sum -c --quiet MANIFEST.sha256 \
     && python3 -c 'import json,sys; sys.exit(0 if json.load(open("verdict.json")).get("topic_deletable") is True else 1)'; then
    if docker exec cap-kafka kafka-topics --bootstrap-server localhost:29092 --delete --topic "$topic" >> "$C/topics.log" 2>&1; then
      log "deleted topic $topic of verified run $name"
    else
      log "topic $topic: deletion failed (kept)"
    fi
  else
    log "topic $topic kept: verdict not deletable or manifest unverified"
  fi
  [ "$rc" = 3 ] && exit 3
  exit "$vrc"
  ;;
*)
  sed -n '2,12p' "$0"; exit 2 ;;
esac

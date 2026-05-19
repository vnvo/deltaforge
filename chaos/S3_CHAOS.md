# S3 Sink Chaos Testing

This document describes how to chaos-test the S3 / Parquet sink against
MinIO via Toxiproxy. The infrastructure (MinIO + toxiproxy route) is
in `docker-compose.chaos.yml` under the `s3-infra` profile.

> Programmatic chaos scenarios (parallel to `sr-outage`, `backlog-drain`)
> are tracked as a Phase 2 follow-up. The procedures below are manual
> runs that exercise the same failure modes.

## Setup

```bash
# Start base + s3-infra + a DeltaForge instance + a source.
docker compose -f docker-compose.chaos.yml \
  --profile base \
  --profile s3-infra \
  --profile mysql-infra \
  --profile df \
  up -d

# Verify MinIO is up and the lifecycle policy was applied.
docker compose -f docker-compose.chaos.yml exec mc-bootstrap \
  mc ilm list local/deltaforge-chaos
```

Apply a pipeline that targets S3:

```bash
curl -X POST http://localhost:8080/pipelines \
  -H 'Content-Type: application/json' \
  --data-binary @chaos/config/mysql-to-s3.yaml
```

Browse output:
- **MinIO web console**: <http://localhost:9001> (login: `minioadmin` / `minioadmin`)
- **Bucket**: `deltaforge-chaos`, prefix `lake/table=...`

## Scenarios

### S1. Network partition — S3 unreachable

**What it proves:** the sink backpressures correctly when S3 is down, does
not produce partial files, and resumes cleanly when connectivity returns.

```bash
# Cut the MinIO proxy
curl -X POST http://localhost:8474/proxies/minio \
  -H 'Content-Type: application/json' \
  -d '{"enabled":false}'

# Insert rows into the source DB; observe pipeline state
mysql -h localhost -P 5100 -u cdc_user -pcdc_password orders \
  -e "INSERT INTO orders (...) VALUES ..."

# Expected:
#   - DeltaForge sink.send_batch starts returning SinkError::Io
#   - Coordinator retries with backoff
#   - No files appear in MinIO under `lake/` (atomicity preserved)
#   - Source checkpoint does NOT advance

# Restore the proxy
curl -X POST http://localhost:8474/proxies/minio \
  -H 'Content-Type: application/json' \
  -d '{"enabled":true}'

# Expected:
#   - Pipeline catches up on the buffered events
#   - Files appear in MinIO
```

### S2. Slow uploads — latency spike

**What it proves:** the sink handles slow S3 without OOM or starvation of
other sinks in the same pipeline.

```bash
# Inject 2s latency on every byte
curl -X POST http://localhost:8474/proxies/minio/toxics \
  -H 'Content-Type: application/json' \
  -d '{"type":"latency","name":"slow","attributes":{"latency":2000}}'

# Run a soak
mysql -h localhost -P 5100 -u cdc_user -pcdc_password orders \
  -e "CALL chaos_soak.insert_n(10000)"

# Expected:
#   - Throughput drops, but no errors
#   - Memory does not blow up (writers flush to S3 as they roll)
#   - No data loss

# Remove the toxic
curl -X DELETE http://localhost:8474/proxies/minio/toxics/slow
```

### S3. Mid-upload kill (atomicity)

**What it proves:** the file-level atomicity guarantee — a process killed
mid-multipart leaves no visible partial file at the destination.

```bash
# Configure a long-rolling pipeline (large max_bytes) so writes are
# buffered for a while before close. Then kill the DeltaForge container
# mid-batch:
docker compose -f docker-compose.chaos.yml kill deltaforge-release

# Expected:
#   - `mc ls local/deltaforge-chaos/lake/...` shows only files that
#     COMPLETED before the kill — no partial Parquet files visible
#   - `mc ls --incomplete local/deltaforge-chaos/` shows orphan multipart
#     uploads (cleaned by the lifecycle policy within 24h)

# Restart DeltaForge — it replays from the last committed checkpoint
docker compose -f docker-compose.chaos.yml up -d deltaforge-release
```

### S4. Backpressure interaction (multi-sink)

**What it proves:** a slow S3 sink doesn't block other sinks in the same
pipeline (when `required: false` on S3, or when both are `required: true`
and the coordinator commit policy is `all`).

Run with the multi-sink config in `chaos/config/mysql-multisink.yaml`
(future work — for now, attach both Kafka and S3 sinks to the same
pipeline manually).

## Observability

Metrics emitted by the S3 sink:
- `deltaforge_sink_s3_files_committed_total{table, reason}`
- `deltaforge_sink_bytes_total{table}` — bytes uploaded
- `deltaforge_sink_s3_writer_open` (gauge) — partitions with in-flight writers
- `deltaforge_sink_s3_encode_errors_total{reason}`
- `deltaforge_sink_s3_put_errors_total{reason}`

Watch in Grafana at <http://localhost:3000> after starting the `base`
profile. The standard CDC dashboard shows them under the "Sink" panels.

## Operational notes

- **Lifecycle policy**: production deployments MUST configure
  `AbortIncompleteMultipartUpload: 1 day` on the target bucket. The
  `mc-bootstrap` container in the chaos compose applies this to MinIO.
  Without it, S3 storage costs grow with every abandoned batch.
- **Direct vs proxied endpoint**: switch `endpoint` in the sink config
  between `http://minio:9000` (direct, no fault injection) and
  `http://toxiproxy:5104` (chaos path) as needed.
- **Multi-region**: not yet supported. Single endpoint per sink.

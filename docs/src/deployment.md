# Deployment

## Docker

```bash
docker run --rm \
  -e MYSQL_USER=cdc_user \
  -e MYSQL_PASSWORD=s3cret \
  -v $(pwd)/pipeline.yaml:/etc/deltaforge/pipeline.yaml:ro \
  ghcr.io/vnvo/deltaforge:latest \
  --config /etc/deltaforge/pipeline.yaml
```

## Docker Compose

See the [chaos testing environment](https://github.com/vnvo/deltaforge/blob/main/docker-compose.chaos.yml) for a full Docker Compose example with MySQL, Kafka, Prometheus, Grafana, and Loki.

## Kubernetes (Helm)

### Install

```bash
helm install deltaforge ./deploy/helm/deltaforge \
  --set secrets.create=true \
  --set secrets.data.MYSQL_USER=cdc_user \
  --set secrets.data.MYSQL_PASSWORD=s3cret
```

### What it deploys

| Resource | Purpose |
|----------|---------|
| StatefulSet | DeltaForge pod with stable identity |
| ConfigMap | Pipeline YAML configuration |
| PVC | Persistent storage for checkpoints + DLQ |
| Service | ClusterIP with API (8080) and metrics (9000) ports |
| ServiceAccount | Pod identity |
| ServiceMonitor | Prometheus Operator integration (optional) |
| PodDisruptionBudget | Availability guard during voluntary disruptions |
| Secret | Credentials (optional, dev only) |

### Secrets

Secrets contain **only credentials** (username, password, tokens). Connection details (host, port, topic) stay in the pipeline config. Pipeline config uses `${VAR_NAME}` shell expansion at startup:

```yaml
# Pipeline config (in ConfigMap)
dsn: "mysql://${MYSQL_USER}:${MYSQL_PASSWORD}@mysql-primary:3306/orders"
brokers: "kafka:9092"     # not secret — stays in config
```

For production, create K8s Secrets separately and reference them:

```yaml
secrets:
  existingSecrets:
    - name: mysql-creds           # keys: MYSQL_USER, MYSQL_PASSWORD
    - name: kafka-sasl-creds      # keys: KAFKA_SASL_USER, KAFKA_SASL_PASSWORD
```

### Health probes

The chart configures liveness and readiness probes automatically:

- **Liveness** (`/health`): returns 200 when healthy, 503 when a pipeline has failed. Triggers pod restart on prolonged failure.
- **Readiness** (`/ready`): returns 200 with pipeline status JSON. Controls traffic routing.

### Monitoring

Prometheus annotations are enabled by default (`prometheus.io/scrape: "true"`). For Prometheus Operator, enable the ServiceMonitor:

```yaml
serviceMonitor:
  enabled: true
  interval: 15s
```

### Storage

**Development/testing:** SQLite on a PersistentVolume (default). Simple, no external dependencies.

**Production:** PostgreSQL is recommended. Benefits:
- Survives pod rescheduling without PVC migration
- Proper backup/restore via `pg_dump`
- Supports multiple replicas sharing state (future operator/sharding)
- Better concurrency under high checkpoint commit rates

```yaml
storage:
  backend: postgres

persistence:
  enabled: false    # no PVC needed with Postgres

secrets:
  existingSecrets:
    - name: deltaforge-storage    # must contain key: STORAGE_DSN
```

The `STORAGE_DSN` Secret should contain a PostgreSQL connection string:

```
postgresql://deltaforge:password@postgres.infra:5432/deltaforge
```

Create the database and user beforehand:

```sql
CREATE USER deltaforge WITH PASSWORD 'password';
CREATE DATABASE deltaforge OWNER deltaforge;
```

DeltaForge creates its tables automatically on first connection.

### Full values reference

See the [Helm chart README](https://github.com/vnvo/deltaforge/blob/main/deploy/helm/deltaforge/README.md) for all configurable values.

## S3 sink prerequisites

If any pipeline uses the [S3 sink](sinks/s3.md), the target bucket **must** have a lifecycle rule that aborts incomplete multipart uploads. DeltaForge does not track multipart upload IDs externally; partial uploads from process crashes are reclaimed by the bucket's lifecycle policy.

**Required policy:**

```json
{
  "Rules": [{
    "ID": "abort-abandoned-multiparts",
    "Status": "Enabled",
    "Filter": {},
    "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 1 }
  }]
}
```

Apply via AWS CLI:
```bash
aws s3api put-bucket-lifecycle-configuration \
  --bucket my-data-lake \
  --lifecycle-configuration file://lifecycle.json
```

All major S3-compatible providers (AWS S3, MinIO, Wasabi, Backblaze B2, GCS S3-interop, R2) support this rule. Without it, S3 storage costs accumulate with every aborted DeltaForge batch.

### IAM permissions

The S3 sink needs `s3:PutObject`, `s3:GetObject`, `s3:ListBucket`, `s3:AbortMultipartUpload`, and `s3:ListBucketMultipartUploads` on the target bucket and its key prefix:

```json
{
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "s3:PutObject",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:AbortMultipartUpload",
      "s3:ListBucketMultipartUploads"
    ],
    "Resource": [
      "arn:aws:s3:::my-data-lake",
      "arn:aws:s3:::my-data-lake/*"
    ]
  }]
}
```

On EKS, prefer **IRSA** (IAM Roles for Service Accounts) over inline access keys. DeltaForge's S3 sink uses the AWS default credential chain when `access_key_id` is omitted from the sink config, so IRSA works without code changes.

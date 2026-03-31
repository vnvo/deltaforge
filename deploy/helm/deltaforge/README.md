# DeltaForge Helm Chart

Deploy DeltaForge CDC engine on Kubernetes.

## Quick Start

```bash
helm install deltaforge ./deploy/helm/deltaforge \
  --set secrets.create=true \
  --set secrets.data.MYSQL_USER=cdc_user \
  --set secrets.data.MYSQL_PASSWORD=s3cret
```

## Configuration

### Pipeline

Provide your pipeline YAML inline in `values.yaml`:

```yaml
pipeline:
  config: |
    apiVersion: deltaforge/v1
    kind: Pipeline
    metadata:
      name: orders-cdc
      tenant: production
    spec:
      source:
        type: mysql
        config:
          id: orders-mysql
          dsn: "mysql://${MYSQL_USER}:${MYSQL_PASSWORD}@mysql-primary:3306/orders"
          tables: ["orders.*"]
          on_schema_drift: adapt
      sinks:
        - type: kafka
          config:
            id: orders-kafka
            brokers: kafka:9092
            topic: cdc.orders
            exactly_once: true
      batch:
        max_events: 4000
        max_bytes: 16777216
      commit_policy:
        mode: required
      journal:
        enabled: true
```

Or reference an existing ConfigMap:

```yaml
pipeline:
  existingConfigMap: my-pipeline-config
```

### Secrets

Secrets contain **only credentials** (username, password, tokens). Connection details (host, port, topic) stay in the pipeline config.

Pipeline config references secrets via `${VAR_NAME}` shell expansion:

```yaml
# In pipeline config:
dsn: "mysql://${MYSQL_USER}:${MYSQL_PASSWORD}@mysql-primary:3306/orders"

# In values.yaml — reference existing secrets:
secrets:
  existingSecrets:
    - name: mysql-creds           # must contain keys: MYSQL_USER, MYSQL_PASSWORD
    - name: kafka-sasl-creds      # must contain keys: KAFKA_SASL_USER, KAFKA_SASL_PASSWORD
```

For dev/testing, create secrets inline (NOT for production):

```yaml
secrets:
  create: true
  data:
    MYSQL_USER: "cdc_user"
    MYSQL_PASSWORD: "s3cret"
```

### Storage

**Dev/testing:** SQLite on a PersistentVolume (default):

```yaml
storage:
  backend: sqlite
  path: /data/deltaforge.db

persistence:
  enabled: true
  size: 1Gi
```

**Production (recommended):** PostgreSQL — survives pod rescheduling, proper backups, supports future multi-replica:

```yaml
storage:
  backend: postgres

persistence:
  enabled: false    # no PVC needed

secrets:
  existingSecrets:
    - name: deltaforge-storage    # must contain key: STORAGE_DSN
```

`STORAGE_DSN` example: `postgresql://deltaforge:password@postgres.infra:5432/deltaforge`

### Monitoring

Prometheus annotations are enabled by default. For Prometheus Operator:

```yaml
serviceMonitor:
  enabled: true
  interval: 15s
```

## Values Reference

| Key | Default | Description |
|-----|---------|-------------|
| `image.repository` | `ghcr.io/vnvo/deltaforge` | Container image |
| `image.tag` | Chart appVersion | Image tag |
| `pipeline.config` | (example) | Pipeline YAML spec |
| `pipeline.existingConfigMap` | `""` | Use existing ConfigMap |
| `storage.backend` | `sqlite` | sqlite, memory, or postgres |
| `storage.path` | `/data/deltaforge.db` | SQLite DB path |
| `persistence.enabled` | `true` | Create PVC for /data |
| `persistence.size` | `1Gi` | PVC size |
| `secrets.create` | `false` | Create Secret from values |
| `secrets.existingSecrets` | `[]` | Reference existing Secrets |
| `service.type` | `ClusterIP` | Service type |
| `service.apiPort` | `8080` | REST API port |
| `service.metricsPort` | `9000` | Prometheus metrics port |
| `resources.requests.cpu` | `250m` | CPU request |
| `resources.requests.memory` | `256Mi` | Memory request |
| `resources.limits.cpu` | `2` | CPU limit |
| `resources.limits.memory` | `1Gi` | Memory limit |
| `serviceMonitor.enabled` | `false` | Create ServiceMonitor |
| `securityContext.runAsUser` | `10001` | Container user ID |

# Quickstart

Get DeltaForge running in minutes.

## 1. Prepare a pipeline spec

Create a YAML file that defines your CDC pipeline. Environment variables are expanded at runtime, so secrets stay out of version control.

```yaml
metadata:
  name: orders-mysql-to-kafka
  tenant: acme

spec:
  source:
    type: mysql
    config:
      id: orders-mysql
      dsn: ${MYSQL_DSN}
      tables:
        - shop.orders

  processors:
    - type: javascript
      id: transform
      inline: |
        (event) => {
          event.tags = ["processed"];
          return [event];
        }

  sinks:
    - type: kafka
      config:
        id: orders-kafka
        brokers: ${KAFKA_BROKERS}
        topic: orders
        required: true

  batch:
    max_events: 500
    max_bytes: 1048576
    max_ms: 1000
```

## 2. Start DeltaForge

**Using Docker (recommended):**

```bash
docker run --rm \
  -p 8080:8080 -p 9000:9000 \
  -e MYSQL_DSN="mysql://user:pass@host:3306/db" \
  -e KAFKA_BROKERS="kafka:9092" \
  -v $(pwd)/pipeline.yaml:/etc/deltaforge/pipeline.yaml:ro \
  -v deltaforge-checkpoints:/app/data \
  ghcr.io/vnvo/deltaforge:latest \
  --config /etc/deltaforge/pipeline.yaml
```

**From source:**

```bash
cargo run -p runner -- --config ./pipeline.yaml
```

### Runner options

| Flag | Default | Description |
|------|---------|-------------|
| `--config` | (required) | Path to pipeline spec file or directory |
| `--api-addr` | `0.0.0.0:8080` | REST API address |
| `--metrics-addr` | `0.0.0.0:9095` | Prometheus metrics address |

## 3. Verify it's running

Check health and pipeline status:

```bash
# Liveness probe
curl http://localhost:8080/healthz

# Readiness with pipeline status
curl http://localhost:8080/readyz

# List all pipelines
curl http://localhost:8080/pipelines
```

## 4. Manage pipelines

Control pipelines via the REST API:

```bash
# Pause a pipeline
curl -X POST http://localhost:8080/pipelines/orders-mysql-to-kafka/pause

# Resume a pipeline
curl -X POST http://localhost:8080/pipelines/orders-mysql-to-kafka/resume

# Stop a pipeline
curl -X POST http://localhost:8080/pipelines/orders-mysql-to-kafka/stop
```

## Next steps

- [CDC Overview](cdc.md) : Understand how Change Data Capture works
- [Configuration](configuration.md) : Full pipeline spec reference
- [Sources](sources/README.md) : MySQL and Postgres setup
- [Sinks](sinks/README.md) : Kafka and Redis configuration
- [Development](development.md) : Build from source, run locally
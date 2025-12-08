# Configuration

DeltaForge pipelines are defined as YAML documents that map directly to the `PipelineSpec` schema. Environment variables inside the YAML are expanded before parsing, so you can keep secrets like DSNs or broker lists outside of version control. You can also inline values directly—placeholders are optional—but `${VAR_NAME}` entries make it easy to switch between local dev secrets and production credentials without editing the files.

- 🔑 **Flexible substitution**: environment variables extend placeholders; hardcoded values remain supported when you prefer explicit config.

## Loading configuration

- Pass a path with `--config <path>` when starting the runner.
- The path may be a single file or a directory. Directories are walked recursively, and every file is parsed as a pipeline spec.
- Multiple specs can be loaded at once; each spawns its own pipeline.

```bash
cargo run -p runner -- --config ./pipelines \
  --api-addr 0.0.0.0:8080 \
  --metrics-addr 0.0.0.0:9095
```

## PipelineSpec structure

### metadata
| field | type | description |
| --- | --- | --- |
| `name` | string (required) | Unique pipeline identifier used in API routes and metrics labels. |
| `tenant` | string (required) | Business tenant or ownership label. |

### spec
| field | type | description |
| --- | --- | --- |
| `sharding` | object (optional) | Downstream partitioning hint with `mode`, optional `count`, and optional `key`. |
| `source` | object (required) | CDC source definition. See [Sources](sources/README.md). |
| `processors` | array (required, can be empty) | Ordered processors executed on each batch. See [Processors](pipelines.md#processors). |
| `sinks` | array (required, at least one) | One or more sinks that receive each batch. See [Sinks](sinks/README.md). |
| `connection_policy` | object (optional) | How the runtime establishes upstream connections (default mode, preferred replicas, and limits). |
| `batch` | object (optional) | Commit unit thresholds. See [Batching](pipelines.md#batching). |
| `commit_policy` | object (optional) | How sink acknowledgements gate checkpoints. See [Commit policy](pipelines.md#commit-policy). |

### Example
```yaml
metadata:
  name: orders-mysql-to-kafka
  tenant: acme
spec:
  sharding:
    mode: hash
    count: 4
    key: customer_id
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
        function process(batch) {
          return batch;
        }
      limits:
        cpu_ms: 50
        mem_mb: 128
        timeout_ms: 500
  sinks:
    - type: kafka
      config:
        id: orders-kafka
        brokers: ${KAFKA_BROKERS}
        topic: orders
        required: true
        exactly_once: false
        client_conf:
          message.timeout.ms: "5000"
    - type: redis
      config:
        id: orders-redis
        uri: ${REDIS_URI}
        stream: orders
  batch:
    max_events: 500
    max_bytes: 1048576
    max_ms: 1000
    respect_source_tx: true
    max_inflight: 2
  commit_policy:
    mode: quorum
    quorum: 2
```
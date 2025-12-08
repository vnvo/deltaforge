# Quickstart

Follow these steps to run DeltaForge with a sample pipeline definition.

## 1. Prepare a pipeline spec
Create a YAML file that matches the `PipelineSpec` schema. Environment variables inside the YAML are expanded before parsing, so secrets and hostnames can be injected at runtime.

- 🧩 **Config-first**: describe the source, processors, and sinks without code changes.

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
        function process(batch) {
          return batch;
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

## 2. Start the runner
Point the runner at a file or directory containing one or more specs. Directories are walked recursively.

```bash
cargo run -p runner -- --config ./pipelines
```

Optional flags:

- `--api-addr 0.0.0.0:8080` — REST control plane for pipeline lifecycle operations.
- `--metrics-addr 0.0.0.0:9095` — Prometheus metrics endpoint.

## 3. Inspect health and readiness
Use the REST endpoints to verify the runtime is healthy and discover pipeline states:

- `GET /healthz` — liveness probe.
- `GET /readyz` — readiness with pipeline status and specs.
- `GET /pipelines` — list all pipelines with their live configuration.

## 4. Iterate safely
Pause, resume, or stop pipelines through the control plane while tweaking batch thresholds, sink requirements, or processor code. DeltaForge will restart pipelines automatically when specs change.
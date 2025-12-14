<p align="center">
  <img src="assets/deltaforge-blc.png" width="450" alt="DeltaForge">
  <br><br>
  <a href="https://github.com/vnvo/deltaforge/releases">
    <img src="https://img.shields.io/github/v/release/vnvo/deltaforge?label=version" alt="Version">
  </a>
  <a href="https://github.com/vnvo/deltaforge">
    <img src="https://img.shields.io/github/stars/vnvo/deltaforge?style=flat" alt="GitHub Stars">
  </a>
  <img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue" alt="License">
</p>

# Introduction

DeltaForge is a modular, config-driven [Change Data Capture](cdc.md) (CDC) micro-framework built in Rust. It streams database changes into downstream systems like Kafka and Redis while giving you full control over how each event is processed.

Pipelines are defined declaratively in YAML, making it straightforward to onboard new use cases without custom code.

## Why DeltaForge?

- ⚡ **Powered by Rust** : Predictable performance, memory safety, and reliable operations.
- 🔌 **Pluggable sources & sinks** : MySQL binlog and Postgres logical replication today, with more planned.
- 🧩 **Config-driven pipelines** : Define sources, processors, sinks, batching, and commit policies in YAML.
- 📦 **Built-in checkpointing** : Resume safely after restarts with exactly-once delivery semantics.
- 🛠️ **Operational guardrails** : Structured logging, Prometheus metrics, and health endpoints out of the box.

## Use Cases

DeltaForge is designed for:

- **Real-time data synchronization** : Keep caches, search indexes, and analytics systems in sync.
- **Event-driven architectures** : Stream database changes to Kafka for downstream consumers.
- **Lightweight ETL** : Transform and route data without heavyweight infrastructure.

> **DeltaForge is _not_ a DAG-based stream processor.** It is a focused CDC engine meant to replace tools like Debezium when you need a lighter, cloud-native, and more customizable runtime.

## Getting Started

| Guide | Description |
|-------|-------------|
| [Quickstart](quickstart.md) | Get DeltaForge running in minutes |
| [CDC Overview](cdc.md) | Understand Change Data Capture concepts |
| [Configuration](configuration.md) | Pipeline spec reference |
| [Development](development.md) | Build from source, contribute |

## Quick Example

```yaml
metadata:
  name: orders-to-kafka
  tenant: acme

spec:
  source:
    type: mysql
    config:
      dsn: ${MYSQL_DSN}
      tables: [shop.orders]

  processors:
    - type: javascript
      inline: |
        (event) => {
          event.processed_at = Date.now();
          return [event];
        }

  sinks:
    - type: kafka
      config:
        brokers: ${KAFKA_BROKERS}
        topic: order-events
```

## Installation

**Docker (recommended):**

```bash
docker pull ghcr.io/vnvo/deltaforge:latest
```

**From source:**

```bash
git clone https://github.com/vnvo/deltaforge.git
cd deltaforge
cargo build --release
```

See the [Development Guide](development.md) for detailed build instructions and available Docker image variants.

## License

DeltaForge is dual-licensed under [MIT](https://github.com/vnvo/deltaforge/blob/main/LICENSE-MIT) and [Apache 2.0](https://github.com/vnvo/deltaforge/blob/main/LICENSE-APACHE).
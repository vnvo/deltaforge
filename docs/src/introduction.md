# Introduction

DeltaForge is a modular, config-driven Change Data Capture (CDC) micro-framework. It streams database changes into downstream systems like Kafka and Redis while giving users control over how each event is processed. Pipelines are defined declaratively in YAML, making it straightforward to onboard new use cases without custom code.


- ⚡ **Powered by Rust**: for predictable performance, security and reliable operations.
- 🔌 **Pluggable sources & sinks**: with first-class MySQL binlog CDC today and more planned for upcoming interations.
- 🧩 **Config-driven pipelines**: specs capture sources, processors, sinks, batching, and commit policy.
- 📦 **Built-in checkpointing and batching**: to keep sinks consistent.
- 🛠️ **Operational guardrails**: structured logging, metrics, and health endpoints.


DeltaForge is designed to enable:

- Efficient and Fast incremental data movement for modern event-driven architectures
- Compact and Inline ETL, making your data processing stack smaller and more cost effective.

----
**DeltaForge is _not_ a DAG-based stream processor; it is a focused CDC engine meant to replace tools like Debezium when you need a lighter, cloud-friendly and more customizable runtime.**

----

This documentation will walk you through installation, configuration, and usage.

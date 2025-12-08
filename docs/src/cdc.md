# Change Data Capture (CDC)

Change Data Capture (CDC) is the practice of streaming database mutations as ordered events so downstream systems can stay in sync without periodic full loads. DeltaForge focuses on **row-level** CDC from a data source to keep consumers accurate and latency-aware.

## Why CDC matters

- ⏱️ **Low latency propagation**: pushes inserts, updates, and deletes in near real time instead of relying on slow batch ETL windows.
- 📉 **Reduced load on primaries**: avoids repeated full-table scans by tailing the transaction log.
- 🧮 **Deterministic replay**: ordered events allow consumers to reconstruct state or power exactly-once sinks with checkpointing.
- 🔄 **Polyglot delivery**: the same change feed can serve caches, queues, warehouses, and search indexes.

## How CDC works in practice

1. 🪪 **Identify change boundaries**: capture table, primary key, and operation type (insert/update/delete) for each row-level event.
2. 🧾 **Read from the transaction log**: follow the data source (or source of truth) to ensure changes reflect committed transactions.
3. 🧰 **Transform and route**: enrich or filter events before delivering to sinks like Redis or Kafka.
4. 🛡️ **Guard with checkpoints**: store offsets so pipelines can resume safely after restarts.

## When to use CDC

Choose CDC when you need:

- 🔥 **Real-time cache population** to keep Redis aligned with MySQL writes.
- 🧠 **Event-driven integrations** that react to specific table mutations.
- 🗃️ **Incremental warehouse loads** without locking source tables.
- 🔀 **Fan-out to multiple sinks** while preserving ordering and idempotency.

## CDC and DeltaForge

- 🧭 **Config-driven flows**: YAML specs declare which tables to watch, how to shape payloads, and where to send them.
- 🧱 **Operational safety**: checkpoints, backpressure-aware batching, and observability reduce drift and replay risk.
- 🧩 **Composable processors**: filters, mappers, and sink-specific serializers keep business logic close to the data stream.

## Considerations

- ⚖️ **Schema evolution**: plan for column additions or renames; prefer backward-compatible changes to avoid consumer breakage.
- 🧲 **Ordering and idempotency**: downstream systems should handle retries and ensure only-once effects when possible.
- 🧮 **Data minimization**: emit only the columns consumers need to reduce bandwidth and exposure of sensitive fields.
- 📊 **Monitoring**: track lag between source position and sink commits to detect regressions early.
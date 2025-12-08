# Sinks

Sinks receive batches from the coordinator after processors run. Each sink lives under `spec.sinks` and can be marked as required or best-effort via the `required` flag. Checkpoint behavior is governed by the pipeline's commit policy.

Current built-in sinks:

- [`redis`](redis.md) — Redis stream sink.
- [`kafka`](kafka.md) — Kafka producer sink.

You can combine multiple sinks in one pipeline to fan out events.

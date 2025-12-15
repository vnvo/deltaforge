# Observability playbook

DeltaForge already ships a Prometheus exporter, structured logging, and a panic hook. The runtime now emits source ingress counters, batching/processor histograms, and sink latency/throughput so operators can build production dashboards immediately. The tables below capture what is wired today and the remaining gaps to make the platform production ready for data and infra engineers.

## What exists today

- Prometheus endpoint served at `/metrics` (default `0.0.0.0:9000`) with descriptors for pipeline counts, source/sink counters, and a stage latency histogram. The recorder is installed automatically when metrics are enabled.
- Structured logging via `tracing_subscriber` with JSON output by default, optional targets, and support for `RUST_LOG` overrides.
- Panic hook increments a `deltaforge_panics_total` counter and logs captured panics before delegating to the default hook.

## Instrumentation gaps and recommendations

The sections below call out concrete metrics and log events to add per component. All metrics should include `pipeline`, `tenant`, and component identifiers where applicable so users can aggregate across fleets.

### Sources (MySQL/Postgres)

| Status | Metric/log | Rationale |
| --- | --- | --- |
| ✅ Implemented | `deltaforge_source_events_total{pipeline,source,table}` counter increments when MySQL events are handed to the coordinator. | Surfaces ingress per table and pipeline. |
| ✅ Implemented | `deltaforge_source_reconnects_total{pipeline,source}` counter when binlog reads reconnect. | Makes retry storms visible. |
| 🚧 Gap | `deltaforge_source_lag_seconds{pipeline,source}` gauge based on binlog/WAL position vs. server time. | Alert when sources fall behind. |
| 🚧 Gap | `deltaforge_source_idle_seconds{pipeline,source}` gauge updated when no events arrive within the inactivity window. | Catch stuck readers before downstream backlogs form. |

### Coordinator and batching

| Status | Metric/log | Rationale |
| --- | --- | --- |
| ✅ Implemented | `deltaforge_batch_events{pipeline}` and `deltaforge_batch_bytes{pipeline}` histograms in `Coordinator::process_deliver_and_maybe_commit`. | Tune batching policies with data. |
| ✅ Implemented | `deltaforge_stage_latency_seconds{pipeline,stage,trigger}` histogram for processor stage. | Provides batch timing per trigger (timer/limits/shutdown). |
| ✅ Implemented | `deltaforge_processor_latency_seconds{pipeline,processor}` histogram around every processor invocation. | Identify slow user functions. |
| 🚧 Gap | `deltaforge_pipeline_channel_depth{pipeline}` gauge from `mpsc::Sender::capacity()`/`len()`. | Detect backpressure between sources and coordinator. |
| 🚧 Gap | Checkpoint outcome counters/logs (`deltaforge_checkpoint_success_total` / `_failure_total`). | Alert on persistence regressions and correlate to data loss risk. |

### Sinks (Kafka/Redis/custom)

| Status | Metric/log | Rationale |
| --- | --- | --- |
| ✅ Implemented | `deltaforge_sink_events_total{pipeline,sink}` counter and `deltaforge_sink_latency_seconds{pipeline,sink}` histogram around each send. | Throughput and responsiveness per sink. |
| 🚧 Gap | Error taxonomy in `deltaforge_sink_failures_total` (add `kind`/`details`). | Easier alerting on specific failure classes (auth, timeout, schema). |
| 🚧 Gap | Backpressure gauge for client buffers (rdkafka queue, Redis pipeline depth). | Early signal before errors occur. |
| 🚧 Gap | Drop/skip counters from processors/sinks. | Auditing and reconciliation. |

### Control plane and health endpoints

| Need | Suggested metric/log | Rationale |
| --- | --- | --- |
| API request accounting | `deltaforge_api_requests_total{route,method,status}` counter and latency histogram using Axum middleware. | Production-grade visibility of operator actions. |
| Ready/Liveness transitions | Logs with pipeline counts and per-pipeline status when readiness changes. | Explain probe failures in log aggregation. |
| Pipeline lifecycle | Counters for create/patch/stop actions with success/error labels; include tenant and caller metadata in logs. | Auditable control-plane operations. |


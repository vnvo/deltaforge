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
| ✅ Implemented | `deltaforge_source_lag_seconds{pipeline}` gauge — replication lag based on last event timestamp vs. wall clock. | Alert when sources fall behind. |
| ✅ Implemented | `deltaforge_source_table_lag_seconds{pipeline,table}` gauge — per-table replication lag within each batch. | Identify which tables are lagging. |
| 🚧 Gap | `deltaforge_source_idle_seconds{pipeline,source}` gauge updated when no events arrive within the inactivity window. | Catch stuck readers before downstream backlogs form. |

### Coordinator and batching

| Status | Metric/log | Rationale |
| --- | --- | --- |
| ✅ Implemented | `deltaforge_batch_events{pipeline}` and `deltaforge_batch_bytes{pipeline}` histograms in `Coordinator::process_deliver_and_maybe_commit`. | Tune batching policies with data. |
| ✅ Implemented | `deltaforge_bytes_total{pipeline}` counter — cumulative bytes processed through the pipeline. `rate()` gives bytes/s throughput. | Monitor data volume and bandwidth utilization. |
| ✅ Implemented | `deltaforge_source_bytes_total{pipeline,source}` counter — cumulative raw bytes received from the source (WAL/binlog). | Track source-side data ingestion rate. |
| ✅ Implemented | `deltaforge_stage_latency_seconds{pipeline,stage,trigger}` histogram for processor stage. | Provides batch timing per trigger (timer/limits/shutdown). |
| ✅ Implemented | `deltaforge_processor_latency_seconds{pipeline,processor}` histogram around every processor invocation. | Identify slow user functions. |
| 🚧 Gap | `deltaforge_pipeline_channel_depth{pipeline}` gauge from `mpsc::Sender::capacity()`/`len()`. | Detect backpressure between sources and coordinator. |
| ✅ Implemented | `deltaforge_checkpoints_total{pipeline}` counter — successful checkpoint commits. | Monitor checkpoint throughput. |

### Sinks (Kafka/Redis/custom)

| Status | Metric/log | Rationale |
| --- | --- | --- |
| ✅ Implemented | `deltaforge_sink_events_total{pipeline,sink}` counter and `deltaforge_sink_latency_seconds{pipeline,sink}` histogram around each send. | Throughput and responsiveness per sink. |
| ✅ Implemented | `deltaforge_sink_batch_total{pipeline,sink}` counter for send. | Number of batches sent per sink. |
| ✅ Implemented | `deltaforge_sink_errors_total{pipeline,sink}` counter with per-sink error tracking. | Alert on sink failures. |
| ✅ Implemented | `deltaforge_sink_txn_commits_total{pipeline,sink}` counter — Kafka transaction commits/s. | Track exactly-once throughput. |
| ✅ Implemented | `deltaforge_sink_txn_aborts_total{pipeline,sink}` counter — Kafka transaction aborts/s. Should be ~0. | Detect fencing or broker issues. |
| ✅ Implemented | `deltaforge_sink_checkpoint_status{pipeline,sink}` gauge (1=ok, 0=behind). | Per-sink checkpoint health. |
| ✅ Implemented | `deltaforge_sink_last_checkpoint_ts{pipeline,sink}` epoch timestamp. | Per-sink checkpoint age. |
| 🚧 Gap | Backpressure gauge for client buffers (rdkafka queue, Redis pipeline depth). | Early signal before errors occur. |

### Pipeline lifecycle

| Status | Metric/log | Rationale |
| --- | --- | --- |
| ✅ Implemented | `deltaforge_pipeline_status{pipeline}` gauge reflecting the current lifecycle state of each pipeline. | Single gauge to alert on stopped or failed pipelines and drive dashboards. |
| ✅ Implemented | `deltaforge_e2e_latency_seconds{pipeline}` histogram measuring wall-clock time from when an event was received by DeltaForge to when it was delivered to the sink. | Measures pipeline delivery latency independently of source clock precision. |
| ✅ Implemented | `deltaforge_source_lag_seconds{pipeline}` gauge — replication lag based on event timestamp vs. wall clock. | Alert when the source is behind real time. |
| ✅ Implemented | `deltaforge_checkpoints_total{pipeline}` counter — checkpoint commits/s. | Monitor checkpoint throughput. |
| ✅ Implemented | `deltaforge_last_checkpoint_ts{pipeline}` epoch timestamp — pipeline-level checkpoint age. | Alert on stale checkpoints. |

#### `deltaforge_pipeline_status` value semantics

The gauge uses a numeric encoding so operators can alert on any non-running state with a single threshold:

| Value | State | Meaning |
|-------|-------|---------|
| `1.0` | **running** | Pipeline is active and processing events |
| `0.5` | **paused** | Source connection alive; event processing suspended |
| `0.0` | **stopped** | Disconnected from source; checkpoint saved; resumable |
| `< 0` | **failed** | Unrecoverable error (position lost, server changed, etc.) |

Example PromQL:

```promql
# Count pipelines in each state
count(deltaforge_pipeline_status == 1)    # running
count(deltaforge_pipeline_status == 0.5) # paused
count(deltaforge_pipeline_status == 0)   # stopped
count(deltaforge_pipeline_status < 0)    # failed

# Alert if any pipeline is not running
count(deltaforge_pipeline_status != 1) > 0
```

#### `deltaforge_e2e_latency_seconds` note

E2E latency is measured from the wall-clock time the event was **received and parsed** by DeltaForge, not from the binlog `header.timestamp`. MySQL binlog timestamps have one-second precision, which would introduce up to 1 s of phantom latency in the histogram. Using the internal receive time gives sub-millisecond accuracy regardless of source clock granularity.

The **replication lag** metric (separate from E2E latency) uses the binlog timestamp and measures how far behind the source is relative to real time — that one-second precision is acceptable for lag alerting.

### Dead Letter Queue

| Status | Metric/log | Rationale |
| --- | --- | --- |
| ✅ Implemented | `deltaforge_dlq_events_total{pipeline,sink,error_kind}` counter. | Track rate of events routed to DLQ. |
| ✅ Implemented | `deltaforge_dlq_entries{pipeline}` gauge — current unacked entries. | Monitor DLQ backlog size. |
| ✅ Implemented | `deltaforge_dlq_saturation_ratio{pipeline}` gauge (0.0-1.0). | Alert at 80% (warning) and 95% (critical). |
| ✅ Implemented | `deltaforge_dlq_evicted_total{pipeline}` counter — entries lost to drop_oldest overflow. | Track data loss from overflow. |
| ✅ Implemented | `deltaforge_dlq_rejected_total{pipeline}` counter — entries lost to reject overflow. | Track data loss from rejection. |
| ✅ Implemented | `deltaforge_dlq_write_failures_total{pipeline}` counter — DLQ storage failures. | Alert on DLQ infrastructure issues. |

### Control plane and health endpoints

| Need | Suggested metric/log | Rationale |
| --- | --- | --- |
| API request accounting | `deltaforge_api_requests_total{route,method,status}` counter and latency histogram using Axum middleware. | Production-grade visibility of operator actions. |
| Ready/Liveness transitions | Logs with pipeline counts and per-pipeline status when readiness changes. | Explain probe failures in log aggregation. |
| Pipeline lifecycle counters | Counters for create/patch/stop/resume actions with success/error labels. | Auditable control-plane operations. |


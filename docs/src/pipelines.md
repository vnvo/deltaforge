# Pipelines

Each pipeline is created from a single `PipelineSpec`. The runtime spawns the source, processors, and sinks defined in the spec and coordinates them with batching and checkpointing.

- 🔄 **Live control**: pause, resume, or stop pipelines through the REST API without redeploying.
- 📦 **Coordinated delivery**: batching and commit policy keep sinks consistent even when multiple outputs are configured.

## Lifecycle controls

The REST API addresses pipelines by `metadata.name` and returns `PipeInfo` records containing the live spec and status.

- `GET /healthz` — liveness probe.
- `GET /readyz` — readiness with pipeline states.
- `GET /pipelines` — list pipelines.
- `POST /pipelines` — create from a full spec.
- `PATCH /pipelines/{name}` — merge a partial spec (for example, adjust batch thresholds) and restart the pipeline.
- `POST /pipelines/{name}/pause` — pause ingestion and coordination.
- `POST /pipelines/{name}/resume` — resume a paused pipeline.
- `POST /pipelines/{name}/stop` — stop a running pipeline.

Pausing halts both source ingestion and the coordinator. Resuming re-enables both ends so buffered events can drain cleanly.

## Processors

Processors run in the declared order for each batch. The built-in processor type is JavaScript, powered by `deno_core`.

- `type: javascript`
  - `id`: processor label.
  - `inline`: JS source. Export a `process(batch)` function that returns the transformed batch.
  - `limits` (optional): resource guardrails (`cpu_ms`, `mem_mb`, `timeout_ms`).

## Batching

The coordinator builds batches using soft thresholds:

- `max_events`: flush after this many events.
- `max_bytes`: flush after the serialized size reaches this limit.
- `max_ms`: flush after this much time has elapsed since the batch started.
- `respect_source_tx`: when true, never split a single source transaction across batches.
- `max_inflight`: cap the number of batches being processed concurrently.

## Commit policy

When multiple sinks are configured, checkpoints can wait for different acknowledgement rules:

- `all`: every sink must acknowledge.
- `required` (default): only sinks marked `required: true` must acknowledge; others are best-effort.
- `quorum`: checkpoint after at least `quorum` sinks acknowledge.

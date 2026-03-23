# Troubleshooting

Common issues and quick checks when running DeltaForge.

- 🩺 **Health-first**: start with `/health` and `/ready` to pinpoint failing components.

## Runner fails to start
- Confirm the config path passed to `--config` exists and is readable.
- Validate YAML syntax and that required fields like `metadata.name` and `spec.source` are present.
- Ensure environment variables referenced in the spec are set (`dsn`, `brokers`, `uri`, etc.).

## Pipelines remain unready
- Check the `/ready` endpoint for per-pipeline status and error messages.
- Verify upstream credentials allow replication (MySQL binlog). Other engines are experimental unless explicitly documented.
- Inspect sink connectivity; a required sink that cannot connect will block checkpoints.

## Slow throughput
- Increase `batch.max_events` or `batch.max_bytes` to reduce flush frequency.
- Adjust `max_inflight` to allow more concurrent batches if sinks can handle parallelism.
- Reduce processor work or add guardrails (`limits`) to prevent slow JavaScript from stalling the pipeline.

## Checkpoints not advancing
- Review the commit policy: `mode: all` or `required` sinks that are unavailable will block progress.
- Look for sink-specific errors (for example, Kafka broker unreachability or Redis backpressure).
- Pause and resume the pipeline to force a clean restart after addressing the underlying issue.

## `/health` returns 503

A `503` means at least one pipeline has entered a permanently failed state — it will not recover on its own. Common causes:

| Cause | Log message | Resolution |
|-------|-------------|------------|
| Failover to a server with no GTID overlap | `position lost after failover` | Re-snapshot from the new primary |
| `RESET BINARY LOGS AND GTIDS` run on same server | `checkpoint GTID set no longer reachable` | Clear the checkpoint DB and re-snapshot |
| Unrecoverable source error | `run task ended with error` | Check source logs; fix the root cause and restart |

Use `GET /pipelines` to see which pipeline has `"status": "failed"` and check its logs for the specific error. After fixing the root cause, restart the DeltaForge process (or the container) to reset pipeline state.

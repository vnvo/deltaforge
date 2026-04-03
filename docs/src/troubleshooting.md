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

## Avro encoding issues

### Schema Registry connection failed

```
WARN Schema Registry unavailable — using cached schema
```

DeltaForge is encoding with a previously cached schema ID. Events are still flowing, but new schema registrations (e.g., after DDL changes) will fail until the SR recovers. Check:
- Schema Registry connectivity: `curl http://<sr-url>/subjects`
- Network/firewall between DeltaForge and the SR
- Monitor `deltaforge_avro_sr_cache_fallback_total` — if increasing, the SR is unreachable

### Schema registration rejected (compatibility failure)

```
WARN DDL change for {table} produced incompatible Avro schema — encoding with previous version
```

The source table DDL changed and the new Avro schema was rejected by the Schema Registry's compatibility rules. DeltaForge attempts to encode with the previous schema version:
- If encoding succeeds: events continue flowing (the old schema still covers the new data)
- If encoding fails: events are routed to DLQ with `schema_mismatch` error

**Resolution:** Either relax the SR subject compatibility mode (e.g., `NONE` or `FORWARD`) or handle the DLQ entries after updating consumer schemas.

### Events routed to DLQ with `schema_mismatch`

A DDL change produced events that can't be encoded under the cached schema. This happens when a non-backward-compatible change is made (e.g., column type change, NOT NULL added to existing column).

**Resolution:**
1. Check `deltaforge_avro_encode_failure_total{reason="schema_mismatch"}`
2. Identify the DDL change from source DB logs
3. Update the SR subject compatibility if needed
4. Restart the pipeline to clear the schema cache and re-register

### BIGINT UNSIGNED overflow warning

```
WARN BIGINT UNSIGNED column {col} mapped to long — values >= 2^63 will fail encoding
```

Only appears when `unsigned_bigint_mode: long` is configured. If a row contains a value ≥ 2^63, encoding will fail and the event is routed to DLQ. The default `unsigned_bigint_mode: string` avoids this entirely.

### Using inferred schema instead of DDL

```
DEBUG no DDL schema available — falling back to JSON inference (Path C)
```

DeltaForge couldn't look up the source table schema and is using a less precise JSON-inferred schema. This happens when:
- The schema loader hasn't cached the table schema yet (first events at startup)
- The table doesn't match the configured table patterns
- The source type doesn't support schema loading

Check `deltaforge_avro_encode_total{path="inferred"}` — if this counter is growing while `path="ddl"` is not, investigate why DDL lookup is failing.

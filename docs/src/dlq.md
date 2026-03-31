# Dead Letter Queue

The Dead Letter Queue (DLQ) routes poison events — events that fail serialization, exceed size limits, or have invalid routing — to a durable queue instead of blocking the pipeline.

## How it works

1. The coordinator dispatches a batch to each sink
2. Each sink pre-serializes events individually. If an event fails serialization or routing, it is flagged as a **DLQ failure** instead of failing the entire batch
3. The remaining (healthy) events are sent to the sink normally
4. Failed events are written to the DLQ queue with error context
5. The pipeline continues — one bad event does not block thousands of good ones

Only **per-event attributable** failures go to the DLQ:

| Error | DLQ eligible | Why |
|-------|-------------|-----|
| Serialization failure | Yes | Event can't be encoded — specific to this event's data |
| Routing failure | Yes | Template resolves to empty/invalid for this event's fields |
| Message too large | Yes | This specific event exceeds the sink's max message size |
| Connection failure | No | Entire sink is down — not caused by one event |
| Auth failure | No | Credentials invalid — affects all events |
| Timeout / backpressure | No | Transient — will resolve with retry |
| Producer fenced | No | Fatal — pipeline stops |

## Configuration

DLQ is opt-in. It is configured under `journal` in the pipeline spec — the journal is DeltaForge's internal event storage system that backs the DLQ (and will support replay in a future release). Enabling the journal with a `dlq` stream activates per-event failure routing:

```yaml
spec:
  journal:
    enabled: true
    max_event_bytes: 262144      # 256KB — truncate larger payloads
    dlq:
      max_entries: 10000         # bounded queue size
      max_age_secs: 604800       # 7 days — auto-purge older entries
      overflow_policy: drop_oldest
```

### Overflow policies

When the DLQ reaches `max_entries`:

| Policy | Behavior |
|--------|----------|
| `drop_oldest` (default) | Evict the oldest DLQ entry to make room for the new one. Most recent failures are usually most valuable for investigation. |
| `reject` | Reject the new DLQ entry. The **failed event is lost** — it is not stored in the DLQ, not retried, and not delivered to the sink. The pipeline continues processing other events normally. An error is logged and `deltaforge_dlq_rejected_total` is incremented. |
| `block` | Block the pipeline until space is available. No events (good or bad) are processed until the operator acks DLQ entries via the REST API. Visible as `degraded` on the health endpoint. |

### Payload truncation

Events that caused `MessageTooLarge` may also be too large for the DLQ. If the event payload exceeds `max_event_bytes` (default 256KB), the `before` and `after` fields are truncated and `payload_truncated: true` is set. All event metadata (source, table, op, id, timestamp) is always preserved.

## REST API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `GET /pipelines/{name}/journal/dlq` | GET | Peek entries (oldest first). Params: `?limit=50&sink_id=...&error_kind=...` |
| `GET /pipelines/{name}/journal/dlq/count` | GET | Count of unacked entries |
| `POST /pipelines/{name}/journal/dlq/ack` | POST | Ack entries up to seq. Body: `{"up_to_seq": 42}` |
| `DELETE /pipelines/{name}/journal/dlq` | DELETE | Purge all entries |

**Filters** (`sink_id`, `error_kind`) affect listing only. Ack is always cumulative from the queue head — it removes all entries up to `up_to_seq`, regardless of filters used when viewing.

### Example: inspect DLQ entries

```bash
# Peek the first 10 DLQ entries
curl -s http://localhost:8080/pipelines/my-pipeline/journal/dlq?limit=10 | jq .

# Filter by sink
curl -s "http://localhost:8080/pipelines/my-pipeline/journal/dlq?sink_id=kafka-primary&limit=5"

# Check DLQ size
curl -s http://localhost:8080/pipelines/my-pipeline/journal/dlq/count
# {"count": 42}

# Ack (remove) entries up to sequence 100
curl -s -X POST http://localhost:8080/pipelines/my-pipeline/journal/dlq/ack \
  -H "Content-Type: application/json" \
  -d '{"up_to_seq": 100}'
# {"acked": 12}

# Purge all entries
curl -s -X DELETE http://localhost:8080/pipelines/my-pipeline/journal/dlq
# {"purged": 42}
```

### Example DLQ entry

```json
{
  "seq": 42,
  "timestamp": 1743350400,
  "pipeline": "orders-pipeline",
  "stream": "dlq",
  "event_id": "01961234-5678-7abc-def0-123456789abc",
  "source_cursor": {"file": "mysql-bin.000005", "pos": 12345},
  "payload_truncated": false,
  "event": {
    "id": "01961234-5678-7abc-def0-123456789abc",
    "source": {"db": "orders", "table": "events"},
    "op": "c",
    "after": {"id": 99, "metadata": "<invalid bytes>"}
  },
  "meta": {
    "sink_id": "kafka-primary",
    "error_kind": "serialization error",
    "error_message": "failed to serialize field 'metadata': invalid UTF-8 sequence",
    "attempts": 1
  }
}
```

## Metrics

| Metric | Type | Labels | Purpose |
|--------|------|--------|---------|
| `deltaforge_dlq_events_total` | counter | pipeline, sink, error_kind | Events routed to DLQ |
| `deltaforge_dlq_entries` | gauge | pipeline | Current unacked entries |
| `deltaforge_dlq_evicted_total` | counter | pipeline | Evicted by drop_oldest |
| `deltaforge_dlq_rejected_total` | counter | pipeline | Rejected by reject policy |
| `deltaforge_dlq_write_failures_total` | counter | pipeline | DLQ storage write failures |
| `deltaforge_dlq_saturation_ratio` | gauge | pipeline | Current / max_entries (0.0-1.0) |

**Health signals:**
- Warning log at 80% saturation
- Error log at 95% saturation

## Storage

The DLQ is built on DeltaForge's existing `StorageBackend` queue primitives (`queue_push`, `queue_peek`, `queue_ack`). It automatically uses whatever storage backend your pipeline is configured with (SQLite, PostgreSQL, or memory). No additional infrastructure is needed.

## Limitations

- DLQ is per-pipeline, not per-sink. Use the `sink_id` filter to view entries for a specific sink.
- There is no automatic retry/reprocessing of DLQ entries. Inspect, fix the root cause, then ack.
- The `block` overflow policy blocks the entire pipeline, not just the failing sink.
- Background age-based cleanup is not yet implemented — entries are only removed via ack/purge or overflow eviction.

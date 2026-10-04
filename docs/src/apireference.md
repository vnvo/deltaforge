# REST API Reference

DeltaForge exposes a REST API for health checks, pipeline management, schema
inspection, and drift detection. All endpoints return JSON.

## Base URL

Default: `http://localhost:8080`

Configure with `--api-addr`:
```bash
deltaforge --config pipelines.yaml --api-addr 0.0.0.0:9090
```

---

## Health Endpoints

### Liveness Probe

```http
GET /health
```

Liveness only: returns `200` while the process and its API are running, and reports how many pipelines have failed. A failed pipeline is a readiness concern (see below), not a reason to restart the process - a pipeline that stopped fail-closed would fail again after every restart.

**Response:** `200 OK`
```json
{"status": "healthy", "pipelines": 3, "failed_pipelines": 1}
```

### Readiness Probe

```http
GET /ready
```

Use for Kubernetes readiness probes. Returns `503` while any pipeline has failed or carries a blocking incident that is open or only acknowledged (acknowledging an incident does not clear it), and names them with their incidents.

**Response:** `503 Service Unavailable`
```json
{
  "status": "not_ready",
  "failed_pipelines": ["orders-cdc"],
  "blocked_pipelines": [
    {
      "name": "orders-cdc",
      "status": "failed",
      "primary_incident": "3f9c...",
      "durability_pending": false,
      "store_unavailable": false,
      "blocking_incidents": [
        {"incident_id": "3f9c...", "reason_code": "unclassified_failure", "status": {"state": "acknowledged"}, "durable": true}
      ],
      "overflow_blocking": 0
    }
  ]
}
```

**Response:** `200 OK`
```json
{
  "status": "ready",
  "pipelines": [
    {
      "name": "orders-cdc",
      "status": "running",
      "spec": { ... }
    }
  ]
}
```

---

## Pipeline Management

### List Pipelines

```http
GET /pipelines
GET /pipelines?label=env:prod
GET /pipelines?label=env:prod&label=team:platform
```

Returns all pipelines with current status. Filter by labels with AND logic. Key-only filter (`?label=env`) matches any value.

**Response:** `200 OK`
```json
[
  {
    "name": "orders-cdc",
    "status": "running",
    "spec": {
      "metadata": { "name": "orders-cdc", "tenant": "acme" },
      "spec": { ... }
    }
  }
]
```

### Get Pipeline

```http
GET /pipelines/{name}
```

Returns a single pipeline by name with operational status.

**Response:** `200 OK`
```json
{
  "name": "orders-cdc",
  "status": "running",
  "spec": { ... },
  "ops": {
    "uptime_seconds": 3600.5,
    "dlq_entries": 0,
    "sink_errors": {},
    "checkpoints": [
      {"sink_id": "kafka-primary", "position": {"file": "mysql-bin.000005", "pos": 12345}, "age_seconds": 0.3}
    ]
  }
}
```

**Errors:**
- `404 Not Found` - Pipeline doesn't exist

### Create Pipeline

```http
POST /pipelines
Content-Type: application/json
```

Creates a new pipeline from a full spec.

**Request:**
```json
{
  "metadata": {
    "name": "orders-cdc",
    "tenant": "acme"
  },
  "spec": {
    "source": {
      "type": "mysql",
      "config": {
        "id": "mysql-1",
        "dsn": "mysql://user:pass@host/db",
        "tables": ["shop.orders"]
      }
    },
    "processors": [],
    "sinks": [
      {
        "type": "kafka",
        "config": {
          "id": "kafka-1",
          "brokers": "localhost:9092",
          "topic": "orders"
        }
      }
    ]
  }
}
```

**Response:** `200 OK`
```json
{
  "name": "orders-cdc",
  "status": "running",
  "spec": { ... }
}
```

**Errors:**
- `409 Conflict` - Pipeline already exists

### Update Pipeline

```http
PATCH /pipelines/{name}
Content-Type: application/json
```

Applies a partial update to an existing pipeline. The spec is merged, the
pipeline is restarted from its last saved checkpoint, and the new config takes
effect immediately. Only the fields present in the request body are changed —
omitted fields retain their current values.

If the pipeline is currently **stopped**, PATCH applies the new config and
restarts it from the saved checkpoint. This is the recommended way to tune
throughput settings before resuming after a planned stop.

**Request:**
```json
{
  "spec": {
    "batch": {
      "max_events": 1000,
      "max_ms": 500
    }
  }
}
```

**Response:** `200 OK`
```json
{
  "name": "orders-cdc",
  "status": "running",
  "spec": { ... }
}
```

**Errors:**
- `404 Not Found` - Pipeline doesn't exist
- `400 Bad Request` - Invalid field value or name mismatch in patch

### Pause Pipeline

```http
POST /pipelines/{name}/pause
```

Suspends event processing while keeping the source connection alive. No new
events are consumed from the binlog/WAL. Resume restarts processing from
exactly where it paused — no events are missed.

**Response:** `200 OK`
```json
{
  "name": "orders-cdc",
  "status": "paused",
  "spec": { ... }
}
```

### Resume Pipeline

```http
POST /pipelines/{name}/resume
```

Resumes a paused or stopped pipeline.

- **From paused** — restarts event processing immediately; source connection was kept alive.
- **From stopped** — reconnects to the source and replays from the last saved checkpoint; any events written to the binlog/WAL while stopped are replayed in order.
- **From failed**: restarts it like a stopped pipeline. A resume never acknowledges or resolves its incidents: an `unclassified_failure` is resolved (`pipeline_recovered`) only once the restarted pipeline has reached verified running (the source finished its startup checks and opened its stream while the coordinator runs and nothing has failed).

**Response:** `200 OK`
```json
{
  "name": "orders-cdc",
  "status": "running",
  "spec": { ... }
}
```

### Stop Pipeline

```http
POST /pipelines/{name}/stop
```

Gracefully stops a pipeline: flushes in-flight events, saves the binlog/WAL
checkpoint, and disconnects from the source. The pipeline remains in the
registry and can be resumed with `POST /pipelines/{name}/resume` or by
issuing a `PATCH` with updated config.

Use stop (rather than delete) when you intend to restart the pipeline later —
for example, before a planned maintenance window or when tuning config for a
backlog drain.

**Response:** `200 OK`
```json
{
  "name": "orders-cdc",
  "status": "stopped",
  "spec": { ... }
}
```

### Delete Pipeline

```http
DELETE /pipelines/{name}
```

Permanently removes a pipeline from the runtime. The checkpoint is **not**
preserved. Use `stop` first if you may want to restart the pipeline later.

**Response:** `204 No Content`

**Errors:**
- `404 Not Found` - Pipeline doesn't exist

---

## Schema Management

### List Database Schemas

```http
GET /pipelines/{name}/schemas
```

Returns all tracked database schemas for a pipeline. These are the schemas
loaded directly from the source database.

**Response:** `200 OK`
```json
[
  {
    "database": "shop",
    "table": "orders",
    "column_count": 5,
    "primary_key": ["id"],
    "fingerprint": "sha256:a1b2c3d4e5f6...",
    "registry_version": 2
  },
  {
    "database": "shop",
    "table": "customers",
    "column_count": 8,
    "primary_key": ["id"],
    "fingerprint": "sha256:f6e5d4c3b2a1...",
    "registry_version": 1
  }
]
```

### Get Schema Details

```http
GET /pipelines/{name}/schemas/{db}/{table}
```

Returns detailed schema information including all columns.

**Response:** `200 OK`
```json
{
  "database": "shop",
  "table": "orders",
  "columns": [
    {
      "name": "id",
      "column_type": "bigint(20) unsigned",
      "data_type": "bigint",
      "nullable": false,
      "ordinal_position": 1,
      "default_value": null,
      "extra": "auto_increment",
      "is_primary_key": true
    },
    {
      "name": "customer_id",
      "column_type": "bigint(20)",
      "data_type": "bigint",
      "nullable": false,
      "ordinal_position": 2,
      "default_value": null,
      "extra": null,
      "is_primary_key": false
    }
  ],
  "primary_key": ["id"],
  "fingerprint": "sha256:a1b2c3d4..."
}
```

---

## Schema Sensing

Schema sensing automatically infers schema structure from JSON event payloads.
This is useful for sources that don't provide schema metadata or for detecting
schema evolution in JSON columns.

### List Inferred Schemas

```http
GET /pipelines/{name}/sensing/schemas
```

Returns all schemas inferred via sensing for a pipeline.

**Response:** `200 OK`
```json
[
  {
    "table": "orders",
    "fingerprint": "sha256:abc123...",
    "sequence": 3,
    "event_count": 1500,
    "stabilized": true,
    "first_seen": "2025-01-15T10:30:00Z",
    "last_seen": "2025-01-15T14:22:00Z"
  }
]
```

| Field | Description |
|-------|-------------|
| `table` | Table name (or `table:column` for JSON column sensing) |
| `fingerprint` | SHA-256 content hash of current schema |
| `sequence` | Monotonic version number (increments on evolution) |
| `event_count` | Total events observed |
| `stabilized` | Whether schema has stopped sampling (structure stable) |
| `first_seen` | First observation timestamp |
| `last_seen` | Most recent observation timestamp |

### Get Inferred Schema Details

```http
GET /pipelines/{name}/sensing/schemas/{table}
```

Returns detailed inferred schema including all fields.

**Response:** `200 OK`
```json
{
  "table": "orders",
  "fingerprint": "sha256:abc123...",
  "sequence": 3,
  "event_count": 1500,
  "stabilized": true,
  "fields": [
    {
      "name": "id",
      "types": ["integer"],
      "nullable": false,
      "optional": false
    },
    {
      "name": "metadata",
      "types": ["object"],
      "nullable": true,
      "optional": false,
      "nested_field_count": 5
    },
    {
      "name": "tags",
      "types": ["array"],
      "nullable": false,
      "optional": true,
      "array_element_types": ["string"]
    }
  ],
  "first_seen": "2025-01-15T10:30:00Z",
  "last_seen": "2025-01-15T14:22:00Z"
}
```

### Export JSON Schema

```http
GET /pipelines/{name}/sensing/schemas/{table}/json-schema
```

Exports the inferred schema as a standard JSON Schema document.

**Response:** `200 OK`
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "orders",
  "type": "object",
  "properties": {
    "id": { "type": "integer" },
    "metadata": { "type": ["object", "null"] },
    "tags": {
      "type": "array",
      "items": { "type": "string" }
    }
  },
  "required": ["id", "metadata"]
}
```

### Get Sensing Cache Statistics

```http
GET /pipelines/{name}/sensing/stats
```

Returns cache performance statistics for schema sensing.

**Response:** `200 OK`
```json
{
  "tables": [
    {
      "table": "orders",
      "cached_structures": 3,
      "max_cache_size": 100,
      "cache_hits": 1450,
      "cache_misses": 50
    }
  ],
  "total_cache_hits": 1450,
  "total_cache_misses": 50,
  "hit_rate": 0.9667
}
```

---

## Drift Detection

Drift detection compares expected database schema against observed data patterns
to detect mismatches, unexpected nulls, and type drift.

### Get Drift Results

```http
GET /pipelines/{name}/drift
```

Returns drift detection results for all tables in a pipeline.

**Response:** `200 OK`
```json
[
  {
    "table": "orders",
    "has_drift": true,
    "columns": [
      {
        "column": "amount",
        "expected_type": "decimal(10,2)",
        "observed_types": ["string"],
        "mismatch_count": 42,
        "examples": ["\"99.99\""]
      }
    ],
    "events_analyzed": 1500,
    "events_with_drift": 42
  }
]
```

### Get Table Drift

```http
GET /pipelines/{name}/drift/{table}
```

Returns drift detection results for a specific table.

**Response:** `200 OK`
```json
{
  "table": "orders",
  "has_drift": false,
  "columns": [],
  "events_analyzed": 1000,
  "events_with_drift": 0
}
```

**Errors:**
- `404 Not Found` - Table not found or no drift data available

---

## Dead Letter Queue

See the [DLQ page](dlq.md) for full documentation.

### Peek DLQ Entries

```http
GET /pipelines/{name}/journal/dlq?limit=50&sink_id=kafka-primary&error_kind=serialization
```

Returns DLQ entries (oldest first). All query params are optional.

### DLQ Count

```http
GET /pipelines/{name}/journal/dlq/count
```

**Response:** `200 OK`
```json
{"count": 42}
```

### Acknowledge DLQ Entries

```http
POST /pipelines/{name}/journal/dlq/ack
Content-Type: application/json

{"up_to_seq": 42}
```

Permanently removes entries from the head up to the given sequence number.

**Response:** `200 OK`
```json
{"acked": 12}
```

### Purge DLQ

```http
DELETE /pipelines/{name}/journal/dlq
```

**Response:** `200 OK`
```json
{"purged": 42}
```

---

## Checkpoint Inspection

### Get Checkpoints

```http
GET /pipelines/{name}/checkpoints
```

Returns per-sink checkpoint positions and ages.

**Response:** `200 OK`
```json
[
  {"sink_id": "kafka-primary", "position": {"file": "mysql-bin.000005", "pos": 12345}, "age_seconds": 0.3},
  {"sink_id": "redis-cache", "position": {"file": "mysql-bin.000005", "pos": 11000}, "age_seconds": 2.1}
]
```

---

## Incidents

A pipeline that stops (or cannot do something safely) records an **incident**: a structured description with a stable `reason_code`, `retryability`, `safety_state` (`halted_safe`, `halted_uncertain`, `running_degraded`), allow-listed `evidence`, `recommended_actions` and an `explanation` generated from them. Incidents never contain error text, DSNs, SQL or row values. The same condition is the same incident across retries and restarts (its `occurrences` grow); a later independent occurrence is a new one.

Lifecycle: `open` -> `acknowledged` -> `resolved`. Acknowledging means an operator has seen it; the incident stays open, keeps blocking the pipeline and readiness, and is resolved only by a verified check (or a future recovery operation). Every transition is recorded in an audit log, including `reclassified`: the same condition raised again with another retryability or safety state (an automatic retry that exhausted its budget now needs an operator) updates the incident rather than opening a new one. A pipeline keeps at most 63 open incidents individually; beyond that one `incident_overflow` incident counts the rest by reason code (blocking ones first).

Reason codes:

| `reason_code` | Raised when | Safety | Resolved by |
|---|---|---|---|
| `pg_different_cluster` | A PostgreSQL source reaches another cluster (or a replaced database); refused before anything is recorded or streamed | `halted_safe` | `lineage_verified`: a later start verifies the source's own server |
| `pg_continuity_unproven` | A PostgreSQL resume position cannot be shown to continue on the server, checked before every `START_REPLICATION` on the session that then streams (see [promotion continuity](failover.md#postgresql-promotion-continuity)). Classes: `slot_missing`, `slot_invalidated`, `wal_lost`, `slot_beyond_checkpoint`, `slot_not_persistent_logical`, `unknown_slot_position`, `timeline_not_descended`, `switch_before_checkpoint`, `switch_before_read_position`, `checkpoint_chain_mismatch` (the checkpoint belongs to another continuity chain, to a transition the record never proved, carries a partial stamp, or predates a recorded transition without being the position it was proven at; evidence carries both chain ids and transitions; recommended: inspect, re-snapshot), `slot_not_synced`, `failover_unsupported_version`, `timeline_unrecorded`, `server_in_recovery` (retried for up to 2 min as `auto_retry`, then `operator_action`; route the source to the writable primary), `wal_behind_checkpoint`, or unknown (`unknown_unreachable`: retried for up to 2 min, during which the incident is open as `auto_retry`, then the same incident becomes `operator_action` when the source stops, or is resolved as `operation_cancelled` when the pipeline is stopped on purpose; `unknown_query_failed`: a server error answer, `operator_action` at once). Evidence includes the recorded and live timelines, the switch point, the slot positions, the WAL flush and read positions; replication never opens | `halted_safe` | `position_verified`: a later start verifies the position |
| `pg_failover_slot_unavailable` | A PostgreSQL 17+ replication slot is not a failover slot: the source keeps streaming on the current primary, but a promotion would stop it (`slot_not_synced`). Recommended action `enable_failover_slot` | `running_degraded` | `failover_slot_verified`: a later stream proves the slot is a failover slot |
| `mysql_gtid_position_unavailable` | The MySQL resume position is not available: `purged`, `not_executed`, `binlog_missing`, or unknown (as above) | `halted_safe` | `position_verified` |
| `schema_drift_blocked` | `on_schema_drift = halt` stopped at a schema change of a table | `halted_safe` | `schema_accepted`: that table is accepted at its first use in a later run (unchanged schema, or an adapt reload) |
| `sink_ack_uncertain` | An S3 durable publish whose HEAD update response was lost and HEAD could not be reread (a Kafka transaction commit whose outcome stays unknown stops the pipeline as `unclassified_failure` until an operator recovery path exists) | `halted_uncertain` | Only a read of exactly that boundary, never a later batch: at the start of a later run the sink rereads the HEAD object the incident names (`object_key`, `expected_generation` = epoch, seq and entry the update was conditioned on, `content_identity`) and resolves it as `sink_boundary_committed` (HEAD's chain holds the proposed entry at the proposed seq) or `sink_boundary_absent` (the complete, integrity-checked chain back to the conditioned generation holds another entry there, or HEAD is still at that generation). A truncated, compacted, corrupt or unreadable history leaves it open |
| `unclassified_failure` | Any other failure of a source, sink or the coordinator (cause code only) | `halted_uncertain` | `pipeline_recovered`: the restarted pipeline reaches verified running |
| `incident_overflow` | More than 63 open incidents; counts the rest by reason | | |

Conditions resolved by a verified start (`unclassified_failure`, `pg_different_cluster`, the two position reasons) are bound to a recovery generation: the same failure before the restarted pipeline reaches verified running updates the same incident; after a genuine recovery it is a new one.

Status (`GET /pipelines/{name}`, `GET /pipelines`, `/ready`) carries an `incidents` summary: `primary` (the blocking incident of a failed pipeline), `primary_final`, `blocking`, `overflow_blocking`, `durability_pending` (an incident is known but its durable write is still being retried - it is not yet auditable) and `store_unavailable`.

### List Incidents

```http
GET /pipelines/{name}/incidents
```

Every incident of the pipeline (open, acknowledged, recently resolved, and any not yet durable), with the primary incident.

### Get Incident

```http
GET /pipelines/{name}/incidents/{id}
```

**Response:** `200 OK`
```json
{
  "incident_id": "3f9c...",
  "reason_code": "unclassified_failure",
  "component": {"kind": "source", "id": "mysql"},
  "retryability": "auto_retry",
  "safety_state": "halted_uncertain",
  "cause_code": "source_connect",
  "explanation": "source mysql failed (source_connect). The pipeline stopped; this failure is not classified yet, see the logs for details.",
  "evidence": {},
  "recommended_actions": ["inspect_logs"],
  "status": {"state": "open"},
  "blocking": true,
  "occurrences": 2,
  "durable": true,
  "audit_pending": false
}
```

### Acknowledge Incident

```http
POST /pipelines/{name}/incidents/{id}/acknowledge
Content-Type: application/json

{"asserted_actor": "alice", "reason": "investigating the source outage"}
```

Moves an open incident to `acknowledged` and records the actor, the request's peer address (`origin`) and the reason in the audit log. The API has no authenticated identity yet: `asserted_actor` is recorded as caller-supplied (`actor_verified: false`). It does not change the pipeline, its source, sinks or checkpoints, and does not resolve the incident.

**Responses:** `200 OK` with the incident; `400` when a field is empty; `404` for an unknown incident; `409` when it is already resolved, or not durable yet (retry once it is).

---

## System Endpoints

### Log Level

```http
GET /log-level
```

Returns the current `RUST_LOG` value.

**Response:** `200 OK`
```json
{"level": "deltaforge=info,sources=info,sinks=info,warn"}
```

### Validate Config

```http
POST /validate
Content-Type: application/json
```

Dry-run validation of a pipeline config without creating it.

**Response:** `200 OK` — config is valid
```json
{"valid": true, "pipeline": "orders-cdc", "source_type": "mysql", "sink_count": 2}
```

**Response:** `400 Bad Request` — config has errors
```json
{"valid": false, "error": "spec: missing field `processors` at line 7 column 3"}
```

---

## Error Responses

All error responses return structured JSON:

```json
{
  "code": "PIPELINE_NOT_FOUND",
  "message": "pipeline orders-cdc not found"
}
```

| Status Code | Code | Meaning |
|-------------|------|---------|
| `400 Bad Request` | `PIPELINE_NAME_MISMATCH` | Invalid request body or name mismatch |
| `404 Not Found` | `PIPELINE_NOT_FOUND` | Resource doesn't exist |
| `409 Conflict` | `PIPELINE_ALREADY_EXISTS` | Resource already exists |
| `500 Internal Server Error` | `INTERNAL_ERROR` | Unexpected server error |
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

Returns `ok` when the process is running and all pipelines are healthy. Returns `503` if any pipeline has entered a failed state (e.g. position lost after failover, binlog purged, unrecoverable source error). Use for Kubernetes liveness probes — a `503` indicates the process should be restarted.

**Response:** `200 OK` — all pipelines healthy
```json
{"status": "healthy", "pipelines": 3}
```

**Response:** `503 Service Unavailable` — one or more pipelines failed
```json
{"status": "unhealthy", "failed_pipelines": ["orders-cdc"]}
```

### Readiness Probe

```http
GET /ready
```

Returns pipeline states. Use for Kubernetes readiness probes.

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
      "type": "bigint(20) unsigned",
      "nullable": false,
      "default": null,
      "extra": "auto_increment"
    },
    {
      "name": "customer_id",
      "type": "bigint(20)",
      "nullable": false,
      "default": null
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
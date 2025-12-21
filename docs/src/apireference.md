# REST API Reference

DeltaForge exposes a REST API for health checks, pipeline management, and schema
inspection. All endpoints return JSON.

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
GET /healthz
```

Returns `ok` if the process is running. Use for Kubernetes liveness probes.

**Response:** `200 OK`
```
ok
```

### Readiness Probe

```http
GET /readyz
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
```

Returns all pipelines with current status.

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
- `409 Conflict` — Pipeline already exists

### Update Pipeline

```http
PATCH /pipelines/{name}
Content-Type: application/json
```

Applies a partial update to an existing pipeline. The pipeline is stopped,
updated, and restarted.

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
- `404 Not Found` — Pipeline doesn't exist
- `400 Bad Request` — Name mismatch in patch

### Pause Pipeline

```http
POST /pipelines/{name}/pause
```

Pauses ingestion. Events in the buffer are not processed until resumed.

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

Resumes a paused pipeline.

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

Stops and removes a pipeline. Final checkpoint is saved.

**Response:** `200 OK`
```json
{
  "name": "orders-cdc",
  "status": "stopped",
  "spec": { ... }
}
```

---

## Schema Management

### List Schemas

```http
GET /pipelines/{name}/schemas
```

Returns all tracked schemas for a pipeline.

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
      "column_type": "int(11)",
      "data_type": "int",
      "nullable": false,
      "ordinal_position": 2,
      "default_value": null,
      "extra": null,
      "is_primary_key": false
    },
    {
      "name": "total",
      "column_type": "decimal(10,2)",
      "data_type": "decimal",
      "nullable": true,
      "ordinal_position": 3,
      "default_value": "0.00",
      "extra": null,
      "is_primary_key": false
    },
    {
      "name": "status",
      "column_type": "enum('pending','shipped','delivered')",
      "data_type": "enum",
      "nullable": false,
      "ordinal_position": 4,
      "default_value": "pending",
      "extra": null,
      "is_primary_key": false
    },
    {
      "name": "created_at",
      "column_type": "datetime",
      "data_type": "datetime",
      "nullable": false,
      "ordinal_position": 5,
      "default_value": "CURRENT_TIMESTAMP",
      "extra": "DEFAULT_GENERATED",
      "is_primary_key": false
    }
  ],
  "primary_key": ["id"],
  "engine": "InnoDB",
  "charset": null,
  "collation": "utf8mb4_0900_ai_ci",
  "fingerprint": "sha256:a1b2c3d4e5f6...",
  "registry_version": 2,
  "loaded_at": "2025-01-15T10:30:00Z"
}
```

**Errors:**
- `404 Not Found` — Pipeline doesn't exist
- `500 Internal Server Error` — Table not found or query failed

### Reload All Schemas

```http
POST /pipelines/{name}/schemas/reload
```

Force-reloads all schemas matching the pipeline's table patterns. Use after
out-of-band DDL changes.

**Response:** `200 OK`
```json
{
  "pipeline": "orders-cdc",
  "tables_reloaded": 3,
  "tables": [
    {
      "database": "shop",
      "table": "orders",
      "status": "ok",
      "changed": true,
      "error": null
    },
    {
      "database": "shop",
      "table": "customers",
      "status": "ok",
      "changed": false,
      "error": null
    },
    {
      "database": "shop",
      "table": "products",
      "status": "ok",
      "changed": true,
      "error": null
    }
  ],
  "elapsed_ms": 245
}
```

### Reload Single Table

```http
POST /pipelines/{name}/schemas/{db}/{table}/reload
```

Reloads schema for a specific table and returns the updated details.

**Response:** `200 OK`

Same format as [Get Schema Details](#get-schema-details).

### Get Schema Versions

```http
GET /pipelines/{name}/schemas/{db}/{table}/versions
```

Returns version history for a table's schema (newest first).

**Response:** `200 OK`
```json
[
  {
    "version": 2,
    "fingerprint": "sha256:a1b2c3d4e5f6...",
    "column_count": 5,
    "registered_at": "2025-01-15T10:30:00Z"
  },
  {
    "version": 1,
    "fingerprint": "sha256:9876543210ab...",
    "column_count": 4,
    "registered_at": "2025-01-10T08:00:00Z"
  }
]
```

---

## Error Responses

All errors return a plain text message with appropriate status code:

| Status | Meaning |
|--------|---------|
| `400 Bad Request` | Invalid request (e.g., name mismatch) |
| `404 Not Found` | Pipeline or table not found |
| `409 Conflict` | Resource already exists |
| `500 Internal Server Error` | Operation failed |

**Example:**
```http
HTTP/1.1 404 Not Found
Content-Type: text/plain

pipeline orders-cdc not found
```

---

## Metrics

Metrics are served on a separate port (default `:9000`):

```http
GET /metrics
```

Returns Prometheus-format metrics including:

- `deltaforge_pipelines_total` — Total pipelines created
- `deltaforge_running_pipeline` — Currently running pipelines (gauge)
- `deltaforge_sink_events_total` — Events sent to sinks
- `deltaforge_sink_batch_total` — Batches sent to sinks
- `deltaforge_sink_latency_seconds` — Sink write latency histogram
- `deltaforge_batch_events` — Events per batch histogram
- `deltaforge_batch_bytes` — Bytes per batch histogram
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

### Get Pipeline

```http
GET /pipelines/{name}
```

Returns a single pipeline by name.

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
- `404 Not Found` - Pipeline doesn't exist
- `400 Bad Request` - Name mismatch in patch

### Delete Pipeline

```http
DELETE /pipelines/{name}
```

Permanently deletes a pipeline. This removes the pipeline from the runtime
and cannot be undone.

**Response:** `204 No Content`

**Errors:**
- `404 Not Found` - Pipeline doesn't exist

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

Stops a pipeline. Final checkpoint is saved.

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

## Error Responses

All error responses follow this format:

```json
{
  "error": "Description of the error"
}
```

| Status Code | Meaning |
|-------------|---------|
| `400 Bad Request` | Invalid request body or parameters |
| `404 Not Found` | Resource doesn't exist |
| `409 Conflict` | Resource already exists |
| `500 Internal Server Error` | Unexpected server error |
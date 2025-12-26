# Example: Turso to Kafka

This example demonstrates streaming changes from a Turso database to Kafka with schema sensing enabled.

## Use Case

You have a Turso database (or local libSQL) and want to:
- Stream table changes to a Kafka topic
- Automatically detect schema structure from JSON payloads
- Transform events with JavaScript before publishing

## Pipeline Configuration

```yaml
apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: turso2kafka
  tenant: acme

spec:
  source:
    type: turso
    config:
      id: turso-main
      # Local libSQL for development
      url: "http://127.0.0.1:8080"
      # For Turso cloud:
      # url: "libsql://your-db.turso.io"
      # auth_token: "${TURSO_AUTH_TOKEN}"
      tables: ["users", "orders", "order_items"]
      poll_interval_ms: 1000
      cdc_mode: auto

  processors:
    - type: javascript
      id: enrich
      inline: |
        function processBatch(events) {
          return events.map(event => {
            // Add custom metadata to events
            event.source_type = "turso";
            event.processed_at = new Date().toISOString();
            return event;
          });
        }

  sinks:
    - type: kafka
      config:
        id: kafka-main
        brokers: "${KAFKA_BROKERS}"
        topic: turso.changes
        required: true
        exactly_once: false
        client_conf:
          message.timeout.ms: "5000"
          acks: "all"

  batch:
    max_events: 100
    max_bytes: 1048576
    max_ms: 500
    respect_source_tx: false
    max_inflight: 2

  commit_policy:
    mode: required

  schema_sensing:
    enabled: true
    deep_inspect:
      enabled: true
      max_depth: 3
    sampling:
      warmup_events: 50
      sample_rate: 5
      structure_cache: true
```

## Running the Example

### 1. Start Infrastructure

```bash
# Start Kafka and other services
./dev.sh up

# Create the target topic
./dev.sh k-create turso.changes 6
```

### 2. Start Local libSQL (Optional)

For local development without Turso cloud:

```bash
# Using sqld (libSQL server)
sqld --http-listen-addr 127.0.0.1:8080

# Or with Docker
docker run -p 8080:8080 ghcr.io/libsql/sqld:latest
```

### 3. Create Test Tables

```sql
CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT UNIQUE,
  metadata TEXT  -- JSON column
);

CREATE TABLE orders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  total REAL NOT NULL,
  status TEXT DEFAULT 'pending',
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
```

### 4. Run DeltaForge

```bash
# Save config as turso-kafka.yaml
cargo run -p runner -- --config turso-kafka.yaml
```

### 5. Insert Test Data

```sql
INSERT INTO users (name, email, metadata) 
VALUES ('Alice', 'alice@example.com', '{"role": "admin", "tags": ["vip"]}');

INSERT INTO orders (user_id, total, status) 
VALUES (1, 99.99, 'completed');
```

### 6. Verify Events in Kafka

```bash
./dev.sh k-consume turso.changes --from-beginning
```

You should see events like:

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "tenant_id": "acme",
  "table": "users",
  "op": "insert",
  "after": {
    "id": 1,
    "name": "Alice",
    "email": "alice@example.com",
    "metadata": "{\"role\": \"admin\", \"tags\": [\"vip\"]}"
  },
  "source_type": "turso",
  "processed_at": "2025-01-15T10:30:00.000Z",
  "timestamp": "2025-01-15T10:30:00.123Z"
}
```

## Monitoring

### Check Pipeline Status

```bash
curl http://localhost:8080/pipelines/turso2kafka
```

### View Inferred Schemas

```bash
# List all inferred schemas
curl http://localhost:8080/pipelines/turso2kafka/sensing/schemas

# Get details for users table
curl http://localhost:8080/pipelines/turso2kafka/sensing/schemas/users

# Export as JSON Schema
curl http://localhost:8080/pipelines/turso2kafka/sensing/schemas/users/json-schema
```

### Check Drift Detection

```bash
curl http://localhost:8080/pipelines/turso2kafka/drift
```

## Turso Cloud Configuration

For production with Turso cloud:

```yaml
source:
  type: turso
  config:
    id: turso-prod
    url: "libsql://mydb-myorg.turso.io"
    auth_token: "${TURSO_AUTH_TOKEN}"
    tables: ["*"]
    cdc_mode: native
    poll_interval_ms: 1000
    native_cdc:
      level: data
```

Set the auth token via environment variable:

```bash
export TURSO_AUTH_TOKEN="your-token-here"
```

## Notes

- **CDC Mode**: `auto` tries native CDC first, then falls back to triggers or polling
- **Poll Interval**: Lower values reduce latency but increase database load
- **Schema Sensing**: Automatically discovers JSON structure in text columns
- **Exactly Once**: Set to `false` for higher throughput; use `true` if Kafka cluster supports EOS
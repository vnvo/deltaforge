# PostgreSQL to NATS

This one streams PostgreSQL logical replication changes to NATS JetStream with CloudEvents envelope format, for serverless architectures for example.

## Overview

| Component | Configuration |
|-----------|---------------|
| **Source** | PostgreSQL logical replication |
| **Processor** | None (passthrough) |
| **Sink** | NATS JetStream |
| **Envelope** | CloudEvents 1.0 |

## Use Case

You have a PostgreSQL database and want to:
- Stream changes to NATS for event-driven microservices
- Use CloudEvents format for AWS Lambda, Azure Functions, or Knative
- Leverage JetStream for durable, replay-capable event streams

## Pipeline Configuration

```yaml
apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: users-postgres-to-nats
  tenant: acme

spec:
  source:
    type: postgres
    config:
      id: users-postgres
      dsn: ${POSTGRES_DSN}
      slot: deltaforge_users
      publication: users_pub
      tables:
        - public.users
        - public.profiles
        - public.user_sessions
      start_position: earliest

  sinks:
    - type: nats
      config:
        id: users-nats
        url: ${NATS_URL}
        subject: users.events
        stream: USERS
        envelope:
          type: cloudevents
          type_prefix: "com.acme.users"
        encoding: json
        required: true
        send_timeout_secs: 5
        batch_timeout_secs: 30

  batch:
    max_events: 500
    max_ms: 500
    respect_source_tx: true

  commit_policy:
    mode: required
```

## Prerequisites

### PostgreSQL Setup

```sql
-- Enable logical replication (postgresql.conf)
-- wal_level = logical

-- Create publication for the tables
CREATE PUBLICATION users_pub FOR TABLE users, profiles, user_sessions;

-- Verify publication
SELECT * FROM pg_publication_tables WHERE pubname = 'users_pub';
```

### NATS JetStream Setup

```bash
# Start NATS with JetStream enabled
./dev.sh up

# Create the stream
./dev.sh nats-stream-add USERS 'users.>'
```

## Running the Example

### 1. Set Environment Variables

```bash
export POSTGRES_DSN="postgres://user:password@localhost:5432/mydb"
export NATS_URL="nats://localhost:4222"
```

### 2. Start DeltaForge

```bash
cargo run -p runner -- --config postgres-nats.yaml
```

### 3. Insert Test Data

```sql
INSERT INTO users (name, email, created_at)
VALUES ('Alice', 'alice@example.com', NOW());

UPDATE users SET email = 'alice.new@example.com' WHERE name = 'Alice';
```

### 4. Verify in NATS

```bash
./dev.sh nats-sub 'users.>'
```

You should see CloudEvents formatted messages:

```json
{
  "specversion": "1.0",
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "source": "deltaforge/users-postgres/public.users",
  "type": "com.acme.users.created",
  "time": "2025-01-15T10:30:00.000Z",
  "datacontenttype": "application/json",
  "subject": "public.users",
  "data": {
    "before": null,
    "after": {
      "id": 1,
      "name": "Alice",
      "email": "alice@example.com",
      "created_at": "2025-01-15T10:30:00.000Z"
    },
    "op": "c"
  }
}
```

## Variations

### With Debezium Envelope

For compatibility with existing Debezium consumers:

```yaml
sinks:
  - type: nats
    config:
      id: users-nats
      url: ${NATS_URL}
      subject: users.events
      stream: USERS
      envelope:
        type: debezium
```

### With Authentication

```yaml
sinks:
  - type: nats
    config:
      id: users-nats
      url: ${NATS_URL}
      subject: users.events
      stream: USERS
      envelope:
        type: cloudevents
        type_prefix: "com.acme.users"
      credentials_file: /path/to/nats.creds
      # Or use username/password:
      # username: ${NATS_USER}
      # password: ${NATS_PASS}
```

### Starting from Latest

Skip existing data and only capture new changes:

```yaml
source:
  type: postgres
  config:
    id: users-postgres
    dsn: ${POSTGRES_DSN}
    slot: deltaforge_users
    publication: users_pub
    tables:
      - public.users
    start_position: latest
```

## Key Concepts Demonstrated

- **PostgreSQL Logical Replication**: Production-ready CDC with slot management
- **CloudEvents Format**: Standard envelope for cloud-native event routing
- **JetStream Durability**: Replay-capable event streams with consumer acknowledgment
- **Transaction Preservation**: `respect_source_tx: true` keeps related changes together

## Related Documentation

- [PostgreSQL Source](../sources/postgres.md) - Replication setup and configuration
- [NATS Sink](../sinks/nats.md) - JetStream configuration and authentication
- [Envelopes](../envelopes.md) - Output format options
- [Configuration Reference](../configuration.md) - Full spec documentation
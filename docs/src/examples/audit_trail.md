# Audit Trail and Compliance Logging

This example demonstrates building an immutable audit trail for compliance requirements (SOC2, HIPAA, GDPR) by capturing all database changes with full context.

## Overview

| Component | Configuration |
|-----------|---------------|
| **Source** | PostgreSQL logical replication |
| **Processor** | JavaScript audit enrichment |
| **Sink** | Kafka (durable audit log) |
| **Pattern** | Compliance-grade change tracking |

## Use Case

You need to meet compliance requirements and want to:
- Capture every change to sensitive tables with before/after values
- Add audit metadata (classification, retention, regulations)
- Redact sensitive fields while preserving change records
- Store immutable audit logs for required retention periods

## Pipeline Configuration

```yaml
apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: compliance-audit-trail
  tenant: acme

spec:
  source:
    type: postgres
    config:
      id: app-postgres
      dsn: ${POSTGRES_DSN}
      slot: deltaforge_audit
      publication: audit_pub
      tables:
        - public.users
        - public.user_profiles
        - public.payment_methods
        - public.transactions
        - public.roles
        - public.permissions
        - public.user_roles
      start_position: earliest

  processors:
    - type: javascript
      id: audit-enrichment
      inline: |
        function processBatch(events) {
          return events.map(event => {
            const table = event.table.split('.')[1];
            
            // Build audit tags (event.tags is a valid Event field)
            const auditTags = [
              'audited',
              `sensitivity:${classifySensitivity(table)}`,
              `classification:${getDataClassification(table)}`,
              `retention:${getRetentionPeriod(table)}d`
            ];
            
            // Add regulation tags
            for (const reg of getApplicableRegulations(table)) {
              auditTags.push(`regulation:${reg}`);
            }
            
            // Track changed fields for updates
            if (event.before && event.after) {
              const changed = detectChangedFields(event.before, event.after);
              for (const field of changed) {
                auditTags.push(`changed:${field}`);
              }
            }
            
            event.tags = (event.tags || []).concat(auditTags);
            
            // Redact sensitive fields in before/after
            if (event.before) {
              event.before = sanitizeRecord(event.before, table);
            }
            if (event.after) {
              event.after = sanitizeRecord(event.after, table);
            }
            
            return event;
          });
        }
        
        function classifySensitivity(table) {
          const highSensitivity = ['users', 'payment_methods', 'transactions'];
          const mediumSensitivity = ['user_profiles', 'user_roles'];
          
          if (highSensitivity.includes(table)) return 'HIGH';
          if (mediumSensitivity.includes(table)) return 'MEDIUM';
          return 'LOW';
        }
        
        function sanitizeRecord(record, table) {
          if (!record) return null;
          const sanitized = { ...record };
          
          const sensitiveFields = {
            'users': ['password_hash', 'ssn', 'tax_id'],
            'payment_methods': ['card_number', 'cvv', 'account_number'],
            'user_profiles': ['date_of_birth']
          };
          
          const fieldsToMask = sensitiveFields[table] || [];
          
          for (const field of fieldsToMask) {
            if (sanitized[field] !== undefined) {
              sanitized[`_${field}_redacted`] = true;
              sanitized[field] = '[REDACTED]';
            }
          }
          
          return sanitized;
        }
        
        function detectChangedFields(before, after) {
          const changed = [];
          const allKeys = new Set([...Object.keys(before), ...Object.keys(after)]);
          for (const key of allKeys) {
            if (JSON.stringify(before[key]) !== JSON.stringify(after[key])) {
              changed.push(key);
            }
          }
          return changed;
        }
        
        function getRetentionPeriod(table) {
          if (['transactions', 'payment_methods'].includes(table)) return 2555;
          if (['users', 'user_profiles'].includes(table)) return 2190;
          return 1095;
        }
        
        function getDataClassification(table) {
          if (['payment_methods', 'transactions'].includes(table)) return 'PCI';
          if (['users', 'user_profiles'].includes(table)) return 'PII';
          return 'INTERNAL';
        }
        
        function getApplicableRegulations(table) {
          const regs = [];
          if (['users', 'user_profiles'].includes(table)) regs.push('GDPR', 'CCPA');
          if (['payment_methods', 'transactions'].includes(table)) regs.push('PCI-DSS', 'SOX');
          return regs;
        }
      limits:
        timeout_ms: 1000
        mem_mb: 256

  sinks:
    - type: kafka
      config:
        id: audit-kafka
        brokers: ${KAFKA_BROKERS}
        topic: audit.trail.events
        envelope:
          type: debezium
        encoding: json
        required: true
        exactly_once: true
        client_conf:
          acks: "all"
          enable.idempotence: "true"
          compression.type: "gzip"

  batch:
    max_events: 500
    max_bytes: 1048576
    max_ms: 1000
    respect_source_tx: true

  commit_policy:
    mode: required
```

## JavaScript Processor Constraints

> **Important**: The JavaScript processor can only modify fields that exist on DeltaForge's `Event` struct. You can:
> - Modify `event.before` and `event.after` values (JSON objects)
> - Set `event.tags` (array of strings)
> - Filter out events (return empty array)
>
> You **cannot** add arbitrary top-level fields like `event.audit` or `event.metadata` - they will be lost during serialization.
> This limitation will be addressed soon.

This example uses `event.tags` to store audit metadata as key:value strings that downstream systems can parse.

## PostgreSQL Setup

```sql
-- Create publication for audited tables
CREATE PUBLICATION audit_pub FOR TABLE 
  public.users,
  public.user_profiles,
  public.payment_methods,
  public.transactions,
  public.roles,
  public.permissions,
  public.user_roles
WITH (publish = 'insert, update, delete');

-- Enable REPLICA IDENTITY FULL to capture before values on updates
ALTER TABLE public.users REPLICA IDENTITY FULL;
ALTER TABLE public.user_profiles REPLICA IDENTITY FULL;
ALTER TABLE public.payment_methods REPLICA IDENTITY FULL;
ALTER TABLE public.transactions REPLICA IDENTITY FULL;
```

## Sample Audit Event Output

With Debezium envelope:

```json
{
  "payload": {
    "before": {
      "id": 42,
      "email": "alice@old-domain.com",
      "name": "Alice Smith",
      "password_hash": "[REDACTED]",
      "_password_hash_redacted": true
    },
    "after": {
      "id": 42,
      "email": "alice@new-domain.com",
      "name": "Alice Smith",
      "password_hash": "[REDACTED]",
      "_password_hash_redacted": true
    },
    "source": {
      "version": "0.1.0",
      "connector": "postgres",
      "name": "app-postgres",
      "ts_ms": 1705312199123,
      "db": "app",
      "schema": "public",
      "table": "users"
    },
    "op": "u",
    "ts_ms": 1705312200000,
    "tags": [
      "audited",
      "sensitivity:HIGH",
      "classification:PII",
      "retention:2190d",
      "regulation:GDPR",
      "regulation:CCPA",
      "changed:email"
    ]
  }
}
```

## Parsing Audit Tags

Downstream consumers can parse the structured tags:

```javascript
// Parse audit tags into an object
function parseAuditTags(tags) {
  const audit = {};
  for (const tag of tags || []) {
    if (tag.includes(':')) {
      const [key, value] = tag.split(':', 2);
      if (audit[key]) {
        if (!Array.isArray(audit[key])) {
          audit[key] = [audit[key]];
        }
        audit[key].push(value);
      } else {
        audit[key] = value;
      }
    } else {
      audit[tag] = true;
    }
  }
  return audit;
}

// Example: parseAuditTags(event.tags)
// Returns: {
//   audited: true,
//   sensitivity: "HIGH",
//   classification: "PII",
//   retention: "2190d",
//   regulation: ["GDPR", "CCPA"],
//   changed: "email"
// }
```

## Kafka Topic Configuration

```bash
kafka-topics.sh --create \
  --topic audit.trail.events \
  --partitions 12 \
  --replication-factor 3 \
  --config retention.ms=189216000000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config compression.type=gzip \
  --bootstrap-server ${KAFKA_BROKERS}
```

## Running the Example

### 1. Set Environment Variables

```bash
export POSTGRES_DSN="postgres://user:password@localhost:5432/app"
export KAFKA_BROKERS="kafka1:9092,kafka2:9092,kafka3:9092"
```

### 2. Start DeltaForge

```bash
cargo run -p runner -- --config audit-trail.yaml
```

### 3. Verify Audit Events

```bash
./dev.sh k-consume audit.trail.events --from-beginning
```

## Querying Audit Data

```sql
-- Find high-sensitivity changes
SELECT * FROM audit_events 
WHERE ARRAY_CONTAINS(payload.tags, 'sensitivity:HIGH');

-- Find all email changes
SELECT * FROM audit_events
WHERE ARRAY_CONTAINS(payload.tags, 'changed:email');

-- Find PCI-regulated changes
SELECT * FROM audit_events
WHERE ARRAY_CONTAINS(payload.tags, 'regulation:PCI-DSS');
```

## Key Concepts Demonstrated

- **Full Change Capture**: Before and after values with REPLICA IDENTITY FULL
- **PII Redaction**: Sensitive fields masked, presence tracked via `_field_redacted`
- **Tag-Based Metadata**: Audit info stored in `event.tags` as parseable strings
- **Immutable Storage**: Exactly-once delivery to append-only Kafka log
- **Compliance Tagging**: Retention periods, classifications, regulations as tags

## Related Documentation

- [PostgreSQL Source](../sources/postgres.md) - Logical replication setup
- [Kafka Sink](../sinks/kafka.md) - Exactly-once and durability settings
- [Processors](../configuration.md#processors) - JavaScript processor constraints
- [Envelopes](../envelopes.md) - Output format options
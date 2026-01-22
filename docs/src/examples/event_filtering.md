# Event Filtering with JavaScript

This example demonstrates using JavaScript processors to filter and selectively drop events before they reach sinks.

## Overview

| Component | Configuration |
|-----------|---------------|
| **Source** | MySQL binlog CDC |
| **Processor** | JavaScript filter + redaction |
| **Sink** | Kafka |
| **Pattern** | Event filtering and transformation |

## Use Case

You have a MySQL database and want to:
- Filter out events from certain tables or with specific conditions
- Drop low-value events to reduce downstream load
- Redact sensitive fields conditionally

## Pipeline Configuration

```yaml
apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: filtered-orders
  tenant: acme

spec:
  source:
    type: mysql
    config:
      id: orders-mysql
      dsn: ${MYSQL_DSN}
      tables:
        - shop.orders
        - shop.order_items
        - shop.audit_log        # We'll filter most of these out

  processors:
    - type: javascript
      id: filter-and-redact
      inline: |
        function processBatch(events) {
          return events
            // 1. Filter out audit_log events except errors
            .filter(event => {
              if (event.table === 'shop.audit_log') {
                // Only keep error-level audit events
                return event.after && event.after.level === 'error';
              }
              return true;
            })
            
            // 2. Filter out soft-deleted records
            .filter(event => {
              if (event.after && event.after.deleted_at !== null) {
                return false;  // Drop soft-deleted records
              }
              return true;
            })
            
            // 3. Filter out test/staging data
            .filter(event => {
              if (event.after && event.after.email) {
                // Drop test accounts
                if (event.after.email.endsWith('@test.local')) {
                  return false;
                }
              }
              return true;
            })
            
            // 4. Transform remaining events
            .map(event => {
              // Redact PII for non-admin tables
              if (event.after) {
                if (event.after.email) {
                  event.after.email = maskEmail(event.after.email);
                }
                if (event.after.phone) {
                  event.after.phone = '[redacted]';
                }
              }
              
              // Add filter tag (tags is a valid Event field)
              event.tags = (event.tags || []).concat(['filtered']);
              
              return event;
            });
        }
        
        // helper: mask email keeping domain
        function maskEmail(email) {
          const [local, domain] = email.split('@');
          if (!domain) return '[invalid-email]';
          const masked = local.charAt(0) + '***' + local.charAt(local.length - 1);
          return masked + '@' + domain;
        }
      limits:
        timeout_ms: 1000
        mem_mb: 128

  sinks:
    - type: kafka
      config:
        id: filtered-kafka
        brokers: ${KAFKA_BROKERS}
        topic: orders.filtered
        envelope:
          type: native
        encoding: json
        required: true

  batch:
    max_events: 500
    max_ms: 500
    respect_source_tx: true

  commit_policy:
    mode: required
```

## Filter Patterns

### Drop Events (Return Empty Array Element)

```javascript
.filter(event => {
  // return false to drop the event
  if (event.table === 'internal_logs') {
    return false;
  }
  return true;
})
```

### Conditional Field-Based Filtering

```javascript
.filter(event => {
  // drop events where status is 'draft'
  if (event.after && event.after.status === 'draft') {
    return false;
  }
  return true;
})
```

### Drop by Operation Type

```javascript
.filter(event => {
  // only keep inserts and updates, drop deletes
  return event.op === 'c' || event.op === 'u';
})
```

### Sample Events (Rate Limiting)

```javascript
// keep only 10% of events (for high-volume tables)
.filter(event => {
  if (event.table === 'high_volume_table') {
    return Math.random() < 0.1;
  }
  return true;
})
```

## Running the Example

### 1. Set Environment Variables

```bash
export MYSQL_DSN="mysql://user:password@localhost:3306/shop"
export KAFKA_BROKERS="localhost:9092"
```

### 2. Start DeltaForge

```bash
cargo run -p runner -- --config filtered-orders.yaml
```

### 3. Insert Test Data

```sql
-- This will be captured and transformed
INSERT INTO shop.orders (customer_email, total, status)
VALUES ('alice@example.com', 99.99, 'pending');

-- This will be filtered out (test account)
INSERT INTO shop.orders (customer_email, total, status)
VALUES ('test@test.local', 50.00, 'pending');

-- This will be filtered out (soft-deleted)
INSERT INTO shop.orders (customer_email, total, status, deleted_at)
VALUES ('bob@example.com', 75.00, 'pending', NOW());

-- Audit log - will be filtered out (not error level)
INSERT INTO shop.audit_log (level, message)
VALUES ('info', 'User logged in');

-- Audit log - will be kept (error level)
INSERT INTO shop.audit_log (level, message)
VALUES ('error', 'Payment failed');
```

### 4. Verify Filtered Output

```bash
./dev.sh k-consume orders.filtered --from-beginning
```

You should only see:
- Alice's order (with masked email: `a***e@example.com`)
- The error-level audit log entry

## Performance Considerations

> **Tip**: Filtering early reduces downstream load. If you're filtering out 50% of events, your sinks process half the data.

```yaml
processors:
  - type: javascript
    id: filter
    inline: |
      function processBatch(events) {
        // Filter FIRST, then transform
        return events
          .filter(e => shouldKeep(e))  // Reduces array size
          .map(e => transform(e));      // Processes fewer events
      }
    limits:
      timeout_ms: 500    # Increase if filtering logic is complex
      mem_mb: 128        # Increase for large batches
```

## Key Concepts Demonstrated

- **Event Filtering**: Drop events before they reach sinks
- **Conditional Logic**: Filter based on table, operation, or field values
- **PII Redaction**: Mask sensitive data in remaining events
- **Sampling**: Rate-limit high-volume event streams

> **Processor Constraints**: JavaScript processors can only modify `event.before`, `event.after`, and `event.tags`. Arbitrary top-level fields like `event.filtered_at` would be lost during serialization.

## Related Documentation

- [Processors](../configuration.md#processors) - JavaScript processor configuration
- [Envelopes](../envelopes.md) - Output format options
- [Configuration Reference](../configuration.md) - Full spec documentation
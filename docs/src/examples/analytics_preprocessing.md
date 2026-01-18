# Real-Time Analytics Preprocessing Pipeline

This example demonstrates preparing CDC events for real-time analytics by enriching events with dimensions, metrics, and routing tags (pre-processing).

## Overview

| Component | Configuration |
|-----------|---------------|
| **Source** | MySQL binlog CDC |
| **Processor** | JavaScript analytics enrichment |
| **Sinks** | Kafka (stream processing) + Redis (real-time counters) |
| **Pattern** | Analytics-ready event preparation |

## Use Case

You have an e-commerce MySQL database and want to:
- Stream order events to real-time dashboards
- Feed a worker that maintains live counters in Redis
- Prepare events for stream processing (Flink, Spark Streaming)

## Pipeline Configuration

```yaml
apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: ecommerce-analytics
  tenant: acme

spec:
  source:
    type: mysql
    config:
      id: ecommerce-mysql
      dsn: ${MYSQL_DSN}
      tables:
        - shop.orders
        - shop.order_items
        - shop.payments
        - shop.cart_events

  processors:
    - type: javascript
      id: analytics-enrichment
      inline: |
        function processBatch(events) {
          return events.map(event => {
            const table = event.table.split('.')[1];
            const record = event.after || event.before;
            
            // Add analytics metadata to event.after
            // (we can modify event.after since it's a JSON Value)
            if (event.after) {
              event.after._analytics = {
                event_type: `${table}.${mapOperation(event.op)}`,
                dimensions: extractDimensions(table, record, event),
                metrics: extractMetrics(table, record)
              };
            }
            
            // Add routing tags
            event.tags = generateTags(table, record, event);
            
            return event;
          });
        }
        
        function mapOperation(op) {
          return { 'c': 'created', 'u': 'updated', 'd': 'deleted', 'r': 'snapshot' }[op] || op;
        }
        
        function extractDimensions(table, record, event) {
          if (!record) return {};
          
          const dims = {
            hour_of_day: new Date(event.timestamp).getUTCHours(),
            day_of_week: new Date(event.timestamp).getUTCDay()
          };
          
          switch (table) {
            case 'orders':
              dims.customer_id = record.customer_id;
              dims.status = record.status;
              dims.channel = record.channel || 'web';
              break;
            case 'order_items':
              dims.order_id = record.order_id;
              dims.product_id = record.product_id;
              break;
            case 'payments':
              dims.order_id = record.order_id;
              dims.payment_method = record.method;
              break;
          }
          
          return dims;
        }
        
        function extractMetrics(table, record) {
          if (!record) return {};
          
          const metrics = { event_count: 1 };
          
          switch (table) {
            case 'orders':
              metrics.order_total = parseFloat(record.total) || 0;
              metrics.item_count = parseInt(record.item_count) || 0;
              break;
            case 'order_items':
              metrics.quantity = parseInt(record.quantity) || 1;
              metrics.line_total = parseFloat(record.line_total) || 0;
              break;
            case 'payments':
              metrics.payment_amount = parseFloat(record.amount) || 0;
              break;
          }
          
          return metrics;
        }
        
        function generateTags(table, record, event) {
          const tags = [table, event.op];
          
          if (!record) return tags;
          
          if (table === 'orders' && record.total > 500) {
            tags.push('high_value');
          }
          if (record.status) {
            tags.push(`status:${record.status}`);
          }
          
          return tags;
        }
      limits:
        timeout_ms: 500
        mem_mb: 256

  sinks:
    - type: kafka
      config:
        id: analytics-kafka
        brokers: ${KAFKA_BROKERS}
        topic: analytics.events
        envelope:
          type: native
        encoding: json
        required: true
        client_conf:
          compression.type: "lz4"

    - type: redis
      config:
        id: realtime-redis
        uri: ${REDIS_URI}
        stream: analytics:realtime
        envelope:
          type: native
        encoding: json
        required: false

  batch:
    max_events: 500
    max_bytes: 1048576
    max_ms: 100
    respect_source_tx: false

  commit_policy:
    mode: required
```

## JavaScript Processor Constraints

> **Important**: Analytics metadata is stored in `event.after._analytics` because the processor can only modify existing Event fields (`before`, `after`, `tags`). Arbitrary top-level fields would be lost during serialization.

## Sample Event Output

```json
{
  "before": null,
  "after": {
    "id": 98765,
    "customer_id": 12345,
    "total": 299.99,
    "status": "pending",
    "channel": "mobile",
    "_analytics": {
      "event_type": "orders.created",
      "dimensions": {
        "hour_of_day": 10,
        "day_of_week": 3,
        "customer_id": 12345,
        "status": "pending",
        "channel": "mobile"
      },
      "metrics": {
        "event_count": 1,
        "order_total": 299.99,
        "item_count": 3
      }
    }
  },
  "source": {
    "connector": "mysql",
    "db": "shop",
    "table": "orders"
  },
  "op": "c",
  "ts_ms": 1705312200000,
  "tags": ["orders", "c", "status:pending"]
}
```

## Redis Counter Worker

```javascript
const Redis = require('ioredis');
const redis = new Redis(process.env.REDIS_URI);

async function processAnalyticsEvents() {
  let lastId = '0';
  
  while (true) {
    const results = await redis.xread(
      'COUNT', 100, 'BLOCK', 1000,
      'STREAMS', 'analytics:realtime', lastId
    );
    
    if (!results) continue;
    
    for (const [stream, messages] of results) {
      for (const [id, fields] of messages) {
        const event = JSON.parse(fields[1]);
        await updateCounters(event);
        lastId = id;
      }
    }
  }
}

async function updateCounters(event) {
  const pipe = redis.pipeline();
  const now = new Date();
  const hourKey = `${now.getUTCFullYear()}:${now.getUTCMonth()+1}:${now.getUTCDate()}:${now.getUTCHours()}`;
  const dayKey = `${now.getUTCFullYear()}:${now.getUTCMonth()+1}:${now.getUTCDate()}`;
  
  // Get analytics from event.after
  const analytics = event.after?._analytics || {};
  const eventType = analytics.event_type || `${event.table}.${event.op}`;
  const metrics = analytics.metrics || {};
  const dimensions = analytics.dimensions || {};
  
  // Event counts
  pipe.hincrby(`stats:events:${hourKey}`, eventType, 1);
  
  // Order-specific counters
  if (eventType === 'orders.created') {
    pipe.incr(`stats:orders:count:${hourKey}`);
    pipe.incrbyfloat(`stats:orders:revenue:${hourKey}`, metrics.order_total || 0);
    
    if (dimensions.channel) {
      pipe.hincrby(`stats:orders:channel:${dayKey}`, dimensions.channel, 1);
    }
  }
  
  // High-value alerts
  if (event.tags?.includes('high_value')) {
    pipe.lpush('alerts:high_value_orders', JSON.stringify({
      order_id: event.after?.id,
      total: metrics.order_total
    }));
    pipe.ltrim('alerts:high_value_orders', 0, 99);
  }
  
  pipe.expire(`stats:events:${hourKey}`, 90000);
  await pipe.exec();
}

processAnalyticsEvents().catch(console.error);
```

## Running the Example

### 1. Set Environment Variables

```bash
export MYSQL_DSN="mysql://user:password@localhost:3306/shop"
export KAFKA_BROKERS="localhost:9092"
export REDIS_URI="redis://localhost:6379"
```

### 2. Create Kafka Topic

```bash
./dev.sh k-create analytics.events 12
```

### 3. Start DeltaForge

```bash
cargo run -p runner -- --config analytics-pipeline.yaml
```

### 4. Start Counter Worker

```bash
node realtime-counter-worker.js
```

## Key Concepts Demonstrated

- **Event Enrichment**: Add dimensions/metrics in `event.after._analytics`
- **Tag-Based Filtering**: Use `event.tags` for high-value order detection
- **Multi-Sink Fan-Out**: Kafka for stream processing + Redis for worker consumption
- **Worker Pattern**: Separate worker consumes Redis stream to update counters

## Related Documentation

- [MySQL Source](../sources/mysql.md) - Binlog configuration
- [Kafka Sink](../sinks/kafka.md) - Producer settings
- [Redis Sink](../sinks/redis.md) - Stream configuration
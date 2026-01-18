# Redis Cache Invalidation

This example demonstrates streaming database changes to a Redis stream where a worker can consume them to invalidate cache entries, ensuring cache consistency without polling.

## Overview

| Component | Configuration |
|-----------|---------------|
| **Source** | MySQL binlog CDC |
| **Processor** | JavaScript cache key generator |
| **Sink** | Redis Streams |
| **Pattern** | CDC-driven cache invalidation |

## Use Case

You have a MySQL database with Redis caching and want to:
- Stream change events that trigger cache invalidation
- Avoid stale cache issues without TTL-based expiration
- Generate cache keys matching your application's key format

## Architecture

```
┌─────────┐     ┌─────────────┐     ┌────────────────┐     ┌─────────┐
│  MySQL  │────>│ DeltaForge  │────>│ Redis Stream   │────>│ Worker  │
│         │     │             │     │ (invalidations)│     │         │
└─────────┘     └─────────────┘     └────────────────┘     └────┬────┘
                                                                │
                                    ┌───────────────┐           │
                                    │  Redis Cache  │<──────────┘
                                    │  (DEL keys)   │
                                    └───────────────┘
```

## Pipeline Configuration

```yaml
apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: cache-invalidation
  tenant: acme

spec:
  source:
    type: mysql
    config:
      id: app-mysql
      dsn: ${MYSQL_DSN}
      tables:
        - app.users
        - app.products
        - app.orders
        - app.inventory

  processors:
    - type: javascript
      id: generate-cache-keys
      inline: |
        function processBatch(events) {
          return events.map(event => {
            const keys = generateCacheKeys(event);
            const strategy = getStrategy(event);
            
            // Store cache keys in event.after metadata
            // (we can modify event.after since it's a JSON Value)
            if (event.after) {
              event.after._cache_keys = keys;
              event.after._invalidation_strategy = strategy;
            } else if (event.before) {
              // For deletes, add to before
              event.before._cache_keys = keys;
              event.before._invalidation_strategy = strategy;
            }
            
            // Add tags for routing/filtering
            event.tags = (event.tags || []).concat([
              'cache:invalidate',
              `strategy:${strategy}`,
              `keys:${keys.length}`
            ]);
            
            return event;
          });
        }
        
        function generateCacheKeys(event) {
          const table = event.table.split('.')[1];
          const keys = [];
          const record = event.after || event.before;
          
          if (!record || !record.id) return keys;
          const id = record.id;
          
          switch (table) {
            case 'users':
              keys.push(`user:${id}`);
              if (record.email) {
                keys.push(`user:email:${record.email}`);
              }
              // If email changed, invalidate old email key
              if (event.before && event.before.email && 
                  event.before.email !== record.email) {
                keys.push(`user:email:${event.before.email}`);
              }
              keys.push(`user:${id}:orders`);
              keys.push(`user:${id}:profile`);
              break;
              
            case 'products':
              keys.push(`product:${id}`);
              keys.push(`product:${id}:details`);
              if (record.category_id) {
                keys.push(`category:${record.category_id}:products`);
              }
              break;
              
            case 'orders':
              keys.push(`order:${id}`);
              if (record.user_id) {
                keys.push(`user:${record.user_id}:orders`);
              }
              break;
              
            case 'inventory':
              keys.push(`inventory:${record.product_id}`);
              keys.push(`product:${record.product_id}:stock`);
              break;
          }
          
          return keys;
        }
        
        function getStrategy(event) {
          const table = event.table.split('.')[1];
          if (table === 'inventory') return 'immediate';
          if (event.op === 'd') return 'immediate';
          return 'batched';
        }
      limits:
        timeout_ms: 500
        mem_mb: 128

  sinks:
    - type: redis
      config:
        id: invalidation-stream
        uri: ${REDIS_URI}
        stream: cache:invalidations
        envelope:
          type: native
        encoding: json
        required: true

  batch:
    max_events: 100
    max_ms: 100
    respect_source_tx: true

  commit_policy:
    mode: required
```

## JavaScript Processor Constraints

> **Important**: The processor stores cache keys in `event.after._cache_keys` (or `event.before` for deletes) because we can only modify existing Event fields. Arbitrary top-level fields like `event.cache_keys` would be lost.

## Cache Worker (Consumer)

```javascript
// cache-invalidation-worker.js
const Redis = require('ioredis');

const redis = new Redis(process.env.REDIS_URI);
const streamKey = 'cache:invalidations';
const consumerGroup = 'cache-workers';
const consumerId = `worker-${process.pid}`;

async function processInvalidations() {
  try {
    await redis.xgroup('CREATE', streamKey, consumerGroup, '0', 'MKSTREAM');
  } catch (e) {
    if (!e.message.includes('BUSYGROUP')) throw e;
  }

  console.log(`Starting cache invalidation worker: ${consumerId}`);

  while (true) {
    try {
      const results = await redis.xreadgroup(
        'GROUP', consumerGroup, consumerId,
        'COUNT', 100, 'BLOCK', 5000,
        'STREAMS', streamKey, '>'
      );

      if (!results) continue;

      for (const [stream, messages] of results) {
        for (const [id, fields] of messages) {
          const event = JSON.parse(fields[1]);
          await invalidateKeys(event);
          await redis.xack(streamKey, consumerGroup, id);
        }
      }
    } catch (error) {
      console.error('Worker error:', error);
      await new Promise(r => setTimeout(r, 1000));
    }
  }
}

async function invalidateKeys(event) {
  // Get cache keys from the event payload
  const record = event.after || event.before;
  const cacheKeys = record?._cache_keys || [];
  
  if (cacheKeys.length === 0) return;

  for (const key of cacheKeys) {
    if (key.includes('*')) {
      await scanAndDelete(key);
    } else {
      await redis.del(key);
    }
  }

  console.log(`Invalidated ${cacheKeys.length} keys for ${event.table}`);
}

async function scanAndDelete(pattern) {
  let cursor = '0';
  do {
    const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
    cursor = nextCursor;
    if (keys.length > 0) {
      await redis.del(...keys);
    }
  } while (cursor !== '0');
}

processInvalidations().catch(console.error);
```

## Running the Example

### 1. Set Environment Variables

```bash
export MYSQL_DSN="mysql://user:password@localhost:3306/app"
export REDIS_URI="redis://localhost:6379"
```

### 2. Start DeltaForge

```bash
cargo run -p runner -- --config cache-invalidation.yaml
```

### 3. Start Worker(s)

```bash
node cache-invalidation-worker.js
```

### 4. Test Invalidation

```sql
UPDATE app.users SET email = 'alice.new@example.com' WHERE id = 1;
```

Worker output:
```
Invalidated 5 keys for app.users
```

## Key Concepts Demonstrated

- **CDC to Stream**: DeltaForge captures changes and writes to Redis Streams
- **Custom Key Generation**: Processor computes cache keys for downstream worker
- **Consumer Groups**: Scalable worker processing with acknowledgments
- **Before/After Diffing**: Compute invalidation keys for both old and new values

> **Note**: DeltaForge streams events to Redis; a separate worker (shown above) consumes them and performs the actual cache invalidation.

## Related Documentation

- [MySQL Source](../sources/mysql.md) - Binlog configuration
- [Redis Sink](../sinks/redis.md) - Stream configuration
- [Processors](../configuration.md#processors) - JavaScript processor options
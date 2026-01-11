<p align="center">
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/redis/redis-original.svg" width="80" height="80" alt="Redis">
</p>

# Redis sink

The Redis sink publishes events to a Redis Stream for real-time consumption with consumer groups.

## When to use Redis

Redis Streams shine when you need low-latency event delivery with simple operational requirements and built-in consumer group support.

### Real-world applications

| Use Case | Description |
|----------|-------------|
| **Real-time notifications** | Push database changes instantly to WebSocket servers for live UI updates |
| **Cache invalidation** | Trigger cache eviction when source records change; keep Redis cache consistent |
| **Session synchronization** | Replicate user session changes across application instances in real-time |
| **Rate limiting state** | Stream counter updates for distributed rate limiting decisions |
| **Live dashboards** | Feed real-time metrics and KPIs to dashboard backends |
| **Job queuing** | Use CDC events to trigger background job processing with consumer groups |
| **Feature flags** | Propagate feature flag changes instantly across all application instances |

### Pros and cons

| Pros | Cons |
|------|------|
| ✅ **Ultra-low latency** - Sub-millisecond publish; ideal for real-time apps | ❌ **Memory-bound** - All data in RAM; expensive for high-volume retention |
| ✅ **Simple operations** - Single binary, minimal configuration | ❌ **Limited retention** - Not designed for long-term event storage |
| ✅ **Consumer groups** - Built-in competing consumers with acknowledgements | ❌ **Durability trade-offs** - AOF/RDB persistence has limitations |
| ✅ **Familiar tooling** - redis-cli, widespread client library support | ❌ **Single-threaded** - CPU-bound for very high throughput |
| ✅ **Versatile** - Combine with caching, pub/sub, and data structures | ❌ **No native replay** - XRANGE exists but no offset management |
| ✅ **Atomic operations** - MULTI/EXEC for transactional guarantees | ❌ **Cluster complexity** - Sharding requires careful key design |

## Configuration

<table>
<tr>
<td>

```yaml
sinks:
  - type: redis
    config:
      id: orders-redis
      uri: ${REDIS_URI}
      stream: orders.events
      required: true
```

</td>
<td>

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | string | — | Sink identifier |
| `uri` | string | — | Redis connection URI |
| `stream` | string | — | Redis stream key |
| `required` | bool | `true` | Gates checkpoints |

</td>
</tr>
</table>

## Consuming events

### Consumer groups (recommended)

```bash
# Create consumer group
redis-cli XGROUP CREATE orders.events mygroup $ MKSTREAM

# Read as consumer (blocking)
redis-cli XREADGROUP GROUP mygroup consumer1 BLOCK 5000 COUNT 10 STREAMS orders.events >

# Acknowledge processing
redis-cli XACK orders.events mygroup 1234567890123-0

# Check pending (unacknowledged) messages
redis-cli XPENDING orders.events mygroup
```

### Simple subscription

```bash
# Read latest entries
redis-cli XREAD COUNT 10 STREAMS orders.events 0-0

# Block for new entries
redis-cli XREAD BLOCK 5000 STREAMS orders.events $
```

### Go consumer example

```go
import "github.com/redis/go-redis/v9"

rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

// Create consumer group (once)
rdb.XGroupCreateMkStream(ctx, "orders.events", "mygroup", "0")

for {
    streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
        Group:    "mygroup",
        Consumer: "worker1",
        Streams:  []string{"orders.events", ">"},
        Count:    10,
        Block:    5 * time.Second,
    }).Result()
    
    if err != nil {
        continue
    }
    
    for _, stream := range streams {
        for _, msg := range stream.Messages {
            var event Event
            json.Unmarshal([]byte(msg.Values["event"].(string)), &event)
            process(event)
            rdb.XAck(ctx, "orders.events", "mygroup", msg.ID)
        }
    }
}
```

### Rust consumer example

```rust
use redis::AsyncCommands;

let client = redis::Client::open("redis://localhost:6379")?;
let mut con = client.get_async_connection().await?;

// Create consumer group
let _: () = redis::cmd("XGROUP")
    .arg("CREATE").arg("orders.events").arg("mygroup").arg("0").arg("MKSTREAM")
    .query_async(&mut con).await.unwrap_or(());

loop {
    let results: Vec<StreamReadReply> = con.xread_options(
        &["orders.events"],
        &[">"],
        &StreamReadOptions::default()
            .group("mygroup", "worker1")
            .count(10)
            .block(5000)
    ).await?;
    
    for stream in results {
        for msg in stream.ids {
            let event: Event = serde_json::from_str(&msg.map["event"])?;
            process(event);
            con.xack("orders.events", "mygroup", &[&msg.id]).await?;
        }
    }
}
```

### Python consumer example

```python
import redis
import json

r = redis.Redis.from_url("redis://localhost:6379")

# Create consumer group (once)
try:
    r.xgroup_create("orders.events", "mygroup", id="0", mkstream=True)
except redis.ResponseError:
    pass  # Group already exists

# Consume events
while True:
    events = r.xreadgroup("mygroup", "worker1", {"orders.events": ">"}, count=10, block=5000)
    for stream, messages in events:
        for msg_id, data in messages:
            event = json.loads(data[b"event"])
            process(event)
            r.xack("orders.events", "mygroup", msg_id)
```

## Failure modes

| Failure | Symptoms | DeltaForge behavior | Resolution |
|---------|----------|---------------------|------------|
| **Server unavailable** | Connection refused | Retries with backoff; blocks checkpoint | Restore Redis; check network |
| **Authentication failure** | `NOAUTH` / `WRONGPASS` | Fails fast, no retry | Fix auth details in URI |
| **OOM (Out of Memory)** | `OOM command not allowed` | Fails batch; retries | Increase `maxmemory`; enable eviction or trim streams |
| **Stream doesn't exist** | Auto-created by XADD | No failure | N/A (XADD creates stream) |
| **Connection timeout** | Command hangs | Timeout after configured duration | Check network; increase timeout |
| **Cluster MOVED/ASK** | Redirect errors | Automatic redirect (if cluster mode) | Ensure cluster client configured |
| **Replication lag** | Writes to replica fail | Fails with `READONLY` | Write to master only |
| **Max stream length** | If MAXLEN enforced | Oldest entries trimmed | Expected behavior; not a failure |
| **Network partition** | Intermittent timeouts | Retries; may have gaps | Restore network |

### Failure scenarios and data guarantees

**Redis OOM during batch delivery**

1. DeltaForge sends batch of 100 events via pipeline
2. 50 events written, Redis hits maxmemory
3. Pipeline fails atomically (all or nothing per pipeline)
4. DeltaForge retries entire batch
5. If OOM persists: batch blocked until memory available
6. Checkpoint only saved after ALL events acknowledged

**DeltaForge crash after XADD, before checkpoint**

1. Batch written to Redis stream successfully
2. DeltaForge crashes before saving checkpoint
3. On restart: replays from last checkpoint
4. Result: Duplicate events in stream (at-least-once)
5. Consumer must handle idempotently (check event.id)

**Redis failover (Sentinel/Cluster)**

1. Master fails, Sentinel promotes replica
2. In-flight XADD may fail with connection error
3. DeltaForge reconnects to new master
4. Retries failed batch
5. Possible duplicates if original write succeeded

### Handling duplicates in consumers

```python
# Idempotent consumer using event ID
processed_ids = set()  # Or use Redis SET for distributed dedup

for msg_id, data in messages:
    event = json.loads(data[b"event"])
    event_id = event["id"]
    
    if event_id in processed_ids:
        r.xack("orders.events", "mygroup", msg_id)
        continue  # Skip duplicate
    
    process(event)
    processed_ids.add(event_id)
    r.xack("orders.events", "mygroup", msg_id)
```

## Monitoring

DeltaForge exposes these metrics for Redis sink monitoring:

```yaml
# DeltaForge sink metrics (exposed at /metrics on port 9000)
deltaforge_sink_events_total{pipeline,sink}      # Events delivered
deltaforge_sink_batch_total{pipeline,sink}       # Batches delivered  
deltaforge_sink_latency_seconds{pipeline,sink}   # Delivery latency histogram
deltaforge_stage_latency_seconds{pipeline,stage="sink"}  # Stage timing
```

For Redis server visibility, use Redis's built-in monitoring:

```bash
# Monitor commands in real-time
redis-cli MONITOR

# Get server stats
redis-cli INFO stats

# Check memory usage
redis-cli INFO memory

# Stream-specific info
redis-cli XINFO STREAM orders.events
redis-cli XINFO GROUPS orders.events
```

## Stream management

```bash
# Check stream length
redis-cli XLEN orders.events

# Trim to last 10000 entries (approximate)
redis-cli XTRIM orders.events MAXLEN ~ 10000

# Trim to exact length
redis-cli XTRIM orders.events MAXLEN 10000

# View consumer group info
redis-cli XINFO GROUPS orders.events

# Check pending messages
redis-cli XPENDING orders.events mygroup

# Claim stuck messages (after 60 seconds)
redis-cli XCLAIM orders.events mygroup worker2 60000 <message-id>

# Delete processed messages (careful!)
redis-cli XDEL orders.events <message-id>
```

## Notes

- Redis Streams provide at-least-once delivery with consumer group acknowledgements
- Use `MAXLEN ~` trimming to prevent unbounded memory growth (approximate is faster)
- Consider Redis Cluster for horizontal scaling with multiple streams
- Combine with Redis pub/sub for fan-out to ephemeral subscribers
- For durability, enable AOF persistence with `appendfsync everysec` or `always`
- Monitor memory usage closely; Redis will reject writes when `maxmemory` is reached
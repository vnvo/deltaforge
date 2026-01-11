<p align="center">
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/nats/nats-original.svg" width="80" height="80" alt="NATS">
</p>

# NATS sink

The NATS sink publishes events to a NATS JetStream stream for durable, at-least-once delivery.

## When to use NATS

NATS JetStream is ideal when you need a lightweight, high-performance messaging system with persistence, without the operational overhead of Kafka.

### Real-world applications

| Use Case | Description |
|----------|-------------|
| **Edge computing** | Lightweight footprint perfect for IoT gateways and edge nodes syncing to cloud |
| **Microservices mesh** | Request-reply and pub/sub patterns with automatic load balancing |
| **Multi-cloud sync** | Leaf nodes and superclusters for seamless cross-cloud data replication |
| **Kubernetes-native events** | NATS Operator for cloud-native deployment; sidecar-friendly architecture |
| **Real-time gaming** | Low-latency state synchronization for multiplayer game servers |
| **Financial data feeds** | Stream market data with subject-based routing and wildcards |
| **Command and control** | Distribute configuration changes and commands to distributed systems |

### Pros and cons

| Pros | Cons |
|------|------|
| ✅ **Lightweight** - Single binary ~20MB; minimal resource footprint | ❌ **Smaller ecosystem** - Fewer connectors and integrations than Kafka |
| ✅ **Simple operations** - Zero external dependencies; easy clustering | ❌ **Younger persistence** - JetStream newer than Kafka's battle-tested log |
| ✅ **Low latency** - Sub-millisecond message delivery | ❌ **Community size** - Smaller community than Kafka or Redis |
| ✅ **Flexible patterns** - Pub/sub, queues, request-reply, streams | ❌ **Tooling maturity** - Fewer monitoring and management tools |
| ✅ **Subject hierarchy** - Powerful wildcard routing (`orders.>`, `*.events`) | ❌ **Learning curve** - JetStream concepts differ from traditional queues |
| ✅ **Multi-tenancy** - Built-in accounts and security isolation | ❌ **Less enterprise adoption** - Fewer case studies at massive scale |
| ✅ **Cloud-native** - Designed for Kubernetes and distributed systems | |

## Configuration

<table>
<tr>
<td>

```yaml
sinks:
  - type: nats
    config:
      id: orders-nats
      url: ${NATS_URL}
      subject: orders.events
      stream: ORDERS
      required: true
      send_timeout_secs: 5
      batch_timeout_secs: 30
```

</td>
<td>

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | string | — | Sink identifier |
| `url` | string | — | NATS server URL |
| `subject` | string | — | Subject to publish to |
| `stream` | string | — | JetStream stream name |
| `required` | bool | `true` | Gates checkpoints |
| `send_timeout_secs` | int | `5` | Publish timeout |
| `batch_timeout_secs` | int | `30` | Batch timeout |
| `connect_timeout_secs` | int | `10` | Connection timeout |

</td>
</tr>
</table>

### Authentication options

```yaml
# Credentials file
credentials_file: /etc/nats/creds/user.creds

# Username/password
username: ${NATS_USER}
password: ${NATS_PASSWORD}

# Token
token: ${NATS_TOKEN}
```

## JetStream setup

Before using the NATS sink with JetStream, create a stream that captures your subject:

```bash
# Using NATS CLI
nats stream add ORDERS \
  --subjects "orders.>" \
  --retention limits \
  --storage file \
  --replicas 3 \
  --max-age 7d

# Verify stream
nats stream info ORDERS
```

## Consuming events

### NATS CLI

```bash
# Subscribe to subject (ephemeral)
nats sub "orders.>"

# Create durable consumer
nats consumer add ORDERS orders-processor \
  --pull \
  --ack explicit \
  --deliver all \
  --max-deliver 3 \
  --filter "orders.events"

# Consume messages
nats consumer next ORDERS orders-processor --count 10
```

### Go consumer example

```go
nc, _ := nats.Connect("nats://localhost:4222")
js, _ := nc.JetStream()

// Create or bind to consumer
sub, _ := js.PullSubscribe("orders.events", "orders-processor",
    nats.Durable("orders-processor"),
    nats.AckExplicit(),
)

for {
    msgs, _ := sub.Fetch(10, nats.MaxWait(5*time.Second))
    for _, msg := range msgs {
        var event Event
        json.Unmarshal(msg.Data, &event)
        process(event)
        msg.Ack()
    }
}
```

### Rust consumer example

```rust
use async_nats::jetstream;

let client = async_nats::connect("nats://localhost:4222").await?;
let js = jetstream::new(client);

let stream = js.get_stream("ORDERS").await?;
let consumer = stream.get_consumer("orders-processor").await?;

let mut messages = consumer.messages().await?;
while let Some(msg) = messages.next().await {
    let msg = msg?;
    let event: Event = serde_json::from_slice(&msg.payload)?;
    process(event);
    msg.ack().await?;
}
```

## Monitoring

DeltaForge exposes these metrics for NATS sink monitoring:

```yaml
# DeltaForge sink metrics (exposed at /metrics on port 9000)
deltaforge_sink_events_total{pipeline,sink}      # Events delivered
deltaforge_sink_batch_total{pipeline,sink}       # Batches delivered
deltaforge_sink_latency_seconds{pipeline,sink}   # Delivery latency histogram
deltaforge_stage_latency_seconds{pipeline,stage="sink"}  # Stage timing
```

For NATS server visibility, use the NATS CLI or monitoring endpoint:

```bash
# Server info
nats server info

# JetStream account info
nats account info

# Stream statistics
nats stream info ORDERS

# Consumer statistics  
nats consumer info ORDERS orders-processor

# Real-time event monitoring
nats events
```

NATS also exposes a monitoring endpoint (default `:8222`) with JSON stats:
- `http://localhost:8222/varz` - General server stats
- `http://localhost:8222/jsz` - JetStream stats
- `http://localhost:8222/connz` - Connection stats

## Subject design patterns

| Pattern | Example | Use Case |
|---------|---------|----------|
| Hierarchical | `orders.us.created` | Regional routing |
| Wildcard single | `orders.*.created` | Any region, specific event |
| Wildcard multi | `orders.>` | All order events |
| Versioned | `v1.orders.events` | API versioning |

## Failure modes

| Failure | Symptoms | DeltaForge behavior | Resolution |
|---------|----------|---------------------|------------|
| **Server unavailable** | Connection refused | Retries with backoff; blocks checkpoint | Restore NATS; check network |
| **Stream not found** | `stream not found` error | Fails batch; no retry | Create stream or remove `stream` config |
| **Authentication failure** | `authorization violation` | Fails fast, no retry | Fix credentials |
| **Subject mismatch** | `no responders` (core NATS) | Fails if no subscribers | Add subscribers or use JetStream |
| **JetStream disabled** | `jetstream not enabled` | Fails fast | Enable JetStream on server |
| **Storage full** | `insufficient resources` | Retries; eventually fails | Add storage; adjust retention |
| **Message too large** | `message size exceeds maximum` | Fails message permanently | Increase `max_payload` or filter large events |
| **Cluster partition** | Intermittent failures | Retries with backoff | Restore network; wait for quorum |
| **Slow consumer** | Publish backpressure | Slows down; may timeout | Scale consumers; increase buffer |
| **TLS errors** | Handshake failures | Fails fast | Fix certificates |

### Failure scenarios and data guarantees

**NATS server restart during batch delivery**

1. DeltaForge sends batch of 100 events
2. 50 events published, server restarts
3. async_nats detects disconnect, starts reconnecting
4. After reconnect, DeltaForge retries remaining 50
5. JetStream deduplication prevents duplicates (if enabled)
6. Checkpoint only saved after ALL events acknowledged

**DeltaForge crash after JetStream ack, before checkpoint**

1. Batch published to JetStream successfully
2. DeltaForge crashes before saving checkpoint
3. On restart: replays from last checkpoint
4. Result: Duplicate events in stream (at-least-once)
5. Consumer must handle idempotently (check event.id)

**Stream storage exhausted**

1. JetStream stream hits max_bytes or max_msgs limit
2. With `discard: old` → oldest messages removed, publish succeeds
3. With `discard: new` → publish rejected
4. DeltaForge retries on rejection
5. Resolution: Increase limits or enable `discard: old`

### JetStream acknowledgement levels

```yaml
# Stream configuration affects durability
nats stream add ORDERS \
  --replicas 3 \           # R=3 for production
  --retention limits \     # or 'workqueue' for single consumer
  --discard old \          # Remove oldest when full
  --max-age 7d \           # Auto-expire after 7 days
  --storage file           # Persistent (vs memory)
```

| Replicas | Guarantee | Use Case |
|----------|-----------|----------|
| R=1 | Single node; lost if node fails | Development, non-critical |
| R=3 | Survives 1 node failure | Production default |
| R=5 | Survives 2 node failures | Critical data |

### Handling duplicates in consumers

```go
// Use event ID for idempotency
processedIDs := make(map[string]bool)  // Or use Redis/DB

for _, msg := range msgs {
    var event Event
    json.Unmarshal(msg.Data, &event)
    
    if processedIDs[event.ID] {
        msg.Ack()  // Already processed
        continue
    }
    
    if err := process(event); err == nil {
        processedIDs[event.ID] = true
    }
    msg.Ack()
}
```

## Notes

- When `stream` is specified, the sink verifies the stream exists at connection time
- Without `stream`, events are published to core NATS (no persistence guarantees)
- Connection pooling ensures efficient reuse across batches
- Use replicated streams (`--replicas 3`) for production durability
- Combine with other sinks to fan out data; use commit policy to control checkpoint behavior
- JetStream provides exactly-once semantics when combined with message deduplication
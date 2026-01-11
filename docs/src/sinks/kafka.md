<p align="center">
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/apachekafka/apachekafka-original.svg" width="80" height="80" alt="Kafka">
</p>

# Kafka sink

The Kafka sink publishes batches to a Kafka topic using `rdkafka`.

## When to use Kafka

Kafka excels as the backbone for event-driven architectures where durability, ordering, and replay capabilities are critical.

### Real-world applications

| Use Case | Description |
|----------|-------------|
| **Event sourcing** | Store all state changes as an immutable log; rebuild application state by replaying events |
| **Microservices integration** | Decouple services with async messaging; each service consumes relevant topics |
| **Real-time analytics pipelines** | Feed CDC events to Spark, Flink, or ksqlDB for streaming transformations |
| **Data lake ingestion** | Stream database changes to S3/HDFS via Kafka Connect for analytics and ML |
| **Audit logging** | Capture every database mutation for compliance, debugging, and forensics |
| **Cross-datacenter replication** | Use MirrorMaker 2 to replicate topics across regions for DR |

### Pros and cons

| Pros | Cons |
|------|------|
| ✅ **Durability** - Configurable replication ensures no data loss | ❌ **Operational complexity** - Requires ZooKeeper/KRaft, careful tuning |
| ✅ **Ordering guarantees** - Per-partition ordering with consumer groups | ❌ **Latency** - Batching and replication add milliseconds of delay |
| ✅ **Replay capability** - Configurable retention allows reprocessing | ❌ **Resource intensive** - High disk I/O and memory requirements |
| ✅ **Ecosystem** - Connect, Streams, Schema Registry, ksqlDB | ❌ **Learning curve** - Partitioning, offsets, consumer groups to master |
| ✅ **Throughput** - Handles millions of messages per second | ❌ **Cold start** - Cluster setup and topic configuration overhead |
| ✅ **Exactly-once semantics** - Transactions for critical workloads | ❌ **Cost** - Managed services can be expensive at scale |

## Configuration

<table>
<tr>
<td>

```yaml
sinks:
  - type: kafka
    config:
      id: orders-kafka
      brokers: ${KAFKA_BROKERS}
      topic: orders
      required: true
      exactly_once: false
      client_conf:
        message.timeout.ms: "5000"
        acks: "all"
```

</td>
<td>

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | string | — | Sink identifier |
| `brokers` | string | — | Comma-separated broker list |
| `topic` | string | — | Destination topic |
| `required` | bool | `true` | Gates checkpoints |
| `exactly_once` | bool | `false` | Enable EOS semantics |
| `client_conf` | map | `{}` | librdkafka overrides |

</td>
</tr>
</table>

## Recommended client_conf settings

| Setting | Recommended | Description |
|---------|-------------|-------------|
| `acks` | `all` | Wait for all replicas for durability |
| `message.timeout.ms` | `30000` | Total time to deliver a message |
| `retries` | `2147483647` | Retry indefinitely (with backoff) |
| `enable.idempotence` | `true` | Prevent duplicates on retry |
| `compression.type` | `lz4` | Balance between CPU and bandwidth |

## Consuming events

### Kafka CLI

```bash
# Consume from beginning
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning

# Consume with consumer group
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group deltaforge-consumers
```

### Go consumer example

```go
config := sarama.NewConfig()
config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
config.Consumer.Offsets.Initial = sarama.OffsetOldest

group, _ := sarama.NewConsumerGroup([]string{"localhost:9092"}, "my-group", config)

for {
    err := group.Consume(ctx, []string{"orders"}, &handler{})
    if err != nil {
        log.Printf("Consumer error: %v", err)
    }
}

type handler struct{}

func (h *handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
    for msg := range claim.Messages() {
        var event Event
        json.Unmarshal(msg.Value, &event)
        process(event)
        session.MarkMessage(msg, "")
    }
    return nil
}
```

### Rust consumer example

```rust
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;

let consumer: StreamConsumer = ClientConfig::new()
    .set("bootstrap.servers", "localhost:9092")
    .set("group.id", "my-group")
    .set("auto.offset.reset", "earliest")
    .create()?;

consumer.subscribe(&["orders"])?;

loop {
    match consumer.recv().await {
        Ok(msg) => {
            let payload = msg.payload_view::<str>().unwrap()?;
            let event: Event = serde_json::from_str(payload)?;
            process(event);
            consumer.commit_message(&msg, CommitMode::Async)?;
        }
        Err(e) => eprintln!("Kafka error: {}", e),
    }
}
```

### Python consumer example

```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers=['localhost:9092'],
    group_id='my-group',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    event = message.value
    process(event)
    consumer.commit()
```

## Failure modes

| Failure | Symptoms | DeltaForge behavior | Resolution |
|---------|----------|---------------------|------------|
| **Broker unavailable** | Connection refused, timeout | Retries with backoff; blocks checkpoint | Restore broker; check network |
| **Topic not found** | `UnknownTopicOrPartition` | Fails batch; retries | Create topic or enable auto-create |
| **Authentication failure** | `SaslAuthenticationFailed` | Fails fast, no retry | Fix credentials in config |
| **Authorization failure** | `TopicAuthorizationFailed` | Fails fast, no retry | Grant ACLs for producer |
| **Message too large** | `MessageSizeTooLarge` | Fails message permanently | Increase `message.max.bytes` or filter large events |
| **Leader election** | `NotLeaderForPartition` | Automatic retry after metadata refresh | Wait for election; usually transient |
| **Disk full** | `KafkaStorageException` | Retries indefinitely | Add disk space; purge old segments |
| **Network partition** | Timeouts, partial failures | Retries; may produce duplicates | Restore network; idempotence prevents dups |
| **SSL/TLS errors** | Handshake failures | Fails fast | Fix certificates, verify truststore |

### Failure scenarios and data guarantees

**Broker failure during batch delivery**

1. DeltaForge sends batch of 100 events
2. 50 events delivered, broker crashes
3. rdkafka detects failure, retries remaining 50
4. If idempotence enabled: no duplicates
5. If not: possible duplicates of events near failure point
6. Checkpoint only saved after ALL events acknowledged

**DeltaForge crash after Kafka ack, before checkpoint**

1. Batch delivered to Kafka successfully
2. DeltaForge crashes before saving checkpoint
3. On restart: replays from last checkpoint
4. Result: Duplicate events in Kafka (at-least-once)
5. Consumer must handle idempotently

### Monitoring recommendations

DeltaForge exposes these metrics for Kafka sink monitoring:

```yaml
# DeltaForge sink metrics (exposed at /metrics on port 9000)
deltaforge_sink_events_total{pipeline,sink}      # Events delivered
deltaforge_sink_batch_total{pipeline,sink}       # Batches delivered
deltaforge_sink_latency_seconds{pipeline,sink}   # Delivery latency histogram
deltaforge_stage_latency_seconds{pipeline,stage="sink"}  # Stage timing
```

For deeper Kafka broker visibility, monitor your Kafka cluster directly:
- Broker metrics via JMX or Kafka's built-in metrics
- Consumer lag via `kafka-consumer-groups.sh`
- Topic throughput via broker dashboards

> **Note**: Internal `rdkafka` producer statistics (message queues, broker RTT, etc.) are not currently exposed by DeltaForge. This is a potential future enhancement.

## Notes

- Combine Kafka with other sinks to fan out data; use commit policy to control checkpoint behavior
- For exactly-once semantics, ensure your Kafka cluster supports transactions (2.5+)
- Adjust `client_conf` for durability (`acks=all`) or performance based on your requirements
- Consider partitioning strategy for ordering guarantees within partitions
- Enable `enable.idempotence=true` to prevent duplicates during retries
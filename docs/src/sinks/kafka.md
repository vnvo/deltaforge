# Kafka sink

The Kafka sink publishes batches to a Kafka topic using `rdkafka`.

## Configuration

- `id` (string): logical identifier for metrics and logging.
- `brokers` (string): comma-separated broker list.
- `topic` (string): destination topic.
- `required` (bool, default `true`): whether acknowledgements from this sink gate checkpoints.
- `exactly_once` (bool, default `false`): enable EOS semantics when supported by the cluster.
- `client_conf` (map, optional): raw `librdkafka` configuration overrides.

### Example
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
```

## Notes

- Combine Kafka with other sinks to fan out data. Use the commit policy to decide whether non-critical sinks should block checkpoints.
- Adjust `client_conf` for durability (for example, `acks=all`) or performance based on your cluster.

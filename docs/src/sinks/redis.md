# Redis sink

The Redis sink writes each batch as entries in a Redis stream.

## Configuration

- `id` (string): logical identifier for metrics and logging.
- `uri` (string): Redis connection URI.
- `stream` (string): Redis stream key to append events to.
- `required` (bool, default `true`): whether acknowledgements from this sink gate checkpoints.

### Example
```yaml
sinks:
  - type: redis
    config:
      id: orders-redis
      uri: ${REDIS_URI}
      stream: orders
      required: true
```

## Notes

- Redis streams preserve event order per pipeline and are well-suited for lightweight consumers.
- Combine with other sinks (for example, Kafka) by marking only the critical ones as `required` if you want checkpoints to proceed without waiting for every sink.

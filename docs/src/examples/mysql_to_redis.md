# MySQL to Redis

This example streams MySQL binlog events into a Redis stream with an inline JavaScript transformation.

```yaml
metadata:
  name: orders-mysql-to-redis
  tenant: acme
spec:
  source:
    type: mysql
    config:
      id: orders-mysql
      dsn: ${MYSQL_DSN}
      tables:
        - shop.orders
  processors:
    - type: javascript
      id: redact-email
      inline: |
        function process(batch) {
          return batch.map((event) => {
            if (event.after && event.after.email) {
              event.after.email = "[redacted]";
            }
            return event;
          });
        }
      limits:
        timeout_ms: 500
  sinks:
    - type: redis
      config:
        id: orders-redis
        uri: ${REDIS_URI}
        stream: orders
        required: true
  batch:
    max_events: 500
    max_bytes: 1048576
    max_ms: 1000
  commit_policy:
    mode: required
```

Feel free to add a Kafka sink alongside Redis. Mark only the critical sink as `required` if you want checkpoints to proceed when optional sinks are unavailable.
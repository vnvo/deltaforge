# Production Kafka Configuration

This example demonstrates a production-ready Kafka sink configuration with authentication, high availability settings, and some  performance tuning.

## Overview

| Component | Configuration |
|-----------|---------------|
| **Source** | PostgreSQL logical replication |
| **Processor** | None |
| **Sink** | Kafka with SASL/SSL authentication |
| **Pattern** | Production-grade reliability |

## Use Case

You're deploying DeltaForge to production and need:
- Secure authentication (SASL/SCRAM or mTLS)
- High availability with proper acknowledgment settings
- Optimal batching and compression for throughput
- Exactly-once semantics for critical data

## Pipeline Configuration

```yaml
apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: orders-to-kafka-prod
  tenant: acme

spec:
  source:
    type: postgres
    config:
      id: orders-postgres
      dsn: ${POSTGRES_DSN}
      slot: deltaforge_orders
      publication: orders_pub
      tables:
        - public.orders
        - public.order_items
        - public.payments
      start_position: earliest

  sinks:
    - type: kafka
      config:
        id: orders-kafka
        brokers: ${KAFKA_BROKERS}
        topic: orders.cdc.events
        envelope:
          type: debezium
        encoding: json
        required: true
        
        # enable exactly-once semantics
        exactly_once: true
        
        # timeout for individual sends
        send_timeout_secs: 60
        
        # librdkafka client configuration
        client_conf:
          # security - SASL/SCRAM authentication
          security.protocol: "SASL_SSL"
          sasl.mechanism: "SCRAM-SHA-512"
          sasl.username: "${KAFKA_USERNAME}"
          sasl.password: "${KAFKA_PASSWORD}"
          
          # SSL/TLS configuration
          ssl.ca.location: "/etc/ssl/certs/kafka-ca.pem"
          ssl.endpoint.identification.algorithm: "https"
          
          # reliability - wait for all replicas
          acks: "all"
          
          # idempotence (required for exactly-once)
          enable.idempotence: "true"
          
          # retries and timeouts
          retries: "2147483647"
          retry.backoff.ms: "100"
          delivery.timeout.ms: "300000"
          request.timeout.ms: "30000"
          
          # kafka batching for throughput
          batch.size: "65536"
          linger.ms: "10"
          
          # compression
          compression.type: "lz4"
          
          # buffer management
          queue.buffering.max.messages: "100000"
          queue.buffering.max.kbytes: "1048576"

  batch:
    max_events: 1000
    max_bytes: 1048576
    max_ms: 100
    respect_source_tx: true
    max_inflight: 4

  commit_policy:
    mode: required
```

## Security Configurations

### SASL/SCRAM (Username/Password)

```yaml
client_conf:
  security.protocol: "SASL_SSL"
  sasl.mechanism: "SCRAM-SHA-512"
  sasl.username: "${KAFKA_USERNAME}"
  sasl.password: "${KAFKA_PASSWORD}"
  ssl.ca.location: "/etc/ssl/certs/ca.pem"
```

### mTLS (Mutual TLS)

```yaml
client_conf:
  security.protocol: "SSL"
  ssl.ca.location: "/etc/ssl/certs/kafka-ca.pem"
  ssl.certificate.location: "/etc/ssl/certs/client.pem"
  ssl.key.location: "/etc/ssl/private/client.key"
  ssl.key.password: "${SSL_KEY_PASSWORD}"
```

### AWS MSK with IAM

```yaml
client_conf:
  security.protocol: "SASL_SSL"
  sasl.mechanism: "AWS_MSK_IAM"
  sasl.jaas.config: "software.amazon.msk.auth.iam.IAMLoginModule required;"
  sasl.client.callback.handler.class: "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
```

### Confluent Cloud

```yaml
client_conf:
  security.protocol: "SASL_SSL"
  sasl.mechanism: "PLAIN"
  sasl.username: "${CONFLUENT_API_KEY}"
  sasl.password: "${CONFLUENT_API_SECRET}"
```

## Performance Tuning

### High Throughput

Optimize for maximum events per second:

```yaml
client_conf:
  acks: "1"                    # Leader-only ack (faster, less safe)
  batch.size: "131072"         # 128KB batches
  linger.ms: "50"              # Wait longer to fill batches
  compression.type: "lz4"      # Fast compression
  
batch:
  max_events: 5000             # Larger batches
  max_ms: 200                  # More time to accumulate
  max_inflight: 8              # More concurrent requests
```

### Low Latency

Optimize for minimal delay:

```yaml
client_conf:
  acks: "1"                    # Don't wait for replicas
  batch.size: "16384"          # Smaller batches
  linger.ms: "0"               # Send immediately
  compression.type: "none"     # Skip compression
  
batch:
  max_events: 100              # Smaller batches
  max_ms: 10                   # Flush quickly
  max_inflight: 2              # Limit in-flight
```

### Maximum Durability

Optimize for zero data loss:

```yaml
client_conf:
  acks: "all"                  # All replicas must ack
  enable.idempotence: "true"   # Prevent duplicates
  max.in.flight.requests.per.connection: "5"  # Required for idempotence
  retries: "2147483647"        # Infinite retries
  
exactly_once: true             # Transactional producer

batch:
  respect_source_tx: true      # Preserve source transactions
  max_inflight: 1              # Strict ordering
```

## Running the Example

### 1. Set Environment Variables

```bash
export POSTGRES_DSN="postgres://user:password@localhost:5432/orders"
export KAFKA_BROKERS="kafka1:9093,kafka2:9093,kafka3:9093"
export KAFKA_USERNAME="deltaforge"
export KAFKA_PASSWORD="secret"
```

### 2. Create Kafka Topic

```bash
kafka-topics.sh --create \
  --topic orders.cdc.events \
  --partitions 12 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --bootstrap-server ${KAFKA_BROKERS}
```

### 3. Start DeltaForge

```bash
cargo run -p runner --release -- --config kafka-prod.yaml
```

## Monitoring

### Key Metrics to Watch

- `deltaforge_sink_events_sent_total` — Events delivered
- `deltaforge_sink_send_latency_seconds` — Delivery latency
- `deltaforge_sink_errors_total` — Delivery failures
- `deltaforge_checkpoint_lag_events` — Events pending checkpoint

### Health Check

```bash
curl http://localhost:8080/health
```

### Pipeline Status

```bash
curl http://localhost:8080/pipelines/orders-to-kafka-prod
```

## Key Concepts Demonstrated

- **SASL/SSL Authentication**: Secure broker connections
- **Exactly-Once Semantics**: Transactional producer for no duplicates
- **Acknowledgment Modes**: Trade-off between durability and latency
- **Batching & Compression**: Optimize throughput
- **Production Tuning**: Real-world configuration patterns

## Related Documentation

- [Kafka Sink](../sinks/kafka.md) - Full configuration reference
- [Envelopes](../envelopes.md) - Output format options
- [Observability](../observability.md) - Metrics and monitoring
- [Configuration Reference](../configuration.md) - Full spec documentation
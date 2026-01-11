# Sinks

Sinks receive batches from the coordinator after processors run. Each sink lives under `spec.sinks` and can be marked as required or best-effort via the `required` flag. Checkpoint behavior is governed by the pipeline's commit policy.

Current built-in sinks:

| | Sink | Description |
|:---:|:-----|:------------|
| <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/apachekafka/apachekafka-original.svg" width="24" height="24"> | [`kafka`](kafka.md) | Kafka producer sink |
| <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/nats/nats-original.svg" width="24" height="24"> | [`nats`](nats.md) | NATS JetStream sink |
| <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/redis/redis-original.svg" width="24" height="24"> | [`redis`](redis.md) | Redis stream sink |


## Multiple sinks in one pipeline

You can combine multiple sinks in one pipeline to fan out events to different destinations. However, multi-sink pipelines introduce complexity that requires careful consideration.

### Why multiple sinks are challenging

**Different performance characteristics**: Kafka might handle 100K events/sec while a downstream HTTP webhook processes 100/sec. The slowest sink becomes the bottleneck for the entire pipeline.

**Independent failure modes**: Each sink can fail independently. Redis might be healthy while Kafka experiences broker failures. Without proper handling, a single sink failure could block the entire pipeline or cause data loss.

**No distributed transactions**: DeltaForge cannot atomically commit across heterogeneous systems. If Kafka succeeds but Redis fails mid-batch, you face a choice: retry Redis (risking duplicates in Kafka) or skip Redis (losing data there).

**Checkpoint semantics**: The checkpoint represents "how far we've processed from the source." With multiple sinks, when is it safe to advance? After one sink succeeds? All of them? A majority?

### The `required` flag

The `required` flag on each sink determines whether that sink must acknowledge successful delivery before the checkpoint advances:

```yaml
sinks:
  - type: kafka
    config:
      id: primary-kafka
      required: true    # Must succeed for checkpoint to advance
      
  - type: redis
    config:
      id: cache-redis
      required: false   # Best-effort; failures don't block checkpoint
```

**When `required: true`** (default): The sink must acknowledge the batch before the checkpoint can advance. If this sink fails, the pipeline blocks and retries until it succeeds or the operator intervenes.

**When `required: false`**: The sink is best-effort. Failures are logged but don't prevent the checkpoint from advancing. Use this for non-critical destinations where some data loss is acceptable.

### Commit policy

The `commit_policy` works with the `required` flag to determine checkpoint behavior:

| Policy | Behavior |
|--------|----------|
| `all` | Every sink (regardless of `required` flag) must acknowledge |
| `required` | Only sinks with `required: true` must acknowledge (default) |
| `quorum` | At least N sinks must acknowledge |

```yaml
commit_policy:
  mode: required   # Only wait for required sinks

sinks:
  - type: kafka
    config:
      required: true   # Checkpoint waits for this
  - type: redis  
    config:
      required: false  # Checkpoint doesn't wait for this
  - type: nats
    config:
      required: true   # Checkpoint waits for this
```

### Practical patterns

**Primary + secondary**: One critical sink (Kafka for durability) marked `required: true`, with secondary sinks (Redis for caching) marked `required: false`.

**Quorum for redundancy**: Three sinks with `commit_policy.mode: quorum` and `quorum: 2`. Checkpoint advances when any two succeed, providing fault tolerance.

**All-or-nothing**: Use `commit_policy.mode: all` when every destination is critical and you need the strongest consistency guarantee (at the cost of availability or rate of delivery).
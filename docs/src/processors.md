# Processors

Processors can transform, modify or take extra action per event batch between the source and sinks. They run in the order listed in `spec.processors`, and each receives the output of the previous one. A processor can modify events, filter them out, or add routing metadata.

## Available Processors

| Processor | Description |
|-----------|-------------|
| [`javascript`](#javascript) | Custom transformations using V8-powered JavaScript |
| [`outbox`](#outbox) | Transactional outbox pattern - extracts payload, resolves topic, sets routing headers |
| [`flatten`](#flatten) | Flatten nested JSON objects into conmbined keys |


## JavaScript

Run arbitrary JavaScript against each event batch. Uses the V8 engine via `deno_core` for near-native speed with configurable resource limits.

```yaml
processors:
  - type: javascript
    id: enrich
    inline: |
      function processBatch(events) {
        return events.map(e => {
          e.tags = ["processed"];
          return e;
        });
      }
    limits:
      cpu_ms: 50
      mem_mb: 128
      timeout_ms: 500
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | string | *(required)* | Processor identifier |
| `inline` | string | *(required)* | JavaScript source code |
| `limits.cpu_ms` | int | `50` | CPU time limit per batch |
| `limits.mem_mb` | int | `128` | V8 heap memory limit |
| `limits.timeout_ms` | int | `500` | Wall-clock timeout per batch |

### Performance

JavaScript processing adds a fixed overhead per batch plus linear scaling per event. Benchmarks show ~70K+ events/sec throughput with typical transforms — roughly 61× slower than native Rust processors, but sufficient for most workloads. If you need higher throughput, keep transform logic simple or move heavy processing downstream.

### Tips

- Return an empty array to drop all events in a batch.
- Return multiple copies of an event to fan out.
- JavaScript numbers are `f64` — integer columns wider than 53 bits may lose precision. Use string representation for such values.

## Outbox

The outbox processor transforms events captured by the [outbox pattern](outbox.md) into routed, sink-ready events. It extracts business fields from the raw outbox payload, resolves the destination topic, and passes through all non-outbox events unchanged.

```yaml
processors:
  - type: outbox
    topic: "${aggregate_type}.${event_type}"
    default_topic: "events.unrouted"
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | string | `"outbox"` | Processor identifier |
| `tables` | array | `[]` | Only process outbox events matching these patterns. Empty = all. |
| `topic` | string | — | Topic template with `${field}` placeholders (resolved against raw payload columns) |
| `default_topic` | string | — | Fallback when template resolution fails |
| `columns` | object | *(defaults below)* | Field name mappings |
| `additional_headers` | map | `{}` | Forward extra payload fields as routing headers. Key = header name, value = column name. |
| `raw_payload` | bool | `false` | Deliver payload as-is, bypassing envelope wrapping |

### Column Defaults

| Key | Default | Description |
|-----|---------|-------------|
| `payload` | `"payload"` | Field containing the event body |
| `aggregate_type` | `"aggregate_type"` | Domain aggregate type |
| `aggregate_id` | `"aggregate_id"` | Aggregate identifier |
| `event_type` | `"event_type"` | Domain event type |
| `topic` | `"topic"` | Optional explicit topic override in payload |

See the [Outbox Pattern](outbox.md) guide for source configuration, complete examples, and multi-outbox routing.


## Flatten

Flattens nested JSON objects in event payloads into top-level keys joined by a configurable separator. Works on every object-valued field present on the event without assuming any particular envelope structure - CDC `before`/`after`, outbox business payloads, or any custom fields introduced by upstream processors are all handled uniformly.

```yaml
processors:
  - type: flatten
    id: flat
    separator: "__"
    max_depth: 3
    on_collision: last
    empty_object: preserve
    lists: preserve
    empty_list: drop
```

**Input:**
```json
{
  "after": {
    "order_id": "abc",
    "customer": {
      "id": 1,
      "address": { "city": "Berlin", "zip": "10115" }
    },
    "tags": ["vip"],
    "meta": {}
  }
}
```

**Output** (with defaults — `separator: "__"`, `empty_object: preserve`, `lists: preserve`):
```json
{
  "after": {
    "order_id": "abc",
    "customer__id": 1,
    "customer__address__city": "Berlin",
    "customer__address__zip": "10115",
    "tags": ["vip"],
    "meta": {}
  }
}
```

### Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | string | `"flatten"` | Processor identifier |
| `separator` | string | `"__"` | Separator inserted between path segments |
| `max_depth` | int | unlimited | Stop recursing at this depth; objects at the boundary are kept as opaque leaves |
| `on_collision` | string | `last` | What to do when two paths produce the same key. `last`, `first`, or `error` |
| `empty_object` | string | `preserve` | How to handle `{}` values. `preserve`, `drop`, or `null` |
| `lists` | string | `preserve` | How to handle array values. `preserve` (keep as-is) or `index` (expand to `field__0`, `field__1`, …) |
| `empty_list` | string | `preserve` | How to handle `[]` values. `preserve`, `drop`, or `null` |

### max_depth in practice

`max_depth` counts nesting levels from the top of the payload object. Objects at the boundary are treated as opaque leaves rather than expanded further, still subject to `empty_object` policy.

```
# max_depth: 2

depth 0: customer               -> object, recurse
depth 1: customer__address      -> object, recurse
depth 2: customer__address__geo -> STOP, kept as leaf {"lat": 52.5, "lng": 13.4}
```

Without `max_depth`, a deeply nested or recursive payload could produce a large number of keys. Setting a limit is recommended for payloads with variable or unknown nesting depth.

### Collision policy

A collision occurs when two input paths produce the same flattened key - typically when a payload already contains a pre-flattened key (e.g. `"a__b": 1`) alongside a nested object (`"a": {"b": 2}`).

- `last` - the last path to write a key wins (default, never fails)
- `first` - the first path to write a key wins, subsequent writes are ignored
- `error` - the batch fails immediately, useful in strict pipelines where collisions indicate a schema problem

### Working with outbox payloads

After the [`outbox`](#outbox) processor runs, `event.after` holds the extracted business payload - there is no `before`. The flatten processor handles this naturally since it operates on whatever fields are present:

```yaml
processors:
  - type: outbox
    topic: "${aggregate_type}.${event_type}"
  - type: flatten
    id: flat
    separator: "."
    empty_list: drop
```


### Envelope interaction

The flatten processor runs on the raw `Event` struct **before** sink delivery. Envelope wrapping happens inside the sink, after all processors have run. This means the envelope always wraps already-flattened data, no special configuration needed.

```
Source → [flatten processor] → Sink (envelope → bytes)
```

All envelope formats work as expected:

```json
// Native
{ "before": null, "after": { "customer__id": 1, "customer__address__city": "Berlin" }, "op": "c" }

// Debezium
{ "payload": { "before": null, "after": { "customer__id": 1, "customer__address__city": "Berlin" }, "op": "c" } }

// CloudEvents
{ "specversion": "1.0", ..., "data": { "before": null, "after": { "customer__id": 1, "customer__address__city": "Berlin" } } }
```

**Outbox + `raw_payload: true`**

When the outbox processor is configured with `raw_payload: true`, the sink delivers `event.after` directly, bypassing the envelope entirely. If the flatten processor runs after outbox, the raw payload delivered to the sink is the flattened object — which is the intended behavior for analytics sinks that can't handle nested JSON.

```yaml
processors:
  - type: outbox
    topic: "${aggregate_type}.${event_type}"
    raw_payload: true       # sink delivers event.after directly, no envelope
  - type: flatten
    id: flat
    separator: "__"
    empty_list: drop
```

The flatten processor runs second, so by the time the sink delivers the raw payload it is already flat.

### Analytics sink example

When sending to column-oriented sinks (ClickHouse, BigQuery, S3 Parquet) that don't handle nested JSON:

```yaml
processors:
  - type: flatten
    id: flat
    separator: "__"
    lists: index          # expand arrays to indexed columns
    empty_object: drop    # remove sparse marker objects
    empty_list: drop      # remove empty arrays
```

## Processor Chain

Processors execute in order. Events flow through each processor sequentially:

```
Source → [Processor 1] → [Processor 2] → ... → Sinks
```

Each processor receives a `Vec<Event>` and returns a `Vec<Event>`. This means processors can:

- **Transform**: Modify event fields in place
- **Filter**: Return a subset of events (drop unwanted ones)
- **Fan-out**: Return more events than received
- **Route**: Set `event.routing.topic` to override sink defaults
- **Enrich**: Add headers via `event.routing.headers`

Non-outbox events always pass through the outbox processor untouched, so it is safe to combine it with JavaScript processors in any order.

## Adding Custom Processors

The processor interface is a simple trait:

```rust
#[async_trait]
pub trait Processor: Send + Sync {
    fn id(&self) -> &str;
    async fn process(&self, events: Vec<Event>) -> Result<Vec<Event>>;
}
```

To add a new processor:

1. Implement the `Processor` trait in `crates/processors`
2. Add a config variant to `ProcessorCfg` in `deltaforge-config`
3. Register the build step in `build_processors()`
# Schema Sensing

Schema sensing automatically infers and tracks schema structure from JSON event payloads. This complements the schema registry by discovering schema from data rather than database metadata.

## When to Use Schema Sensing

Schema sensing is useful when:

- **Source doesn't provide schema**: Some sources emit JSON without metadata
- **JSON columns**: Database JSON/JSONB columns have dynamic structure
- **Schema evolution tracking**: Detect when payload structure changes over time
- **Downstream integration**: Generate JSON Schema for consumers
- **Dynamic map keys**: Session IDs, trace IDs, or other high-cardinality keys in JSON

## How It Works

```
┌──────────────┐     ┌─────────────────┐     ┌──────────────────┐
│    Event     │────▶│  Schema Sensor  │────▶│  Inferred Schema │
│   Payload    │     │   (sampling)    │     │   + Fingerprint  │
└──────────────┘     └─────────────────┘     └──────────────────┘
                              │
                              ▼
                     ┌─────────────────┐
                     │ Structure Cache │
                     │ + HC Classifier │
                     └─────────────────┘
```

1. **Observation**: Events flow through the sensor during batch processing
2. **Sampling**: Not every event is fully analyzed (configurable rate)
3. **Deep inspection**: Nested JSON structures are recursively analyzed
4. **High-cardinality detection**: Dynamic map keys are classified and normalized
5. **Fingerprinting**: Schema changes are detected via SHA-256 fingerprints
6. **Caching**: Repeated structures skip full analysis for performance

## High-Cardinality Key Handling

JSON payloads often contain dynamic keys like session IDs, trace IDs, or user-generated identifiers:

```json
{
  "id": 1,
  "sessions": {
    "sess_abc123": {"user_id": 42, "started_at": 1700000000},
    "sess_xyz789": {"user_id": 43, "started_at": 1700000001}
  }
}
```

Without special handling, each unique key (`sess_abc123`, `sess_xyz789`) triggers a "schema evolution" event, causing:
- 0% cache hit rate
- Constant false evolution alerts
- Unbounded schema growth

### How It Works

DeltaForge uses probabilistic data structures (HyperLogLog, SpaceSaving) to classify fields:

| Classification | Description | Example |
|----------------|-------------|---------|
| **Stable fields** | Appear in most events | `id`, `type`, `timestamp` |
| **Dynamic fields** | Unique per event, high cardinality | `sess_*`, `trace_*`, `uuid_*` |

When dynamic fields are detected, the schema sensor:
1. **Normalizes keys**: Replaces `sess_abc123` with `<dynamic>` placeholder
2. **Uses adaptive hashing**: Structure cache ignores dynamic key names
3. **Produces stable fingerprints**: Same schema despite different keys

### Results

| Scenario | Without HC | With HC |
|----------|------------|---------|
| Nested dynamic keys | 100% evolution rate | <1% evolution rate |
| Top-level dynamic keys | 0% cache hits | >99% cache hits |
| Stable structs | Baseline | ~20% overhead during warmup, then ~0% |

## Configuration

<table>
<tr>
<td width="50%" valign="top">

### Example

```yaml
spec:
  schema_sensing:
    enabled: true
    
    deep_inspect:
      enabled: true
      max_depth: 3
      max_sample_size: 500
    
    sampling:
      warmup_events: 50
      sample_rate: 5
      structure_cache: true
      structure_cache_size: 50
    
    high_cardinality:
      enabled: true
      min_events: 100
      stable_threshold: 0.5
      min_dynamic_fields: 5
      confidence_threshold: 0.7
      reevaluate_interval: 10000
```

</td>
<td width="50%" valign="top">

### Options

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Enable schema sensing |
| **deep_inspect** |
| `enabled` | bool | `false` | Inspect nested JSON |
| `max_depth` | int | `3` | Max nesting depth |
| `max_sample_size` | int | `500` | Max events for deep analysis |
| **sampling** |
| `warmup_events` | int | `50` | Full analysis before sampling |
| `sample_rate` | int | `5` | After warmup, analyze 1 in N |
| `structure_cache` | bool | `true` | Cache structure hashes |
| `structure_cache_size` | int | `50` | Max cached per table |
| **high_cardinality** |
| `enabled` | bool | `true` | Detect dynamic map keys |
| `min_events` | int | `100` | Events before classification |
| `stable_threshold` | float | `0.5` | Frequency for stable fields |
| `min_dynamic_fields` | int | `5` | Min unique for map detection |
| `confidence_threshold` | float | `0.7` | Required confidence |
| `reevaluate_interval` | int | `10000` | Re-check interval (0=never) |

</td>
</tr>
</table>

## Inferred Types

Schema sensing infers these JSON types:

| Type | Description |
|------|-------------|
| `null` | JSON null value |
| `boolean` | true/false |
| `integer` | Whole numbers |
| `number` | Floating point numbers |
| `string` | Text values |
| `array` | JSON arrays (element types tracked) |
| `object` | Nested objects (fields recursively analyzed) |

For fields with varying types across events, all observed types are recorded.

## Schema Evolution

When schema structure changes, the sensor:

1. **Detects change**: Fingerprint differs from previous version
2. **Increments sequence**: Monotonic version number increases
3. **Logs evolution**: Emits structured log with old/new fingerprints
4. **Updates cache**: New structure becomes current

Evolution events are available via the REST API and can trigger alerts.

## Stabilization

After observing enough events, a schema "stabilizes":

- Warmup phase completes
- Structure stops changing
- Sampling rate takes effect
- Cache hit rate increases

Stabilized schemas have `stabilized: true` in API responses.

## API Access

### List Inferred Schemas

```bash
curl http://localhost:8080/pipelines/my-pipeline/sensing/schemas
```

### Get Schema Details

```bash
curl http://localhost:8080/pipelines/my-pipeline/sensing/schemas/orders
```

### Export as JSON Schema

```bash
curl http://localhost:8080/pipelines/my-pipeline/sensing/schemas/orders/json-schema
```

### Cache Statistics

```bash
curl http://localhost:8080/pipelines/my-pipeline/sensing/stats
```

### Dynamic Map Classifications

```bash
curl http://localhost:8080/pipelines/my-pipeline/sensing/schemas/orders/classifications
```

Response:
```json
{
  "table": "orders",
  "paths": {
    "": {"stable_fields": ["id", "type", "timestamp"], "has_dynamic_fields": false},
    "sessions": {"stable_fields": [], "has_dynamic_fields": true, "unique_keys": 1523},
    "metadata": {"stable_fields": ["version"], "has_dynamic_fields": true, "unique_keys": 42}
  }
}
```

## Drift Detection

Schema sensing integrates with drift detection to compare:

- **Expected schema**: From database metadata (schema registry)
- **Observed schema**: From event payloads (schema sensing)

When mismatches occur, drift events are recorded:

| Drift Type | Description |
|------------|-------------|
| `unexpected_null` | Non-nullable column has null values |
| `type_mismatch` | Observed type differs from declared type |
| `undeclared_column` | Field in data not in schema |
| `missing_column` | Schema field never seen in data |
| `json_structure_change` | JSON column structure changed |

Access drift data via:

```bash
curl http://localhost:8080/pipelines/my-pipeline/drift
```

## Performance Considerations

### Sampling Tradeoffs

| Setting | Effect |
|---------|--------|
| Higher `warmup_events` | Better initial accuracy, slower stabilization |
| Higher `sample_rate` | Lower CPU usage, slower evolution detection |
| Larger `structure_cache_size` | More memory, better hit rate |

### Recommended Settings

**High-throughput pipelines** (>10k events/sec):
```yaml
sampling:
  warmup_events: 100
  sample_rate: 10
  structure_cache: true
  structure_cache_size: 100
high_cardinality:
  enabled: true
  min_events: 200
```

**Schema evolution monitoring**:
```yaml
sampling:
  warmup_events: 25
  sample_rate: 2
  structure_cache: true
high_cardinality:
  enabled: true
  min_events: 50
```

**Payloads with dynamic keys** (session stores, feature flags):
```yaml
sampling:
  structure_cache: true
  structure_cache_size: 50
high_cardinality:
  enabled: true
  min_events: 100
  min_dynamic_fields: 3
  stable_threshold: 0.5
```

**Development/debugging**:
```yaml
sampling:
  warmup_events: 10
  sample_rate: 1  # Analyze every event
high_cardinality:
  enabled: true
  min_events: 20  # Faster classification
```

## Example: JSON Column Sensing

For tables with JSON columns, sensing reveals the internal structure:

```yaml
# Database schema shows: metadata JSON
# Sensing reveals:
fields:
  - name: "metadata.user_agent"
    types: ["string"]
    nullable: false
  - name: "metadata.ip_address"
    types: ["string"]
    nullable: true
  - name: "metadata.tags"
    types: ["array"]
    array_element_types: ["string"]
```

This enables downstream consumers to understand JSON column structure without manual documentation.

## Metrics

Schema sensing emits these Prometheus metrics:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `deltaforge_schema_events_total` | Counter | `table` | Total events observed |
| `deltaforge_schema_cache_hits_total` | Counter | `table` | Structure cache hits |
| `deltaforge_schema_cache_misses_total` | Counter | `table` | Structure cache misses |
| `deltaforge_schema_evolutions_total` | Counter | `table` | Schema evolutions detected |
| `deltaforge_schema_tables_total` | Gauge | - | Tables with detected schemas |
| `deltaforge_schema_dynamic_maps_total` | Gauge | - | Paths classified as dynamic maps |
| `deltaforge_schema_sensing_seconds` | Histogram | `table` | Per-event sensing latency |

### Example Queries

```promql
# Cache hit rate per table
sum(rate(deltaforge_schema_cache_hits_total[5m])) by (table)
/
sum(rate(deltaforge_schema_events_total[5m])) by (table)

# Schema evolution rate (should be near zero after warmup)
sum(rate(deltaforge_schema_evolutions_total[5m])) by (table)

# P99 sensing latency
histogram_quantile(0.99, rate(deltaforge_schema_sensing_seconds_bucket[5m]))
```
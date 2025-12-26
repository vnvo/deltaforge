# Schema Sensing

Schema sensing automatically infers and tracks schema structure from JSON event payloads. This complements the schema registry by discovering schema from data rather than database metadata.

## When to Use Schema Sensing

Schema sensing is useful when:

- **Source doesn't provide schema**: Some sources emit JSON without metadata
- **JSON columns**: Database JSON/JSONB columns have dynamic structure
- **Schema evolution tracking**: Detect when payload structure changes over time
- **Downstream integration**: Generate JSON Schema for consumers

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
                     └─────────────────┘
```

1. **Observation**: Events flow through the sensor during batch processing
2. **Sampling**: Not every event is fully analyzed (configurable rate)
3. **Deep inspection**: Nested JSON structures are recursively analyzed
4. **Fingerprinting**: Schema changes are detected via SHA-256 fingerprints
5. **Caching**: Repeated structures skip full analysis for performance

## Configuration

Enable schema sensing in your pipeline spec:

```yaml
spec:
  schema_sensing:
    enabled: true
    
    # Deep inspection for nested JSON
    deep_inspect:
      enabled: true
      max_depth: 3
      max_sample_size: 500
    
    # Sampling configuration
    sampling:
      warmup_events: 50
      sample_rate: 5
      structure_cache: true
      structure_cache_size: 50
    
    # Output configuration
    output:
      include_stats: true
```

### Configuration Options

#### Top Level

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Enable schema sensing |

#### Deep Inspection (`deep_inspect`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Enable deep inspection of nested objects |
| `max_depth` | integer | `3` | Maximum nesting depth to analyze |
| `max_sample_size` | integer | `500` | Max events to sample for deep analysis |

#### Sampling (`sampling`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `warmup_events` | integer | `50` | Events to analyze before sampling kicks in |
| `sample_rate` | integer | `5` | Analyze 1 in N events after warmup |
| `structure_cache` | bool | `true` | Cache structure hashes for performance |
| `structure_cache_size` | integer | `50` | Max cached structures per table |

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
```

**Schema evolution monitoring**:
```yaml
sampling:
  warmup_events: 25
  sample_rate: 2
  structure_cache: true
```

**Development/debugging**:
```yaml
sampling:
  warmup_events: 10
  sample_rate: 1  # Analyze every event
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

Schema sensing emits these metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `deltaforge_schema_evolutions_total{pipeline}` | Counter | Schema evolution events detected |
| `deltaforge_schema_drift_detected{pipeline}` | Counter | Incremented when drift is detected in a batch |
| `deltaforge_stage_latency_seconds{pipeline,stage="schema_sensing"}` | Histogram | Time spent in schema sensing per batch |

Cache statistics (hits, misses, hit rate) are available via the REST API at `/pipelines/{name}/sensing/stats` but are not currently exposed as Prometheus metrics.
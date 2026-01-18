# Schema Sensing and Drift Detection

This example demonstrates DeltaForge's automatic schema inference for JSON columns and drift detection capabilities.

## Overview

| Component | Configuration |
|-----------|---------------|
| **Source** | PostgreSQL logical replication |
| **Processor** | None |
| **Sink** | Kafka |
| **Feature** | Schema sensing with deep JSON inspection |

## Use Case

You have a PostgreSQL database with JSON/JSONB columns and want to:
- Automatically discover the structure of JSON payloads
- Detect when JSON schemas change over time (drift)
- Export inferred schemas as JSON Schema for downstream validation
- Monitor schema evolution without manual tracking

## Pipeline Configuration

```yaml
apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: products-with-sensing
  tenant: acme

spec:
  source:
    type: postgres
    config:
      id: products-postgres
      dsn: ${POSTGRES_DSN}
      slot: deltaforge_products
      publication: products_pub
      tables:
        - public.products
        - public.product_variants
      start_position: earliest

  sinks:
    - type: kafka
      config:
        id: products-kafka
        brokers: ${KAFKA_BROKERS}
        topic: products.changes
        envelope:
          type: debezium
        encoding: json
        required: true

  batch:
    max_events: 500
    max_ms: 1000
    respect_source_tx: true

  commit_policy:
    mode: required

  # Schema sensing configuration
  schema_sensing:
    enabled: true
    
    deep_inspect:
      enabled: true
      max_depth: 5           # How deep to traverse nested JSON
      max_sample_size: 500   # Events to analyze for deep inspection
    
    sampling:
      warmup_events: 100     # Analyze every event during warmup
      sample_rate: 20        # After warmup, analyze 1 in 20 events
      structure_cache: true  # Cache seen structures to avoid re-analysis
      structure_cache_size: 100
    
    tracking:
      detect_drift: true     # Enable drift detection
      drift_threshold: 0.1   # Alert if >10% of events have new fields
    
    output:
      emit_schemas: true     # Include schema info in API responses
      json_schema_format: draft-07
```

## Table Structure

```sql
CREATE TABLE products (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  sku TEXT UNIQUE,
  price DECIMAL(10,2),
  
  -- JSON columns that schema sensing will analyze
  metadata JSONB,          -- Product attributes, tags, etc.
  specifications JSONB,    -- Technical specs
  
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE product_variants (
  id SERIAL PRIMARY KEY,
  product_id INTEGER REFERENCES products(id),
  variant_name TEXT,
  
  -- Nested JSON with variable structure
  attributes JSONB,        -- Color, size, material, etc.
  pricing JSONB,           -- Regional pricing, discounts
  
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create publication
CREATE PUBLICATION products_pub FOR TABLE products, product_variants;
```

## Sample Data

```sql
-- Insert products with JSON metadata
INSERT INTO products (name, sku, price, metadata, specifications) VALUES
(
  'Wireless Headphones',
  'WH-1000',
  299.99,
  '{
    "brand": "AudioTech",
    "category": "Electronics",
    "tags": ["wireless", "bluetooth", "noise-canceling"],
    "ratings": {"average": 4.5, "count": 1250}
  }',
  '{
    "battery_life_hours": 30,
    "driver_size_mm": 40,
    "frequency_response": {"min_hz": 20, "max_hz": 20000},
    "connectivity": ["bluetooth", "aux"]
  }'
);

-- Insert variant with nested attributes
INSERT INTO product_variants (product_id, variant_name, attributes, pricing) VALUES
(
  1,
  'Midnight Black',
  '{
    "color": {"name": "Midnight Black", "hex": "#1a1a2e"},
    "material": "Premium Plastic",
    "weight_grams": 250
  }',
  '{
    "base_price": 299.99,
    "regional": {
      "US": {"price": 299.99, "currency": "USD"},
      "EU": {"price": 279.99, "currency": "EUR"}
    },
    "discounts": [
      {"code": "SAVE20", "percent": 20, "expires": "2025-12-31"}
    ]
  }'
);
```

## Running the Example

### 1. Set Environment Variables

```bash
export POSTGRES_DSN="postgres://user:password@localhost:5432/shop"
export KAFKA_BROKERS="localhost:9092"
```

### 2. Start DeltaForge

```bash
cargo run -p runner -- --config products-sensing.yaml
```

### 3. Insert Data and Let Sensing Analyze

```sql
-- Insert several products to build schema profile
INSERT INTO products (name, sku, price, metadata, specifications) VALUES
('Smart Watch', 'SW-200', 399.99, 
 '{"brand": "TechWear", "tags": ["fitness", "smart"]}',
 '{"battery_days": 7, "water_resistant": true}');
```

## Using the Schema Sensing API

### List Inferred Schemas

```bash
curl http://localhost:8080/pipelines/products-with-sensing/sensing/schemas
```

Response:
```json
{
  "schemas": [
    {
      "table": "public.products",
      "columns": {
        "metadata": {
          "type": "object",
          "inferred_at": "2025-01-15T10:30:00Z",
          "sample_count": 150
        },
        "specifications": {
          "type": "object",
          "inferred_at": "2025-01-15T10:30:00Z",
          "sample_count": 150
        }
      }
    }
  ]
}
```

### Get Detailed Schema for a Table

```bash
curl http://localhost:8080/pipelines/products-with-sensing/sensing/schemas/public.products
```

Response:
```json
{
  "table": "public.products",
  "json_columns": {
    "metadata": {
      "inferred_schema": {
        "type": "object",
        "properties": {
          "brand": {"type": "string"},
          "category": {"type": "string"},
          "tags": {"type": "array", "items": {"type": "string"}},
          "ratings": {
            "type": "object",
            "properties": {
              "average": {"type": "number"},
              "count": {"type": "integer"}
            }
          }
        }
      },
      "sample_count": 150,
      "last_updated": "2025-01-15T10:35:00Z"
    }
  }
}
```

### Export as JSON Schema

```bash
curl http://localhost:8080/pipelines/products-with-sensing/sensing/schemas/public.products/json-schema
```

Response:
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "public.products.metadata",
  "type": "object",
  "properties": {
    "brand": {"type": "string"},
    "category": {"type": "string"},
    "tags": {
      "type": "array",
      "items": {"type": "string"}
    },
    "ratings": {
      "type": "object",
      "properties": {
        "average": {"type": "number"},
        "count": {"type": "integer"}
      }
    }
  }
}
```

### Check Drift Detection

```bash
curl http://localhost:8080/pipelines/products-with-sensing/drift
```

Response:
```json
{
  "drift_detected": true,
  "tables": {
    "public.products": {
      "metadata": {
        "new_fields": ["promotion", "seasonal"],
        "removed_fields": [],
        "type_changes": [],
        "drift_percentage": 0.15,
        "first_seen": "2025-01-15T11:00:00Z"
      }
    }
  }
}
```

### Get Sensing Statistics

```bash
curl http://localhost:8080/pipelines/products-with-sensing/sensing/stats
```

Response:
```json
{
  "total_events_analyzed": 1500,
  "total_events_sampled": 250,
  "cache_hits": 1250,
  "cache_misses": 250,
  "tables_tracked": 2,
  "json_columns_tracked": 4
}
```

## Performance Tuning

> **Performance tip**: Schema sensing can be CPU-intensive. Tune based on your throughput needs.

### High-Throughput Configuration

```yaml
schema_sensing:
  enabled: true
  deep_inspect:
    enabled: true
    max_depth: 3           # Limit depth for faster processing
    max_sample_size: 200   # Fewer samples
  sampling:
    warmup_events: 50      # Shorter warmup
    sample_rate: 100       # Analyze 1 in 100 events
    structure_cache: true
    structure_cache_size: 200  # Larger cache
```

### Development/Debugging Configuration

```yaml
schema_sensing:
  enabled: true
  deep_inspect:
    enabled: true
    max_depth: 10          # Full depth
    max_sample_size: 1000  # More samples
  sampling:
    warmup_events: 500     # Longer warmup
    sample_rate: 1         # Analyze every event
```

## Key Concepts Demonstrated

- **Automatic Schema Inference**: Discover JSON structure without manual definition
- **Deep JSON Inspection**: Traverse nested objects and arrays
- **Drift Detection**: Alert when schemas change unexpectedly
- **JSON Schema Export**: Generate standard schemas for validation
- **Sampling Strategy**: Balance accuracy vs. performance

## Related Documentation

- [Schema Sensing](../schemasensing.md) - Detailed schema sensing documentation
- [PostgreSQL Source](../sources/postgres.md) - Replication setup
- [Configuration Reference](../configuration.md#schema-sensing) - Full sensing options
- [API Reference](../apireference.md) - All sensing endpoints
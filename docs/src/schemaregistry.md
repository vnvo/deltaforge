# Schema Registry

DeltaForge includes an in-memory schema registry that tracks table schemas across
your CDC pipelines. The registry provides versioning, change detection, and a REST
API for schema inspection.

## Overview

The schema registry serves three purposes:

1. **Column mapping** — Maps binlog row data to named columns using cached schemas
2. **Change detection** — Fingerprints schemas to detect DDL changes
3. **Version history** — Maintains schema versions for replay and debugging

## How It Works

### Startup

When a pipeline starts, the schema loader:

1. Expands table patterns (e.g., `shop.*` → `shop.orders`, `shop.customers`)
2. Queries `INFORMATION_SCHEMA.COLUMNS` for each table
3. Computes a fingerprint (SHA-256 of column definitions)
4. Registers the schema with the registry

```
MySqlSource.run()
    └── MySqlSchemaLoader.preload(&tables)
            ├── expand_patterns()     → [(shop, orders), (shop, customers)]
            ├── load_schema(shop, orders)
            │       ├── fetch from INFORMATION_SCHEMA
            │       ├── compute fingerprint
            │       └── registry.register() → version 1
            └── load_schema(shop, customers)
                    └── ...
```

### Runtime

During CDC streaming:

- **Row events** use cached column names for JSON field mapping
- **DDL events** (CREATE/ALTER/DROP) trigger cache invalidation
- Schema is reloaded on next row event for that table

### Manual Reload

After out-of-band DDL (e.g., direct changes on a replica), trigger a reload:

```bash
curl -X POST http://localhost:8080/pipelines/my-pipeline/schemas/reload
```

## Configuration

### Table Patterns

The `tables` config supports SQL LIKE patterns:

```yaml
spec:
  source:
    type: mysql
    config:
      tables:
        - shop.orders           # exact match
        - shop.order_%          # prefix: order_items, order_history
        - inventory.*           # all tables in database
        - %.audit_log           # audit_log in any database
```

Pattern expansion happens at startup. New tables matching patterns are **not**
automatically detected; restart the pipeline or use the reload API.

## REST API

### List Schemas

```http
GET /pipelines/{pipeline}/schemas
```

Returns all tracked schemas with summary info:

```json
[
  {
    "database": "shop",
    "table": "orders",
    "column_count": 5,
    "primary_key": ["id"],
    "fingerprint": "sha256:a1b2c3d4...",
    "registry_version": 2
  }
]
```

### Get Schema Details

```http
GET /pipelines/{pipeline}/schemas/{db}/{table}
```

Returns full column definitions:

```json
{
  "database": "shop",
  "table": "orders",
  "columns": [
    {
      "name": "id",
      "column_type": "bigint(20) unsigned",
      "data_type": "bigint",
      "nullable": false,
      "ordinal_position": 1,
      "default_value": null,
      "extra": "auto_increment",
      "is_primary_key": true
    },
    {
      "name": "total",
      "column_type": "decimal(10,2)",
      "data_type": "decimal",
      "nullable": true,
      "ordinal_position": 2,
      "default_value": "0.00",
      "extra": null,
      "is_primary_key": false
    }
  ],
  "primary_key": ["id"],
  "engine": "InnoDB",
  "collation": "utf8mb4_0900_ai_ci",
  "fingerprint": "sha256:a1b2c3d4...",
  "registry_version": 2,
  "loaded_at": "2025-01-15T10:30:00Z"
}
```

### Reload All Schemas

```http
POST /pipelines/{pipeline}/schemas/reload
```

Force-reloads all schemas matching the pipeline's table patterns:

```json
{
  "pipeline": "orders-cdc",
  "tables_reloaded": 3,
  "tables": [
    {
      "database": "shop",
      "table": "orders",
      "status": "ok",
      "changed": true,
      "error": null
    },
    {
      "database": "shop",
      "table": "customers",
      "status": "ok",
      "changed": false,
      "error": null
    }
  ],
  "elapsed_ms": 245
}
```

### Reload Single Table

```http
POST /pipelines/{pipeline}/schemas/{db}/{table}/reload
```

Reloads a specific table and returns the updated schema.

### Get Version History

```http
GET /pipelines/{pipeline}/schemas/{db}/{table}/versions
```

Returns all registered versions (newest first):

```json
[
  {
    "version": 2,
    "fingerprint": "sha256:a1b2c3d4...",
    "column_count": 5,
    "registered_at": "2025-01-15T10:30:00Z"
  },
  {
    "version": 1,
    "fingerprint": "sha256:e5f6g7h8...",
    "column_count": 4,
    "registered_at": "2025-01-10T08:00:00Z"
  }
]
```

## Schema Fingerprinting

Fingerprints are computed from column definitions and primary key:

```rust
struct FingerprintData {
    columns: Vec<MySqlColumn>,  // name, type, nullable, position
    primary_key: Vec<String>,
}

let fingerprint = sha256(serde_json::to_string(&data));
```

Changes to `engine` or `collation` do **not** change the fingerprint since they
don't affect row data interpretation.

## DDL Handling

When the binlog contains DDL events:

| DDL Type | Action |
|----------|--------|
| `CREATE TABLE` | Schema loaded on first row event |
| `ALTER TABLE` | Cache invalidated, reloaded on next row |
| `DROP TABLE` | Cache entry removed |
| `TRUNCATE` | No schema change |
| `RENAME TABLE` | Old removed, new loaded on first row |

DDL detection uses the `QueryEvent` type in the binlog. The schema loader
invalidates the entire database cache since MySQL doesn't always specify the
exact table in DDL events.

## Limitations

- **In-memory only** — Schema versions are lost on restart (future: persistent backend)
- **No cross-pipeline sharing** — Each pipeline has its own registry instance
- **Pattern expansion at startup** — New tables require pipeline restart or reload
- **MySQL only** — PostgreSQL schema loader coming soon

## Troubleshooting

### Unknown table_id errors

If you see warnings like:

```
WARN write_rows for unknown table_id, table_id=42
```

This means the binlog contains row events for a table that wasn't in the table_map. 
This can happen if:
- The table was created after the CDC stream started
- Table patterns don't match the table

Trigger a reload to pick up new tables:

```bash
curl -X POST http://localhost:8080/pipelines/my-pipeline/schemas/reload
```

### Schema fetch returned 0 columns

If you see:

```
WARN schema fetch returned 0 columns, db=shop, table=orders
```

This usually means:
1. The table doesn't exist
2. The MySQL user lacks `SELECT` privilege on `INFORMATION_SCHEMA`
3. The table was dropped between detection and schema load

### Missing table

If a table isn't tracked:

1. Check your `tables` patterns match
2. Verify the user has `SELECT` privilege on `INFORMATION_SCHEMA`
3. Reload schemas: `POST /pipelines/{name}/schemas/reload`

### Slow schema loading

If you see `slow schema fetch` warnings (>200ms):

```
WARN slow schema fetch, db=shop, table=orders, ms=350
```

Consider:
- Narrowing table patterns
- Using exact table names instead of wildcards
- Ensuring indexes on system tables (usually automatic)
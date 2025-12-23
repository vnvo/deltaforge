# Schema Registry

DeltaForge's schema registry tracks table schemas across time, enabling accurate event interpretation during replay and providing change detection for DDL operations.

## Design Philosophy: Source-Owned Schemas

DeltaForge takes a fundamentally different approach to schema handling than many CDC tools. Rather than normalizing all database schemas into a universal type system, **each source defines and owns its schema semantics**.

This means:

- **MySQL schemas capture MySQL semantics** - column types like `bigint(20) unsigned`, `varchar(255)`, and `json` are preserved exactly as MySQL defines them
- **PostgreSQL schemas capture PostgreSQL semantics** - arrays, custom types, and pg-specific attributes remain intact
- **No lossy normalization** - you don't lose precision or database-specific information by forcing everything into a common format

This design avoids the maintenance burden of keeping a universal type system synchronized across all databases, and it ensures that downstream consumers receive schemas that accurately reflect the source database's capabilities and constraints.

## The SourceSchema Trait

Every CDC source implements the `SourceSchema` trait, which provides a common interface for fingerprinting and column access while allowing source-specific schema representations:

```rust
pub trait SourceSchema: Serialize + DeserializeOwned + Send + Sync {
    /// Source type identifier (e.g., "mysql", "postgres", "mongodb").
    fn source_kind(&self) -> &'static str;

    /// Content-addressable fingerprint for change detection.
    /// Two schemas with the same fingerprint are identical.
    fn fingerprint(&self) -> String;

    /// Column/field names in ordinal order.
    fn column_names(&self) -> Vec<&str>;

    /// Primary key column names.
    fn primary_key(&self) -> Vec<&str>;

    /// Human-readable description.
    fn describe(&self) -> String;
}
```

### Fingerprinting

Schema fingerprints use SHA-256 hashing over JSON-serialized content to provide:

- **Stability** - the same schema always produces the same fingerprint
- **Change detection** - any structural change produces a different fingerprint
- **Content-addressability** - fingerprints can be used as cache keys or deduplication identifiers

```rust
pub fn compute_fingerprint<T: Serialize>(value: &T) -> String {
    let json = serde_json::to_vec(value).unwrap_or_default();
    let hash = Sha256::digest(&json);
    format!("sha256:{}", hex::encode(hash))
}
```

The fingerprint only includes structurally significant fields. For MySQL, this means columns and primary key are included, but engine and charset are excluded since they don't affect how CDC events should be interpreted.

## MySQL Schema Implementation

The `MySqlTableSchema` struct captures comprehensive MySQL table metadata:

```rust
pub struct MySqlTableSchema {
    /// Columns in ordinal order
    pub columns: Vec<MySqlColumn>,
    
    /// Primary key column names
    pub primary_key: Vec<String>,
    
    /// Storage engine (InnoDB, MyISAM, etc.)
    pub engine: Option<String>,
    
    /// Default charset
    pub charset: Option<String>,
    
    /// Default collation
    pub collation: Option<String>,
}

pub struct MySqlColumn {
    pub name: String,
    pub column_type: String,      // e.g., "bigint(20) unsigned"
    pub data_type: String,        // e.g., "bigint"
    pub nullable: bool,
    pub ordinal_position: u32,
    pub default_value: Option<String>,
    pub extra: Option<String>,    // e.g., "auto_increment"
    pub comment: Option<String>,
    pub char_max_length: Option<i64>,
    pub numeric_precision: Option<i64>,
    pub numeric_scale: Option<i64>,
}
```

Schema information is fetched from `INFORMATION_SCHEMA` at startup and cached for the pipeline's lifetime.

## Schema Registry Architecture

The schema registry serves three core functions:

1. **Version tracking** - maintains a history of schema versions per table
2. **Change detection** - compares fingerprints to detect DDL changes
3. **Replay correlation** - associates schemas with checkpoint positions for accurate replay

### Schema Versions

Each registered schema version includes:

| Field | Description |
|-------|-------------|
| `version` | Per-table version number (starts at 1) |
| `hash` | Content fingerprint for deduplication |
| `schema_json` | Full schema as JSON |
| `registered_at` | Registration timestamp |
| `sequence` | Global monotonic sequence number |
| `checkpoint` | Source checkpoint at registration time |

### Sequence Numbers for Replay

The registry maintains a global monotonic sequence counter. When a schema is registered, it receives the next sequence number. Events carry this sequence number, enabling the replay engine to look up the correct schema version:

```rust
// During replay: find schema active at event's sequence
let schema = registry.get_at_sequence(tenant, db, table, event.schema_sequence);
```

This ensures events are always interpreted with the schema that was active when they were produced, even if the table structure has since changed.

### Checkpoint Correlation

Schemas can be registered with an associated checkpoint, creating a correlation between schema versions and source positions:

```rust
registry.register_with_checkpoint(
    tenant, db, table, 
    &fingerprint, 
    &schema_json,
    Some(checkpoint_bytes),  // Optional: binlog position when schema was observed
).await?;
```

This correlation supports scenarios like:

- Replaying events from a specific checkpoint with the correct schema
- Determining which schema was active at a particular binlog position
- Rolling back schema state along with checkpoint rollback

## Schema Loader

The `MySqlSchemaLoader` handles schema discovery and caching:

### Pattern Expansion

Tables are specified using patterns that support wildcards:

| Pattern | Description |
|---------|-------------|
| `db.table` | Exact match |
| `db.*` | All tables in database |
| `db.prefix%` | Tables starting with prefix |
| `%.table` | Table in any database |

### Preloading

At startup, the loader expands patterns and preloads all matching schemas:

```rust
let schema_loader = MySqlSchemaLoader::new(dsn, registry, tenant);
let tracked_tables = schema_loader.preload(&["shop.orders", "shop.order_%"]).await?;
```

This ensures schemas are available before the first CDC event arrives.

### Caching

Loaded schemas are cached to avoid repeated `INFORMATION_SCHEMA` queries:

```rust
// Fast path: return cached schema
if let Some(cached) = cache.get(&(db, table)) {
    return Ok(cached.clone());
}

// Slow path: fetch from database, register, cache
let schema = fetch_schema(db, table).await?;
let version = registry.register(...).await?;
cache.insert((db, table), loaded_schema);
```

## DDL Handling

When the binlog contains DDL events, the schema loader responds:

| DDL Type | Action |
|----------|--------|
| `CREATE TABLE` | Schema loaded on first row event |
| `ALTER TABLE` | Cache invalidated, reloaded on next row |
| `DROP TABLE` | Cache entry removed |
| `TRUNCATE` | No schema change |
| `RENAME TABLE` | Old removed, new loaded on first row |

DDL detection uses the `QueryEvent` type in the binlog. On DDL, the entire database's schema cache is invalidated since MySQL doesn't always specify the exact table in DDL events.

## API Endpoints

### Reload Schemas

Force reload schemas from the database:

```bash
curl -X POST http://localhost:8080/pipelines/{name}/schemas/reload
```

This clears the cache and re-fetches schemas for all tracked tables.

### List Cached Schemas

View currently cached schemas:

```bash
curl http://localhost:8080/pipelines/{name}/schemas
```

## Limitations

- **In-memory registry** - Schema versions are lost on restart. Persistent backends (SQLite, then PostgreSQL for HA) are planned.
- **No cross-pipeline sharing** - Each pipeline maintains its own registry instance
- **Pattern expansion at startup** - New tables matching patterns require pipeline restart or reload

## Best Practices

1. **Use explicit table patterns** in production to avoid accidentally capturing unwanted tables
2. **Monitor schema reload times** - slow reloads may indicate overly broad patterns
3. **Trigger schema reload after DDL** if your deployment process modifies schemas
4. **Include schema version in downstream events** for consumers that need schema evolution awareness

## Troubleshooting

### Unknown table_id Errors

```
WARN write_rows for unknown table_id, table_id=42
```

The binlog contains row events for a table not in the table_map. This happens when:
- A table was created after the CDC stream started
- Table patterns don't match the table

Solution: Trigger a schema reload via the REST API.

### Schema Fetch Returned 0 Columns

```
WARN schema fetch returned 0 columns, db=shop, table=orders
```

Usually indicates:
- Table doesn't exist
- MySQL user lacks `SELECT` privilege on `INFORMATION_SCHEMA`
- Table was dropped between detection and schema load

### Slow Schema Loading

```
WARN slow schema fetch, db=shop, table=orders, ms=350
```

Consider:
- Narrowing table patterns to reduce the number of tables
- Using exact table names instead of wildcards
- Verifying network latency to the MySQL server
# Data Types

Every row value is decoded once, at the source, by its column's declared
type, into an exact typed value. Every sink renders that value; none
reinterprets source bytes or guesses a type from a value's shape. A snapshot
row and a CDC row of the same stored value produce the same output.

## Mapping version

Each event carries the type-mapping version its values render with, in
`source.type_mapping` (native JSON), the Kafka header `df-type-mapping`, the
Avro schema (`deltaforge.type_mapping`, and subjects `<subject>-v2`) and the
S3 object identity.

```yaml
source:
  type: mysql
  config:
    type_mapping: v2   # default
```

`v2` is the default and the only version PostgreSQL sources accept. MySQL
sources may pin the deprecated `v1`, which differs from `v2` only in native
JSON: BIGINT, BIGINT UNSIGNED and BIT(n>1) as JSON numbers, TIMESTAMP as
epoch microseconds, TIME with six fractional digits, DATETIME as
`YYYY-MM-DD HH:MM:SS.ffffff`, FLOAT as its widened double value.

## Native JSON (v2)

| Source type | JSON |
|---|---|
| TINYINT ... INT (signed and unsigned), smallint, integer, YEAR | number |
| BIGINT, BIGINT UNSIGNED, bigint | exact decimal string (`json_wide_integers: string`, default) or number (`number`) |
| DECIMAL, numeric | string with the declared scale; PostgreSQL `"NaN"`, `"Infinity"`, `"-Infinity"` |
| FLOAT, real / DOUBLE, double precision | number (shortest exact digits); non-finite PostgreSQL values as the strings above |
| BIT(1), boolean | `true` / `false` |
| BIT(n>1) | `{"_base64": "..."}` of exactly ceil(n/8) bytes |
| CHAR, VARCHAR, TEXT, text, varchar | string, decoded from the column's character set (CHAR without trailing spaces) |
| BINARY, VARBINARY, BLOB, GEOMETRY, bytea | `{"_base64": "..."}` (BINARY keeps its zero padding) |
| ENUM / SET | the label / `"a,c"` |
| JSON, json, jsonb | a string holding the document's text (JSON `null` is the string `"null"`; SQL NULL is `null`) |
| DATE, date | `"YYYY-MM-DD"`; MySQL zero dates as written (`"0000-00-00"`); `"infinity"` / `"-infinity"` |
| TIME, time | `"[-]HH:MM:SS[.f]"` with the declared precision (MySQL TIME is a signed duration) |
| DATETIME, timestamp | `"YYYY-MM-DDTHH:MM:SS[.f]"`, no zone; MySQL zero `"0000-00-00 00:00:00"` |
| TIMESTAMP, timestamptz | RFC 3339 UTC: `"2024-03-30T23:30:00Z"` |
| uuid | lowercase hyphenated string |
| PostgreSQL arrays of the types above | JSON array (nested for multi-dimensional values); a NULL array is `null`, a NULL element `null` inside the array |
| Any other PostgreSQL type (inet, interval, ranges, enums, domains, ...) | its PostgreSQL text |

A PostgreSQL column that an update did not send (an unchanged TOAST value) is
filled from the old row when the table has `REPLICA IDENTITY FULL`.
Otherwise it is never rendered as a value: it is left out of `after` and
listed in `unchanged_columns`.

Values a processor added to a row (see [Processors](processors.md)) appear
as ordinary fields in native JSON.

## Typed sinks

Avro, Parquet and ClickHouse convert each value to its column's target type
exactly, or fail with a conversion failure (the `sink_conversion_failed`
incident: dead-lettered when a DLQ acknowledges it; otherwise the pipeline
stops and the sink's checkpoint stays before the event, so a restart meets
it again; events after it in the same batch may already be delivered and
are delivered again). Nothing is rounded, truncated, wrapped or defaulted:

| Source type | Avro | Parquet |
|---|---|---|
| integers | int / long (BIGINT UNSIGNED: string, or long with `unsigned_bigint_mode: long`) | Int32 / Int64 (BIGINT UNSIGNED: Utf8 or Int64) |
| DECIMAL | string | Decimal128(p, s) for p <= 38, else Utf8; PostgreSQL numeric without precision: Utf8 |
| FLOAT / DOUBLE | float / double | Float32 / Float64 |
| BIT(1) / BIT(n) | boolean / fixed(ceil(n/8)) | Boolean / FixedSizeBinary(ceil(n/8)) |
| text, ENUM, SET, JSON | string | Utf8 |
| binary | bytes | Binary |
| DATE | int (date) | Date32 |
| MySQL TIME | long microseconds (`deltaforge.logical: mysql-time-micros`) | Duration(Microsecond) |
| PostgreSQL time | long (time-micros) | Time64(Microsecond) |
| DATETIME / timestamp | string, or local-timestamp-micros (`naive_timestamp_mode: timestamp`) | Utf8, or Timestamp(Microsecond, no zone) |
| TIMESTAMP / timestamptz | long (timestamp-micros) | Timestamp(Microsecond, UTC) |

MySQL zero dates and timestamps, infinities, PostgreSQL `24:00:00` and
values finer than a target's unit fail in typed temporal targets. A
PostgreSQL array column (any number of dimensions in PostgreSQL) maps to a
one-dimensional typed list: a multi-dimensional value fails in Avro and
Parquet. PostgreSQL columns are nullable in Avro and Parquet schemas (a
change event's schema cannot prove NOT NULL), identically for snapshot and
change events.

Values a processor added to a row travel in a stable slot of their own,
never as columns: Avro `before_extensions` / `after_extensions` (nullable
map of JSON text), Parquet `before_extensions` / `after_extensions` (a JSON
object as text), ClickHouse `extensions_column` (default `_extensions`,
`Nullable(String)`). A row column the schema does not define is refused,
never dropped.

**PostgreSQL unchanged TOAST values:** Avro, Parquet, ClickHouse and
Elasticsearch need every value. A pipeline with such a sink requires
`REPLICA IDENTITY FULL` on every captured table with a TOAST-capable column,
so each unchanged value is filled from the old row; otherwise the pipeline is
refused before it streams (see
[PostgreSQL: Unchanged TOAST values](sources/postgres.md#unchanged-toast-values)).
A missing value is never written as NULL.

## Table identity

`source.db` is the database and, for PostgreSQL, `source.schema` the
table's schema: a table is database + schema + table (MySQL: database +
table), identically in snapshot and change events. Caches, schema
versions, idempotency keys and durable S3 objects key on that full
identity; Avro record names carry its escaped parts; legacy S3 partitions
are `database=<db>/namespace=<schema or db>/table=<table>`. Same-named
tables of two schemas or databases never share any of them; a name too
long for an object-key segment is stored as its digest, never truncated. A
ClickHouse table or Elasticsearch index holds one source table, durably
bound: rows of a second one are refused (`sink_destination_conflict`; give
each its own index, for example with `{schema}` and `{table}` in the index
template).

## Decoding failures

A value its declared type cannot decode exactly (bytes invalid in the
column's character set, a MySQL ENUM index 0 or one beyond the definition, a
SET bit outside it, a PostgreSQL array with lower bounds other than 1) stops
the source before the value's transaction, with a `source_decode_failed`
incident naming the table, column and type (never the value).

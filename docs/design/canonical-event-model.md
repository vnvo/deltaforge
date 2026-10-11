# Canonical event model

Status: implementation design for canonical-types slices 3-6, landed as one
feature (reviewer rulings of 2026-10-11). Extends
[MySQL canonical types](mysql-canonical-types.md) with the event model, the
PostgreSQL mapping and the processor boundary.

## 1. One representation

Row images (`Event::before`, `Event::after`) are `CanonicalRow`s: ordered
`(column name, CanonicalValue)` pairs. There is no JSON row image and no
sidecar. Every consumer (native JSON, Avro, Arrow/Parquet, ClickHouse,
Elasticsearch, routing templates, processors, schema sensing) renders from the
canonical values, according to the event's mapping version and its own
settings.

`CanonicalValue` is a closed sum type. No two variants mean the same thing:

| Variant | Meaning |
|---|---|
| `Null` | SQL NULL |
| `Unchanged` | PostgreSQL unchanged TOAST value: the column was not sent. Legal only in a PostgreSQL update's after image |
| `Db(DbValue)` | a schema-typed database value (section 2) |
| `Opaque { source_type, text }` | a value of a source type without a canonical mapping, as the source's deterministic text under a pinned session |
| `Document(Document)` | an untyped JSON document produced without a column type (processor-added fields). Valid canonical JSON only |
| `Bytes(Vec<u8>)` | arbitrary bytes without a column type (PostgreSQL logical-message content) |

A SQL JSON column is `Db(Json)`, never `Document`. A binary column is
`Db(Binary)`, never `Bytes`.

## 2. Database values

| `DbValue` | Holds | Invariant |
|---|---|---|
| `Int { value, width }` | i64 and the declared width (8/16/24/32/64) | value fits the width |
| `UInt { value, width }` | u64 and the declared width | value fits the width |
| `Decimal` | exact decimal text, or NaN/+Infinity/-Infinity (PostgreSQL) | canonical text: no leading zeros, no `-0` |
| `Float32` / `Float64` | IEEE value; non-finite values only from PostgreSQL | |
| `Bool` | | |
| `Text` | decoded Unicode text | |
| `Binary` | bytes as stored (BINARY padding kept) | |
| `Bits { len, bytes }` | MySQL BIT(len): ceil(len/8) big-endian bytes | no bit above len |
| `Enum { label, index }` | label, and MySQL's 1-based index | MySQL: index 1..N; PostgreSQL enums are opaque |
| `Set { bitmask, members }` | MySQL SET | members in definition order, matching the bitmask |
| `Json` | the source's canonical JSON text | valid JSON |
| `Date` | civil date (MySQL zero parts allowed), or +/-infinity | |
| `Time { micros, precision }` | MySQL duration or PostgreSQL time of day | |
| `DateTime` | civil date-time with precision, MySQL zero, or +/-infinity | |
| `Timestamp` | UTC instant in microseconds with precision, MySQL zero, or +/-infinity | |
| `Year` | 0 or 1901..2155 | |
| `Uuid` | 16 bytes | |
| `Array { element_type, items }` | PostgreSQL array of a mapped builtin element type, one dimension level per nesting, lower bound 1 | items typed by the declared element type; a value with other lower bounds is a source decoding failure (it has no canonical form, and the column's representation never changes by value); arrays of an unmapped element type are `Opaque` as a whole |

Precision is `Fixed(0..=6)` (declared) or `Variable` (PostgreSQL without a
typmod: rendered with trailing fractional zeros removed).

## 3. Mapping versions

Every event carries `type_mapping` (`v1` or `v2`), stamped at the source
before batching, the replay journal or the DLQ. Encoders render the event's
version, never the pipeline's current setting. A batch holding more than one
version is split at the version boundaries before it reaches a sink.

v2 native JSON renderings (the default):

| Value | Native JSON |
|---|---|
| Int/UInt of width 64 | exact decimal string (`json_wide_integers: string`, default) or a number (`number`; UInt above i64::MAX is then a sink conversion failure) |
| other Int/UInt | number |
| Decimal | string; NaN/Infinity as `"NaN"`, `"Infinity"`, `"-Infinity"` |
| Float32 | shortest f32 round-trip number; non-finite as the strings above |
| Float64 | shortest round-trip number; non-finite as the strings above |
| Bool | `true` / `false` |
| Text, Opaque | string |
| Binary, Bytes | `{"_base64":"..."}` |
| Bits | len 1: `true`/`false`; otherwise `{"_base64":"..."}` of ceil(len/8) bytes |
| Enum | label |
| Set | `"a,c"` |
| Json | string (canonical text) |
| Document | embedded JSON |
| Date | `YYYY-MM-DD`; MySQL zero parts as written (`0000-00-00`); `infinity`/`-infinity` |
| Time | `[-]H+:MM:SS[.f]` |
| DateTime | `YYYY-MM-DDTHH:MM:SS[.f]`; MySQL zero `0000-00-00 00:00:00`; infinities |
| Timestamp | RFC 3339 UTC with `Z`; MySQL zero `0000-00-00 00:00:00`; infinities |
| Year | number |
| Uuid | lowercase hyphenated text |
| Array | JSON array of the item renderings |
| Null | `null` |

An `Unchanged` column is omitted from the native row image, and the event
lists such columns in `unchanged_columns`, so it can never be mistaken for a
value. Typed sinks refuse `Unchanged`, `Document` and `Bytes` values in a
schema-typed column (a sink conversion failure); they never omit them.

Years outside 0000..9999 render with a sign and at least five digits
(ISO 8601 expanded, astronomical year numbering: 1 BC is `0000`).

v1 differs from v2 only where the legacy output was lossless, and only for
MySQL events: wide integers and BIT(n>1) as JSON numbers, TIMESTAMP as epoch
microseconds, TIME with six fractional digits, DATETIME as the legacy
`YYYY-MM-DD HH:MM:SS.ffffff`, FLOAT as the widened f64. v1 reproduces no
output that was lossy. PostgreSQL had no lossless legacy form to keep, so a
PostgreSQL source refuses `type_mapping: v1` and stamps every event v2.

## 4. Durable form

The replay journal (envelope version 2), the DLQ and S3 durable objects keep
the lossless canonical form: each value as a tagged JSON object, integers and
floats as exact text (floats as IEEE bit patterns), bytes as base64, documents
as canonical JSON. Canonical JSON sorts object keys bytewise at every level
and writes numbers as `serde_json` does (integers exact, floats shortest
round-trip). Replay envelopes of version 1 (untyped JSON events) are refused
before any event is emitted.

## 5. Processor boundary

The JavaScript processor receives each event's native rendering for its
version, with wide integers as strings. The authoritative column types stay
outside the script. On return, a value equal to its original rendering is
restored exactly; a changed value of an existing column is parsed exactly
against the column's original type, and a value that does not fit fails the
batch closed. A removed column is removed. An added column becomes a
`Document`.

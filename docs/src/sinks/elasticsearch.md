# Elasticsearch Sink

Streams CDC events into [Elasticsearch](https://www.elastic.co) as a
**current-state mirror**: each source row becomes one document, upserted by its
primary key and versioned by the source position so the index converges to the
source's current state — idempotent under retries and safe against out-of-order
delivery.

```yaml
sinks:
  - type: elasticsearch
    config:
      id: es-orders
      url: "https://es:9200"           # HTTP(S) endpoint
      index: "cdc-{db}.{table}"        # template or static name
      auth:
        type: basic                    # basic | api_key | none
        username: elastic
        password: "${ES_PASSWORD}"     # ${ENV} expansion
```

## Configuration

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `id` | yes | — | Unique sink identifier |
| `url` | yes | — | Elasticsearch HTTP(S) endpoint (`${ENV}` supported) |
| `index` | yes | — | Index name or template — `{db}`, `{schema}`, `{table}` |
| `auto_create_index` | no | `true` | Create each index with a generated mapping on first use |
| `id_fields` | no | `[]` | Fields forming `_id`; empty → source primary key |
| `id_separator` | no | `_` | Separator joining composite `_id` values |
| `version_source` | no | `source_position` | External `version`: `source_position` (LSN/binlog) or `ts_ms` |
| `auth` | no | none | `basic` (`username`/`password`) or `api_key` (`api_key`); `${ENV}` supported |
| `tls.enabled` | no | `true` | TLS for `https://` endpoints |
| `tls.ca_file` | no | — | PEM CA bundle for a private CA |
| `tls.insecure_skip_verify` | no | `false` | Skip certificate verification |
| `send_timeout_secs` | no | `30` | Per-`_bulk` timeout (timeouts → backpressure) |
| `required` | no | `true` | Required (blocks) vs best-effort (log + continue) |

## How it writes

Each batch is sent as **one `_bulk` request**:

- Insert / update → `index` action carrying the source `after` document.
- Delete → `delete` action.
- Every action uses `version_type=external` with `version` = the source
  position, so Elasticsearch keeps the **highest version per `_id`**.

### Document `_id`

`_id` is built from `id_fields` if set, otherwise from the source table's
**primary key**, joined by `id_separator`. A table with no primary key and no
`id_fields` is rejected at startup — the sink never falls back to an
auto-generated id (which would silently break upsert and delete).

### Index naming

`index` is a template: `{db}`, `{schema}` (Postgres schema, falls back to the
database), and `{table}` are substituted per event, so one sink can span many
tables. A template with no placeholders is a static index name. Index names are
lowercased (an Elasticsearch requirement).

## Mapping — generated from the source schema

With `auto_create_index: true` (default), the sink creates each index on first
use with an **explicit mapping derived from the source column types** — so
decimals stay exact and dates stay dates, instead of the mistypes you get from
Elasticsearch dynamic mapping (decimals → `float`, dates → `text`). Set
`auto_create_index: false` to rely on a pre-created index or dynamic mapping.

| Source | Elasticsearch |
| --- | --- |
| `bigint` / `bigint unsigned` | `long` / `unsigned_long` |
| `int`, `smallint`, `tinyint` | `integer`, `short`, `byte` |
| `decimal(p,s)` | `scaled_float` (`scaling_factor = 10^s`) |
| `float` / `double` | `double` |
| `boolean` | `boolean` |
| `date` / `datetime` / `timestamp` | `date` |
| `json` | `flattened` |
| `varchar` / `text` / other | `text` with a `keyword` sub-field |

## Delivery guarantees

**At-least-once, idempotent.** The checkpoint advances only after the `_bulk`
acks, so nothing is lost; a crash between ack and checkpoint can replay a batch.
External versioning makes that safe:

- Replays and out-of-order batches converge to the source's current state —
  Elasticsearch keeps the highest `version` per `_id`.
- A stale write is rejected with `409 version_conflict`; the sink treats that as
  **success** (the document was already superseded), not a failure.

Per-document non-retryable errors (e.g. a mapping conflict) are routed to the
**DLQ** with their row's index; whole-batch retryable conditions (429/503,
timeouts, connection loss) return backpressure so the coordinator replays the
batch and the `required` policy is honored.

End-to-end exactly-once is **not** claimed — Elasticsearch writes are not
transactional with the DeltaForge checkpoint.

## Requirements

The sink needs the **source column types + primary key**. It loads these **on
demand** the first time it sees a table (querying the source catalog — MySQL
`INFORMATION_SCHEMA`, Postgres `pg_catalog`) and caches them, so it works under
**any snapshot mode**, including `snapshot: never`. If you set `id_fields`
explicitly *and* `auto_create_index: false`, no schema lookup is needed at all.

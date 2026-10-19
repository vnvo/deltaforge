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
| `auto_create_index` | no | `true` | Must be `true`: the sink writes only to indices it creates (see [Destination ownership](#destination-ownership-and-fencing)); `false` is rejected |
| `id_fields` | no | `[]` | Fields forming `_id`; empty → source primary key |
| `id_separator` | no | `_` | Separator joining composite `_id` values |
| `version_source` | no | `source_position` | External `version`: `source_position` (LSN/binlog) or `ts_ms` |
| `auth` | no | none | `basic` (`username`/`password`) or `api_key` (`api_key`); `${ENV}` supported |
| `tls.enabled` | no | `true` | Accepted for parity; TLS is active whenever `url` is `https://` |
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
**primary key**, joined by `id_separator`. Rows from a table with no primary key
and no `id_fields` are routed to the **DLQ** (a per-row serialization failure) —
the sink never falls back to an auto-generated id, which would silently break
upsert and delete.

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
| `date` / `datetime` / `timestamp` | `date` (format accepts epoch millis + ISO/MySQL strings) |
| `time` | `keyword` (a duration, not a point in time) |
| `blob` / `binary` / `varbinary` | `binary` (base64) |
| `json` | `flattened` |
| `varchar` / `text` / other | `text` with a `keyword` sub-field |

### Value handling

Source column values are normalized to match these mappings before indexing, so
real CDC payloads land correctly regardless of source path:

- **TEXT/BLOB** arrive base64-wrapped from the MySQL binlog (`{"_base64": …}`).
  Character columns are decoded to UTF-8 text; binary columns keep the base64
  string and map to an ES `binary` field.
- **`TIMESTAMP`** arrives as epoch **microseconds** and is converted to
  `epoch_millis`; string temporals (snapshot ISO-8601, binlog `DATETIME`) pass
  through and are parsed by the `date` field's format list.
- **`DECIMAL`** arrives as a string and is stored exactly via `scaled_float`.

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

## Destination ownership and fencing

Documents are keyed by their source table's primary key, so an index holds one source table. Give each source table its own index (an `index` template with `{schema}` and `{table}`, or `{db}` when several databases share a cluster).

**The sink writes only to indices it creates.** The index is created in one request, with:

- an immutable ownership marker in `mappings._meta.deltaforge`, naming:
  - the owner (the deployment, which is the state store's identity, plus the pipeline and the sink);
  - the source table (connector, source, database, schema, table);
  - the index (the cluster's `cluster_uuid` and the index name);
  - the encoding;
  - a random instance;
- the internal write alias `df-w-<instance>-<fence>`.

Every bulk action (index and delete) targets that alias, with `require_alias=true`. An index the sink did not create is refused, with no adoption, as is a name that is an alias or a data stream.

**Claims.** Before a session first writes to an index, and again after a reconnect or a client recreation, the sink *claims* it:

1. It proves the marker and the record in the state store agree.
2. It rotates the fence by one atomic `_aliases` request, which removes the old alias (`must_exist`) and adds the new one.

The state store keeps the index's instance and its current and pending fence for crash recovery; the index enforces. Claims never write `_meta`, and other `_meta` keys and user aliases are left alone. Between claims, writes make no ownership request.

**The fencing guarantee.** After a claim has rotated an index's fence, every newly evaluated write through the old alias is rejected by the cluster: index actions by `require_alias`, deletes by the auto-create rule below. A bulk request the cluster had already accepted may still complete, so writes around an ownership change are at least once, as everywhere else.

A rejected batch fails whole as `sink_destination_conflict` (class `fenced`):

- it is never dead-lettered;
- the checkpoint does not advance;
- the pipeline stops.

**Deployment requirement.** `require_alias` does not apply to delete actions. Deletes are fenced only when the cluster refuses to auto-create indices named `df-w-*`, for example:

```json
PUT _cluster/settings
{ "persistent": { "action.auto_create_index": "-df-w-*,+*" } }
```

Rules are evaluated in order, and the first match decides. Every claim reads the effective setting (transient, then persistent, then the default) and refuses with `fencing_unenforced` when it cannot read it or cannot prove that `df-w-*` is forbidden. The sink's user needs:

- the `monitor` cluster privilege, to read the setting;
- on its indices: `create_index`, `manage_aliases`, `write` and `view_index_metadata`.

**Clone and restore.**

- **Two separately created state stores** are two deployments: neither can write to the other's indices (`owned_by_other`).
- **A copied state store** has the same identity. The first copy to claim an index fences the other, whose next write fails at the cluster.
- **A state store restored from an older backup** is refused (`fenced`). Restored from its latest state, it continues.
- **A deleted index** is never recreated or auto-created (`destination_missing`). Its ownership record is removed only by deleting the pipeline, which resets the sink's checkpoints in the same operation.

**Refusals.** Every refusal is the `sink_destination_conflict` incident: nothing of the batch is acknowledged and the checkpoint stays. The incident names the index, the source table and the class, and only a digest of another owner; it never contains row data or credentials. The classes:

- `marker_missing`, `marker_malformed`, `marker_unsupported_version`;
- `owned_by_other`, `bound_to_another_relation`;
- `destination_identity_mismatch`, `encoding_mismatch`;
- `fenced`, `fence_malformed` (none, or several, internal aliases);
- `alias_not_supported`, `destination_missing`, `unrecorded_destination`;
- `fencing_unenforced`.

## Requirements

The sink needs the **source column types + primary key**. It loads these **on
demand** the first time it sees a table (querying the source catalog — MySQL
`INFORMATION_SCHEMA`, Postgres `pg_catalog`) and caches them, so it works under
**any snapshot mode**, including `snapshot: never`. The cluster must forbid
auto-creating `df-w-*` indices (see [Destination ownership](#destination-ownership-and-fencing)).

## Examples

**Elastic Cloud — API-key auth, one fixed index, explicit `_id`:**

```yaml
sinks:
  - type: elasticsearch
    config:
      id: es-customers
      url: "https://my-cluster.es.cloud:9243"
      index: customers               # static index (no placeholders)
      id_fields: [customer_id]       # override the source PK
      auth:
        type: api_key
        api_key: "${ES_API_KEY}"
```

**Self-hosted / dev — no auth, one index per source table:**

```yaml
sinks:
  - type: elasticsearch
    config:
      id: es-dev
      url: "http://elasticsearch:9200"
      index: "cdc-{db}.{table}"      # per-table indices
      auth:
        type: none
```

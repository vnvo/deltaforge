# Retained-state compatibility check

**Status:** design for review (release checklist C10). No implementation.

## Problem

A pipeline's durable state outlives its registration. Pipelines created through the API are not restored when the server restarts, and recreating the same name and source id is the restart mechanism: it resumes from the retained state. Today that state is adopted without comparing the new spec with the one that wrote it. Only the source server's identity and lineage are checked.

None of the position-bearing state is tenant-scoped. A spec of another tenant, with other tables, sinks or processing, therefore inherits the earlier pipeline's position and ownership:
- newly selected tables are captured from the resume position with no snapshot;
- new sinks start at the old resume position;
- changed processing is applied from the middle of a stream.

PATCH has the same gap on a running pipeline. It refuses only a change of the sink-id set and the per-table metrics cap.

Retained state is also wider than what DELETE removes (see the [inventory](#retained-state-inventory)). Some of it is reused even on a start with no checkpoints:
- a completed snapshot's anchor;
- a PostgreSQL slot's position;
- a legacy aggregate checkpoint.

So today, DELETE followed by a recreate with the same source id is not always a fresh start.

## Decisions

1. **Recreating stays the restart mechanism.** A compatible recreate resumes, as today.
2. **No reused state goes unprotected.** Every piece of retained state that a start can reuse is protected, either by the binding or by its own demonstrated equivalent check. A mismatch refuses; there is no warn-only mode and no override flag.
3. **The comparison is an allowlist.** Only fields classified as operational, credential, or fenced endpoint may differ. Every other field must match, including fields added later until they are classified.
4. **Endpoints, query parameters and headers are classified.** Credentials are removed, values that matter to the destination or to delivery are kept and compared, and unclassified ones are rejected.
5. **Changing `source.id` is a new source lifecycle.** PATCH refuses it.
6. **Existing stores** with retained state and no record are refused until one explicit, reviewed adoption binds them. There is no bulk mode and no history.

## The state binding

There is one record per source id: slot namespace `state_binding`, key `{source_id}`. It holds only the fields that must match, in canonical form:

```
StateBinding {
  format_version: 1,
  source_id, pipeline, tenant,
  engine,                    // mysql | postgres
  source: { ... },           // selection, processing, source contract
  sinks: { sink_id -> { type, destination, contract } },
  digest                     // SHA-256 over the canonical form above
}
```

- **Immutable for the life of the retained state it protects.** It is created on the first start that writes that state and removed only when that state is gone (see DELETE). A compatible change touches no recorded field, so PATCH never rewrites it.
- **Separate from the claim.** The claim's value stays the raw pipeline name.
- **No credentials.** Credential values never enter it (see Canonical form).

## Retained-state inventory

What a start can find, whether DELETE removes it, whether it is reused on a start without per-sink checkpoints, and what protects that reuse:

| State | Key | Removed by DELETE | Reused on a start without checkpoints | Protection |
|---|---|---|---|---|
| Per-sink checkpoints | `{source_id}::sink::*` | yes | (they are the checkpoints) | **binding** |
| Legacy aggregate checkpoint | `{source_id}` | no | yes, read as the resume position | **binding** |
| Destination ownership | `sink_destination_ownership/{pipeline}/{sink}/{destination}` | yes | yes, by the sink's claim | **binding**, plus the fence rule below |
| Snapshot generation control and plan | `snapshot_state/snapshot_generation:{source}`, `snapshot_plan/...` | no | yes. A completed generation is streamed from its anchor without comparing its fingerprint, and new sinks get only a non-blocking `sink_snapshot_incomplete` incident | **binding** |
| Snapshot progress | `snapshot_progress:{id}`, `mysql_snapshot_progress:{id}` | no | treated as reusable | **binding** |
| PostgreSQL slot owner and the slot | `slot_owner:{source_id}`; the server's slot | no | yes. A start without a snapshot reuses an existing slot's `confirmed_flush_lsn` with no ownership check | **binding** (the slot owner record marks retained state) |
| Recovery authorization | `recovery.authorization/pg_slot_recreation:{source}` | no | yes, by its recovery operation | **binding** |
| DLQ entries | `journal/{pipeline}:dlq` | no | yes, visible to the same name | **binding** |
| Schema history | `schemas.v1*`, keyed by tenant, source and lineage | no | only after its own check. MySQL writes a start barrier, so versions are reused only after a new proof or as candidates matched by signature under FULL metadata (MINIMAL fails closed). PostgreSQL reuses only identical-content version numbers, register-or-retrieve by fingerprint and then verified | own check |
| MySQL activation records | `schemas.v1.activation*` | no | superseded by the start barrier | own check |
| Schema lineage | `schema_lineage/{tenant}/{source_id}` | no | the same lineage is reused; another lineage rotates it | own check (server identity) |
| Failover identity | `failover/identity:{source_id}` | no | yes. MySQL treats another server as a failover and needs GTID coverage; PostgreSQL refuses another cluster | own check (supported failover) |
| PostgreSQL continuity | `failover/pg_continuity:{source_id}` | no | the same cluster is reused; another cluster refuses | own check |
| Publication registration | `{tenant}/{source_id}/{publication}` | no | yes. A changed digest refuses; a start before `registered_lsn` refuses | own check (live registration); the publication name is in the binding |
| Replay stream | `{pipeline}:{incarnation}:replay` | orphaned | no: the incarnation slot is removed | n/a |
| Kafka transactional id | `deltaforge-{pipeline}-{sink.id}` | n/a | fences the earlier producer | n/a (name and sink id are in the binding) |
| S3 durable_v2 HEAD | in the bucket under `prefix` | no | yes, acquired at start | **binding** (bucket, prefix, endpoint) |

**Binding-protected state** means the rows marked **binding**. The binding is authoritative while any of it exists for its source id or pipeline.

## Fences: what they establish

- **Source.** The failover identity, the lineage, PostgreSQL continuity and the publication registration verify that the source is the same server, cluster and publication, or a supported failover. That establishes compatibility of the endpoint for the retained position. So the source DSN host and port are not recorded; the PostgreSQL database name is.
- **ClickHouse.** The ownership key is `clickhouse/{db}.{table}` and the record carries the fence, the destination instance and the server identity. With a changed `url`:
  - a missing table with an existing record refuses (`destination_missing`);
  - a table without a marker refuses (`marker_missing`);
  - another marker or instance refuses (`owned_by_other`, `bound_to_another_relation`, `destination_identity_mismatch`);
  - another server refuses (`server_changed`).

  A changed endpoint therefore never selects an empty destination silently, and the ClickHouse `url` host and port are not recorded. Its `database` and `table` are.
- **Elasticsearch: not sufficient today.** The ownership key includes the cluster UUID (`elasticsearch/{cluster_uuid}/{index}`). A `url` pointing at another cluster finds no record under the new key, and the claim **creates a fresh empty index** and writes from the retained checkpoint, with no backfill. Two changes close this:
  1. **The Elasticsearch credential-free `url` is recorded in the binding.** A change refuses.
  2. **A fence rule in the ownership claim:** a claim that finds no record for its destination refuses (`destination_changed`) when the same `{pipeline}/{sink}` holds a record for another destination. That covers a DNS change behind an unchanged `url`, for both sinks. It is a small change in `ownership.rs`, in scope for C10.
- **Kafka, Redis, NATS, HTTP, S3: no fence.** Their credential-free endpoint is recorded, and a change refuses.

## What must match

| Area | Fields |
|---|---|
| Owner | `metadata.tenant`, `metadata.name`, `source.id`, engine |
| MySQL selection | `tables`, `table_options`, `outbox.tables`, `sharding` |
| PostgreSQL selection | `publication`, `slot`, `tables`, `table_options`, `outbox` prefixes, `sharding`, the DSN's database name |
| Source contract | `type_mapping`, `on_schema_drift` |
| Processing | `processors` (in order), each sink's `filter`, `schema_sensing.output.emit_events` |
| Delivery policy | `commit_policy` mode and quorum, each sink's `required`, `journal.enabled` |
| Sink set | The set of sink ids |
| Sink destination | Kafka `topic`, `brokers`, and the destination keys of `client_conf`; Redis `stream`, `uri`; NATS `subject`, `stream`, `url`; HTTP `url`, `method`, and the destination headers; S3 `bucket`, `prefix`, `region`, `endpoint`; ClickHouse `database`, `table`, `mode`, and the destination query parameters of `url`; Elasticsearch `url`, `index`, `id_fields`, `id_separator` |
| Sink contract | `type`; `envelope`, `encoding`, `key`, `json_wide_integers`, `exactly_once`, and the delivery keys of `client_conf`; S3 `format`, `compression`, `durability`; ClickHouse and Elasticsearch `version_source` |

**Selection changes refuse in both directions.** Safe reconfiguration can come later as a separate, explicit operation.

## What may differ (the allowlist)

- **Operational:**
  - `batch`, `sink_batch_deadline_secs`, `commit_policy` timeouts, `connection_policy`;
  - `journal` other than `enabled`;
  - `schema_sensing` other than `output.emit_events`;
  - `metrics` (the per-table cap keeps its own check);
  - MySQL `proofs` bounds; snapshot parallelism, chunk and lock knobs;
  - `snapshot.mode` (it acts only on a first start, and binding-protected snapshot state makes it subject to the binding anyway);
  - `max_message_bytes`;
  - sink timeouts, roll and flush settings, `encode_workers`;
  - the operational keys of `client_conf`.
- **Credentials:**
  - inline DSN user and password, `dsn_secret`, `credentials`, the `secrets` providers and `rotation`;
  - sink `*_ref`, `secret_refs` and `auth`;
  - the credential components classified below.
- **Fenced endpoints:** the source DSN host and port; the ClickHouse `url` host and port.

A test enumerates every spec field and fails if one is in neither list. An unclassified field is compared, so it refuses until someone classifies it.

## Canonical form and classification

### Endpoints, query parameters and headers

An endpoint is canonicalized as scheme, lowercased host, port (with the scheme's default filled in) and path. Userinfo is a credential and is removed; the fragment is removed. Every query parameter, every HTTP header and every Kafka `client_conf` key falls in one of four classes:

| Class | Meaning | Treatment |
|---|---|---|
| credential | authenticates; its value is secret | removed before storing and comparing; never shown |
| destination | selects where data goes | kept, canonical value compared |
| delivery | changes what or how data is delivered | kept, canonical value compared |
| operational | neither | ignored |

Classification is per sink, in code next to the sink config:

- **HTTP:**
  - **Headers.** Credentials: `Authorization`, `Proxy-Authorization`, `Cookie`, and any header supplied through `secret_refs`. Delivery: `Content-Type`, `Content-Encoding`. Everything else is unclassified.
  - **Query parameters.** None are built in. DeltaForge does not interpret them, and the remote endpoint may.
- **ClickHouse URL query parameters:**
  - credential: `user`, `password`;
  - destination: `database`. It must also agree with the `database` field, and a conflict is a validation error;
  - DeltaForge-owned and refused in configuration: `query`, `insert_deduplication_token`, `async_insert`, `wait_for_async_insert`;
  - everything else is unclassified.
- **Elasticsearch URL, NATS URL, S3 endpoint:** no query parameters are classified, so any query parameter is unclassified.
- **Redis URI:**
  - userinfo is a credential;
  - the database index in the path, and the `db` parameter, are destination;
  - `protocol` is delivery;
  - everything else is unclassified.
- **Kafka `client_conf`:** librdkafka's configuration keys are a finite documented set and are classified from it:
  - credentials: `sasl.password`, `sasl.username`, `ssl.key.password`, `ssl.keystore.password`, `sasl.oauthbearer.client.secret`, and the like;
  - destination: `bootstrap.servers`;
  - delivery: `acks`, `enable.idempotence`, `transactional.id`, `compression.type`, `partitioner`, `message.max.bytes`, and the like;
  - operational: the rest.

**An unclassified query parameter, header or `client_conf` key is rejected** at create and PATCH validation, with the name and the way to classify it.

**The spec can classify additional names** for HTTP query parameters and headers, Redis and ClickHouse query parameters, through an optional per-sink map `classify: { <name>: destination | delivery | credential }`. Built-in classes cannot be overridden. Rejection then never erases a compatibility difference silently, and redaction never removes a value that matters to the destination or delivery.

### Other canonical rules

- **Sets are sorted and deduplicated:** table patterns, outbox tables and prefixes, the sink-id set, `id_fields`, Kafka broker lists.
- **Maps are sorted by key:** `table_options`, sinks by sink id, kept headers and query parameters. Header names are lowercased.
- **Order is kept only where it changes behavior:** `processors`.
- **Defaults are made explicit:** an omitted field and the same field set to its default compare equal.
- **Credential fields and credential components are dropped before canonicalization.**

## Redaction

The binding and every refusal contain only canonical values, which by construction exclude credential components. A refusal names each differing field with its stored and new canonical value. As a backstop, refusal text also passes through the existing configuration sanitizer (userinfo passwords, `password=` pairs) before it is logged or returned.

Tests plant each of these and assert that none appears in the binding or in any refusal:
- a password in userinfo;
- a ClickHouse `password` query parameter;
- an `Authorization` header;
- a Kafka `sasl.password`.

## When the check runs, and crash behavior

The process-local lifecycle lock serializes operations within one server; the store gate allows one server per store. Durable consistency comes from the order of durable writes, the atomicity of slot operations (`slot_create` is create-if-absent; `slot_cas` and `slot_delete` act on a version), and one rule:

> **A binding is authoritative while any binding-protected state (inventory) exists for its source id or pipeline.** Only a binding with none of it is stale, and only a stale binding is replaced.

### Create and start

The check runs in `start_pipeline_locked`, after `held_for_recovery` and before `claim_source_id`. That is before anything reads or writes checkpoints, ownership, snapshot state, the PostgreSQL slot, the S3 HEAD or other source state.

1. Read the binding, and check for binding-protected state. This is store reads only, with no connection to the source or sinks.
2. Act on what was found:
   - **Binding present, protected state present:** compare canonical forms. A difference refuses with `409 retained_state_incompatible` (fields listed, redacted), and nothing is claimed, spawned or written.
   - **Binding present, no protected state:** the binding is stale. Replace it with `slot_cas` on its version.
   - **Binding absent, protected state present:** refuse with `retained_state_unbound`, naming the adoption command.
   - **Binding absent, no protected state:** a first start. Create the binding with `slot_create`.
3. Claim the source id, then spawn.

A crash after step 2 leaves a binding that either has no protected state (stale, so replaced or found equal next time) or matches the spec that is about to write it.

### PATCH

The merged spec is canonicalized, validated against the classification (unclassified names reject), and compared with the binding before anything is stopped:
- a change of `source.id` refuses explicitly;
- any other recorded difference refuses with `retained_state_incompatible`.

PATCH never writes the binding, so a crash during PATCH leaves it as it was.

### DELETE

DELETE keeps its fail-closed order (replay incarnation, stop, checkpoints, ownership records, claim). Then it removes the binding **only if no binding-protected state remains**: no legacy aggregate checkpoint, snapshot state, snapshot progress, PostgreSQL slot owner, recovery authorization or DLQ entries. Otherwise the binding stays, and a later create with the same source id must match it, or use a new source id.

The refusal and the API reference say this explicitly. Today's DELETE leaves that state behind, so DELETE plus recreate is not a fresh start for it.

Whether DELETE should also remove those records, and drop the PostgreSQL slot, is a separate decision. It is not part of C10.

Crash points:
- **Before the claim is released:** the delete is retried as today.
- **After the claim is released, before the binding is removed:** the binding is stale exactly when no protected state remains, and is then replaced at the next first start.

### Startup `--config` pipelines

Same check as create. A refusal fails that pipeline closed with an incident naming the differing fields; the other pipelines continue.

## Adopting an existing source

A store written before this change can hold binding-protected state with no binding. Every create, PATCH or startup of that source id then refuses with `retained_state_unbound`. There is one explicit operation per source id, following the schema-history migration:

1. **`deltaforge state-binding adopt --spec <file> --dry-run`** lists the source id's binding-protected state from the inventory.

   Against the spec it checks:
   - sink ids against checkpoint keys;
   - the pipeline name against the claim, the ownership keys and the DLQ key;
   - the tenant against the lineage key;
   - the snapshot generation's fingerprint against the spec's engine and table patterns;
   - the PostgreSQL slot owner's slot, database and pipeline against the spec.

   Any mismatch refuses the adoption. It prints the binding it would write, with redacted canonical values, and a proof digest over the state found and that binding. Fields with no retained record to check against are listed as adopted on the operator's statement.
2. **`--apply --expect-proof <digest>`** writes the binding with `slot_create`, under the store gate, only if the store still produces the same proof, and writes an audit record.

The release notes make this an upgrade step for every source that already exists.

## Tests the implementation must carry

- **Same spec:** a recreate resumes, and the binding is unchanged.
- **Allowlist:** each allowlisted field changed, and credentials rotated, resumes with the binding unchanged.
- **Each recorded field, changed alone:** generated from the table. On recreate it refuses, and nothing is claimed, spawned or written. On PATCH it refuses, and nothing is stopped.
- **Classification:** an unclassified query parameter, header or `client_conf` key is rejected. A declared one is compared. A built-in credential changes nothing; a built-in destination or delivery value refuses.
- **Ordering and defaults:** reordered sets and maps and omitted defaults compare equal; reordered processors refuse.
- **Specific refusals:**
  - another tenant with the same name and source id;
  - a Kafka, Redis, NATS, HTTP, S3 or Elasticsearch endpoint change;
  - a `source.id` change through PATCH.
- **Elasticsearch:** a claim against another cluster with a retained record for the same sink refuses (`destination_changed`), and no index is created.
- **Retained state after DELETE:** the legacy aggregate checkpoint, a completed snapshot generation, a PostgreSQL slot owner and DLQ entries each keep the binding authoritative, so a different spec refuses. With none of them left, a different spec binds.
- **Redaction:** the four planted credentials never appear in the binding or in any refusal.
- **Crash points:** each one above with an injected failure, then the specified next start.
- **Adoption:** unbound protected state refuses. Dry run and apply bind it. A store changed between the two fails the apply. A mismatching sink id, claim, fingerprint or slot owner refuses the adoption.
- **Unclassified spec field:** fails the classification test.

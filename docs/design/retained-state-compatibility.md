# Retained-state compatibility check

**Status:** design for review (release checklist C10). No implementation.

## Problem

A pipeline's durable state outlives its registration. Pipelines created through the API are not restored when the server restarts, and recreating the same name and source id is the restart mechanism: it resumes from the retained state. Today that state is adopted without comparing the new spec with the one that wrote it. Only the source server's identity and lineage are checked.

The state that carries a position is:
- the per-sink checkpoints `{source_id}::sink::*`;
- the destination ownership records `sink_destination_ownership/{pipeline}/...`;
- the source-id claim `active_sources/{source_id}`, whose value is the pipeline name.

None of it is tenant-scoped. A spec of another tenant, with other tables, sinks or processing, therefore inherits the earlier pipeline's position and ownership:
- newly selected tables are captured from the resume position with no snapshot;
- new sinks start at the old resume position;
- changed processing is applied from the middle of a stream.

PATCH has the same gap on a running pipeline. It refuses only a change of the sink-id set and the per-table metrics cap, so tenant, selection, destinations, processing and `source.id` can all be patched.

## Decisions

1. **Recreating stays the restart mechanism.** A compatible recreate resumes, as today.
2. **Before any retained state is adopted,** the spec is compared with a durable record of the spec that wrote it. A mismatch refuses; there is no warn-only mode and no override flag.
3. **The comparison is an allowlist.** Only fields classified as operational, credential, or fenced endpoint may differ. Every other field must match, including fields added later until they are classified.
4. **Changing `source.id` is a new source lifecycle.** PATCH refuses it, as it refuses a change of the sink set.
5. **Existing stores** with retained state and no record are refused until one explicit, reviewed adoption binds them. There is no bulk mode in this change.

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

- **Immutable for the life of the retained state.** It is created on the first start that writes it and removed by DELETE. Because a compatible change by definition touches no recorded field, PATCH never rewrites it and there is no history.
- **Separate from the claim.** The claim's value stays the raw pipeline name, so existing claims and the claim's equality check are untouched.
- **No credentials.** Credential fields are not part of the binding (see Canonical form).

## What must match

| Area | Fields |
|---|---|
| Owner | `metadata.tenant`, `metadata.name`, `source.id`, engine |
| MySQL selection | `tables`, `table_options`, `outbox.tables`, `sharding` |
| PostgreSQL selection | `publication`, `slot`, `tables`, `table_options`, `outbox` prefixes, `sharding`, the DSN's database name |
| Source contract | `type_mapping`, `on_schema_drift` |
| Processing | `processors` (in order), each sink's `filter`, `schema_sensing.output.emit_events` |
| Delivery policy | `commit_policy` mode and quorum, each sink's `required`, `journal.enabled` (whether a failing event is set aside instead of blocking) |
| Sink set | The set of sink ids |
| Sink destination | Kafka `topic` and the credential-free `brokers`; Redis `stream` and the credential-free `uri`; NATS `subject`, `stream` and the credential-free `url`; HTTP the credential-free `url` and `method`; S3 `bucket`, `prefix`, `region`, `endpoint`; ClickHouse `database`, `table`, `mode`; Elasticsearch `index`, `id_fields`, `id_separator` |
| Sink contract | `type`; `envelope`, `encoding`, `key`, `json_wide_integers`, `exactly_once`; S3 `format`, `compression`, `durability`; ClickHouse and Elasticsearch `version_source` |

**Endpoints:**
- **Sources.** The source endpoint (DSN host and port) is not recorded. The source has its own identity fence: MySQL compares the server identity and needs GTID coverage for another server; PostgreSQL refuses another cluster through its continuity chain and slot owner. A supported failover therefore needs no binding change.
- **Sinks with a fence.** ClickHouse and Elasticsearch verify the destination instance through an ownership marker with a random instance id, also recorded in the store. Their `url` is therefore not recorded either.
- **Every other sink.** Kafka, Redis, NATS, HTTP and S3 have no such fence. Their credential-free endpoint is recorded, and an endpoint change refuses; it is never assumed to reach the same destination. Kafka `client_conf` keys that change the destination or the transaction identity (`bootstrap.servers`, `transactional.id`) are compared as part of the destination and contract.

**Selection changes refuse in both directions.** Adding tables would leave them without history; removing them would let a later re-add silently skip the interval in between. Safe reconfiguration can come later as a separate, explicit operation.

## What may differ (the allowlist)

- **Operational:**
  - `batch`, `sink_batch_deadline_secs`, `commit_policy` timeouts, `connection_policy`;
  - `journal` settings other than `enabled`;
  - `schema_sensing` other than `output.emit_events`;
  - `metrics` (the per-table cap keeps its own check);
  - MySQL `proofs` bounds; snapshot parallelism, chunk and lock knobs;
  - `snapshot.mode` (it only acts on a first start);
  - `max_message_bytes`;
  - sink timeouts, roll and flush settings, `encode_workers`;
  - Kafka `client_conf` other than the two keys above.
- **Credentials:**
  - inline DSN user and password, `dsn_secret`, `credentials`, the `secrets` providers and `rotation`;
  - sink `*_ref` and `secret_refs`;
  - userinfo and query strings in sink endpoints;
  - HTTP `headers`.
- **Fenced endpoints:** the source DSN host and port; ClickHouse and Elasticsearch `url`.

A test enumerates every spec field and fails if one is in neither list. An unclassified field is compared, so it refuses until someone classifies it.

## Canonical form

The binding and the comparison use the same canonical form, so that irrelevant differences never refuse:

- **Sets are sorted and deduplicated:** table patterns, outbox tables and prefixes, the sink-id set, `id_fields`.
- **Maps are sorted by key:** `table_options`, sinks by sink id.
- **Order is kept only where it changes behavior:** `processors`.
- **Defaults are made explicit:** a field omitted and the same field set to its default compare equal.
- **Endpoints are reduced to scheme, lowercased host, port (explicit, with the scheme's default filled in) and path.** Userinfo, query string and fragment are removed before storing and before comparing.

Credential fields are dropped before canonicalization. They are never stored, never compared and never shown.

## Redaction

Stored values and refusal messages contain only canonical, credential-free values. A refusal names each differing field with its stored and new canonical value. A value that would contain a credential is not in the canonical form in the first place: endpoint userinfo, query strings and headers are removed, and secret references are excluded.

As a backstop, refusal text passes through the existing DSN redaction before it is logged or returned. A test plants a password, a token in a query string and an `Authorization` header in a spec and asserts that none appears in the binding or in any refusal.

## When the check runs, and crash behavior

The process-local lifecycle lock serializes create, PATCH and DELETE within one server. Across servers, the store gate allows only one server per store. Durable consistency comes from the order of durable writes, the slot operations' atomicity (`slot_create` is create-if-absent; `slot_cas` and `slot_delete` act on a version), and one rule:

> **A binding is authoritative only while position-bearing state exists for its source id.** That means a `{source_id}::sink::*` checkpoint, or a destination ownership record of the binding's pipeline. A binding without such state is stale and is replaced at the next first start.

This rule makes every crash point below recoverable without operator action.

### Create and start

The check runs in `start_pipeline_locked`, after `held_for_recovery` and before `claim_source_id`, so it runs before anything reads or writes checkpoints, ownership, the S3 HEAD or source state.

1. Read the binding, and check for position-bearing state.
2. Act on what was found:
   - **Binding present, position-bearing state present:** compare canonical forms. A difference refuses with `409 retained_state_incompatible` (fields listed, redacted), and nothing is claimed, spawned or written. Equal means proceed.
   - **Binding present, no position-bearing state:** the binding is stale. Replace it with `slot_cas` on its version.
   - **Binding absent, position-bearing state present:** refuse with `retained_state_unbound`. The error names the adoption command.
   - **Binding absent, no position-bearing state:** a first start. Create the binding with `slot_create`.
3. Claim the source id, then spawn.

Crash points:
- **After the binding is written, before the claim:** a binding with no position-bearing state. The next start replaces it, or finds it equal.
- **After the claim, before the first checkpoint:** the same, plus a held claim for the same name, which the existing claim logic treats as a restart.
- **After the first checkpoint:** a binding with state, compared on every later start.

### PATCH

The merged spec is canonicalized and compared with the binding before anything is stopped:
- a change of `source.id` refuses explicitly;
- any other recorded-field difference refuses with `retained_state_incompatible`;
- an allowed change proceeds as today.

PATCH never writes the binding, so a crash during PATCH leaves the binding as it was. The running pipeline's spec stays within the allowlist of it.

### DELETE

The existing fail-closed order is kept, and the binding is removed last:

1. replay incarnation;
2. stop;
3. checkpoints;
4. ownership records;
5. claim;
6. binding.

Crash points:
- **Before step 5:** the claim is held and the delete is retried as today. The binding still matches whatever state remains.
- **Between steps 5 and 6:** no claim and no position-bearing state, so the binding is stale and is replaced at the next first start.

A delete never removes the binding while checkpoints or ownership records remain.

### Startup `--config` pipelines

Same check as create. A refusal fails that pipeline closed with an incident naming the differing fields; the other pipelines continue.

### Out of scope

- **Records not removed by DELETE:** the failover identity, lineage, PostgreSQL continuity and slot owner records, and snapshot generation control. They are not position-bearing in the sense above and keep their own checks. Whether DELETE should remove some of them is a separate question.
- **Schema history and MySQL activation records:** a first start invalidates them as proof with its start barrier.

## Adopting an existing source

A store written before this change can hold position-bearing state with no binding. Every create, PATCH or startup of that source id then refuses with `retained_state_unbound`. There is one explicit operation per source id, following the schema-history migration (dry run, proof digest, apply with the expected proof, under the store gate, never deleting):

1. **`deltaforge state-binding adopt --spec <file> --dry-run`** lists the source id's position-bearing state:
   - the checkpoints, by sink id, with their positions;
   - the ownership records;
   - the claim and its pipeline name.

   Against the spec it checks:
   - the sink ids against the checkpoint keys;
   - the pipeline name against the claim and the ownership keys;
   - the tenant against the lineage key, where that record exists.

   Any mismatch refuses the adoption. It prints the binding it would write, with redacted canonical values, and a proof digest over the state found and that binding. Fields with no retained record to check against (selection, processing, destinations) are listed as adopted on the operator's statement.
2. **`--apply --expect-proof <digest>`** writes the binding with `slot_create`, only if the store still produces the same proof, and writes an audit record.

The release notes make this an upgrade step for every source that already exists.

## Tests the implementation must carry

- **Same spec:** a recreate resumes, and the binding is unchanged.
- **Allowlist:** a recreate with each allowlisted field changed, and with credentials rotated, resumes, and the binding is unchanged.
- **Each recorded field, changed alone:** generated from the table. On recreate, it refuses with that field named, and nothing is claimed, spawned or written. On PATCH, it refuses with nothing stopped.
- **Ordering and defaults:** reordered table patterns, sinks or `table_options`, and an omitted default, compare equal. Reordered processors refuse.
- **Specific refusals:**
  - another tenant with the same name and source id;
  - a Kafka broker list, Redis URI, NATS URL, HTTP URL or S3 endpoint change;
  - a `source.id` change through PATCH.
- **Redaction:** the planted password, token and header never appear in the binding or in a refusal.
- **Crash points:** each one above, with an injected failure, then a next start that resumes or replaces as specified.
- **Unbound state:** position-bearing state with no binding refuses. Dry run and apply bind it, a store changed between the two fails the apply, and an adoption whose sink ids or claim do not match refuses.
- **Classification:** an unclassified new spec field fails the classification test.

# Retained-state compatibility check

**Status:** design for review (release checklist C10). No implementation.

## Problem

A pipeline's durable state outlives its registration. Pipelines created through the API are not restored when the server restarts, and recreating the same name and source id is the restart mechanism: it resumes from the retained state. Today that state is adopted without comparing the new spec with the one that wrote it. Only the source server's identity and lineage are checked.

- **Retained state, keyed by source id:**
  - per-sink checkpoints `{source_id}::sink::*`;
  - the source-id claim `active_sources/{source_id}`, whose value is the pipeline name;
  - the failover identity `failover/identity:{source_id}`;
  - PostgreSQL slot owner and continuity records.
- **Retained state, keyed by tenant and source id:** the schema lineage `schema_lineage/{tenant}/{source_id}` and the schema history (`schemas.v1*`, with MySQL activation records).
- **Retained state, keyed by pipeline name:** destination ownership `sink_destination_ownership/{pipeline}/{sink}/...`.

Checkpoint keys and the claim are not tenant-scoped. A spec of another tenant, with other tables or other sinks, therefore inherits the earlier pipeline's position and destination ownership:
- newly selected tables are captured from the resume position with no snapshot;
- new sinks start at the old resume position;
- the old sinks' orphaned checkpoints can hold the resume position back.

PATCH has the same gap on a running pipeline. It refuses only a change of the sink-id set and the per-table metrics cap, so tenant, table selection, destinations and `source.id` can all be patched.

## Decisions this design follows

1. **Recreating stays the restart mechanism.** A compatible recreate resumes, as today.
2. **Before any retained state is adopted,** tenant, source selection and sink identities are checked against a durable record of the spec that wrote the state.
3. **A mismatch refuses;** it never only warns.
4. **The check tells apart** compatible operational changes, credential rotation, supported failover, and changes that need a migration or fresh state.
5. **Stores without a recorded spec** are handled explicitly.

## The state binding

There is one durable record per source id. It sits in the existing slot table: namespace `state_binding`, key `{source_id}`. It is written with `slot_create` and updated with `slot_cas`.

```
StateBinding {
  format_version: 1,
  source_id, pipeline, tenant,
  engine: mysql | postgres,
  selection: { ... canonical identity fields of the source, below ... },
  sinks: { sink_id -> { type, destination: { ... }, contract: { ... } } },
  bound_at, bound_by_version,
  history: [ { at, by_version, kind: created | operational | credential | failover_endpoint, fields: [...] } ]   // bounded, newest last
}
```

- **Canonical fields, not just a digest.** A refusal can then name every differing field and its old and new value. Credential material is never recorded: no user, password, secret value or secret location.
- **Separate from the claim.** The claim's value stays the raw pipeline name. Existing claims keep working, and the claim's legacy equality check is untouched.
- **Keyed by source id**, the same collision domain as the checkpoints and the claim. The tenant is a recorded field, so a different tenant refuses.

## Classification

Every spec field falls in exactly one class, and the implementation keeps the table in code next to the spec types. A new spec field fails a test until it is classified, so nothing is compatible by omission.

### Must match: a mismatch refuses

| Area | Fields |
|---|---|
| Owner | `metadata.tenant`, `metadata.name`. A different name with the same source id is already refused by the claim. |
| Source kind | Engine (`mysql` or `postgres`), `source.id` |
| MySQL selection | `tables`, `table_options`, `outbox.tables`, `type_mapping` |
| PostgreSQL selection | `publication`, `slot`, `tables`, `table_options`, `outbox` prefixes, `type_mapping`, the DSN's database name |
| Partitioning | `sharding` (it decides which part of the source this pipeline owns) |
| Sink set | The set of sink ids |
| Per sink, destination | Kafka `topic`; Redis `stream`; NATS `subject`, `stream`; HTTP `url` path and `method`; S3 `bucket`, `prefix`; ClickHouse `database`, `table`, `mode`; Elasticsearch `index`, `id_fields`, `id_separator` |
| Per sink, consumer contract | `type`; `envelope`, `encoding`, `key`, `json_wide_integers`, `exactly_once` (Kafka: the transactional id derives from the pipeline and sink id); S3 `format`, `compression`, `durability`; ClickHouse and Elasticsearch `version_source` |

These changes make the retained position wrong, or wrong for some tables, sinks or consumers:
- added tables have no history before the resume position;
- a new destination has nothing before it;
- a changed key, envelope or encoding mixes two contracts in one destination.

Each one needs a fresh state or an explicit migration (see Refusal).

### Compatible: adopted, recorded in `history`

- **Operational:**
  - `batch`, `commit_policy` timeouts, `sink_batch_deadline_secs`, `connection_policy`, `journal`, `schema_sensing`, `metrics` (the per-table cap keeps its own check);
  - MySQL `proofs` bounds;
  - snapshot parallelism, chunk and lock knobs;
  - `snapshot.mode` (it only acts on a first start);
  - `max_message_bytes`;
  - sink timeouts, roll and flush settings, `encode_workers`;
  - `on_schema_drift` (a halting or adapting policy, not a position).
- **Delivered content from the resume position on:** `processors`, the sink `filter`, a sink's `required` flag, and `commit_policy` mode and quorum. These change what is delivered next, but not whether the retained position is valid for the selection. They are compatible and recorded, so the change is visible in the binding history and the audit log.
- **Credential rotation:** an inline DSN's user and password, `dsn_secret`, `credentials`, sink `*_ref` and `secret_refs`, `secrets` providers, and `rotation`. They are excluded from the binding entirely, so they never refuse. A whole-DSN secret hides its endpoint as well; that is acceptable because the endpoint is not part of the binding.
- **Endpoint changes:** source DSN host and port, Kafka `brokers`, Redis `uri`, NATS `url` host, ClickHouse and Elasticsearch `url`, S3 `endpoint` and `region`. These are not in the binding.
  - For the source, supported failover keeps its existing gates. MySQL compares the server identity and needs GTID coverage for another server; PostgreSQL refuses another cluster through the continuity chain and the slot owner. A changed endpoint with the same server is an operational move. The binding records the change kind `failover_endpoint` when the source's identity check reports a transition.
  - For sinks, destination ownership fencing (#164) already detects another destination instance where it is implemented. Kafka `client_conf` is operational, except two keys: `bootstrap.servers` follows `brokers`, and `transactional.id` follows the consumer contract.

## Where the check runs

**Create and start:** in `start_pipeline_locked`, under the lifecycle lock, after `held_for_recovery` and before `claim_source_id`. At that point nothing has yet read or written checkpoints, ownership, the S3 HEAD or source state.

1. Read `state_binding/{source_id}`.
2. **Binding present:** compare.
   - **Identical or compatible:** proceed. A compatible difference is written with `slot_cas` before the claim, so a crash between the two leaves a binding that already matches the running spec.
   - **Must-match difference:** refuse with `409 Conflict`, code `retained_state_incompatible`. The body lists every differing field (old and new value, credential fields never shown) and the remediations below. Nothing is claimed, spawned or written.
3. **Binding absent:** look for retained state for this source id. That means any `{source_id}::sink::*` checkpoint, the claim, destination ownership records under the name, the failover identity, a PostgreSQL slot owner or continuity record, or snapshot generation control.
   - **None:** a first start. Create the binding with `slot_create` before the claim. The create-if-absent makes two concurrent first starts settle on one binding.
   - **Some:** the store predates the binding, or it was written by an unclean path. Refuse with `retained_state_unbound` unless the store was adopted (see Existing stores).

**PATCH:** the same comparison, between the binding and the merged spec, before anything is stopped. A must-match change refuses with the same error. That closes the PATCH path for tenant, selection, destination and `source.id`. The sink-set rule stays as is.

**Startup `--config` pipelines:** the same check. A refusal fails that pipeline closed, with an incident whose class names the differing fields, and the other pipelines continue.

**DELETE:** removes the binding in the same fail-closed sequence, after the ownership records and before the claim. A delete that fails keeps the binding, and so does the claim.

Schema history and activation records left after a DELETE do not count as retained state. A later first start writes a start barrier, so they are never reused as proof.

## Refusal and remediation

The refusal names the path that fits the change:

- **Different tenant or source, or a reused source id:** use a new source id.
- **Changed table selection:**
  - to keep the position for the unchanged tables and copy the added ones, a future explicit selection change (outside this design);
  - today, a new source id, or `DELETE` and recreate (a first start that snapshots according to `snapshot.mode`).
- **Changed sink set, destination or contract:** `DELETE` and recreate, or a new source id. This matches today's rule for sink-set changes.

No override flag adopts incompatible state. The only path that binds state the server would otherwise refuse is the reviewed adoption below.

## Existing stores (retained state, no binding)

A store written before this change has checkpoints, claims and ownership records, but no binding. Refusing silently would stop every existing pipeline at upgrade, and adopting silently would keep the gap open. Adoption is therefore explicit and reviewed. It follows the schema-history migration (dry run, proof digest, `--apply --expect-proof` under the store gate, never deleting):

1. **`deltaforge state-binding adopt --spec <file> --dry-run`** lists, for the spec's source id, the retained state found:
   - the checkpoints with their positions;
   - the claim and its pipeline name;
   - the ownership records;
   - the lineage, the identity and the PostgreSQL records;
   - the snapshot generation and its `config_fingerprint`.

   It also shows the binding it would write and a proof digest over all of that. Where retained records carry identity of their own, it checks them against the spec, and a mismatch is reported as a refusal of the adoption:
   - the claim's pipeline name;
   - the lineage `tenant/source_id` key;
   - the PostgreSQL slot owner's slot, database and pipeline;
   - the publication registration digest;
   - the snapshot `config_fingerprint` over engine and table patterns.

   Fields with no retained record to check against, such as sink destinations, are adopted on the operator's statement and listed as such.
2. **`--apply --expect-proof <digest>`** writes the binding only if the store still matches the dry run, with an audit record.
3. **Until adopted,** a create, PATCH or startup of that source id refuses with `retained_state_unbound`, and the error names the command. The release notes make this an upgrade step for pipelines that already exist.

A store with no retained state for the source id needs nothing: its first start binds.

## Interactions

- **Snapshot generation fingerprint:** the existing `config_fingerprint` stays. The binding is checked first, so a selection change refuses before the snapshot logic sees it.
- **Failover and continuity:** unchanged. The binding only adds the facts these checks never compared (tenant, selection, sinks).
- **Destination ownership:** unchanged and still keyed by pipeline name. A same-name recreate with a different destination now refuses at the binding instead of relying on the first write's fence.
- **Docs:** `docs/src/secrets.md` says rotation needs a restart, while the code has opt-in live rotation. That is to be reconciled separately. The binding treats both alike.

## Tests the implementation must carry

- A recreate with the same spec resumes, and its binding is unchanged.
- A recreate with a changed operational field or credential resumes, with one history entry, or none for a credential.
- Each must-match field, changed alone on recreate, refuses with that field named, and with no claim, checkpoint, ownership or source state touched. This is generated from the classification table.
- The same for PATCH, with nothing stopped.
- A different tenant with the same name and source id refuses (the cross-tenant case).
- Retained state with no binding refuses with `retained_state_unbound`. Dry run and apply bind it, and a changed store between the two fails the apply.
- DELETE removes the binding, and a failed delete keeps it.
- Two concurrent first starts settle on one binding.
- A crash after the binding is written and before the claim leaves a consistent next start.
- An unclassified new spec field fails the classification test.

## Open questions for review

1. **Sink endpoints** (`brokers`, `url`, `endpoint`, `region`) are compatible here, on the grounds that hosts move. The alternative is to treat a sink endpoint as destination identity where no ownership fence exists (Kafka, Redis, NATS, HTTP).
2. **Narrowing the table selection** (removing tables) is refused here, like widening. It could be compatible, since the remaining tables keep a valid position. It is refused because a later re-widen would then silently skip the removed interval.
3. **`processors`, `filter` and the commit policy** are compatible here and recorded. Should a change to them refuse unless acknowledged?
4. **Adoption of existing stores** is per source id, through the CLI. Should a bulk mode exist for fleets, with one proof per source id?

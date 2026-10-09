# PostgreSQL schema identity, coherent capture and publication markers

Status: revision 7, partly superseded. Sections 1 (event schema, coherent capture) and 8 stand and are implemented. The publication-marker protocol (sections 2-7 and its tests) is superseded by the immutable-publication contract in the addendum at the end and was never implemented.
Base: main `47974f1`. Code references are to that tree.

Facts marked **verified** were probed on throwaway PostgreSQL containers: 17.10 unless stated, and 14.24 to 18.6 where stated.

Rulings incorporated:
- D1-D4 and R4;
- M1 with corrections;
- M2 restricted to re-snapshot or abandonment;
- M3 and M4;
- S1 (explicit entries and FOR ALL TABLES only; a direct leaf partition is conditional on L-1);
- the revision 5 P0 corrections:
  - non-superuser owner;
  - marker replay binding;
  - `pubtruncate`;
  - construction details;
  - integrity record;
  - nonce context;
- the revision 6 corrections:
  - retirement of keys and generations (section 3.1);
  - the implementable digest construction (section 4.1);
  - the `baseline` wire kind (sections 5 and 7).

## 1. Approved design (revisions 3-4), restated for implementation

- **Event schema `pg_event_v2`:**
  - replica identity;
  - per published column, in Relation order: name, type (builtin name if OID < 10000, else `user_defined`), typmod and key flag;
  - derived proven non-null = key and replica identity in {`d`, `i`}.

  The event schema is built from the Relation alone.
- **Builtin type names** are verified identical on PostgreSQL 14-18 (198 OIDs, no conflicts). The table is pinned; an unknown builtin OID fails closed.
- **Public content fingerprint:** over the event schema only. No OIDs.
- **Relation binding:** digest over OID, replica identity, and per column name, type OID, typmod and key flag.
  - Records are deterministic bytes, appended only if absent and keyed by digest.
  - Lookup is by digest only.
- **Write order:** R (register or retrieve the version), V (read back the fingerprint), B (append the binding), C (cache, then emit). Crash at any boundary converges.
- **Legacy versions:** never rewritten. They stay addressable byte-for-byte. The one-time `pg_event_v2` transition is noted in the CHANGELOG.
- **Annotations:**
  - labelled `catalog_at_capture` with `captured_lsn`;
  - deduplicated by content; the latest 8 are kept;
  - pagination exposes the truncation horizon (a cursor older than the horizon returns `{status: "truncated", horizon}`);
  - `unavailable` is the durable incident `pg_annotation_unavailable`, resolved by `annotation_recorded`.
- **Coherent capture:** REPEATABLE READ READ ONLY, `LOCK TABLE ONLY ... IN ACCESS SHARE MODE` before the snapshot, bounded lock and statement timeouts, a cancel request on drop.
- **Catalog work per Relation:** one proof statement reads the canonical inputs. A locked capture runs only when their digest changes. No `xmin` is used.
- **Downstream:** consumers resolve the event's version; see section 8 for the rules added in this revision.

## 2. Why markers (P11 and protocol probes, verified)

- **Publication changes are visible but their contents are not:**
  - every publication change re-sends the Relation, but a row filter or a `publish` flag can still drop rows silently;
  - `pubtruncate = false` silently drops TRUNCATE (**verified**);
  - no publication kind is immutable.
- **Rollback:** transactional messages vanish on ROLLBACK and on ROLLBACK TO SAVEPOINT.
- **Transaction-local settings** persist from `ddl_command_start` to `ddl_command_end`, and revert on ROLLBACK TO SAVEPOINT.
- **Transaction ID:** `pg_current_xact_id()` inside a subtransaction returns the top-level full (64-bit) transaction id.
- **Spoofing:** `pg_logical_emit_message` is executable by PUBLIC, so anyone can emit our prefix.
- **Non-superuser owner works:**
  - a **NOSUPERUSER NOLOGIN** owner's SECURITY DEFINER event-trigger function emits markers when an **ordinary publication owner** runs ALTER PUBLICATION;
  - that user cannot read the key;
  - the event triggers themselves must be created by a superuser and hold no code.
- **HMAC:** a PL/pgSQL HMAC-SHA256 built only on core `sha256()` matches RFC 4231 test cases 1, 2 and 6 (6 uses a key longer than the block size).

## 3. Installation and ownership

**Installer:** `deltaforge pg-install-publication-markers` runs as a superuser, in one transaction, and creates:
- **Role** `deltaforge_marker_owner`: NOLOGIN, NOSUPERUSER, NOCREATEDB, NOCREATEROLE, NOREPLICATION, NOBYPASSRLS, member of nothing. It owns the schema, the tables and the functions.
  - It needs no other privilege. Reading `pg_publication*` and executing `pg_logical_emit_message` are PUBLIC; the live proof is in section 2.
  - The event triggers are created and owned by the installing superuser (PostgreSQL requires that). They contain no code and call the owner's function.
- **Schema `deltaforge`:** `REVOKE ALL FROM PUBLIC`.
- **Table `deltaforge.marker_key`:** the key ring `(key_id text, k bytea NULL)`. `REVOKE ALL FROM PUBLIC`. `k` is set to NULL only by retirement (section 3.1).
- **Table `deltaforge.marker_domain`:** the durable domain history, never deleted:
  - `(domain_id, generation, key_id, protocol_version, activated_lsn, demoted_lsn NULL, retired_lsn NULL, verifier_digest)`;
  - a domain is one (generation, key) pair;
  - exactly one domain is `active` (`demoted_lsn` NULL).
- **Table `deltaforge.marker_consumer`:** `(slot_name, source_digest, registered_lsn)`. Sources register through the owner function `deltaforge.register_consumer(slot)`, whose `EXECUTE` is granted to the configured source role.
- **View `deltaforge.marker_install`:** owner-defined, `GRANT SELECT TO PUBLIC`. It exposes only non-secret facts:
  - every `marker_domain` row;
  - each key's `key_digest = sha256('deltaforge-key-id' || k)`, or `retired` once the key material is removed;
  - the registered consumers.
- **Functions** `deltaforge.hmac256`, `deltaforge.pub_marker_before_v<gen>` and `deltaforge.pub_marker_after_v<gen>`:
  - `SECURITY DEFINER` (only to read the key);
  - `SET search_path = pg_catalog, pg_temp`;
  - every identifier and function schema-qualified;
  - `REVOKE ALL ... FROM PUBLIC`.
- **Event triggers** `deltaforge_pub_before_v<gen>` (`ddl_command_start`) and `deltaforge_pub_after_v<gen>` (`ddl_command_end`), tags CREATE, ALTER and DROP PUBLICATION, `ENABLE ALWAYS`. There is no `sql_drop` trigger: drops are derived at `ddl_command_end` (section 4), so a drop cannot be emitted twice.
- **Key:** 32 bytes from the operating system's CSPRNG, generated by the installer (the DeltaForge binary) and written once. The installer prints the key id and key to the operator for the source secret.

**Integrity record:** read by the source at startup and at every catalog proof, from catalogs and the view only. A difference from the expected record fails closed (`pg_marker_integrity`):
- the owner role's attributes (all NO-* flags above, `rolsuper = false`, `rolcanlogin = false`) and its memberships (none);
- each function's owner, `prosrc` digest, `prosecdef = true`, `proconfig = {search_path=pg_catalog, pg_temp}` and ACL (no PUBLIC);
- each trigger's `evtevent`, `evttags`, `evtenabled = 'A'`, `evtfoid` and generation suffix;
- the schema's owner and ACL;
- the key table's owner and ACL;
- `marker_install`:
  - exactly one active domain;
  - the domain history is monotonic (activated < demoted < retired);
- the source's key ring must hold the key for **every domain whose `retired_lsn` is NULL or greater than the source's checkpoint**, each matching its `key_digest`;
- the source binary must support every such domain's protocol version.

Otherwise the source fails closed at startup (`pg_marker_verifier_missing`), naming the domain.

Only fields PostgreSQL stores verbatim are digested (`prosrc`, ACL arrays, attribute booleans), so the record is identical on 14-18 (lane E-5).

### 3.1 Rotation, upgrade and retirement

Keys and generations rotate through the same three steps. A domain is never deleted, only its key material once it is provably unneeded.

1. **Activate** (one transaction):
   - insert the new key and domain row, with `activated_lsn = pg_current_wal_lsn()`;
   - for an upgrade, install generation `N+1` functions and triggers next to `N`;
   - set the old domain's `demoted_lsn`; its triggers stop signing.

   Sources must already hold the new key, since the rotation command refuses until every registered source reports it. Section 5 accepts any non-retired domain.
2. **Seal.** The old domain's `retired_lsn` is set to `pg_current_wal_lsn()`, but only after no transaction that started before the demotion is still running (checked with `pg_stat_activity.backend_xid` and `backend_xmin` against the demotion xid). Any marker signed by the old domain therefore has a commit LSN below `retired_lsn`.
   - For an upgrade, the old generation's triggers and functions are dropped in this step.
3. **Retire key material** (`k := NULL`). Allowed only if every registered consumer slot has `confirmed_flush_lsn ≥ retired_lsn`.
   - A source acknowledges a flush only up to its durable checkpoint, so every source's checkpoint is then past every marker of that domain, and no retained WAL it can still decode holds one.
   - If a registered slot is missing or behind, retirement is **refused** and the report names the slot and source.
   - The operator may instead re-snapshot that source, or abandon its backlog past `retired_lsn` (section 7), then retry.

**Consequences:**
- There is no fixed "at most one previous key". The ring holds every key not yet retired, and is bounded in practice by sources keeping up.
- A source offline across several rotations needs every non-retired key in its secret. Retirement cannot proceed past it.

**Marker classification by domain** (section 5, step 2):

| Header domain | Key available | Result |
|---|---|---|
| Not in `marker_domain` | - | Unauthenticated: ignored, with an incident. |
| Known | yes | Verify. A bad MAC is unauthenticated: ignored, with an incident. |
| Known | no (retired material, or absent from the source's ring) | **Fail closed** (`pg_marker_verifier_missing`). A historical marker is never treated as a spoof. |

**Uninstall:** refused while any consumer is registered.

**Residual (documented):** a superuser can read the key, or tamper with the triggers between checks.

## 4. Marker construction (structured)

**`before_v<gen>` (`ddl_command_start`):**
- increments the transaction-local statement counter `deltaforge.stmt` (`set_config(..., true)`);
- stores `{publication oid -> state digest}` for all publications in `deltaforge.before`.

  The digest is `state_digest` from section 4.1, pass 1 only. Memory is O(number of publications) for the map, plus pass 1's bounded buffer and hash list.

**`after_v<gen>` (`ddl_command_end`):** recomputes the map and diffs it against `deltaforge.before`:
- an OID new or with a changed digest gets a `state` marker (create, alter, rename: the name is part of the state);
- an OID gone gets an `absent` marker (drop; one per dropped publication, so a multi-publication DROP needs no SQL parsing).

**Why `marker_id` is unique:** `marker_id = (generation, xid8, stmt, pub oid)`.
- `stmt` reverts on ROLLBACK TO SAVEPOINT, but the rolled-back statement's markers are discarded with it, so reuse cannot collide.
- Two installed generations differ in `generation`.

**Canonical state:** a line-oriented UTF-8 format, so it can be parsed in a stream. Rows are ordered with `COLLATE "C"`, so the bytes are deterministic.
- Line 1:
  - `name`, `oid`, `owner`;
  - `puballtables`, `pubinsert`, `pubupdate`, `pubdelete`, `pubtruncate`, `pubviaroot`;
  - `pubgencols` (18+, else `-`);
  - the count of schema entries and the count of relation entries.
- Then one line per schema entry (`nspname`).
- Then one line per explicit relation entry, sorted by `(nspname, relname)`: `relid, nspname, relname, row_filter_present, row_filter_digest, column_list_present, column_list_digest`.

Row-filter text and column names are never emitted, only their presence and digests. The predicate rejects any filter or column list.

### 4.1 Digest construction (two passes, core `sha256` only)

PostgreSQL's `sha256(bytea)` is one-shot. The construction uses only one-shot hashes over bounded inputs.

**Chunking:**
- The canonical bytes are divided at fixed 64 KiB byte boundaries. A line, or a multibyte UTF-8 character, may straddle two chunks: chunks are bytes, not text.
- `n = ceil(total_bytes / 65536)`, and `n ≥ 1`, because line 1 always exists.

**Hashes:**
- `h_i = sha256("dfpub-chunk\0" || u32be(i) || chunk_i)`.
- `state_digest = sha256("dfpub-state\0" || u32be(format_version) || u64be(total_bytes) || u32be(n) || h_0 || ... || h_{n-1})`.

**Pass 1 (digest):**
- The function generates the canonical bytes by iterating the ordered catalog rows.
- It keeps a buffer of at most 64 KiB plus one line, and emits `h_i` each time the buffer reaches 64 KiB, keeping the remainder.
- It keeps the hash list (`n × 32` bytes, at most 8 KiB at the 16 MiB cap), `total_bytes` and `n`.
- If `total_bytes` would exceed 16 MiB, it stops and produces `oversize`.

**Pass 2 (emit):**
- The function regenerates the identical bytes and the identical chunks.
- For each chunk it recomputes `h_i` and compares it with pass 1. Then it emits the message with the header carrying `state_digest`, `total_bytes` and `n`.
- A mismatch means the catalog changed between the passes. The function **raises an error, failing the DDL**: the transaction aborts and every message it emitted is discarded (verified: rolled-back messages never reach the stream).

**Snapshot:**
- The function's queries run under READ COMMITTED, so the two passes may see different snapshots. The comparison detects that, and it can only fail the DDL, never emit inconsistent state.
- The DDL's own publication is locked by the DDL. Only concurrent changes to other publications can cause the mismatch; the user retries.

**The before/after maps** (section 4) use the same `state_digest` (pass 1 only).

**Rust verifier:**
- It recomputes `h_i` per received chunk, buffering only the chunk.
- It accumulates the hash list (at most 8 KiB) and computes `state_digest` at the last chunk.
- It rejects a mismatch, or a `total_bytes` or `n` different from the header.

**Tests (D-1):** both implementations (PL/pgSQL and Rust) on the same fixture corpus:
- total sizes 0 (rejected), 1, 65535, 65536, 65537 and 16 MiB;
- multibyte UTF-8 at a chunk boundary;
- reordered input rows (same digest after ordering);
- a moved boundary (different `h_i`);
- a catalog mutation between the passes (the DDL fails, nothing is emitted).

## 5. Marker message format, authentication and replay

**Each message:** `pg_logical_emit_message(true, 'deltaforge.pub', bytes)` with
- `bytes = header_len (u16) || header || chunk || mac`;
- `header` = canonical JSON, at most 1 KiB: `{v: 1, gen, domain_id, key_id, sysid, dboid, marker_id, kind: state|absent|oversize|baseline, pub_oid, chunk, chunks, state_bytes, state_digest}`. A `baseline` adds `{source_digest, request_id, l_s}` (section 7);
- `chunk` = a slice of the canonical state only (≤ 64 KiB), so identical states from different markers yield identical chunk bytes;
- `mac = HMAC-SHA256(key, "deltaforge-pub-marker\0" || header || chunk)`.

The header binds:
- the protocol version and installation generation;
- the database's system identifier and OID, so a marker from another database does not verify;
- the marker id, including the full xid8.

**Sizes:**
- `state_bytes` ≤ 16 MiB is checked from the header **before** any allocation or write.
- `chunks = ceil(state_bytes / 64 KiB)` and each chunk length are checked before use.
- Above 16 MiB the trigger emits one `oversize` marker (header only). The user's DDL never fails. The source fails closed on `oversize`, naming the publication.

**Source processing order for a message with the reserved prefix:**
1. **Bounded parse.** Header length ≤ 1 KiB and the total message length are within bounds. The message bytes are already in the replication frame; nothing else is allocated.
2. **Authenticate.** Classify `domain_id` (section 3.1): an unknown domain is ignored; a known domain without its key fails closed. Otherwise verify the MAC with that domain's key, then compare `gen`, `sysid` and `dboid` with the domain and the stream's database. Any failure means the message is **ignored for state**, with the security incident `pg_marker_rejected` (class `mac` / `domain` / `generation`).
   - The incident is upserted at most once per minute per class; a counter metric counts every rejection.
   - Nothing is allocated or written.
   - Any role can emit a message, so failing closed would let any role stop CDC; ignoring it cannot change state.
3. **Position binding.**
   - The low 32 bits of `marker_id.xid8` must equal the xid of the pgoutput transaction carrying the message (`BEGIN`). Otherwise it is a replay: ignored, with `pg_marker_rejected(class=replay)`.
   - The durable marker record `{marker_id, commit_lsn, xid}` is appended only if absent, keyed by `marker_id`:
     - same `commit_lsn`: idempotent (legitimate replay after restart);
     - different `commit_lsn`: replay, ignored with an incident, never applied.

   Marker records are kept forever: one per publication change per generation, which is small. A 32-bit xid coincidence after wraparound still meets the durable binding.
4. **Unknown `v` or `kind`** in an authenticated header: fail closed.
5. **Chunk sequence.**
   - Chunks of one marker must be consecutive, starting at 0, ending at `chunks - 1`, inside one transaction. A gap, reorder, duplicate within one delivery, count or byte mismatch, an interleaved second marker, or a transaction ending mid-marker: fail closed.
   - Each verified chunk is written to the content-addressed store `pub_state_chunk/<state_digest>/<i>`. Identical bytes on replay make this idempotent.
   - The per-chunk `h_i` is accumulated (at most 8 KiB). On the last chunk `state_digest` is recomputed and verified (section 4.1).
   - Memory is one chunk, the hash list and the parser state (section 6). The whole state is never materialised.
6. **Apply** (section 6), then continue with the next message.

**Incomplete journals:**
- At most one marker is in progress per source (they are consecutive).
- A chunk set whose digest is not referenced by any state record is orphaned by a crash or failure. Orphans are deleted by a cleanup pass at startup and hourly, once they are older than 24 hours.
- Bound: 16 MiB per source.

## 6. Source state machine

**Streaming evaluation:**
- While chunks arrive, the line parser keeps only:
  - the flags line;
  - the set of explicit relation entries, as a compact map `relid -> (row_filter_present, column_list_present)`;
  - schema-entry presence.
- Representation: a sorted `Vec<u32>` of relids, built by push then `sort_unstable`, plus two parallel bit vectors.
- The bound is **measured, not estimated**. Test E-3 runs under a counting global allocator at the 16 MiB cap (about 150,000 entries) and asserts the peak allocator-inclusive bytes of assembly plus evaluation. The design target is at most 4 MiB, and the measured value is recorded in the PR.
- On the final verified chunk the new positional state replaces the old.

**Durable state record:** `schemas.v1.pg.publication_state`.
- Record: `{marker_id, commit_lsn, kind (state|absent|baseline), state_digest, request_id (baseline only)}`, appended only if absent.
- It is written before any later message is processed. A row is emitted only after every earlier marker is durable, and checkpoints commit only acknowledged rows, so a checkpoint never passes a non-durable marker.
- Write failure: bounded retry, then fail closed. The stream does not advance meanwhile.

**Resume:** the latest record at or before the checkpoint, re-hydrated by streaming its chunks.

**Acceptance predicate** for each tracked table, evaluated when the state changes and at the table's first row per run:
- membership comes only from `puballtables` or an explicit entry for its relid;
- any **schema entry** (`TABLES IN SCHEMA`) covering it is rejected (S1);
- no row filter and no column list;
- `pubinsert`, `pubupdate`, `pubdelete` and `pubtruncate` all true;
- not reached through a partition root with `pubviaroot` (M4); the error names the publication, the root and the table;
- on 18+, `pubgencols = 'n'`;
- the publication is present.

A failure stops the source before the next row of that table, naming the publication and table. It does not stop on markers for publications other than the configured one.

**Rows and markers in one transaction:** rows after a marker are evaluated under the new state (messages are delivered at their position).

## 7. Baselines and upgrade (M2, final)

**Where the first position's state comes from:**
- **Resume:** the durable record (state, absent or baseline) at or before the checkpoint.
- **Snapshot start:** the state is read in the exported snapshot.
- **CDC-only first start:** `CREATE_REPLICATION_SLOT ... (SNAPSHOT 'export')`, with the state read in that snapshot.

**Existing sources without a record:** re-snapshot (recommended, `history: proven (snapshot)`), or abandonment below.

### 7.1 The `baseline` marker

- **Emitter:** `deltaforge.emit_baseline_v<gen>(source_digest text, request_id text)`.
  - It is owned by the marker owner and SECURITY DEFINER, with `EXECUTE` granted only to the configured source role.
  - It runs as the **only statement of its own transaction**: the recovery operation issues it in autocommit.
- **What it does:**
  - reads `l_s = pg_current_wal_lsn()`;
  - builds the configured publication's canonical state with the section 4.1 two-pass construction;
  - emits the chunks with `kind = baseline`.
- **Header:** as section 5, plus:
  - `source_digest` (SHA-256 of the source id);
  - `request_id` (the recovery operation's id);
  - `l_s`;
  - `marker_id = (gen, xid8, 0, pub_oid)`.

  It is authenticated like every marker, with the same domain rules.
- **Chunking, MAC and position binding:** identical to state markers (section 5).
- **Coexistence:** the decoder fails closed if a `baseline` shares a transaction with any other marker or with a row of a tracked table. The operation then re-issues it.

### 7.2 Abandonment through the real decoder

`pg-reset-to-marker-boundary <source>` runs through the recovery-operation framework (plan, impact report, confirmation, audit):

1. **Plan.** Record `request_id`, the source's checkpoint C and the affected tables.
2. **Emit.** The operation emits the baseline: commit LSN B, carrying `l_s`.
3. **Scan-only run.** The source decodes from C in scan-only mode: rows are discarded, never emitted, and markers are processed by the normal decoder (authentication, binding, chunk store).
   - A publication marker at a position in `(l_s, B)` **invalidates** the baseline: the operation returns to step 2 with a new baseline.
   - At the baseline with a matching `request_id` and `source_digest`, the source appends the durable record `{kind: baseline, marker_id, commit_lsn: B, state_digest, request_id}`, sets the positional state, and commits the checkpoint to B as the operation's final step.
   - A baseline whose `request_id` is not the active operation's, or whose `source_digest` is another source's, is ignored for state. It is recorded in the operation log and never applied.
4. **Normal streaming** resumes after B.

**Replay:**
- The same baseline at the same position is idempotent.
- At another position, it is rejected as a replay (section 5).
- After the operation completes, a re-delivered baseline lies at or before the checkpoint and is never re-applied.

**Report and provenance:** the exact discarded interval `(C, B]` and the affected tables, with `history: discarded`. Nothing before B is called proven.

**Regression A-1:** runs the whole operation against live PostgreSQL through the real decoder, marker store and checkpoint store. It covers:
- invalidation by a marker in `(l_s, B)`;
- a foreign `request_id`;
- a crash after the baseline record but before the checkpoint (it converges on rerun).

## 8. Visibility proof (accepted, with the context record)

**Per-stream nonce:** `application_name = 'deltaforge:' || 128-bit random hex`, regenerated for every stream, replacement, reconnect and credential rotation. The vendored client gains an `application_name` option (`worker.rs:251`).

**Context record**, kept per stream:
- `{nonce, slot, pid, backend_start, system_identifier, database_oid, marker_generation}`;
- the pid and start time come from the session's own `pg_stat_activity` row, read with the session facts.

**Proof statement** (first statement of every catalog read, taking its snapshot). Accepted only if all hold:
- not in recovery;
- same system identifier;
- same database OID;
- the slot is active;
- the walsender row matches pid, `backend_start`, the exact nonce and `datid`;
- the installation generation is still `marker_generation`, or its accepted successor;
- `pg_current_wal_lsn() ≥` the target commit LSN.

Otherwise: wait up to 30 s, then fail closed (`pg_catalog_visibility_unproven`). No guard passes and no annotation is written before the proof.

## 9. Downstream rules

- **JSON and native paths** never require a `SchemaRef`.
- **Every schema-dependent path** must resolve **both** fingerprint and sequence: Avro, Arrow, ClickHouse, Elasticsearch, replay delivery and sensing enrichment.
  - A version whose fingerprint matches but whose sequence differs is refused (retryable `SchemaUnavailable`, logged with both values), never accepted.
  - A missing version fails retryably before encoding.
- **M3, REPLICA IDENTITY FULL:**
  - ClickHouse auto-create requires configured key columns;
  - Elasticsearch requires `id_fields`;
  - the configured columns are validated to exist in the event's version;
  - missing or invalid configuration fails closed before any write;
  - the primary key is never inferred from annotations.
- **M4:** a directly published leaf partition is supported. Its snapshot reads the leaf; CDC publishes the leaf (`pubviaroot` false). Parity test L-1.
- **Consumer classification:** revision 4, accepted, unchanged.


## 10. Tests and mutants (additions to revisions 3-5)

| Id | Test |
|---|---|
| O-1 | The installed owner has `rolsuper` and `rolcanlogin` false and no memberships. Markers are emitted when an ordinary non-superuser publication owner runs CREATE, ALTER and DROP. That user cannot read `marker_key`. |
| H-1 | `deltaforge.hmac256` matches RFC 4231 test cases 1-7 on PG 14-18, and the Rust verifier matches the same vectors. |
| E-1 | Structured diff: CREATE, RENAME, ALTER (options, set/add/drop table), a two-publication DROP, and several statements in one transaction each produce exactly the expected markers, one drop marker each, with no SQL text inspected. |
| E-2 | Rollback, and savepoint rollback with counter reuse after it: no stray marker, no `marker_id` conflict. |
| E-3 | Chunking at 100,000 explicit tables: memory stays within the bound (measured), replay is idempotent, an orphaned partial journal is cleaned. `oversize` at the cap. |
| E-4 | Spoofed MAC, other database, other generation, replayed old valid state after a newer state (same bytes, later transaction), and replay after an xid coincidence (simulated): all ignored, state unchanged, incident rate-limited. An authenticated unknown version fails closed. |
| E-5 | Integrity record on PG 14-18. Each tampering form fails closed: superuser owner, login owner, PUBLIC grants, changed body or search path, disabled trigger, a second current key, a key-id or digest mismatch with the source secret. |
| E-6 | Two generations active together during an upgrade, with concurrent publication DDL: distinct marker ids, identical state digests, no record conflict, no unmarked change. |
| E-7 | Each state-machine row in section 6, including a record write failure under a fault-injecting backend and a crash mid-chunk. |
| E-8 | `pubtruncate = false`: the source refuses before relying on the publication, and a TRUNCATE is never silently missing. |
| E-9 | A `TABLES IN SCHEMA` publication covering a tracked table is refused (S1). |
| A-1 | Abandonment end-to-end through the real decoder and stores (section 7.2). |
| D-1 | Digest construction parity and boundaries (section 4.1). |
| K-2 | A source offline across one rotation, then across two rotations and an upgrade, with retained WAL containing old-generation markers: it verifies with its ring and evaluates correctly. Retirement is refused while it lags. A source missing a non-retired key fails closed at startup. A marker from a known domain without a key fails closed; an unknown domain is ignored. |
| N-1 | A sibling node with the same pid and `backend_start` but another nonce is refused. Mutant: remove the nonce check. |
| N-2 | Nonces regenerate on reconnect and on credential rotation, with split routing during a rotation: a stale nonce is refused. |
| L-1 | Directly published leaf partition parity, the condition for supporting it. A root published with `pubviaroot` is refused, naming the publication, root and table. |
| K-1 | FULL identity with ClickHouse or Elasticsearch: missing or nonexistent key columns fail closed before writes. |
| R-1 | Replay with a matching fingerprint but a different sequence is refused. |

Kept from earlier revisions: V1-V4, M-1-M-7, C-1, C-2, T-1, P1-P12, S1-S8.

**Mutants** (each must fail a test):

| Mutant | Fails |
|---|---|
| Owner created SUPERUSER | O-1, E-5 |
| MAC domain without `dboid` | E-4, other database |
| Position binding removed | E-4, replay |
| Predicate without `pubtruncate` | E-8 |
| `sql_drop` trigger added | E-1, double drop |
| Whole-state buffering | E-3, memory bound |
| Key material retired while a slot lags | K-2 |
| Known domain without a key treated as a spoof | K-2 |
| Pass-2 hash comparison removed | D-1, mutation between passes |
| `baseline` kind unknown to the decoder | A-1 |

## 11. Residuals (documented)

- A superuser can read the key or tamper with the triggers between checks.
- Annotations are capture-time only.
- Partition roots and `TABLES IN SCHEMA` membership are rejected.
- A replication frame is read whole by the client before the prefix is seen. That is existing behaviour for every message, and marker parsing adds no allocation beyond it.


## Addendum: the immutable-publication contract (supersedes sections 2-7)

Online publication mutation is unsupported. A registered publication cannot change while any registration exists in its database.

- **Registration** (`deltaforge pg-publication register`, superuser, after an explicit confirmation of the database-wide impact): explicit `FOR TABLE` publications of ordinary tables only, all four publish operations, no row filter, column list, `pubviaroot` or `pubgencols`. The publication is transferred to the NOLOGIN, member-less `deltaforge_publication_owner`; its canonical digest (OIDs only, ordered by OID) and the registration's WAL position are recorded in `deltaforge.registration`.
- **Enforcement, active while DeltaForge is offline:** a `ddl_command_start` trigger refuses every ALTER and DROP PUBLICATION while any registration exists, for every role including superusers, before PostgreSQL starts the command; a `sql_drop` trigger refuses any drop removing a registered publication or member. There is no `ddl_command_end` trigger. Measured on PostgreSQL 14-18: refused 1K/4K/10K-table `SET TABLE` statements grow the backend by 5-7 MiB (a no-op `sql_drop` trigger alone lets an accepted 4K-table one reach 2.7 GB).
- **Source verification** at startup and before every stream's first message: enforcement intact, registration present, live digest equal to the registration, the registration equal to the one the source accepted (or re-registered under a recorded maintenance decision), and no stream from before the registration. Failures: `pg_publication_changed`, `pg_publication_enforcement`.
- **Maintenance** is database-wide: every source stopped; a `pg-publication-maintenance` decision per source (`resnapshot`, with the `resnapshot` operation, or audited `abandon`); every registration removed; enforcement uninstalled (refused while any registration remains); the change applied; publications registered again; sources restarted. Registration, unregistration and uninstall are single transactions.
- **Residual:** a superuser can disable or drop the triggers. Detection is at the next startup or reconnect; completeness between tampering and detection is not guaranteed.

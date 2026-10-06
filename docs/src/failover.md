# Failover Handling

DeltaForge detects a change of database server automatically. A MySQL source resumes streaming on the new primary without operator intervention when it can prove the position to continue from. A PostgreSQL source never resumes on another cluster: it stops before reading anything from it (see [PostgreSQL: another cluster is refused](#postgresql-another-cluster-is-refused)). After a promotion of a physical standby it continues only when the promoted server provably continues its checkpoint (see [PostgreSQL: promotion continuity](#postgresql-promotion-continuity)). This page explains how detection works, what happens during MySQL reconciliation, and how to configure behaviour when the new primary has a different schema.

## How Detection Works

Every time the source reconnects - at startup or after a transient error - it queries the server's stable identity:

- **MySQL**: `@@server_uuid` from `performance_schema.replication_group_members`
- **PostgreSQL**: `system_identifier` from `pg_control_system()`, together with the database OID

The result is compared against the value stored in DeltaForge's storage backend. Three outcomes are possible:

| Result | Meaning | Action |
|--------|---------|--------|
| `FirstSeen` | No identity stored yet | Store and continue |
| `Same` | Same server as before | Verify checkpoint GTID is still reachable, then continue |
| `Changed` | Server identity differs | MySQL: run failover reconciliation. PostgreSQL: stop (refused) |

Identity is written to the durable storage backend (SQLite or PostgreSQL), so it survives process restarts and is correctly preserved across pipeline reloads.

## PostgreSQL: another cluster is refused

A PostgreSQL checkpoint is an LSN in one cluster's WAL history, and a replication slot is a position in that same history. A server with another `system_identifier` (an independently initialised cluster, a logical replica, a dump/restore), or the same cluster with a dropped and recreated database, has unrelated positions: an LSN that compares equal or larger there proves nothing, and resuming from it could silently skip changes.

So the PostgreSQL source compares the live `system_identifier` and database OID with the ones it recorded, before it writes any lineage or identity state, takes a snapshot, creates or reads a slot position, or sends `START_REPLICATION`: at startup, again right before the stream opens, and before every reconnect. A difference stops the source with a typed lineage error, and nothing is streamed, snapshotted or recorded:

```
source lineage error: source '<id>' is bound to PostgreSQL ... but is connected to .... Its checkpoint and replication slot position belong to the first server's WAL history and are never resumed on another server; nothing was streamed, snapshotted or recorded. To capture this server, configure a new source id (it starts with a snapshot).
```

To capture the other cluster, configure a new source id; it starts with a snapshot.

This is a cross-cluster check. A promoted physical standby keeps the cluster's `system_identifier`; continuing on it is decided by the continuity proof below.

## PostgreSQL: promotion continuity

Before **every** `START_REPLICATION` (startup, reconnect and credential rotation), the source proves on the replication session that then streams - with `IDENTIFY_SYSTEM`, `TIMELINE_HISTORY` and a read of the slot on that same authenticated session - that the server continues its durable checkpoint F. All of these must hold; anything unknown stops the source:

1. the same `system_identifier` and database OID, on a primary (not a server in recovery);
2. the server is on the timeline F was written on, or on a timeline that descends from it, with that timeline's switch point at or after F **and** at or after the position the stream reads from (changes already read past the switch point are not in the new history);
3. after a timeline switch: PostgreSQL 17 or later, and the slot is a synchronized failover slot (`failover` and `synced` true). A slot of the same name is not enough;
4. the slot is persistent and logical, not invalidated, its WAL is not lost, and `restart_lsn` and `confirmed_flush_lsn` are at or before F;
5. the server's WAL extends at least to F.

When a stream becomes authoritative (before its first event is consumed), its proven continuity is recorded durably: a chain id created once for the source, the number of proven timeline transitions in that chain, and the timeline. Every checkpoint carries this stamp. Timeline numbers alone prove no ancestry (two standbys promoted from the same primary get sibling timelines), so checkpoints are ordered only within one chain: by LSN within a transition, by the proven transition across transitions; checkpoints of different chains, or one transition stamped with two timelines, are incomparable. A checkpoint from before this release orders only with others like it and with the first link of a chain; while the chain is at its first link, every start adopts all of the source's checkpoints (each sink's, or the single aggregate one) into the chain before anything is consumed, keeping their positions, so no later transition meets an unstamped checkpoint. A checkpoint that cannot be adopted (another chain or transition, a partial stamp, a malformed checkpoint) stops the source (`checkpoint_chain_mismatch`) with nothing rewritten. A credential rotation never crosses a transition: a replacement stream that would change the stamp is refused before anything is recorded, and the ordinary reconnect that follows proves the transition. A failed proof stops the source with a `pg_continuity_unproven` incident whose class names the condition: `timeline_not_descended`, `switch_before_checkpoint`, `switch_before_read_position`, `checkpoint_chain_mismatch`, `slot_not_synced`, `failover_unsupported_version`, `timeline_unrecorded`, `server_in_recovery`, `wal_behind_checkpoint`, `slot_not_persistent_logical`, `slot_beyond_checkpoint`, `slot_missing`, `slot_invalidated`, `wal_lost` or `unknown_slot_position`. Nothing is streamed and the checkpoint is not moved.

A session on a server in recovery (`server_in_recovery`) is retried like a connection failure for up to 2 minutes, since the endpoint may be mid-failover (an open `auto_retry` incident; stopping the pipeline withdraws it), then the source stops on the same incident as operator action: route the source to the writable primary. A promoted server (no longer in recovery) continues only after the timeline and synchronized-slot proof above succeeds.

> **Upgrade limitation (`timeline_unrecorded`).** Sources created before this release have checkpoints but no recorded timeline. On a server that has never switched timeline (timeline 1) the timeline is recorded automatically at the first start. On a server that has switched timeline (any earlier promotion or point-in-time recovery), the source stops with `timeline_unrecorded`: nothing on the server distinguishes that history from another one. Recover with the [`pg-adopt-timeline` recovery operation](recovery.md#pg-adopt-timeline), which records the current timeline for those checkpoints without moving them (after verifying the endpoint, slot and checkpoint position), or with [`resnapshot`](recovery.md#resnapshot). Check `SELECT timeline_id FROM pg_control_checkpoint()` before upgrading.

### HA setup (PostgreSQL 17)

Continuation across a promotion needs PostgreSQL 17 logical slot synchronization:

- DeltaForge creates its slot as a failover slot (`failover => true`) on PostgreSQL 17. A pre-existing slot without it is reported as a running-degraded `pg_failover_slot_unavailable` incident: streaming continues on the current primary, but a promotion stops the source (`slot_not_synced`). Recreate the slot as a failover slot (or `ALTER_REPLICATION_SLOT ... (FAILOVER true)` on a replication connection) to clear it.
- Standby: `sync_replication_slots = on`, `hot_standby_feedback = on`, `primary_slot_name` set to a physical slot on the primary, and a `dbname` in `primary_conninfo`.
- Primary: `synchronized_standby_slots` naming that physical slot, so logical changes are sent to DeltaForge only after the standby has received them. Without it, a promotion can lose changes DeltaForge already read from the old primary, and the source then stops with `switch_before_read_position`.
- HA tools that recreate slots on the promoted server (for example Patroni `permanent_slots` without PostgreSQL 17 synchronization) produce slots that are not `synced`; they are refused after a promotion.

Before PostgreSQL 17, a timeline switch always stops the source (`failover_unsupported_version`). PostgreSQL 17 is the validated version.

**Unsupported (documented residual):** a filesystem-level rewind on the same timeline that later diverges cannot be detected; nothing on the server distinguishes it from the original history.

## What Happens During Failover (MySQL)

When a `Changed` identity is detected, DeltaForge runs reconciliation before allowing any events to flow. Reconciliation is idempotent - if the process dies mid-run, it will re-execute correctly on the next startup.

### 1. Position reachability check

DeltaForge verifies that the checkpoint position from the old primary still exists on the new primary:

- **MySQL**: checks whether the GTID set from the last checkpoint is present in B's executed GTID history or purged range
- **PostgreSQL** (same server only, on every resume): checks that the replication slot exists, is not invalidated and has not lost its WAL

If the position is confirmed **lost**, the source stops immediately with an error and `/health` returns `503`. This covers two distinct cases:

- **Server changed (`Changed`)**: B's GTID history does not contain A's checkpoint (e.g. B was a lagging replica).
- **Same server, history wiped (`Same`)**: `RESET BINARY LOGS AND GTIDS` was run on the same server, clearing all GTID state without changing the server UUID. DeltaForge detects this on the first reconnect by checking `GTID_SUBSET(checkpoint, @@gtid_executed)`.

In both cases the error message is:

```
position lost: <reason>. Re-snapshot required.
```

Silently skipping data is worse than halting. Restart the pipeline with a fresh snapshot to recover.

If reachability cannot be determined (e.g. the health query fails transiently), DeltaForge logs a warning and continues — it does not halt on uncertainty.

### 2. Schema drift detection

DeltaForge compares each table's schema last registered under the old primary with its schema on the new primary.

Per table, when the table is first used on the new primary - its first rows, a DDL of it, or a snapshot of it - not when the connection is re-established. Nothing is enumerated, so tables matched by wildcard patterns are covered. The comparison is against the table's shape at the exact failover position, proven by capturing the table and scanning the new primary's binlog. If anything between the failover position and the table's first rows could have changed the table (a DDL of the table, a statement DeltaForge cannot attribute to tables, a purged interval), or the table's first event is itself a DDL, the comparison is unprovable. The outcome is recorded durably per table, so a completed check is not repeated.

### 3. Resume

After reconciliation, DeltaForge stores B's identity and resumes streaming. The first events from B use the updated schema.

## Position Adjustment

A subtle but critical detail: simply reconnecting at A's checkpoint position can cause data loss on its own, before reconciliation even runs.

**MySQL**: DeltaForge detects the identity change *before* opening the binlog stream and continues on B only from the exact position it proved: the GTID set the stream resumes from, which B must have executed in full (checked on a connection verified to be B). It never skips to B's binlog tail. If B has not executed that set, or the source runs without GTID mode (binlog file positions are not comparable across servers), the source stops with a typed error and changes nothing; re-snapshot from B or restore the missing transactions. That proven position is also the failover position the per-table schema drift checks are anchored to.

**PostgreSQL**: no position is adjusted. Another cluster is refused before `START_REPLICATION` is sent to it (see above), so neither the checkpoint nor any slot on that server moves.

## Schema Drift Policy

By default DeltaForge adapts to the new primary's schema and continues streaming. This is safe for additive drift (B has a new column A didn't have) but can be risky if B is missing columns that A had - row events encoded against A's schema may decode incorrectly against B's.

The `on_schema_drift` field controls this behaviour:

```yaml
source:
  type: mysql
  config:
    id: my-pipeline
    dsn: ${MYSQL_DSN}
    tables: [shop.orders]
    on_schema_drift: halt   # default: adapt
```

| Value | Behaviour |
|-------|-----------|
| `adapt` | Record drift, use the new primary's schema, continue streaming. Default. On MySQL an unprovable comparison is recorded as such and normal schema proof continues. |
| `halt` | Stop the source when schema drift is detected (on MySQL also when drift cannot be ruled out), before anything is registered or emitted for the table. Requires operator intervention. |

On MySQL the source stops at the table's first event on the new primary, with an error naming the table:

```
table shop.orders after failover: schema drift since the failover (1 change(s)) and on_schema_drift=halt.
Nothing was registered or emitted. ...
```

On PostgreSQL a cross-cluster change stops the source regardless of `on_schema_drift`; the policy applies to schema changes in the stream (and to a change made while the source was down, at each table's first Relation message).

Use `halt` when your failover environments do not guarantee DDL sync to replicas before promotion.

## What DeltaForge Does Not Handle

**DSN switching is external.** DeltaForge detects a new server by comparing identities, not by monitoring cluster topology. The DSN must already point to B before the pipeline reconnects - this is typically handled by a load balancer VIP, DNS failover, or connection proxy. If the DSN still resolves to A, the pipeline will retry A's dead connection rather than discovering B.

**Data loss from replica lag is not recoverable.** If B was a lagging replica and never received transactions that A committed before failing, those rows are gone at the database level. DeltaForge can detect the position gap but cannot reconstruct missing data. A re-snapshot from B is required in this case.

**Mid-flight DDL during active streaming is handled separately** by the normal schema reload mechanism, not by failover reconciliation. Failover reconciliation only runs when the server identity changes.

## Infrastructure Requirements

For clean automatic failover:

- **MySQL**: GTID mode must be enabled (`gtid_mode=ON`, `enforce_gtid_consistency=ON`). Without GTID, DeltaForge falls back to file/position coordinates which are meaningless across servers.
- **PostgreSQL**: continuation on another cluster is not supported (it is refused). After a promotion of a physical standby the source continues only when [promotion continuity](#postgresql-promotion-continuity) is proven, which needs PostgreSQL 17 synchronized failover slots.
- **MySQL**: the CDC user must exist on B with the same privileges as on A.
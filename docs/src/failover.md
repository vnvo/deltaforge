# Failover Handling

DeltaForge detects a change of database server automatically. A MySQL source resumes streaming on the new primary without operator intervention when it can prove the position to continue from. A PostgreSQL source never resumes on another cluster: it stops before reading anything from it (see [PostgreSQL: another cluster is refused](#postgresql-another-cluster-is-refused)). Cross-primary PostgreSQL failover is not supported for production yet. This page explains how detection works, what happens during MySQL reconciliation, and how to configure behaviour when the new primary has a different schema.

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

This is a cross-cluster check, not a check of every endpoint change:

- **Different `system_identifier` (or replaced database)**: refused before anything is recorded, snapshotted or streamed.
- **Same `system_identifier`, promotion or timeline change**: not detected by this check. A promoted physical standby keeps the cluster's `system_identifier`, so DeltaForge cannot yet tell it from the original primary, and continuity across it is **not proven safe**.
- **Cross-primary PostgreSQL failover is therefore unsupported for production** until the continuity proof below lands.

 Continuing on it after a promotion (a new timeline) requires proving that its timeline history contains the checkpoint and that a synchronized logical slot covers it; that proof is not implemented yet. Until it is, a promotion is resumed only if the slot exists on the promoted server and is healthy, and DeltaForge does not yet verify the timeline or the slot's bounds against the checkpoint: treat continuity across a PostgreSQL promotion as unverified.

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
- **PostgreSQL**: continuation on another cluster is not supported (it is refused), and cross-primary failover is unsupported for production until promotion continuity is proven. After a promotion of a physical standby the logical slot must exist on the promoted server (for example PostgreSQL 17 slot synchronization, or a slot-aware HA tool such as Patroni with `permanent_slots`); see the note on unverified promotion continuity above.
- **MySQL**: the CDC user must exist on B with the same privileges as on A.
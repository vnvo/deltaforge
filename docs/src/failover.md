# Failover Handling

DeltaForge detects database failover automatically and resumes streaming on the new primary without operator intervention. This page explains how detection works, what happens during reconciliation, and how to configure behaviour when the new primary has a different schema.

## How Detection Works

Every time the source reconnects - at startup or after a transient error - it queries the server's stable identity:

- **MySQL**: `@@server_uuid` from `performance_schema.replication_group_members`
- **PostgreSQL**: `system_identifier` from `pg_control_system()`

The result is compared against the value stored in DeltaForge's storage backend. Three outcomes are possible:

| Result | Meaning | Action |
|--------|---------|--------|
| `FirstSeen` | No identity stored yet | Store and continue |
| `Same` | Same server as before | Normal reconnect |
| `Changed` | Server identity differs | Run failover reconciliation |

Identity is written to the durable storage backend (SQLite or PostgreSQL), so it survives process restarts and is correctly preserved across pipeline reloads.

## What Happens During Failover

When a `Changed` identity is detected, DeltaForge runs reconciliation before allowing any events to flow. Reconciliation is idempotent - if the process dies mid-run, it will re-execute correctly on the next startup.

### 1. Position reachability check

DeltaForge verifies that the checkpoint position from the old primary still exists on the new primary:

- **MySQL**: checks whether the GTID set from the last checkpoint is present in B's executed GTID history or purged range
- **PostgreSQL**: checks whether the replication slot's `confirmed_flush_lsn` is reachable

If the position is confirmed **lost** (e.g. the replication slot was not preserved, or binlogs were purged without GTID overlap), the source stops immediately with an error:

```
position lost after failover: <reason>. Re-snapshot required.
```

This is intentional. Silently skipping data is worse than halting. Restart the pipeline with a fresh snapshot to recover.

If reachability cannot be determined (e.g. the health query fails), DeltaForge logs a warning and continues - it does not halt on uncertainty.

### 2. Schema drift detection

DeltaForge compares the schema last registered from the old primary against the live catalog on the new primary. Any column additions, removals, or renames are recorded as a `ReconcileRecord` in the storage backend.

If drift is found, the schema cache is invalidated so the next row event triggers a fresh load with the correct column mapping.

### 3. Resume

After reconciliation, DeltaForge stores B's identity and resumes streaming. The first events from B use the updated schema.

## Position Adjustment

A subtle but critical detail: simply reconnecting at A's checkpoint position can cause data loss on its own, before reconciliation even runs.

**MySQL**: B rejects A's GTID set at the protocol level with "purged required binary logs". DeltaForge detects the identity change *before* opening the binlog stream, resolves B's current binlog tail via `SHOW BINARY LOG STATUS`, and connects there instead. A's original GTID checkpoint is preserved separately for the reachability check.

**PostgreSQL**: `START_REPLICATION` at A's LSN immediately advances the slot's `confirmed_flush_lsn` to `max(A_checkpoint, slot_lsn)`. If B's slot was created at an LSN behind A's checkpoint, any changes B committed in that gap are permanently discarded - even if you reconnect at the correct LSN afterwards. DeltaForge detects the identity change before opening the replication stream and fetches the slot's actual `confirmed_flush_lsn` to use as the start position instead.

In both cases the original checkpoint is preserved for the reachability check, separate from the adjusted streaming position.

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
| `adapt` | Record drift, reload schema cache, continue streaming. Default. |
| `halt` | Stop the source when any schema drift is detected. Requires operator intervention. |

When `halt` fires, the reconciliation record is persisted before the source stops - you can inspect what changed before restarting:

```
schema drift detected after failover and on_schema_drift=halt.
Verify B's schema and apply any missing migrations before restarting.
```

Use `halt` when your failover environments do not guarantee DDL sync to replicas before promotion.

## What DeltaForge Does Not Handle

**DSN switching is external.** DeltaForge detects a new server by comparing identities, not by monitoring cluster topology. The DSN must already point to B before the pipeline reconnects - this is typically handled by a load balancer VIP, DNS failover, or connection proxy. If the DSN still resolves to A, the pipeline will retry A's dead connection rather than discovering B.

**Data loss from replica lag is not recoverable.** If B was a lagging replica and never received transactions that A committed before failing, those rows are gone at the database level. DeltaForge can detect the position gap but cannot reconstruct missing data. A re-snapshot from B is required in this case.

**Mid-flight DDL during active streaming is handled separately** by the normal schema reload mechanism, not by failover reconciliation. Failover reconciliation only runs when the server identity changes.

## Infrastructure Requirements

For clean automatic failover:

- **MySQL**: GTID mode must be enabled (`gtid_mode=ON`, `enforce_gtid_consistency=ON`). Without GTID, DeltaForge falls back to file/position coordinates which are meaningless across servers.
- **PostgreSQL**: The replication slot must exist on B before the pipeline connects to it. Slots are not automatically transferred during failover - use a slot-aware HA tool (e.g. Patroni with `permanent_slots`) or pre-create the slot on standbys.
- **Both**: The CDC user must exist on B with the same privileges as on A.
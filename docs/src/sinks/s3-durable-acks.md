# S3 durable acknowledgements (durable_v2)

Operations guide for the crash-durable S3 sink. Covers what the mode guarantees,
rollout, lifecycle and garbage-collection settings, alarms, rollback, and the
multipart-upload posture.

## What durable_v2 guarantees

A batch is acknowledged to the source only after its manifest entry is committed to
`_manifest/HEAD` by a conditional compare-and-swap (CAS). Data objects are immutable
single PUTs written before the entry; the entry is written before the HEAD CAS. A
crash at any point leaves either a fully committed batch or an uncommitted orphan
that reconciliation removes. Acknowledged data is never lost across a crash, and
recovery is verified from the rollup chain plus the retained entry tail before a new
owner takes the epoch.

`durability` is the default: an unspecified `durability` resolves to `durable_v2`.
`legacy_rolling` is the explicit, non-durable rolling-file mode kept only for
rollback and compatibility (see [Rollback](#rollback)).

```yaml
sinks:
  - type: s3
    bucket: my-bucket
    # durability: durable_v2   # the default; may be omitted
```

## Rollout

1. Provision an S3-compatible bucket. The provider MUST honor `If-None-Match: *`
   (create-only) and `If-Match: <etag>` (CAS). The sink runs a capability probe at
   startup BEFORE acquiring the HEAD; a provider that silently ignores these
   conditions fails the probe and the sink refuses to start in durable mode. Do not
   force durable_v2 onto an unverified provider.
2. Stage first against MinIO or a staging bucket and run the live integration
   matrix:
   ```text
   export DELTAFORGE_IT_S3_ENDPOINT=http://localhost:9000
   export DELTAFORGE_IT_S3_BUCKET=deltaforge-it
   export DELTAFORGE_IT_S3_ACCESS_KEY=minioadmin
   export DELTAFORGE_IT_S3_SECRET_KEY=minioadmin
   cargo test -p sinks --lib -- --ignored minio
   ```
   These tests are `#[ignore]`d and require the environment above; run explicitly
   they exercise the real backend or fail loudly. They cover the probe, publish/ack
   plus restart recovery, concurrent-writer epoch fencing, cumulative-rollup
   fallback after entry GC, compaction, both GC domains, orphan reconciliation, and
   end-to-end recoverability after combined compaction, entry expiry, original
   deletion, and process restart.
3. Roll out one pipeline at a time. A single owner holds the HEAD per pipeline;
   epoch fencing makes a second concurrent owner safe (the stale one is fenced), but
   a rolling upgrade should still avoid deliberately running two writers.

## Lifecycle and garbage-collection settings

Deletion is opt-in and staged. Nothing on the acknowledgement path deletes. Three
background actors, all owner-fenced and serialized, reclaim space:

- **Mark entries** (`safety_window_ms`): writes immutable GC marks for manifest
  entries covered by the horizon (`prev_rollup.end_seq`). An entry is eligible only
  once the current rollup generation has been HEAD-published at least
  `safety_window_ms` ago. Set `safety_window_ms` to comfortably exceed your
  recovery/restart window and any read-after-write skew. Marking is non-destructive.
- **Expire entries**: consumes durable marks and deletes the marked entries.
- **Delete originals**: deletes originals that a HEAD-reachable, equivalence-true
  compaction record has superseded. Equivalence is proven at compaction time and
  bound into the record; deletion never recomputes it.

Reconciliation removes unreferenced (orphan) objects:

- **Reconcile** (`grace_period_ms`): deletes only objects unreachable from
  authoritative HEAD state AND older than `grace_period_ms`. The grace period
  prevents deleting an object that was just written and is about to be referenced.
  Set `grace_period_ms` to exceed the longest gap between a data PUT and its HEAD
  commit (seconds in normal operation; size for your worst case). Reconciliation is
  reachability-driven, never name-pattern-driven, and re-reads HEAD before any
  destructive step.

Older rollup and inventory objects are retained indefinitely, are excluded from GC,
and are never treated as orphans.

## Alarms

Wire these to your alerting. They are designed to be machine-readable.

- **`s3_non_durable_ack_mode` metric** (`deltaforge_sink_s3_non_durable_ack_mode`,
  labels `pipeline`, `sink`): `1` when a sink is running in `legacy_rolling`
  (non-durable) mode, `0` for durable sinks. Alert on `== 1`. A prominent startup
  warning is logged alongside it.
- **Missing referenced object** (hard alarm): recovery or reconciliation found HEAD
  referencing an object that is absent. This is a durability integrity violation.
  Page immediately; do not run any GC or reconciliation until resolved.
- **GC/reconcile stopped early**: an actor stopped on epoch change, a stale plan,
  listing uncertainty, an ambiguous provider response, or an integrity failure.
  Deletion never proceeds past a stop. Investigate the reason; a persistent stop
  means the actor is making no progress.
- **Writer fenced**: a writer lost ownership (another owner took a higher epoch).
  Expected once during a handover; sustained fencing means two owners are contending
  for one pipeline.
- **CAS conflicts rising** (`cas_conflicts`): HEAD contention, usually two writers.
  Confirm single ownership per pipeline.
- **Compaction lag growing** (`compaction_lag`): acknowledged originals are not
  being superseded; compaction is not keeping up and data GC has nothing to reclaim.

The durable writer also exposes an operational snapshot (`epoch`, `seq`,
`watermark`, current/previous rollup end sequences, and the GC horizon) for
dashboards.

## Multipart uploads

durable_v2 does not use S3 multipart uploads. Every data object and manifest object
is a single PUT, so there are no in-flight multipart sessions to abort or leak. The
reconciliation report's `mpu_aborts` is therefore always 0 on this path; a non-zero
value would indicate objects written by some other process. No multipart lifecycle
rule is required for correctness. An `AbortIncompleteMultipartUpload` lifecycle rule
remains harmless and is fine to keep for defense in depth.

## Rollback

`legacy_rolling` is selectable at any time for rollback:

```yaml
sinks:
  - type: s3
    bucket: my-bucket
    durability: legacy_rolling   # explicit, non-durable rolling files
```

When a sink starts in `legacy_rolling`, it logs a prominent warning and sets
`deltaforge_sink_s3_non_durable_ack_mode = 1`: acknowledged data can be lost before
a roll. Rollback is a config change and restart; it does not delete or rewrite any
durable_v2 state already in the bucket. To return to durable mode, remove the
override (or set `durability: durable_v2`) and restart; recovery verifies the
existing HEAD/chain and resumes.

`legacy_rolling` and `durable_v2` use different object layouts. Running legacy
against a bucket that holds durable_v2 state does not corrupt that state, but the two
do not share a manifest; treat a rollback as switching writers, not as continuing the
same durable stream.

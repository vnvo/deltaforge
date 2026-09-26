# Event Replay

Event replay re-delivers historical events from the durable replay journal to selected
sinks, without re-reading the source. Use it to recover from a consumer bug (re-deliver a
range after fixing a downstream processor), or to catch a sink up after an outage. Replay is
not a snapshot rebuild; it delivers exactly the commit units that were captured to the
journal.

> **Security requirement.** Replay endpoints start and cancel are mutating operations, and
> the DeltaForge HTTP API currently has no request-level authentication or authorization
> (this applies to every mutating endpoint, not just replay). Until platform-wide
> authN/authZ exists, you MUST restrict the API at the network layer (private network,
> firewall, service mesh, or an authenticating reverse proxy). Do not expose it to
> untrusted callers.

## How it works

Replay builds on the journal (the same internal storage that backs the [DLQ](dlq.md)):

1. When `journal.replay` is enabled, the coordinator captures every committed source commit
   unit (a transaction, a standalone CDC row, or a snapshot chunk) into a durable,
   append-only, incarnation-scoped log, idempotently (a retried capture does not duplicate).
2. A replay job reads that log from a starting sequence and re-delivers each commit unit to
   the job's selected sinks, running the pipeline's CURRENT processors and schema sensing
   (so replay picks up processor fixes).
3. While a job runs, its selected sinks are paused from live delivery and excluded from
   commit-policy evaluation, so a paused required sink does not stall the rest of the
   pipeline. Unselected sinks keep receiving live traffic.
4. When the job catches up to the journal tail, it performs a handoff: ingestion is briefly
   quiesced to a stable tail `H`, the selected sinks are delivered through `H`, then returned
   to the live set. The first live delivery to a restored sink is strictly after `H`.

Replay never advances or rewinds the source or per-sink checkpoints during the historical
and catch-up phases; only normal live delivery moves checkpoints after the handoff.

## Configuration

Replay is opt-in and requires BOTH the journal master switch and the replay sub-switch:

```yaml
spec:
  journal:
    enabled: true               # journal master switch (also gates the DLQ)
    max_event_bytes: 262144
    replay:
      enabled: true
      retention_secs: 86400     # drop journal entries older than this (0 = no age limit)
      max_entries: 0            # capacity cap (0 = unbounded)
      max_bytes: 0              # capacity cap (0 = unbounded)
      max_envelope_bytes: 8388608   # 8 MiB; a commit unit larger than this fails closed
```

Replay capture requires transaction-aligned batching: a replay-enabled pipeline must run
with `batch.respect_source_tx = true`, otherwise it is rejected at startup (the legacy path
ignores transaction markers and would capture nothing).

Retention never truncates a range an active job still needs: while a job runs, its cursor
pins the journal so entries it has not yet replayed are retained.

## REST API

All endpoints live under a pipeline's journal namespace, alongside the DLQ endpoints.

### Start (or dry-run) a replay

```
POST /pipelines/{name}/journal/replay
Content-Type: application/json

{
  "selected_sinks": ["kafka"],      // existing live sinks to pause and replay to (required)
  "from_seq": 0,                     // replay journal entries with seq > from_seq
  "through_seq": null,               // optional inclusive upper bound of the historical range
  "encoder_schema_policy": "current",// optional; see Limitations
  "dry_run": false                   // optional; report the range/targets, deliver nothing
}
```

Response `200`:

```json
{ "job_id": "018f..." }
```

Errors:

| Status | Code | When |
|--------|------|------|
| 400 | `BAD_REQUEST` | malformed request: unknown/unsupported encoder policy, invalid range (`through_seq < from_seq`), a selected sink that is not part of the pipeline, an empty target set, a staged sink (not supported), or a replay that would leave the commit quorum unsatisfiable |
| 409 | `CONFLICT` | a replay job is already active for this pipeline, or the pipeline changed (stop/restart/reconfigure) during the start |
| 500 | `INTERNAL_ERROR` | storage or internal failure |

### Get the current job status

```
GET /pipelines/{name}/journal/replay
```

Response `200` is the current (or most recent) job, or `null` when none exists:

```json
{
  "job_id": "018f...",
  "phase": "catching_up",
  "cursor": 4210,
  "from_seq": 0,
  "through_seq": null,
  "selected_sinks": ["kafka"],
  "staged_sinks": [],
  "dry_run": false,
  "created_at_ms": 1750000000000,
  "updated_at_ms": 1750000000500,
  "error": null
}
```

### Cancel the active job

```
POST /pipelines/{name}/journal/replay/cancel
```

Returns `204 No Content`. Cancellation is idempotent when no job is active; it persists the
cancellation durably before returning the paused sinks to the live set.

## Job phases

```
Running -> CatchingUp -> HandoffQuiesced(H) -> DeliveredThrough(H) -> LiveRestored -> Completed
```

- **running** - delivering the requested historical range to the selected sinks.
- **catching_up** - delivering entries captured after the job started, toward the tail.
- **handoff_quiesced** / **delivered_through** - the acknowledged handoff at the frozen tail `H`.
- **live_restored** - the sinks have rejoined the live set; live delivery resumes after `H`.
- **completed** - terminal, live delivery fully restored.
- **cancelled** / **failed** - terminal off-ramps (`failed` records a reason in `error`).

The job phase and cursor are durable. A restart resumes from the recorded phase: an
in-flight historical/catch-up job continues from its cursor, and a job interrupted mid-handoff
re-quiesces and finishes. Delivery is at-least-once, so a re-run may re-deliver units the sink
already received.

## Guarantees

- **At-least-once.** An interruption after a sink acknowledges an envelope but before the
  cursor is persisted causes that envelope to be re-delivered on restart. Dedup-capable sinks
  absorb the duplicate; append-only sinks receive it twice by design.
- **Fail-closed capture.** A commit unit that cannot be captured (missing event id, serialize
  failure, or an envelope over `max_envelope_bytes`) aborts the pipeline rather than advancing
  the checkpoint past uncaptured data.
- **Pause/handoff.** Selected sinks are excluded from live delivery and commit policy while a
  job runs, and rejoin the live set only at the handoff, with the first live delivery strictly
  after the frozen tail `H`.
- **No checkpoint mutation.** Replay does not touch the source or per-sink checkpoints during
  the historical and catch-up phases.

## Limitations (current release)

- **Encoder schema policy.** Only the default `current` policy is honored (the sink API
  cannot yet select a schema). `at_capture_seq` and `pinned:<seq>` are rejected at start.
- **Staged-sink backfill.** Adding a brand-new sink and backfilling it (staged sinks) is not
  supported yet; `staged_sinks` must be empty. Replay targets existing pipeline sinks only.
- **One job per pipeline.** At most one active replay job exists per pipeline incarnation; a
  second start returns `409`.

## Operational validation (canary)

Before relying on replay in an environment, run the disposable-pipeline canary in
[`docs/ops/replay-canary/`](https://github.com/deltaforge/deltaforge/tree/main/docs/ops/replay-canary).
It verifies selected-sink pause/restoration, at-least-once duplicate handling,
restart-during-replay resume, retention and journal-growth metrics, and the
network-restriction requirement above.

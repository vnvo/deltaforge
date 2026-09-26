# Event Replay canary runbook (disposable pipeline)

Goal: validate the shipped Event Replay milestone end to end on a throwaway
stack before starting the deterministic-rebuild design audit. Five checks:

1. Selected-sink pause and restoration
2. Duplicate handling under at-least-once delivery
3. Restart during replay
4. Retention and journal-growth metrics
5. Network restriction around the REST API

Everything is disposable: use `docker compose -f docker-compose.dev.yml` for
postgres + redis, drop the whole stack afterwards. You run docker manually; this
runbook only drives verification.

## Relationship to the chaos suite

This is the manual/operator form of the canary. Checks 1-4 are runtime behaviours
that also belong in the [chaos suite](../../../crates/chaos/README.md) as a
`replay_canary` scenario (the harness already does SIGKILL/crash recovery, metric
scraping, and sink-offset reads). Check 5 (network restriction) is a deployment
configuration assertion, not a runtime fault, so it stays here as an operator
check rather than a harness scenario. Until the scenario lands, run the canary
from this runbook.

## Topology

- Source: postgres (logical replication), table `public.orders`.
- Two REQUIRED redis-stream sinks on the same redis, both trivially observable:
  - `replay_target` -> stream `df.canary.replay`  (SELECTED for replay; paused during a job)
  - `live_only`     -> stream `df.canary.live`     (stays live; proves the rest of the pipeline keeps flowing)
- Both sinks required + `commit_policy.mode=required`: excluding `replay_target`
  still leaves one required sink, so the start-authorization quorum check passes.

Files in this kit: `canary-pipeline.yaml` (the spec), `canary.sh` (the driver).

## 0. Bring up the disposable stack

```bash
docker compose -f docker-compose.dev.yml up -d postgres redis

# pre-create the slot + publication the source expects
docker compose -f docker-compose.dev.yml exec -T postgres \
  psql -U postgres -c "CREATE TABLE IF NOT EXISTS public.orders(id serial primary key, note text);" \
  -c "CREATE PUBLICATION deltaforge_pub FOR TABLE public.orders;" \
  -c "SELECT pg_create_logical_replication_slot('deltaforge_slot','pgoutput');"
```

Start the runner against the canary spec (env expands inside the YAML):

```bash
export POSTGRES_DSN="postgres://postgres:postgres@127.0.0.1:5432/postgres"
export REDIS_URI="redis://127.0.0.1:6379"
# storage: sqlite file is fine for a canary; use a fresh path so restart reuses it
cargo run -p runner -- \
  --config docs/ops/replay-canary/canary-pipeline.yaml \
  --storage-backend sqlite --storage-path ./data/canary.db \
  --api-addr 127.0.0.1:8080          # loopback bind is itself part of check 5
# metrics: Prometheus text at http://127.0.0.1:9000/metrics
```

Driver env (in the shell you run `canary.sh` from):

```bash
export API=http://127.0.0.1:8080 METRICS=http://127.0.0.1:9000 REDIS_URL=redis://127.0.0.1:6379
```

Seed some history so there is a journal to replay: INSERT a few dozen rows into
`public.orders`, let them flow to both sinks, confirm `XLEN df.canary.replay`
and `df.canary.live` both grew.

## 1. Selected-sink pause and restoration

```bash
./canary.sh 1
```

The driver starts a replay selecting `replay_target`, then asks you to commit
~20 fresh live rows. Expected:

- `df.canary.live` (live_only) keeps growing during the job -> the pipeline is
  not stalled by the paused required sink.
- `df.canary.replay` receives the replayed range, then the handoff.
- Job phase walks `running -> catching_up -> handoff_quiesced -> delivered_through
  -> live_restored -> completed`.
- After `completed`, `replay_target` is back in the live set; its first live
  delivery is strictly after the frozen tail `H` (no live delivery during pause).

## 2. Duplicate handling under at-least-once

```bash
./canary.sh 2
```

Runs the same bounded historical range twice. On an append-only redis stream the
second run adds entries again -> at-least-once re-delivery confirmed (duplicates
by design). A dedup-capable sink would instead absorb them (also acceptable).
`deltaforge_replay_delivered_total` increases across both runs.

For a stricter interruption test (ack-before-cursor-persist), combine with
check 3: kill the runner mid-job and confirm the envelopes after the last
persisted cursor are re-delivered on resume.

## 3. Restart during replay

```bash
./canary.sh 3-arm      # starts a large replay, prints cursor + job_id
# --> hard-kill the runner (docker kill / SIGKILL) while it is running/catching_up
# --> restart it with the SAME --config and SAME --storage-path
./canary.sh 3-verify   # confirms the SAME job resumed from its durable cursor
```

Expected: after restart, `GET .../journal/replay` shows the same `job_id`, phase
resumes (in-flight historical/catch-up continues from cursor; a job killed
mid-handoff re-quiesces and finishes), cursor is monotonic, job reaches
`completed`. A restart in `handoff_quiesced`/`delivered_through` must re-quiesce
and finish, not roll back to live prematurely.

## 4. Retention and journal-growth metrics

```bash
./canary.sh 4
```

Scrapes and interprets:

| Metric | Meaning / pass signal |
|---|---|
| `deltaforge_replay_captured_total` | grows as live commits land -> journal capturing |
| `deltaforge_replay_delivered_total` | grows during a replay job |
| `deltaforge_replay_scanned_total` | dry-run/scan progress |
| `deltaforge_replay_retention_removed_total` | > 0 only after entries age past `retention_secs` |
| `deltaforge_replay_retention_capacity_pinned_total` | increments if a cap was hit while a job pinned the range (retention held back) |
| `deltaforge_replay_capture_oversized_total` | envelopes over `max_envelope_bytes` (should be 0) |
| `deltaforge_replay_capture_failures_total` | fail-closed capture errors (should be 0) |
| `deltaforge_replay_job_failed_total` | failed jobs (should be 0) |

To exercise retention actively: set `retention_secs: 30` in the spec, generate
traffic, idle > 30s with no active job, and watch `retention_removed_total` rise
while journal size drops. To exercise the pin: start a job, then confirm old
entries the job still needs are NOT removed until the job completes
(`retention_capacity_pinned_total` may increment under a capacity cap).

## 5. Network restriction around the REST API

```bash
./canary.sh 5
```

The API has NO request-level authN/authZ, and replay start/cancel are mutating.
Pass criteria (manual):

- Trusted vantage (loopback / private net) reaches `/pipelines`.
- From an untrusted host, `curl -m 3 http://<public-ip>:8080/pipelines/replay-canary/journal/replay`
  is refused/timed out (blocked by firewall / private compose network / mesh /
  authenticating proxy).
- Control confirmed: runner bound to `127.0.0.1` (as above) or otherwise
  network-restricted, matching the security requirement in `docs/src/replay.md`.

Caveat worth recording: the Prometheus scrape listener is hardcoded to
`0.0.0.0:9000` in `crates/runner/src/main.rs` and is NOT governed by
`--metrics-addr`. Binding the API to loopback does not restrict `:9000`, so the
firewall/private-network control must cover the metrics port too (metrics are
read-only, but still exposed). Flag this as a small follow-up: make the metrics
bind configurable.

## Teardown

```bash
# stop the runner, then:
docker compose -f docker-compose.dev.yml down -v
rm -f ./data/canary.db
```

## Report back

Note for each of the 5 checks: PASS / observation. A stable canary (all five
green, no `capture_failures_total`/`job_failed_total`) is the gate for starting
the deterministic snapshot + replay rebuild as a fresh design audit.

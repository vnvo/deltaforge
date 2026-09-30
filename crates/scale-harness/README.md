# scale-harness

Measurement harness for the dense-catalog work: proves that schema registry
startup, lookups and migration scale with the active working set, not with
the total catalog or schema-history size, and that one source never reads
another source's records.

One implementation, two tiers:

- **CI tier** (`cargo test -p scale-harness`): fresh 1K and 2K-table stores,
  counters reset after generation, and every scenario must produce the SAME
  complete per-primitive storage-operation vector (calls and bytes) at both
  sizes, with no enumeration of registry namespaces. Structural only; nothing
  is timed.
- **Scale tier** (`registry-scale` binary): 100K / 1M tables, numbers recorded
  in a JSON report, not asserted.

## Running the scale tier

```
cargo run --release -p scale-harness --bin registry-scale -- \
    --tables 100000 --versions 3 --sources 2 \
    --legacy-tables 10000 --legacy-versions 1,5 --migration-pages 16,256 \
    --backend sqlite --store /data/reg-100k.db \
    --live postgres,mysql --live-tables 100000 --live-dir /data \
    --out report-100k.json
```

- The store is generated through the real registration path and stamped with
  a manifest (generator format version, seed, counts, payload shape, backend,
  git revision). A later run with the same arguments reuses it; a store whose
  manifest differs, an interrupted generation, or a store with schema data but
  no manifest is refused. `--allow-revision-mismatch` reuses a store generated
  at another revision.
- `--backend postgres --dsn ...` runs against a PostgreSQL state store.
- `--live` (needs Docker) adds scenario 8 on a fresh SQLite store per engine.
- `--live-only` skips scenarios 1-7.

## Scenarios

| # | Scenario | Reported |
|---|---|---|
| 1 | Registry startup on a populated store | ops, time |
| 2 | Cold then hot `get_latest` of the active set | ops, latency; hot must touch no storage |
| 3 | Active set 4x larger than the cache byte budget | evictions, resident bytes vs budget |
| 4 | Concurrent lookups of one cold key | backend loads (must be 1) |
| 5 | History paging of one table | pages, versions, ops |
| 6 | Cross-source isolation, logical table names colliding across sources | reads under another source's prefix (must be 0), returned identity, cache residency |
| 7 | Migration plan + apply of a legacy population | time, ops; each run in a fresh process: process peak and increase over its start |
| 8 | Time to first CDC event on a CDC-only restart (live PostgreSQL / MySQL) | time, ops and reads per namespace in the window, registry enumeration (must be 0) |

Scenario 8 boundaries: the database is already ready and the synthetic
registry for the live source's verified lineage is generated first (both
excluded); a first run commits a checkpoint and stops; a known row is
committed while stopped; the timer starts immediately before source startup
and stops when an instrumented sink receives exactly that row.

Migration memory: history is processed one page at a time per table, but the
mapping, canonical plan, progress and report are proportional to the number of
mapped tables. The report gives the total process peak and the per-table
increase for each (versions per table, page size) combination; it does not
claim total migration memory is bounded by the page size.

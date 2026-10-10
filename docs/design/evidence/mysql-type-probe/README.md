# MySQL type probe evidence

Produced by `crates/runner/tests/mysql_type_probe.rs`, which pins every container image by digest, and reported by `scripts/type-probe-report.py`. The analysis is in `docs/design/mysql-canonical-types.md`.

| Directory | Run | S3 sink |
|---|---|---|
| `main/` | `20261010T215519Z` | `legacy_rolling`, so the Parquet encoder is observable |
| `durable-s3/` | `20261010T220017Z` | `durable_v2` (`TYPE_PROBE_S3_DURABILITY=durable_v2`); every Parquet pipeline fails, see `s3-key-excerpt.txt` |

Each directory holds:

| File | Contents |
|---|---|
| `report.md` | the sanitized per-value report |
| `env.json` | the environment manifest: MySQL version, SQL modes, character sets, collations, time zones, binlog settings, pinned images, process time zone. Its `deltaforge_revision` is the base commit the run was made on. |
| `SHA256SUMS` | SHA-256 of every raw file of the run: records, schemas, SQL log, pipeline outcomes, warnings log. The raw files are kept by the author, not committed. |
| `CODE-SHA256SUMS` | SHA-256 of the probe and the report script used, identifying the exact code independently of commits |

**To reproduce,** run `cargo test -p runner --test mysql_type_probe -- --include-ignored --nocapture` (Docker required), then `python3 scripts/type-probe-report.py target/type-probe/<run>`.

- **What changes between runs:** timestamps, server UUIDs and GTID sets, so the raw-file checksums differ.
- **What stays the same:** the per-value tables.
- **One known non-deterministic detail:** how many rows a failing typed pipeline delivers before its first failing row (`t_time` via Avro: 1 or 2) depends on batch boundaries. The failure itself is deterministic.

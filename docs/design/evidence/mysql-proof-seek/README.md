# Evidence: MySQL proof seek and exact GTID sets

The probe scripts and logs referenced by `docs/design/mysql-proof-seek.md`.
Each script starts its own throwaway MySQL 8.4 container (removed with its
volumes on exit) and writes its log beside itself. The logs here are the
runs the design cites.

| Script | Log | What it shows |
|---|---|---|
| `run.sh` (probes 1-5) | `protocol-probes-964mib.log`, `protocol-probes-15mb.log` | A GTID dump given a file and offset starts there. Bad offsets, purged files and late anchors behave as the design's table describes. The seek cost at about 964 MiB is 1,252 ms against 2 ms positioned. |
| `gaps.sh <runner>` (probe 6) | `gtid-gap-restart.log` | A GTID gap that fills while DeltaForge is down: the pre-#170 runner loses the gap's rows. |
| `group-replication.sh <runner> <label>` | `group-replication-main-cd716061.log`, `group-replication-fixed-d254837d.log` | The same on a two-member multi-primary group. Runner digests are in `group-replication-runners.sha256`. |
| `capture.sh` (probe 7) | `batched-capture-probe7.log` | The cost of batched captures, and their behavior with concurrent DDL and lock order. |
| `frontier.sh` (probe 8) | `frontier-probe8.log` | Positioned starts at a compressed transaction and at a record frontier, and the live end. |

`gaps.sh` and `group-replication.sh` use `receiver.py`, an HTTP sink
target that appends each request body to a file.

`run.sh` and `frontier.sh` run `seekprobe`, built from `seekprobe.rs` with
a copy of the vendored `mysql-binlog-connector-rust`. The copy has one
change: `DumpBinlogGtidCommand` also sends the client's `binlog_filename`
and `binlog_position`, where the vendored code sends an empty name and
position 4.

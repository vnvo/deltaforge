# DeltaForge patch of mysql-binlog-connector-rust 0.3.3

Upstream: https://github.com/apecloud/mysql-binlog-connector-rust (MIT OR
Apache-2.0, license files kept). Used through `[patch.crates-io]` in the
workspace manifest. Changes are in `src/event/table_map/` (metadata parsing)
and one addition in `src/network/packet_channel.rs` / `src/binlog_stream.rs`
(abortive close); row decoding is untouched.

The optional TableMap metadata (`binlog_row_metadata=FULL`) was parsed onto
the wrong columns, which DeltaForge's schema-signature comparison (design
spec 7.14 #4) cannot accept. Fixes, matching MySQL's writer
(`Table_map_log_event` metadata fields):

- **Signedness:** MySQL's numeric set includes YEAR; without it every bit
  after the first YEAR column shifted onto the next numeric column and the
  last bits were never read. A set bit means UNSIGNED, so `is_signed` is its
  negation (it was stored as-is).
- **COLUMN_CHARSET / ENUM_AND_SET_COLUMN_CHARSET:** one collation per
  eligible column in column order (character columns: string and BLOB
  types, binary included, not ENUM/SET/JSON/GEOMETRY; ENUM/SET columns),
  instead of columns 0, 1, 2, ...
- **DEFAULT_CHARSET / ENUM_AND_SET_DEFAULT_CHARSET:** expanded to every
  eligible column, applying the `(index among eligible columns, collation)`
  exceptions.
- **GEOMETRY_TYPE:** one subtype per GEOMETRY column, instead of columns
  0, 1, 2, ...
- **Primary key:** `TableMetadata::primary_key` keeps the key in key order
  with prefix lengths (SIMPLE_PRIMARY_KEY / PRIMARY_KEY_WITH_PREFIX); the
  per-column flags were order-less.
- More collations or geometry types than eligible columns is an error.
- `TableMetadata` and `DefaultCharset` derive `PartialEq`/`Eq` so a parsed
  TableMap can be compared as part of a signature (in
  `src/event/table_map/default_charset.rs` as well).

Verified by the crate's unit tests (two updated to MySQL's signedness
semantics, six added) and by DeltaForge's live test
`mysql_signature::tests::live::a_real_table_map_matches_its_captured_schema`
(MySQL 8.4, every supported type, composite prefixed key, invisible column,
charset exceptions; FULL and MINIMAL).

## Abortive close (`abort_on_drop`, `linger`)

`PacketChannel::abort_on_drop` / `BinlogStream::abort_on_drop` set
`SO_LINGER` to zero on the connection's socket, so dropping it resets the
connection instead of closing it gracefully. A MySQL binlog dump thread does
not read from its connection; it notices a closed client only when a write
fails. After a reset its next write (a heartbeat on an idle server) fails at
once. DeltaForge's binlog interval scanner uses it to end its short-lived
dump sessions promptly without killing any connection by id. It acts only on
the connection it is called on.

`PacketChannel::linger` / `BinlogStream::linger` read the socket's
`SO_LINGER`, so callers and tests can verify the abortive close is armed.
DeltaForge arms it right after the TCP connection is established, before
any other awaited step, so a cancelled open also resets the connection.

## Structured temporal values and YEAR 0

`ColumnValue::Date`, `Time` and `DateTime` carry the stored parts
(`MysqlDate`, `MysqlTime`, `MysqlDateTime`) instead of formatted strings, so
DeltaForge's schema-driven decoder reads the exact value without parsing
text: zero date parts, a signed TIME duration with its microseconds, and
DATETIME's microseconds. The pre-5.6.4 TIME format is read as the signed
`HHMMSS` integer it is. YEAR's stored byte 0 is the zero year (MySQL shows
`0000`), not 1900. File: `src/column/column_value.rs`.

## Files and lint policy

Files changed from upstream 0.3.3 (each is listed as `patched` in
`scripts/vendor-crates.txt`, so DeltaForge's gate holds it to rustfmt and
clippy with no findings, while the rest of the crate stays as upstream ships
it): `src/binlog_stream.rs`, `src/column/column_value.rs`,
`src/event/table_map/default_charset.rs`,
`src/event/table_map/table_metadata.rs`, `src/network/packet_channel.rs`.
The patched files are formatted with rustfmt; `manual_div_ceil` and
`bool_assert_comparison` findings in them are fixed (`div_ceil`, `assert!`),
with no behavior change.


## A start position for a GTID dump (`gtid_start`)

`BinlogClient::gtid_start` (default `None`) gives a GTID-mode dump the
binlog file and offset to start at. `DumpBinlogGtidCommand` sends them in
the `COM_BINLOG_DUMP_GTID` packet's file name and position fields, which
the vendored code always sent empty and 4. The server then starts at that
offset and still skips the transactions in the GTID set. Without it, the
server picks the first file not covered by the set and reads it from its
start, which costs time proportional to the file's earlier events (about
1.8 s per dump at 1 GiB of single-row transactions on a laptop). With
`None` the packet is byte-identical to before.

DeltaForge's proof scanner sets it only to a transaction boundary it has
observed in a binlog stream, and validates the first events the server
sends before trusting the dump (`docs/design/mysql-proof-seek.md`). Changed
files: `src/binlog_client.rs`, `src/command/command_util.rs` and
`src/command/dump_binlog_gtid_command.rs`, with two unit tests of the
packet encoding.

## Cancel-safe packet reads

`PacketChannel::read` (and `read_with_sequece`) keep the bytes read from the
socket in the channel, not in the read's future, and take a packet out only
once all of it (all chunks of a large packet) has arrived. Each socket read
appends what it read before its future completes. A read dropped at any
await point (a `select!` or a timeout around it) therefore loses nothing:
the next read returns the same packet. The vendored code read the header
and the body into local buffers with `read_exact`, so a cancelled read
dropped part of a packet and the next read misframed the stream. Socket
reads are still bounded by `timeout_secs` each, and zero-length reads are
still counted the same way. DeltaForge's binlog loop needs this to bound
an activation window by time (`docs/design/mysql-proof-seek.md`) and
already raced reads against credential-rotation activity. File:
`src/network/packet_channel.rs`, with two unit tests (a read cancelled
inside a packet, and a large packet in chunks).

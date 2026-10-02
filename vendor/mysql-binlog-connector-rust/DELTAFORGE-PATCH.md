# DeltaForge patch of mysql-binlog-connector-rust 0.3.3

Upstream: https://github.com/apecloud/mysql-binlog-connector-rust (MIT OR
Apache-2.0, license files kept). Used through `[patch.crates-io]` in the
workspace manifest. Only `src/event/table_map/` changes;
row decoding is untouched.

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

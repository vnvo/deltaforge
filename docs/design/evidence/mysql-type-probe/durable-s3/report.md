# MySQL type probe: raw comparison

Probe run: `20261010T220017Z`

## Environment

| setting | value |
|---|---|
| `version` | `8.4.9` |
| `version_comment` | `MySQL Community Server - GPL` |
| `GLOBAL.sql_mode` | `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION` |
| `SESSION.sql_mode` | `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION` |
| `character_set_server` | `utf8mb4` |
| `collation_server` | `utf8mb4_0900_ai_ci` |
| `character_set_client` | `utf8mb4` |
| `character_set_connection` | `utf8mb4` |
| `character_set_results` | `utf8mb4` |
| `character_set_database` | `utf8mb4` |
| `collation_connection` | `utf8mb4_general_ci` |
| `default_collation_for_utf8mb4` | `utf8mb4_0900_ai_ci` |
| `GLOBAL.time_zone` | `+02:00` |
| `SESSION.time_zone` | `+02:00` |
| `system_time_zone` | `UTC` |
| `binlog_format` | `ROW` |
| `binlog_row_image` | `FULL` |
| `binlog_row_metadata` | `MINIMAL` |
| `binlog_row_value_options` | `` |
| `explicit_defaults_for_timestamp` | `1` |
| `lower_case_table_names` | `0` |
| `gtid_mode` | `ON` |
| `server_uuid` | `07cfa96d-c4f6-11f1-82cf-0242ac110003` |
| `server_now_and_utc` | `['2026-10-11 00:00:49.433608', '2026-10-10 22:00:49.433608']` |
| `images` | `{'mysql': 'mysql:8.4@sha256:c36050afdca850f23cef85703f84c7531a5ae155a11b5ee1c60acb09937c4084', 'kafka': 'confluentinc/cp-kafka:7.5.0@sha256:fbbb6fa11b258a88b83f54d4f0bddfcffbf2279f99d66a843486e3da7bdfbf41', 'schema_registry': 'confluentinc/cp-schema-registry:7.5.0@sha256:e51684b472a2481f065f44616d3d8ad2182029a7011f949a612d35b54566a1f6', 'rustfs': 'rustfs/rustfs:1.0.1@sha256:1803faef57627e2d9c2e7d89d655d712ddded5389040054987163043fecb6a3c'}` |
| `server_args_default_time_zone` | `+02:00` |
| `process_tz_env` | `None` |
| `process_local_offset` | `+03:00` |
| `deltaforge_revision` | `47edd54b1cb31c0881ba1e517665ed36b9a70358` |

## Pipeline outcomes

| run | sink | table | status | records / expected | first error |
|---|---|---|---|---|---|
| full_initial | avro | t_bin | failed | 0 / 25 |  |
| full_initial | avro | t_bit | failed | 0 / 21 |  |
| full_initial | avro | t_enum | failed | 0 / 21 |  |
| full_initial | avro | t_int | failed | 0 / 17 |  |
| full_initial | avro | t_json | failed | 0 / 53 |  |
| full_initial | avro | t_num | failed | 0 / 25 |  |
| full_initial | avro | t_str | failed | 0 / 17 |  |
| full_initial | avro | t_time | failed | 0 / 21 |  |
| full_initial | json | t_bin | running | 25 / 25 |  |
| full_initial | json | t_bit | running | 21 / 21 |  |
| full_initial | json | t_enum | running | 21 / 21 |  |
| full_initial | json | t_int | running | 17 / 17 |  |
| full_initial | json | t_json | running | 53 / 53 |  |
| full_initial | json | t_num | running | 25 / 25 |  |
| full_initial | json | t_str | running | 17 / 17 |  |
| full_initial | json | t_time | running | 21 / 21 |  |
| full_initial | parquet | t_bin | failed | 0 / 25 | backpressure: durable publish retryable: object store: object store error: Generic S3 error: Error performing  |
| full_initial | parquet | t_bit | failed | 0 / 21 | fatal: parquet encode for probe_full_initial.t_bit: b1: expected bool, got String("\0") |
| full_initial | parquet | t_enum | failed | 0 / 21 | backpressure: durable publish retryable: object store: object store error: Generic S3 error: Error performing  |
| full_initial | parquet | t_int | failed | 0 / 17 | fatal: parquet encode for probe_full_initial.t_int: ti: expected number, got String("-128") |
| full_initial | parquet | t_json | failed | 0 / 53 | backpressure: durable publish retryable: object store: object store error: Generic S3 error: Error performing  |
| full_initial | parquet | t_num | failed | 0 / 25 | fatal: parquet encode for probe_full_initial.t_num: f: expected number, got String("3.40282e38") |
| full_initial | parquet | t_str | failed | 0 / 17 | backpressure: durable publish retryable: object store: object store error: Generic S3 error: Error performing  |
| full_initial | parquet | t_time | failed | 0 / 21 | fatal: parquet encode for probe_full_initial.t_time: d: parse 0000-00-00 as YYYY-MM-DD |
| full_never | avro | t_bin | running | 19 / 19 |  |
| full_never | avro | t_bit | failed | 2 / 16 |  |
| full_never | avro | t_enum | running | 16 / 16 |  |
| full_never | avro | t_int | running | 13 / 13 |  |
| full_never | avro | t_json | running | 40 / 40 |  |
| full_never | avro | t_num | running | 19 / 19 |  |
| full_never | avro | t_str | running | 13 / 13 |  |
| full_never | avro | t_time | failed | 1 / 16 |  |
| full_never | json | t_bin | running | 19 / 19 |  |
| full_never | json | t_bit | running | 16 / 16 |  |
| full_never | json | t_enum | running | 16 / 16 |  |
| full_never | json | t_int | running | 13 / 13 |  |
| full_never | json | t_json | running | 40 / 40 |  |
| full_never | json | t_num | running | 19 / 19 |  |
| full_never | json | t_str | running | 13 / 13 |  |
| full_never | json | t_time | running | 16 / 16 |  |
| full_never | parquet | t_bin | failed | 0 / 19 | backpressure: durable publish retryable: object store: object store error: Generic S3 error: Error performing  |
| full_never | parquet | t_bit | failed | 0 / 16 | backpressure: durable publish retryable: object store: object store error: Generic S3 error: Error performing  |
| full_never | parquet | t_enum | failed | 0 / 16 | backpressure: durable publish retryable: object store: object store error: Generic S3 error: Error performing  |
| full_never | parquet | t_int | failed | 0 / 13 | backpressure: durable publish retryable: object store: object store error: Generic S3 error: Error performing  |
| full_never | parquet | t_json | failed | 0 / 40 | backpressure: durable publish retryable: object store: object store error: Generic S3 error: Error performing  |
| full_never | parquet | t_num | failed | 0 / 19 | backpressure: durable publish retryable: object store: object store error: Generic S3 error: Error performing  |
| full_never | parquet | t_str | failed | 0 / 13 | backpressure: durable publish retryable: object store: object store error: Generic S3 error: Error performing  |
| full_never | parquet | t_time | failed | 0 / 16 | fatal: parquet encode for probe_full_never.t_time: unsupported Arrow data type for column tm: Time32(Milliseco |
| minimal_initial | avro | t_bin | failed | 0 / 25 |  |
| minimal_initial | avro | t_bit | failed | 0 / 21 |  |
| minimal_initial | avro | t_enum | failed | 0 / 21 |  |
| minimal_initial | avro | t_int | failed | 0 / 17 |  |
| minimal_initial | avro | t_json | failed | 0 / 53 |  |
| minimal_initial | avro | t_num | failed | 0 / 25 |  |
| minimal_initial | avro | t_str | failed | 0 / 17 |  |
| minimal_initial | avro | t_time | failed | 0 / 21 |  |
| minimal_initial | json | t_bin | running | 25 / 25 |  |
| minimal_initial | json | t_bit | running | 21 / 21 |  |
| minimal_initial | json | t_enum | running | 21 / 21 |  |
| minimal_initial | json | t_int | running | 17 / 17 |  |
| minimal_initial | json | t_json | running | 53 / 53 |  |
| minimal_initial | json | t_num | running | 25 / 25 |  |
| minimal_initial | json | t_str | running | 17 / 17 |  |
| minimal_initial | json | t_time | running | 21 / 21 |  |
| minimal_initial | parquet | t_bin | failed | 0 / 25 | backpressure: durable publish retryable: object store: object store error: Generic S3 error: Error performing  |
| minimal_initial | parquet | t_bit | failed | 0 / 21 | fatal: parquet encode for probe_minimal_initial.t_bit: b1: expected bool, got String("\0") |
| minimal_initial | parquet | t_enum | failed | 0 / 21 | backpressure: durable publish retryable: object store: object store error: Generic S3 error: Error performing  |
| minimal_initial | parquet | t_int | failed | 0 / 17 | fatal: parquet encode for probe_minimal_initial.t_int: ti: expected number, got String("-128") |
| minimal_initial | parquet | t_json | failed | 0 / 53 | backpressure: durable publish retryable: object store: object store error: Generic S3 error: Error performing  |
| minimal_initial | parquet | t_num | failed | 0 / 25 | fatal: parquet encode for probe_minimal_initial.t_num: f: expected number, got String("3.40282e38") |
| minimal_initial | parquet | t_str | failed | 0 / 17 | backpressure: durable publish retryable: object store: object store error: Generic S3 error: Error performing  |
| minimal_initial | parquet | t_time | failed | 0 / 21 | fatal: parquet encode for probe_minimal_initial.t_time: d: parse 0000-00-00 as YYYY-MM-DD |
| minimal_never | avro | t_bin | running | 19 / 19 |  |
| minimal_never | avro | t_bit | failed | 1 / 16 |  |
| minimal_never | avro | t_enum | running | 16 / 16 |  |
| minimal_never | avro | t_int | running | 13 / 13 |  |
| minimal_never | avro | t_json | running | 40 / 40 |  |
| minimal_never | avro | t_num | running | 19 / 19 |  |
| minimal_never | avro | t_str | running | 13 / 13 |  |
| minimal_never | avro | t_time | failed | 1 / 16 |  |
| minimal_never | json | t_bin | running | 19 / 19 |  |
| minimal_never | json | t_bit | running | 16 / 16 |  |
| minimal_never | json | t_enum | running | 16 / 16 |  |
| minimal_never | json | t_int | running | 13 / 13 |  |
| minimal_never | json | t_json | running | 40 / 40 |  |
| minimal_never | json | t_num | running | 19 / 19 |  |
| minimal_never | json | t_str | running | 13 / 13 |  |
| minimal_never | json | t_time | running | 16 / 16 |  |
| minimal_never | parquet | t_bin | failed | 0 / 19 | backpressure: durable publish retryable: object store: object store error: Generic S3 error: Error performing  |
| minimal_never | parquet | t_bit | failed | 0 / 16 | backpressure: durable publish retryable: object store: object store error: Generic S3 error: Error performing  |
| minimal_never | parquet | t_enum | failed | 0 / 16 | backpressure: durable publish retryable: object store: object store error: Generic S3 error: Error performing  |
| minimal_never | parquet | t_int | failed | 0 / 13 | backpressure: durable publish retryable: object store: object store error: Generic S3 error: Error performing  |
| minimal_never | parquet | t_json | failed | 0 / 40 | backpressure: durable publish retryable: object store: object store error: Generic S3 error: Error performing  |
| minimal_never | parquet | t_num | failed | 0 / 19 | backpressure: durable publish retryable: object store: object store error: Generic S3 error: Error performing  |
| minimal_never | parquet | t_str | failed | 0 / 13 | backpressure: durable publish retryable: object store: object store error: Generic S3 error: Error performing  |
| minimal_never | parquet | t_time | failed | 0 / 16 | fatal: parquet encode for probe_minimal_never.t_time: unsupported Arrow data type for column tm: Time32(Millis |

## SQL statements refused or warned

- `DROP DATABASE IF EXISTS probe_minimal_initial` (warned, sql_mode `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION`): Can't drop database 'probe_minimal_initial'; database doesn't exist
- `CREATE TABLE probe_minimal_initial.t_int (id BIGINT PRIMARY KEY, ti TINYINT, tiu TINYINT UNSIGNED, si SMALLINT` (warned, sql_mode `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION`): Integer display width is deprecated and will be removed in a future release.
- `INSERT INTO probe_minimal_initial.t_int (id, tiu) VALUES (900, -1)` (refused, sql_mode `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION`): Server error: `ERROR 22003 (1264): Out of range value for column 'tiu' at row 1'
- `INSERT INTO probe_minimal_initial.t_int (id, biu) VALUES (901, 18446744073709551616)` (refused, sql_mode `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION`): Server error: `ERROR 22003 (1264): Out of range value for column 'biu' at row 1'
- `INSERT INTO probe_minimal_initial.t_num (id, db) VALUES (900, 'NaN')` (refused, sql_mode `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION`): Server error: `ERROR 01000 (1265): Data truncated for column 'db' at row 1'
- `INSERT INTO probe_minimal_initial.t_num (id, db) VALUES (901, 1e309)` (refused, sql_mode `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION`): Server error: `ERROR 22007 (1367): Illegal double '1e309' value found during parsing'
- `INSERT INTO probe_minimal_initial.t_num (id, d52) VALUES (902, 1000.00)` (refused, sql_mode `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION`): Server error: `ERROR 22003 (1264): Out of range value for column 'd52' at row 1'
- `INSERT INTO probe_minimal_initial.t_enum (id, e) VALUES (900, 'zzz')` (refused, sql_mode `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION`): Server error: `ERROR 01000 (1265): Data truncated for column 'e' at row 1'
- `INSERT INTO probe_minimal_initial.t_enum (id, e, s) VALUES (4, 'zzz', 'x,bogus')` (warned, sql_mode ``): Data truncated for column 'e' at row 1; Data truncated for column 's' at row 1
- `INSERT INTO probe_minimal_initial.t_time (id, d) VALUES (900, '0000-00-00')` (refused, sql_mode `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION`): Server error: `ERROR 22007 (1292): Incorrect date value: '0000-00-00' for column 'd' at row 1'
- `INSERT INTO probe_minimal_initial.t_time (id, d) VALUES (901, '2023-02-29')` (refused, sql_mode `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION`): Server error: `ERROR 22007 (1292): Incorrect date value: '2023-02-29' for column 'd' at row 1'
- `INSERT INTO probe_minimal_initial.t_enum (id, e, s) VALUES (104, 'zzz', 'x,bogus')` (warned, sql_mode ``): Data truncated for column 'e' at row 1; Data truncated for column 's' at row 1
- `UPDATE probe_minimal_initial.t_enum SET e = 'zzz', s = 'x,bogus' WHERE id = 103` (warned, sql_mode ``): Data truncated for column 'e' at row 1; Data truncated for column 's' at row 1

## t_int

| column | row | SQL | MySQL type | JSON snapshot | JSON CDC | snap=cdc | min=full | images | Avro CDC | Parquet CDC |
|---|---|---|---|---|---|---|---|---|---|---|
| ti | min | `-128` | tinyint | string:"-128" | i64:-128 | **NO** | yes | yes | int:-128 | - |
| ti | max | `127` | tinyint | string:"127" | u64:127 | **NO** | yes | yes | int:127 | - |
| ti | mid | `-1` | tinyint | string:"-1" | i64:-1 | **NO** | yes | yes | int:-1 | - |
| ti | null | `NULL` | tinyint | null:null | null:null | yes | yes | yes | null:null | - |
| tiu | min | `0` | tinyint unsigned | string:"0" | u64:0 | **NO** | yes | yes | int:0 | - |
| tiu | max | `255` | tinyint unsigned | string:"255" | u64:255 | **NO** | yes | yes | int:255 | - |
| tiu | mid | `128` | tinyint unsigned | string:"128" | u64:128 | **NO** | yes | yes | int:128 | - |
| tiu | null | `NULL` | tinyint unsigned | null:null | null:null | yes | yes | yes | null:null | - |
| si | min | `-32768` | smallint | string:"-32768" | i64:-32768 | **NO** | yes | yes | int:-32768 | - |
| si | max | `32767` | smallint | string:"32767" | u64:32767 | **NO** | yes | yes | int:32767 | - |
| si | mid | `-1` | smallint | string:"-1" | i64:-1 | **NO** | yes | yes | int:-1 | - |
| si | null | `NULL` | smallint | null:null | null:null | yes | yes | yes | null:null | - |
| siu | min | `0` | smallint unsigned | string:"0" | u64:0 | **NO** | yes | yes | int:0 | - |
| siu | max | `65535` | smallint unsigned | string:"65535" | u64:65535 | **NO** | yes | yes | int:65535 | - |
| siu | mid | `32768` | smallint unsigned | string:"32768" | u64:32768 | **NO** | yes | yes | int:32768 | - |
| siu | null | `NULL` | smallint unsigned | null:null | null:null | yes | yes | yes | null:null | - |
| mi | min | `-8388608` | mediumint | string:"-8388608" | i64:-8388608 | **NO** | yes | yes | int:-8388608 | - |
| mi | max | `8388607` | mediumint | string:"8388607" | u64:8388607 | **NO** | yes | yes | int:8388607 | - |
| mi | mid | `-1` | mediumint | string:"-1" | i64:-1 | **NO** | yes | yes | int:-1 | - |
| mi | null | `NULL` | mediumint | null:null | null:null | yes | yes | yes | null:null | - |
| miu | min | `0` | mediumint unsigned | string:"0" | u64:0 | **NO** | yes | yes | int:0 | - |
| miu | max | `16777215` | mediumint unsigned | string:"16777215" | u64:16777215 | **NO** | yes | yes | int:16777215 | - |
| miu | mid | `8388608` | mediumint unsigned | string:"8388608" | u64:8388608 | **NO** | yes | yes | int:8388608 | - |
| miu | null | `NULL` | mediumint unsigned | null:null | null:null | yes | yes | yes | null:null | - |
| i | min | `-2147483648` | int | string:"-2147483648" | i64:-2147483648 | **NO** | yes | yes | int:-2147483648 | - |
| i | max | `2147483647` | int | string:"2147483647" | u64:2147483647 | **NO** | yes | yes | int:2147483647 | - |
| i | mid | `-1` | int | string:"-1" | i64:-1 | **NO** | yes | yes | int:-1 | - |
| i | null | `NULL` | int | null:null | null:null | yes | yes | yes | null:null | - |
| iu | min | `0` | int unsigned | string:"0" | u64:0 | **NO** | yes | yes | long:0 | - |
| iu | max | `4294967295` | int unsigned | string:"4294967295" | u64:4294967295 | **NO** | yes | yes | long:4294967295 | - |
| iu | mid | `2147483648` | int unsigned | string:"2147483648" | u64:2147483648 | **NO** | yes | yes | long:2147483648 | - |
| iu | null | `NULL` | int unsigned | null:null | null:null | yes | yes | yes | null:null | - |
| bi | min | `-9223372036854775808` | bigint | string:"-9223372036854775808" | i64:-9223372036854775808 | **NO** | yes | yes | long:-9223372036854775808 | - |
| bi | max | `9223372036854775807` | bigint | string:"9223372036854775807" | u64:9223372036854775807 | **NO** | yes | yes | long:9223372036854775807 | - |
| bi | mid | `-1` | bigint | string:"-1" | i64:-1 | **NO** | yes | yes | long:-1 | - |
| bi | null | `NULL` | bigint | null:null | null:null | yes | yes | yes | null:null | - |
| biu | min | `0` | bigint unsigned | string:"0" | u64:0 | **NO** | yes | yes | string:"0" | - |
| biu | max | `18446744073709551615` | bigint unsigned | string:"18446744073709551615" | u64:18446744073709551615 | **NO** | yes | yes | string:"18446744073709551615" | - |
| biu | mid | `9223372036854775808` | bigint unsigned | string:"9223372036854775808" | u64:9223372036854775808 | **NO** | yes | yes | string:"9223372036854775808" | - |
| biu | null | `NULL` | bigint unsigned | null:null | null:null | yes | yes | yes | null:null | - |
| t1 | min | `-128` | tinyint(1) | string:"-128" | i64:-128 | **NO** | yes | yes | int:-128 | - |
| t1 | max | `127` | tinyint(1) | string:"127" | u64:127 | **NO** | yes | yes | int:127 | - |
| t1 | mid | `5` | tinyint(1) | string:"5" | u64:5 | **NO** | yes | yes | int:5 | - |
| t1 | null | `NULL` | tinyint(1) | null:null | null:null | yes | yes | yes | null:null | - |
| bo | min | `FALSE` | tinyint(1) | string:"0" | u64:0 | **NO** | yes | yes | int:0 | - |
| bo | max | `TRUE` | tinyint(1) | string:"1" | u64:1 | **NO** | yes | yes | int:1 | - |
| bo | mid | `2` | tinyint(1) | string:"2" | u64:2 | **NO** | yes | yes | int:2 | - |
| bo | null | `NULL` | tinyint(1) | null:null | null:null | yes | yes | yes | null:null | - |

## t_num

| column | row | SQL | MySQL type | JSON snapshot | JSON CDC | snap=cdc | min=full | images | Avro CDC | Parquet CDC |
|---|---|---|---|---|---|---|---|---|---|---|
| d65 | max | `99999999999999999999999999999999999.9...` | decimal(65,30) | string:"99999999999999999999999999999999999.99999999999999999999... | string:"99999999999999999999999999999999999.99999999999999999999... | yes | yes | yes | string:"99999999999999999999999999999999999.99999999999999999999... | - |
| d65 | min | `-99999999999999999999999999999999999....` | decimal(65,30) | string:"-99999999999999999999999999999999999.9999999999999999999... | string:"-99999999999999999999999999999999999.9999999999999999999... | yes | yes | yes | string:"-99999999999999999999999999999999999.9999999999999999999... | - |
| d65 | small | `0.000000000000000000000000000001` | decimal(65,30) | string:"0.000000000000000000000000000001" | string:"0.000000000000000000000000000001" | yes | yes | yes | string:"0.000000000000000000000000000001" | - |
| d65 | zero | `0` | decimal(65,30) | string:"0.000000000000000000000000000000" | string:"0.000000000000000000000000000000" | yes | yes | yes | string:"0.000000000000000000000000000000" | - |
| d65 | tiny | `12345.678900000000000000000000000000` | decimal(65,30) | string:"12345.678900000000000000000000000000" | string:"12345.678900000000000000000000000000" | yes | yes | yes | string:"12345.678900000000000000000000000000" | - |
| d65 | null | `NULL` | decimal(65,30) | null:null | null:null | yes | yes | yes | null:null | - |
| d10 | max | `9999999999` | decimal(10,0) | string:"9999999999" | string:"9999999999" | yes | yes | yes | string:"9999999999" | - |
| d10 | min | `-9999999999` | decimal(10,0) | string:"-9999999999" | string:"-9999999999" | yes | yes | yes | string:"-9999999999" | - |
| d10 | small | `1` | decimal(10,0) | string:"1" | string:"1" | yes | yes | yes | string:"1" | - |
| d10 | zero | `0` | decimal(10,0) | string:"0" | string:"0" | yes | yes | yes | string:"0" | - |
| d10 | tiny | `0` | decimal(10,0) | string:"0" | string:"0" | yes | yes | yes | string:"0" | - |
| d10 | null | `NULL` | decimal(10,0) | null:null | null:null | yes | yes | yes | null:null | - |
| d52 | max | `999.99` | decimal(5,2) | string:"999.99" | string:"999.99" | yes | yes | yes | string:"999.99" | - |
| d52 | min | `-999.99` | decimal(5,2) | string:"-999.99" | string:"-999.99" | yes | yes | yes | string:"-999.99" | - |
| d52 | small | `-0.01` | decimal(5,2) | string:"-0.01" | string:"-0.01" | yes | yes | yes | string:"-0.01" | - |
| d52 | zero | `0.00` | decimal(5,2) | string:"0.00" | string:"0.00" | yes | yes | yes | string:"0.00" | - |
| d52 | tiny | `1.50` | decimal(5,2) | string:"1.50" | string:"1.50" | yes | yes | yes | string:"1.50" | - |
| d52 | null | `NULL` | decimal(5,2) | null:null | null:null | yes | yes | yes | null:null | - |
| f | max | `3.40282e38` | float | string:"3.40282e38" | f64:3.402820018375656e+38 | **NO** | yes | yes | float:3.402820018375656e+38 | - |
| f | min | `-3.40282e38` | float | string:"-3.40282e38" | f64:-3.402820018375656e+38 | **NO** | yes | yes | float:-3.402820018375656e+38 | - |
| f | small | `1.1` | float | string:"1.1" | f64:1.100000023841858 | **NO** | yes | yes | float:1.100000023841858 | - |
| f | zero | `-0.0` | float | string:"0" | f64:0.0 | **NO** | yes | yes | float:0.0 | - |
| f | tiny | `1.17549e-38` | float | string:"1.17549e-38" | f64:1.1754900067970481e-38 | **NO** | yes | yes | float:1.1754900067970481e-38 | - |
| f | null | `NULL` | float | null:null | null:null | yes | yes | yes | null:null | - |
| db | max | `1.7976931348623157e308` | double | string:"1.7976931348623157e308" | f64:1.7976931348623157e+308 | **NO** | yes | yes | double:1.7976931348623157e+308 | - |
| db | min | `-1.7976931348623157e308` | double | string:"-1.7976931348623157e308" | f64:-1.7976931348623157e+308 | **NO** | yes | yes | double:-1.7976931348623157e+308 | - |
| db | small | `0.1` | double | string:"0.1" | f64:0.1 | **NO** | yes | yes | double:0.1 | - |
| db | zero | `-0.0` | double | string:"0" | f64:0.0 | **NO** | yes | yes | double:0.0 | - |
| db | tiny | `4.9e-324` | double | string:"5e-324" | f64:5e-324 | **NO** | yes | yes | double:5e-324 | - |
| db | null | `NULL` | double | null:null | null:null | yes | yes | yes | null:null | - |

## t_bit

| column | row | SQL | MySQL type | JSON snapshot | JSON CDC | snap=cdc | min=full | images | Avro CDC | Parquet CDC |
|---|---|---|---|---|---|---|---|---|---|---|
| b1 | zero | `b'0'` | bit(1) | string:"\u0000" | u64:0 | **NO** | yes | yes | - | - |
| b1 | one | `b'1'` | bit(1) | string:"\u0001" | u64:1 | **NO** | yes | yes | - | - |
| b1 | max | `b'1'` | bit(1) | string:"\u0001" | u64:1 | **NO** | yes | yes | - | - |
| b1 | pattern | `b'1'` | bit(1) | string:"\u0001" | u64:1 | **NO** | yes | yes | - | - |
| b1 | null | `NULL` | bit(1) | null:null | null:null | yes | yes | yes | - | - |
| b8 | zero | `b'0'` | bit(8) | string:"\u0000" | u64:0 | **NO** | yes | yes | - | - |
| b8 | one | `b'1'` | bit(8) | string:"\u0001" | u64:1 | **NO** | yes | yes | - | - |
| b8 | max | `b'11111111'` | bit(8) | object:{"_base64": "/w=="} | u64:255 | **NO** | yes | yes | - | - |
| b8 | pattern | `b'10100101'` | bit(8) | object:{"_base64": "pQ=="} | u64:165 | **NO** | yes | yes | - | - |
| b8 | null | `NULL` | bit(8) | null:null | null:null | yes | yes | yes | - | - |
| b64 | zero | `b'0'` | bit(64) | string:"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" | u64:0 | **NO** | yes | yes | - | - |
| b64 | one | `b'1'` | bit(64) | string:"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0001" | u64:1 | **NO** | yes | yes | - | - |
| b64 | max | `b'11111111111111111111111111111111111...` | bit(64) | object:{"_base64": "//////////8="} | u64:18446744073709551615 | **NO** | yes | yes | - | - |
| b64 | pattern | `0x8000000000000001` | bit(64) | object:{"_base64": "gAAAAAAAAAE="} | u64:9223372036854775809 | **NO** | yes | yes | - | - |
| b64 | null | `NULL` | bit(64) | null:null | null:null | yes | yes | yes | - | - |

## t_str

| column | row | SQL | MySQL type | JSON snapshot | JSON CDC | snap=cdc | min=full | images | Avro CDC | Parquet CDC |
|---|---|---|---|---|---|---|---|---|---|---|
| vu | text | `'héllo 😀'` | varchar(32) utf8mb4/utf8mb4_0900_ai_ci | string:"héllo 😀" | string:"héllo 😀" | yes | yes | yes | string:"héllo 😀" | - |
| vu | empty | `''` | varchar(32) utf8mb4/utf8mb4_0900_ai_ci | string:"" | string:"" | yes | yes | yes | string:"" | - |
| vu | spaces | `' x '` | varchar(32) utf8mb4/utf8mb4_0900_ai_ci | string:" x " | string:" x " | yes | yes | yes | string:" x " | - |
| vu | null | `NULL` | varchar(32) utf8mb4/utf8mb4_0900_ai_ci | null:null | null:null | yes | yes | yes | null:null | - |
| vl | text | `'héllo ÿ'` | varchar(32) latin1/latin1_swedish_ci | string:"héllo ÿ" | object:{"_base64": "aOlsbG8g/w=="} | **NO** | yes | yes | string:"h�llo �" | - |
| vl | empty | `''` | varchar(32) latin1/latin1_swedish_ci | string:"" | string:"" | yes | yes | yes | string:"" | - |
| vl | spaces | `' x '` | varchar(32) latin1/latin1_swedish_ci | string:" x " | string:" x " | yes | yes | yes | string:" x " | - |
| vl | null | `NULL` | varchar(32) latin1/latin1_swedish_ci | null:null | null:null | yes | yes | yes | null:null | - |
| cu | text | `'ab  '` | char(8) utf8mb4/utf8mb4_0900_ai_ci | string:"ab" | string:"ab" | yes | yes | yes | string:"ab" | - |
| cu | empty | `''` | char(8) utf8mb4/utf8mb4_0900_ai_ci | string:"" | string:"" | yes | yes | yes | string:"" | - |
| cu | spaces | `'  '` | char(8) utf8mb4/utf8mb4_0900_ai_ci | string:"" | string:"" | yes | yes | yes | string:"" | - |
| cu | null | `NULL` | char(8) utf8mb4/utf8mb4_0900_ai_ci | null:null | null:null | yes | yes | yes | null:null | - |
| tu | text | `'multi\nline 😀'` | text utf8mb4/utf8mb4_0900_ai_ci | string:"multi\nline 😀" | object:{"_base64": "bXVsdGkKbGluZSDwn5iA"} | **NO** | yes | yes | string:"multi\nline 😀" | - |
| tu | empty | `''` | text utf8mb4/utf8mb4_0900_ai_ci | string:"" | object:{"_base64": ""} | **NO** | yes | yes | string:"" | - |
| tu | spaces | `' x '` | text utf8mb4/utf8mb4_0900_ai_ci | string:" x " | object:{"_base64": "IHgg"} | **NO** | yes | yes | string:" x " | - |
| tu | null | `NULL` | text utf8mb4/utf8mb4_0900_ai_ci | null:null | null:null | yes | yes | yes | null:null | - |
| tl | text | `'déjà vu ÿ'` | text latin1/latin1_swedish_ci | string:"déjà vu ÿ" | object:{"_base64": "ZOlq4CB2dSD/"} | **NO** | yes | yes | string:"d�j� vu �" | - |
| tl | empty | `''` | text latin1/latin1_swedish_ci | string:"" | object:{"_base64": ""} | **NO** | yes | yes | string:"" | - |
| tl | spaces | `' x '` | text latin1/latin1_swedish_ci | string:" x " | object:{"_base64": "IHgg"} | **NO** | yes | yes | string:" x " | - |
| tl | null | `NULL` | text latin1/latin1_swedish_ci | null:null | null:null | yes | yes | yes | null:null | - |
| va | text | `'plain'` | varchar(16) ascii/ascii_general_ci | string:"plain" | string:"plain" | yes | yes | yes | string:"plain" | - |
| va | empty | `''` | varchar(16) ascii/ascii_general_ci | string:"" | string:"" | yes | yes | yes | string:"" | - |
| va | spaces | `' '` | varchar(16) ascii/ascii_general_ci | string:" " | string:" " | yes | yes | yes | string:" " | - |
| va | null | `NULL` | varchar(16) ascii/ascii_general_ci | null:null | null:null | yes | yes | yes | null:null | - |

## t_bin

| column | row | SQL | MySQL type | JSON snapshot | JSON CDC | snap=cdc | min=full | images | Avro CDC | Parquet CDC |
|---|---|---|---|---|---|---|---|---|---|---|
| bn | printable | `'ab'` | binary(4) | string:"ab\u0000\u0000" | string:"ab" | **NO** | yes | yes | bytes:0x6162 | - |
| bn | invalid_utf8 | `0xFF00C3FE` | binary(4) | object:{"_base64": "/wDD/g=="} | object:{"_base64": "/wDD/g=="} | yes | yes | yes | bytes:0xff00c3fe | - |
| bn | zero_bytes | `0x00000000` | binary(4) | string:"\u0000\u0000\u0000\u0000" | string:"" | **NO** | yes | yes | bytes:0x | - |
| bn | trailing | `0x61200000` | binary(4) | string:"a \u0000\u0000" | string:"a " | **NO** | yes | yes | bytes:0x6120 | - |
| bn | empty | `''` | binary(4) | string:"\u0000\u0000\u0000\u0000" | string:"" | **NO** | yes | yes | bytes:0x | - |
| bn | null | `NULL` | binary(4) | null:null | null:null | yes | yes | yes | null:null | - |
| vb | printable | `'hello'` | varbinary(16) | string:"hello" | string:"hello" | yes | yes | yes | bytes:0x68656c6c6f | - |
| vb | invalid_utf8 | `0xFF` | varbinary(16) | object:{"_base64": "/w=="} | object:{"_base64": "/w=="} | yes | yes | yes | bytes:0xff | - |
| vb | zero_bytes | `0x00` | varbinary(16) | string:"\u0000" | string:"\u0000" | yes | yes | yes | bytes:0x00 | - |
| vb | trailing | `0x612000` | varbinary(16) | string:"a \u0000" | string:"a \u0000" | yes | yes | yes | bytes:0x612000 | - |
| vb | empty | `''` | varbinary(16) | string:"" | string:"" | yes | yes | yes | bytes:0x | - |
| vb | null | `NULL` | varbinary(16) | null:null | null:null | yes | yes | yes | null:null | - |
| bl | printable | `'blob text'` | blob | string:"blob text" | object:{"_base64": "YmxvYiB0ZXh0"} | **NO** | yes | yes | bytes:0x626c6f622074657874 | - |
| bl | invalid_utf8 | `0xC328FFFE` | blob | object:{"_base64": "wyj//g=="} | object:{"_base64": "wyj//g=="} | yes | yes | yes | bytes:0xc328fffe | - |
| bl | zero_bytes | `0x0000` | blob | string:"\u0000\u0000" | object:{"_base64": "AAA="} | **NO** | yes | yes | bytes:0x0000 | - |
| bl | trailing | `0x6120` | blob | string:"a " | object:{"_base64": "YSA="} | **NO** | yes | yes | bytes:0x6120 | - |
| bl | empty | `''` | blob | string:"" | object:{"_base64": ""} | **NO** | yes | yes | bytes:0x | - |
| bl | null | `NULL` | blob | null:null | null:null | yes | yes | yes | null:null | - |

## t_enum

| column | row | SQL | MySQL type | JSON snapshot | JSON CDC | snap=cdc | min=full | images | Avro CDC | Parquet CDC |
|---|---|---|---|---|---|---|---|---|---|---|
| e | first | `'a'` | enum('a','b','c') utf8mb4/utf8mb4_0900_ai_ci | string:"a" | u64:1 | **NO** | yes | yes | string:"1" | - |
| e | last | `'c'` | enum('a','b','c') utf8mb4/utf8mb4_0900_ai_ci | string:"c" | u64:3 | **NO** | yes | yes | string:"3" | - |
| e | mid | `'b'` | enum('a','b','c') utf8mb4/utf8mb4_0900_ai_ci | string:"b" | u64:2 | **NO** | yes | yes | string:"2" | - |
| e | invalid | `'zzz'` | enum('a','b','c') utf8mb4/utf8mb4_0900_ai_ci | string:"" | u64:0 | **NO** | yes | yes | string:"0" | - |
| e | null | `NULL` | enum('a','b','c') utf8mb4/utf8mb4_0900_ai_ci | null:null | null:null | yes | yes | yes | null:null | - |
| s | first | `''` | set('x','y','z') utf8mb4/utf8mb4_0900_ai_ci | string:"" | u64:0 | **NO** | yes | yes | string:"0" | - |
| s | last | `'x,y,z'` | set('x','y','z') utf8mb4/utf8mb4_0900_ai_ci | string:"x,y,z" | u64:7 | **NO** | yes | yes | string:"7" | - |
| s | mid | `'z,x'` | set('x','y','z') utf8mb4/utf8mb4_0900_ai_ci | string:"x,z" | u64:5 | **NO** | yes | yes | string:"5" | - |
| s | invalid | `'x,bogus'` | set('x','y','z') utf8mb4/utf8mb4_0900_ai_ci | string:"x" | u64:1 | **NO** | yes | yes | string:"1" | - |
| s | null | `NULL` | set('x','y','z') utf8mb4/utf8mb4_0900_ai_ci | null:null | null:null | yes | yes | yes | null:null | - |

## t_json

| column | row | SQL | MySQL type | JSON snapshot | JSON CDC | snap=cdc | min=full | images | Avro CDC | Parquet CDC |
|---|---|---|---|---|---|---|---|---|---|---|
| j | null_literal | `'null'` | json | string:"null" | null:null | **NO** | yes | yes | null:null | - |
| j | int | `'1'` | json | string:"1" | u64:1 | **NO** | yes | yes | string:"1" | - |
| j | u64_max | `'18446744073709551615'` | json | string:"18446744073709551615" | u64:18446744073709551615 | **NO** | yes | yes | string:"18446744073709551615" | - |
| j | i64_min | `'-9223372036854775808'` | json | string:"-9223372036854775808" | i64:-9223372036854775808 | **NO** | yes | yes | string:"-9223372036854775808" | - |
| j | float | `'1.5'` | json | string:"1.5" | f64:1.5 | **NO** | yes | yes | string:"1.5" | - |
| j | float_int | `'1.0'` | json | string:"1.0" | f64:1.0 | **NO** | yes | yes | string:"1.0" | - |
| j | big_float | `'1e300'` | json | string:"1e300" | f64:1e+300 | **NO** | yes | yes | string:"1e+300" | - |
| j | string | `'"str ü 😀"'` | json | string:"\"str ü 😀\"" | string:"str ü 😀" | **NO** | yes | yes | string:"str ü 😀" | - |
| j | bool | `'true'` | json | string:"true" | bool:true | **NO** | yes | yes | string:"true" | - |
| j | array | `'[1, "a", null, [2.5]]'` | json | string:"[1, \"a\", null, [2.5]]" | array:[1, "a", null, [2.5]] | **NO** | yes | yes | string:"[1,\"a\",null,[2.5]]" | - |
| j | object | `'{"k": {"n": 1.0, "u": "😀"}, "a": 1}'` | json | string:"{\"a\": 1, \"k\": {\"n\": 1.0, \"u\": \"😀\"}}" | object:{"a": 1, "k": {"n": 1.0, "u": "😀"}} | **NO** | yes | yes | string:"{\"a\":1,\"k\":{\"n\":1.0,\"u\":\"😀\"}}" | - |
| j | empty_object | `'{}'` | json | string:"{}" | object:{} | **NO** | yes | yes | string:"{}" | - |
| j | null | `NULL` | json | null:null | null:null | yes | yes | yes | null:null | - |

## t_time

| column | row | SQL | MySQL type | JSON snapshot | JSON CDC | snap=cdc | min=full | images | Avro CDC | Parquet CDC |
|---|---|---|---|---|---|---|---|---|---|---|
| d | min | `'1000-01-01'` | date | string:"1000-01-01" | string:"1000-01-01" | yes | yes | yes | - | - |
| d | max | `'9999-12-31'` | date | string:"9999-12-31" | string:"9999-12-31" | yes | yes | yes | - | - |
| d | mid | `'2024-02-29'` | date | string:"2024-02-29" | string:"2024-02-29" | yes | yes | yes | - | - |
| d | zero | `'0000-00-00'` | date | string:"0000-00-00" | string:"0-00-00" | **NO** | yes | yes | - | - |
| d | null | `NULL` | date | null:null | null:null | yes | yes | yes | - | - |
| tm | min | `'-838:59:59'` | time | string:"-838:59:59" | string:"-838:59:59.000000" | **NO** | yes | yes | - | - |
| tm | max | `'838:59:59'` | time | string:"838:59:59" | string:"838:59:59.000000" | **NO** | yes | yes | - | - |
| tm | mid | `'12:34:56'` | time | string:"12:34:56" | string:"12:34:56.000000" | **NO** | yes | yes | - | - |
| tm | zero | `'00:00:00'` | time | string:"00:00:00" | string:"00:00:00.000000" | **NO** | yes | yes | - | - |
| tm | null | `NULL` | time | null:null | null:null | yes | yes | yes | - | - |
| tm6 | min | `'-838:59:59.000000'` | time(6) | string:"-838:59:59.000000" | string:"-838:59:59.000000" | yes | yes | yes | - | - |
| tm6 | max | `'838:59:59.000000'` | time(6) | string:"838:59:59.000000" | string:"838:59:59.000000" | yes | yes | yes | - | - |
| tm6 | mid | `'-00:00:00.500000'` | time(6) | string:"-00:00:00.500000" | string:"-00:00:00.500000" | yes | yes | yes | - | - |
| tm6 | zero | `'00:00:00.000000'` | time(6) | string:"00:00:00.000000" | string:"00:00:00.000000" | yes | yes | yes | - | - |
| tm6 | null | `NULL` | time(6) | null:null | null:null | yes | yes | yes | - | - |
| dt | min | `'1000-01-01 00:00:00'` | datetime | string:"1000-01-01 00:00:00" | string:"1000-01-01 00:00:00.000000" | **NO** | yes | yes | - | - |
| dt | max | `'9999-12-31 23:59:59'` | datetime | string:"9999-12-31 23:59:59" | string:"9999-12-31 23:59:59.000000" | **NO** | yes | yes | - | - |
| dt | mid | `'2024-02-29 12:34:56'` | datetime | string:"2024-02-29 12:34:56" | string:"2024-02-29 12:34:56.000000" | **NO** | yes | yes | - | - |
| dt | zero | `'0000-00-00 00:00:00'` | datetime | string:"0000-00-00 00:00:00" | string:"0-00-00 00:00:00.000000" | **NO** | yes | yes | - | - |
| dt | null | `NULL` | datetime | null:null | null:null | yes | yes | yes | - | - |
| dt6 | min | `'1000-01-01 00:00:00.000000'` | datetime(6) | string:"1000-01-01 00:00:00.000000" | string:"1000-01-01 00:00:00.000000" | yes | yes | yes | - | - |
| dt6 | max | `'9999-12-31 23:59:59.999999'` | datetime(6) | string:"9999-12-31 23:59:59.999999" | string:"9999-12-31 23:59:59.999999" | yes | yes | yes | - | - |
| dt6 | mid | `'2024-02-29 12:34:56.123456'` | datetime(6) | string:"2024-02-29 12:34:56.123456" | string:"2024-02-29 12:34:56.123456" | yes | yes | yes | - | - |
| dt6 | zero | `'0000-00-00 00:00:00.000000'` | datetime(6) | string:"0000-00-00 00:00:00.000000" | string:"0-00-00 00:00:00.000000" | **NO** | yes | yes | - | - |
| dt6 | null | `NULL` | datetime(6) | null:null | null:null | yes | yes | yes | - | - |
| ts | min | `'1970-01-01 02:00:01'` | timestamp | string:"1970-01-01 02:00:01" | u64:1000000 | **NO** | yes | yes | - | - |
| ts | max | `'2038-01-19 05:14:07'` | timestamp | string:"2038-01-19 05:14:07" | u64:2147483647000000 | **NO** | yes | yes | - | - |
| ts | mid | `'2024-03-31 01:30:00'` | timestamp | string:"2024-03-31 01:30:00" | u64:1711841400000000 | **NO** | yes | yes | - | - |
| ts | zero | `'0000-00-00 00:00:00'` | timestamp | string:"0000-00-00 00:00:00" | u64:0 | **NO** | yes | yes | - | - |
| ts | null | `NULL` | timestamp | null:null | null:null | yes | yes | yes | - | - |
| ts6 | min | `'1970-01-01 02:00:01.000000'` | timestamp(6) | string:"1970-01-01 02:00:01.000000" | u64:1000000 | **NO** | yes | yes | - | - |
| ts6 | max | `'2038-01-19 05:14:07.999999'` | timestamp(6) | string:"2038-01-19 05:14:07.999999" | u64:2147483647999999 | **NO** | yes | yes | - | - |
| ts6 | mid | `'2024-10-27 02:30:00.654321'` | timestamp(6) | string:"2024-10-27 02:30:00.654321" | u64:1729989000654321 | **NO** | yes | yes | - | - |
| ts6 | zero | `'0000-00-00 00:00:00.000000'` | timestamp(6) | string:"0000-00-00 00:00:00.000000" | u64:0 | **NO** | yes | yes | - | - |
| ts6 | null | `NULL` | timestamp(6) | null:null | null:null | yes | yes | yes | - | - |
| y | min | `1901` | year | string:"1901" | u64:1901 | **NO** | yes | yes | - | - |
| y | max | `2155` | year | string:"2155" | u64:2155 | **NO** | yes | yes | - | - |
| y | mid | `2024` | year | string:"2024" | u64:2024 | **NO** | yes | yes | - | - |
| y | zero | `0` | year | string:"0000" | u64:1900 | **NO** | yes | yes | - | - |
| y | null | `NULL` | year | null:null | null:null | yes | yes | yes | - | - |

## Typed sinks: CDC image and metadata consistency

- avro: 0 differences
- parquet: 0 differences


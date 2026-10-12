//! Event → partition key router.
//!
//! Phase 1d: Hive-style `table=X/year=Y/month=M/day=D` partitioning derived
//! from the event's `op_ts`. Hour granularity is configurable but deferred.
//!
//! Path produced: `{prefix}/database={database}/namespace={namespace}/
//! table={table}/year={Y}/month={M:02}/day={D:02}`, each identity value
//! percent-escaped (every byte but ASCII letters, digits, `-`, `.` and `_`).
//! The prefix is configuration, so two pipelines can share it: the path
//! carries the relation's database, namespace and table itself.

use std::fmt::Write;

use super::keys::MAX_KEY_SEGMENT_BYTES;
use chrono::{DateTime, Datelike, Utc};
use deltaforge_core::Event;

/// Stable key identifying which in-progress file an event belongs to.
///
/// Two events with the same `PartitionKey` are written to the same Parquet /
/// JSONL file. The key is `Hash + Eq + Ord` so it works as a `HashMap` key
/// and so partitions can be sorted for stable test output.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionKey {
    /// The relation's database (`source.db`).
    pub database: String,
    /// The table's namespace (`SourceInfo::namespace`: the PostgreSQL
    /// schema, or the MySQL database). With the database part of the key
    /// and of the path, so same-named tables of different databases or
    /// namespaces never share a writer or a partition; schema resolution
    /// looks the table up by `namespace.table` within the source.
    pub namespace: String,
    pub table: String,
    /// The mapping version of the file's events: a file never mixes
    /// versions (part of the writer identity, not of the path).
    pub type_mapping: deltaforge_core::TypeMapping,
    pub year: i32,
    pub month: u8,
    pub day: u8,
}

impl PartitionKey {
    /// Hive-style partition path fragment (no leading or trailing slash).
    pub fn hive_path(&self) -> String {
        let mut s = String::with_capacity(48);
        let _ = write!(
            s,
            "database={b}/namespace={n}/table={t}/year={y}/month={m:02}/day={d:02}",
            b = value("database", &self.database),
            n = value("namespace", &self.namespace),
            t = value("table", &self.table),
            y = self.year,
            m = self.month,
            d = self.day,
        );
        s
    }
}

/// Routes an event to its partition key.
///
/// The table name comes from `event.source.table`; date components come from
/// The partition value of `s` under `name`: its escaped form when the
/// segment `name=value` fits [`MAX_KEY_SEGMENT_BYTES`], otherwise `~` and
/// the lowercase hex SHA-256 of the length-prefixed domain tag, `name` and
/// `s` (never truncated; the rows' `source_*` columns keep the readable
/// identity). The escaping never emits `~`, so the forms never collide.
fn value(name: &str, s: &str) -> String {
    use sha2::{Digest, Sha256};
    let escaped = escape(s);
    if name.len() + 1 + escaped.len() <= MAX_KEY_SEGMENT_BYTES {
        return escaped;
    }
    let mut h = Sha256::new();
    for part in [PARTITION_VALUE_DOMAIN, name.as_bytes(), s.as_bytes()] {
        h.update((part.len() as u64).to_be_bytes());
        h.update(part);
    }
    let hex: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
    format!("~{hex}")
}

/// Domain-separation tag of digest partition values.
const PARTITION_VALUE_DOMAIN: &[u8] = b"deltaforge:s3:partition-value";

/// `s` as a partition value: every byte but ASCII letters, digits, `-`,
/// `.` and `_` as `%XX` (so `/` and `=` never split or fake a segment).
fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'.' | b'_' => {
                out.push(b as char)
            }
            _ => {
                let _ = write!(out, "%{b:02X}");
            }
        }
    }
    out
}

/// `event.ts_ms` interpreted as UTC milliseconds since the Unix epoch.
pub fn partition_for(event: &Event) -> PartitionKey {
    let dt: DateTime<Utc> = DateTime::<Utc>::from_timestamp_millis(event.ts_ms)
        .unwrap_or_else(Utc::now);
    PartitionKey {
        database: event.source.db.clone(),
        // The table's namespace within the source's database, which the
        // source's schema loader keys on.
        namespace: event.source.namespace().to_string(),
        table: event.source.table.clone(),
        type_mapping: event.type_mapping,
        year: dt.year(),
        month: dt.month() as u8,
        day: dt.day() as u8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The longest identifiers, escaped, keep every legacy segment and the
    /// path within the key limits: an over-long value takes its digest.
    #[test]
    fn legacy_partitions_stay_bounded_for_the_longest_identifiers() {
        let longest = "€".repeat(64); // 192 bytes, 576 escaped
        let p = PartitionKey {
            database: longest.clone(),
            namespace: longest.clone(),
            table: longest.clone(),
            type_mapping: deltaforge_core::TypeMapping::V2,
            year: 2026,
            month: 1,
            day: 5,
        };
        let path = p.hive_path();
        for seg in path.split('/') {
            assert!(
                seg.len() <= crate::s3::keys::MAX_KEY_SEGMENT_BYTES,
                "{seg}"
            );
        }
        assert!(path.starts_with("database=~"), "{path}");
        let key =
            format!("deltaforge/prefix/{path}/{}.parquet", "0".repeat(26));
        crate::s3::keys::check_key_bounds(&key).unwrap();
        // Different values, different digests.
        let mut q = p.clone();
        q.table = format!("{longest}x");
        assert_ne!(q.hive_path(), path);
        // A value that fits stays readable, `/` and `=` escaped.
        let mut r = p.clone();
        r.database = "a/b=c".into();
        assert!(r.hive_path().starts_with("database=a%2Fb%3Dc/"));
    }
    use deltaforge_core::{Event, Op, SourceInfo, SourcePosition, TypeMapping};

    fn event_at(table: &str, ts_ms: i64) -> Event {
        Event {
            before: None,
            after: None,
            source: SourceInfo {
                version: "1".into(),
                connector: "mysql".into(),
                name: "test".into(),
                ts_ms,
                db: "shop".into(),
                schema: None,
                table: table.into(),
                snapshot: None,
                position: SourcePosition::default(),
            },
            op: Op::Create,
            ts_ms,
            transaction: None,
            type_mapping: TypeMapping::V2,
            event_id: None,
            tenant_id: None,
            schema_version: None,
            schema_sequence: None,
            ddl: None,
            trace_id: None,
            tags: None,
            synthetic: None,
            routing: None,
            tx_end: false,
            boundary: None,
            size_bytes: 0,

            received_at_ms: ts_ms,
        }
    }

    #[test]
    fn partitions_by_table_and_utc_date() {
        let ts_ms = chrono::NaiveDate::from_ymd_opt(2026, 5, 19)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis();
        let p = partition_for(&event_at("orders", ts_ms));
        assert_eq!(
            p,
            PartitionKey {
                database: "shop".into(),
                namespace: "shop".into(),
                table: "orders".into(),
                type_mapping: TypeMapping::V2,
                year: 2026,
                month: 5,
                day: 19
            }
        );
    }

    #[test]
    fn hive_path_zero_pads_month_and_day() {
        let p = PartitionKey {
            database: "shop".into(),
            namespace: "shop".into(),
            table: "orders".into(),
            type_mapping: TypeMapping::V2,
            year: 2026,
            month: 1,
            day: 5,
        };
        // db is intentionally NOT part of the path (no lake-layout change).
        assert_eq!(
            p.hive_path(),
            "database=shop/namespace=shop/table=orders/year=2026/month=01/day=05"
        );
    }

    #[test]
    fn midnight_crossing_changes_partition() {
        let midnight = chrono::NaiveDate::from_ymd_opt(2026, 5, 20)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis();
        let p_late = partition_for(&event_at("orders", midnight - 1));
        let p_early = partition_for(&event_at("orders", midnight));
        assert_eq!(p_late.day, 19);
        assert_eq!(p_early.day, 20);
        assert_ne!(p_late, p_early);
    }

    #[test]
    fn different_tables_get_different_partitions() {
        let p1 = partition_for(&event_at("orders", 0));
        let p2 = partition_for(&event_at("customers", 0));
        assert_ne!(p1, p2);
    }

    #[test]
    fn invalid_ts_falls_back_to_now() {
        // i64::MAX millis would overflow chrono; fallback to current UTC.
        let now = Utc::now();
        let p = partition_for(&event_at("orders", i64::MAX));
        assert_eq!(p.year, now.year());
        assert_eq!(p.month, now.month() as u8);
    }

    #[test]
    fn mapping_versions_never_share_a_writer() {
        let v2 = event_at("orders", 0);
        let mut v1 = event_at("orders", 0);
        v1.type_mapping = TypeMapping::V1;
        let (p1, p2) = (partition_for(&v1), partition_for(&v2));
        assert_ne!(p1, p2, "a file never mixes mapping versions");
        assert_eq!(p1.type_mapping, TypeMapping::V1);
        // The version is writer identity, not part of the lake layout.
        assert_eq!(p1.hive_path(), p2.hive_path());
    }
}

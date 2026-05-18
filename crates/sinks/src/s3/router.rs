//! Event → partition key router.
//!
//! Phase 1d: Hive-style `table=X/year=Y/month=M/day=D` partitioning derived
//! from the event's `op_ts`. Hour granularity is configurable but deferred.
//!
//! Path produced: `{prefix}/table={table}/year={Y}/month={M:02}/day={D:02}`

use std::fmt::Write;

use chrono::{DateTime, Datelike, Utc};
use deltaforge_core::Event;

/// Stable key identifying which in-progress file an event belongs to.
///
/// Two events with the same `PartitionKey` are written to the same Parquet /
/// JSONL file. The key is `Hash + Eq + Ord` so it works as a `HashMap` key
/// and so partitions can be sorted for stable test output.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionKey {
    pub table: String,
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
            "table={t}/year={y}/month={m:02}/day={d:02}",
            t = self.table,
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
/// `event.ts_ms` interpreted as UTC milliseconds since the Unix epoch.
pub fn partition_for(event: &Event) -> PartitionKey {
    let dt: DateTime<Utc> = DateTime::<Utc>::from_timestamp_millis(event.ts_ms)
        .unwrap_or_else(Utc::now);
    PartitionKey {
        table: event.source.table.clone(),
        year: dt.year(),
        month: dt.month() as u8,
        day: dt.day() as u8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_core::{Event, Op, SourceInfo, SourcePosition};

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
            checkpoint: None,
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
                table: "orders".into(),
                year: 2026,
                month: 5,
                day: 19
            }
        );
    }

    #[test]
    fn hive_path_zero_pads_month_and_day() {
        let p = PartitionKey {
            table: "orders".into(),
            year: 2026,
            month: 1,
            day: 5,
        };
        assert_eq!(p.hive_path(), "table=orders/year=2026/month=01/day=05");
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
}

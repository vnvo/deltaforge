//! Timestamp conversion utilities.
//!
//! This module provides helpers for converting between different timestamp
//! formats used by various databases and systems:
//!
//! - **Unix timestamps**: Seconds or milliseconds since 1970-01-01
//! - **PostgreSQL timestamps**: Microseconds since 2000-01-01
//! - **MySQL timestamps**: Seconds since 1970-01-01 (in binlog)
//!
//! # Examples
//!
//! ```
//! use common::time::{pg_timestamp_to_unix_ms, ts_sec_to_ms};
//!
//! // Convert MySQL binlog timestamp (seconds) to milliseconds
//! let mysql_ts = 1704067200u32; // 2024-01-01 00:00:00 UTC
//! let ms = ts_sec_to_ms(mysql_ts);
//! assert_eq!(ms, 1704067200000);
//!
//! // Convert PostgreSQL timestamp to Unix milliseconds
//! let pg_ts = 0i64; // PostgreSQL epoch (2000-01-01)
//! let unix_ms = pg_timestamp_to_unix_ms(pg_ts);
//! assert_eq!(unix_ms, 946684800000); // Unix timestamp for 2000-01-01
//! ```

/// PostgreSQL epoch (2000-01-01 00:00:00 UTC) offset from Unix epoch in microseconds.
///
/// PostgreSQL stores timestamps as microseconds since 2000-01-01, while Unix
/// timestamps are seconds since 1970-01-01. This constant represents the
/// difference between these two epochs in microseconds.
///
/// Value: 946,684,800,000,000 microseconds = 10,957 days
pub const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

/// Convert PostgreSQL timestamp (microseconds since 2000-01-01) to Unix milliseconds.
///
/// PostgreSQL uses a different epoch (2000-01-01) than Unix (1970-01-01).
/// This function converts between the two, also changing the unit from
/// microseconds to milliseconds.
///
/// # Arguments
///
/// * `pg_micros` - Microseconds since PostgreSQL epoch (2000-01-01)
///
/// # Returns
///
/// Unix timestamp in milliseconds since 1970-01-01
///
/// # Examples
///
/// ```
/// use common::time::pg_timestamp_to_unix_ms;
///
/// // PostgreSQL epoch (2000-01-01 00:00:00 UTC)
/// assert_eq!(pg_timestamp_to_unix_ms(0), 946684800000);
///
/// // One second after PostgreSQL epoch
/// assert_eq!(pg_timestamp_to_unix_ms(1_000_000), 946684801000);
/// ```
pub fn pg_timestamp_to_unix_ms(pg_micros: i64) -> i64 {
    (pg_micros + PG_EPOCH_OFFSET_MICROS) / 1000
}

/// Convert Unix timestamp in seconds to milliseconds.
///
/// Simple conversion helper for MySQL binlog timestamps which are typically
/// in seconds. Also useful for other sources that use second-precision timestamps.
///
/// # Arguments
///
/// * `ts_sec` - Unix timestamp in seconds
///
/// # Returns
///
/// Unix timestamp in milliseconds
///
/// # Examples
///
/// ```
/// use common::time::ts_sec_to_ms;
///
/// assert_eq!(ts_sec_to_ms(1), 1000);
/// assert_eq!(ts_sec_to_ms(1704067200), 1704067200000); // 2024-01-01
/// ```
pub fn ts_sec_to_ms(ts_sec: u32) -> i64 {
    (ts_sec as i64) * 1000
}

/// Convert Unix timestamp in seconds (i64) to milliseconds.
///
/// Same as `ts_sec_to_ms` but accepts i64 for sources that use signed timestamps.
pub fn ts_sec_to_ms_i64(ts_sec: i64) -> i64 {
    ts_sec * 1000
}

/// Convert Unix milliseconds to PostgreSQL microseconds.
///
/// Inverse of `pg_timestamp_to_unix_ms`. Useful when you need to
/// send timestamps back to PostgreSQL.
///
/// # Arguments
///
/// * `unix_ms` - Unix timestamp in milliseconds
///
/// # Returns
///
/// PostgreSQL timestamp in microseconds since 2000-01-01
pub fn unix_ms_to_pg_timestamp(unix_ms: i64) -> i64 {
    (unix_ms * 1000) - PG_EPOCH_OFFSET_MICROS
}

/// Get current Unix timestamp in milliseconds.
///
/// Convenience function that returns the current time as Unix milliseconds.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_epoch_converts_to_correct_unix_timestamp() {
        // PostgreSQL epoch (2000-01-01 00:00:00 UTC) = 0 in PG
        // Unix epoch for same date = 946684800 seconds = 946684800000 ms
        assert_eq!(pg_timestamp_to_unix_ms(0), 946_684_800_000);
    }

    #[test]
    fn pg_timestamp_handles_negative_values() {
        // Before PostgreSQL epoch (1999-12-31 00:00:00 UTC)
        // -1 day = -86,400,000,000 microseconds
        let one_day_micros = 86_400_000_000i64;
        let unix_ms = pg_timestamp_to_unix_ms(-one_day_micros);
        assert_eq!(unix_ms, 946_598_400_000);
    }

    #[test]
    fn ts_sec_to_ms_handles_max_u32() {
        // Verify no overflow when converting max u32 seconds to i64 milliseconds
        let max = u32::MAX;
        let result = ts_sec_to_ms(max);
        assert_eq!(result, (max as i64) * 1000);
        assert!(result > 0, "should not overflow to negative");
    }

    #[test]
    fn roundtrip_pg_unix_conversion() {
        // Converting PG -> Unix -> PG should return original value
        let original_pg = 123_456_789_000i64;
        let unix_ms = pg_timestamp_to_unix_ms(original_pg);
        let back_to_pg = unix_ms_to_pg_timestamp(unix_ms);
        assert_eq!(back_to_pg, original_pg);
    }

    #[test]
    fn now_ms_returns_positive_value() {
        // Basic sanity check - should return a positive timestamp
        let now = now_ms();
        assert!(now > 0, "current time should be positive");
    }
}

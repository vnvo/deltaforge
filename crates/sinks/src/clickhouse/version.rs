//! Derive a monotonic `UInt64` `_version` from an event's source position.
//!
//! `ReplacingMergeTree(_version, _deleted)` keeps the row with the max `_version`
//! per key, so the version must increase with commit order. The Postgres LSN and
//! (eventually) the MySQL binlog position provide exactly that.

use deltaforge_config::ChVersionSource;
use deltaforge_core::Event;

/// Parse a Postgres LSN of the form `"X/Y"` (hex/hex) into a u64 that preserves
/// ordering: high 32 bits = `X`, low 32 bits = `Y`.
pub(crate) fn lsn_to_u64(lsn: &str) -> Option<u64> {
    let (hi, lo) = lsn.split_once('/')?;
    let hi = u64::from_str_radix(hi.trim(), 16).ok()?;
    let lo = u64::from_str_radix(lo.trim(), 16).ok()?;
    Some((hi << 32) | lo)
}

/// Derive the `_version` used for `ReplacingMergeTree` replacement / ordering.
///
/// For `SourcePosition`: Postgres LSN → else fall back to `ts_ms`. (MySQL binlog
/// file+pos normalization is added when the source layer exposes a combined u64
/// position; until then MySQL falls back to `ts_ms`, documented as weaker — see
/// the RFC open question on version fidelity.)
pub fn derive_version(event: &Event, source: ChVersionSource) -> u64 {
    match source {
        ChVersionSource::TsMs => event.ts_ms.max(0) as u64,
        ChVersionSource::SourcePosition => event
            .source
            .position
            .lsn
            .as_deref()
            .and_then(lsn_to_u64)
            .unwrap_or_else(|| event.ts_ms.max(0) as u64),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_pg_lsn_to_monotonic_u64() {
        let v = lsn_to_u64("16/B374D848").unwrap();
        assert_eq!(v, (0x16u64 << 32) | 0xB374_D848);
        assert!(lsn_to_u64("16/B374D849").unwrap() > v);
        assert!(lsn_to_u64("17/0").unwrap() > v);
    }

    #[test]
    fn invalid_lsn_returns_none() {
        assert!(lsn_to_u64("not-an-lsn").is_none());
        assert!(lsn_to_u64("16").is_none());
    }
}

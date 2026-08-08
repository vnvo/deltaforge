//! Derive the external `version` for ES upserts — monotonic with commit order.
//!
//! `version_type=external` makes ES keep the highest version per `_id`, so
//! replays and out-of-order batches converge to the source's current state.
//! Reuses the ClickHouse LSN parser (both sinks need a monotonic u64 position).

use crate::clickhouse::version::lsn_to_u64;
use deltaforge_config::EsVersionSource;
use deltaforge_core::Event;

pub fn derive_es_version(ev: &Event, src: EsVersionSource) -> u64 {
    match src {
        EsVersionSource::TsMs => ev.ts_ms.max(0) as u64,
        EsVersionSource::SourcePosition => ev
            .source
            .position
            .lsn
            .as_deref()
            .and_then(lsn_to_u64)
            .unwrap_or_else(|| ev.ts_ms.max(0) as u64),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::elasticsearch::test_support::mk_event;
    use deltaforge_core::Op;
    use serde_json::json;

    fn ev(lsn: Option<&str>, ts_ms: i64) -> Event {
        mk_event(Op::Update, json!({}), json!(null), "d", None, "t", ts_ms, lsn)
    }

    #[test]
    fn uses_lsn_when_present() {
        let e = ev(Some("16/B374D848"), 1000);
        assert_eq!(
            derive_es_version(&e, EsVersionSource::SourcePosition),
            (0x16u64 << 32) | 0xB374_D848
        );
    }

    #[test]
    fn falls_back_to_ts_ms_without_lsn() {
        let e = ev(None, 1234);
        assert_eq!(derive_es_version(&e, EsVersionSource::SourcePosition), 1234);
    }

    #[test]
    fn ts_ms_source_ignores_lsn() {
        let e = ev(Some("16/B374D848"), 1234);
        assert_eq!(derive_es_version(&e, EsVersionSource::TsMs), 1234);
    }
}

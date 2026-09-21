//! Provisional stable-`EventId` derivation for PostgreSQL row events.
//!
//! Computes the `pgrow` id from an event's already-plumbed coordinates: the
//! cluster `system_identifier` (per-connection lineage), the transaction's
//! final LSN (from `BEGIN`), the relation OID, and the per-transaction change
//! ordinal. Provisional: not yet stored in `Event.event_id`. A row change with
//! no active transaction identity is an error here; fatal runtime enforcement
//! is deferred to the cutover.

use deltaforge_core::{EventId, SourceInfo};

/// Parse a PostgreSQL LSN (`"X/Y"`, hex/hex) into an order-preserving `u64`.
fn lsn_to_u64(lsn: &str) -> Option<u64> {
    let (hi, lo) = lsn.split_once('/')?;
    let hi = u64::from_str_radix(hi.trim(), 16).ok()?;
    let lo = u64::from_str_radix(lo.trim(), 16).ok()?;
    Some((hi << 32) | lo)
}

/// Derive the provisional `pgrow` [`EventId`]. `system_identifier` is captured
/// once per connection (the cluster lineage) and passed in.
pub fn pg_row_event_id(
    source: &SourceInfo,
    system_identifier: u64,
) -> Result<EventId, String> {
    // A row observed without an active transaction identity (no BEGIN final LSN)
    // is a coordinate error — never a silently-derived id.
    let final_lsn_str =
        source.position.tx_final_lsn.as_deref().ok_or(
            "pg event id: row change without active transaction identity",
        )?;
    let final_lsn = lsn_to_u64(final_lsn_str).ok_or_else(|| {
        format!("pg event id: unparseable LSN {final_lsn_str:?}")
    })?;
    let relation_oid = source
        .position
        .relation_oid
        .ok_or("pg event id: missing relation oid")?;
    let ordinal = source
        .position
        .change_ordinal
        .ok_or("pg event id: missing change ordinal")?;
    Ok(EventId::pg_row(
        system_identifier,
        final_lsn,
        relation_oid,
        ordinal,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_core::SourcePosition;

    fn src(pos: SourcePosition) -> SourceInfo {
        SourceInfo {
            version: "1".into(),
            connector: "postgresql".into(),
            name: "p".into(),
            ts_ms: 0,
            db: "d".into(),
            schema: Some("public".into()),
            table: "t".into(),
            snapshot: None,
            position: pos,
        }
    }

    fn pos(
        final_lsn: Option<&str>,
        oid: Option<u32>,
        ord: Option<u32>,
    ) -> SourcePosition {
        SourcePosition {
            lsn: Some("16/B374E0".into()), // per-message lsn (distinct from final)
            tx_final_lsn: final_lsn.map(String::from),
            relation_oid: oid,
            change_ordinal: ord,
            ..Default::default()
        }
    }

    #[test]
    fn matches_pinned_pgrow_vector() {
        let si = src(pos(Some("16/B374D8"), Some(16384), Some(0)));
        let id = pg_row_event_id(&si, 0x1234_5678_90AB_CDEF).unwrap();
        assert_eq!(
            id.to_string(),
            "dfid:v1:pgrow:4ae35fefb20b943d9b79f12b27a16465"
        );
    }

    #[test]
    fn uses_final_lsn_not_per_message_lsn() {
        // Same relation/ordinal/system, different final LSN → different id;
        // the per-message `lsn` field must not affect identity.
        let a =
            pg_row_event_id(&src(pos(Some("16/B374D8"), Some(1), Some(0))), 7)
                .unwrap();
        let b =
            pg_row_event_id(&src(pos(Some("16/B374FF"), Some(1), Some(0))), 7)
                .unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn row_without_transaction_identity_is_error() {
        let si = src(pos(None, Some(1), Some(0)));
        assert!(pg_row_event_id(&si, 7).is_err());
    }

    #[test]
    fn missing_relation_or_ordinal_is_error() {
        assert!(
            pg_row_event_id(&src(pos(Some("16/B374D8"), None, Some(0))), 7)
                .is_err()
        );
        assert!(
            pg_row_event_id(&src(pos(Some("16/B374D8"), Some(1), None)), 7)
                .is_err()
        );
    }

    #[test]
    fn multi_relation_same_ordinal_do_not_collide() {
        // Two relations in one transaction (same final LSN + ordinal window)
        // must produce distinct ids.
        let r1 = pg_row_event_id(
            &src(pos(Some("16/B374D8"), Some(100), Some(0))),
            7,
        )
        .unwrap();
        let r2 = pg_row_event_id(
            &src(pos(Some("16/B374D8"), Some(200), Some(0))),
            7,
        )
        .unwrap();
        assert_ne!(r1, r2);
    }
}

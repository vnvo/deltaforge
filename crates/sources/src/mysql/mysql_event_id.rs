//! Provisional stable-`EventId` derivation for MySQL row events.
//!
//! Computes the `myrow` id from an event's already-plumbed coordinates
//! (exact per-transaction `SID:GNO`, rows-event `end_log_pos`, row ordinal;
//! or the non-GTID `server_id`+file fallback). This is *provisional*: it is not
//! yet stored in `Event.event_id` — that atomic cutover comes later. Missing
//! required coordinates are an error here; fatal runtime enforcement is deferred
//! to the cutover.

use deltaforge_core::{EventId, SourceInfo};

/// Parse a MySQL GTID UUID (`3e11fa47-71ca-11e1-9e33-c80aa9429562`) into 16
/// bytes. Returns `None` if it isn't a well-formed 32-hex-digit (dashed) UUID.
pub(crate) fn parse_uuid16(uuid: &str) -> Option<[u8; 16]> {
    let hex: String = uuid.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return None;
    }
    let bytes = hex::decode(hex).ok()?;
    let mut out = [0u8; 16];
    out.copy_from_slice(&bytes);
    Some(out)
}

/// Derive the provisional `myrow` [`EventId`] from a MySQL event's source
/// coordinates. `gtid` here must be the **current transaction's** exact
/// `uuid:gno` (not the accumulated executed set).
pub fn mysql_row_event_id(source: &SourceInfo) -> Result<EventId, String> {
    let pos = source
        .position
        .pos
        .ok_or("mysql event id: missing binlog position")?;
    let row = source
        .position
        .row
        .ok_or("mysql event id: missing row ordinal")?;

    // Prefer the GTID form when a single, parseable `uuid:gno` is present.
    if let Some(gtid) = source.position.gtid.as_deref() {
        if let Some((uuid_str, gno_str)) = gtid.rsplit_once(':') {
            if let (Some(sid), Ok(gno)) =
                (parse_uuid16(uuid_str), gno_str.parse::<u64>())
            {
                return Ok(EventId::mysql_row_gtid(&sid, gno, pos, row));
            }
        }
        // A gtid that doesn't parse as a single transaction is a coordinate
        // error rather than a silent fallback — the plumbing must supply the
        // exact per-transaction GTID.
        return Err(format!(
            "mysql event id: gtid {gtid:?} is not a single uuid:gno"
        ));
    }

    // Non-GTID fallback: server_id + binlog file.
    let server_id = source
        .position
        .server_id
        .ok_or("mysql event id: missing server_id")?;
    let file = source
        .position
        .file
        .as_deref()
        .ok_or("mysql event id: missing binlog file")?;
    Ok(EventId::mysql_row_server(server_id, file, pos, row))
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_core::SourcePosition;

    fn src(pos: SourcePosition) -> SourceInfo {
        SourceInfo {
            version: "1".into(),
            connector: "mysql".into(),
            name: "p".into(),
            ts_ms: 0,
            db: "d".into(),
            schema: None,
            table: "t".into(),
            snapshot: None,
            position: pos,
        }
    }

    #[test]
    fn gtid_form_matches_pinned_vector() {
        // Same coordinates as the RFC's myrow pinned vector.
        let pos = SourcePosition::mysql(
            0,
            Some("3e11fa47-71ca-11e1-9e33-c80aa9429562:23".into()),
            None,
            Some(1547),
            Some(0),
        );
        let id = mysql_row_event_id(&src(pos)).unwrap();
        assert_eq!(
            id.to_string(),
            "dfid:v1:myrow:b8107ad4ab475b60e3a0c28b6c336688"
        );
    }

    #[test]
    fn falls_back_to_server_form_without_gtid() {
        let pos = SourcePosition::mysql(
            42,
            None,
            Some("mysql-bin.000008".into()),
            Some(15248355),
            Some(2),
        );
        let id = mysql_row_event_id(&src(pos)).unwrap();
        // server form is a distinct, stable id.
        assert_eq!(id.class(), deltaforge_core::EventClass::MyRow);
        assert_eq!(
            id,
            EventId::mysql_row_server(42, "mysql-bin.000008", 15248355, 2)
        );
    }

    #[test]
    fn missing_row_ordinal_is_error() {
        let pos = SourcePosition::mysql(
            42,
            None,
            Some("mysql-bin.000008".into()),
            Some(100),
            None, // no row ordinal
        );
        assert!(mysql_row_event_id(&src(pos)).is_err());
    }

    #[test]
    fn accumulated_gtid_set_is_rejected() {
        // A merged executed set (range) is not a single transaction id.
        let pos = SourcePosition::mysql(
            0,
            Some("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100".into()),
            None,
            Some(1547),
            Some(0),
        );
        assert!(mysql_row_event_id(&src(pos)).is_err());
    }
}

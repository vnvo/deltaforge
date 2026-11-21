use crate::{
    binlog_error::BinlogError,
    constants,
    event::{
        delete_rows_event::DeleteRowsEvent, event_data::*, event_header::EventHeader,
        gtid_event::GtidEvent, previous_gtids_event::PreviousGtidsEvent, query_event::QueryEvent,
        rotate_event::RotateEvent, rows_query_event::RowsQueryEvent,
        table_map_event::TableMapEvent, transaction_payload_event::TransactionPayloadEvent,
        update_rows_event::UpdateRowsEvent, write_rows_event::WriteRowsEvent,
        xa_prepare_event::XaPrepareEvent, xid_event::XidEvent,
    },
    event::{event_type::EventType, format_description_event::FormatDescriptionEvent},
};

use std::{
    collections::HashMap,
    io::{Cursor, Read, Seek, SeekFrom},
    sync::Arc,
};

pub struct BinlogParser {
    pub checksum_length: u8,
    pub table_map_event_by_table_id: HashMap<u64, Arc<TableMapEvent>>,
    /// DeltaForge patch: decides, for each parsed event, whether it ends the
    /// transaction the events before it belong to. When it does, the table
    /// maps are released: a rows event only references a table map of its
    /// own statement, which MySQL writes before the statement's rows. `None`
    /// keeps every table map (upstream behavior).
    pub transaction_end: Option<TransactionEnd>,
    /// DeltaForge patch: table maps installed so far (a TABLE_MAP whose
    /// table id was not mapped at the time). Counts installations, not
    /// distinct ids: an id mapped again after a release counts again.
    pub table_maps_installed: u64,
    /// DeltaForge patch: the last TABLE_MAP parsed for each table id, with
    /// its event bytes. The same bytes again (the table's next transaction)
    /// reuse the parsed map instead of parsing and copying it: parsing is a
    /// function of those bytes alone. Kept across transaction ends, unlike
    /// the table maps above; emptied when it holds
    /// [`TABLE_MAP_REUSE_MAX`] ids and a new one arrives.
    parsed_table_maps: HashMap<u64, (Vec<u8>, Arc<TableMapEvent>)>,
    /// DeltaForge patch: return TABLE_MAP and rows events as
    /// [`EventData::Skipped`] without reading their bodies into memory or
    /// decoding them, for a reader that needs only the binlog's framing and
    /// statements. Compressed transaction payloads are parsed the same way.
    /// `false` parses them (upstream behavior).
    pub skip_rows: bool,
}

/// DeltaForge patch: the most table ids [`BinlogParser`] keeps a parsed
/// TABLE_MAP for, for reuse.
pub const TABLE_MAP_REUSE_MAX: usize = 4096;

/// See [`BinlogParser::transaction_end`].
pub type TransactionEnd = Box<dyn FnMut(&EventData) -> bool + Send>;

const MAGIC_VALUE: [u8; 4] = [0xfeu8, 0x62, 0x69, 0x6e];

impl BinlogParser {
    /// DeltaForge patch: a parser with no table maps and no
    /// [`transaction_end`](Self::transaction_end) rule.
    pub fn new(checksum_length: u8) -> Self {
        Self {
            checksum_length,
            table_map_event_by_table_id: HashMap::new(),
            transaction_end: None,
            table_maps_installed: 0,
            parsed_table_maps: HashMap::new(),
            skip_rows: false,
        }
    }

    pub fn check_magic<S: Read + Seek>(&mut self, stream: &mut S) -> Result<(), BinlogError> {
        let mut magic = [0u8; 4];
        stream.read_exact(&mut magic)?;
        match magic {
            MAGIC_VALUE => Ok(()),
            _ => Err(BinlogError::UnexpectedData("bad magic".into())),
        }
    }

    pub fn next<S: Read + Seek>(
        &mut self,
        stream: &mut S,
    ) -> Result<(EventHeader, EventData), BinlogError> {
        let (header, data) = self.parse_next(stream)?;
        if let Some(end) = self.transaction_end.as_mut() {
            if end(&data) {
                self.table_map_event_by_table_id.clear();
            }
        }
        Ok((header, data))
    }

    /// DeltaForge patch: the table ids a parsed TABLE_MAP is kept for, for
    /// reuse (at most [`TABLE_MAP_REUSE_MAX`]).
    pub fn reusable_table_maps(&self) -> usize {
        self.parsed_table_maps.len()
    }

    /// DeltaForge patch: the parsed TABLE_MAP kept for exactly these event
    /// bytes, if any.
    fn reusable_table_map(&self, buf: &[u8]) -> Option<Arc<TableMapEvent>> {
        let id: [u8; 6] = buf.get(..6)?.try_into().ok()?;
        let mut le = [0u8; 8];
        le[..6].copy_from_slice(&id);
        let (bytes, tm) = self.parsed_table_maps.get(&u64::from_le_bytes(le))?;
        (bytes[..] == *buf).then(|| tm.clone())
    }

    /// DeltaForge patch: keep `tm`, parsed from `buf`, for reuse.
    fn keep_table_map(&mut self, buf: Vec<u8>, tm: Arc<TableMapEvent>) {
        if self.parsed_table_maps.len() >= TABLE_MAP_REUSE_MAX
            && !self.parsed_table_maps.contains_key(&tm.table_id)
        {
            self.parsed_table_maps.clear();
        }
        self.parsed_table_maps.insert(tm.table_id, (buf, tm));
    }

    fn parse_next<S: Read + Seek>(
        &mut self,
        stream: &mut S,
    ) -> Result<(EventHeader, EventData), BinlogError> {
        let header = EventHeader::parse(stream)?;
        let data_length = header.event_length as usize
            - constants::EVENT_HEADER_LENGTH
            - self.checksum_length as usize;

        let event_type = EventType::from_code(header.event_type);
        if self.skip_rows && Self::is_rows_or_table_map(&event_type) {
            stream.seek(SeekFrom::Current(
                (data_length + self.checksum_length as usize) as i64,
            ))?;
            return Ok((header, EventData::Skipped));
        }

        let buf = self.read_event_data(stream, data_length)?;
        let mut cursor = Cursor::new(&buf);

        match event_type {
            EventType::FormatDescription => {
                let event_data = FormatDescriptionEvent::parse(&mut cursor, data_length)?;
                self.checksum_length = event_data.checksum_type.get_length();
                Ok((header, EventData::FormatDescription(event_data)))
            }

            EventType::PreviousGtids => Ok((
                header,
                EventData::PreviousGtids(PreviousGtidsEvent::parse(&mut cursor)?),
            )),

            EventType::Gtid => Ok((header, EventData::Gtid(GtidEvent::parse(&mut cursor)?))),

            EventType::Query => Ok((header, EventData::Query(QueryEvent::parse(&mut cursor)?))),

            EventType::TableMap => {
                let event_data = match self.reusable_table_map(&buf) {
                    Some(tm) => tm,
                    None => {
                        let tm = Arc::new(TableMapEvent::parse(&mut cursor)?);
                        self.keep_table_map(buf, tm.clone());
                        tm
                    }
                };
                if self
                    .table_map_event_by_table_id
                    .insert(event_data.table_id, event_data.clone())
                    .is_none()
                {
                    self.table_maps_installed += 1;
                }
                Ok((header, EventData::TableMap(event_data)))
            }

            EventType::WriteRows | EventType::ExtWriteRows => {
                let row_event_version = Self::get_row_event_version(&event_type);
                let event_data = WriteRowsEvent::parse(
                    &mut cursor,
                    &mut self.table_map_event_by_table_id,
                    row_event_version,
                )?;
                Ok((header, EventData::WriteRows(event_data)))
            }

            EventType::UpdateRows | EventType::ExtUpdateRows => {
                let row_event_version = Self::get_row_event_version(&event_type);
                let event_data = UpdateRowsEvent::parse(
                    &mut cursor,
                    &mut self.table_map_event_by_table_id,
                    row_event_version,
                )?;
                Ok((header, EventData::UpdateRows(event_data)))
            }

            EventType::DeleteRows | EventType::ExtDeleteRows => {
                let row_event_version = Self::get_row_event_version(&event_type);
                let event_data = DeleteRowsEvent::parse(
                    &mut cursor,
                    &mut self.table_map_event_by_table_id,
                    row_event_version,
                )?;
                Ok((header, EventData::DeleteRows(event_data)))
            }

            EventType::Xid => Ok((header, EventData::Xid(XidEvent::parse(&mut cursor)?))),

            EventType::XaPrepare => Ok((
                header,
                EventData::XaPrepare(XaPrepareEvent::parse(&mut cursor)?),
            )),

            EventType::TransactionPayload => Ok((
                header,
                EventData::TransactionPayload(TransactionPayloadEvent::parse_with(
                    &mut cursor,
                    self.skip_rows,
                )?),
            )),

            EventType::RowsQuery => Ok((
                header,
                EventData::RowsQuery(RowsQueryEvent::parse(&mut cursor)?),
            )),

            EventType::Rotate => Ok((header, EventData::Rotate(RotateEvent::parse(&mut cursor)?))),

            EventType::HeartBeat => Ok((header, EventData::HeartBeat)),

            _ => Ok((header, EventData::NotSupported)),
        }
    }

    fn read_event_data<S: Read + Seek>(
        &mut self,
        stream: &mut S,
        data_length: usize,
    ) -> Result<Vec<u8>, BinlogError> {
        // read data for current event
        let mut buf = vec![0u8; data_length];
        stream.read_exact(&mut buf)?;
        // skip checksum
        stream.seek(SeekFrom::Current(self.checksum_length as i64))?;
        Ok(buf)
    }

    /// DeltaForge patch: the events [`skip_rows`](Self::skip_rows) skips.
    fn is_rows_or_table_map(event_type: &EventType) -> bool {
        matches!(
            event_type,
            EventType::TableMap
                | EventType::WriteRows
                | EventType::ExtWriteRows
                | EventType::UpdateRows
                | EventType::ExtUpdateRows
                | EventType::DeleteRows
                | EventType::ExtDeleteRows
        )
    }

    fn get_row_event_version(event_type: &EventType) -> u8 {
        match event_type {
            EventType::ExtWriteRows | EventType::ExtUpdateRows | EventType::ExtDeleteRows => 2,
            _ => 1,
        }
    }
}

#[cfg(test)]
mod skip_rows_tests {
    use super::*;

    /// One event: header, `body`, a 4-byte checksum.
    fn event(event_type: u8, body: &[u8]) -> Vec<u8> {
        let len = (constants::EVENT_HEADER_LENGTH + body.len() + 4) as u32;
        let mut e = Vec::new();
        e.extend_from_slice(&0u32.to_le_bytes()); // timestamp
        e.push(event_type);
        e.extend_from_slice(&1u32.to_le_bytes()); // server id
        e.extend_from_slice(&len.to_le_bytes());
        e.extend_from_slice(&0u32.to_le_bytes()); // next position
        e.extend_from_slice(&0u16.to_le_bytes()); // flags
        e.extend_from_slice(body);
        e.extend_from_slice(&[0xaa; 4]); // checksum
        e
    }

    /// A TABLE_MAP-less WRITE_ROWS (v2) event then an XID event.
    fn rows_then_xid() -> Vec<u8> {
        let mut rows = vec![7, 0, 0, 0, 0, 0, 0, 0, 2, 0, 1, 0xff];
        rows.extend_from_slice(&[0x5a; 40]);
        let mut s = event(30, &rows);
        s.extend(event(16, &42u64.to_le_bytes()));
        s
    }

    #[test]
    fn a_skipped_rows_event_leaves_the_next_event_framed() {
        let bytes = rows_then_xid();
        let mut cursor = Cursor::new(&bytes);
        let mut parser = BinlogParser::new(4);
        parser.skip_rows = true;
        let (h, d) = parser.next(&mut cursor).unwrap();
        assert_eq!(h.event_type, 30);
        assert!(matches!(d, EventData::Skipped), "{d:?}");
        let (_, d) = parser.next(&mut cursor).unwrap();
        assert!(matches!(d, EventData::Xid(XidEvent { xid: 42 })), "{d:?}");
        assert_eq!(cursor.position() as usize, bytes.len());
    }

    #[test]
    fn without_skip_rows_the_same_rows_event_is_decoded() {
        let bytes = rows_then_xid();
        let mut parser = BinlogParser::new(4);
        // No table map for table id 7: decoding the rows fails.
        assert!(parser.next(&mut Cursor::new(&bytes)).is_err());
    }
}

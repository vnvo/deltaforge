use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::{
    delete_rows_event::DeleteRowsEvent, format_description_event::FormatDescriptionEvent,
    gtid_event::GtidEvent, previous_gtids_event::PreviousGtidsEvent, query_event::QueryEvent,
    rotate_event::RotateEvent, rows_query_event::RowsQueryEvent, table_map_event::TableMapEvent,
    transaction_payload_event::TransactionPayloadEvent, update_rows_event::UpdateRowsEvent,
    write_rows_event::WriteRowsEvent, xa_prepare_event::XaPrepareEvent, xid_event::XidEvent,
};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum EventData {
    NotSupported,
    FormatDescription(FormatDescriptionEvent),
    PreviousGtids(PreviousGtidsEvent),
    Gtid(GtidEvent),
    Query(QueryEvent),
    TableMap(Arc<TableMapEvent>),
    WriteRows(WriteRowsEvent),
    UpdateRows(UpdateRowsEvent),
    DeleteRows(DeleteRowsEvent),
    Xid(XidEvent),
    XaPrepare(XaPrepareEvent),
    Rotate(RotateEvent),
    TransactionPayload(TransactionPayloadEvent),
    RowsQuery(RowsQueryEvent),
    HeartBeat,
    /// DeltaForge patch: a TABLE_MAP or rows event whose body was not parsed
    /// ([`BinlogParser::skip_rows`](crate::binlog_parser::BinlogParser::skip_rows));
    /// its header is as received.
    Skipped,
}

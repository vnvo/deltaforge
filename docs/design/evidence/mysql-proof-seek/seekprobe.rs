//! Proof-seek protocol probe: open a binlog dump in one of three modes and
//! report where it starts, how long until the first transaction, and the
//! GTIDs it reads.
//!
//!   seekprobe <url> <mode: gtid|gtidpos|pos> <gtid_set> <file> <pos> <max_txns> <max_secs>
use std::time::Instant;

use mysql_binlog_connector_rust::{
    binlog_client::{BinlogClient, StartPosition},
    event::event_data::EventData,
};
use serde_json::json;

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (url, mode, set, file, pos) = (&a[1], &a[2], &a[3], &a[4], a[5].parse::<u32>().unwrap());
    let (max_txns, max_secs) = (a[6].parse::<usize>().unwrap(), a[7].parse::<f64>().unwrap());
    let mut client = match mode.as_str() {
        "pos" => BinlogClient::new(url, 77_001, StartPosition::BinlogPosition(file.clone(), pos)),
        _ => BinlogClient::new(url, 77_001, StartPosition::Gtid(set.clone())),
    };
    if mode == "gtidpos" {
        client.binlog_filename = file.clone();
        client.binlog_position = pos;
    }
    client.heartbeat_interval_secs = 1;
    client.timeout_secs = 30;
    let t0 = Instant::now();
    let mut stream = match client.connect().await {
        Ok(s) => s,
        Err(e) => {
            println!("{}", json!({"mode": mode, "error": format!("{e:?}"), "ms": t0.elapsed().as_secs_f64() * 1e3}));
            return;
        }
    };
    let opened = t0.elapsed().as_secs_f64() * 1e3;
    let mut first_txn_ms = None;
    let mut first_events = Vec::new();
    let mut gtids = Vec::new();
    let mut heartbeats_after = 0;
    let mut err = None;
    while t0.elapsed().as_secs_f64() < max_secs && gtids.len() < max_txns {
        match stream.read().await {
            Ok((h, d)) => {
                let start = h.next_event_position.saturating_sub(h.event_length);
                let kind = match &d {
                    EventData::HeartBeat => { heartbeats_after += 1; if !gtids.is_empty() && heartbeats_after > 2 { break } ; "heartbeat".to_string() }
                    EventData::Rotate(r) => format!("rotate {} {}", r.binlog_filename, r.binlog_position),
                    EventData::FormatDescription(_) => "format_description".into(),
                    EventData::PreviousGtids(p) => format!("previous_gtids {}", p.gtid_set),
                    EventData::Gtid(g) => {
                        first_txn_ms.get_or_insert(t0.elapsed().as_secs_f64() * 1e3);
                        heartbeats_after = 0;
                        gtids.push(g.gtid.clone());
                        format!("gtid {}", g.gtid)
                    }
                    EventData::Query(q) => format!("query {}", q.query.chars().take(60).collect::<String>()),
                    other => format!("{:?}", std::mem::discriminant(other)),
                };
                if first_events.len() < 8 {
                    first_events.push(json!({"type": h.event_type, "start": start, "end": h.next_event_position, "len": h.event_length, "what": kind}));
                }
            }
            Err(e) => { err = Some(format!("{e:?}")); break; }
        }
    }
    println!("{}", json!({
        "mode": mode, "open_ms": opened, "first_txn_ms": first_txn_ms,
        "txns": gtids.len(), "first_gtids": gtids.iter().take(5).collect::<Vec<_>>(),
        "last_gtid": gtids.last(), "gtids": gtids, "first_events": first_events,
        "error": err, "total_ms": t0.elapsed().as_secs_f64() * 1e3,
    }));
}

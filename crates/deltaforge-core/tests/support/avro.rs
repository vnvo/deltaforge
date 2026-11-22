//! A recording mock schema registry and row events of `shop.<table>`.

use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

use deltaforge_core::canonical::{
    CanonicalRow, CanonicalValue, DbValue, IntWidth, TypeMapping,
};
use deltaforge_core::encoding::avro::{
    EnvelopeSchemaLookup, SchemaIdentity, SourceSchemaProvider,
};
use deltaforge_core::encoding::avro_schema::{
    build_envelope_schema, build_value_schema,
};
use deltaforge_core::encoding::avro_types::{
    ColumnDesc, TypeConversionOpts, mysql_column_to_avro,
};
use deltaforge_core::{Event, EventId, Op, SourceInfo, SourcePosition};

/// The DDL-derived envelope schema of `shop.<table>`: an `id` column and
/// `extra` more, alternately nullable `bigint` and `varchar(255)`.
pub struct Provider {
    pub extra: usize,
}

impl SourceSchemaProvider for Provider {
    fn get_envelope_schema(
        &self,
        connector: &str,
        db: &str,
        _: &str,
        table: &str,
        _: &SchemaIdentity,
    ) -> EnvelopeSchemaLookup {
        let id = column("id", "int", "int", false);
        let extra = (1..=self.extra).map(|i| {
            if i % 2 == 0 {
                column(&format!("c{i}"), "bigint", "bigint", true)
            } else {
                column(&format!("c{i}"), "varchar", "varchar(255)", true)
            }
        });
        let columns = std::iter::once(id).chain(extra).collect();
        let value = build_value_schema(connector, db, table, columns);
        let (json, schema) =
            build_envelope_schema(connector, db, table, value).unwrap();
        EnvelopeSchemaLookup::Found(json, Arc::new(schema))
    }

    fn mapping_id(&self) -> &str {
        "subjects"
    }
}

fn column(
    name: &str,
    data_type: &str,
    column_type: &str,
    nullable: bool,
) -> serde_json::Value {
    mysql_column_to_avro(
        &ColumnDesc {
            name: name.into(),
            data_type: data_type.into(),
            column_type: column_type.into(),
            nullable,
            precision: None,
            scale: None,
            unsigned: false,
            is_array: false,
            element_type: None,
        },
        &TypeConversionOpts::default(),
    )
}

/// A schema registry that registers every schema and records the subject
/// of each registration.
pub fn registry() -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let subjects = Arc::new(Mutex::new(Vec::new()));
    let seen = subjects.clone();
    std::thread::spawn(move || {
        let mut next = 1;
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { continue };
            let mut buf = vec![0u8; 1 << 20];
            let mut read = 0;
            let mut head = String::new();
            loop {
                let n = s.read(&mut buf[read..]).unwrap_or(0);
                if n == 0 {
                    break;
                }
                read += n;
                let text = String::from_utf8_lossy(&buf[..read]).to_string();
                if let Some(h) = text.find("\r\n\r\n") {
                    head = text[..h].to_string();
                    let len = head
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    if read >= h + 4 + len {
                        break;
                    }
                }
            }
            let path = head.split_whitespace().nth(1).unwrap_or("").to_string();
            let (status, body) = if head.starts_with("POST") {
                if let Some(subject) = path
                    .strip_prefix("/subjects/")
                    .and_then(|p| p.strip_suffix("/versions"))
                {
                    seen.lock().unwrap().push(subject.to_string());
                }
                next += 1;
                ("200 OK", format!(r#"{{"id":{}}}"#, next - 1))
            } else {
                (
                    "404 Not Found",
                    r#"{"error_code":40401,"message":"Subject not found"}"#
                        .to_string(),
                )
            };
            let _ = write!(
                s,
                "HTTP/1.1 {status}\r\ncontent-type: application/json\r\n\
                 content-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    (url, subjects)
}

pub fn event(table: &str, ddl_schema: bool) -> Event {
    let row = CanonicalRow::new(vec![(
        "id".to_string(),
        CanonicalValue::Db(DbValue::int(1, IntWidth::W32).unwrap()),
    )])
    .unwrap();
    let mut e = Event::new_row(
        EventId::mysql_row_server(7, "binlog.000002", 100, 0),
        SourceInfo {
            version: "deltaforge-test".into(),
            connector: "mysql".into(),
            name: "subjects".into(),
            ts_ms: 1_760_000_000_000,
            db: "shop".into(),
            schema: None,
            table: table.into(),
            snapshot: None,
            position: SourcePosition {
                server_id: Some(7),
                file: Some("binlog.000002".into()),
                pos: Some(100),
                ..Default::default()
            },
        },
        Op::Create,
        None,
        Some(row),
        TypeMapping::V2,
        1_760_000_000_000,
        64,
    );
    if ddl_schema {
        e.schema_version = Some(format!("fp-{table}"));
        e.schema_sequence = Some(1);
    }
    e
}

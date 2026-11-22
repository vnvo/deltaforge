//! The schema registry subjects of row events: several tables on one topic
//! register under one subject per table with the record-name strategies,
//! and under the topic's one subject with `topic_name`, for both the
//! DDL-derived schemas and inferred ones.

use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

use deltaforge_core::canonical::{
    CanonicalRow, CanonicalValue, DbValue, IntWidth, TypeMapping,
};
use deltaforge_core::encoding::avro::{
    AvroEncoder, EnvelopeSchemaLookup, SchemaIdentity, SourceSchemaProvider,
    SubjectStrategy,
};
use deltaforge_core::encoding::avro_schema::{
    build_envelope_schema, build_value_schema,
};
use deltaforge_core::encoding::avro_types::{
    ColumnDesc, TypeConversionOpts, mysql_column_to_avro,
};
use deltaforge_core::envelope::EnvelopeType;
use deltaforge_core::{
    Event, EventId, NativeOpts, Op, SourceInfo, SourcePosition,
};

/// The DDL-derived envelope schema of `shop.<table>` (one `id` column).
struct Provider;

impl SourceSchemaProvider for Provider {
    fn get_envelope_schema(
        &self,
        connector: &str,
        db: &str,
        _: &str,
        table: &str,
        _: &SchemaIdentity,
    ) -> EnvelopeSchemaLookup {
        let id = mysql_column_to_avro(
            &ColumnDesc {
                name: "id".into(),
                data_type: "int".into(),
                column_type: "int".into(),
                nullable: false,
                precision: None,
                scale: None,
                unsigned: false,
                is_array: false,
                element_type: None,
            },
            &TypeConversionOpts::default(),
        );
        let value = build_value_schema(connector, db, table, vec![id]);
        let (json, schema) =
            build_envelope_schema(connector, db, table, value).unwrap();
        EnvelopeSchemaLookup::Found(json, Arc::new(schema))
    }

    fn mapping_id(&self) -> &str {
        "subjects"
    }
}

/// A schema registry that registers every schema and records the subject
/// of each registration.
fn registry() -> (String, Arc<Mutex<Vec<String>>>) {
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

fn event(table: &str, ddl_schema: bool) -> Event {
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

/// The subjects registered for one event of each of `orders` and `users`
/// on topic `cdc`.
async fn subjects(strategy: SubjectStrategy, ddl_schema: bool) -> Vec<String> {
    let (url, seen) = registry();
    let provider: Option<Arc<dyn SourceSchemaProvider>> =
        ddl_schema.then(|| Arc::new(Provider) as _);
    let encoder =
        AvroEncoder::with_source_schemas(&url, strategy, None, None, provider)
            .unwrap();
    let envelope = EnvelopeType::Native.build();
    for table in ["orders", "users"] {
        let e = event(table, ddl_schema);
        let data = envelope.wrap(&e, NativeOpts::default()).unwrap();
        encoder.encode_event("cdc", &e, &data).await.unwrap();
    }
    seen.lock().unwrap().clone()
}

#[tokio::test]
async fn record_name_strategies_give_each_table_of_a_topic_its_subject() {
    let envelope = |t: &str| format!("deltaforge.cdc.mysql.shop.{t}.Envelope");
    assert_eq!(
        subjects(SubjectStrategy::TopicRecordName, true).await,
        [
            format!("cdc-{}-v2", envelope("orders")),
            format!("cdc-{}-v2", envelope("users"))
        ]
    );
    assert_eq!(
        subjects(SubjectStrategy::RecordName, true).await,
        [
            format!("{}-v2", envelope("orders")),
            format!("{}-v2", envelope("users"))
        ]
    );
    // Inferred schemas are named per table too.
    let value = |t: &str| format!("deltaforge.mysql.shop.{t}.Value");
    assert_eq!(
        subjects(SubjectStrategy::TopicRecordName, false).await,
        [
            format!("cdc-{}-v2", value("orders")),
            format!("cdc-{}-v2", value("users"))
        ]
    );
}

#[tokio::test]
async fn topic_name_keeps_one_subject_per_topic() {
    assert_eq!(
        subjects(SubjectStrategy::TopicName, true).await,
        ["cdc-value-v2", "cdc-value-v2"]
    );
}

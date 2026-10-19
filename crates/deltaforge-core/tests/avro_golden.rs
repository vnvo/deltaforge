//! Avro encodings are exactly the reviewed ones: every MySQL column type
//! of the DDL-derived path (inserts, updates with both images, deletes),
//! the inferred path, each envelope and processor extensions, encoded with
//! a local schema registry. The golden (`avro_golden.txt`) was generated
//! before the encoder stopped rendering row images to JSON;
//! `DF_WRITE_AVRO_GOLDEN=1` rewrites it.

use std::io::{Read, Write};
use std::sync::Arc;

use deltaforge_core::canonical::{
    Bits, CanonicalRow, CanonicalValue, CivilDate, DateTimeValue, DateValue,
    DbValue, Decimal, Document, EnumValue, IntWidth, JsonText, Precision,
    SetValue, TimeValue, TimestampValue, TypeMapping,
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
    Event, EventId, NativeOpts, Op, SourceInfo, SourcePosition, Transaction,
};
use serde_json::json;

/// `(column, data type, column type, precision/scale, unsigned, value)`.
type Column = (
    &'static str,
    &'static str,
    &'static str,
    Option<(i64, i64)>,
    bool,
    fn(usize) -> CanonicalValue,
);

fn db(v: DbValue) -> CanonicalValue {
    CanonicalValue::Db(v)
}

fn columns() -> Vec<Column> {
    vec![
        ("id", "int", "int", None, false, |i| {
            db(DbValue::int(i as i64, IntWidth::W32).unwrap())
        }),
        ("t8", "tinyint", "tinyint", None, false, |i| {
            db(DbValue::int(-(i as i64) - 1, IntWidth::W8).unwrap())
        }),
        ("u32", "int", "int unsigned", None, true, |_| {
            db(DbValue::uint(4_294_967_295, IntWidth::W32).unwrap())
        }),
        ("b64", "bigint", "bigint", None, false, |i| {
            db(DbValue::int(i64::MIN + i as i64, IntWidth::W64).unwrap())
        }),
        ("ub64", "bigint", "bigint unsigned", None, true, |_| {
            db(DbValue::uint(u64::MAX, IntWidth::W64).unwrap())
        }),
        ("name", "varchar", "varchar(64)", None, false, |i| {
            db(DbValue::Text(format!("héllo \"{i}\"\n")))
        }),
        (
            "price",
            "decimal",
            "decimal(12,3)",
            Some((12, 3)),
            false,
            |i| {
                db(DbValue::Decimal(
                    Decimal::parse(&format!("-{}.125", i * 7)).unwrap(),
                ))
            },
        ),
        ("ratio", "double", "double", None, false, |i| {
            db(DbValue::Float64(i as f64 / 3.0))
        }),
        ("f", "float", "float", None, false, |_| {
            db(DbValue::Float32(1.1))
        }),
        ("day", "date", "date", None, false, |i| {
            db(DbValue::Date(DateValue::Civil(
                CivilDate::new(1969 + i as i32, 12, 31).unwrap(),
            )))
        }),
        ("dur", "time", "time(3)", None, false, |i| {
            db(DbValue::Time(
                TimeValue::mysql(
                    -((i as i64) * 3_600_000_000 + 15_000),
                    Precision::Fixed(3),
                )
                .unwrap(),
            ))
        }),
        ("dt", "datetime", "datetime(6)", None, false, |i| {
            db(DbValue::DateTime(
                DateTimeValue::civil(
                    CivilDate::new(2026, 2, 28).unwrap(),
                    (i as i64) * 1_000_001,
                    Precision::Fixed(6),
                )
                .unwrap(),
            ))
        }),
        ("zero_dt", "datetime", "datetime", None, false, |_| {
            db(DbValue::DateTime(
                DateTimeValue::civil(
                    CivilDate::mysql(0, 0, 0).unwrap(),
                    0,
                    Precision::Fixed(0),
                )
                .unwrap(),
            ))
        }),
        ("ts", "timestamp", "timestamp(6)", None, false, |i| {
            db(DbValue::Timestamp(
                TimestampValue::instant(
                    1_760_000_000_123_456 + i as i64,
                    Precision::Fixed(6),
                )
                .unwrap(),
            ))
        }),
        ("doc", "json", "json", None, false, |i| {
            db(DbValue::Json(
                JsonText::new(format!(r#"{{"k": [{i}, null, "x"]}}"#)).unwrap(),
            ))
        }),
        (
            "mood",
            "enum",
            "enum('sad','ok','happy')",
            None,
            false,
            |i| {
                let labels = ["sad", "ok", "happy"];
                db(DbValue::Enum(
                    EnumValue::mysql(1 + (i % 3) as u16, labels[i % 3].into())
                        .unwrap(),
                ))
            },
        ),
        ("tags", "set", "set('a','b','c')", None, false, |i| {
            let def: Vec<String> = ["a", "b", "c"].map(String::from).to_vec();
            db(DbValue::Set(
                SetValue::from_bitmask((i % 8) as u64, &def).unwrap(),
            ))
        }),
        ("flags", "bit", "bit(12)", Some((12, 0)), false, |i| {
            db(DbValue::Bits(Bits::from_u64(12, (i * 291) as u64).unwrap()))
        }),
        ("blob", "blob", "blob", None, false, |i| {
            db(DbValue::Binary(vec![0, 255, i as u8]))
        }),
        ("yr", "year", "year", None, false, |_| {
            db(DbValue::year(2155).unwrap())
        }),
        ("gone", "varchar", "varchar(8)", None, false, |_| {
            CanonicalValue::Null
        }),
    ]
}

fn row(i: usize, ext: bool) -> CanonicalRow {
    let cols = columns()
        .into_iter()
        .map(|(n, .., v)| (n.to_string(), v(i)))
        .collect();
    if ext {
        CanonicalRow::with_extensions(
            cols,
            [(
                "enriched".to_string(),
                Document::new(json!({"z": 1, "a": [true, null]})),
            )],
        )
        .unwrap()
    } else {
        CanonicalRow::new(cols).unwrap()
    }
}

fn schema() -> String {
    let fields = columns()
        .into_iter()
        .map(|(name, data_type, column_type, ps, unsigned, _)| {
            mysql_column_to_avro(
                &ColumnDesc {
                    name: name.into(),
                    data_type: data_type.into(),
                    column_type: column_type.into(),
                    nullable: name != "id",
                    precision: ps.map(|p| p.0),
                    scale: ps.map(|p| p.1),
                    unsigned,
                    is_array: false,
                    element_type: None,
                },
                &TypeConversionOpts::default(),
            )
        })
        .collect();
    let value = build_value_schema("mysql", "shop", "orders", fields);
    build_envelope_schema("mysql", "shop", "orders", value)
        .unwrap()
        .0
}

struct Provider(String);

impl SourceSchemaProvider for Provider {
    fn get_envelope_schema(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
        _: &SchemaIdentity,
    ) -> EnvelopeSchemaLookup {
        let schema = apache_avro::Schema::parse_str(&self.0).unwrap();
        EnvelopeSchemaLookup::Found(self.0.clone(), Arc::new(schema))
    }

    fn mapping_id(&self) -> &str {
        "golden"
    }
}

/// A schema registry that registers every schema, numbering them from 1,
/// and has no subject registered yet.
fn registry() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
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
            let (status, body) = if head.starts_with("POST") {
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
    url
}

fn event(i: usize, op: Op, ddl_schema: bool) -> Event {
    let (before, after) = match op {
        Op::Create => (None, Some(row(i, i % 2 == 1))),
        Op::Update => (Some(row(i, false)), Some(row(i + 1, true))),
        Op::Delete => (Some(row(i, false)), None),
        _ => unreachable!(),
    };
    let mut e = Event::new_row(
        EventId::mysql_row_server(7, "binlog.000002", 100 + i as u64, 0),
        SourceInfo {
            version: "deltaforge-golden".into(),
            connector: "mysql".into(),
            name: "golden".into(),
            ts_ms: 1_760_000_000_000,
            db: "shop".into(),
            schema: None,
            table: "orders".into(),
            snapshot: None,
            position: SourcePosition {
                server_id: Some(7),
                file: Some("binlog.000002".into()),
                pos: Some(100 + i as u64),
                ..Default::default()
            },
        },
        op,
        before,
        after,
        TypeMapping::V2,
        1_760_000_000_000 + i as i64,
        512,
    );
    e.transaction = Some(Transaction {
        id: format!("gtid:{i}"),
        total_order: Some(i as u64),
        data_collection_order: None,
    });
    if ddl_schema {
        e.schema_version = Some("fp-orders".into());
        e.schema_sequence = Some(3);
    }
    e
}

async fn golden() -> String {
    let encoder = AvroEncoder::with_source_schemas(
        &registry(),
        SubjectStrategy::TopicName,
        None,
        None,
        Some(Arc::new(Provider(schema()))),
    )
    .unwrap();
    let mut out = String::new();
    let envelopes = [
        ("native", EnvelopeType::Native),
        ("debezium", EnvelopeType::Debezium),
        (
            "cloudevents",
            EnvelopeType::CloudEvents {
                type_prefix: "com.example".into(),
            },
        ),
    ];
    for (name, envelope) in envelopes {
        let envelope = envelope.build();
        for (i, op) in [Op::Create, Op::Create, Op::Update, Op::Delete]
            .into_iter()
            .enumerate()
        {
            for ddl in [true, false] {
                let e = event(i, op, ddl);
                let data = envelope.wrap(&e, NativeOpts::default()).unwrap();
                let topic =
                    format!("{name}-{}", if ddl { "ddl" } else { "inf" });
                let line = match encoder.encode_event(&topic, &e, &data).await {
                    Ok(bytes) => hex::encode(bytes),
                    Err(err) => format!("error: {err}"),
                };
                out.push_str(&format!("{name} {op:?} ddl={ddl} {line}\n"));
            }
        }
    }
    out
}

#[tokio::test]
async fn avro_encodings_match_the_golden_bytes() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/avro_golden.txt");
    let actual = golden().await;
    if std::env::var_os("DF_WRITE_AVRO_GOLDEN").is_some() {
        std::fs::write(path, &actual).unwrap();
    }
    let golden = std::fs::read_to_string(path).unwrap();
    for (a, g) in actual.lines().zip(golden.lines()) {
        assert_eq!(a, g);
    }
    assert_eq!(actual.lines().count(), golden.lines().count());
}

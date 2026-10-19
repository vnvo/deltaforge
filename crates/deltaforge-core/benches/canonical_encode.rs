//! Per-event cost of the canonical delivery hotspots, in process: native
//! JSON rendering (narrow, wide, temporal, null-heavy and 200-integer rows),
//! DDL-derived Avro encoding and the default Kafka key. Each case reports
//! the median time per event over several runs, the exact heap allocations
//! per event (a counting allocator) and the output bytes per event.
//!
//! Run with: `cargo bench -p deltaforge-core --bench canonical_encode`
//! (`-- <filter>` runs only the cases whose name contains it).

// The counting global allocator is the only unsafe code: it forwards every
// call to the system allocator unchanged.
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::{Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use deltaforge_core::canonical::{
    CanonicalRow, CanonicalValue, CivilDate, DateTimeValue, DateValue, DbValue,
    Decimal, IntWidth, Precision, TimeValue, TimestampValue, TypeMapping,
};
use deltaforge_core::encoding::Json;
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
use deltaforge_core::envelope::{Envelope, Native};
use deltaforge_core::{
    Event, EventId, NativeOpts, Op, SourceInfo, SourcePosition, Transaction,
};

struct Counting;

static ALLOCS: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(
        &self,
        ptr: *mut u8,
        layout: Layout,
        new_size: usize,
    ) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// A MySQL column of the fixtures: its DDL type and a value of row `i`.
#[derive(Clone, Copy)]
enum Col {
    Int,
    BigInt,
    Varchar,
    Decimal,
    Double,
    Date,
    Time,
    DateTime,
    Timestamp,
}

impl Col {
    fn value(self, i: usize) -> CanonicalValue {
        let i = i as i64;
        let micros = 1_760_000_000_000_000 + i * 1_234_567;
        CanonicalValue::Db(match self {
            Col::Int => DbValue::int(i * 7, IntWidth::W32).unwrap(),
            Col::BigInt => {
                DbValue::int(i * 1_000_003 + 9_000_000_000, IntWidth::W64)
                    .unwrap()
            }
            Col::Varchar => DbValue::Text(format!("{:0>60}{i:04}", "row")),
            Col::Decimal => DbValue::Decimal(
                Decimal::parse(&format!("{}.{:03}", i * 13, i % 1000)).unwrap(),
            ),
            Col::Double => DbValue::Float64(i as f64 * 1.25),
            Col::Date => DbValue::Date(DateValue::Civil(
                CivilDate::new(2026, 1 + (i % 12) as u8, 1 + (i % 28) as u8)
                    .unwrap(),
            )),
            Col::Time => DbValue::Time(
                TimeValue::mysql(
                    (i % 86_400) * 1_000_000 + 123_456,
                    Precision::fixed(6).unwrap(),
                )
                .unwrap(),
            ),
            Col::DateTime => DbValue::DateTime(
                DateTimeValue::civil(
                    CivilDate::new(2026, 10, 1 + (i % 28) as u8).unwrap(),
                    (i % 86_400) * 1_000_000 + 654_321,
                    Precision::fixed(6).unwrap(),
                )
                .unwrap(),
            ),
            Col::Timestamp => DbValue::Timestamp(
                TimestampValue::instant(micros, Precision::fixed(6).unwrap())
                    .unwrap(),
            ),
        })
    }

    fn ddl(self) -> (&'static str, &'static str, Option<(i64, i64)>) {
        match self {
            Col::Int => ("int", "int", None),
            Col::BigInt => ("bigint", "bigint", None),
            Col::Varchar => ("varchar", "varchar(64)", None),
            Col::Decimal => ("decimal", "decimal(12,3)", Some((12, 3))),
            Col::Double => ("double", "double", None),
            Col::Date => ("date", "date", None),
            Col::Time => ("time", "time(6)", None),
            Col::DateTime => ("datetime", "datetime(6)", None),
            Col::Timestamp => ("timestamp", "timestamp(6)", None),
        }
    }
}

/// A table shape: its columns, and every how-many-th value is NULL (0:
/// none).
struct Shape {
    name: &'static str,
    cols: Vec<Col>,
    null_every: usize,
}

fn shapes() -> Vec<Shape> {
    use Col::*;
    let wide: Vec<Col> =
        [Int, BigInt, Varchar, Decimal, Double, Date, DateTime]
            .into_iter()
            .cycle()
            .take(60)
            .collect();
    vec![
        Shape {
            name: "narrow",
            cols: vec![Int, Varchar, Decimal, Timestamp],
            null_every: 0,
        },
        Shape {
            name: "wide",
            cols: wide.clone(),
            null_every: 0,
        },
        Shape {
            name: "temporal",
            cols: [Date, Time, DateTime, Timestamp]
                .into_iter()
                .cycle()
                .take(20)
                .collect(),
            null_every: 0,
        },
        Shape {
            name: "nullheavy",
            cols: wide,
            null_every: 1, // 9 of every 10 values, below
        },
        Shape {
            name: "ints200",
            cols: vec![Int; 200],
            null_every: 0,
        },
    ]
}

impl Shape {
    fn column(&self, c: usize) -> String {
        if c == 0 {
            "id".to_string()
        } else {
            format!("c{c}")
        }
    }

    fn row(&self, i: usize) -> CanonicalRow {
        let cols = self
            .cols
            .iter()
            .enumerate()
            .map(|(c, col)| {
                let null =
                    self.null_every > 0 && c > 0 && !(c + i).is_multiple_of(10);
                let v = if null {
                    CanonicalValue::Null
                } else {
                    col.value(i + c)
                };
                (self.column(c), v)
            })
            .collect();
        CanonicalRow::new(cols).unwrap()
    }

    fn event(&self, i: usize) -> Event {
        let mut e = Event::new_row(
            EventId::mysql_row_server(1, "binlog.000001", 4 + i as u64, 0),
            SourceInfo {
                version: "deltaforge-0.1.0".into(),
                connector: "mysql".into(),
                name: "perf-mysql".into(),
                ts_ms: 1_760_000_000_000,
                db: "perf".into(),
                schema: None,
                table: self.name.into(),
                snapshot: None,
                position: SourcePosition {
                    server_id: Some(1),
                    file: Some("binlog.000001".into()),
                    pos: Some(4 + i as u64),
                    ..Default::default()
                },
            },
            Op::Create,
            None,
            Some(self.row(i)),
            TypeMapping::V2,
            1_760_000_000_000,
            256,
        );
        e.transaction = Some(Transaction {
            id: format!("3e11fa47-71ca-11e1-9e33-c80aa9429562:{}", 1 + i / 500),
            total_order: None,
            data_collection_order: None,
        });
        e.schema_version = Some("fp1".into());
        e.schema_sequence = Some(1);
        e
    }

    /// The table's DDL-derived Avro envelope schema.
    fn avro_schema(&self) -> String {
        let fields = self
            .cols
            .iter()
            .enumerate()
            .map(|(c, col)| {
                let (data_type, column_type, ps) = col.ddl();
                mysql_column_to_avro(
                    &ColumnDesc {
                        name: self.column(c),
                        data_type: data_type.into(),
                        column_type: column_type.into(),
                        nullable: c > 0,
                        precision: ps.map(|p| p.0),
                        scale: ps.map(|p| p.1),
                        unsigned: false,
                        is_array: false,
                        element_type: None,
                    },
                    &TypeConversionOpts::default(),
                )
            })
            .collect();
        let value = build_value_schema("mysql", "perf", self.name, fields);
        build_envelope_schema("mysql", "perf", self.name, value)
            .unwrap()
            .0
    }
}

struct Provider(Vec<(String, String)>);

impl SourceSchemaProvider for Provider {
    fn get_envelope_schema(
        &self,
        _connector: &str,
        _database: &str,
        _namespace: &str,
        table: &str,
        _identity: &SchemaIdentity,
    ) -> EnvelopeSchemaLookup {
        let json = &self.0.iter().find(|(t, _)| t == table).unwrap().1;
        let schema = apache_avro::Schema::parse_str(json).unwrap();
        EnvelopeSchemaLookup::Found(json.clone(), Arc::new(schema))
    }

    fn mapping_id(&self) -> &str {
        "bench"
    }
}

/// A schema registry that registers every schema as ID 1.
fn registry() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { continue };
            let mut buf = vec![0u8; 1 << 20];
            let mut read = 0;
            // Headers, then the declared body.
            loop {
                let n = s.read(&mut buf[read..]).unwrap_or(0);
                if n == 0 {
                    break;
                }
                read += n;
                let text = String::from_utf8_lossy(&buf[..read]);
                if let Some(h) = text.find("\r\n\r\n") {
                    let len = text[..h]
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
            let body = r#"{"id":1}"#;
            let _ = write!(
                s,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                 content-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    url
}

/// Time `per_event` over `events`: the median nanoseconds per event of
/// `RUNS` runs, the allocations per event and the output bytes per event.
const RUNS: usize = 9;

fn measure(
    name: &str,
    events: &[Event],
    mut per_event: impl FnMut(&Event) -> usize,
) {
    // Warm up (and size caches) once.
    for e in events {
        per_event(e);
    }
    let mut times = Vec::with_capacity(RUNS);
    let mut allocs = 0;
    let mut bytes = 0;
    for _ in 0..RUNS {
        let a0 = ALLOCS.load(Ordering::Relaxed);
        let t0 = Instant::now();
        bytes = 0;
        for e in events {
            bytes += per_event(e);
        }
        times.push(t0.elapsed().as_nanos() as f64 / events.len() as f64);
        allocs = ALLOCS.load(Ordering::Relaxed) - a0;
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = events.len() as f64;
    println!(
        "| {name} | {:.0} | {:.1} | {:.0} |",
        times[RUNS / 2],
        allocs as f64 / n,
        bytes as f64 / n
    );
}

fn main() {
    let filter: Vec<String> = std::env::args()
        .skip(1)
        .filter(|a| !a.starts_with('-'))
        .collect();
    let wanted = |name: &str| {
        filter.is_empty() || filter.iter().any(|f| name.contains(f))
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    println!("| case | ns/event | allocs/event | bytes/event |");
    println!("|---|---|---|---|");
    let shapes = shapes();
    let count = |s: &Shape| if s.cols.len() > 50 { 2_000 } else { 10_000 };
    for shape in &shapes {
        let name = format!("json-{}", shape.name);
        if !wanted(&name) {
            continue;
        }
        let events: Vec<Event> =
            (0..count(shape)).map(|i| shape.event(i)).collect();
        measure(&name, &events, |e| {
            let data = Native.wrap(e, NativeOpts::default()).unwrap();
            Json.encode(&data).unwrap().len()
        });
    }
    let avro: Vec<&Shape> = shapes
        .iter()
        .filter(|s| ["narrow", "wide", "temporal"].contains(&s.name))
        .collect();
    if avro.iter().any(|s| wanted(&format!("avro-{}", s.name))) {
        let provider = Provider(
            avro.iter()
                .map(|s| (s.name.to_string(), s.avro_schema()))
                .collect(),
        );
        let encoder = AvroEncoder::with_source_schemas(
            &registry(),
            SubjectStrategy::TopicName,
            None,
            None,
            Some(Arc::new(provider)),
        )
        .unwrap();
        for shape in avro {
            let name = format!("avro-{}", shape.name);
            if !wanted(&name) {
                continue;
            }
            let events: Vec<Event> =
                (0..count(shape)).map(|i| shape.event(i)).collect();
            measure(&name, &events, |e| {
                let data = Native.wrap(e, NativeOpts::default()).unwrap();
                rt.block_on(encoder.encode_event(shape.name, e, &data))
                    .unwrap()
                    .len()
            });
        }
    }
    if wanted("key") {
        let events: Vec<Event> =
            (0..10_000).map(|i| shapes[0].event(i)).collect();
        measure("key", &events, |e| e.idempotency_key().len());
    }
}

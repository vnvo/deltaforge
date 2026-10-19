//! Per-event cost of the typed JavaScript boundary, in process: the batch's
//! schema resolution (as the coordinator runs it before any processor) and
//! the JavaScript processor (rendering into V8, the script, re-typing the
//! outputs), for MySQL rows of the perf harness's narrow shape and its
//! script. Reports the median time per event of several runs and the exact
//! heap allocations per event (a counting allocator).
//!
//! Run with: `cargo bench -p processors --bench js_boundary_bench`

// The counting global allocator is the only unsafe code: it forwards every
// call to the system allocator unchanged.
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use async_trait::async_trait;
use deltaforge_core::canonical::schema::{
    ColumnType, ColumnTypes, ResolvedSchemas, SchemaContext,
    SchemaContextError, SchemaRef,
};
use deltaforge_core::canonical::{
    CanonicalRow, CanonicalValue, DbValue, Decimal, IntWidth, Precision,
    TimestampValue, TypeMapping,
};
use deltaforge_core::{
    BatchContext, Event, EventId, Op, Processor, SourceInfo, SourcePosition,
    Transaction,
};
use processors::JsProcessor;

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

const SCRIPT: &str = "function processBatch(events) { return events.map(e => \
     { if (e.after) { e.after.tagged = true; } return e; }); }";

fn types() -> Arc<ColumnTypes> {
    Arc::new(ColumnTypes::new(
        "fp1".into(),
        Some(1),
        vec![
            (
                "id".into(),
                ColumnType::Int {
                    width: IntWidth::W32,
                    unsigned: false,
                },
            ),
            ("name".into(), ColumnType::Text),
            ("price".into(), ColumnType::Decimal),
            (
                "ts".into(),
                ColumnType::Timestamp {
                    precision: Precision::Fixed(6),
                },
            ),
        ],
    ))
}

fn event(i: usize) -> Event {
    let row = CanonicalRow::new(vec![
        (
            "id".into(),
            CanonicalValue::Db(DbValue::int(i as i64, IntWidth::W32).unwrap()),
        ),
        (
            "name".into(),
            CanonicalValue::Db(DbValue::Text(format!("name-{i:08}"))),
        ),
        (
            "price".into(),
            CanonicalValue::Db(DbValue::Decimal(
                Decimal::parse(&format!("{}.{:03}", i, i % 1000)).unwrap(),
            )),
        ),
        (
            "ts".into(),
            CanonicalValue::Db(DbValue::Timestamp(
                TimestampValue::instant(
                    1_760_000_000_000_000 + i as i64,
                    Precision::Fixed(6),
                )
                .unwrap(),
            )),
        ),
    ])
    .unwrap();
    let mut e = Event::new_row(
        EventId::mysql_row_server(1, "binlog.000001", 4 + i as u64, 0),
        SourceInfo {
            version: "deltaforge-0.1.0".into(),
            connector: "mysql".into(),
            name: "perf-mysql".into(),
            ts_ms: 1_760_000_000_000,
            db: "perf".into(),
            schema: None,
            table: "narrow".into(),
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
        Some(row),
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

struct Context(Arc<ColumnTypes>);

#[async_trait]
impl SchemaContext for Context {
    async fn resolve(
        &self,
        _: &SchemaRef,
    ) -> Result<Arc<ColumnTypes>, SchemaContextError> {
        Ok(Arc::clone(&self.0))
    }
}

const BATCH: usize = 500;
const BATCHES: usize = 20;
const RUNS: usize = 7;

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let js = JsProcessor::new("js".into(), SCRIPT.into(), None).unwrap();
    let context = Context(types());
    let batches: Vec<Vec<Event>> = (0..BATCHES)
        .map(|b| (0..BATCH).map(|i| event(b * BATCH + i)).collect())
        .collect();
    let n = (BATCH * BATCHES) as f64;
    let (mut resolve_ns, mut process_ns) = (Vec::new(), Vec::new());
    let (mut resolve_allocs, mut process_allocs) = (0u64, 0u64);
    for run in 0..=RUNS {
        let inputs = batches.clone();
        let (mut rt_ns, mut pt_ns, mut ra, mut pa) = (0u128, 0u128, 0, 0);
        for events in inputs {
            let a0 = ALLOCS.load(Ordering::Relaxed);
            let t0 = Instant::now();
            let resolved = rt
                .block_on(ResolvedSchemas::resolve(&context, &events))
                .unwrap();
            rt_ns += t0.elapsed().as_nanos();
            ra += ALLOCS.load(Ordering::Relaxed) - a0;
            let ctx = BatchContext::from_batch(&events)
                .with_schemas(Arc::new(resolved));
            let a0 = ALLOCS.load(Ordering::Relaxed);
            let t0 = Instant::now();
            let out = rt.block_on(js.process(events, &ctx)).unwrap();
            pt_ns += t0.elapsed().as_nanos();
            pa += ALLOCS.load(Ordering::Relaxed) - a0;
            assert_eq!(out.len(), BATCH);
            drop(out);
        }
        if run > 0 {
            // The first run warms up.
            resolve_ns.push(rt_ns as f64 / n);
            process_ns.push(pt_ns as f64 / n);
            resolve_allocs = ra;
            process_allocs = pa;
        }
    }
    println!("| stage | ns/event | allocs/event |");
    println!("|---|---|---|");
    println!(
        "| schema resolution | {:.0} | {:.1} |",
        median(resolve_ns),
        resolve_allocs as f64 / n
    );
    println!(
        "| JS processor | {:.0} | {:.1} |",
        median(process_ns),
        process_allocs as f64 / n
    );
}

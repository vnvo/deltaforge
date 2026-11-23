//! The memory an Avro encoder holds per table: a DDL-derived table schema is
//! kept as its compiled plan only (a few KB), so a catalog of many tables
//! stays small.

#![allow(unsafe_code)]

mod support;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, Ordering};

use deltaforge_core::NativeOpts;
use deltaforge_core::encoding::avro::{
    AvroEncoder, SourceSchemaProvider, SubjectStrategy,
};
use deltaforge_core::envelope::EnvelopeType;
use support::avro::{Provider, event, registry};

/// Live heap bytes of the test process.
struct Counting;

static LIVE: AtomicIsize = AtomicIsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        LIVE.fetch_add(l.size() as isize, Ordering::Relaxed);
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size() as isize, Ordering::Relaxed);
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, size: usize) -> *mut u8 {
        LIVE.fetch_add(size as isize - l.size() as isize, Ordering::Relaxed);
        unsafe { System.realloc(p, l, size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The tests measure the one process heap: one at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn each_table_schema_costs_a_few_kilobytes() {
    let _serial = SERIAL.lock().await;
    let (url, _) = registry();
    let encoder = encoder(&url, 11, None);
    // The first encode sets up the client and its connection pool.
    encode(&encoder, "warm").await;
    let tables = 200;
    let before = LIVE.load(Ordering::Relaxed);
    for i in 0..tables {
        encode(&encoder, &format!("t{i:04}")).await;
    }
    let per_table = (LIVE.load(Ordering::Relaxed) - before) / tables;
    // A 12-column table: its plan, registry ID and cache keys, about 6.4 KB
    // (measured). Its parsed schema and one apache-avro writer were about
    // 300 KB (eight writers and every named type cloned, 1.88 MB).
    assert!(per_table < 16_000, "{per_table} bytes per table");
}

/// The encoder's estimate of what it holds is the heap it holds, within a
/// tenth, for narrow and wide tables: the bound is in real bytes. Measured
/// as the heap a clear of the caches frees (the process heap moves by more
/// than a few small entries meanwhile).
#[tokio::test]
async fn the_cache_estimate_tracks_the_heap_held() {
    let _serial = SERIAL.lock().await;
    for extra in [0, 11, 60, 250] {
        let (url, _) = registry();
        let encoder = encoder(&url, extra, None);
        for i in 0..50 {
            encode(&encoder, &format!("t{i:04}")).await;
        }
        let charged = encoder.schema_cache_bytes() as f64;
        let before = LIVE.load(Ordering::Relaxed);
        encoder.clear_caches();
        let freed = (before - LIVE.load(Ordering::Relaxed)) as f64;
        assert_eq!(encoder.schema_cache_bytes(), 0);
        let ratio = charged / freed;
        assert!(
            (0.9..1.1).contains(&ratio),
            "{} columns: charged {charged} for {freed} heap bytes",
            extra + 1
        );
    }
}

/// A bounded encoder holds about its bound however many tables it encodes;
/// an evicted table encodes again, to the same schema ID and bytes, without
/// registering again (its ID is still cached).
#[tokio::test]
async fn a_bounded_encoder_stays_within_its_bound_and_re_resolves() {
    let _serial = SERIAL.lock().await;
    let (url, seen) = registry();
    // About 150 tables of 12 columns.
    let limit = 1_000_000;
    let encoder = encoder(&url, 11, Some(limit));
    let first = encode(&encoder, "t0000").await;
    for i in 1..400 {
        encode(&encoder, &format!("t{i:04}")).await;
    }
    assert!(encoder.schema_cache_evictions() > 0);
    // Within the bound plus one entry (an insert evicts after it charges).
    assert!(encoder.schema_cache_bytes() < limit + 16_000);
    let registered = seen.lock().unwrap().len();
    assert_eq!(registered, 400);
    assert_eq!(encode(&encoder, "t0000").await, first);
    assert_eq!(
        seen.lock().unwrap().len(),
        registered,
        "no new registration"
    );
    // The heap it holds is within the bound too (what a clear frees).
    let before = LIVE.load(Ordering::Relaxed);
    encoder.clear_caches();
    let freed = before - LIVE.load(Ordering::Relaxed);
    assert!(
        freed < (limit + 16_000) as isize * 11 / 10,
        "{freed} heap bytes held"
    );
}

fn encoder(url: &str, extra: usize, limit: Option<u64>) -> AvroEncoder {
    let provider: Arc<dyn SourceSchemaProvider> = Arc::new(Provider { extra });
    AvroEncoder::with_source_schemas(
        url,
        SubjectStrategy::TopicRecordName,
        None,
        None,
        Some(provider),
    )
    .unwrap()
    .with_schema_cache(limit, None)
}

async fn encode(encoder: &AvroEncoder, table: &str) -> bytes::Bytes {
    let e = event(table, true);
    let envelope = EnvelopeType::Native.build();
    let data = envelope.wrap(&e, NativeOpts::default()).unwrap();
    encoder.encode_event("cdc", &e, &data).await.unwrap()
}

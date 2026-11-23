//! The memory an Avro encoder holds per table: each registered table schema
//! is kept once, with one writer until concurrent encodes of that schema need
//! more, so the memory of a catalog of many tables stays near the size of
//! their schemas.

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
async fn each_table_schema_costs_about_its_parsed_size() {
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
    // A 12-column table's parsed envelope schema is about 68 KB and one
    // writer's resolved copy of it about 204 KB (apache-avro 0.17); eight
    // writers and every named type cloned were 1.88 MB.
    assert!(per_table < 400_000, "{per_table} bytes per table");
}

/// The encoder's estimate of what it holds is the heap it holds, within a
/// tenth (measured: about 1%), for narrow and wide tables: the bound is in real bytes.
#[tokio::test]
async fn the_cache_estimate_tracks_the_heap_held() {
    let _serial = SERIAL.lock().await;
    for extra in [0, 11, 60, 250] {
        let (url, _) = registry();
        let encoder = encoder(&url, extra, None);
        encode(&encoder, "warm").await;
        let (heap, charged) =
            (LIVE.load(Ordering::Relaxed), encoder.schema_cache_bytes());
        for i in 0..20 {
            encode(&encoder, &format!("t{i:04}")).await;
        }
        let heap = (LIVE.load(Ordering::Relaxed) - heap) as f64;
        let charged = (encoder.schema_cache_bytes() - charged) as f64;
        let ratio = charged / heap;
        assert!(
            (0.9..1.1).contains(&ratio),
            "{} columns: charged {charged} for {heap} heap bytes",
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
    let limit = 4_000_000;
    let encoder = encoder(&url, 11, Some(limit));
    let first = encode(&encoder, "t0000").await;
    let before = LIVE.load(Ordering::Relaxed);
    for i in 1..100 {
        encode(&encoder, &format!("t{i:04}")).await;
    }
    let held = LIVE.load(Ordering::Relaxed) - before;
    assert!(encoder.schema_cache_evictions() > 0);
    // Within the bound plus one entry (an insert evicts after it charges).
    assert!(encoder.schema_cache_bytes() < limit + 400_000);
    assert!(held < (limit + 400_000) as isize, "{held} heap bytes held");
    let registered = seen.lock().unwrap().len();
    assert_eq!(registered, 100);
    assert_eq!(encode(&encoder, "t0000").await, first);
    assert_eq!(
        seen.lock().unwrap().len(),
        registered,
        "no new registration"
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

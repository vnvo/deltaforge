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

#[tokio::test]
async fn each_table_schema_costs_about_its_parsed_size() {
    let (url, _) = registry();
    let provider: Arc<dyn SourceSchemaProvider> =
        Arc::new(Provider { extra: 11 });
    let encoder = AvroEncoder::with_source_schemas(
        &url,
        SubjectStrategy::TopicRecordName,
        None,
        None,
        Some(provider),
    )
    .unwrap();
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

async fn encode(encoder: &AvroEncoder, table: &str) {
    let e = event(table, true);
    let envelope = EnvelopeType::Native.build();
    let data = envelope.wrap(&e, NativeOpts::default()).unwrap();
    encoder.encode_event("cdc", &e, &data).await.unwrap();
}

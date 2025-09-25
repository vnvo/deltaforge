use anyhow::Result;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use deltaforge_core::{Event, Op, SourceMeta, Processor};
use deltaforge_processor_js::JsProcessor;
use once_cell::sync::Lazy;
use serde_json::json;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

static RT: Lazy<Runtime> = Lazy::new(|| Runtime::new().unwrap());

fn make_small_event() -> Event {
    Event::new_row(
        "t1".into(),
        SourceMeta { kind: "mysql".into(), host: "h".into(), db: "d".into() },
        "d.orders".into(),
        Op::Insert,
        None,
        Some(json!({"id": 1, "sku": "ABC-1", "qty": 2})),
        1_700_000_000_000,
    )
}

fn make_large_event(bytes: usize) -> Event {
    let big = "x".repeat(bytes);
    Event::new_row(
        "t1".into(),
        SourceMeta { kind: "mysql".into(), host: "h".into(), db: "d".into() },
        "d.orders".into(),
        Op::Insert,
        None,
        Some(json!({"id": 1, "desc": big, "tags": ["a","b","c"], "nested": {"k": [1,2,3,4,5]}})),
        1_700_000_000_000,
    )
}

// A tiny Rust baseline processor (does nothing)
struct RustNoop;

#[async_trait::async_trait]
impl Processor for RustNoop {
    async fn process(&self, e: Event) -> Result<Vec<Event>> {
        Ok(vec![e])
    }
}

// -------- Pooled JS prototype (reuses one runtime) --------
mod pooled {
    use super::*;
    use deno_core::{extension, JsRuntime, RuntimeOptions};
    use deno_core::{serde_v8, v8};
    use serde_json::Value;

    extension!(df_ext, ops = [op_log]);
    #[deno_core::op2(fast)]
    fn op_log(#[string] _msg: &str) { /* no-op in bench */ }

    pub struct PooledJs {
        rt: std::sync::Mutex<JsRuntime>,
    }

    impl PooledJs {
        pub fn new(source: String) -> Self {
            let ext = df_ext::init();
            let mut rt = JsRuntime::new(RuntimeOptions {
                extensions: vec![ext],
                ..Default::default()
            });
            // Install compiled function once
            let code = format!(
                r#"
                globalThis.processEvent = (event) => {{
                    const userFn = (event) => {{ {source} }};
                    return userFn(event);
                }};
                "#,
                source = source
            );
            rt.execute_script("bootstrap.js", code).unwrap();
            Self { rt: std::sync::Mutex::new(rt) }
        }

        pub fn call(&self, event: &Event) -> Vec<Event> {
            let ev_json: Value = serde_json::to_value(event).unwrap();
            let mut rt = self.rt.lock().unwrap();
            // Call processEvent(event)
            let result_val: Value = {
                let scope = &mut rt.handle_scope();
                let ctx = scope.get_current_context();
                let global = ctx.global(scope);
                let name = v8::String::new(scope, "processEvent").unwrap();
                let val = global.get(scope, name.into()).unwrap();
                let func = v8::Local::<v8::Function>::try_from(val).unwrap();
                let arg = serde_v8::to_v8(scope, ev_json).unwrap();
                let call_result = func.call(scope, global.into(), &[arg]).unwrap();
                serde_v8::from_v8(scope, call_result).unwrap()
            };
            let arr = result_val.as_array().cloned()
                .unwrap_or_else(|| vec![serde_json::to_value(event).unwrap()]);
            let mut out = Vec::with_capacity(arr.len());
            for v in arr {
                out.push(serde_json::from_value::<Event>(v).expect("valid event from JS"));
            }
            out
        }
    }
}

// -------- Bench cases --------

fn bench_js_current_small(c: &mut Criterion) {
    let mut group = c.benchmark_group("js_current_small");
    group.throughput(Throughput::Elements(1));

    let js = r#"(event) => ({ ...event, after: { ...event.after, qty: (event.after.qty||0)+1 } })"#.to_string();
    let proc = JsProcessor::new(js);
    let ev = make_small_event();

    group.bench_function("current_js_runtime_per_event", |b| {
        b.iter_custom(|iters| {
            RT.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    let _ = black_box(proc.process(ev.clone()).await).unwrap();
                }
                start.elapsed()
            })
        })
    });

    group.finish();
}

fn bench_js_current_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("js_current_large_64k");
    group.throughput(Throughput::Bytes(64 * 1024));
    let js = "(event)=>event".to_string(); // identity
    let proc = JsProcessor::new(js);
    let ev = make_large_event(64 * 1024);

    group.bench_function("current_js_runtime_per_event", |b| {
        b.iter_custom(|iters| {
            RT.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    let _ = black_box(proc.process(ev.clone()).await).unwrap();
                }
                start.elapsed()
            })
        })
    });
    group.finish();
}

fn bench_js_array_return(c: &mut Criterion) {
    let mut group = c.benchmark_group("js_array_return");
    group.throughput(Throughput::Elements(2)); // returns two
    let js = r#"(event)=>[{...event,tags:["a"]},{...event,tags:["b"]}]"#.to_string();
    let proc = JsProcessor::new(js);
    let ev = make_small_event();

    group.bench_function("current_js_runtime_per_event", |b| {
        b.iter_custom(|iters| {
            RT.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    let v = black_box(proc.process(ev.clone()).await).unwrap();
                    assert_eq!(v.len(), 2);
                }
                start.elapsed()
            })
        })
    });
    group.finish();
}

fn bench_rust_noop_baseline(c: &mut Criterion) {
    let mut group = c.benchmark_group("rust_noop_baseline");
    group.throughput(Throughput::Elements(1));
    let proc = RustNoop;
    let ev = make_small_event();

    group.bench_function("rust_noop", |b| {
        b.iter_custom(|iters| {
            RT.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    let _ = black_box(proc.process(ev.clone()).await).unwrap();
                }
                start.elapsed()
            })
        })
    });
    group.finish();
}

fn bench_pooled_js_small(c: &mut Criterion) {
    use pooled::PooledJs;

    let mut group = c.benchmark_group("pooled_js_small");
    group.throughput(Throughput::Elements(1));

    let js = r#"(event) => ({ ...event, after: { ...event.after, qty: (event.after.qty||0)+1 } })"#.to_string();
    let pooled = PooledJs::new(js);
    let ev = make_small_event();

    group.bench_function("pooled_single_runtime", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                let _ = black_box(pooled.call(&ev));
            }
            start.elapsed()
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_rust_noop_baseline,
    bench_js_current_small,
    bench_js_array_return,
    bench_js_current_large,
    bench_pooled_js_small,
);
criterion_main!(benches);

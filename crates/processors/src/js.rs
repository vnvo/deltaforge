//! JavaScript processor using Deno runtime.
//!
//! # Number Type Limitations
//!
//! JavaScript represents all numbers as IEEE 754 double-precision floats (f64).
//! When events pass through the JS processor, integer fields undergo conversion:
//!
//! ```text
//! Rust i64 → serde_v8 → V8 Number (f64) → serde_v8 → Rust f64
//! ```
//!
//! **This affects ALL numeric values**, even if the JS code doesn't modify them.
//! Simply returning the input array causes integer→float conversion because
//! V8 stores all numbers as f64 internally.
//!
//! ## Event Struct Fields
//!
//! Event metadata fields (`ts_ms`, `size_bytes`, etc.) use lenient deserialization
//! that accepts both integers and whole-number floats, so they round-trip correctly.
//!
//! ## Payload Fields (before/after)
//!
//! Payload fields are NOT normalized to preserve user data schema. This means:
//! - Integer values may become floats: `{"id": 1}` → `{"id": 1.0}`
//! - Precision loss for large integers (>2^53-1): `9007199254740993` → `9007199254740992.0`
//! - Original float values are preserved: `{"price": 10.5}` → `{"price": 10.5}`
//!
//! ## Recommendations
//!
//! - Use JS processor for routing, filtering, and metadata-based transforms
//! - Avoid modifying numeric payload fields when downstream systems require strict types
//! - For tables with BIGINT primary keys, consider filtering those events from JS processing
//! - Use native Rust processors when numeric precision is critical

use std::thread;

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use deltaforge_core::{Event, Processor};
use deno_core::{JsRuntime, RuntimeOptions, extension};
use deno_core::{serde_v8, v8};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info};

extension!(df_ext, ops = [op_log],);

#[deno_core::op2(fast)]
fn op_log(#[string] msg: &str) {
    println!("[js] {msg}");
}

type JsJob = (Vec<Event>, oneshot::Sender<Result<Vec<Event>>>);

pub struct JsProcessor {
    id: String,
    tx: mpsc::Sender<JsJob>,
    worker_handle: Option<std::thread::JoinHandle<()>>,
}

impl JsProcessor {
    /// Create a new JS processor.
    ///
    /// `inline` is full JS source that must define a global function:
    /// ```javascript
    /// function processBatch(events) {
    ///     // Process events and return array, single event, or null (use mutated input)
    ///     return events;
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - JS syntax is invalid
    /// - `processBatch` function is not defined
    /// - Thread spawn fails
    pub fn new(id: String, inline: String) -> Result<Self> {
        // Channel to send jobs to JS thread
        let (tx, mut rx) = mpsc::channel::<JsJob>(1024);

        // Clone things needed in the thread
        let id_clone = id.clone();
        let script = inline.clone();

        let worker_handle = thread::Builder::new()
            .name(format!("df-js-{}", id_clone))
            .spawn(move || {
                if let Err(e) = js_worker_thread(id_clone, script, &mut rx) {
                    error!(error=?e, "js worker thread crashed");
                }
            })
            .context("spawn js processor thread")?;

        // Give thread time to initialize and validate script
        std::thread::sleep(std::time::Duration::from_millis(10));

        Ok(Self {
            id,
            tx,
            worker_handle: Some(worker_handle),
        })
    }

    pub fn is_alive(&self) -> bool {
        self.worker_handle
            .as_ref()
            .map(|h| !h.is_finished())
            .unwrap_or(false)
    }
}

fn js_worker_thread(
    id: String,
    script: String,
    rx: &mut mpsc::Receiver<JsJob>,
) -> Result<()> {
    let ext = df_ext::init();
    let mut rt = JsRuntime::new(RuntimeOptions {
        extensions: vec![ext],
        ..Default::default()
    });

    // Execute user script once
    rt.execute_script("df_processor.js", script.clone())
        .context("failed to execute JS processor script")?;

    // Sanity-check presence of processBatch
    {
        let scope = &mut rt.handle_scope();
        let ctx = scope.get_current_context();
        let global = ctx.global(scope);
        let key = v8::String::new(scope, "processBatch")
            .ok_or_else(|| anyhow!("failed to allocate v8 string"))?;
        let val = global
            .get(scope, key.into())
            .ok_or_else(|| anyhow!("global 'processBatch' not found"))?;
        if !val.is_function() {
            bail!("global 'processBatch' is not a function");
        }
    }

    info!(processor_id=%id, "JS processor runtime initialized");

    // Main loop: handle jobs
    while let Some((events, reply_tx)) = rx.blocking_recv() {
        let start = std::time::Instant::now();
        let input_len = events.len();
        let res = process_batch_in_runtime(&mut rt, &id, events);
        if let Err(ref e) = res {
            error!(processor_id=%id, error=%e, "JS batch processing failed");
        }

        match &res {
            Ok(out) => {
                debug!(
                    processor_id=%id,
                    events_count=input_len,
                    out_count=out.len(),
                    elapsed_us=start.elapsed().as_micros(),
                    "JS batch processed"
                );
            }
            Err(e) => {
                error!(
                    processor_id=%id,
                    error=%e,
                    elapsed_us=start.elapsed().as_micros(),
                    "JS processing failure"
                );
            }
        }

        let _ = reply_tx.send(res);
    }

    info!(processor_id=%id, "JS processor worker exiting");
    Ok(())
}

/// Runs one batch inside the JsRuntime (single-threaded).
///
/// Note: All numeric values pass through V8's f64 representation.
/// Event struct fields are normalized back to i64, but payload fields
/// (before/after) retain their JS-converted types. See module docs.
fn process_batch_in_runtime(
    rt: &mut JsRuntime,
    id: &str,
    events: Vec<Event>,
) -> Result<Vec<Event>> {
    debug!(processor_id=%id, in_len=events.len(), "JS processing batch");

    let json_events =
        serde_json::to_value(&events).context("serialize events for JS")?;

    let scope = &mut rt.handle_scope();
    let ctx = scope.get_current_context();
    let global = ctx.global(scope);

    let name = v8::String::new(scope, "processBatch")
        .ok_or_else(|| anyhow!("failed to allocate v8 string"))?;
    let val = global
        .get(scope, name.into())
        .ok_or_else(|| anyhow!("global 'processBatch' not found"))?;
    let func = v8::Local::<v8::Function>::try_from(val)
        .ok()
        .ok_or_else(|| anyhow!("'processBatch' is not a function"))?;

    let arg = serde_v8::to_v8(scope, json_events)
        .context("failed to convert events JSON to v8 value")?;

    let mut try_catch = v8::TryCatch::new(scope);
    let call_res = func.call(&mut try_catch, global.into(), &[arg]);

    if call_res.is_none() {
        let exc = try_catch.exception().unwrap();
        let msg = v8::Exception::create_message(&mut try_catch, exc)
            .get(&mut try_catch)
            .to_rust_string_lossy(&mut try_catch);
        error!(processor_id=%id, msg=%msg, "JS exception");
        bail!("JS processor threw: {msg}");
    }

    let result = call_res.unwrap();

    // undefined / null -> use mutated input array
    if result.is_null_or_undefined() {
        let mutated_val: Value = serde_v8::from_v8(&mut try_catch, arg)
            .context("failed to read mutated JS events arg")?;
        return deserialize_events_lenient(mutated_val);
    }

    // Interpret return value
    let ret_json: Value = serde_v8::from_v8(&mut try_catch, result)
        .context("failed to convert JS return value to JSON")?;

    match ret_json {
        Value::Array(_) => deserialize_events_lenient(ret_json),
        Value::Object(_) => {
            let events =
                deserialize_events_lenient(Value::Array(vec![ret_json]))?;
            Ok(events)
        }
        other => {
            bail!(
                "JS processor returned unsupported type: {} (expected array, object, or null)",
                value_type_name(&other)
            );
        }
    }
}

/// Deserialize events with lenient number handling.
///
/// Event struct fields (ts_ms, size_bytes, etc.) accept both i64 and f64,
/// converting whole-number floats back to integers. Payload fields (before/after)
/// are left unchanged to preserve user data schema.
fn deserialize_events_lenient(mut val: Value) -> Result<Vec<Event>> {
    // Normalize only Event struct fields, not payloads
    if let Value::Array(ref mut arr) = val {
        for event in arr.iter_mut() {
            if let Value::Object(obj) = event {
                normalize_event_fields(obj);
            }
        }
    }

    serde_json::from_value(val).context("failed to decode events from JS")
}

/// Normalize numeric fields in Event struct (not payloads).
///
/// Converts whole-number floats back to integers for known Event fields.
/// Leaves `before` and `after` payloads untouched.
fn normalize_event_fields(obj: &mut serde_json::Map<String, Value>) {
    // Top-level Event fields that should be i64
    for key in ["ts_ms", "size_bytes"] {
        if let Some(val) = obj.get_mut(key) {
            normalize_to_i64(val);
        }
    }

    // Nested source.ts_ms
    if let Some(Value::Object(source)) = obj.get_mut("source") {
        if let Some(val) = source.get_mut("ts_ms") {
            normalize_to_i64(val);
        }
        // source.position fields if any are numeric
        if let Some(Value::Object(pos)) = source.get_mut("position") {
            for val in pos.values_mut() {
                normalize_to_i64(val);
            }
        }
    }

    // transaction.total_order, transaction.data_collection_order
    if let Some(Value::Object(tx)) = obj.get_mut("transaction") {
        for key in ["total_order", "data_collection_order"] {
            if let Some(val) = tx.get_mut(key) {
                normalize_to_i64(val);
            }
        }
    }

    // Note: `before` and `after` are intentionally NOT normalized
    // to preserve user data schema
}

/// Convert a whole-number float to i64.
fn normalize_to_i64(val: &mut Value) {
    if let Value::Number(n) = val {
        if let Some(f) = n.as_f64() {
            if f.fract() == 0.0 && f.abs() <= 9_007_199_254_740_991.0 {
                *val = Value::Number(serde_json::Number::from(f as i64));
            }
        }
    }
}

fn value_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[async_trait]
impl Processor for JsProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    async fn process(&self, events: Vec<Event>) -> Result<Vec<Event>> {
        if !self.is_alive() {
            bail!("JS processor worker has crashed");
        }

        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send((events, reply_tx))
            .await
            .context("send batch to JS worker failed")?;

        tokio::time::timeout(std::time::Duration::from_secs(5), reply_rx)
            .await
            .context("JS processor timed out after 5s")?
            .context("JS worker dropped reply channel")?
    }
}

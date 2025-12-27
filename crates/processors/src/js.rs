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
    /// `inline` is full JS source.
    /// It must define a global function:
    ///   function processBatch(events) { ... }
    pub fn new(id: String, inline: String) -> Result<Self> {
        // Channel to send jobs to JS thread
        let (tx, mut rx) = mpsc::channel::<JsJob>(1024);

        // Clone things needed in the thread
        let id_clone = id.clone();
        // Move script into thread
        let script = inline.clone();

        let worker_handle = thread::Builder::new()
            .name(format!("df-js-{}", id_clone))
            .spawn(move || {
                if let Err(e) = js_worker_thread(id_clone, script, &mut rx) {
                    error!(error=?e, "js worker thread crashed");
                }
            })
            .context("spawn js processor thread")?;

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
                    "JS processing failre"
                );
            }
        }

        let _ = reply_tx.send(res);
    }

    info!(processor_id=%id, "JS processor worker exiting");
    Ok(())
}

/// Runs one batch inside the JsRuntime (single-threaded)
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
        bail!("JS processor can not continue");
    }

    let result = call_res.unwrap();

    // undefined / null -> use mutated input array
    if result.is_null_or_undefined() {
        let mutated_val: Value = serde_v8::from_v8(&mut try_catch, arg)
            .context("failed to read mutated JS events arg")?;
        let out: Vec<Event> = serde_json::from_value(mutated_val)
            .context("failed to decode mutated events from JS")?;
        return Ok(out);
    }

    // Interpret return value
    let ret_json: Value = serde_v8::from_v8(&mut try_catch, result)
        .context("failed to convert JS return value to JSON")?;

    let out_events = match ret_json {
        Value::Array(_) => {
            let out: Vec<Event> = serde_json::from_value(ret_json)
                .context("failed to decode events array from JS")?;
            out
        }
        Value::Object(_) => {
            let ev: Event = serde_json::from_value(ret_json)
                .context("failed to decode single event from JS object")?;
            vec![ev]
        }
        other => {
            bail!(
                "js processor returned unsupported type: {other:?} (expected array, object, or null)"
            );
        }
    };

    debug!(processor_id=%id, out_len=out_events.len(), "JS returned batch");
    Ok(out_events)
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
        // send job to worker thread
        self.tx
            .send((events, reply_tx))
            .await
            .context("send batch to JS worker failed")?;

        tokio::time::timeout(std::time::Duration::from_secs(5), reply_rx)
            .await
            .context("JS processor timed out after 5s")?
            .context("JS worker dropped reply channel")?

        // await result
        //reply_rx.await.context("JS worker dropped reply channel")?
    }
}

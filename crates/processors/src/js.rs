//! JavaScript processor using Deno runtime.
//!
//! # Values at the JavaScript boundary
//!
//! Scripts see each event's native rendering for its own mapping version,
//! with every 64-bit integer as its exact decimal string (JavaScript numbers
//! are doubles). Column types never enter the script: each output's rows are
//! re-typed against the input event it retains or derives from (see
//! [`crate::js_boundary`]). An unchanged value comes back exactly; a changed
//! value parses exactly as its column's type or fails the batch; a new
//! column is an untyped document.

use std::sync::Arc;
use std::thread;

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use deltaforge_core::{
    BatchContext, Event, EventId, EventRouting, Op, Processor, SourceInfo,
    Transaction,
};
use serde::Deserialize;

use crate::js_boundary::{Authority, JsInput, SchemaCtx, from_js_row, to_js};
use deltaforge_core::canonical::schema::SchemaRef;
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

/// JavaScript preamble injected before user script.
///
/// Provides `ev.route({topic, key, headers})` for per-event routing control.
/// - `ev.route(opts)`: set routing on an existing event (method on each input event)
/// - `route(ev, opts)`: global function for cloned/injected events that lack .route()
///
/// Routing is stored as `ev.__routing` and extracted by Rust after processBatch returns.
/// The `__df_setup` function is called by Rust before each processBatch invocation.
const JS_PREAMBLE: &str = r#"
function route(ev, opts) { ev.__routing = opts; }
// Declare explicit lineage for a newly-created (derived) event. `parent` is an
// input event (or its id string); `output` is the new event object. The Rust
// bridge reads `__df_parent`, mints a stable synthetic id, and strips it.
function derive(parent, output) {
    output.__df_parent =
        (parent && typeof parent === 'object') ? parent.__df_id : parent;
    return output;
}
function __df_setup(events, existingRouting) {
    for (let i = 0; i < events.length; i++) {
        const ev = events[i];
        if (existingRouting[i]) ev.__routing = existingRouting[i];
        ev.route = (opts) => { ev.__routing = opts; };
    }
}
"#;

/// Reserved JS field carrying an input event's stable id (injected by the
/// bridge, stripped before the output becomes a Rust `Event`).
pub(crate) const DF_ID: &str = "__df_id";
/// Reserved JS field set by `derive(parent, output)` naming the parent's id.
const DF_PARENT: &str = "__df_parent";

/// A batch, each event with its schema context, and where to reply.
type JsJob = (Vec<(Event, SchemaCtx)>, oneshot::Sender<Result<Vec<Event>>>);

pub struct JsProcessor {
    id: String,
    tx: mpsc::Sender<JsJob>,
    worker_handle: Option<std::thread::JoinHandle<()>>,
    /// Stable identity digest over the exact source bytes + canonical limits.
    digest: String,
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
    pub fn new(
        id: String,
        inline: String,
        limits: Option<deltaforge_config::Limits>,
    ) -> Result<Self> {
        // Identity digest over the EXACT source bytes + canonical limits.
        let digest = crate::digest::js_digest(&inline, &limits);

        // Channel to send jobs to JS thread
        let (tx, mut rx) = mpsc::channel::<JsJob>(1024);

        // Clone things needed in the thread
        let id_clone = id.clone();
        let script = inline.clone();
        let digest_clone = digest.clone();

        let worker_handle = thread::Builder::new()
            .name(format!("df-js-{}", id_clone))
            .spawn(move || {
                if let Err(e) =
                    js_worker_thread(id_clone, script, digest_clone, &mut rx)
                {
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
            digest,
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
    digest: String,
    rx: &mut mpsc::Receiver<JsJob>,
) -> Result<()> {
    let ext = df_ext::init();
    let mut rt = JsRuntime::new(RuntimeOptions {
        extensions: vec![ext],
        ..Default::default()
    });

    // Execute routing preamble, then user script
    rt.execute_script("df_preamble.js", JS_PREAMBLE.to_string())
        .context("failed to execute JS preamble")?;

    // Execute user provided script once
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
        let res = process_batch_in_runtime(&mut rt, &id, &digest, events);
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
/// Flow:
/// 1. Render each event for the script (see [`crate::js_boundary`])
/// 2. Collect existing routing (it is not part of the rendering)
/// 3. Call __df_setup(events, existingRouting) to inject .route() methods
/// 4. Call processBatch(events)
/// 5. Re-type each output's rows against its input and schema context,
///    and reattach its `__routing`
fn process_batch_in_runtime(
    rt: &mut JsRuntime,
    id: &str,
    digest: &str,
    batch: Vec<(Event, SchemaCtx)>,
) -> Result<Vec<Event>> {
    debug!(processor_id=%id, in_len=batch.len(), "JS processing batch");
    let (events, schemas): (Vec<Event>, Vec<SchemaCtx>) =
        batch.into_iter().unzip();

    // Collect existing routing (it is not part of the rendering)
    let existing_routing: Vec<Value> = events
        .iter()
        .map(|e| {
            e.routing
                .as_ref()
                .and_then(|r| serde_json::to_value(r).ok())
                .unwrap_or(Value::Null)
        })
        .collect();

    // The stable provisional ids of the input batch — the only valid parents
    // for derive() and the retained-id set for 1:1 transforms.
    let input_ids: std::collections::HashSet<EventId> =
        events.iter().filter_map(|e| e.event_id).collect();

    // Each input's typed rows and their renderings for the script, kept (by
    // id) to re-type the outputs with.
    let typed: Vec<Authority> = events
        .iter()
        .zip(schemas)
        .map(|(ev, schema)| to_js(ev, schema))
        .collect::<Result<_>>()?;

    let scope = &mut rt.handle_scope();
    let ctx = scope.get_current_context();
    let global = ctx.global(scope);

    // --- Call __df_setup(events, existingRouting) ---
    // Each input as the script sees it, written straight into V8; with its
    // stable id as `__df_id`, so scripts can pass it to derive() and so
    // retained ids are visible.
    let inputs: Vec<JsInput<'_>> = events
        .iter()
        .zip(&typed)
        .map(|(event, authority)| JsInput { event, authority })
        .collect();
    let arg = serde_v8::to_v8(scope, &inputs)
        .context("failed to convert events to v8")?;
    drop(inputs);
    let mut authorities: std::collections::HashMap<EventId, Authority> =
        std::collections::HashMap::with_capacity(typed.len());
    for (ev, authority) in events.iter().zip(typed) {
        if let Some(pid) = ev.event_id {
            authorities.insert(pid, authority);
        }
    }

    let routing_arg = serde_v8::to_v8(scope, Value::Array(existing_routing))
        .context("failed to convert routing to v8")?;

    {
        let setup_name = v8::String::new(scope, "__df_setup")
            .ok_or_else(|| anyhow!("failed to allocate v8 string"))?;
        let setup_val = global
            .get(scope, setup_name.into())
            .ok_or_else(|| anyhow!("__df_setup not found"))?;
        let setup_func = v8::Local::<v8::Function>::try_from(setup_val)
            .ok()
            .ok_or_else(|| anyhow!("__df_setup is not a function"))?;

        let mut try_catch = v8::TryCatch::new(scope);
        let setup_res =
            setup_func.call(&mut try_catch, global.into(), &[arg, routing_arg]);
        if setup_res.is_none() {
            let exc = try_catch.exception().unwrap();
            let msg = v8::Exception::create_message(&mut try_catch, exc)
                .get(&mut try_catch)
                .to_rust_string_lossy(&mut try_catch);
            bail!("__df_setup failed: {msg}");
        }
    }

    // --- Call processBatch(events) ---
    // `arg` is the same v8 value, now mutated by __df_setup (has .route() methods)
    let name = v8::String::new(scope, "processBatch")
        .ok_or_else(|| anyhow!("failed to allocate v8 string"))?;
    let val = global
        .get(scope, name.into())
        .ok_or_else(|| anyhow!("global 'processBatch' not found"))?;
    let func = v8::Local::<v8::Function>::try_from(val)
        .ok()
        .ok_or_else(|| anyhow!("'processBatch' is not a function"))?;

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
        return finalize_output(mutated_val, &input_ids, &authorities, digest);
    }

    // Interpret return value
    let ret_json: Value = serde_v8::from_v8(&mut try_catch, result)
        .context("failed to convert JS return value to JSON")?;

    match ret_json {
        Value::Array(_) => {
            finalize_output(ret_json, &input_ids, &authorities, digest)
        }
        Value::Object(_) => finalize_output(
            Value::Array(vec![ret_json]),
            &input_ids,
            &authorities,
            digest,
        ),
        other => {
            bail!(
                "JS processor returned unsupported type: {} (expected array, object, or null)",
                value_type_name(&other)
            );
        }
    }
}

/// Resolve explicit lineage on each output, deserialize, and stamp the
/// provisional stable id. Fails closed on missing / cross-batch / duplicate
/// parentage.
fn finalize_output(
    mut val: Value,
    input_ids: &std::collections::HashSet<EventId>,
    authorities: &std::collections::HashMap<EventId, Authority>,
    digest: &str,
) -> Result<Vec<Event>> {
    let lineage = resolve_output_lineage(&mut val, input_ids, digest)?;
    let routings = extract_routing(&mut val);
    let Value::Array(outputs) = val else {
        bail!("JS processor output must be an array of events");
    };
    let mut events = Vec::with_capacity(outputs.len());
    for (i, out) in outputs.into_iter().enumerate() {
        let (id, authority) = lineage.get(i).copied().unwrap_or((None, None));
        let mut event =
            event_from_js(out, authority.and_then(|a| authorities.get(&a)))?;
        event.event_id = id;
        if let Some(Some(routing)) = routings.get(i) {
            event.routing = Some(routing.clone());
        }
        events.push(event);
    }
    Ok(events)
}

/// The metadata of an event a script returned (its rows are re-typed
/// separately).
#[derive(Deserialize)]
struct JsEventMeta {
    source: SourceInfo,
    op: Op,
    ts_ms: i64,
    #[serde(default)]
    transaction: Option<Transaction>,
    #[serde(default)]
    tenant_id: Option<String>,
    #[serde(default)]
    schema_version: Option<String>,
    #[serde(default)]
    schema_sequence: Option<u64>,
    #[serde(default)]
    ddl: Option<Value>,
    #[serde(default)]
    trace_id: Option<String>,
    #[serde(default)]
    tags: Option<Vec<String>>,
    #[serde(default)]
    synthetic: Option<String>,
    #[serde(default)]
    tx_end: bool,
}

/// An event from a script's output object, its rows re-typed against
/// `authority` (the input it retains or derives from).
fn event_from_js(out: Value, authority: Option<&Authority>) -> Result<Event> {
    let Value::Object(mut obj) = out else {
        bail!("JS processor output must be objects");
    };
    normalize_event_fields(&mut obj);
    let row =
        |v: Option<Value>| -> Result<Option<serde_json::Map<String, Value>>> {
            match v {
                None | Some(Value::Null) => Ok(None),
                Some(Value::Object(m)) => Ok(Some(m)),
                Some(_) => bail!("a row image must be an object or null"),
            }
        };
    let before = row(obj.remove("before"))?;
    let after = row(obj.remove("after"))?;
    obj.remove("unchanged_columns");
    obj.remove("event_id");
    // The mapping version is the input's: a script cannot change how values
    // are typed.
    if let Some(Value::Object(src)) = obj.get_mut("source") {
        src.remove("type_mapping");
    }
    let meta: JsEventMeta = serde_json::from_value(Value::Object(obj))
        .context("failed to decode event metadata from JS")?;
    let mapping = authority
        .map(|a| a.mapping)
        .unwrap_or(deltaforge_core::TypeMapping::V2);
    // An output without an input (an id-less legacy batch) names no schema.
    let no_schema = SchemaCtx::NoReference;
    let schema = authority.map_or(&no_schema, |a| &a.schema);
    let before = before
        .map(|m| {
            from_js_row(
                "before",
                authority.and_then(|a| a.before.as_ref()),
                schema,
                m,
                mapping,
            )
        })
        .transpose()?;
    let after = after
        .map(|m| {
            from_js_row(
                "after",
                authority.and_then(|a| a.after.as_ref()),
                schema,
                m,
                mapping,
            )
        })
        .transpose()?;
    Ok(Event {
        before,
        after,
        source: meta.source,
        op: meta.op,
        ts_ms: meta.ts_ms,
        transaction: meta.transaction,
        type_mapping: mapping,
        event_id: None,
        tenant_id: meta.tenant_id,
        schema_version: meta.schema_version,
        schema_sequence: meta.schema_sequence,
        ddl: meta.ddl,
        trace_id: meta.trace_id,
        tags: meta.tags,
        synthetic: meta.synthetic,
        routing: None,
        tx_end: meta.tx_end,
        boundary: None,
        size_bytes: 0,
        received_at_ms: 0,
    })
}

/// For each output object: strip the reserved lineage fields and compute its
/// provisional [`EventId`]. A `__df_parent` marks a derived (synthetic) output;
/// otherwise the output must retain an input's `__df_id` (a 1:1 transform).
/// Missing lineage, an unknown/cross-batch parent, or a parent id retained by
/// more than one output are all errors — never guessed.
#[allow(clippy::type_complexity)]
fn resolve_output_lineage(
    val: &mut Value,
    input_ids: &std::collections::HashSet<EventId>,
    digest: &str,
) -> Result<Vec<(Option<EventId>, Option<EventId>)>> {
    let arr = match val {
        Value::Array(a) => a,
        _ => return Ok(vec![]),
    };
    // The lineage contract binds only when the input batch actually carries
    // stable ids. Real source events always do (post cutover, unconditionally);
    // an entirely id-less batch predates identity and is passed through as-is.
    if input_ids.is_empty() {
        for out in arr.iter_mut() {
            if let Value::Object(obj) = out {
                obj.remove(DF_PARENT);
                obj.remove(DF_ID);
            }
        }
        return Ok(vec![(None, None); arr.len()]);
    }
    let mut ids = Vec::with_capacity(arr.len());
    let mut retained: std::collections::HashSet<EventId> =
        std::collections::HashSet::new();
    let mut ordinals: std::collections::HashMap<EventId, u32> =
        std::collections::HashMap::new();

    for out in arr.iter_mut() {
        let obj = match out {
            Value::Object(o) => o,
            _ => bail!("JS processor output must be objects"),
        };
        let parent = obj.remove(DF_PARENT);
        let retained_id = obj.remove(DF_ID);

        let id = match parent {
            // Derived output: mint a synthetic id from the declared parent.
            Some(p) => {
                let ps = p.as_str().ok_or_else(|| {
                    anyhow!("derive() parent id must be a string")
                })?;
                let parent_id: EventId = ps.parse().map_err(|e| {
                    anyhow!("invalid derive() parent id {ps:?}: {e}")
                })?;
                if !input_ids.contains(&parent_id) {
                    bail!(
                        "derive() parent {ps} is not an input event of this batch"
                    );
                }
                let ord = ordinals.entry(parent_id).or_insert(0);
                let id = EventId::synthetic(&parent_id, digest, *ord);
                *ord += 1;
                (id, parent_id)
            }
            // 1:1 transform: the output must retain exactly one input id.
            None => {
                let rs = retained_id
                    .as_ref()
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        anyhow!(
                            "JS processor produced a new event without lineage; \
                             use derive(parent, output) to create derived events"
                        )
                    })?;
                let rid: EventId = rs.parse().map_err(|e| {
                    anyhow!("invalid retained event id {rs:?}: {e}")
                })?;
                if !input_ids.contains(&rid) {
                    bail!(
                        "retained event id {rs} is not an input event of this batch"
                    );
                }
                if !retained.insert(rid) {
                    bail!(
                        "event id {rs} is retained by more than one output; \
                         fan-out must derive() its additional outputs"
                    );
                }
                (rid, rid)
            }
        };
        ids.push((Some(id.0), Some(id.1)));
    }
    Ok(ids)
}

/// Extract and remove `__routing` from each event JSON object.
///
/// Also removes the `route` function property (non-serializable, but serde_v8
/// may produce a null or object for it).
fn extract_routing(val: &mut Value) -> Vec<Option<EventRouting>> {
    let mut routings = Vec::new();

    if let Value::Array(arr) = val {
        for event_json in arr.iter_mut() {
            if let Value::Object(obj) = event_json {
                // Remove __routing and the .route function stub
                let routing = obj.remove("__routing").and_then(|v| {
                    serde_json::from_value::<EventRouting>(v).ok()
                });
                obj.remove("route");
                routings.push(routing);
            } else {
                routings.push(None);
            }
        }
    }

    routings
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

    fn identity_digest(&self) -> &str {
        &self.digest
    }

    async fn process(
        &self,
        events: Vec<Event>,
        ctx: &BatchContext,
    ) -> Result<Vec<Event>> {
        if !self.is_alive() {
            bail!("JS processor worker has crashed");
        }

        // The batch's schema versions were resolved and validated before
        // any processor ran; each event's types are looked up once per
        // distinct version and shared.
        let mut resolved: std::collections::HashMap<SchemaRef, SchemaCtx> =
            std::collections::HashMap::new();
        let mut batch = Vec::with_capacity(events.len());
        // Consecutive events usually name one version: the last reference is
        // compared in place instead of being built and looked up again.
        let mut last: Option<(SchemaRef, SchemaCtx)> = None;
        for ev in events {
            let schema = match &last {
                Some((r, s)) if r.is_of(&ev) => s.clone(),
                _ => match SchemaRef::of(&ev) {
                    None => SchemaCtx::NoReference,
                    Some(r) => {
                        let s = match resolved.get(&r) {
                            Some(s) => s.clone(),
                            None => {
                                let s = match ctx.schemas.as_ref().map(|s| s.get(&r)) {
                                    None => SchemaCtx::Missing(
                                        "the pipeline provides no schema context".into(),
                                    ),
                                    Some(None) => SchemaCtx::Missing(
                                        "the batch did not resolve the event's schema version".into(),
                                    ),
                                    Some(Some(types)) => SchemaCtx::Resolved(Arc::clone(types)),
                                };
                                resolved.insert(r.clone(), s.clone());
                                s
                            }
                        };
                        last = Some((r, s.clone()));
                        s
                    }
                },
            };
            batch.push((ev, schema));
        }

        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send((batch, reply_tx))
            .await
            .context("send batch to JS worker failed")?;

        tokio::time::timeout(std::time::Duration::from_secs(5), reply_rx)
            .await
            .context("JS processor timed out after 5s")?
            .context("JS worker dropped reply channel")?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A document (a column and an extension, objects nested in arrays at
    /// several levels) renders the same bytes live, in the stored durable
    /// form and after replay, here where `serde_json` keeps insertion order.
    #[test]
    fn documents_render_identically_live_stored_and_replayed() {
        use deltaforge_core::canonical::{
            CanonicalRow, CanonicalValue, Document,
        };
        let doc = || {
            Document::new(
                serde_json::from_str(
                    r#"{"z":[{"y":{"b":[{"d":1,"c":2}],"a":0}}],"a":[[{"q":1,"p":2}]]}"#,
                )
                .unwrap(),
            )
        };
        let row = CanonicalRow::with_extensions(
            vec![
                (
                    "id".into(),
                    CanonicalValue::Db(
                        deltaforge_core::canonical::DbValue::int(
                            7,
                            deltaforge_core::canonical::IntWidth::W64,
                        )
                        .unwrap(),
                    ),
                ),
                ("payload".into(), CanonicalValue::Document(doc())),
            ],
            [("added".to_string(), doc())],
        )
        .unwrap();
        let live = deltaforge_core::Event::new_row(
            deltaforge_core::EventId::mysql_row_server(1, "t", 1, 1),
            deltaforge_core::SourceInfo {
                version: "1".into(),
                connector: "mysql".into(),
                name: "p".into(),
                ts_ms: 0,
                db: "d".into(),
                schema: None,
                table: "t".into(),
                snapshot: None,
                position: Default::default(),
            },
            deltaforge_core::Op::Create,
            None,
            Some(row),
            deltaforge_core::TypeMapping::V2,
            0,
            0,
        );
        let stored = serde_json::to_vec(&live.to_durable()).unwrap();
        let replayed = deltaforge_core::Event::from_durable(
            serde_json::from_slice(&stored).unwrap(),
        )
        .unwrap();
        let render = |e: &deltaforge_core::Event| {
            serde_json::to_vec(&e.native(Default::default()).unwrap()).unwrap()
        };
        assert_eq!(render(&live), render(&replayed));
        let expected = r#"{"a":[[{"p":2,"q":1}]],"z":[{"y":{"a":0,"b":[{"c":2,"d":1}]}}]}"#;
        let live_json = String::from_utf8(render(&live)).unwrap();
        assert!(live_json.contains(expected), "{live_json}");
        assert_eq!(
            serde_json::to_vec(&replayed.to_durable()).unwrap(),
            stored,
            "the stored form is a fixed point"
        );
    }

    /// This crate's JavaScript engine makes `serde_json` keep insertion
    /// order; a document still has one key order (sorted), so it renders the
    /// same live and after the durable round trip.
    #[test]
    fn documents_have_one_key_order_whatever_the_insertion() {
        let inserted: serde_json::Value = serde_json::from_str(
            r#"{"z":1,"a":{"y":[{"q":1,"b":2}],"b":true}}"#,
        )
        .unwrap();
        let d = deltaforge_core::canonical::Document::new(inserted);
        assert_eq!(
            serde_json::to_string(d.value()).unwrap(),
            r#"{"a":{"b":true,"y":[{"b":2,"q":1}]},"z":1}"#
        );
    }

    #[test]
    fn normalize_to_i64_converts_only_whole_in_range_numbers() {
        // Whole-number float → i64.
        let mut whole = json!(42.0);
        assert!(whole.is_f64());
        normalize_to_i64(&mut whole);
        assert!(whole.is_i64(), "whole float must become i64");
        assert_eq!(whole.as_i64(), Some(42));

        // Fractional float must be left untouched — pins the `&&` (a `||`
        // mutation would truncate 1.5 to 1).
        let mut frac = json!(1.5);
        normalize_to_i64(&mut frac);
        assert!(frac.is_f64(), "fractional float must stay f64");
        assert_eq!(frac.as_f64(), Some(1.5));
    }
}

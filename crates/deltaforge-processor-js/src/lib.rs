use std::sync::Arc;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use deltaforge_config::{PipelineSpec, ProcessorCfg};
use deltaforge_core::{ArcDynProcessor, Event, Processor};
use deno_core::{extension, JsRuntime, RuntimeOptions};
use deno_core::{serde_v8, v8};
use serde_json::Value;
use tracing::{debug, info};

extension!(df_ext, ops = [op_log],);

#[deno_core::op2(fast)]
fn op_log(#[string] msg: &str) {
    println!("[js] {}", msg);
}

pub struct JsProcessor {
    source: String,
}

impl JsProcessor {
    pub fn new(source: String) -> Self {
        Self { source }
    }
}

#[async_trait]
impl Processor for JsProcessor {
    async fn process(&self, event: &mut Event) -> Result<Vec<Event>> {
        debug!("js processed event {}", event.event_id);

        // setup the runtime
        let ext = df_ext::init();
        let mut rt = JsRuntime::new(RuntimeOptions {
            extensions: vec![ext],
            ..Default::default()
        });

        // install user function as processEvent
        let code = format!(
            r#"
            (function () {{
                const userFn = ({source});
                if (typeof userFn !== "function") {{
                    throw new Error("DeltaForge JS processor: source must be a function, e.g. (event)=>event");
                }}
                globalThis.processEvent = userFn;
            }})();
            "#,
            source = self.source
        );
        rt.execute_script("df_bootstrap.js", code)?;

        // convert current event to JS value
        let ev_json = serde_json::to_value(&*event)
            .context("failed to serialize event for JS")?;

        let scope = &mut rt.handle_scope();
        let ctx = scope.get_current_context();
        let global = ctx.global(scope);

        let name = v8::String::new(scope, "processEvent")
            .context("failed to alloc v8 string")?;
        let val = global
            .get(scope, name.into())
            .context("processEvent is not defined")?;
        let func = v8::Local::<v8::Function>::try_from(val)
            .ok()
            .context("processEvent is not a function")?;

        let arg = serde_v8::to_v8(scope, ev_json)
            .context("failed to convert event to v8")?;

        // call JS
        let mut try_catch = v8::TryCatch::new(scope);
        let call_res = func.call(&mut try_catch, global.into(), &[arg]);
        if call_res.is_none() {
            let exc = try_catch.exception().expect("exception present");
            let message = v8::Exception::create_message(&mut try_catch, exc);
            let msg = message
                .get(&mut try_catch)
                .to_rust_string_lossy(&mut try_catch);
            let file = message
                .get_script_resource_name(&mut try_catch)
                .and_then(|v| v8::Local::<v8::String>::try_from(v).ok())
                .map(|s| s.to_rust_string_lossy(&mut try_catch))
                .unwrap_or_else(|| "<unknown>".to_string());
            let line =
                message.get_line_number(&mut try_catch).unwrap_or_default();
            bail!("js error at {file}:{line}: {msg}");
        }

        let result = call_res.unwrap();
        let result_val: Value = serde_v8::from_v8(&mut try_catch, result)
            .context("failed to convert JS result to JSON value")?;

        // interpret JS result:
        //    - undefined/null → JS mutated the arg in place: read arg back into `event`
        //    - object → replace current event with that object
        //    - array → first element (if any) replaces current event, the rest are "extra"
        //
        // re-read the possibly mutated arg:
        let mutated_val: Value = serde_v8::from_v8(&mut try_catch, arg)
            .context("failed to read mutated JS arg")?;

        // helper to convert JSON -> Event
        fn val_to_event(v: Value, ctx: &str) -> Result<Event> {
            Ok(serde_json::from_value::<Event>(v)
                .with_context(|| format!("invalid JS return in {ctx}: expected Event shape"))?)
        }

        let mut extra: Vec<Event> = Vec::new();

        match result_val {
            // JS returned nothing => just update our &mut Event with mutated arg
            Value::Null => {
                let new_ev = val_to_event(mutated_val, "mutated arg")?;
                *event = new_ev;
            }

            // JS returned single object => that's the new event
            Value::Object(_) => {
                let new_ev = val_to_event(result_val, "object return")?;
                *event = new_ev;
            }

            // JS returned array => first replaces current, rest are extras
            Value::Array(arr) => {
                if !arr.is_empty() {
                    // first
                    let first_ev = val_to_event(arr[0].clone(), "array[0]")?;
                    *event = first_ev;

                    // rest = extra
                    for (idx, item) in arr.into_iter().enumerate().skip(1) {
                        if item.is_null() {
                            continue;
                        }
                        let ev = val_to_event(item, &format!("array[{idx}]"))?;
                        extra.push(ev);
                    }
                } else {
                    // empty array => drop event? we'll just keep mutated arg
                    let new_ev = val_to_event(mutated_val, "empty array mutated arg")?;
                    *event = new_ev;
                }
            }

            _ => bail!("invalid JS return: expected object, array, or null"),
        }

        info!("js processed event");
        Ok(extra)
    }
}

pub fn build_processors(ps: &PipelineSpec) -> Arc<[ArcDynProcessor]> {
    ps.spec
        .processors
        .iter()
        .map(|p| match p {
            ProcessorCfg::Javascript { inline, .. } => {
                Arc::new(JsProcessor::new(inline.clone())) as ArcDynProcessor
            }
        })
        .collect::<Vec<_>>()
        .into()
}

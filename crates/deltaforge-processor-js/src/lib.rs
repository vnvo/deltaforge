use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use deltaforge_core::{Event, Processor};
use deno_core::{extension, JsRuntime, RuntimeOptions};
use deno_core::{serde_v8, v8};
use serde_json::Value;
use tracing::{debug, info};

extension!(df_ext, ops = [op_log],);

// Fast-call compatible op: &str + (fast)
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
    async fn process(&self, event: Event) -> Result<Vec<Event>> {
        debug!("js processed event {}", event.event_id);

        let ext = df_ext::init();
        let mut rt = JsRuntime::new(RuntimeOptions {
            extensions: vec![ext],
            ..Default::default()
        });

        // prepare the wrapper
        let code = format!(
            r#"
            globalThis.processEvent = (event) => {{
                const userFn = (event) => {{ {source} }};
                return userFn(event);
            }};
        "#,
            source = self.source
        );
        // todo: use the process-step id, script path or something like that as the name.
        rt.execute_script("df_bootstrap.js", code)?;

        // prepare call | todo: needs to become more efficient and robust
        let ev_json = serde_json::to_value(&event)?;
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

        // call with try-catch
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

        // convert the result to json; undefined -> Value::Null
        let result = call_res.unwrap();
        let result_val: Value = serde_v8::from_v8(&mut try_catch, result)
            .context("failed to convert JS result to JSON value")?;

        // if JS returned undefined/null, fall back to the (possibly) mutated argument.
        let effective_val: Value = if result_val.is_null() {
            serde_v8::from_v8(&mut try_catch, arg)
                .context("failed to read mutated event arg")?
        } else {
            result_val
        };

        // map effective_val to Vec<Event>
        let out: Vec<Event> = match effective_val {
            Value::Null => vec![event], // (unlikely now, but keep a sane default)
            Value::Object(_) => {
                vec![serde_json::from_value::<Event>(effective_val)
                    .context("invalid JS return: expected Event shape")?]
            }
            Value::Array(arr) => {
                if arr.is_empty() {
                    vec![]
                } else {
                    let mut v = Vec::with_capacity(arr.len());
                    for item in arr {
                        if item.is_null() {
                            continue;
                        }
                        v.push(serde_json::from_value::<Event>(item).context(
                            "invalid JS return: array element not an Event",
                        )?);
                    }
                    v
                }
            }
            _ => bail!("invalid JS return: expected object, array, or null"),
        };

        info!("js processed event");
        Ok(out)
    }
}

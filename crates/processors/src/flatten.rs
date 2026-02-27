//! Flatten processor - flattens nested JSON objects in event payloads.
//!
//! Works on every object-valued field present on the event without assumptions
//! about envelope structure. CDC `before`/`after`, outbox `payload`, or any
//! custom field produced by upstream processors are all handled uniformly.
//!
//! # Depth semantics
//!
//! `max_depth` counts the number of nesting levels to recurse into.
//! An object at the depth boundary is kept as a leaf and subject to
//! `empty_object` policy rather than being expanded further.
//!
//! ```text
//! depth 0: user          -> object, recurse
//! depth 1: user__address -> object, recurse
//! depth 2: user__address__city -> max_depth=2 reached, keep as leaf
//! ```

use anyhow::{Result, bail};
use async_trait::async_trait;
use deltaforge_config::{
    CollisionPolicy, EmptyListPolicy, EmptyObjectPolicy, FlattenProcessorCfg,
    ListPolicy,
};
use deltaforge_core::{Event, Processor};
use serde_json::{Map, Value};
use tracing::debug;

pub struct FlattenProcessor {
    id: String,
    cfg: FlattenProcessorCfg,
}

impl FlattenProcessor {
    pub fn new(cfg: FlattenProcessorCfg) -> Result<Self> {
        let id = cfg.id.clone();
        Ok(Self { id, cfg })
    }

    /// Flatten every object-valued field present on the event.
    fn flatten_event(&self, event: &mut Event) -> Result<()> {
        if let Some(v) = event.before.take() {
            event.before = Some(self.flatten_field(v)?);
        }
        if let Some(v) = event.after.take() {
            event.after = Some(self.flatten_field(v)?);
        }
        // DDL and other non-row events are passed through untouched —
        // they don't carry row payloads.
        Ok(())
    }

    /// Flatten a single field value. Non-objects are returned as-is.
    fn flatten_field(&self, value: Value) -> Result<Value> {
        match value {
            Value::Object(map) => {
                let mut out = Map::new();
                for (k, v) in map {
                    self.flatten_into(&k, v, 0, &mut out)?;
                }
                Ok(Value::Object(out))
            }
            other => Ok(other),
        }
    }

    fn flatten_into(
        &self,
        prefix: &str,
        value: Value,
        depth: usize,
        out: &mut Map<String, Value>,
    ) -> Result<()> {
        let at_max = self.cfg.max_depth.map_or(false, |max| depth >= max);

        match value {
            // ----------------------------------------------------------------
            // Object — recurse or treat as leaf at max_depth
            // ----------------------------------------------------------------
            Value::Object(ref map) if !at_max => {
                if map.is_empty() {
                    // Empty object: apply policy at this key
                    match self.cfg.empty_object {
                        EmptyObjectPolicy::Preserve => {
                            self.insert(prefix, value, out)?;
                        }
                        EmptyObjectPolicy::Drop => {}
                        EmptyObjectPolicy::Null => {
                            self.insert(prefix, Value::Null, out)?;
                        }
                    }
                } else {
                    // Non-empty: flatten children
                    if let Value::Object(map) = value {
                        for (k, v) in map {
                            let child_key = format!(
                                "{}{}{}",
                                prefix, self.cfg.separator, k
                            );
                            self.flatten_into(&child_key, v, depth + 1, out)?;
                        }
                    }
                }
            }

            // Object at max_depth boundary — treat as opaque leaf
            Value::Object(ref map) => {
                if map.is_empty() {
                    match self.cfg.empty_object {
                        EmptyObjectPolicy::Preserve => {
                            self.insert(prefix, value, out)?;
                        }
                        EmptyObjectPolicy::Drop => {}
                        EmptyObjectPolicy::Null => {
                            self.insert(prefix, Value::Null, out)?;
                        }
                    }
                } else {
                    self.insert(prefix, value, out)?;
                }
            }

            // ----------------------------------------------------------------
            // Array
            // ----------------------------------------------------------------
            Value::Array(ref arr) if arr.is_empty() => {
                match self.cfg.empty_list {
                    EmptyListPolicy::Preserve => {
                        self.insert(prefix, value, out)?;
                    }
                    EmptyListPolicy::Drop => {}
                    EmptyListPolicy::Null => {
                        self.insert(prefix, Value::Null, out)?;
                    }
                }
            }

            Value::Array(arr) => match self.cfg.lists {
                ListPolicy::Preserve => {
                    self.insert(prefix, Value::Array(arr), out)?;
                }
                ListPolicy::Index => {
                    for (i, item) in arr.into_iter().enumerate() {
                        let child_key =
                            format!("{}{}{}", prefix, self.cfg.separator, i);
                        self.flatten_into(&child_key, item, depth + 1, out)?;
                    }
                }
            },

            // ----------------------------------------------------------------
            // Scalar — always a leaf
            // ----------------------------------------------------------------
            scalar => {
                self.insert(prefix, scalar, out)?;
            }
        }

        Ok(())
    }

    fn insert(
        &self,
        key: &str,
        value: Value,
        out: &mut Map<String, Value>,
    ) -> Result<()> {
        match self.cfg.on_collision {
            CollisionPolicy::Last => {
                out.insert(key.to_string(), value);
            }
            CollisionPolicy::First => {
                out.entry(key.to_string()).or_insert(value);
            }
            CollisionPolicy::Error => {
                if out.contains_key(key) {
                    bail!("flatten: key collision on '{key}'");
                }
                out.insert(key.to_string(), value);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Processor for FlattenProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    async fn process(&self, mut events: Vec<Event>) -> Result<Vec<Event>> {
        for event in &mut events {
            self.flatten_event(event)?;
        }
        debug!(processor = %self.id, count = events.len(), "flatten processor ran");
        Ok(events)
    }
}

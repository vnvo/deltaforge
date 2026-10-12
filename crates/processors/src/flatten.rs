//! Flatten processor - flattens nested JSON documents in event row images.
//!
//! Works on every document-valued column, and every processor-added
//! document extension (which flattens into extensions, never columns), of
//! the `before` and `after` images
//! without assumptions about envelope structure: outbox payloads and values
//! added by upstream processors are untyped documents
//! ([`CanonicalValue::Document`]), and their objects (and, under
//! `lists: index`, arrays) expand into `parent<separator>child` columns,
//! each child a document. Schema-typed values, SQL JSON columns included,
//! are leaves: a JSON column is its own typed value, never reinterpreted.
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

use std::collections::HashMap;

use anyhow::{Result, bail};
use async_trait::async_trait;
use deltaforge_config::{
    CollisionPolicy, EmptyListPolicy, EmptyObjectPolicy, FlattenProcessorCfg,
    ListPolicy,
};
use deltaforge_core::canonical::Document;
use deltaforge_core::{
    BatchContext, CanonicalRow, CanonicalValue, Event, Processor,
};
use serde_json::Value;
use tracing::debug;

pub struct FlattenProcessor {
    id: String,
    cfg: FlattenProcessorCfg,
    digest: String,
}

/// Bump when a code change alters this processor's output for identical config.
const FLATTEN_IMPL_VERSION: u32 = 2;

/// Flattened columns in insertion order, with the collision policy applied.
#[derive(Default)]
struct Columns {
    out: Vec<(String, CanonicalValue)>,
    index: HashMap<String, usize>,
}

fn doc(v: Value) -> CanonicalValue {
    CanonicalValue::Document(Document::new(v))
}

impl FlattenProcessor {
    pub fn new(cfg: FlattenProcessorCfg) -> Result<Self> {
        let id = cfg.id.clone();
        let digest = crate::digest::builtin_digest(
            "flatten",
            FLATTEN_IMPL_VERSION,
            &cfg,
        );
        Ok(Self { id, cfg, digest })
    }

    /// Flatten every document-valued column of the event's row images.
    fn flatten_event(&self, event: &mut Event) -> Result<()> {
        if let Some(r) = event.before.take() {
            event.before = Some(self.flatten_row(r)?);
        }
        if let Some(r) = event.after.take() {
            event.after = Some(self.flatten_row(r)?);
        }
        // DDL and other non-row events are passed through untouched —
        // they don't carry row payloads.
        Ok(())
    }

    fn flatten_row(&self, row: CanonicalRow) -> Result<CanonicalRow> {
        let (columns, extensions) = row.into_parts();
        let mut out = Columns::default();
        for (k, v) in columns {
            match v {
                CanonicalValue::Document(d) => {
                    self.flatten_into(&k, d.into_value(), 0, &mut out)?
                }
                typed => self.insert(&k, typed, &mut out)?,
            }
        }
        // Extension documents flatten into extensions: they never become
        // columns.
        let mut ext = Columns::default();
        for (k, d) in extensions {
            self.flatten_into(&k, d.into_value(), 0, &mut ext)?;
        }
        let ext = ext.out.into_iter().map(|(k, v)| match v {
            CanonicalValue::Document(d) => (k, d),
            _ => unreachable!("flattened documents are documents"),
        });
        CanonicalRow::with_extensions(out.out, ext)
            .map_err(|e| anyhow::anyhow!("flatten: {e}"))
    }

    fn flatten_into(
        &self,
        prefix: &str,
        value: Value,
        depth: usize,
        out: &mut Columns,
    ) -> Result<()> {
        let at_max = self.cfg.max_depth.is_some_and(|max| depth >= max);

        match value {
            // ----------------------------------------------------------------
            // Object — recurse or treat as leaf at max_depth
            // ----------------------------------------------------------------
            Value::Object(map) if map.is_empty() => match self.cfg.empty_object
            {
                EmptyObjectPolicy::Preserve => {
                    self.insert(prefix, doc(Value::Object(map)), out)?;
                }
                EmptyObjectPolicy::Drop => {}
                EmptyObjectPolicy::Null => {
                    self.insert(prefix, doc(Value::Null), out)?;
                }
            },
            Value::Object(map) if !at_max => {
                for (k, v) in map {
                    let child_key =
                        format!("{}{}{}", prefix, self.cfg.separator, k);
                    self.flatten_into(&child_key, v, depth + 1, out)?;
                }
            }
            // Object at max_depth boundary — kept as one document
            object @ Value::Object(_) => {
                self.insert(prefix, doc(object), out)?
            }

            // ----------------------------------------------------------------
            // Array
            // ----------------------------------------------------------------
            Value::Array(arr) if arr.is_empty() => match self.cfg.empty_list {
                EmptyListPolicy::Preserve => {
                    self.insert(prefix, doc(Value::Array(arr)), out)?;
                }
                EmptyListPolicy::Drop => {}
                EmptyListPolicy::Null => {
                    self.insert(prefix, doc(Value::Null), out)?;
                }
            },

            Value::Array(arr) => match self.cfg.lists {
                ListPolicy::Preserve => {
                    self.insert(prefix, doc(Value::Array(arr)), out)?;
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
            scalar => self.insert(prefix, doc(scalar), out)?,
        }

        Ok(())
    }

    fn insert(
        &self,
        key: &str,
        value: CanonicalValue,
        out: &mut Columns,
    ) -> Result<()> {
        match (out.index.get(key), &self.cfg.on_collision) {
            (None, _) => {
                out.index.insert(key.to_string(), out.out.len());
                out.out.push((key.to_string(), value));
            }
            (Some(&i), CollisionPolicy::Last) => out.out[i].1 = value,
            (Some(_), CollisionPolicy::First) => {}
            (Some(_), CollisionPolicy::Error) => {
                bail!("flatten: key collision on '{key}'");
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

    fn identity_digest(&self) -> &str {
        &self.digest
    }

    async fn process(
        &self,
        mut events: Vec<Event>,
        _ctx: &BatchContext,
    ) -> Result<Vec<Event>> {
        for event in &mut events {
            self.flatten_event(event)?;
        }
        debug!(processor = %self.id, count = events.len(), "flatten processor ran");
        Ok(events)
    }
}

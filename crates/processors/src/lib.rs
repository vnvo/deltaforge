use std::sync::Arc;

use anyhow::Result;
use deltaforge_config::{PipelineSpec, ProcessorCfg};
use deltaforge_core::ArcDynProcessor;

mod js;
pub use js::JsProcessor;

mod outbox;
pub use outbox::OutboxProcessor;

pub fn build_processors(ps: &PipelineSpec) -> Result<Arc<[ArcDynProcessor]>> {
    let mut out: Vec<ArcDynProcessor> = Vec::new();

    for p in &ps.spec.processors {
        let proc: ArcDynProcessor = match p {
            ProcessorCfg::Javascript { id, inline, .. } => {
                Arc::new(JsProcessor::new(id.clone(), inline.clone())?)
                    as ArcDynProcessor
            }
            ProcessorCfg::Outbox { config } => {
                Arc::new(OutboxProcessor::new(config.as_ref().clone())?)
                    as ArcDynProcessor
            }
        };
        out.push(proc);
    }

    Ok(out.into())
}

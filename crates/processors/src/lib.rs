use std::sync::Arc;

use anyhow::Result;
use deltaforge_config::{PipelineSpec, ProcessorCfg};
use deltaforge_core::ArcDynProcessor;

pub mod synthetic;
pub use synthetic::SyntheticMarkingProcessor;

mod js;
pub use js::JsProcessor;

mod outbox;
pub use outbox::OutboxProcessor;

mod flatten;
pub use flatten::FlattenProcessor;

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
            ProcessorCfg::Flatten { config } => {
                Arc::new(FlattenProcessor::new(config.as_ref().clone())?)
                    as ArcDynProcessor
            }
        };
        out.push(SyntheticMarkingProcessor::wrap(proc));
    }

    Ok(out.into())
}

use std::sync::Arc;

use deltaforge_config::{PipelineSpec, SinkCfg};
use deltaforge_core::ArcDynSink;

use crate::{kafka::KafkaSink, redis::RedisSink};

pub mod kafka;
pub mod redis;

pub fn build_sinks(ps: &PipelineSpec) -> anyhow::Result<Vec<ArcDynSink>> {
    ps.spec.sinks
        .iter()
        .map(|s| {
            let sink: ArcDynSink = match s {
                SinkCfg::Kafka(kafka_cfg) => {
                    let sink = KafkaSink::new(kafka_cfg)?; 
                    Arc::new(sink) as ArcDynSink
                },
                SinkCfg::Redis(redis_cfg) => {
                    let sink = RedisSink::new(redis_cfg)?; 
                    Arc::new(sink) as ArcDynSink
                },
            };
            Ok(sink)
        })
        .collect()
}
use std::sync::Arc;

use deltaforge_config::{PipelineSpec, SinkCfg};
use deltaforge_core::ArcDynSink;

use crate::{kafka::KafkaSink, redis::RedisSink};

pub mod kafka;
pub mod redis;

pub fn build_sinks(ps: &PipelineSpec) -> Vec<ArcDynSink> {
    ps.spec.sinks
        .iter()
        .map(|s| {
            match s {
                SinkCfg::Kafka(kafka_cfg) => {
                    let sink = KafkaSink::new(kafka_cfg).unwrap(); 
                    Arc::new(sink) as ArcDynSink
                },
                SinkCfg::Redis(redis_cfg) => {
                    let sink = RedisSink::new(&redis_cfg.uri, &redis_cfg.stream).unwrap(); 
                    Arc::new(sink) as ArcDynSink
                },
            }
        })
        .collect()
}
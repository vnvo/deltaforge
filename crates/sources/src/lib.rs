pub(crate) mod conn_utils;
pub mod mysql;
//pub mod postgres;

use anyhow::Result;
use deltaforge_config::{PipelineSpec, SourceCfg};
use deltaforge_core::ArcDynSource;
use std::sync::Arc;

use crate::mysql::MySqlSource;

pub fn build_source(pipeline: &PipelineSpec) -> Result<ArcDynSource> {
    match &pipeline.spec.source {
        SourceCfg::Postgres(_c) => todo!(),
        SourceCfg::Mysql(c) => Ok(Arc::new(MySqlSource {
            id: c.id.clone(),
            dsn: c.dsn.clone(),
            tables: c.tables.clone(),
            tenant: pipeline.metadata.tenant.clone(),
            pipeline: pipeline.metadata.name.clone(),
        })),
    }
}

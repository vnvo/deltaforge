pub(crate) mod conn_utils;
pub mod mysql;
//pub mod postgres;

use std::sync::Arc;
use anyhow::Result;
use deltaforge_config::{SourceCfg, PipelineSpec};
use deltaforge_core::ArcDynSource;

use crate::mysql::MySqlSource;

pub fn build_source(pipeline: &PipelineSpec) -> Result<ArcDynSource> {
    match &pipeline.spec.source {
        SourceCfg::Postgres(_c) => todo!(),
        SourceCfg::Mysql(c) => Ok(Arc::new(MySqlSource {
            id: c.id.clone(),
            dsn: c.dsn.clone(),
            tables: c.tables.clone(),
            tenant: pipeline.metadata.tenant.clone(),
        })),
    }
}

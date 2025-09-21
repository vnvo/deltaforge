use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs, path::PathBuf, sync::Arc};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Checkpoints {
    pub mysql: HashMap<String, MySqlPos>, // key: pipeline_id
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MySqlPos {
    pub file: String,
    pub pos: u64,
    pub gtid_set: Option<String>, // optional GTID set string (preferred)
}

#[derive(Clone)]
pub struct FsCheckpointStore {
    path: PathBuf,
    state: Arc<RwLock<Checkpoints>>,
}

impl FsCheckpointStore {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let state = if path.exists() {
            let content = fs::read_to_string(&path).context("read checkpoints")?;
            serde_json::from_str::<Checkpoints>(&content).context("parse checkpoints")?
        } else {
            Checkpoints::default()
        };
        Ok(Self { path, state: Arc::new(RwLock::new(state)) })
    }

    pub async fn get_mysql(&self, pipeline_id: &str) -> Option<MySqlPos> {
        self.state.read().await.mysql.get(pipeline_id).cloned()
    }

    pub async fn put_mysql(&self, pipeline_id: &str, pos: MySqlPos) -> Result<()> {
        self.state.write().await.mysql.insert(pipeline_id.to_string(), pos);
        self.flush().await
    }

    async fn flush(&self) -> Result<()> {
        let s = self.state.read().await;
        let tmp = self.path.with_extension("tmp");
        fs::write(&tmp, serde_json::to_vec_pretty(&*s)?).context("write tmp")?;
        fs::rename(tmp, &self.path).context("atomic rename")?;
        Ok(())
    }
}

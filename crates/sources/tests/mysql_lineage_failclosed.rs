//! R3-C8 (production path): the MySQL source fails closed and opens ZERO binlog
//! streams when the source lineage cannot be verified.
//!
//! Lineage is captured once at the very start of `run_inner` with `?`, before any
//! snapshot, `prepare_client`, or stream open. Here the source points at a dead
//! TCP port, so the lineage capture fails first and the source stops before
//! opening a stream. This exercises the real `run()` startup path, not just the
//! `mysql_server_lineage` value-validation unit tests. No container required.

use std::sync::Arc;
use std::time::Duration;

use checkpoints::{CheckpointStore, MemCheckpointStore};
use common::AllowList;
use deltaforge_config::SnapshotCfg;
use deltaforge_core::Source;
use sources::mysql::MySqlSource;
use sources::stream_probe::{reset_streams_opened, streams_opened};
use storage::{ArcStorageBackend, MemoryStorageBackend};
use tokio::time::timeout;

mod test_common;
use test_common::make_registry;

#[tokio::test]
async fn unverified_lineage_prevents_stream_opening() {
    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let src = MySqlSource {
        id: "no_lineage".into(),
        // Nothing listens on this port: the first startup step (lineage capture)
        // fails, so the source never reaches a stream open.
        dsn: "mysql://root:root@127.0.0.1:59999/db".into(),
        tables: vec!["db.t".into()],
        tenant: "acme".into(),
        pipeline: "test".into(),
        registry: make_registry().await,
        registry_scope: sources::registry_scope::SharedRegistryScope::default(),
        backend,
        outbox_tables: AllowList::default(),
        snapshot_cfg: SnapshotCfg::default(),
        on_schema_drift: deltaforge_config::OnSchemaDrift::Adapt,
        table_options: Default::default(),
        rotation: None,
        snapshot_cohort: Default::default(),
    };

    reset_streams_opened();
    let ckpt: Arc<dyn CheckpointStore> =
        Arc::new(MemCheckpointStore::new().unwrap());
    let (tx, _rx) = test_common::acked_channel(&src, &ckpt, &src.id, 64);
    let handle = src.run(tx, ckpt).await;

    match timeout(Duration::from_secs(15), handle.join()).await {
        Ok(Err(_)) => {} // failed closed, as required
        Ok(Ok(())) => {
            panic!("source must not succeed when lineage cannot be verified")
        }
        Err(_) => panic!("source did not fail closed within the timeout"),
    }

    assert_eq!(
        streams_opened(),
        0,
        "no binlog stream may open when the source lineage is unverified"
    );
}

//! Integration tests for `StorageBackend`.
//!
//! Each test runs against both `MemoryStorageBackend` and `SqliteStorageBackend`
//! via the `for_each_backend!` macro.

use std::sync::Arc;

use checkpoints::CheckpointStore;
use deltaforge_core::SchemaRegistry;
use storage::{
    ArcStorageBackend, MemoryStorageBackend,
    adapters::{BackendCheckpointStore, DurableSchemaRegistry},
};

#[cfg(feature = "sqlite")]
use storage::SqliteStorageBackend;

async fn memory_backend() -> ArcStorageBackend {
    Arc::new(MemoryStorageBackend::new())
}

#[cfg(feature = "sqlite")]
async fn sqlite_backend() -> ArcStorageBackend {
    SqliteStorageBackend::in_memory().expect("sqlite in-memory")
}

/// Run a test closure against all backends.
macro_rules! for_each_backend {
    ($test:expr) => {{
        $test(memory_backend().await).await;
        #[cfg(feature = "sqlite")]
        $test(sqlite_backend().await).await;
    }};
}

#[tokio::test]
async fn kv_put_get_delete() {
    for_each_backend!(|b: ArcStorageBackend| async move {
        b.kv_put("checkpoints", "src1", b"hello").await.unwrap();
        assert_eq!(
            b.kv_get("checkpoints", "src1").await.unwrap(),
            Some(b"hello".to_vec())
        );

        b.kv_delete("checkpoints", "src1").await.unwrap();
        assert_eq!(b.kv_get("checkpoints", "src1").await.unwrap(), None);
    });
}

#[tokio::test]
async fn kv_ttl_expiry() {
    for_each_backend!(|b: ArcStorageBackend| async move {
        b.kv_put_with_ttl("leases", "pipe1", b"alive", 1)
            .await
            .unwrap();
        // Before expiry: present
        assert!(b.kv_get("leases", "pipe1").await.unwrap().is_some());

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        // After expiry: lazy expiry returns None
        assert_eq!(b.kv_get("leases", "pipe1").await.unwrap(), None);
    });
}

#[tokio::test]
async fn kv_list_with_prefix() {
    for_each_backend!(|b: ArcStorageBackend| async move {
        b.kv_put("fsm", "pipe-a", b"1").await.unwrap();
        b.kv_put("fsm", "pipe-b", b"2").await.unwrap();
        b.kv_put("fsm", "other", b"3").await.unwrap();

        let keys = b.kv_list("fsm", Some("pipe")).await.unwrap();
        assert_eq!(keys, vec!["pipe-a", "pipe-b"]);
    });
}

#[tokio::test]
async fn log_global_seq_monotonic_across_keys() {
    for_each_backend!(|b: ArcStorageBackend| async move {
        // Interleave appends to different keys — seq must be strictly increasing globally.
        let s1 = b.log_append("schemas", "t/db/tbl1", b"v1").await.unwrap();
        let s2 = b.log_append("schemas", "t/db/tbl2", b"v1").await.unwrap();
        let s3 = b.log_append("schemas", "t/db/tbl1", b"v2").await.unwrap();

        assert!(s1 < s2, "s1={s1} s2={s2}");
        assert!(s2 < s3, "s2={s2} s3={s3}");
    });
}

#[tokio::test]
async fn log_since_returns_only_newer_entries() {
    for_each_backend!(|b: ArcStorageBackend| async move {
        let s1 = b.log_append("schemas", "t/db/t", b"a").await.unwrap();
        let s2 = b.log_append("schemas", "t/db/t", b"b").await.unwrap();
        let _s3 = b.log_append("schemas", "t/db/t", b"c").await.unwrap();

        let since = b.log_since("schemas", "t/db/t", s1).await.unwrap();
        assert_eq!(since.len(), 2);
        assert_eq!(since[0].0, s2);
    });
}


#[tokio::test]
async fn slot_cas_exactly_one_winner_under_contention() {
    for_each_backend!(|b: ArcStorageBackend| async move {
        let v = b
            .slot_upsert("snapshots", "pipe/tbl", b"state0")
            .await
            .unwrap();
        assert_eq!(v, 1);

        // Two concurrent CAS with same expected version → exactly one wins
        let (ok1, ok2) = tokio::join!(
            b.slot_cas("snapshots", "pipe/tbl", 1, b"state-a"),
            b.slot_cas("snapshots", "pipe/tbl", 1, b"state-b"),
        );
        let wins = [ok1.unwrap(), ok2.unwrap()];
        assert_eq!(
            wins.iter().filter(|&&x| x).count(),
            1,
            "exactly one CAS must win"
        );
    });
}

#[tokio::test]
async fn slot_cas_wrong_version_returns_false() {
    for_each_backend!(|b: ArcStorageBackend| async move {
        b.slot_upsert("snapshots", "pipe/t", b"v0").await.unwrap();
        let ok = b
            .slot_cas("snapshots", "pipe/t", 99, b"wrong")
            .await
            .unwrap();
        assert!(!ok);
        // State unchanged
        let (ver, val) =
            b.slot_get("snapshots", "pipe/t").await.unwrap().unwrap();
        assert_eq!(ver, 1);
        assert_eq!(val, b"v0");
    });
}


#[tokio::test]
async fn queue_ack_removes_up_to_id_inclusive() {
    for_each_backend!(|b: ArcStorageBackend| async move {
        let mut ids = Vec::new();
        for i in 0..10u8 {
            ids.push(
                b.queue_push("quarantine", "pipe/tbl", &[i]).await.unwrap(),
            );
        }

        // Peek first 3
        let peeked = b.queue_peek("quarantine", "pipe/tbl", 3).await.unwrap();
        assert_eq!(peeked.len(), 3);

        // Ack up to index 2 (3rd entry)
        b.queue_ack("quarantine", "pipe/tbl", ids[2]).await.unwrap();
        assert_eq!(b.queue_len("quarantine", "pipe/tbl").await.unwrap(), 7);

        // Next peek starts from ids[3]
        let next = b.queue_peek("quarantine", "pipe/tbl", 1).await.unwrap();
        assert_eq!(next[0].0, ids[3]);
    });
}

#[tokio::test]
async fn queue_drop_oldest_removes_from_front() {
    for_each_backend!(|b: ArcStorageBackend| async move {
        let mut ids = Vec::new();
        for i in 0..5u8 {
            ids.push(b.queue_push("dlq", "pipe1", &[i]).await.unwrap());
        }

        b.queue_drop_oldest("dlq", "pipe1", 2).await.unwrap();
        assert_eq!(b.queue_len("dlq", "pipe1").await.unwrap(), 3);

        // Oldest entries gone, newest remain
        let remaining = b.queue_peek("dlq", "pipe1", 10).await.unwrap();
        assert_eq!(remaining[0].0, ids[2]); // ids[0] and ids[1] dropped
    });
}


#[tokio::test]
async fn checkpoint_adapter_round_trip() {
    for_each_backend!(|b: ArcStorageBackend| async move {
        let store = BackendCheckpointStore::new(Arc::clone(&b));
        store
            .put_raw("mysql-source-1", b"checkpoint-data")
            .await
            .unwrap();

        let got = store.get_raw("mysql-source-1").await.unwrap();
        assert_eq!(got, Some(b"checkpoint-data".to_vec()));

        let keys = store.list().await.unwrap();
        assert!(keys.contains(&"mysql-source-1".to_string()));

        store.delete("mysql-source-1").await.unwrap();
        assert_eq!(store.get_raw("mysql-source-1").await.unwrap(), None);
    });
}

#[tokio::test]
async fn schema_registry_cold_start_replay() {
    for_each_backend!(|b: ArcStorageBackend| async move {
        // Register schemas using first registry instance
        {
            let reg = DurableSchemaRegistry::new(Arc::clone(&b)).await.unwrap();
            let v1 = reg
                .register_with_checkpoint(
                    "t",
                    "db",
                    "tbl",
                    "h1",
                    &serde_json::json!({"cols":["id"]}),
                    Some(b"cp1"),
                )
                .await
                .unwrap();
            assert_eq!(v1, 1);

            // Idempotent: same hash → same version
            let v1b = reg
                .register_with_checkpoint(
                    "t",
                    "db",
                    "tbl",
                    "h1",
                    &serde_json::json!({"cols":["id"]}),
                    None,
                )
                .await
                .unwrap();
            assert_eq!(v1b, 1);

            reg.register_with_checkpoint(
                "t",
                "db",
                "tbl",
                "h2",
                &serde_json::json!({"cols":["id","name"]}),
                None,
            )
            .await
            .unwrap();
        }

        // Cold-start: new registry from same backend should replay log
        let reg2 = DurableSchemaRegistry::new(Arc::clone(&b)).await.unwrap();
        let versions = reg2.list_versions("t", "db", "tbl");
        assert_eq!(
            versions.len(),
            2,
            "both schema versions should be replayed"
        );
        assert_eq!(versions[0].hash, "h1");
        assert_eq!(versions[1].hash, "h2");

        // Checkpoint correlation preserved
        assert_eq!(versions[0].checkpoint.as_deref(), Some(b"cp1".as_slice()));

        // Sequence ordering preserved
        assert!(versions[0].sequence < versions[1].sequence);
    });
}

#[tokio::test]
async fn schema_registry_get_at_sequence() {
    for_each_backend!(|b: ArcStorageBackend| async move {
        let reg = DurableSchemaRegistry::new(Arc::clone(&b)).await.unwrap();

        reg.register("t", "db", "tbl", "h1", &serde_json::json!({}))
            .await
            .unwrap();
        let seq_after_v1 = reg.current_sequence();

        reg.register("t", "db", "tbl", "h2", &serde_json::json!({}))
            .await
            .unwrap();

        // At seq_after_v1, should resolve v1
        let sv = reg.get_at_sequence("t", "db", "tbl", seq_after_v1).unwrap();
        assert_eq!(sv.version, 1);

        // Latest seq resolves v2
        let sv = reg
            .get_at_sequence("t", "db", "tbl", reg.current_sequence())
            .unwrap();
        assert_eq!(sv.version, 2);
    });
}


#[tokio::test]
async fn kv_namespaces_are_isolated() {
    for_each_backend!(|b: ArcStorageBackend| async move {
        b.kv_put("checkpoints", "key1", b"cp").await.unwrap();
        b.kv_put("schemas", "key1", b"schema").await.unwrap();

        assert_eq!(
            b.kv_get("checkpoints", "key1").await.unwrap(),
            Some(b"cp".to_vec())
        );
        assert_eq!(
            b.kv_get("schemas", "key1").await.unwrap(),
            Some(b"schema".to_vec())
        );

        b.kv_delete("checkpoints", "key1").await.unwrap();
        // Schema namespace unaffected
        assert_eq!(
            b.kv_get("schemas", "key1").await.unwrap(),
            Some(b"schema".to_vec())
        );
    });
}


#[tokio::test]
async fn checkpoint_multiple_pipelines_isolated() {
    for_each_backend!(|b: ArcStorageBackend| async move {
        let store = BackendCheckpointStore::new(Arc::clone(&b));
        store.put_raw("mysql-pipe-a", b"cp-a").await.unwrap();
        store.put_raw("mysql-pipe-b", b"cp-b").await.unwrap();

        assert_eq!(
            store.get_raw("mysql-pipe-a").await.unwrap(),
            Some(b"cp-a".to_vec())
        );
        assert_eq!(
            store.get_raw("mysql-pipe-b").await.unwrap(),
            Some(b"cp-b".to_vec())
        );

        store.delete("mysql-pipe-a").await.unwrap();
        assert_eq!(store.get_raw("mysql-pipe-a").await.unwrap(), None);
        // pipe-b unaffected
        assert_eq!(
            store.get_raw("mysql-pipe-b").await.unwrap(),
            Some(b"cp-b".to_vec())
        );
    });
}

#[tokio::test]
async fn checkpoint_overwrite_returns_latest() {
    for_each_backend!(|b: ArcStorageBackend| async move {
        let store = BackendCheckpointStore::new(Arc::clone(&b));
        store.put_raw("pg-pipe", b"pos-1").await.unwrap();
        store.put_raw("pg-pipe", b"pos-2").await.unwrap();

        let got = store.get_raw("pg-pipe").await.unwrap();
        assert_eq!(got, Some(b"pos-2".to_vec()));
    });
}

#[tokio::test]
async fn checkpoint_concurrent_writes_no_error() {
    for_each_backend!(|b: ArcStorageBackend| async move {
        let store = Arc::new(BackendCheckpointStore::new(Arc::clone(&b)));
        let s1 = Arc::clone(&store);
        let s2 = Arc::clone(&store);

        // Both write different values — one must survive without error
        tokio::join!(
            async move { s1.put_raw("pipe", b"writer-a").await.unwrap() },
            async move { s2.put_raw("pipe", b"writer-b").await.unwrap() },
        );

        // Must have a value — either is valid (last-write-wins)
        let got = store.get_raw("pipe").await.unwrap();
        assert!(
            got == Some(b"writer-a".to_vec())
                || got == Some(b"writer-b".to_vec()),
            "expected one of the two writers to win, got {:?}",
            got
        );
    });
}


#[tokio::test]
async fn schema_registry_multi_tenant_isolation() {
    for_each_backend!(|b: ArcStorageBackend| async move {
        let reg = DurableSchemaRegistry::new(Arc::clone(&b)).await.unwrap();

        reg.register("tenant-a", "db", "orders", "h1", &serde_json::json!({}))
            .await
            .unwrap();
        reg.register("tenant-b", "db", "orders", "h2", &serde_json::json!({}))
            .await
            .unwrap();

        let va = reg.list_versions("tenant-a", "db", "orders");
        let vb = reg.list_versions("tenant-b", "db", "orders");

        assert_eq!(va.len(), 1);
        assert_eq!(vb.len(), 1);
        assert_eq!(va[0].hash, "h1");
        assert_eq!(vb[0].hash, "h2");
    });
}

#[tokio::test]
async fn schema_registry_sequence_monotonic_across_tables() {
    for_each_backend!(|b: ArcStorageBackend| async move {
        let reg = DurableSchemaRegistry::new(Arc::clone(&b)).await.unwrap();

        reg.register("t", "db", "tbl1", "h1", &serde_json::json!({}))
            .await
            .unwrap();
        let s1 = reg.current_sequence();
        reg.register("t", "db", "tbl2", "h1", &serde_json::json!({}))
            .await
            .unwrap();
        let s2 = reg.current_sequence();
        reg.register("t", "db", "tbl1", "h2", &serde_json::json!({}))
            .await
            .unwrap();
        let s3 = reg.current_sequence();

        assert!(
            s1 < s2 && s2 < s3,
            "sequence must be globally monotonic: {s1} < {s2} < {s3}"
        );
    });
}

#[tokio::test]
async fn schema_registry_cold_start_sequence_continues() {
    // After replay, new registrations must not reuse old sequence numbers.
    for_each_backend!(|b: ArcStorageBackend| async move {
        let seq_before_restart = {
            let reg = DurableSchemaRegistry::new(Arc::clone(&b)).await.unwrap();
            reg.register("t", "db", "tbl", "h1", &serde_json::json!({}))
                .await
                .unwrap();
            reg.current_sequence()
        };

        let reg2 = DurableSchemaRegistry::new(Arc::clone(&b)).await.unwrap();
        reg2.register("t", "db", "tbl", "h2", &serde_json::json!({}))
            .await
            .unwrap();
        let seq_after_restart = reg2.current_sequence();

        assert!(
            seq_after_restart > seq_before_restart,
            "sequence must continue after cold-start replay: {seq_after_restart} > {seq_before_restart}"
        );
    });
}

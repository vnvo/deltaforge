//! Adoption of a PostgreSQL source's stored checkpoints into its continuity
//! chain.
//!
//! Checkpoints carry the continuity stamp (`timeline`, `chain`, `transition`)
//! of the stream they were read on, and checkpoints of different transitions
//! order only within one chain; an unstamped checkpoint (written before
//! continuity was recorded) orders only with unstamped ones and transition 0
//! (see `compare_pg_checkpoints`). Per-sink checkpoints are written
//! independently, so after an upgrade some sinks can still hold unstamped
//! checkpoints when a promotion records transition 1, and the fold of an
//! unstamped and a transition-1 checkpoint fails closed.
//!
//! So while the chain is at transition 0, every activation of a stream first
//! adopts the source's checkpoint candidates into it: the `{source_id}::sink::*`
//! keys, or the exact aggregate key when there is none (never snapshot
//! progress or any other key). Every candidate is classified before anything
//! is written:
//! - stamped by this chain at transition 0 on its timeline: nothing to do;
//! - unstamped: adopted (the chain was created on the history they belong
//!   to: a fresh source, or a server that never switched timeline);
//! - anything else - another chain, another transition or timeline, a partial
//!   stamp, a malformed checkpoint - refuses the whole adoption and rewrites
//!   nothing.
//!
//! A rewrite inserts only the stamp members into the stored JSON object; the
//! position and every other byte of meaning are kept. Adoption is idempotent:
//! a crash part-way leaves stamped and unstamped checkpoints (comparable by
//! design), and the next activation finishes it before anything is consumed.
//! Once it has completed, no later transition can meet an unstamped
//! checkpoint of this source.

use checkpoints::CheckpointStore;
use pgwire_replication::Lsn;
use tracing::info;

use super::PostgresCheckpoint;
use super::postgres_continuity::ContinuityRecord;

/// Why adoption was refused (nothing was rewritten).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AdoptionRefused {
    pub reason: String,
    pub checkpoint_chain: Option<String>,
    pub checkpoint_transition: Option<u64>,
}

#[derive(Debug)]
pub(crate) enum AdoptionError {
    Refused(AdoptionRefused),
    Store(anyhow::Error),
}

/// Adopt `source_id`'s unstamped checkpoints into `record`'s chain (module
/// docs). Only while the chain is at transition 0; returns how many were
/// rewritten.
pub(crate) async fn adopt_into_chain(
    chkpt: &dyn CheckpointStore,
    source_id: &str,
    record: &ContinuityRecord,
) -> Result<usize, AdoptionError> {
    if record.transition_id != 0 {
        return Ok(0);
    }
    let store = |e: &dyn std::fmt::Display| {
        AdoptionError::Store(anyhow::anyhow!(
            "checkpoint chain adoption for {source_id}: {e}"
        ))
    };
    let refused =
        |reason: String, chain: Option<String>, transition: Option<u64>| {
            AdoptionError::Refused(AdoptionRefused {
                reason,
                checkpoint_chain: chain,
                checkpoint_transition: transition,
            })
        };

    // Candidates: per-sink keys, or the exact aggregate key without them.
    let prefix = format!("{source_id}::sink::");
    let mut keys = chkpt
        .list_with_prefix(&prefix)
        .await
        .map_err(|e| store(&e))?;
    keys.retain(|k| k.starts_with(&prefix));
    if keys.is_empty() {
        keys.push(source_id.to_string());
    }

    // 1. Classify every candidate before anything is written.
    let mut to_stamp = Vec::new();
    for key in keys {
        let Some(raw) = chkpt.get_raw(&key).await.map_err(|e| store(&e))?
        else {
            continue;
        };
        let (Ok(cp), Ok(serde_json::Value::Object(obj))) = (
            serde_json::from_slice::<PostgresCheckpoint>(&raw),
            serde_json::from_slice::<serde_json::Value>(&raw),
        ) else {
            return Err(refused(
                "a checkpoint is malformed (not a PostgreSQL checkpoint)"
                    .into(),
                None,
                None,
            ));
        };
        if Lsn::parse(&cp.lsn).is_err() {
            return Err(refused(
                format!("a checkpoint has a malformed position {:?}", cp.lsn),
                None,
                None,
            ));
        }
        match (cp.chain, cp.transition, cp.timeline) {
            (None, None, None) => to_stamp.push((key, obj)),
            (Some(chain), Some(0), Some(timeline))
                if chain == record.chain_id && timeline == record.timeline => {}
            (Some(chain), Some(transition), Some(_)) => {
                return Err(refused(
                    "a checkpoint is stamped by another chain, transition or \
                     timeline"
                        .into(),
                    Some(chain),
                    Some(transition),
                ));
            }
            (chain, transition, _) => {
                return Err(refused(
                    "a checkpoint carries a partial continuity stamp".into(),
                    chain,
                    transition,
                ));
            }
        }
    }

    // 2. Stamp: insert only the continuity members.
    let adopted = to_stamp.len();
    for (key, mut obj) in to_stamp {
        obj.insert("timeline".into(), record.timeline.into());
        obj.insert("chain".into(), record.chain_id.clone().into());
        obj.insert("transition".into(), 0u64.into());
        let bytes = serde_json::to_vec(&obj).map_err(|e| store(&e))?;
        chkpt.put_raw(&key, &bytes).await.map_err(|e| store(&e))?;
    }
    if adopted > 0 {
        info!(
            source_id,
            chain_id = %record.chain_id,
            timeline = record.timeline,
            checkpoints = adopted,
            "checkpoints adopted into the continuity chain"
        );
    }
    Ok(adopted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use checkpoints::MemCheckpointStore;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

    fn record(transition_id: u64) -> ContinuityRecord {
        ContinuityRecord {
            format: 1,
            chain_id: "c".into(),
            system_identifier: 7,
            database_oid: 5,
            timeline: 1,
            transition_id,
            proven_at: None,
        }
    }

    fn legacy(lsn: &str) -> Vec<u8> {
        format!(r#"{{"lsn":"{lsn}","tx_id":42}}"#).into_bytes()
    }

    async fn json(store: &dyn CheckpointStore, key: &str) -> serde_json::Value {
        serde_json::from_slice(&store.get_raw(key).await.unwrap().unwrap())
            .unwrap()
    }

    fn stamped(lsn: &str, chain: &str, transition: u64) -> serde_json::Value {
        serde_json::json!({
            "lsn": lsn, "tx_id": 42, "timeline": 1, "chain": chain,
            "transition": transition
        })
    }

    #[tokio::test]
    async fn every_sink_checkpoint_joins_the_chain_unchanged() {
        let store = MemCheckpointStore::new().unwrap();
        store
            .put_raw("orders::sink::kafka", &legacy("0/200"))
            .await
            .unwrap();
        store
            .put_raw("orders::sink::s3", &legacy("0/100"))
            .await
            .unwrap();
        // Not candidates: another source, snapshot progress, the aggregate
        // key next to per-sink keys.
        store
            .put_raw("orders-archive::sink::kafka", &legacy("0/300"))
            .await
            .unwrap();
        store
            .put_raw("pg_snapshot_progress:orders", br#"{"finished":true}"#)
            .await
            .unwrap();
        store.put_raw("orders", &legacy("0/50")).await.unwrap();

        let n = adopt_into_chain(&store, "orders", &record(0))
            .await
            .unwrap();
        assert_eq!(n, 2);
        assert_eq!(
            json(&store, "orders::sink::kafka").await,
            stamped("0/200", "c", 0),
            "position and tx_id kept, stamp added"
        );
        assert_eq!(
            json(&store, "orders::sink::s3").await,
            stamped("0/100", "c", 0)
        );
        assert_eq!(
            store.get_raw("orders-archive::sink::kafka").await.unwrap(),
            Some(legacy("0/300"))
        );
        assert_eq!(
            store.get_raw("orders").await.unwrap(),
            Some(legacy("0/50"))
        );
        assert_eq!(
            store.get_raw("pg_snapshot_progress:orders").await.unwrap(),
            Some(br#"{"finished":true}"#.to_vec())
        );
        // Idempotent.
        assert_eq!(
            adopt_into_chain(&store, "orders", &record(0))
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn the_aggregate_key_is_adopted_without_sink_keys() {
        let store = MemCheckpointStore::new().unwrap();
        store.put_raw("orders", &legacy("0/50")).await.unwrap();
        assert_eq!(
            adopt_into_chain(&store, "orders", &record(0))
                .await
                .unwrap(),
            1
        );
        assert_eq!(json(&store, "orders").await, stamped("0/50", "c", 0));
    }

    /// Fails every write after the first `ok` ones (a crash part-way).
    struct CrashAfter {
        inner: MemCheckpointStore,
        ok: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl CheckpointStore for CrashAfter {
        async fn get_raw(
            &self,
            k: &str,
        ) -> checkpoints::CheckpointResult<Option<Vec<u8>>> {
            self.inner.get_raw(k).await
        }
        async fn put_raw(
            &self,
            k: &str,
            v: &[u8],
        ) -> checkpoints::CheckpointResult<()> {
            if self.ok.load(SeqCst) == 0 {
                return Err(checkpoints::CheckpointError::Database(
                    "injected crash".into(),
                ));
            }
            self.ok.fetch_sub(1, SeqCst);
            self.inner.put_raw(k, v).await
        }
        async fn delete(&self, k: &str) -> checkpoints::CheckpointResult<bool> {
            self.inner.delete(k).await
        }
        async fn list(&self) -> checkpoints::CheckpointResult<Vec<String>> {
            self.inner.list().await
        }
        async fn list_with_prefix(
            &self,
            p: &str,
        ) -> checkpoints::CheckpointResult<Vec<String>> {
            self.inner.list_with_prefix(p).await
        }
    }

    #[tokio::test]
    async fn a_crash_part_way_is_finished_by_the_next_start() {
        let store = Arc::new(CrashAfter {
            inner: MemCheckpointStore::new().unwrap(),
            ok: AtomicUsize::new(usize::MAX),
        });
        for (k, l) in [("a", "0/100"), ("b", "0/200"), ("c", "0/300")] {
            store
                .put_raw(&format!("orders::sink::{k}"), &legacy(l))
                .await
                .unwrap();
        }
        store.ok.store(1, SeqCst);
        assert!(matches!(
            adopt_into_chain(store.as_ref(), "orders", &record(0)).await,
            Err(AdoptionError::Store(_))
        ));
        let stamped_now = {
            let mut n = 0;
            for k in ["a", "b", "c"] {
                if json(store.as_ref(), &format!("orders::sink::{k}")).await["chain"]
                    == "c"
                {
                    n += 1;
                }
            }
            n
        };
        assert_eq!(stamped_now, 1, "a crash after one rewrite");

        // The next start: the mix of stamped and unstamped is accepted and
        // the adoption finishes.
        store.ok.store(usize::MAX, SeqCst);
        assert_eq!(
            adopt_into_chain(store.as_ref(), "orders", &record(0))
                .await
                .unwrap(),
            2
        );
        for (k, l) in [("a", "0/100"), ("b", "0/200"), ("c", "0/300")] {
            assert_eq!(
                json(store.as_ref(), &format!("orders::sink::{k}")).await,
                stamped(l, "c", 0)
            );
        }
    }

    #[tokio::test]
    async fn one_unacceptable_candidate_blocks_every_rewrite() {
        let foreign = br#"{"lsn":"0/90","tx_id":null,"timeline":1,"chain":"other","transition":0}"#;
        let later = br#"{"lsn":"0/90","tx_id":null,"timeline":2,"chain":"c","transition":1}"#;
        let wrong_timeline = br#"{"lsn":"0/90","tx_id":null,"timeline":2,"chain":"c","transition":0}"#;
        let partial = br#"{"lsn":"0/90","tx_id":null,"chain":"c"}"#;
        let malformed = br#"{"lsn":"not-an-lsn","tx_id":null}"#;
        let garbage = b"not json";
        for bad in [
            &foreign[..],
            &later[..],
            &wrong_timeline[..],
            &partial[..],
            &malformed[..],
            &garbage[..],
        ] {
            let store = MemCheckpointStore::new().unwrap();
            store
                .put_raw("orders::sink::a", &legacy("0/100"))
                .await
                .unwrap();
            store.put_raw("orders::sink::b", bad).await.unwrap();
            store
                .put_raw("orders::sink::c", &legacy("0/300"))
                .await
                .unwrap();
            assert!(
                matches!(
                    adopt_into_chain(&store, "orders", &record(0)).await,
                    Err(AdoptionError::Refused(_))
                ),
                "{}",
                String::from_utf8_lossy(bad)
            );
            for (k, l) in [("a", "0/100"), ("c", "0/300")] {
                assert_eq!(
                    store.get_raw(&format!("orders::sink::{k}")).await.unwrap(),
                    Some(legacy(l)),
                    "nothing rewritten"
                );
            }
        }
    }

    #[tokio::test]
    async fn nothing_is_adopted_after_a_transition() {
        let store = MemCheckpointStore::new().unwrap();
        store
            .put_raw("orders::sink::a", &legacy("0/100"))
            .await
            .unwrap();
        assert_eq!(
            adopt_into_chain(&store, "orders", &record(1))
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            store.get_raw("orders::sink::a").await.unwrap(),
            Some(legacy("0/100"))
        );
    }
}

use anyhow::Result;
use async_trait::async_trait;
use checkpoints::{CheckpointStore, MemCheckpointStore};
use deltaforge_core::{
    Event, Op, Source, SourceHandle, SourceMeta, SourceResult,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::{
    select,
    sync::mpsc,
    task::JoinHandle,
    time::{Duration, Instant, sleep, timeout},
};
use tokio_util::sync::CancellationToken;

/// A tiny in-process source that emits a counter event every `period`,
/// and honors pause/resume/stop via the SourceHandle mechanisms.
///
#[derive(Clone)]
struct FakeSource {
    id: &'static str,
    tenant: &'static str,
    period: Duration,
}

#[async_trait]
impl Source for FakeSource {
    fn checkpoint_key(&self) -> &str {
        "fake-source"
    }
    async fn run(
        &self,
        tx: mpsc::Sender<Event>,
        _ckpt: Arc<dyn CheckpointStore>,
    ) -> SourceHandle {
        let cancel = CancellationToken::new();
        let paused = Arc::new(AtomicBool::new(false));
        let pause_notify = Arc::new(tokio::sync::Notify::new());

        let cancel_task = cancel.clone();
        let paused_task = paused.clone();
        let pause_notify_task = pause_notify.clone();
        let tenant = self.tenant.to_string();
        let period = self.period;
        let _id = self.id.to_string();

        let join: JoinHandle<SourceResult<()>> = tokio::spawn(async move {
            let mut n: u64 = 0;
            loop {
                // Handle cancellation
                if cancel_task.is_cancelled() {
                    break;
                }

                // Honor pause
                if paused_task.load(Ordering::SeqCst) {
                    // Wait until resumed or cancelled
                    select! {
                        _ = cancel_task.cancelled() => break,
                        _ = pause_notify_task.notified() => { /* resumed */ }
                    }
                    continue;
                }

                // Emit one event
                let ev = Event::new_row(
                    tenant.clone(),
                    SourceMeta {
                        kind: "fake".into(),
                        host: "local".into(),
                        db: "mem".into(),
                    },
                    "mem.ticks".into(),
                    Op::Insert,
                    None,
                    Some(serde_json::json!({ "n": n })),
                    // use wall clock; not important here
                    (chrono::Utc::now().timestamp_millis()) as i64,
                    10 as usize,
                );

                // If the receiver is dropped, end the task.
                if tx.send(ev).await.is_err() {
                    break;
                }
                n += 1;

                // Wait for the tick period or cancellation
                select! {
                    _ = cancel_task.cancelled() => break,
                    _ = sleep(period) => {}
                }
            }
            Ok(())
        });

        SourceHandle {
            cancel,
            paused,
            pause_notify,
            join,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_handle_pause_resume_stop_join() -> Result<()> {
    // arrange a fake source that ticks fast
    let src = FakeSource {
        id: "fake-1",
        tenant: "t1",
        period: Duration::from_millis(50),
    };
    let ckpt: Arc<dyn CheckpointStore> = Arc::new(MemCheckpointStore::new()?);
    let (tx, mut rx) = mpsc::channel::<Event>(128);

    // start and keep handle
    let handle = src.run(tx, ckpt).await;

    // 1) receive a couple of events under normal operation
    let mut seen = 0usize;
    let t_until = Instant::now() + Duration::from_millis(400);
    while Instant::now() < t_until && seen < 2 {
        if let Ok(Some(_e)) =
            timeout(Duration::from_millis(150), rx.recv()).await
        {
            seen += 1;
        }
    }
    assert!(seen >= 2, "should see at least 2 events before pause");

    // 2) pause and ensure no events arrive for a window
    handle.pause();
    assert!(handle.is_paused());
    let paused_window = Duration::from_millis(300);
    let paused_start = Instant::now();
    let mut paused_count = 0usize;
    while Instant::now() - paused_start < paused_window {
        match timeout(Duration::from_millis(150), rx.recv()).await {
            Ok(Some(_)) => paused_count += 1,
            Ok(None) => break, // sender closed (unexpected here)
            Err(_) => { /* no event within 150ms -> expected while paused */ }
        }
    }
    assert_eq!(paused_count, 0, "should not receive events while paused");

    // 3) Resume and ensure events start flowing again
    handle.resume();
    assert!(!handle.is_paused());
    let mut post_resume = 0usize;
    let resume_until = Instant::now() + Duration::from_millis(400);
    while Instant::now() < resume_until && post_resume < 2 {
        if let Ok(Some(_e)) =
            timeout(Duration::from_millis(200), rx.recv()).await
        {
            post_resume += 1;
        }
    }
    assert!(post_resume >= 1, "should get events after resume");

    // 4) stop and ensure the task exits; channel should eventually close
    handle.stop();

    // after stop, join should complete successfully in a bounded time
    let join_res = timeout(Duration::from_secs(5), handle.join()).await;
    assert!(join_res.is_ok(), "join() should complete after stop()");
    assert!(
        join_res.unwrap().is_ok(),
        "source task should return Ok(()) on clean stop"
    );

    // The sender is dropped by the task; rx should close
    let closed = timeout(Duration::from_millis(200), rx.recv()).await;
    match closed {
        Ok(None) => { /* channel closed as expected */ }
        Ok(Some(_)) => panic!("unexpected event after stop/join"),
        Err(_) => panic!("receiver did not close after stop/join"),
    }

    Ok(())
}

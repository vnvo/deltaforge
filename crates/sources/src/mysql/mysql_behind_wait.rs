//! The bounded wait before a capture is made again because an earlier one
//! started before the rows it is for: an activation window's deferred
//! tables (`admit_batch`), and the FULL fallback's live capture
//! ([`capture_at_or_after`]). For a window's deferred tables:
//! the server's position is read until it covers every recorded row start,
//! within [`BEHIND_WAIT`] in all (pool acquisition, queries and backoff),
//! or the window fails closed. The bound covers this readiness wait only,
//! not the whole recovery: the recapture that follows has its own budget
//! (the capture's lock wait, `capture_lock_wait_secs`).
//!
//! The position read is a readiness check, not proof: it does not show that
//! the recapture covers the rows, so a recapture still behind after a
//! covered wait fails closed too (`admit_batch`). Each read takes a control
//! connection and returns it only after a complete read; a read that fails,
//! or is dropped by cancellation or the bound, closes its connection, which
//! is never reused.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use deltaforge_core::CheckpointOrder;
use tokio_util::sync::CancellationToken;

use super::MySqlCheckpoint;
use super::mysql_binlog_scan::ProofError;
use super::mysql_control::{Class, ControlError, Link, Pool};
use super::mysql_session::SessionError;
use crate::durable_checkpoint::{
    WmPos, mysql_checkpoint_position, order_positions,
};

/// The readiness wait's bound (not the recapture's, which has its own): a
/// recovery ceiling, not a routine pause.
pub(crate) const BEHIND_WAIT: Duration = Duration::from_secs(5);
/// The first pause between reads, doubled after each up to [`MAX_BACKOFF`].
const FIRST_BACKOFF: Duration = Duration::from_millis(5);
const MAX_BACKOFF: Duration = Duration::from_millis(100);

/// Why the wait ended without the server's position covering the rows.
#[derive(Debug)]
pub(crate) enum WaitError {
    /// The bound passed; the last position read, if any.
    TimedOut {
        waited: Duration,
        last: Option<MySqlCheckpoint>,
    },
    Cancelled,
    /// Two row starts, or a row start and the server's position, cannot be
    /// ordered (or one cannot be parsed).
    Unordered(String),
    /// No control connection could be had.
    Control(ControlError),
    /// The position read failed.
    Read(ProofError),
}

impl WaitError {
    /// The outcome label of the wait's metrics.
    pub(crate) fn outcome(&self) -> &'static str {
        match self {
            Self::TimedOut { .. } => "timed_out",
            Self::Cancelled => "cancelled",
            Self::Unordered(_) => "unordered",
            Self::Control(_) | Self::Read(_) => "read_error",
        }
    }
}

fn pos(c: &MySqlCheckpoint) -> Result<WmPos, String> {
    mysql_checkpoint_position(&c.file, c.pos, c.gtid_set.as_deref())
        .ok_or_else(|| format!("unparseable position {c:?}"))
}

/// The one target that covers every row start: the latest, checked to be
/// at or after each of them. Two row starts that cannot be ordered: `Err`.
/// No row starts: `None`.
pub(crate) fn covering_target(
    targets: &[MySqlCheckpoint],
) -> Result<Option<WmPos>, String> {
    let ps = targets.iter().map(pos).collect::<Result<Vec<_>, _>>()?;
    let Some(mut latest) = ps.first().cloned() else {
        return Ok(None);
    };
    for p in &ps[1..] {
        if matches!(order_positions(&latest, p), CheckpointOrder::Before) {
            latest = p.clone();
        }
    }
    for p in &ps {
        if !matches!(
            order_positions(p, &latest),
            CheckpointOrder::Before | CheckpointOrder::Equal
        ) {
            return Err(format!(
                "the row starts {p:?} and {latest:?} cannot be ordered"
            ));
        }
    }
    Ok(Some(latest))
}

/// A position read on the connection.
pub(crate) type ReadFn<L> = for<'c> fn(
    &'c mut L,
    &'c ReadArgs,
) -> Pin<
    Box<dyn Future<Output = Result<MySqlCheckpoint, ProofError>> + Send + 'c>,
>;

/// What a position read needs besides its connection.
pub(crate) struct ReadArgs {
    pub gtid_mode: bool,
    pub lineage: String,
}

/// One position read on a connection of `pool`, waiting at most `wait` for
/// it. The connection goes back to the pool only after a complete read:
/// on a failed read it is closed, and dropped mid-read (cancellation, the
/// bound) it is closed by [`super::mysql_control::Checked`]'s drop.
pub(crate) async fn read_once<L, F, Fut>(
    pool: &Pool<L>,
    ident: &str,
    wait: Duration,
    open: F,
    read: ReadFn<L>,
    args: &ReadArgs,
) -> Result<MySqlCheckpoint, WaitError>
where
    L: Link,
    F: FnOnce(Duration) -> Fut,
    Fut: Future<Output = Result<L, SessionError>>,
{
    let mut conn = pool
        .checkout(Class::Position, ident, wait, open)
        .await
        .map_err(WaitError::Control)?;
    let at = read(conn.link(), args).await.map_err(WaitError::Read)?;
    conn.release().await;
    Ok(at)
}

/// Read the server's position with `read` (given what is left of the
/// bound) until it covers every row start in `targets`, pausing with a
/// doubling backoff between reads, within `bound` in all and stopped by
/// `cancel`. Returns how long it waited.
pub(crate) async fn wait_covered<R, Fut>(
    targets: &[MySqlCheckpoint],
    cancel: &CancellationToken,
    bound: Duration,
    mut read: R,
) -> Result<Duration, WaitError>
where
    R: FnMut(Duration) -> Fut,
    Fut: Future<Output = Result<MySqlCheckpoint, WaitError>>,
{
    let t0 = tokio::time::Instant::now();
    let deadline = t0 + bound;
    let Some(target) =
        covering_target(targets).map_err(WaitError::Unordered)?
    else {
        return Ok(Duration::ZERO);
    };
    let mut last = None;
    let waiting = async {
        let mut backoff = FIRST_BACKOFF;
        loop {
            let left =
                deadline.saturating_duration_since(tokio::time::Instant::now());
            let at = read(left).await?;
            let server = pos(&at).map_err(WaitError::Unordered)?;
            match order_positions(&target, &server) {
                CheckpointOrder::Before | CheckpointOrder::Equal => {
                    return Ok(());
                }
                CheckpointOrder::After => {}
                CheckpointOrder::Incomparable => {
                    return Err(WaitError::Unordered(format!(
                        "the row start {target:?} and the server's position \
                         {server:?} cannot be ordered"
                    )));
                }
            }
            last = Some(at);
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    };
    let ended = tokio::select! {
        biased;
        _ = cancel.cancelled() => None,
        r = tokio::time::timeout_at(deadline, waiting) => Some(r),
    };
    match ended {
        None => Err(WaitError::Cancelled),
        Some(Ok(r)) => r.map(|()| t0.elapsed()),
        Some(Err(_)) => Err(WaitError::TimedOut {
            waited: t0.elapsed(),
            last,
        }),
    }
}

/// Why a capture that must be at or after a row start is not.
#[derive(Debug)]
pub(crate) enum NotAfter<E> {
    /// A capture failed, with its own error.
    Capture(E),
    /// The row start and a capture's position cannot be ordered (or one
    /// cannot be parsed).
    Unordered(String),
    /// The wait for the server's position ended without covering the row
    /// start.
    Wait(WaitError),
    /// After a covered wait, the recapture's position is still before the
    /// row start.
    StillBefore(MySqlCheckpoint),
}

/// A capture (by `capture`, with its position) at or after the row start
/// `row`. A capture ordered before it (the server's executed set lagged the
/// stream) is not used: the server's position is read with `read` until it
/// covers `row`, within `bound` and stopped by `cancel` ([`wait_covered`]),
/// and the capture is made once more. Anything else ends it, never used: an
/// unordered position, a wait that ends otherwise, or a recapture still
/// before `row`.
pub(crate) async fn capture_at_or_after<T, E, C, CFut, R, RFut>(
    row: &MySqlCheckpoint,
    cancel: &CancellationToken,
    bound: Duration,
    mut capture: C,
    read: R,
) -> Result<T, NotAfter<E>>
where
    C: FnMut() -> CFut,
    CFut: Future<Output = Result<(T, MySqlCheckpoint), E>>,
    R: FnMut(Duration) -> RFut,
    RFut: Future<Output = Result<MySqlCheckpoint, WaitError>>,
{
    let row_at = pos(row).map_err(NotAfter::Unordered)?;
    let order = |at: &MySqlCheckpoint| -> Result<CheckpointOrder, String> {
        Ok(order_positions(&row_at, &pos(at)?))
    };
    let (t, at) = capture().await.map_err(NotAfter::Capture)?;
    match order(&at).map_err(NotAfter::Unordered)? {
        CheckpointOrder::Before | CheckpointOrder::Equal => return Ok(t),
        CheckpointOrder::After => {}
        CheckpointOrder::Incomparable => {
            return Err(NotAfter::Unordered(format!(
                "the row start {row:?} and the capture's position {at:?} \
                 cannot be ordered"
            )));
        }
    }
    wait_covered(std::slice::from_ref(row), cancel, bound, read)
        .await
        .map_err(NotAfter::Wait)?;
    let (t, at) = capture().await.map_err(NotAfter::Capture)?;
    match order(&at).map_err(NotAfter::Unordered)? {
        CheckpointOrder::Before | CheckpointOrder::Equal => Ok(t),
        CheckpointOrder::After => Err(NotAfter::StillBefore(at)),
        CheckpointOrder::Incomparable => Err(NotAfter::Unordered(format!(
            "the row start {row:?} and the recapture's position {at:?} \
             cannot be ordered"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mysql::mysql_control::budget;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const U: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
    const V: &str = "4e11fa47-71ca-11e1-9e33-c80aa9429562";

    fn at(set: &str) -> MySqlCheckpoint {
        MySqlCheckpoint {
            file: "mysql-bin.000001".into(),
            pos: 4,
            gtid_set: Some(set.into()),
            lineage: None,
            snapshot_completed: None,
            snapshot_chain: None,
        }
    }

    fn u(k: u64) -> MySqlCheckpoint {
        at(&format!("{U}:1-{k}"))
    }

    #[test]
    fn the_covering_target_is_the_latest_when_every_start_is_before_it() {
        assert_eq!(covering_target(&[]).unwrap(), None);
        assert_eq!(
            covering_target(&[u(51), u(57), u(53)]).unwrap(),
            Some(pos(&u(57)).unwrap())
        );
        let other = at(&format!("{U}:1-50,{V}:1"));
        assert!(covering_target(&[u(51), other]).is_err(), "unordered");
    }

    /// Positions read in turn (the last repeats), each read taking `query`.
    fn scripted(
        script: Vec<MySqlCheckpoint>,
        query: Duration,
        reads: Arc<AtomicUsize>,
        lefts: Arc<Mutex<Vec<Duration>>>,
    ) -> impl FnMut(
        Duration,
    ) -> Pin<
        Box<dyn Future<Output = Result<MySqlCheckpoint, WaitError>> + Send>,
    > {
        move |left| {
            let n = reads.fetch_add(1, Ordering::SeqCst);
            lefts.lock().unwrap().push(left);
            let p = script[n.min(script.len() - 1)].clone();
            Box::pin(async move {
                tokio::time::sleep(query).await;
                Ok(p)
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_wait_ends_when_the_server_covers_every_row_start() {
        let reads = Arc::new(AtomicUsize::new(0));
        let lefts = Arc::new(Mutex::new(Vec::new()));
        let waited = wait_covered(
            &[u(51), u(53)],
            &CancellationToken::new(),
            BEHIND_WAIT,
            scripted(
                vec![u(50), u(52), u(53)],
                Duration::ZERO,
                reads.clone(),
                lefts,
            ),
        )
        .await
        .unwrap();
        assert_eq!(reads.load(Ordering::SeqCst), 3, "covering both starts");
        assert_eq!(waited, Duration::from_millis(5 + 10), "backoff 5, 10 ms");
    }

    /// The bound covers the reads too, not only the pauses: each read takes
    /// 2 s, and the wait ends at 5 s with the last position read.
    #[tokio::test(start_paused = true)]
    async fn the_bound_covers_reads_and_backoff() {
        let reads = Arc::new(AtomicUsize::new(0));
        let lefts = Arc::new(Mutex::new(Vec::new()));
        let e = wait_covered(
            &[u(51)],
            &CancellationToken::new(),
            BEHIND_WAIT,
            scripted(
                vec![u(50)],
                Duration::from_secs(2),
                reads.clone(),
                lefts.clone(),
            ),
        )
        .await
        .unwrap_err();
        let WaitError::TimedOut { waited, last } = e else {
            panic!("{e:?}")
        };
        assert_eq!(waited, BEHIND_WAIT);
        assert_eq!(last, Some(u(50)));
        assert_eq!(reads.load(Ordering::SeqCst), 3, "at 0, 2.005, 4.015 s");
        let lefts = lefts.lock().unwrap();
        assert_eq!(lefts[0], BEHIND_WAIT, "each read is given what is left");
        assert!(lefts.windows(2).all(|w| w[1] < w[0]), "{lefts:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_position_that_cannot_be_ordered_fails_at_once() {
        let reads = Arc::new(AtomicUsize::new(0));
        let e = wait_covered(
            &[u(51)],
            &CancellationToken::new(),
            BEHIND_WAIT,
            scripted(
                vec![at(&format!("{V}:1-9"))],
                Duration::ZERO,
                reads.clone(),
                Arc::default(),
            ),
        )
        .await
        .unwrap_err();
        assert!(matches!(e, WaitError::Unordered(_)), "{e:?}");
        assert_eq!(e.outcome(), "unordered");
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        let e = wait_covered(
            &[u(51), at(&format!("{U}:1-50,{V}:1"))],
            &CancellationToken::new(),
            BEHIND_WAIT,
            scripted(
                vec![u(60)],
                Duration::ZERO,
                reads.clone(),
                Arc::default(),
            ),
        )
        .await
        .unwrap_err();
        assert!(matches!(e, WaitError::Unordered(_)), "{e:?}");
        assert_eq!(
            reads.load(Ordering::SeqCst),
            1,
            "unordered targets: no read"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_read_error_ends_the_wait_with_its_class() {
        let reads = Arc::new(AtomicUsize::new(0));
        let r = reads.clone();
        let e = wait_covered(
            &[u(51)],
            &CancellationToken::new(),
            BEHIND_WAIT,
            move |_| {
                r.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Err(WaitError::Read(ProofError::Capture(
                    "binary logging is off".into(),
                ))))
            },
        )
        .await
        .unwrap_err();
        assert!(
            matches!(e, WaitError::Read(ProofError::Capture(_))),
            "{e:?}"
        );
        assert_eq!(e.outcome(), "read_error");
        assert_eq!(reads.load(Ordering::SeqCst), 1, "not retried");
    }

    /// A connection that counts its closes and resets.
    struct Fake {
        closed: Arc<AtomicUsize>,
    }

    impl Link for Fake {
        fn reset(&mut self) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
            Box::pin(async { true })
        }
        fn alive(&mut self) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
            Box::pin(async { true })
        }
        fn close(self) -> Pin<Box<dyn Future<Output = ()> + Send>> {
            Box::pin(async move {
                self.closed.fetch_add(1, Ordering::SeqCst);
            })
        }
    }

    fn opener(
        opened: &Arc<AtomicUsize>,
        closed: &Arc<AtomicUsize>,
    ) -> impl FnOnce(Duration) -> std::future::Ready<Result<Fake, SessionError>>
    {
        let (opened, closed) = (opened.clone(), closed.clone());
        move |_| {
            opened.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok(Fake { closed }))
        }
    }

    fn covered_read<'c>(
        _: &'c mut Fake,
        _: &'c ReadArgs,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<MySqlCheckpoint, ProofError>>
                + Send
                + 'c,
        >,
    > {
        Box::pin(async { Ok(u(60)) })
    }

    fn hung_read<'c>(
        _: &'c mut Fake,
        _: &'c ReadArgs,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<MySqlCheckpoint, ProofError>>
                + Send
                + 'c,
        >,
    > {
        Box::pin(std::future::pending())
    }

    fn failed_read<'c>(
        _: &'c mut Fake,
        _: &'c ReadArgs,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<MySqlCheckpoint, ProofError>>
                + Send
                + 'c,
        >,
    > {
        Box::pin(async {
            Err(ProofError::Read(
                crate::mysql::mysql_failure::Failure::Transport,
                "reset".into(),
            ))
        })
    }

    async fn settle() {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }

    const ARGS: ReadArgs = ReadArgs {
        gtid_mode: true,
        lineage: String::new(),
    };

    /// A completed read returns its connection to the pool (released
    /// between reads); a failed one, or one cancelled or cut by the bound
    /// in the middle of its query, is closed, never returned.
    #[tokio::test(start_paused = true)]
    async fn only_a_complete_read_returns_its_connection() {
        let (opened, closed): (Arc<AtomicUsize>, Arc<AtomicUsize>) =
            (Arc::default(), Arc::default());
        let pool: Pool<Fake> = Pool::detached(budget(), "behind-wait", 2);
        let read = |r: ReadFn<Fake>| {
            let pool = pool.clone();
            let (o, c) = (opened.clone(), closed.clone());
            move |left: Duration| {
                let pool = pool.clone();
                let (o, c) = (o.clone(), c.clone());
                async move {
                    read_once(&pool, "a", left, opener(&o, &c), r, &ARGS).await
                }
            }
        };

        let cancel = CancellationToken::new();
        wait_covered(&[u(51)], &cancel, BEHIND_WAIT, read(covered_read))
            .await
            .unwrap();
        settle().await;
        assert!(pool.has_idle(), "returned after a complete read");
        assert_eq!(closed.load(Ordering::SeqCst), 0);

        // Cancelled in the middle of its query.
        let stop = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            stop.cancel();
        });
        let e = wait_covered(&[u(51)], &cancel, BEHIND_WAIT, read(hung_read))
            .await
            .unwrap_err();
        assert!(matches!(e, WaitError::Cancelled), "{e:?}");
        settle().await;
        assert!(!pool.has_idle(), "not returned");
        assert_eq!(closed.load(Ordering::SeqCst), 1, "closed");
        assert_eq!(pool.open(), 0);

        // Cut by the bound in the middle of its query.
        let e = wait_covered(
            &[u(51)],
            &CancellationToken::new(),
            BEHIND_WAIT,
            read(hung_read),
        )
        .await
        .unwrap_err();
        assert!(matches!(e, WaitError::TimedOut { .. }), "{e:?}");
        settle().await;
        assert!(!pool.has_idle());
        assert_eq!(closed.load(Ordering::SeqCst), 2);
        assert_eq!(pool.open(), 0);

        // A failed read.
        let e = wait_covered(
            &[u(51)],
            &CancellationToken::new(),
            BEHIND_WAIT,
            read(failed_read),
        )
        .await
        .unwrap_err();
        assert!(matches!(e, WaitError::Read(_)), "{e:?}");
        settle().await;
        assert!(!pool.has_idle());
        assert_eq!(closed.load(Ordering::SeqCst), 3);
        assert_eq!(pool.open(), 0);
        // The cancelled read reused the connection the first one returned.
        assert_eq!(opened.load(Ordering::SeqCst), 3);
    }

    /// Waiting for a connection counts against the bound: with the pool
    /// taken, the wait ends at the bound.
    #[tokio::test(start_paused = true)]
    async fn waiting_for_a_connection_counts_against_the_bound() {
        let (opened, closed): (Arc<AtomicUsize>, Arc<AtomicUsize>) =
            (Arc::default(), Arc::default());
        let pool: Pool<Fake> = Pool::detached(budget(), "behind-wait-busy", 1);
        let _held = pool
            .checkout(
                Class::Capture,
                "a",
                BEHIND_WAIT,
                opener(&opened, &closed),
            )
            .await
            .unwrap();
        let t0 = tokio::time::Instant::now();
        let e = wait_covered(
            &[u(51)],
            &CancellationToken::new(),
            BEHIND_WAIT,
            |left| {
                let pool = pool.clone();
                let (o, c) = (opened.clone(), closed.clone());
                async move {
                    read_once(
                        &pool,
                        "a",
                        left,
                        opener(&o, &c),
                        covered_read,
                        &ARGS,
                    )
                    .await
                }
            },
        )
        .await
        .unwrap_err();
        assert_eq!(t0.elapsed(), BEHIND_WAIT);
        assert!(
            matches!(
                e,
                WaitError::TimedOut { .. }
                    | WaitError::Control(ControlError::Saturated { .. })
            ),
            "{e:?}"
        );
    }

    /// Captures made in turn (the last repeats): each returns its number
    /// (from 1) and its position.
    fn captures(
        script: Vec<MySqlCheckpoint>,
        made: Arc<AtomicUsize>,
    ) -> impl FnMut() -> std::future::Ready<
        Result<(usize, MySqlCheckpoint), &'static str>,
    > {
        move || {
            let n = made.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok((
                n + 1,
                script[n.min(script.len() - 1)].clone(),
            )))
        }
    }

    /// A capture at or after the row start is used as it is: no read.
    #[tokio::test(start_paused = true)]
    async fn a_capture_at_or_after_the_row_is_used_without_a_wait() {
        for p in [u(51), u(60)] {
            let (made, reads) = (Arc::default(), Arc::new(AtomicUsize::new(0)));
            let got = capture_at_or_after(
                &u(51),
                &CancellationToken::new(),
                BEHIND_WAIT,
                captures(vec![p], Arc::clone(&made)),
                scripted(
                    vec![u(60)],
                    Duration::ZERO,
                    reads.clone(),
                    Arc::default(),
                ),
            )
            .await
            .unwrap();
            assert_eq!(got, 1);
            assert_eq!(made.load(Ordering::SeqCst), 1);
            assert_eq!(reads.load(Ordering::SeqCst), 0);
        }
    }

    /// A capture before the row start waits for the server's position to
    /// cover it, then captures once more and uses that capture.
    #[tokio::test(start_paused = true)]
    async fn a_capture_before_the_row_waits_then_recaptures() {
        let (made, reads) = (Arc::default(), Arc::new(AtomicUsize::new(0)));
        let got = capture_at_or_after(
            &u(51),
            &CancellationToken::new(),
            BEHIND_WAIT,
            captures(vec![u(50), u(53)], Arc::clone(&made)),
            scripted(
                vec![u(50), u(53)],
                Duration::ZERO,
                reads.clone(),
                Arc::default(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(got, 2, "the recapture is used");
        assert_eq!(made.load(Ordering::SeqCst), 2);
        assert_eq!(reads.load(Ordering::SeqCst), 2, "until covered");
    }

    /// After a covered wait, a recapture still before the row start ends
    /// it: two captures in all, never a third.
    #[tokio::test(start_paused = true)]
    async fn a_recapture_still_before_the_row_fails() {
        let made = Arc::new(AtomicUsize::new(0));
        let e = capture_at_or_after(
            &u(51),
            &CancellationToken::new(),
            BEHIND_WAIT,
            captures(vec![u(50)], Arc::clone(&made)),
            scripted(
                vec![u(60)],
                Duration::ZERO,
                Arc::default(),
                Arc::default(),
            ),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&e, NotAfter::StillBefore(p) if *p == u(50)),
            "{e:?}"
        );
        assert_eq!(made.load(Ordering::SeqCst), 2);
    }

    /// A server that never covers the row start: it ends at the bound,
    /// with no recapture.
    #[tokio::test(start_paused = true)]
    async fn a_wait_that_times_out_does_not_recapture() {
        let made = Arc::new(AtomicUsize::new(0));
        let t0 = tokio::time::Instant::now();
        let e = capture_at_or_after(
            &u(51),
            &CancellationToken::new(),
            BEHIND_WAIT,
            captures(vec![u(50)], Arc::clone(&made)),
            scripted(
                vec![u(50)],
                Duration::ZERO,
                Arc::default(),
                Arc::default(),
            ),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(e, NotAfter::Wait(WaitError::TimedOut { .. })),
            "{e:?}"
        );
        assert_eq!(t0.elapsed(), BEHIND_WAIT);
        assert_eq!(made.load(Ordering::SeqCst), 1);
    }

    /// A stop during the wait ends it at once, with no recapture.
    #[tokio::test(start_paused = true)]
    async fn a_stop_during_the_wait_does_not_recapture() {
        let made = Arc::new(AtomicUsize::new(0));
        let cancel = CancellationToken::new();
        let stop = cancel.clone();
        let t0 = tokio::time::Instant::now();
        let row = u(51);
        let (e, ()) = tokio::join!(
            capture_at_or_after(
                &row,
                &cancel,
                BEHIND_WAIT,
                captures(vec![u(50)], Arc::clone(&made)),
                scripted(
                    vec![u(50)],
                    Duration::ZERO,
                    Arc::default(),
                    Arc::default()
                ),
            ),
            async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                stop.cancel();
            }
        );
        let e = e.unwrap_err();
        assert!(matches!(e, NotAfter::Wait(WaitError::Cancelled)), "{e:?}");
        assert_eq!(t0.elapsed(), Duration::from_secs(1));
        assert_eq!(made.load(Ordering::SeqCst), 1);
    }

    /// A capture whose position cannot be ordered against the row start
    /// ends it at once: no wait, no recapture. So does a failed capture.
    #[tokio::test(start_paused = true)]
    async fn an_unordered_capture_or_a_failed_one_ends_it_at_once() {
        let (made, reads) = (Arc::default(), Arc::new(AtomicUsize::new(0)));
        let e = capture_at_or_after(
            &u(51),
            &CancellationToken::new(),
            BEHIND_WAIT,
            captures(vec![at(&format!("{U}:1-50,{V}:1"))], Arc::clone(&made)),
            scripted(
                vec![u(60)],
                Duration::ZERO,
                reads.clone(),
                Arc::default(),
            ),
        )
        .await
        .unwrap_err();
        assert!(matches!(e, NotAfter::Unordered(_)), "{e:?}");
        assert_eq!(made.load(Ordering::SeqCst), 1);
        assert_eq!(reads.load(Ordering::SeqCst), 0);

        let e = capture_at_or_after(
            &u(51),
            &CancellationToken::new(),
            BEHIND_WAIT,
            || std::future::ready(Err::<((), MySqlCheckpoint), _>("lock wait")),
            scripted(
                vec![u(60)],
                Duration::ZERO,
                reads.clone(),
                Arc::default(),
            ),
        )
        .await
        .unwrap_err();
        assert!(matches!(e, NotAfter::Capture("lock wait")), "{e:?}");
        assert_eq!(reads.load(Ordering::SeqCst), 0);
    }
}

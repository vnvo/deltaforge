//! The CDC stream's reader thread.
//!
//! Reading and parsing binlog events is a third of the source's work per
//! row, and it needs nothing of the run: only the replication session. A
//! [`ReadAhead`] moves the session to a thread of its own, which reads and
//! parses ahead of the run and hands the events over in batches, so the
//! run's thread spends its time on what only it can do.
//!
//! The reader is bounded twice: by count ([`AHEAD`] handovers of at most
//! [`BATCH`] events) and by bytes ([`AHEAD_BYTES`] of the events' binlog
//! lengths, beside the one being read), so a run of large events (a
//! compressed transaction can be hundreds of MiB) waits rather than piles
//! up; an event larger than the whole budget goes alone.
//!
//! What the reader holds is part of the stream, as the socket's buffer is:
//! it is delivered in order, an error comes after the events read before
//! it and ends the reader, and what is not taken goes when the stream is
//! dropped (a reconnect or a rotation resumes from a proven boundary, never
//! from what the reader held). Dropping the [`ReadAhead`] stops the reader
//! and returns only once the session is dropped, so the session's
//! connection is gone when the stream is.
//!
//! The session reads through the connector's own reactor (async-std), not
//! the run's runtime, so it can be read from any thread.

use std::collections::VecDeque;
use std::future::Future;

use mysql_binlog_connector_rust::binlog_error::BinlogError;
use mysql_binlog_connector_rust::event::{
    event_data::EventData, event_header::EventHeader,
};
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

/// At most this many events per handover.
const BATCH: usize = 64;
/// Handovers the reader may be ahead by: at most `AHEAD * BATCH` events.
const AHEAD: usize = 8;
/// The binlog bytes of the events the reader may hold.
const AHEAD_BYTES: usize = 8 * 1024 * 1024;

/// One parsed event (or the error that ended the stream).
pub(crate) type Event = (EventHeader, EventData);

/// What the reader thread reads from: the replication session.
pub(crate) trait EventStream: Send + 'static {
    /// The next event. Cancel-safe: a read dropped mid-way loses nothing.
    fn read(&mut self) -> impl Future<Output = Result<Event, BinlogError>>;
    /// Shut the connection down in both directions.
    fn close(&mut self) -> impl Future<Output = Result<(), BinlogError>>;
    /// The parser's table maps: installed so far, held now.
    fn table_maps(&self) -> (u64, usize);
}

impl EventStream for super::mysql_session::Session {
    fn read(&mut self) -> impl Future<Output = Result<Event, BinlogError>> {
        std::ops::DerefMut::deref_mut(self).read()
    }
    fn close(&mut self) -> impl Future<Output = Result<(), BinlogError>> {
        std::ops::DerefMut::deref_mut(self).close()
    }
    fn table_maps(&self) -> (u64, usize) {
        (
            self.parser.table_maps_installed,
            self.parser.table_map_event_by_table_id.len(),
        )
    }
}

/// A read, the parser's table maps right after it, and its share of the
/// byte budget (returned when the read is taken).
struct Read {
    event: Result<Event, BinlogError>,
    table_maps: (u64, usize),
    _bytes: Option<OwnedSemaphorePermit>,
}

/// The CDC stream, read on a thread of its own.
pub(crate) struct ReadAhead {
    batches: mpsc::Receiver<Vec<Read>>,
    held: VecDeque<Read>,
    close: Option<mpsc::Sender<oneshot::Sender<Result<(), BinlogError>>>>,
    stop: CancellationToken,
    thread: Option<std::thread::JoinHandle<()>>,
    table_maps: (u64, usize),
}

impl ReadAhead {
    /// Start reading `stream` on its own thread.
    pub(crate) fn spawn<S: EventStream>(stream: S) -> Self {
        Self::bounded(stream, AHEAD_BYTES)
    }

    fn bounded<S: EventStream>(stream: S, budget: usize) -> Self {
        let (tx, batches) = mpsc::channel(AHEAD);
        let (close, closes) = mpsc::channel(1);
        let stop = CancellationToken::new();
        let thread = {
            let stop = stop.clone();
            std::thread::Builder::new()
                .name("df-binlog-read".into())
                .spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .build()
                        .expect("the binlog reader's runtime");
                    rt.block_on(read_ahead(stream, tx, closes, stop, budget));
                })
                .expect("the binlog reader thread")
        };
        Self {
            batches,
            held: VecDeque::new(),
            close: Some(close),
            stop,
            thread: Some(thread),
            table_maps: (0, 0),
        }
    }

    /// The next event. Cancel-safe: what was handed over stays held.
    pub(crate) async fn read(&mut self) -> Result<Event, BinlogError> {
        if self.held.is_empty() {
            match self.batches.recv().await {
                Some(batch) => self.held.extend(batch),
                None => return Err(ended()),
            }
        }
        let read = self.held.pop_front().expect("a batch is never empty");
        self.table_maps = read.table_maps;
        read.event
    }

    /// The parser's table maps as of the last event read: installed so far,
    /// held now.
    pub(crate) fn table_maps(&self) -> (u64, usize) {
        self.table_maps
    }

    /// Shut the connection down in both directions (confirmed), then stop
    /// the reader.
    pub(crate) async fn close(&mut self) -> Result<(), BinlogError> {
        let Some(close) = self.close.take() else {
            return Err(ended());
        };
        let (reply, done) = oneshot::channel();
        if close.send(reply).await.is_err() {
            return Err(ended());
        }
        done.await.unwrap_or_else(|_| Err(ended()))
    }
}

impl Drop for ReadAhead {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(thread) = self.thread.take() {
            // Prompt: the reader waits on `stop` wherever it waits.
            let _ = thread.join();
        }
    }
}

fn ended() -> BinlogError {
    BinlogError::UnexpectedData("the binlog reader has ended".into())
}

async fn read_ahead<S: EventStream>(
    mut stream: S,
    tx: mpsc::Sender<Vec<Read>>,
    mut closes: mpsc::Receiver<oneshot::Sender<Result<(), BinlogError>>>,
    stop: CancellationToken,
    budget: usize,
) {
    let bytes = Arc::new(Semaphore::new(budget));
    // An event's share: its binlog length, at most the whole budget.
    let share = |event: &Result<Event, BinlogError>| match event {
        Ok((header, _)) => (header.event_length as usize).clamp(1, budget),
        Err(_) => 0,
    };
    // An event read but over budget: the next handover starts with it.
    let mut carried: Option<(Result<Event, BinlogError>, (u64, usize))> = None;
    loop {
        let (first, table_maps) = match carried.take() {
            Some(read) => read,
            None => {
                let event = tokio::select! {
                    biased;
                    () = stop.cancelled() => return,
                    Some(reply) = closes.recv() => {
                        let _ = reply.send(stream.close().await);
                        return;
                    }
                    event = stream.read() => event,
                };
                (event, stream.table_maps())
            }
        };
        // The first event waits for its share (or for the stop, or a close).
        let want = share(&first);
        let permit = if want == 0 {
            None
        } else {
            tokio::select! {
                biased;
                () = stop.cancelled() => return,
                Some(reply) = closes.recv() => {
                    let _ = reply.send(stream.close().await);
                    return;
                }
                p = Arc::clone(&bytes).acquire_many_owned(want as u32) => {
                    Some(p.expect("the budget is never closed"))
                }
            }
        };
        let mut failed = first.is_err();
        let mut batch = vec![Read {
            event: first,
            table_maps,
            _bytes: permit,
        }];
        // What is already received, and fits, joins it without waiting.
        while !failed && batch.len() < BATCH {
            let ready = tokio::select! {
                biased;
                event = stream.read() => Some(event),
                () = std::future::ready(()) => None,
            };
            let Some(event) = ready else { break };
            let table_maps = stream.table_maps();
            let want = share(&event);
            let permit = if want == 0 {
                None
            } else {
                match Arc::clone(&bytes).try_acquire_many_owned(want as u32) {
                    Ok(p) => Some(p),
                    Err(_) => {
                        carried = Some((event, table_maps));
                        break;
                    }
                }
            };
            failed = event.is_err();
            batch.push(Read {
                event,
                table_maps,
                _bytes: permit,
            });
        }
        tokio::select! {
            biased;
            () = stop.cancelled() => return,
            // A close while the run is not taking: what is held goes with it.
            Some(reply) = closes.recv() => {
                let _ = reply.send(stream.close().await);
                return;
            }
            sent = tx.send(batch) => if sent.is_err() { return },
        }
        if failed {
            // An error ends the stream; it is never read again.
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    /// Events numbered by `timestamp`; `fail_at` ends the stream with an
    /// error; after `events` the read waits forever.
    struct Fake {
        next: u32,
        length: u32,
        events: u32,
        fail_at: Option<u32>,
        reads: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
        closed: Arc<AtomicBool>,
    }

    impl Drop for Fake {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    impl EventStream for Fake {
        async fn read(&mut self) -> Result<Event, BinlogError> {
            if self.next >= self.events {
                std::future::pending::<()>().await;
            }
            self.next += 1;
            self.reads.fetch_add(1, Ordering::SeqCst);
            if Some(self.next) == self.fail_at {
                return Err(BinlogError::UnexpectedData("broken".into()));
            }
            let header = EventHeader {
                timestamp: self.next,
                event_type: 0,
                server_id: 1,
                event_length: self.length,
                next_event_position: 0,
                event_flags: 0,
            };
            Ok((header, EventData::NotSupported))
        }
        async fn close(&mut self) -> Result<(), BinlogError> {
            self.closed.store(true, Ordering::SeqCst);
            Ok(())
        }
        fn table_maps(&self) -> (u64, usize) {
            (u64::from(self.next), 1)
        }
    }

    struct Probe {
        reads: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
        closed: Arc<AtomicBool>,
    }

    fn fake(events: u32, fail_at: Option<u32>) -> (Fake, Probe) {
        sized(events, fail_at, 0)
    }

    fn sized(events: u32, fail_at: Option<u32>, length: u32) -> (Fake, Probe) {
        let probe = Probe {
            reads: Arc::default(),
            dropped: Arc::default(),
            closed: Arc::default(),
        };
        let f = Fake {
            next: 0,
            length,
            events,
            fail_at,
            reads: probe.reads.clone(),
            dropped: probe.dropped.clone(),
            closed: probe.closed.clone(),
        };
        (f, probe)
    }

    #[tokio::test]
    async fn events_arrive_in_order_with_their_table_maps() {
        let (f, _p) = fake(1000, None);
        let mut s = ReadAhead::spawn(f);
        for n in 1..=1000u32 {
            let (h, _) = s.read().await.unwrap();
            assert_eq!(h.timestamp, n);
            assert_eq!(s.table_maps(), (u64::from(n), 1));
        }
    }

    #[tokio::test]
    async fn an_error_follows_the_events_before_it_and_ends_the_stream() {
        // The error first in a handover, and after events in one.
        for fail_at in [1u32, 5] {
            let (f, _p) = fake(1000, Some(fail_at));
            let mut s = ReadAhead::spawn(f);
            for n in 1..fail_at {
                assert_eq!(s.read().await.unwrap().0.timestamp, n);
            }
            assert!(matches!(
                s.read().await,
                Err(BinlogError::UnexpectedData(m)) if m == "broken"
            ));
            assert!(
                matches!(s.read().await, Err(BinlogError::UnexpectedData(m)) if m.contains("ended"))
            );
        }
    }

    #[tokio::test]
    async fn the_reader_stays_within_its_bound() {
        let (f, p) = fake(100_000, None);
        let s = ReadAhead::spawn(f);
        tokio::time::sleep(Duration::from_millis(200)).await;
        // AHEAD handovers queued, one being sent, one being read.
        let bound = (AHEAD + 2) * BATCH;
        assert!(p.reads.load(Ordering::SeqCst) <= bound);
        drop(s);
    }

    #[tokio::test]
    async fn the_reader_holds_at_most_its_byte_budget() {
        // 100 KiB events against a 1 MiB budget: ten held, one being read.
        let (f, p) = sized(100_000, None, 100 * 1024);
        let mut s = ReadAhead::bounded(f, 1024 * 1024);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(p.reads.load(Ordering::SeqCst) <= 11);
        // Taking events frees their share: the reader goes on, in order.
        for n in 1..=200u32 {
            assert_eq!(s.read().await.unwrap().0.timestamp, n);
        }
        // An event larger than the whole budget still goes, alone.
        let (f, _p) = sized(3, None, 4 * 1024 * 1024);
        let mut s = ReadAhead::bounded(f, 1024 * 1024);
        for n in 1..=3u32 {
            assert_eq!(s.read().await.unwrap().0.timestamp, n);
        }
    }

    #[tokio::test]
    async fn close_is_answered_while_the_handover_is_full() {
        for length in [0, 100 * 1024] {
            let (f, p) = sized(100_000, None, length);
            let mut s = ReadAhead::bounded(f, 1024 * 1024);
            tokio::time::sleep(Duration::from_millis(100)).await;
            tokio::time::timeout(Duration::from_secs(5), s.close())
                .await
                .expect("close answered")
                .unwrap();
            assert!(p.closed.load(Ordering::SeqCst));
        }
    }

    #[tokio::test]
    async fn dropping_the_stream_drops_the_session_before_it_returns() {
        // Waiting on its socket, and waiting on a full handover.
        for events in [3, 100_000] {
            let (f, p) = fake(events, None);
            let s = ReadAhead::spawn(f);
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(s);
            assert!(p.dropped.load(Ordering::SeqCst));
        }
    }

    #[tokio::test]
    async fn close_shuts_the_session_down_and_ends_the_reader() {
        let (f, p) = fake(3, None);
        let mut s = ReadAhead::spawn(f);
        assert_eq!(s.read().await.unwrap().0.timestamp, 1);
        s.close().await.unwrap();
        assert!(p.closed.load(Ordering::SeqCst));
        drop(s);
        assert!(p.dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn a_cancelled_read_loses_nothing() {
        let (f, _p) = fake(200, None);
        let mut s = ReadAhead::spawn(f);
        let mut seen = Vec::new();
        while seen.len() < 200 {
            // A zero timeout cancels many reads mid-way.
            if let Ok(r) = tokio::time::timeout(Duration::ZERO, s.read()).await
            {
                seen.push(r.unwrap().0.timestamp);
            } else {
                tokio::task::yield_now().await;
            }
        }
        assert_eq!(seen, (1..=200).collect::<Vec<_>>());
    }
}

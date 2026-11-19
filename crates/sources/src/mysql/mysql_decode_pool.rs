//! Row decoding off the source task.
//!
//! The source task keeps everything ordered or stateful: reading, the
//! activation window, selection, checkpoints and commit boundaries. For a
//! rows event it builds the events without their row images ([`RowsJob`]:
//! the shells, the images as received and the table's decoder). Decoding
//! those images and filling the shells is a pure function of the decoder and
//! the bytes ([`RowsJob::fill`]), so it runs on worker threads, a batch of
//! items at a time. Finished batches are taken back strictly in the order
//! they were queued: the items leave the source exactly as they would
//! without workers.
//!
//! A batch that fails to decode yields the items before the failing rows
//! event and the error; nothing queued after it is ever handed back.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, mpsc};

use deltaforge_core::{CanonicalRow, Event, SourceItem, SourceResult};
use mysql_binlog_connector_rust::column::column_value::ColumnValue;

use super::mysql_decode::TableDecoder;

/// A rows event's images as received.
pub(crate) enum Images {
    /// Inserted rows: the image bitmap and each row's values.
    Insert {
        included: Vec<bool>,
        rows: Vec<Vec<ColumnValue>>,
    },
    /// Updated rows: before and after bitmaps, each row's before and after
    /// values.
    Update {
        before: Vec<bool>,
        after: Vec<bool>,
        rows: Vec<(Vec<ColumnValue>, Vec<ColumnValue>)>,
    },
    /// Deleted rows: the image bitmap and each row's values.
    Delete {
        included: Vec<bool>,
        rows: Vec<Vec<ColumnValue>>,
    },
}

impl Images {
    pub(crate) fn rows(&self) -> usize {
        match self {
            Images::Insert { rows, .. } | Images::Delete { rows, .. } => {
                rows.len()
            }
            Images::Update { rows, .. } => rows.len(),
        }
    }
}

/// One rows event: its events without row images, the images and the
/// table's decoder.
pub(crate) struct RowsJob {
    pub decoder: Arc<TableDecoder>,
    /// The source and `db.table`, for the decode failure's incident.
    pub source_id: String,
    pub table: String,
    pub images: Images,
    /// One event per row, in row order, with `before` / `after` unset.
    pub shells: Vec<Event>,
}

impl RowsJob {
    /// Decode every image and fill the shells. Every image is decoded before
    /// any event is returned, so a value that cannot be decoded yields none
    /// of the event's rows; for an update every before image is decoded
    /// before any after image. A failure is the `source_decode_failed`
    /// incident.
    pub(crate) fn fill(self) -> SourceResult<Vec<Event>> {
        let RowsJob {
            decoder,
            source_id,
            table,
            images,
            mut shells,
        } = self;
        let decode = |included: &[bool], values: &[ColumnValue]| {
            decode_row(&decoder, &source_id, &table, included, values)
        };
        match images {
            Images::Insert { included, rows } => {
                let afters = rows
                    .iter()
                    .map(|v| decode(&included, v))
                    .collect::<SourceResult<Vec<_>>>()?;
                for (ev, after) in shells.iter_mut().zip(afters) {
                    ev.after = Some(after);
                }
            }
            Images::Update {
                before,
                after,
                rows,
            } => {
                let befores = rows
                    .iter()
                    .map(|(b, _)| decode(&before, b))
                    .collect::<SourceResult<Vec<_>>>()?;
                let afters = rows
                    .iter()
                    .map(|(_, a)| decode(&after, a))
                    .collect::<SourceResult<Vec<_>>>()?;
                for ((ev, b), a) in shells.iter_mut().zip(befores).zip(afters) {
                    ev.before = Some(b);
                    ev.after = Some(a);
                }
            }
            Images::Delete { included, rows } => {
                let befores = rows
                    .iter()
                    .map(|v| decode(&included, v))
                    .collect::<SourceResult<Vec<_>>>()?;
                for (ev, before) in shells.iter_mut().zip(befores) {
                    ev.before = Some(before);
                }
            }
        }
        Ok(shells)
    }

    /// The decode failure this job's images hold, if any (the error
    /// [`Self::fill`] returns), without building events.
    pub(crate) fn decode_failure(
        decoder: &TableDecoder,
        source_id: &str,
        table: &str,
        images: &Images,
    ) -> Option<deltaforge_core::SourceError> {
        let check = |included: &[bool], values: &[ColumnValue]| {
            decode_row(decoder, source_id, table, included, values).err()
        };
        match images {
            Images::Insert { included, rows }
            | Images::Delete { included, rows } => {
                rows.iter().find_map(|v| check(included, v))
            }
            Images::Update {
                before,
                after,
                rows,
            } => rows
                .iter()
                .find_map(|(b, _)| check(before, b))
                .or_else(|| rows.iter().find_map(|(_, a)| check(after, a))),
        }
    }
}

/// One image decoded; a failure is the `source_decode_failed` incident.
fn decode_row(
    decoder: &TableDecoder,
    source_id: &str,
    table: &str,
    included: &[bool],
    values: &[ColumnValue],
) -> SourceResult<CanonicalRow> {
    decoder.decode_binlog_row(included, values).map_err(|e| {
        crate::incident_drafts::source_decode_failed(
            source_id,
            table,
            &e.column,
            &e.column_type,
            &e.reason,
        )
    })
}

/// An item of a batch: ready as it is, or a rows event to decode.
pub(crate) enum Pending {
    Ready(SourceItem),
    Rows(RowsJob),
}

/// A finished batch: its items in order, cut at the first rows event that
/// failed to decode (then with that failure).
pub(crate) struct Done {
    pub items: Vec<SourceItem>,
    pub failure: Option<deltaforge_core::SourceError>,
}

/// Decode a batch (a worker's whole job).
fn run_batch(batch: Vec<Pending>) -> Done {
    let mut items = Vec::with_capacity(batch.len());
    for p in batch {
        match p {
            Pending::Ready(item) => items.push(item),
            Pending::Rows(job) => match job.fill() {
                Ok(events) => {
                    items.extend(events.into_iter().map(SourceItem::event))
                }
                Err(e) => {
                    return Done {
                        items,
                        failure: Some(e),
                    };
                }
            },
        }
    }
    Done {
        items,
        failure: None,
    }
}

/// The worker threads and the batches between them and the source task.
pub(crate) struct DecodePool {
    jobs: mpsc::Sender<(u64, Vec<Pending>)>,
    done: tokio::sync::mpsc::UnboundedReceiver<(u64, Done)>,
    /// Finished out of order, waiting for an earlier batch.
    finished: BTreeMap<u64, Done>,
    next_seq: u64,
    next_out: u64,
    max_in_flight: usize,
}

impl DecodePool {
    /// `workers` threads (at least 1), at most `max_in_flight` batches
    /// queued or decoding (at least 1). The threads end when the pool is
    /// dropped (after the batch they are decoding, whose result is
    /// discarded).
    pub(crate) fn new(workers: usize, max_in_flight: usize) -> Self {
        let (jobs, rx) = mpsc::channel::<(u64, Vec<Pending>)>();
        let rx = Arc::new(Mutex::new(rx));
        let (done_tx, done) = tokio::sync::mpsc::unbounded_channel();
        for i in 0..workers.max(1) {
            let rx = Arc::clone(&rx);
            let done_tx = done_tx.clone();
            std::thread::Builder::new()
                .name(format!("df-decode-{i}"))
                .spawn(move || {
                    loop {
                        let next = rx.lock().map(|r| r.recv());
                        let Ok(Ok((seq, batch))) = next else {
                            return;
                        };
                        if done_tx.send((seq, run_batch(batch))).is_err() {
                            return;
                        }
                    }
                })
                .expect("spawn a decode worker");
        }
        Self {
            jobs,
            done,
            finished: BTreeMap::new(),
            next_seq: 0,
            next_out: 0,
            max_in_flight: max_in_flight.max(1),
        }
    }

    fn in_flight(&self) -> usize {
        (self.next_seq - self.next_out) as usize
    }

    /// No batch queued, decoding or waiting to be taken.
    pub(crate) fn is_idle(&self) -> bool {
        self.in_flight() == 0
    }

    /// Whether a batch can be queued without waiting.
    pub(crate) fn has_room(&self) -> bool {
        self.in_flight() < self.max_in_flight
    }

    /// Wait until a batch can be queued. Cancel-safe: a result received
    /// while waiting is kept.
    pub(crate) async fn room(&mut self) {
        while !self.has_room() {
            if self.finished.contains_key(&self.next_out) {
                // The oldest batch is done; it frees its room only when
                // taken, so the caller must take it.
                return;
            }
            self.receive().await;
        }
    }

    /// Queue a batch. The caller first waits for [`Self::room`]; queueing
    /// never waits.
    pub(crate) fn submit(&mut self, batch: Vec<Pending>) {
        let seq = self.next_seq;
        self.next_seq += 1;
        // The workers outlive every send: they end only with the pool.
        let _ = self.jobs.send((seq, batch));
    }

    /// The oldest batch, if it is finished; never waits.
    pub(crate) fn ready(&mut self) -> Option<Done> {
        while let Ok((seq, done)) = self.done.try_recv() {
            self.finished.insert(seq, done);
        }
        self.take()
    }

    /// The oldest batch, waiting for it; `None` when nothing is queued.
    /// Cancel-safe: a result received while waiting is kept.
    pub(crate) async fn next(&mut self) -> Option<Done> {
        loop {
            if self.is_idle() {
                return None;
            }
            if let Some(d) = self.take() {
                return Some(d);
            }
            self.receive().await;
        }
    }

    fn take(&mut self) -> Option<Done> {
        let d = self.finished.remove(&self.next_out)?;
        self.next_out += 1;
        Some(d)
    }

    async fn receive(&mut self) {
        match self.done.recv().await {
            Some((seq, done)) => {
                self.finished.insert(seq, done);
            }
            // Unreachable while the pool holds `jobs` (the workers hold
            // their senders until it is dropped); never spin.
            None => std::future::pending::<()>().await,
        }
    }
}

/// The default worker count: the cores beyond two (the source task and
/// the binlog reader), at most four.
pub(crate) fn default_workers() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().saturating_sub(2))
        .unwrap_or(0)
        .min(4)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mysql::mysql_table_schema::{MySqlColumn, MySqlTableSchema};
    use deltaforge_core::{EventId, Op, SourceInfo, SourcePosition};

    fn decoder() -> Arc<TableDecoder> {
        Arc::new(TableDecoder::new(&MySqlTableSchema::new(vec![
            MySqlColumn::new("id", "int", "int", false, 1),
            MySqlColumn::new("n", "bigint", "bigint", true, 2),
        ])))
    }

    fn shell(op: Op, row: u32) -> Event {
        let source = SourceInfo {
            version: "test".into(),
            connector: "mysql".into(),
            name: "p".into(),
            ts_ms: 0,
            db: "d".into(),
            schema: None,
            table: "t".into(),
            snapshot: None,
            position: SourcePosition::default(),
        };
        Event::new_row(
            EventId::mysql_row_gtid(&[7; 16], 1, 100, row),
            source,
            op,
            None,
            None,
            Default::default(),
            0,
            0,
        )
    }

    fn values(id: i32) -> Vec<ColumnValue> {
        vec![
            ColumnValue::Long(id),
            ColumnValue::LongLong(i64::from(id) * 10),
        ]
    }

    /// An insert of `ids`; a `None` row has no values (cannot be decoded).
    fn insert(ids: &[Option<i32>]) -> RowsJob {
        RowsJob {
            decoder: decoder(),
            source_id: "s".into(),
            table: "d.t".into(),
            images: Images::Insert {
                included: vec![true, true],
                rows: ids
                    .iter()
                    .map(|id| id.map(values).unwrap_or_default())
                    .collect(),
            },
            shells: (0..ids.len() as u32)
                .map(|r| shell(Op::Create, r))
                .collect(),
        }
    }

    fn marker(n: usize) -> Pending {
        Pending::Ready(SourceItem::TxBegin {
            tx_id: n.to_string(),
        })
    }

    /// Each item as text: a marker's id, a row's decoded `id`.
    fn names(items: &[SourceItem]) -> Vec<String> {
        items
            .iter()
            .map(|i| match i {
                SourceItem::TxBegin { tx_id } => tx_id.clone(),
                SourceItem::Event(e) => {
                    let row = e.after.as_ref().or(e.before.as_ref()).unwrap();
                    let Some(deltaforge_core::canonical::CanonicalValue::Db(
                        deltaforge_core::canonical::DbValue::Int {
                            value, ..
                        },
                    )) = row.get("id")
                    else {
                        panic!("id is not an integer: {row:?}")
                    };
                    format!("row {value}")
                }
                other => panic!("unexpected {other:?}"),
            })
            .collect()
    }

    /// A worker fills the same events the source task does inline.
    #[test]
    fn a_batch_decodes_as_its_rows_do_inline() {
        let inline = insert(&[Some(1), Some(2)]).fill().unwrap();
        let done = run_batch(vec![
            marker(0),
            Pending::Rows(insert(&[Some(1), Some(2)])),
        ]);
        assert!(done.failure.is_none());
        assert_eq!(done.items.len(), 3);
        for (got, want) in done.items[1..].iter().zip(&inline) {
            let SourceItem::Event(got) = got else {
                panic!("not an event")
            };
            assert_eq!(format!("{:?}", got.after), format!("{:?}", want.after));
            assert_eq!(got.event_id, want.event_id);
        }
    }

    /// A rows event that cannot be decoded cuts its batch there: the items
    /// before it and the failure, none of its rows, nothing after it.
    #[test]
    fn a_decode_failure_cuts_the_batch_before_its_rows() {
        let done = run_batch(vec![
            marker(0),
            Pending::Rows(insert(&[Some(1)])),
            Pending::Rows(insert(&[Some(2), None])),
            marker(3),
        ]);
        assert_eq!(names(&done.items), ["0", "row 1"].map(String::from));
        let failure = done.failure.expect("the second rows event fails");
        let inline = insert(&[Some(2), None]).fill().unwrap_err();
        assert_eq!(failure.to_string(), inline.to_string());
        assert!(failure.to_string().contains("value_not_decodable"));
    }

    /// An update decodes every before image before any after image: the
    /// failure is the first before image's, as inline.
    #[test]
    fn an_update_fails_on_its_first_undecodable_before_image() {
        let job = RowsJob {
            decoder: decoder(),
            source_id: "s".into(),
            table: "d.t".into(),
            images: Images::Update {
                before: vec![true, true],
                after: vec![true, true, true],
                rows: vec![(values(1), values(1)), (vec![], values(2))],
            },
            shells: vec![shell(Op::Update, 0), shell(Op::Update, 1)],
        };
        let want =
            RowsJob::decode_failure(&job.decoder, "s", "d.t", &job.images)
                .map(|e| format!("{e:?}"));
        let got = job.fill().err().map(|e| format!("{e:?}"));
        assert!(want.as_deref().unwrap().contains("missing the column"));
        assert_eq!(got, want);
    }

    /// Batches come back in the order they were queued, whichever worker
    /// finishes first.
    #[tokio::test]
    async fn batches_come_back_in_queue_order() {
        let mut pool = DecodePool::new(4, 64);
        for b in 0..40usize {
            pool.room().await;
            let rows = (0..(b % 7) as i32 * 50).map(Some).collect::<Vec<_>>();
            pool.submit(vec![marker(b), Pending::Rows(insert(&rows))]);
        }
        for b in 0..40usize {
            let done = pool.next().await.expect("a queued batch");
            assert!(done.failure.is_none());
            assert_eq!(names(&done.items[..1]), [b.to_string()]);
            assert_eq!(done.items.len(), 1 + (b % 7) * 50);
        }
        assert!(pool.next().await.is_none());
        assert!(pool.is_idle());
    }

    /// At most `max_in_flight` batches are queued: room waits for the
    /// oldest to finish.
    #[tokio::test]
    async fn room_is_bounded() {
        let mut pool = DecodePool::new(1, 2);
        pool.submit(vec![marker(0)]);
        pool.submit(vec![marker(1)]);
        assert!(!pool.has_room());
        pool.room().await;
        // The oldest is finished but not taken: still no room until it is.
        let first = match pool.ready() {
            Some(d) => d,
            None => pool.next().await.unwrap(),
        };
        assert_eq!(names(&first.items), ["0"]);
        assert!(pool.has_room());
    }

    mod outbox {
        use super::*;
        use crate::mysql::mysql_event::{Outbox, emit, flush, queue_rows};
        use tokio::sync::mpsc;

        fn received(rx: &mut mpsc::Receiver<SourceItem>) -> Vec<String> {
            let mut got = Vec::new();
            while let Ok(i) = rx.try_recv() {
                got.push(i);
            }
            names(&got)
        }

        fn ready(n: usize) -> SourceItem {
            SourceItem::TxBegin {
                tx_id: n.to_string(),
            }
        }

        /// Markers and rows across many batches reach the channel in the
        /// order they were emitted.
        #[tokio::test]
        async fn items_and_rows_keep_their_order_across_batches() {
            let (tx, mut rx) = mpsc::channel(100_000);
            let mut out = Outbox::with_workers(3);
            out.set_hold(true);
            let mut want = Vec::new();
            for t in 0..600usize {
                assert!(emit(&tx, &mut out, ready(t)).await);
                want.push(t.to_string());
                let ids: Vec<_> = (0..(t % 3) as i32)
                    .map(|r| Some(t as i32 * 10 + r))
                    .collect();
                want.extend(
                    ids.iter().map(|id| format!("row {}", id.unwrap())),
                );
                assert!(queue_rows(&tx, &mut out, insert(&ids)).await);
            }
            assert!(flush(&tx, &mut out).await);
            assert!(out.is_empty());
            assert!(out.take_failure().is_none());
            assert_eq!(received(&mut rx), want);
        }

        /// A rows event a worker cannot decode: everything before it is
        /// sent, none of its rows, nothing emitted after it; the failure is
        /// handed over once.
        #[tokio::test]
        async fn nothing_after_a_decode_failure_is_sent() {
            let (tx, mut rx) = mpsc::channel(1024);
            let mut out = Outbox::with_workers(2);
            out.set_hold(true);
            assert!(emit(&tx, &mut out, ready(0)).await);
            assert!(queue_rows(&tx, &mut out, insert(&[Some(1)])).await);
            assert!(queue_rows(&tx, &mut out, insert(&[Some(2), None])).await);
            assert!(emit(&tx, &mut out, ready(3)).await);
            assert!(queue_rows(&tx, &mut out, insert(&[Some(4)])).await);
            assert!(flush(&tx, &mut out).await);
            assert_eq!(received(&mut rx), ["0", "row 1"].map(String::from));
            assert!(out.take_failure().is_some());
            assert!(out.take_failure().is_none());
            assert!(!out.parallel());
            assert!(emit(&tx, &mut out, ready(5)).await);
            assert!(flush(&tx, &mut out).await);
            assert!(received(&mut rx).is_empty());
        }

        /// An item sent at once waits for every queued batch before it.
        #[tokio::test]
        async fn an_immediate_item_follows_queued_rows() {
            let (tx, mut rx) = mpsc::channel(1024);
            let mut out = Outbox::with_workers(2);
            out.set_hold(true);
            assert!(
                queue_rows(&tx, &mut out, insert(&[Some(1), Some(2)])).await
            );
            out.set_hold(false);
            assert!(emit(&tx, &mut out, ready(9)).await);
            assert_eq!(
                received(&mut rx),
                ["row 1", "row 2", "9"].map(String::from)
            );
        }
    }
}

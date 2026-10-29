//! MySQL control-plane connections (`docs/design/mysql-mass-invalidation.md`,
//! section 5): a runner-wide budget of `G` connections and, per source, a
//! pool of at most `P`.
//!
//! - **Permits:** a control-plane connection holds one permit of its source's
//!   pool and one of the runner's budget from before it opens until it has
//!   closed. `R` of the budget's `G` permits are the cancellation reserve
//!   ([`KILL_RESERVE`]): cancellations draw only on it, never on a pool or
//!   on ordinary capacity, so ordinary use has `G - R`.
//! - **Idle:** a returned connection is reset and kept as its source's one
//!   idle connection, for at most [`IDLE_TTL`]; any other is closed. When
//!   the budget has no permit free, every idle connection of the runner is
//!   closed at once to free permits.
//! - **Waiting:** acquisition is FIFO within a source and within the budget,
//!   bounded by the class's wait ([`Class`]). A wait that runs out is
//!   [`ControlError::Saturated`], counted by class. Health checks are one in
//!   flight per source.
//! - **Draining:** a rotation or failover [`Pool::drain`]s its source's
//!   pool: the idle connection closes at once, connections in use close when
//!   returned, and the drain waits, bounded, for them.

use std::collections::{BTreeMap, HashMap};

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tracing::warn;

use super::mysql_binlog_scan::KILL_RESERVE;
use super::mysql_session::SessionError;

/// The runner-wide budget when none is configured (`G`).
pub const DEFAULT_MAX_CONTROL_CONNECTIONS: u32 = 32;
/// The per-source pool when none is configured (`P`).
pub const DEFAULT_POOL_SIZE: usize = 4;
/// How long a source's idle connection is kept.
pub(crate) const IDLE_TTL: Duration = Duration::from_secs(10);
/// The bound on a drain's wait for its source's connections to close.
pub(crate) const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
/// The bound on checking that an idle connection still answers.
const ALIVE_TIMEOUT: Duration = Duration::from_secs(2);
/// The bound on opening one control connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// What a control-plane connection is for, which bounds its wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Class {
    /// A schema capture (window, baseline, drift): bounded by the capture's
    /// own lock-wait budget, which the caller passes.
    Capture,
    /// Schema-loader catalog reads.
    Catalog,
    /// Position and identity queries.
    Position,
    /// Health and preflight checks: one in flight per source.
    Health,
}

impl Class {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Capture => "capture",
            Self::Catalog => "catalog",
            Self::Position => "position",
            Self::Health => "health",
        }
    }

    /// The class's wait, for every class but [`Class::Capture`] (whose
    /// caller passes its lock-wait budget).
    pub(crate) fn wait(self) -> Duration {
        match self {
            Self::Capture | Self::Catalog => Duration::from_secs(30),
            Self::Position | Self::Health => Duration::from_secs(5),
        }
    }
}

/// Why no control-plane connection was given.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ControlError {
    /// No permit freed up within the class's wait.
    #[error(
        "the MySQL control plane stayed saturated for {waited:?} (a {class} connection waited for a {scope} permit)"
    )]
    Saturated {
        class: &'static str,
        /// `source` (the source's pool) or `runner` (the runner's budget).
        scope: &'static str,
        waited: Duration,
    },
    /// The connection could not be opened or is not the expected server.
    #[error("{0:?}")]
    Session(SessionError),
}

impl From<SessionError> for ControlError {
    fn from(e: SessionError) -> Self {
        Self::Session(e)
    }
}

impl ControlError {
    /// As a session error: saturation is a (retryable) connect failure.
    pub(crate) fn into_session(self) -> SessionError {
        match self {
            Self::Session(e) => e,
            saturated => SessionError::Connect(saturated.to_string()),
        }
    }

    /// The source error: identity failures are lineage errors, connect
    /// failures and saturation stay retryable connection errors.
    pub(crate) fn into_source_error(
        self,
        expected: &str,
    ) -> deltaforge_core::SourceError {
        match self {
            Self::Session(e) => e.into_source_error(expected),
            saturated => deltaforge_core::SourceError::Connect {
                details: saturated.to_string().into(),
            },
        }
    }
}

/// One open connection the pool manages.
pub(crate) trait Link: Send + 'static {
    /// Clear the session for its next user. `false`: it cannot be reused.
    fn reset(&mut self) -> Pin<Box<dyn Future<Output = bool> + Send + '_>>;
    /// Whether it still answers (an idle connection, before its reuse).
    fn alive(&mut self) -> Pin<Box<dyn Future<Output = bool> + Send + '_>>;
    /// Close it.
    fn close(self) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}

/// The runner-wide budget, and its pools' idle connections (for
/// eviction).
pub(crate) struct Budget {
    ordinary: Arc<Semaphore>,
    total: usize,
    pools: Mutex<Vec<Weak<dyn Evict>>>,
}

trait Evict: Send + Sync {
    /// Close the idle connection, if any. Whether there was one.
    fn evict(&self) -> bool;
}

impl Budget {
    /// A budget of `total` connections, [`KILL_RESERVE`] of them reserved
    /// for cancellation.
    pub(crate) fn new(total: usize) -> Self {
        Self {
            ordinary: Arc::new(Semaphore::new(total - KILL_RESERVE)),
            total,
            pools: Mutex::default(),
        }
    }

    /// The whole budget (`G`).
    #[cfg(test)]
    pub(crate) fn total(&self) -> usize {
        self.total
    }

    /// Ordinary permits free now.
    #[cfg(test)]
    pub(crate) fn available(&self) -> usize {
        self.ordinary.available_permits()
    }

    fn register(&self, pool: Weak<dyn Evict>) {
        let mut pools = self.pools.lock().expect("pools lock");
        pools.retain(|p| p.strong_count() > 0);
        pools.push(pool);
    }

    /// Close every idle connection of the runner. How many were closed.
    fn evict_idle(&self) -> usize {
        let pools: Vec<Arc<dyn Evict>> = {
            let mut pools = self.pools.lock().expect("pools lock");
            pools.retain(|p| p.strong_count() > 0);
            pools.iter().filter_map(Weak::upgrade).collect()
        };
        let evicted = pools.iter().filter(|p| p.evict()).count();
        if evicted > 0 {
            metrics::counter!("deltaforge_mysql_control_idle_evictions_total")
                .increment(evicted as u64);
        }
        evicted
    }
}

static BUDGET: OnceLock<Budget> = OnceLock::new();

/// Set the runner-wide budget once, at startup. `Err` below the
/// cancellation reserve plus one pool's least size, or on a second call
/// with another value.
pub fn configure(total: u32) -> Result<(), String> {
    let least = (KILL_RESERVE + 2) as u32;
    if total < least {
        return Err(format!(
            "max_mysql_control_connections must be at least {least} (the \
             cancellation reserve of {KILL_RESERVE} and one pool of 2), got {total}"
        ));
    }
    let set = BUDGET.get_or_init(|| Budget::new(total as usize));
    if set.total == total as usize {
        Ok(())
    } else {
        Err(format!(
            "max_mysql_control_connections is already {} for this process",
            set.total
        ))
    }
}

/// The runner's budget.
pub(crate) fn budget() -> &'static Budget {
    BUDGET.get_or_init(|| Budget::new(DEFAULT_MAX_CONTROL_CONNECTIONS as usize))
}

/// The pool's link of a connection: what it is and who it was opened for.
struct Entry<L> {
    link: L,
    /// Who it was opened for: reused only for the same.
    ident: String,
    epoch: u64,
    permits: Permits,
}

/// The two permits a connection holds until it has closed, and its count.
struct Permits {
    source: Option<OwnedSemaphorePermit>,
    debt: Arc<AtomicUsize>,
    _runner: OwnedSemaphorePermit,
    _open: Open,
}

/// A source's pool.
pub(crate) struct Pool<L: Link> {
    inner: Arc<Inner<L>>,
}

impl<L: Link> Clone for Pool<L> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

struct Inner<L: Link> {
    budget: &'static Budget,
    source: String,
    cap: Arc<Semaphore>,
    size: Mutex<usize>,
    /// Permits to forget as they return, after a shrink found too few free.
    debt: Arc<AtomicUsize>,
    idle: Mutex<Option<(Entry<L>, u64)>>,
    /// A connection went idle: a waiter for the source's permits looks.
    idled: Notify,
    /// Bumped by every idle store and take: a TTL expiry closes only the
    /// connection it was set for.
    idle_gen: AtomicU64,
    /// Bumped by a drain: connections of earlier epochs are never reused.
    epoch: AtomicU64,
    /// Connections open by epoch, and a notification of every close.
    open: Arc<(Mutex<BTreeMap<u64, usize>>, Notify)>,
    health: Arc<Semaphore>,
}

/// One open connection's count in its pool's epoch.
struct Open {
    open: Arc<(Mutex<BTreeMap<u64, usize>>, Notify)>,
    epoch: u64,
    _gauge: super::mysql_connections::Counted,
}

impl Drop for Open {
    fn drop(&mut self) {
        let (map, closed) = &*self.open;
        let mut map = map.lock().expect("open lock");
        if let Some(n) = map.get_mut(&self.epoch) {
            *n -= 1;
            if *n == 0 {
                map.remove(&self.epoch);
            }
        }
        drop(map);
        closed.notify_waiters();
    }
}

impl Drop for Permits {
    fn drop(&mut self) {
        // A shrink that found too few permits free takes this one back.
        if let Some(p) = self.source.take()
            && self
                .debt
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |d| {
                    d.checked_sub(1)
                })
                .is_ok()
        {
            p.forget();
        }
    }
}

impl<L: Link> Evict for Inner<L> {
    fn evict(&self) -> bool {
        let taken = self.take_idle();
        let found = taken.is_some();
        if let Some(e) = taken {
            close(e);
        }
        found
    }
}

impl<L: Link> Drop for Inner<L> {
    fn drop(&mut self) {
        if let Some((e, _)) = self.idle.get_mut().expect("idle lock").take() {
            close(e);
        }
    }
}

/// Close `e` and only then release its permits.
fn close<L: Link>(e: Entry<L>) {
    let Entry { link, permits, .. } = e;
    match tokio::runtime::Handle::try_current() {
        Ok(rt) => {
            rt.spawn(async move {
                link.close().await;
                drop(permits);
            });
        }
        Err(_) => {
            drop(link);
            drop(permits);
        }
    }
}

impl<L: Link> Inner<L> {
    fn take_idle(&self) -> Option<Entry<L>> {
        let taken = self.idle.lock().expect("idle lock").take();
        taken.map(|(e, _)| {
            self.idle_gen.fetch_add(1, Ordering::SeqCst);
            e
        })
    }

    /// The idle connection when it is reusable for `ident`; any other
    /// idle connection is closed.
    fn reuse(&self, ident: &str) -> Option<Entry<L>> {
        let e = self.take_idle()?;
        if !ident.is_empty()
            && e.ident == ident
            && e.epoch == self.epoch.load(Ordering::SeqCst)
        {
            Some(e)
        } else {
            close(e);
            None
        }
    }

    fn resize(&self, size: usize) {
        let mut cur = self.size.lock().expect("size lock");
        if size > *cur {
            let mut grow = size - *cur;
            // Settle a pending shrink first.
            while grow > 0 {
                let d = self.debt.load(Ordering::SeqCst);
                if d == 0 {
                    break;
                }
                let n = d.min(grow);
                if self
                    .debt
                    .compare_exchange(
                        d,
                        d - n,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    grow -= n;
                }
            }
            self.cap.add_permits(grow);
        } else if size < *cur {
            let shrink = *cur - size;
            let forgotten = self.cap.forget_permits(shrink);
            self.debt.fetch_add(shrink - forgotten, Ordering::SeqCst);
        }
        *cur = size;
    }
}

/// A connection checked out of a pool: returned with [`Checked::release`];
/// dropped, it is closed.
pub(crate) struct Checked<L: Link> {
    entry: Option<Entry<L>>,
    pool: Arc<Inner<L>>,
    _health: Option<OwnedSemaphorePermit>,
}

impl<L: Link> Checked<L> {
    pub(crate) fn link(&mut self) -> &mut L {
        &mut self.entry.as_mut().expect("checked out").link
    }

    /// Return the connection: reset and kept as the source's idle one, or
    /// closed (its source was drained, or an idle one is already kept, or
    /// the reset failed).
    pub(crate) async fn release(mut self) {
        let Some(mut e) = self.entry.take() else {
            return;
        };
        let pool = self.pool.clone();
        if e.ident.is_empty()
            || e.epoch != pool.epoch.load(Ordering::SeqCst)
            || !e.link.reset().await
        {
            close(e);
            return;
        }
        let gen_ = {
            let mut idle = pool.idle.lock().expect("idle lock");
            if idle.is_some() || e.epoch != pool.epoch.load(Ordering::SeqCst) {
                drop(idle);
                close(e);
                return;
            }
            let g = pool.idle_gen.fetch_add(1, Ordering::SeqCst) + 1;
            *idle = Some((e, g));
            g
        };
        pool.idled.notify_one();
        // The idle connection expires after its TTL, unless taken first.
        let weak = Arc::downgrade(&pool);
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.spawn(async move {
                tokio::time::sleep(IDLE_TTL).await;
                let Some(pool) = weak.upgrade() else { return };
                let expired = {
                    let mut idle = pool.idle.lock().expect("idle lock");
                    match &*idle {
                        Some((_, g)) if *g == gen_ => idle.take(),
                        _ => None,
                    }
                };
                if let Some((e, _)) = expired {
                    close(e);
                }
            });
        }
    }
}

impl<L: Link> Drop for Checked<L> {
    fn drop(&mut self) {
        if let Some(e) = self.entry.take() {
            close(e);
        }
    }
}

impl<L: Link> Pool<L> {
    fn new(budget: &'static Budget, source: &str, size: usize) -> Self {
        let pool = Self {
            inner: Arc::new(Inner {
                budget,
                source: source.to_string(),
                cap: Arc::new(Semaphore::new(size)),
                size: Mutex::new(size),
                debt: Arc::default(),
                idle: Mutex::default(),
                idled: Notify::new(),
                idle_gen: AtomicU64::new(0),
                epoch: AtomicU64::new(0),
                open: Arc::default(),
                health: Arc::new(Semaphore::new(1)),
            }),
        };
        let evict: Arc<dyn Evict> = pool.inner.clone();
        budget.register(Arc::downgrade(&evict));
        pool
    }

    /// A pool of its own on `budget` (tests).
    #[cfg(test)]
    pub(crate) fn detached(
        budget: &'static Budget,
        source: &str,
        size: usize,
    ) -> Self {
        Self::new(budget, source, size)
    }

    /// The source this pool is for.
    pub(crate) fn source(&self) -> &str {
        &self.inner.source
    }

    /// Connections open now (in use or idle).
    #[cfg(test)]
    pub(crate) fn open(&self) -> usize {
        self.inner.open.0.lock().expect("open lock").values().sum()
    }

    /// Whether an idle connection is kept.
    #[cfg(test)]
    pub(crate) fn has_idle(&self) -> bool {
        self.inner.idle.lock().expect("idle lock").is_some()
    }

    /// A connection for `ident` (what it connects to: reused only for the
    /// same), waiting at most `wait` for permits: the idle one when it
    /// fits, else a new one opened by `open` under a source and a runner
    /// permit.
    pub(crate) async fn checkout<F, Fut>(
        &self,
        class: Class,
        ident: &str,
        wait: Duration,
        open: F,
    ) -> Result<Checked<L>, ControlError>
    where
        F: FnOnce(Duration) -> Fut,
        Fut: Future<Output = Result<L, SessionError>>,
    {
        let inner = &self.inner;
        let t0 = Instant::now();
        let deadline = tokio::time::Instant::now() + wait;
        let saturated = |scope| {
            let waited = t0.elapsed();
            metrics::counter!(
                "deltaforge_mysql_control_permit_timeouts_total",
                "class" => class.label(),
                "scope" => scope,
            )
            .increment(1);
            ControlError::Saturated {
                class: class.label(),
                scope,
                waited,
            }
        };
        let health = if class == Class::Health {
            Some(
                tokio::time::timeout_at(
                    deadline,
                    inner.health.clone().acquire_owned(),
                )
                .await
                .map_err(|_| saturated("source"))?
                .expect("never closed"),
            )
        } else {
            None
        };
        // The source's permit, or its idle connection when that fits.
        let source = loop {
            let idled = inner.idled.notified();
            tokio::pin!(idled);
            idled.as_mut().enable();
            if let Some(mut e) = inner.reuse(ident) {
                // An idle connection may have died meanwhile (the server
                // or the network closed it): never handed out unchecked.
                if e.link.alive().await {
                    waited(class, t0);
                    return Ok(Checked {
                        entry: Some(e),
                        pool: inner.clone(),
                        _health: health,
                    });
                }
                close(e);
                continue;
            }
            tokio::select! {
                p = inner.cap.clone().acquire_owned() => {
                    break p.expect("never closed");
                }
                _ = &mut idled => {}
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(saturated("source"));
                }
            }
        };
        // The runner's permit: under pressure every idle connection of the
        // runner closes at once to free one.
        let runner = match inner.budget.ordinary.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                inner.budget.evict_idle();
                tokio::time::timeout_at(
                    deadline,
                    inner.budget.ordinary.clone().acquire_owned(),
                )
                .await
                .map_err(|_| saturated("runner"))?
                .expect("never closed")
            }
        };
        waited(class, t0);
        let epoch = inner.epoch.load(Ordering::SeqCst);
        let counted = {
            let (map, _) = &*inner.open;
            *map.lock().expect("open lock").entry(epoch).or_default() += 1;
            Open {
                open: inner.open.clone(),
                epoch,
                _gauge: super::mysql_connections::Counted::new(
                    &inner.source,
                    super::mysql_connections::Kind::Control,
                ),
            }
        };
        let permits = Permits {
            source: Some(source),
            debt: inner.debt.clone(),
            _runner: runner,
            _open: counted,
        };
        let remaining = deadline
            .saturating_duration_since(tokio::time::Instant::now())
            .max(Duration::from_secs(1))
            .min(CONNECT_TIMEOUT);
        // A failed open releases its permits here (nothing is open).
        let link = open(remaining).await?;
        Ok(Checked {
            entry: Some(Entry {
                link,
                ident: ident.to_string(),
                epoch,
                permits,
            }),
            pool: inner.clone(),
            _health: health,
        })
    }

    /// Never reuse a connection opened so far: close the idle one now and
    /// those in use when they return, waiting at most `bound` for them.
    /// What was still open when the bound ran out.
    pub(crate) async fn drain(&self, bound: Duration) -> usize {
        let inner = &self.inner;
        let epoch = inner.epoch.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some(e) = inner.take_idle() {
            close(e);
        }
        let earlier = || -> usize {
            inner
                .open
                .0
                .lock()
                .expect("open lock")
                .range(..epoch)
                .map(|(_, n)| n)
                .sum()
        };
        let deadline = tokio::time::Instant::now() + bound;
        loop {
            let closed = inner.open.1.notified();
            tokio::pin!(closed);
            closed.as_mut().enable();
            let left = earlier();
            if left == 0 {
                return 0;
            }
            if tokio::time::timeout_at(deadline, closed).await.is_err() {
                let left = earlier();
                if left > 0 {
                    warn!(
                        source = %inner.source,
                        open = left,
                        "drained control connections still open after {bound:?}"
                    );
                }
                return left;
            }
        }
    }
}

fn waited(class: Class, t0: Instant) {
    metrics::histogram!(
        "deltaforge_mysql_control_permit_wait_seconds",
        "class" => class.label(),
    )
    .record(t0.elapsed().as_secs_f64());
}

/// A MySQL control connection.
pub(crate) struct MySqlLink(mysql_async::Conn);

impl Link for MySqlLink {
    fn reset(&mut self) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
        Box::pin(async move { self.0.reset().await.is_ok() })
    }

    fn alive(&mut self) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
        use mysql_async::prelude::Queryable as _;
        Box::pin(async move {
            matches!(
                tokio::time::timeout(ALIVE_TIMEOUT, self.0.ping()).await,
                Ok(Ok(()))
            )
        })
    }

    fn close(self) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move {
            self.0.disconnect().await.ok();
        })
    }
}

/// A source's MySQL control-plane pool.
pub(crate) type ControlPool = Pool<MySqlLink>;
/// A MySQL control connection checked out of its source's pool.
pub(crate) type ControlConn = Checked<MySqlLink>;

impl std::ops::Deref for Checked<MySqlLink> {
    type Target = mysql_async::Conn;
    fn deref(&self) -> &mysql_async::Conn {
        &self.entry.as_ref().expect("checked out").link.0
    }
}

impl std::ops::DerefMut for Checked<MySqlLink> {
    fn deref_mut(&mut self) -> &mut mysql_async::Conn {
        &mut self.link().0
    }
}

/// Every source's pool, by tenant and source.
type Sources = Mutex<HashMap<(String, String), Weak<Inner<MySqlLink>>>>;

static SOURCES: OnceLock<Sources> = OnceLock::new();

/// `(tenant, source)`'s pool on the runner's budget: the one in use,
/// resized to `size`, or a new one.
pub(crate) fn source_pool(
    tenant: &str,
    source: &str,
    size: usize,
) -> ControlPool {
    keyed(tenant, source, Some(size))
}

/// `(tenant, source)`'s pool as it is, or a new one of the default size
/// (resized when its source starts a run).
pub(crate) fn source_pool_as_is(tenant: &str, source: &str) -> ControlPool {
    keyed(tenant, source, None)
}

fn keyed(tenant: &str, source: &str, size: Option<usize>) -> ControlPool {
    let mut pools = SOURCES
        .get_or_init(Mutex::default)
        .lock()
        .expect("sources lock");
    pools.retain(|_, p| p.strong_count() > 0);
    let key = (tenant.to_string(), source.to_string());
    if let Some(inner) = pools.get(&key).and_then(Weak::upgrade) {
        if let Some(size) = size {
            inner.resize(size);
        }
        return Pool { inner };
    }
    let pool = Pool::new(budget(), source, size.unwrap_or(DEFAULT_POOL_SIZE));
    pools.insert(key, Arc::downgrade(&pool.inner));
    pool
}

impl ControlPool {
    /// A control connection to the server `server_uuid` through `dsn`,
    /// waiting at most `wait` for permits. A new connection proves it is
    /// that server before it is returned; the idle one is reused only for
    /// the same DSN and server.
    pub(crate) async fn connect(
        &self,
        class: Class,
        dsn: &str,
        server_uuid: &str,
        wait: Duration,
    ) -> Result<ControlConn, ControlError> {
        let ident = ident(dsn, server_uuid);
        self.checkout(class, &ident, wait, |t| async move {
            super::mysql_session::open_control_connection(dsn, server_uuid, t)
                .await
                .map(MySqlLink)
        })
        .await
    }
}

impl ControlPool {
    /// A pool of the default size on the runner's budget, outside any
    /// source's registry entry (callers with no tenant to key it by).
    pub(crate) fn standalone(source: &str) -> Self {
        Pool::new(budget(), source, DEFAULT_POOL_SIZE)
    }

    /// [`Pool::connect`], on a connection of its own that is never reused
    /// (closed when returned): for checks whose every run proves the
    /// server on its own connection.
    pub(crate) async fn connect_once(
        &self,
        class: Class,
        dsn: &str,
        server_uuid: &str,
        wait: Duration,
    ) -> Result<ControlConn, ControlError> {
        self.checkout(class, UNVERIFIED, wait, |t| async move {
            super::mysql_session::open_control_connection(dsn, server_uuid, t)
                .await
                .map(MySqlLink)
        })
        .await
    }

    /// A control connection through `dsn` to whichever server answers, for
    /// a caller that reads the server's identity itself: budgeted like any
    /// other, never reused (closed when returned).
    pub(crate) async fn connect_unverified(
        &self,
        class: Class,
        dsn: &str,
        wait: Duration,
    ) -> Result<ControlConn, ControlError> {
        self.checkout(class, UNVERIFIED, wait, |t| async move {
            let opts = mysql_async::Opts::from_url(dsn)
                .map_err(|e| SessionError::Connect(e.to_string()))?;
            tokio::time::timeout(t, mysql_async::Conn::new(opts))
                .await
                .map_err(|_| SessionError::Connect("connect timed out".into()))?
                .map(MySqlLink)
                .map_err(|e| SessionError::Connect(e.to_string()))
        })
        .await
    }
}

/// The identity of a connection opened for no particular server: never
/// reused.
const UNVERIFIED: &str = "";

/// What a connection is reusable for: its DSN (as a digest: no credential
/// is kept in clear) and its server.
fn ident(dsn: &str, server_uuid: &str) -> String {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(dsn.as_bytes());
    format!(
        "{}|{}",
        hex::encode(h.finalize()),
        server_uuid.trim().to_ascii_lowercase()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    /// A connection that counts its closes; its close takes `delay`.
    struct Fake {
        closed: Arc<AtomicUsize>,
        delay: Duration,
        reset_ok: bool,
        dead: Arc<AtomicBool>,
    }

    impl Link for Fake {
        fn reset(&mut self) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
            let ok = self.reset_ok;
            Box::pin(async move { ok })
        }

        fn alive(&mut self) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
            let dead = self.dead.load(Ordering::SeqCst);
            Box::pin(async move { !dead })
        }

        fn close(self) -> Pin<Box<dyn Future<Output = ()> + Send>> {
            Box::pin(async move {
                tokio::time::sleep(self.delay).await;
                self.closed.fetch_add(1, Ordering::SeqCst);
            })
        }
    }

    /// Opens fakes, counting opens and closes.
    #[derive(Clone, Default)]
    struct Server {
        opened: Arc<AtomicUsize>,
        closed: Arc<AtomicUsize>,
        delay: Duration,
        fail: Arc<AtomicBool>,
        bad_reset: Arc<AtomicBool>,
        /// Every connection opened so far stops answering.
        dead: Arc<AtomicBool>,
    }

    impl Server {
        fn open(
            &self,
        ) -> impl FnOnce(Duration) -> std::future::Ready<Result<Fake, SessionError>>
        {
            let s = self.clone();
            move |_| {
                std::future::ready(if s.fail.load(Ordering::SeqCst) {
                    Err(SessionError::Connect("refused".into()))
                } else {
                    s.opened.fetch_add(1, Ordering::SeqCst);
                    Ok(Fake {
                        closed: s.closed.clone(),
                        delay: s.delay,
                        reset_ok: !s.bad_reset.load(Ordering::SeqCst),
                        dead: s.dead.clone(),
                    })
                })
            }
        }

        fn live(&self) -> usize {
            self.opened.load(Ordering::SeqCst)
                - self.closed.load(Ordering::SeqCst)
        }
    }

    fn budget(total: usize) -> &'static Budget {
        Box::leak(Box::new(Budget::new(total)))
    }

    const W: Duration = Duration::from_secs(5);

    async fn get(
        pool: &Pool<Fake>,
        s: &Server,
        class: Class,
    ) -> Result<Checked<Fake>, ControlError> {
        pool.checkout(class, "a", W, s.open()).await
    }

    /// Let spawned closes and expiries run.
    async fn settle() {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_source_never_holds_more_than_its_pool() {
        let s = Server::default();
        let pool = Pool::detached(budget(32), "src", 2);
        let a = get(&pool, &s, Class::Capture).await.unwrap();
        let _b = get(&pool, &s, Class::Capture).await.unwrap();
        let t = tokio::time::Instant::now();
        let e = get(&pool, &s, Class::Position).await.err().unwrap();
        assert!(
            matches!(
                e,
                ControlError::Saturated {
                    class: "position",
                    scope: "source",
                    ..
                }
            ),
            "{e}"
        );
        assert_eq!(t.elapsed(), W);
        assert_eq!(s.opened.load(Ordering::SeqCst), 2);
        // A returned connection serves the next waiter.
        let p = pool.clone();
        let s2 = s.clone();
        let waiter = tokio::spawn(async move {
            get(&p, &s2, Class::Catalog).await.map(|_| ())
        });
        settle().await;
        a.release().await;
        waiter.await.unwrap().unwrap();
        assert!(s.live() <= 2);
    }

    #[tokio::test(start_paused = true)]
    async fn the_runner_budget_bounds_every_source_together() {
        let s = Server::default();
        // G = R + 3: three ordinary connections for the whole runner.
        let b = budget(KILL_RESERVE + 3);
        assert_eq!(b.available(), 3);
        let pools: Vec<_> = (0..3)
            .map(|i| Pool::<Fake>::detached(b, &format!("s{i}"), 2))
            .collect();
        let mut held = Vec::new();
        for p in &pools {
            held.push(get(p, &s, Class::Capture).await.unwrap());
        }
        let e = get(&pools[0], &s, Class::Capture).await.err().unwrap();
        assert!(
            matches!(
                e,
                ControlError::Saturated {
                    scope: "runner",
                    ..
                }
            ),
            "{e}"
        );
        assert_eq!(s.live(), 3);
        drop(held);
        settle().await;
        assert_eq!(s.live(), 0);
        assert_eq!(b.available(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn returned_connections_keep_one_idle_per_source() {
        let s = Server::default();
        let pool = Pool::detached(budget(32), "src", 4);
        let held = [
            get(&pool, &s, Class::Capture).await.unwrap(),
            get(&pool, &s, Class::Capture).await.unwrap(),
            get(&pool, &s, Class::Capture).await.unwrap(),
        ];
        for c in held {
            c.release().await;
        }
        settle().await;
        assert!(pool.has_idle());
        assert_eq!(s.live(), 1);
        assert_eq!(pool.open(), 1);
        // The idle connection is reused, not a new one opened.
        let c = get(&pool, &s, Class::Health).await.unwrap();
        assert_eq!(s.opened.load(Ordering::SeqCst), 3);
        c.release().await;
        // It expires after its TTL.
        tokio::time::sleep(IDLE_TTL + Duration::from_millis(1)).await;
        settle().await;
        assert!(!pool.has_idle());
        assert_eq!(s.live(), 0);
        assert_eq!(pool.open(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn the_idle_connection_is_reused_only_for_the_same_target() {
        let s = Server::default();
        let pool = Pool::detached(budget(32), "src", 4);
        get(&pool, &s, Class::Capture)
            .await
            .unwrap()
            .release()
            .await;
        let other = pool
            .checkout(Class::Capture, "b", W, s.open())
            .await
            .unwrap();
        settle().await;
        assert_eq!(s.opened.load(Ordering::SeqCst), 2);
        assert_eq!(s.live(), 1, "the idle connection to `a` was closed");
        drop(other);
        // A connection whose reset fails is never kept.
        s.bad_reset.store(true, Ordering::SeqCst);
        get(&pool, &s, Class::Capture)
            .await
            .unwrap()
            .release()
            .await;
        settle().await;
        assert!(!pool.has_idle());
        assert_eq!(s.live(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_dead_idle_connection_is_replaced_not_reused() {
        let s = Server::default();
        let pool = Pool::detached(budget(32), "src", 4);
        get(&pool, &s, Class::Capture)
            .await
            .unwrap()
            .release()
            .await;
        s.dead.store(true, Ordering::SeqCst);
        let fresh = Server {
            opened: s.opened.clone(),
            closed: s.closed.clone(),
            ..Default::default()
        };
        let c = get(&pool, &fresh, Class::Capture).await.unwrap();
        settle().await;
        assert_eq!(s.opened.load(Ordering::SeqCst), 2, "a new one opened");
        assert_eq!(s.live(), 1, "the dead one closed");
        assert!(!c.entry.as_ref().unwrap().link.dead.load(Ordering::SeqCst));
    }

    #[tokio::test(start_paused = true)]
    async fn idle_connections_are_evicted_when_the_budget_is_full() {
        let s = Server::default();
        let b = budget(KILL_RESERVE + 2);
        let (x, y, z) = (
            Pool::<Fake>::detached(b, "x", 2),
            Pool::detached(b, "y", 2),
            Pool::detached(b, "z", 2),
        );
        get(&x, &s, Class::Catalog).await.unwrap().release().await;
        get(&y, &s, Class::Catalog).await.unwrap().release().await;
        assert_eq!(b.available(), 0);
        // Every permit is held by an idle connection: a capture evicts them
        // and acquires at once.
        let t = tokio::time::Instant::now();
        let c = get(&z, &s, Class::Capture).await.unwrap();
        assert!(t.elapsed() < Duration::from_millis(10), "{:?}", t.elapsed());
        assert!(!x.has_idle() && !y.has_idle());
        drop(c);
    }

    #[tokio::test(start_paused = true)]
    async fn permits_return_only_once_the_connection_has_closed() {
        let s = Server {
            delay: Duration::from_secs(2),
            ..Default::default()
        };
        let b = budget(KILL_RESERVE + 1);
        let pool = Pool::detached(b, "src", 2);
        drop(get(&pool, &s, Class::Capture).await.unwrap());
        settle().await;
        assert_eq!(b.available(), 0, "still closing");
        tokio::time::sleep(Duration::from_secs(2)).await;
        settle().await;
        assert_eq!(b.available(), 1);
        assert_eq!(pool.open(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_open_releases_its_permits() {
        let s = Server::default();
        let b = budget(KILL_RESERVE + 1);
        let pool = Pool::detached(b, "src", 2);
        s.fail.store(true, Ordering::SeqCst);
        let e = get(&pool, &s, Class::Capture).await.err().unwrap();
        assert!(matches!(e, ControlError::Session(_)), "{e}");
        assert_eq!(b.available(), 1);
        assert_eq!(pool.open(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn health_checks_are_one_in_flight_per_source() {
        let s = Server::default();
        let pool = Pool::detached(budget(32), "src", 4);
        let h = get(&pool, &s, Class::Health).await.unwrap();
        let e = get(&pool, &s, Class::Health).await.err().unwrap();
        assert!(
            matches!(
                e,
                ControlError::Saturated {
                    class: "health",
                    ..
                }
            ),
            "{e}"
        );
        // Other classes are not held back by it.
        let _c = get(&pool, &s, Class::Capture).await.unwrap();
        h.release().await;
        get(&pool, &s, Class::Health).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn waiters_are_served_in_arrival_order_across_classes() {
        let s = Server::default();
        let pool = Pool::detached(budget(32), "src", 1);
        let first = get(&pool, &s, Class::Capture).await.unwrap();
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut waiters = Vec::new();
        for class in [
            Class::Position,
            Class::Capture,
            Class::Health,
            Class::Catalog,
        ] {
            let (p, s, order) = (pool.clone(), s.clone(), order.clone());
            waiters.push(tokio::spawn(async move {
                let c = get(&p, &s, class).await.unwrap();
                order.lock().unwrap().push(class);
                tokio::time::sleep(Duration::from_millis(10)).await;
                c.release().await;
            }));
            settle().await;
        }
        first.release().await;
        for w in waiters {
            w.await.unwrap();
        }
        assert_eq!(
            *order.lock().unwrap(),
            [
                Class::Position,
                Class::Capture,
                Class::Health,
                Class::Catalog
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_drain_closes_the_idle_connection_and_those_in_use_when_returned()
    {
        let s = Server::default();
        let pool = Pool::detached(budget(32), "src", 4);
        get(&pool, &s, Class::Capture)
            .await
            .unwrap()
            .release()
            .await;
        let in_use = get(&pool, &s, Class::Capture).await.unwrap();
        let p = pool.clone();
        let drain = tokio::spawn(async move { p.drain(DRAIN_TIMEOUT).await });
        settle().await;
        assert!(!pool.has_idle());
        assert_eq!(s.live(), 1);
        assert!(!drain.is_finished(), "waits for the connection in use");
        in_use.release().await;
        assert_eq!(drain.await.unwrap(), 0);
        assert_eq!(s.live(), 0);
        assert!(!pool.has_idle(), "a drained connection is never kept");
        // Bounded: what is still open is reported.
        let kept = get(&pool, &s, Class::Capture).await.unwrap();
        assert_eq!(pool.drain(Duration::from_secs(1)).await, 1);
        drop(kept);
    }

    #[tokio::test(start_paused = true)]
    async fn a_shrunk_pool_takes_its_permits_back_as_they_return() {
        let s = Server::default();
        let pool = Pool::detached(budget(32), "src", 3);
        let held = [
            get(&pool, &s, Class::Capture).await.unwrap(),
            get(&pool, &s, Class::Capture).await.unwrap(),
            get(&pool, &s, Class::Capture).await.unwrap(),
        ];
        pool.inner.resize(2);
        drop(held);
        settle().await;
        let _a = get(&pool, &s, Class::Capture).await.unwrap();
        let _b = get(&pool, &s, Class::Capture).await.unwrap();
        let e = get(&pool, &s, Class::Capture).await.err().unwrap();
        assert!(
            matches!(
                e,
                ControlError::Saturated {
                    scope: "source",
                    ..
                }
            ),
            "{e}"
        );
    }

    #[test]
    fn the_budget_reserves_cancellation_capacity() {
        let b = Budget::new(DEFAULT_MAX_CONTROL_CONNECTIONS as usize);
        assert_eq!(b.total(), 32);
        assert_eq!(b.available(), 32 - KILL_RESERVE);
        assert!(configure(KILL_RESERVE as u32 + 1).is_err());
    }
}

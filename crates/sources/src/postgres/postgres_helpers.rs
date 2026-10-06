//! PostgreSQL source helper utilities.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use deltaforge_core::{CheckpointMeta, SourceError, SourceResult};
use pgwire_replication::{
    Lsn, PgWireError, ReplicationClient, ReplicationConfig, TlsConfig,
};
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use url::Url;

use checkpoints::CheckpointStore;
use common::{DsnComponents, RetryOutcome, RetryPolicy, retry_async};

use deltaforge_core::incident::IncidentId;
use storage::adapters::incidents::IncidentStore;

use super::postgres_continuity::{
    ChainPosition, Checkpoint, Expected, FailoverSlot, Proven, Refusal,
    SessionFacts, SlotFacts, Stamp, history_needed, load_record, new_chain_id,
    parse_history, prove, store_record,
};

/// A started stream and what its session proved; not yet authoritative.
pub(super) struct Opened {
    client: ReplicationClient,
    proven: Proven,
}
use super::{
    AsPgEndpoint, PostgresCheckpoint, PostgresSourceError, PostgresSourceResult,
};

// ----------------------------- Configuration Constants -----------------------------

pub const STATUS_INTERVAL_SECS: u64 = 1;
pub const IDLE_WAKEUP_INTERVAL_SECS: u64 = 30;
pub const BUFFER_EVENTS: usize = 8192;

// ----------------------------- Public Helpers -----------------------------

/// Parse DSN and extract connection components.
pub(super) fn parse_dsn(dsn: &str) -> PostgresSourceResult<DsnComponents> {
    if dsn.starts_with("postgres://") || dsn.starts_with("postgresql://") {
        DsnComponents::from_url(dsn, 5432)
            .map_err(PostgresSourceError::InvalidDsn)
    } else {
        Ok(DsnComponents::from_keyvalue(
            dsn, 5432, "postgres", "postgres",
        ))
    }
}

/// Prepare ReplicationClient from DSN + last checkpoint.
pub(super) async fn prepare_replication_client(
    dsn: &str,
    source_id: &str,
    slot: &str,
    publication: &str,
    ckpt_store: &Arc<dyn CheckpointStore>,
) -> PostgresSourceResult<(
    DsnComponents,
    ReplicationConfig,
    Option<PostgresCheckpoint>,
    bool,
)> {
    let components = parse_dsn(dsn)?;

    // A snapshot position is not a stream position: no checkpoint to resume
    // from, and the snapshot it belongs to never completed.
    let resume = ckpt_store
        .get_raw(source_id)
        .await
        .map_err(|e| PostgresSourceError::Checkpoint(e.to_string()))?
        .map(|raw| super::classify_pg_checkpoint(&raw))
        .transpose()
        .map_err(PostgresSourceError::Checkpoint)?;
    let incomplete_snapshot =
        matches!(resume, Some(super::PgResumePosition::Snapshot { .. }));
    let last_checkpoint = match resume {
        Some(super::PgResumePosition::Stream(cp)) => Some(cp),
        _ => None,
    };

    info!(
        source_id = %source_id,
        host = %components.host,
        checkpoint = ?last_checkpoint,
        "preparing replication client"
    );

    let start_lsn = if let Some(ref cp) = last_checkpoint {
        Lsn::parse(&cp.lsn).map_err(|e| {
            PostgresSourceError::LsnParse(format!(
                "invalid checkpoint LSN '{}': {}",
                cp.lsn, e
            ))
        })?
    } else {
        Lsn::parse("0/0").unwrap()
    };

    let config =
        build_replication_config(&components, slot, publication, start_lsn);

    Ok((components, config, last_checkpoint, incomplete_snapshot))
}

/// Build a `ReplicationConfig` from parsed DSN components at a specific start
/// position. Single source of truth for the connection parameters used by both
/// startup and credential-rotation reconnects, so a rotated stream is opened with
/// exactly the production settings.
pub(super) fn build_replication_config(
    components: &DsnComponents,
    slot: &str,
    publication: &str,
    start_lsn: Lsn,
) -> ReplicationConfig {
    // ReplicationConfig is #[non_exhaustive] as of 0.4.0 — build it via the
    // constructor + builder methods rather than a struct literal.
    ReplicationConfig::new(
        components.host.clone(),
        components.user.clone(),
        components.password.clone(),
        components.database.clone(),
        slot,
        publication,
    )
    .with_port(components.port)
    .with_tls(TlsConfig::disabled())
    .with_start_lsn(start_lsn)
    .with_status_interval(Duration::from_secs(STATUS_INTERVAL_SECS))
    .with_wakeup_interval(Duration::from_secs(IDLE_WAKEUP_INTERVAL_SECS))
    .with_buffer_size(BUFFER_EVENTS)
}

/// The proof every replication stream passes before START_REPLICATION
/// (startup, reconnect and credential rotation alike), on the authenticated
/// replication session that then streams: the server, timeline and slot it
/// shows must continue the source's durable checkpoint F, read from the
/// checkpoint store immediately before (never an in-memory read position).
/// See [`super::postgres_continuity`] for the conditions.
///
/// The stream acknowledges F to the server (not where it starts reading), so
/// the slot never advances past what the source can recover after a crash.
/// A source with no durable checkpoint yet has no position to prove: only its
/// server and timeline are.
#[derive(Clone)]
pub(crate) struct StreamProof {
    pub(super) dsn: crate::credentials::ProtectedDsn,
    pub(super) slot: String,
    pub(super) source_id: String,
    pub(super) pipeline: String,
    pub(super) tenant: String,
    pub(super) checkpoints: Arc<dyn CheckpointStore>,
    pub(super) backend: storage::ArcStorageBackend,
    /// The continuity stamp of the authoritative stream (`None` until one is
    /// activated), recorded in the checkpoints its events carry. Only
    /// [`StreamProof::activate`] changes it.
    pub(super) active: Arc<std::sync::RwLock<Option<Stamp>>>,
    /// A session on a server in recovery is retried like a connection
    /// failure (the endpoint may be mid-failover) for this long, then stops.
    pub(super) recovery_window: Duration,
    pub(super) recovery_retry: Arc<std::sync::Mutex<RecoveryRetry>>,
}

/// The bounded retry of sessions on a server in recovery: when it began and
/// its auto-retry incident.
#[derive(Debug, Default)]
pub(crate) struct RecoveryRetry {
    since: Option<Instant>,
    incident: Option<IncidentId>,
}

/// A slot's `(restart_lsn, confirmed_flush_lsn)`: `None` when the slot does
/// not exist, a `None` position when the server reports none.
type SlotBounds = Option<(Option<Lsn>, Option<Lsn>)>;

impl StreamProof {
    /// The same proof for a stream opened with another DSN (rotation).
    pub(super) fn with_dsn(
        &self,
        dsn: crate::credentials::ProtectedDsn,
    ) -> Self {
        Self {
            dsn,
            ..self.clone()
        }
    }

    /// The durable checkpoint F, read now.
    async fn durable_checkpoint(&self) -> SourceResult<Option<Checkpoint>> {
        let raw =
            self.checkpoints
                .get_raw(&self.source_id)
                .await
                .map_err(|e| SourceError::Checkpoint {
                    details: format!("read the durable checkpoint: {e}").into(),
                })?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let checkpoint =
            match super::classify_pg_checkpoint(&raw).map_err(|e| {
                SourceError::Checkpoint {
                    details: format!("read the durable checkpoint: {e}").into(),
                }
            })? {
                // An incomplete snapshot's durable point is its anchor, on
                // the history the anchor was taken on.
                super::PgResumePosition::Snapshot { anchor, stamp } => {
                    return Ok(Some(Checkpoint {
                        lsn: anchor,
                        chain: stamp,
                    }));
                }
                // A sink entered the current generation and committed
                // nothing since: the generation's anchor (with its stamp),
                // where its stream starts, is the durable point.
                super::PgResumePosition::Started => {
                    let anchor = match crate::snapshot_queue::QueueStore::new(
                        self.backend.clone(),
                        &self.source_id,
                    )
                    .read()
                    .await
                    {
                        Ok(Some(crate::snapshot_queue::Stored::Current {
                            control,
                            ..
                        })) => control
                            .anchor
                            .as_ref()
                            .and_then(super::pg_anchor_of),
                        _ => None,
                    };
                    let Some(anchor) = anchor else {
                        return Err(SourceError::Checkpoint {
                            details: "the durable checkpoint is a snapshot \
                                      generation's start, and the generation \
                                      records no anchor"
                                .into(),
                        });
                    };
                    let lsn = Lsn::parse(&anchor.lsn).map_err(|e| {
                        SourceError::Checkpoint {
                            details: format!(
                                "invalid snapshot anchor '{}': {e}",
                                anchor.lsn
                            )
                            .into(),
                        }
                    })?;
                    let chain = anchor.chain_position().map_err(|e| {
                        SourceError::Checkpoint { details: e.into() }
                    })?;
                    return Ok(Some(Checkpoint { lsn, chain }));
                }
                super::PgResumePosition::Stream(cp) => cp,
            };
        let lsn = Lsn::parse(&checkpoint.lsn).map_err(|e| {
            SourceError::Checkpoint {
                details: format!(
                    "invalid checkpoint LSN '{}': {e}",
                    checkpoint.lsn
                )
                .into(),
            }
        })?;
        let chain = match (
            checkpoint.chain,
            checkpoint.transition,
            checkpoint.timeline,
        ) {
            (Some(chain_id), Some(transition), Some(timeline)) => {
                Some(ChainPosition {
                    chain_id,
                    transition,
                    timeline,
                })
            }
            (None, None, None) => None,
            (chain, transition, _) => {
                // A partial stamp is no proof of anything: fail closed.
                return Err(super::continuity_refusal(
                    &self.source_id,
                    &self.slot,
                    Some(lsn),
                    "checkpoint_chain_mismatch",
                    &super::postgres_continuity::RefusalEvidence {
                        checkpoint_chain: chain,
                        checkpoint_transition: transition,
                        ..Default::default()
                    },
                ));
            }
        };
        Ok(Some(Checkpoint { lsn, chain }))
    }

    /// Open a stream at `cfg.start_lsn`: connect and authenticate, prove
    /// continuity on that session, then START_REPLICATION; once the server
    /// holds the slot for this session, recheck the slot bound. Returns the
    /// started stream with what it proved; nothing is persisted and the
    /// active stamp is not changed (see [`StreamProof::activate`]). Nothing
    /// has been consumed from it.
    async fn open(&self, cfg: ReplicationConfig) -> SourceResult<Opened> {
        let checkpoint = self.durable_checkpoint().await?;
        let f = checkpoint.as_ref().map(|c| c.lsn);
        let start = cfg.start_lsn;
        let cfg = match f {
            Some(f) => cfg.with_ack_lsn(f),
            None => cfg,
        };
        // Dropping the client before `start` ends the session without
        // replication.
        let mut client = connect_replication(&self.source_id, cfg).await?;
        let proven = match tokio::time::timeout(
            Duration::from_secs(30),
            self.prove_on_session(&mut client, checkpoint.as_ref(), start),
        )
        .await
        {
            Ok(proven) => proven?,
            Err(_) => {
                return Err(SourceError::Timeout {
                    action: "prove_stream_continuity".into(),
                });
            }
        };
        crate::stream_probe::after_slot_proof().await;

        client.start().await.map_err(pgwire_error_to_source_error)?;
        started(&self.source_id, &mut client).await?;
        crate::stream_probe::record_stream_opened();
        // The slot is now held by this session: prove again, against the same
        // F the stream acknowledges, that it did not move past F between the
        // proof and START_REPLICATION. Nothing has been consumed yet.
        if let Some(f) = f
            && let Err(e) = self.check_bounds(f).await
        {
            let _ = client.shutdown().await;
            return Err(e);
        }
        Ok(Opened { client, proven })
    }

    /// Make an opened stream the authoritative one, before anything is
    /// consumed from it: persist its proven continuity (a new record or a
    /// proven transition), then switch the active stamp. A failed write
    /// consumes nothing and leaves the record and the stamp as they were.
    /// Without `allow_transition` (a credential-rotation candidate) a stream
    /// that would change the continuity - another chain, transition or
    /// timeline than the active stream's - is refused instead.
    pub(super) async fn activate(
        &self,
        opened: Opened,
        allow_transition: bool,
    ) -> SourceResult<ReplicationClient> {
        let Opened { mut client, proven } = opened;
        let stamp = Stamp::of(&proven.record);
        if !allow_transition {
            let active = self.active.read().expect("not poisoned").clone();
            if proven.record_changed || active.as_ref() != Some(&stamp) {
                let _ = client.shutdown().await;
                warn!(
                    source_id = %self.source_id, ?active, proposed = ?stamp,
                    "a credential-rotation stream would change the \
                     continuity; refused (an ordinary reconnect proves it)"
                );
                return Err(SourceError::Incompatible {
                    details: "a credential-rotation stream may not cross a \
                              continuity transition"
                        .into(),
                });
            }
        }
        if proven.record_changed {
            if let Err(e) =
                store_record(&self.backend, &self.source_id, &proven.record)
                    .await
            {
                let _ = client.shutdown().await;
                return Err(SourceError::Checkpoint {
                    details: format!("{e:#}").into(),
                });
            }
            info!(
                source_id = %self.source_id,
                chain_id = %proven.record.chain_id,
                timeline = proven.record.timeline,
                transition_id = proven.record.transition_id,
                checkpoint = ?proven.record.proven_at,
                "continuity record updated"
            );
        }
        // While the chain is at transition 0, every checkpoint of the source
        // joins it before anything is consumed.
        if let Err(e) = super::postgres_checkpoint_chain::adopt_into_chain(
            self.checkpoints.as_ref(),
            &self.source_id,
            &proven.record,
        )
        .await
        {
            let _ = client.shutdown().await;
            return Err(match e {
                super::postgres_checkpoint_chain::AdoptionError::Store(e) => {
                    SourceError::Checkpoint {
                        details: format!("{e:#}").into(),
                    }
                }
                super::postgres_checkpoint_chain::AdoptionError::Refused(r) => {
                    warn!(
                        source_id = %self.source_id, reason = %r.reason,
                        "stored checkpoints cannot join the continuity chain; \
                         nothing was rewritten"
                    );
                    super::continuity_refusal(
                        &self.source_id,
                        &self.slot,
                        None,
                        "checkpoint_chain_mismatch",
                        &super::postgres_continuity::RefusalEvidence {
                            checkpoint_chain: r.checkpoint_chain,
                            checkpoint_transition: r.checkpoint_transition,
                            recorded_chain: Some(
                                proven.record.chain_id.clone(),
                            ),
                            recorded_transition: Some(
                                proven.record.transition_id,
                            ),
                            ..Default::default()
                        },
                    )
                }
            });
        }
        self.report_failover_slot(proven.failover_slot).await;
        *self.active.write().expect("not poisoned") = Some(stamp);
        Ok(client)
    }

    async fn prove_on_session(
        &self,
        client: &mut ReplicationClient,
        checkpoint: Option<&Checkpoint>,
        start: Lsn,
    ) -> SourceResult<Proven> {
        let facts = read_session_facts(client, &self.slot)
            .await
            .map_err(|e| session_error(&self.slot, e))?;
        let lineage = storage::adapters::source_lineage::load(
            &self.backend,
            &self.tenant,
            &self.source_id,
        )
        .await
        .map_err(SourceError::Other)?
        .map(|r| r.current.descriptor.endpoint());
        let Some(super::PgEndpoint {
            system_identifier: Some(sysid),
            database_oid: Some(dboid),
        }) = lineage
        else {
            return Err(SourceError::Lineage {
                details: format!(
                    "source '{}' has no recorded PostgreSQL lineage; refusing \
                     to stream",
                    self.source_id
                )
                .into(),
            });
        };
        let record = load_record(&self.backend, &self.source_id)
            .await
            .map_err(SourceError::Other)?;
        let new_chain = new_chain_id();
        let expected = Expected {
            lineage: (sysid, dboid),
            record: record.as_ref(),
            checkpoint,
            start,
            new_chain_id: &new_chain,
        };
        let history = match history_needed(&expected, &facts) {
            None => None,
            Some(timeline) => match client.timeline_history(timeline).await {
                Ok(h) => {
                    match parse_history(&String::from_utf8_lossy(&h.content)) {
                        Ok(entries) => Some(entries),
                        Err(reason) => {
                            warn!(
                                source_id = %self.source_id, timeline, %reason,
                                "unreadable timeline history; continuity unproven"
                            );
                            None
                        }
                    }
                }
                // The server has no history for it: nothing descends.
                Err(e) if e.is_server() => None,
                Err(e) => return Err(session_error(&self.slot, e)),
            },
        };
        let refusal = match prove(&expected, &facts, history.as_deref()) {
            Ok(proven) => {
                self.recovery_settled().await;
                return Ok(proven);
            }
            Err(Refusal::Unproven {
                class: "server_in_recovery",
                evidence,
            }) => {
                return self
                    .in_recovery(checkpoint.map(|c| c.lsn), *evidence)
                    .await;
            }
            Err(refusal) => refusal,
        };
        Err({
            match refusal {
                Refusal::DifferentCluster { expected, live } => {
                    let endpoint = |(s, d): (u64, u64)| super::PgEndpoint {
                        system_identifier: Some(s),
                        database_oid: Some(d),
                    };
                    super::server_changed(
                        &self.source_id,
                        &endpoint(expected),
                        &endpoint(live),
                    )
                }
                Refusal::Unproven { class, evidence } => {
                    super::continuity_refusal(
                        &self.source_id,
                        &self.slot,
                        checkpoint.map(|c| c.lsn),
                        class,
                        &evidence,
                    )
                }
            }
        })
    }

    /// The session is on a server in recovery. Within the retry window it is
    /// retried like a connection failure (an open `auto_retry` incident);
    /// then the source stops on the same incident, now operator action.
    async fn in_recovery(
        &self,
        f: Option<Lsn>,
        evidence: super::postgres_continuity::RefusalEvidence,
    ) -> SourceResult<Proven> {
        let (since, recorded) = {
            let mut r = self.recovery_retry.lock().expect("not poisoned");
            (
                *r.since.get_or_insert_with(Instant::now),
                r.incident.is_some(),
            )
        };
        if since.elapsed() >= self.recovery_window {
            return Err(super::continuity_refusal(
                &self.source_id,
                &self.slot,
                f,
                "server_in_recovery",
                &evidence,
            ));
        }
        if !recorded {
            let draft = super::continuity_unproven_draft(
                &self.source_id,
                &self.slot,
                &f.map_or("none".to_string(), |f| f.to_string()),
                "server_in_recovery",
                deltaforge_core::incident::Retryability::AutoRetry,
                deltaforge_core::incident::CauseCode::SourceConnect,
            );
            let id = crate::incident_drafts::record_retrying(
                &self.incidents(),
                &draft,
            )
            .await;
            self.recovery_retry.lock().expect("not poisoned").incident = id;
        }
        warn!(
            source_id = %self.source_id,
            "the server is in recovery (a standby); retrying until a \
             primary answers (replication stays closed)"
        );
        Err(SourceError::Connect {
            details: "the server is in recovery (a standby)".into(),
        })
    }

    /// A proof succeeded: a retrying `server_in_recovery` incident is over.
    async fn recovery_settled(&self) {
        let incident = {
            let mut r = self.recovery_retry.lock().expect("not poisoned");
            r.since = None;
            r.incident.take()
        };
        if let Some(id) = incident
            && let Err(e) = self
                .incidents()
                .resolve(
                    &id,
                    storage::adapters::incidents::Resolution::VerifiedRecovery {
                        check: storage::adapters::incidents::POSITION_VERIFIED
                            .to_string(),
                    },
                )
                .await
        {
            warn!(error = %format!("{e:#}"), "could not resolve the retrying incident");
        }
    }

    /// Stopped on purpose while retrying: withdraw the auto-retry incident.
    async fn recovery_cancelled(&self) {
        let incident = {
            let mut r = self.recovery_retry.lock().expect("not poisoned");
            r.since = None;
            r.incident.take()
        };
        crate::incident_drafts::cancel_retrying(&self.incidents(), incident)
            .await;
    }

    fn incidents(&self) -> IncidentStore {
        IncidentStore::new(Arc::clone(&self.backend), &self.pipeline)
    }

    /// Raise (or resolve) the running-degraded incident of a PostgreSQL 17+
    /// slot that cannot continue across a failover. Never fails the stream.
    async fn report_failover_slot(&self, failover: FailoverSlot) {
        let store = self.incidents();
        match failover {
            FailoverSlot::Disabled => {
                warn!(
                    source_id = %self.source_id, slot = %self.slot,
                    "the replication slot is not a failover slot: the source \
                     cannot continue on a promoted standby after a failover"
                );
                let draft = super::failover_slot_unavailable_draft(
                    &self.source_id,
                    &self.slot,
                );
                if let Err(e) = store.raise(&draft, 1).await {
                    warn!(
                        error = %format!("{e:#}"),
                        "could not record the failover-slot incident"
                    );
                }
            }
            FailoverSlot::Enabled => {
                let component = deltaforge_core::incident::Component::Source {
                    id: self.source_id.clone(),
                };
                if let Err(e) = store
                    .resolve_matching(
                        deltaforge_core::incident::ReasonCode::PgFailoverSlotUnavailable,
                        &component,
                        None,
                        storage::adapters::incidents::FAILOVER_SLOT_VERIFIED,
                    )
                    .await
                {
                    warn!(
                        error = %format!("{e:#}"),
                        "could not resolve the failover-slot incident"
                    );
                }
            }
            FailoverSlot::Unsupported => {}
        }
    }

    /// Require the slot's `restart_lsn` and `confirmed_flush_lsn` to be at or
    /// before `f`, read on a control connection right after the server
    /// accepted START_REPLICATION (the slot is then held, so it can no longer
    /// move between the check and the stream).
    pub(super) async fn check_bounds(&self, f: Lsn) -> SourceResult<()> {
        let bounds = read_slot_bounds(self.dsn.expose(), &self.slot)
            .await
            .map_err(|e| SourceError::Connect {
                details: format!(
                    "read replication slot '{}' before streaming: {e}",
                    self.slot
                )
                .into(),
            })?;
        let (class, restart, confirmed) = match bounds {
            None => ("slot_missing", None, None),
            Some((Some(r), Some(c))) if r <= f && c <= f => {
                return Ok(());
            }
            Some((Some(r), Some(c))) => {
                ("slot_beyond_checkpoint", Some(r), Some(c))
            }
            Some((r, c)) => ("unknown_slot_position", r, c),
        };
        let shown =
            |l: Option<Lsn>| l.map_or("none".to_string(), |l| l.to_string());
        error!(
            source_id = %self.source_id, slot = %self.slot, checkpoint = %f,
            restart_lsn = %shown(restart), confirmed_flush_lsn = %shown(confirmed),
            class, "the replication slot is not at or before the durable checkpoint; \
             refusing to stream"
        );
        Err(super::slot_bound_incident(
            &self.source_id,
            &self.slot,
            &f.to_string(),
            class,
            restart.map(|l| l.to_string()),
            confirmed.map(|l| l.to_string()),
        ))
    }
}

/// A failure to read the session's facts: retried like a connection failure.
fn session_error(slot: &str, e: PgWireError) -> SourceError {
    SourceError::Connect {
        details: format!(
            "read the replication session's server, timeline and slot \
             '{slot}' before streaming: {e}"
        )
        .into(),
    }
}

/// Read what the gated replication session shows before START_REPLICATION:
/// IDENTIFY_SYSTEM, then the database OID, server version, recovery state and
/// slot row in one query on the same session.
async fn read_session_facts(
    client: &mut ReplicationClient,
    slot: &str,
) -> Result<SessionFacts, PgWireError> {
    let malformed =
        |what: &str| PgWireError::Protocol(format!("unexpected {what}"));
    let id = client.identify_system().await?;
    let system_identifier = id
        .system_id
        .parse::<u64>()
        .map_err(|_| malformed("system identifier"))?;
    let sql = format!(
        "SELECT (SELECT oid FROM pg_database \
                 WHERE datname = current_database())::text, \
                current_setting('server_version_num'), \
                pg_is_in_recovery()::text, \
                (SELECT to_jsonb(s)::text FROM pg_replication_slots s \
                 WHERE s.slot_name = '{}')",
        slot.replace('\'', "''")
    );
    let rows = client.simple_query(&sql).await?;
    let [row] = rows.as_slice() else {
        return Err(malformed("session facts row count"));
    };
    let text = |i: usize| {
        row.get(i)
            .and_then(|v| v.as_ref())
            .map(|b| String::from_utf8_lossy(b).into_owned())
    };
    let database_oid = text(0)
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| malformed("database oid"))?;
    let server_version_num = text(1)
        .and_then(|s| s.parse::<u32>().ok())
        .ok_or_else(|| malformed("server version"))?;
    let in_recovery = match text(2).as_deref() {
        Some("false") => false,
        Some("true") => true,
        _ => return Err(malformed("recovery state")),
    };
    let slot = match text(3) {
        None => None,
        Some(json) => Some(SlotFacts::from_json(
            &serde_json::from_str(&json).map_err(|_| malformed("slot row"))?,
        )),
    };
    Ok(SessionFacts {
        system_identifier,
        database_oid,
        timeline: id.timeline,
        flush: id.xlogpos,
        server_version_num,
        in_recovery,
        slot,
    })
}
async fn read_slot_bounds(
    dsn: &str,
    slot: &str,
) -> Result<SlotBounds, tokio_postgres::Error> {
    let (client, conn) = tokio_postgres::connect(dsn, NoTls).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let row = client
        .query_opt(
            "SELECT restart_lsn::text, confirmed_flush_lsn::text \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await?;
    Ok(row.map(|r| {
        let lsn = |i: usize| {
            r.get::<_, Option<String>>(i)
                .and_then(|s| Lsn::parse(&s).ok())
        };
        (lsn(0), lsn(1))
    }))
}

/// Open a replication stream with retries. Every attempt proves continuity
/// on its own session first ([`StreamProof`]) and acknowledges the durable
/// checkpoint.
pub(super) async fn connect_replication_with_retries(
    source_id: &str,
    config: ReplicationConfig,
    proof: &StreamProof,
    allow_transition: bool,
    cancel: &CancellationToken,
    retry_policy: RetryPolicy,
) -> SourceResult<ReplicationClient> {
    let cfg = config.clone();
    let proof = proof.clone();

    let attempt = proof.clone();
    let result = retry_async(
        move |_| {
            let cfg = cfg.clone();
            let proof = attempt.clone();
            async move {
                let opened = proof.open(cfg).await?;
                proof.activate(opened, allow_transition).await
            }
        },
        is_retryable_source_error,
        Duration::from_secs(30),
        retry_policy,
        cancel,
        "replication_connect",
    )
    .await;
    if matches!(result, Err(RetryOutcome::Cancelled)) {
        proof.recovery_cancelled().await;
    }
    result.map_err(|outcome| match outcome {
        RetryOutcome::Cancelled => SourceError::Cancelled,
        RetryOutcome::Timeout { action } => SourceError::Timeout { action },
        RetryOutcome::Exhausted {
            last_error,
            attempts,
        } => {
            warn!(
                source_id,
                attempts = attempts,
                "replication connection exhausted retries"
            );
            last_error
        }
        RetryOutcome::Failed(e) => e,
    })
}

/// Wait until the server accepted START_REPLICATION (bounded like connect).
async fn started(
    source_id: &str,
    client: &mut ReplicationClient,
) -> SourceResult<()> {
    match tokio::time::timeout(Duration::from_secs(30), client.wait_started())
        .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => {
            error!(source_id = %source_id, error = %e, "START_REPLICATION failed");
            Err(pgwire_error_to_source_error(e))
        }
        Err(_) => {
            let _ = client.shutdown().await;
            Err(SourceError::Timeout {
                action: "start_replication".into(),
            })
        }
    }
}

/// Determine if a SourceError is worth retrying.
fn is_retryable_source_error(e: &SourceError) -> bool {
    match e {
        SourceError::Timeout { .. }
        | SourceError::Connect { .. }
        | SourceError::Io(_) => true,
        SourceError::Cancelled | SourceError::Auth { .. } => false,
        SourceError::Other(inner) => {
            let msg = inner.to_string().to_lowercase();
            msg.contains("connection")
                || msg.contains("timeout")
                || msg.contains("reset")
                || msg.contains("refused")
                || msg.contains("already active")
        }
        _ => false,
    }
}

/// Connect to PostgreSQL replication with timeout.
async fn connect_replication(
    source_id: &str,
    config: ReplicationConfig,
) -> SourceResult<ReplicationClient> {
    debug!(source_id = %source_id, host = %config.host, "connecting to postgres replication");
    let t0 = Instant::now();

    match tokio::time::timeout(
        Duration::from_secs(30),
        ReplicationClient::connect_gated(config),
    )
    .await
    {
        Ok(Ok(client)) => {
            info!(source_id = %source_id, ms = t0.elapsed().as_millis() as u64, "connected to postgres replication");
            Ok(client)
        }
        Ok(Err(e)) => {
            error!(source_id = %source_id, error = %e, "replication connect failed");
            print_connection_hints(&e);
            Err(pgwire_error_to_source_error(e))
        }
        Err(_) => Err(SourceError::Timeout {
            action: "replication_connect".into(),
        }),
    }
}

/// Convert PgWireError to SourceError for connection phase.
/// Note: This is different from LoopControl which is used in the event loop.
fn pgwire_error_to_source_error(e: PgWireError) -> SourceError {
    use PgWireError::*;
    match e {
        Auth(msg) => SourceError::Auth {
            details: msg.into(),
        },
        Io(err) => SourceError::Connect {
            details: format!("{err} ({:?})", err.kind()).into(),
        },
        Task(msg) => SourceError::Connect {
            details: msg.into(),
        },
        Tls(msg) => SourceError::Incompatible {
            details: format!("TLS: {msg}").into(),
        },
        Server(msg) | Protocol(msg) => SourceError::Connect {
            details: msg.into(),
        },
        Internal(msg) => {
            SourceError::Other(anyhow::anyhow!("pgwire internal: {}", msg))
        }
    }
}

/// Print helpful hints for common connection errors.
fn print_connection_hints(err: &PgWireError) {
    let msg = err.to_string();

    if err.is_auth()
        || msg.contains("password")
        || msg.contains("authentication")
    {
        error!("PostgreSQL auth issue hints:");
        error!("  1) Ensure pg_hba.conf allows replication connections");
        error!(
            "  2) Verify user has REPLICATION role: ALTER ROLE user REPLICATION;"
        );
        error!("  3) Grant usage: GRANT USAGE ON SCHEMA public TO user;");
    } else if msg.contains("slot") && msg.contains("does not exist") {
        error!("Replication slot issue hints:");
        error!(
            "  1) Create slot: SELECT pg_create_logical_replication_slot('slot','pgoutput');"
        );
    } else if msg.contains("slot") && msg.contains("already active") {
        warn!("Slot is in use by another connection - will retry");
    } else if msg.contains("publication") {
        error!("Publication issue hints:");
        error!(
            "  1) Create publication: CREATE PUBLICATION pub FOR ALL TABLES;"
        );
    } else if msg.contains("wal_level") {
        error!("WAL level issue hints:");
        error!("  1) Set wal_level=logical in postgresql.conf");
        error!("  2) Restart PostgreSQL after changing");
    } else if err.is_tls() {
        error!("TLS issue hints:");
        error!("  1) Check SSL certificates are valid");
        error!("  2) Try disabling TLS if not required");
    }
}

/// Verify publication exists and ensure slot exists, get start LSN.
///
/// Publications must be pre-created by a DBA - this function will NOT auto-create them.
/// Slots will be auto-created if missing (safe, no ownership issues).
///
/// Returns LoopControl::Reconnect for missing publication (retryable - admin can create it).
/// Returns LoopControl::Fail for fatal errors (auth, incompatible config).
pub(super) async fn ensure_slot_and_publication(
    dsn: &str,
    slot: &str,
    publication: &str,
    tables: &[String],
) -> Result<Lsn, super::postgres_errors::LoopControl> {
    use super::postgres_errors::LoopControl;

    let (client, conn) =
        tokio_postgres::connect(dsn, NoTls).await.map_err(|e| {
            error!(error = %e, "control plane connect failed");
            LoopControl::from_tokio_postgres_error(&e)
        })?;

    tokio::spawn(async move {
        if let Err(e) = conn.await {
            warn!("control plane connection error: {}", e);
        }
    });

    // Check publication exists (do NOT auto-create)
    let pub_exists = client
        .query_opt(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await
        .map_err(|e| {
            error!(error = %e, "failed to check publication");
            LoopControl::from_tokio_postgres_error(&e)
        })?
        .is_some();

    if !pub_exists {
        let table_list = if tables.is_empty() {
            "ALL TABLES".to_string()
        } else {
            tables.join(", ")
        };

        error!(
            publication = %publication,
            tables = %table_list,
            "Publication does not exist. DeltaForge requires a pre-created publication."
        );
        error!("To create it, run as superuser or table owner:");
        if tables.is_empty() {
            error!("  CREATE PUBLICATION {} FOR ALL TABLES;", publication);
        } else {
            error!(
                "  CREATE PUBLICATION {} FOR TABLE {};",
                publication, table_list
            );
        }
        error!(
            "DeltaForge will retry and pick up the publication once created."
        );

        return Err(LoopControl::Reconnect);
    }

    // Check/create slot (auto-creation is safe - no ownership issues)
    let slot_row = client
        .query_opt(
            "SELECT confirmed_flush_lsn::text, restart_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .map_err(|e| {
            error!(error = %e, slot = %slot, "failed to check replication slot");
            LoopControl::from_tokio_postgres_error(&e)
        })?;

    let start_lsn = if let Some(row) = slot_row {
        let confirmed: Option<String> = row.get(0);
        let restart: Option<String> = row.get(1);

        confirmed
            .or(restart)
            .map(|s| {
                Lsn::parse(&s).map_err(|e| {
                    error!(error = %e, lsn = %s, "failed to parse LSN");
                    LoopControl::Fail(SourceError::Other(e.into()))
                })
            })
            .transpose()?
            .unwrap_or(get_current_wal_lsn(&client).await?)
    } else {
        // Auto-create slot (safe - only requires REPLICATION privilege)
        super::postgres_slot_owner::create_logical_slot(&client, slot)
            .await
            .map_err(|e| {
                error!(error = %e, slot = %slot, "failed to create replication slot");
                error!("Ensure user has REPLICATION privilege: ALTER ROLE username REPLICATION;");
                LoopControl::from_tokio_postgres_error(&e)
            })?;

        info!(slot = %slot, "created replication slot");
        get_current_wal_lsn(&client).await?
    };

    Ok(start_lsn)
}

/// Verify the publication exists (never auto-created), without touching the
/// replication slot. Used on the snapshot path, where slot creation/anchoring is
/// owned by `postgres_slot_owner::prepare_snapshot_slot_anchor` so the CDC anchor
/// is the slot's consistent point rather than a sampled WAL position.
pub(super) async fn ensure_publication_exists(
    dsn: &str,
    publication: &str,
    tables: &[String],
) -> Result<(), super::postgres_errors::LoopControl> {
    use super::postgres_errors::LoopControl;

    let (client, conn) =
        tokio_postgres::connect(dsn, NoTls).await.map_err(|e| {
            error!(error = %e, "control plane connect failed");
            LoopControl::from_tokio_postgres_error(&e)
        })?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            warn!("control plane connection error: {}", e);
        }
    });

    let pub_exists = client
        .query_opt(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await
        .map_err(|e| {
            error!(error = %e, "failed to check publication");
            LoopControl::from_tokio_postgres_error(&e)
        })?
        .is_some();

    if !pub_exists {
        let table_list = if tables.is_empty() {
            "ALL TABLES".to_string()
        } else {
            tables.join(", ")
        };
        error!(
            publication = %publication,
            tables = %table_list,
            "Publication does not exist. DeltaForge requires a pre-created publication."
        );
        return Err(LoopControl::Reconnect);
    }

    Ok(())
}

/// Get current WAL LSN.
async fn get_current_wal_lsn(
    client: &tokio_postgres::Client,
) -> Result<Lsn, super::postgres_errors::LoopControl> {
    use super::postgres_errors::LoopControl;

    let row = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .map_err(|e| {
            error!(error = %e, "failed to get current WAL LSN");
            LoopControl::from_tokio_postgres_error(&e)
        })?;

    let lsn_str: String = row.get(0);
    Lsn::parse(&lsn_str).map_err(|e| {
        error!(error = %e, lsn = %lsn_str, "failed to parse WAL LSN");
        LoopControl::Fail(SourceError::Other(e.into()))
    })
}

// ----------------------------- Utility Functions -----------------------------

/// Redact password from DSN for logging.
pub(crate) fn redact_password(dsn: &str) -> String {
    if dsn.starts_with("postgres://") || dsn.starts_with("postgresql://") {
        if let Ok(mut url) = Url::parse(dsn) {
            if url.password().is_some() {
                let _ = url.set_password(Some("***"));
            }
            return url.to_string();
        }
    }
    dsn.split_whitespace()
        .map(|p| {
            if p.to_lowercase().starts_with("password=") {
                "password=***"
            } else {
                p
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Build checkpoint metadata from LSN.
///
/// Hand-writes the JSON to avoid serde_json overhead on every event.
/// Format: `{"lsn":"X/Y","tx_id":N|null}` plus `stamp`, the authoritative
/// stream's continuity members (`Stamp::checkpoint_members`; empty before
/// one is proven).
pub(crate) fn make_checkpoint_meta(
    lsn: &Lsn,
    tx_id: Option<u32>,
    stamp: &str,
) -> CheckpointMeta {
    make_checkpoint_meta_str(&lsn.to_string(), tx_id, stamp)
}

/// Build checkpoint metadata from a pre-formatted LSN string.
/// Avoids reformatting the LSN when it's already cached.
pub(crate) fn make_checkpoint_meta_str(
    lsn_str: &str,
    tx_id: Option<u32>,
    stamp: &str,
) -> CheckpointMeta {
    use std::fmt::Write;
    let mut buf = String::with_capacity(48 + stamp.len());
    let _ = write!(buf, r#"{{"lsn":"{}","tx_id":"#, lsn_str);
    match tx_id {
        Some(id) => {
            let _ = write!(buf, "{}", id);
        }
        None => buf.push_str("null"),
    }
    buf.push_str(stamp);
    buf.push('}');
    CheckpointMeta::from_vec(buf.into_bytes())
}

/// Convert PostgreSQL epoch timestamp (microseconds since 2000-01-01) to Unix milliseconds.
pub(crate) fn pg_timestamp_to_unix_ms(pg_timestamp_us: i64) -> i64 {
    const PG_EPOCH_OFFSET_MS: i64 = 946_684_800_000;
    (pg_timestamp_us / 1000) + PG_EPOCH_OFFSET_MS
}

#[cfg(test)]
mod tests {
    use super::*;

    mod activation {
        use super::*;
        use crate::postgres::postgres_continuity::{
            ContinuityRecord, FailoverSlot, Proven, Stamp,
        };
        use storage::adapters::test_util::FaultBackend;

        fn record(transition_id: u64, timeline: u32) -> ContinuityRecord {
            ContinuityRecord {
                format: 1,
                chain_id: "c".into(),
                system_identifier: 7,
                database_oid: 5,
                timeline,
                transition_id,
                proven_at: None,
            }
        }

        /// A started stream as `open` returns it (the client is never read).
        async fn opened(record: ContinuityRecord, changed: bool) -> Opened {
            let cfg =
                ReplicationConfig::new("127.0.0.1", "u", "p", "d", "s", "pub")
                    .with_port(1);
            Opened {
                client: ReplicationClient::connect_gated(cfg).await.unwrap(),
                proven: Proven {
                    record,
                    record_changed: changed,
                    failover_slot: FailoverSlot::Unsupported,
                },
            }
        }

        fn proof_on(backend: storage::ArcStorageBackend) -> StreamProof {
            StreamProof {
                backend,
                ..super::server_in_recovery::proof(Duration::from_secs(1))
            }
        }

        fn active(p: &StreamProof) -> Option<Stamp> {
            p.active.read().unwrap().clone()
        }

        #[tokio::test]
        async fn a_rotation_candidate_never_crosses_a_transition() {
            let p = proof_on(Arc::new(storage::MemoryStorageBackend::new()));
            let old = record(0, 1);
            store_record(&p.backend, &p.source_id, &old).await.unwrap();
            *p.active.write().unwrap() = Some(Stamp::of(&old));

            // The candidate proved a promotion (timeline 2, transition 1)
            // while the old stream was the authoritative one.
            let r = p.activate(opened(record(1, 2), true).await, false).await;
            assert!(r.is_err(), "the candidate is refused");
            assert_eq!(
                active(&p),
                Some(Stamp::of(&old)),
                "the old stamp stays"
            );
            assert_eq!(
                load_record(&p.backend, &p.source_id).await.unwrap(),
                Some(old.clone()),
                "the record stays"
            );

            // A candidate continuing the active stream is accepted.
            assert!(
                p.activate(opened(old.clone(), false).await, false)
                    .await
                    .is_ok()
            );
            assert_eq!(active(&p), Some(Stamp::of(&old)));
        }

        #[tokio::test]
        async fn a_failed_record_write_activates_nothing() {
            let backend = Arc::new(FaultBackend::new());
            backend
                .fail_writes_to
                .lock()
                .unwrap()
                .push("failover".into());
            let p = proof_on(backend);
            let r = p.activate(opened(record(0, 1), true).await, true).await;
            assert!(
                matches!(r, Err(SourceError::Checkpoint { .. })),
                "{:?}",
                r.err()
            );
            assert_eq!(active(&p), None, "no stamp");
            assert_eq!(
                load_record(&p.backend, &p.source_id).await.unwrap(),
                None
            );
        }

        #[tokio::test]
        async fn a_partial_continuity_stamp_fails_closed() {
            use deltaforge_core::incident::{
                ActionCode, EvidenceKey, EvidenceValue, ReasonCode,
                Retryability, SafetyState,
            };
            let p = proof_on(Arc::new(storage::MemoryStorageBackend::new()));
            p.checkpoints
                .put_raw(
                    &p.source_id,
                    br#"{"lsn":"0/3000","tx_id":null,"chain":"c","transition":1}"#,
                )
                .await
                .unwrap();
            let err = p.durable_checkpoint().await.unwrap_err();
            let d = err.draft().expect("an incident");
            assert_eq!(d.reason_code, ReasonCode::PgContinuityUnproven);
            assert_eq!(d.safety_state, SafetyState::HaltedSafe);
            assert_eq!(d.retryability, Retryability::OperatorAction);
            assert_eq!(
                d.actions,
                vec![ActionCode::InspectLogs, ActionCode::Resnapshot]
            );
            assert_eq!(
                d.evidence.get(EvidenceKey::ReasonClass),
                Some(&EvidenceValue::Text {
                    value: "checkpoint_chain_mismatch".into()
                })
            );
            assert_eq!(
                d.evidence.get(EvidenceKey::CheckpointChain),
                Some(&EvidenceValue::Text { value: "c".into() })
            );
        }

        #[tokio::test]
        async fn activation_persists_then_stamps() {
            let p = proof_on(Arc::new(storage::MemoryStorageBackend::new()));
            let new = record(1, 2);
            p.activate(opened(new.clone(), true).await, true)
                .await
                .unwrap();
            assert_eq!(
                load_record(&p.backend, &p.source_id).await.unwrap(),
                Some(new.clone())
            );
            assert_eq!(active(&p), Some(Stamp::of(&new)));
        }
    }

    mod server_in_recovery {
        use super::*;
        use deltaforge_core::incident::{
            ActionCode, EvidenceKey, EvidenceValue, Retryability,
        };
        use storage::adapters::incidents::{
            IncidentStatus, Resolution, bind_epoch,
        };

        pub(super) fn proof(window: Duration) -> StreamProof {
            StreamProof {
                dsn: crate::credentials::ProtectedDsn::from("host=unused"),
                slot: "s".into(),
                source_id: "src".into(),
                pipeline: "p".into(),
                tenant: "t".into(),
                checkpoints: Arc::new(
                    checkpoints::MemCheckpointStore::new().unwrap(),
                ),
                backend: Arc::new(storage::MemoryStorageBackend::new()),
                active: Default::default(),
                recovery_window: window,
                recovery_retry: Default::default(),
            }
        }

        fn checkpoint() -> Option<Lsn> {
            Some(Lsn::from(0x3000))
        }

        async fn records(
            p: &StreamProof,
        ) -> Vec<storage::adapters::incidents::IncidentRecord> {
            p.incidents().list().await.unwrap()
        }

        #[tokio::test]
        async fn within_the_window_it_retries_on_one_auto_retry_incident() {
            let p = proof(Duration::from_secs(60));
            for _ in 0..3 {
                let r = p.in_recovery(checkpoint(), Default::default()).await;
                assert!(
                    matches!(r, Err(SourceError::Connect { .. })),
                    "a retryable connection error: {r:?}"
                );
            }
            let all = records(&p).await;
            assert_eq!(all.len(), 1, "one incident across the retries");
            assert_eq!(all[0].retryability, Retryability::AutoRetry);
            assert_eq!(
                all[0].evidence.get(EvidenceKey::ReasonClass),
                Some(&EvidenceValue::Text {
                    value: "server_in_recovery".into()
                })
            );
            assert_eq!(
                all[0].actions,
                vec![ActionCode::VerifyEndpoint, ActionCode::InspectLogs],
                "route the source to the primary"
            );
        }

        #[tokio::test]
        async fn after_the_window_it_stops_on_the_same_incident() {
            let p = proof(Duration::from_millis(50));
            assert!(
                p.in_recovery(checkpoint(), Default::default())
                    .await
                    .is_err()
            );
            tokio::time::sleep(Duration::from_millis(80)).await;
            let err = p
                .in_recovery(checkpoint(), Default::default())
                .await
                .unwrap_err();
            let draft = err.draft().expect("an incident").clone();
            assert_eq!(draft.retryability, Retryability::OperatorAction);
            assert_eq!(
                draft.actions,
                vec![ActionCode::VerifyEndpoint, ActionCode::InspectLogs]
            );
            // Raised by the supervisor under the same recovery epoch, it is
            // the retrying incident, now needing an operator.
            let store = p.incidents();
            let epoch = store.recovery_epoch().await.unwrap();
            store.raise(&bind_epoch(draft, epoch), 1).await.unwrap();
            let all = records(&p).await;
            assert_eq!(all.len(), 1);
            assert_eq!(all[0].retryability, Retryability::OperatorAction);
        }

        #[tokio::test]
        async fn a_stop_withdraws_and_a_proven_start_resolves_it() {
            let p = proof(Duration::from_secs(60));
            let _ = p.in_recovery(checkpoint(), Default::default()).await;
            p.recovery_cancelled().await;
            assert!(matches!(
                records(&p).await[0].status,
                IncidentStatus::Resolved {
                    by: Resolution::OperationCancelled,
                    ..
                }
            ));

            let p = proof(Duration::from_secs(60));
            let _ = p.in_recovery(checkpoint(), Default::default()).await;
            p.recovery_settled().await;
            assert!(matches!(
                &records(&p).await[0].status,
                IncidentStatus::Resolved {
                    by: Resolution::VerifiedRecovery { check },
                    ..
                } if check == storage::adapters::incidents::POSITION_VERIFIED
            ));
            // The window starts again after a proven start.
            assert!(p.recovery_retry.lock().unwrap().since.is_none());
        }
    }

    #[test]
    fn test_pg_timestamp_conversion() {
        let pg_ts = 631_152_000_000_000i64; // 2020-01-01 in PG epoch
        assert_eq!(pg_timestamp_to_unix_ms(pg_ts), 1_577_836_800_000);
    }

    #[test]
    fn test_is_retryable_source_error() {
        assert!(is_retryable_source_error(&SourceError::Timeout {
            action: "x".into()
        }));
        assert!(is_retryable_source_error(&SourceError::Connect {
            details: "failed".into()
        }));
        assert!(!is_retryable_source_error(&SourceError::Cancelled));
        assert!(!is_retryable_source_error(&SourceError::Auth {
            details: "denied".into()
        }));
    }

    #[test]
    fn test_pgwire_error_to_source_error() {
        let io =
            |s| PgWireError::Io(std::sync::Arc::new(std::io::Error::other(s)));

        assert!(matches!(
            pgwire_error_to_source_error(PgWireError::Auth("x".into())),
            SourceError::Auth { .. }
        ));
        assert!(matches!(
            pgwire_error_to_source_error(io("x")),
            SourceError::Connect { .. }
        ));
        assert!(matches!(
            pgwire_error_to_source_error(PgWireError::Tls("x".into())),
            SourceError::Incompatible { .. }
        ));
    }
}

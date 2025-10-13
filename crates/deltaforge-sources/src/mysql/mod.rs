//! MySQL source module.
//! Uses binlog steaming to consume the change events
//! and send them to the next layer (processors)

use std::{
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use anyhow::Result;
use async_trait::async_trait;
use mysql_binlog_connector_rust::{
    binlog_client::BinlogClient, binlog_stream::BinlogStream,
    event::table_map_event::TableMapEvent,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Notify};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use deltaforge_checkpoints::{CheckpointStore, CheckpointStoreExt};
use deltaforge_core::{Event, Source, SourceHandle};

mod mysql_helpers;
use mysql_helpers::{pause_until_resumed, prepare_client, AllowList};

mod object;

mod mysql_schema;
use mysql_schema::MySqlSchemaCache;

mod mysql_event;
use mysql_event::*;

use crate::{
    conn_utils::RetryPolicy,
    mysql::mysql_helpers::{connect_binlog_with_retries, resolve_binlog_tail},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MySqlCheckpoint {
    pub file: String,
    pub pos: u64,
    pub gtid_set: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MySqlSource {
    pub id: String,          // checkpoint key + server_id seed
    pub dsn: String,         // mysql://user:pass@host:3306/db
    pub tables: Vec<String>, // ["db.table"]; empty = all
    pub tenant: String,
}

// shared state for mysql source
struct RunCtx {
    source_id: String,
    tenant: String,
    dsn: String,
    host: String,
    default_db: String,
    server_id: u64,
    tx: mpsc::Sender<Event>,
    chkpt: Arc<dyn CheckpointStore>,
    cancel: CancellationToken,
    paused: Arc<AtomicBool>,
    pause_notify: Arc<Notify>,
    schema: MySqlSchemaCache,
    allow: AllowList,
    retry: RetryPolicy,
    inactivity: Duration,
    table_map: HashMap<u64, TableMapEvent>,
    last_file: String,
    last_pos: u64,
    last_gtid: Option<String>,
}

impl MySqlSource {
    async fn run_inner(
        &self,
        tx: mpsc::Sender<Event>,
        chkpt_store: Arc<dyn CheckpointStore>,
        cancel: CancellationToken,
        paused: Arc<AtomicBool>,
        pause_notify: Arc<Notify>,
    ) -> Result<()> {
        let (host, default_db, server_id, client) =
            prepare_client(&self.dsn, &self.id, &self.tables, &chkpt_store)
                .await?;

        info!(source_id=%self.id, host=%host, db=%default_db, "MySQL CDC: starting source");

        let mut ctx = RunCtx {
            source_id: self.id.clone(),
            tenant: self.tenant.clone(),
            dsn: self.dsn.clone(),
            host,
            default_db,
            server_id,
            tx,
            chkpt: chkpt_store,
            cancel,
            paused,
            pause_notify,
            schema: MySqlSchemaCache::new(&self.dsn),
            allow: AllowList::new(&self.tables),
            retry: RetryPolicy::default(),
            inactivity: Duration::from_secs(60),
            table_map: HashMap::new(),
            last_file: String::new(),
            last_pos: 0,
            last_gtid: None,
        };

        let mut stream = connect_first_stream(&ctx, client).await?;

        info!("entering binlog read loop");
        loop {
            if !pause_until_resumed(&ctx.cancel, &ctx.paused, &ctx.pause_notify)
                .await
            {
                break;
            }

            match read_next_event(&mut stream, &mut ctx).await {
                Ok((header, data)) => {
                    ctx.last_pos = header.next_event_position as u64;
                    dispatch_event(&mut ctx, &header, data).await?;
                }
                Err(LoopControl::Reconnect) => {
                    stream = reconnect_stream(&mut ctx).await?;
                }
                Err(LoopControl::Stop) => break,
                Err(LoopControl::Fail(e)) => return Err(e),
            }
        }

        // best-effort final checkpoint update
        let _ = ctx
            .chkpt
            .put(
                &ctx.source_id,
                MySqlCheckpoint {
                    file: ctx.last_file,
                    pos: ctx.last_pos,
                    gtid_set: ctx.last_gtid,
                },
            )
            .await;

        Ok(())
    }
}

#[async_trait]
impl Source for MySqlSource {
    /// start the source in a background task and return a control handle.
    /// outside callers have to use this to start a CDC source.
    /// see `SourceHandler` for available controls returned by run.
    async fn run(
        &self,
        tx: mpsc::Sender<Event>,
        chkpt_store: Arc<dyn CheckpointStore>,
    ) -> SourceHandle {
        let cancel = CancellationToken::new();
        let paused = Arc::new(AtomicBool::new(false));
        let pause_notify = Arc::new(Notify::new());

        let this = self.clone();
        let cancel_for_task = cancel.clone();
        let paused_for_task = paused.clone();
        let pause_notify_for_task = pause_notify.clone();

        let join = tokio::spawn(async move {
            let res = this
                .run_inner(
                    tx,
                    chkpt_store,
                    cancel_for_task,
                    paused_for_task,
                    pause_notify_for_task,
                )
                .await;
            if let Err(e) = &res {
                error!(error=?e, "mysql source task ended with error");
            }
            res
        });

        SourceHandle {
            cancel,
            paused,
            pause_notify,
            join,
        }
    }
}

/// build a factory from the inital client's resume point
async fn connect_first_stream(
    ctx: &RunCtx,
    client: BinlogClient,
) -> Result<BinlogStream> {
    let init_gtid = client.gtid_enabled.then(|| client.gtid_set.clone());
    let init_file = if client.gtid_enabled {
        None
    } else {
        Some(client.binlog_filename.clone())
    };
    let init_pos = if client.gtid_enabled {
        None
    } else {
        Some(client.binlog_position)
    };

    let dsn = ctx.dsn.clone();
    let sid = ctx.server_id;
    let make_client = move || {
        let mut c = BinlogClient::default();
        c.url = dsn.clone();
        c.server_id = sid;
        c.heartbeat_interval_secs = 15;
        c.timeout_secs = 60;

        if let Some(gtid) = init_gtid.clone() {
            c.gtid_enabled = true;
            c.gtid_set = gtid
        } else if let (Some(fname), Some(pos)) = (init_file.clone(), init_pos) {
            c.gtid_enabled = false;
            c.binlog_filename = fname;
            c.binlog_position = pos;
        }
        c
    };

    let stream = connect_binlog_with_retries(
        &ctx.source_id,
        make_client,
        &ctx.cancel,
        &ctx.default_db,
        ctx.retry.clone(),
    )
    .await?;

    Ok(stream)
}

async fn reconnect_stream(ctx: &mut RunCtx) -> Result<BinlogStream> {
    //use mysql_binlog_connector_rust::binlog_client::BinlogClient;

    // Choose best resume: GTID > file:pos > tail
    let (gtid_to_use, file_to_use, pos_to_use) = if let Some(g) = &ctx.last_gtid
    {
        (Some(g.clone()), None, None)
    } else if !ctx.last_file.is_empty() && ctx.last_pos > 0 {
        (None, Some(ctx.last_file.clone()), Some(ctx.last_pos as u32))
    } else {
        match resolve_binlog_tail(&ctx.dsn).await {
            Ok((f, p)) => (None, Some(f), Some(p as u32)),
            Err(err) => return Err(err),
        }
    };

    let dsn = ctx.dsn.clone();
    let sid = ctx.server_id;
    let make_client = move || {
        let mut c = BinlogClient::default();
        c.url = dsn.clone();
        c.server_id = sid;
        c.heartbeat_interval_secs = 15;
        c.timeout_secs = 60;
        if let Some(g) = gtid_to_use.clone() {
            c.gtid_enabled = true;
            c.gtid_set = g;
        } else if let (Some(f), Some(p)) = (file_to_use.clone(), pos_to_use) {
            c.gtid_enabled = false;
            c.binlog_filename = f;
            c.binlog_position = p;
        }
        c
    };

    let stream = connect_binlog_with_retries(
        &ctx.source_id,
        make_client,
        &ctx.cancel,
        &ctx.default_db,
        ctx.retry.clone(),
    )
    .await?;

    Ok(stream)
}

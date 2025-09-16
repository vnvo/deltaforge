use anyhow::Result;
use axum::Router; // no Server in axum 0.8
use clap::Parser;
use deltaforge_api::{router, AppState, PipeInfo};
use deltaforge_config::{load_from_path, ProcessorCfg, SinkCfg, SourceCfg};
use deltaforge_core::{DynProcessor, DynSink, DynSource, Pipeline};
use deltaforge_processor_js::JsProcessor;
use deltaforge_sinks::{kafka::KafkaSink, redis::RedisSink};
use deltaforge_sources::{mysql::MysqlSource, postgres::PostgresSource};
use deltaforge_metrics as metrics
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::info;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    config: String,
    #[arg(long, default_value = "0.0.0.0:8080")]
    api_addr: String,
    #[arg(long, default_value = "0.0.0.0:9095")]
    metrics_addr: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    metrics::init();
    let args = Args::parse();
    metrics::install_prometheus(&args.metrics_addr);    
    let spec = load_from_path(&args.config)?;

    // Build sources
    let mut sources: Vec<DynSource> = Vec::new();
    for src in &spec.spec.sources {
        match src {
            SourceCfg::Postgres {
                id,
                dsn,
                publication,
                slot,
                tables,
            } => {
                sources.push(Box::new(PostgresSource {
                    id: id.clone(),
                    dsn: dsn.clone(),
                    tenant: spec.metadata.tenant.clone(),
                    tables: tables.clone(),
                    publication: publication.clone(),
                    slot: slot.clone(),
                }));
            }
            SourceCfg::Mysql { id, dsn, tables } => {
                sources.push(Box::new(MysqlSource {
                    id: id.clone(),
                    dsn: dsn.clone(),
                    tables: tables.clone(),
                    tenant: spec.metadata.tenant.clone(),
                }));
            }
        }
    }

    // Build processors
    let mut processors: Vec<DynProcessor> = Vec::new();
    for p in &spec.spec.processors {
        match p {
            // NOTE: variant is `JavaScript` (capital S) per your config crate
            ProcessorCfg::Javascript {
                id: _,
                inline,
                limits: _,
            } => {
                processors.push(Box::new(JsProcessor::new(inline.clone())));
            }
        }
    }

    // Build sinks
    let mut sinks: Vec<DynSink> = Vec::new();
    for s in &spec.spec.sinks {
        match s {
            // field is `exactlyOnce` in your config (camelCase); ignore it here
            SinkCfg::Kafka {
                id: _,
                brokers,
                topic,
                exactly_once: _,
            } => {
                sinks.push(Box::new(KafkaSink::new(brokers, topic)?));
            }
            // pass the configured stream instead of hardcoding "events"
            SinkCfg::Redis { id: _, uri, stream } => {
                sinks.push(Box::new(RedisSink::new(uri, stream)?));
            }
        }
    }

    // Start pipeline
    let pipeline = Pipeline {
        sources,
        processors,
        sinks,
    };
    tokio::spawn(async move {
        if let Err(e) = pipeline.start().await {
            tracing::error!(error = %e, "pipeline stopped with error");
        }
    });

    // Start API (axum 0.8 style)
    let state = AppState::default();
    state.pipelines.write().push(PipeInfo {
        id: spec.metadata.name.clone(),
        status: "running".into(),
    });
    let app: Router = router(state);

    let addr: SocketAddr =
        args.api_addr.parse().expect("api_addr must be host:port");
    info!(%addr, "api listening");

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

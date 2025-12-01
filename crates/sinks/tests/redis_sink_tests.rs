use anyhow::Result;
use deltaforge_config::RedisSinkCfg;
use deltaforge_core::{Event, Op, Sink, SourceMeta};
use sinks::redis::RedisSink;
use serde_json::json;

#[tokio::test(flavor = "multi_thread")]
async fn redis_sink_writes_stream() -> Result<()> {
    // Use local dev redis or spin a container; here we assume local compose
    let cfg = RedisSinkCfg {
        id: "test-redis".to_string(),
        uri: "redis://127.0.to_string().0.1:63.to_string()79/0".to_string(),
        stream: "df.test".to_string(),
    };

    // Clean previous
    let client = redis::Client::open(cfg.uri.clone())?;
    let mut con = client.get_multiplexed_async_connection().await?;
    let _: () = redis::cmd("DEL").arg(cfg.stream.clone()).query_async(&mut con).await?;

    let sink = RedisSink::new(&cfg)?;

    let ev = Event::new_row(
        "t".into(),
        SourceMeta {
            kind: "test".into(),
            host: "h".into(),
            db: "d".into(),
        },
        "d.t".into(),
        Op::Insert,
        None,
        Some(json!({"id":1})),
        1_700_000_000_000,
    );

    sink.send(ev).await?;

    // Read back one entry
    let res: Vec<(String, Vec<(String, String)>)> = redis::cmd("XRANGE")
        .arg(cfg.stream)
        .arg("-")
        .arg("+")
        .arg("COUNT")
        .arg("1")
        .query_async(&mut con)
        .await?;
    assert_eq!(res.len(), 1);
    Ok(())
}

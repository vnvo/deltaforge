use anyhow::Result;
use deltaforge_config::RedisSinkCfg;
use deltaforge_core::{Event, Op, Sink, SourceMeta};
use serde_json::json;
use sinks::redis::RedisSink;
use redis::AsyncCommands;

/// This test assumes a Redis instance is running on localhost:6379 (db 0)
/// and that it's OK to create/delete the `df.test` stream.
#[tokio::test(flavor = "multi_thread")]
async fn redis_sink_writes_stream() -> Result<()> {
    // Arrange: config for the sink
    let cfg = RedisSinkCfg {
        id: "test-redis".to_string(),
        uri: "redis://127.0.0.1:6379/0".to_string(),
        stream: "df.test".to_string(),
    };

    // Clean previous data in the test stream
    let client = redis::Client::open(cfg.uri.clone())?;
    let mut con = client.get_multiplexed_async_connection().await?;
    let _: () = con.del(&cfg.stream).await.unwrap_or(());

    // Build the sink
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

    // Act: send the event into the Redis sink
    sink.send(ev).await?;

    // Assert: read back one entry from the stream
    let res: Vec<(String, Vec<(String, String)>)> = redis::cmd("XRANGE")
        .arg(&cfg.stream)
        .arg("-")
        .arg("+")
        .arg("COUNT")
        .arg("1")
        .query_async(&mut con)
        .await?;

    assert_eq!(res.len(), 1, "expected exactly one entry in the stream");

    let (_id, fields) = &res[0];
    assert!(
        !fields.is_empty(),
        "expected at least one field in the stream entry"
    );

    Ok(())
}

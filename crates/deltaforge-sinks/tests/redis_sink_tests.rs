use anyhow::Result;
use deltaforge_core::{Event, Op, SourceMeta, Sink};
use deltaforge_sinks::redis::RedisSink;
use serde_json::json;

#[tokio::test(flavor = "multi_thread")]
async fn redis_sink_writes_stream() -> Result<()> {
    // Use local dev redis or spin a container; here we assume local compose
    let uri = "redis://127.0.0.1:6379/0";
    let stream = "df.test";

    // Clean previous
    let client = redis::Client::open(uri)?;
    let mut con = client.get_multiplexed_async_connection().await?;
    let _: () = redis::cmd("DEL").arg(stream).query_async(&mut con).await?;

    let sink = RedisSink::new(&uri.to_string(), &stream.to_string())?;

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
        .arg(stream)
        .arg("-")
        .arg("+")
        .arg("COUNT")
        .arg("1")
        .query_async(&mut con)
        .await?;
    assert_eq!(res.len(), 1);
    Ok(())
}

use anyhow::Result;
use deltaforge_config::KafkaSinkCfg;
use deltaforge_core::{Event, Op, Sink, SourceMeta};
use serde_json::json;
use sinks::kafka::KafkaSink;

use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use tokio::time::{Duration, timeout};

fn make_test_event() -> Event {
    Event::new_row(
        "t".into(),
        SourceMeta {
            kind: "test".into(),
            host: "h".into(),
            db: "d".into(),
        },
        "d.t".into(),
        Op::Insert,
        None,
        Some(json!({ "id": 1 })),
        1_700_000_000_000,
        4 as usize,
    )
}

/// This test assumes a Kafka broker is available at localhost:9092 and
/// auto-topic-creation is enabled.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn kafka_sink_writes_to_topic() -> Result<()> {
    let cfg = KafkaSinkCfg {
        id: "test-kafka".to_string(),
        brokers: "localhost:9092".to_string(),
        topic: "df.test.kafka".to_string(),
        required: None,
        exactly_once: None,
        client_conf: Default::default(),
    };

    // Produce one event via our sink
    let sink = KafkaSink::new(&cfg)?;
    let ev = make_test_event();
    sink.send(ev).await?;

    // Build a consumer to verify the message is on the topic
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.brokers)
        .set("group.id", "deltaforge-kafka-sink-test")
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer.subscribe(&[&cfg.topic])?;

    // Wait up to 10 seconds for a message
    let msg = timeout(Duration::from_secs(10), consumer.recv()).await??;

    assert_eq!(msg.topic(), cfg.topic);
    assert!(msg.payload().is_some(), "expected non-empty payload");

    Ok(())
}

/// Smoke test for the `exactly_once` branch in KafkaSink::new.
/// We don't validate EOS semantics here, just that send works.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn kafka_sink_with_exactly_once_enabled() -> Result<()> {
    let cfg = KafkaSinkCfg {
        id: "test-kafka-eo".to_string(),
        brokers: "localhost:9092".to_string(),
        topic: "df.test.kafka.eo".to_string(),
        required: None,
        exactly_once: Some(true),
        client_conf: Default::default(),
    };

    let sink = KafkaSink::new(&cfg)?;
    let ev = make_test_event();
    sink.send(ev).await?;

    // If we got here without error, producer creation + send succeeded.
    Ok(())
}

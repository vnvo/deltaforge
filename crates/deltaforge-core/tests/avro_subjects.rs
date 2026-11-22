//! The schema registry subjects of row events: several tables on one topic
//! register under one subject per table with the record-name strategies,
//! and under the topic's one subject with `topic_name`, for both the
//! DDL-derived schemas and inferred ones.

mod support;

use std::sync::Arc;

use deltaforge_core::NativeOpts;
use deltaforge_core::encoding::avro::{
    AvroEncoder, SourceSchemaProvider, SubjectStrategy,
};
use deltaforge_core::envelope::EnvelopeType;
use support::avro::{Provider, event, registry};

/// The subjects registered for one event of each of `orders` and `users`
/// on topic `cdc`.
async fn subjects(strategy: SubjectStrategy, ddl_schema: bool) -> Vec<String> {
    let (url, seen) = registry();
    let provider: Option<Arc<dyn SourceSchemaProvider>> =
        ddl_schema.then(|| Arc::new(Provider { extra: 0 }) as _);
    let encoder =
        AvroEncoder::with_source_schemas(&url, strategy, None, None, provider)
            .unwrap();
    let envelope = EnvelopeType::Native.build();
    for table in ["orders", "users"] {
        let e = event(table, ddl_schema);
        let data = envelope.wrap(&e, NativeOpts::default()).unwrap();
        encoder.encode_event("cdc", &e, &data).await.unwrap();
    }
    seen.lock().unwrap().clone()
}

#[tokio::test]
async fn record_name_strategies_give_each_table_of_a_topic_its_subject() {
    let envelope = |t: &str| format!("deltaforge.cdc.mysql.shop.{t}.Envelope");
    assert_eq!(
        subjects(SubjectStrategy::TopicRecordName, true).await,
        [
            format!("cdc-{}-v2", envelope("orders")),
            format!("cdc-{}-v2", envelope("users"))
        ]
    );
    assert_eq!(
        subjects(SubjectStrategy::RecordName, true).await,
        [
            format!("{}-v2", envelope("orders")),
            format!("{}-v2", envelope("users"))
        ]
    );
    // Inferred schemas are named per table too.
    let value = |t: &str| format!("deltaforge.mysql.shop.{t}.Value");
    assert_eq!(
        subjects(SubjectStrategy::TopicRecordName, false).await,
        [
            format!("cdc-{}-v2", value("orders")),
            format!("cdc-{}-v2", value("users"))
        ]
    );
}

#[tokio::test]
async fn topic_name_keeps_one_subject_per_topic() {
    assert_eq!(
        subjects(SubjectStrategy::TopicName, true).await,
        ["cdc-value-v2", "cdc-value-v2"]
    );
}

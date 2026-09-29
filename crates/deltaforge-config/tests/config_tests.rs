use deltaforge_config::{
    CollisionPolicy, CommitPolicy, ConfigError, EmptyListPolicy,
    EmptyObjectPolicy, EncodingCfg, EnvelopeCfg, ListPolicy, ProcessorCfg,
    SinkCfg, SourceCfg, load_from_path,
};
use pretty_assertions::assert_eq;
use serial_test::serial;
use std::io::Write;

fn write_temp(contents: &str) -> tempfile::TempPath {
    let mut f = tempfile::NamedTempFile::new().expect("temp file");
    f.write_all(contents.as_bytes()).expect("write");
    f.into_temp_path()
}

// ============================================================================
// Core Pipeline Parsing
// ============================================================================

#[test]
#[serial]
#[allow(unsafe_code)]
fn parses_postgres_pipeline_with_env_expansion() {
    unsafe {
        std::env::set_var(
            "PG_ORDERS_DSN",
            "postgres://pgu:pgpass@localhost:5432/orders",
        );
    }

    let yaml = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: unit
  tenant: test
spec:
  source:
    type: postgres
    config:
      id: pg
      dsn: ${PG_ORDERS_DSN}
      publication: df_pub
      slot: df_slot
      tables: [public.t1]
      start_position: latest
  processors:
    - type: javascript
      id: js
      inline: |
        return [event];
  sinks:
    - type: kafka
      config:
        id: k
        brokers: localhost:9092
        topic: unit.events
"#;

    let path = write_temp(yaml);
    let spec = load_from_path(path.to_str().unwrap()).expect("parse yaml");

    assert_eq!(spec.metadata.name, "unit");
    assert_eq!(spec.metadata.tenant, "test");

    match &spec.spec.source {
        SourceCfg::Postgres(pc) => {
            assert_eq!(
                pc.dsn.as_deref(),
                Some("postgres://pgu:pgpass@localhost:5432/orders")
            );
            assert!(matches!(
                pc.start_position,
                deltaforge_config::PostgresStartPosition::Latest
            ));
        }
        _ => panic!("expected postgres source"),
    }

    assert_eq!(spec.spec.processors.len(), 1);
    match &spec.spec.processors[0] {
        ProcessorCfg::Javascript { id, inline, .. } => {
            assert_eq!(id, "js");
            assert!(inline.contains("return [event];"));
        }
        _ => {
            panic!("expected js processor, got something else")
        }
    }

    match &spec.spec.sinks[0] {
        SinkCfg::Kafka(kc) => {
            assert_eq!(kc.topic, "unit.events");
            // Verify defaults
            assert_eq!(kc.envelope, EnvelopeCfg::Native);
            assert_eq!(kc.encoding, EncodingCfg::Json);
        }
        _ => panic!("expected kafka sink"),
    }
}

#[test]
#[serial]
#[allow(unsafe_code)]
fn parses_mysql_with_multiple_sinks() {
    unsafe {
        std::env::set_var(
            "MYSQL_ORDERS_DSN",
            "mysql://root:pws@localhost:3306/orders",
        );
    }

    let yaml = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: unit2, tenant: t }
spec:
  source:
    type: mysql
    config:
      id: m
      dsn: ${MYSQL_ORDERS_DSN}
      tables: [orders, order_items]
  processors: []
  sinks:
    - type: kafka
      config:
        id: k
        brokers: localhost:9092
        topic: t.orders
    - type: redis
      config:
        id: r
        uri: redis://127.0.0.1:6379
        stream: s
    - type: nats
      config:
        id: n
        url: nats://localhost:4222
        subject: events
"#;

    let path = write_temp(yaml);
    let spec = load_from_path(path.to_str().unwrap()).expect("parse ok");

    match &spec.spec.source {
        SourceCfg::Mysql(mc) => {
            assert_eq!(mc.tables, vec!["orders", "order_items"]);
        }
        _ => panic!("expected mysql source"),
    }

    assert_eq!(spec.spec.sinks.len(), 3);
    assert!(matches!(&spec.spec.sinks[0], SinkCfg::Kafka(_)));
    assert!(matches!(&spec.spec.sinks[1], SinkCfg::Redis(_)));
    assert!(matches!(&spec.spec.sinks[2], SinkCfg::Nats(_)));
}

#[test]
#[serial]
fn invalid_yaml_returns_parse_error() {
    let yaml = "this is: [ definitely: not: valid: yaml";
    let path = write_temp(yaml);
    let err = load_from_path(path.to_str().unwrap()).expect_err("should fail");
    assert!(matches!(err, ConfigError::Parse { .. }));
}

// ============================================================================
// Batch and Commit Policy
// ============================================================================

#[test]
#[serial]
fn batch_and_commit_policy_parsing() {
    let yaml = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: batch_test, tenant: t }
spec:
  batch:
    max_events: 1000
    max_bytes: 65536
    max_ms: 250
    respect_source_tx: true
    max_inflight: 4
  commit_policy:
    mode: quorum
    quorum: 2
  source:
    type: postgres
    config:
      id: pg
      dsn: postgres://u:p@localhost/db
      publication: pub
      slot: slot
      tables: [t1]
  processors: []
  sinks: []
"#;

    let path = write_temp(yaml);
    let spec = load_from_path(path.to_str().unwrap()).expect("parse ok");

    let batch = spec.spec.batch.as_ref().expect("batch present");
    assert_eq!(batch.max_events, Some(1000));
    assert_eq!(batch.max_bytes, Some(65536));
    assert_eq!(batch.max_ms, Some(250));
    assert_eq!(batch.respect_source_tx, Some(true));
    assert_eq!(batch.max_inflight, Some(4));

    match spec.spec.commit_policy {
        Some(CommitPolicy::Quorum { quorum }) => assert_eq!(quorum, 2),
        other => panic!("expected Quorum, got {other:?}"),
    }
}

#[test]
#[serial]
fn commit_policy_all_variants() {
    for (mode, expected) in [
        ("all", CommitPolicy::All),
        ("required", CommitPolicy::Required),
    ] {
        let yaml = format!(
            r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: {{ name: cp_{mode}, tenant: t }}
spec:
  commit_policy:
    mode: {mode}
  source:
    type: postgres
    config:
      id: pg
      dsn: postgres://u:p@localhost/db
      publication: pub
      slot: slot
      tables: [t1]
  processors: []
  sinks: []
"#
        );
        let spec = load_from_path(write_temp(&yaml).to_str().unwrap()).unwrap();
        assert_eq!(spec.spec.commit_policy, Some(expected));
    }
}

// ============================================================================
// Schema Sensing
// ============================================================================

#[test]
#[serial]
fn schema_sensing_config_parsing() {
    let yaml = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: sensing, tenant: t }
spec:
  schema_sensing:
    enabled: true
    deep_inspect:
      enabled: true
      max_depth: 5
      max_sample_size: 500
    sampling:
      warmup_events: 100
      sample_rate: 10
      structure_cache: true
      structure_cache_size: 50
  source:
    type: mysql
    config:
      id: m
      dsn: mysql://root:pw@localhost:3306/db
      tables: [orders]
  processors: []
  sinks: []
"#;

    let path = write_temp(yaml);
    let spec = load_from_path(path.to_str().unwrap()).expect("parse ok");

    let sensing = &spec.spec.schema_sensing;
    assert!(sensing.enabled);
    assert!(sensing.deep_inspect.enabled);
    assert_eq!(sensing.deep_inspect.max_depth, 5);
    assert_eq!(sensing.sampling.warmup_events, 100);
    assert_eq!(sensing.sampling.sample_rate, 10);
    assert!(sensing.sampling.structure_cache);
}

// ============================================================================
// Envelope and Encoding Configuration
// ============================================================================

#[test]
#[serial]
fn all_envelope_types_across_sinks() {
    let yaml = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: envelopes, tenant: t }
spec:
  source:
    type: postgres
    config:
      id: pg
      dsn: postgres://u:p@localhost/db
      publication: pub
      slot: slot
      tables: [events]
  processors: []
  sinks:
    # Native envelope (default)
    - type: kafka
      config:
        id: kafka-native
        brokers: localhost:9092
        topic: events.native
    # Debezium envelope
    - type: redis
      config:
        id: redis-debezium
        uri: redis://localhost:6379
        stream: events
        envelope:
          type: debezium
    # CloudEvents envelope
    - type: nats
      config:
        id: nats-cloudevents
        url: nats://localhost:4222
        subject: events
        envelope:
          type: cloudevents
          type_prefix: "com.example.cdc"
"#;

    let path = write_temp(yaml);
    let spec = load_from_path(path.to_str().unwrap()).expect("parse ok");

    // Kafka: native (default)
    match &spec.spec.sinks[0] {
        SinkCfg::Kafka(kc) => {
            assert_eq!(kc.envelope, EnvelopeCfg::Native);
            assert_eq!(kc.encoding, EncodingCfg::Json);
        }
        _ => panic!("expected kafka"),
    }

    // Redis: debezium
    match &spec.spec.sinks[1] {
        SinkCfg::Redis(rc) => {
            assert_eq!(rc.envelope, EnvelopeCfg::Debezium);
        }
        _ => panic!("expected redis"),
    }

    // NATS: cloudevents
    match &spec.spec.sinks[2] {
        SinkCfg::Nats(nc) => {
            assert_eq!(
                nc.envelope,
                EnvelopeCfg::CloudEvents {
                    type_prefix: "com.example.cdc".to_string()
                }
            );
        }
        _ => panic!("expected nats"),
    }
}

#[test]
#[serial]
fn envelope_and_encoding_conversion_to_core() {
    // Envelope conversions
    let native = EnvelopeCfg::Native.to_envelope_type();
    let debezium = EnvelopeCfg::Debezium.to_envelope_type();
    let cloudevents = EnvelopeCfg::CloudEvents {
        type_prefix: "com.test".to_string(),
    }
    .to_envelope_type();

    assert!(matches!(
        native,
        deltaforge_core::envelope::EnvelopeType::Native
    ));
    assert!(matches!(
        debezium,
        deltaforge_core::envelope::EnvelopeType::Debezium
    ));
    match cloudevents {
        deltaforge_core::envelope::EnvelopeType::CloudEvents {
            type_prefix,
        } => {
            assert_eq!(type_prefix, "com.test");
        }
        _ => panic!("expected CloudEvents"),
    }

    // Encoding conversion
    let json = EncodingCfg::Json.to_encoding_type();
    assert!(matches!(
        json,
        deltaforge_core::encoding::EncodingType::Json
    ));
}

// ============================================================================
// Full Sink Configuration (timeouts, auth, client_conf)
// ============================================================================

#[test]
#[serial]
fn kafka_sink_full_configuration() {
    let yaml = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: kafka_full, tenant: t }
spec:
  source:
    type: mysql
    config:
      id: m
      dsn: mysql://root:pw@localhost:3306/db
      tables: [orders]
  processors: []
  sinks:
    - type: kafka
      config:
        id: kafka-prod
        brokers: broker1:9092,broker2:9092
        topic: orders.events
        envelope:
          type: cloudevents
          type_prefix: "com.example.shop"
        encoding: json
        required: true
        exactly_once: false
        send_timeout_secs: 60
        client_conf:
          security.protocol: SASL_SSL
          sasl.mechanism: PLAIN
          linger.ms: "10"
"#;

    let path = write_temp(yaml);
    let spec = load_from_path(path.to_str().unwrap()).expect("parse ok");

    match &spec.spec.sinks[0] {
        SinkCfg::Kafka(kc) => {
            assert_eq!(kc.brokers, "broker1:9092,broker2:9092");
            assert_eq!(
                kc.envelope,
                EnvelopeCfg::CloudEvents {
                    type_prefix: "com.example.shop".to_string()
                }
            );
            assert_eq!(kc.required, Some(true));
            assert_eq!(kc.exactly_once, Some(false));
            assert_eq!(kc.send_timeout_secs, Some(60));
            assert_eq!(
                kc.client_conf.get("security.protocol").map(String::as_str),
                Some("SASL_SSL")
            );
        }
        _ => panic!("expected kafka"),
    }
}

#[test]
#[serial]
fn nats_sink_with_jetstream_and_auth() {
    let yaml = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: nats_full, tenant: t }
spec:
  source:
    type: mysql
    config:
      id: m
      dsn: mysql://root:pw@localhost:3306/db
      tables: [orders]
  processors: []
  sinks:
    - type: nats
      config:
        id: nats-prod
        url: nats://nats1:4222,nats://nats2:4222
        subject: orders.>
        stream: ORDERS
        envelope:
          type: cloudevents
          type_prefix: "io.nats.orders"
        required: true
        send_timeout_secs: 5
        credentials_file: /etc/nats/user.creds
"#;

    let path = write_temp(yaml);
    let spec = load_from_path(path.to_str().unwrap()).expect("parse ok");

    match &spec.spec.sinks[0] {
        SinkCfg::Nats(nc) => {
            assert_eq!(nc.url, "nats://nats1:4222,nats://nats2:4222");
            assert_eq!(nc.stream, Some("ORDERS".to_string()));
            assert_eq!(
                nc.envelope,
                EnvelopeCfg::CloudEvents {
                    type_prefix: "io.nats.orders".to_string()
                }
            );
            assert_eq!(
                nc.credentials_file.as_deref(),
                Some("/etc/nats/user.creds")
            );
        }
        _ => panic!("expected nats"),
    }
}

// ============================================================================
// Dynamic Routing Configuration (key + template topics)
// ============================================================================

/// Verifies `key` field and template strings parse for all sink types.
/// Template vars like `${source.table}` pass through env expansion
/// (they're compiled later by the sink at runtime).
#[test]
#[serial]
fn sink_key_and_template_topic_parsing() {
    let yaml = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: routing_cfg, tenant: t }
spec:
  source:
    type: mysql
    config:
      id: m
      dsn: mysql://root:pw@localhost:3306/db
      tables: [orders, users]
  processors: []
  sinks:
    - type: kafka
      config:
        id: kafka-routed
        brokers: localhost:9092
        topic: "cdc.${source.table}"
        key: "${after.customer_id}"
        envelope:
          type: debezium
    - type: redis
      config:
        id: redis-routed
        uri: redis://localhost:6379
        stream: "events.${source.db}.${source.table}"
        key: "${after.id}"
    - type: nats
      config:
        id: nats-routed
        url: nats://localhost:4222
        subject: "cdc.${source.db}.${source.table}"
        key: "${after.order_id}"
        stream: CDC
"#;

    let path = write_temp(yaml);
    let spec = load_from_path(path.to_str().unwrap()).expect("parse ok");

    // Kafka: template topic + key preserved
    match &spec.spec.sinks[0] {
        SinkCfg::Kafka(kc) => {
            assert_eq!(kc.topic, "cdc.${source.table}");
            assert_eq!(kc.key.as_deref(), Some("${after.customer_id}"));
        }
        _ => panic!("expected kafka"),
    }

    // Redis: template stream + key preserved
    match &spec.spec.sinks[1] {
        SinkCfg::Redis(rc) => {
            assert_eq!(rc.stream, "events.${source.db}.${source.table}");
            assert_eq!(rc.key.as_deref(), Some("${after.id}"));
        }
        _ => panic!("expected redis"),
    }

    // NATS: template subject + key preserved
    match &spec.spec.sinks[2] {
        SinkCfg::Nats(nc) => {
            assert_eq!(nc.subject, "cdc.${source.db}.${source.table}");
            assert_eq!(nc.key.as_deref(), Some("${after.order_id}"));
        }
        _ => panic!("expected nats"),
    }
}

/// key defaults to None when omitted (existing configs unaffected).
#[test]
#[serial]
fn sink_key_defaults_to_none() {
    let yaml = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: no_key, tenant: t }
spec:
  source:
    type: mysql
    config:
      id: m
      dsn: mysql://root:pw@localhost:3306/db
      tables: [orders]
  processors: []
  sinks:
    - type: kafka
      config:
        id: k
        brokers: localhost:9092
        topic: orders
    - type: redis
      config:
        id: r
        uri: redis://localhost:6379
        stream: orders
    - type: nats
      config:
        id: n
        url: nats://localhost:4222
        subject: orders
"#;

    let path = write_temp(yaml);
    let spec = load_from_path(path.to_str().unwrap()).expect("parse ok");

    match &spec.spec.sinks[0] {
        SinkCfg::Kafka(kc) => assert!(kc.key.is_none()),
        _ => panic!("expected kafka"),
    }
    match &spec.spec.sinks[1] {
        SinkCfg::Redis(rc) => assert!(rc.key.is_none()),
        _ => panic!("expected redis"),
    }
    match &spec.spec.sinks[2] {
        SinkCfg::Nats(nc) => assert!(nc.key.is_none()),
        _ => panic!("expected nats"),
    }
}

// ============================================================================
// Flatten Processor Config Parsing
// ============================================================================

#[test]
#[serial]
fn flatten_processor_defaults() {
    // All policy fields omitted - verify serde defaults kick in correctly.
    // This is the most important test: #[serde(flatten)] + #[serde(default)]
    // on the config struct is the most likely place for a silent deserialization bug.
    let yaml = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: flatten_defaults, tenant: t }
spec:
  source:
    type: mysql
    config:
      id: m
      dsn: mysql://root:pw@localhost/db
      tables: [orders]
  processors:
    - type: flatten
      id: flat
  sinks: []
"#;

    let path = write_temp(yaml);
    let spec = load_from_path(path.to_str().unwrap()).expect("parse ok");

    match &spec.spec.processors[0] {
        ProcessorCfg::Flatten { config } => {
            assert_eq!(config.id, "flat");
            assert_eq!(config.separator, "__");
            assert!(config.max_depth.is_none());
            assert_eq!(config.on_collision, CollisionPolicy::Last);
            assert_eq!(config.empty_object, EmptyObjectPolicy::Preserve);
            assert_eq!(config.lists, ListPolicy::Preserve);
            assert_eq!(config.empty_list, EmptyListPolicy::Preserve);
        }
        other => panic!("expected flatten processor, got {other:?}"),
    }
}

#[test]
#[serial]
fn flatten_processor_all_policies_explicit() {
    // Every policy field explicitly set - verifies rename_all = "lowercase"
    // is applied correctly on all enum variants.
    let yaml = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: flatten_explicit, tenant: t }
spec:
  source:
    type: mysql
    config:
      id: m
      dsn: mysql://root:pw@localhost/db
      tables: [orders]
  processors:
    - type: flatten
      id: flat
      separator: "."
      max_depth: 3
      on_collision: error
      empty_object: "null"
      lists: index
      empty_list: drop
  sinks: []
"#;

    let path = write_temp(yaml);
    let spec = load_from_path(path.to_str().unwrap()).expect("parse ok");

    match &spec.spec.processors[0] {
        ProcessorCfg::Flatten { config } => {
            assert_eq!(config.separator, ".");
            assert_eq!(config.max_depth, Some(3));
            assert_eq!(config.on_collision, CollisionPolicy::Error);
            assert_eq!(config.empty_object, EmptyObjectPolicy::Null);
            assert_eq!(config.lists, ListPolicy::Index);
            assert_eq!(config.empty_list, EmptyListPolicy::Drop);
        }
        other => panic!("expected flatten processor, got {other:?}"),
    }
}

#[test]
#[serial]
fn flatten_default_id_when_omitted() {
    // id field omitted - should default to "flatten"
    let yaml = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: flatten_id, tenant: t }
spec:
  source:
    type: mysql
    config:
      id: m
      dsn: mysql://root:pw@localhost/db
      tables: [orders]
  processors:
    - type: flatten
  sinks: []
"#;

    let path = write_temp(yaml);
    let spec = load_from_path(path.to_str().unwrap()).expect("parse ok");

    match &spec.spec.processors[0] {
        ProcessorCfg::Flatten { config } => {
            assert_eq!(config.id, "flatten");
        }
        other => panic!("expected flatten processor, got {other:?}"),
    }
}

#[test]
#[serial]
fn flatten_chained_after_outbox() {
    // Common real-world pattern: outbox extracts payload, flatten normalizes it.
    // Verifies both processors parse correctly in sequence.
    let yaml = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: outbox_then_flatten, tenant: t }
spec:
  source:
    type: mysql
    config:
      id: m
      dsn: mysql://root:pw@localhost/db
      tables: [outbox]
  processors:
    - type: outbox
      topic: "${aggregate_type}.${event_type}"
    - type: flatten
      id: flat
      empty_list: drop
  sinks: []
"#;

    let path = write_temp(yaml);
    let spec = load_from_path(path.to_str().unwrap()).expect("parse ok");

    assert_eq!(spec.spec.processors.len(), 2);
    assert!(matches!(
        &spec.spec.processors[0],
        ProcessorCfg::Outbox { .. }
    ));
    assert!(matches!(
        &spec.spec.processors[1],
        ProcessorCfg::Flatten { .. }
    ));
}

#[test]
#[serial]
fn filter_processor_parses() {
    let yaml = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: filter_proc_test, tenant: t }
spec:
  source:
    type: mysql
    config:
      id: m
      dsn: mysql://root:pw@localhost/db
      tables: [orders]
  processors:
    - type: filter
      id: only-active
      ops: [create, update]
      tables:
        include: ["shop.orders"]
      fields:
        - path: status
          op: eq
          value: "active"
    - type: filter   # bare filter: ops/tables/fields all omitted (defaults)
  sinks: []
"#;

    let path = write_temp(yaml);
    let spec = load_from_path(path.to_str().unwrap()).expect("parse ok");

    assert_eq!(spec.spec.processors.len(), 2);
    match &spec.spec.processors[0] {
        ProcessorCfg::Filter { config } => {
            assert_eq!(config.id, "only-active");
            assert_eq!(config.ops.len(), 2);
            assert_eq!(config.fields.len(), 1);
        }
        other => panic!("expected Filter, got {other:?}"),
    }
    // Bare `type: filter` must parse via serde defaults (empty ops/tables/fields).
    match &spec.spec.processors[1] {
        ProcessorCfg::Filter { config } => {
            assert!(config.ops.is_empty() && config.fields.is_empty());
        }
        other => panic!("expected Filter, got {other:?}"),
    }
}

#[test]
#[serial]
fn sink_filter_exclude_synthetic_parses() {
    let yaml = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: filter_test, tenant: t }
spec:
  source:
    type: mysql
    config:
      id: m
      dsn: mysql://root:pw@localhost:3306/db
      tables: [orders]
  processors: []
  sinks:
    - type: kafka
      config:
        id: business
        brokers: localhost:9092
        topic: cdc.orders
        filter:
          exclude_synthetic: true
    - type: kafka
      config:
        id: metrics
        brokers: localhost:9092
        topic: _deltaforge.metrics
        filter:
          synthetic_only: true
          producers: ["analytics"]
    - type: redis
      config:
        id: all-events
        uri: redis://localhost:6379
        stream: events
        # no filter - gets everything
"#;

    let path = write_temp(yaml);
    let spec = load_from_path(path.to_str().unwrap()).expect("parse ok");

    match &spec.spec.sinks[0] {
        SinkCfg::Kafka(kc) => {
            let f = kc.filter.as_ref().expect("filter present");
            assert!(f.exclude_synthetic);
            assert!(!f.synthetic_only);
            assert!(f.producers.is_empty());
        }
        _ => panic!("expected kafka"),
    }

    match &spec.spec.sinks[1] {
        SinkCfg::Kafka(kc) => {
            let f = kc.filter.as_ref().expect("filter present");
            assert!(f.synthetic_only);
            assert_eq!(f.producers, vec!["analytics"]);
        }
        _ => panic!("expected kafka"),
    }

    match &spec.spec.sinks[2] {
        SinkCfg::Redis(rc) => {
            assert!(rc.filter.is_none(), "no filter on all-events sink");
        }
        _ => panic!("expected redis"),
    }
}

// ============================================================================
// Credential redaction (status serialization + Debug)
// ============================================================================

fn postgres_cfg_with_inline_dsn(
    dsn: &str,
) -> deltaforge_config::PostgresSrcCfg {
    deltaforge_config::PostgresSrcCfg {
        id: "pg".to_string(),
        dsn: Some(dsn.to_string()),
        dsn_secret: None,
        credentials: None,
        publication: "pub".to_string(),
        slot: "slot".to_string(),
        tables: vec![],
        table_options: Default::default(),
        start_position: Default::default(),
        outbox: None,
        snapshot: Default::default(),
        on_schema_drift: Default::default(),
        rotation: None,
    }
}

fn sanitized_spec_json(spec: &deltaforge_config::PipelineSpec) -> String {
    let mut buf = Vec::new();
    let mut ser = serde_json::Serializer::new(&mut buf);
    deltaforge_config::serialize_sanitized_spec(spec, &mut ser).unwrap();
    String::from_utf8(buf).unwrap()
}

#[test]
fn status_serialization_redacts_inline_dsn_password() {
    let yaml = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: redact, tenant: t }
spec:
  source:
    type: postgres
    config:
      id: pg
      dsn: postgres://user:supersecret@db.internal:5432/orders
      publication: pub
      slot: slot
      tables: [public.t1]
  processors: []
  sinks: []
"#;
    let spec = load_from_path(write_temp(yaml).to_str().unwrap()).unwrap();

    // Lossless persistence/round-trip still contains the password.
    let raw = serde_json::to_string(&spec).unwrap();
    assert!(raw.contains("supersecret"), "persistence must be lossless");

    // The sanitized status/API serialization must never expose it.
    let shown = sanitized_spec_json(&spec);
    assert!(
        !shown.contains("supersecret"),
        "status serialization leaked password: {shown}"
    );
    assert!(shown.contains("db.internal"), "host must survive: {shown}");
}

#[test]
fn sanitized_and_normal_json_match_except_source_dsn() {
    // Credential-less libpq key=value DSN: redaction is identity, so the sanitized
    // JSON must be structurally identical to the normal JSON. A Spec (or source
    // config) field dropped from the sanitized serializer would break this - the
    // regression guard for the borrowed sanitizer, which the type system does not
    // enforce (SanitizedSpec is a separate struct from Spec).
    let yaml = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: shape, tenant: t }
spec:
  source:
    type: postgres
    config:
      id: pg
      dsn: host=db.internal dbname=orders
      publication: pub
      slot: slot
      tables: [public.t1]
  processors: []
  sinks: []
"#;
    let spec = load_from_path(write_temp(yaml).to_str().unwrap()).unwrap();
    let normal: serde_json::Value = serde_json::to_value(&spec).unwrap();
    let sanitized: serde_json::Value =
        serde_json::from_str(&sanitized_spec_json(&spec)).unwrap();
    assert_eq!(
        normal, sanitized,
        "sanitized JSON shape diverged from normal (a field may be missing from \
         the sanitized serializer)"
    );
}

#[test]
fn sanitized_serialization_preserves_secret_references() {
    let yaml = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: refs, tenant: t }
spec:
  source:
    type: postgres
    config:
      id: pg
      dsn: postgres://db.internal/orders
      credentials:
        username: { provider: env, location: DF_PG_USER }
        password: { provider: file, location: /run/secrets/pg/password }
      publication: pub
      slot: slot
      tables: [public.t1]
  processors: []
  sinks: []
"#;
    let spec = load_from_path(write_temp(yaml).to_str().unwrap()).unwrap();
    let shown = sanitized_spec_json(&spec);
    // References remain visible and usable in sanitized output.
    assert!(shown.contains("DF_PG_USER"), "{shown}");
    assert!(shown.contains("/run/secrets/pg/password"), "{shown}");
}

#[test]
fn config_debug_does_not_reveal_dsn_password() {
    let src = postgres_cfg_with_inline_dsn(
        "postgres://user:supersecret@db.internal/orders",
    );
    let shown = format!("{src:?}");
    assert!(
        !shown.contains("supersecret"),
        "config Debug leaked password: {shown}"
    );
}

// --- sink credential redaction (Slice 1) ---

/// Every current sink type, each secret field set to a unique sentinel, so a leak is
/// unambiguous and a newly-added sink secret can be caught by adding it here.
const ALL_SINKS_YAML: &str = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: sinks, tenant: t }
spec:
  source:
    type: postgres
    config:
      id: pg
      dsn: host=db dbname=o
      publication: pub
      slot: s
      tables: [public.t]
  processors: []
  sinks:
    - type: clickhouse
      config:
        id: ch
        url: "http://ch:8123"
        database: db
        table: t
        user: chuser
        password: SENTINEL_CH_PW
    - type: elasticsearch
      config:
        id: es
        url: "http://es:9200"
        index: idx
        auth: { type: basic, username: esuser, password: SENTINEL_ES_PW }
    - type: kafka
      config:
        id: k
        brokers: "b:9092"
        topic: t
        client_conf:
          "sasl.username": SENTINEL_KAFKA_USER
          "sasl.password": SENTINEL_KAFKA_PW
          "security.protocol": SASL_SSL
        encoding:
          type: avro
          schema_registry_url: "http://sr:8081"
          username: sruser
          password: SENTINEL_SR_PW
    - type: redis
      config:
        id: r
        uri: "redis://:SENTINEL_REDIS_PW@localhost:6379/0"
        stream: st
    - type: nats
      config:
        id: n
        url: "nats://localhost:4222"
        subject: sub
        username: natsuser
        password: SENTINEL_NATS_PW
        token: SENTINEL_NATS_TOKEN
    - type: http
      config:
        id: h
        url: "http://x/y"
        headers:
          "Authorization": "Bearer SENTINEL_HTTP_TOKEN"
    - type: s3
      config:
        id: s3
        bucket: b
        access_key_id: SENTINEL_S3_AKID
        secret_access_key: SENTINEL_S3_SECRET
"#;

const SINK_SENTINELS: &[&str] = &[
    "SENTINEL_CH_PW",
    "SENTINEL_ES_PW",
    "SENTINEL_KAFKA_USER",
    "SENTINEL_KAFKA_PW",
    "SENTINEL_SR_PW",
    "SENTINEL_REDIS_PW",
    "SENTINEL_NATS_PW",
    "SENTINEL_NATS_TOKEN",
    "SENTINEL_HTTP_TOKEN",
    "SENTINEL_S3_AKID",
    "SENTINEL_S3_SECRET",
];

#[test]
fn sink_credentials_never_leak_in_sanitized_output() {
    let spec =
        load_from_path(write_temp(ALL_SINKS_YAML).to_str().unwrap()).unwrap();

    // Persistence/round-trip stays lossless (the fixture really sets each secret).
    let raw = serde_json::to_string(&spec).unwrap();
    for s in SINK_SENTINELS {
        assert!(raw.contains(s), "fixture must set sentinel {s}");
    }

    // The sanitized status/API serialization must expose none of them.
    let shown = sanitized_spec_json(&spec);
    for s in SINK_SENTINELS {
        assert!(
            !shown.contains(s),
            "sanitized output leaked sink secret {s}: {shown}"
        );
    }
    // Non-secret fields still survive (host/port, message topic).
    assert!(shown.contains("ch:8123"), "non-secret host must survive");
    assert!(shown.contains("localhost:6379"), "redis host must survive");
}

/// Shape guard: the sanitized sinks must differ from the normal serialization ONLY by
/// redaction (a `***REDACTED***` value or a `***`-masked URL password). A new sink field
/// that is silently dropped, or altered to anything other than a redaction, fails here;
/// a new *secret* field is additionally caught by the sentinel leak test above.
#[test]
fn sanitized_sinks_differ_from_normal_only_by_redaction() {
    let spec =
        load_from_path(write_temp(ALL_SINKS_YAML).to_str().unwrap()).unwrap();
    let normal: serde_json::Value = serde_json::to_value(&spec).unwrap();
    let sanitized: serde_json::Value =
        serde_json::from_str(&sanitized_spec_json(&spec)).unwrap();

    let n_sinks = normal["spec"]["sinks"].as_array().unwrap();
    let s_sinks = sanitized["spec"]["sinks"].as_array().unwrap();
    assert_eq!(n_sinks.len(), s_sinks.len(), "sink count changed");
    assert_eq!(n_sinks.len(), 7, "expected all 7 sink types in the fixture");

    let mut redactions = 0usize;
    for (n, s) in n_sinks.iter().zip(s_sinks) {
        assert_leaf_diffs_are_redactions(n, s, &mut redactions);
    }
    assert!(redactions > 0, "expected sink redactions but found none");
}

fn assert_leaf_diffs_are_redactions(
    normal: &serde_json::Value,
    sanitized: &serde_json::Value,
    redactions: &mut usize,
) {
    use serde_json::Value;
    match (normal, sanitized) {
        (Value::Object(n), Value::Object(s)) => {
            assert_eq!(
                n.keys().collect::<Vec<_>>(),
                s.keys().collect::<Vec<_>>(),
                "sanitized object dropped/added a key (normal={n:?})"
            );
            for (k, nv) in n {
                assert_leaf_diffs_are_redactions(nv, &s[k], redactions);
            }
        }
        (Value::Array(n), Value::Array(s)) => {
            assert_eq!(n.len(), s.len(), "sanitized array length changed");
            for (nv, sv) in n.iter().zip(s) {
                assert_leaf_diffs_are_redactions(nv, sv, redactions);
            }
        }
        (n, s) if n == s => {} // unchanged non-secret leaf
        (_, Value::String(s)) => {
            assert!(
                s == "***REDACTED***" || s.contains("***"),
                "a leaf changed to a non-redaction value: {s}"
            );
            *redactions += 1;
        }
        (n, s) => panic!("unexpected non-redaction change: {n:?} -> {s:?}"),
    }
}

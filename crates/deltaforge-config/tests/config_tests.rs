use deltaforge_config::{
    CommitPolicy, ConfigError, EncodingCfg, EnvelopeCfg, ProcessorCfg, SinkCfg,
    SourceCfg, load_from_path,
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
            assert_eq!(pc.dsn, "postgres://pgu:pgpass@localhost:5432/orders");
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

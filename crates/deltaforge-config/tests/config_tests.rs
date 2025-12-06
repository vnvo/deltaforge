use deltaforge_config::{
    load_from_path, CommitPolicy, ConfigError, ProcessorCfg, SinkCfg, SourceCfg,
};
use pretty_assertions::assert_eq;
use serial_test::serial;
use std::io::Write;

fn write_temp(contents: &str) -> tempfile::TempPath {
    let mut f = tempfile::NamedTempFile::new().expect("temp file");
    f.write_all(contents.as_bytes()).expect("write");
    f.into_temp_path()
}

#[test]
#[serial] // because we mutate process env
fn parses_minimal_postgres_pipeline_with_env_expansion() {
    // arrange
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

    // act
    let spec = load_from_path(path.to_str().unwrap()).expect("parse yaml");

    // assert basics
    assert_eq!(spec.metadata.name, "unit");
    assert_eq!(spec.metadata.tenant, "test");

    // assert source
    match &spec.spec.source {
        SourceCfg::Postgres(pc) => {
            assert_eq!(pc.id, "pg");
            assert_eq!(pc.dsn, "postgres://pgu:pgpass@localhost:5432/orders");
            assert_eq!(pc.publication.as_deref(), Some("df_pub"));
            assert_eq!(pc.slot.as_deref(), Some("df_slot"));
            assert_eq!(pc.tables, vec!["public.t1".to_string()]);
        }
        _ => panic!("expected postgres source"),
    }

    // assert processors
    assert_eq!(spec.spec.processors.len(), 1);
    match &spec.spec.processors[0] {
        ProcessorCfg::Javascript { id, inline, limits } => {
            assert_eq!(id, "js");
            assert!(inline.contains("return [event];"));
            assert!(limits.is_none());
        }
    }

    // assert sink
    assert_eq!(spec.spec.sinks.len(), 1);
    match &spec.spec.sinks[0] {
        SinkCfg::Kafka(kc) => {
            assert_eq!(kc.id, "k");
            assert_eq!(kc.brokers, "localhost:9092");
            assert_eq!(kc.topic, "unit.events");
            // optional fields should default correctly
            assert!(kc.required.is_none());
            assert!(kc.exactly_once.is_none());
            assert!(kc.client_conf.is_empty());
        }
        _ => panic!("expected kafka sink"),
    }

    // no batch / commit_policy specified
    assert!(spec.spec.batch.is_none());
    assert!(spec.spec.commit_policy.is_none());
}

#[test]
#[serial]
fn parses_mysql_and_multiple_sinks() {
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
"#;

    let path = write_temp(yaml);
    let spec = load_from_path(path.to_str().unwrap()).expect("parse ok");

    // source
    match &spec.spec.source {
        SourceCfg::Mysql(mc) => {
            assert_eq!(mc.id, "m");
            assert_eq!(
                mc.dsn,
                "mysql://root:pws@localhost:3306/orders",
                "env expansion failed for MYSQL_ORDERS_DSN"
            );
            assert_eq!(
                mc.tables,
                vec!["orders".to_string(), "order_items".to_string()]
            );
        }
        _ => panic!("expected mysql source"),
    }

    // sinks
    assert_eq!(spec.spec.sinks.len(), 2);
    match &spec.spec.sinks[0] {
        SinkCfg::Kafka(kc) => {
            assert_eq!(kc.id, "k");
            assert_eq!(kc.brokers, "localhost:9092");
            assert_eq!(kc.topic, "t.orders");
        }
        _ => panic!("expected kafka"),
    }
    match &spec.spec.sinks[1] {
        SinkCfg::Redis(rc) => {
            assert_eq!(rc.id, "r");
            assert_eq!(rc.uri, "redis://127.0.0.1:6379");
            assert_eq!(rc.stream, "s");
        }
        _ => panic!("expected redis"),
    }

    // no batch / commit_policy specified
    assert!(spec.spec.batch.is_none());
    assert!(spec.spec.commit_policy.is_none());
}

#[test]
#[serial]
fn invalid_yaml_errors() {
    let yaml = r#"
this is: [ definitely: not: valid: yaml
"#;
    let path = write_temp(yaml);
    let err = load_from_path(path.to_str().unwrap())
        .expect_err("should fail to parse invalid yaml");

    match err {
        ConfigError::Parse { .. } => {} // expected
        other => panic!("expected ConfigError::Parse, got: {other:?}"),
    }
}

#[test]
#[serial]
fn missing_inline_for_js_processor_is_an_error() {
    let yaml = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: badjs, tenant: t }
spec:
  source:
    type: postgres
    config:
      id: pg
      dsn: postgres://pgu:pgpass@localhost:5432/orders
      tables: [public.t1]
  processors:
    - type: javascript
      id: js
      # inline is intentionally missing
  sinks: []
"#;

    let path = write_temp(yaml);

    let res = load_from_path(path.to_str().unwrap());

    assert!(
        res.is_err(),
        "should fail because `inline` is required in current schema"
    );
}

#[test]
#[serial]
fn connection_policy_is_optional_and_parses_when_present() {
    let with = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: a, tenant: t }
spec:
  connection_policy:
    default_mode: dedicated
    preferred_replica: read-replica-1
    limits: { max_dedicated_per_source: 3 }
  source:
    type: postgres
    config:
      id: pg
      dsn: postgres://pgu:pgpass@localhost:5432/orders
      tables: [public.t1]
  processors: []
  sinks: []
"#;
    let path = write_temp(with);
    let spec = load_from_path(path.to_str().unwrap()).expect("parse ok");
    let cp = spec.spec.connection_policy.as_ref().expect("present");
    assert_eq!(cp.default_mode.as_deref(), Some("dedicated"));
    assert_eq!(cp.preferred_replica.as_deref(), Some("read-replica-1"));
    assert_eq!(
        cp.limits.as_ref().unwrap().max_dedicated_per_source,
        Some(3)
    );

    let without = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: b, tenant: t }
spec:
  source:
    type: mysql
    config:
      id: m
      dsn: mysql://root:pws@localhost:3306/orders
      tables: [orders]
  processors: []
  sinks: []
"#;
    let path2 = write_temp(without);
    let spec2 = load_from_path(path2.to_str().unwrap()).expect("parse ok");
    assert!(spec2.spec.connection_policy.is_none());
}

#[test]
#[serial]
fn env_expansion_honors_multiple_vars() {
    unsafe {
        std::env::set_var("KAFKA_BROKERS", "localhost:9092");
        std::env::set_var("REDIS_URI", "redis://127.0.0.1:6379");
    }

    let yaml = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: envtest, tenant: t }
spec:
  source:
    type: mysql
    config:
      id: m
      dsn: mysql://root:pws@localhost:3306/orders
      tables: [orders, order_items]
  processors: []
  sinks:
    - type: kafka
      config:
        id: k
        brokers: ${KAFKA_BROKERS}
        topic: t.orders
    - type: redis
      config:
        id: r
        uri: ${REDIS_URI}
        stream: s
"#;
    let path = write_temp(yaml);
    let spec = load_from_path(path.to_str().unwrap()).expect("parse ok");

    match &spec.spec.sinks[0] {
        SinkCfg::Kafka(ks) => assert_eq!(ks.brokers, "localhost:9092"),
        _ => panic!("expected kafka"),
    }
    match &spec.spec.sinks[1] {
        SinkCfg::Redis(rc) => assert_eq!(rc.uri, "redis://127.0.0.1:6379"),
        _ => panic!("expected redis"),
    }
}

/// batch config coverage
#[test]
#[serial]
fn batch_config_is_optional_and_parses_when_present() {
    let without = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: nobatch, tenant: t }
spec:
  source:
    type: postgres
    config:
      id: pg
      dsn: postgres://pgu:pgpass@localhost:5432/orders
      tables: [public.t1]
  processors: []
  sinks: []
"#;

    let path1 = write_temp(without);
    let spec1 = load_from_path(path1.to_str().unwrap()).expect("parse ok");
    assert!(spec1.spec.batch.is_none());

    let with = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: withbatch, tenant: t }
spec:
  batch:
    max_events: 1000
    max_bytes: 65536
    max_ms: 250
    respect_source_tx: true
    max_inflight: 4
  source:
    type: postgres
    config:
      id: pg
      dsn: postgres://pgu:pgpass@localhost:5432/orders
      tables: [public.t1]
  processors: []
  sinks: []
"#;

    let path2 = write_temp(with);
    let spec2 = load_from_path(path2.to_str().unwrap()).expect("parse ok");

    let batch = spec2.spec.batch.as_ref().expect("batch should be present");
    assert_eq!(batch.max_events, Some(1000));
    assert_eq!(batch.max_bytes, Some(65536));
    assert_eq!(batch.max_ms, Some(250));
    assert_eq!(batch.respect_source_tx, Some(true));
    assert_eq!(batch.max_inflight, Some(4));
}

/// commit_policy coverage – All / Required / Quorum
#[test]
#[serial]
fn commit_policy_parses_all_variants() {
    // All
    let yaml_all = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: cp_all, tenant: t }
spec:
  commit_policy:
    mode: all
  source:
    type: postgres
    config:
      id: pg
      dsn: postgres://pgu:pgpass@localhost:5432/orders
      tables: [public.t1]
  processors: []
  sinks: []
"#;
    let p_all = write_temp(yaml_all);
    let spec_all = load_from_path(p_all.to_str().unwrap()).expect("parse all");
    assert!(
        matches!(spec_all.spec.commit_policy, Some(CommitPolicy::All)),
        "expected CommitPolicy::All, got {:?}",
        spec_all.spec.commit_policy
    );

    // Required
    let yaml_required = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: cp_req, tenant: t }
spec:
  commit_policy:
    mode: required
  source:
    type: postgres
    config:
      id: pg
      dsn: postgres://pgu:pgpass@localhost:5432/orders
      tables: [public.t1]
  processors: []
  sinks: []
"#;
    let p_req = write_temp(yaml_required);
    let spec_req = load_from_path(p_req.to_str().unwrap()).expect("parse required");
    assert!(
        matches!(spec_req.spec.commit_policy, Some(CommitPolicy::Required)),
        "expected CommitPolicy::Required, got {:?}",
        spec_req.spec.commit_policy
    );

    // Quorum
    let yaml_quorum = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: cp_quorum, tenant: t }
spec:
  commit_policy:
    mode: quorum
    quorum: 2
  source:
    type: postgres
    config:
      id: pg
      dsn: postgres://pgu:pgpass@localhost:5432/orders
      tables: [public.t1]
  processors: []
  sinks: []
"#;
    let p_quorum = write_temp(yaml_quorum);
    let spec_quorum =
        load_from_path(p_quorum.to_str().unwrap()).expect("parse quorum");

    match spec_quorum.spec.commit_policy {
        Some(CommitPolicy::Quorum { quorum }) => assert_eq!(quorum, 2),
        other => panic!("expected CommitPolicy::Quorum {{ quorum: 2 }}, got {other:?}"),
    }
}

#[test]
#[serial]
fn commit_policy_is_optional_when_omitted() {
    let yaml = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: cp_none, tenant: t }
spec:
  source:
    type: postgres
    config:
      id: pg
      dsn: postgres://pgu:pgpass@localhost:5432/orders
      tables: [public.t1]
  processors: []
  sinks: []
"#;

    let path = write_temp(yaml);
    let spec = load_from_path(path.to_str().unwrap()).expect("parse ok");

    assert!(
        spec.spec.commit_policy.is_none(),
        "commit_policy should be None when omitted"
    );
}

/// Kafka client_conf: default empty map and explicit overrides
#[test]
#[serial]
fn kafka_client_conf_defaults_and_overrides() {
    let yaml = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: kclient, tenant: t }
spec:
  source:
    type: postgres
    config:
      id: pg
      dsn: postgres://pgu:pgpass@localhost:5432/orders
      tables: [public.t1]
  processors: []
  sinks:
    - type: kafka
      config:
        id: k1
        brokers: localhost:9092
        topic: t.orders
    - type: kafka
      config:
        id: k2
        brokers: localhost:9092
        topic: t.orders
        client_conf:
          linger.ms: "10"
          message.max.bytes: "1048576"
"#;

    let path = write_temp(yaml);
    let spec = load_from_path(path.to_str().unwrap()).expect("parse ok");

    assert_eq!(spec.spec.sinks.len(), 2);

    match &spec.spec.sinks[0] {
        SinkCfg::Kafka(k1) => {
            // serde(default) -> empty map when field is omitted
            assert!(k1.client_conf.is_empty());
        }
        _ => panic!("expected first sink to be kafka"),
    }

    match &spec.spec.sinks[1] {
        SinkCfg::Kafka(k2) => {
            assert_eq!(
                k2.client_conf.get("linger.ms").map(String::as_str),
                Some("10")
            );
            assert_eq!(
                k2.client_conf
                    .get("message.max.bytes")
                    .map(String::as_str),
                Some("1048576")
            );
        }
        _ => panic!("expected second sink to be kafka"),
    }
}

use deltaforge_config::{load_from_path, ProcessorCfg, SinkCfg, SourceCfg};
use pretty_assertions::assert_eq;
use serial_test::serial;
use std::io::Write;

fn write_temp(contents: &str) -> tempfile::TempPath {
    let mut f = tempfile::NamedTempFile::new().expect("temp file");
    f.write_all(contents.as_bytes()).expect("write");
    f.into_temp_path() //.to_path_buf()
}

#[test]
#[serial] // because we mutate process env
fn parses_minimal_postgres_pipeline_with_env_expansion() {
    // arrange
    std::env::set_var(
        "PG_ORDERS_DSN",
        "postgres://pgu:pgpass@localhost:5432/orders",
    );
    let yaml = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata:
  name: unit
  tenant: test
spec:
  sharding: { mode: table }
  connection_policy: { defaultMode: shared }
  sources:
    - type: postgres
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
    assert!(spec.spec.connection_policy.is_some());

    // assert source
    assert_eq!(spec.spec.sources.len(), 1);
    match &spec.spec.sources[0] {
        SourceCfg::Postgres { id, dsn, publication, slot, tables } => {
            assert_eq!(id, "pg");
            assert_eq!(dsn, "postgres://pgu:pgpass@localhost:5432/orders");
            assert_eq!(publication.as_deref(), Some("df_pub"));
            assert_eq!(slot.as_deref(), Some("df_slot"));
            assert_eq!(tables, &vec!["public.t1".to_string()]);
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
        SinkCfg::Kafka { id, brokers, topic, exactly_once } => {
            assert_eq!(id, "k");
            assert_eq!(brokers, "localhost:9092");
            assert_eq!(topic, "unit.events");
            assert!(exactly_once.is_none());
        }
        _ => panic!("expected kafka sink"),
    }

}



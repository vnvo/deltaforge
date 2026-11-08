use super::*;

/// A MySQL pipeline with every sink type and every field set, carrying a
/// planted credential in every credential position.
const MYSQL: &str = r#"
metadata:
  name: orders
  tenant: acme
  labels: {env: prod}
  annotations: {owner: team}
spec:
  sharding: {mode: hash, count: 4, key: id}
  source:
    type: mysql
    config:
      id: src-orders
      dsn: "mysql://df:PLANTED-SRC-PW@db.example:3306/shop"
      tables: ["shop.orders", "shop.items"]
      table_options:
        shop.orders: {identity_columns: [id], assume_unique: true}
        shop.items: {identity_columns: [id]}
      outbox: {tables: ["shop.outbox"]}
      snapshot: {mode: initial, max_parallel_tables: 2, chunk_size: 1000}
      on_schema_drift: halt
      type_mapping: v2
  processors:
    - {type: javascript, id: p1, inline: "function processBatch(e){return e}"}
    - {type: javascript, id: p2, inline: "function processBatch(e){return e}"}
  sinks:
    - type: kafka
      config:
        id: k
        brokers: "Kafka-1:9092,kafka-2:9092"
        topic: orders
        key: "${after.id}"
        envelope: {type: native}
        encoding: json
        json_wide_integers: string
        required: true
        exactly_once: false
        send_timeout_secs: 30
        client_conf:
          acks: all
          linger.ms: "5"
          sasl.password: PLANTED-SASL-PW
          compression.type: lz4
        secret_refs:
          sasl.username: {provider: env, location: SASL_USER}
        filter: {exclude_synthetic: true}
        encode_workers: 2
    - type: redis
      config:
        id: r
        uri: "redis://user:PLANTED-REDIS-PW@Redis.example:6379/2?protocol=resp3"
        stream: orders
        send_timeout_secs: 5
        batch_timeout_secs: 5
        connect_timeout_secs: 5
    - type: nats
      config:
        id: n
        url: "nats://nats-1:4222,nats://nats-2:4222"
        subject: orders
        stream: ORDERS
        password: PLANTED-NATS-PW
        username: u
    - type: http
      config:
        id: h
        url: "https://hooks.example/ingest"
        method: POST
        headers:
          Authorization: "Bearer PLANTED-HTTP-TOKEN"
          Content-Type: application/json
        batch_mode: true
    - type: s3
      config:
        id: s
        bucket: lake
        prefix: cdc/orders
        region: eu-west-1
        endpoint: "http://minio:9000"
        access_key_id: AKIA
        secret_access_key: PLANTED-S3-SECRET
        file_roll: {max_bytes: 1048576, max_events: 1000, max_age_secs: 60, idle_age_secs: 30}
    - type: clickhouse
      config:
        id: c
        url: "http://ch:8123/?user=df&password=PLANTED-CH-PW&database=analytics"
        database: analytics
        table: orders
        mode: upsert
        version_source: ts_ms
    - type: elasticsearch
      config:
        id: e
        url: "https://es.example:9200"
        index: orders
        id_fields: [tenant, id]
        id_separator: ":"
        auth: {type: basic, username: elastic, password: PLANTED-ES-PW}
  commit_policy: {mode: required}
  batch: {max_events: 100}
  sink_batch_deadline_secs: 60
  schema_sensing: {enabled: true, output: {emit_events: false}}
  journal: {enabled: true, max_event_bytes: 1024}
  metrics: {per_table: {enabled: true, max_tables: 50}}
"#;

const POSTGRES: &str = r#"
metadata: {name: pg, tenant: acme}
spec:
  source:
    type: postgres
    config:
      id: src-pg
      dsn: "postgres://df:PLANTED-PG-PW@pg.example:5432/app"
      publication: pub1
      slot: slot1
      tables: ["public.orders"]
      outbox: {prefixes: ["outbox_"]}
      max_message_bytes: 1048576
      start_position: latest
  processors: []
  sinks:
    - type: kafka
      config: {id: k, brokers: "kafka:9092", topic: orders}
"#;

const PLANTED: &[&str] = &[
    "PLANTED-SRC-PW",
    "PLANTED-SASL-PW",
    "PLANTED-REDIS-PW",
    "PLANTED-NATS-PW",
    "PLANTED-HTTP-TOKEN",
    "PLANTED-S3-SECRET",
    "PLANTED-CH-PW",
    "PLANTED-ES-PW",
    "PLANTED-PG-PW",
];

/// One edit of a spec's value tree.
type Edit = Box<dyn Fn(&mut Value)>;

fn spec(yaml: &str) -> PipelineSpec {
    serde_yaml::from_str(yaml).expect("fixture parses")
}

fn bind(yaml: &str) -> StateBinding {
    binding(&spec(yaml), &ResolvedEndpoints::default())
        .unwrap_or_else(|r| panic!("{r:?}"))
}

/// `yaml` with `edit` applied to its value tree.
fn edited(yaml: &str, edit: impl FnOnce(&mut Value)) -> String {
    let mut v: Value = serde_yaml::from_str(yaml).unwrap();
    edit(&mut v);
    serde_yaml::to_string(&v).unwrap()
}

fn sink<'a>(v: &'a mut Value, id: &str) -> &'a mut Value {
    v["spec"]["sinks"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|s| s["config"]["id"] == id)
        .map(|s| &mut s["config"])
        .unwrap()
}

fn rejections(yaml: &str) -> Vec<Rejection> {
    binding(&spec(yaml), &ResolvedEndpoints::default()).expect_err("refused")
}

fn assert_no_planted(text: &str) {
    for p in PLANTED {
        assert!(!text.contains(p), "{p} leaked into: {text}");
    }
}

#[test]
fn every_field_of_the_fixtures_is_classified() {
    // An unclassified field would be compared (refusing until classified);
    // this test makes adding one a deliberate decision.
    fn leaves(path: &str, v: &Value, out: &mut Vec<String>) {
        match v {
            Value::Object(m) if !m.is_empty() => {
                for (k, c) in m {
                    leaves(&format!("{path}.{k}"), c, out);
                }
            }
            _ => out.push(path.to_string()),
        }
    }
    for yaml in [MYSQL, POSTGRES] {
        let s = spec(yaml);
        let mut paths = Vec::new();
        let (engine, src) = match &s.spec.source {
            SourceCfg::Postgres(c) => ("postgres", to_value(c)),
            SourceCfg::Mysql(c) => ("mysql", to_value(c)),
        };
        leaves(
            &format!("source.{engine}"),
            &Value::Object(strip(src, &["id"])),
            &mut paths,
        );
        leaves(
            "spec",
            &Value::Object(strip(to_value(&s.spec), &["source", "sinks"])),
            &mut paths,
        );
        for k in &s.spec.sinks {
            leaves(
                &format!("sinks.{}", sink_type(k)),
                &Value::Object(strip(sink_config(k), &["id"])),
                &mut paths,
            );
        }
        let unnamed: Vec<_> =
            paths.into_iter().filter(|p| !class_of(p).1).collect();
        assert!(unnamed.is_empty(), "unclassified spec fields: {unnamed:?}");
    }
}

#[test]
fn the_same_spec_binds_identically() {
    for yaml in [MYSQL, POSTGRES] {
        let (a, b) = (bind(yaml), bind(yaml));
        assert_eq!(a, b);
        assert!(diff(&a, &b).is_empty());
        assert_eq!(a.digest, digest_of(&a));
    }
    let pg = bind(POSTGRES);
    assert_eq!(pg.canonical["source"]["database"], "app");
}

#[test]
fn credentials_never_enter_the_binding_or_a_refusal() {
    let a = bind(MYSQL);
    assert_no_planted(&serde_json::to_string(&a).unwrap());
    assert_no_planted(&serde_json::to_string(&bind(POSTGRES)).unwrap());
    // A refusal shows canonical values only, also next to a credential
    // change.
    let other = edited(MYSQL, |v| {
        v["spec"]["source"]["config"]["dsn"] =
            "mysql://df:OTHER@db2.example:3306/shop".into();
        sink(v, "r")["uri"] =
            "redis://user:PLANTED-REDIS-PW@redis-2.example:6379/3".into();
        sink(v, "e")["url"] = "https://es-2.example:9200".into();
    });
    let d = diff(&a, &bind(&other));
    assert!(!d.is_empty());
    for x in &d {
        assert_no_planted(&x.to_string());
    }
    // And a rejection names only the field.
    let bad = edited(MYSQL, |v| {
        sink(v, "h")["headers"]["X-Api-Key"] = "PLANTED-HTTP-TOKEN".into();
    });
    for r in rejections(&bad) {
        assert_no_planted(&r.to_string());
    }
}

#[test]
fn allowlisted_fields_and_rotated_credentials_change_nothing() {
    let base = bind(MYSQL);
    let changes: Vec<Edit> = vec![
        Box::new(|v| v["metadata"]["labels"]["env"] = "staging".into()),
        Box::new(|v| v["metadata"]["annotations"]["owner"] = "x".into()),
        Box::new(|v| v["spec"]["batch"]["max_events"] = 9.into()),
        Box::new(|v| v["spec"]["sink_batch_deadline_secs"] = 9.into()),
        Box::new(|v| v["spec"]["journal"]["max_event_bytes"] = 9.into()),
        Box::new(|v| v["spec"]["schema_sensing"]["enabled"] = false.into()),
        Box::new(|v| {
            v["spec"]["metrics"]["per_table"]["max_tables"] = 9.into()
        }),
        Box::new(|v| {
            v["spec"]["source"]["config"]["snapshot"]["mode"] = "never".into()
        }),
        Box::new(|v| {
            v["spec"]["source"]["config"]["dsn"] =
                "mysql://other:ROTATED@db-moved.example:3307/shop".into()
        }),
        Box::new(|v| sink(v, "k")["send_timeout_secs"] = 9.into()),
        Box::new(|v| sink(v, "k")["encode_workers"] = 9.into()),
        Box::new(|v| sink(v, "k")["client_conf"]["linger.ms"] = "50".into()),
        Box::new(|v| {
            sink(v, "k")["client_conf"]["sasl.password"] = "ROTATED".into()
        }),
        Box::new(|v| {
            sink(v, "r")["uri"] =
                "redis://u2:ROTATED@redis.example:6379/2?protocol=resp3".into()
        }),
        Box::new(|v| sink(v, "n")["password"] = "ROTATED".into()),
        Box::new(|v| {
            sink(v, "h")["headers"]["Authorization"] = "Bearer ROTATED".into()
        }),
        Box::new(|v| sink(v, "s")["secret_access_key"] = "ROTATED".into()),
        Box::new(|v| sink(v, "s")["file_roll"]["max_events"] = 9.into()),
        Box::new(|v| {
            sink(v, "c")["url"] =
            "http://ch-moved:9000/?user=x&password=ROTATED&database=analytics".into()
        }),
        Box::new(|v| sink(v, "e")["auth"]["password"] = "ROTATED".into()),
    ];
    for (i, change) in changes.iter().enumerate() {
        let b = bind(&edited(MYSQL, change));
        assert!(
            diff(&base, &b).is_empty(),
            "change {i}: {:?}",
            diff(&base, &b)
        );
        assert_eq!(base.digest, b.digest, "change {i}");
    }
}

#[test]
fn each_recorded_field_changed_alone_refuses() {
    let base = bind(MYSQL);
    let changes: Vec<(&str, Edit)> = vec![
        (
            "metadata.tenant",
            Box::new(|v| v["metadata"]["tenant"] = "other".into()),
        ),
        (
            "metadata.name",
            Box::new(|v| v["metadata"]["name"] = "other".into()),
        ),
        (
            "source.id",
            Box::new(|v| v["spec"]["source"]["config"]["id"] = "x".into()),
        ),
        (
            "source.tables",
            Box::new(|v| {
                v["spec"]["source"]["config"]["tables"][1] = "shop.more".into()
            }),
        ),
        (
            "source.table_options",
            Box::new(|v| {
                v["spec"]["source"]["config"]["table_options"]["shop.items"]["assume_unique"] =
                    true.into()
            }),
        ),
        (
            "source.outbox",
            Box::new(|v| {
                v["spec"]["source"]["config"]["outbox"]["tables"][0] =
                    "shop.ob2".into()
            }),
        ),
        (
            "source.on_schema_drift",
            Box::new(|v| {
                v["spec"]["source"]["config"]["on_schema_drift"] =
                    "adapt".into()
            }),
        ),
        (
            "source.type_mapping",
            Box::new(|v| {
                v["spec"]["source"]["config"]["type_mapping"] = "v1".into()
            }),
        ),
        (
            "spec.processors",
            Box::new(|v| {
                v["spec"]["processors"][0]["inline"] =
                    "function processBatch(e){return []}".into()
            }),
        ),
        (
            "spec.sharding",
            Box::new(|v| v["spec"]["sharding"]["count"] = 8.into()),
        ),
        (
            "spec.commit_policy",
            Box::new(|v| {
                v["spec"]["commit_policy"] = serde_json::json!({"mode": "all"})
            }),
        ),
        (
            "spec.journal.enabled",
            Box::new(|v| v["spec"]["journal"]["enabled"] = false.into()),
        ),
        (
            "spec.schema_sensing.output.emit_events",
            Box::new(|v| {
                v["spec"]["schema_sensing"]["output"]["emit_events"] =
                    true.into()
            }),
        ),
        (
            "sinks.k.topic",
            Box::new(|v| sink(v, "k")["topic"] = "x".into()),
        ),
        (
            "sinks.k.brokers",
            Box::new(|v| sink(v, "k")["brokers"] = "kafka-9:9092".into()),
        ),
        (
            "sinks.k.client_conf.acks",
            Box::new(|v| sink(v, "k")["client_conf"]["acks"] = "1".into()),
        ),
        (
            "sinks.k.key",
            Box::new(|v| sink(v, "k")["key"] = "${after.other}".into()),
        ),
        (
            "sinks.k.envelope",
            Box::new(|v| {
                sink(v, "k")["envelope"] = serde_json::json!({"type": "cloudevents", "type_prefix": "x"})
            }),
        ),
        (
            "sinks.k.json_wide_integers",
            Box::new(|v| sink(v, "k")["json_wide_integers"] = "number".into()),
        ),
        (
            "sinks.k.exactly_once",
            Box::new(|v| sink(v, "k")["exactly_once"] = true.into()),
        ),
        (
            "sinks.k.required",
            Box::new(|v| sink(v, "k")["required"] = false.into()),
        ),
        (
            "sinks.k.filter",
            Box::new(|v| {
                sink(v, "k")["filter"]["exclude_synthetic"] = false.into()
            }),
        ),
        (
            "sinks.r.stream",
            Box::new(|v| sink(v, "r")["stream"] = "x".into()),
        ),
        (
            "sinks.r.uri",
            Box::new(|v| {
                sink(v, "r")["uri"] = "redis://user:PLANTED-REDIS-PW@redis.example:6379/5?protocol=resp3".into()
            }),
        ),
        (
            "sinks.n.subject",
            Box::new(|v| sink(v, "n")["subject"] = "x".into()),
        ),
        (
            "sinks.n.url",
            Box::new(|v| sink(v, "n")["url"] = "nats://nats-3:4222".into()),
        ),
        (
            "sinks.h.url",
            Box::new(|v| {
                sink(v, "h")["url"] = "https://hooks.example/other".into()
            }),
        ),
        (
            "sinks.h.method",
            Box::new(|v| sink(v, "h")["method"] = "PUT".into()),
        ),
        (
            "sinks.h.headers.content-type",
            Box::new(|v| {
                sink(v, "h")["headers"]["Content-Type"] = "text/plain".into()
            }),
        ),
        (
            "sinks.s.bucket",
            Box::new(|v| sink(v, "s")["bucket"] = "x".into()),
        ),
        (
            "sinks.s.prefix",
            Box::new(|v| sink(v, "s")["prefix"] = "x".into()),
        ),
        (
            "sinks.s.region",
            Box::new(|v| sink(v, "s")["region"] = "us-east-1".into()),
        ),
        (
            "sinks.s.endpoint",
            Box::new(|v| {
                sink(v, "s")["endpoint"] = "http://minio-2:9000".into()
            }),
        ),
        (
            "sinks.c.database",
            Box::new(|v| {
                sink(v, "c")["database"] = "other".into();
                sink(v, "c")["url"] = "http://ch:8123/?database=other".into();
            }),
        ),
        (
            "sinks.c.table",
            Box::new(|v| sink(v, "c")["table"] = "x".into()),
        ),
        (
            "sinks.c.mode",
            Box::new(|v| sink(v, "c")["mode"] = "changelog".into()),
        ),
        (
            "sinks.e.url",
            Box::new(|v| {
                sink(v, "e")["url"] = "https://es-2.example:9200".into()
            }),
        ),
        (
            "sinks.e.index",
            Box::new(|v| sink(v, "e")["index"] = "x".into()),
        ),
        (
            "sinks.e.id_fields",
            Box::new(|v| {
                sink(v, "e")["id_fields"] = serde_json::json!(["id", "tenant"])
            }),
        ),
        (
            "sinks.e.id_separator",
            Box::new(|v| sink(v, "e")["id_separator"] = "/".into()),
        ),
        (
            "sinks (set)",
            Box::new(|v| {
                v["spec"]["sinks"].as_array_mut().unwrap().pop();
            }),
        ),
    ];
    for (field, change) in changes {
        let d = diff(&base, &bind(&edited(MYSQL, change)));
        assert!(!d.is_empty(), "{field}: no difference");
        let prefix = field.split(' ').next().unwrap();
        assert!(
            d.iter().any(|x| x.field.starts_with(prefix)),
            "{field}: {d:?}"
        );
    }
    let pg = bind(POSTGRES);
    for (field, change) in [
        (
            "source.publication",
            Box::new(|v: &mut Value| {
                v["spec"]["source"]["config"]["publication"] = "p2".into()
            }) as Edit,
        ),
        (
            "source.slot",
            Box::new(|v| v["spec"]["source"]["config"]["slot"] = "s2".into()),
        ),
        (
            "source.database",
            Box::new(|v| {
                v["spec"]["source"]["config"]["dsn"] =
                    "postgres://df:x@pg.example:5432/other".into()
            }),
        ),
        (
            "source.outbox",
            Box::new(|v| {
                v["spec"]["source"]["config"]["outbox"]["prefixes"][0] =
                    "ob_".into()
            }),
        ),
    ] {
        let d = diff(&pg, &bind(&edited(POSTGRES, change)));
        assert!(
            d.iter().any(|x| x.field.starts_with(field)),
            "{field}: {d:?}"
        );
    }
}

#[test]
fn maps_and_defaults_compare_equal_and_lists_keep_order() {
    let base = bind(MYSQL);
    // Header names are case-insensitive; map order does not matter.
    let b = bind(&edited(MYSQL, |v| {
        let h = &mut sink(v, "h")["headers"];
        let ct = h["Content-Type"].clone();
        h.as_object_mut().unwrap().remove("Content-Type");
        h["content-type"] = ct;
    }));
    assert!(diff(&base, &b).is_empty(), "{:?}", diff(&base, &b));
    // An omitted default equals it set explicitly.
    let b = bind(&edited(MYSQL, |v| {
        sink(v, "k").as_object_mut().unwrap().remove("required");
        sink(v, "k").as_object_mut().unwrap().remove("exactly_once");
    }));
    assert!(diff(&base, &b).is_empty(), "{:?}", diff(&base, &b));
    // Lists keep their order: processors and Elasticsearch id_fields change
    // behavior; no list is assumed to be a set.
    for change in [
        Box::new(|v: &mut Value| {
            v["spec"]["processors"].as_array_mut().unwrap().reverse()
        }) as Edit,
        Box::new(|v| {
            sink(v, "e")["id_fields"].as_array_mut().unwrap().reverse()
        }),
        Box::new(|v| {
            v["spec"]["source"]["config"]["tables"]
                .as_array_mut()
                .unwrap()
                .reverse()
        }),
    ] {
        assert!(!diff(&base, &bind(&edited(MYSQL, change))).is_empty());
    }
}

#[test]
fn unclassified_names_and_ambiguous_secrets_are_rejected() {
    let cases: Vec<(&str, Edit)> = vec![
        (
            "sinks.k.client_conf.topic.metadata.whatever",
            Box::new(|v| {
                sink(v, "k")["client_conf"]["topic.metadata.whatever"] =
                    "1".into()
            }),
        ),
        (
            "sinks.h.headers.X-Tenant",
            Box::new(|v| sink(v, "h")["headers"]["X-Tenant"] = "t1".into()),
        ),
        (
            "sinks.h.url?tenant",
            Box::new(|v| {
                sink(v, "h")["url"] =
                    "https://hooks.example/ingest?tenant=a".into()
            }),
        ),
        (
            "sinks.e.url?routing",
            Box::new(|v| {
                sink(v, "e")["url"] =
                    "https://es.example:9200/?routing=x".into()
            }),
        ),
        (
            "sinks.s.endpoint?x",
            Box::new(|v| {
                sink(v, "s")["endpoint"] = "http://minio:9000/?x=1".into()
            }),
        ),
        (
            "sinks.r.uri?cluster",
            Box::new(|v| {
                sink(v, "r")["uri"] =
                    "redis://redis.example:6379/2?cluster=a".into()
            }),
        ),
        (
            "sinks.c.url?max_threads",
            Box::new(|v| {
                sink(v, "c")["url"] = "http://ch:8123/?max_threads=2".into()
            }),
        ),
        (
            "sinks.c.url?async_insert",
            Box::new(|v| {
                sink(v, "c")["url"] = "http://ch:8123/?async_insert=1".into()
            }),
        ),
        (
            "sinks.c.url?database",
            Box::new(|v| {
                sink(v, "c")["url"] =
                    "http://ch:8123/?database=elsewhere".into()
            }),
        ),
        (
            "sinks.k.secret_refs.acks",
            Box::new(|v| {
                sink(v, "k")["secret_refs"]["acks"] =
                    serde_json::json!({"provider": "env", "location": "ACKS"})
            }),
        ),
        (
            "sinks.h.secret_refs.X-Tenant",
            Box::new(
                |v| sink(v, "h")["secret_refs"] = serde_json::json!({"X-Tenant": {"provider": "env", "location": "T"}}),
            ),
        ),
    ];
    for (field, change) in cases {
        let r = rejections(&edited(MYSQL, change));
        assert!(r.iter().any(|x| x.field == field), "{field}: {r:?}");
    }
    // A credential header from a secret is accepted.
    let ok = edited(MYSQL, |v| {
        sink(v, "h")["secret_refs"] = serde_json::json!({"Authorization": {"provider": "env", "location": "TOKEN"}});
    });
    bind(&ok);
}

#[test]
fn secret_supplied_endpoints_need_their_resolved_value() {
    // A PostgreSQL DSN from a secret: the database comes from the resolved
    // value; unresolved, the binding is refused rather than guessed.
    let y = edited(POSTGRES, |v| {
        let c = &mut v["spec"]["source"]["config"];
        c.as_object_mut().unwrap().remove("dsn");
        c["dsn_secret"] =
            serde_json::json!({"provider": "env", "location": "PG_DSN"});
    });
    let s = spec(&y);
    assert!(binding(&s, &ResolvedEndpoints::default()).is_err());
    validate(&s).expect("validation does not need the secret");
    let resolved = ResolvedEndpoints {
        source_dsn: Some("postgres://df:PLANTED-PG-PW@pg.example/app".into()),
        ..Default::default()
    };
    let b = binding(&s, &resolved).unwrap();
    assert_eq!(b.canonical["source"]["database"], "app");
    assert!(diff(&bind(POSTGRES), &b).is_empty());
    assert_no_planted(&serde_json::to_string(&b).unwrap());
}

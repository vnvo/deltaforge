//! Debezium on Kafka Connect as a system under test.
//!
//! A run starts a fresh Kafka Connect worker (the Debezium image, pinned by
//! digest) on the harness network, with its own group and internal topics,
//! registers one Debezium MySQL connector, and drives it through the same
//! protocol as a DeltaForge pipeline: wait for streaming, pause, write the
//! backlog, resume, and time the last row's arrival. Its CPU, system time
//! and memory are the worker container's cgroup counters.
//!
//! The worker runs with the image's defaults (heap `-Xms256M -Xmx2G`, Kafka
//! 4.3 producer: `acks=all`, idempotence, 5 ms linger). Two profiles:
//! - `defaults`: those, and the connector's defaults;
//! - `tuned`: configured as DeltaForge runs: the producer with LZ4 (and
//!   `acks=all`, idempotence, 5 ms linger stated), and the connector polling
//!   its queue every 50 ms (DeltaForge's batch deadline) with batches of
//!   2,048 events (DeltaForge: 2,000), and snapshotting up to 8 tables in
//!   parallel (`snapshot.max.threads`; DeltaForge's `max_parallel_tables`
//!   default).

use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use gate_ownership::GateOwned;
use serde_json::{Value, json};
use testcontainers::core::{AccessMode, IntoContainerPort, Mount};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

/// The Debezium Kafka Connect image, pinned by digest.
pub const CONNECT_IMAGE: (&str, &str) = (
    "quay.io/debezium/connect",
    "3.6.3.Final@sha256:87905532e5592b3566d6aa9f25819e2f6505275fef0e49328642d15106d1a428",
);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Profile {
    Defaults,
    Tuned,
}

impl Profile {
    pub fn parse(s: &str) -> Result<Profile> {
        Ok(match s {
            "defaults" => Profile::Defaults,
            "tuned" => Profile::Tuned,
            other => {
                bail!("unknown Debezium profile {other} (defaults, tuned)")
            }
        })
    }
}

/// The worker's JMX port (inside its container).
const JMX_PORT: u16 = 9012;

/// A running Kafka Connect worker.
pub struct Connect {
    container: ContainerAsync<GenericImage>,
    /// Its REST address (resolved again after a restart: the container's
    /// host port can change).
    rest: std::sync::Mutex<String>,
}

impl Connect {
    /// Start a worker for one run (`tag` names its group and internal
    /// topics) and wait until its REST API answers.
    pub async fn start(
        network: &str,
        bootstrap: &str,
        tag: &str,
        profile: Profile,
        offset_flush_ms: Option<u64>,
        extras: &crate::extras::Extras,
    ) -> Result<Connect> {
        let mut image = GenericImage::new(CONNECT_IMAGE.0, CONNECT_IMAGE.1)
            .with_exposed_port(8083.tcp())
            .with_network(network)
            // Debezium's scripting transformations, with GraalJS added to
            // the MySQL connector's plugin, and Confluent's Avro converter
            // as a plugin of its own.
            .with_env_var("ENABLE_DEBEZIUM_SCRIPTING", "true")
            // GraalJS runs on Truffle's fallback (interpreting) runtime: the
            // image's OpenJDK has no optimizing Truffle runtime (that needs a
            // GraalVM JDK), with or without JVMCI and module-path setups.
            .with_mount(
                Mount::bind_mount(
                    extras.graaljs.display().to_string(),
                    "/kafka/connect/debezium-connector-mysql/graaljs",
                )
                .with_access_mode(AccessMode::ReadOnly),
            )
            .with_mount(
                Mount::bind_mount(
                    extras.avro_converter.display().to_string(),
                    "/kafka/connect/confluent-avro-converter",
                )
                .with_access_mode(AccessMode::ReadWrite),
            )
            .with_mount(
                Mount::bind_mount(
                    extras.s3_sink.display().to_string(),
                    "/kafka/connect/confluent-s3-sink",
                )
                .with_access_mode(AccessMode::ReadWrite),
            )
            // JMX inside the container only, read by `counters`.
            .with_env_var("JMXPORT", JMX_PORT.to_string())
            .with_env_var("JMXHOST", "localhost")
            .with_env_var("BOOTSTRAP_SERVERS", bootstrap)
            .with_env_var("GROUP_ID", format!("connect-{tag}"))
            .with_env_var(
                "CONFIG_STORAGE_TOPIC",
                format!("connect-config-{tag}"),
            )
            .with_env_var(
                "OFFSET_STORAGE_TOPIC",
                format!("connect-offsets-{tag}"),
            )
            .with_env_var(
                "STATUS_STORAGE_TOPIC",
                format!("connect-status-{tag}"),
            )
            .with_env_var("CONFIG_STORAGE_REPLICATION_FACTOR", "1")
            .with_env_var("OFFSET_STORAGE_REPLICATION_FACTOR", "1")
            .with_env_var("STATUS_STORAGE_REPLICATION_FACTOR", "1");
        if profile == Profile::Tuned {
            image = image
                .with_env_var("CONNECT_PRODUCER_ACKS", "all")
                .with_env_var("CONNECT_PRODUCER_ENABLE_IDEMPOTENCE", "true")
                .with_env_var("CONNECT_PRODUCER_COMPRESSION_TYPE", "lz4")
                .with_env_var("CONNECT_PRODUCER_LINGER_MS", "5");
        }
        if let Some(ms) = offset_flush_ms {
            image =
                image.with_env_var("OFFSET_FLUSH_INTERVAL_MS", ms.to_string());
        }
        let container =
            image.gate_owned().start().await.context("start connect")?;
        let port = container.get_host_port_ipv4(8083).await?;
        let rest = format!("http://127.0.0.1:{port}");
        let deadline = Instant::now() + Duration::from_secs(180);
        loop {
            if let Ok(r) = reqwest::get(format!("{rest}/connectors")).await
                && r.status().is_success()
            {
                break;
            }
            if Instant::now() > deadline {
                let logs = std::process::Command::new("docker")
                    .args(["logs", "--tail", "40", container.id()])
                    .output()
                    .map(|o| {
                        String::from_utf8_lossy(&o.stdout).into_owned()
                            + &String::from_utf8_lossy(&o.stderr)
                    })
                    .unwrap_or_default();
                bail!("Kafka Connect never answered; its log ends:\n{logs}");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Ok(Connect {
            container,
            rest: std::sync::Mutex::new(rest),
        })
    }

    async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Value> {
        let c = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;
        let rest = self.rest.lock().unwrap().clone();
        let mut req = c.request(method, format!("{rest}{path}"));
        if let Some(b) = body {
            req = req.json(b);
        }
        let resp = req.send().await?;
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            bail!("connect {path}: {status}: {text}");
        }
        Ok(serde_json::from_str(&text).unwrap_or(Value::Null))
    }

    pub async fn create(&self, connector: &Value) -> Result<()> {
        self.call(reqwest::Method::POST, "/connectors", Some(connector))
            .await
            .map(|_| ())
    }

    /// The connector's and its task's states (`RUNNING`, `PAUSED`,
    /// `FAILED`, ...), and a failed task's trace.
    pub async fn status(
        &self,
        name: &str,
    ) -> Result<(String, Vec<String>, Option<String>)> {
        let v = self
            .call(
                reqwest::Method::GET,
                &format!("/connectors/{name}/status"),
                None,
            )
            .await?;
        let connector =
            v["connector"]["state"].as_str().unwrap_or("").to_string();
        let tasks: Vec<String> = v["tasks"]
            .as_array()
            .map(|t| {
                t.iter()
                    .map(|t| t["state"].as_str().unwrap_or("").to_string())
                    .collect()
            })
            .unwrap_or_default();
        let trace = v["tasks"]
            .as_array()
            .and_then(|t| t.iter().find_map(|t| t["trace"].as_str()))
            .map(|s| s.lines().take(3).collect::<Vec<_>>().join(" | "));
        Ok((connector, tasks, trace))
    }

    /// Wait until the connector and its task are in `state`.
    pub async fn wait(&self, name: &str, state: &str) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            if let Ok((c, tasks, trace)) = self.status(name).await {
                if c == "FAILED" || tasks.iter().any(|t| t == "FAILED") {
                    bail!("connector failed: {}", trace.unwrap_or_default());
                }
                if c == state
                    && !tasks.is_empty()
                    && tasks.iter().all(|t| t == state)
                {
                    return Ok(());
                }
            }
            if Instant::now() > deadline {
                bail!("connector {name} never reached {state}");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Stop a connector (Kafka Connect's `stop`: its tasks shut down and
    /// commit their offsets; `resume` starts it again from them).
    pub async fn stop_connector(&self, name: &str) -> Result<()> {
        self.call(
            reqwest::Method::PUT,
            &format!("/connectors/{name}/stop"),
            None,
        )
        .await?;
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            if let Ok((c, tasks, _)) = self.status(name).await
                && c == "STOPPED"
                && tasks.is_empty()
            {
                return Ok(());
            }
            if Instant::now() > deadline {
                bail!("connector {name} never stopped");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Stop the worker container (a graceful shutdown: its tasks commit
    /// their offsets).
    pub async fn down(&self) -> Result<()> {
        let ok = std::process::Command::new("docker")
            .args(["stop", "--time", "60", self.container.id()])
            .status()?
            .success();
        if !ok {
            bail!("docker stop failed");
        }
        Ok(())
    }

    /// Kill the worker container at once (`docker kill`: no shutdown, no
    /// final offset commit).
    pub fn crash(&self) -> Result<()> {
        let ok = std::process::Command::new("docker")
            .args(["kill", self.container.id()])
            .status()?
            .success();
        if !ok {
            bail!("docker kill failed");
        }
        Ok(())
    }

    /// Start the worker again and wait until its REST API answers; its
    /// connectors start from their stored configuration and offsets.
    pub async fn up(&self) -> Result<()> {
        let ok = std::process::Command::new("docker")
            .args(["start", self.container.id()])
            .status()?
            .success();
        if !ok {
            bail!("docker start failed");
        }
        let port = self.container.get_host_port_ipv4(8083).await?;
        let rest = format!("http://127.0.0.1:{port}");
        *self.rest.lock().unwrap() = rest.clone();
        let deadline = Instant::now() + Duration::from_secs(180);
        loop {
            if let Ok(r) = reqwest::get(format!("{rest}/connectors")).await
                && r.status().is_success()
            {
                return Ok(());
            }
            if Instant::now() > deadline {
                bail!("Kafka Connect never answered after a restart");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Wait until the worker has committed an offset for the connector
    /// (`offset.flush.interval.ms`, 60 s by default). Until then a crash
    /// leaves no offset, and a `no_data` connector restarts at the binlog's
    /// end, past every row it had not delivered.
    pub async fn committed(&self, name: &str) -> Result<Duration> {
        let start = Instant::now();
        loop {
            if let Ok(v) = self
                .call(
                    reqwest::Method::GET,
                    &format!("/connectors/{name}/offsets"),
                    None,
                )
                .await
                && v["offsets"].as_array().is_some_and(|o| !o.is_empty())
            {
                return Ok(start.elapsed());
            }
            if start.elapsed() > Duration::from_secs(180) {
                bail!("connector {name} committed no offset");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    pub async fn pause(&self, name: &str) -> Result<()> {
        self.call(
            reqwest::Method::PUT,
            &format!("/connectors/{name}/pause"),
            None,
        )
        .await?;
        self.wait(name, "PAUSED").await
    }

    pub async fn resume(&self, name: &str) -> Result<()> {
        self.call(
            reqwest::Method::PUT,
            &format!("/connectors/{name}/resume"),
            None,
        )
        .await
        .map(|_| ())
    }

    /// Why the connector failed, if it did.
    pub async fn failure(&self, name: &str) -> Option<String> {
        let (c, tasks, trace) = self.status(name).await.ok()?;
        (c == "FAILED" || tasks.iter().any(|t| t == "FAILED"))
            .then(|| trace.unwrap_or_else(|| "failed".into()))
    }

    pub async fn delete(&self, name: &str) {
        self.call(
            reqwest::Method::DELETE,
            &format!("/connectors/{name}"),
            None,
        )
        .await
        .ok();
    }

    fn cgroup(&self, file: &str) -> String {
        std::fs::read_to_string(format!(
            "/sys/fs/cgroup/system.slice/docker-{}.scope/{file}",
            self.container.id()
        ))
        .unwrap_or_default()
    }

    fn stat(text: &str, key: &str) -> u64 {
        text.lines()
            .find_map(|l| l.strip_prefix(key))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0)
    }

    /// (user, system) CPU seconds of the worker container.
    pub fn cpu_split(&self) -> (f64, f64) {
        let s = self.cgroup("cpu.stat");
        (
            Self::stat(&s, "user_usec ") as f64 / 1e6,
            Self::stat(&s, "system_usec ") as f64 / 1e6,
        )
    }

    /// The worker's anonymous (heap and stack) memory, KiB: its resident
    /// memory without the page cache.
    /// The worker container's main process (host pid).
    pub fn pid(&self) -> Option<u32> {
        let out = std::process::Command::new("docker")
            .args([
                "inspect",
                "--format",
                "{{.State.Pid}}",
                self.container.id(),
            ])
            .output()
            .ok()?;
        String::from_utf8(out.stdout).ok()?.trim().parse().ok()
    }

    pub fn rss_kib(&self) -> u64 {
        Self::stat(&self.cgroup("memory.stat"), "anon ") / 1024
    }

    /// Read MBean attributes with Kafka's JmxTool inside the worker's
    /// container: the sum of every matching MBean's attributes. The tool's
    /// JVM runs in the worker's cgroup, so callers read outside a measured
    /// window.
    fn jmx_sum(&self, object: &str, attribute: &str) -> Option<f64> {
        let out = std::process::Command::new("docker")
            .args(["exec", self.container.id()])
            .args([
                "/kafka/bin/kafka-run-class.sh",
                "org.apache.kafka.tools.JmxTool",
                "--one-time",
                "true",
                "--report-format",
                "csv",
                "--jmx-url",
            ])
            .arg(format!(
                "service:jmx:rmi:///jndi/rmi://localhost:{JMX_PORT}/jmxrmi"
            ))
            .args(["--object-name", object, "--attributes", attribute])
            .output()
            .ok()?;
        let text = String::from_utf8(out.stdout).ok()?;
        // One `name:attribute,"value"` line per MBean, after a `time` line.
        let values: Vec<f64> = text
            .lines()
            .filter(|l| l.contains(&format!(":{attribute},")))
            .filter_map(|l| {
                l.rsplit_once(',')?.1.trim_matches('"').parse().ok()
            })
            .collect();
        (!values.is_empty()).then(|| values.iter().sum())
    }

    /// The worker's Kafka requests (every producer: the connector's records
    /// and the worker's offset, status and config writes) and its offset
    /// records written (its checkpoint writes: the offset store's producer
    /// sends one record per committed source partition), so far.
    pub fn counters(&self) -> Option<[f64; 2]> {
        Some([
            self.jmx_sum(
                "kafka.producer:type=producer-metrics,client-id=*",
                "request-total",
            )?,
            self.jmx_sum(
                "kafka.producer:type=producer-metrics,client-id=*-offsets",
                "record-send-total",
            )?,
        ])
    }

    /// Stop the worker (at the end of an invocation, or after a failed
    /// run), keeping its log.
    pub async fn stop(self, log: &std::path::Path) {
        if let Ok(o) = std::process::Command::new("docker")
            .args(["logs", self.container.id()])
            .output()
        {
            let mut text = o.stdout;
            text.extend_from_slice(&o.stderr);
            std::fs::write(log, text).ok();
        }
        self.container.stop().await.ok();
        self.container.rm().await.ok();
    }
}

/// The format a connector writes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Format {
    /// JSON without embedded schemas (`schemas.enable=false`): the
    /// Debezium payload alone, as DeltaForge's native envelope writes it.
    Json,
    /// Avro through Confluent's converter and the schema registry at this
    /// URL (keys and values), as DeltaForge's Avro encoding writes it.
    Avro(String),
}

/// The Debezium MySQL connector of one run: table `{ns}.t`, its change
/// events on topic `{prefix}.{ns}.t`. `snapshot` copies the table first
/// (`initial`); otherwise only the schema is captured (`no_data`) and the
/// connector streams from the current binlog position.
#[allow(clippy::too_many_arguments)]
pub fn mysql_connector(
    name: &str,
    tag: &str,
    mysql_host: &str,
    password: &str,
    bootstrap: &str,
    server_id: u32,
    ns: &str,
    tables: &[String],
    prefix: &str,
    snapshot: bool,
    format: Format,
    js: Js,
    rows: usize,
    profile: Profile,
) -> Value {
    let mut connector = json!({
        "name": name,
        "config": {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "tasks.max": "1",
            "database.hostname": mysql_host,
            "database.port": "3306",
            "database.user": "root",
            "database.password": password,
            "database.server.id": server_id.to_string(),
            "topic.prefix": prefix,
            "database.include.list": ns,
            "table.include.list": tables.join(","),
            "include.schema.changes": "false",
            "schema.history.internal.kafka.bootstrap.servers": bootstrap,
            "schema.history.internal.kafka.topic": format!("dbz-history-{tag}"),
            "snapshot.mode": if snapshot { "initial" } else { "no_data" },
        }
    });
    let config = &mut connector["config"];
    match format {
        Format::Json => {
            for side in ["key", "value"] {
                config[format!("{side}.converter")] =
                    json!("org.apache.kafka.connect.json.JsonConverter");
                config[format!("{side}.converter.schemas.enable")] =
                    json!("false");
            }
        }
        Format::Avro(registry) => {
            for side in ["key", "value"] {
                config[format!("{side}.converter")] =
                    json!("io.confluent.connect.avro.AvroConverter");
                config[format!("{side}.converter.schema.registry.url")] =
                    json!(registry);
            }
        }
    }
    match js {
        Js::Off => {}
        // The JavaScript counterpart of DeltaForge's JavaScript processor: a
        // JavaScript expression evaluated per record that reads the row image
        // (Debezium's scripting transformations filter or route records; none
        // adds a field, so this one keeps every record).
        Js::Tag => {
            config["transforms"] = json!("js");
            config["transforms.js.type"] =
                json!("io.debezium.transforms.Filter");
            config["transforms.js.language"] = json!("jsr223.graal.js");
            config["transforms.js.condition"] =
                json!("value.after == null || value.after.id != null");
        }
        // The same filter and routing as DeltaForge's script (`Js::Route`):
        // its Filter, then its ContentBasedRouter, both JavaScript.
        Js::Route => {
            let [a, b] = route_topics(prefix);
            config["transforms"] = json!("filter,route");
            config["transforms.filter.type"] =
                json!("io.debezium.transforms.Filter");
            config["transforms.filter.language"] = json!("jsr223.graal.js");
            config["transforms.filter.condition"] = json!(format!(
                "value.after != null && (value.after.id % 2 == 0 || value.after.id > {rows})"
            ));
            config["transforms.route.type"] =
                json!("io.debezium.transforms.ContentBasedRouter");
            config["transforms.route.language"] = json!("jsr223.graal.js");
            config["transforms.route.topic.expression"] =
                json!(format!("value.after.id % 4 == 0 ? '{a}' : '{b}'"));
        }
    }
    if profile == Profile::Tuned {
        connector["config"]["poll.interval.ms"] = json!("50");
        connector["config"]["max.batch.size"] = json!("2048");
        // Tables snapshotted in parallel, as many as DeltaForge's default
        // (`max_parallel_tables`, 8).
        connector["config"]["snapshot.max.threads"] = json!("8");
    }
    connector
}

/// The Parquet path of a Debezium run: Confluent's S3 sink connector
/// writing the connector's topic (Avro through `registry`) to `bucket` under
/// `prefix`, `rows_per_object` rows per Parquet object as DeltaForge's
/// durable S3 sink writes one object per batch (2,000 rows by default), and a
/// partial object at least every second (DeltaForge's batch timer delivers a
/// partial batch too).
#[allow(clippy::too_many_arguments)]
pub fn s3_sink(
    name: &str,
    topics: &[String],
    endpoint: &str,
    bucket: &str,
    prefix: &str,
    access_key: &str,
    secret_key: &str,
    registry: &str,
    rows_per_object: usize,
) -> Value {
    json!({
        "name": name,
        "config": {
            "connector.class": "io.confluent.connect.s3.S3SinkConnector",
            "tasks.max": "1",
            "topics": topics.join(","),
            "store.url": endpoint,
            "s3.bucket.name": bucket,
            "s3.region": "us-east-1",
            "s3.path.style.access.enabled": "true",
            "aws.access.key.id": access_key,
            "aws.secret.access.key": secret_key,
            "storage.class": "io.confluent.connect.s3.storage.S3Storage",
            "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
            "parquet.codec": "snappy",
            "topics.dir": prefix,
            "flush.size": rows_per_object.to_string(),
            "rotate.schedule.interval.ms": "1000",
            "timezone": "UTC",
            "schema.compatibility": "NONE",
            "key.converter": "io.confluent.connect.avro.AvroConverter",
            "key.converter.schema.registry.url": registry,
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter.schema.registry.url": registry,
        }
    })
}

/// The JavaScript a cell runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Js {
    Off,
    /// DeltaForge adds a field to every row; Debezium, whose scripting
    /// transformations cannot, evaluates a condition per record that keeps
    /// every one.
    Tag,
    /// Both keep the rows with an even id (and the markers, above the
    /// fixture's ids) and route each to one of two topics by `id % 4`
    /// (`route_topics`, the first for 0).
    Route,
}

/// The two topics a `Js::Route` cell routes to.
pub fn route_topics(base: &str) -> [String; 2] {
    [format!("{base}.a"), format!("{base}.b")]
}

/// The topic a connector of `prefix` writes `table` (`db.table`) to.
pub fn topic(prefix: &str, table: &str) -> String {
    format!("{prefix}.{table}")
}

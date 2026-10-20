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
//!   2,048 events (DeltaForge: 2,000).

use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use gate_ownership::GateOwned;
use serde_json::{Value, json};
use testcontainers::core::IntoContainerPort;
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

/// A running Kafka Connect worker.
pub struct Connect {
    container: ContainerAsync<GenericImage>,
    rest: String,
}

impl Connect {
    /// Start a worker for one run (`tag` names its group and internal
    /// topics) and wait until its REST API answers.
    pub async fn start(
        network: &str,
        bootstrap: &str,
        tag: &str,
        profile: Profile,
    ) -> Result<Connect> {
        let mut image = GenericImage::new(CONNECT_IMAGE.0, CONNECT_IMAGE.1)
            .with_exposed_port(8083.tcp())
            .with_network(network)
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
                bail!("Kafka Connect never answered");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Ok(Connect { container, rest })
    }

    async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Value> {
        let c = reqwest::Client::new();
        let mut req = c.request(method, format!("{}{path}", self.rest));
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
    pub fn rss_kib(&self) -> u64 {
        Self::stat(&self.cgroup("memory.stat"), "anon ") / 1024
    }

    /// Stop the worker (at the end of an invocation).
    pub async fn stop(self) {
        self.container.stop().await.ok();
        self.container.rm().await.ok();
    }
}

/// The value format a connector writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    /// JSON without embedded schemas (`schemas.enable=false`): the
    /// Debezium payload alone, as DeltaForge's native envelope writes it.
    Json,
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
    prefix: &str,
    snapshot: bool,
    format: Format,
    profile: Profile,
) -> Value {
    let Format::Json = format;
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
            "table.include.list": format!("{ns}.t"),
            "include.schema.changes": "false",
            "schema.history.internal.kafka.bootstrap.servers": bootstrap,
            "schema.history.internal.kafka.topic": format!("dbz-history-{tag}"),
            "snapshot.mode": if snapshot { "initial" } else { "no_data" },
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
        }
    });
    if profile == Profile::Tuned {
        connector["config"]["poll.interval.ms"] = json!("50");
        connector["config"]["max.batch.size"] = json!("2048");
    }
    connector
}

/// The topic a connector of `prefix` writes table `{ns}.t` to.
pub fn topic(prefix: &str, ns: &str) -> String {
    format!("{prefix}.{ns}.t")
}

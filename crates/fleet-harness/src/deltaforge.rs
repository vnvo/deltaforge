//! The instance under test: pipeline specs and its REST API.
//!
//! One pipeline per cluster (design section 1): a MySQL source over every
//! customer database of the server and a Kafka sink with one topic per
//! cluster, in DeltaForge's native envelope. The harness only drives the
//! public API; it never reaches into the process.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

use crate::config::{KeyConfig, RunConfig, Server};
use crate::topology::Naming;

pub fn pipeline_name(cfg: &RunConfig, server: &Server) -> String {
    format!("{}-{}", cfg.deltaforge.pipeline_prefix, server.name)
}

pub fn topic(cfg: &RunConfig, server: &Server) -> String {
    format!("{}.{}", cfg.deltaforge.topic_prefix, server.name)
}

/// The pipeline spec (JSON, as the REST API takes it) for one server.
pub fn render_spec(
    cfg: &RunConfig,
    server: &Server,
    naming: &Naming,
) -> Result<Value> {
    let d = &cfg.deltaforge;
    let (host, port) = server.cdc_endpoint();
    let mut sink = json!({
        "id": "kafka",
        "brokers": d.kafka_brokers,
        "topic": topic(cfg, server),
        "envelope": {"type": "native"},
        "required": true,
    });
    if let KeyConfig::Template { template } = &d.key {
        sink["key"] = json!(template);
    }
    let mut spec = json!({
        "apiVersion": "deltaforge/v1",
        "kind": "Pipeline",
        "metadata": {"name": pipeline_name(cfg, server), "tenant": d.tenant},
        "spec": {
            "source": {"type": "mysql", "config": {
                "id": format!("src-{}", server.name),
                "dsn": format!("mysql://{}:{}@{host}:{port}/", d.cdc_user, d.cdc_password),
                "tables": [naming.table_pattern()],
            }},
            "processors": [],
            "sinks": [{"type": "kafka", "config": sink}],
        }
    });
    if let Some(overrides) = &d.spec_overrides {
        let overrides: Value =
            serde_json::to_value(overrides).context("spec_overrides")?;
        if !overrides.is_object() {
            bail!("deltaforge.spec_overrides must be a mapping");
        }
        merge(&mut spec["spec"], &overrides);
    }
    Ok(spec)
}

/// Deep-merge `patch` into `base` (objects merge, anything else replaces).
pub fn merge(base: &mut Value, patch: &Value) {
    match (base, patch) {
        (Value::Object(b), Value::Object(p)) => {
            for (k, v) in p {
                merge(b.entry(k.clone()).or_insert(Value::Null), v);
            }
        }
        (b, p) => *b = p.clone(),
    }
}

/// The DeltaForge REST API.
#[derive(Clone)]
pub struct Api {
    base: String,
    http: reqwest::Client,
}

impl Api {
    pub fn new(base: &str) -> Result<Api> {
        Ok(Api {
            base: base.trim_end_matches('/').to_string(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()?,
        })
    }

    async fn send(
        &self,
        req: reqwest::RequestBuilder,
        what: &str,
    ) -> Result<Value> {
        let resp = req.send().await.with_context(|| what.to_string())?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("{what}: {status}: {body}");
        }
        Ok(serde_json::from_str(&body).unwrap_or(Value::Null))
    }

    pub async fn create(&self, spec: &Value) -> Result<Value> {
        self.send(
            self.http
                .post(format!("{}/pipelines", self.base))
                .json(spec),
            "create pipeline",
        )
        .await
    }

    pub async fn get(&self, name: &str) -> Result<Value> {
        self.send(
            self.http.get(format!("{}/pipelines/{name}", self.base)),
            "get pipeline",
        )
        .await
    }

    pub async fn status(&self, name: &str) -> Result<String> {
        Ok(self.get(name).await?["status"]
            .as_str()
            .unwrap_or("")
            .to_string())
    }

    /// The pipeline's open incidents (why it failed).
    pub async fn incidents(&self, name: &str) -> Result<Value> {
        self.send(
            self.http
                .get(format!("{}/pipelines/{name}/incidents", self.base)),
            "pipeline incidents",
        )
        .await
    }

    pub async fn stop(&self, name: &str) -> Result<Value> {
        self.send(
            self.http
                .post(format!("{}/pipelines/{name}/stop", self.base)),
            "stop pipeline",
        )
        .await
    }

    pub async fn resume(&self, name: &str) -> Result<Value> {
        self.send(
            self.http
                .post(format!("{}/pipelines/{name}/resume", self.base)),
            "resume pipeline",
        )
        .await
    }

    pub async fn patch(&self, name: &str, patch: &Value) -> Result<Value> {
        self.send(
            self.http
                .patch(format!("{}/pipelines/{name}", self.base))
                .json(patch),
            "patch pipeline",
        )
        .await
    }

    pub async fn delete(&self, name: &str) -> Result<()> {
        self.send(
            self.http.delete(format!("{}/pipelines/{name}", self.base)),
            "delete pipeline",
        )
        .await
        .map(|_| ())
    }

    /// Wait until `name` reports `running` (or fail after `timeout`).
    pub async fn until_running(
        &self,
        name: &str,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            match self.status(name).await {
                Ok(s) if s == "running" => return Ok(()),
                Ok(s) if s == "failed" => {
                    bail!("pipeline {name} failed to start")
                }
                _ => {}
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("pipeline {name} not running after {timeout:?}");
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::tests::EXAMPLE;

    #[test]
    fn a_spec_per_cluster_with_its_topic_key_and_overrides() {
        let mut cfg: RunConfig = serde_yaml::from_str(EXAMPLE).unwrap();
        cfg.deltaforge.spec_overrides = Some(
            serde_yaml::from_str("batch: {max_events: 4000}\nsinks: null")
                .unwrap(),
        );
        let naming = Naming {
            database_prefix: "cust_".into(),
        };
        let server = &cfg.topology.servers[0];
        let spec = render_spec(&cfg, server, &naming).unwrap();
        assert_eq!(spec["metadata"]["name"], "fleet-c01");
        let src = &spec["spec"]["source"]["config"];
        assert_eq!(src["tables"][0], "cust_*.*");
        assert_eq!(src["dsn"], "mysql://df:df@127.0.0.1:3306/");
        assert_eq!(spec["spec"]["batch"]["max_events"], 4000);
        assert!(
            spec["spec"]["sinks"].is_null(),
            "overrides replace non-objects"
        );

        cfg.deltaforge.spec_overrides = None;
        let spec = render_spec(&cfg, server, &naming).unwrap();
        let sink = &spec["spec"]["sinks"][0]["config"];
        assert_eq!(sink["topic"], "fleet.c01");
        assert_eq!(sink["key"], "${after.id}");
        assert_eq!(sink["envelope"]["type"], "native");
    }

    #[test]
    fn the_cdc_endpoint_can_differ_from_the_harness_endpoint() {
        let mut cfg: RunConfig = serde_yaml::from_str(EXAMPLE).unwrap();
        cfg.topology.servers[0].cdc_host = Some("toxiproxy".into());
        cfg.topology.servers[0].cdc_port = Some(23306);
        let naming = Naming {
            database_prefix: "cust_".into(),
        };
        let spec =
            render_spec(&cfg, &cfg.topology.servers[0], &naming).unwrap();
        assert_eq!(
            spec["spec"]["source"]["config"]["dsn"],
            "mysql://df:df@toxiproxy:23306/"
        );
    }
}

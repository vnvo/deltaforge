use anyhow::Result;
use reqwest::Client;
use serde_json::{Value, json};

const TOXIPROXY_API: &str = "http://localhost:8474";

pub struct ToxiproxyClient {
    http: Client,
}

impl ToxiproxyClient {
    pub fn new() -> Self {
        Self {
            http: Client::new(),
        }
    }

    /// Get the enabled state of all known proxies (Toxiproxy-level, not pipeline DSN).
    #[allow(dead_code)]
    /// Returns a list of (name, enabled) pairs.
    pub async fn proxy_states(&self) -> Result<Vec<(String, bool)>> {
        let resp: Value = self
            .http
            .get(format!("{TOXIPROXY_API}/proxies"))
            .send()
            .await?
            .json()
            .await?;
        let mut states = Vec::new();
        if let Some(obj) = resp.as_object() {
            for (name, info) in obj {
                let enabled = info["enabled"].as_bool().unwrap_or(true);
                states.push((name.clone(), enabled));
            }
        }
        states.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(states)
    }

    /// Format proxy states as a human-readable summary for scenario logs.
    #[allow(dead_code)]
    pub async fn proxy_summary(&self) -> String {
        match self.proxy_states().await {
            Ok(states) => {
                let parts: Vec<String> = states
                    .iter()
                    .map(|(name, enabled)| {
                        format!(
                            "{}={}",
                            name,
                            if *enabled { "on" } else { "DISABLED" }
                        )
                    })
                    .collect();
                parts.join(", ")
            }
            Err(_) => "unknown (toxiproxy unreachable)".to_string(),
        }
    }

    /// Cut all connections through a proxy.
    pub async fn disable(&self, proxy: &str) -> Result<()> {
        self.http
            .post(format!("{TOXIPROXY_API}/proxies/{proxy}"))
            .json(&json!({"enabled": false}))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    /// Change the upstream a proxy points at without touching enabled state.
    pub async fn update_upstream(
        &self,
        proxy: &str,
        upstream: &str,
    ) -> Result<()> {
        self.http
            .post(format!("{TOXIPROXY_API}/proxies/{proxy}"))
            .json(&json!({"upstream": upstream}))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    /// Restore a previously disabled proxy.
    pub async fn enable(&self, proxy: &str) -> Result<()> {
        self.http
            .post(format!("{TOXIPROXY_API}/proxies/{proxy}"))
            .json(&json!({"enabled": true}))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    /// Add a toxic. `attributes` is toxic-specific (latency, rate, etc).
    #[allow(dead_code)]
    pub async fn add_toxic(
        &self,
        proxy: &str,
        name: &str,
        kind: &str,
        attributes: Value,
    ) -> Result<()> {
        self.http
            .post(format!("{TOXIPROXY_API}/proxies/{proxy}/toxics"))
            .json(&json!({
                "name": name,
                "type": kind,
                "attributes": attributes,
            }))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    pub async fn remove_toxic(&self, proxy: &str, name: &str) -> Result<()> {
        self.http
            .delete(format!("{TOXIPROXY_API}/proxies/{proxy}/toxics/{name}"))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    /// Convenience: add a bandwidth toxic that throttles to ~0 (simulates packet loss).
    #[allow(dead_code)]
    pub async fn throttle(&self, proxy: &str) -> Result<()> {
        self.add_toxic(proxy, "throttle", "bandwidth", json!({"rate": 0}))
            .await
    }

    /// Convenience: add a latency toxic.
    #[allow(dead_code)]
    pub async fn add_latency(
        &self,
        proxy: &str,
        latency_ms: u64,
        jitter_ms: u64,
    ) -> Result<()> {
        self.add_toxic(
            proxy,
            "latency",
            "latency",
            json!({"latency": latency_ms, "jitter": jitter_ms}),
        )
        .await
    }

    /// Remove all toxics from a proxy (clean slate).
    pub async fn reset(&self, proxy: &str) -> Result<()> {
        let resp: Value = self
            .http
            .get(format!("{TOXIPROXY_API}/proxies/{proxy}"))
            .send()
            .await?
            .json()
            .await?;

        if let Some(toxics) = resp["toxics"].as_array() {
            let names: Vec<String> = toxics
                .iter()
                .filter_map(|t| t["name"].as_str().map(String::from))
                .collect();
            for name in names {
                self.remove_toxic(proxy, &name).await?;
            }
        }
        self.enable(proxy).await?;
        Ok(())
    }

    /// Reset all known proxies to clean state.
    pub async fn reset_all(&self) -> Result<()> {
        for proxy in ["mysql", "postgres", "kafka"] {
            self.reset(proxy).await?;
        }
        Ok(())
    }
}

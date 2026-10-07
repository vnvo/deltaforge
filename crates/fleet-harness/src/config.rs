//! Run configuration.
//!
//! Every value the owner has not supplied yet (traffic, DDL, lifecycle,
//! budgets) is a [`Param`] with a [`Provenance`]: a bare value in the file is
//! a **placeholder**; only `{ value: .., provenance: owner }` is an owner
//! input. Exploratory runs accept placeholders; a qualification run refuses
//! to start unless every parameter is an owner input (see
//! [`RunConfig::qualification_errors`]), so a placeholder can never become a
//! published capacity claim.

use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Deserializer, Serialize};

/// Where a value came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provenance {
    /// Supplied by the owner (production evidence or a decided budget).
    Owner,
    /// A stand-in until the owner supplies it.
    Placeholder,
}

/// A configurable value and where it came from.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Param<T> {
    pub value: T,
    pub provenance: Provenance,
}

impl<T> Param<T> {
    pub fn owner(value: T) -> Self {
        Self {
            value,
            provenance: Provenance::Owner,
        }
    }

    pub fn placeholder(value: T) -> Self {
        Self {
            value,
            provenance: Provenance::Placeholder,
        }
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Param<T> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Full<T> {
            value: T,
            provenance: Provenance,
        }
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Either<T> {
            Full(Full<T>),
            Bare(T),
        }
        Ok(match Either::<T>::deserialize(d)? {
            Either::Full(f) => Param {
                value: f.value,
                provenance: f.provenance,
            },
            // A bare value is never an owner input.
            Either::Bare(value) => Param::placeholder(value),
        })
    }
}

/// Collects the paths of placeholder (or missing) parameters.
pub trait Provenanced {
    fn collect(&self, path: &str, out: &mut Vec<String>);
}

impl<T> Provenanced for Param<T> {
    fn collect(&self, path: &str, out: &mut Vec<String>) {
        if self.provenance != Provenance::Owner {
            out.push(path.to_string());
        }
    }
}

impl<T: Provenanced> Provenanced for Option<T> {
    fn collect(&self, path: &str, out: &mut Vec<String>) {
        match self {
            Some(v) => v.collect(path, out),
            None => out.push(format!("{path} (missing)")),
        }
    }
}

/// Exploratory results never support capacity claims; qualification results
/// may, once reviewed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunClass {
    Exploratory,
    Qualification,
}

impl RunClass {
    pub fn dir(self) -> &'static str {
        match self {
            RunClass::Exploratory => "exploratory",
            RunClass::Qualification => "qualification",
        }
    }
}

/// The qualification profile. Only CDC is in scope (owner decision,
/// 2026-10-08); snapshot qualification is a separate milestone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    Cdc,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunConfig {
    pub class: RunClass,
    pub profile: Profile,
    /// Results go to `<output_dir>/<class>/<run id>/`.
    pub output_dir: String,
    pub topology: Topology,
    pub deltaforge: DeltaForge,
    pub verifier: Verifier,
    pub traffic: Traffic,
    #[serde(default)]
    pub ddl: Ddl,
    #[serde(default)]
    pub lifecycle: Lifecycle,
    #[serde(default)]
    pub budgets: Budgets,
    #[serde(default)]
    pub policy: Policy,
    #[serde(default)]
    pub disruptions: Disruptions,
    #[serde(default)]
    pub actions: BTreeMap<String, String>,
    pub measure: Measure,
    pub run: Run,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Server {
    /// The cluster name; also the pipeline suffix.
    pub name: String,
    /// Where the harness (fixture builder, workload driver) connects.
    pub host: String,
    pub port: u16,
    /// Where DeltaForge connects (a proxy for disruption scenarios);
    /// defaults to `host:port`.
    #[serde(default)]
    pub cdc_host: Option<String>,
    #[serde(default)]
    pub cdc_port: Option<u16>,
    /// Toxiproxy proxy name in front of `cdc_host:cdc_port`, if any.
    #[serde(default)]
    pub proxy: Option<String>,
}

impl Server {
    pub fn cdc_endpoint(&self) -> (String, u16) {
        (
            self.cdc_host.clone().unwrap_or_else(|| self.host.clone()),
            self.cdc_port.unwrap_or(self.port),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Topology {
    pub servers: Vec<Server>,
    /// Harness credentials (fixture, driver, measurement).
    pub admin_user: String,
    pub admin_password: String,
    pub databases_per_server: Param<u32>,
    pub tables_per_database: Param<u32>,
    #[serde(default = "default_db_prefix")]
    pub database_prefix: String,
    /// Seed of the deterministic table templates.
    #[serde(default)]
    pub seed: u64,
}

fn default_db_prefix() -> String {
    "cust_".into()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ProcessLocator {
    /// No resource sampling (results say so).
    None,
    Pid {
        pid: u32,
    },
    DockerContainer {
        name: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum KeyConfig {
    /// DeltaForge's default (idempotency) key.
    Default,
    /// A `key:` template rendered into the sink, e.g. `${after.id}` today, or
    /// a coalescing expression once the product supports one.
    Template { template: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeltaForge {
    /// REST API, e.g. `http://127.0.0.1:8080`.
    pub api_url: String,
    /// Prometheus endpoint, e.g. `http://127.0.0.1:9000/metrics`.
    pub metrics_url: String,
    pub process: ProcessLocator,
    /// The state store the instance under test uses (`postgres` for
    /// qualification; recorded in results).
    pub state_store: String,
    #[serde(default = "default_pipeline_prefix")]
    pub pipeline_prefix: String,
    #[serde(default = "default_tenant")]
    pub tenant: String,
    pub cdc_user: String,
    pub cdc_password: String,
    pub kafka_brokers: String,
    #[serde(default = "default_topic_prefix")]
    pub topic_prefix: String,
    pub key: KeyConfig,
    /// Merged into every rendered pipeline spec (YAML mapping under `spec`),
    /// for settings the harness does not model (batching, future key
    /// features, ...).
    #[serde(default)]
    pub spec_overrides: Option<serde_yaml::Value>,
}

fn default_pipeline_prefix() -> String {
    "fleet".into()
}
fn default_tenant() -> String {
    "fleet".into()
}
fn default_topic_prefix() -> String {
    "fleet".into()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpectedKey {
    /// Keys are not checked.
    None,
    /// Every operation, deletes included, must carry the row's primary key
    /// as its message key (the coalesced-key behavior production requires).
    PrimaryKey,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Verifier {
    pub kafka_brokers: String,
    pub expected_key: ExpectedKey,
    /// Where ledgers and consumed records are spilled for the final
    /// completeness check.
    pub work_dir: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpMix {
    pub insert: f64,
    pub update: f64,
    pub delete: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "pattern")]
pub enum ActiveSetPattern {
    /// A fixed fraction of all tables.
    Fixed { fraction: f64 },
    /// `fraction` of the tables at a time, moving to a disjoint set every
    /// `period_secs`.
    Moving { fraction: f64, period_secs: u64 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Traffic {
    /// Per source (cluster).
    pub changes_per_sec_avg: Param<f64>,
    pub changes_per_sec_peak: Param<f64>,
    /// Fraction of the run spent at peak rate (in bursts).
    pub peak_fraction: Param<f64>,
    pub op_mix: Param<OpMix>,
    pub row_bytes: Param<u32>,
    pub rows_per_txn: Param<u32>,
    /// Skew within the active set (Zipf exponent; 0 = uniform).
    pub zipf_exponent: Param<f64>,
    pub active_set: Param<ActiveSetPattern>,
    /// Writer connections per server.
    #[serde(default = "default_writers")]
    pub writers_per_server: u32,
}

fn default_writers() -> u32 {
    4
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Rollout {
    /// Every customer database in turn.
    Sequential,
    /// Groups of `group_size` databases, `gap_secs` apart.
    Staged { group_size: u32, gap_secs: u64 },
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ddl {
    /// `ALTER TABLE` statements per customer database in one migration.
    pub migration_statements: Option<Param<u32>>,
    /// Statements per second per server while a migration runs.
    pub migration_pace: Option<Param<f64>>,
    pub migration_rollout: Option<Param<Rollout>>,
    pub single_customer_ddl_per_hour: Option<Param<f64>>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lifecycle {
    pub onboard_per_hour: Option<Param<f64>>,
    pub offboard_per_hour: Option<Param<f64>>,
}

/// Budgets are evaluated only when they are owner inputs; placeholder
/// budgets are reported as "not evaluated".
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Budgets {
    pub lag_p99_secs: Option<Param<f64>>,
    pub memory_bytes: Option<Param<u64>>,
    pub cpu_cores: Option<Param<f64>>,
    pub recovery_p50_secs: Option<Param<f64>>,
    pub recovery_p90_secs: Option<Param<f64>>,
    pub recovery_p100_secs: Option<Param<f64>>,
    pub shutdown_secs: Option<Param<f64>>,
    pub connections_per_server: Option<Param<u64>>,
}

/// What a qualification repetition tolerates beyond correctness.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    /// Committed operations over the configured target in the measured
    /// window, per server, at least this (an owner input for a
    /// qualification).
    pub min_achieved_ratio: Option<Param<f64>>,
    /// Transactions whose commit outcome could not be determined. Absent or
    /// not an owner input means none is tolerated.
    pub max_uncertain_transactions: Option<Param<u64>>,
}

impl Policy {
    /// The uncertain transactions a qualification tolerates.
    pub fn uncertain_allowance(&self) -> u64 {
        match &self.max_uncertain_transactions {
            Some(p) if p.provenance == Provenance::Owner => p.value,
            _ => 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Disruptions {
    pub endpoint_outage_secs: u64,
    pub sink_outage_secs: u64,
    /// Toxiproxy API, for endpoint disruptions.
    #[serde(default)]
    pub toxiproxy_url: Option<String>,
}

impl Default for Disruptions {
    fn default() -> Self {
        Self {
            endpoint_outage_secs: 300,
            sink_outage_secs: 600,
            toxiproxy_url: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Measure {
    #[serde(default = "default_interval")]
    pub interval_secs: u64,
    /// The PostgreSQL state store, for size measurements.
    #[serde(default)]
    pub state_store_dsn: Option<String>,
    /// MySQL users whose connections count as DeltaForge's.
    pub cdc_users: Vec<String>,
}

fn default_interval() -> u64 {
    5
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Run {
    /// Scenario id (`S1`..`S17`) or active-set point (`A1`..`A5`).
    pub scenario: String,
    pub duration_secs: u64,
    #[serde(default)]
    pub warmup_secs: u64,
    /// Measured repetitions (short scenarios need at least 5).
    #[serde(default = "default_reps")]
    pub repetitions: u32,
    /// Source counts to run at; each uses the first N servers.
    pub sources: Vec<u32>,
    /// Stop the sweep at the first source count whose run fails to start
    /// or loses data (later counts are not attempted).
    #[serde(default = "default_true")]
    pub stop_sweep_on_failure: bool,
}

fn default_reps() -> u32 {
    1
}
fn default_true() -> bool {
    true
}

impl RunConfig {
    pub fn load(path: &str) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("read run config {path}"))?;
        let cfg: RunConfig = serde_yaml::from_str(&raw)
            .with_context(|| format!("parse run config {path}"))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Structural checks for any run.
    pub fn validate(&self) -> Result<()> {
        if self.topology.servers.is_empty() {
            bail!("topology.servers is empty");
        }
        let max = self.run.sources.iter().copied().max().unwrap_or(0);
        if self.run.sources.is_empty() || max == 0 {
            bail!("run.sources must list source counts of at least 1");
        }
        if max as usize > self.topology.servers.len() {
            bail!(
                "run.sources asks for {max} sources but topology has {} servers",
                self.topology.servers.len()
            );
        }
        let mix = self.traffic.op_mix.value;
        let total = mix.insert + mix.update + mix.delete;
        if (total - 1.0).abs() > 1e-6 || mix.insert <= 0.0 {
            bail!("traffic.op_mix must sum to 1 with a positive insert share");
        }
        match &self.traffic.active_set.value {
            ActiveSetPattern::Fixed { fraction }
            | ActiveSetPattern::Moving { fraction, .. } => {
                if !(*fraction > 0.0 && *fraction <= 1.0) {
                    bail!("active-set fraction must be in (0, 1]");
                }
            }
        }
        Ok(())
    }

    /// Every placeholder or missing owner input.
    pub fn placeholders(&self) -> Vec<String> {
        let mut out = Vec::new();
        let t = &self.topology;
        t.databases_per_server
            .collect("topology.databases_per_server", &mut out);
        t.tables_per_database
            .collect("topology.tables_per_database", &mut out);
        let tr = &self.traffic;
        tr.changes_per_sec_avg
            .collect("traffic.changes_per_sec_avg", &mut out);
        tr.changes_per_sec_peak
            .collect("traffic.changes_per_sec_peak", &mut out);
        tr.peak_fraction.collect("traffic.peak_fraction", &mut out);
        tr.op_mix.collect("traffic.op_mix", &mut out);
        tr.row_bytes.collect("traffic.row_bytes", &mut out);
        tr.rows_per_txn.collect("traffic.rows_per_txn", &mut out);
        tr.zipf_exponent.collect("traffic.zipf_exponent", &mut out);
        tr.active_set.collect("traffic.active_set", &mut out);
        let d = &self.ddl;
        d.migration_statements
            .collect("ddl.migration_statements", &mut out);
        d.migration_pace.collect("ddl.migration_pace", &mut out);
        d.migration_rollout
            .collect("ddl.migration_rollout", &mut out);
        d.single_customer_ddl_per_hour
            .collect("ddl.single_customer_ddl_per_hour", &mut out);
        let l = &self.lifecycle;
        l.onboard_per_hour
            .collect("lifecycle.onboard_per_hour", &mut out);
        l.offboard_per_hour
            .collect("lifecycle.offboard_per_hour", &mut out);
        let b = &self.budgets;
        b.lag_p99_secs.collect("budgets.lag_p99_secs", &mut out);
        b.memory_bytes.collect("budgets.memory_bytes", &mut out);
        b.cpu_cores.collect("budgets.cpu_cores", &mut out);
        b.recovery_p50_secs
            .collect("budgets.recovery_p50_secs", &mut out);
        b.recovery_p90_secs
            .collect("budgets.recovery_p90_secs", &mut out);
        b.recovery_p100_secs
            .collect("budgets.recovery_p100_secs", &mut out);
        b.shutdown_secs.collect("budgets.shutdown_secs", &mut out);
        b.connections_per_server
            .collect("budgets.connections_per_server", &mut out);
        self.policy
            .min_achieved_ratio
            .collect("policy.min_achieved_ratio", &mut out);
        out
    }

    /// Why this configuration cannot run as a qualification (empty if it
    /// can). Exploratory runs ignore these.
    pub fn qualification_errors(&self) -> Vec<String> {
        let mut errors: Vec<String> = self
            .placeholders()
            .into_iter()
            .map(|p| format!("{p} is not an owner input"))
            .collect();
        if self.deltaforge.state_store != "postgres" {
            errors.push(
                "deltaforge.state_store must be postgres (SQLite is not \
                 qualified for this target)"
                    .into(),
            );
        }
        if self.verifier.expected_key != ExpectedKey::PrimaryKey {
            errors.push(
                "verifier.expected_key must be primary_key (primary-key \
                 partitioning across deletes is mandatory for production)"
                    .into(),
            );
        }
        if self.run.repetitions < 5 && self.is_short_scenario() {
            errors.push(format!(
                "{} is a short scenario: at least 5 measured repetitions",
                self.run.scenario
            ));
        }
        errors
    }

    /// Scenarios measured in minutes (design section 5.3).
    pub fn is_short_scenario(&self) -> bool {
        matches!(
            self.run.scenario.as_str(),
            "S5" | "S8" | "S11" | "S12" | "S15" | "S17"
        )
    }

    /// Refuse a qualification run whose inputs are incomplete.
    pub fn check_class(&self) -> Result<()> {
        if self.class == RunClass::Qualification {
            let errors = self.qualification_errors();
            if !errors.is_empty() {
                bail!(
                    "this configuration cannot run as a qualification:\n  - {}",
                    errors.join("\n  - ")
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) const EXAMPLE: &str =
        include_str!("../configs/t1-exploratory.yaml");

    #[test]
    fn a_bare_value_is_a_placeholder_and_only_an_explicit_owner_is_owner() {
        let p: Param<f64> = serde_yaml::from_str("2000").unwrap();
        assert_eq!(p.provenance, Provenance::Placeholder);
        let p: Param<f64> =
            serde_yaml::from_str("{value: 2000, provenance: owner}").unwrap();
        assert_eq!(p, Param::owner(2000.0));
        assert!(
            serde_yaml::from_str::<Param<f64>>("{value: 1, provenance: x}")
                .is_err()
        );
    }

    #[test]
    fn the_example_config_is_exploratory_and_lists_its_placeholders() {
        let cfg: RunConfig = serde_yaml::from_str(EXAMPLE).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.class, RunClass::Exploratory);
        cfg.check_class().unwrap();
        let placeholders = cfg.placeholders();
        assert!(placeholders.contains(&"traffic.changes_per_sec_avg".into()));
        assert!(
            placeholders
                .iter()
                .any(|p| p.starts_with("budgets.lag_p99_secs"))
        );
    }

    #[test]
    fn a_qualification_with_placeholders_is_refused() {
        let mut cfg: RunConfig = serde_yaml::from_str(EXAMPLE).unwrap();
        cfg.class = RunClass::Qualification;
        let err = cfg.check_class().unwrap_err().to_string();
        assert!(
            err.contains("traffic.changes_per_sec_avg is not an owner input")
        );
        assert!(err.contains("expected_key must be primary_key"));
    }

    #[test]
    fn a_sweep_beyond_the_topology_is_rejected() {
        let mut cfg: RunConfig = serde_yaml::from_str(EXAMPLE).unwrap();
        cfg.run.sources = vec![1, 1_000];
        assert!(cfg.validate().unwrap_err().to_string().contains("servers"));
    }

    #[test]
    fn uncertain_transactions_are_tolerated_only_by_an_owner_policy() {
        let mut cfg: RunConfig = serde_yaml::from_str(EXAMPLE).unwrap();
        assert_eq!(cfg.policy.uncertain_allowance(), 0);
        cfg.policy.max_uncertain_transactions = Some(Param::placeholder(5));
        assert_eq!(
            cfg.policy.uncertain_allowance(),
            0,
            "a placeholder allows nothing"
        );
        cfg.policy.max_uncertain_transactions = Some(Param::owner(5));
        assert_eq!(cfg.policy.uncertain_allowance(), 5);
        assert!(
            cfg.placeholders()
                .iter()
                .any(|p| p.starts_with("policy.min_achieved_ratio"))
        );
    }
}

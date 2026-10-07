//! Scenario plans (design sections 5.1-5.3).
//!
//! A plan is the active-set override and the timed actions of a run (times
//! are seconds after warm-up). Soaks (S1, S2 and the active-set points)
//! carry repeated disruptions inside the run (design 5.3): a pipeline
//! restart every two hours, one sink outage and one endpoint outage. An
//! action whose hook or proxy is not configured is recorded as not run, so
//! a result never looks as if it had been exercised.

use anyhow::{Result, bail};
use serde::Serialize;

use crate::config::{ActiveSetPattern, RunConfig};

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Action {
    /// Multiply the change rate of the first `servers` servers.
    SetRate { servers: usize, multiplier: f64 },
    /// Run the configured migration on every server of the run.
    Migration,
    /// Run onboarding and offboarding at the configured rates.
    Lifecycle,
    /// Disconnect every MySQL endpoint (Toxiproxy, or hooks
    /// `endpoint_down` / `endpoint_up`) for `secs`.
    EndpointOutage { secs: u64 },
    /// Hooks `sink_outage_start` / `sink_outage_stop`, `secs` apart.
    SinkOutage { secs: u64 },
    /// Hook `store_restart`.
    StoreRestart,
    /// Hook `failover` for the first server.
    Failover,
    /// Hook `rotate_credentials` for every server.
    RotateCredentials,
    /// Hook `lineage_barrier` for the first server, then the first change of
    /// many tables is timed.
    LineageBarrier,
    /// Stop and resume every pipeline through the REST API.
    RestartPipelines,
    /// Hooks `process_stop` (clean shutdown, exit timed through the PID) and
    /// `process_start`.
    ProcessRestart,
    /// `cycles` rounds of stop, resume and patch across the pipelines.
    LifecycleCycles { cycles: u32 },
    /// Scrape the metrics endpoint every `interval_secs` for the rest of the
    /// run.
    ScrapeLoad { interval_secs: u64 },
}

impl Action {
    /// The hooks the action needs (`None` for built-ins).
    pub fn hooks(&self) -> &'static [&'static str] {
        match self {
            Action::SinkOutage { .. } => {
                &["sink_outage_start", "sink_outage_stop"]
            }
            Action::StoreRestart => &["store_restart"],
            Action::Failover => &["failover"],
            Action::RotateCredentials => &["rotate_credentials"],
            Action::LineageBarrier => &["lineage_barrier"],
            Action::ProcessRestart => &["process_stop", "process_start"],
            _ => &[],
        }
    }

    /// Whether recovery is measured after the action.
    pub fn disrupts(&self) -> bool {
        !matches!(
            self,
            Action::SetRate { .. }
                | Action::Migration
                | Action::Lifecycle
                | Action::ScrapeLoad { .. }
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Step {
    pub at_secs: u64,
    pub action: Action,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Plan {
    pub scenario: String,
    pub active_set: Option<ActiveSetPattern>,
    pub steps: Vec<Step>,
}

/// Disruptions repeated inside a soak of `duration` seconds.
fn soak_disruptions(cfg: &RunConfig, duration: u64) -> Vec<Step> {
    let mut steps: Vec<Step> = (1..)
        .map(|i| i * 7_200)
        .take_while(|&t| t < duration)
        .map(|at_secs| Step {
            at_secs,
            action: Action::RestartPipelines,
        })
        .collect();
    steps.push(Step {
        at_secs: duration / 3,
        action: Action::SinkOutage {
            secs: cfg.disruptions.sink_outage_secs,
        },
    });
    steps.push(Step {
        at_secs: 2 * duration / 3,
        action: Action::EndpointOutage {
            secs: cfg.disruptions.endpoint_outage_secs,
        },
    });
    steps.sort_by_key(|s| s.at_secs);
    steps
}

pub fn plan(cfg: &RunConfig) -> Result<Plan> {
    let d = cfg.run.duration_secs;
    let id = cfg.run.scenario.as_str();
    let at = |at_secs: u64, action: Action| Step { at_secs, action };
    let (active_set, steps) = match id {
        "S1" | "S2" => (None, soak_disruptions(cfg, d)),
        "S3" => (None, vec![at(0, Action::Migration)]),
        "S4" => (None, vec![at(0, Action::Lifecycle)]),
        "S5" => (None, vec![at(d / 2, Action::RestartPipelines)]),
        "S6" => bail!(
            "S6 (full-cluster snapshot) belongs to the snapshot profile, which \
             is not in scope: this milestone is CDC-qualified only"
        ),
        "S7" => (None, vec![at(d / 2, Action::Failover)]),
        "S8" => (None, vec![at(d / 3, Action::LineageBarrier)]),
        "S9" => (
            None,
            vec![at(
                d / 3,
                Action::SinkOutage {
                    secs: cfg.disruptions.sink_outage_secs,
                },
            )],
        ),
        "S10" => (None, vec![at(d / 2, Action::StoreRestart)]),
        "S11" => (
            None,
            vec![at(
                d / 3,
                Action::EndpointOutage {
                    secs: cfg.disruptions.endpoint_outage_secs,
                },
            )],
        ),
        "S12" => (None, vec![at(d / 2, Action::RotateCredentials)]),
        "S13" => (
            None,
            vec![at(
                0,
                Action::SetRate {
                    servers: 1,
                    multiplier: 10.0,
                },
            )],
        ),
        "S14" => (
            None,
            vec![at(
                0,
                Action::SetRate {
                    servers: 5,
                    multiplier: 5.0,
                },
            )],
        ),
        "S15" => (None, vec![at(d / 2, Action::ProcessRestart)]),
        "S16" => (None, vec![at(0, Action::LifecycleCycles { cycles: 100 })]),
        "S17" => (
            None,
            vec![
                at(0, Action::ScrapeLoad { interval_secs: 5 }),
                at(
                    d / 4,
                    Action::EndpointOutage {
                        secs: cfg.disruptions.endpoint_outage_secs,
                    },
                ),
                at(3 * d / 4, Action::ProcessRestart),
            ],
        ),
        "A1" | "A2" | "A3" | "A4" | "A5" => {
            let pattern = match id {
                "A1" => ActiveSetPattern::Fixed { fraction: 0.0001 },
                "A2" => ActiveSetPattern::Fixed { fraction: 0.001 },
                "A3" => ActiveSetPattern::Fixed { fraction: 0.01 },
                "A4" => ActiveSetPattern::Fixed { fraction: 0.1 },
                _ => ActiveSetPattern::Moving {
                    fraction: 0.01,
                    period_secs: 1_800,
                },
            };
            (Some(pattern), soak_disruptions(cfg, d))
        }
        other => bail!("unknown scenario {other} (S1-S17 or A1-A5)"),
    };
    Ok(Plan {
        scenario: id.to_string(),
        active_set,
        steps,
    })
}

/// Why `action` cannot run with this configuration (`None` if it can).
pub fn unavailable(cfg: &RunConfig, action: &Action) -> Option<String> {
    if let Action::EndpointOutage { .. } = action
        && cfg.disruptions.toxiproxy_url.is_none()
        && !(cfg.actions.contains_key("endpoint_down")
            && cfg.actions.contains_key("endpoint_up"))
    {
        return Some(
            "no disruptions.toxiproxy_url and no endpoint_down/endpoint_up hooks".into(),
        );
    }
    let missing: Vec<&str> = action
        .hooks()
        .iter()
        .copied()
        .filter(|h| !cfg.actions.contains_key(*h))
        .collect();
    (!missing.is_empty())
        .then(|| format!("hooks not configured: {}", missing.join(", ")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::tests::EXAMPLE;

    fn cfg(scenario: &str, duration: u64) -> RunConfig {
        let mut c: RunConfig = serde_yaml::from_str(EXAMPLE).unwrap();
        c.run.scenario = scenario.into();
        c.run.duration_secs = duration;
        c
    }

    #[test]
    fn soaks_carry_repeated_disruptions() {
        let p = plan(&cfg("S1", 6 * 3_600)).unwrap();
        let restarts = p
            .steps
            .iter()
            .filter(|s| s.action == Action::RestartPipelines)
            .count();
        assert_eq!(restarts, 2, "every two hours inside six");
        assert!(
            p.steps
                .iter()
                .any(|s| matches!(s.action, Action::SinkOutage { .. }))
        );
        assert!(
            p.steps
                .iter()
                .any(|s| matches!(s.action, Action::EndpointOutage { .. }))
        );
        assert!(p.steps.windows(2).all(|w| w[0].at_secs <= w[1].at_secs));
    }

    #[test]
    fn the_active_set_matrix_and_the_snapshot_exclusion() {
        let a5 = plan(&cfg("A5", 43_200)).unwrap();
        assert_eq!(
            a5.active_set,
            Some(ActiveSetPattern::Moving {
                fraction: 0.01,
                period_secs: 1_800
            })
        );
        assert_eq!(
            plan(&cfg("A1", 3_600)).unwrap().active_set,
            Some(ActiveSetPattern::Fixed { fraction: 0.0001 })
        );
        let err = plan(&cfg("S6", 60)).unwrap_err().to_string();
        assert!(err.contains("CDC-qualified only"));
        assert!(plan(&cfg("S99", 60)).is_err());
    }

    #[test]
    fn unconfigured_hooks_make_an_action_unavailable() {
        let mut c = cfg("S15", 60);
        let why = unavailable(&c, &Action::ProcessRestart).unwrap();
        assert!(why.contains("process_stop") && why.contains("process_start"));
        c.actions.insert("process_stop".into(), "true".into());
        c.actions.insert("process_start".into(), "true".into());
        assert_eq!(unavailable(&c, &Action::ProcessRestart), None);
        assert!(unavailable(&c, &Action::EndpointOutage { secs: 1 }).is_some());
        c.disruptions.toxiproxy_url = Some("http://t:8474".into());
        assert_eq!(unavailable(&c, &Action::EndpointOutage { secs: 1 }), None);
        assert_eq!(unavailable(&c, &Action::RestartPipelines), None);
    }
}

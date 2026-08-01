//! Pre-flight check: before running a scenario, verify that the Docker
//! Compose profiles it depends on are actually up. Without this, scenarios
//! die with cryptic "connection refused" errors that send new users on a
//! debugging detour.
//!
//! The check is best-effort — it parses `docker ps --format '{{.Labels}}'`
//! and looks for `com.docker.compose.service` labels matching the services
//! we expect for each declared profile. If `docker` isn't available the
//! check warns and continues (don't block scenario execution on a missing
//! Docker binary in CI).

use std::collections::HashSet;
use std::process::Command;

use crate::scenarios::meta::ScenarioMeta;

/// Map a Docker Compose profile to the services it brings up.
///
/// Sourced from `docker-compose.chaos.yml`. When a service joins or leaves
/// a profile, update this table. (We can't introspect compose at runtime
/// without parsing the yaml, which adds a dep + brittleness.)
fn services_for_profile(profile: &str) -> &'static [&'static str] {
    match profile {
        "base" => &["toxiproxy", "prometheus", "grafana", "loki", "promtail"],
        "mysql-infra" => &["mysql", "mysql-b"],
        "pg-infra" => &["postgres"],
        "kafka-infra" => &["kafka", "schema-registry"],
        "s3-infra" => &["minio"],
        "df" => &[
            "deltaforge-release",
            "deltaforge-debug",
            "deltaforge-profile",
        ],
        _ => &[],
    }
}

/// Inspect Docker for running services. Returns a set of service names that
/// are currently `running`. Returns `None` if we couldn't talk to docker
/// (binary missing, daemon not running, permission issue) — the caller
/// should treat `None` as "skip the check" rather than failure.
pub fn running_services() -> Option<HashSet<String>> {
    let output = Command::new("docker")
        .args([
            "ps",
            "--filter",
            "status=running",
            "--format",
            "{{.Label \"com.docker.compose.service\"}}",
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let out = String::from_utf8_lossy(&output.stdout);
    Some(
        out.lines()
            .filter(|l| !l.is_empty())
            .map(|s| s.to_string())
            .collect(),
    )
}

/// Run the pre-flight check for a scenario. Returns the list of missing
/// services keyed by their profile, or `Ok(())` if everything is up.
///
/// If Docker is unreachable, returns `Ok(())` with a warning printed (we
/// don't want to block scenario execution in CI environments where Docker
/// might be replaced by an equivalent).
pub fn check_or_advise(meta: &ScenarioMeta) -> Result<(), PreflightError> {
    let Some(running) = running_services() else {
        eprintln!(
            "⚠  pre-flight: docker not reachable — skipping profile check. \
             If the scenario fails with connection errors, verify your Docker \
             Compose stack is up."
        );
        return Ok(());
    };
    evaluate(meta, &running)
}

/// Pure decision logic for the pre-flight check: given the set of running
/// service names, return the profiles whose services are all absent. Split
/// out of [`check_or_advise`] (which owns the `docker ps` I/O) so the rule
/// is unit-testable without a live Docker daemon.
fn evaluate(
    meta: &ScenarioMeta,
    running: &HashSet<String>,
) -> Result<(), PreflightError> {
    let mut missing: Vec<(&'static str, Vec<&'static str>)> = Vec::new();
    for profile in meta.required_profiles {
        let needed = services_for_profile(profile);
        if needed.is_empty() {
            continue; // unknown profile — skip
        }
        // We consider a profile "up" if at least ONE of its services is
        // running. Stricter "all services" check tends to false-fail on
        // optional services (mc-bootstrap is one-shot, mysql-b is a
        // secondary, etc.).
        let any_running = needed.iter().any(|s| running.contains(*s));
        if !any_running {
            missing.push((profile, needed.to_vec()));
        }
    }
    if missing.is_empty() {
        return Ok(());
    }
    Err(PreflightError { missing })
}

#[derive(Debug)]
pub struct PreflightError {
    /// (profile_name, services_expected_for_that_profile)
    pub missing: Vec<(&'static str, Vec<&'static str>)>,
}

impl PreflightError {
    /// Render a multi-line, actionable error message.
    pub fn render(&self, meta: &ScenarioMeta) -> String {
        let mut s = format!(
            "pre-flight check failed for scenario '{}': required Docker \
             Compose profiles are not running\n",
            meta.name
        );
        for (profile, services) in &self.missing {
            s.push_str(&format!(
                "  • profile '{}' (expected services: {})\n",
                profile,
                services.join(", ")
            ));
        }
        let profiles: Vec<&str> =
            self.missing.iter().map(|(p, _)| *p).collect();
        s.push_str(&format!(
            "\nbring them up with:\n  docker compose -f docker-compose.chaos.yml \\\n    --profile {} up -d\n",
            profiles.join(" --profile ")
        ));
        s
    }
}

impl std::fmt::Display for PreflightError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "missing {} profile(s)", self.missing.len())
    }
}

impl std::error::Error for PreflightError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scenarios::meta::{ScenarioCategory, ScenarioMeta};

    fn fake_meta(profiles: &'static [&'static str]) -> ScenarioMeta {
        ScenarioMeta {
            name: "fake",
            description: "test",
            expected: "test",
            category: ScenarioCategory::Generic,
            required_profiles: profiles,
            tags: &[],
        }
    }

    #[test]
    fn render_message_includes_compose_command() {
        let err = PreflightError {
            missing: vec![
                ("base", vec!["toxiproxy"]),
                ("s3-infra", vec!["minio"]),
            ],
        };
        let msg = err.render(&fake_meta(&["base", "s3-infra"]));
        assert!(msg.contains("scenario 'fake'"));
        assert!(msg.contains("'base'"));
        assert!(msg.contains("'s3-infra'"));
        assert!(msg.contains("--profile base --profile s3-infra"));
    }

    #[test]
    fn services_for_known_profiles() {
        assert!(!services_for_profile("base").is_empty());
        assert!(services_for_profile("s3-infra").contains(&"minio"));
        assert!(services_for_profile("kafka-infra").contains(&"kafka"));
        // Assert every profile arm resolves to its own distinct services so a
        // deleted/merged match arm is caught.
        assert_eq!(services_for_profile("mysql-infra"), &["mysql", "mysql-b"]);
        assert_eq!(services_for_profile("pg-infra"), &["postgres"]);
        assert_eq!(
            services_for_profile("df"),
            &[
                "deltaforge-release",
                "deltaforge-debug",
                "deltaforge-profile"
            ]
        );
        assert_eq!(services_for_profile("nonexistent").len(), 0);
    }

    fn running_set(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn evaluate_passes_when_a_required_service_is_running() {
        let meta = fake_meta(&["s3-infra"]);
        assert!(evaluate(&meta, &running_set(&["minio"])).is_ok());
    }

    #[test]
    fn evaluate_fails_when_no_service_of_a_profile_is_running() {
        let meta = fake_meta(&["s3-infra"]);
        let err = evaluate(&meta, &running_set(&["mysql"])).unwrap_err();
        assert_eq!(err.missing.len(), 1);
        assert_eq!(err.missing[0].0, "s3-infra");
    }

    #[test]
    fn evaluate_treats_one_running_service_as_profile_up() {
        // mysql-infra expects both mysql + mysql-b, but a single running
        // service must satisfy the "any one" rule (mysql-b is a secondary).
        let meta = fake_meta(&["mysql-infra"]);
        assert!(evaluate(&meta, &running_set(&["mysql"])).is_ok());
    }

    #[test]
    fn evaluate_skips_unknown_profiles() {
        // Unknown profiles map to no services and must not be reported missing.
        let meta = fake_meta(&["totally-unknown"]);
        assert!(evaluate(&meta, &HashSet::new()).is_ok());
    }

    #[test]
    fn evaluate_reports_every_missing_profile() {
        let meta = fake_meta(&["s3-infra", "kafka-infra"]);
        let err = evaluate(&meta, &HashSet::new()).unwrap_err();
        assert_eq!(err.missing.len(), 2);
    }
}

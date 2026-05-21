//! Scenario metadata — single source of truth for the CLI, pre-flight
//! checks, and UI listing.
//!
//! Each entry in `REGISTRY` carries everything a user/operator needs to
//! decide whether and how to run a scenario: what it does, what success
//! looks like, what Docker Compose profiles must be running first, and
//! free-form tags for filtering. Pre-flight checks read `required_profiles`
//! to fail fast with an actionable error instead of letting the scenario
//! die with a cryptic "connection refused".
//!
//! Adding a new scenario means: (1) write the `run()` in
//! `scenarios/your_scenario.rs`, (2) add one entry to `REGISTRY` here,
//! (3) add the `Scenario` enum variant + dispatch in `main.rs`.

/// Category determines source-applicability and how `--scenario all`
/// iterates scenarios.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScenarioCategory {
    /// Generic: applicable to any source backend.
    Generic,
    /// MySQL-specific.
    Mysql,
    /// PostgreSQL-specific.
    Postgres,
    /// Heavy benchmark — not included in `--scenario all` runs.
    Benchmark,
    /// Lakehouse / object-storage focused (S3 sink).
    S3,
    /// Avro / Schema Registry focused.
    Avro,
}

impl ScenarioCategory {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Generic => "Generic",
            Self::Mysql => "MySQL",
            Self::Postgres => "PostgreSQL",
            Self::Benchmark => "Benchmark",
            Self::S3 => "S3 / Lakehouse",
            Self::Avro => "Avro / Schema Registry",
        }
    }
}

/// Self-description of a scenario. All fields are `&'static` so this lives
/// in a `const` and can be referenced anywhere without allocation.
#[derive(Debug, Clone, Copy)]
pub struct ScenarioMeta {
    /// CLI name, e.g. `"sr-outage"`. Must be unique across the registry
    /// and must match the kebab-case form of the `Scenario` enum variant.
    pub name: &'static str,
    /// One-line summary of what the scenario does.
    pub description: &'static str,
    /// One-line summary of the expected outcome / what success looks like.
    pub expected: &'static str,
    /// Source / behavior category for grouping in the CLI list.
    pub category: ScenarioCategory,
    /// Docker Compose profiles that MUST be running before this scenario
    /// can succeed. Pre-flight check uses this to fail-fast with an
    /// actionable error (`docker compose --profile X up -d`).
    pub required_profiles: &'static [&'static str],
    /// Free-form tags for filtering, e.g. `&["s3", "dlq", "backpressure"]`.
    /// Convention: lowercase, no leading `#`.
    pub tags: &'static [&'static str],
}

// =============================================================================
// Registry — all scenarios in one place
// =============================================================================

/// Helper to build a `ScenarioMeta` succinctly inline.
const fn meta(
    name: &'static str,
    description: &'static str,
    expected: &'static str,
    category: ScenarioCategory,
    required_profiles: &'static [&'static str],
    tags: &'static [&'static str],
) -> ScenarioMeta {
    ScenarioMeta {
        name,
        description,
        expected,
        category,
        required_profiles,
        tags,
    }
}

use ScenarioCategory::*;

/// Source-agnostic profile bundles. A scenario typically needs `base`
/// (toxiproxy + observability) + a source-infra profile + `df` (DeltaForge
/// instances). S3 scenarios add `s3-infra` for MinIO + the toxiproxy route.
///
/// These are reused by `REGISTRY` entries below; allow(dead_code) tolerates
/// the ones not yet referenced (kept for ready-to-use when new scenarios
/// land).
#[allow(dead_code)]
const BASE_DF: &[&str] = &["base", "df"];
const MYSQL_BASE_DF: &[&str] = &["base", "mysql-infra", "df"];
#[allow(dead_code)]
const PG_BASE_DF: &[&str] = &["base", "pg-infra", "df"];
const MYSQL_KAFKA: &[&str] = &["base", "mysql-infra", "kafka-infra", "df"];
const PG_KAFKA: &[&str] = &["base", "pg-infra", "kafka-infra", "df"];
const MYSQL_S3: &[&str] = &["base", "mysql-infra", "s3-infra", "df"];
#[allow(dead_code)]
const PG_S3: &[&str] = &["base", "pg-infra", "s3-infra", "df"];

pub const REGISTRY: &[ScenarioMeta] = &[
    // ── Generic (source-agnostic; the harness picks the source via --source) ──
    meta(
        "network-partition",
        "Cuts source DB proxy mid-stream then restores it.",
        "Pipeline reconnects and resumes without data loss.",
        Generic,
        MYSQL_BASE_DF,
        &["resilience", "source"],
    ),
    meta(
        "sink-outage",
        "Cuts sink proxy mid-stream then restores it.",
        "Pipeline backpressures, recovers, no data loss.",
        Generic,
        MYSQL_KAFKA,
        &["resilience", "sink"],
    ),
    meta(
        "crash-recovery",
        "Kills DeltaForge mid-batch and restarts.",
        "Source replays from last committed checkpoint; no data loss.",
        Generic,
        MYSQL_KAFKA,
        &["resilience", "checkpoint"],
    ),
    meta(
        "schema-drift",
        "Adds a column to the source table during streaming.",
        "Schema reload happens; downstream events use the new schema.",
        Generic,
        MYSQL_KAFKA,
        &["schema", "drift"],
    ),
    meta(
        "exactly-once",
        "Crash-recovery test with Kafka `exactly_once: true`.",
        "Consumer in read_committed mode sees each event exactly once.",
        Generic,
        MYSQL_KAFKA,
        &["kafka", "exactly-once"],
    ),
    meta(
        "dlq-poison",
        "Injects poison events that fail routing/serialization.",
        "Failed events land in DLQ; pipeline keeps progressing.",
        Generic,
        MYSQL_KAFKA,
        &["dlq", "poison"],
    ),
    // ── S3 / Lakehouse ──
    meta(
        "s3-soak",
        "Sustained writes to S3 (Parquet) — throughput baseline + memory \
         stability over time. Exercises file rolling, partitioning, multipart \
         upload, and per-row DLQ.",
        "All written rows present in the bucket; memory does not grow \
         unboundedly; reported throughput is stable.",
        S3,
        MYSQL_S3,
        &["s3", "parquet", "soak", "throughput"],
    ),
    meta(
        "s3-outage",
        "Cuts MinIO via toxiproxy mid-stream then restores it.",
        "Required S3 sink backpressures the source; no partial files \
         visible; pipeline catches up cleanly after MinIO returns.",
        S3,
        MYSQL_S3,
        &["s3", "resilience", "backpressure"],
    ),
    // ── Avro / Schema Registry ──
    meta(
        "sr-outage",
        "Cuts Schema Registry proxy while Avro pipeline is running.",
        "Events continue flowing via cached schema; no data loss.",
        Avro,
        MYSQL_KAFKA,
        &["avro", "schema-registry", "resilience"],
    ),
    // ── MySQL-specific ──
    meta(
        "failover",
        "MySQL primary failover during streaming.",
        "Pipeline detects failover, reconnects to the new primary, no loss.",
        Mysql,
        MYSQL_KAFKA,
        &["mysql", "failover", "ha"],
    ),
    meta(
        "binlog-purge",
        "Purges MySQL binlogs while DeltaForge is behind.",
        "Pipeline emits a clear binlog-purged error; operator must reseed.",
        Mysql,
        MYSQL_KAFKA,
        &["mysql", "data-loss"],
    ),
    // ── PostgreSQL-specific ──
    meta(
        "pg-failover",
        "PostgreSQL primary failover during streaming.",
        "Pipeline detects failover, reconnects, no data loss.",
        Postgres,
        PG_KAFKA,
        &["postgres", "failover", "ha"],
    ),
    meta(
        "slot-dropped",
        "Drops the replication slot while DeltaForge is connected.",
        "Pipeline emits a clear slot-dropped error; operator must reseed.",
        Postgres,
        PG_KAFKA,
        &["postgres", "data-loss"],
    ),
    // ── Benchmarks (not in --scenario all) ──
    meta(
        "soak",
        "Long-running endurance with random fault injection.",
        "Pipeline survives N hours with the configured fault frequency; \
         no unexpected data loss.",
        Benchmark,
        MYSQL_KAFKA,
        &["benchmark", "soak"],
    ),
    meta(
        "backlog-drain",
        "Drains a 1M-row backlog and measures catch-up throughput.",
        "All 1M rows delivered; reports avg/p50/peak events/s.",
        Benchmark,
        MYSQL_KAFKA,
        &["benchmark", "throughput"],
    ),
    meta(
        "tpcc",
        "TPC-C benchmark (OLTP workload).",
        "Pipeline keeps up; reports per-operation throughput.",
        Benchmark,
        MYSQL_KAFKA,
        &["benchmark", "tpc"],
    ),
    meta(
        "tpc-di",
        "TPC-DI (Data Integration) benchmark requirements check.",
        "Prints requirements; not yet implemented end-to-end.",
        Benchmark,
        MYSQL_KAFKA,
        &["benchmark", "tpc", "stub"],
    ),
    meta(
        "tpc-e",
        "TPC-E (financial OLTP) benchmark requirements check.",
        "Prints requirements; not yet implemented end-to-end.",
        Benchmark,
        MYSQL_KAFKA,
        &["benchmark", "tpc", "stub"],
    ),
];

// =============================================================================
// Query helpers
// =============================================================================

/// Find a scenario by CLI name.
pub fn lookup(name: &str) -> Option<&'static ScenarioMeta> {
    REGISTRY.iter().find(|m| m.name == name)
}

/// Group scenarios by category in a stable display order.
pub fn grouped() -> Vec<(ScenarioCategory, Vec<&'static ScenarioMeta>)> {
    let order = [Generic, S3, Avro, Mysql, Postgres, Benchmark];
    order
        .iter()
        .map(|&cat| {
            let metas: Vec<_> =
                REGISTRY.iter().filter(|m| m.category == cat).collect();
            (cat, metas)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn registry_names_are_unique() {
        let mut seen = HashSet::new();
        for m in REGISTRY {
            assert!(seen.insert(m.name), "duplicate scenario name: {}", m.name);
        }
    }

    #[test]
    fn registry_names_are_kebab_case() {
        for m in REGISTRY {
            assert!(
                m.name.chars().all(|c| c.is_ascii_lowercase()
                    || c.is_ascii_digit()
                    || c == '-'),
                "scenario name {} must be kebab-case (lowercase + dashes)",
                m.name
            );
        }
    }

    #[test]
    fn lookup_finds_known_scenario() {
        assert!(lookup("sr-outage").is_some());
        assert!(lookup("s3-soak").is_some());
        assert!(lookup("does-not-exist").is_none());
    }

    #[test]
    fn s3_scenarios_require_s3_infra_profile() {
        for m in REGISTRY.iter().filter(|m| m.category == S3) {
            assert!(
                m.required_profiles.contains(&"s3-infra"),
                "S3 scenario {} should list 's3-infra' as required",
                m.name
            );
        }
    }

    #[test]
    fn grouped_returns_all_scenarios() {
        let total: usize = grouped().into_iter().map(|(_, m)| m.len()).sum();
        assert_eq!(total, REGISTRY.len());
    }
}

//! Deployment preflight: validate pipeline configs against their live source
//! before deploying, and fail closed with actionable errors.
//!
//! Reuses the existing source preflight checks (`mysql_health::run_preflight`,
//! `postgres_health::run_preflight`) plus local config validation
//! (`validate_commit_policy`) and an explicit PostgreSQL `wal_level` check. It
//! resolves secrets and the source DSN the same way startup does, so a missing
//! secret or an unreachable source is reported here rather than at run time.
//!
//! Sink reachability is not yet probed (the sink trait has no health primitive);
//! that is a documented follow-up.

use std::fmt::Write as _;

use anyhow::Result;
use deltaforge_config::{PipelineSpec, SourceCfg, load_cfg};
use serde::Serialize;
use sources::mysql::mysql_health;
use sources::postgres::postgres_health;

use crate::coordinator::validate_commit_policy;

/// Result of preflighting one pipeline.
#[derive(Debug, Serialize)]
pub struct PipelineCheck {
    pub pipeline: String,
    pub source: String,
    pub hard_errors: Vec<String>,
    pub warnings: Vec<String>,
}

impl PipelineCheck {
    pub fn ok(&self) -> bool {
        self.hard_errors.is_empty()
    }
}

/// Aggregate preflight report across all pipelines in a config.
#[derive(Debug, Serialize)]
pub struct PreflightReport {
    pub ok: bool,
    pub pipelines: Vec<PipelineCheck>,
}

impl PreflightReport {
    pub fn from_checks(pipelines: Vec<PipelineCheck>) -> Self {
        let ok = pipelines.iter().all(PipelineCheck::ok);
        Self { ok, pipelines }
    }

    /// Human-readable report.
    pub fn to_text(&self) -> String {
        let mut out = String::new();
        for p in &self.pipelines {
            let status = if p.ok() { "OK    " } else { "FAILED" };
            let _ = writeln!(out, "[{status}] {} ({})", p.pipeline, p.source);
            for e in &p.hard_errors {
                let _ = writeln!(out, "         ERROR: {e}");
            }
            for w in &p.warnings {
                let _ = writeln!(out, "         warn:  {w}");
            }
        }
        let _ = writeln!(
            out,
            "\npreflight: {}",
            if self.ok { "PASS" } else { "FAIL" }
        );
        out
    }
}

/// Split configured table patterns into concrete `(schema/db, table)` pairs and
/// the wildcard patterns that cannot be validated per-table. The server-level
/// checks run regardless; only per-table size/engine checks need concrete names.
fn parse_concrete_tables(
    patterns: &[String],
) -> (Vec<(String, String)>, Vec<String>) {
    let mut concrete = Vec::new();
    let mut skipped = Vec::new();
    for p in patterns {
        match p.split_once('.') {
            Some((s, t))
                if !s.is_empty()
                    && !t.is_empty()
                    && !s.contains('*')
                    && !t.contains('*') =>
            {
                concrete.push((s.to_string(), t.to_string()));
            }
            _ => skipped.push(p.clone()),
        }
    }
    (concrete, skipped)
}

/// Preflight every pipeline in `specs`. Never panics: each pipeline's failures
/// are collected into its `PipelineCheck`.
pub async fn check_all(specs: &[PipelineSpec]) -> PreflightReport {
    let mut checks = Vec::with_capacity(specs.len());
    for spec in specs {
        checks.push(check_pipeline(spec).await);
    }
    PreflightReport::from_checks(checks)
}

async fn check_pipeline(spec: &PipelineSpec) -> PipelineCheck {
    let mut hard = Vec::new();
    let mut warn = Vec::new();

    let source_label = match &spec.spec.source {
        SourceCfg::Postgres(c) => format!("postgres:{}", c.id),
        SourceCfg::Mysql(c) => format!("mysql:{}", c.id),
    };

    // Local, pure checks first (no connection needed).
    if let Err(e) =
        validate_commit_policy(&spec.spec.commit_policy, spec.spec.sinks.len())
    {
        hard.push(format!("commit policy: {e}"));
    }
    if spec.spec.sinks.is_empty() {
        hard.push("pipeline has no sinks configured".to_string());
    }

    // Resolve secrets + source DSN exactly as startup does, failing closed.
    match sources::source_secret_resolver(spec).await {
        Ok(resolver) => {
            match sources::resolve_source_dsn(spec, resolver.as_ref()).await {
                Ok(dsn) => {
                    check_source_live(spec, dsn.expose(), &mut hard, &mut warn)
                        .await
                }
                Err(e) => {
                    hard.push(format!("resolve source credentials: {e:#}"))
                }
            }
        }
        Err(e) => hard.push(format!("resolve secret providers: {e:#}")),
    }

    PipelineCheck {
        pipeline: spec.metadata.name.clone(),
        source: source_label,
        hard_errors: hard,
        warnings: warn,
    }
}

async fn check_source_live(
    spec: &PipelineSpec,
    dsn: &str,
    hard: &mut Vec<String>,
    warn: &mut Vec<String>,
) {
    match &spec.spec.source {
        SourceCfg::Postgres(c) => {
            match postgres_health::fetch_wal_level(dsn).await {
                Ok(level) if level == "logical" => {}
                Ok(level) => hard.push(format!(
                    "wal_level is '{level}', must be 'logical' for logical \
                     replication; set wal_level=logical and restart the server"
                )),
                Err(e) => hard.push(format!("cannot read wal_level: {e:#}")),
            }

            let (tables, skipped) = parse_concrete_tables(&c.tables);
            if !skipped.is_empty() {
                warn.push(format!(
                    "wildcard table patterns are not validated per-table: {}",
                    skipped.join(", ")
                ));
            }
            match postgres_health::run_preflight(
                dsn,
                Some(&c.slot),
                &c.publication,
                &tables,
                c.snapshot.max_parallel_tables,
            )
            .await
            {
                Ok(report) => {
                    hard.extend(report.hard_errors);
                    warn.extend(report.warnings);
                }
                Err(e) => hard.push(format!("postgres preflight: {e:#}")),
            }
        }
        SourceCfg::Mysql(c) => {
            let (tables, skipped) = parse_concrete_tables(&c.tables);
            if !skipped.is_empty() {
                warn.push(format!(
                    "wildcard table patterns are not validated per-table: {}",
                    skipped.join(", ")
                ));
            }
            match mysql_health::run_preflight(
                dsn,
                &tables,
                c.snapshot.max_parallel_tables,
            )
            .await
            {
                Ok(report) => {
                    hard.extend(report.hard_errors);
                    warn.extend(report.warnings);
                }
                Err(e) => hard.push(format!("mysql preflight: {e:#}")),
            }
        }
    }
}

/// Entry point for the `preflight` subcommand. Loads the config, preflights every
/// pipeline, prints a report, and exits non-zero if any hard error was found.
pub async fn run(config_path: &str, json: bool) -> Result<()> {
    let specs = load_cfg(config_path)?;
    if specs.is_empty() {
        anyhow::bail!("no pipeline specs found at '{config_path}'");
    }

    let report = check_all(&specs).await;

    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print!("{}", report.to_text());
    }

    if !report.ok {
        std::process::exit(1);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_concrete_tables_splits_and_skips_wildcards() {
        let (concrete, skipped) = parse_concrete_tables(&[
            "public.orders".to_string(),
            "public.*".to_string(),
            "shop.items".to_string(),
            "noschema".to_string(),
        ]);
        assert_eq!(
            concrete,
            vec![
                ("public".to_string(), "orders".to_string()),
                ("shop".to_string(), "items".to_string()),
            ]
        );
        assert_eq!(
            skipped,
            vec!["public.*".to_string(), "noschema".to_string()]
        );
    }

    #[test]
    fn report_ok_only_when_no_hard_errors() {
        let checks = vec![
            PipelineCheck {
                pipeline: "a".into(),
                source: "postgres:pg".into(),
                hard_errors: vec![],
                warnings: vec!["w".into()],
            },
            PipelineCheck {
                pipeline: "b".into(),
                source: "mysql:my".into(),
                hard_errors: vec!["boom".into()],
                warnings: vec![],
            },
        ];
        let report = PreflightReport::from_checks(checks);
        assert!(!report.ok);

        let text = report.to_text();
        assert!(text.contains("[OK    ] a"));
        assert!(text.contains("[FAILED] b"));
        assert!(text.contains("ERROR: boom"));
        assert!(text.contains("preflight: FAIL"));
    }

    #[test]
    fn report_ok_when_all_clean() {
        let report = PreflightReport::from_checks(vec![PipelineCheck {
            pipeline: "a".into(),
            source: "postgres:pg".into(),
            hard_errors: vec![],
            warnings: vec![],
        }]);
        assert!(report.ok);
        assert!(report.to_text().contains("preflight: PASS"));
    }
}

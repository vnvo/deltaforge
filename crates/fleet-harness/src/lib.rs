//! Single-instance MySQL fleet qualification harness
//! (`docs/design/mysql-fleet-qualification.md`).
//!
//! Tooling only: it builds MySQL fixtures, drives traffic, DDL and lifecycle
//! events, disrupts endpoints and processes, verifies what reaches Kafka, and
//! records what the DeltaForge instance under test achieved. It never decides
//! a capacity: runs are exploratory unless every input is an owner input
//! ([`config::RunConfig::check_class`]).

pub mod actions;
pub mod activeset;
pub mod config;
pub mod consumer;
pub mod deltaforge;
pub mod driver;
pub mod fixture;
pub mod ledger;
pub mod measure;
pub mod results;
pub mod run;
pub mod scenario;
pub mod stats;
pub mod store;
pub mod topology;
pub mod trace;
pub mod verify;

/// The git revision this harness was built from.
pub fn git_revision() -> &'static str {
    env!("FLEET_HARNESS_GIT_REVISION")
}

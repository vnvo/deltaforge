//! Ownership labels for test containers.
//!
//! `scripts/gate.sh` exports the run it is executing
//! (`DELTAFORGE_GATE_RUN`) and the process that owns it
//! (`DELTAFORGE_GATE_OWNER`, `<pid>:<start time>`). Every test container is
//! started through [`GateOwned::gate_owned`], which copies them onto the
//! container as labels; the gate then removes exactly the containers (and
//! their anonymous volumes) of its own run, on success, failure or
//! interruption, and reaps a stale run only once its owner process is gone.
//! Outside a gate the variables are unset and no label is added.

use testcontainers::{ContainerRequest, Image, ImageExt};

/// The label naming the gate run that owns a container.
pub const RUN_LABEL: &str = "deltaforge.gate.run";
/// The label naming that run's owner process (`<pid>:<start time>`).
pub const OWNER_LABEL: &str = "deltaforge.gate.owner";

/// The ownership labels of the current gate run (none outside a gate).
pub fn labels() -> Vec<(&'static str, String)> {
    labels_from(|name| std::env::var(name).ok())
}

fn labels_from(
    env: impl Fn(&str) -> Option<String>,
) -> Vec<(&'static str, String)> {
    let set = |name: &str| env(name).filter(|v| !v.is_empty());
    match (set("DELTAFORGE_GATE_RUN"), set("DELTAFORGE_GATE_OWNER")) {
        (Some(run), Some(owner)) => {
            vec![(RUN_LABEL, run), (OWNER_LABEL, owner)]
        }
        // A run without its owner could never be proven stale: label
        // nothing rather than half.
        _ => Vec::new(),
    }
}

/// Start every test container through this.
pub trait GateOwned<I: Image> {
    /// The request with the current gate run's ownership labels.
    fn gate_owned(self) -> ContainerRequest<I>;
}

impl<I: Image, R: Into<ContainerRequest<I>>> GateOwned<I> for R {
    fn gate_owned(self) -> ContainerRequest<I> {
        self.into().with_labels(labels())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_gate_run_labels_its_run_and_owner() {
        let env = |name: &str| match name {
            "DELTAFORGE_GATE_RUN" => Some("deltaforge-gate-1-2".to_string()),
            "DELTAFORGE_GATE_OWNER" => Some("41:9000".to_string()),
            _ => None,
        };
        assert_eq!(
            labels_from(env),
            vec![
                (RUN_LABEL, "deltaforge-gate-1-2".to_string()),
                (OWNER_LABEL, "41:9000".to_string())
            ]
        );
    }

    #[test]
    fn outside_a_gate_or_half_set_nothing_is_labelled() {
        assert!(labels_from(|_| None).is_empty());
        let run_only = |name: &str| {
            (name == "DELTAFORGE_GATE_RUN")
                .then(|| "deltaforge-gate-1-2".to_string())
        };
        assert!(labels_from(run_only).is_empty());
        let empty = |_: &str| Some(String::new());
        assert!(labels_from(empty).is_empty());
    }
}

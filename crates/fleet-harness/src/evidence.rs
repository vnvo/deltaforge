//! Evidence identity: no run, sweep or measurement ever overwrites another's
//! evidence, and no run reuses another's row namespace.
//!
//! - Ids carry a nanosecond timestamp and a random suffix, so runs started
//!   in the same second (or concurrently) never share one.
//! - Evidence directories and files are created atomically and never
//!   reused: an existing one is an error, not a destination.
//! - A run's row namespace (`run_tag`, the high bits of every row id it
//!   writes) is claimed atomically in the output directory, so two runs
//!   sharing an output directory never share a namespace, whenever and from
//!   whichever process they start.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};

use crate::config::RunClass;
use crate::results::RunResult;

/// Bits of a row id that hold its run's namespace (`run_tag << 40`).
pub const RUN_TAG_BITS: u32 = 23;
const RUN_TAG_MASK: u64 = (1 << RUN_TAG_BITS) - 1;

/// A nanosecond timestamp and a random suffix.
pub fn unique_stem(at: DateTime<Utc>) -> String {
    format!(
        "{}-{:08x}",
        at.format("%Y%m%dT%H%M%S%.9fZ"),
        rand::random::<u32>()
    )
}

/// Create `path` as a new directory (its parents as needed). An existing
/// `path` is an error: evidence is never written into a directory another
/// run, sweep or measurement may own.
pub fn create_new_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create {}", parent.display()))?;
    }
    match std::fs::create_dir(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == ErrorKind::AlreadyExists => bail!(
            "evidence directory {} already exists (never reused)",
            path.display()
        ),
        Err(e) => Err(e).with_context(|| format!("create {}", path.display())),
    }
}

/// Write `bytes` to a new file at `path`; an existing file is an error.
pub fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write as _;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| {
            format!("create {} (never overwritten)", path.display())
        })?;
    f.write_all(bytes)?;
    Ok(())
}

/// A new run's identity.
#[derive(Debug, Clone)]
pub struct RunIdentity {
    pub run_id: String,
    /// The run's result directory, already created and owned by this run.
    pub dir: PathBuf,
    /// The run's row namespace, claimed for this run only.
    pub run_tag: u64,
}

/// Allocate a run: a unique id, its new result directory and its row
/// namespace.
pub fn allocate_run(
    output: &str,
    class: RunClass,
    scenario: &str,
    sources: u32,
    rep: u32,
    at: DateTime<Utc>,
) -> Result<RunIdentity> {
    let run_id = format!("{}-{scenario}-n{sources}-r{rep}", unique_stem(at));
    let dir = RunResult::dir(output, class, &run_id);
    create_new_dir(&dir)?;
    let run_tag = claim_run_tag(output, at)?;
    Ok(RunIdentity {
        run_id,
        dir,
        run_tag,
    })
}

/// Claim a row namespace: the start second's (wrapping every ~97 days), or
/// the next one not yet claimed in `output`. A claim is a marker file
/// created atomically, so concurrent runs never claim the same namespace.
pub fn claim_run_tag(output: &str, at: DateTime<Utc>) -> Result<u64> {
    let claims = Path::new(output).join(".run-tags");
    std::fs::create_dir_all(&claims)
        .with_context(|| format!("create {}", claims.display()))?;
    let first = (at.timestamp() as u64) & RUN_TAG_MASK;
    for i in 0..=RUN_TAG_MASK {
        let tag = (first + i) & RUN_TAG_MASK;
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(claims.join(tag.to_string()))
        {
            Ok(_) => return Ok(tag),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => continue,
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("claim a run tag in {}", claims.display())
                });
            }
        }
    }
    bail!("every run tag in {} is claimed", claims.display())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn allocate(out: &Path, at: DateTime<Utc>) -> RunIdentity {
        allocate_run(
            out.to_str().unwrap(),
            RunClass::Exploratory,
            "S1",
            1,
            1,
            at,
        )
        .unwrap()
    }

    #[test]
    fn runs_starting_in_the_same_second_get_their_own_ids_dirs_and_tags() {
        let out = tempfile::tempdir().unwrap();
        let at = Utc::now();
        let runs: Vec<RunIdentity> =
            (0..50).map(|_| allocate(out.path(), at)).collect();
        let ids: BTreeSet<_> = runs.iter().map(|r| &r.run_id).collect();
        let tags: BTreeSet<_> = runs.iter().map(|r| r.run_tag).collect();
        assert_eq!(ids.len(), 50);
        assert_eq!(tags.len(), 50);
        assert!(runs.iter().all(|r| r.dir.is_dir()));
        assert!(runs.iter().all(|r| r.run_tag <= RUN_TAG_MASK));
    }

    #[test]
    fn concurrent_runs_never_share_an_id_a_dir_or_a_tag() {
        let out = tempfile::tempdir().unwrap();
        let at = Utc::now();
        let runs: Vec<RunIdentity> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..16)
                .map(|_| {
                    s.spawn(|| {
                        (0..8)
                            .map(|_| allocate(out.path(), at))
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            handles
                .into_iter()
                .flat_map(|h| h.join().unwrap())
                .collect()
        });
        let ids: BTreeSet<_> = runs.iter().map(|r| &r.run_id).collect();
        let tags: BTreeSet<_> = runs.iter().map(|r| r.run_tag).collect();
        assert_eq!(ids.len(), 128);
        assert_eq!(tags.len(), 128);
        let dirs = std::fs::read_dir(out.path().join("exploratory"))
            .unwrap()
            .count();
        assert_eq!(dirs, 128);
    }

    #[test]
    fn an_existing_result_directory_is_never_reused() {
        let out = tempfile::tempdir().unwrap();
        let dir = out.path().join("exploratory").join("a-run");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("result.json"), b"earlier evidence").unwrap();
        let err = create_new_dir(&dir).unwrap_err().to_string();
        assert!(err.contains("already exists"), "{err}");
        let err = write_new(&dir.join("result.json"), b"later")
            .unwrap_err()
            .to_string();
        assert!(err.contains("never overwritten"), "{err}");
        assert_eq!(
            std::fs::read(dir.join("result.json")).unwrap(),
            b"earlier evidence"
        );
    }

    #[test]
    fn a_claimed_tag_is_skipped_even_across_the_wrap() {
        let out = tempfile::tempdir().unwrap();
        let o = out.path().to_str().unwrap();
        // The last tag before the wrap, already claimed.
        let at = DateTime::from_timestamp(RUN_TAG_MASK as i64, 0).unwrap();
        assert_eq!(claim_run_tag(o, at).unwrap(), RUN_TAG_MASK);
        assert_eq!(claim_run_tag(o, at).unwrap(), 0);
        assert_eq!(claim_run_tag(o, at).unwrap(), 1);
    }
}

//! File rotation watcher for projected-volume secrets.
//!
//! Kubernetes projected `Secret` volumes publish each key as a symlink into a
//! `..data` directory, and rotate by **atomically swapping the `..data` symlink**
//! to a fresh timestamped directory. The strict file resolver rejects symlinks, so
//! projected Secrets are watched through a [`FileResolver`] in
//! [`FileMode::ProjectedVolume`](crate::FileMode::ProjectedVolume) with an explicit
//! trusted root; every resolved target must still be a regular file inside that
//! root.
//!
//! This watcher **detects** rotation and **re-resolves and validates** the whole
//! credential set atomically. It does **not** reconnect any connector - applying a
//! rotated credential set at a safe connection boundary is a later slice. The
//! validated set is surfaced via [`FileWatcher::latest`] and the `on_rotation`
//! callback.
//!
//! Every watched reference must be **file-backed**; non-file references are rejected
//! at construction. (Environment variables are process-immutable and do not rotate.)
//!
//! # Change detection
//!
//! Detection is by **file identity metadata** (inode/device/len/mtime of each
//! resolved target), not content: a `..data` swap changes the resolved inode, which
//! the watcher observes. Content-hash detection would need a keyed cryptographic
//! hash (deferred with the rotation-fingerprint work), so it is intentionally not
//! used here; the limitation is explicit. Changes are **debounced**: a change must
//! remain stable for the configured window before re-resolution, so a multi-step or
//! racy replacement is not acted on mid-swap.
//!
//! # Atomicity across the resolve
//!
//! On a stable change the entire field set is re-resolved in **one pass**
//! ([`SecretResolver::resolve_set`]). Because projected files are opened
//! sequentially, a `..data` swap during resolution could otherwise mix old and new
//! values, so the watcher fingerprints **again after** resolution and publishes only
//! when the fingerprint before, after, and at debounce all match. If they differ
//! (a swap raced the reads), the candidate is discarded, debounce restarts, and the
//! last validated set is retained. Re-resolution is also all-or-nothing: a missing
//! field, an escape outside the trusted root, or non-UTF-8/oversize/malformed input
//! applies nothing.
//!
//! All filesystem work (the identity fingerprint pass and the resolution) runs on
//! `spawn_blocking` so a slow or unhealthy mount cannot stall the async runtime.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::credential_set::CredentialSet;
use crate::error::{ProviderFailureKind, SecretError};
use crate::providers::{FileMode, FilePolicy, FileResolver};
use crate::reference::{SecretProvider, SecretReference};
use crate::resolver::{CredentialFieldRequest, SecretResolver};

/// A metadata fingerprint of the watched files (keyed by location, deduplicated).
type Fingerprint = BTreeMap<String, crate::providers::FileIdentity>;

/// Outcome of a single watcher [`tick`](FileWatcher::tick).
#[derive(Debug)]
pub enum RotationOutcome {
    /// The watched files are unchanged since the last applied set.
    NoChange,
    /// A change was observed but has not been stable for the debounce window yet,
    /// or a candidate was discarded because the files moved during resolution.
    Debouncing,
    /// A stable change was re-resolved and validated into a new credential set.
    Rotated { generation: u64 },
    /// A change was seen but re-resolution was incomplete (a field is missing -
    /// mid-swap or deleted). The last validated set is retained.
    Incomplete,
    /// Re-resolution failed for a safety reason (escapes the trusted root,
    /// non-UTF-8, oversize, malformed record). The last validated set is retained.
    Rejected(SecretError),
}

/// Watches a set of file-backed credential fields for rotation and re-resolves
/// them atomically.
pub struct FileWatcher {
    resolver: FileResolver,
    fields: Vec<CredentialFieldRequest>,
    debounce: Duration,
    applied: Option<Fingerprint>,
    pending: Option<(Instant, Fingerprint)>,
    generation: u64,
    latest: Option<Arc<CredentialSet>>,
}

impl std::fmt::Debug for FileWatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileWatcher")
            .field("fields", &self.fields.len())
            .field("debounce", &self.debounce)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

impl FileWatcher {
    /// Watch `fields` through the given resolver, debouncing changes by `debounce`.
    /// Every reference must be file-backed; a non-file reference is rejected with
    /// [`SecretError::UnsupportedProvider`] rather than failing on every poll.
    pub fn new(
        resolver: FileResolver,
        fields: Vec<CredentialFieldRequest>,
        debounce: Duration,
    ) -> Result<Self, SecretError> {
        for f in &fields {
            if f.reference.provider != SecretProvider::File {
                return Err(SecretError::UnsupportedProvider(
                    f.reference.safe(),
                ));
            }
        }
        Ok(Self {
            resolver,
            fields,
            debounce,
            applied: None,
            pending: None,
            generation: 0,
            latest: None,
        })
    }

    /// Convenience constructor for a projected-volume watcher rooted at
    /// `trusted_root` (the explicit trusted root for symlink resolution).
    pub fn projected_volume(
        trusted_root: PathBuf,
        max_size: usize,
        fields: Vec<CredentialFieldRequest>,
        debounce: Duration,
    ) -> Result<Self, SecretError> {
        let resolver = FileResolver::new(FilePolicy {
            max_size,
            mode: FileMode::ProjectedVolume { trusted_root },
            trim_trailing_newline: false,
        });
        Self::new(resolver, fields, debounce)
    }

    /// The most recently validated credential set, if any. Shared (`Arc`) because
    /// `CredentialSet` is intentionally not `Clone`.
    pub fn latest(&self) -> Option<Arc<CredentialSet>> {
        self.latest.clone()
    }

    /// The number of validated rotations applied so far (the initial load is 1).
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Snapshot the identity of every watched file, deduplicated by location, in a
    /// **single** `spawn_blocking` task so the whole pass runs off the async
    /// runtime. Any unreadable/absent/escaping target makes the snapshot fail
    /// (treated as an unstable/incomplete state); typed redacted errors are
    /// preserved.
    async fn fingerprint(&self) -> Result<Fingerprint, SecretError> {
        let mut refs: Vec<SecretReference> = Vec::new();
        let mut seen: BTreeSet<String> = BTreeSet::new();
        for f in &self.fields {
            if seen.insert(f.reference.location.clone()) {
                refs.push(f.reference.clone());
            }
        }
        if refs.is_empty() {
            return Ok(Fingerprint::new());
        }
        let safe = refs[0].safe();
        let resolver = self.resolver.clone();
        match tokio::task::spawn_blocking(move || {
            let mut fp = Fingerprint::new();
            for r in &refs {
                fp.insert(r.location.clone(), resolver.identity(r)?);
            }
            Ok::<Fingerprint, SecretError>(fp)
        })
        .await
        {
            Ok(result) => result,
            Err(_join) => Err(SecretError::Provider {
                reference: safe,
                kind: ProviderFailureKind::Unavailable,
            }),
        }
    }

    /// Advance the watcher using `now` as the clock (injected so debounce is
    /// deterministic in tests and driven by [`FileWatcher::run`] in production).
    pub async fn tick(&mut self, now: Instant) -> RotationOutcome {
        // Pre-fingerprint (this poll's observed state).
        let current = match self.fingerprint().await {
            Ok(fp) => fp,
            Err(e) => {
                self.pending = None;
                return classify(e);
            }
        };

        if self.applied.as_ref() == Some(&current) {
            self.pending = None;
            return RotationOutcome::NoChange;
        }

        match &self.pending {
            // Stable: pre == debounced (this guard) and the window has elapsed.
            Some((since, pending_fp)) if pending_fp == &current => {
                if now.duration_since(*since) < self.debounce {
                    return RotationOutcome::Debouncing;
                }
                let candidate =
                    match self.resolver.resolve_set(&self.fields).await {
                        Ok(set) => set,
                        Err(e) => {
                            // Incomplete/unsafe: keep last-good, retry after another
                            // debounce window against the same fingerprint.
                            self.pending = Some((now, current));
                            return classify(e);
                        }
                    };
                // Post-fingerprint: publish only if nothing swapped during the
                // reads (pre == post == debounced). Otherwise discard the candidate.
                let post = match self.fingerprint().await {
                    Ok(fp) => fp,
                    Err(e) => {
                        // A file vanished during/after resolution: keep last-good.
                        self.pending = None;
                        return classify(e);
                    }
                };
                if post == current {
                    self.applied = Some(current);
                    self.pending = None;
                    self.generation += 1;
                    self.latest = Some(Arc::new(candidate));
                    RotationOutcome::Rotated {
                        generation: self.generation,
                    }
                } else {
                    // A swap raced the resolve: discard the (possibly mixed)
                    // candidate, restart debounce toward the newly observed state.
                    self.pending = Some((now, post));
                    RotationOutcome::Debouncing
                }
            }
            _ => {
                // First observation of this changed fingerprint: (re)start debounce.
                self.pending = Some((now, current));
                RotationOutcome::Debouncing
            }
        }
    }

    /// Background driver: poll every `poll_interval` while `running` is set,
    /// invoking `on_rotation` with each newly validated credential set. This does
    /// not reconnect any connector; it only surfaces the validated set.
    pub async fn run(
        &mut self,
        poll_interval: Duration,
        running: Arc<AtomicBool>,
        mut on_rotation: impl FnMut(u64, &CredentialSet),
    ) {
        while running.load(Ordering::Relaxed) {
            tokio::time::sleep(poll_interval).await;
            if let RotationOutcome::Rotated { generation } =
                self.tick(Instant::now()).await
            {
                if let Some(set) = &self.latest {
                    on_rotation(generation, set);
                }
            }
        }
    }
}

/// Classify a resolution/identity error into a transient (`Incomplete`) or unsafe
/// (`Rejected`) outcome for observability. Both keep the last validated set.
fn classify(e: SecretError) -> RotationOutcome {
    match &e {
        SecretError::NotFound(_) | SecretError::Empty(_) => {
            RotationOutcome::Incomplete
        }
        _ => RotationOutcome::Rejected(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use std::os::unix::fs::symlink;
    use std::sync::atomic::AtomicU64;

    use crate::reference::{SecretProvider, SecretReference};

    static UNIQ: AtomicU64 = AtomicU64::new(0);
    const LIMIT: usize = 1 << 20;

    /// A Kubernetes-projected-Secret-like layout under a unique temp root.
    struct Projected {
        root: PathBuf,
        version: u64,
    }

    impl Projected {
        fn new(files: &[(&str, &[u8])]) -> Self {
            let n = UNIQ.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir().join(format!(
                "df-rot-{}-{}",
                std::process::id(),
                n
            ));
            fs::create_dir_all(&root).unwrap();
            let mut p = Projected { root, version: 0 };
            p.publish(files);
            p
        }

        /// Publish a fresh data directory, swap `..data` to it, and re-point the
        /// top-level key symlinks. Mirrors a projected-Secret update.
        fn publish(&mut self, files: &[(&str, &[u8])]) {
            self.version += 1;
            let data_dir = self.root.join(format!("..{:04}", self.version));
            fs::create_dir_all(&data_dir).unwrap();
            for (name, contents) in files {
                let mut f = fs::File::create(data_dir.join(name)).unwrap();
                f.write_all(contents).unwrap();
                f.sync_all().unwrap();
            }
            let data_link = self.root.join("..data");
            let _ = fs::remove_file(&data_link);
            symlink(&data_dir, &data_link).unwrap();
            for (name, _) in files {
                let key_link = self.root.join(name);
                let _ = fs::remove_file(&key_link);
                symlink(self.root.join("..data").join(name), &key_link)
                    .unwrap();
            }
        }

        fn key_ref(&self, name: &str) -> SecretReference {
            SecretReference::new(
                SecretProvider::File,
                self.root.join(name).to_str().unwrap(),
            )
        }
    }

    impl Drop for Projected {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn deb() -> Duration {
        Duration::from_millis(100)
    }

    fn watcher_for(p: &Projected, keys: &[&str]) -> FileWatcher {
        let fields = keys
            .iter()
            .map(|k| CredentialFieldRequest::new(*k, p.key_ref(k)))
            .collect();
        FileWatcher::projected_volume(p.root.clone(), LIMIT, fields, deb())
            .unwrap()
    }

    #[tokio::test]
    async fn non_file_reference_is_rejected_at_construction() {
        let fields = vec![CredentialFieldRequest::new(
            "x",
            SecretReference::new(SecretProvider::Env, "X"),
        )];
        let err = FileWatcher::projected_volume(
            std::env::temp_dir(),
            LIMIT,
            fields,
            deb(),
        )
        .unwrap_err();
        assert!(matches!(err, SecretError::UnsupportedProvider(_)));
    }

    #[tokio::test]
    async fn rotation_is_detected_and_reresolved() {
        let p = Projected::new(&[("username", b"u1"), ("password", b"p1")]);
        let mut w = watcher_for(&p, &["username", "password"]);
        let t0 = Instant::now();

        assert!(matches!(w.tick(t0).await, RotationOutcome::Debouncing));
        assert!(matches!(
            w.tick(t0 + deb()).await,
            RotationOutcome::Rotated { generation: 1 }
        ));
        let set = w.latest().unwrap();
        assert_eq!(
            set.require("password").unwrap().material().as_utf8(),
            Some("p1")
        );
        assert!(matches!(
            w.tick(t0 + deb() * 2).await,
            RotationOutcome::NoChange
        ));

        let mut p = p;
        p.publish(&[("username", b"u2"), ("password", b"p2")]);
        let t1 = t0 + deb() * 3;
        assert!(matches!(w.tick(t1).await, RotationOutcome::Debouncing));
        assert!(matches!(
            w.tick(t1 + deb()).await,
            RotationOutcome::Rotated { generation: 2 }
        ));
        assert_eq!(
            w.latest()
                .unwrap()
                .require("password")
                .unwrap()
                .material()
                .as_utf8(),
            Some("p2")
        );
    }

    #[tokio::test]
    async fn debounce_holds_until_stable() {
        let p = Projected::new(&[("password", b"p1")]);
        let mut w = watcher_for(&p, &["password"]);
        let t0 = Instant::now();
        assert!(matches!(w.tick(t0).await, RotationOutcome::Debouncing));
        assert!(matches!(
            w.tick(t0 + deb() / 2).await,
            RotationOutcome::Debouncing
        ));
        assert!(matches!(
            w.tick(t0 + deb()).await,
            RotationOutcome::Rotated { generation: 1 }
        ));
    }

    #[tokio::test]
    async fn incomplete_multifield_update_is_not_applied() {
        let p = Projected::new(&[("username", b"u1"), ("password", b"p1")]);
        let mut w = watcher_for(&p, &["username", "password"]);
        let t0 = Instant::now();
        w.tick(t0).await;
        w.tick(t0 + deb()).await;
        assert_eq!(w.generation(), 1);

        let mut p = p;
        p.publish(&[("username", b"u2")]); // password symlink now dangles
        let t1 = t0 + deb() * 2;
        assert!(matches!(w.tick(t1).await, RotationOutcome::Incomplete));
        assert_eq!(w.generation(), 1);
        assert_eq!(
            w.latest()
                .unwrap()
                .require("password")
                .unwrap()
                .material()
                .as_utf8(),
            Some("p1")
        );

        p.publish(&[("username", b"u2"), ("password", b"p2")]);
        let t2 = t1 + deb();
        assert!(matches!(w.tick(t2).await, RotationOutcome::Debouncing));
        assert!(matches!(
            w.tick(t2 + deb()).await,
            RotationOutcome::Rotated { generation: 2 }
        ));
    }

    #[tokio::test]
    async fn escape_attempt_is_rejected() {
        let p = Projected::new(&[("password", b"p1")]);
        let mut w = watcher_for(&p, &["password"]);
        let t0 = Instant::now();
        w.tick(t0).await;
        w.tick(t0 + deb()).await;

        let outside = Projected::new(&[("stolen", b"evil")]);
        let escape_target = outside.root.join("..data").join("stolen");
        let key_link = p.root.join("password");
        let _ = fs::remove_file(&key_link);
        symlink(&escape_target, &key_link).unwrap();

        assert!(matches!(
            w.tick(t0 + deb() * 2).await,
            RotationOutcome::Rejected(SecretError::OutsideTrustedRoot(_))
        ));
        assert_eq!(w.generation(), 1);
        assert_eq!(
            w.latest()
                .unwrap()
                .require("password")
                .unwrap()
                .material()
                .as_utf8(),
            Some("p1")
        );
    }

    #[tokio::test]
    async fn deletion_keeps_last_good() {
        let p = Projected::new(&[("password", b"p1")]);
        let mut w = watcher_for(&p, &["password"]);
        let t0 = Instant::now();
        w.tick(t0).await;
        w.tick(t0 + deb()).await;

        let _ = fs::remove_file(p.root.join("password"));
        let _ = fs::remove_file(p.root.join("..data"));

        assert!(matches!(
            w.tick(t0 + deb() * 2).await,
            RotationOutcome::Incomplete
        ));
        assert_eq!(w.generation(), 1);
        assert!(w.latest().is_some());
    }

    #[tokio::test]
    async fn replacement_race_debounces_to_final_value() {
        let p = Projected::new(&[("password", b"p1")]);
        let mut w = watcher_for(&p, &["password"]);
        let t0 = Instant::now();
        w.tick(t0).await;
        w.tick(t0 + deb()).await;

        let mut p = p;
        p.publish(&[("password", b"p2")]);
        let t1 = t0 + deb() * 2;
        assert!(matches!(w.tick(t1).await, RotationOutcome::Debouncing));
        p.publish(&[("password", b"p3")]);
        let t2 = t1 + deb() / 2;
        assert!(matches!(w.tick(t2).await, RotationOutcome::Debouncing));
        assert!(matches!(
            w.tick(t2 + deb()).await,
            RotationOutcome::Rotated { generation: 2 }
        ));
        assert_eq!(
            w.latest()
                .unwrap()
                .require("password")
                .unwrap()
                .material()
                .as_utf8(),
            Some("p3")
        );
    }

    #[tokio::test]
    async fn structured_json_record_rotates_atomically() {
        let p = Projected::new(&[(
            "db.json",
            br#"{"username":"u1","password":"p1"}"#,
        )]);
        let user = p.key_ref("db.json").with_selector("username");
        let pass = p.key_ref("db.json").with_selector("password");
        let mut w = FileWatcher::projected_volume(
            p.root.clone(),
            LIMIT,
            vec![
                CredentialFieldRequest::new("username", user),
                CredentialFieldRequest::new("password", pass),
            ],
            deb(),
        )
        .unwrap();
        let t0 = Instant::now();
        w.tick(t0).await;
        assert!(matches!(
            w.tick(t0 + deb()).await,
            RotationOutcome::Rotated { generation: 1 }
        ));
        let set = w.latest().unwrap();
        assert!(set.single_resolution_group().is_ok());
        assert_eq!(
            set.require("username").unwrap().material().as_utf8(),
            Some("u1")
        );
    }

    // Blocker 1: a swap between the two file reads must not publish a mixed set.
    // The second field's *read* is slowed (identity/fingerprint stays fast); a swap
    // is raced in during that read, so the first field reads the old generation and
    // the second reads the new one. The post-resolve fingerprint detects the swap
    // and discards the mixed candidate, keeping the last-good set.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn swap_between_reads_discards_mixed_candidate() {
        let mut p = Projected::new(&[("a", b"a1"), ("__slowread__b", b"b1")]);
        let mut w = watcher_for(&p, &["a", "__slowread__b"]);
        let t0 = Instant::now();
        w.tick(t0).await;
        w.tick(t0 + deb()).await; // generation 1 (a1,b1)
        assert_eq!(w.generation(), 1);

        // Stage a change so the next tick debounces to it.
        p.publish(&[("a", b"a2"), ("__slowread__b", b"b2")]);
        let t1 = t0 + deb() * 2;
        assert!(matches!(w.tick(t1).await, RotationOutcome::Debouncing));

        // Debounced tick: resolve reads `a` (fast, a2) then `__slowread__b` (slow).
        // A racing swap to a3/b3 lands during the slow read, so the candidate would
        // be {a: a2, b: b3} - mixed. Post-fingerprint must catch it.
        let racy = w.tick(t1 + deb());
        let swap = async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            p.publish(&[("a", b"a3"), ("__slowread__b", b"b3")]);
        };
        let (outcome, ()) = tokio::join!(racy, swap);
        assert!(
            matches!(outcome, RotationOutcome::Debouncing),
            "mixed candidate must be discarded, got {outcome:?}"
        );
        // Last-good (generation 1) retained; the mixed set was never published.
        assert_eq!(w.generation(), 1);
        assert_eq!(
            w.latest()
                .unwrap()
                .require("a")
                .unwrap()
                .material()
                .as_utf8(),
            Some("a1")
        );
    }

    // Blocker 2: the fingerprint pass runs on spawn_blocking, so a slow mount does
    // not stall async tasks.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slow_fingerprint_does_not_block_async_tasks() {
        let p = Projected::new(&[("__slow__pw", b"p1")]);
        let mut w = watcher_for(&p, &["__slow__pw"]);
        let start = Instant::now();
        let timers = async {
            for _ in 0..5 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            start.elapsed()
        };
        let (_outcome, elapsed) = tokio::join!(w.tick(Instant::now()), timers);
        assert!(
            elapsed < Duration::from_millis(400),
            "async tasks were blocked by the slow fingerprint: {elapsed:?}"
        );
    }
}

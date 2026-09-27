//! Mounted-file resolver. Reads a secret from an absolute path, enforcing a size
//! limit both before (metadata) and after (actual bytes) the read, and applying an
//! explicit symlink policy. Bytes are preserved exactly; nothing is silently
//! trimmed. Empty material fails closed.
//!
//! # Blocking I/O boundary
//!
//! A mounted secret can live on slow or unhealthy storage, so the **entire**
//! filesystem operation for one resolution - path/symlink policy, canonicalize,
//! open, metadata checks, the bounded read, and JSON parsing - runs inside a single
//! [`tokio::task::spawn_blocking`] task. Canonicalize/open/read are never split
//! across separate blocking tasks (that would reopen the TOCTOU window). The
//! blocking task owns cloned safe reference/policy data and returns protected
//! material or a typed redacted error.
//!
//! **Cancellation boundary (documented honestly):** if the awaiting future is
//! dropped, we stop waiting for the `spawn_blocking` task, but Tokio cannot cancel
//! a thread already inside a blocking syscall - the underlying read may still run
//! to completion on the blocking pool. Cancellation frees the async caller, not
//! necessarily the filesystem operation.
//!
//! # Symlink / trusted-root threat model
//!
//! Two policies:
//! - [`FileMode::Strict`]: the path must not be a symlink. Suitable for operator
//!   files that should be exactly where configured.
//! - [`FileMode::ProjectedVolume`]: symlinks are followed, but the fully resolved
//!   target must be a regular file **inside a configured trusted root**. This
//!   supports Kubernetes projected Secrets, which publish `key -> ..data/key` and
//!   `..data -> ..<timestamp>/` symlinks and swap `..data` atomically on rotation.
//!   Paths that resolve outside the trusted root are rejected, so a tampered or
//!   attacker-planted symlink cannot redirect a read to `/etc/shadow`.
//!
//! **TOCTOU boundary (documented honestly):** resolution uses `canonicalize`
//! followed by `open`, which the standard filesystem APIs cannot make atomic. A
//! swap between those two steps is a residual window. We narrow the exposure by
//! reading through the opened file handle (so the bytes come from the inode we
//! opened, not a path re-lookup) and by re-checking size/mtime after the read to
//! fail closed on a same-inode modification. Full TOCTOU protection would need
//! `openat2(RESOLVE_BENEATH)` or an fd-relative traversal, which is out of scope
//! for this slice. [`FileIdentity`] records the safe metadata the later rotation
//! watcher uses to detect target replacement.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use async_trait::async_trait;
use zeroize::Zeroize;

use crate::credential_set::CredentialSet;
use crate::error::{
    InconsistencyKind, ProviderFailureKind, ReferenceOption, SecretError,
};
use crate::material::{DEFAULT_MAX_SECRET_BYTES, SecretBytes, SecretString};
use crate::reference::{SecretProvider, SecretReference, SecretRepr};
use crate::resolved::{ResolutionGroup, ResolvedSecret, SecretMaterial};
use crate::resolver::{
    CredentialFieldRequest, SecretResolver, check_no_duplicate_fields,
};

/// Symlink-handling policy for file resolution.
#[derive(Debug, Clone)]
pub enum FileMode {
    /// The referenced path must not itself be a symlink.
    Strict,
    /// Follow symlinks (Kubernetes projected-volume layout), but constrain the
    /// fully resolved target to `trusted_root`.
    ProjectedVolume { trusted_root: PathBuf },
}

/// File-resolution policy.
#[derive(Debug, Clone)]
pub struct FilePolicy {
    /// Maximum accepted size, in bytes, enforced before and after the read.
    pub max_size: usize,
    /// Symlink handling.
    pub mode: FileMode,
    /// When true, a single trailing newline (`\n`, or `\r\n`) is removed from a
    /// **UTF-8 whole-file** value. Off by default: passwords are not silently
    /// trimmed. Never applied to binary reads or to structured JSON fields.
    pub trim_trailing_newline: bool,
}

impl Default for FilePolicy {
    fn default() -> Self {
        Self {
            max_size: DEFAULT_MAX_SECRET_BYTES,
            mode: FileMode::Strict,
            trim_trailing_newline: false,
        }
    }
}

/// Safe, non-secret metadata identifying a file version. The rotation watcher
/// (later slice) compares these to detect that a projected-volume target was
/// atomically replaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileIdentity {
    pub len: u64,
    pub modified: Option<SystemTime>,
    #[cfg(unix)]
    pub inode: u64,
    #[cfg(unix)]
    pub device: u64,
}

impl FileIdentity {
    fn from_metadata(meta: &fs::Metadata) -> Self {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Self {
                len: meta.len(),
                modified: meta.modified().ok(),
                inode: meta.ino(),
                device: meta.dev(),
            }
        }
        #[cfg(not(unix))]
        {
            Self {
                len: meta.len(),
                modified: meta.modified().ok(),
            }
        }
    }
}

/// Owns the parsed string values of a structured record and **zeroizes every
/// remaining value on drop**. Selected values are moved out via [`Self::remove`]
/// (so they are no longer in the map and will not be double-scrubbed); everything
/// left behind is scrubbed when the map goes out of scope, including on an early
/// error return.
struct ZeroizingStringMap(BTreeMap<String, String>);

impl ZeroizingStringMap {
    fn remove(&mut self, key: &str) -> Option<String> {
        self.0.remove(key)
    }

    fn zeroize_remaining(&mut self) {
        for value in self.0.values_mut() {
            value.zeroize();
        }
    }
}

impl Drop for ZeroizingStringMap {
    fn drop(&mut self) {
        self.zeroize_remaining();
    }
}

/// Resolves `SecretProvider::File` references.
pub struct FileResolver {
    policy: FilePolicy,
}

impl FileResolver {
    pub fn new(policy: FilePolicy) -> Self {
        Self { policy }
    }

    /// Safe metadata for the current resolved target (for the rotation watcher).
    pub fn identity(
        &self,
        reference: &SecretReference,
    ) -> Result<FileIdentity, SecretError> {
        let target = resolve_target(&self.policy, reference)?;
        let meta = fs::metadata(&target).map_err(|e| map_io(&e, reference))?;
        if !meta.is_file() {
            return Err(SecretError::NotRegularFile(reference.safe()));
        }
        Ok(FileIdentity::from_metadata(&meta))
    }

    /// Reject reference options this provider does not enforce. File-version
    /// pinning has no defined semantics yet, so a version pin fails closed rather
    /// than being silently ignored.
    fn validate_options(
        reference: &SecretReference,
    ) -> Result<(), SecretError> {
        if reference.version.is_some() {
            return Err(SecretError::UnsupportedReferenceOption {
                reference: reference.safe(),
                option: ReferenceOption::Version,
            });
        }
        Ok(())
    }
}

#[async_trait]
impl SecretResolver for FileResolver {
    async fn resolve(
        &self,
        reference: &SecretReference,
    ) -> Result<ResolvedSecret, SecretError> {
        if reference.provider != SecretProvider::File {
            return Err(SecretError::UnsupportedProvider(reference.safe()));
        }
        Self::validate_options(reference)?;

        // Whole filesystem operation runs on one blocking task (see module docs).
        let policy = self.policy.clone();
        let reference = reference.clone();
        let safe = reference.safe();
        match tokio::task::spawn_blocking(move || {
            resolve_blocking(&policy, &reference)
        })
        .await
        {
            Ok(result) => result,
            // The blocking task panicked or was aborted; report a redacted error.
            Err(_join) => Err(SecretError::Provider {
                reference: safe,
                kind: ProviderFailureKind::Unavailable,
            }),
        }
    }

    async fn resolve_set(
        &self,
        requests: &[CredentialFieldRequest],
    ) -> Result<CredentialSet, SecretError> {
        check_no_duplicate_fields(requests)?;
        for req in requests {
            if req.reference.provider != SecretProvider::File {
                return Err(SecretError::UnsupportedProvider(
                    req.reference.safe(),
                ));
            }
            Self::validate_options(&req.reference)?;
        }
        let Some(first) = requests.first() else {
            return Ok(CredentialSet::new());
        };
        let safe = first.reference.safe();
        let policy = self.policy.clone();
        let requests = requests.to_vec();
        match tokio::task::spawn_blocking(move || {
            resolve_set_blocking(&policy, &requests)
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
}

// --- Synchronous core (runs inside spawn_blocking) ---

fn resolve_blocking(
    policy: &FilePolicy,
    reference: &SecretReference,
) -> Result<ResolvedSecret, SecretError> {
    maybe_test_delay(reference);
    let target = resolve_target(policy, reference)?;
    match reference.selector {
        None => resolve_whole_file(policy, &target, reference),
        Some(_) => {
            let mut map = load_string_map(policy, &target, reference)?;
            let material = select_field(&mut map, policy.max_size, reference)?;
            Ok(ResolvedSecret::new(material))
        }
    }
}

fn resolve_set_blocking(
    policy: &FilePolicy,
    requests: &[CredentialFieldRequest],
) -> Result<CredentialSet, SecretError> {
    let mut cs = CredentialSet::new();

    // Selector requests grouped by path so each JSON record is read and parsed
    // exactly once and its fields share one resolution group. Whole-file (no
    // selector) requests are independent single values.
    let mut by_path: BTreeMap<&str, Vec<&CredentialFieldRequest>> =
        BTreeMap::new();
    let mut singles: Vec<&CredentialFieldRequest> = Vec::new();
    for req in requests {
        if req.reference.selector.is_some() {
            by_path
                .entry(req.reference.location.as_str())
                .or_default()
                .push(req);
        } else {
            singles.push(req);
        }
    }

    for (_path, reqs) in by_path {
        let anchor = reqs[0];
        let target = resolve_target(policy, &anchor.reference)?;
        let mut map = load_string_map(policy, &target, &anchor.reference)?;
        // One read+parse for this record => one provenance group.
        let group = ResolutionGroup::next();
        for req in reqs {
            let material =
                select_field(&mut map, policy.max_size, &req.reference)?;
            let secret = ResolvedSecret::new(material).in_group(group);
            cs.insert(req.field.clone(), secret)?;
        }
        // `map` drops here: any unselected value is zeroized.
    }

    for req in singles {
        let secret = resolve_blocking(policy, &req.reference)?;
        cs.insert(req.field.clone(), secret)?;
    }

    Ok(cs)
}

/// Apply path + symlink policy, returning the concrete path to open.
fn resolve_target(
    policy: &FilePolicy,
    reference: &SecretReference,
) -> Result<PathBuf, SecretError> {
    let path = Path::new(&reference.location);
    if !path.is_absolute() {
        return Err(SecretError::PathNotAbsolute(reference.safe()));
    }
    match &policy.mode {
        FileMode::Strict => {
            let lmeta = fs::symlink_metadata(path)
                .map_err(|e| map_io(&e, reference))?;
            if lmeta.file_type().is_symlink() {
                return Err(SecretError::SymlinkRejected(reference.safe()));
            }
            Ok(path.to_path_buf())
        }
        FileMode::ProjectedVolume { trusted_root } => {
            let canon_root = fs::canonicalize(trusted_root)
                .map_err(|e| map_io(&e, reference))?;
            let canon =
                fs::canonicalize(path).map_err(|e| map_io(&e, reference))?;
            if !canon.starts_with(&canon_root) {
                return Err(SecretError::OutsideTrustedRoot(reference.safe()));
            }
            Ok(canon)
        }
    }
}

/// Read the target's bytes, bounded to `max_size`, failing closed on oversize,
/// non-regular files, or same-inode modification during the read.
fn read_bounded(
    policy: &FilePolicy,
    target: &Path,
    reference: &SecretReference,
) -> Result<Vec<u8>, SecretError> {
    let mut file = File::open(target).map_err(|e| map_io(&e, reference))?;
    let meta1 = file.metadata().map_err(|e| map_io(&e, reference))?;
    if !meta1.is_file() {
        return Err(SecretError::NotRegularFile(reference.safe()));
    }
    let limit = policy.max_size;
    // Early reject on advertised size, but never trust it as the read bound.
    if meta1.len() > limit as u64 {
        return Err(SecretError::SizeExceeded {
            reference: reference.safe(),
            limit,
        });
    }
    // Read at most limit+1 through the opened handle, regardless of metadata.
    let mut buf = Vec::new();
    (&mut file)
        .take(limit as u64 + 1)
        .read_to_end(&mut buf)
        .map_err(|e| map_io(&e, reference))?;
    if buf.len() > limit {
        buf.zeroize();
        return Err(SecretError::SizeExceeded {
            reference: reference.safe(),
            limit,
        });
    }
    // Detect a same-inode change under the read (truncation/rewrite).
    let meta2 = file.metadata().map_err(|e| map_io(&e, reference))?;
    if meta1.len() != meta2.len()
        || meta1.modified().ok() != meta2.modified().ok()
    {
        buf.zeroize();
        return Err(SecretError::ReplacedDuringRead(reference.safe()));
    }
    Ok(buf)
}

/// Whole-file read (no selector): the entire file is the secret value.
fn resolve_whole_file(
    policy: &FilePolicy,
    target: &Path,
    reference: &SecretReference,
) -> Result<ResolvedSecret, SecretError> {
    let bytes = read_bounded(policy, target, reference)?;
    let material = match reference.repr {
        SecretRepr::Utf8 => {
            let bytes = if policy.trim_trailing_newline {
                trim_trailing_newline(bytes)
            } else {
                bytes
            };
            if bytes.is_empty() {
                return Err(SecretError::Empty(reference.safe()));
            }
            SecretMaterial::Utf8(SecretString::from_utf8(
                bytes,
                policy.max_size,
                reference,
            )?)
        }
        SecretRepr::Bytes => {
            if bytes.is_empty() {
                return Err(SecretError::Empty(reference.safe()));
            }
            SecretMaterial::Bytes(SecretBytes::new(
                bytes,
                policy.max_size,
                reference,
            )?)
        }
    };
    Ok(ResolvedSecret::new(material))
}

/// Parse the target as a bounded JSON object of string values, moving each value
/// into an owned map (no per-value clone). Non-string values, arrays, nested
/// objects, or a non-object top level are rejected as `MalformedRecord`.
fn load_string_map(
    policy: &FilePolicy,
    target: &Path,
    reference: &SecretReference,
) -> Result<ZeroizingStringMap, SecretError> {
    let mut buf = read_bounded(policy, target, reference)?;
    // Deserialize straight into an owned string map: serde moves each parsed
    // string in, and any non-string value (number, bool, null, array, nested
    // object) makes the parse fail -> MalformedRecord. On the error path serde may
    // have made temporary allocations it owns and drops (unavoidable library
    // boundary); the successful path adds no duplicate copy.
    let parsed: Result<BTreeMap<String, String>, _> =
        serde_json::from_slice(&buf);
    buf.zeroize(); // raw record bytes no longer needed
    let map =
        parsed.map_err(|_| SecretError::MalformedRecord(reference.safe()))?;
    Ok(ZeroizingStringMap(map))
}

/// Move one field's value out of the parsed record into protected material. The
/// value is removed from the map (not cloned); on any error the value that was
/// moved out is scrubbed before returning, and the map scrubs the rest on drop.
fn select_field(
    map: &mut ZeroizingStringMap,
    limit: usize,
    reference: &SecretReference,
) -> Result<SecretMaterial, SecretError> {
    let selector = reference.selector.as_deref().ok_or_else(|| {
        SecretError::SelectorMissing {
            reference: reference.safe(),
            selector: None,
        }
    })?;
    let mut value =
        map.remove(selector)
            .ok_or_else(|| SecretError::SelectorMissing {
                reference: reference.safe(),
                selector: Some(selector.to_string()),
            })?;
    match reference.repr {
        SecretRepr::Utf8 => {
            if value.is_empty() {
                return Err(SecretError::Empty(reference.safe()));
            }
            // Moves `value` into the protected type (no clone).
            Ok(SecretMaterial::Utf8(SecretString::new(
                value, limit, reference,
            )?))
        }
        // Binary fields inside JSON need an explicit encoding (e.g. base64); rather
        // than guess, this slice requires a raw (whole-file) reference for binary
        // material. Scrub the moved value before reporting the mismatch.
        SecretRepr::Bytes => {
            value.zeroize();
            Err(SecretError::Inconsistent {
                kind: InconsistencyKind::RepresentationMismatch,
                field: Some(selector.to_string()),
            })
        }
    }
}

/// Map an I/O error to a typed, redacted `SecretError` (no path contents, no OS
/// message that could carry data).
fn map_io(err: &std::io::Error, reference: &SecretReference) -> SecretError {
    use std::io::ErrorKind;
    match err.kind() {
        ErrorKind::NotFound => SecretError::NotFound(reference.safe()),
        ErrorKind::PermissionDenied => SecretError::Provider {
            reference: reference.safe(),
            kind: ProviderFailureKind::Forbidden,
        },
        _ => SecretError::Provider {
            reference: reference.safe(),
            kind: ProviderFailureKind::Unavailable,
        },
    }
}

/// Remove a single trailing `\n` (and a preceding `\r`) if present.
fn trim_trailing_newline(mut bytes: Vec<u8>) -> Vec<u8> {
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
    bytes
}

/// Test-only slowness seam: a resolution whose path contains this marker sleeps in
/// the blocking task, so a concurrency test can prove the blocking work does not
/// stall the async runtime. Compiled out entirely in non-test builds.
fn maybe_test_delay(_reference: &SecretReference) {
    #[cfg(test)]
    if _reference.location.contains("__slow__") {
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};

    static UNIQ: AtomicU64 = AtomicU64::new(0);

    /// A unique temp directory for one test, removed on drop.
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new() -> Self {
            let n = UNIQ.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "df-secrets-{}-{}",
                std::process::id(),
                n
            ));
            fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        fn write(&self, name: &str, contents: &[u8]) -> PathBuf {
            let p = self.path.join(name);
            let mut f = File::create(&p).unwrap();
            f.write_all(contents).unwrap();
            f.sync_all().unwrap();
            p
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn file_ref(path: &Path) -> SecretReference {
        SecretReference::new(SecretProvider::File, path.to_str().unwrap())
    }

    fn strict() -> FileResolver {
        FileResolver::new(FilePolicy::default())
    }

    #[tokio::test]
    async fn reads_raw_utf8_file() {
        let dir = TempDir::new();
        let p = dir.write("pw", b"hunter2");
        let secret = strict().resolve(&file_ref(&p)).await.unwrap();
        assert_eq!(secret.material().as_utf8(), Some("hunter2"));
    }

    #[tokio::test]
    async fn reads_exact_binary_file() {
        let dir = TempDir::new();
        let raw = [0x00u8, 0xff, 0x10, 0x0a, 0x80];
        let p = dir.write("key.bin", &raw);
        let secret = strict()
            .resolve(&file_ref(&p).with_repr(SecretRepr::Bytes))
            .await
            .unwrap();
        assert_eq!(secret.material().expose_bytes(), &raw);
    }

    #[tokio::test]
    async fn trailing_newline_preserved_unless_requested() {
        let dir = TempDir::new();
        let p = dir.write("pw", b"hunter2\n");
        let kept = strict().resolve(&file_ref(&p)).await.unwrap();
        assert_eq!(kept.material().as_utf8(), Some("hunter2\n"));
        let trimmer = FileResolver::new(FilePolicy {
            trim_trailing_newline: true,
            ..FilePolicy::default()
        });
        let trimmed = trimmer.resolve(&file_ref(&p)).await.unwrap();
        assert_eq!(trimmed.material().as_utf8(), Some("hunter2"));
    }

    #[tokio::test]
    async fn empty_file_fails_closed() {
        let dir = TempDir::new();
        let p = dir.write("empty", b"");
        let err = strict().resolve(&file_ref(&p)).await.unwrap_err();
        assert!(matches!(err, SecretError::Empty(_)));
        // Binary empty likewise.
        let err = strict()
            .resolve(&file_ref(&p).with_repr(SecretRepr::Bytes))
            .await
            .unwrap_err();
        assert!(matches!(err, SecretError::Empty(_)));
    }

    #[tokio::test]
    async fn trim_to_empty_fails_closed() {
        let dir = TempDir::new();
        let p = dir.write("nl", b"\n");
        let trimmer = FileResolver::new(FilePolicy {
            trim_trailing_newline: true,
            ..FilePolicy::default()
        });
        let err = trimmer.resolve(&file_ref(&p)).await.unwrap_err();
        assert!(matches!(err, SecretError::Empty(_)));
    }

    #[tokio::test]
    async fn version_on_file_reference_is_rejected() {
        let dir = TempDir::new();
        let p = dir.write("pw", b"x");
        let err = strict()
            .resolve(&file_ref(&p).with_version("7"))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            SecretError::UnsupportedReferenceOption {
                option: ReferenceOption::Version,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn oversized_file_fails_closed() {
        let dir = TempDir::new();
        let p = dir.write("big", b"0123456789");
        let r = FileResolver::new(FilePolicy {
            max_size: 4,
            ..FilePolicy::default()
        });
        let err = r.resolve(&file_ref(&p)).await.unwrap_err();
        assert!(matches!(err, SecretError::SizeExceeded { limit: 4, .. }));
        let p2 = dir.write("exact", b"0123");
        assert!(r.resolve(&file_ref(&p2)).await.is_ok());
    }

    #[tokio::test]
    async fn missing_file_fails_closed() {
        let dir = TempDir::new();
        let p = dir.path.join("nope");
        let err = strict().resolve(&file_ref(&p)).await.unwrap_err();
        assert!(matches!(err, SecretError::NotFound(_)));
    }

    #[tokio::test]
    async fn directory_is_rejected() {
        let dir = TempDir::new();
        let err = strict().resolve(&file_ref(&dir.path)).await.unwrap_err();
        assert!(matches!(err, SecretError::NotRegularFile(_)));
    }

    #[tokio::test]
    async fn relative_path_is_rejected() {
        let r = SecretReference::new(SecretProvider::File, "relative/path");
        let err = strict().resolve(&r).await.unwrap_err();
        assert!(matches!(err, SecretError::PathNotAbsolute(_)));
    }

    #[tokio::test]
    async fn non_utf8_file_as_utf8_fails_closed() {
        let dir = TempDir::new();
        let p = dir.write("bad", &[0xff, 0xfe]);
        let err = strict().resolve(&file_ref(&p)).await.unwrap_err();
        assert!(matches!(err, SecretError::NotUtf8(_)));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn strict_mode_rejects_symlink() {
        use std::os::unix::fs::symlink;
        let dir = TempDir::new();
        let target = dir.write("real", b"secret");
        let link = dir.path.join("link");
        symlink(&target, &link).unwrap();
        let err = strict().resolve(&file_ref(&link)).await.unwrap_err();
        assert!(matches!(err, SecretError::SymlinkRejected(_)));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn projected_volume_follows_symlink_within_trusted_root() {
        use std::os::unix::fs::symlink;
        let dir = TempDir::new();
        let data_dir = dir.path.join("..2026_dir");
        fs::create_dir_all(&data_dir).unwrap();
        let mut f = File::create(data_dir.join("token")).unwrap();
        f.write_all(b"projected-secret").unwrap();
        f.sync_all().unwrap();
        symlink(&data_dir, dir.path.join("..data")).unwrap();
        symlink(
            dir.path.join("..data").join("token"),
            dir.path.join("token"),
        )
        .unwrap();

        let r = FileResolver::new(FilePolicy {
            mode: FileMode::ProjectedVolume {
                trusted_root: dir.path.clone(),
            },
            ..FilePolicy::default()
        });
        let secret =
            r.resolve(&file_ref(&dir.path.join("token"))).await.unwrap();
        assert_eq!(secret.material().as_utf8(), Some("projected-secret"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn projected_volume_rejects_escape_outside_root() {
        use std::os::unix::fs::symlink;
        let outside = TempDir::new();
        let secret_file = outside.write("host", b"escaped");
        let root = TempDir::new();
        let link = root.path.join("token");
        symlink(&secret_file, &link).unwrap();

        let r = FileResolver::new(FilePolicy {
            mode: FileMode::ProjectedVolume {
                trusted_root: root.path.clone(),
            },
            ..FilePolicy::default()
        });
        let err = r.resolve(&file_ref(&link)).await.unwrap_err();
        assert!(matches!(err, SecretError::OutsideTrustedRoot(_)));
    }

    #[tokio::test]
    async fn atomic_replacement_seen_on_next_resolution() {
        let dir = TempDir::new();
        let p = dir.write("pw", b"old");
        let r = strict();
        assert_eq!(
            r.resolve(&file_ref(&p)).await.unwrap().material().as_utf8(),
            Some("old")
        );
        dir.write("pw", b"new-value");
        assert_eq!(
            r.resolve(&file_ref(&p)).await.unwrap().material().as_utf8(),
            Some("new-value")
        );
    }

    #[tokio::test]
    async fn structured_json_single_field() {
        let dir = TempDir::new();
        let p =
            dir.write("db.json", br#"{"username":"df","password":"s3cr3t"}"#);
        let secret = strict()
            .resolve(&file_ref(&p).with_selector("password"))
            .await
            .unwrap();
        assert_eq!(secret.material().as_utf8(), Some("s3cr3t"));
    }

    #[tokio::test]
    async fn structured_json_batch_single_read_one_group() {
        let dir = TempDir::new();
        let p =
            dir.write("db.json", br#"{"username":"df","password":"s3cr3t"}"#);
        let user = file_ref(&p).with_selector("username");
        let pass = file_ref(&p).with_selector("password");
        let reqs = vec![
            CredentialFieldRequest::new("username", user),
            CredentialFieldRequest::new("password", pass),
        ];
        let cs = strict().resolve_set(&reqs).await.unwrap();
        assert!(cs.single_resolution_group().is_ok());
        assert_eq!(
            cs.require("username").unwrap().material().as_utf8(),
            Some("df")
        );
    }

    #[tokio::test]
    async fn structured_json_non_string_value_is_malformed() {
        let dir = TempDir::new();
        // A numeric value violates the "object of string values" contract.
        let p = dir.write("db.json", br#"{"port":5432}"#);
        let err = strict()
            .resolve(&file_ref(&p).with_selector("port"))
            .await
            .unwrap_err();
        assert!(matches!(err, SecretError::MalformedRecord(_)));
        // Nested object likewise.
        let p2 = dir.write("n.json", br#"{"a":{"b":"c"}}"#);
        let err = strict()
            .resolve(&file_ref(&p2).with_selector("a"))
            .await
            .unwrap_err();
        assert!(matches!(err, SecretError::MalformedRecord(_)));
    }

    #[tokio::test]
    async fn independent_files_have_distinct_provenance() {
        let dir = TempDir::new();
        let a = dir.write("a.json", br#"{"k":"va"}"#);
        let b = dir.write("b.json", br#"{"k":"vb"}"#);
        let reqs = vec![
            CredentialFieldRequest::new(
                "first",
                file_ref(&a).with_selector("k"),
            ),
            CredentialFieldRequest::new(
                "second",
                file_ref(&b).with_selector("k"),
            ),
        ];
        let cs = strict().resolve_set(&reqs).await.unwrap();
        assert!(matches!(
            cs.single_resolution_group(),
            Err(SecretError::Inconsistent {
                kind: InconsistencyKind::GenerationMismatch,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn malformed_json_and_missing_selector_fail_closed() {
        let dir = TempDir::new();
        let bad = dir.write("bad.json", b"{not json");
        let err = strict()
            .resolve(&file_ref(&bad).with_selector("k"))
            .await
            .unwrap_err();
        assert!(matches!(err, SecretError::MalformedRecord(_)));

        let ok = dir.write("ok.json", br#"{"a":"1"}"#);
        let err = strict()
            .resolve(&file_ref(&ok).with_selector("missing"))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            SecretError::SelectorMissing {
                selector: Some(_),
                ..
            }
        ));
    }

    #[tokio::test]
    async fn file_contents_absent_from_errors() {
        let dir = TempDir::new();
        let p = dir.write("s.json", br#"{bad S3NT1NEL-file"#);
        let err = strict()
            .resolve(&file_ref(&p).with_selector("k"))
            .await
            .unwrap_err();
        assert!(!format!("{err}").contains("S3NT1NEL-file"));
        assert!(!format!("{err:?}").contains("S3NT1NEL-file"));
    }

    #[test]
    fn select_field_moves_value_and_leftovers_are_scrubbed() {
        // Focused unit test for the zeroization cleanup path (blocker 3).
        let mut map = ZeroizingStringMap(BTreeMap::from([
            ("a".to_string(), "secret-a".to_string()),
            ("b".to_string(), "secret-b".to_string()),
        ]));
        let reference =
            SecretReference::new(SecretProvider::File, "/x").with_selector("a");
        let material =
            select_field(&mut map, DEFAULT_MAX_SECRET_BYTES, &reference)
                .unwrap();
        // Selected value was moved out (not present in the map any more).
        assert_eq!(material.as_utf8(), Some("secret-a"));
        assert!(map.remove("a").is_none());
        // The leftover is still present until cleanup runs...
        assert_eq!(map.0.get("b").map(String::as_str), Some("secret-b"));
        // ...and the cleanup path zeroizes it to empty.
        map.zeroize_remaining();
        assert!(map.0.get("b").unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slow_file_io_does_not_block_async_tasks() {
        use std::time::{Duration, Instant};
        let dir = TempDir::new();
        // Path marker triggers a 500ms sleep inside the blocking task.
        let p = dir.write("__slow__pw", b"eventually");
        let r = std::sync::Arc::new(strict());
        let rc = r.clone();
        let pc = p.clone();
        let slow =
            tokio::spawn(async move { rc.resolve(&file_ref(&pc)).await });

        // Unrelated async work must make progress while the file op blocks a
        // blocking-pool thread (not a runtime worker).
        let start = Instant::now();
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            start.elapsed() < Duration::from_millis(400),
            "async tasks were blocked by the slow filesystem read"
        );

        let secret = slow.await.unwrap().unwrap();
        assert_eq!(secret.material().as_utf8(), Some("eventually"));
    }
}

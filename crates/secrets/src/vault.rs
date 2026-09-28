//! Native HashiCorp Vault provider (feature `vault`).
//!
//! Phase 1 scope: **KV v2 static secrets** with **token-file** and **Kubernetes**
//! auth. The Vault HTTP surface is small (KV v2 read, Kubernetes login, token
//! lookup-self, token renew-self), so this talks to Vault directly with the
//! workspace `reqwest` (0.12, rustls) rather than pulling a second HTTP/TLS stack.
//! All Vault I/O sits behind the internal [`VaultApi`]/[`TokenAuthority`] traits so
//! resolution and the token lifecycle are unit-testable without a live server.
//!
//! Auth is not a one-time login. After authenticating, the token's TTL and
//! renewability are discovered from `auth/token/lookup-self` (authoritative even
//! for token-file auth, which carries no login lease), and a background task
//! renews before expiry - scheduled from the lease with an absolute safety margin,
//! a fraction-of-TTL target, and downward jitter. Non-renewable or finite tokens
//! are re-authenticated (re-reading the token file / kubelet-rotated SA JWT) before
//! they expire. The task is bound to the resolver's lifetime and exits on drop.
//!
//! Auth material on disk (token files, projected SA JWTs) is read through the
//! vetted [`FileResolver`] path: bounded, symlink-policy-aware, zeroizing. Leased /
//! dynamic engines are later phases; static KV reads carry no `expires_at`, but the
//! KV v2 record version is honored (pinned reads) and propagated as
//! `provider_version` for change-polling.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use reqwest::Url;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::RwLock;
use zeroize::Zeroizing;

use crate::credential_set::CredentialSet;
use crate::error::{ProviderFailureKind, SecretError};
use crate::material::SecretString;
use crate::providers::{FileMode, FilePolicy, FileResolver};
use crate::reference::{SecretProvider, SecretReference};
use crate::resolved::{ResolutionGroup, ResolvedSecret, SecretMaterial};
use crate::resolver::{
    CredentialFieldRequest, SecretResolver, check_no_duplicate_fields,
};
use crate::structured::{StructuredSecret, credential_set_from_record};

/// Response body cap for auth/lookup/renew endpoints (tokens are small).
const CONTROL_RESPONSE_CAP: usize = 64 * 1024;
/// Absolute floor for the KV response cap.
const KV_RESPONSE_FLOOR: usize = 64 * 1024;

// -----------------------------------------------------------------------------
// Configuration (validated at construction)
// -----------------------------------------------------------------------------

/// A startup-time configuration error for a Vault connection. Distinct from
/// [`SecretError`] because it is a config defect surfaced before any resolution.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum VaultConfigError {
    #[error("vault address is not a valid absolute URL")]
    InvalidAddress,
    #[error(
        "vault address must use https (set allow_insecure_http for a dev/test Vault)"
    )]
    InsecureScheme,
    #[error("vault address must not embed credentials (userinfo)")]
    AddressHasUserinfo,
    #[error("vault address must include a host")]
    AddressMissingHost,
    #[error("vault address must not carry a query or fragment")]
    AddressHasExtraComponents,
    #[error("max_secret_bytes must be non-zero")]
    ZeroSizeLimit,
    #[error("renew safety margin must be non-zero")]
    ZeroSafetyMargin,
    #[error("renew fraction must be in (0.0, 1.0]")]
    BadFraction,
    #[error("renew jitter must be in [0.0, 1.0)")]
    BadJitter,
    #[error("renew interval must be at least one second")]
    RenewIntervalTooSmall,
    #[error("connect and request timeouts must be non-zero")]
    ZeroTimeout,
    #[error("request timeout must be >= connect timeout")]
    RequestTimeoutTooSmall,
    #[error("kubernetes auth mount is not a valid vault path")]
    InvalidAuthMount,
    #[error("kubernetes auth role must not be empty")]
    EmptyAuthRole,
}

/// A file holding auth material (a Vault token or a Kubernetes SA JWT), with the
/// symlink policy used to read it through [`FileResolver`].
#[derive(Debug, Clone)]
pub enum VaultAuthFile {
    /// Operator-managed file; the path must not itself be a symlink.
    Strict { path: PathBuf },
    /// Kubernetes projected volume: symlinks are followed, but the fully resolved
    /// target must stay within `trusted_root`.
    ProjectedVolume {
        path: PathBuf,
        trusted_root: PathBuf,
    },
}

impl VaultAuthFile {
    fn path(&self) -> &Path {
        match self {
            VaultAuthFile::Strict { path } => path,
            VaultAuthFile::ProjectedVolume { path, .. } => path,
        }
    }

    fn policy(&self, max_size: usize) -> FilePolicy {
        let mode = match self {
            VaultAuthFile::Strict { .. } => FileMode::Strict,
            VaultAuthFile::ProjectedVolume { trusted_root, .. } => {
                FileMode::ProjectedVolume {
                    trusted_root: trusted_root.clone(),
                }
            }
        };
        FilePolicy {
            max_size,
            mode,
            // Token/JWT files commonly carry a trailing newline; strip exactly one.
            trim_trailing_newline: true,
        }
    }

    /// Read the file as protected UTF-8 through the vetted file-resolver path.
    async fn read(
        &self,
        max_size: usize,
    ) -> Result<ResolvedSecret, SecretError> {
        let resolver = FileResolver::new(self.policy(max_size));
        let reference = SecretReference::new(
            SecretProvider::File,
            self.path().to_string_lossy().into_owned(),
        );
        resolver.resolve(&reference).await
    }
}

/// How the Vault client authenticates. Both variants re-read their file material
/// on every (re-)authentication, so externally rotated tokens / JWTs are picked up.
#[derive(Debug, Clone)]
pub enum VaultAuth {
    /// A Vault token read from a mounted/projected file.
    TokenFile(VaultAuthFile),
    /// Kubernetes auth: POST the projected SA JWT to `auth/<mount>/login`.
    Kubernetes {
        mount: String,
        role: String,
        jwt: VaultAuthFile,
    },
}

/// Token-renewal scheduling policy. Renewal fires before expiry, driven by the
/// lease discovered from Vault, never by a fixed interval alone.
#[derive(Debug, Clone)]
pub struct RenewPolicy {
    /// Re-check cadence for tokens with no expiry (root/`ttl=0`) and an upper
    /// bound on any computed delay.
    pub interval: Duration,
    /// Absolute margin: stop normal operation at least this long before expiry.
    pub safety_margin: Duration,
    /// Fraction of the TTL at which to renew, in `(0.0, 1.0]`.
    pub fraction: f32,
    /// Maximum downward jitter fraction applied to the delay, in `[0.0, 1.0)`.
    pub jitter: f32,
    /// Floor for any computed delay.
    pub min_delay: Duration,
}

impl Default for RenewPolicy {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(600),
            safety_margin: Duration::from_secs(10),
            fraction: 0.66,
            jitter: 0.1,
            min_delay: Duration::from_secs(1),
        }
    }
}

/// Bounded network timeouts. Every Vault request (startup auth, resolution,
/// renewal, rotation) is capped so a hung connection cannot stall indefinitely.
#[derive(Debug, Clone)]
pub struct VaultTimeouts {
    pub connect: Duration,
    pub request: Duration,
}

impl Default for VaultTimeouts {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(5),
            request: Duration::from_secs(15),
        }
    }
}

/// A Vault path is one or more `/`-separated segments. Each segment must be
/// non-empty, not a `.`/`..` traversal, and free of control characters and
/// query/fragment markers, so it cannot alter the intended `/v1/...` endpoint.
/// Nested secret paths are allowed.
fn valid_vault_path(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    s.split('/').all(|seg| {
        !seg.is_empty()
            && seg != "."
            && seg != ".."
            && !seg.chars().any(|c| c.is_control() || c == '?' || c == '#')
    })
}

/// Validated connection parameters. Construct via [`VaultConnection::new`], which
/// rejects unsafe addresses and out-of-range renewal settings.
#[derive(Debug, Clone)]
pub struct VaultConnection {
    address: String,
    namespace: Option<String>,
    auth: VaultAuth,
    max_secret_bytes: usize,
    renew: RenewPolicy,
    timeouts: VaultTimeouts,
}

impl VaultConnection {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        address: impl Into<String>,
        namespace: Option<String>,
        auth: VaultAuth,
        max_secret_bytes: usize,
        renew: RenewPolicy,
        timeouts: VaultTimeouts,
        allow_insecure_http: bool,
    ) -> Result<Self, VaultConfigError> {
        let address = address.into();
        let url = Url::parse(&address)
            .map_err(|_| VaultConfigError::InvalidAddress)?;
        match url.scheme() {
            "https" => {}
            "http" if allow_insecure_http => {}
            "http" => return Err(VaultConfigError::InsecureScheme),
            _ => return Err(VaultConfigError::InvalidAddress),
        }
        // The address appears in SafeRef/diagnostics, so it must not carry
        // credentials or other unexpected components.
        if !url.username().is_empty() || url.password().is_some() {
            return Err(VaultConfigError::AddressHasUserinfo);
        }
        if url.host_str().is_none() {
            return Err(VaultConfigError::AddressMissingHost);
        }
        if url.query().is_some() || url.fragment().is_some() {
            return Err(VaultConfigError::AddressHasExtraComponents);
        }
        if max_secret_bytes == 0 {
            return Err(VaultConfigError::ZeroSizeLimit);
        }
        if renew.safety_margin.is_zero() {
            return Err(VaultConfigError::ZeroSafetyMargin);
        }
        if !(renew.fraction > 0.0 && renew.fraction <= 1.0) {
            return Err(VaultConfigError::BadFraction);
        }
        if !(renew.jitter >= 0.0 && renew.jitter < 1.0) {
            return Err(VaultConfigError::BadJitter);
        }
        if renew.interval < Duration::from_secs(1) {
            return Err(VaultConfigError::RenewIntervalTooSmall);
        }
        if timeouts.connect.is_zero() || timeouts.request.is_zero() {
            return Err(VaultConfigError::ZeroTimeout);
        }
        if timeouts.request < timeouts.connect {
            return Err(VaultConfigError::RequestTimeoutTooSmall);
        }
        if let VaultAuth::Kubernetes { mount, role, .. } = &auth {
            if !valid_vault_path(mount) {
                return Err(VaultConfigError::InvalidAuthMount);
            }
            if role.is_empty() {
                return Err(VaultConfigError::EmptyAuthRole);
            }
        }
        Ok(Self {
            address,
            namespace,
            auth,
            max_secret_bytes,
            renew,
            timeouts,
        })
    }
}

// -----------------------------------------------------------------------------
// Internal seams
// -----------------------------------------------------------------------------

/// A KV v2 record read: field materials plus the record's version (propagated as
/// `provider_version` and used by change-polling).
pub(crate) struct KvRecord {
    fields: BTreeMap<String, SecretMaterial>,
    version: Option<String>,
}

/// The Vault KV read the resolver needs, behind a trait for mock testing.
#[async_trait]
pub(crate) trait VaultApi: Send + Sync {
    async fn read_kv2(
        &self,
        mount: &str,
        path: &str,
        version: Option<&str>,
        reference: &SecretReference,
    ) -> Result<KvRecord, SecretError>;
}

/// A discovered token lease: how long the current token is valid and whether it
/// can be renewed. `ttl == 0` means "no expiry" (e.g. a root token).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Lease {
    ttl: Duration,
    renewable: bool,
}

/// The token lifecycle operations, behind a trait so renewal/reauth can be tested
/// deterministically against a mock.
#[async_trait]
pub(crate) trait TokenAuthority: Send + Sync {
    /// Authenticate from source material (read the token file, or read the SA JWT
    /// and log in), then discover the lease. Returns a fresh token and its lease.
    async fn authenticate(&self) -> Result<(SecretString, Lease), SecretError>;

    /// Renew the current token in place (identity unchanged), returning the
    /// refreshed lease.
    async fn renew(&self, token: &str) -> Result<Lease, SecretError>;

    /// Verify the current token server-side (lookup-self), returning its live
    /// lease. Fails if the token was revoked or has expired.
    async fn lookup(&self, token: &str) -> Result<Lease, SecretError>;

    /// For file-backed auth: if the on-disk token now differs from `current`,
    /// return the freshly read token and its lease so a rotated token can be
    /// adopted proactively even while the old one is still valid. Returns `None`
    /// when the material is unchanged, and for auth methods with no cheap
    /// change-detection (Kubernetes login mints a new token every call, so it is
    /// not re-run speculatively).
    async fn reread_if_rotated(
        &self,
        current: &str,
    ) -> Result<Option<(SecretString, Lease)>, SecretError>;
}

// -----------------------------------------------------------------------------
// Renewal scheduling (pure) and driver (effectful, mockable)
// -----------------------------------------------------------------------------

/// What to do at the next renewal wake-up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RenewMode {
    /// Renewable, finite: extend the current token.
    Renew,
    /// Finite and non-renewable: re-authenticate before expiry.
    Reauth,
    /// No expiry: nothing to do but re-check later.
    Recheck,
}

fn renew_mode(lease: &Lease) -> RenewMode {
    if lease.ttl.is_zero() {
        RenewMode::Recheck
    } else if lease.renewable {
        RenewMode::Renew
    } else {
        RenewMode::Reauth
    }
}

/// Delay until the next renewal action, from the token's **remaining** safe
/// lifetime (TTL minus time already elapsed). Renews at the earliest of (fraction
/// of remaining), (remaining - safety margin), and the interval cap, then applies
/// downward jitter and a floor. Because it works from the remaining lifetime, a
/// retry after a failed attempt is bounded by how much of the old token's life is
/// left - it grows more urgent as expiry nears rather than waiting a fixed
/// interval - and a remaining lifetime shorter than the margin collapses to the
/// floor. A non-expiring token (`no_expiry`) re-checks at the interval cadence.
fn renew_delay(
    remaining: Duration,
    no_expiry: bool,
    p: &RenewPolicy,
    jitter_seed: u64,
) -> Duration {
    if no_expiry {
        return p.interval;
    }
    let by_fraction = remaining.mul_f32(p.fraction);
    let by_margin = remaining.saturating_sub(p.safety_margin);
    let base = by_fraction.min(by_margin).min(p.interval);
    apply_downward_jitter(base, p.jitter, jitter_seed).max(p.min_delay)
}

/// Reduce `base` by up to `jitter` (never increase it, so renewal stays before
/// expiry). Deterministic in `seed` for testing.
fn apply_downward_jitter(base: Duration, jitter: f32, seed: u64) -> Duration {
    if jitter <= 0.0 {
        return base;
    }
    let frac = (seed % 1000) as f32 / 1000.0; // [0.0, 1.0)
    base.mul_f32(1.0 - jitter * frac)
}

fn jitter_seed() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0)
}

/// Outcome of one renewal tick. Returned so the driver loop (and tests) can react.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TickOutcome {
    Renewed,
    Reauthenticated,
    Rechecked,
    /// Both renewal and re-authentication failed; the current token is retained
    /// (reads fail closed) and the loop backs off.
    Failed,
}

/// Perform one renewal action against the authority, updating the shared token and
/// lease in place. No sleeping here: the caller schedules via [`renew_delay`].
async fn drive_tick(
    authority: &dyn TokenAuthority,
    token: &RwLock<SecretString>,
    lease: &mut Lease,
) -> TickOutcome {
    match renew_mode(lease) {
        RenewMode::Recheck => recheck(authority, token, lease).await,
        RenewMode::Renew => {
            let renewed = {
                let guard = token.read().await;
                authority.renew(guard.expose_secret()).await
            };
            match renewed {
                Ok(new_lease) => {
                    *lease = new_lease;
                    TickOutcome::Renewed
                }
                // Renewal failed: fall back to a full re-authentication.
                Err(_) => reauth(authority, token, lease).await,
            }
        }
        RenewMode::Reauth => reauth(authority, token, lease).await,
    }
}

async fn reauth(
    authority: &dyn TokenAuthority,
    token: &RwLock<SecretString>,
    lease: &mut Lease,
) -> TickOutcome {
    match authority.authenticate().await {
        Ok((new_token, new_lease)) => {
            *token.write().await = new_token;
            *lease = new_lease;
            TickOutcome::Reauthenticated
        }
        Err(_) => TickOutcome::Failed,
    }
}

/// Recovery path for a non-expiring token (`ttl == 0`). Proactively adopts a
/// rotated file-backed token, then verifies the current token is still valid
/// server-side, re-authenticating if it was revoked or replaced.
async fn recheck(
    authority: &dyn TokenAuthority,
    token: &RwLock<SecretString>,
    lease: &mut Lease,
) -> TickOutcome {
    let current =
        Zeroizing::new(token.read().await.expose_secret().to_string());
    // 1. Adopt a rotated on-disk token even while the old one is still valid.
    if let Ok(Some((new_token, new_lease))) =
        authority.reread_if_rotated(&current).await
    {
        *token.write().await = new_token;
        *lease = new_lease;
        return TickOutcome::Reauthenticated;
    }
    // 2. Confirm the current token is still valid; recover if it was revoked.
    match authority.lookup(&current).await {
        Ok(new_lease) => {
            *lease = new_lease;
            TickOutcome::Rechecked
        }
        Err(_) => reauth(authority, token, lease).await,
    }
}

/// Background task: renew/re-authenticate for the resolver's lifetime. Exits when
/// the resolver (and thus the shared state) is dropped.
///
/// Delays are computed from the current lease's *remaining* safe lifetime, tracked
/// via `acquired`. On a failed tick the lease and `acquired` are kept, so the next
/// delay is recomputed from what remains of the old token's life (bounded, and
/// increasingly urgent as expiry approaches) rather than the general interval.
fn spawn_renewal(weak: Weak<VaultShared>, mut lease: Lease) {
    tokio::spawn(async move {
        let mut acquired = Instant::now();
        // Alarm latch: warn once on entering a failure state, one recovery event
        // when it clears, so a Vault outage cannot flood logs near expiry (where
        // retries fire every min_delay).
        let mut in_failure = false;
        loop {
            let delay = match weak.upgrade() {
                None => return,
                Some(shared) => {
                    let remaining =
                        lease.ttl.saturating_sub(acquired.elapsed());
                    renew_delay(
                        remaining,
                        lease.ttl.is_zero(),
                        &shared.conn.renew,
                        jitter_seed(),
                    )
                }
            };
            tokio::time::sleep(delay).await;

            let Some(shared) = weak.upgrade() else {
                return;
            };
            // Secret-free lifecycle observability: outcome + non-secret lease
            // shape only (never the token, JWT, or address credentials).
            let outcome = drive_tick(&*shared, &shared.token, &mut lease).await;
            match outcome {
                // A new lease started now: reset the elapsed-time baseline.
                TickOutcome::Renewed
                | TickOutcome::Reauthenticated
                | TickOutcome::Rechecked => {
                    if in_failure {
                        in_failure = false;
                        tracing::info!(
                            "vault token lifecycle recovered after failures"
                        );
                    }
                    tracing::debug!(
                        outcome = ?outcome,
                        ttl_secs = lease.ttl.as_secs(),
                        renewable = lease.renewable,
                        "vault token lifecycle advanced"
                    );
                    acquired = Instant::now();
                }
                // Vault unreachable / auth broken: retain the current token and
                // lease. `acquired` is unchanged, so the next retry is bounded by
                // the old token's remaining safe lifetime, not the interval.
                // Warn only on the transition into failure so repeated near-expiry
                // retries during an outage cannot flood the logs.
                TickOutcome::Failed => {
                    if !in_failure {
                        in_failure = true;
                        tracing::warn!(
                            "vault token renewal and reauthentication both \
                             failed; retaining current token and retrying within \
                             its remaining safe lifetime"
                        );
                    }
                }
            }
        }
    });
}

// -----------------------------------------------------------------------------
// reqwest-backed Vault client
// -----------------------------------------------------------------------------

struct VaultShared {
    http: reqwest::Client,
    base: Url,
    conn: VaultConnection,
    token: RwLock<SecretString>,
}

impl VaultShared {
    fn build(conn: VaultConnection) -> Result<Self, SecretError> {
        let base = Url::parse(&conn.address)
            .map_err(|_| unavailable(&conn_reference(&conn.address)))?;
        let http = reqwest::Client::builder()
            .connect_timeout(conn.timeouts.connect)
            .timeout(conn.timeouts.request)
            .build()
            .map_err(|_| unavailable(&conn_reference(&conn.address)))?;
        let empty = SecretString::new(
            String::new(),
            conn.max_secret_bytes,
            &conn_reference(&conn.address),
        )?;
        Ok(Self {
            http,
            base,
            conn,
            token: RwLock::new(empty),
        })
    }

    fn reference(&self) -> SecretReference {
        conn_reference(&self.conn.address)
    }

    /// Build an endpoint URL `/v1/<api_path>`, preserving any base path prefix.
    /// Query parameters must be added by the caller via `query_pairs_mut` so they
    /// are encoded as a real query, not folded into the path.
    fn url(&self, api_path: &str) -> Url {
        let mut url = self.base.clone();
        url.set_path(&format!(
            "{}/v1/{}",
            self.base.path().trim_end_matches('/'),
            api_path
        ));
        url
    }

    fn with_headers(
        &self,
        req: reqwest::RequestBuilder,
        token: Option<&str>,
    ) -> reqwest::RequestBuilder {
        let mut req = req;
        if let Some(ns) = &self.conn.namespace {
            req = req.header("X-Vault-Namespace", ns);
        }
        if let Some(t) = token {
            req = req.header("X-Vault-Token", t);
        }
        req
    }

    /// Send a request, enforce status, and read the body under `cap`. When
    /// `kv_not_found` is set, a 404 maps to [`SecretError::NotFound`].
    async fn execute(
        &self,
        req: reqwest::RequestBuilder,
        cap: usize,
        kv_not_found: bool,
        reference: &SecretReference,
    ) -> Result<Zeroizing<Vec<u8>>, SecretError> {
        let resp = req.send().await.map_err(|e| send_err(&e, reference))?;
        let status = resp.status();
        if !status.is_success() {
            if kv_not_found && status.as_u16() == 404 {
                return Err(SecretError::NotFound(reference.safe()));
            }
            return Err(map_status(status, reference));
        }
        read_body_capped(resp, cap, reference).await
    }

    async fn get_json<T: DeserializeOwned>(
        &self,
        url: Url,
        token: Option<&str>,
        cap: usize,
        kv_not_found: bool,
        reference: &SecretReference,
    ) -> Result<T, SecretError> {
        let req = self.with_headers(self.http.get(url), token);
        let bytes = self.execute(req, cap, kv_not_found, reference).await?;
        serde_json::from_slice(&bytes).map_err(|_| {
            provider(reference, ProviderFailureKind::InvalidResponse)
        })
    }

    async fn post_json<B: Serialize, T: DeserializeOwned>(
        &self,
        url: Url,
        token: Option<&str>,
        body: &B,
        cap: usize,
        reference: &SecretReference,
    ) -> Result<T, SecretError> {
        let req = self.with_headers(self.http.post(url).json(body), token);
        let bytes = self.execute(req, cap, false, reference).await?;
        serde_json::from_slice(&bytes).map_err(|_| {
            provider(reference, ProviderFailureKind::InvalidResponse)
        })
    }

    /// Discover the current token's lease from `auth/token/lookup-self`.
    async fn lookup_self(&self, token: &str) -> Result<Lease, SecretError> {
        let reference = self.reference();
        let resp: LookupResponse = self
            .get_json(
                self.url("auth/token/lookup-self"),
                Some(token),
                CONTROL_RESPONSE_CAP,
                false,
                &reference,
            )
            .await?;
        Ok(Lease {
            ttl: Duration::from_secs(resp.data.ttl),
            renewable: resp.data.renewable,
        })
    }

    fn secret_token(&self, raw: String) -> Result<SecretString, SecretError> {
        SecretString::new(raw, self.conn.max_secret_bytes, &self.reference())
    }

    /// A scrubbed-on-drop copy of the current client token for a request header.
    async fn current_token(&self) -> zeroize::Zeroizing<String> {
        zeroize::Zeroizing::new(
            self.token.read().await.expose_secret().to_string(),
        )
    }

    /// Execute a lease-management request, mapping Vault's **absent-lease** responses
    /// to [`SecretError::NotFound`] so the caller can treat an already-gone lease as
    /// idempotent. Absent is signalled by a 404 or, in the versions that use it, a
    /// 4xx whose small error body carries a known marker (matched without surfacing
    /// the body). Everything else maps through [`map_status`]. A 2xx returns the body
    /// (possibly empty for a 204).
    async fn execute_lease(
        &self,
        req: reqwest::RequestBuilder,
        reference: &SecretReference,
    ) -> Result<Zeroizing<Vec<u8>>, SecretError> {
        let resp = req.send().await.map_err(|e| send_err(&e, reference))?;
        let status = resp.status();
        if status.is_success() {
            return read_body_capped(resp, CONTROL_RESPONSE_CAP, reference)
                .await;
        }
        if status.as_u16() == 404 {
            return Err(SecretError::NotFound(reference.safe()));
        }
        // Read the capped, scrubbed error body only to classify absent-lease; never
        // surfaced. A read failure falls through to the status-based mapping.
        let body = read_body_capped(resp, CONTROL_RESPONSE_CAP, reference)
            .await
            .unwrap_or_default();
        if lease_absent_marker(&body) {
            return Err(SecretError::NotFound(reference.safe()));
        }
        Err(map_status(status, reference))
    }

    /// POST a lease-management request and parse a JSON body, with absent-lease
    /// mapping (see [`execute_lease`](Self::execute_lease)).
    async fn lease_post_json<B: Serialize, T: DeserializeOwned>(
        &self,
        url: Url,
        token: Option<&str>,
        body: &B,
        reference: &SecretReference,
    ) -> Result<T, SecretError> {
        let req = self.with_headers(self.http.post(url).json(body), token);
        let bytes = self.execute_lease(req, reference).await?;
        serde_json::from_slice(&bytes).map_err(|_| {
            provider(reference, ProviderFailureKind::InvalidResponse)
        })
    }
}

/// Whether a Vault error body indicates the lease is absent (already revoked or
/// expired). Matches Vault's stable markers ASCII-case-insensitively **without
/// allocating** or copying the body (which may carry lease identifiers or other
/// sensitive context) into a new buffer; the body is only classified, never surfaced.
fn lease_absent_marker(bytes: &[u8]) -> bool {
    contains_ascii_ci(bytes, b"invalid lease")
        || contains_ascii_ci(bytes, b"lease not found")
}

/// Allocation-free ASCII-case-insensitive substring search.
fn contains_ascii_ci(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || needle.len() > haystack.len() {
        return needle.is_empty();
    }
    haystack
        .windows(needle.len())
        .any(|w| w.eq_ignore_ascii_case(needle))
}

/// A single Vault path segment safe to interpolate into an endpoint path: non-empty,
/// no traversal/control/query characters, and no embedded `/` that could alter the
/// intended endpoint.
fn valid_path_segment(s: &str) -> bool {
    !s.is_empty() && !s.contains('/') && valid_vault_path(s)
}

fn kv_response_cap(max_secret_bytes: usize) -> usize {
    max_secret_bytes.saturating_mul(16).max(KV_RESPONSE_FLOOR)
}

#[async_trait]
impl VaultApi for VaultShared {
    async fn read_kv2(
        &self,
        mount: &str,
        path: &str,
        version: Option<&str>,
        reference: &SecretReference,
    ) -> Result<KvRecord, SecretError> {
        let mut url = self.url(&format!("{mount}/data/{path}"));
        if let Some(v) = version {
            url.query_pairs_mut().append_pair("version", v);
        }
        // Transient copy for the request header, scrubbed on drop.
        let token = zeroize::Zeroizing::new(
            self.token.read().await.expose_secret().to_string(),
        );
        let resp: KvReadResponse = self
            .get_json(
                url,
                Some(&token),
                kv_response_cap(self.conn.max_secret_bytes),
                true,
                reference,
            )
            .await?;

        let limit = self.conn.max_secret_bytes;
        let mut fields = BTreeMap::new();
        for (k, v) in resp.data.data {
            // Phase 1: KV credential/DSN fields are UTF-8 strings.
            let s = match v {
                serde_json::Value::String(s) => s,
                _ => return Err(SecretError::NotUtf8(reference.safe())),
            };
            let material =
                SecretMaterial::Utf8(SecretString::new(s, limit, reference)?);
            fields.insert(k, material);
        }
        Ok(KvRecord {
            fields,
            version: Some(resp.data.metadata.version.to_string()),
        })
    }
}

/// A dynamic (leased) credential read from a Vault secrets engine: the atomic
/// credential set plus its lease handle fields. The `credentials` carry
/// `expires_at`/`renewable` so the rotation core's expiry handling applies. Callers
/// (the sources lease manager) wrap `lease_id` in their own redacted, durable type.
pub struct LeasedRead {
    pub credentials: CredentialSet,
    /// The Vault lease id. Sensitive-adjacent: never log it; the caller stores it in
    /// a redacted, durable handle.
    pub lease_id: String,
    pub lease_duration: Duration,
    pub renewable: bool,
}

/// The result of a lease lookup or renew: the (possibly extended) remaining duration
/// and whether the lease is still renewable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseInfo {
    pub lease_duration: Duration,
    pub renewable: bool,
}

/// The Vault lease operations the sources lease manager drives, behind a trait so
/// they are mockable without a live server. Engine reads issue a lease; `sys/leases`
/// renews/looks-up/revokes it by id.
#[async_trait]
pub(crate) trait VaultLeaseApi: Send + Sync {
    /// Read a dynamic credential from `<mount>/creds/<role>`, creating a lease.
    async fn read_db_credentials(
        &self,
        mount: &str,
        role: &str,
    ) -> Result<LeasedRead, SecretError>;
    async fn lookup_lease(
        &self,
        lease_id: &str,
    ) -> Result<LeaseInfo, SecretError>;
    async fn renew_lease(
        &self,
        lease_id: &str,
        increment_secs: Option<u64>,
    ) -> Result<LeaseInfo, SecretError>;
    async fn revoke_lease(&self, lease_id: &str) -> Result<(), SecretError>;
}

#[derive(serde::Deserialize)]
struct DbCredsResponse {
    data: BTreeMap<String, serde_json::Value>,
    lease_id: String,
    lease_duration: u64,
    renewable: bool,
}
#[derive(serde::Deserialize)]
struct LeaseLookupResponse {
    data: LeaseLookupData,
}
#[derive(serde::Deserialize)]
struct LeaseLookupData {
    ttl: u64,
    renewable: bool,
}
#[derive(serde::Deserialize)]
struct LeaseRenewResponse {
    lease_duration: u64,
    renewable: bool,
}

/// Borrowing request body for lookup/revoke: serializes the lease id by reference,
/// avoiding a `serde_json::Value` copy of the (sensitive) lease id.
#[derive(Serialize)]
struct LeaseIdBody<'a> {
    lease_id: &'a str,
}

/// Borrowing request body for renew, with an optional increment.
#[derive(Serialize)]
struct LeaseRenewBody<'a> {
    lease_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    increment: Option<u64>,
}

#[async_trait]
impl VaultLeaseApi for VaultShared {
    async fn read_db_credentials(
        &self,
        mount: &str,
        role: &str,
    ) -> Result<LeasedRead, SecretError> {
        let reference = SecretReference::new(
            SecretProvider::Vault,
            format!("{mount}/creds/{role}"),
        );
        // The dynamic mount/role are interpolated into the endpoint path, so validate
        // them as single safe segments before the request. An invalid value is a hard
        // configuration error (not an absent lease): fail closed.
        if !valid_path_segment(mount) || !valid_path_segment(role) {
            return Err(provider(&reference, ProviderFailureKind::Other));
        }
        let token = self.current_token().await;
        let resp: DbCredsResponse = self
            .get_json(
                self.url(&format!("{mount}/creds/{role}")),
                Some(&token),
                kv_response_cap(self.conn.max_secret_bytes),
                false,
                &reference,
            )
            .await?;

        let limit = self.conn.max_secret_bytes;
        let now = SystemTime::now();
        let expires_at = now
            .checked_add(Duration::from_secs(resp.lease_duration))
            .unwrap_or(now);
        // One issue read -> one resolution group; each field is expiry-aware so the
        // rotation core treats the leased credential as time-bounded.
        let group = ResolutionGroup::next();
        let mut credentials = CredentialSet::new();
        for (k, v) in resp.data {
            let s = match v {
                serde_json::Value::String(s) => s,
                _ => return Err(SecretError::NotUtf8(reference.safe())),
            };
            let material =
                SecretMaterial::Utf8(SecretString::new(s, limit, &reference)?);
            let resolved = ResolvedSecret::new(material)
                .in_group(group)
                .with_expires_at(expires_at)
                .with_renewable(resp.renewable);
            credentials.insert(k, resolved)?;
        }
        Ok(LeasedRead {
            credentials,
            lease_id: resp.lease_id,
            lease_duration: Duration::from_secs(resp.lease_duration),
            renewable: resp.renewable,
        })
    }

    async fn lookup_lease(
        &self,
        lease_id: &str,
    ) -> Result<LeaseInfo, SecretError> {
        let reference = self.reference();
        let token = self.current_token().await;
        let resp: LeaseLookupResponse = self
            .lease_post_json(
                self.url("sys/leases/lookup"),
                Some(&token),
                &LeaseIdBody { lease_id },
                &reference,
            )
            .await?;
        Ok(LeaseInfo {
            lease_duration: Duration::from_secs(resp.data.ttl),
            renewable: resp.data.renewable,
        })
    }

    async fn renew_lease(
        &self,
        lease_id: &str,
        increment_secs: Option<u64>,
    ) -> Result<LeaseInfo, SecretError> {
        let reference = self.reference();
        let token = self.current_token().await;
        let resp: LeaseRenewResponse = self
            .lease_post_json(
                self.url("sys/leases/renew"),
                Some(&token),
                &LeaseRenewBody {
                    lease_id,
                    increment: increment_secs,
                },
                &reference,
            )
            .await?;
        Ok(LeaseInfo {
            lease_duration: Duration::from_secs(resp.lease_duration),
            renewable: resp.renewable,
        })
    }

    async fn revoke_lease(&self, lease_id: &str) -> Result<(), SecretError> {
        let reference = self.reference();
        let token = self.current_token().await;
        // A 2xx (incl. 204) is success; an absent lease maps to `NotFound` so the
        // caller treats revocation as idempotent.
        let req = self.with_headers(
            self.http
                .post(self.url("sys/leases/revoke"))
                .json(&LeaseIdBody { lease_id }),
            Some(&token),
        );
        self.execute_lease(req, &reference).await.map(|_| ())
    }
}

#[async_trait]
impl TokenAuthority for VaultShared {
    async fn authenticate(&self) -> Result<(SecretString, Lease), SecretError> {
        let token = match &self.conn.auth {
            VaultAuth::TokenFile(file) => {
                let resolved = file.read(self.conn.max_secret_bytes).await?;
                let raw = resolved
                    .material()
                    .as_utf8()
                    .ok_or_else(|| {
                        SecretError::NotUtf8(self.reference().safe())
                    })?
                    .to_string();
                self.secret_token(raw)?
            }
            VaultAuth::Kubernetes { mount, role, jwt } => {
                let resolved = jwt.read(self.conn.max_secret_bytes).await?;
                let jwt_str =
                    resolved.material().as_utf8().ok_or_else(|| {
                        SecretError::NotUtf8(self.reference().safe())
                    })?;
                let reference = self.reference();
                // Borrowing request struct: the JWT is serialized straight from the
                // resolved material, never cloned into an owned `serde_json::Value`.
                let body = KubernetesLoginRequest { role, jwt: jwt_str };
                let resp: AuthResponse = self
                    .post_json(
                        self.url(&format!("auth/{mount}/login")),
                        None,
                        &body,
                        CONTROL_RESPONSE_CAP,
                        &reference,
                    )
                    .await?;
                self.secret_token(resp.auth.client_token)?
            }
        };
        // Discover the authoritative lease regardless of auth method (token-file
        // auth reports no login lease; a root token reports ttl=0).
        let lease = self.lookup_self(token.expose_secret()).await?;
        Ok((token, lease))
    }

    async fn renew(&self, token: &str) -> Result<Lease, SecretError> {
        let reference = self.reference();
        let resp: AuthResponse = self
            .post_json(
                self.url("auth/token/renew-self"),
                Some(token),
                &serde_json::json!({}),
                CONTROL_RESPONSE_CAP,
                &reference,
            )
            .await?;
        Ok(Lease {
            ttl: Duration::from_secs(resp.auth.lease_duration),
            renewable: resp.auth.renewable,
        })
    }

    async fn lookup(&self, token: &str) -> Result<Lease, SecretError> {
        self.lookup_self(token).await
    }

    async fn reread_if_rotated(
        &self,
        current: &str,
    ) -> Result<Option<(SecretString, Lease)>, SecretError> {
        // Only file-backed token auth has a cheap change signal (re-read the file).
        // Kubernetes login would mint a new token on every call, so it is not run
        // speculatively; its finite lease drives renewal/reauth instead.
        let VaultAuth::TokenFile(file) = &self.conn.auth else {
            return Ok(None);
        };
        let resolved = file.read(self.conn.max_secret_bytes).await?;
        let raw = resolved
            .material()
            .as_utf8()
            .ok_or_else(|| SecretError::NotUtf8(self.reference().safe()))?;
        if raw == current {
            return Ok(None);
        }
        let token = self.secret_token(raw.to_string())?;
        let lease = self.lookup_self(token.expose_secret()).await?;
        Ok(Some((token, lease)))
    }
}

// --- Vault JSON request/response shapes ---

/// Kubernetes login request. Borrows its fields so the JWT is never copied into an
/// owned intermediate value.
#[derive(Serialize)]
struct KubernetesLoginRequest<'a> {
    role: &'a str,
    jwt: &'a str,
}

#[derive(serde::Deserialize)]
struct KvReadResponse {
    data: KvReadData,
}
#[derive(serde::Deserialize)]
struct KvReadData {
    data: BTreeMap<String, serde_json::Value>,
    metadata: KvMetadata,
}
#[derive(serde::Deserialize)]
struct KvMetadata {
    version: u64,
}

#[derive(serde::Deserialize)]
struct AuthResponse {
    auth: AuthData,
}
#[derive(serde::Deserialize)]
struct AuthData {
    client_token: String,
    lease_duration: u64,
    renewable: bool,
}

#[derive(serde::Deserialize)]
struct LookupResponse {
    data: LookupData,
}
#[derive(serde::Deserialize)]
struct LookupData {
    ttl: u64,
    renewable: bool,
}

// --- error helpers (all redacted; never surface response bodies or tokens) ---

fn conn_reference(address: &str) -> SecretReference {
    SecretReference::new(SecretProvider::Vault, address.to_string())
}

fn provider(
    reference: &SecretReference,
    kind: ProviderFailureKind,
) -> SecretError {
    SecretError::Provider {
        reference: reference.safe(),
        kind,
    }
}

fn unavailable(reference: &SecretReference) -> SecretError {
    provider(reference, ProviderFailureKind::Unavailable)
}

fn map_status(
    status: reqwest::StatusCode,
    reference: &SecretReference,
) -> SecretError {
    let kind = match status.as_u16() {
        401 => ProviderFailureKind::Unauthorized,
        403 => ProviderFailureKind::Forbidden,
        408 | 429 => ProviderFailureKind::Timeout,
        500..=599 => ProviderFailureKind::Unavailable,
        _ => ProviderFailureKind::Other,
    };
    provider(reference, kind)
}

fn send_err(e: &reqwest::Error, reference: &SecretReference) -> SecretError {
    let kind = if e.is_timeout() {
        ProviderFailureKind::Timeout
    } else {
        // Connect/transport failures: unavailable. Never include the error body.
        ProviderFailureKind::Unavailable
    };
    provider(reference, kind)
}

/// Read a response body under `cap`, into a buffer that is scrubbed on drop
/// (bodies carry KV values and client tokens).
async fn read_body_capped(
    mut resp: reqwest::Response,
    cap: usize,
    reference: &SecretReference,
) -> Result<Zeroizing<Vec<u8>>, SecretError> {
    let mut acc = Zeroizing::new(Vec::new());
    loop {
        match resp.chunk().await {
            Ok(Some(chunk)) => {
                if acc.len() + chunk.len() > cap {
                    return Err(provider(
                        reference,
                        ProviderFailureKind::InvalidResponse,
                    ));
                }
                acc.extend_from_slice(&chunk);
            }
            Ok(None) => break,
            Err(_) => {
                return Err(provider(
                    reference,
                    ProviderFailureKind::InvalidResponse,
                ));
            }
        }
    }
    Ok(acc)
}

// -----------------------------------------------------------------------------
// Resolver
// -----------------------------------------------------------------------------

/// A [`SecretResolver`] backed by Vault. Phase 1: KV v2 static reads with pinned
/// versions honored and the record version propagated as `provider_version`.
pub struct VaultResolver {
    api: Arc<dyn VaultApi>,
    /// Vault lease operations (dynamic-credential issue + `sys/leases` lifecycle).
    /// For the live client this is the same [`VaultShared`] as `api`.
    lease: Arc<dyn VaultLeaseApi>,
}

impl std::fmt::Debug for VaultResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VaultResolver").finish_non_exhaustive()
    }
}

impl VaultResolver {
    /// Authenticate, discover the token lease, and start the background
    /// renewal/re-auth task (bound to this resolver's lifetime).
    pub async fn connect(conn: VaultConnection) -> Result<Self, SecretError> {
        let shared = VaultShared::build(conn)?;
        let (token, lease) = shared.authenticate().await?;
        // Secret-free: non-secret lease shape only.
        tracing::debug!(
            ttl_secs = lease.ttl.as_secs(),
            renewable = lease.renewable,
            "vault authenticated; starting token renewal task"
        );
        *shared.token.write().await = token;
        let shared = Arc::new(shared);
        spawn_renewal(Arc::downgrade(&shared), lease);
        Ok(Self {
            api: shared.clone(),
            lease: shared,
        })
    }

    #[cfg(test)]
    pub(crate) fn with_api(api: Arc<dyn VaultApi>) -> Self {
        Self {
            api,
            lease: Arc::new(UnsupportedLease),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_lease_api(lease: Arc<dyn VaultLeaseApi>) -> Self {
        Self {
            api: Arc::new(UnsupportedKv),
            lease,
        }
    }

    /// Read a dynamic (leased) credential, creating a Vault lease. The caller owns
    /// the returned lease id and must persist/renew/revoke it.
    pub async fn read_db_credentials(
        &self,
        mount: &str,
        role: &str,
    ) -> Result<LeasedRead, SecretError> {
        self.lease.read_db_credentials(mount, role).await
    }

    /// Look up a lease's remaining duration and renewability.
    pub async fn lookup_lease(
        &self,
        lease_id: &str,
    ) -> Result<LeaseInfo, SecretError> {
        self.lease.lookup_lease(lease_id).await
    }

    /// Renew a lease, optionally requesting an increment (seconds).
    pub async fn renew_lease(
        &self,
        lease_id: &str,
        increment_secs: Option<u64>,
    ) -> Result<LeaseInfo, SecretError> {
        self.lease.renew_lease(lease_id, increment_secs).await
    }

    /// Revoke a lease.
    pub async fn revoke_lease(
        &self,
        lease_id: &str,
    ) -> Result<(), SecretError> {
        self.lease.revoke_lease(lease_id).await
    }

    /// Split a reference location `"<mount>/<path>"` into `(mount, path)` and
    /// validate both against path-traversal / endpoint-alteration before they are
    /// interpolated into the URL. `mount` is a single segment; `path` may be
    /// nested. Rejects empty, `.`/`..`, control-character, and query/fragment-like
    /// input.
    fn split_location(
        reference: &SecretReference,
    ) -> Result<(&str, &str), SecretError> {
        let (mount, path) = reference
            .location
            .split_once('/')
            .ok_or_else(|| SecretError::NotFound(reference.safe()))?;
        if !valid_vault_path(mount) || !valid_vault_path(path) {
            return Err(SecretError::NotFound(reference.safe()));
        }
        Ok((mount, path))
    }
}

/// Test stub: a resolver built with a mock KV api has no lease backend.
#[cfg(test)]
struct UnsupportedLease;
#[cfg(test)]
#[async_trait]
impl VaultLeaseApi for UnsupportedLease {
    async fn read_db_credentials(
        &self,
        _mount: &str,
        _role: &str,
    ) -> Result<LeasedRead, SecretError> {
        Err(unsupported_stub())
    }
    async fn lookup_lease(
        &self,
        _lease_id: &str,
    ) -> Result<LeaseInfo, SecretError> {
        Err(unsupported_stub())
    }
    async fn renew_lease(
        &self,
        _lease_id: &str,
        _increment_secs: Option<u64>,
    ) -> Result<LeaseInfo, SecretError> {
        Err(unsupported_stub())
    }
    async fn revoke_lease(&self, _lease_id: &str) -> Result<(), SecretError> {
        Err(unsupported_stub())
    }
}

/// Test stub: a resolver built with a mock lease api has no KV backend.
#[cfg(test)]
struct UnsupportedKv;
#[cfg(test)]
#[async_trait]
impl VaultApi for UnsupportedKv {
    async fn read_kv2(
        &self,
        _mount: &str,
        _path: &str,
        _version: Option<&str>,
        _reference: &SecretReference,
    ) -> Result<KvRecord, SecretError> {
        Err(unsupported_stub())
    }
}

#[cfg(test)]
fn unsupported_stub() -> SecretError {
    SecretError::UnsupportedProvider(
        SecretReference::new(SecretProvider::Vault, "stub").safe(),
    )
}

#[async_trait]
impl SecretResolver for VaultResolver {
    async fn resolve(
        &self,
        reference: &SecretReference,
    ) -> Result<ResolvedSecret, SecretError> {
        if reference.provider != SecretProvider::Vault {
            return Err(SecretError::UnsupportedProvider(reference.safe()));
        }
        let (mount, path) = Self::split_location(reference)?;
        let rec = self
            .api
            .read_kv2(mount, path, reference.version.as_deref(), reference)
            .await?;
        let mut record = StructuredSecret::new(rec.fields, rec.version);
        record.take(reference)
    }

    async fn resolve_set(
        &self,
        requests: &[CredentialFieldRequest],
    ) -> Result<CredentialSet, SecretError> {
        // Reject duplicate connector field names before any provider access.
        check_no_duplicate_fields(requests)?;

        // Group by (record location, pinned version): one read per distinct
        // record, and different version pins of the same location are distinct
        // records with distinct provenance. A non-Vault reference fails here,
        // still before any read.
        let mut by_record: BTreeMap<
            (&str, Option<&str>),
            Vec<&CredentialFieldRequest>,
        > = BTreeMap::new();
        for req in requests {
            if req.reference.provider != SecretProvider::Vault {
                return Err(SecretError::UnsupportedProvider(
                    req.reference.safe(),
                ));
            }
            let key = (
                req.reference.location.as_str(),
                req.reference.version.as_deref(),
            );
            by_record.entry(key).or_default().push(req);
        }

        let mut cs = CredentialSet::new();
        for ((_loc, version), reqs) in by_record {
            let reference = &reqs[0].reference;
            let (mount, path) = Self::split_location(reference)?;
            let rec =
                self.api.read_kv2(mount, path, version, reference).await?;
            let record = StructuredSecret::new(rec.fields, rec.version);
            let fields: Vec<(&str, &SecretReference)> = reqs
                .iter()
                .map(|r| (r.field.as_str(), &r.reference))
                .collect();
            // One read per record -> its fields share one resolution group.
            let sub = credential_set_from_record(record, &fields)?;
            for (name, secret) in sub.into_fields() {
                cs.insert(name, secret)?;
            }
        }
        Ok(cs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::material::DEFAULT_MAX_SECRET_BYTES;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // --- KV resolution tests (mock VaultApi) ---

    struct MockApi {
        records: BTreeMap<String, (BTreeMap<String, String>, Option<String>)>,
    }

    #[async_trait]
    impl VaultApi for MockApi {
        async fn read_kv2(
            &self,
            mount: &str,
            path: &str,
            version: Option<&str>,
            reference: &SecretReference,
        ) -> Result<KvRecord, SecretError> {
            // A pinned version selects a distinct key so version routing is
            // observable in tests.
            let key = match version {
                Some(v) => format!("{mount}/{path}@{v}"),
                None => format!("{mount}/{path}"),
            };
            let (rec, ver) = self
                .records
                .get(&key)
                .ok_or_else(|| SecretError::NotFound(reference.safe()))?;
            let mut fields = BTreeMap::new();
            for (k, v) in rec {
                fields.insert(
                    k.clone(),
                    SecretMaterial::Utf8(
                        SecretString::new(
                            v.clone(),
                            DEFAULT_MAX_SECRET_BYTES,
                            reference,
                        )
                        .unwrap(),
                    ),
                );
            }
            Ok(KvRecord {
                fields,
                version: ver.clone(),
            })
        }
    }

    // --- lease client (mock VaultLeaseApi) ---

    struct MockLeaseApi;

    #[async_trait]
    impl VaultLeaseApi for MockLeaseApi {
        async fn read_db_credentials(
            &self,
            _mount: &str,
            _role: &str,
        ) -> Result<LeasedRead, SecretError> {
            let reference =
                SecretReference::new(SecretProvider::Vault, "database/creds/r");
            let expires_at = SystemTime::now() + Duration::from_secs(3600);
            let group = ResolutionGroup::next();
            let mut credentials = CredentialSet::new();
            for (k, v) in [("username", "df-dyn"), ("password", "p@ss")] {
                let material = SecretMaterial::Utf8(
                    SecretString::new(
                        v.to_string(),
                        DEFAULT_MAX_SECRET_BYTES,
                        &reference,
                    )
                    .unwrap(),
                );
                let resolved = ResolvedSecret::new(material)
                    .in_group(group)
                    .with_expires_at(expires_at)
                    .with_renewable(true);
                credentials.insert(k, resolved).unwrap();
            }
            Ok(LeasedRead {
                credentials,
                lease_id: "lease-xyz".to_string(),
                lease_duration: Duration::from_secs(3600),
                renewable: true,
            })
        }
        async fn lookup_lease(
            &self,
            _lease_id: &str,
        ) -> Result<LeaseInfo, SecretError> {
            Ok(LeaseInfo {
                lease_duration: Duration::from_secs(1800),
                renewable: true,
            })
        }
        async fn renew_lease(
            &self,
            _lease_id: &str,
            _increment_secs: Option<u64>,
        ) -> Result<LeaseInfo, SecretError> {
            Ok(LeaseInfo {
                lease_duration: Duration::from_secs(120),
                renewable: true,
            })
        }
        async fn revoke_lease(
            &self,
            _lease_id: &str,
        ) -> Result<(), SecretError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn leased_read_is_grouped_and_expiry_aware() {
        let r = VaultResolver::with_lease_api(Arc::new(MockLeaseApi));
        let read = r
            .read_db_credentials("database", "orders-ro")
            .await
            .unwrap();
        assert_eq!(read.lease_id, "lease-xyz");
        assert!(read.renewable);
        // One issue read -> single provenance group; fields are expiry-aware.
        assert!(read.credentials.single_resolution_group().is_ok());
        let u = read.credentials.require("username").unwrap();
        assert_eq!(u.material().as_utf8(), Some("df-dyn"));
        assert!(u.expires_at().is_some());
        assert!(u.renewable());
    }

    #[tokio::test]
    async fn lease_ops_delegate_to_backend() {
        let r = VaultResolver::with_lease_api(Arc::new(MockLeaseApi));
        assert!(r.lookup_lease("l").await.unwrap().renewable);
        assert_eq!(
            r.renew_lease("l", Some(60)).await.unwrap().lease_duration,
            Duration::from_secs(120)
        );
        r.revoke_lease("l").await.unwrap();
    }

    fn field_ref(location: &str, selector: &str) -> SecretReference {
        SecretReference::new(SecretProvider::Vault, location)
            .with_selector(selector)
    }

    /// (location, fields, record version) for a mocked KV record.
    type RecordSpec<'a> = (&'a str, &'a [(&'a str, &'a str)], Option<&'a str>);

    fn mock(records: &[RecordSpec<'_>]) -> VaultResolver {
        let mut m = BTreeMap::new();
        for (loc, fields, ver) in records {
            m.insert(
                loc.to_string(),
                (
                    fields
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                    ver.map(str::to_string),
                ),
            );
        }
        VaultResolver::with_api(Arc::new(MockApi { records: m }))
    }

    #[tokio::test]
    async fn resolve_selects_field_and_propagates_version() {
        let r = mock(&[("secret/orders", &[("password", "p@ss")], Some("7"))]);
        let got = r
            .resolve(&field_ref("secret/orders", "password"))
            .await
            .unwrap();
        assert_eq!(got.material().as_utf8(), Some("p@ss"));
        // KV v2 record version is propagated for change-polling.
        assert_eq!(got.provider_version(), Some("7"));
        // KV v2 static: no lease expiry.
        assert_eq!(got.expires_at(), None);
    }

    #[tokio::test]
    async fn resolve_honors_pinned_version() {
        let r = mock(&[("secret/orders@3", &[("password", "old")], Some("3"))]);
        let reference =
            field_ref("secret/orders", "password").with_version("3");
        let got = r.resolve(&reference).await.unwrap();
        assert_eq!(got.material().as_utf8(), Some("old"));
        assert_eq!(got.provider_version(), Some("3"));
    }

    #[tokio::test]
    async fn resolve_set_from_one_record_is_atomic() {
        let r = mock(&[(
            "secret/orders",
            &[("username", "df"), ("password", "p@ss")],
            Some("1"),
        )]);
        let cs = r
            .resolve_set(&[
                CredentialFieldRequest::new(
                    "username",
                    field_ref("secret/orders", "username"),
                ),
                CredentialFieldRequest::new(
                    "password",
                    field_ref("secret/orders", "password"),
                ),
            ])
            .await
            .unwrap();
        assert!(
            cs.single_resolution_group().is_ok(),
            "fields from one KV record must share one resolution group"
        );
    }

    #[tokio::test]
    async fn resolve_set_different_versions_are_distinct_records() {
        let r = mock(&[
            ("secret/orders@1", &[("password", "v1")], Some("1")),
            ("secret/orders@2", &[("username", "u")], Some("2")),
        ]);
        let cs = r
            .resolve_set(&[
                CredentialFieldRequest::new(
                    "password",
                    field_ref("secret/orders", "password").with_version("1"),
                ),
                CredentialFieldRequest::new(
                    "username",
                    field_ref("secret/orders", "username").with_version("2"),
                ),
            ])
            .await
            .unwrap();
        // Two distinct version pins => two reads => not one atomic group.
        assert!(cs.single_resolution_group().is_err());
    }

    #[tokio::test]
    async fn whole_dsn_field_resolves() {
        let r =
            mock(&[("secret/orders", &[("dsn", "postgres://u:p@h/db")], None)]);
        let got = r.resolve(&field_ref("secret/orders", "dsn")).await.unwrap();
        assert_eq!(got.material().as_utf8(), Some("postgres://u:p@h/db"));
    }

    #[tokio::test]
    async fn missing_selector_fails_closed() {
        let r = mock(&[("secret/orders", &[("password", "p")], None)]);
        let err = r
            .resolve(&field_ref("secret/orders", "username"))
            .await
            .unwrap_err();
        assert!(matches!(err, SecretError::SelectorMissing { .. }));
    }

    #[tokio::test]
    async fn non_vault_reference_rejected() {
        let r = mock(&[]);
        let reference = SecretReference::new(SecretProvider::File, "secret/x")
            .with_selector("password");
        let err = r.resolve(&reference).await.unwrap_err();
        assert!(matches!(err, SecretError::UnsupportedProvider(_)));
    }

    // --- connection config validation ---

    fn token_auth() -> VaultAuth {
        VaultAuth::TokenFile(VaultAuthFile::Strict {
            path: PathBuf::from("/run/secrets/vault-token"),
        })
    }

    fn conn(
        address: &str,
        insecure: bool,
        max: usize,
        renew: RenewPolicy,
    ) -> Result<VaultConnection, VaultConfigError> {
        VaultConnection::new(
            address,
            None,
            token_auth(),
            max,
            renew,
            VaultTimeouts::default(),
            insecure,
        )
    }

    #[test]
    fn https_address_accepted() {
        assert!(
            conn(
                "https://vault.internal:8200",
                false,
                4096,
                RenewPolicy::default()
            )
            .is_ok()
        );
    }

    #[test]
    fn http_rejected_without_insecure_opt_in() {
        assert_eq!(
            conn("http://vault:8200", false, 4096, RenewPolicy::default())
                .unwrap_err(),
            VaultConfigError::InsecureScheme
        );
        // Explicit dev opt-in permits http.
        assert!(
            conn("http://vault:8200", true, 4096, RenewPolicy::default())
                .is_ok()
        );
    }

    #[test]
    fn address_with_userinfo_rejected() {
        assert_eq!(
            conn(
                "https://user:pass@vault:8200",
                false,
                4096,
                RenewPolicy::default()
            )
            .unwrap_err(),
            VaultConfigError::AddressHasUserinfo
        );
    }

    #[test]
    fn zero_size_limit_and_bad_renew_rejected() {
        assert_eq!(
            conn("https://vault:8200", false, 0, RenewPolicy::default())
                .unwrap_err(),
            VaultConfigError::ZeroSizeLimit
        );
        let bad_margin = RenewPolicy {
            safety_margin: Duration::ZERO,
            ..RenewPolicy::default()
        };
        assert_eq!(
            conn("https://vault:8200", false, 16, bad_margin).unwrap_err(),
            VaultConfigError::ZeroSafetyMargin
        );
        let bad_fraction = RenewPolicy {
            fraction: 1.5,
            ..RenewPolicy::default()
        };
        assert_eq!(
            conn("https://vault:8200", false, 16, bad_fraction).unwrap_err(),
            VaultConfigError::BadFraction
        );
        let bad_interval = RenewPolicy {
            interval: Duration::from_millis(10),
            ..RenewPolicy::default()
        };
        assert_eq!(
            conn("https://vault:8200", false, 16, bad_interval).unwrap_err(),
            VaultConfigError::RenewIntervalTooSmall
        );
    }

    fn conn_timeouts(
        timeouts: VaultTimeouts,
    ) -> Result<VaultConnection, VaultConfigError> {
        VaultConnection::new(
            "https://vault:8200",
            None,
            token_auth(),
            16,
            RenewPolicy::default(),
            timeouts,
            false,
        )
    }

    #[test]
    fn bad_timeouts_rejected() {
        assert_eq!(
            conn_timeouts(VaultTimeouts {
                connect: Duration::ZERO,
                request: Duration::from_secs(5),
            })
            .unwrap_err(),
            VaultConfigError::ZeroTimeout
        );
        assert_eq!(
            conn_timeouts(VaultTimeouts {
                connect: Duration::from_secs(10),
                request: Duration::from_secs(5),
            })
            .unwrap_err(),
            VaultConfigError::RequestTimeoutTooSmall
        );
        assert!(conn_timeouts(VaultTimeouts::default()).is_ok());
    }

    #[test]
    fn bad_kubernetes_auth_rejected() {
        let bad_mount = VaultConnection::new(
            "https://vault:8200",
            None,
            VaultAuth::Kubernetes {
                mount: "../evil".to_string(),
                role: "app".to_string(),
                jwt: VaultAuthFile::Strict {
                    path: PathBuf::from("/run/secrets/token"),
                },
            },
            16,
            RenewPolicy::default(),
            VaultTimeouts::default(),
            false,
        );
        assert_eq!(bad_mount.unwrap_err(), VaultConfigError::InvalidAuthMount);

        let empty_role = VaultConnection::new(
            "https://vault:8200",
            None,
            VaultAuth::Kubernetes {
                mount: "kubernetes".to_string(),
                role: String::new(),
                jwt: VaultAuthFile::Strict {
                    path: PathBuf::from("/run/secrets/token"),
                },
            },
            16,
            RenewPolicy::default(),
            VaultTimeouts::default(),
            false,
        );
        assert_eq!(empty_role.unwrap_err(), VaultConfigError::EmptyAuthRole);
    }

    // --- path validation ---

    #[test]
    fn valid_vault_path_accepts_nested_and_rejects_traversal() {
        assert!(valid_vault_path("secret"));
        assert!(valid_vault_path("secret/orders"));
        assert!(valid_vault_path("team/kv/orders/db"));
        // Rejected shapes.
        assert!(!valid_vault_path(""));
        assert!(!valid_vault_path("secret/"));
        assert!(!valid_vault_path("/secret"));
        assert!(!valid_vault_path("secret//orders"));
        assert!(!valid_vault_path("secret/../admin"));
        assert!(!valid_vault_path("."));
        assert!(!valid_vault_path("secret/./x"));
        assert!(!valid_vault_path("secret/x?version=2"));
        assert!(!valid_vault_path("secret/x#frag"));
        assert!(!valid_vault_path("secret/\u{0007}bell"));
    }

    #[tokio::test]
    async fn resolve_rejects_traversal_location() {
        let r = mock(&[("secret/orders", &[("password", "p")], None)]);
        let reference =
            SecretReference::new(SecretProvider::Vault, "secret/../admin")
                .with_selector("password");
        let err = r.resolve(&reference).await.unwrap_err();
        assert!(matches!(err, SecretError::NotFound(_)));
    }

    // --- HTTP status classification ---

    #[test]
    fn status_maps_to_typed_provider_kinds() {
        use reqwest::StatusCode;
        let reference = conn_reference("https://vault");
        let kind = |code: u16| match map_status(
            StatusCode::from_u16(code).unwrap(),
            &reference,
        ) {
            SecretError::Provider { kind, .. } => kind,
            other => panic!("expected provider error, got {other:?}"),
        };
        assert_eq!(kind(401), ProviderFailureKind::Unauthorized);
        assert_eq!(kind(403), ProviderFailureKind::Forbidden);
        assert_eq!(kind(429), ProviderFailureKind::Timeout);
        assert_eq!(kind(503), ProviderFailureKind::Unavailable);
        assert_eq!(kind(400), ProviderFailureKind::Other);
    }

    #[test]
    fn lease_absent_marker_matches_vault_markers() {
        assert!(lease_absent_marker(br#"{"errors":["invalid lease"]}"#));
        assert!(lease_absent_marker(br#"{"errors":["lease not found"]}"#));
        // Case-insensitive.
        assert!(lease_absent_marker(b"Invalid Lease"));
        // Unrelated errors are not absent.
        assert!(!lease_absent_marker(br#"{"errors":["permission denied"]}"#));
        assert!(!lease_absent_marker(b""));
    }

    #[test]
    fn valid_path_segment_rejects_endpoint_alteration() {
        assert!(valid_path_segment("database"));
        assert!(valid_path_segment("orders-ro"));
        // No embedded slash, traversal, control, or query/fragment markers.
        assert!(!valid_path_segment("db/creds"));
        assert!(!valid_path_segment(".."));
        assert!(!valid_path_segment(""));
        assert!(!valid_path_segment("role?x"));
        assert!(!valid_path_segment("role#x"));
    }

    // --- renewal scheduling (pure) ---

    fn policy() -> RenewPolicy {
        RenewPolicy {
            interval: Duration::from_secs(600),
            safety_margin: Duration::from_secs(10),
            fraction: 0.5,
            jitter: 0.0, // deterministic
            min_delay: Duration::from_secs(1),
        }
    }

    #[test]
    fn schedule_long_ttl_uses_fraction_capped_by_interval() {
        // remaining 100s, fraction 0.5 -> 50s; margin gives 90s; interval 600s.
        assert_eq!(
            renew_delay(Duration::from_secs(100), false, &policy(), 0),
            Duration::from_secs(50)
        );
    }

    #[test]
    fn schedule_huge_ttl_capped_by_interval() {
        // fraction*remaining and margin are both huge; the interval cap binds.
        assert_eq!(
            renew_delay(Duration::from_secs(1_000_000), false, &policy(), 0),
            Duration::from_secs(600)
        );
    }

    #[test]
    fn schedule_short_ttl_collapses_to_floor_not_interval() {
        // remaining 5s < margin 10s: by_margin saturates to 0 -> floored to
        // min_delay, so a short-lived token renews promptly.
        assert_eq!(
            renew_delay(Duration::from_secs(5), false, &policy(), 0),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn schedule_retry_grows_urgent_as_remaining_shrinks() {
        // As the old token's remaining lifetime shrinks (after failed attempts),
        // the delay shrinks too - bounded by the remaining safe window, never the
        // 600s interval.
        assert_eq!(
            renew_delay(Duration::from_secs(40), false, &policy(), 0),
            Duration::from_secs(20) // fraction 0.5 of 40
        );
        assert_eq!(
            renew_delay(Duration::from_secs(12), false, &policy(), 0),
            Duration::from_secs(2) // margin (12-10) binds below fraction (6)
        );
        assert_eq!(
            renew_delay(Duration::ZERO, false, &policy(), 0),
            Duration::from_secs(1) // expired -> floor, still not the interval
        );
    }

    #[test]
    fn schedule_zero_ttl_rechecks_at_interval() {
        assert_eq!(
            renew_delay(Duration::ZERO, true, &policy(), 0),
            Duration::from_secs(600)
        );
        assert_eq!(
            renew_mode(&Lease {
                ttl: Duration::ZERO,
                renewable: false,
            }),
            RenewMode::Recheck
        );
    }

    #[test]
    fn jitter_only_reduces_delay_within_bound() {
        let mut p = policy();
        p.jitter = 0.2;
        let base = Duration::from_secs(50); // fraction 0.5 of 100
        for seed in [0u64, 1, 250, 500, 999] {
            let d = renew_delay(Duration::from_secs(100), false, &p, seed);
            assert!(d <= base, "jitter must not increase the delay");
            assert!(
                d >= base.mul_f32(0.8),
                "jitter must not exceed the configured 20%"
            );
        }
    }

    // --- token lifecycle driver (mock TokenAuthority) ---

    struct MockAuthority {
        // Each authenticate() hands out the next token (simulating a rotated
        // file/JWT), with the given lease.
        tokens: std::sync::Mutex<Vec<String>>,
        auth_lease: Lease,
        renew_result: RenewBehavior,
        lookup_result: LookupBehavior,
        // When set and the current token differs, reread_if_rotated adopts it once.
        rotated: std::sync::Mutex<Option<(String, Lease)>>,
        auth_count: AtomicUsize,
        renew_count: AtomicUsize,
        lookup_count: AtomicUsize,
        reread_count: AtomicUsize,
    }

    #[derive(Clone, Copy)]
    enum RenewBehavior {
        Ok(Lease),
        Fail,
    }

    #[derive(Clone, Copy)]
    enum LookupBehavior {
        Ok(Lease),
        Fail,
    }

    impl MockAuthority {
        fn new(
            tokens: &[&str],
            auth_lease: Lease,
            renew_result: RenewBehavior,
        ) -> Self {
            Self {
                tokens: std::sync::Mutex::new(
                    tokens.iter().rev().map(|s| s.to_string()).collect(),
                ),
                auth_lease,
                renew_result,
                lookup_result: LookupBehavior::Fail,
                rotated: std::sync::Mutex::new(None),
                auth_count: AtomicUsize::new(0),
                renew_count: AtomicUsize::new(0),
                lookup_count: AtomicUsize::new(0),
                reread_count: AtomicUsize::new(0),
            }
        }

        fn with_lookup(mut self, lookup: LookupBehavior) -> Self {
            self.lookup_result = lookup;
            self
        }

        fn with_rotation(self, token: &str, lease: Lease) -> Self {
            *self.rotated.lock().unwrap() = Some((token.to_string(), lease));
            self
        }
    }

    fn mock_secret(raw: &str) -> SecretString {
        SecretString::new(
            raw.to_string(),
            DEFAULT_MAX_SECRET_BYTES,
            &conn_reference("https://vault"),
        )
        .unwrap()
    }

    #[async_trait]
    impl TokenAuthority for MockAuthority {
        async fn authenticate(
            &self,
        ) -> Result<(SecretString, Lease), SecretError> {
            self.auth_count.fetch_add(1, Ordering::Relaxed);
            let mut toks = self.tokens.lock().unwrap();
            let raw = toks.pop().ok_or_else(|| {
                provider(
                    &conn_reference("https://vault"),
                    ProviderFailureKind::Unavailable,
                )
            })?;
            Ok((mock_secret(&raw), self.auth_lease))
        }

        async fn renew(&self, _token: &str) -> Result<Lease, SecretError> {
            self.renew_count.fetch_add(1, Ordering::Relaxed);
            match self.renew_result {
                RenewBehavior::Ok(l) => Ok(l),
                RenewBehavior::Fail => Err(provider(
                    &conn_reference("https://vault"),
                    ProviderFailureKind::Unavailable,
                )),
            }
        }

        async fn lookup(&self, _token: &str) -> Result<Lease, SecretError> {
            self.lookup_count.fetch_add(1, Ordering::Relaxed);
            match self.lookup_result {
                LookupBehavior::Ok(l) => Ok(l),
                LookupBehavior::Fail => Err(provider(
                    &conn_reference("https://vault"),
                    ProviderFailureKind::Unauthorized,
                )),
            }
        }

        async fn reread_if_rotated(
            &self,
            current: &str,
        ) -> Result<Option<(SecretString, Lease)>, SecretError> {
            self.reread_count.fetch_add(1, Ordering::Relaxed);
            let mut slot = self.rotated.lock().unwrap();
            match slot.as_ref() {
                Some((tok, lease)) if tok != current => {
                    let adopted = (mock_secret(tok), *lease);
                    *slot = None; // adopt once
                    Ok(Some(adopted))
                }
                _ => Ok(None),
            }
        }
    }

    fn cell(initial: &str) -> RwLock<SecretString> {
        RwLock::new(
            SecretString::new(
                initial.to_string(),
                DEFAULT_MAX_SECRET_BYTES,
                &conn_reference("https://vault"),
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn tick_renews_renewable_token_in_place() {
        let renewed = Lease {
            ttl: Duration::from_secs(200),
            renewable: true,
        };
        let authority = MockAuthority::new(
            &["should-not-be-used"],
            Lease {
                ttl: Duration::from_secs(100),
                renewable: true,
            },
            RenewBehavior::Ok(renewed),
        );
        let token = cell("current");
        let mut lease = Lease {
            ttl: Duration::from_secs(100),
            renewable: true,
        };
        let outcome = drive_tick(&authority, &token, &mut lease).await;
        assert_eq!(outcome, TickOutcome::Renewed);
        assert_eq!(lease, renewed);
        // Renew does not change the token identity, and never re-authenticates.
        assert_eq!(token.read().await.expose_secret(), "current");
        assert_eq!(authority.auth_count.load(Ordering::Relaxed), 0);
        assert_eq!(authority.renew_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn tick_reauthenticates_non_renewable_token() {
        let authority = MockAuthority::new(
            &["fresh-from-rotated-file"],
            Lease {
                ttl: Duration::from_secs(300),
                renewable: true,
            },
            RenewBehavior::Ok(Lease {
                ttl: Duration::from_secs(1),
                renewable: true,
            }),
        );
        let token = cell("stale");
        let mut lease = Lease {
            ttl: Duration::from_secs(60),
            renewable: false, // finite, non-renewable -> must reissue
        };
        let outcome = drive_tick(&authority, &token, &mut lease).await;
        assert_eq!(outcome, TickOutcome::Reauthenticated);
        // The rotated token/JWT was re-read; renew was never attempted.
        assert_eq!(
            token.read().await.expose_secret(),
            "fresh-from-rotated-file"
        );
        assert_eq!(authority.renew_count.load(Ordering::Relaxed), 0);
        assert_eq!(authority.auth_count.load(Ordering::Relaxed), 1);
        assert_eq!(lease.ttl, Duration::from_secs(300));
    }

    #[tokio::test]
    async fn tick_falls_back_to_reauth_when_renew_fails() {
        let authority = MockAuthority::new(
            &["fresh-token"],
            Lease {
                ttl: Duration::from_secs(300),
                renewable: true,
            },
            RenewBehavior::Fail,
        );
        let token = cell("expiring");
        let mut lease = Lease {
            ttl: Duration::from_secs(100),
            renewable: true,
        };
        let outcome = drive_tick(&authority, &token, &mut lease).await;
        // Renew failed, so it re-authenticated and re-read the token.
        assert_eq!(outcome, TickOutcome::Reauthenticated);
        assert_eq!(token.read().await.expose_secret(), "fresh-token");
        assert_eq!(authority.renew_count.load(Ordering::Relaxed), 1);
        assert_eq!(authority.auth_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn tick_fails_closed_when_renew_and_reauth_both_fail() {
        let authority = MockAuthority::new(
            &[], // authenticate() will fail (no tokens left)
            Lease {
                ttl: Duration::from_secs(300),
                renewable: true,
            },
            RenewBehavior::Fail,
        );
        let token = cell("last-known-good");
        let mut lease = Lease {
            ttl: Duration::from_secs(100),
            renewable: true,
        };
        let outcome = drive_tick(&authority, &token, &mut lease).await;
        assert_eq!(outcome, TickOutcome::Failed);
        // The last-known-good token is retained; reads then fail closed upstream.
        assert_eq!(token.read().await.expose_secret(), "last-known-good");
    }

    #[tokio::test]
    async fn recheck_healthy_non_expiring_token_stays_valid() {
        // ttl=0, lookup-self confirms the token is still good.
        let authority = MockAuthority::new(
            &["unused"],
            Lease {
                ttl: Duration::from_secs(300),
                renewable: true,
            },
            RenewBehavior::Fail,
        )
        .with_lookup(LookupBehavior::Ok(Lease {
            ttl: Duration::ZERO,
            renewable: false,
        }));
        let token = cell("root");
        let mut lease = Lease {
            ttl: Duration::ZERO,
            renewable: false,
        };
        let outcome = drive_tick(&authority, &token, &mut lease).await;
        assert_eq!(outcome, TickOutcome::Rechecked);
        // Verified server-side; not re-authenticated; token retained.
        assert_eq!(authority.lookup_count.load(Ordering::Relaxed), 1);
        assert_eq!(authority.auth_count.load(Ordering::Relaxed), 0);
        assert_eq!(token.read().await.expose_secret(), "root");
    }

    #[tokio::test]
    async fn recheck_recovers_revoked_non_expiring_token() {
        // ttl=0 token revoked server-side (lookup fails) -> reauthenticate.
        let authority = MockAuthority::new(
            &["fresh-after-revocation"],
            Lease {
                ttl: Duration::ZERO,
                renewable: false,
            },
            RenewBehavior::Fail,
        )
        .with_lookup(LookupBehavior::Fail);
        let token = cell("revoked");
        let mut lease = Lease {
            ttl: Duration::ZERO,
            renewable: false,
        };
        let outcome = drive_tick(&authority, &token, &mut lease).await;
        assert_eq!(outcome, TickOutcome::Reauthenticated);
        assert_eq!(
            token.read().await.expose_secret(),
            "fresh-after-revocation"
        );
        assert_eq!(authority.auth_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn recheck_adopts_rotated_file_token_while_old_valid() {
        // The on-disk token changed; adopt it proactively without waiting for the
        // old (still-valid) token to be revoked, and without re-authenticating.
        let authority = MockAuthority::new(
            &["should-not-be-used"],
            Lease {
                ttl: Duration::from_secs(1),
                renewable: false,
            },
            RenewBehavior::Fail,
        )
        .with_lookup(LookupBehavior::Ok(Lease {
            ttl: Duration::ZERO,
            renewable: false,
        }))
        .with_rotation(
            "rotated-on-disk",
            Lease {
                ttl: Duration::ZERO,
                renewable: false,
            },
        );
        let token = cell("old-but-valid");
        let mut lease = Lease {
            ttl: Duration::ZERO,
            renewable: false,
        };
        let outcome = drive_tick(&authority, &token, &mut lease).await;
        assert_eq!(outcome, TickOutcome::Reauthenticated);
        assert_eq!(token.read().await.expose_secret(), "rotated-on-disk");
        // Adopted via the file re-read, not via a full authenticate() or lookup.
        assert_eq!(authority.reread_count.load(Ordering::Relaxed), 1);
        assert_eq!(authority.auth_count.load(Ordering::Relaxed), 0);
        assert_eq!(authority.lookup_count.load(Ordering::Relaxed), 0);
    }
}

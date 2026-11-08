//! Enforcement of the retained-state binding
//! (`docs/design/retained-state-compatibility.md`).
//!
//! One record per source id holds the canonical form of the spec that its
//! retained state was written under ([`deltaforge_config::state_binding`]).
//! A start compares the spec with it before anything reads or writes that
//! state; PATCH compares before anything is stopped; DELETE removes it only
//! when no protected state remains.
//!
//! The rule: **a binding is authoritative while any binding-protected
//! state exists for its source id or pipeline.** Protected state is looked
//! for in the state store and, when the store holds none, on the source
//! server (a PostgreSQL slot) and at the sinks (an S3 durable_v2 HEAD): a
//! missing local record never proves a fresh start. When protected state
//! cannot be determined, the start fails closed.

use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use deltaforge_config::state_binding::{
    self, Difference, Rejection, ResolvedEndpoints, StateBinding,
};
use deltaforge_config::{PipelineSpec, S3Durability, SinkCfg, SourceCfg};
use sha2::{Digest, Sha256};
use storage::{ArcStorageBackend, CHECKPOINTS_NS};

/// The slot namespace of bindings, keyed by source id.
pub const NAMESPACE: &str = "state_binding";
/// Where adoptions are audited.
pub const AUDIT_NAMESPACE: &str = "state_binding.audit";

/// Why a binding check refuses.
#[derive(Debug)]
pub enum BindingError {
    /// A spec field or name cannot be classified (a validation error).
    Unclassified(Vec<Rejection>),
    /// The spec differs from the binding while protected state exists, or
    /// while the recorded destination's state cannot be verified absent
    /// (`unverifiable`).
    Incompatible {
        source_id: String,
        differences: Vec<Difference>,
        unverifiable: bool,
    },
    /// Protected state exists with no binding: adopt it first.
    Unbound {
        source_id: String,
        found: Vec<Found>,
    },
    /// Whether protected state exists cannot be determined.
    Undetermined(String),
}

impl std::fmt::Display for BindingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BindingError::Unclassified(r) => {
                write!(
                    f,
                    "the spec cannot be classified for its retained-state binding: "
                )?;
                join(f, r.iter().map(ToString::to_string))
            }
            BindingError::Incompatible {
                source_id,
                differences,
                unverifiable,
            } => {
                if *unverifiable {
                    write!(
                        f,
                        "retained_state_incompatible: source id '{source_id}' \
                         is bound to a different spec whose source slot or \
                         durable sink state lives where the new spec cannot \
                         check it, so that state cannot be verified absent; \
                         nothing was claimed, started or written. Differences: "
                    )?;
                } else {
                    write!(
                        f,
                        "retained_state_incompatible: source id '{source_id}' \
                         has retained state written under a different spec; \
                         nothing was claimed, started or written. Differences: "
                    )?;
                }
                join(f, differences.iter().map(ToString::to_string))?;
                write!(
                    f,
                    ". Use the recorded values, or a new source id for a new \
                     source lifecycle (DELETE does not remove all retained \
                     state: see \"Delete Pipeline\" in the API reference)"
                )
            }
            BindingError::Unbound { source_id, found } => {
                write!(
                    f,
                    "retained_state_unbound: source id '{source_id}' has \
                     retained state with no recorded binding ("
                )?;
                join(f, found.iter().map(|x| x.kind.to_string()))?;
                write!(
                    f,
                    "); nothing was claimed, started or written. Bind it \
                     once with `deltaforge state-binding adopt --spec <file>` \
                     (dry run, then --apply --expect-proof), or use a new \
                     source id"
                )
            }
            BindingError::Undetermined(why) => write!(
                f,
                "retained_state_undetermined: whether retained state exists \
                 cannot be determined ({why}); nothing was claimed, started \
                 or written. Retry once the state store, the source server \
                 and the durable sinks' buckets are reachable"
            ),
        }
    }
}

fn join(
    f: &mut std::fmt::Formatter<'_>,
    items: impl Iterator<Item = String>,
) -> std::fmt::Result {
    let items: Vec<String> = items.collect();
    f.write_str(&items.join("; "))
}

/// One piece of protected state found: what it is, where, and a digest of
/// its content (for the adoption proof; never its content).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Found {
    pub kind: &'static str,
    pub key: String,
    pub digest: String,
}

fn found(kind: &'static str, key: impl Into<String>, content: &[u8]) -> Found {
    Found {
        kind,
        key: key.into(),
        digest: hex(&Sha256::digest(content)),
    }
}

/// Length-prefixed, so adjacent parts cannot run together.
fn framed(h: &mut Sha256, part: &[u8]) {
    h.update((part.len() as u64).to_be_bytes());
    h.update(part);
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// Protected state outside the state store: a PostgreSQL slot, an S3
/// durable_v2 HEAD. An error means it cannot be told.
#[async_trait]
pub trait ExternalState: Send + Sync {
    async fn protected(&self, spec: &PipelineSpec) -> Result<Vec<Found>>;
}

/// The live probes: the source server and the durable sinks' buckets.
pub struct LiveExternalState;

#[async_trait]
impl ExternalState for LiveExternalState {
    async fn protected(&self, spec: &PipelineSpec) -> Result<Vec<Found>> {
        let mut out = Vec::new();
        let durable: Vec<&deltaforge_config::S3SinkCfg> = spec
            .spec
            .sinks
            .iter()
            .filter_map(|s| match s {
                SinkCfg::S3(c) if c.durability == S3Durability::DurableV2 => {
                    Some(c)
                }
                _ => None,
            })
            .collect();
        let pg = matches!(spec.spec.source, SourceCfg::Postgres(_));
        if !pg && durable.is_empty() {
            return Ok(out);
        }
        let resolver = sources::source_secret_resolver(spec)
            .await
            .context("resolve the secret providers")?;
        if let SourceCfg::Postgres(c) = &spec.spec.source {
            let dsn = sources::resolve_source_dsn(spec, resolver.as_ref())
                .await
                .context("resolve the source DSN")?;
            // Its identity and state, not only its name: a slot dropped and
            // recreated, moved or consumed under the same name differs.
            if let Some(state) =
                sources::postgres::postgres_slot_owner::slot_state(
                    dsn.expose(),
                    &c.slot,
                )
                .await
                .context("look for the replication slot")?
            {
                out.push(found(
                    "postgresql_replication_slot",
                    &c.slot,
                    state.as_bytes(),
                ));
            }
        }
        if !durable.is_empty() {
            let secrets = sinks::resolve_sink_secrets(spec, resolver.as_ref())
                .await
                .context("resolve the sink credentials")?;
            for cfg in durable {
                // The HEAD's content: a HEAD rewritten under the same key
                // differs.
                if let Some(head) = sinks::s3::durable_state(
                    cfg,
                    secrets.for_sink(&cfg.id),
                    &spec.metadata.name,
                )
                .await
                .with_context(|| {
                    format!("look for sink '{}' durable state", cfg.id)
                })? {
                    let key = format!(
                        "{}/{}/{}/{}",
                        cfg.endpoint.as_deref().unwrap_or("aws"),
                        cfg.bucket,
                        cfg.prefix,
                        spec.metadata.name
                    );
                    out.push(found("s3_durable_head", key, &head));
                }
            }
        }
        Ok(out)
    }
}

/// The values a binding needs from secrets (the PostgreSQL database name,
/// a Redis URI's host and database), resolved only when the spec gives
/// them through a secret.
pub async fn resolve_endpoints(
    spec: &PipelineSpec,
) -> Result<ResolvedEndpoints> {
    let pg_secret = matches!(
        &spec.spec.source,
        SourceCfg::Postgres(c) if c.dsn.is_none()
    );
    let redis_secret = spec
        .spec
        .sinks
        .iter()
        .any(|s| matches!(s, SinkCfg::Redis(r) if r.uri_secret.is_some()));
    let mut out = ResolvedEndpoints::default();
    if !pg_secret && !redis_secret {
        return Ok(out);
    }
    let resolver = sources::source_secret_resolver(spec)
        .await
        .context("resolve the secret providers")?;
    if pg_secret {
        let dsn = sources::resolve_source_dsn(spec, resolver.as_ref())
            .await
            .context("resolve the source DSN")?;
        out.source_dsn = Some(dsn.expose().to_string());
    }
    if redis_secret {
        let secrets = sinks::resolve_sink_secrets(spec, resolver.as_ref())
            .await
            .context("resolve the sink credentials")?;
        for s in &spec.spec.sinks {
            if let SinkCfg::Redis(r) = s
                && r.uri_secret.is_some()
                && let Some(uri) = secrets.for_sink(&r.id).get("uri")
            {
                out.sink_uris.insert(r.id.clone(), uri.to_string());
            }
        }
    }
    Ok(out)
}

/// The binding of `source_id` and its slot version; a malformed record
/// fails closed.
pub async fn read(
    backend: &ArcStorageBackend,
    source_id: &str,
) -> Result<Option<(u64, StateBinding)>> {
    match backend.slot_get(NAMESPACE, source_id).await? {
        None => Ok(None),
        Some((v, bytes)) => {
            let b: StateBinding = serde_json::from_slice(&bytes)
                .context("the state binding record is malformed")?;
            if b.format_version != state_binding::FORMAT_VERSION {
                anyhow::bail!(
                    "the state binding record has format {}, this release \
                     reads {}",
                    b.format_version,
                    state_binding::FORMAT_VERSION
                );
            }
            Ok(Some((v, b)))
        }
    }
}

/// Binding-protected state in the state store for `source_id` and the
/// pipelines `pipelines` (pipeline-keyed state: ownership records, DLQ).
pub async fn local_protected(
    backend: &ArcStorageBackend,
    source_id: &str,
    pipelines: &[&str],
) -> Result<Vec<Found>> {
    let mut out = Vec::new();
    let sink_prefix = format!("{source_id}::sink::");
    for key in backend.kv_list(CHECKPOINTS_NS, Some(&sink_prefix)).await? {
        let v = backend
            .kv_get(CHECKPOINTS_NS, &key)
            .await?
            .unwrap_or_default();
        out.push(found("sink_checkpoint", key, &v));
    }
    for (kind, key) in [
        ("legacy_checkpoint", source_id.to_string()),
        (
            "snapshot_progress",
            format!("snapshot_progress:{source_id}"),
        ),
        (
            "snapshot_progress",
            sources::mysql::mysql_snapshot::progress_key(source_id),
        ),
        ("postgresql_slot_owner", format!("slot_owner:{source_id}")),
    ] {
        if let Some(v) = backend.kv_get(CHECKPOINTS_NS, &key).await? {
            out.push(found(kind, key, &v));
        }
    }
    for (kind, ns, key) in [
        (
            "snapshot_generation",
            sources::snapshot_queue::CONTROL_NS,
            sources::snapshot_queue::control_key(source_id),
        ),
        (
            "recovery_authorization",
            sources::snapshot_recovery::AUTHORIZATION_NS,
            format!("pg_slot_recreation:{source_id}"),
        ),
    ] {
        if let Some((version, v)) = backend.slot_get(ns, &key).await? {
            let mut content = version.to_be_bytes().to_vec();
            content.extend_from_slice(&v);
            out.push(found(kind, key, &content));
        }
    }
    // The snapshot plan: every item, with its version and content.
    let plan_prefix = format!("{}/", hex(source_id.as_bytes()));
    let mut plan = Sha256::new();
    let mut items = 0u64;
    let mut cursor: Option<String> = None;
    loop {
        let page = backend
            .slot_list(
                sources::snapshot_queue::PLAN_NS,
                Some(&plan_prefix),
                cursor.as_deref(),
                storage::SLOT_LIST_MAX_LIMIT,
            )
            .await?;
        for r in &page.records {
            items += 1;
            framed(&mut plan, r.key.as_bytes());
            plan.update(r.version.to_be_bytes());
            framed(&mut plan, &r.value);
        }
        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    if items > 0 {
        out.push(Found {
            kind: "snapshot_plan",
            key: format!("{plan_prefix} ({items} items)"),
            digest: hex(&plan.finalize()),
        });
    }
    for pipeline in pipelines {
        // Ownership records: each with its version and content.
        let prefix = sinks::ownership_prefix(pipeline);
        let mut cursor: Option<String> = None;
        loop {
            let page = backend
                .slot_list(
                    crate::destination_ownership::NAMESPACE,
                    Some(&prefix),
                    cursor.as_deref(),
                    storage::SLOT_LIST_MAX_LIMIT,
                )
                .await?;
            for r in &page.records {
                let mut content = r.version.to_be_bytes().to_vec();
                content.extend_from_slice(&r.value);
                out.push(found(
                    "destination_ownership",
                    r.key.clone(),
                    &content,
                ));
            }
            match page.next_cursor {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        // DLQ entries: every entry's id and content.
        let dlq = format!("{pipeline}:dlq");
        let len = backend.queue_len(crate::dlq::JOURNAL_NS, &dlq).await?;
        if len > 0 {
            let mut h = Sha256::new();
            let entries = backend
                .queue_peek(crate::dlq::JOURNAL_NS, &dlq, len as usize)
                .await?;
            for (id, value) in &entries {
                h.update(id.to_be_bytes());
                framed(&mut h, value);
            }
            out.push(Found {
                kind: "dlq_entries",
                key: format!("{dlq} ({} entries)", entries.len()),
                digest: hex(&h.finalize()),
            });
        }
    }
    out.sort();
    out.dedup();
    Ok(out)
}

/// All protected state: the store's, and when it holds none, the external
/// probes'.
async fn protected(
    backend: &ArcStorageBackend,
    external: &dyn ExternalState,
    spec: &PipelineSpec,
    pipelines: &[&str],
) -> Result<Vec<Found>, BindingError> {
    let source_id = spec.spec.source.source_id();
    let local = local_protected(backend, source_id, pipelines)
        .await
        .map_err(|e| {
            BindingError::Undetermined(format!("state store: {e:#}"))
        })?;
    if !local.is_empty() {
        return Ok(local);
    }
    external
        .protected(spec)
        .await
        .map_err(|e| BindingError::Undetermined(sanitized(&format!("{e:#}"))))
}

/// An error text with any userinfo password or `password=` pair removed
/// (a backstop: the probes' errors are not expected to carry one).
fn sanitized(s: &str) -> String {
    s.split(' ')
        .map(|t| {
            if t.contains("://") {
                common::dsn::redact_url_password(t)
            } else if t.to_ascii_lowercase().contains("password=") {
                common::dsn::redact_keyvalue_password(t)
            } else {
                t.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Whether probing the new spec reaches exactly the external state the
/// recorded binding could have left: never for PostgreSQL (the binding does
/// not record the server, which a new DSN may change), and for each durable
/// S3 sink only at the same endpoint, region, bucket and prefix under the
/// same pipeline name.
fn probes_the_recorded_state(
    stored: &StateBinding,
    new: &StateBinding,
) -> bool {
    if stored.engine == "postgres" {
        return false;
    }
    let empty = serde_json::Map::new();
    let sinks = |b: &StateBinding| {
        b.canonical["sinks"]
            .as_object()
            .cloned()
            .unwrap_or_else(|| empty.clone())
    };
    let (old, now) = (sinks(stored), sinks(new));
    for (id, sink) in &old {
        let durable = sink["type"] == "s3"
            && sink["durability"]
                .as_str()
                .is_none_or(|d| d == "durable_v2");
        if !durable {
            continue;
        }
        let Some(next) = now.get(id) else {
            return false;
        };
        let same = stored.pipeline == new.pipeline
            && [
                "type",
                "endpoint",
                "region",
                "bucket",
                "prefix",
                "local",
                "durability",
            ]
            .iter()
            .all(|k| sink.get(*k) == next.get(*k));
        if !same {
            return false;
        }
    }
    true
}

/// What a start found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The binding matches the spec.
    Matched,
    /// A first start: the binding was created.
    Created,
    /// A stale binding (no protected state) was replaced.
    Replaced,
}

/// The binding of `spec`, or why it cannot be classified.
pub async fn binding_of(
    spec: &PipelineSpec,
) -> Result<StateBinding, BindingError> {
    state_binding::validate(spec).map_err(BindingError::Unclassified)?;
    let resolved = resolve_endpoints(spec).await.map_err(|e| {
        BindingError::Undetermined(sanitized(&format!(
            "resolving the endpoints the binding records: {e:#}"
        )))
    })?;
    state_binding::binding(spec, &resolved).map_err(BindingError::Unclassified)
}

/// The start check: compare with the binding, or create it on a first
/// start. Runs before the source id is claimed and before anything reads
/// or writes retained state; a refusal leaves everything as it was.
pub async fn check_and_bind(
    backend: &ArcStorageBackend,
    external: &dyn ExternalState,
    spec: &PipelineSpec,
) -> Result<Outcome, BindingError> {
    let new = binding_of(spec).await?;
    let source_id = new.source_id.clone();
    let store_err = |e: anyhow::Error| {
        BindingError::Undetermined(format!("state store: {e:#}"))
    };
    for _ in 0..2 {
        match read(backend, &source_id).await.map_err(store_err)? {
            Some((version, stored)) => {
                let differences = state_binding::diff(&stored, &new);
                if differences.is_empty() {
                    return Ok(Outcome::Matched);
                }
                let local = local_protected(
                    backend,
                    &source_id,
                    &[&stored.pipeline, &new.pipeline],
                )
                .await
                .map_err(store_err)?;
                if !local.is_empty() {
                    return Err(BindingError::Incompatible {
                        source_id,
                        differences,
                        unverifiable: false,
                    });
                }
                // The new spec's probes reach only its own slot and
                // buckets: an empty new destination proves nothing about
                // the recorded one. Replace only when the recorded external
                // state is exactly what the new spec probes.
                if !probes_the_recorded_state(&stored, &new) {
                    return Err(BindingError::Incompatible {
                        source_id,
                        differences,
                        unverifiable: true,
                    });
                }
                let found = external.protected(spec).await.map_err(|e| {
                    BindingError::Undetermined(sanitized(&format!("{e:#}")))
                })?;
                if !found.is_empty() {
                    return Err(BindingError::Incompatible {
                        source_id,
                        differences,
                        unverifiable: false,
                    });
                }
                // Stale: nothing it protects remains.
                let bytes =
                    serde_json::to_vec(&new).expect("binding serializes");
                if backend
                    .slot_cas(NAMESPACE, &source_id, version, &bytes)
                    .await
                    .map_err(store_err)?
                {
                    return Ok(Outcome::Replaced);
                }
            }
            None => {
                let found =
                    protected(backend, external, spec, &[&new.pipeline])
                        .await?;
                if !found.is_empty() {
                    return Err(BindingError::Unbound { source_id, found });
                }
                let bytes =
                    serde_json::to_vec(&new).expect("binding serializes");
                if backend
                    .slot_create(NAMESPACE, &source_id, &bytes)
                    .await
                    .map_err(store_err)?
                    .is_some()
                {
                    return Ok(Outcome::Created);
                }
            }
        }
        // Changed meanwhile: read it again once.
    }
    Err(BindingError::Undetermined(
        "the binding changed while it was being checked".into(),
    ))
}

/// The PATCH check: the merged spec against the binding (or, without one,
/// against the running spec), before anything is stopped. Never writes.
pub async fn check_patch(
    backend: &ArcStorageBackend,
    old: &PipelineSpec,
    new: &PipelineSpec,
) -> Result<(), BindingError> {
    let new_b = binding_of(new).await?;
    let base =
        match read(backend, old.spec.source.source_id())
            .await
            .map_err(|e| {
                BindingError::Undetermined(format!("state store: {e:#}"))
            })? {
            Some((_, b)) => b,
            None => binding_of(old).await?,
        };
    let differences = state_binding::diff(&base, &new_b);
    if differences.is_empty() {
        Ok(())
    } else {
        Err(BindingError::Incompatible {
            source_id: base.source_id,
            differences,
            unverifiable: false,
        })
    }
}

/// After a delete: remove the binding when no protected state remains.
/// Kept (authoritative, or stale and replaced at the next first start)
/// when any does, or when that cannot be told.
pub async fn release_if_unprotected(
    backend: &ArcStorageBackend,
    external: &dyn ExternalState,
    spec: &PipelineSpec,
) -> Result<bool> {
    let source_id = spec.spec.source.source_id();
    let Some((_, stored)) = read(backend, source_id).await? else {
        return Ok(false);
    };
    let found = protected(
        backend,
        external,
        spec,
        &[&stored.pipeline, &spec.metadata.name],
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    if !found.is_empty() {
        return Ok(false);
    }
    backend.slot_delete(NAMESPACE, source_id).await
}

// ── Adoption ───────────────────────────────────────────────────────────────

/// The plan of one adoption: the binding it would write, the protected
/// state found, what was verified against the spec and what is adopted on
/// the operator's statement, and the proof over all of it.
#[derive(Debug, Clone, serde::Serialize)]
pub struct AdoptPlan {
    pub source_id: String,
    pub binding: serde_json::Value,
    pub found: Vec<(String, String)>,
    pub verified: Vec<String>,
    pub unverified: Vec<String>,
    pub proof: String,
}

/// Why an adoption is refused.
#[derive(Debug)]
pub struct AdoptRefused(pub String);

impl std::fmt::Display for AdoptRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "adoption refused: {}", self.0)
    }
}

impl std::error::Error for AdoptRefused {}

/// The adoption plan of `spec` (a dry run: reads only).
pub async fn adopt_plan(
    backend: &ArcStorageBackend,
    external: &dyn ExternalState,
    spec: &PipelineSpec,
) -> Result<AdoptPlan> {
    let b = binding_of(spec).await.map_err(|e| anyhow::anyhow!("{e}"))?;
    let source_id = b.source_id.clone();
    if read(backend, &source_id).await?.is_some() {
        return Err(AdoptRefused(format!(
            "source id '{source_id}' is already bound; a start compares with \
             that binding"
        ))
        .into());
    }
    let mut found =
        local_protected(backend, &source_id, &[&b.pipeline]).await?;
    found.extend(
        external
            .protected(spec)
            .await
            .map_err(|e| anyhow::anyhow!(sanitized(&format!("{e:#}"))))?,
    );
    found.sort();
    found.dedup();
    if found.is_empty() {
        return Err(AdoptRefused(format!(
            "source id '{source_id}' has no retained state: its first start \
             binds it"
        ))
        .into());
    }

    let refuse = |why: String| -> anyhow::Error { AdoptRefused(why).into() };
    let mut verified = Vec::new();
    let mut unverified = Vec::new();
    let sink_ids: std::collections::BTreeSet<&str> =
        spec.spec.sinks.iter().map(|s| s.sink_id()).collect();

    // Sink ids against the per-sink checkpoint keys.
    for f in found.iter().filter(|f| f.kind == "sink_checkpoint") {
        let sink = f.key.rsplit("::sink::").next().unwrap_or_default();
        if !sink_ids.contains(sink) {
            return Err(refuse(format!(
                "checkpoint '{}' belongs to sink '{sink}', which the spec \
                 does not have",
                f.key
            )));
        }
    }
    if found.iter().any(|f| f.kind == "sink_checkpoint") {
        verified.push("sink ids against the per-sink checkpoint keys".into());
    }
    // The pipeline name against the claim.
    match backend.slot_get("active_sources", &source_id).await? {
        Some((_, owner)) if owner != b.pipeline.as_bytes() => {
            return Err(refuse(format!(
                "the source id is claimed by pipeline '{}', the spec names \
                 '{}'",
                String::from_utf8_lossy(&owner),
                b.pipeline
            )));
        }
        Some(_) => verified.push("the pipeline name against the claim".into()),
        None => unverified
            .push("no claim to check the pipeline name against".into()),
    }
    // Ownership records: their sink segment against the spec's sinks.
    for f in found.iter().filter(|f| f.kind == "destination_ownership") {
        let sink = f.key.split('/').nth(1).unwrap_or_default();
        let sink = sink.replace("%2F", "/").replace("%25", "%");
        if !sink_ids.contains(sink.as_str()) {
            return Err(refuse(format!(
                "destination ownership record '{}' belongs to sink '{sink}', \
                 which the spec does not have",
                f.key
            )));
        }
    }
    // The tenant against the lineage key.
    let (mine, other) = storage::adapters::source_lineage::recorded_for(
        backend, &b.tenant, &source_id,
    )
    .await?;
    if other && !mine {
        return Err(refuse(format!(
            "the source's lineage is recorded under another tenant, not \
             '{}'",
            b.tenant
        )));
    }
    if mine {
        verified.push("the tenant against the lineage record".into());
    } else {
        unverified.push("no lineage record to check the tenant against".into());
    }
    // The snapshot generation's fingerprint.
    if let Some((_, bytes)) = backend
        .slot_get(
            sources::snapshot_queue::CONTROL_NS,
            &sources::snapshot_queue::control_key(&source_id),
        )
        .await?
    {
        let v: serde_json::Value = serde_json::from_slice(&bytes)
            .context("the snapshot control record is malformed")?;
        let (engine, tables) = match &spec.spec.source {
            SourceCfg::Postgres(c) => {
                (sources::snapshot_recovery::Engine::Postgres, &c.tables)
            }
            SourceCfg::Mysql(c) => {
                (sources::snapshot_recovery::Engine::Mysql, &c.tables)
            }
        };
        let expected =
            sources::snapshot_recovery::config_fingerprint(engine, tables);
        let format = v
            .get("fingerprint_format")
            .and_then(|f| f.as_u64())
            .unwrap_or(0);
        match v.get("config_fingerprint").and_then(|f| f.as_str()) {
            Some(fp) if format == 3 && fp != expected => {
                return Err(refuse(
                    "the snapshot generation was taken for another engine or \
                     other table patterns"
                        .into(),
                ));
            }
            Some(_) if format == 3 => verified.push(
                "the snapshot generation against the engine and table patterns"
                    .into(),
            ),
            _ => unverified.push(
                "the snapshot generation's fingerprint is of an earlier format"
                    .into(),
            ),
        }
    }
    // The PostgreSQL slot owner record.
    if let Some(bytes) = backend
        .kv_get(CHECKPOINTS_NS, &format!("slot_owner:{source_id}"))
        .await?
    {
        let SourceCfg::Postgres(c) = &spec.spec.source else {
            return Err(refuse(
                "a PostgreSQL slot owner record exists for a MySQL spec".into(),
            ));
        };
        let rec: serde_json::Value = serde_json::from_slice(&bytes)
            .context("the slot owner record is malformed")?;
        let field = |k: &str| {
            rec.get(k)
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string()
        };
        let database = b.canonical["source"]["database"]
            .as_str()
            .unwrap_or_default();
        for (what, recorded, spec_value) in [
            ("slot", field("slot"), c.slot.clone()),
            ("pipeline", field("pipeline"), b.pipeline.clone()),
            ("database", field("database"), database.to_string()),
        ] {
            if recorded != spec_value {
                return Err(refuse(format!(
                    "the slot owner record names {what} {recorded:?}, the \
                     spec {spec_value:?}"
                )));
            }
        }
        verified.push("the slot owner's slot, database and pipeline".into());
    }
    for f in &found {
        if matches!(f.kind, "postgresql_replication_slot" | "s3_durable_head") {
            unverified.push(format!(
                "{} {} is adopted on the operator's statement",
                f.kind, f.key
            ));
        }
    }
    unverified.push(
        "every other recorded field is adopted on the operator's statement"
            .into(),
    );

    let proof = proof_of(&b, &found);
    Ok(AdoptPlan {
        source_id,
        binding: serde_json::to_value(&b).expect("binding serializes"),
        found: found
            .iter()
            .map(|f| (f.kind.to_string(), f.key.clone()))
            .collect(),
        verified,
        unverified,
        proof,
    })
}

fn proof_of(b: &StateBinding, found: &[Found]) -> String {
    let mut h = Sha256::new();
    h.update(b.digest.as_bytes());
    for f in found {
        for part in [f.kind, f.key.as_str(), f.digest.as_str()] {
            h.update((part.len() as u64).to_be_bytes());
            h.update(part.as_bytes());
        }
    }
    hex(&h.finalize())
}

/// Apply an adoption: only if the store still produces `expected`, under
/// the store gate (the server must be stopped). Writes the binding and an
/// audit record.
pub async fn adopt_apply(
    backend: &ArcStorageBackend,
    external: &dyn ExternalState,
    spec: &PipelineSpec,
    expected: &str,
    actor: &str,
) -> Result<AdoptPlan> {
    let gate = storage::adapters::store_gate::acquire(
        backend,
        storage::adapters::store_gate::GateRole::Server,
    )
    .await
    .context("acquire the store gate (stop the DeltaForge server first)")?;
    let result = async {
        let plan = adopt_plan(backend, external, spec).await?;
        if plan.proof != expected {
            return Err(AdoptRefused(format!(
                "the store no longer produces proof {expected} (now {}); run \
                 the dry run again and review it",
                plan.proof
            ))
            .into());
        }
        let bytes = serde_json::to_vec(&plan.binding)?;
        if backend
            .slot_create(NAMESPACE, &plan.source_id, &bytes)
            .await?
            .is_none()
        {
            return Err(AdoptRefused(
                "the source id was bound meanwhile".into(),
            )
            .into());
        }
        let audit = serde_json::json!({
            "source_id": plan.source_id,
            "proof": plan.proof,
            "actor": actor,
            "at_ms": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            "found": plan.found,
            "verified": plan.verified,
            "unverified": plan.unverified,
        });
        backend
            .kv_put(
                AUDIT_NAMESPACE,
                &format!("{}:{}", plan.source_id, plan.proof),
                &serde_json::to_vec(&audit)?,
            )
            .await?;
        Ok::<_, anyhow::Error>(plan)
    }
    .await;
    gate.release().await.context("release the store gate")?;
    result
}

/// The manager's handle on the external probes.
pub type SharedExternalState = Arc<dyn ExternalState>;

#[cfg(test)]
mod tests;

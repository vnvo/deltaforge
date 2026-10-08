//! Binding a pgoutput Relation to its event schema version (design section 4,
//! revision 3): R register-or-retrieve the content-addressed version, V read it
//! back and verify the fingerprint, B append the binding (deterministic bytes,
//! appended only if absent), C cache and emit. A crash at any boundary
//! converges on the next run; a binding is appended only after its version is
//! durable and verified, so it never references a missing or different one.

use std::sync::Arc;

use storage::adapters::SchemaKey;
use storage::{ArcStorageBackend, DurableSchemaRegistry, LogError};

use super::postgres_event_schema::{
    PgEventSchema, RelationBinding, RelationProjection,
};
use super::postgres_table_schema::PostgresTableSchema;
use deltaforge_core::{SourceError, SourceResult};
use schema_registry::SourceSchema;

/// Log namespace of the Relation bindings (one stream per table key).
pub(crate) const BINDING_NS: &str = "schemas.v1.pg.relation_binding";

/// The event schema version a Relation's rows are stamped with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedEventSchema {
    pub fingerprint: Arc<str>,
    /// The version's own registry sequence.
    pub sequence: u64,
    pub version: i32,
    pub digest: String,
    /// Registry scope generation the resolution was made under.
    pub scope_generation: u64,
}

fn corrupt(details: String) -> SourceError {
    SourceError::Schema {
        details: details.into(),
    }
}

/// R, V and B for one Relation (C is the caller's: cache, then emit).
pub(crate) async fn bind(
    registry: &DurableSchemaRegistry,
    backend: &ArcStorageBackend,
    key: &SchemaKey,
    scope_generation: u64,
    projection: &RelationProjection,
    event: &PgEventSchema,
) -> SourceResult<ResolvedEventSchema> {
    let fingerprint = event.fingerprint();
    let json = serde_json::to_value(event)
        .map_err(|e| SourceError::Other(e.into()))?;
    // R: content-addressed, idempotent through the hash index.
    let version = registry
        .register_with_checkpoint(key, &fingerprint, &json, None)
        .await
        .map_err(SourceError::Other)?;
    // V: the version must be durable and carry exactly this fingerprint.
    let stored = registry
        .get_version(key, version)
        .await
        .map_err(SourceError::Other)?
        .ok_or_else(|| {
            corrupt(format!(
                "schema registry: version {version} of {}.{} is missing \
                 right after it was registered",
                key.db, key.table
            ))
        })?;
    if stored.hash != fingerprint {
        return Err(corrupt(format!(
            "schema registry: version {version} of {}.{} has fingerprint \
             {} instead of {fingerprint}",
            key.db, key.table, stored.hash
        )));
    }
    // B: deterministic bytes; identical bytes are a no-op, different bytes
    // under the same digest are corruption.
    let binding = RelationBinding::new(projection, version, &fingerprint);
    let bytes = serde_json::to_vec(&binding)
        .map_err(|e| SourceError::Other(e.into()))?;
    match backend
        .log_append_if_absent(
            BINDING_NS,
            &key.backend_key(),
            &binding.digest,
            &bytes,
        )
        .await
    {
        Ok(_) => {}
        Err(e)
            if matches!(
                e.downcast_ref::<LogError>(),
                Some(LogError::CaptureIdentityConflict { .. })
            ) =>
        {
            return Err(corrupt(format!(
                "relation binding {} of {}.{} conflicts with the stored \
                 binding of the same digest",
                binding.digest, key.db, key.table
            )));
        }
        Err(e) => return Err(SourceError::Other(e)),
    }
    Ok(ResolvedEventSchema {
        fingerprint: fingerprint.into(),
        sequence: stored.sequence,
        version,
        digest: binding.digest,
        scope_generation,
    })
}

/// The table's latest binding, if any.
pub(crate) async fn latest_binding(
    backend: &ArcStorageBackend,
    key: &SchemaKey,
) -> SourceResult<Option<RelationBinding>> {
    let Some((_, bytes)) = backend
        .log_latest(BINDING_NS, &key.backend_key())
        .await
        .map_err(SourceError::Other)?
    else {
        return Ok(None);
    };
    serde_json::from_slice(&bytes).map(Some).map_err(|e| {
        corrupt(format!(
            "relation binding of {}.{} is unreadable: {e}",
            key.db, key.table
        ))
    })
}

/// Outcome of a table's first Relation in a run, compared with what is
/// durably recorded for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FirstResolution {
    /// Nothing recorded (first use) or the same content.
    NoDrift,
    /// Same content under another relation (dropped and recreated): a new
    /// binding is recorded, not drift.
    Replaced,
    /// The content changed while the source was not reading.
    Drift(String),
}

/// Compare a table's first Relation of a run with its latest binding, or,
/// for a table with only pre-binding history, with the latest registered
/// version (the fields that history carries).
pub(crate) async fn first_resolution(
    registry: &DurableSchemaRegistry,
    backend: &ArcStorageBackend,
    key: &SchemaKey,
    projection: &RelationProjection,
    event: &PgEventSchema,
) -> SourceResult<FirstResolution> {
    if let Some(b) = latest_binding(backend, key).await? {
        let Some(previous) = b.projection() else {
            return Err(corrupt(format!(
                "relation binding of {}.{} has an invalid replica identity",
                key.db, key.table
            )));
        };
        return Ok(if previous.same_content(projection) {
            if previous.oid == projection.oid {
                FirstResolution::NoDrift
            } else {
                FirstResolution::Replaced
            }
        } else {
            FirstResolution::Drift(format!(
                "{} -> {}",
                describe(&previous),
                describe(projection)
            ))
        });
    }
    let Some(latest) =
        registry.get_latest(key).await.map_err(SourceError::Other)?
    else {
        return Ok(FirstResolution::NoDrift);
    };
    // A `pg_event_v2` version without a binding: a crash between R and B.
    if let Ok(v2) =
        serde_json::from_value::<PgEventSchema>(latest.schema_json.clone())
        && v2.format == super::postgres_event_schema::PG_EVENT_V2
    {
        return Ok(if v2.fingerprint() == event.fingerprint() {
            FirstResolution::NoDrift
        } else {
            FirstResolution::Drift(format!(
                "registered event schema {} -> {}",
                v2.fingerprint(),
                event.fingerprint()
            ))
        });
    }
    // Legacy catalog capture: compare what it carries, (name, type OID) and
    // the replica identity. Its typmods and key flags were not recorded.
    let legacy: PostgresTableSchema =
        serde_json::from_value(latest.schema_json).map_err(|e| {
            corrupt(format!(
                "stored schema of {}.{} (version {}) is unreadable: {e}",
                key.db, key.table, latest.version
            ))
        })?;
    let stored: Vec<(String, Option<u32>)> = legacy.signature();
    let live: Vec<(String, Option<u32>)> = projection
        .columns
        .iter()
        .map(|(n, t, _, _)| (n.clone(), Some(*t)))
        .collect();
    let identity_differs = legacy
        .replica_identity_char()
        .is_some_and(|c| c != projection.replica_identity);
    if stored != live || identity_differs {
        return Ok(FirstResolution::Drift(format!(
            "columns {stored:?} -> {live:?}, replica identity {:?} -> {}",
            legacy.replica_identity, projection.replica_identity
        )));
    }
    Ok(if legacy.oid.is_some_and(|o| o != projection.oid) {
        FirstResolution::Replaced
    } else {
        FirstResolution::NoDrift
    })
}

/// `name:type_oid(typmod)[key]` per column, plus the replica identity.
pub(crate) fn describe(p: &RelationProjection) -> String {
    let cols = p
        .columns
        .iter()
        .map(|(n, t, m, k)| {
            format!("{n}:{t}({m}){}", if *k { "[key]" } else { "" })
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{cols}] identity {}", p.replica_identity)
}

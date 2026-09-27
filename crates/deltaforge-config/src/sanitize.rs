//! Borrowed sanitized serialization for status/API output.
//!
//! Public pipeline responses must never expose an inline DSN password. Rather than
//! cloning the whole spec (which would allocate another cleartext DSN), these
//! borrowed wrappers serialize the source DSN **directly as redacted** while
//! preserving `SecretReference` metadata and everything else by reference. Resolved
//! secret material is never present in the config type, so it cannot be serialized.
//!
//! The persistence/input round-trip continues to use the lossless config type; only
//! public output goes through [`serialize_sanitized_spec`].
//!
//! Extension point: sink credential redaction belongs in [`SanitizedSpec`] (the
//! `sinks` field), in the connector-migration slice that covers sinks.

use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};

use crate::{
    BatchConfig, CommitPolicy, ConnectionPolicy, JournalConfig, Metadata,
    MysqlSrcCfg, PipelineSpec, PostgresSrcCfg, ProcessorCfg,
    SchemaSensingConfig, Sharding, SinkCfg, SourceCfg, Spec,
};

/// `serialize_with` entry point for a `PipelineSpec` field that must be redacted on
/// output (e.g. `PipeInfo.spec`). Never clones the cleartext DSN.
pub fn serialize_sanitized_spec<S>(
    spec: &PipelineSpec,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    SanitizedPipelineSpec {
        metadata: &spec.metadata,
        spec: SanitizedSpec::new(&spec.spec),
    }
    .serialize(serializer)
}

#[derive(Serialize)]
struct SanitizedPipelineSpec<'a> {
    metadata: &'a Metadata,
    spec: SanitizedSpec<'a>,
}

/// Mirrors `Spec`'s serialized shape by reference, overriding only `source`.
///
/// This is a **separate struct** from `Spec`, so the compiler does NOT flag a new
/// `Spec` field that is missing here - it would silently vanish from sanitized
/// output. The `sanitized_and_normal_json_match_except_source_dsn` regression test
/// guards against that by comparing ordinary and sanitized JSON.
#[derive(Serialize)]
struct SanitizedSpec<'a> {
    sharding: &'a Option<Sharding>,
    source: SanitizedSourceCfg<'a>,
    processors: &'a Vec<ProcessorCfg>,
    // Extension point: sink credential redaction wraps this in a later slice.
    sinks: &'a Vec<SinkCfg>,
    connection_policy: &'a Option<ConnectionPolicy>,
    batch: &'a Option<BatchConfig>,
    commit_policy: &'a Option<CommitPolicy>,
    sink_batch_deadline_secs: &'a Option<u32>,
    schema_sensing: &'a SchemaSensingConfig,
    journal: &'a Option<JournalConfig>,
}

impl<'a> SanitizedSpec<'a> {
    fn new(s: &'a Spec) -> Self {
        Self {
            sharding: &s.sharding,
            source: SanitizedSourceCfg(&s.source),
            processors: &s.processors,
            sinks: &s.sinks,
            connection_policy: &s.connection_policy,
            batch: &s.batch,
            commit_policy: &s.commit_policy,
            sink_batch_deadline_secs: &s.sink_batch_deadline_secs,
            schema_sensing: &s.schema_sensing,
            journal: &s.journal,
        }
    }
}

/// Adjacently tagged like `SourceCfg` (`{"type":..,"config":..}`), but the config
/// is serialized with the inline DSN redacted.
struct SanitizedSourceCfg<'a>(&'a SourceCfg);

impl Serialize for SanitizedSourceCfg<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut st = serializer.serialize_struct("SourceCfg", 2)?;
        match self.0 {
            SourceCfg::Postgres(c) => {
                st.serialize_field("type", "postgres")?;
                st.serialize_field("config", &SanitizedPgCfg(c))?;
            }
            SourceCfg::Mysql(c) => {
                st.serialize_field("type", "mysql")?;
                st.serialize_field("config", &SanitizedMyCfg(c))?;
            }
        }
        st.end()
    }
}

/// Redacted view of `redact_dsn` applied to an optional inline DSN.
fn redacted(dsn: &Option<String>) -> Option<String> {
    dsn.as_ref().map(|d| common::dsn::redact_dsn(d))
}

struct SanitizedPgCfg<'a>(&'a PostgresSrcCfg);

impl Serialize for SanitizedPgCfg<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let c = self.0;
        let mut st = serializer.serialize_struct("PostgresSrcCfg", 12)?;
        st.serialize_field("id", &c.id)?;
        // dsn / dsn_secret / credentials skip when None, matching the config type.
        match redacted(&c.dsn) {
            Some(d) => st.serialize_field("dsn", &d)?,
            None => st.skip_field("dsn")?,
        }
        match &c.dsn_secret {
            Some(r) => st.serialize_field("dsn_secret", r)?,
            None => st.skip_field("dsn_secret")?,
        }
        match &c.credentials {
            Some(cr) => st.serialize_field("credentials", cr)?,
            None => st.skip_field("credentials")?,
        }
        st.serialize_field("publication", &c.publication)?;
        st.serialize_field("slot", &c.slot)?;
        st.serialize_field("tables", &c.tables)?;
        st.serialize_field("table_options", &c.table_options)?;
        st.serialize_field("start_position", &c.start_position)?;
        st.serialize_field("outbox", &c.outbox)?;
        st.serialize_field("snapshot", &c.snapshot)?;
        st.serialize_field("on_schema_drift", &c.on_schema_drift)?;
        st.end()
    }
}

struct SanitizedMyCfg<'a>(&'a MysqlSrcCfg);

impl Serialize for SanitizedMyCfg<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let c = self.0;
        let mut st = serializer.serialize_struct("MysqlSrcCfg", 9)?;
        st.serialize_field("id", &c.id)?;
        match redacted(&c.dsn) {
            Some(d) => st.serialize_field("dsn", &d)?,
            None => st.skip_field("dsn")?,
        }
        match &c.dsn_secret {
            Some(r) => st.serialize_field("dsn_secret", r)?,
            None => st.skip_field("dsn_secret")?,
        }
        match &c.credentials {
            Some(cr) => st.serialize_field("credentials", cr)?,
            None => st.skip_field("credentials")?,
        }
        st.serialize_field("tables", &c.tables)?;
        st.serialize_field("table_options", &c.table_options)?;
        st.serialize_field("outbox", &c.outbox)?;
        st.serialize_field("snapshot", &c.snapshot)?;
        st.serialize_field("on_schema_drift", &c.on_schema_drift)?;
        st.end()
    }
}

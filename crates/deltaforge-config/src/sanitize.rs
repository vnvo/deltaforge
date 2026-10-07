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
//! Sink credentials are redacted here too: each sink is serialized to a
//! `serde_json::Value` and passed through [`redact_secrets`], which recursively replaces
//! any secret-classified field (by key) and redacts passwords embedded in connection
//! URLs/DSNs. This is deny-leaning: an unknown key that looks secret is redacted, and the
//! `sink_sanitization_*` shape tests fail if a new sink field is neither a known public
//! field nor redacted, so a future secret field cannot silently bypass redaction.

use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};

/// Placeholder emitted in place of any redacted secret value.
pub(crate) const REDACTED: &str = "***REDACTED***";

/// Whether a field key names a secret whose value must be redacted in public output.
/// Case-insensitive. Deliberately conservative (over-redaction is safe for status
/// output); `*_file` keys are treated as paths, not secrets.
pub(crate) fn is_secret_key(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    if k.ends_with("_file") {
        return false; // a path to material, not the material itself
    }
    const EXACT: &[&str] = &[
        "password",
        "passwd",
        "token",
        "authorization",
        "cookie",
        "api_key",
        "apikey",
        "access_key_id",
        "secret_access_key",
        "session_token",
        "private_key",
        "secret",
    ];
    if EXACT.contains(&k.as_str()) {
        return true;
    }
    // Substrings catch map-entry keys like `sasl.password`, `sasl.username`,
    // `x-api-key`, `X-Auth-Token`. NB: bare `key` is intentionally NOT secret (it is a
    // message-key expression on Kafka/Redis/NATS sinks).
    const PARTS: &[&str] = &[
        "password",
        "secret",
        "token",
        "sasl",
        "api_key",
        "apikey",
        "authorization",
        "credential",
    ];
    PARTS.iter().any(|p| k.contains(p))
}

/// Whether a field key holds a connection URL/DSN that may embed a password.
fn is_url_key(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "uri" | "url" | "dsn" | "endpoint"
    )
}

/// Redact an embedded password from a connection string, returning `Some(redacted)`
/// only when a password was actually present. A password-less URL/DSN is returned as
/// `None` (left untouched) so non-secret endpoints are not cosmetically rewritten.
fn redact_conn_password(s: &str) -> Option<String> {
    if s.contains("://") {
        if url_has_password(s) {
            Some(common::dsn::redact_url_password(s))
        } else {
            None
        }
    } else if s.to_ascii_lowercase().contains("password") {
        Some(common::dsn::redact_keyvalue_password(s))
    } else {
        None
    }
}

/// Whether a URL's authority carries a `user:password@` userinfo password, without a
/// URL-parsing dependency.
fn url_has_password(s: &str) -> bool {
    let Some((_, rest)) = s.split_once("://") else {
        return false;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    authority
        .rsplit_once('@')
        .is_some_and(|(userinfo, _host)| userinfo.contains(':'))
}

/// Recursively redact secrets in a serialized config value: secret-classified keys are
/// fully redacted; URL/DSN keys have any embedded password stripped; everything else
/// recurses. Used for sink output (and reusable for any config value).
pub(crate) fn redact_secrets(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (k, v) in map.iter_mut() {
                if is_secret_key(k) {
                    redact_value(v);
                } else if is_url_key(k) {
                    if let serde_json::Value::String(s) = v {
                        if let Some(red) = redact_conn_password(s) {
                            *s = red;
                        }
                    }
                } else {
                    redact_secrets(v);
                }
            }
        }
        serde_json::Value::Array(arr) => {
            arr.iter_mut().for_each(redact_secrets)
        }
        _ => {}
    }
}

/// Replace every leaf string within `value` with [`REDACTED`] (a secret field may be a
/// string, or a nested object/array of secret material).
fn redact_value(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::String(s) => *s = REDACTED.to_string(),
        serde_json::Value::Object(map) => {
            map.values_mut().for_each(redact_value)
        }
        serde_json::Value::Array(arr) => arr.iter_mut().for_each(redact_value),
        _ => {}
    }
}

/// Serialize a sink to a redacted `serde_json::Value`. Fails closed: a serialization
/// error yields a redaction placeholder rather than risking raw output.
fn sanitized_sink(sink: &SinkCfg) -> serde_json::Value {
    match serde_json::to_value(sink) {
        Ok(mut v) => {
            redact_secrets(&mut v);
            v
        }
        Err(_) => serde_json::Value::String(REDACTED.to_string()),
    }
}

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
    /// Redacted per sink: each sink is serialized then passed through
    /// [`redact_secrets`], so no sink credential reaches public output.
    sinks: Vec<serde_json::Value>,
    connection_policy: &'a Option<ConnectionPolicy>,
    batch: &'a Option<BatchConfig>,
    commit_policy: &'a Option<CommitPolicy>,
    sink_batch_deadline_secs: &'a Option<u32>,
    schema_sensing: &'a SchemaSensingConfig,
    journal: &'a Option<JournalConfig>,
    metrics: &'a crate::MetricsCfg,
}

impl<'a> SanitizedSpec<'a> {
    fn new(s: &'a Spec) -> Self {
        Self {
            sharding: &s.sharding,
            source: SanitizedSourceCfg(&s.source),
            processors: &s.processors,
            sinks: s.sinks.iter().map(sanitized_sink).collect(),
            connection_policy: &s.connection_policy,
            batch: &s.batch,
            commit_policy: &s.commit_policy,
            sink_batch_deadline_secs: &s.sink_batch_deadline_secs,
            schema_sensing: &s.schema_sensing,
            journal: &s.journal,
            metrics: &s.metrics,
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

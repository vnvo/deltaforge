//! Resolve sink credential references to **protected runtime values** before any sink
//! client is constructed.
//!
//! Per the secret-coverage design (P2), resolved secrets are never materialized back into
//! the serializable config. They are held here in `Zeroizing` buffers, keyed by sink id
//! and logical field, and handed to each sink builder, which passes them straight to its
//! client (never storing them on a config struct). Resolution runs before construction, so
//! a missing/invalid reference or an inline/reference conflict fails closed before any
//! network access.

use std::collections::HashMap;

use anyhow::{Result, bail};
use deltaforge_config::{
    ClickHouseSinkCfg, ElasticsearchSinkCfg, EncodingCfg, EsAuth, HttpSinkCfg,
    KafkaSinkCfg, NatsSinkCfg, PipelineSpec, RedisSinkCfg, S3SinkCfg, SinkCfg,
};
use secrets::{SecretReference, SecretResolver};
use zeroize::Zeroizing;

/// Resolved secret values for one sink, keyed by logical field name. Runtime-only,
/// zeroized on drop, never serialized.
#[derive(Default)]
pub struct ResolvedSinkCreds {
    fields: HashMap<String, Zeroizing<String>>,
    sr_username: Option<Zeroizing<String>>,
    sr_password: Option<Zeroizing<String>>,
}

impl ResolvedSinkCreds {
    /// The resolved value for `field`, if a reference supplied one.
    pub fn get(&self, field: &str) -> Option<&str> {
        self.fields.get(field).map(|z| z.as_str())
    }
    /// Iterate resolved (field, value) pairs - used by map-valued sinks (HTTP headers,
    /// Kafka client_conf) that merge every resolved entry. Schema Registry credentials
    /// are held out of this iterator so they never leak into request headers or broker
    /// config; read them through the dedicated accessors instead.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.fields.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }
    /// Resolved Schema Registry basic-auth username, if a reference supplied one.
    pub fn schema_registry_username(&self) -> Option<&str> {
        self.sr_username.as_ref().map(|z| z.as_str())
    }
    /// Resolved Schema Registry basic-auth password, if a reference supplied one.
    pub fn schema_registry_password(&self) -> Option<&str> {
        self.sr_password.as_ref().map(|z| z.as_str())
    }
    fn insert(&mut self, field: impl Into<String>, value: Zeroizing<String>) {
        self.fields.insert(field.into(), value);
    }
}

/// Resolved secrets for every sink in a pipeline, keyed by sink id.
#[derive(Default)]
pub struct ResolvedSinkSecrets {
    by_sink: HashMap<String, ResolvedSinkCreds>,
}

impl ResolvedSinkSecrets {
    /// The resolved credentials for a sink id, or an empty set (no references).
    pub fn for_sink(&self, id: &str) -> &ResolvedSinkCreds {
        static EMPTY: std::sync::OnceLock<ResolvedSinkCreds> =
            std::sync::OnceLock::new();
        self.by_sink
            .get(id)
            .unwrap_or_else(|| EMPTY.get_or_init(ResolvedSinkCreds::default))
    }
}

/// Resolve every sink's credential references up front. Fails closed on a missing/invalid
/// reference or an inline/reference conflict, before any sink client is built.
pub async fn resolve_sink_secrets(
    spec: &PipelineSpec,
    resolver: &dyn SecretResolver,
) -> Result<ResolvedSinkSecrets> {
    let mut by_sink = HashMap::new();
    for sink in &spec.spec.sinks {
        let creds = match sink {
            SinkCfg::ClickHouse(c) => resolve_clickhouse(c, resolver).await?,
            SinkCfg::Redis(c) => resolve_redis(c, resolver).await?,
            SinkCfg::Nats(c) => resolve_nats(c, resolver).await?,
            SinkCfg::Http(c) => resolve_http(c, resolver).await?,
            SinkCfg::Kafka(c) => resolve_kafka(c, resolver).await?,
            SinkCfg::Elasticsearch(c) => {
                resolve_elasticsearch(c, resolver).await?
            }
            SinkCfg::S3(c) => resolve_s3(c, resolver).await?,
        };
        by_sink.insert(sink.sink_id().to_string(), creds);
    }
    Ok(ResolvedSinkSecrets { by_sink })
}

async fn resolve_clickhouse(
    cfg: &ClickHouseSinkCfg,
    resolver: &dyn SecretResolver,
) -> Result<ResolvedSinkCreds> {
    let mut creds = ResolvedSinkCreds::default();
    resolve_field(
        &mut creds,
        "clickhouse",
        &cfg.id,
        "user",
        &cfg.user,
        &cfg.user_ref,
        resolver,
    )
    .await?;
    resolve_field(
        &mut creds,
        "clickhouse",
        &cfg.id,
        "password",
        &cfg.password,
        &cfg.password_ref,
        resolver,
    )
    .await?;
    Ok(creds)
}

/// Whether a URL's authority carries a `user:password@` userinfo password.
pub(crate) fn url_has_password(s: &str) -> bool {
    let Some((_, rest)) = s.split_once("://") else {
        return false;
    };
    rest.split(['/', '?', '#'])
        .next()
        .unwrap_or("")
        .rsplit_once('@')
        .is_some_and(|(userinfo, _)| userinfo.contains(':'))
}

async fn resolve_redis(
    cfg: &RedisSinkCfg,
    resolver: &dyn SecretResolver,
) -> Result<ResolvedSinkCreds> {
    let mut creds = ResolvedSinkCreds::default();
    let uri_has_pw = url_has_password(&cfg.uri);
    match (&cfg.uri_secret, &cfg.credentials) {
        (Some(_), Some(_)) => bail!(
            "redis sink '{}': set either uri_secret or credentials, not both",
            cfg.id
        ),
        (Some(_), None) if uri_has_pw => bail!(
            "redis sink '{}': uri_secret is set but the inline uri also embeds a \
             password",
            cfg.id
        ),
        (None, Some(_)) if uri_has_pw => bail!(
            "redis sink '{}': credentials are set but the base uri embeds a \
             password (that would be overridden)",
            cfg.id
        ),
        _ => {}
    }
    if let Some(reference) = &cfg.uri_secret {
        creds.insert("uri", resolve_ref(resolver, reference).await?);
    } else if let Some(c) = &cfg.credentials {
        if let Some(user) = &c.username {
            creds.insert("username", resolve_ref(resolver, user).await?);
        }
        match &c.password {
            Some(pw) => {
                creds.insert("password", resolve_ref(resolver, pw).await?)
            }
            None => bail!(
                "redis sink '{}': credentials require a password reference",
                cfg.id
            ),
        }
    }
    resolve_encoding_sr("redis", &cfg.id, &cfg.encoding, resolver, &mut creds)
        .await?;
    Ok(creds)
}

async fn resolve_nats(
    cfg: &NatsSinkCfg,
    resolver: &dyn SecretResolver,
) -> Result<ResolvedSinkCreds> {
    let mut creds = ResolvedSinkCreds::default();
    for (field, inline, reference) in [
        ("username", &cfg.username, &cfg.username_ref),
        ("password", &cfg.password, &cfg.password_ref),
        ("token", &cfg.token, &cfg.token_ref),
    ] {
        resolve_field(
            &mut creds, "nats", &cfg.id, field, inline, reference, resolver,
        )
        .await?;
    }
    resolve_encoding_sr("nats", &cfg.id, &cfg.encoding, resolver, &mut creds)
        .await?;
    Ok(creds)
}

async fn resolve_http(
    cfg: &HttpSinkCfg,
    resolver: &dyn SecretResolver,
) -> Result<ResolvedSinkCreds> {
    let mut creds = resolve_map_refs(
        "http",
        &cfg.id,
        "header",
        &cfg.headers,
        &cfg.secret_refs,
        resolver,
    )
    .await?;
    resolve_encoding_sr("http", &cfg.id, &cfg.encoding, resolver, &mut creds)
        .await?;
    Ok(creds)
}

async fn resolve_kafka(
    cfg: &KafkaSinkCfg,
    resolver: &dyn SecretResolver,
) -> Result<ResolvedSinkCreds> {
    let mut creds = resolve_map_refs(
        "kafka",
        &cfg.id,
        "client_conf key",
        &cfg.client_conf,
        &cfg.secret_refs,
        resolver,
    )
    .await?;
    resolve_encoding_sr("kafka", &cfg.id, &cfg.encoding, resolver, &mut creds)
        .await?;
    Ok(creds)
}

async fn resolve_s3(
    cfg: &S3SinkCfg,
    resolver: &dyn SecretResolver,
) -> Result<ResolvedSinkCreds> {
    let mut creds = ResolvedSinkCreds::default();
    for (field, inline, reference) in [
        ("access_key_id", &cfg.access_key_id, &cfg.access_key_id_ref),
        (
            "secret_access_key",
            &cfg.secret_access_key,
            &cfg.secret_access_key_ref,
        ),
        ("session_token", &cfg.session_token, &cfg.session_token_ref),
    ] {
        resolve_field(
            &mut creds, "s3", &cfg.id, field, inline, reference, resolver,
        )
        .await?;
    }
    // The access/secret pair completeness (xor) is enforced at build time, where inline
    // and reference values are combined into the effective params.
    Ok(creds)
}

async fn resolve_elasticsearch(
    cfg: &ElasticsearchSinkCfg,
    resolver: &dyn SecretResolver,
) -> Result<ResolvedSinkCreds> {
    let mut creds = ResolvedSinkCreds::default();
    match &cfg.auth {
        Some(EsAuth::Basic {
            username,
            password,
            username_ref,
            password_ref,
        }) => {
            resolve_field(
                &mut creds,
                "elasticsearch",
                &cfg.id,
                "username",
                username,
                username_ref,
                resolver,
            )
            .await?;
            resolve_field(
                &mut creds,
                "elasticsearch",
                &cfg.id,
                "password",
                password,
                password_ref,
                resolver,
            )
            .await?;
        }
        Some(EsAuth::ApiKey {
            api_key,
            api_key_ref,
        }) => {
            resolve_field(
                &mut creds,
                "elasticsearch",
                &cfg.id,
                "api_key",
                api_key,
                api_key_ref,
                resolver,
            )
            .await?;
        }
        Some(EsAuth::None) | None => {}
    }
    Ok(creds)
}

/// Resolve a `secret_refs` overlay for a free-form string map (HTTP headers, Kafka
/// client_conf): each referenced key must NOT also be set in the plaintext map (a
/// fail-closed conflict), and resolves to a protected value keyed by that name.
async fn resolve_map_refs(
    connector: &str,
    id: &str,
    kind: &str,
    plaintext: &std::collections::HashMap<String, String>,
    secret_refs: &std::collections::HashMap<String, SecretReference>,
    resolver: &dyn SecretResolver,
) -> Result<ResolvedSinkCreds> {
    let mut creds = ResolvedSinkCreds::default();
    for (name, reference) in secret_refs {
        if plaintext.contains_key(name) {
            bail!(
                "{connector} sink '{id}': {kind} '{name}' is set both inline and \
                 in secret_refs; use exactly one"
            );
        }
        creds.insert(name.clone(), resolve_ref(resolver, reference).await?);
    }
    Ok(creds)
}

/// Resolve Schema Registry basic-auth references on an Avro encoding into protected
/// values held apart from the flat field map (so they never merge into HTTP headers or
/// Kafka client config). Rejects an inline/reference conflict; fails closed on an empty
/// reference. Non-Avro encodings contribute nothing.
async fn resolve_encoding_sr(
    connector: &str,
    id: &str,
    encoding: &EncodingCfg,
    resolver: &dyn SecretResolver,
    creds: &mut ResolvedSinkCreds,
) -> Result<()> {
    let EncodingCfg::Avro {
        username,
        password,
        username_ref,
        password_ref,
        ..
    } = encoding
    else {
        return Ok(());
    };
    if username.is_some() && username_ref.is_some() {
        bail!(
            "{connector} sink '{id}': schema registry username sets both an \
             inline value and a reference; use exactly one"
        );
    }
    if password.is_some() && password_ref.is_some() {
        bail!(
            "{connector} sink '{id}': schema registry password sets both an \
             inline value and a reference; use exactly one"
        );
    }
    if let Some(reference) = username_ref {
        creds.sr_username = Some(resolve_ref(resolver, reference).await?);
    }
    if let Some(reference) = password_ref {
        creds.sr_password = Some(resolve_ref(resolver, reference).await?);
    }
    Ok(())
}

/// Resolve one field: reject an inline/reference conflict; resolve a reference to a
/// non-empty protected value; leave an inline-only field for `${ENV}` expansion at
/// construction (nothing resolved here).
#[allow(clippy::too_many_arguments)]
async fn resolve_field(
    creds: &mut ResolvedSinkCreds,
    connector: &str,
    id: &str,
    field: &'static str,
    inline: &Option<String>,
    reference: &Option<SecretReference>,
    resolver: &dyn SecretResolver,
) -> Result<()> {
    match (inline, reference) {
        (Some(_), Some(_)) => bail!(
            "{connector} sink '{id}': {field} sets both an inline value and a \
             reference; use exactly one"
        ),
        (_, Some(reference)) => {
            let value = resolve_ref(resolver, reference).await?;
            creds.insert(field, value);
        }
        _ => {}
    }
    Ok(())
}

async fn resolve_ref(
    resolver: &dyn SecretResolver,
    reference: &SecretReference,
) -> Result<Zeroizing<String>> {
    let resolved = resolver
        .resolve(reference)
        .await
        .map_err(|e| anyhow::anyhow!("resolve sink secret: {e}"))?;
    let value = resolved
        .material()
        .as_utf8()
        .ok_or_else(|| anyhow::anyhow!("sink secret is not valid UTF-8"))?;
    if value.is_empty() {
        bail!("sink secret resolved to an empty value");
    }
    Ok(Zeroizing::new(value.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaforge_config::{ChMode, ChVersionSource, SinkCfg};
    use secrets::{
        CompositeResolver, EnvResolver, FilePolicy, FileResolver,
        SecretProvider,
    };

    fn resolver() -> CompositeResolver {
        CompositeResolver::new(
            EnvResolver::from_process(),
            FileResolver::new(FilePolicy::default()),
        )
    }

    fn write_secret(v: &str) -> tempfile::NamedTempFile {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(v.as_bytes()).unwrap();
        f
    }

    fn ch(user: Option<&str>, password: Option<&str>) -> ClickHouseSinkCfg {
        ClickHouseSinkCfg {
            id: "ch".into(),
            url: "http://ch:8123".into(),
            database: "db".into(),
            table: "t".into(),
            mode: ChMode::Upsert,
            user: user.map(String::from),
            password: password.map(String::from),
            user_ref: None,
            password_ref: None,
            tls: None,
            version_source: ChVersionSource::SourcePosition,
            send_timeout_secs: 30,
            required: Some(true),
            auto_create: true,
        }
    }

    async fn resolve_one(cfg: ClickHouseSinkCfg) -> Result<ResolvedSinkCreds> {
        resolve_clickhouse(&cfg, &resolver()).await
    }

    #[tokio::test]
    async fn inline_and_ref_conflict_rejected() {
        let mut c = ch(Some("u"), None);
        c.user_ref = Some(SecretReference::new(SecretProvider::File, "/x"));
        assert!(resolve_one(c).await.is_err());
    }

    #[tokio::test]
    async fn reference_resolves_to_protected_value() {
        let pw = write_secret("s3cr3t");
        let mut c = ch(Some("default"), None); // inline user, referenced password
        c.password_ref = Some(SecretReference::new(
            SecretProvider::File,
            pw.path().to_str().unwrap(),
        ));
        let creds = resolve_one(c).await.unwrap();
        assert_eq!(creds.get("password"), Some("s3cr3t"));
        // Inline-only user is left for ${ENV} expansion at construction.
        assert_eq!(creds.get("user"), None);
    }

    #[tokio::test]
    async fn empty_reference_fails_closed() {
        let empty = write_secret("");
        let mut c = ch(None, None);
        c.password_ref = Some(SecretReference::new(
            SecretProvider::File,
            empty.path().to_str().unwrap(),
        ));
        assert!(resolve_one(c).await.is_err());
    }

    #[tokio::test]
    async fn resolve_sink_secrets_keys_by_id() {
        let pw = write_secret("p");
        let mut c = ch(Some("u"), None);
        c.password_ref = Some(SecretReference::new(
            SecretProvider::File,
            pw.path().to_str().unwrap(),
        ));
        let spec = spec_with(vec![SinkCfg::ClickHouse(c)]);
        let out = resolve_sink_secrets(&spec, &resolver()).await.unwrap();
        assert_eq!(out.for_sink("ch").get("password"), Some("p"));
        assert_eq!(out.for_sink("missing").get("password"), None);
    }

    fn avro(
        password: Option<&str>,
        password_ref: Option<SecretReference>,
    ) -> EncodingCfg {
        EncodingCfg::Avro {
            schema_registry_url: "http://sr:8081".into(),
            subject_strategy: Default::default(),
            username: None,
            password: password.map(String::from),
            username_ref: None,
            password_ref,
            unsigned_bigint_mode: None,
            enum_mode: None,
            naive_timestamp_mode: None,
        }
    }

    #[tokio::test]
    async fn schema_registry_password_ref_held_apart_from_flat_map() {
        let pw = write_secret("sr-pass");
        let encoding = avro(
            None,
            Some(SecretReference::new(
                SecretProvider::File,
                pw.path().to_str().unwrap(),
            )),
        );
        let mut creds = ResolvedSinkCreds::default();
        resolve_encoding_sr("kafka", "k", &encoding, &resolver(), &mut creds)
            .await
            .unwrap();
        assert_eq!(creds.schema_registry_password(), Some("sr-pass"));
        // Must not merge into the flat map that HTTP headers / Kafka config draw from.
        assert_eq!(creds.iter().count(), 0);
        assert_eq!(creds.get("password"), None);
    }

    #[tokio::test]
    async fn schema_registry_inline_and_ref_conflict_rejected() {
        let encoding = avro(
            Some("inline"),
            Some(SecretReference::new(SecretProvider::File, "/x")),
        );
        let mut creds = ResolvedSinkCreds::default();
        let err = resolve_encoding_sr(
            "kafka",
            "k",
            &encoding,
            &resolver(),
            &mut creds,
        )
        .await;
        assert!(err.is_err());
    }

    fn spec_with(sinks: Vec<SinkCfg>) -> PipelineSpec {
        let yaml = r#"
apiVersion: deltaforge/v1
kind: Pipeline
metadata: { name: t, tenant: t }
spec:
  source:
    type: postgres
    config: { id: pg, dsn: host=h dbname=d, publication: p, slot: s, tables: [public.t] }
  processors: []
  sinks: []
"#;
        let mut spec: PipelineSpec = serde_yaml::from_str(yaml).unwrap();
        spec.spec.sinks = sinks;
        spec
    }
}

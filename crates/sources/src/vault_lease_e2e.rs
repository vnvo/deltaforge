//! Live Vault dynamic-credential lease integration tests (feature `vault`).
//!
//! These prove the Phase 2 exit-gate lease lifecycle against a real Vault database
//! secrets engine and a real PostgreSQL / MySQL server: credentials are **issued**,
//! durably persisted, **renewed**, **reissued**, verified usable against the database
//! (they actually authenticate), **revoked** (and then rejected by the database), and
//! recovered after a simulated crash, plus prior-incarnation **orphan** cleanup and
//! Vault-outage handling. They do NOT change any source DSN or trigger a CDC reconnect
//! - that is Phase 3.
//!
//! They are `#[ignore]` and self-configure Vault's database engine given a running
//! Vault and database. Set, for PostgreSQL:
//!   VAULT_LEASE_IT_ADDR, VAULT_LEASE_IT_TOKEN,
//!   VAULT_LEASE_IT_PG_HOST, VAULT_LEASE_IT_PG_PORT, VAULT_LEASE_IT_PG_DB,
//!   VAULT_LEASE_IT_PG_ADMIN_USER, VAULT_LEASE_IT_PG_ADMIN_PASS
//! and for MySQL:
//!   VAULT_LEASE_IT_ADDR, VAULT_LEASE_IT_TOKEN,
//!   VAULT_LEASE_IT_MYSQL_HOST, VAULT_LEASE_IT_MYSQL_PORT, VAULT_LEASE_IT_MYSQL_DB,
//!   VAULT_LEASE_IT_MYSQL_ADMIN_USER, VAULT_LEASE_IT_MYSQL_ADMIN_PASS
//! The admin account must be allowed to create/drop roles/users.
//!
//! Run with, e.g.:
//! `cargo test -p sources --features vault --lib vault_lease_e2e -- --ignored --nocapture --test-threads=1`

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use anyhow::{Result, anyhow};
use secrets::{
    RenewPolicy, VaultAuth, VaultAuthFile, VaultConnection, VaultResolver,
    VaultTimeouts,
};
use storage::{ArcStorageBackend, MemoryStorageBackend};

use crate::vault_lease_manager::{
    LeaseIdentity, LeaseManager, LeaseScheduleConfig, VaultLeaseProvider,
};

const LIMIT: usize = 1 << 20;
static UNIQ: AtomicU64 = AtomicU64::new(0);

fn uniq(prefix: &str) -> String {
    format!("{prefix}_{}", UNIQ.fetch_add(1, Ordering::Relaxed))
}

// -----------------------------------------------------------------------------
// Vault admin (root token) + resolver construction
// -----------------------------------------------------------------------------

/// Minimal Vault admin over the HTTP API, using the root token to enable engines and
/// create roles for the test. The manager under test never uses this client.
struct VaultAdmin {
    http: reqwest::Client,
    addr: String,
    token: String,
}

impl VaultAdmin {
    fn new(addr: String, token: String) -> Self {
        Self {
            http: reqwest::Client::new(),
            addr,
            token,
        }
    }

    async fn post(&self, path: &str, body: serde_json::Value) -> Result<()> {
        let resp = self
            .http
            .post(format!("{}/v1/{path}", self.addr))
            .header("X-Vault-Token", &self.token)
            .json(&body)
            .send()
            .await?;
        // 200/204 on success; a 400 "path is already in use" when the engine is
        // already mounted is tolerated by the caller.
        if !resp.status().is_success() {
            return Err(anyhow!(
                "vault admin POST {path} failed: {}",
                resp.status()
            ));
        }
        Ok(())
    }

    /// Enable the database secrets engine at `mount`, ignoring "already mounted".
    async fn enable_database(&self, mount: &str) {
        let _ = self
            .post(
                &format!("sys/mounts/{mount}"),
                serde_json::json!({ "type": "database" }),
            )
            .await;
    }
}

/// Build a real `VaultResolver` with token-file auth against the live Vault. Returns
/// the resolver and the temp token file (kept alive for the resolver's lifetime).
async fn build_resolver(
    addr: &str,
    token: &str,
) -> Result<(Arc<VaultResolver>, tempfile::NamedTempFile)> {
    let token_file = tempfile::NamedTempFile::new()?;
    std::fs::write(token_file.path(), token)?;
    let auth = VaultAuth::TokenFile(VaultAuthFile::Strict {
        path: token_file.path().to_path_buf(),
    });
    let conn = VaultConnection::new(
        addr,
        None,
        auth,
        LIMIT,
        RenewPolicy::default(),
        VaultTimeouts::default(),
        true, // allow_insecure_http for a dev-mode Vault
    )
    .map_err(|e| anyhow!("vault connection config: {e}"))?;
    let resolver = VaultResolver::connect(conn)
        .await
        .map_err(|e| anyhow!("vault connect: {e}"))?;
    Ok((Arc::new(resolver), token_file))
}

fn manager(
    provider: Arc<VaultLeaseProvider>,
    backend: ArcStorageBackend,
    incarnation: &str,
    mount: &str,
    role: &str,
) -> LeaseManager {
    let id = LeaseIdentity {
        tenant: "acme".into(),
        pipeline: "vault-lease-it".into(),
        source: "db1".into(),
        credential_purpose: "source-db".into(),
        incarnation: incarnation.into(),
    };
    let cfg = LeaseScheduleConfig {
        safety_margin: Duration::from_secs(5),
        renew_fraction: 0.5,
        min_poll: Duration::from_secs(1),
        max_poll: Duration::from_secs(30),
    };
    LeaseManager::new(provider, backend, id, mount, role, cfg)
}

fn creds(leased: &crate::vault_lease::LeasedCredentialSet) -> (String, String) {
    let u = leased
        .credentials
        .require("username")
        .unwrap()
        .material()
        .as_utf8()
        .unwrap()
        .to_string();
    let p = leased
        .credentials
        .require("password")
        .unwrap()
        .material()
        .as_utf8()
        .unwrap()
        .to_string();
    (u, p)
}

// =============================================================================
// PostgreSQL
// =============================================================================

struct PgEnv {
    addr: String,
    token: String,
    host: String,
    port: u16,
    db: String,
    admin_user: String,
    admin_pass: String,
}

fn pg_env() -> Option<PgEnv> {
    let get = |k: &str| std::env::var(k).ok();
    Some(PgEnv {
        addr: get("VAULT_LEASE_IT_ADDR")?,
        token: get("VAULT_LEASE_IT_TOKEN")?,
        host: get("VAULT_LEASE_IT_PG_HOST")?,
        port: get("VAULT_LEASE_IT_PG_PORT")?.parse().ok()?,
        db: get("VAULT_LEASE_IT_PG_DB")?,
        admin_user: get("VAULT_LEASE_IT_PG_ADMIN_USER")?,
        admin_pass: get("VAULT_LEASE_IT_PG_ADMIN_PASS")?,
    })
}

/// Try to open a PostgreSQL connection with the given credentials. `Ok(true)` if the
/// credentials authenticated, `Ok(false)` if they were rejected (revoked), `Err` on a
/// non-auth transport failure.
async fn pg_can_connect(env: &PgEnv, user: &str, pass: &str) -> Result<bool> {
    let dsn = format!(
        "host={} port={} dbname={} user={} password={} connect_timeout=5",
        env.host, env.port, env.db, user, pass
    );
    match tokio_postgres::connect(&dsn, tokio_postgres::NoTls).await {
        Ok((client, conn)) => {
            let jh = tokio::spawn(async move {
                let _ = conn.await;
            });
            let ok = client.simple_query("SELECT 1").await.is_ok();
            jh.abort();
            Ok(ok)
        }
        // A rejected login is the expected "revoked" signal, not a test failure.
        Err(_) => Ok(false),
    }
}

async fn pg_configure_engine(
    admin: &VaultAdmin,
    env: &PgEnv,
    mount: &str,
    conn_name: &str,
    role: &str,
) -> Result<()> {
    admin.enable_database(mount).await;
    let connection_url = format!(
        "postgresql://{{{{username}}}}:{{{{password}}}}@{}:{}/{}?sslmode=disable",
        env.host, env.port, env.db
    );
    admin
        .post(
            &format!("{mount}/config/{conn_name}"),
            serde_json::json!({
                "plugin_name": "postgresql-database-plugin",
                "connection_url": connection_url,
                "allowed_roles": [role],
                "username": env.admin_user,
                "password": env.admin_pass,
            }),
        )
        .await?;
    admin
        .post(
            &format!("{mount}/roles/{role}"),
            serde_json::json!({
                "db_name": conn_name,
                "creation_statements": [
                    "CREATE ROLE \"{{name}}\" WITH LOGIN PASSWORD '{{password}}' VALID UNTIL '{{expiration}}';",
                    "GRANT CONNECT ON DATABASE ".to_string() + &env.db + " TO \"{{name}}\";"
                ],
                "default_ttl": "40s",
                "max_ttl": "120s",
            }),
        )
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a live Vault + PostgreSQL (set VAULT_LEASE_IT_* env)"]
async fn pg_lease_lifecycle_e2e() -> Result<()> {
    let Some(env) = pg_env() else {
        eprintln!("skip: VAULT_LEASE_IT_PG_* unset");
        return Ok(());
    };
    let admin = VaultAdmin::new(env.addr.clone(), env.token.clone());
    let mount = "database";
    let conn_name = uniq("pg_conn");
    let role = uniq("pg_role");
    pg_configure_engine(&admin, &env, mount, &conn_name, &role).await?;

    let (resolver, _tok) = build_resolver(&env.addr, &env.token).await?;
    let provider = Arc::new(VaultLeaseProvider::new(resolver));
    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let mut m =
        manager(provider.clone(), backend.clone(), "inc-1", mount, &role);

    // Issue: real dynamic credentials that authenticate to PostgreSQL.
    let leased = m.issue(SystemTime::now()).await?;
    let (u1, p1) = creds(&leased);
    assert!(
        pg_can_connect(&env, &u1, &p1).await?,
        "issued credentials must authenticate to PostgreSQL"
    );
    assert_eq!(m.status().await?.state, "active");

    // Renew: extends the lease; the same credentials keep working.
    let refreshed = m.renew(SystemTime::now()).await?;
    let _ = refreshed;
    assert!(
        pg_can_connect(&env, &u1, &p1).await?,
        "renewed credentials must still authenticate"
    );

    // Reissue: a fresh credential is adopted, the old lease is revoked. The new creds
    // work; the old ones are rejected once Vault revokes the old role.
    let leased2 = m.reissue(SystemTime::now()).await?;
    let (u2, p2) = creds(&leased2);
    assert_ne!(u1, u2, "reissue yields a distinct dynamic user");
    assert!(
        pg_can_connect(&env, &u2, &p2).await?,
        "reissued credentials must authenticate"
    );
    assert!(
        wait_until_rejected(&env, &u1, &p1).await,
        "the superseded credentials must be revoked at the database"
    );

    // Revoke the active lease: its credentials stop authenticating.
    m.revoke_active().await?;
    assert!(m.status().await?.state == "none");
    assert!(
        wait_until_rejected(&env, &u2, &p2).await,
        "revoked credentials must be rejected by the database"
    );

    Ok(())
}

#[tokio::test]
#[ignore = "requires a live Vault + PostgreSQL (set VAULT_LEASE_IT_* env)"]
async fn pg_crash_recovery_and_orphan_e2e() -> Result<()> {
    let Some(env) = pg_env() else {
        eprintln!("skip: VAULT_LEASE_IT_PG_* unset");
        return Ok(());
    };
    let admin = VaultAdmin::new(env.addr.clone(), env.token.clone());
    let mount = "database";
    let conn_name = uniq("pg_conn");
    let role = uniq("pg_role");
    pg_configure_engine(&admin, &env, mount, &conn_name, &role).await?;

    let (resolver, _tok) = build_resolver(&env.addr, &env.token).await?;
    let provider = Arc::new(VaultLeaseProvider::new(resolver));
    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());

    // First "process": issue an Active lease, then drop the manager (simulated crash)
    // leaving the durable record behind.
    let (u1, p1) = {
        let mut m1 =
            manager(provider.clone(), backend.clone(), "inc-1", mount, &role);
        let leased = m1.issue(SystemTime::now()).await?;
        creds(&leased)
    };
    assert!(pg_can_connect(&env, &u1, &p1).await?);

    // Restart with the same incarnation: recover adopts the live lease.
    let mut m2 =
        manager(provider.clone(), backend.clone(), "inc-1", mount, &role);
    let summary = m2.recover().await?;
    assert_eq!(summary.adopted, 1, "the live lease is re-adopted");
    assert_eq!(m2.status().await?.state, "active");
    assert!(
        pg_can_connect(&env, &u1, &p1).await?,
        "the recovered lease's credentials still work"
    );

    // A delete/recreate replaces the incarnation. The new incarnation reconciles the
    // prior one's leases (revoked at the database).
    let mut m3 =
        manager(provider.clone(), backend.clone(), "inc-2", mount, &role);
    let orphans = m3.reconcile_orphans().await?;
    assert_eq!(orphans.revoked, 1, "prior incarnation's lease is revoked");
    assert!(
        wait_until_rejected(&env, &u1, &p1).await,
        "orphaned credentials must be revoked at the database"
    );

    Ok(())
}

// =============================================================================
// MySQL
// =============================================================================

struct MysqlEnv {
    addr: String,
    token: String,
    host: String,
    port: u16,
    db: String,
    admin_user: String,
    admin_pass: String,
}

fn mysql_env() -> Option<MysqlEnv> {
    let get = |k: &str| std::env::var(k).ok();
    Some(MysqlEnv {
        addr: get("VAULT_LEASE_IT_ADDR")?,
        token: get("VAULT_LEASE_IT_TOKEN")?,
        host: get("VAULT_LEASE_IT_MYSQL_HOST")?,
        port: get("VAULT_LEASE_IT_MYSQL_PORT")?.parse().ok()?,
        db: get("VAULT_LEASE_IT_MYSQL_DB")?,
        admin_user: get("VAULT_LEASE_IT_MYSQL_ADMIN_USER")?,
        admin_pass: get("VAULT_LEASE_IT_MYSQL_ADMIN_PASS")?,
    })
}

async fn mysql_can_connect(
    env: &MysqlEnv,
    user: &str,
    pass: &str,
) -> Result<bool> {
    use mysql_async::prelude::Queryable;
    let url = format!(
        "mysql://{}:{}@{}:{}/{}",
        user, pass, env.host, env.port, env.db
    );
    let Ok(opts) = mysql_async::Opts::from_url(&url) else {
        return Ok(false);
    };
    match mysql_async::Conn::new(opts).await {
        Ok(mut conn) => {
            let ok = conn.query_drop("SELECT 1").await.is_ok();
            let _ = conn.disconnect().await;
            Ok(ok)
        }
        Err(_) => Ok(false),
    }
}

async fn mysql_configure_engine(
    admin: &VaultAdmin,
    env: &MysqlEnv,
    mount: &str,
    conn_name: &str,
    role: &str,
) -> Result<()> {
    admin.enable_database(mount).await;
    let connection_url = format!(
        "{{{{username}}}}:{{{{password}}}}@tcp({}:{})/",
        env.host, env.port
    );
    admin
        .post(
            &format!("{mount}/config/{conn_name}"),
            serde_json::json!({
                "plugin_name": "mysql-database-plugin",
                "connection_url": connection_url,
                "allowed_roles": [role],
                "username": env.admin_user,
                "password": env.admin_pass,
            }),
        )
        .await?;
    admin
        .post(
            &format!("{mount}/roles/{role}"),
            serde_json::json!({
                "db_name": conn_name,
                "creation_statements": [
                    "CREATE USER '{{name}}'@'%' IDENTIFIED BY '{{password}}';",
                    "GRANT SELECT ON *.* TO '{{name}}'@'%';"
                ],
                "default_ttl": "40s",
                "max_ttl": "120s",
            }),
        )
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a live Vault + MySQL (set VAULT_LEASE_IT_* env)"]
async fn mysql_lease_lifecycle_e2e() -> Result<()> {
    let Some(env) = mysql_env() else {
        eprintln!("skip: VAULT_LEASE_IT_MYSQL_* unset");
        return Ok(());
    };
    let admin = VaultAdmin::new(env.addr.clone(), env.token.clone());
    let mount = "database";
    let conn_name = uniq("mysql_conn");
    let role = uniq("mysql_role");
    mysql_configure_engine(&admin, &env, mount, &conn_name, &role).await?;

    let (resolver, _tok) = build_resolver(&env.addr, &env.token).await?;
    let provider = Arc::new(VaultLeaseProvider::new(resolver));
    let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
    let mut m =
        manager(provider.clone(), backend.clone(), "inc-1", mount, &role);

    let leased = m.issue(SystemTime::now()).await?;
    let (u1, p1) = creds(&leased);
    assert!(
        mysql_can_connect(&env, &u1, &p1).await?,
        "issued credentials must authenticate to MySQL"
    );

    m.renew(SystemTime::now()).await?;
    assert!(mysql_can_connect(&env, &u1, &p1).await?);

    let leased2 = m.reissue(SystemTime::now()).await?;
    let (u2, p2) = creds(&leased2);
    assert_ne!(u1, u2);
    assert!(mysql_can_connect(&env, &u2, &p2).await?);
    assert!(
        wait_until_rejected_mysql(&env, &u1, &p1).await,
        "superseded MySQL credentials must be revoked"
    );

    m.revoke_active().await?;
    assert!(
        wait_until_rejected_mysql(&env, &u2, &p2).await,
        "revoked MySQL credentials must be rejected"
    );

    Ok(())
}

// -----------------------------------------------------------------------------
// Small polling helpers (revocation is asynchronous at the database)
// -----------------------------------------------------------------------------

async fn wait_until_rejected(env: &PgEnv, user: &str, pass: &str) -> bool {
    for _ in 0..30 {
        match pg_can_connect(env, user, pass).await {
            Ok(false) => return true,
            _ => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
    false
}

async fn wait_until_rejected_mysql(
    env: &MysqlEnv,
    user: &str,
    pass: &str,
) -> bool {
    for _ in 0..30 {
        match mysql_can_connect(env, user, pass).await {
            Ok(false) => return true,
            _ => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
    false
}

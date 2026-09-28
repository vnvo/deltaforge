//! Vault provider end-to-end tests (feature `vault`).
//!
//! Two tiers:
//!
//! - **Hermetic HTTP-server tests** (no docker, run by default): a local stub
//!   server validates the Kubernetes login wire path (JWT re-read from file ->
//!   `POST auth/<mount>/login` -> token adoption -> KV read), and a never-
//!   responding server proves requests are bounded by the configured timeout
//!   instead of hanging. A real Kubernetes `TokenReview` API is not available in
//!   CI, so live k8s login is covered here at the wire-protocol level.
//!
//! - **Live Vault tests** (`#[ignore = "requires docker"]`): a dev-mode Vault
//!   container covers KV v2 version pinning, token-file auth, background token
//!   renewal keeping reads working past the original TTL, and recovery from a
//!   revoked token via a rotated token file. Run with:
//!   `cargo test -p secrets --features vault -- --ignored`.
#![cfg(feature = "vault")]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use secrets::{
    ProviderFailureKind, RenewPolicy, SecretError, SecretProvider,
    SecretReference, SecretResolver, VaultAuth, VaultAuthFile, VaultConnection,
    VaultResolver, VaultTimeouts,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn vault_ref(location: &str, selector: &str) -> SecretReference {
    SecretReference::new(SecretProvider::Vault, location)
        .with_selector(selector)
}

// ---------------------------------------------------------------------------
// Minimal HTTP plumbing for the hermetic server tests
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct CapturedRequest {
    method: String,
    path: String,
    body: String,
}

fn header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

/// Read one HTTP/1.1 request (request line + headers + Content-Length body).
async fn read_request(stream: &mut TcpStream) -> Option<CapturedRequest> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 2048];
    let head_end = loop {
        if let Some(pos) = header_end(&buf) {
            break pos;
        }
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > 64 * 1024 {
            return None;
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let mut lines = head.split("\r\n");
    let mut req_line = lines.next()?.split_whitespace();
    let method = req_line.next()?.to_string();
    let path = req_line.next()?.to_string();
    let mut content_length = 0usize;
    for line in lines {
        let lower = line.to_ascii_lowercase();
        if let Some(v) = lower.strip_prefix("content-length:") {
            content_length = v.trim().parse().unwrap_or(0);
        }
    }
    let mut body = buf[head_end + 4..].to_vec();
    while body.len() < content_length {
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    Some(CapturedRequest {
        method,
        path,
        body: String::from_utf8_lossy(&body).to_string(),
    })
}

async fn write_json(stream: &mut TcpStream, status: &str, body: &str) {
    let resp = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(resp.as_bytes()).await;
    let _ = stream.flush().await;
}

/// Canned Vault-shaped responses for the Kubernetes login path.
fn route(req: &CapturedRequest) -> (&'static str, String) {
    if req.method == "POST" && req.path.ends_with("/v1/auth/kubernetes/login") {
        (
            "200 OK",
            r#"{"auth":{"client_token":"k8s-minted-token","lease_duration":3600,"renewable":true}}"#
                .to_string(),
        )
    } else if req.method == "GET"
        && req.path.contains("/v1/auth/token/lookup-self")
    {
        (
            "200 OK",
            r#"{"data":{"ttl":3600,"renewable":true}}"#.to_string(),
        )
    } else if req.method == "POST"
        && req.path.contains("/v1/auth/token/renew-self")
    {
        (
            "200 OK",
            r#"{"auth":{"lease_duration":3600,"renewable":true}}"#.to_string(),
        )
    } else if req.method == "GET" && req.path.contains("/v1/secret/data/orders")
    {
        (
            "200 OK",
            r#"{"data":{"data":{"password":"p@ss","username":"df"},"metadata":{"version":1}}}"#
                .to_string(),
        )
    } else if req.method == "POST"
        && req.path.ends_with("/v1/sys/leases/lookup")
    {
        // An absent lease: Vault answers 4xx with an "invalid lease" marker body.
        (
            "400 Bad Request",
            r#"{"errors":["invalid lease"]}"#.to_string(),
        )
    } else if req.method == "POST" && req.path.ends_with("/v1/sys/leases/renew")
    {
        // An absent lease can also surface as a 404.
        (
            "404 Not Found",
            r#"{"errors":["lease not found"]}"#.to_string(),
        )
    } else if req.method == "POST"
        && req.path.ends_with("/v1/sys/leases/revoke")
    {
        if req.body.contains("gone-lease") {
            ("404 Not Found", r#"{"errors":[]}"#.to_string())
        } else {
            // Real revocation: 204 No Content.
            ("204 No Content", String::new())
        }
    } else if req.method == "GET" && req.path.contains("/v1/database/creds/") {
        (
            "200 OK",
            r#"{"data":{"username":"dyn","password":"pw"},"lease_id":"database/creds/x/abc","lease_duration":3600,"renewable":true}"#
                .to_string(),
        )
    } else {
        ("404 Not Found", r#"{"errors":[]}"#.to_string())
    }
}

/// Start a stub Vault, capturing every request. One request per connection
/// (`Connection: close`), so no keep-alive bookkeeping is needed.
async fn spawn_stub(captured: Arc<Mutex<Vec<CapturedRequest>>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let captured = captured.clone();
            tokio::spawn(async move {
                if let Some(req) = read_request(&mut stream).await {
                    let (status, body) = route(&req);
                    captured.lock().unwrap().push(req);
                    write_json(&mut stream, status, &body).await;
                }
            });
        }
    });
    addr
}

// ---------------------------------------------------------------------------
// Hermetic tests (no docker)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn kubernetes_login_wire_path() {
    let captured = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let addr = spawn_stub(captured.clone()).await;

    let dir = tempfile::tempdir().unwrap();
    let jwt_path = dir.path().join("sa-token");
    // Projected SA JWTs are re-read on each auth; include a trailing newline to
    // prove it is trimmed before being sent.
    std::fs::write(&jwt_path, "header.payload.sig\n").unwrap();

    let conn = VaultConnection::new(
        addr,
        None,
        VaultAuth::Kubernetes {
            mount: "kubernetes".to_string(),
            role: "orders-app".to_string(),
            jwt: VaultAuthFile::Strict { path: jwt_path },
        },
        4096,
        RenewPolicy::default(),
        VaultTimeouts::default(),
        true, // http stub
    )
    .unwrap();

    let resolver = VaultResolver::connect(conn)
        .await
        .expect("connect via kubernetes login");
    let got = resolver
        .resolve(&vault_ref("secret/orders", "password"))
        .await
        .expect("resolve after k8s login");
    assert_eq!(got.material().as_utf8(), Some("p@ss"));

    // The login request carried the role and the JWT read from the file, trimmed.
    let reqs = captured.lock().unwrap();
    let login = reqs
        .iter()
        .find(|r| r.path.ends_with("/login"))
        .expect("a login request was made");
    assert!(login.body.contains("orders-app"), "role in login body");
    assert!(
        login.body.contains("header.payload.sig"),
        "jwt in login body"
    );
    assert!(!login.body.contains('\n'), "jwt newline trimmed");
}

#[tokio::test]
async fn lease_ops_map_absent_and_validate_paths() {
    let captured = Arc::new(Mutex::new(Vec::<CapturedRequest>::new()));
    let addr = spawn_stub(captured.clone()).await;

    let dir = tempfile::tempdir().unwrap();
    let token_path = dir.path().join("token");
    std::fs::write(&token_path, "root").unwrap();
    let conn = VaultConnection::new(
        addr,
        None,
        VaultAuth::TokenFile(VaultAuthFile::Strict { path: token_path }),
        4096,
        RenewPolicy::default(),
        VaultTimeouts::default(),
        true,
    )
    .unwrap();
    let resolver = VaultResolver::connect(conn).await.unwrap();

    // Absent lease on lookup (400 + marker) and renew (404) -> NotFound.
    assert!(matches!(
        resolver.lookup_lease("x").await.unwrap_err(),
        SecretError::NotFound(_)
    ));
    assert!(matches!(
        resolver.renew_lease("x", None).await.unwrap_err(),
        SecretError::NotFound(_)
    ));

    // Revoke: 204 succeeds; an absent (404) lease maps to NotFound (idempotent).
    resolver.revoke_lease("present-lease").await.unwrap();
    assert!(matches!(
        resolver.revoke_lease("gone-lease").await.unwrap_err(),
        SecretError::NotFound(_)
    ));

    // A dynamic role with a path-altering segment is rejected before any request as a
    // hard (non-absent) provider error.
    let before = captured.lock().unwrap().len();
    // `LeasedRead` is intentionally not `Debug` (it holds secret material), so match
    // rather than `unwrap_err`.
    match resolver.read_db_credentials("database", "bad/role").await {
        Ok(_) => panic!("invalid role must be rejected"),
        Err(SecretError::Provider {
            kind: ProviderFailureKind::Other,
            ..
        }) => {}
        Err(e) => {
            panic!("expected hard provider error for invalid role: {e:?}")
        }
    }
    assert_eq!(
        captured.lock().unwrap().len(),
        before,
        "invalid role must not issue a request"
    );

    // A valid role reads dynamic credentials with a lease id.
    let leased = resolver
        .read_db_credentials("database", "orders")
        .await
        .unwrap();
    assert_eq!(leased.lease_id, "database/creds/x/abc");
}

#[tokio::test]
async fn requests_are_bounded_by_timeout_not_hung() {
    // A server that accepts connections but never responds.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let _held = stream;
                tokio::time::sleep(Duration::from_secs(60)).await;
            });
        }
    });

    let dir = tempfile::tempdir().unwrap();
    let token_path = dir.path().join("token");
    std::fs::write(&token_path, "root\n").unwrap();

    let conn = VaultConnection::new(
        format!("http://127.0.0.1:{port}"),
        None,
        VaultAuth::TokenFile(VaultAuthFile::Strict { path: token_path }),
        4096,
        RenewPolicy::default(),
        VaultTimeouts {
            connect: Duration::from_secs(1),
            request: Duration::from_secs(1),
        },
        true,
    )
    .unwrap();

    let start = Instant::now();
    let err = VaultResolver::connect(conn)
        .await
        .expect_err("connect must time out, not hang");
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(10),
        "connect hung: {elapsed:?}"
    );
    // Redacted provider error (timeout classified, no body leaked).
    let shown = format!("{err}");
    assert!(!shown.contains("root"));
}

// ---------------------------------------------------------------------------
// Live Vault tests (require docker)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod live {
    use super::*;
    use testcontainers::{
        ContainerAsync, GenericImage, ImageExt, core::WaitFor,
        runners::AsyncRunner,
    };

    const ROOT: &str = "root";

    async fn start_vault() -> (ContainerAsync<GenericImage>, String) {
        let image = GenericImage::new("hashicorp/vault", "1.15")
            .with_wait_for(WaitFor::message_on_stdout("Vault server started!"))
            .with_env_var("VAULT_DEV_ROOT_TOKEN_ID", ROOT)
            .with_env_var("VAULT_DEV_LISTEN_ADDRESS", "0.0.0.0:8200");
        let container = image.start().await.expect("start vault dev container");
        let port = container
            .get_host_port_ipv4(8200)
            .await
            .expect("vault host port");
        (container, format!("http://127.0.0.1:{port}"))
    }

    fn admin() -> reqwest::Client {
        reqwest::Client::new()
    }

    async fn put_kv(base: &str, path: &str, data: serde_json::Value) {
        admin()
            .post(format!("{base}/v1/secret/data/{path}"))
            .header("X-Vault-Token", ROOT)
            .json(&serde_json::json!({ "data": data }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    async fn put_read_policy(base: &str, name: &str, kv_path: &str) {
        let hcl = format!(
            "path \"secret/data/{kv_path}\" {{ capabilities = [\"read\"] }}"
        );
        admin()
            .put(format!("{base}/v1/sys/policies/acl/{name}"))
            .header("X-Vault-Token", ROOT)
            .json(&serde_json::json!({ "policy": hcl }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    async fn create_token(
        base: &str,
        ttl: &str,
        renewable: bool,
        policies: &[&str],
    ) -> String {
        let resp: serde_json::Value = admin()
            .post(format!("{base}/v1/auth/token/create"))
            .header("X-Vault-Token", ROOT)
            .json(&serde_json::json!({
                "ttl": ttl,
                "renewable": renewable,
                "policies": policies,
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        resp["auth"]["client_token"].as_str().unwrap().to_string()
    }

    async fn revoke_token(base: &str, token: &str) {
        admin()
            .post(format!("{base}/v1/auth/token/revoke"))
            .header("X-Vault-Token", ROOT)
            .json(&serde_json::json!({ "token": token }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    fn write_token_file(
        dir: &std::path::Path,
        token: &str,
    ) -> std::path::PathBuf {
        let path = dir.join("vault-token");
        std::fs::write(&path, token).unwrap();
        path
    }

    fn root_conn(
        base: &str,
        token_path: std::path::PathBuf,
    ) -> VaultConnection {
        VaultConnection::new(
            base,
            None,
            VaultAuth::TokenFile(VaultAuthFile::Strict { path: token_path }),
            4096,
            RenewPolicy::default(),
            VaultTimeouts::default(),
            true, // dev-mode Vault is http
        )
        .unwrap()
    }

    #[tokio::test]
    #[ignore = "requires docker"]
    async fn token_file_auth_and_kv_version_pinning() {
        let (_c, base) = start_vault().await;
        // v1 then v2 of the same record.
        put_kv(&base, "orders", serde_json::json!({ "password": "p1" })).await;
        put_kv(&base, "orders", serde_json::json!({ "password": "p2" })).await;

        let dir = tempfile::tempdir().unwrap();
        let resolver = VaultResolver::connect(root_conn(
            &base,
            write_token_file(dir.path(), ROOT),
        ))
        .await
        .expect("connect with root token file");

        // Latest resolves to v2 and carries the record version.
        let latest = resolver
            .resolve(&vault_ref("secret/orders", "password"))
            .await
            .unwrap();
        assert_eq!(latest.material().as_utf8(), Some("p2"));
        assert_eq!(latest.provider_version(), Some("2"));

        // A pinned version reads that exact version.
        let pinned = resolver
            .resolve(&vault_ref("secret/orders", "password").with_version("1"))
            .await
            .unwrap();
        assert_eq!(pinned.material().as_utf8(), Some("p1"));
        assert_eq!(pinned.provider_version(), Some("1"));
    }

    #[tokio::test]
    #[ignore = "requires docker"]
    async fn background_renewal_keeps_reads_working_past_ttl() {
        let (_c, base) = start_vault().await;
        put_kv(
            &base,
            "orders",
            serde_json::json!({ "password": "secret1" }),
        )
        .await;
        put_read_policy(&base, "orders-read", "orders").await;
        // Short-TTL renewable token: without renewal it would expire in ~6s.
        let token = create_token(&base, "6s", true, &["orders-read"]).await;

        let dir = tempfile::tempdir().unwrap();
        let resolver = VaultResolver::connect(root_conn(
            &base,
            write_token_file(dir.path(), &token),
        ))
        .await
        .expect("connect with short-ttl token");

        // Sleep well past the original TTL; background renewal must keep it alive.
        tokio::time::sleep(Duration::from_secs(9)).await;
        let got = resolver
            .resolve(&vault_ref("secret/orders", "password"))
            .await
            .expect(
                "read must still work after original TTL (token was renewed)",
            );
        assert_eq!(got.material().as_utf8(), Some("secret1"));
    }

    #[tokio::test]
    #[ignore = "requires docker"]
    async fn revoked_token_recovers_via_rotated_file() {
        let (_c, base) = start_vault().await;
        put_kv(
            &base,
            "orders",
            serde_json::json!({ "password": "secret1" }),
        )
        .await;
        put_read_policy(&base, "orders-read", "orders").await;

        let t1 = create_token(&base, "60s", true, &["orders-read"]).await;
        let dir = tempfile::tempdir().unwrap();
        let token_path = write_token_file(dir.path(), &t1);

        // Aggressive renewal so the failure/recovery cycle fires within seconds.
        let conn = VaultConnection::new(
            &base,
            None,
            VaultAuth::TokenFile(VaultAuthFile::Strict {
                path: token_path.clone(),
            }),
            4096,
            RenewPolicy {
                interval: Duration::from_secs(600),
                safety_margin: Duration::from_secs(58),
                fraction: 0.5,
                jitter: 0.0,
                min_delay: Duration::from_secs(1),
            },
            VaultTimeouts::default(),
            true,
        )
        .unwrap();
        let resolver =
            VaultResolver::connect(conn).await.expect("connect with t1");
        assert_eq!(
            resolver
                .resolve(&vault_ref("secret/orders", "password"))
                .await
                .unwrap()
                .material()
                .as_utf8(),
            Some("secret1")
        );

        // Rotate the on-disk token to a fresh one, then revoke the old one.
        let t2 = create_token(&base, "60s", true, &["orders-read"]).await;
        std::fs::write(&token_path, &t2).unwrap();
        revoke_token(&base, &t1).await;

        // Recovery: renewal of t1 fails -> reauth re-reads the file -> adopts t2.
        // Poll until reads work again (bounded).
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            match resolver
                .resolve(&vault_ref("secret/orders", "password"))
                .await
            {
                Ok(v) if v.material().as_utf8() == Some("secret1") => break,
                _ if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                other => {
                    panic!("did not recover after token rotation: {other:?}")
                }
            }
        }
    }
}

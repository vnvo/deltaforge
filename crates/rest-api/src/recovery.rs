//! The recovery admin API (`docs/design/recovery-cli.md`, section 2).
//!
//! Served only on the loopback admin listener, never on the public API.
//! Every request needs the admin bearer token, compared in constant time,
//! and must come from a loopback peer (the accepted connection's address:
//! forwarded headers are never read). Bodies are bounded; each request has
//! an explicit deadline. Errors carry codes and fixed messages only.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{ConnectInfo, DefaultBodyLimit, Path, Request, State},
    http::{StatusCode, header::AUTHORIZATION},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// The largest request body the admin API reads.
pub const MAX_BODY_BYTES: usize = 64 * 1024;
/// Deadlines of the read-only requests.
pub const READ_TIMEOUT: Duration = Duration::from_secs(60);
/// How long an apply request waits for its operation. The operation itself
/// is not bound to the request: on timeout it continues, and diagnose
/// shows its progress.
pub const APPLY_WAIT: Duration = Duration::from_secs(300);

/// A plan request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanRequest {
    pub operation: String,
    #[serde(default)]
    pub incident: Option<String>,
    #[serde(default)]
    pub args: std::collections::BTreeMap<String, String>,
}

/// An apply request: the plan request plus the reviewed proof and the
/// operator's asserted identity and reason.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyRequest {
    #[serde(flatten)]
    pub plan: PlanRequest,
    pub expect_proof: String,
    pub actor: String,
    pub reason: String,
}

/// Who called, as the server observed it.
#[derive(Debug, Clone)]
pub struct Caller {
    /// The accepted connection's peer address.
    pub origin: String,
    /// How the caller authenticated.
    pub credential: &'static str,
}

/// A recovery request that did not succeed: a stable code, an HTTP status
/// and a fixed, secret-free message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryApiError {
    pub status: u16,
    pub code: &'static str,
    pub message: String,
    /// Extra structured detail (for example the newly computed proof).
    pub detail: Option<Value>,
}

impl RecoveryApiError {
    pub fn new(
        status: u16,
        code: &'static str,
        message: impl Into<String>,
    ) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            detail: None,
        }
    }

    pub fn with_detail(mut self, detail: Value) -> Self {
        self.detail = Some(detail);
        self
    }
}

impl IntoResponse for RecoveryApiError {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.status)
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let mut body = json!({ "code": self.code, "message": self.message });
        if let Some(d) = self.detail {
            body["detail"] = d;
        }
        (status, Json(body)).into_response()
    }
}

#[async_trait]
pub trait RecoveryController: Send + Sync {
    /// Read-only: state, pending or last operation, incidents and the
    /// operations that apply to them.
    async fn diagnose(&self, pipeline: &str)
    -> Result<Value, RecoveryApiError>;
    /// Read-only: the canonical plan, its proof, and whether apply is
    /// currently possible.
    async fn plan(
        &self,
        pipeline: &str,
        req: PlanRequest,
    ) -> Result<Value, RecoveryApiError>;
    /// Apply (or resume) an operation. Runs to completion even if the
    /// request is abandoned.
    async fn apply(
        &self,
        pipeline: &str,
        req: ApplyRequest,
        caller: Caller,
    ) -> Result<Value, RecoveryApiError>;
}

/// The admin bearer token. Never printed: `Debug` is redacted.
#[derive(Clone)]
pub struct AdminToken(Arc<[u8; 32]>);

impl std::fmt::Debug for AdminToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AdminToken(<redacted>)")
    }
}

impl AdminToken {
    /// The smallest accepted token.
    pub const MIN_LEN: usize = 32;

    /// A token from its text: non-empty, at least [`Self::MIN_LEN`] bytes,
    /// without newline, carriage return or NUL.
    pub fn new(token: &str) -> Result<Self, &'static str> {
        if token.is_empty() {
            return Err("the admin token is empty");
        }
        if token.bytes().any(|b| b == b'\n' || b == b'\r' || b == 0) {
            return Err("the admin token contains a newline or NUL");
        }
        if token.len() < Self::MIN_LEN {
            return Err("the admin token is shorter than 32 bytes");
        }
        Ok(Self(Arc::new(Sha256::digest(token.as_bytes()).into())))
    }

    /// Whether `presented` is the token. Constant time in the presented
    /// value: both sides are hashed and every byte is compared.
    pub fn matches(&self, presented: &[u8]) -> bool {
        if presented.is_empty() {
            return false;
        }
        let p: [u8; 32] = Sha256::digest(presented).into();
        p.iter()
            .zip(self.0.iter())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0
    }
}

#[derive(Clone)]
pub struct RecoveryState {
    pub controller: Arc<dyn RecoveryController>,
    pub token: AdminToken,
}

/// The admin router. Serve it with
/// `into_make_service_with_connect_info::<SocketAddr>()`.
pub fn router(state: RecoveryState) -> Router {
    Router::new()
        .route("/recovery/pipelines/{name}", get(handle_diagnose))
        .route("/recovery/pipelines/{name}/plan", post(handle_plan))
        .route("/recovery/pipelines/{name}/apply", post(handle_apply))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .layer(middleware::from_fn_with_state(state.clone(), admit))
        .with_state(state)
}

fn unauthorized() -> Response {
    RecoveryApiError::new(401, "unauthorized", "admin credential required")
        .into_response()
}

/// Loopback peer and bearer token, before anything else.
async fn admit(
    State(st): State<RecoveryState>,
    req: Request,
    next: Next,
) -> Response {
    let peer = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0);
    match peer {
        Some(p) if p.ip().is_loopback() => {}
        _ => {
            return RecoveryApiError::new(
                403,
                "loopback_only",
                "the recovery admin API accepts loopback connections only",
            )
            .into_response();
        }
    }
    let presented = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.as_bytes().strip_prefix(b"Bearer "));
    match presented {
        Some(t) if st.token.matches(t) => next.run(req).await,
        _ => unauthorized(),
    }
}

fn caller(peer: SocketAddr) -> Caller {
    Caller {
        origin: peer.to_string(),
        credential: "token",
    }
}

fn timed_out() -> RecoveryApiError {
    RecoveryApiError::new(504, "timeout", "the request did not finish in time")
}

async fn handle_diagnose(
    State(st): State<RecoveryState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, RecoveryApiError> {
    tokio::time::timeout(READ_TIMEOUT, st.controller.diagnose(&name))
        .await
        .map_err(|_| timed_out())?
        .map(Json)
}

async fn handle_plan(
    State(st): State<RecoveryState>,
    Path(name): Path<String>,
    Json(req): Json<PlanRequest>,
) -> Result<Json<Value>, RecoveryApiError> {
    tokio::time::timeout(READ_TIMEOUT, st.controller.plan(&name, req))
        .await
        .map_err(|_| timed_out())?
        .map(Json)
}

async fn handle_apply(
    State(st): State<RecoveryState>,
    Path(name): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Json(req): Json<ApplyRequest>,
) -> Result<Json<Value>, RecoveryApiError> {
    if req.actor.trim().is_empty() || req.reason.trim().is_empty() {
        return Err(RecoveryApiError::new(
            400,
            "bad_request",
            "actor and reason are required",
        ));
    }
    if req.expect_proof.trim().is_empty() {
        return Err(RecoveryApiError::new(
            400,
            "bad_request",
            "expect_proof is required (run plan first)",
        ));
    }
    // The operation runs in its own task: an abandoned request never
    // interrupts it halfway.
    let controller = Arc::clone(&st.controller);
    let task = tokio::spawn(async move {
        controller.apply(&name, req, caller(peer)).await
    });
    match tokio::time::timeout(APPLY_WAIT, task).await {
        Ok(Ok(r)) => r.map(Json),
        Ok(Err(_)) => Err(RecoveryApiError::new(
            500,
            "apply_failed",
            "the operation stopped unexpectedly; run diagnose",
        )),
        Err(_) => Err(RecoveryApiError::new(
            504,
            "still_applying",
            "the operation is still running; run diagnose for its progress",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use tower::ServiceExt;

    const TOKEN: &str = "0123456789abcdef0123456789abcdef";

    struct Echo;

    #[async_trait]
    impl RecoveryController for Echo {
        async fn diagnose(&self, p: &str) -> Result<Value, RecoveryApiError> {
            Ok(json!({ "pipeline": p }))
        }
        async fn plan(
            &self,
            p: &str,
            req: PlanRequest,
        ) -> Result<Value, RecoveryApiError> {
            Ok(json!({ "pipeline": p, "operation": req.operation }))
        }
        async fn apply(
            &self,
            p: &str,
            req: ApplyRequest,
            caller: Caller,
        ) -> Result<Value, RecoveryApiError> {
            Ok(json!({
                "pipeline": p,
                "actor": req.actor,
                "origin": caller.origin,
                "credential": caller.credential,
            }))
        }
    }

    fn app() -> Router {
        router(RecoveryState {
            controller: Arc::new(Echo),
            token: AdminToken::new(TOKEN).unwrap(),
        })
    }

    fn req(
        method: &str,
        uri: &str,
        peer: Option<&str>,
        auth: Option<&str>,
        body: Option<Value>,
    ) -> HttpRequest<Body> {
        let mut b = HttpRequest::builder().method(method).uri(uri);
        if let Some(a) = auth {
            b = b.header(AUTHORIZATION, a);
        }
        // A forwarded header is never trusted.
        b = b.header("x-forwarded-for", "127.0.0.1");
        b = b.header("content-type", "application/json");
        let mut r = b
            .body(match body {
                Some(v) => Body::from(v.to_string()),
                None => Body::empty(),
            })
            .unwrap();
        if let Some(p) = peer {
            r.extensions_mut()
                .insert(ConnectInfo::<SocketAddr>(p.parse().unwrap()));
        }
        r
    }

    async fn status(r: HttpRequest<Body>) -> (u16, Value) {
        let resp = app().oneshot(r).await.unwrap();
        let s = resp.status().as_u16();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (s, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
    }

    fn bearer(t: &str) -> String {
        format!("Bearer {t}")
    }

    #[tokio::test]
    async fn only_a_loopback_peer_with_the_token_is_admitted() {
        let uri = "/recovery/pipelines/p";
        let ok = bearer(TOKEN);
        assert_eq!(
            status(req("GET", uri, Some("127.0.0.1:5"), Some(&ok), None))
                .await
                .0,
            200
        );
        assert_eq!(
            status(req("GET", uri, Some("[::1]:5"), Some(&ok), None))
                .await
                .0,
            200
        );
        // Another peer, whatever the headers say; no peer at all.
        assert_eq!(
            status(req("GET", uri, Some("10.0.0.7:5"), Some(&ok), None))
                .await
                .0,
            403
        );
        assert_eq!(status(req("GET", uri, None, Some(&ok), None)).await.0, 403);
        // Missing, empty, wrong, or not a bearer credential.
        for auth in [
            None,
            Some("Bearer ".to_string()),
            Some(bearer("0123456789abcdef0123456789abcdeX")),
            Some(bearer(&TOKEN[..31])),
            Some(format!("Basic {TOKEN}")),
            Some(TOKEN.to_string()),
        ] {
            let (s, body) = status(req(
                "GET",
                uri,
                Some("127.0.0.1:5"),
                auth.as_deref(),
                None,
            ))
            .await;
            assert_eq!(s, 401, "{auth:?}");
            assert_eq!(body["code"], "unauthorized");
            assert!(!body.to_string().contains(TOKEN));
        }
    }

    #[tokio::test]
    async fn apply_records_the_accepted_peer_and_needs_its_fields() {
        let ok = bearer(TOKEN);
        let uri = "/recovery/pipelines/p/apply";
        let body = json!({
            "operation": "resnapshot",
            "expect_proof": "abc",
            "actor": "alice",
            "reason": "why",
        });
        let (s, v) = status(req(
            "POST",
            uri,
            Some("127.0.0.1:4242"),
            Some(&ok),
            Some(body.clone()),
        ))
        .await;
        assert_eq!(s, 200);
        assert_eq!(v["origin"], "127.0.0.1:4242");
        assert_eq!(v["credential"], "token");
        for missing in ["actor", "reason", "expect_proof"] {
            let mut b = body.clone();
            b[missing] = json!(" ");
            let (s, _) = status(req(
                "POST",
                uri,
                Some("127.0.0.1:1"),
                Some(&ok),
                Some(b),
            ))
            .await;
            assert_eq!(s, 400, "{missing}");
        }
    }

    #[tokio::test]
    async fn bodies_are_bounded() {
        let ok = bearer(TOKEN);
        let big = json!({
            "operation": "resnapshot",
            "args": { "x": "y".repeat(MAX_BODY_BYTES) },
        });
        let (s, _) = status(req(
            "POST",
            "/recovery/pipelines/p/plan",
            Some("127.0.0.1:1"),
            Some(&ok),
            Some(big),
        ))
        .await;
        assert_eq!(s, 413);
    }

    #[test]
    fn tokens_are_validated_and_never_printed() {
        assert!(AdminToken::new("").is_err());
        assert!(AdminToken::new(&"a".repeat(31)).is_err());
        assert!(AdminToken::new(&format!("{}\n", "a".repeat(32))).is_err());
        assert!(AdminToken::new(&format!("{}\0b", "a".repeat(32))).is_err());
        assert!(AdminToken::new(&format!("{}\rb", "a".repeat(32))).is_err());
        let t = AdminToken::new(TOKEN).unwrap();
        assert!(t.matches(TOKEN.as_bytes()));
        assert!(!t.matches(b""));
        assert!(!format!("{t:?}").contains(TOKEN));
    }
}

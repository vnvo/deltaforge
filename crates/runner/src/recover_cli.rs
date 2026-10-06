//! `deltaforge recover`: the client of the recovery admin API
//! (`docs/design/recovery-cli.md`).
//!
//! The token comes from a file only and is never printed. The admin URL is
//! loopback HTTP (remote administration goes through an SSH tunnel).
//! Redirects are refused, responses are bounded and must be the API's JSON.
//! An apply is never retried: after a timeout or a lost connection the
//! operator runs diagnose and re-applies the same proof explicitly.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;
use std::time::Duration;

use reqwest::{StatusCode, Url};
use serde_json::{Value, json};

/// Largest response body read from the server.
pub const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Diagnose and plan: the server's 60 s deadline plus a margin.
pub const READ_TIMEOUT: Duration = Duration::from_secs(70);
/// Apply: the server's 300 s response window plus a margin.
pub const APPLY_TIMEOUT: Duration = Duration::from_secs(310);

/// Exit codes.
pub mod exit {
    pub const OK: i32 = 0;
    /// Invalid input, configuration, authentication or unknown target.
    pub const VALIDATION: i32 = 2;
    /// The state changed since the plan: review the new plan.
    pub const PROOF_MISMATCH: i32 = 3;
    /// The pipeline is not in a state that allows the request.
    pub const PIPELINE_STATE: i32 = 4;
    /// A recovery operation is pending, diverged, stopped or unreadable.
    pub const RECOVERY: i32 = 5;
    /// Transport or server failure; the outcome may be unknown.
    pub const TRANSPORT: i32 = 6;
}

/// A failed command: its exit code and what to tell the operator.
#[derive(Debug)]
pub struct CliError {
    pub code: i32,
    pub message: String,
}

impl CliError {
    fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// How the server is reached.
pub struct Client {
    base: Url,
    token: String,
    http: reqwest::Client,
    pub read_timeout: Duration,
    pub apply_timeout: Duration,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("base", &self.base.as_str())
            .field("token", &"<redacted>")
            .finish()
    }
}

/// The admin URL: `http`, a loopback IP literal, no credentials, query or
/// fragment.
pub fn admin_url(value: &str) -> Result<Url, CliError> {
    let bad = |why: &str| {
        CliError::new(exit::VALIDATION, format!("--admin-url '{value}': {why}"))
    };
    let url = Url::parse(value).map_err(|_| bad("not a URL"))?;
    if url.scheme() != "http" {
        return Err(bad(
            "only loopback http is supported in this release (use an SSH \
             tunnel for remote administration)",
        ));
    }
    let loopback = url
        .host_str()
        .map(|h| h.trim_start_matches('[').trim_end_matches(']'))
        .and_then(|h| h.parse::<std::net::IpAddr>().ok())
        .is_some_and(|ip| ip.is_loopback());
    if !loopback {
        return Err(bad("the host must be a loopback IP address"));
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !matches!(url.path(), "" | "/")
    {
        return Err(bad("expected http://<loopback-ip>:<port> only"));
    }
    Ok(url)
}

impl Client {
    pub fn new(
        admin_url_value: &str,
        token_file: &Path,
    ) -> Result<Self, CliError> {
        let base = admin_url(admin_url_value)?;
        let token = crate::recovery::read_admin_token(token_file)
            .map_err(|e| CliError::new(exit::VALIDATION, format!("{e:#}")))?;
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(CONNECT_TIMEOUT)
            .no_proxy()
            .build()
            .map_err(|_| {
                CliError::new(exit::TRANSPORT, "cannot build the HTTP client")
            })?;
        Ok(Self {
            base,
            token,
            http,
            read_timeout: READ_TIMEOUT,
            apply_timeout: APPLY_TIMEOUT,
        })
    }

    /// `base/recovery/pipelines/<name>[/<last>]`, the name percent-encoded
    /// as one path segment.
    fn url(&self, pipeline: &str, last: Option<&str>) -> Url {
        let mut url = self.base.clone();
        {
            let mut seg = url
                .path_segments_mut()
                .expect("an http URL has path segments");
            seg.clear()
                .push("recovery")
                .push("pipelines")
                .push(pipeline);
            if let Some(l) = last {
                seg.push(l);
            }
        }
        url
    }

    async fn call(
        &self,
        req: reqwest::RequestBuilder,
        timeout: Duration,
        apply: bool,
    ) -> Result<Value, CliError> {
        let lost = |what: &str| {
            if apply {
                CliError::new(
                    exit::TRANSPORT,
                    format!(
                        "{what}: the outcome is unknown and the operation may \
                         still be running on the server; run `deltaforge \
                         recover diagnose`, then re-apply the same proof \
                         explicitly if it is not completed (never retried \
                         automatically)"
                    ),
                )
            } else {
                CliError::new(exit::TRANSPORT, what.to_string())
            }
        };
        let resp = req
            .bearer_auth(&self.token)
            .timeout(timeout)
            .send()
            .await
            .map_err(|e| {
            if e.is_timeout() {
                lost("the request timed out")
            } else if e.is_connect() {
                lost("cannot connect to the admin listener")
            } else {
                lost("the connection failed")
            }
        })?;
        let status = resp.status();
        if status.is_redirection() {
            return Err(CliError::new(
                exit::TRANSPORT,
                "the server answered with a redirect, which is refused",
            ));
        }
        let json_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("application/json"));
        let body = read_bounded(resp).await.map_err(|e| {
            if e.code == exit::TRANSPORT && e.message.is_empty() {
                lost("the response was cut off")
            } else {
                e
            }
        })?;
        if !json_type {
            return Err(unexpected());
        }
        let value: Value =
            serde_json::from_slice(&body).map_err(|_| unexpected())?;
        if !value.is_object() {
            return Err(unexpected());
        }
        if status.is_success() {
            return Ok(value);
        }
        Err(classify(status, &value))
    }

    pub async fn diagnose(&self, pipeline: &str) -> Result<Value, CliError> {
        let v = self
            .call(
                self.http.get(self.url(pipeline, None)),
                self.read_timeout,
                false,
            )
            .await?;
        require(&v, &["pipeline", "status", "recovery", "incidents"])?;
        Ok(v)
    }

    pub async fn plan(
        &self,
        pipeline: &str,
        req: &Value,
    ) -> Result<Value, CliError> {
        let v = self
            .call(
                self.http.post(self.url(pipeline, Some("plan"))).json(req),
                self.read_timeout,
                false,
            )
            .await?;
        require(&v, &["plan", "proof", "apply"])?;
        proof_of(&v)?;
        Ok(v)
    }

    pub async fn apply(
        &self,
        pipeline: &str,
        req: &Value,
    ) -> Result<Value, CliError> {
        let v = self
            .call(
                self.http.post(self.url(pipeline, Some("apply"))).json(req),
                self.apply_timeout,
                true,
            )
            .await?;
        require(&v, &["state", "proof"])?;
        Ok(v)
    }
}

fn unexpected() -> CliError {
    CliError::new(
        exit::TRANSPORT,
        "the server's response is not a recovery API response of this \
         release",
    )
}

fn require(v: &Value, fields: &[&str]) -> Result<(), CliError> {
    if fields.iter().all(|f| v.get(f).is_some()) {
        Ok(())
    } else {
        Err(unexpected())
    }
}

fn proof_of(v: &Value) -> Result<&str, CliError> {
    match v["proof"].as_str() {
        Some(p)
            if p.len() == 64 && p.bytes().all(|b| b.is_ascii_hexdigit()) =>
        {
            Ok(p)
        }
        _ => Err(unexpected()),
    }
}

async fn read_bounded(
    mut resp: reqwest::Response,
) -> Result<Vec<u8>, CliError> {
    if resp
        .content_length()
        .is_some_and(|n| n > MAX_RESPONSE_BYTES as u64)
    {
        return Err(CliError::new(
            exit::TRANSPORT,
            "the server's response is too large",
        ));
    }
    let mut body = Vec::new();
    loop {
        match resp.chunk().await {
            Ok(Some(c)) => {
                if body.len() + c.len() > MAX_RESPONSE_BYTES {
                    return Err(CliError::new(
                        exit::TRANSPORT,
                        "the server's response is too large",
                    ));
                }
                body.extend_from_slice(&c);
            }
            Ok(None) => return Ok(body),
            Err(_) => return Err(CliError::new(exit::TRANSPORT, "")),
        }
    }
}

/// An API error response as an exit code and message.
fn classify(status: StatusCode, v: &Value) -> CliError {
    let (Some(code), Some(message)) =
        (v["code"].as_str(), v["message"].as_str())
    else {
        return unexpected();
    };
    let exit_code = match code {
        "bad_request" | "unknown_operation" | "unauthorized"
        | "loopback_only" | "not_found" | "not_applicable" => exit::VALIDATION,
        "proof_mismatch" => exit::PROOF_MISMATCH,
        "pipeline_not_quiescent"
        | "pipeline_deleting"
        | "recovery_in_progress"
        | "incident_resolved"
        | "precondition_failed" => exit::PIPELINE_STATE,
        "recovery_pending" | "recovery_diverged" | "apply_stopped"
        | "state_unreadable" | "manual_repair" => exit::RECOVERY,
        _ if status.is_server_error() => exit::TRANSPORT,
        _ => return unexpected(),
    };
    let mut text = format!("{code}: {message}");
    match code {
        "proof_mismatch" => {
            if let Some(p) = v["detail"]["proof"].as_str() {
                text.push_str(&format!(
                    "\nthe state now plans to proof {p}; it was not used. \
                     Review it with `deltaforge recover plan` before applying"
                ));
            }
        }
        "recovery_pending" => {
            if let (Some(op), Some(p)) = (
                v["detail"]["operation"].as_str(),
                v["detail"]["proof"].as_str(),
            ) {
                text.push_str(&format!(
                    "\nunfinished operation: {op}, proof {p}; finish it by \
                     re-applying that proof"
                ));
            }
        }
        "recovery_diverged" => {
            text.push_str(&format!(
                "\n{}\nrun `deltaforge recover diagnose` for the next safe action",
                v["detail"]
            ));
        }
        "still_applying" | "apply_stopped" => {
            text.push_str(
                "\nrun `deltaforge recover diagnose`, then re-apply the same \
                 proof explicitly if it is not completed",
            );
        }
        _ => {}
    }
    CliError::new(exit_code, text)
}

/// `--arg key=value` pairs.
pub fn parse_args(
    pairs: &[String],
) -> Result<BTreeMap<String, String>, CliError> {
    pairs
        .iter()
        .map(|p| match p.split_once('=') {
            Some((k, v)) if !k.is_empty() => Ok((k.to_string(), v.to_string())),
            _ => Err(CliError::new(
                exit::VALIDATION,
                format!("--arg '{p}': expected key=value"),
            )),
        })
        .collect()
}

pub fn plan_request(
    operation: &str,
    incident: Option<&str>,
    args: &BTreeMap<String, String>,
) -> Value {
    json!({ "operation": operation, "incident": incident, "args": args })
}

fn line(out: &mut dyn Write, s: impl AsRef<str>) {
    let _ = writeln!(out, "{}", s.as_ref());
}

/// Diagnose, printed for the operator.
pub fn render_diagnose(v: &Value, out: &mut dyn Write) {
    line(
        out,
        format!("pipeline {}", v["pipeline"].as_str().unwrap_or("?")),
    );
    line(
        out,
        format!("status {}", v["status"].as_str().unwrap_or("?")),
    );
    line(out, format!("quiescent {}", v["quiescent"]));
    if v["recovery_in_progress"] == json!(true) {
        line(out, "a recovery request is being applied right now");
    }
    let r = &v["recovery"];
    if r.is_null() {
        line(out, "recovery none");
    } else if let Some(e) = r["error"].as_str() {
        line(out, format!("recovery record: {e}"));
    } else {
        line(
            out,
            format!(
                "recovery {} {} proof {}",
                r["operation"].as_str().unwrap_or("?"),
                r["state"].as_str().unwrap_or("?"),
                r["proof"].as_str().unwrap_or("?"),
            ),
        );
        line(out, format!("  step {} of {}", r["step"], r["steps"]));
        if !r["current_step"].is_null() {
            line(out, format!("  current step {}", r["current_step"]));
        }
        if !r["last_verified"].is_null() {
            line(out, format!("  last verified {}", r["last_verified"]));
        }
        if !r["divergence"].is_null() {
            line(out, format!("  diverged {}", r["divergence"]));
        }
        line(
            out,
            format!(
                "  next safe action: {}",
                r["remediation"].as_str().unwrap_or("?")
            ),
        );
    }
    let incidents = v["incidents"].as_array().cloned().unwrap_or_default();
    if incidents.is_empty() {
        line(out, "incidents none open");
    }
    for i in incidents {
        line(
            out,
            format!(
                "incident {} {}",
                i["incident_id"].as_str().unwrap_or("?"),
                i["reason_code"].as_str().unwrap_or("?"),
            ),
        );
        if let Some(x) = i["explanation"].as_str() {
            line(out, format!("  {x}"));
        }
        let ops: Vec<&str> = i["operations"]
            .as_array()
            .map(|a| a.iter().filter_map(|o| o.as_str()).collect())
            .unwrap_or_default();
        line(
            out,
            format!(
                "  operations {}",
                if ops.is_empty() {
                    "none in this release".to_string()
                } else {
                    ops.join(", ")
                }
            ),
        );
    }
}

/// A plan, printed in a fixed order, ending with `proof <sha256>`.
pub fn render_plan(v: &Value, apply_cmd: &str, out: &mut dyn Write) {
    let p = &v["plan"];
    line(
        out,
        format!(
            "plan {} (format {}, {})",
            p["operation"].as_str().unwrap_or("?"),
            p["format"],
            p["domain"].as_str().unwrap_or("?"),
        ),
    );
    line(
        out,
        format!("pipeline {}", p["pipeline"].as_str().unwrap_or("?")),
    );
    line(
        out,
        format!("source {}", p["source"].as_str().unwrap_or("?")),
    );
    line(out, "bindings");
    if let Some(b) = p["bindings"].as_object() {
        // serde_json's map is ordered by key.
        for (k, val) in b {
            line(out, format!("  {k} = {}", val.as_str().unwrap_or("?")));
        }
    }
    line(out, "steps");
    for (i, s) in p["steps"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .enumerate()
    {
        line(
            out,
            format!("  {} {}", i + 1, s["name"].as_str().unwrap_or("?")),
        );
        line(
            out,
            format!("    pre  {}", s["pre"].as_str().unwrap_or("?")),
        );
        line(
            out,
            format!("    post {}", s["post"].as_str().unwrap_or("?")),
        );
        if let Some(d) = s["detail"].as_object() {
            for (k, val) in d {
                line(out, format!("    {k} = {}", val.as_str().unwrap_or("?")));
            }
        }
    }
    line(out, "consequences");
    for c in p["consequences"].as_array().cloned().unwrap_or_default() {
        line(out, format!("  - {}", c.as_str().unwrap_or("?")));
    }
    if let Some(r) = v["resolves"].as_array().filter(|r| !r.is_empty()) {
        let ids: Vec<&str> = r.iter().filter_map(|i| i.as_str()).collect();
        line(out, format!("resolves {}", ids.join(", ")));
    }
    if let Some(o) = v["observed"].as_object().filter(|o| !o.is_empty()) {
        line(out, "observed (not part of the proof)");
        for (k, val) in o {
            line(out, format!("  {k} = {}", val.as_str().unwrap_or("?")));
        }
    }
    if v["apply"]["possible"] == json!(true) {
        line(out, "apply possible now");
    } else {
        line(
            out,
            format!(
                "apply not possible now: {}",
                v["apply"]["blocked_by"].as_str().unwrap_or("?")
            ),
        );
    }
    line(out, format!("apply with: {apply_cmd}"));
    line(out, format!("proof {}", v["proof"].as_str().unwrap_or("?")));
}

/// The command that applies a plan (the actor and reason are the
/// operator's to fill in).
pub fn apply_command(
    operation: &str,
    pipeline: &str,
    incident: Option<&str>,
    args: &BTreeMap<String, String>,
    proof: &str,
) -> String {
    let mut cmd =
        format!("deltaforge recover apply {operation} --pipeline {pipeline}");
    if let Some(i) = incident {
        cmd.push_str(&format!(" --incident {i}"));
    }
    for (k, v) in args {
        cmd.push_str(&format!(" --arg {k}={v}"));
    }
    cmd.push_str(&format!(
        " --expect-proof {proof} --actor <name> --reason <text>"
    ));
    cmd
}

/// One `deltaforge recover` invocation.
pub enum Action {
    Diagnose {
        pipeline: String,
    },
    Plan {
        operation: String,
        pipeline: String,
        incident: Option<String>,
        args: Vec<String>,
    },
    Apply {
        operation: String,
        pipeline: String,
        incident: Option<String>,
        args: Vec<String>,
        expect_proof: String,
        actor: String,
        reason: String,
    },
}

/// Run `action`, writing to `out` (results) and `err` (failures); returns
/// the exit code.
pub async fn run(
    client: &Client,
    action: Action,
    json_out: bool,
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> i32 {
    match run_inner(client, action, json_out, out).await {
        Ok(()) => exit::OK,
        Err(e) => {
            let _ = writeln!(err, "{}", e.message);
            e.code
        }
    }
}

async fn run_inner(
    client: &Client,
    action: Action,
    json_out: bool,
    out: &mut dyn Write,
) -> Result<(), CliError> {
    let print_json = |out: &mut dyn Write, v: &Value| {
        line(out, serde_json::to_string_pretty(v).unwrap_or_default());
    };
    match action {
        Action::Diagnose { pipeline } => {
            let v = client.diagnose(&pipeline).await?;
            if json_out {
                print_json(out, &v);
            } else {
                render_diagnose(&v, out);
            }
        }
        Action::Plan {
            operation,
            pipeline,
            incident,
            args,
        } => {
            let args = parse_args(&args)?;
            let req = plan_request(&operation, incident.as_deref(), &args);
            let v = client.plan(&pipeline, &req).await?;
            if json_out {
                print_json(out, &v);
            } else {
                let cmd = apply_command(
                    &operation,
                    &pipeline,
                    incident.as_deref(),
                    &args,
                    proof_of(&v)?,
                );
                render_plan(&v, &cmd, out);
            }
        }
        Action::Apply {
            operation,
            pipeline,
            incident,
            args,
            expect_proof,
            actor,
            reason,
        } => {
            for (flag, v) in [
                ("--expect-proof", &expect_proof),
                ("--actor", &actor),
                ("--reason", &reason),
            ] {
                if v.trim().is_empty() {
                    return Err(CliError::new(
                        exit::VALIDATION,
                        format!("{flag} must not be empty"),
                    ));
                }
            }
            let args = parse_args(&args)?;
            let mut req = plan_request(&operation, incident.as_deref(), &args);
            req["expect_proof"] = json!(expect_proof);
            req["actor"] = json!(actor);
            req["reason"] = json!(reason);
            let v = client.apply(&pipeline, &req).await?;
            if json_out {
                print_json(out, &v);
            } else {
                line(
                    out,
                    format!(
                        "{} {} proof {}",
                        operation,
                        v["state"].as_str().unwrap_or("?"),
                        v["proof"].as_str().unwrap_or("?"),
                    ),
                );
                if v["already_completed"] == json!(true) {
                    line(out, "it had already completed; nothing was written");
                }
                if let Some(o) = v["outcomes"].as_object() {
                    for (k, val) in o {
                        line(
                            out,
                            format!("  {k} = {}", val.as_str().unwrap_or("?")),
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline_manager::{
        PipelineManager, PipelineRuntime, PipelineStatus,
    };
    use crate::recovery::RecoveryService;
    use crate::recovery::test_support::{NS, TestOp};
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use storage::adapters::incidents::IncidentStore;
    use storage::{ArcStorageBackend, MemoryStorageBackend};

    const TOKEN: &str = "s3cr3t-T0KEN-0123456789abcdef-ZZZZ";
    const OTHER: &str = "other-token-0123456789abcdef-XXXXXX";
    /// A pipeline name that needs percent-encoding.
    const PIPE: &str = "orders a/b%c";

    struct Server {
        url: String,
        manager: Arc<PipelineManager>,
        backend: ArcStorageBackend,
        gate: Arc<tokio::sync::Semaphore>,
        entered: Arc<tokio::sync::Notify>,
        fail_once: Arc<std::sync::atomic::AtomicBool>,
        dir: tempfile::TempDir,
    }

    fn token_file(
        dir: &tempfile::TempDir,
        name: &str,
        token: &str,
    ) -> std::path::PathBuf {
        let p = dir.path().join(name);
        std::fs::write(&p, format!("{token}\n")).unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600))
            .unwrap();
        p
    }

    async fn serve(app: axum::Router) -> String {
        let listener =
            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap();
        });
        format!("http://{addr}")
    }

    async fn server(permits: usize) -> Server {
        let backend: ArcStorageBackend = Arc::new(MemoryStorageBackend::new());
        let manager = Arc::new(
            PipelineManager::with_backend(backend.clone())
                .await
                .unwrap(),
        );
        let mut spec = crate::pipeline_manager::tests_support_spec(PIPE);
        spec.metadata.name = PIPE.into();
        manager.pipelines.write().insert(
            PIPE.into(),
            PipelineRuntime::held(
                spec,
                IncidentStore::new(backend.clone(), PIPE),
            ),
        );
        let gate = Arc::new(tokio::sync::Semaphore::new(permits));
        let entered = Arc::new(tokio::sync::Notify::new());
        let op = TestOp::new(Arc::clone(&gate), Arc::clone(&entered));
        let fail_once = Arc::clone(&op.fail_once);
        let service = RecoveryService::new(Arc::clone(&manager))
            .with_operation(Arc::new(op));
        let url = serve(rest_api::recovery::router(
            rest_api::recovery::RecoveryState {
                controller: Arc::new(service),
                token: rest_api::recovery::AdminToken::new(TOKEN).unwrap(),
            },
        ))
        .await;
        Server {
            url,
            manager,
            backend,
            gate,
            entered,
            fail_once,
            dir: tempfile::tempdir().unwrap(),
        }
    }

    impl Server {
        fn client(&self) -> Client {
            Client::new(&self.url, &token_file(&self.dir, "t", TOKEN)).unwrap()
        }
    }

    /// Run one action: (exit code, stdout, stderr).
    async fn cli(c: &Client, a: Action, json: bool) -> (i32, String, String) {
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let code = run(c, a, json, &mut out, &mut err).await;
        let (out, err) = (
            String::from_utf8(out).unwrap(),
            String::from_utf8(err).unwrap(),
        );
        assert!(!out.contains(TOKEN) && !err.contains(TOKEN), "token leaked");
        (code, out, err)
    }

    fn plan(pipe: &str) -> Action {
        Action::Plan {
            operation: "test-op".into(),
            pipeline: pipe.into(),
            incident: None,
            args: vec![],
        }
    }

    fn apply(proof: &str) -> Action {
        Action::Apply {
            operation: "test-op".into(),
            pipeline: PIPE.into(),
            incident: None,
            args: vec![],
            expect_proof: proof.into(),
            actor: "alice".into(),
            reason: "test".into(),
        }
    }

    fn diagnose() -> Action {
        Action::Diagnose {
            pipeline: PIPE.into(),
        }
    }

    fn proof_line(out: &str) -> String {
        let last = out.lines().last().unwrap();
        let p = last.strip_prefix("proof ").expect("ends with the proof");
        assert_eq!(p.len(), 64);
        p.to_string()
    }

    #[tokio::test]
    async fn plan_then_apply_round_trip() {
        let s = server(1).await;
        let c = s.client();
        let (code, out, _) = cli(&c, plan(PIPE), false).await;
        assert_eq!(code, exit::OK);
        let proof = proof_line(&out);
        assert!(out.contains("apply possible now"));
        assert!(out.contains("observed (not part of the proof)"));
        assert!(out.contains(&format!("--expect-proof {proof}")));
        // Stable: the same state prints the same plan.
        assert_eq!(cli(&c, plan(PIPE), false).await.1, out);
        let (code, out, _) = cli(&c, apply(&proof), false).await;
        assert_eq!(code, exit::OK, "{out}");
        assert!(out.contains("completed"));
        // Applying the completed proof again writes nothing.
        let (code, out, _) = cli(&c, apply(&proof), false).await;
        assert_eq!(code, exit::OK);
        assert!(out.contains("already completed"));
        let (code, out, _) = cli(&c, diagnose(), false).await;
        assert_eq!(code, exit::OK);
        assert!(out.contains("next safe action: none"), "{out}");
    }

    #[tokio::test]
    async fn authentication_and_configuration_fail_with_validation() {
        let s = server(1).await;
        let wrong =
            Client::new(&s.url, &token_file(&s.dir, "w", OTHER)).unwrap();
        let (code, _, err) = cli(&wrong, diagnose(), false).await;
        assert_eq!(code, exit::VALIDATION);
        assert!(err.contains("unauthorized"), "{err}");
        for url in [
            "https://127.0.0.1:9091",
            "http://10.0.0.1:9091",
            "http://localhost:9091",
            "http://user:pw@127.0.0.1:9091",
            "http://127.0.0.1:9091/x",
            "http://127.0.0.1:9091/?a=b",
        ] {
            let e =
                Client::new(url, &token_file(&s.dir, "t", TOKEN)).unwrap_err();
            assert_eq!(e.code, exit::VALIDATION, "{url}");
        }
        let loose = s.dir.path().join("loose");
        std::fs::write(&loose, TOKEN).unwrap();
        std::fs::set_permissions(
            &loose,
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        let e = Client::new(&s.url, &loose).unwrap_err();
        assert_eq!(e.code, exit::VALIDATION);
        assert!(!e.message.contains(TOKEN));
        assert!(!format!("{:?}", s.client()).contains(TOKEN));
        // An unknown pipeline and empty apply fields.
        let (code, _, _) = cli(&s.client(), plan("nope"), false).await;
        assert_eq!(code, exit::VALIDATION);
        let (code, _, _) = cli(&s.client(), apply(" "), false).await;
        assert_eq!(code, exit::VALIDATION);
    }

    #[tokio::test]
    async fn a_proof_mismatch_shows_the_new_proof_without_using_it() {
        let s = server(1).await;
        let c = s.client();
        let proof = proof_line(&cli(&c, plan(PIPE), false).await.1);
        s.backend.slot_upsert(NS, PIPE, b"moved").await.unwrap();
        let (code, _, err) = cli(&c, apply(&proof), false).await;
        assert_eq!(code, exit::PROOF_MISMATCH);
        let fresh = proof_line(&cli(&c, plan(PIPE), false).await.1);
        assert_ne!(fresh, proof);
        assert!(
            err.contains(&fresh) && err.contains("was not used"),
            "{err}"
        );
        let rec = storage::adapters::recovery::RecoveryStore::new(
            s.backend.clone(),
            PIPE,
        );
        assert!(rec.read().await.unwrap().is_none(), "nothing applied");
    }

    #[tokio::test]
    async fn a_timed_out_apply_is_never_retried_and_its_proof_finishes_it() {
        let s = server(0).await;
        let mut c = s.client();
        let proof = proof_line(&cli(&c, plan(PIPE), false).await.1);
        c.apply_timeout = Duration::from_millis(300);
        let (code, _, err) = cli(&c, apply(&proof), false).await;
        assert_eq!(code, exit::TRANSPORT);
        assert!(
            err.contains("diagnose") && err.contains("re-apply the same proof"),
            "{err}"
        );
        // The server's operation went on: diagnose shows it in flight.
        s.entered.notified().await;
        let (code, out, _) = cli(&c, diagnose(), false).await;
        assert_eq!(code, exit::OK);
        assert!(out.contains("recovery test-op applying"), "{out}");
        assert!(out.contains("being applied right now"), "{out}");
        // A second apply meanwhile is refused for the pipeline state.
        let (code, _, _) = cli(&c, apply(&proof), false).await;
        assert_eq!(code, exit::PIPELINE_STATE);
        s.gate.add_permits(1);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while s.manager.recovery_status(PIPE).await.is_some() {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        c.apply_timeout = APPLY_TIMEOUT;
        let (code, out, _) = cli(&c, apply(&proof), false).await;
        assert_eq!(code, exit::OK);
        assert!(out.contains("already completed"), "{out}");
    }

    #[tokio::test]
    async fn a_stopped_operation_resumes_with_the_same_proof() {
        let s = server(2).await;
        let c = s.client();
        let proof = proof_line(&cli(&c, plan(PIPE), false).await.1);
        s.fail_once.store(true, Ordering::SeqCst);
        let (code, _, err) = cli(&c, apply(&proof), false).await;
        assert_eq!(code, exit::RECOVERY, "{err}");
        let (_, out, _) = cli(&c, diagnose(), false).await;
        assert!(out.contains("next safe action: re-run"), "{out}");
        assert!(out.contains(&proof));
        // Another proof refuses; the same proof finishes it.
        let (code, _, err) = cli(&c, apply(&"0".repeat(64)), false).await;
        assert_eq!(code, exit::RECOVERY, "{err}");
        assert!(err.contains(&proof), "{err}");
        let (code, out, _) = cli(&c, apply(&proof), false).await;
        assert_eq!(code, exit::OK, "{out}");
    }

    #[tokio::test]
    async fn a_running_pipeline_is_a_pipeline_state_error() {
        let s = server(1).await;
        let c = s.client();
        let proof = proof_line(&cli(&c, plan(PIPE), false).await.1);
        s.manager.pipelines.write().get_mut(PIPE).unwrap().status =
            PipelineStatus::Running;
        let (code, out, _) = cli(&c, plan(PIPE), false).await;
        assert_eq!(code, exit::OK);
        assert!(out.contains("apply not possible now: pipeline_not_quiescent"));
        let (code, _, _) = cli(&c, apply(&proof), false).await;
        assert_eq!(code, exit::PIPELINE_STATE);
    }

    #[tokio::test]
    async fn malformed_responses_and_redirects_are_refused() {
        use axum::routing::get;
        let big = "x".repeat(MAX_RESPONSE_BYTES + 1);
        let app = axum::Router::new()
            .route(
                "/recovery/pipelines/redirect",
                get(|| async {
                    (
                        axum::http::StatusCode::FOUND,
                        [(axum::http::header::LOCATION, "http://10.0.0.1/")],
                    )
                }),
            )
            .route("/recovery/pipelines/text", get(|| async { "hello" }))
            .route(
                "/recovery/pipelines/jsontext",
                get(|| async {
                    (
                        [(axum::http::header::CONTENT_TYPE, "text/plain")],
                        r#"{"pipeline":"p","status":"s","recovery":null,"incidents":[]}"#,
                    )
                }),
            )
            .route(
                "/recovery/pipelines/stream",
                get(|| async {
                    // Chunked, no Content-Length: only the read cap stops it.
                    let chunk = bytes::Bytes::from(vec![b' '; 64 * 1024]);
                    let n = MAX_RESPONSE_BYTES / chunk.len() + 2;
                    let stream = futures::stream::iter(
                        (0..n).map(move |_| Ok::<_, std::io::Error>(chunk.clone())),
                    );
                    (
                        [(axum::http::header::CONTENT_TYPE, "application/json")],
                        axum::body::Body::from_stream(stream),
                    )
                }),
            )
            .route(
                "/recovery/pipelines/shape",
                get(|| async { axum::Json(serde_json::json!({ "x": 1 })) }),
            )
            .route(
                "/recovery/pipelines/error",
                get(|| async {
                    (
                        axum::http::StatusCode::CONFLICT,
                        axum::Json(serde_json::json!({ "code": "new_thing", "message": "?" })),
                    )
                }),
            )
            .route(
                "/recovery/pipelines/big",
                get(move || {
                    let big = big.clone();
                    async move { axum::Json(serde_json::json!({ "pipeline": big })) }
                }),
            );
        let url = serve(app).await;
        let dir = tempfile::tempdir().unwrap();
        let c = Client::new(&url, &token_file(&dir, "t", TOKEN)).unwrap();
        for (pipe, want) in [
            ("redirect", "redirect"),
            ("text", "not a recovery API response"),
            ("jsontext", "not a recovery API response"),
            ("stream", "too large"),
            ("shape", "not a recovery API response"),
            ("error", "not a recovery API response"),
            ("big", "too large"),
        ] {
            let (code, _, err) = cli(
                &c,
                Action::Diagnose {
                    pipeline: pipe.into(),
                },
                false,
            )
            .await;
            assert_eq!(code, exit::TRANSPORT, "{pipe}");
            assert!(err.contains(want), "{pipe}: {err}");
        }
        // Nothing listening.
        let gone =
            Client::new("http://127.0.0.1:1", &token_file(&dir, "t", TOKEN))
                .unwrap();
        let (code, _, _) = cli(&gone, diagnose(), false).await;
        assert_eq!(code, exit::TRANSPORT);
    }
}

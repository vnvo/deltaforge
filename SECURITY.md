# Security Policy

DeltaForge is pre-1.0 software under active development. We take security issues
seriously and appreciate responsible disclosure.

## Supported versions

While in the `0.1.0-beta` line, only the **most recent beta release** (and
`main`) receives security fixes. Older betas are not patched — please upgrade.

| Version | Supported |
| --- | --- |
| latest `0.1.0-beta.*` / `main` | ✅ |
| older betas | ❌ |

## Reporting a vulnerability

**Please do not open a public GitHub issue for security vulnerabilities.**

Report privately via GitHub's private vulnerability reporting:

1. Go to the repository's **Security** tab → **Report a vulnerability**
   (`https://github.com/vnvo/deltaforge/security/advisories/new`).
2. Include a description, affected version/commit, reproduction steps, and impact.

We aim to acknowledge a report within **5 business days** and to provide a
remediation plan or timeline after triage. Please give us a reasonable window to
release a fix before any public disclosure.

## Scope

In scope: the DeltaForge engine, its sources/sinks, the REST control plane, and
the published container image. Out of scope: issues in third-party dependencies
(report those upstream; we track advisories via `cargo audit`), and misconfigured
deployments (e.g. exposing the control-plane API without network controls).

## Hardening notes

- The published production image currently runs as **root**; run it with a
  read-only root filesystem, a dropped capability set, and/or your orchestrator's
  `runAsUser` until the non-root image variant ships.
- The control-plane API (`:8080`) has no built-in authentication — do not expose
  it to untrusted networks.

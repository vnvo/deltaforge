# Contributing to DeltaForge

Thanks for your interest in contributing. DeltaForge is a Rust change-data-capture
engine; this guide covers the local workflow and the checks a change must pass.

## Prerequisites

- **Rust** — the project builds and is tested on the current `stable` toolchain
  (the release images use Rust 1.91). `rust-toolchain.toml` pins `stable`.
- **Docker** — required for the integration and chaos test suites (databases,
  Kafka, MinIO, Elasticsearch, etc.).

## Build & checks

Every change must pass the same gates CI enforces:

```bash
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace          # unit + fast integration tests
```

### Integration tests (Docker required)

Tests that need real infrastructure are marked `#[ignore]` and pull containers
via testcontainers. Run them explicitly, single-threaded:

```bash
# a specific sink/source suite
cargo test -p sinks --test elasticsearch_sink_tests -- --ignored --test-threads=1
cargo test -p sources --test mysql_cdc_e2e         -- --ignored --test-threads=1
```

## Local development

```bash
./dev.sh up        # Postgres, MySQL, Kafka, Redis, NATS via docker-compose.dev.yml
./dev.sh help      # all dev helpers
```

## Chaos / benchmarks

```bash
./dev.sh chaos-build   # builds deltaforge:dev-debug (from Dockerfile.debug)
docker compose -f docker-compose.chaos.yml \
  --profile base --profile mysql-infra --profile kafka-infra --profile df up -d
cargo run -p chaos -- --list-scenarios
```

## Documentation

- User docs live in `docs/src/` (mdBook). Build with `mdbook build docs`.
- Keep any user-facing behavior change reflected in the relevant
  `docs/src/sinks/*.md` (or other) page **in the same PR**.
- Add a bullet under `## [Unreleased]` in `CHANGELOG.md`
  ([Keep a Changelog](https://keepachangelog.com/) format).

## Pull requests

1. Branch off `main`.
2. Keep the change focused; update docs + CHANGELOG alongside code.
3. Ensure `fmt`, `clippy -D warnings`, and `cargo test --workspace` are green;
   run the relevant `--ignored` integration tests when you touch a source/sink.
4. Use clear commit messages — conventional-commit prefixes
   (`feat:`, `fix:`, `docs:`, `test:`, `chore:`, `ci:`) are preferred.
5. Describe what changed and how you verified it in the PR description.

## Reporting bugs & security issues

- Functional bugs: open a GitHub issue using the provided templates.
- Security vulnerabilities: **do not** open a public issue — see
  [SECURITY.md](SECURITY.md).

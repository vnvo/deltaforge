<p align="center">
  <img src="assets/deltaforge-dev.png" width="250" alt="DeltaForge">
</p>


# Development Guide

Use this guide to build, test, and extend DeltaForge. It covers local workflows, optional dependency containers, and how to work with the published Docker image.

All contributions are welcome and highly appreciated.

## Local prerequisites
- Rust toolchain 1.89+ (install via [`rustup`](https://rustup.rs)).
- Optional: Docker or Podman for running the dev dependency stack and the container image.

## Workspace layout
- `crates/deltaforge-core` — shared event model, pipeline engine, and checkpointing primitives.
- `crates/sources` — database CDC readers (MySQL binlog, Postgres logical replication) implemented as pluggable sources.
- `crates/processors` — JavaScript-based processors and support code for transforming batches.
- `crates/sinks` — sink implementations (Kafka producer, Redis streams) plus sink utilities.
- `crates/rest-api` — HTTP control plane with health/readiness and pipeline lifecycle endpoints.
- `crates/runner` — CLI entrypoint that wires the runtime, metrics, and control plane together.

Use these crate boundaries as reference points when adding new sources, sinks, or pipeline behaviors.

## Start dev dependencies
Bring up the optional backing services (MySQL, Kafka, Redis) with Docker Compose:

```bash
docker compose -f docker-compose.dev.yml up -d
```

Each service is exposed on localhost for local runs (`5432`, `3306`, `9092`, `6379`). The MySQL container seeds demo data from `./init-scripts` and configures binlog settings required for CDC.

Prefer the convenience `dev.sh` wrapper to keep common tasks consistent:

```bash
./dev.sh up     # start the dependency stack
./dev.sh down   # stop and remove it
./dev.sh ps     # see container status
```

There are a few extra commands in dev.sh to help you interact with the dev setup, check `./dev.sh help`. 

## Build and test locally
Run the usual Rust workflow from the repo root:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets --all-features
cargo test --workspace
```

Or use the helper script for a single command that mirrors CI expectations:

```bash
./dev.sh fmt   # format
./dev.sh lint  # clippy with warnings as errors
./dev.sh test  # full test suite
./dev.sh check # fmt --check + clippy + tests
```

Use `cargo run -p runner -- --config ./pipelines` to launch the runner against your pipeline specs while iterating.

## Build and run the container image
Use the provided Dockerfile to build and run the DeltaForge runner in a container.

### Build the image

Build a production-ready image tagged `deltaforge:local`:

```bash
docker build -t deltaforge:local .
```

### Run the image

Run the container by mounting your pipeline specs (environment variables inside the YAML are expanded before parsing) and exposing the API and metrics ports:

```bash
docker run --rm \
  -p 8080:8080 -p 9000:9000 \
  -v $(pwd)/examples/dev.yaml:/etc/deltaforge/pipeline.yaml:ro \
  -v deltaforge-checkpoints:/app/data \
  deltaforge:local \
  --config /etc/deltaforge/pipelines.yaml
```

Notes:

- The container runs as a non-root user and listens on `0.0.0.0:8080` for the control plane API with metrics on `:9000`.
- Checkpoints are written to `/app/data/df_checkpoints.json`; mount a volume to persist them across restarts.
- Pass any other runner flags as needed (e.g., `--api-addr` or `--metrics-addr`) to align with your environment.
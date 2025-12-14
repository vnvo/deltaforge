FROM rust:1.91-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        pkg-config \
        libssl-dev \
        curl \
        build-essential \
        zlib1g-dev \
        libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY examples ./examples

RUN cargo build --locked --release --bin runner

FROM gcr.io/distroless/base-debian12 AS runtime

COPY --from=builder /app/target/release/runner /deltaforge

USER nonroot:nonroot
WORKDIR /app

EXPOSE 8080 9000
ENTRYPOINT ["/deltaforge"]
CMD ["--help"]
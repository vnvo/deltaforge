# Multi-arch minimal image - the default for production
# Supports: linux/amd64, linux/arm64

FROM rust:1.91-bookworm AS builder

ARG TARGETARCH

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

# Collect runtime dependencies
RUN mkdir -p /runtime/lib /runtime/lib64 && \
    ldd /app/target/release/runner | grep "=>" | awk '{print $3}' | xargs -I {} cp {} /runtime/lib/ && \
    if [ "$TARGETARCH" = "amd64" ]; then \
        cp /lib64/ld-linux-x86-64.so.2 /runtime/lib64/; \
    elif [ "$TARGETARCH" = "arm64" ]; then \
        cp /lib/ld-linux-aarch64.so.1 /runtime/lib/; \
    fi

FROM scratch

ARG TARGETARCH

# Copy runtime libs
COPY --from=builder /runtime/lib/* /lib/
COPY --from=builder /runtime/lib64/* /lib64/

# Copy CA certificates for TLS connections
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy binary
COPY --from=builder /app/target/release/runner /deltaforge

EXPOSE 8080 9000
ENTRYPOINT ["/deltaforge"]
CMD ["--help"]
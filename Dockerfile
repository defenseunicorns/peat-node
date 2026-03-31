# Multi-stage build for peat-sidecar
#
# Build: docker build -t peat-sidecar:latest .

FROM rust:1.93-bookworm AS builder

# Install build tools: clang + mold linker + protoc
RUN apt-get update && apt-get install -y --no-install-recommends \
    clang mold protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Configure mold linker for faster Docker builds
RUN mkdir -p .cargo && printf '[target.x86_64-unknown-linux-gnu]\nlinker = "clang"\nrustflags = ["-C", "link-arg=-fuse-ld=mold"]\n' > .cargo/config.toml

# Copy everything needed for the build
COPY Cargo.toml Cargo.lock ./
COPY build.rs build.rs
COPY proto proto
COPY src src

RUN cargo build --release

# -- Runtime ------------------------------------------------------------------
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tini curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/peat-sidecar /usr/local/bin/peat-sidecar

# Data directory for Automerge CRDT state and Iroh blobs
VOLUME /data/peat-sidecar

# gRPC API (TCP mode)
EXPOSE 50051/tcp

ENTRYPOINT ["tini", "--"]
CMD ["peat-sidecar", "--data-dir=/data/peat-sidecar"]

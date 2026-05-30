# Multi-stage build for peat-node
#
# Build: docker build -t peat-node:latest .

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
COPY crates crates

# --workspace builds both the peat-node root package and the peat-cli
# crate. peat-cli's binary is named `peat` and gets included in the
# runtime image below so `kubectl exec` reaches a built-in debug surface
# per peat-node ADR-001.
RUN cargo build --release --workspace

# Confirm the peat binary actually launches at image build time —
# `peat --help` exiting 0 is the in-container smoke test ADR-001
# §"CI gates" calls for. Catches any link-time / library-path issue
# before the runtime stage.
RUN /build/target/release/peat --help > /dev/null

# -- Runtime ------------------------------------------------------------------
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tini curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/peat-node /usr/local/bin/peat-node
COPY --from=builder /build/target/release/peat /usr/local/bin/peat

# Data directory for Automerge CRDT state and Iroh blobs
VOLUME /data/peat-node

# gRPC API (TCP mode)
EXPOSE 50051/tcp

ENTRYPOINT ["tini", "--"]
CMD ["peat-node", "--data-dir=/data/peat-node"]

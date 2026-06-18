# Multi-stage build for peat-node, with cargo-chef dependency caching.
#
# Build: docker build -t peat-node:latest .
#
# cargo-chef splits dependency compilation — the bulk of build time — into its
# own layer keyed on Cargo.lock (`recipe.json`). With buildx's per-architecture
# GHA cache (see .github/workflows/release.yml), that dependency layer is reused
# across builds/releases, so only the workspace crates recompile when our source
# changes. Before this, the single `cargo build --release --workspace` layer
# cache-missed on every source change and rebuilt all dependencies from scratch.

FROM rust:1.93-bookworm AS chef
# Build tools: clang + mold linker + protoc. cargo-chef plans/cooks the deps.
RUN apt-get update && apt-get install -y --no-install-recommends \
    clang mold protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef --locked
WORKDIR /build
# mold linker for faster links.
RUN mkdir -p .cargo && printf '[target.x86_64-unknown-linux-gnu]\nlinker = "clang"\nrustflags = ["-C", "link-arg=-fuse-ld=mold"]\n' > .cargo/config.toml

# -- Planner: capture the dependency graph into recipe.json --------------------
FROM chef AS planner
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto proto
COPY src src
COPY crates crates
RUN cargo chef prepare --recipe-path recipe.json

# -- Builder: cook deps (cached), then build the workspace ---------------------
FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json
# Our build.rs (captured in the recipe) compiles proto/sidecar.proto during the
# cook step's stubbed crate build, so proto/ must be present here.
COPY proto proto
COPY build.rs build.rs
# Cook compiles ONLY dependencies. This layer is keyed on recipe.json
# (i.e. Cargo.lock) + proto/, so it stays cached across source-only changes.
RUN cargo chef cook --release --workspace --recipe-path recipe.json
# Now the real source. Re-copy build.rs + proto: cargo-chef stubs the workspace
# crates (build.rs included) during cook, so the real build needs our real
# proto-codegen build script back. `cargo clean -p peat-node` then drops cook's
# stale peat-node fingerprint so build.rs actually re-runs and regenerates
# OUT_DIR/_connectrpc.rs. Cooked *dependency* artifacts are left intact.
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto proto
COPY src src
COPY crates crates
RUN cargo clean -p peat-node --release
# --workspace builds the peat-node root package and the peat-cli crate (binary
# `peat`), both copied into the runtime image below (ADR-001 debug surface).
RUN cargo build --release --workspace
# In-container smoke test (ADR-001 §"CI gates"): `peat --help` exiting 0 catches
# link-time / library-path issues before the runtime stage.
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

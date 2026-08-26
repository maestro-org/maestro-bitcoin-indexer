# Build with the repository root as context:
#   docker build -f services/compressor-xbt/Dockerfile .
# compressor-xbt builds standalone (own Cargo.lock), outside the cargo workspace —
# its `ord` fork pins a `bitcoin` crate version incompatible with the other services.

FROM rust:1.90-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    clang cmake protobuf-compiler && rm -rf /var/lib/apt/lists/*

# Cap parallel rustc jobs so release builds fit in the default Docker Desktop
# memory allocation; override with --build-arg CARGO_BUILD_JOBS=<n> on big VMs.
ARG CARGO_BUILD_JOBS=4
ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS}

WORKDIR /build
COPY proto ./proto
COPY services/compressor-xbt ./services/compressor-xbt

WORKDIR /build/services/compressor-xbt
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo build --release && cp target/release/compressor-xbt /compressor-xbt

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl tini && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 1000 --create-home nonroot \
    && mkdir -p /data && chown nonroot:nonroot /data

COPY --from=builder /compressor-xbt /usr/local/bin/compressor-xbt

USER nonroot
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "compressor-xbt"]

# Build with the repository root as context:
#   docker build -f services/mapi-xbt/Dockerfile .

FROM rust:1.90-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    clang cmake protobuf-compiler && rm -rf /var/lib/apt/lists/*

# Cap parallel rustc jobs so release builds fit in the default Docker Desktop
# memory allocation; override with --build-arg CARGO_BUILD_JOBS=<n> on big VMs.
ARG CARGO_BUILD_JOBS=4
ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS}

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY proto ./proto
COPY crates ./crates
COPY services/polyphony-xbt ./services/polyphony-xbt
COPY services/mapi-xbt ./services/mapi-xbt
COPY services/tikv-gc ./services/tikv-gc
COPY services/daw ./services/daw

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo build --release -p mapi-xbt \
    && cp target/release/mapi-xbt /mapi-xbt

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl tini && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 1000 --create-home nonroot

COPY --from=builder /mapi-xbt /usr/local/bin/mapi-xbt

USER nonroot
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "mapi-xbt"]

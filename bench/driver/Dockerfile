FROM rust:1.85.1-bookworm AS build
WORKDIR /app
ENV CARGO_RESOLVER_INCOMPATIBLE_RUST_VERSIONS=fallback
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked && cp target/release/cairn-benchmark /tmp/cairn-benchmark
FROM debian:bookworm-slim
ARG BENCH_SOURCE_SHA256
LABEL cairn.benchmark.source-sha256=$BENCH_SOURCE_SHA256
COPY --from=build /tmp/cairn-benchmark /usr/local/bin/cairn-benchmark
ENTRYPOINT ["cairn-benchmark"]

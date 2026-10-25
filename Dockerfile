FROM rust:1.85.1-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cairn-server-target,target=/app/target \
    cargo build --release --locked -p cairn-server && cp target/release/cairn-server /tmp/cairn-server

FROM debian:bookworm-slim
COPY --from=build /tmp/cairn-server /usr/local/bin/cairn-server
USER 65534:65534
EXPOSE 5432
ENTRYPOINT ["cairn-server"]

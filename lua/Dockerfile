FROM rust:1.85.1-bookworm AS verified
WORKDIR /app
COPY crates/cairn-verified/src/lib.rs crates/cairn-verified/src/lib.rs
COPY crates/cairn-verified/native/Cargo.toml crates/cairn-verified/native/Cargo.lock crates/cairn-verified/native/lib.rs crates/cairn-verified/native/
RUN cargo build --release --locked --manifest-path crates/cairn-verified/native/Cargo.toml
FROM tarantool/tarantool:3.3.5
COPY --from=verified /app/crates/cairn-verified/native/target/release/libcairn_verified_native.so /usr/local/lib/libcairn_verified_native.so
# Including scripts in the image makes `compose up --build` restart storage
# after a Lua/configuration change, even with the development bind mount.
COPY lua/storage.lua lua/transactions.lua lua/volatile.lua lua/replicated.yaml /opt/cairn/

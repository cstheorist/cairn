# Continuous integration

`check.sh` is the local and GitHub Actions entry point for static, standalone,
replicated and formal checks. Docker builds pin the Rust 1.85 release baseline,
Tarantool 3.3.5 and PostgreSQL 16. The development/Verus compiler is pinned by
`rust-toolchain.toml`; formal downloads have SHA-256 checks in
`scripts/bootstrap_formal.py`.

The replication job runs the original coordinator protocol and all optional
vote/local-finish/volatile modes. Failures preserve container logs in the Actions
output. Formal runs retain bounded-model and negative-check output under `tla/out`.

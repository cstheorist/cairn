# Contributing to Cairn

Bug reports, SQL compatibility cases, documentation fixes and code are welcome.
For larger changes, use an issue or draft pull request to make the scope clear.

## Development setup

| Tool | Purpose |
|---|---|
| Rust via rustup | `rust-toolchain.toml` pins 1.99.1; install rustfmt and clippy |
| Docker with Compose | Storage, PostgreSQL reference, integration and fault tests |
| Python 3 | Cluster/test orchestration; client dependencies live in the test image |
| Verus and Java/TLC | Formal checks; `scripts/bootstrap_formal.py` fetches pinned tools |

Start with [getting started](docs/getting-started.md) and [design](docs/design.md).

## Checks

```sh
make check
cargo test --workspace --locked
make up test recovery
make ha-up ha-test ha-recovery
make vote-test local-finish-test volatile-test
python3 scripts/bootstrap_formal.py
make formal
python3 scripts/check_model_sensitivity.py
```

Run the suites relevant to a change and the complete affected checks before
merging. The Docker suites exercise real wire/storage behavior; compiling the
Rust crates alone does not replace them. On the original macOS development
machine, use `PYTHON=/usr/bin/python3` if the system's other Python is an
incompatible binary.

## Protocol and verified code

* Changes to `lua/transactions.lua`, `lua/volatile.lua` or the coordinator in
  `crates/cairn-sql/src/transaction.rs` need the corresponding TLA+ contract
  checked and live storage/fault regressions. Add a negative model check when
  adding a safety guard.
* Edit `crates/cairn-verified/verify.rs` and regenerate with
  `python3 scripts/extract_core.py`. Keep Verus at zero errors. The generated
  runtime bodies must remain identical to their checked source.
* Never replay a non-idempotent storage request after transport uncertainty.
  Preserve the distinction between known abort (`40001`) and uncertain commit
  (`40003`).
* SQL changes are compared with PostgreSQL 16, including values, column types,
  NULLs, errors and transaction behavior. Unsupported syntax must fail explicitly.
* Performance changes need per-key correctness checks. Comparative claims need
  repeated matched runs and complete environment/source evidence.

## Style and reviews

Use rustfmt, warning-free Clippy, and comments explaining protocol decisions.
Keep one logical change per pull request. Describe the concrete behavior, why it
changed, and what was tested. Update the relevant docs and `CHANGELOG.md` for
user-visible changes. Keep performance limitations and proof boundaries explicit.

## License

Contributions are dual licensed under MIT OR Apache-2.0 on the terms in the
[README](README.md#license), unless explicitly stated otherwise.

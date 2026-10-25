# Working on Cairn

Cairn is the PostgreSQL counterpart to Tarn: Rust crates over native Tarantool
storage, with TLA+ contracts and shared executable Verus bodies. Read
`docs/design.md` and `docs/verification.md` first.

## Commands

```sh
make check
cargo test --workspace --locked
make up test recovery
make ha-up ha-test ha-recovery
make vote-test local-finish-test volatile-test
make formal
python3 scripts/check_model_sensitivity.py
```

Use `PYTHON=/usr/bin/python3` on the original macOS workspace if needed.

## Implementation rules

- Match PostgreSQL behavior with differential tests and reject unsupported SQL explicitly.
- Changes to commit/locking/recovery need the corresponding TLA+ checks and live faults.
- Edit `crates/cairn-verified/verify.rs`, then run `scripts/extract_core.py`; keep Verus at zero errors.
- Never replay a non-idempotent mutation after transport uncertainty. Preserve `40001` versus `40003`.
- Keep native user tuples as committed data, with documented strong versus raw native read semantics.
- Keep benchmark claims tied to complete, matched, repeated evidence. Record uncertainty and proof boundaries.
- Use `codex/` branches for continued implementation and describe behavior and validation in pull requests.

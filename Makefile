.PHONY: up stop test recovery formal bench compare comparison-report check ha-up ha-test ha-recovery vote-test local-finish-test volatile-test pipeline-test open-point-test
PYTHON ?= python3
up:
	docker compose up -d --build --wait gateway
stop:
	docker compose stop
test:
	docker compose --profile test build tests
	docker compose --profile test run --rm tests
recovery:
	sh scripts/check_recovery.sh
ha-up:
	docker compose -f compose.replication.yaml up -d --build --wait gateway
ha-test:
	docker compose -f compose.replication.yaml --profile test build tests
	mkdir -p artifacts
	docker cp cairn-ha-s0a-1:/usr/local/lib/libcairn_verified_native.so artifacts/libcairn_verified_native.so
	docker compose -f compose.replication.yaml --profile test run --no-deps --rm tests python /tests/native_core.py /artifacts/libcairn_verified_native.so
	docker compose -f compose.replication.yaml --profile reference up -d --wait reference
	docker compose -f compose.replication.yaml --profile test run --no-deps --rm tests python /tests/encoding.py
	docker compose -f compose.replication.yaml --profile test run --no-deps --rm tests python /tests/scalar_text.py
	docker compose -f compose.replication.yaml --profile test run --no-deps --rm tests python /tests/extended_cycles.py
	docker compose -f compose.replication.yaml --profile test run --no-deps --rm tests python /tests/transactions.py
	docker compose -f compose.replication.yaml --profile test run --no-deps --rm tests python /tests/transaction_protocol_retries.py
	docker compose -f compose.replication.yaml --profile test run --no-deps --rm tests python /tests/transaction_protocol.py
	docker compose -f compose.replication.yaml --profile test run --no-deps --rm tests python /tests/local_commit.py
	docker compose -f compose.replication.yaml --profile test run --no-deps --rm tests python /tests/transaction_start.py
	docker compose -f compose.replication.yaml --profile test run --no-deps --rm tests python /tests/scans.py
	docker compose -f compose.replication.yaml --profile test run --no-deps --rm tests python /tests/read_batches.py
	docker compose -f compose.replication.yaml --profile test run --no-deps --rm tests python /tests/relational.py
	docker compose -f compose.replication.yaml --profile test run --no-deps --rm tests python /tests/mutations.py
	docker compose -f compose.replication.yaml --profile test run --no-deps --rm tests python /tests/storage_formats.py
	$(PYTHON) scripts/check_replication.py
ha-recovery:
	$(PYTHON) scripts/check_transaction_recovery.py
vote-test:
	$(PYTHON) scripts/check_votes.py
	$(PYTHON) scripts/check_vote_recovery.py
local-finish-test:
	CAIRN_VOTE_COMMIT=2 $(PYTHON) scripts/check_votes.py
	CAIRN_VOTE_COMMIT=2 $(PYTHON) scripts/check_vote_recovery.py
volatile-test:
	$(PYTHON) scripts/check_volatile.py
	$(PYTHON) scripts/check_volatile_recovery.py
pipeline-test:
	CAIRN_STORAGE_PIPELINE=1 $(MAKE) -j1 ha-up ha-test ha-recovery vote-test local-finish-test volatile-test open-point-test PYTHON="$(PYTHON)"
open-point-test:
	$(PYTHON) scripts/check_open_point_reads.py
formal:
	sh scripts/check_formal.sh
bench:
	docker compose --profile test run --rm tests python /bench/read.py
compare:
	$(PYTHON) scripts/run_comparison.py
comparison-report:
	$(PYTHON) scripts/report_comparison.py
check:
	$(PYTHON) -B tests/profile_storage.py
	$(PYTHON) -B tests/replication_probe.py
	$(PYTHON) -B -m unittest discover -s tests -p test_read_batch_settings.py
	$(PYTHON) scripts/extract_core.py --check
	cargo fmt --all --check
	cargo clippy --workspace --locked --all-targets -- -D warnings
	cargo fmt --manifest-path crates/cairn-verified/native/Cargo.toml --check
	cargo clippy --manifest-path crates/cairn-verified/native/Cargo.toml --locked --release -- -D warnings

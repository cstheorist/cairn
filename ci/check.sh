#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
: "${PYTHON:=python3}"
case "${1:-static}" in
    static) make check PYTHON="$PYTHON"; cargo test --workspace --locked ;;
    integration) make up test recovery PYTHON="$PYTHON" ;;
    replication) make ha-up ha-test ha-recovery vote-test local-finish-test volatile-test open-point-test PYTHON="$PYTHON" ;;
    formal) "$PYTHON" scripts/bootstrap_formal.py; make formal PYTHON="$PYTHON"; "$PYTHON" scripts/check_model_sensitivity.py ;;
    *) echo 'Usage: ci/check.sh static|integration|replication|formal' >&2; exit 2 ;;
esac

#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
: "${PYTHON:=python3}"
"$PYTHON" scripts/extract_core.py --check
if [ -d .tools/cargo/bin ]; then
    export CARGO_HOME="$PWD/.tools/cargo"
    export RUSTUP_HOME="$PWD/.tools/rustup"
    export PATH="$CARGO_HOME/bin:$PATH"
fi
if [ -z "${VERUS:-}" ]; then
    case "$(uname -s):$(uname -m)" in
        Darwin:arm64) VERUS="$PWD/.tools/verus-arm64-macos/verus" ;;
        Darwin:x86_64) VERUS="$PWD/.tools/verus-x86-macos/verus" ;;
        Linux:x86_64) VERUS="$PWD/.tools/verus-x86-linux/verus" ;;
        *) VERUS=verus ;;
    esac
fi
: "${TLA2TOOLS:=.tools/tla2tools.jar}"
mkdir -p tla/out
if "$VERUS" --crate-type lib crates/cairn-verified/verify.rs > tla/out/verus.out 2>&1; then
    cat tla/out/verus.out
else
    cat tla/out/verus.out
    exit 1
fi
for model in ShardStore ReplicaEpoch TwoPhaseCommit PredicateLocks ReadBatch VoteCommit LocalFinish VolatileOpen IprotoPipeline OpenPointRead; do
  configurations="$model"
  if [ "$model" = IprotoPipeline ]; then configurations="$model IprotoPipelineCount"; fi
  if [ "$model" = OpenPointRead ]; then configurations="$model OpenPointReadBatch"; fi
  for configuration in $configurations; do
    output="tla/out/$configuration.out"
    if [ "${TLC_USE_JAVA:-0}" = 1 ]; then
        if java -XX:+UseParallelGC -cp "$TLA2TOOLS" tlc2.TLC -workers 2 -metadir "tla/states/$configuration" -config "tla/$configuration.cfg" "tla/$model.tla" > "$output" 2>&1; then
            cat "$output"
        else cat "$output"; exit 1; fi
    else
        if docker run --rm -v "$PWD:/app" -w /app eclipse-temurin:21-jre java -XX:+UseParallelGC -cp "$TLA2TOOLS" tlc2.TLC -workers 2 -metadir "tla/states/$configuration" -config "tla/$configuration.cfg" "tla/$model.tla" > "$output" 2>&1; then
            cat "$output"
        else cat "$output"; exit 1; fi
    fi
  done
done

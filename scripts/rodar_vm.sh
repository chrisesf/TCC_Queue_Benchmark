#!/bin/sh
# Campanha na VM sem Docker e sem sudo.
#
# Uso:
#   ./scripts/instalar_deps_vm.sh       # uma vez (usuario)
#   REPS=2 ./scripts/rodar_vm.sh        # smoke
#   ./scripts/rodar_vm.sh               # N=30

set -eu

ROOT=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cd "$ROOT"

if [ -f "$HOME/.tcc-bench-env" ]; then
  # shellcheck disable=SC1091
  . "$HOME/.tcc-bench-env"
elif [ -f "$HOME/.cargo/env" ]; then
  # shellcheck disable=SC1091
  . "$HOME/.cargo/env"
fi

PHASE=${PHASE:-tudo}
REPS=${REPS:-30}
OUT=${OUT:-$ROOT/results/tcc}
BIN_DIR=${BIN_DIR:-$ROOT/target/release}
export PHASE REPS OUT BIN_DIR
export AMQP_URL="${AMQP_URL:-amqp://127.0.0.1:5672/%2f}"
export KAFKA_BROKERS="${KAFKA_BROKERS:-127.0.0.1:9092}"
export BROKER_CONTAINER=none

echo "=== VM: $(hostname) | user=$(id -un) | nucleos=$(nproc) | phase=$PHASE reps=$REPS ==="
command -v cargo >/dev/null || {
  echo "cargo nao encontrado. Rode: ./scripts/instalar_deps_vm.sh" >&2
  exit 1
}

echo "=== 1/5 build release (features brokers) ==="
cargo build --release --features brokers --bins

mkdir -p "$OUT"

if [ "$PHASE" = "memoria" ] || [ "$PHASE" = "tudo" ]; then
  echo "=== 2/5 filas em memoria ==="
  sh scripts/kafka_local.sh stop >/dev/null 2>&1 || true
  sh scripts/rabbitmq_local.sh stop >/dev/null 2>&1 || true
  PHASE=memoria sh scripts/campanha_tcc.sh
fi

if [ "$PHASE" = "brokers" ] || [ "$PHASE" = "tudo" ]; then
  echo "=== 3/5 sobe RabbitMQ e Kafka (usuario) ==="
  sh scripts/rabbitmq_local.sh start
  sh scripts/kafka_local.sh start

  echo "=== 4/5 benchmarks dos brokers ==="
  PHASE=brokers sh scripts/campanha_tcc.sh

  echo "=== derruba brokers ==="
  sh scripts/kafka_local.sh stop
  sh scripts/rabbitmq_local.sh stop
fi

echo "=== 5/5 comparacao estatistica (Secao 4.6) ==="
CSVS=
[ -f "$OUT/memoria.csv" ] && CSVS="$CSVS $OUT/memoria.csv"
[ -f "$OUT/brokers.csv" ] && CSVS="$CSVS $OUT/brokers.csv"
if [ -n "$CSVS" ]; then
  # shellcheck disable=SC2086
  "$BIN_DIR/compare_runs" $CSVS | tee "$OUT/comparacao.txt"
  echo "relatorio: $OUT/comparacao.txt"
else
  echo "nenhum CSV em $OUT" >&2
  exit 1
fi

echo "=== pronto ==="
ls -la "$OUT"

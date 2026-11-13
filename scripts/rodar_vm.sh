#!/bin/sh
# Orquestra a campanha na VM: build, filas em memoria (brokers desligados),
# brokers, comparacao estatistica e limpeza.
#
# Uso (na raiz do repositorio, na VM):
#   chmod +x scripts/*.sh
#   ./scripts/rodar_vm.sh              # campanha completa (N=30)
#   REPS=3 ./scripts/rodar_vm.sh       # smoke test
#   PHASE=memoria ./scripts/rodar_vm.sh
#   PHASE=brokers ./scripts/rodar_vm.sh

set -eu

ROOT=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cd "$ROOT"

PHASE=${PHASE:-tudo}
REPS=${REPS:-30}
OUT=${OUT:-$ROOT/results/tcc}
export PHASE REPS OUT

echo "=== VM: $(hostname) | nucleos=$(nproc) | phase=$PHASE reps=$REPS ==="
command -v docker >/dev/null || { echo "instale o Docker" >&2; exit 1; }
docker compose version >/dev/null || { echo "instale o plugin docker compose" >&2; exit 1; }

wait_healthy() {
  name=$1
  i=0
  while [ "$i" -lt 60 ]; do
    status=$(docker inspect -f '{{.State.Health.Status}}' "$name" 2>/dev/null || echo missing)
    if [ "$status" = "healthy" ]; then
      echo "$name healthy"
      return 0
    fi
    i=$((i + 1))
    sleep 2
  done
  echo "timeout esperando $name (status=$status)" >&2
  docker logs --tail 40 "$name" || true
  exit 1
}

run_in_bench() {
  docker compose run --rm -T \
    -e PHASE \
    -e REPS \
    -e OUT=/app/results/tcc \
    -e PRODUCERS="${PRODUCERS:-4}" \
    -e CONSUMERS="${CONSUMERS:-4}" \
    -e BIN_DIR=/build/release \
    bench "$@"
}

echo "=== 1/5 build da imagem e dos binarios ==="
docker compose build bench
run_in_bench cargo build --release --features brokers --bins

mkdir -p "$OUT"

if [ "$PHASE" = "memoria" ] || [ "$PHASE" = "tudo" ]; then
  echo "=== 2/5 filas em memoria (brokers parados) ==="
  docker compose --profile brokers stop >/dev/null 2>&1 || true
  PHASE=memoria run_in_bench sh -c 'tr -d "\r" < scripts/campanha_tcc.sh | sh'
fi

if [ "$PHASE" = "brokers" ] || [ "$PHASE" = "tudo" ]; then
  echo "=== 3/5 sobe RabbitMQ e Kafka ==="
  docker compose --profile brokers up -d rabbitmq kafka
  wait_healthy tcc-rabbitmq
  wait_healthy tcc-kafka

  echo "=== 4/5 benchmarks dos brokers ==="
  PHASE=brokers run_in_bench sh -c 'tr -d "\r" < scripts/campanha_tcc.sh | sh'

  echo "=== derruba brokers ==="
  docker compose --profile brokers stop
fi

echo "=== 5/5 comparacao estatistica (Secao 4.6) ==="
CSVS=
[ -f "$OUT/memoria.csv" ] && CSVS="$CSVS /app/results/tcc/memoria.csv"
[ -f "$OUT/brokers.csv" ] && CSVS="$CSVS /app/results/tcc/brokers.csv"
if [ -n "$CSVS" ]; then
  # shellcheck disable=SC2086
  run_in_bench sh -c "/build/release/compare_runs $CSVS" | tee "$OUT/comparacao.txt"
  echo "relatorio: $OUT/comparacao.txt"
else
  echo "nenhum CSV em $OUT" >&2
  exit 1
fi

echo "=== pronto ==="
echo "CSVs: $OUT/"
ls -la "$OUT"

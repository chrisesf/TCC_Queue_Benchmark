#!/bin/sh
# Campanha do TCC (Secoes 4.2, 4.3 e 4.6) com varredura de escalabilidade.
# Topologia MPMC N×N, N na progressao geometrica (padrao: 1 2 4 8 16 32).
# Payloads 64 B / 1 KB / 64 KB, chegada rajada e equilibrada, N_reps=30.
# Ring buffer: payload inline. Mutex e Michael-Scott: heap.
#
# Variaveis (opcionais):
#   PHASE=memoria|brokers|tudo   (padrao: tudo)
#   REPS=30
#   SCALE="1 2 4 8 16 32"      # produtores = consumidores = cada valor
#   SCALE_MAX=                 # se definido, corta N > SCALE_MAX (ex.: $(nproc))
#   PIN_CORES=0|1              # 1 = taskset nos primeiros 2N CPUs (se existir)
#   OUT=results/tcc
#   BIN_DIR=target/release
#   AMQP_URL=amqp://127.0.0.1:5672/%2f
#   KAFKA_BROKERS=127.0.0.1:9092
#   BROKER_CONTAINER=none      (sem Docker nao ha stats do container)
#
# Uso nativo (via rodar_vm.sh):
#   PHASE=memoria sh scripts/campanha_tcc.sh
#   SCALE="1 2 4 8" REPS=5 PHASE=memoria sh scripts/campanha_tcc.sh   # smoke escala
#   SCALE_MAX=24 PIN_CORES=1 sh scripts/campanha_tcc.sh
#
# Graficos: o CSV ja traz producers/consumers por linha — eixo X = N (ou 2N threads).

set -eu

ROOT=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cd "$ROOT"

PHASE=${PHASE:-tudo}
REPS=${REPS:-30}
SCALE=${SCALE:-"1 2 4 8 16 32"}
SCALE_MAX=${SCALE_MAX:-}
PIN_CORES=${PIN_CORES:-0}
OUT=${OUT:-$ROOT/results/tcc}
BIN_DIR=${BIN_DIR:-}
if [ -z "$BIN_DIR" ]; then
  if [ -x /build/release/mutex_bench ]; then
    BIN_DIR=/build/release
  else
    BIN_DIR=$ROOT/target/release
  fi
fi

mkdir -p "$OUT"
DONE=$OUT/done.txt
touch "$DONE"

NPROC=$(nproc 2>/dev/null || echo 0)

echo "campanha: phase=$PHASE reps=$REPS scale=[$SCALE] pin=$PIN_CORES bin=$BIN_DIR out=$OUT"
echo "nucleos visiveis: ${NPROC:-?}"

need() {
  if [ ! -x "$1" ]; then
    echo "binario ausente: $1 (rode cargo build --release --features brokers --bins)" >&2
    exit 1
  fi
}

run() {
  key=$1
  shift
  if grep -Fqx "$key" "$DONE" 2>/dev/null; then
    echo "pula $key"
    return 0
  fi
  echo "---- $(date -u +%Y-%m-%dT%H:%M:%SZ) $key"
  if ! "$@"; then
    echo "falhou $key" >&2
    exit 1
  fi
  echo "$key" >> "$DONE"
}

# Prefixa taskset quando PIN_CORES=1 e ha CPUs suficientes.
with_pin() {
  n=$1
  shift
  if [ "$PIN_CORES" != "1" ]; then
    "$@"
    return $?
  fi
  if ! command -v taskset >/dev/null 2>&1; then
    echo "aviso: PIN_CORES=1 mas taskset ausente; seguindo sem pinagem" >&2
    "$@"
    return $?
  fi
  # MPMC N×N → ate 2N threads de trabalho; pin nos primeiros 2N CPUs (cap em NPROC-1).
  last=$((n * 2 - 1))
  if [ "$NPROC" -gt 0 ] && [ "$last" -ge "$NPROC" ]; then
    last=$((NPROC - 1))
  fi
  if [ "$last" -lt 0 ]; then last=0; fi
  echo "  taskset -c 0-$last"
  taskset -c "0-$last" "$@"
}

msgs_for() {
  producers=$1
  payload=$2
  base=$((40000 / producers))
  if [ "$base" -lt 1000 ]; then base=1000; fi
  if [ "$payload" -eq 65536 ]; then
    base=$((base / 8))
    if [ "$base" -lt 200 ]; then base=200; fi
  elif [ "$payload" -eq 1024 ]; then
    base=$((base / 2))
    if [ "$base" -lt 500 ]; then base=500; fi
  fi
  echo "$base"
}

rate_for() {
  producers=$1
  payload=$2
  case "$payload" in
    65536) cap=400 ;;
    1024)  cap=2000 ;;
    *)     cap=8000 ;;
  esac
  rate=$((cap / producers))
  if [ "$rate" -lt 50 ]; then rate=50; fi
  echo "$rate"
}

cap_for() {
  if [ "$1" -eq 65536 ]; then echo 1024; else echo 8192; fi
}

if [ "$PHASE" = "memoria" ] || [ "$PHASE" = "tudo" ]; then
  need "$BIN_DIR/mutex_bench"
  need "$BIN_DIR/michael_scott_bench"
  need "$BIN_DIR/ring_buffer_bench"
fi
if [ "$PHASE" = "brokers" ] || [ "$PHASE" = "tudo" ]; then
  need "$BIN_DIR/rabbitmq_bench"
  need "$BIN_DIR/kafka_bench"
fi

AMQP_URL=${AMQP_URL:-amqp://127.0.0.1:5672/%2f}
KAFKA_BROKERS=${KAFKA_BROKERS:-127.0.0.1:9092}
BROKER_CONTAINER=${BROKER_CONTAINER:-none}

# Monta lista de escalas (filtra SCALE_MAX se definido).
SCALES=
for n in $SCALE; do
  case "$n" in
    ''|*[!0-9]*)
      echo "SCALE invalido: '$n' (use inteiros)" >&2
      exit 1
      ;;
  esac
  if [ "$n" -lt 1 ]; then
    echo "SCALE deve ser >= 1 (obtido $n)" >&2
    exit 1
  fi
  if [ -n "$SCALE_MAX" ] && [ "$n" -gt "$SCALE_MAX" ]; then
    echo "pula N=$n (> SCALE_MAX=$SCALE_MAX)"
    continue
  fi
  SCALES="$SCALES $n"
done
SCALES=$(echo "$SCALES" | sed 's/^ *//')
if [ -z "$SCALES" ]; then
  echo "nenhuma escala restante apos filtro SCALE_MAX" >&2
  exit 1
fi

for n in $SCALES; do
  producers=$n
  consumers=$n
  threads=$((producers + consumers))
  echo "======== escala MPMC ${producers}x${consumers} (threads trabalho=$threads) ========"
  if [ "$NPROC" -gt 0 ] && [ "$threads" -gt "$NPROC" ]; then
    echo "aviso: $threads threads > $NPROC nucleos visiveis (oversubscribe)"
  fi

  for payload in 64 1024 65536; do
    msgs=$(msgs_for "$producers" "$payload")
    warm=$((msgs / 10))
    if [ "$warm" -lt 50 ]; then warm=50; fi
    cap=$(cap_for "$payload")
    rate=$(rate_for "$producers" "$payload")
    pmsgs=$rate
    if [ "$pmsgs" -lt 200 ]; then pmsgs=200; fi
    pwarm=$((pmsgs / 10))
    if [ "$pwarm" -lt 20 ]; then pwarm=20; fi
    tx=$msgs
    if [ "$tx" -gt 1000 ]; then tx=1000; fi
    half=$((msgs / 2))
    if [ "$tx" -gt "$half" ] && [ "$half" -ge 50 ]; then tx=$half; fi

    if [ "$PHASE" = "memoria" ] || [ "$PHASE" = "tudo" ]; then
      for kind_bin in "heap mutex_bench" "heap michael_scott_bench" "inline ring_buffer_bench"; do
        kind=${kind_bin%% *}
        name=${kind_bin#* }
        bin=$BIN_DIR/$name
        run "mem $name $kind p${producers} c${consumers} b${payload} burst" \
          with_pin "$n" "$bin" --payload-kind "$kind" --producers "$producers" --consumers "$consumers" \
          --payload "$payload" --capacity "$cap" --messages "$msgs" --warmup "$warm" \
          --repetitions "$REPS" --arrival burst --batch 1000 --pause-ns 1000000 \
          --csv "$OUT/memoria.csv"
        run "mem $name $kind p${producers} c${consumers} b${payload} paced" \
          with_pin "$n" "$bin" --payload-kind "$kind" --producers "$producers" --consumers "$consumers" \
          --payload "$payload" --capacity "$cap" --messages "$pmsgs" --warmup "$pwarm" \
          --repetitions "$REPS" --arrival paced --rate "$rate" \
          --csv "$OUT/memoria.csv"
      done
    fi

    if [ "$PHASE" = "brokers" ] || [ "$PHASE" = "tudo" ]; then
      for sem in at-most-once at-least-once; do
        run "rabbit $sem p${producers} c${consumers} b${payload} burst" \
          with_pin "$n" "$BIN_DIR/rabbitmq_bench" --semantics "$sem" \
          --amqp-url "$AMQP_URL" --broker-container "$BROKER_CONTAINER" \
          --producers "$producers" --consumers "$consumers" \
          --payload "$payload" --messages "$msgs" --warmup "$warm" --repetitions "$REPS" \
          --arrival burst --batch 1000 --pause-ns 1000000 --idle-timeout-ms 500 \
          --csv "$OUT/brokers.csv"
        run "rabbit $sem p${producers} c${consumers} b${payload} paced" \
          with_pin "$n" "$BIN_DIR/rabbitmq_bench" --semantics "$sem" \
          --amqp-url "$AMQP_URL" --broker-container "$BROKER_CONTAINER" \
          --producers "$producers" --consumers "$consumers" \
          --payload "$payload" --messages "$pmsgs" --warmup "$pwarm" --repetitions "$REPS" \
          --arrival paced --rate "$rate" --idle-timeout-ms 500 \
          --csv "$OUT/brokers.csv"
      done
      for sem in at-most-once at-least-once exactly-once; do
        run "kafka $sem p${producers} c${consumers} b${payload} burst" \
          with_pin "$n" "$BIN_DIR/kafka_bench" --semantics "$sem" \
          --kafka-brokers "$KAFKA_BROKERS" --broker-container "$BROKER_CONTAINER" \
          --producers "$producers" --consumers "$consumers" \
          --payload "$payload" --messages "$msgs" --warmup "$warm" --repetitions "$REPS" \
          --arrival burst --batch 1000 --pause-ns 1000000 --tx-batch "$tx" --idle-timeout-ms 500 \
          --csv "$OUT/brokers.csv"
        run "kafka $sem p${producers} c${consumers} b${payload} paced" \
          with_pin "$n" "$BIN_DIR/kafka_bench" --semantics "$sem" \
          --kafka-brokers "$KAFKA_BROKERS" --broker-container "$BROKER_CONTAINER" \
          --producers "$producers" --consumers "$consumers" \
          --payload "$payload" --messages "$pmsgs" --warmup "$pwarm" --repetitions "$REPS" \
          --arrival paced --rate "$rate" --tx-batch "$tx" --idle-timeout-ms 500 \
          --csv "$OUT/brokers.csv"
      done
    fi
  done
done

echo "campanha concluida ($PHASE) escalas=[$SCALES]"

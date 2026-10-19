#!/bin/sh
# Protocolo da Secao 4.6 (N=30, warm-up) em MPMC 4x4, sem varrer a quantidade de
# nucleos. Payloads 64 B, 1 KB e 64 KB; chegada em rajada e equilibrada. O ring buffer
# usa payload inline; mutex e Michael-Scott usam heap (Secao 4.2). O equilibrado oferece
# no maximo ~8 mil msg/s no total em 64 B (menos com payload grande).
set -u
cd /app
# memoria | brokers | tudo. Os brokers so entram com os containers no ar.
PHASE=${PHASE:-tudo}
OUT=/app/results/tcc
mkdir -p "$OUT"
B=/build/release
REPS=30
DONE=$OUT/done.txt
touch "$DONE"

run() {
  key=$1
  shift
  if grep -Fqx "$key" "$DONE"; then
    echo "pula $key"
    return 0
  fi
  echo "---- $(date -u +%H:%M:%S) $key"
  if ! "$@"; then
    echo "falhou $key" >&2
    exit 1
  fi
  echo "$key" >> "$DONE"
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
    1024) cap=2000 ;;
    *) cap=8000 ;;
  esac
  rate=$((cap / producers))
  if [ "$rate" -lt 50 ]; then rate=50; fi
  echo "$rate"
}

cap_for() {
  if [ "$1" -eq 65536 ]; then echo 1024; else echo 8192; fi
}

producers=4
consumers=4
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
      for kind_bin in "heap $B/mutex_bench" "heap $B/michael_scott_bench" "inline $B/ring_buffer_bench"; do
        kind=${kind_bin%% *}
        bin=${kind_bin#* }
        name=$(basename "$bin")
        run "mem $name $kind p${producers} c${consumers} b${payload} burst" \
          "$bin" --payload-kind "$kind" --producers "$producers" --consumers "$consumers" \
          --payload "$payload" --capacity "$cap" --messages "$msgs" --warmup "$warm" \
          --repetitions "$REPS" --arrival burst --batch 1000 --pause-ns 1000000 \
          --csv "$OUT/memoria.csv"
        run "mem $name $kind p${producers} c${consumers} b${payload} paced" \
          "$bin" --payload-kind "$kind" --producers "$producers" --consumers "$consumers" \
          --payload "$payload" --capacity "$cap" --messages "$pmsgs" --warmup "$pwarm" \
          --repetitions "$REPS" --arrival paced --rate "$rate" \
          --csv "$OUT/memoria.csv"
      done
      fi

      # Brokers: mesmas dimensoes. Semanticas da Secao 2.4; RabbitMQ nao tem exactly-once.
      if [ "$PHASE" = "brokers" ] || [ "$PHASE" = "tudo" ]; then
      for sem in at-most-once at-least-once; do
        run "rabbit $sem p${producers} c${consumers} b${payload} burst" \
          "$B/rabbitmq_bench" --semantics "$sem" --producers "$producers" --consumers "$consumers" \
          --payload "$payload" --messages "$msgs" --warmup "$warm" --repetitions "$REPS" \
          --arrival burst --batch 1000 --pause-ns 1000000 --idle-timeout-ms 500 \
          --csv "$OUT/brokers.csv"
        run "rabbit $sem p${producers} c${consumers} b${payload} paced" \
          "$B/rabbitmq_bench" --semantics "$sem" --producers "$producers" --consumers "$consumers" \
          --payload "$payload" --messages "$pmsgs" --warmup "$pwarm" --repetitions "$REPS" \
          --arrival paced --rate "$rate" --idle-timeout-ms 500 \
          --csv "$OUT/brokers.csv"
      done
      for sem in at-most-once at-least-once exactly-once; do
        run "kafka $sem p${producers} c${consumers} b${payload} burst" \
          "$B/kafka_bench" --semantics "$sem" --producers "$producers" --consumers "$consumers" \
          --payload "$payload" --messages "$msgs" --warmup "$warm" --repetitions "$REPS" \
          --arrival burst --batch 1000 --pause-ns 1000000 --tx-batch "$tx" --idle-timeout-ms 500 \
          --csv "$OUT/brokers.csv"
        run "kafka $sem p${producers} c${consumers} b${payload} paced" \
          "$B/kafka_bench" --semantics "$sem" --producers "$producers" --consumers "$consumers" \
          --payload "$payload" --messages "$pmsgs" --warmup "$pwarm" --repetitions "$REPS" \
          --arrival paced --rate "$rate" --tx-batch "$tx" --idle-timeout-ms 500 \
          --csv "$OUT/brokers.csv"
      done
      fi
done

echo "campanha concluida"

#!/bin/sh
# Sobe / para / status do Kafka local (KRaft), sem Docker.
# Uso: ./scripts/kafka_local.sh start|stop|status

set -eu

KAFKA_HOME=${KAFKA_HOME:-$HOME/kafka}
PROP=$KAFKA_HOME/config/kraft/tcc.properties
PIDFILE=$KAFKA_HOME/kafka.pid
LOG=$KAFKA_HOME/logs/server.log
export KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xms2G -Xmx2G}"

cmd=${1:-status}

if [ ! -d "$KAFKA_HOME" ]; then
  echo "Kafka nao encontrado em $KAFKA_HOME" >&2
  echo "rode: sudo ./scripts/instalar_deps_vm.sh" >&2
  exit 1
fi

is_up() {
  ss -lnt 2>/dev/null | grep -q ':9092 ' || \
    netstat -lnt 2>/dev/null | grep -q ':9092 '
}

case "$cmd" in
  start)
    if is_up; then
      echo "Kafka ja esta escutando em 127.0.0.1:9092"
      exit 0
    fi
    mkdir -p "$KAFKA_HOME/logs" "$KAFKA_HOME/data"
    if [ ! -f "$KAFKA_HOME/data/meta.properties" ]; then
      echo "formatando storage KRaft..."
      CLUSTER_ID=$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)
      "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$PROP" --standalone
    fi
    echo "iniciando Kafka..."
    nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$PROP" >"$LOG" 2>&1 &
    echo $! >"$PIDFILE"
    i=0
    while [ "$i" -lt 60 ]; do
      if is_up; then
        echo "Kafka ok (pid $(cat "$PIDFILE"))"
        exit 0
      fi
      i=$((i + 1))
      sleep 1
    done
    echo "timeout ao subir Kafka; veja $LOG" >&2
    tail -40 "$LOG" || true
    exit 1
    ;;
  stop)
    if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
      kill "$(cat "$PIDFILE")" 2>/dev/null || true
      # espera graceful
      i=0
      while [ "$i" -lt 20 ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; do
        i=$((i + 1))
        sleep 1
      done
      kill -9 "$(cat "$PIDFILE")" 2>/dev/null || true
      rm -f "$PIDFILE"
    fi
    # fallback: mata pelo script de start se sobrou processo
    pkill -f "kafka.Kafka $PROP" 2>/dev/null || true
    echo "Kafka parado"
    ;;
  status)
    if is_up; then
      echo "Kafka: UP (127.0.0.1:9092)"
    else
      echo "Kafka: DOWN"
      exit 1
    fi
    ;;
  *)
    echo "uso: $0 start|stop|status" >&2
    exit 2
    ;;
esac

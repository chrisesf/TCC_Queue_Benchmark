#!/bin/sh
# Sobe / para / status do RabbitMQ em $HOME (generic-unix), sem systemd/sudo.
# Uso: ./scripts/rabbitmq_local.sh start|stop|status

set -eu

if [ -f "$HOME/.tcc-bench-env" ]; then
  # shellcheck disable=SC1091
  . "$HOME/.tcc-bench-env"
fi

RABBITMQ_HOME=${RABBITMQ_HOME:-$HOME/opt/rabbitmq}
export PATH="${RABBITMQ_HOME}/sbin:${ERLANG_HOME:-$HOME/opt/erlang}/bin:$PATH"
# NODENAME unico por usuario, evita conflito com /var/lib/rabbitmq do sistema.
export RABBITMQ_NODENAME="${RABBITMQ_NODENAME:-rabbit@localhost}"
export RABBITMQ_NODE_IP_ADDRESS=127.0.0.1
export RABBITMQ_NODE_PORT=5672
export RABBITMQ_MNESIA_BASE="${RABBITMQ_MNESIA_BASE:-$HOME/opt/rabbitmq-data/mnesia}"
export RABBITMQ_LOG_BASE="${RABBITMQ_LOG_BASE:-$HOME/opt/rabbitmq-data/log}"
export RABBITMQ_ENABLED_PLUGINS_FILE="${RABBITMQ_ENABLED_PLUGINS_FILE:-$HOME/opt/rabbitmq-data/enabled_plugins}"
export RABBITMQ_CONFIG_FILE="${RABBITMQ_CONFIG_FILE:-$HOME/opt/rabbitmq-data/rabbitmq}"

mkdir -p "$RABBITMQ_MNESIA_BASE" "$RABBITMQ_LOG_BASE"

cmd=${1:-status}

if [ ! -x "$RABBITMQ_HOME/sbin/rabbitmq-server" ]; then
  echo "RabbitMQ nao encontrado em $RABBITMQ_HOME" >&2
  echo "rode: ./scripts/instalar_deps_vm.sh" >&2
  exit 1
fi

is_up() {
  rabbitmq-diagnostics -q ping >/dev/null 2>&1
}

case "$cmd" in
  start)
    if is_up; then
      echo "RabbitMQ ja esta no ar (5672)"
      exit 0
    fi
    echo "iniciando RabbitMQ (usuario)..."
    rabbitmq-server -detached
    i=0
    while [ "$i" -lt 60 ]; do
      if is_up; then
        echo "RabbitMQ ok"
        exit 0
      fi
      i=$((i + 1))
      sleep 1
    done
    echo "timeout ao subir RabbitMQ; logs em $RABBITMQ_LOG_BASE" >&2
    exit 1
    ;;
  stop)
    if is_up; then
      rabbitmqctl stop >/dev/null 2>&1 || true
    fi
    echo "RabbitMQ parado"
    ;;
  status)
    if is_up; then
      echo "RabbitMQ: UP"
      rabbitmqctl status 2>/dev/null | head -20 || true
    else
      echo "RabbitMQ: DOWN"
      exit 1
    fi
    ;;
  *)
    echo "uso: $0 start|stop|status" >&2
    exit 2
    ;;
esac

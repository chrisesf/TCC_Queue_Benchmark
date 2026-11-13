#!/bin/sh
# Instala tudo no $HOME, sem root/sudo:
#   Rust (rustup), JDK, Kafka (KRaft), Erlang + RabbitMQ (generic-unix).
#
# Uso:
#   chmod +x scripts/*.sh
#   ./scripts/instalar_deps_vm.sh
#
# Precisa de gcc/g++/make/cmake/pkg-config/zlib no sistema (geralmente ja
# existem em VMs academicas). Sem isso o cliente Kafka (librdkafka) nao compila.

set -eu

# VMs academicas muitas vezes nao tem locale gerado; sem isso o JDK falha com
# "platform encoding not initialized".
export LANG="${LANG:-C.UTF-8}"
export LC_ALL="${LC_ALL:-C.UTF-8}"

OPT=${OPT:-$HOME/opt}
KAFKA_VERSION=4.3.1
# RabbitMQ 4.x pede Erlang 26+.
ERLANG_VERSION=26.2.5
RABBIT_VERSION=4.0.5
JDK_VERSION=21.0.6+7
JDK_URL_VERSION=21.0.6%2B7

mkdir -p "$OPT" "$HOME/bin"
PATH_SNIP="$OPT/jdk/bin:$OPT/erlang/bin:$OPT/rabbitmq/sbin:$HOME/kafka/bin:$HOME/.cargo/bin"

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "comando obrigatorio ausente: $1" >&2
    echo "peca ao admin para instalar as ferramentas de build (gcc, cmake, pkg-config, zlib)." >&2
    exit 1
  fi
}

echo "=== checagem de ferramentas de build (sem instalar) ==="
need_cmd curl
need_cmd tar
need_cmd gcc
need_cmd g++
need_cmd make
need_cmd cmake
need_cmd pkg-config
# zlib headers: tenta compilar um include minimo
if ! echo '#include <zlib.h>' | gcc -E - >/dev/null 2>&1; then
  echo "libz (zlib.h) ausente. peca zlib1g-dev ao admin." >&2
  exit 1
fi
echo "ok: gcc=$(gcc -dumpversion) cmake=$(cmake --version | head -1)"

echo "=== Rust (rustup, usuario) ==="
if [ ! -x "$HOME/.cargo/bin/cargo" ]; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
fi
# shellcheck disable=SC1091
. "$HOME/.cargo/env"
rustc --version
cargo --version

echo "=== JDK Temurin $JDK_VERSION ==="
if [ ! -x "$OPT/jdk/bin/java" ]; then
  TMP=$(mktemp -d)
  cd "$TMP"
  curl -fsSL -o jdk.tgz \
    "https://github.com/adoptium/temurin21-binaries/releases/download/jdk-${JDK_URL_VERSION}/OpenJDK21U-jdk_x64_linux_hotspot_$(echo "$JDK_VERSION" | tr '+' '_').tar.gz"
  tar -xzf jdk.tgz
  rm -rf "$OPT/jdk"
  mv jdk-21* "$OPT/jdk"
  rm -rf "$TMP"
fi
if ! "$OPT/jdk/bin/java" -version; then
  echo "JDK falhou ao iniciar. Tentando com LANG=C..." >&2
  LANG=C LC_ALL=C "$OPT/jdk/bin/java" -version
fi

echo "=== Erlang OTP $ERLANG_VERSION (build Hex, usuario) ==="
if [ ! -x "$OPT/erlang/Install" ] && [ ! -x "$OPT/erlang/bin/erl" ]; then
  TMP=$(mktemp -d)
  cd "$TMP"
  # Builds oficiais usados pelo Hex/Elixir CI (Ubuntu 20.04/22.04 x86_64).
  for dist in ubuntu-22.04 ubuntu-20.04; do
    if curl -fsSL -o otp.tgz \
      "https://repo.hex.pm/builds/otp/${dist}/OTP-${ERLANG_VERSION}.tar.gz"
    then
      break
    fi
  done
  if [ ! -f otp.tgz ]; then
    echo "falha ao baixar Erlang OTP-${ERLANG_VERSION}" >&2
    exit 1
  fi
  rm -rf "$OPT/erlang"
  mkdir -p "$OPT/erlang"
  tar -xzf otp.tgz -C "$OPT/erlang"
  # O tarball do Hex vem com script Install; gera bin/erl no destino.
  if [ -x "$OPT/erlang/Install" ]; then
    (cd "$OPT/erlang" && ./Install -sasl "$OPT/erlang")
  fi
  rm -rf "$TMP"
fi
export PATH="$OPT/erlang/bin:$PATH"
if [ ! -x "$OPT/erlang/bin/erl" ]; then
  echo "Erlang instalado, mas bin/erl nao encontrado em $OPT/erlang" >&2
  ls -la "$OPT/erlang" >&2 || true
  exit 1
fi
erl -eval 'erlang:display(erlang:system_info(otp_release)), halt().' -noshell

echo "=== RabbitMQ $RABBIT_VERSION (generic-unix) ==="
if [ ! -x "$OPT/rabbitmq/sbin/rabbitmq-server" ]; then
  TMP=$(mktemp -d)
  cd "$TMP"
  curl -fsSL -o rabbit.txz \
    "https://github.com/rabbitmq/rabbitmq-server/releases/download/v${RABBIT_VERSION}/rabbitmq-server-generic-unix-${RABBIT_VERSION}.tar.xz"
  tar -xJf rabbit.txz
  rm -rf "$OPT/rabbitmq"
  mv rabbitmq_server-* "$OPT/rabbitmq"
  rm -rf "$TMP"
fi

echo "=== Kafka $KAFKA_VERSION ==="
KAFKA_HOME=$HOME/kafka
if [ ! -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]; then
  TMP=$(mktemp -d)
  cd "$TMP"
  curl -fsSL -o kafka.tgz \
    "https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz" \
    || curl -fsSL -o kafka.tgz \
    "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz"
  tar -xzf kafka.tgz
  rm -rf "$KAFKA_HOME"
  mv kafka_2.13-* "$KAFKA_HOME"
  rm -rf "$TMP"
fi
mkdir -p "$KAFKA_HOME/data" "$KAFKA_HOME/logs"
cat > "$KAFKA_HOME/config/kraft/tcc.properties" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
controller.listener.names=CONTROLLER
log.dirs=$KAFKA_HOME/data
num.network.threads=8
num.io.threads=8
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=false
EOF

# PATH persistente
PROFILE=$HOME/.bashrc
MARKER="# tcc-bench PATH"
if ! grep -Fq "$MARKER" "$PROFILE" 2>/dev/null; then
  {
    echo ""
    echo "$MARKER"
    echo "export JAVA_HOME=\"$OPT/jdk\""
    echo "export PATH=\"$PATH_SNIP:\$PATH\""
  } >> "$PROFILE"
fi

# env file carregado pelos scripts da campanha
cat > "$HOME/.tcc-bench-env" <<EOF
export LANG="\${LANG:-C.UTF-8}"
export LC_ALL="\${LC_ALL:-C.UTF-8}"
export JAVA_HOME="$OPT/jdk"
export PATH="$PATH_SNIP:\$PATH"
export KAFKA_HOME="$KAFKA_HOME"
export RABBITMQ_HOME="$OPT/rabbitmq"
export ERLANG_HOME="$OPT/erlang"
EOF

cat <<EOF

Instalacao concluida (tudo em $HOME, sem sudo).

Carregue o ambiente:
  . "\$HOME/.tcc-bench-env"

Depois:
  cd ~/TCC_Queue_Benchmark
  ./scripts/rodar_vm.sh
EOF

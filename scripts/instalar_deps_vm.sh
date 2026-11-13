#!/bin/sh
# Instala Rust, dependencias de build, RabbitMQ e Kafka na VM (sem Docker).
# Uso: sudo ./scripts/instalar_deps_vm.sh
#      (o cargo/rustup fica no usuario que chamar rustup-init; rode a parte
#       do Rust sem sudo se preferir — veja o final do script.)

set -eu

if [ "$(id -u)" -ne 0 ]; then
  echo "rode com sudo: sudo $0" >&2
  exit 1
fi

REAL_USER=${SUDO_USER:-$USER}
REAL_HOME=$(getent passwd "$REAL_USER" | cut -d: -f6)
KAFKA_VERSION=4.3.1
KAFKA_DIR=$REAL_HOME/kafka

echo "=== pacotes do sistema ==="
apt-get update
apt-get install -y --no-install-recommends \
  build-essential pkg-config cmake perl python3 zlib1g-dev \
  curl ca-certificates git procps htop \
  openjdk-21-jre-headless \
  rabbitmq-server

echo "=== RabbitMQ ==="
systemctl enable --now rabbitmq-server
rabbitmqctl await_startup || true
rabbitmqctl status >/dev/null
echo "RabbitMQ ok em amqp://127.0.0.1:5672"

echo "=== Kafka $KAFKA_VERSION (KRaft, broker unico) ==="
install -d -o "$REAL_USER" -g "$REAL_USER" "$KAFKA_DIR"
TMP=$(mktemp -d)
cd "$TMP"
curl -fsSL -o kafka.tgz \
  "https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz" \
  || curl -fsSL -o kafka.tgz \
  "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz"
tar -xzf kafka.tgz
# conteudo em kafka_2.13-VERSION/
SRC=$(echo kafka_2.13-*)
# copia preservando dono
rm -rf "$KAFKA_DIR"/*
cp -a "$SRC"/. "$KAFKA_DIR"/
chown -R "$REAL_USER:$REAL_USER" "$KAFKA_DIR"
rm -rf "$TMP"

# Config KRaft single-node (listener so em localhost).
cat > "$KAFKA_DIR/config/kraft/tcc.properties" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
controller.listener.names=CONTROLLER
log.dirs=$KAFKA_DIR/data
num.network.threads=8
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=false
EOF
chown "$REAL_USER:$REAL_USER" "$KAFKA_DIR/config/kraft/tcc.properties"
install -d -o "$REAL_USER" -g "$REAL_USER" "$KAFKA_DIR/data" "$KAFKA_DIR/logs"

echo "=== Rust (usuario $REAL_USER) ==="
if [ ! -x "$REAL_HOME/.cargo/bin/cargo" ]; then
  sudo -u "$REAL_USER" -H bash -lc \
    'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable'
fi
sudo -u "$REAL_USER" -H bash -lc 'rustc --version; cargo --version'

cat <<EOF

Dependencias instaladas.
Proximos passos (como $REAL_USER):

  cd ~/TCC_Queue_Benchmark   # ou onde clonou o repo
  chmod +x scripts/*.sh
  ./scripts/rodar_vm.sh

Kafka fica em $KAFKA_DIR (o rodar_vm.sh sobe e desce sozinho).
RabbitMQ ja esta como servico (systemctl status rabbitmq-server).
EOF

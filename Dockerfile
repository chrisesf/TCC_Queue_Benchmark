FROM rust:1.99-slim-bookworm

RUN rustup component add clippy

# cmake/perl/python3/zlib: compilacao do librdkafka (cliente Kafka, feature `kafka`).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        pkg-config \
        cmake \
        perl \
        python3 \
        zlib1g-dev \
        procps \
        htop \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV CARGO_TARGET_DIR=/build

CMD ["bash"]

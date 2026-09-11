FROM rust:slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        pkg-config \
        procps \
        htop \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV CARGO_TARGET_DIR=/build

CMD ["bash"]

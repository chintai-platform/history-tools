FROM ubuntu:20.04 as base

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y \
        autoconf2.13 \
        build-essential \
        cmake \
        curl \
        wget \
        git \
        libboost-all-dev \
        libcurl4-openssl-dev \
        libgmp-dev \
        libpq-dev \
        libpqxx-dev \
        libssl-dev \
        ninja-build \
        openssl \
        python3-pkgconfig && \
    apt-get clean && \

    rm -rf /var/cache/apt/lists/*

FROM base as builder
WORKDIR /root
RUN mkdir /root/history-tools
COPY cmake /root/history-tools/cmake
COPY CMakeLists.txt /root/history-tools
COPY libraries /root/history-tools/libraries
COPY src /root/history-tools/src
COPY unittests /root/history-tools/unittests

RUN mkdir /root/history-tools/build
WORKDIR /root/history-tools/build
RUN cmake -GNinja -DCMAKE_BUILD_TYPE=Release -DPostgreSQL_TYPE_INCLUDE_DIR=/usr/include/postgresql .. && \
    ninja && \
    ctest --output-on-failure


FROM ubuntu:20.04
RUN apt-get update && \
    apt-get install -y \
        netcat \
        openssl \
        postgresql-client && \
        wget && \
    wget https://github.com/EOSIO/eos/releases/download/v2.1.0/eosio_2.1.0-1-ubuntu-20.04_amd64.deb && \

    apt install -y ./eosio_2.1.0-1-ubuntu-20.04_amd64.deb && \
    apt-get clean && \
    rm -rf /var/cache/apt/lists/*

COPY --from=builder /root/history-tools/build/fill-pg /usr/local/bin/fill-pg

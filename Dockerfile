FROM rust:1-bookworm AS debian

ARG GIT_CONFIG_PARAMETERS=""
ARG CARGO_HOME=/usr/src/.cargo/
ARG CARGO_BUILD_FLAGS=""

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        cmake \
        clang \
        libclang-dev \
        patch && \
    rm -rf /var/lib/apt/lists/*;

WORKDIR /usr/src

COPY . .

RUN --mount=type=cache,target=/usr/src/.cargo/registry \
    --mount=type=cache,target=/usr/src/target \
    cargo build ${CARGO_BUILD_FLAGS};

FROM rust:1-alpine3.22 AS alpine

ARG GIT_CONFIG_PARAMETERS=""
ARG CARGO_HOME=/usr/src/.cargo/
ARG CARGO_BUILD_FLAGS=""

RUN apk add --no-cache --update \
        build-base \
        clang-static \
        clang-dev \
        cmake \
        openssl-dev \
        patch \
        pkgconf;

WORKDIR /usr/src

COPY . .

ENV LIBCLANG_PATH=/usr/lib
ENV BINDGEN_EXTRA_CLANG_ARGS="-I/usr/include"
ENV RUSTFLAGS="-C target-feature=-crt-static"

RUN --mount=type=cache,target=/usr/src/.cargo/registry \
    --mount=type=cache,target=/usr/src/target \
    cargo build ${CARGO_BUILD_FLAGS};
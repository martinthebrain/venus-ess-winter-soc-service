#!/bin/sh
set -eu

ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
CARGO_CACHE=${CARGO_CACHE:-/tmp/winter-soc-rust-cargo}
TARGET_CACHE=${TARGET_CACHE:-/tmp/winter-soc-rust-arm-target}
TARGET=armv7-unknown-linux-gnueabihf
RUST_IMAGE=${RUST_IMAGE:-rust:1.88-bookworm@sha256:af306cfa71d987911a781c37b59d7d67d934f49684058f96cf72079c3626bfe0}

mkdir -p "$CARGO_CACHE" "$TARGET_CACHE" "$ROOT/bin"

docker run --rm \
    -e CARGO_HOME=/tmp/cargo \
    -e CARGO_TARGET_DIR=/tmp/target \
    -v "$CARGO_CACHE:/tmp/cargo" \
    -v "$TARGET_CACHE:/tmp/target" \
    -v "$ROOT:/work:ro" \
    -w /work \
    "$RUST_IMAGE" \
    sh -c 'apt-get update -qq &&
        apt-get install -y -qq gcc-arm-linux-gnueabihf >/dev/null &&
        rustup target add armv7-unknown-linux-gnueabihf >/dev/null &&
        CARGO_TARGET_ARMV7_UNKNOWN_LINUX_GNUEABIHF_LINKER=arm-linux-gnueabihf-gcc \
        cargo build --release --locked --target armv7-unknown-linux-gnueabihf'

install -m 0755 \
    "$TARGET_CACHE/$TARGET/release/venus-ess-winter-soc-service" \
    "$ROOT/bin/venus-ess-winter-soc-service"

echo "ARMv7 binary: $ROOT/bin/venus-ess-winter-soc-service"

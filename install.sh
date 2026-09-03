#!/bin/sh
# Install from a checkout, or bootstrap the immutable bundle of this release.

set -eu

REPOSITORY=martinthebrain/venus-ess-winter-soc-service
SOURCE_REF='@RELEASE_TAG@'
BUNDLE_NAME=venus-ess-winter-soc-service-armv7.tar.gz
CHECKSUMS_NAME=SHA256SUMS
SCRIPT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)

die() {
    echo "venus-ess-winter-soc-service: $*" >&2
    exit 1
}

install_checkout() {
    export ESS_WINTER_RUST_ROOT=${ESS_WINTER_RUST_ROOT:-$SCRIPT_DIR}
    export ESS_WINTER_INSTALL_MODE=${ESS_WINTER_INSTALL_MODE:-activate}
    exec "$SCRIPT_DIR/deploy/venus/install.sh"
}

download() {
    url=$1
    destination=$2
    if command -v curl >/dev/null 2>&1; then
        curl --fail --location --silent --show-error --output "$destination" "$url"
    elif command -v wget >/dev/null 2>&1; then
        wget -q -O "$destination" "$url"
    else
        die "curl or wget is required"
    fi
}

preserve_local_state() {
    old_root=$1
    new_root=$2
    for name in config.env gui-restore-state.json; do
        if [ -f "$old_root/$name" ]; then
            cp -p "$old_root/$name" "$new_root/$name"
        fi
    done
}

install_release() {
    [ "$(id -u)" = "0" ] || die "installer must run as root"
    case "$SOURCE_REF" in
        *'@'*) die "standalone installation requires install.sh from a published release" ;;
    esac

    target=${ESS_WINTER_RUST_ROOT:-/data/venus-ess-winter-soc-service-rust}
    [ ! -L "$target" ] || die "refusing symlink deployment root: $target"
    [ ! -e "$target" ] || [ -d "$target" ] ||
        die "deployment root exists but is not a directory: $target"
    parent=$(dirname -- "$target")
    mkdir -p "$parent"

    work=$(mktemp -d "${TMPDIR:-/tmp}/venus-ess-winter-soc.XXXXXX") ||
        die "could not create temporary workspace"
    next="${target}.next.$$"
    previous="${target}.previous.$$"
    cleanup() {
        rm -rf "$work" "$next"
    }
    trap cleanup EXIT INT TERM HUP

    release_base=${ESS_RELEASE_BASE_URL:-https://github.com/$REPOSITORY/releases/download/$SOURCE_REF}
    download "$release_base/$BUNDLE_NAME" "$work/$BUNDLE_NAME"
    download "$release_base/$CHECKSUMS_NAME" "$work/$CHECKSUMS_NAME"

    expected=$(awk -v name="$BUNDLE_NAME" '$2 == name { print $1; exit }' "$work/$CHECKSUMS_NAME")
    printf '%s\n' "$expected" | grep -Eq '^[0-9a-fA-F]{64}$' ||
        die "release checksum is missing or malformed"
    actual=$(sha256sum "$work/$BUNDLE_NAME" | awk '{ print $1 }')
    [ "$actual" = "$expected" ] || die "release bundle checksum mismatch"

    tar -tzf "$work/$BUNDLE_NAME" >"$work/manifest"
    if grep -Eq '(^/|(^|/)\.\.(/|$))' "$work/manifest"; then
        die "release bundle contains an unsafe path"
    fi

    rm -rf "$next" "$previous"
    mkdir -p "$next"
    tar -xzf "$work/$BUNDLE_NAME" -C "$next"
    [ -x "$next/bin/venus-ess-winter-soc-service" ] ||
        die "release bundle has no executable ARMv7 service"
    [ -x "$next/deploy/venus/install.sh" ] ||
        die "release bundle has no deployment installer"
    if [ -d "$target" ]; then
        preserve_local_state "$target" "$next"
        mv "$target" "$previous"
    fi
    mv "$next" "$target"

    if ! ESS_WINTER_RUST_ROOT="$target" ESS_WINTER_INSTALL_MODE=activate \
        "$target/install.sh"; then
        rm -rf "$target"
        if [ -d "$previous" ]; then
            mv "$previous" "$target"
            ESS_WINTER_RUST_ROOT="$target" ESS_WINTER_INSTALL_MODE=activate \
                "$target/install.sh" || true
        fi
        die "release activation failed; previous deployment restored"
    fi

    rm -rf "$previous"
    echo "Installed immutable release $SOURCE_REF."
}

if [ -x "$SCRIPT_DIR/deploy/venus/install.sh" ]; then
    install_checkout
fi
install_release

#!/bin/sh
# Build the immutable assets consumed by the standalone release installer.

set -eu

[ "$#" = "2" ] || {
    echo "usage: $0 RELEASE_TAG OUTPUT_DIRECTORY" >&2
    exit 2
}

release_tag=$1
output=$2
case "$release_tag" in
    '' | *[!A-Za-z0-9._-]*)
        echo "invalid release tag: $release_tag" >&2
        exit 2
        ;;
esac

root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
bundle=venus-ess-winter-soc-service-armv7.tar.gz
binary=venus-ess-winter-soc-service

mkdir -p "$output"
output=$(CDPATH='' cd -- "$output" && pwd)

tar -czf "$output/$bundle" -C "$root" \
    LICENSE README.md CONTRACTS.md config.env.example install.sh uninstall.sh \
    bin deploy service service-shadow

sed "s/@RELEASE_TAG@/$release_tag/g" "$root/install.sh" >"$output/install.sh"
chmod 755 "$output/install.sh"
cp "$root/bin/$binary" "$output/$binary"

grep -F "SOURCE_REF='$release_tag'" "$output/install.sh" >/dev/null
if grep -F '@RELEASE_TAG@' "$output/install.sh" >/dev/null; then
    echo "release installer still contains the tag placeholder" >&2
    exit 1
fi

(
    cd "$output"
    sha256sum install.sh "$bundle" "$binary" >SHA256SUMS
    sha256sum -c SHA256SUMS
)

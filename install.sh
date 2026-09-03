#!/bin/sh
# Install and activate the native Winter SoC service from this checkout.

set -eu

SCRIPT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
export ESS_WINTER_RUST_ROOT=${ESS_WINTER_RUST_ROOT:-$SCRIPT_DIR}
export ESS_WINTER_INSTALL_MODE=${ESS_WINTER_INSTALL_MODE:-activate}

exec "$SCRIPT_DIR/deploy/venus/install.sh"

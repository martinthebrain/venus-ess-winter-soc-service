#!/bin/sh
# Restore owned settings and remove the native Winter SoC service integration.

set -eu

SCRIPT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
export ESS_WINTER_RUST_ROOT=${ESS_WINTER_RUST_ROOT:-$SCRIPT_DIR}
export ESS_WINTER_INSTALL_MODE=uninstall

exec "$SCRIPT_DIR/deploy/venus/install.sh"

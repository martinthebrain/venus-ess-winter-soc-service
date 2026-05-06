#!/bin/sh
# Exercise installer update/uninstall paths in a temporary sandbox.

set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}/venus-ess-winter-soc-service-install-test.$$"
DATA_ROOT="${TMP_ROOT}/data"
SERVICE_ROOT="${TMP_ROOT}/service"
RC_LOCAL="${DATA_ROOT}/rc.local"

cleanup() {
    rm -rf "${TMP_ROOT}"
}

trap cleanup EXIT INT TERM

run_install() {
    ESS_INSTALL_DATA_ROOT="${DATA_ROOT}" \
    ESS_SERVICE_ROOT="${SERVICE_ROOT}" \
    ESS_RC_LOCAL="${RC_LOCAL}" \
    ESS_START_SERVICE=0 \
    ESS_ALLOW_NON_ROOT=1 \
    sh "${ROOT}/install.sh"
}

run_uninstall() {
    ESS_INSTALL_DATA_ROOT="${DATA_ROOT}" \
    ESS_SERVICE_ROOT="${SERVICE_ROOT}" \
    ESS_RC_LOCAL="${RC_LOCAL}" \
    sh "${ROOT}/uninstall.sh"
}

assert_exists() {
    [ -e "$1" ] || {
        echo "FAIL: missing $1" >&2
        exit 1
    }
}

assert_not_exists() {
    [ ! -e "$1" ] || {
        echo "FAIL: should not exist: $1" >&2
        exit 1
    }
}

assert_grep() {
    grep -F "$1" "$2" >/dev/null 2>&1 || {
        echo "FAIL: '$1' not found in $2" >&2
        exit 1
    }
}

main() {
    mkdir -p "${DATA_ROOT}" "${SERVICE_ROOT}"
    run_install
    install_dir="${DATA_ROOT}/etc/venus-ess-winter-soc-service"
    service_link="${SERVICE_ROOT}/venus-ess-winter-soc-service"
    assert_exists "${install_dir}/socSteuerung.py"
    assert_exists "${install_dir}/venus_ess_winter_soc_service/paths.py"
    assert_exists "${install_dir}/scripts/live_dbus_core.py"
    assert_exists "${service_link}"
    assert_grep "# venus-ess-winter-soc-service start" "${RC_LOCAL}"

    echo "corrupt" > "${install_dir}/socSteuerung.py"
    run_install
    assert_grep "Compatibility wrapper" "${install_dir}/socSteuerung.py"

    run_uninstall
    assert_not_exists "${install_dir}"
    assert_not_exists "${service_link}"
    if [ -f "${RC_LOCAL}" ]; then
        ! grep -F "# venus-ess-winter-soc-service start" "${RC_LOCAL}" >/dev/null 2>&1 || {
            echo "FAIL: rc.local block still present" >&2
            exit 1
        }
    fi

    echo "Installer self-test passed."
}

main "$@"

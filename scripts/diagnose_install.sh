#!/bin/sh
# Print a compact installation and runtime diagnosis for Venus OS.

set -u

SERVICE_NAME="venus-ess-winter-soc-service"
DATA_ROOT="${ESS_INSTALL_DATA_ROOT:-/data}"
SERVICE_ROOT="${ESS_SERVICE_ROOT:-/service}"
INSTALL_DIR="${ESS_INSTALL_DIR:-${DATA_ROOT}/etc/${SERVICE_NAME}}"
SERVICE_LINK="${ESS_SERVICE_LINK:-${SERVICE_ROOT}/${SERVICE_NAME}}"
RC_LOCAL="${ESS_RC_LOCAL:-${DATA_ROOT}/rc.local}"
LOG_FILE="/dev/shm/ess_winter_log.txt"
STATE_FILE="/dev/shm/ess_winter_logic.json"

ok() {
    echo "OK: $*"
}

warn() {
    echo "WARN: $*"
}

fail() {
    echo "FAIL: $*"
}

have_cmd() {
    command -v "$1" >/dev/null 2>&1
}

check_path() {
    path="$1"
    if [ -e "${path}" ]; then
        ok "${path} exists"
    else
        fail "${path} missing"
    fi
}

check_executable() {
    path="$1"
    if [ -x "${path}" ]; then
        ok "${path} executable"
    else
        fail "${path} not executable"
    fi
}

check_service() {
    if [ -L "${SERVICE_LINK}" ]; then
        ok "${SERVICE_LINK} -> $(readlink "${SERVICE_LINK}")"
    else
        fail "${SERVICE_LINK} is not a symlink"
    fi
    if have_cmd svstat && [ -e "${SERVICE_LINK}" ]; then
        svstat "${SERVICE_LINK}" || true
    else
        warn "svstat unavailable or service link missing"
    fi
}

check_python_import() {
    if python3 -c "import sys; sys.path.insert(0, '${INSTALL_DIR}'); import venus_ess_winter_soc_service; print('OK: package import works')" 2>/tmp/${SERVICE_NAME}.diag.err; then
        rm -f "/tmp/${SERVICE_NAME}.diag.err"
    else
        fail "package import failed"
        cat "/tmp/${SERVICE_NAME}.diag.err"
        rm -f "/tmp/${SERVICE_NAME}.diag.err"
    fi
}

check_rc_local() {
    if [ -f "${RC_LOCAL}" ] && grep -F "# ${SERVICE_NAME} start" "${RC_LOCAL}" >/dev/null 2>&1; then
        ok "${RC_LOCAL} contains service block"
    else
        fail "${RC_LOCAL} does not contain service block"
    fi
}

check_dbus_probe() {
    if ! have_cmd dbus; then
        warn "dbus command unavailable"
        return
    fi
    echo "D-Bus probes:"
    dbus -y com.victronenergy.settings /Settings/CGwacs/BatteryLife/MinimumSocLimit GetValue 2>/dev/null || warn "MinimumSocLimit probe failed"
    dbus -y com.victronenergy.settings /Settings/SystemSetup/MaxChargeCurrent GetValue 2>/dev/null || warn "MaxChargeCurrent probe failed"
    dbus -y com.victronenergy.system /Dc/Battery/Soc GetValue 2>/dev/null || warn "Battery SoC probe failed"
}

check_logs() {
    if [ -f "${LOG_FILE}" ]; then
        ok "${LOG_FILE} exists"
        tail -n 20 "${LOG_FILE}"
    else
        warn "${LOG_FILE} not present yet"
    fi
    if [ -f "${STATE_FILE}" ]; then
        ok "${STATE_FILE} exists"
    else
        warn "${STATE_FILE} not present yet"
    fi
}

main() {
    check_path "${INSTALL_DIR}"
    check_executable "${INSTALL_DIR}/socSteuerung.py"
    check_executable "${INSTALL_DIR}/service/run"
    check_executable "${INSTALL_DIR}/uninstall.sh"
    check_executable "${INSTALL_DIR}/scripts/dbus_scenario_simulator.py"
    check_executable "${INSTALL_DIR}/scripts/live_dbus_testbed.py"
    check_service
    check_rc_local
    check_python_import
    check_dbus_probe
    check_logs
}

main "$@"

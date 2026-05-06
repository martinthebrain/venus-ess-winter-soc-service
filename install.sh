#!/bin/sh
# Install the Venus ESS winter SoC controller on Victron Venus OS.
#
# The script can be run from a complete checkout or as a single downloaded file.
# Missing repository files are fetched with wget because Venus OS does not ship
# with git by default.

set -eu

SERVICE_NAME="venus-ess-winter-soc-service"
DATA_ROOT="${ESS_INSTALL_DATA_ROOT:-/data}"
SERVICE_ROOT="${ESS_SERVICE_ROOT:-/service}"
INSTALL_DIR="${ESS_INSTALL_DIR:-${DATA_ROOT}/etc/${SERVICE_NAME}}"
SERVICE_DIR="${INSTALL_DIR}/service"
SERVICE_LINK="${ESS_SERVICE_LINK:-${SERVICE_ROOT}/${SERVICE_NAME}}"
RC_LOCAL="${ESS_RC_LOCAL:-${DATA_ROOT}/rc.local}"
RAW_BASE_URL="${ESS_RAW_BASE_URL:-https://raw.githubusercontent.com/martinthebrain/venus-ess-winter-soc-service/main}"
START_SERVICE="${ESS_START_SERVICE:-1}"
ALLOW_NON_ROOT="${ESS_ALLOW_NON_ROOT:-0}"
RC_START="# ${SERVICE_NAME} start"
RC_END="# ${SERVICE_NAME} end"
PACKAGE_FILES="
venus_ess_winter_soc_service/__init__.py
venus_ess_winter_soc_service/base.py
venus_ess_winter_soc_service/config.py
venus_ess_winter_soc_service/controller.py
venus_ess_winter_soc_service/dbus_iface.py
venus_ess_winter_soc_service/dvcc.py
venus_ess_winter_soc_service/paths.py
venus_ess_winter_soc_service/persistence.py
venus_ess_winter_soc_service/power.py
venus_ess_winter_soc_service/runtime.py
venus_ess_winter_soc_service/socpolicy.py
venus_ess_winter_soc_service/storage.py
venus_ess_winter_soc_service/tracking.py
venus_ess_winter_soc_service/windows.py
"
SCRIPT_FILES="
scripts/dbus_scenario_simulator.py
scripts/dbus_sim_core.py
scripts/dbus_sim_harness.py
scripts/dbus_sim_scenarios.py
scripts/live_dbus_testbed.py
scripts/live_dbus_core.py
scripts/live_dbus_harness.py
scripts/live_dbus_scenarios.py
scripts/diagnose_install.sh
scripts/install_selftest.sh
"

script_dir() {
    case "$0" in
        */*) cd "$(dirname "$0")" && pwd ;;
        *) pwd ;;
    esac
}

SCRIPT_DIR="$(script_dir)"

die() {
    echo "ERROR: $*" >&2
    exit 1
}

have_cmd() {
    command -v "$1" >/dev/null 2>&1
}

require_root() {
    if [ "${ALLOW_NON_ROOT}" = "1" ]; then
        return
    fi
    if have_cmd id && [ "$(id -u)" != "0" ]; then
        die "Please run this installer as root on Venus OS."
    fi
}

download_file() {
    rel_path="$1"
    dst_path="$2"
    url="${RAW_BASE_URL}/${rel_path}"
    have_cmd wget || die "wget is required to fetch missing files from ${RAW_BASE_URL}"
    echo "Downloading ${url}"
    wget -O "${dst_path}" "${url}" || die "Could not download ${url}"
}

ensure_source_file() {
    rel_path="$1"
    src_path="${SCRIPT_DIR}/${rel_path}"
    if [ -f "${src_path}" ]; then
        return
    fi
    mkdir -p "$(dirname "${src_path}")"
    download_file "${rel_path}" "${src_path}"
}

install_file() {
    rel_path="$1"
    mode="$2"
    src_path="${SCRIPT_DIR}/${rel_path}"
    dst_path="${INSTALL_DIR}/${rel_path}"
    mkdir -p "$(dirname "${dst_path}")"
    cp "${src_path}" "${dst_path}"
    chmod "${mode}" "${dst_path}"
}

install_files() {
    ensure_source_file "socSteuerung.py"
    for rel_path in ${PACKAGE_FILES}; do
        ensure_source_file "${rel_path}"
    done
    ensure_source_file "service/run"
    for rel_path in ${SCRIPT_FILES}; do
        ensure_source_file "${rel_path}"
    done
    ensure_source_file "uninstall.sh"
    mkdir -p "${INSTALL_DIR}"
    install_file "socSteuerung.py" 755
    for rel_path in ${PACKAGE_FILES}; do
        install_file "${rel_path}" 644
    done
    install_file "service/run" 755
    for rel_path in ${SCRIPT_FILES}; do
        install_file "${rel_path}" 755
    done
    install_file "uninstall.sh" 755
    install_file "install.sh" 755
}

install_service_link() {
    mkdir -p "${SERVICE_ROOT}"
    if [ -e "${SERVICE_LINK}" ] && [ ! -L "${SERVICE_LINK}" ]; then
        die "${SERVICE_LINK} exists and is not a symlink."
    fi
    rm -f "${SERVICE_LINK}"
    ln -s "${SERVICE_DIR}" "${SERVICE_LINK}"
}

ensure_rc_local_exists() {
    if [ ! -f "${RC_LOCAL}" ]; then
        printf '%s\n\n' '#!/bin/sh' > "${RC_LOCAL}"
    fi
    chmod 755 "${RC_LOCAL}"
}

rc_local_has_entry() {
    grep -F "${RC_START}" "${RC_LOCAL}" >/dev/null 2>&1
}

append_rc_local_entry() {
    tmp_file="${RC_LOCAL}.tmp.$$"
    {
        echo "${RC_START}"
        echo "mkdir -p '${SERVICE_ROOT}'"
        echo "[ -L '${SERVICE_LINK}' ] || ln -s '${SERVICE_DIR}' '${SERVICE_LINK}'"
        echo "${RC_END}"
    } > "${tmp_file}.block"
    awk -v block_file="${tmp_file}.block" '
        $0 == "exit 0" && inserted != 1 {
            while ((getline line < block_file) > 0) {
                print line
            }
            close(block_file)
            print ""
            inserted = 1
        }
        { print }
        END {
            if (inserted != 1) {
                print ""
                while ((getline line < block_file) > 0) {
                    print line
                }
                close(block_file)
            }
        }
    ' "${RC_LOCAL}" > "${tmp_file}"
    mv "${tmp_file}" "${RC_LOCAL}"
    rm -f "${tmp_file}.block"
    chmod 755 "${RC_LOCAL}"
}

install_rc_local_entry() {
    ensure_rc_local_exists
    if rc_local_has_entry; then
        echo "${RC_LOCAL} already contains ${SERVICE_NAME} entry"
        return
    fi
    append_rc_local_entry
}

start_service_if_possible() {
    if [ "${START_SERVICE}" != "1" ]; then
        return
    fi
    if have_cmd svc; then
        svc -u "${SERVICE_LINK}" >/dev/null 2>&1 || true
    fi
}

main() {
    require_root
    install_files
    install_service_link
    install_rc_local_entry
    start_service_if_possible
    echo ""
    echo "${SERVICE_NAME} installed."
    echo "Installed files: ${INSTALL_DIR}"
    echo "Service link: ${SERVICE_LINK}"
    echo "RAM log: /dev/shm/ess_winter_log.txt"
    echo "Uninstall with: ${INSTALL_DIR}/uninstall.sh"
}

main "$@"

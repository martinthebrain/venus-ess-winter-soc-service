#!/bin/sh
# Remove the Venus ESS winter SoC controller service from Venus OS.

set -eu

SERVICE_NAME="venus-ess-winter-soc-service"
INSTALL_DIR="/data/etc/${SERVICE_NAME}"
SERVICE_LINK="/service/${SERVICE_NAME}"
RC_LOCAL="/data/rc.local"
RC_START="# ${SERVICE_NAME} start"
RC_END="# ${SERVICE_NAME} end"

have_cmd() {
    command -v "$1" >/dev/null 2>&1
}

stop_service_if_possible() {
    if have_cmd svc && [ -e "${SERVICE_LINK}" ]; then
        svc -d "${SERVICE_LINK}" >/dev/null 2>&1 || true
    fi
}

remove_service_link() {
    if [ -L "${SERVICE_LINK}" ]; then
        rm -f "${SERVICE_LINK}"
    fi
}

remove_rc_local_entry() {
    if [ ! -f "${RC_LOCAL}" ]; then
        return
    fi
    tmp_file="${RC_LOCAL}.tmp.$$"
    awk -v start="${RC_START}" -v end="${RC_END}" '
        $0 == start { skip = 1; next }
        $0 == end { skip = 0; next }
        skip != 1 { print }
    ' "${RC_LOCAL}" > "${tmp_file}"
    mv "${tmp_file}" "${RC_LOCAL}"
    chmod 755 "${RC_LOCAL}"
}

remove_installed_files() {
    if [ -d "${INSTALL_DIR}" ]; then
        rm -rf "${INSTALL_DIR}"
    fi
}

main() {
    stop_service_if_possible
    remove_service_link
    remove_rc_local_entry
    remove_installed_files
    echo "${SERVICE_NAME} uninstalled."
}

main "$@"

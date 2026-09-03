#!/bin/sh
set -eu

ROOT=${ESS_WINTER_RUST_ROOT:-/data/venus-ess-winter-soc-service-rust}
MODE=${ESS_WINTER_INSTALL_MODE:-shadow}
SERVICE_ROOT=${ESS_SERVICE_ROOT:-/service}
SERVICE_NAME=venus-ess-winter-soc-service
ACTIVE_LINK="$SERVICE_ROOT/$SERVICE_NAME"
SHADOW_LINK="$SERVICE_ROOT/${SERVICE_NAME}-rust-shadow"
ACTIVE_SERVICE="$ROOT/service"
SHADOW_SERVICE="$ROOT/service-shadow"
RC_LOCAL=${ESS_RC_LOCAL:-/data/rc.local}
RC_START="# ${SERVICE_NAME} start"
RC_END="# ${SERVICE_NAME} end"
BINARY="$ROOT/bin/venus-ess-winter-soc-service"

die() {
    echo "venus-ess-winter-soc-service-rust: $*" >&2
    exit 1
}

require_layout() {
    [ "$(id -u)" = "0" ] || die "installer must run as root"
    [ -x "$BINARY" ] || die "missing ARMv7 executable: $BINARY"
    [ -f "$ACTIVE_SERVICE/run" ] || die "missing active run script"
    [ -f "$SHADOW_SERVICE/run" ] || die "missing shadow run script"
    chmod 755 "$BINARY" "$ACTIVE_SERVICE/run" "$SHADOW_SERVICE/run"
    mkdir -p "$SERVICE_ROOT"
}

wait_for_supervisor_exit() {
    attempts=0
    while pgrep -f "supervise ${SERVICE_NAME}$" >/dev/null 2>&1; do
        attempts=$((attempts + 1))
        [ "$attempts" -lt 30 ] || return 1
        sleep 1
    done
}

wait_for_service_up() {
    link=$1
    attempts=0
    while [ "$attempts" -lt 15 ]; do
        status=$(svstat "$link" 2>/dev/null || true)
        first_pid=$(printf '%s\n' "$status" | sed -n 's/.*(pid \([0-9][0-9]*\)).*/\1/p')
        if [ -n "$first_pid" ] && printf '%s\n' "$status" | grep -q ': up '; then
            sleep 3
            status=$(svstat "$link" 2>/dev/null || true)
            second_pid=$(printf '%s\n' "$status" | sed -n 's/.*(pid \([0-9][0-9]*\)).*/\1/p')
            if [ "$first_pid" = "$second_pid" ] && printf '%s\n' "$status" | grep -q ': up '; then
                return 0
            fi
        fi
        attempts=$((attempts + 1))
        sleep 1
    done
    return 1
}

wait_for_service_down() {
    link=$1
    attempts=0
    while [ "$attempts" -lt 30 ]; do
        status=$(svstat "$link" 2>/dev/null || true)
        if ! printf '%s\n' "$status" | grep -q ': up '; then
            return 0
        fi
        attempts=$((attempts + 1))
        sleep 1
    done
    return 1
}

replace_active_link() {
    target=$1
    if [ -e "$ACTIVE_LINK" ] && [ ! -L "$ACTIVE_LINK" ]; then
        die "refusing to replace non-symlink service path: $ACTIVE_LINK"
    fi
    svc -dx "$ACTIVE_LINK" 2>/dev/null || true
    rm -f "$ACTIVE_LINK"
    wait_for_supervisor_exit || return 1
    ln -s "$target" "$ACTIVE_LINK" || return 1
    wait_for_service_up "$ACTIVE_LINK"
}

write_rc_local_block() {
    [ -f "$RC_LOCAL" ] || die "missing $RC_LOCAL"
    temporary="${RC_LOCAL}.tmp.$$"
    block="${RC_LOCAL}.block.$$"
    {
        echo "$RC_START"
        echo "mkdir -p '$SERVICE_ROOT'"
        echo "if [ ! -e '$ACTIVE_LINK' ] || [ -L '$ACTIVE_LINK' ]; then ln -sfn '$ACTIVE_SERVICE' '$ACTIVE_LINK'; fi"
        echo "$RC_END"
    } >"$block"
    awk -v start="$RC_START" -v end="$RC_END" -v block="$block" '
        function emit(line) {
            while ((getline line < block) > 0) print line
            close(block)
        }
        $0 == start { skipping = 1; next }
        $0 == end { skipping = 0; next }
        skipping { next }
        $0 == "exit 0" && !emitted { emit(); emitted = 1 }
        { print }
        END { if (!emitted) emit() }
    ' "$RC_LOCAL" >"$temporary"
    mv "$temporary" "$RC_LOCAL"
    rm -f "$block"
    chmod 755 "$RC_LOCAL"
}

remove_rc_local_block() {
    [ -f "$RC_LOCAL" ] || return 0
    temporary="${RC_LOCAL}.tmp.$$"
    awk -v start="$RC_START" -v end="$RC_END" '
        $0 == start { skipping = 1; next }
        $0 == end { skipping = 0; next }
        !skipping { print }
    ' "$RC_LOCAL" >"$temporary"
    mv "$temporary" "$RC_LOCAL"
    chmod 755 "$RC_LOCAL"
}

restore_all_owned_settings() {
    config=${ESS_WINTER_RUST_CONFIG:-$ROOT/config.env}
    if [ -r "$config" ]; then
        (
            set -a
            . "$config"
            set +a
            exec "$BINARY" --restore-all-owned-settings
        )
    else
        "$BINARY" --restore-all-owned-settings
    fi
}

start_shadow() {
    if [ -e "$SHADOW_LINK" ] && [ ! -L "$SHADOW_LINK" ]; then
        die "refusing to replace non-symlink shadow path: $SHADOW_LINK"
    fi
    ln -sfn "$SHADOW_SERVICE" "$SHADOW_LINK"
    sleep 2
    svc -u "$SHADOW_LINK" 2>/dev/null || true
    wait_for_service_up "$SHADOW_LINK" || die "shadow service did not stay up"
    echo "Rust shadow service is running; active Python service was not changed."
}

stop_shadow() {
    svc -dx "$SHADOW_LINK" 2>/dev/null || true
    rm -f "$SHADOW_LINK"
    echo "Rust shadow service stopped."
}

activate() {
    svc -dx "$SHADOW_LINK" 2>/dev/null || true
    rm -f "$SHADOW_LINK"
    replace_active_link "$ACTIVE_SERVICE" || die "Rust activation failed"
    write_rc_local_block
    echo "Rust winter SoC service is active and persistent across reboot."
}

uninstall_service() {
    svc -d "$ACTIVE_LINK" 2>/dev/null || true
    wait_for_service_down "$ACTIVE_LINK" || die "active service did not stop cleanly"
    restore_all_owned_settings || die "owned settings could not be restored safely"
    svc -dx "$ACTIVE_LINK" 2>/dev/null || true
    svc -dx "$SHADOW_LINK" 2>/dev/null || true
    rm -f "$ACTIVE_LINK" "$SHADOW_LINK"
    remove_rc_local_block
    echo "Rust winter SoC service removed; all unambiguously owned settings restored."
}

require_layout
case "$MODE" in
    shadow) start_shadow ;;
    stop-shadow) stop_shadow ;;
    activate) activate ;;
    uninstall) uninstall_service ;;
    *) die "ESS_WINTER_INSTALL_MODE must be shadow, stop-shadow, activate, or uninstall" ;;
esac

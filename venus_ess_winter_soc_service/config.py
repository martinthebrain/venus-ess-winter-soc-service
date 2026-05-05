# SPDX-License-Identifier: GPL-3.0-or-later
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Callable, Optional, TypeAlias

# This controller protects the battery during winter and transition periods by
# raising the ESS minimum SoC when PV production is no longer sufficient to
# regularly bring the battery to a healthy higher SoC. The raised target does
# not need to be reached immediately; while the controller is actively raising
# SoC, it may defer or limit charging to low-load windows so the grid is treated
# gently without compromising the seasonal battery-protection goal.
#
# Seasonal policy summary:
# - Summer/default: use 10% MinSoC and preserve temporary manual overrides.
# - Pre-winter low PV: stage a reserve raise toward 40%.
# - Winter: stage a reserve raise toward 65%; balancing may temporarily target 100%.
# - Winter low SoC plus low PV history: stage toward 40% first, then continue to 65%.
# - Post-winter: hold 40% until PV recovery is confirmed, then return to 10%.
# - Outside charge windows: hold the reached SoC; do not jump to a higher reserve target.
# - During staged raises: capture the previous DVCC MaxChargeCurrent once and restore it
#   when the controller no longer needs its temporary charge-current limit.
#
# Runtime state is written to RAM first. A deliberately small subset is written
# to SD only in seasonal windows, so Cerbo flash is not used and SD wear remains
# low. The SD file is therefore a partial durable state, not a full replacement
# for the RAM state.
#
# Main loop flow:
# 1. Update SD-window state and PV history.
# 2. Read and validate current battery SoC.
# 3. Update full-SoC and balancing progress tracking.
# 4. Determine the seasonal MinSoC target.
# 5. Apply MinSoC/DVCC policy:
#    - honor summer/manual override handling,
#    - enforce the default MinSoC floor,
#    - run the charge-needed branch when SoC is below target,
#    - run the not-needed branch when SoC is already at or above target.

# --- CONFIGURATION ---
# RAM-backed paths. /dev/shm is volatile and is intentionally lost on reboot.
STATE_FILE = "/dev/shm/ess_winter_logic.json"
LOG_FILE = "/dev/shm/ess_winter_log.txt"  # RAM diagnostics log, also volatile.

# D-Bus services used by Victron Venus OS. The environment overrides are only
# meant for testbeds that publish simulated services on the system bus. Normal
# installations should leave them unset.
SERVICE_SETTINGS = os.getenv("ESS_SERVICE_SETTINGS", 'com.victronenergy.settings')
SERVICE_SYSTEM = os.getenv("ESS_SERVICE_SYSTEM", 'com.victronenergy.system')
PREFERRED_BATTERY_SERVICE = os.getenv(
    "ESS_PREFERRED_BATTERY_SERVICE",
    'com.victronenergy.battery.socketcan_vecan1',
)

# Thresholds and timing constants for SoC policy, balancing, grid protection,
# diagnostics, and persistence.
DBUS_CALL_TIMEOUT_SECONDS = 2.0
BALANCING_INTERVAL_DAYS = 14
BALANCING_DURATION_HOURS = 4  # Required continuous time near full SoC.
BALANCING_MAX_HOURS = 12      # Maximum runtime of one balancing attempt.
BALANCING_RETRY_COOLDOWN_HOURS = 24
BALANCING_BOOT_GRACE_HOURS = 24
BALANCING_FULL_SOC = 99.0
FULL_SOC_CONFIRM_MINUTES = 10
GRID_LOAD_LIMIT = 4000  # Soft AC import comfort target, not a safety limit.
GRID_PAUSE_HEADROOM_W = 100
GRID_SOFT_MIN_CHARGE_CURRENT_A = 10.0  # Minimum progress current during allowed charge windows.
CHARGE_WINDOW_START_HOUR = 23
CHARGE_WINDOW_BASE_HOURS = 4
CHARGE_WINDOW_ESCALATION_NIGHTS = 2
CHARGE_WINDOW_MAX_MULTIPLIER = 4
CHARGE_EFFICIENCY = 0.9
# Optional fixed restore fallback in ampere. The preferred restore source is the
# DVCC value captured immediately before the script applies its first stricter
# limit. Set this to -1 only if the normal Victron state is "no explicit limit".
NORMAL_CHARGE_CURRENT: Optional[float] = None
CHARGE_LIMIT_UPDATE_THRESHOLD_A = 1.0
CHARGE_LIMIT_MIN_UPDATE_INTERVAL_SECONDS = 300
SAFE_CHARGE_CURRENT_A: Optional[float] = 50.0
SOC_HYSTERESIS = 1.0
STATUS_LOG_INTERVAL_SECONDS = 300
SOC_INVALID_LOG_INTERVAL_SECONDS = 300
BOOT_RECOVERY_SECONDS = 600
LOG_MAX_BYTES = 2_000_000
LOG_TRUNCATE_BYTES = 200_000
PV_THRESHOLD = 3000     # Watt average during the 09:00-17:00 PV sample window.
TRANSITION_DAYS = 4     # Number of daily averages required for transition decisions.
LOOP_INTERVAL_SECONDS = 60
DEFAULT_SOC = 10.0
PV_FALLBACK_MIN_VALID_AGE_DAYS = 3
MIN_SOC_EPSILON = 0.1
SUMMER_MANUAL_MINSOC_HOLD_SECONDS = 24 * 3600
MIN_SOC_SCRIPT_WRITE_MATCH_SECONDS = 180
SD_SAVE_INTERVAL_SECONDS = 21600
SD_BACKOFF_MAX_SECONDS = 300
SD_DIR_NAME = "socSteuerung"
SD_LOOKUP_INTERVAL_SECONDS = 3600
BATTERY_SERVICE_RESCAN_SECONDS = 300
WINTER_START_MMDD = 1125      # Nov 25.
WINTER_END_MMDD = 205         # Feb 05.
TRANS_PRE_START_MMDD = 1105   # Nov 05.
TRANS_PRE_END_MMDD = 1124     # Nov 24.
TRANS_POST_START_MMDD = 206   # Feb 06.
TRANS_POST_END_MMDD = 225     # Feb 25.
# Only these keys are allowed to survive on SD. Runtime flags, in-progress
# timers, and per-loop counters stay RAM-only because a later SD write must not
# replace a more complete RAM state after restart.
SD_PERSISTENT_BASE_KEYS = (
    "max_charge_current_raw",
    "max_charge_current_raw_set",
    "charge_current_owned_by_script",
    "max_charge_current_script_last_set",
)
SD_PERSISTENT_PV_KEYS = (
    "pv_history",
    "last_sample_date",
)
SD_PERSISTENT_WINTER_KEYS = (
    "last_balance_ts",
    "last_balance_attempt_ts",
    "last_full_ts",
)

SdPathResult: TypeAlias = tuple[Optional[Path], str]
State: TypeAlias = dict[str, Any]
SdSignature: TypeAlias = dict[str, Any]
SdWriteRequest: TypeAlias = dict[str, Any]
PowerReader: TypeAlias = Callable[[], Optional[float]]
TargetMode: TypeAlias = tuple[float, str]
ChargeContext: TypeAlias = dict[str, Any]

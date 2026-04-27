#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
import dbus
import time
import json
import os
import sys
import signal
import threading
import copy
from typing import Any, Callable, Optional, Sequence, TypeAlias, cast
from pathlib import Path
from datetime import datetime

# This controller protects the battery during winter and transition periods by
# raising the ESS minimum SoC when PV production is no longer sufficient to
# regularly bring the battery to a healthy higher SoC. The raised target does
# not need to be reached immediately; while the controller is actively raising
# SoC, it may defer or limit charging to low-load windows so the grid is treated
# gently without compromising the seasonal battery-protection goal.
#
# The temporary DVCC charge-current limit is a comfort preference, not a hard
# safety boundary: the script prefers the soft grid import target, but it can
# still allow a minimum progress current so seasonal battery protection does not
# stall indefinitely.
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

# D-Bus services used by Victron Venus OS.
SERVICE_SETTINGS = 'com.victronenergy.settings'
SERVICE_SYSTEM = 'com.victronenergy.system'
PREFERRED_BATTERY_SERVICE = 'com.victronenergy.battery.socketcan_vecan1'

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

def path_exists(path: Path) -> bool:
    """Return whether a path exists, isolated so SD probing can be unit-tested."""
    return path.exists()

def find_sd_by_path_env() -> SdPathResult:
    """Resolve ESS_SD_PATH when it points to an existing path."""
    sd_path = os.getenv("ESS_SD_PATH", "").strip()
    if not sd_path:
        return None, ""
    p = Path(sd_path)
    if path_exists(p):
        return p, f"SD path: {p}"
    return None, f"SD path not found: {p}"

def find_sd_by_label_env(media_roots: Sequence[Path]) -> SdPathResult:
    """Resolve ESS_SD_LABEL below removable-media roots."""
    sd_label = os.getenv("ESS_SD_LABEL", "").strip()
    if not sd_label:
        return None, ""
    for root in media_roots:
        p = root / sd_label
        if path_exists(p):
            return p, f"SD label: {p}"
    return None, f"SD label not found: {sd_label}"

def find_sd_from_env(media_roots: Sequence[Path]) -> SdPathResult:
    """Resolve an SD card from explicit environment configuration."""
    env_path, env_info = find_sd_by_path_env()
    if env_path or env_info:
        return env_path, env_info
    return find_sd_by_label_env(media_roots)

def find_auto_sd_in_root(root: Path) -> Optional[Path]:
    """Return the first mmcblk-style folder below one media root."""
    if not path_exists(root):
        return None
    for folder in root.iterdir():
        if "mmcblk" in folder.name:
            return folder
    return None

def find_auto_sd(media_roots: Sequence[Path]) -> SdPathResult:
    """Find the first mmcblk-style mount below the usual removable-media roots."""
    try:
        for root in media_roots:
            folder = find_auto_sd_in_root(root)
            if folder:
                return folder, f"SD auto: {folder}"
    except Exception:
        pass
    return None, ""

def get_sd_path() -> SdPathResult:
    """Locate an SD mount via env var, label, or removable-media mountpoints."""
    # Lookup order:
    # 1. ESS_SD_PATH, when set to an existing mount path.
    # 2. ESS_SD_LABEL, resolved below /media or /run/media.
    # 3. First mmcblk* mount below /media or /run/media.
    media_roots = [Path("/media"), Path("/run/media")]
    env_path, env_info = find_sd_from_env(media_roots)
    if env_path:
        return env_path, env_info

    auto_path, auto_info = find_auto_sd(media_roots)
    if auto_path:
        return auto_path, auto_info

    return None, (env_info or "No SD found")

def atomic_write(path: Path, data: str, fsync: bool = False) -> None:
    """Write data through a temporary file and atomically replace the target."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        f.write(data)
        f.flush()
        if fsync:
            os.fsync(f.fileno())
    os.replace(tmp, path)
    if fsync:
        try:
            dir_fd = os.open(str(path.parent), os.O_DIRECTORY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except Exception:
            pass

# --- D-Bus Wrapper ---
class DBusInterface:
    """Small Victron D-Bus facade with tolerant reads, typed writes, and logging."""
    def __init__(self) -> None:
        """Connect to the system D-Bus."""
        self.bus: Any = dbus.SystemBus()

    def get_value(self, service: str, path: str, default: Optional[float] = 0.0) -> Optional[float]:
        """Read a measured numeric D-Bus value and hide invalid None/-1 readings."""
        try:
            obj = self.bus.get_object(service, path)
            try:
                val = obj.GetValue(
                    dbus_interface='com.victronenergy.BusItem',
                    timeout=DBUS_CALL_TIMEOUT_SECONDS
                )
            except TypeError:
                # Older dbus-python bindings do not accept a typed timeout parameter.
                val = obj.GetValue(dbus_interface='com.victronenergy.BusItem')
            # Measurements use -1 or None for unavailable values on several paths.
            if val is None or val == -1:
                return default
            return float(val)
        except Exception:
            return default

    def get_raw_value(self, service: str, path: str, default: Optional[float] = None) -> Optional[float]:
        """Read a raw D-Bus value without treating -1 as invalid."""
        try:
            obj = self.bus.get_object(service, path)
            try:
                val = obj.GetValue(
                    dbus_interface='com.victronenergy.BusItem',
                    timeout=DBUS_CALL_TIMEOUT_SECONDS
                )
            except TypeError:
                val = obj.GetValue(dbus_interface='com.victronenergy.BusItem')
            if val is None:
                return default
            return float(val)
        except Exception:
            return default

    def set_value(self, service: str, path: str, value: Any) -> bool:
        """Write a D-Bus value, preserving explicit dbus.* types when provided."""
        try:
            value = self.coerce_dbus_value(value)
            obj = self.bus.get_object(service, path)
            self.set_bus_item_value(obj, value)
            return True
        except Exception as e:
            self.log(f"Error while setting {path}: {e}")
            return False

    def coerce_dbus_value(self, value: Any) -> Any:
        """Convert plain Python scalars to explicit D-Bus scalar types."""
        if isinstance(value, self.dbus_scalar_types()):
            return value
        if isinstance(value, bool):
            return dbus.Boolean(value)
        if isinstance(value, float):
            return dbus.Double(value)
        if isinstance(value, int):
            return dbus.Int32(value)
        return value

    def dbus_scalar_types(self) -> tuple[type[Any], ...]:
        """Return scalar D-Bus wrapper classes that should be preserved."""
        return (
            dbus.Boolean,
            dbus.Int16,
            dbus.UInt16,
            dbus.Int32,
            dbus.UInt32,
            dbus.Int64,
            dbus.UInt64,
            dbus.Double,
            dbus.Byte,
        )

    def set_bus_item_value(self, obj: Any, value: Any) -> None:
        """Call SetValue with timeout support and old-binding fallback."""
        try:
            obj.SetValue(
                value,
                dbus_interface='com.victronenergy.BusItem',
                timeout=DBUS_CALL_TIMEOUT_SECONDS
            )
        except TypeError:
            obj.SetValue(value, dbus_interface='com.victronenergy.BusItem')

    def log(self, msg: str) -> None:
        """Log to stdout and to the volatile RAM log with simple size rotation."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"{timestamp}: {msg}"
        print(log_entry)
        # Keep diagnostics in RAM so normal operation does not write Cerbo flash.
        try:
            with open(LOG_FILE, "a") as f:
                f.write(log_entry + "\n")
            try:
                if os.path.getsize(LOG_FILE) > LOG_MAX_BYTES:
                    with open(LOG_FILE, "rb") as f:
                        f.seek(0, os.SEEK_END)
                        size = f.tell()
                        keep = LOG_TRUNCATE_BYTES if size > LOG_TRUNCATE_BYTES else size
                        f.seek(-keep, os.SEEK_END)
                        data = f.read(keep)
                    with open(LOG_FILE, "wb") as f:
                        f.write(data)
            except Exception:
                pass
        except:
            pass

    def list_services(self, prefix: str) -> list[str]:
        """Return list of services matching a prefix."""
        try:
            # dbus-python exposes list_names without a typed timeout parameter in many stubs.
            # Keep this call simple for compatibility and static type checkers.
            names = cast(list[str], self.bus.list_names() or [])
            return [name for name in names if name.startswith(prefix)]
        except Exception:
            return []

# --- Main Control Logic ---
class WinterController:
    """Seasonal ESS controller for MinSoC targets, balancing, and charge limiting."""
    def __init__(self) -> None:  # pragma: no cover
        """Initialize controller state, SD handling, and background writer."""
        self.dbus = DBusInterface()
        self.sd_last_persist_ts = 0.0
        self.sd_error_count = 0
        self.sd_next_try_ts = 0.0
        self.sd_last_signature: Optional[SdSignature] = None
        self.sd_pending_signature: Optional[SdSignature] = None
        self.sd_pending_fsync = False
        self.sd_window_active = False
        self.sd_card_path: Optional[Path] = None
        self.sd_state_dir: Optional[Path] = None
        self.sd_state_file: Optional[Path] = None
        self.sd_info = ""
        self.sd_last_lookup_ts = 0.0
        self.last_charge_limit_set_ts = 0.0
        self.sd_write_lock = threading.Lock()
        self.sd_write_event = threading.Event()
        self.sd_write_pending: Optional[SdWriteRequest] = None
        self.sd_write_inflight = False
        self.sd_writer_thread = threading.Thread(target=self._sd_writer_loop, daemon=True)
        self.sd_writer_thread.start()
        self.refresh_sd_paths(force=True)
        self.state: State = self.load_state()
        self.register_signal_handlers()
        self.sd_window_active = self.is_sd_window()
        if self.sd_state_file is None:
            self.dbus.log(f"SD disabled: {self.sd_info}")
        elif not self.sd_window_active:
            self.dbus.log(f"SD present but inactive outside the seasonal window: {self.sd_info}")
        else:
            self.dbus.log(f"SD enabled: {self.sd_info}")
        
    def load_state(self) -> State:
        """Load RAM state and merge the seasonal SD subset over defaults."""
        defaults = self.default_state()
        ram_data = self.read_state_file(Path(STATE_FILE))
        sd_data = self.read_state_file(self.sd_state_file) if self.is_sd_window() else None
        self.merge_state(defaults, ram_data)
        self.merge_sd_state_if_newer(defaults, sd_data, ram_data)
        defaults["boot_ts"] = time.time()
        return defaults

    def default_state(self) -> State:
        """Return a complete fresh controller state with isolated mutable values."""
        return {
            "pv_history": [],
            "last_balance_ts": 0,
            "last_balance_attempt_ts": 0,
            "balancing_active": False,
            "balancing_start_ts": 0,
            "balance_full_seconds": 0.0,
            "full_soc_seconds": 0.0,
            "last_full_ts": 0,
            "charging_mode_active": False,
            "charging_paused": False,
            "charge_deficit_start_ts": 0,
            "battery_service": None,
            "battery_max_current_last": None,
            "battery_service_last_scan_ts": 0,
            "normal_charge_current": None,
            "max_charge_current_raw": None,
            "max_charge_current_raw_set": False,
            "charge_current_owned_by_script": False,
            "max_charge_current_script_last_set": None,
            "current_day_samples": [],
            "pv_energy_ws": 0.0,
            "pv_time_s": 0.0,
            "pv_last_sample_ts": 0.0,
            "pv_last_sample_power": 0.0,
            "last_pv_integral_ts": 0.0,
            "last_sample_date": "",
            "last_mode": "",
            "last_loop_ts": 0,
            "last_status_log_ts": 0,
            "last_soc_invalid_log_ts": 0,
            "last_min_soc_invalid_log_ts": 0,
            "manual_override_until_ts": 0,
            "min_soc_last_seen": None,
            "min_soc_last_script_set": None,
            "min_soc_last_script_set_ts": 0,
            "last_manual_override_log_ts": 0,
            "ts": 0,
            "boot_ts": time.time()
        }

    def sd_should_override_ram(self, sd_data: object, ram_data: object) -> bool:
        """Return True when the partial SD state is newer than RAM and may be merged."""
        if not isinstance(sd_data, dict):
            return False
        if not isinstance(ram_data, dict):
            return True
        sd_state = cast(State, sd_data)
        ram_state = cast(State, ram_data)
        return float(sd_state.get("ts", 0)) > float(ram_state.get("ts", 0))

    def sd_persistent_keys(self) -> list[str]:
        """Return the durable SD keys that are meaningful in the active season."""
        keys: list[str] = list(SD_PERSISTENT_BASE_KEYS)
        if self.is_pv_history_window():
            keys.extend(SD_PERSISTENT_PV_KEYS)
        if self.is_winter_window():
            keys.extend(SD_PERSISTENT_WINTER_KEYS)
        return keys

    def merge_sd_persistent_state(
        self,
        state: State,
        sd_data: object,
        ram_data: object = None,
    ) -> None:
        """Merge only explicit SD keys, never treating SD as a full controller state."""
        if not self.sd_should_override_ram(sd_data, ram_data) or not isinstance(sd_data, dict):
            return
        sd_state = cast(State, sd_data)
        for key in self.sd_persistent_keys():
            if key in sd_state:
                state[key] = copy.deepcopy(sd_state[key])

    def merge_sd_state_if_newer(self, state: State, sd_data: object, ram_data: object) -> bool:
        """Merge SD subset and seed cache only when SD is newer than the RAM state."""
        if not isinstance(sd_data, dict):
            return False
        sd_state = cast(State, sd_data)
        if self.sd_should_override_ram(sd_state, ram_data):
            self.merge_sd_persistent_state(state, sd_state, ram_data)
            self.init_sd_state_cache(sd_state)
            return True
        self.sd_last_signature = None
        self.sd_last_persist_ts = 0
        return False

    def merge_state(self, defaults: State, chosen: object) -> None:
        """Merge known keys from a persisted full state into the default structure."""
        if not isinstance(chosen, dict):
            return
        for key in defaults:
            if key in chosen:
                defaults[key] = chosen[key]

    def save_state_to_ram(self, force_persist: bool = False) -> None:
        """Persist the full state to RAM and optionally queue the SD subset."""
        self.state["ts"] = time.time()
        try:
            atomic_write(Path(STATE_FILE), json.dumps(self.state), fsync=False)
        except Exception as e:
            self.dbus.log(f"Could not save state to RAM: {e}")
        self.persist_state_to_sd(force_persist=force_persist)

    def read_state_file(self, path: Optional[Path]) -> Optional[State]:
        """Read a JSON state file and quarantine it when it cannot be parsed."""
        if not path:
            return None
        try:
            if path.exists():
                data = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    return cast(State, data)
                self.dbus.log(f"State file does not contain a JSON object: {path}")
        except Exception as e:
            self.dbus.log(f"State file is unreadable: {path}: {e}")
            self.backup_bad_state_file(path)
            return None
        return None

    def backup_bad_state_file(self, path: Path) -> None:
        """Move an unreadable state file aside so the failure can be inspected later."""
        try:
            stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            backup = path.with_name(f"{path.name}.bad-{stamp}")
            os.replace(path, backup)
            self.dbus.log(f"Bad state file moved aside: {backup}")
        except Exception as e:
            self.dbus.log(f"Could not move bad state file aside: {path}: {e}")

    def refresh_sd_paths(self, force: bool = False) -> None:
        """Resolve the SD mount lazily to tolerate card insertion/removal."""
        now = time.time()
        if not force and (now - self.sd_last_lookup_ts) < SD_LOOKUP_INTERVAL_SECONDS:
            return
        path, info = get_sd_path()
        self.sd_last_lookup_ts = now
        self.sd_info = info
        if path:
            self.sd_card_path = path
            self.sd_state_dir = path / SD_DIR_NAME
            self.sd_state_file = self.sd_state_dir / "ess_winter_logic.json"
        else:
            self.sd_card_path = None
            self.sd_state_dir = None
            self.sd_state_file = None

    def persist_state_to_sd(self, force_persist: bool = False) -> None:
        """Queue a partial SD state write with interval limiting and retry backoff."""
        if not self.can_attempt_sd_write(force_persist):
            return
        now = time.time()
        if self.is_sd_backoff_active(now, force_persist):
            return

        signature = self.build_sd_signature()
        payload = dict(signature)
        payload["ts"] = now
        if self.should_skip_sd_persist(now, signature, force_persist):
            return

        try:
            self._enqueue_sd_write(
                payload_json=json.dumps(payload),
                signature=signature,
                fsync=force_persist,
                sd_state_dir=self.sd_state_dir,
                sd_state_file=self.sd_state_file,
            )
        except Exception as e:
            self.sd_error_count += 1
            backoff = min(SD_BACKOFF_MAX_SECONDS, 2 ** min(self.sd_error_count, 8))
            self.sd_next_try_ts = now + backoff
            self.dbus.log(f"SD write error: {e} (backoff {backoff:.0f}s)")

    def can_attempt_sd_write(self, force_persist: bool) -> bool:
        """Return True when seasonal SD persistence is allowed and has a target path."""
        if not self.is_sd_window():
            return False
        if self.sd_state_file is not None:
            return True
        self.refresh_sd_paths(force=True)
        return self.sd_state_file is not None

    def is_sd_backoff_active(self, now: float, force_persist: bool) -> bool:
        """Return True when a non-forced SD write should wait for backoff."""
        return (now < self.sd_next_try_ts) and not force_persist

    def build_sd_signature(self) -> SdSignature:
        """Build the partial durable state that is worth persisting on SD."""
        return copy.deepcopy({key: self.state.get(key) for key in self.sd_persistent_keys()})

    def should_skip_sd_persist(
        self,
        now: float,
        signature: SdSignature,
        force_persist: bool,
    ) -> bool:
        """Skip unchanged or too-frequent non-forced SD writes."""
        if force_persist:
            return False
        if (now - self.sd_last_persist_ts) < SD_SAVE_INTERVAL_SECONDS:
            return True
        if signature == self.sd_last_signature:
            return True
        with self.sd_write_lock:
            return signature == self.sd_pending_signature

    def init_sd_state_cache(self, sd_data: object) -> None:
        """Seed write-deduplication metadata from an already loaded SD state."""
        if not isinstance(sd_data, dict):
            return
        sd_state = cast(State, sd_data)
        signature: SdSignature = {
            "max_charge_current_raw": sd_state.get("max_charge_current_raw"),
            "max_charge_current_raw_set": sd_state.get("max_charge_current_raw_set", False),
            "charge_current_owned_by_script": sd_state.get("charge_current_owned_by_script", False),
            "max_charge_current_script_last_set": sd_state.get("max_charge_current_script_last_set"),
        }
        if self.is_pv_history_window():
            signature["pv_history"] = sd_state.get("pv_history", [])
            signature["last_sample_date"] = sd_state.get("last_sample_date", "")
        if self.is_winter_window():
            signature["last_balance_ts"] = sd_state.get("last_balance_ts", 0)
            signature["last_balance_attempt_ts"] = sd_state.get("last_balance_attempt_ts", 0)
            signature["last_full_ts"] = sd_state.get("last_full_ts", 0)
        self.sd_last_signature = copy.deepcopy(signature)
        self.sd_last_persist_ts = float(sd_state.get("ts", 0))
        self.sd_pending_signature = None
        self.sd_pending_fsync = False

    def _enqueue_sd_write(
        self,
        payload_json: str,
        signature: SdSignature,
        fsync: bool,
        sd_state_dir: Optional[Path],
        sd_state_file: Optional[Path],
    ) -> None:
        """Queue an SD write for the background writer, keeping only the latest state."""
        signature = copy.deepcopy(signature)
        with self.sd_write_lock:
            # Last-write-wins: pending writes are coalesced into the newest payload.
            if self.sd_write_pending:
                self.sd_write_pending["payload_json"] = payload_json
                self.sd_write_pending["signature"] = signature
                # Preserve fsync if any coalesced write represented a critical event.
                self.sd_write_pending["fsync"] = self.sd_write_pending["fsync"] or fsync
                self.sd_write_pending["sd_state_dir"] = sd_state_dir
                self.sd_write_pending["sd_state_file"] = sd_state_file
            else:
                self.sd_write_pending = {
                    "payload_json": payload_json,
                    "signature": signature,
                    "fsync": fsync,
                    "sd_state_dir": sd_state_dir,
                    "sd_state_file": sd_state_file,
                }
            self.sd_pending_signature = signature
            self.sd_pending_fsync = self.sd_pending_fsync or fsync
            self.sd_write_event.set()

    def _sd_writer_loop(self) -> None:  # pragma: no cover
        """Background thread that performs SD writes and retries failed payloads."""
        while True:
            self.sd_write_event.wait()
            while self.process_next_sd_write():
                pass

    def process_next_sd_write(self) -> bool:  # pragma: no cover
        """Process one queued SD write request and return whether work was found."""
        req = self.pop_sd_write_request()
        if req is None:
            return False
        try:
            self.perform_sd_write(req)
            self.mark_sd_write_success(req, time.time())
        except Exception as e:
            self.mark_sd_write_failure(req, e, time.time())
        finally:
            self.clear_sd_write_inflight()
        return True

    def pop_sd_write_request(self) -> Optional[SdWriteRequest]:  # pragma: no cover
        """Take the newest queued SD write request from the background queue."""
        with self.sd_write_lock:
            req = self.sd_write_pending
            self.sd_write_pending = None
            if req is None:
                self.sd_write_event.clear()
                return None
            self.sd_write_inflight = True
            return req

    def perform_sd_write(self, req: SdWriteRequest) -> None:  # pragma: no cover
        """Write one SD request to disk, validating that a target path exists."""
        sd_state_dir = req.get("sd_state_dir")
        sd_state_file = req.get("sd_state_file")
        if sd_state_dir is None or sd_state_file is None:
            raise RuntimeError("SD path is not available")
        sd_state_dir.mkdir(parents=True, exist_ok=True)
        atomic_write(sd_state_file, req["payload_json"], fsync=req["fsync"])

    def mark_sd_write_success(self, req: SdWriteRequest, now: float) -> None:  # pragma: no cover
        """Update SD write metadata after a successful background write."""
        with self.sd_write_lock:
            self.sd_last_persist_ts = now
            self.sd_last_signature = copy.deepcopy(req["signature"])
            self.sd_error_count = 0
            self.sd_next_try_ts = 0.0
            self.sync_pending_sd_metadata()

    def sync_pending_sd_metadata(self) -> None:  # pragma: no cover
        """Mirror the currently queued SD request in de-duplication metadata."""
        if self.sd_write_pending is None:
            self.sd_pending_signature = None
            self.sd_pending_fsync = False
            return
        self.sd_pending_signature = copy.deepcopy(self.sd_write_pending["signature"])
        self.sd_pending_fsync = self.sd_write_pending["fsync"]

    def mark_sd_write_failure(
        self,
        req: SdWriteRequest,
        error: Exception,
        now: float,
    ) -> None:  # pragma: no cover
        """Requeue a failed SD write and sleep for the calculated backoff."""
        with self.sd_write_lock:
            self.sd_error_count += 1
            backoff = min(SD_BACKOFF_MAX_SECONDS, 2 ** min(self.sd_error_count, 8))
            self.sd_next_try_ts = now + backoff
            if self.sd_write_pending is None:
                self.sd_write_pending = req
            self.sd_pending_signature = copy.deepcopy(req["signature"])
            self.sd_pending_fsync = req["fsync"]
        self.dbus.log(f"SD write error: {error} (backoff {backoff:.0f}s)")
        time.sleep(backoff)

    def clear_sd_write_inflight(self) -> None:  # pragma: no cover
        """Mark the background writer as idle after one request attempt."""
        with self.sd_write_lock:
            self.sd_write_inflight = False

    def flush_sd_writes(self, timeout_seconds: float = 5) -> bool:
        """Wait briefly for pending SD writes before shutdown or tests continue."""
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            with self.sd_write_lock:
                pending = self.sd_write_pending is not None
                inflight = self.sd_write_inflight
            if not pending and not inflight:
                return True
            time.sleep(0.05)
        return False

    def register_signal_handlers(self) -> None:
        """Install SIGTERM/SIGINT handlers so RAM and critical SD state are flushed."""
        def _handler(signum: int, frame: Any) -> None:
            """Signal handler: persist state and exit."""
            self.dbus.log(f"Shutdown signal {signum}; saving state.")
            self.save_state_to_ram(force_persist=True)
            self.flush_sd_writes(timeout_seconds=5)
            sys.exit(0)

        signal.signal(signal.SIGTERM, _handler)
        signal.signal(signal.SIGINT, _handler)

    def is_pv_history_window(self, now: Optional[datetime] = None) -> bool:
        """Return True during transition windows where PV history is collected."""
        if now is None:
            now = datetime.now()
        current_date_val = now.month * 100 + now.day
        return (
            TRANS_PRE_START_MMDD <= current_date_val <= TRANS_PRE_END_MMDD
        ) or (
            TRANS_POST_START_MMDD <= current_date_val <= TRANS_POST_END_MMDD
        )

    def is_winter_window(self, now: Optional[datetime] = None) -> bool:
        """Return True during the winter window where balancing and higher SoC apply."""
        if now is None:
            now = datetime.now()
        current_date_val = now.month * 100 + now.day
        return (current_date_val >= WINTER_START_MMDD) or (current_date_val <= WINTER_END_MMDD)

    def is_sd_window(self, now: Optional[datetime] = None) -> bool:
        """Return True when seasonal SD reads/writes are allowed."""
        return self.is_winter_window(now) or self.is_pv_history_window(now)

    def is_boot_recovery_window(self, now_ts: Optional[float] = None) -> bool:
        """Return True shortly after boot so active SoC raises can be recovered."""
        if now_ts is None:
            now_ts = time.time()
        boot_ts = float(self.state.get("boot_ts", now_ts))
        return (now_ts - boot_ts) <= BOOT_RECOVERY_SECONDS

    def set_min_soc(self, path: str, value: float) -> bool:
        """Write MinSoC and remember that this value was script-driven."""
        target = float(value)
        if self.dbus.set_value(SERVICE_SETTINGS, path, target):
            now_ts = time.time()
            self.state["min_soc_last_script_set"] = target
            self.state["min_soc_last_script_set_ts"] = now_ts
            self.state["min_soc_last_seen"] = target
            return True
        return False

    def track_manual_min_soc_change(
        self,
        current_setting: float,
        now_ts: float,
        in_control_window: bool,
    ) -> bool:
        """Detect external MinSoC changes and preserve summer manual overrides."""
        current_setting = float(current_setting)
        last_seen = self.state.get("min_soc_last_seen")
        if self.remember_initial_min_soc(current_setting, last_seen):
            return True
        if self.same_min_soc(current_setting, last_seen):
            return False
        changed = self.register_manual_override_if_needed(
            current_setting,
            now_ts,
            in_control_window,
        )
        self.state["min_soc_last_seen"] = current_setting
        return changed

    def remember_initial_min_soc(self, current_setting: float, last_seen: Any) -> bool:
        """Store the first observed MinSoC value so later changes can be detected."""
        if last_seen is not None:
            return False
        self.state["min_soc_last_seen"] = current_setting
        return True

    def same_min_soc(self, current_setting: float, last_seen: Any) -> bool:
        """Return True when two MinSoC readings are equivalent within tolerance."""
        if last_seen is None:
            return False
        return abs(current_setting - float(last_seen)) <= MIN_SOC_EPSILON

    def was_recent_script_min_soc_write(self, current_setting: float, now_ts: float) -> bool:
        """Return True when the observed MinSoC matches a recent controller write."""
        script_val = self.state.get("min_soc_last_script_set")
        script_ts = self.state.get("min_soc_last_script_set_ts", 0)
        return (
            script_val is not None
            and abs(current_setting - float(script_val)) <= MIN_SOC_EPSILON
            and (now_ts - float(script_ts)) <= MIN_SOC_SCRIPT_WRITE_MATCH_SECONDS
        )

    def register_manual_override_if_needed(
        self,
        current_setting: float,
        now_ts: float,
        in_control_window: bool,
    ) -> bool:
        """Start a summer override when an external MinSoC change should be honored."""
        if in_control_window or self.was_recent_script_min_soc_write(current_setting, now_ts):
            return False
        self.state["manual_override_until_ts"] = now_ts + SUMMER_MANUAL_MINSOC_HOLD_SECONDS
        self.state["last_manual_override_log_ts"] = now_ts
        self.dbus.log("Manual MinSoC change detected; controller leaves the value unchanged for 24h")
        return True

    def get_total_pv_power(self) -> float:
        """Sum AC PV on grid/output sides plus DC PV power."""
        pv_ac = 0.0
        for phase in ['L1', 'L2', 'L3']:
            pv_ac += self.dbus.get_value(SERVICE_SYSTEM, f'/Ac/PvOnGrid/{phase}/Power', 0.0) or 0.0
            pv_ac += self.dbus.get_value(SERVICE_SYSTEM, f'/Ac/PvOnOutput/{phase}/Power', 0.0) or 0.0
             
        pv_dc = self.dbus.get_value(SERVICE_SYSTEM, '/Dc/Pv/Power', 0.0) or 0.0
        return pv_ac + pv_dc

    def get_grid_power_net(self) -> float:
        """Net grid power: import positive, export negative."""
        total = self.sum_phase_values('/Ac/Grid/{phase}/Power')
        return total if total is not None else 0.0

    def get_battery_service(self) -> Optional[str]:
        """Return a cached valid battery service, rescanning only when it becomes invalid."""
        cached = self.state.get("battery_service")
        if self.is_valid_battery_service(cached):
            return str(cached)
        if not self.battery_service_rescan_due():
            return None
        return self.scan_battery_service(str(cached) if cached is not None else None)

    def battery_service_rescan_due(self) -> bool:
        """Return True when enough time has passed to scan D-Bus battery services."""
        now = time.time()
        last_scan = self.state.get("battery_service_last_scan_ts", 0)
        if (now - last_scan) >= BATTERY_SERVICE_RESCAN_SECONDS:
            self.state["battery_service_last_scan_ts"] = now
            return True
        return False

    def scan_battery_service(self, cached: Optional[str]) -> Optional[str]:
        """Scan D-Bus and select the preferred or strongest battery service."""
        services = self.dbus.list_services('com.victronenergy.battery')
        if not services:
            return None
        preferred = self.select_preferred_battery_service(services)
        return preferred or self.store_best_battery_service(services, cached)

    def store_best_battery_service(
        self,
        services: Sequence[str],
        cached: Optional[str],
    ) -> Optional[str]:
        """Store and return the strongest valid battery service discovered."""
        best = self.select_best_battery_service(services)
        if best and best != cached:
            self.state["battery_service"] = best
            self.save_state_to_ram()
        return best

    def is_valid_battery_service(self, service: object) -> bool:
        """Return True when a battery service exposes a usable BMS charge current."""
        if not isinstance(service, str) or not service:
            return False
        val = self.dbus.get_value(service, '/Info/MaxChargeCurrent', None)
        return val is not None and val > 0

    def select_preferred_battery_service(self, services: Sequence[str]) -> Optional[str]:
        """Use configured preferred battery service when it is available and valid."""
        if PREFERRED_BATTERY_SERVICE not in services:
            return None
        if not self.is_valid_battery_service(PREFERRED_BATTERY_SERVICE):
            return None
        self.state["battery_service"] = PREFERRED_BATTERY_SERVICE
        self.save_state_to_ram()
        return PREFERRED_BATTERY_SERVICE

    def select_best_battery_service(self, services: Sequence[str]) -> Optional[str]:
        """Choose the battery service with the highest advertised charge current."""
        best: Optional[str] = None
        best_val = 0.0
        for svc in services:
            val = self.dbus.get_value(svc, '/Info/MaxChargeCurrent', None)
            if val is not None and val > best_val:
                best_val = val
                best = svc
        return best

    def get_battery_max_charge_current(self) -> Optional[float]:
        """Read the BMS maximum charge current, falling back to the last valid value."""
        svc = self.get_battery_service()
        live_current = self.read_live_battery_max_current(svc)
        if live_current is not None:
            return live_current
        return self.get_cached_battery_max_current()

    def read_live_battery_max_current(self, service: Optional[str]) -> Optional[float]:
        """Read and cache a live BMS maximum charge current when available."""
        if not service:
            return None
        val = self.dbus.get_value(service, '/Info/MaxChargeCurrent', None)
        if val is None or val <= 0:
            return None
        self.state["battery_max_current_last"] = val
        return val

    def get_cached_battery_max_current(self) -> Optional[float]:
        """Return the last known valid BMS maximum charge current."""
        cached = self.state.get("battery_max_current_last")
        if cached is not None and cached > 0:
            return float(cached)
        return None

    def get_battery_power(self) -> Optional[float]:
        """Read the current DC battery power from the system service."""
        return self.dbus.get_value(SERVICE_SYSTEM, '/Dc/Battery/Power', 0)

    def get_battery_voltage(self) -> Optional[float]:
        """Read the current DC battery voltage used for charge-current calculations."""
        return self.dbus.get_value(SERVICE_SYSTEM, '/Dc/Battery/Voltage', None)

    def get_house_load_power(
        self,
        grid_power_net: Optional[float] = None,
        batt_power: Optional[float] = None,
    ) -> float:
        """Compute house load from consumption paths or fallback."""
        total = self.sum_phase_values('/Ac/ConsumptionOnInput/{phase}/Power')
        if total is not None:
            return total

        total = self.sum_phase_values('/Ac/Consumption/{phase}/Power')
        if total is not None:
            return total

        return self.compute_house_load_fallback(grid_power_net, batt_power)

    def sum_phase_values(self, path_template: str) -> Optional[float]:
        """Sum L1-L3 D-Bus values, returning None when no phase exists."""
        total = 0.0
        found = False
        for phase in ['L1', 'L2', 'L3']:
            val = self.dbus.get_value(SERVICE_SYSTEM, path_template.format(phase=phase), None)
            if val is not None:
                total += val
                found = True
        return total if found else None

    def compute_house_load_fallback(
        self,
        grid_power_net: Optional[float] = None,
        batt_power: Optional[float] = None,
    ) -> float:
        """Fallback house-load estimate from grid and positive battery discharge."""
        grid_power_net = self.resolve_power_value(grid_power_net, self.get_grid_power_net)
        batt_power = self.resolve_power_value(batt_power, self.get_battery_power)
        fallback = grid_power_net + max(0.0, batt_power)
        return max(fallback, 0.0)

    def resolve_power_value(self, value: Optional[float], reader: PowerReader) -> float:
        """Return a provided power value or read one, normalizing missing data to zero."""
        if value is None:
            value = reader()
        return value if value is not None else 0.0

    def state_float(self, value: Any) -> float:
        """Convert numeric state values loaded from JSON or D-Bus to float."""
        return float(value)

    def state_float_list(self, values: Any) -> list[float]:
        """Convert a JSON-loaded numeric list into floats."""
        return [self.state_float(value) for value in values]

    def get_max_charge_current_raw(self) -> Optional[float]:
        """Return the script-owned raw DVCC restore value, including Victron -1."""
        if self.state.get("max_charge_current_raw_set"):
            current = self.state.get("max_charge_current_raw")
            return None if current is None else float(current)
        return None

    def capture_original_dvcc_before_limit(self, current_raw: Optional[float]) -> bool:
        """Capture the pre-script DVCC limit once, immediately before restricting it."""
        if self.state.get("max_charge_current_raw_set") or current_raw is None:
            return current_raw is not None
        self.state["max_charge_current_raw"] = current_raw
        self.state["max_charge_current_raw_set"] = True
        if current_raw > 0:
            self.state["normal_charge_current"] = current_raw
        self.save_state_to_ram(force_persist=True)
        self.dbus.log(f"DVCC restore value captured: {current_raw:.1f}A")
        return True

    def clear_saved_max_charge_current_raw(self) -> None:
        """Clear script-owned DVCC restore state after restoring the previous setting."""
        updates = {
            "max_charge_current_raw": None,
            "max_charge_current_raw_set": False,
            "normal_charge_current": None,
            "charge_current_owned_by_script": False,
            "max_charge_current_script_last_set": None,
        }
        changed = any(self.state.get(key) != value for key, value in updates.items())
        if not changed:
            return
        self.state.update(updates)
        self.save_state_to_ram(force_persist=True)

    def would_restrict_charge_current(self, current_raw: float, desired_a: float) -> bool:
        """Return True when a requested positive value would make DVCC stricter."""
        if current_raw < 0:
            return desired_a >= 0
        return desired_a < (current_raw - CHARGE_LIMIT_UPDATE_THRESHOLD_A)

    def _same_charge_current(
        self,
        current_a: Optional[float],
        desired_a: Optional[float],
    ) -> bool:
        """Compare two DVCC current values using the configured change threshold."""
        if current_a is None or desired_a is None:
            return False
        return abs(float(current_a) - float(desired_a)) < CHARGE_LIMIT_UPDATE_THRESHOLD_A

    def _write_max_charge_current(self, path: str, value: float) -> bool:
        """Write DVCC MaxChargeCurrent using the integer type expected by settings."""
        if value < 0:
            return self.dbus.set_value(SERVICE_SETTINGS, path, dbus.Int32(-1))
        return self.dbus.set_value(SERVICE_SETTINGS, path, dbus.Int32(int(round(value))))

    def _remember_script_charge_current(
        self,
        desired_a: float,
        force_persist: bool = False,
    ) -> None:
        """Record the DVCC value that was last written by this controller."""
        self.state["charge_current_owned_by_script"] = True
        self.state["max_charge_current_script_last_set"] = float(round(desired_a))
        self.save_state_to_ram(force_persist=force_persist)

    def _restore_max_charge_current(
        self,
        path: str,
        current_raw: float,
        desired_a: float,
        reason: str,
        now: float,
    ) -> bool:
        """Restore DVCC and clear ownership when the restore is already or newly applied."""
        if self._same_charge_current(current_raw, desired_a):
            self.clear_saved_max_charge_current_raw()
            return True

        if not self._write_max_charge_current(path, desired_a):
            return False

        self.dbus.log(f"MaxChargeCurrent -> {desired_a:.1f}A ({reason})")
        self.last_charge_limit_set_ts = now
        self.clear_saved_max_charge_current_raw()
        return True

    def _current_charge_ownership_valid(self, current_raw: float) -> bool:
        """Return True while the actual DVCC value still matches the script-owned value."""
        if not self.state.get("charge_current_owned_by_script", False):
            return False
        last_script_set = self.state.get("max_charge_current_script_last_set")
        if last_script_set is None or self._same_charge_current(current_raw, last_script_set):
            return True
        self.dbus.log("MaxChargeCurrent changed externally; controller releases DVCC ownership")
        self.clear_saved_max_charge_current_raw()
        return False

    def _ensure_charge_current_ownership(self, current_raw: float, desired_a: float) -> bool:
        """Take DVCC ownership only when the requested value is a stricter limit."""
        if self._current_charge_ownership_valid(current_raw):
            return True
        if not self.would_restrict_charge_current(current_raw, desired_a):
            self.dbus.log(
                "MaxChargeCurrent unchanged "
                f"({current_raw:.1f}A is already equal to or stricter than {desired_a:.1f}A)"
            )
            return False
        return self.capture_original_dvcc_before_limit(current_raw)

    def _clamp_to_captured_charge_current(self, desired_a: float) -> float:
        """Never raise a script-owned limit above the captured positive restore value."""
        original_raw = self.get_max_charge_current_raw()
        if original_raw is not None and original_raw >= 0 and desired_a > original_raw:
            return float(original_raw)
        return desired_a

    def _charge_current_update_rate_limited(
        self,
        current_raw: float,
        desired_a: float,
        now: float,
    ) -> bool:
        """Rate-limit charge-current relaxations while allowing reductions immediately."""
        is_reduction = current_raw < 0 or desired_a < current_raw
        if is_reduction:
            return False
        return (now - self.last_charge_limit_set_ts) < CHARGE_LIMIT_MIN_UPDATE_INTERVAL_SECONDS

    def get_normal_charge_current(
        self,
        battery_max_current: Optional[float] = None,
    ) -> Optional[float]:
        """Determine the restore current from captured DVCC, fixed fallback, or BMS."""
        return self.first_normal_charge_current([
            self.get_max_charge_current_raw(),
            NORMAL_CHARGE_CURRENT if NORMAL_CHARGE_CURRENT != 0 else None,
            self.state.get("normal_charge_current"),
            battery_max_current,
        ])

    def first_normal_charge_current(self, candidates: Sequence[Any]) -> Optional[float]:
        """Return the first usable DVCC restore current from ordered candidates."""
        for current in candidates:
            if current is None:
                continue
            current_float = float(current)
            if current_float < 0 or current_float > 0:
                return current_float
        return None

    def compute_charge_current_limit(
        self,
        house_load: float,
        battery_max_current: Optional[float],
        voltage: Optional[float],
    ) -> Optional[float]:
        """Compute a DC charge-current limit that favors grid softness but still progresses."""
        if battery_max_current is None:
            return None
        if voltage is None or voltage <= 1:
            return self.compute_safe_charge_current(battery_max_current)

        available_ac_w = self.available_grid_charge_power(house_load)
        limit_by_grid_a = (available_ac_w * CHARGE_EFFICIENCY) / voltage
        min_progress_current = min(GRID_SOFT_MIN_CHARGE_CURRENT_A, battery_max_current)
        limit_current = min(battery_max_current, max(limit_by_grid_a, min_progress_current))
        return self.clamp_to_normal_current(int(limit_current), battery_max_current)

    def available_grid_charge_power(self, house_load: float) -> float:
        """Return AC watts available before crossing the soft grid-comfort target."""
        return max(0.0, GRID_LOAD_LIMIT - house_load - GRID_PAUSE_HEADROOM_W)

    def compute_safe_charge_current(self, battery_max_current: float) -> Optional[float]:
        """Return a conservative current when battery voltage is unavailable."""
        if SAFE_CHARGE_CURRENT_A is None or SAFE_CHARGE_CURRENT_A <= 0:
            return None
        safe_current = min(SAFE_CHARGE_CURRENT_A, battery_max_current)
        return self.clamp_to_normal_current(safe_current, battery_max_current)

    def clamp_to_normal_current(self, current: float, battery_max_current: float) -> float:
        """Clamp a computed current to known normal/BMS limits and never below zero."""
        normal_current = self.get_normal_charge_current(battery_max_current)
        if normal_current is not None and normal_current > 0 and current > normal_current:
            current = normal_current
        return max(current, 0.0)

    def set_max_charge_current(self, desired_a: Optional[float], reason: str) -> bool:
        """Set DVCC MaxChargeCurrent while preserving manual or previously captured limits."""
        if desired_a is None:
            return False
        desired_a = float(desired_a)
        max_curr_path = '/Settings/SystemSetup/MaxChargeCurrent'
        current_raw = self.read_current_max_charge_current(max_curr_path)
        if current_raw is None:
            return False
        now = time.time()
        if reason == "Restore":
            return self._restore_max_charge_current(max_curr_path, current_raw, desired_a, reason, now)
        if not self.can_apply_charge_limit(current_raw, desired_a):
            return False
        desired_a = self._clamp_to_captured_charge_current(desired_a)
        return self.write_charge_current_limit(max_curr_path, current_raw, desired_a, reason, now)

    def read_current_max_charge_current(self, max_curr_path: str) -> Optional[float]:
        """Read the current raw DVCC MaxChargeCurrent setting as float."""
        current_raw = self.dbus.get_raw_value(SERVICE_SETTINGS, max_curr_path, None)
        return None if current_raw is None else float(current_raw)

    def can_apply_charge_limit(self, current_raw: float, desired_a: float) -> bool:
        """Return True when a non-restore charge-current limit may be applied."""
        if desired_a < 0:
            return False
        return self._ensure_charge_current_ownership(current_raw, desired_a)

    def write_charge_current_limit(
        self,
        max_curr_path: str,
        current_raw: float,
        desired_a: float,
        reason: str,
        now: float,
    ) -> bool:
        """Write a script-owned DVCC limit when it differs and is not rate-limited."""
        if self._same_charge_current(current_raw, desired_a):
            self._remember_script_charge_current(desired_a)
            return True
        if self._charge_current_update_rate_limited(current_raw, desired_a, now):
            return False
        if not self._write_max_charge_current(max_curr_path, desired_a):
            return False
        self.record_charge_current_write(desired_a, reason, now)
        return True

    def record_charge_current_write(self, desired_a: float, reason: str, now: float) -> None:
        """Record and log a successful script-owned DVCC write."""
        had_ownership = self.state.get("charge_current_owned_by_script", False)
        self.dbus.log(f"MaxChargeCurrent -> {desired_a:.1f}A ({reason})")
        self.last_charge_limit_set_ts = now
        self._remember_script_charge_current(desired_a, force_persist=not had_ownership)

    def restore_normal_charge_current(self, battery_max_current: Optional[float]) -> None:
        """Restore DVCC only when a captured or configured restore value is known."""
        raw = self.get_max_charge_current_raw()
        if raw is not None:
            self.set_max_charge_current(raw, "Restore")
            return
        if NORMAL_CHARGE_CURRENT is None or NORMAL_CHARGE_CURRENT == 0:
            return
        self.set_max_charge_current(NORMAL_CHARGE_CURRENT, "Restore")

    def maybe_log_status(
        self,
        house_load: Optional[float],
        battery_max_current: Optional[float],
    ) -> None:
        """Write a periodic diagnostic snapshot without flooding the RAM log."""
        now_ts = time.time()
        if not self.status_log_due(now_ts):
            return
        max_curr_path = '/Settings/SystemSetup/MaxChargeCurrent'
        current_limit = self.dbus.get_raw_value(SERVICE_SETTINGS, max_curr_path, None)
        self.dbus.log(self.build_status_message(house_load, battery_max_current, current_limit, now_ts))
        self.state["last_status_log_ts"] = now_ts
        self.save_state_to_ram()

    def status_log_due(self, now_ts: float) -> bool:
        """Return True when the periodic status log interval has elapsed."""
        last_ts = float(self.state.get("last_status_log_ts", 0))
        return (now_ts - last_ts) >= STATUS_LOG_INTERVAL_SECONDS

    def format_watts(self, value: Optional[float]) -> str:
        """Format a nullable watt value for status logging."""
        return "n/a" if value is None else f"{value:.0f}W"

    def format_amps(self, value: Optional[float]) -> str:
        """Format a nullable ampere value for status logging."""
        return "n/a" if value is None else f"{value:.1f}A"

    def sd_status_text(self, now_ts: float) -> str:
        """Return a compact SD persistence status string."""
        if self.sd_state_file is None:
            return "SD missing"
        if now_ts < self.sd_next_try_ts:
            return f"SD backoff {int(self.sd_next_try_ts - now_ts)}s"
        if self.sd_error_count > 0:
            return f"SD errors {self.sd_error_count}"
        return "SD ok"

    def build_status_message(
        self,
        house_load: Optional[float],
        battery_max_current: Optional[float],
        current_limit: Optional[float],
        now_ts: float,
    ) -> str:
        """Build one human-readable periodic status line."""
        return (
            f"Status: HouseLoad {self.format_watts(house_load)}, "
            f"BMS Max {self.format_amps(battery_max_current)}, "
            f"MaxChargeCurrent {self.format_amps(current_limit)}, "
            f"{self.sd_status_text(now_ts)}"
        )

    def update_full_soc_tracking(self, current_soc: float, now_ts: float) -> bool:
        """Track confirmed full SoC time and active balancing progress."""
        changed = False
        delta = self.loop_delta_seconds(now_ts)
        self.state["last_loop_ts"] = now_ts

        changed = self.track_full_soc_seconds(current_soc, now_ts, delta) or changed
        changed = self.track_balancing_progress(current_soc, now_ts, delta) or changed
        return changed

    def loop_delta_seconds(self, now_ts: float) -> float:
        """Return a bounded elapsed time so suspend/restart gaps are not integrated."""
        last_ts = float(self.state.get("last_loop_ts", 0))
        if last_ts <= 0:
            return 0
        delta = now_ts - last_ts
        if delta < 0 or delta > (LOOP_INTERVAL_SECONDS * 5):
            return 0
        return delta

    def track_full_soc_seconds(self, current_soc: float, now_ts: float, delta: float) -> bool:
        """Track winter full-charge confirmation used to defer new balancing cycles."""
        if self.is_confirming_full_soc(current_soc):
            return self.add_full_soc_seconds(now_ts, delta)
        return self.reset_full_soc_seconds_if_needed()

    def is_confirming_full_soc(self, current_soc: float) -> bool:
        """Return True when current SoC contributes to winter full-charge tracking."""
        return self.is_winter_window() and current_soc >= BALANCING_FULL_SOC

    def add_full_soc_seconds(self, now_ts: float, delta: float) -> bool:
        """Accumulate full-SoC time and persist once the confirmation threshold is met."""
        prev_full = float(self.state["full_soc_seconds"])
        self.state["full_soc_seconds"] += delta
        if not self.full_soc_threshold_crossed(prev_full):
            return False
        self.state["last_full_ts"] = now_ts
        self.save_state_to_ram(force_persist=True)
        return True

    def full_soc_threshold_crossed(self, previous_seconds: float) -> bool:
        """Return True when continuous full SoC just crossed the confirmation threshold."""
        threshold_s = FULL_SOC_CONFIRM_MINUTES * 60
        return previous_seconds < threshold_s <= float(self.state["full_soc_seconds"])

    def reset_full_soc_seconds_if_needed(self) -> bool:
        """Clear full-SoC tracking when SoC drops below the confirmation threshold."""
        if self.state["full_soc_seconds"] != 0:
            self.state["full_soc_seconds"] = 0
            return True
        return False

    def track_balancing_progress(self, current_soc: float, now_ts: float, delta: float) -> bool:
        """Track timeout and success conditions for an active balancing attempt."""
        if not self.state["balancing_active"]:
            return False

        if (now_ts - float(self.state["balancing_start_ts"])) > (BALANCING_MAX_HOURS * 3600):
            self.finish_balancing_attempt(now_ts, success=False)
            self.dbus.log("Balancing aborted (timeout without sustained near-full SoC)")
            return True

        changed = self.update_balancing_full_seconds(current_soc, delta)
        if self.state["balance_full_seconds"] >= (BALANCING_DURATION_HOURS * 3600):
            self.finish_balancing_attempt(now_ts, success=True)
            self.dbus.log("Balancing completed successfully")
            return True
        return changed

    def update_balancing_full_seconds(self, current_soc: float, delta: float) -> bool:
        """Accumulate continuous near-full time while balancing is active."""
        if current_soc >= BALANCING_FULL_SOC:
            self.state["balance_full_seconds"] += delta
            return False
        if self.state["balance_full_seconds"] != 0:
            self.state["balance_full_seconds"] = 0
            return True
        return False

    def finish_balancing_attempt(self, now_ts: float, success: bool) -> None:
        """Close a balancing attempt and persist important result timestamps."""
        self.state["balancing_active"] = False
        if success:
            self.state["last_balance_ts"] = now_ts
        self.state["balance_full_seconds"] = 0
        self.state["last_balance_attempt_ts"] = now_ts
        self.save_state_to_ram(force_persist=True)

    def should_start_balancing(self, now_ts: float) -> bool:
        """Decide whether a new balancing cycle is due and allowed."""
        if self.state["balancing_active"]:
            return False
        if not self.is_balancing_due(now_ts):
            return False
        return not self.is_balance_retry_cooling_down(now_ts)

    def is_balancing_due(self, now_ts: float) -> bool:
        """Return True when the balancing interval or boot grace period has elapsed."""
        due_source_ts = self.balancing_due_source_ts()
        required_s = self.balancing_due_interval_seconds(due_source_ts)
        return (now_ts - due_source_ts) >= required_s

    def balancing_due_source_ts(self) -> float:
        """Return the timestamp from which the next balancing due time is measured."""
        if self.state["last_balance_ts"] > 0:
            return float(self.state["last_balance_ts"])
        if self.is_winter_window() and self.state["last_full_ts"] > 0:
            return float(self.state["last_full_ts"])
        return float(self.state["boot_ts"])

    def balancing_due_interval_seconds(self, due_source_ts: float) -> int:
        """Return the required delay before a balancing cycle may start."""
        if due_source_ts == self.state["boot_ts"]:
            return BALANCING_BOOT_GRACE_HOURS * 3600
        return BALANCING_INTERVAL_DAYS * 86400

    def is_balance_retry_cooling_down(self, now_ts: float) -> bool:
        """Return True when a recent failed/started attempt is still cooling down."""
        elapsed = now_ts - float(self.state["last_balance_attempt_ts"])
        return elapsed < (BALANCING_RETRY_COOLDOWN_HOURS * 3600)

    def update_pv_history(self) -> None:
        """Integrate PV power during the daily sample window and roll history."""
        now = datetime.now()
        today_str = now.strftime("%Y-%m-%d")
        now_ts = time.time()
        
        if self.state["last_sample_date"] != today_str:
            self.roll_pv_day(today_str, now_ts)

        if 9 <= now.hour < 17:
            self.collect_pv_sample(now_ts)
        else:
            self.reset_pv_sample_gap()

    def roll_pv_day(self, today_str: str, now_ts: float) -> None:
        """Store yesterday's PV average and reset the current-day integrators."""
        avg = self.compute_completed_pv_average(now_ts)
        force_persist = avg is not None
        if avg is not None:
            self.add_pv_history_value(avg)

        self.state["current_day_samples"] = []
        self.state["pv_energy_ws"] = 0.0
        self.state["pv_time_s"] = 0.0
        self.reset_pv_sample_gap()
        self.state["last_sample_date"] = today_str
        self.save_state_to_ram(force_persist=force_persist)

    def compute_completed_pv_average(self, now_ts: float) -> Optional[float]:
        """Return the previous day's PV average when there is enough valid data."""
        pv_time_s = float(self.state.get("pv_time_s", 0))
        if pv_time_s > 0:
            self.state["last_pv_integral_ts"] = now_ts
            return float(self.state.get("pv_energy_ws", 0.0)) / pv_time_s

        samples = self.state.get("current_day_samples")
        if isinstance(samples, list) and samples and self.is_pv_fallback_old_enough(now_ts):
            sample_values = self.state_float_list(samples)
            return sum(sample_values) / len(sample_values)
        return None

    def is_pv_fallback_old_enough(self, now_ts: float) -> bool:
        """Return True when simple sample averaging is old enough to trust."""
        last_valid_ts = float(self.state.get("last_pv_integral_ts", 0.0))
        if last_valid_ts <= 0:
            last_valid_ts = float(self.state.get("boot_ts", now_ts))
        return (now_ts - last_valid_ts) >= (PV_FALLBACK_MIN_VALID_AGE_DAYS * 86400)

    def add_pv_history_value(self, avg: float) -> None:
        """Append one daily PV average and keep only the transition decision window."""
        self.state["pv_history"].append(avg)
        if len(self.state["pv_history"]) > TRANSITION_DAYS:
            self.state["pv_history"].pop(0)
        self.dbus.log(f"PV daily average stored: {avg:.2f} W")

    def collect_pv_sample(self, now_ts: float) -> None:
        """Collect and integrate one PV sample inside the configured day window."""
        pv_total = self.get_total_pv_power()
        self.state["current_day_samples"].append(pv_total)
        self.integrate_pv_sample(now_ts, pv_total)
        self.state["pv_last_sample_ts"] = now_ts
        self.state["pv_last_sample_power"] = pv_total

    def integrate_pv_sample(self, now_ts: float, pv_total: float) -> None:
        """Integrate PV power using trapezoidal area between adjacent samples."""
        last_ts = float(self.state.get("pv_last_sample_ts", 0.0))
        if last_ts <= 0:
            return
        dt = now_ts - last_ts
        if 0 < dt <= (LOOP_INTERVAL_SECONDS * 5):
            last_power = float(self.state.get("pv_last_sample_power", 0.0))
            avg_power = (last_power + pv_total) / 2.0
            self.state["pv_energy_ws"] = self.state.get("pv_energy_ws", 0.0) + (avg_power * dt)
            self.state["pv_time_s"] = self.state.get("pv_time_s", 0.0) + dt

    def reset_pv_sample_gap(self) -> None:
        """Break PV integration across out-of-window or long sampling gaps."""
        self.state["pv_last_sample_ts"] = 0.0
        self.state["pv_last_sample_power"] = 0.0

    def determine_target_soc(
        self,
        now_ts: float,
        current_soc: Optional[float] = None,
    ) -> TargetMode:
        """Compute the target MinSoC from season, PV history, and balancing state."""
        now = datetime.now()
        current_date_val = now.month * 100 + now.day
        if self.mmdd_is_winter(current_date_val):
            return self.determine_winter_target(now_ts, current_soc)
        return self.determine_transition_target(current_date_val)

    def mmdd_is_winter(self, current_date_val: int) -> bool:
        """Return True when an MMDD integer falls inside the winter window."""
        return current_date_val >= WINTER_START_MMDD or current_date_val <= WINTER_END_MMDD

    def determine_transition_target(self, current_date_val: int) -> TargetMode:
        """Return the transition-window target or the default summer target."""
        if TRANS_PRE_START_MMDD <= current_date_val <= TRANS_PRE_END_MMDD:
            return self.determine_pre_winter_target()
        if TRANS_POST_START_MMDD <= current_date_val <= TRANS_POST_END_MMDD:
            return self.determine_post_winter_target()
        return DEFAULT_SOC, "Default"

    def determine_pre_winter_target(self) -> TargetMode:
        """Return the pre-winter target based on recent low-PV history."""
        if self.has_transition_history_below_threshold():
            return 40.0, "Pre-Winter Low PV"
        return DEFAULT_SOC, "Default"

    def determine_post_winter_target(self) -> TargetMode:
        """Return the post-winter guard target until PV has clearly recovered."""
        if self.has_transition_history_above_threshold():
            return DEFAULT_SOC, "Post-Winter PV Recovered"
        return 40.0, "Post-Winter Guard"

    def determine_winter_target(
        self,
        now_ts: float,
        current_soc: Optional[float] = None,
    ) -> TargetMode:
        """Return the winter target and start a balancing cycle when it is due."""
        if self.should_start_balancing(now_ts):
            self.start_balancing(now_ts)
        if self.state["balancing_active"]:
            return 100.0, "Winter Balancing"
        if self.should_use_winter_40_stage(current_soc):
            return 40.0, "Winter Low PV Stage"
        return 65.0, "Winter"

    def should_use_winter_40_stage(self, current_soc: Optional[float]) -> bool:
        """Return True when low PV history should first build a 40% winter reserve."""
        if current_soc is None:
            return False
        if current_soc >= (40.0 - SOC_HYSTERESIS):
            return False
        return self.has_transition_history_below_threshold()

    def start_balancing(self, now_ts: float) -> None:
        """Mark a new balancing cycle as active and persist the attempt timestamp."""
        self.state["balancing_active"] = True
        self.state["balancing_start_ts"] = now_ts
        self.state["balance_full_seconds"] = 0
        self.state["last_balance_attempt_ts"] = now_ts
        self.dbus.log("Starting balancing cycle")
        self.save_state_to_ram(force_persist=True)

    def transition_history_ready(self) -> Optional[list[float]]:
        """Return the recent PV history window only when it is complete."""
        hist = self.state["pv_history"]
        if not isinstance(hist, list):
            return None
        history = self.state_float_list(hist)
        if len(history) < TRANSITION_DAYS:
            return None
        return history[-TRANSITION_DAYS:]

    def has_transition_history_below_threshold(self) -> bool:
        """Return True when all recent transition PV values are below threshold."""
        hist = self.transition_history_ready()
        return hist is not None and all(val < PV_THRESHOLD for val in hist)

    def has_transition_history_above_threshold(self) -> bool:
        """Return True when all recent transition PV values are above threshold."""
        hist = self.transition_history_ready()
        return hist is not None and all(val > PV_THRESHOLD for val in hist)

    def is_charge_control_active(self) -> bool:
        """Return True when this controller is actively raising or holding SoC."""
        return bool(self.state["charging_mode_active"] or self.state["charging_paused"])

    def set_charge_state(self, active: bool, paused: bool) -> bool:
        """Set charge-control flags together and return whether either flag changed."""
        active = bool(active)
        paused = bool(paused)
        changed = (
            self.state["charging_mode_active"] != active
            or self.state["charging_paused"] != paused
        )
        self.state["charging_mode_active"] = active
        self.state["charging_paused"] = paused
        return bool(changed)

    def clear_charge_state(self) -> bool:
        """Mark charge-control as inactive and return whether the state changed."""
        return self.set_charge_state(False, False)

    def _handle_summer_manual_override(
        self,
        current_setting: float,
        now_ts: float,
        in_control_window: bool,
    ) -> bool:
        """Honor temporary manual MinSoC changes outside seasonal control windows."""
        state_changed = self.track_manual_min_soc_change(current_setting, now_ts, in_control_window)
        state_changed = self.expire_summer_override_if_needed(now_ts, in_control_window) or state_changed
        if not self.summer_override_active(now_ts, in_control_window):
            self.save_if_state_changed(state_changed)
            return False
        return self.handle_active_summer_override(now_ts, state_changed)

    def expire_summer_override_if_needed(self, now_ts: float, in_control_window: bool) -> bool:
        """Expire a summer manual override after its hold window has elapsed."""
        manual_override_until = float(self.state.get("manual_override_until_ts", 0))
        if in_control_window or manual_override_until <= 0 or now_ts < manual_override_until:
            return False
        self.state["manual_override_until_ts"] = 0
        self.dbus.log("Summer MinSoC override expired; controller returns to default")
        return True

    def summer_override_active(self, now_ts: float, in_control_window: bool) -> bool:
        """Return True when a summer manual override is currently active."""
        override_until = float(self.state.get("manual_override_until_ts", 0))
        return (not in_control_window) and override_until > now_ts

    def handle_active_summer_override(self, now_ts: float, state_changed: bool) -> bool:
        """Keep an active summer override untouched and restore normal DVCC current."""
        state_changed = self.log_active_summer_override_if_due(now_ts) or state_changed
        state_changed = self.clear_charge_state() or state_changed
        self.save_if_state_changed(state_changed)
        self.restore_normal_charge_current(self.get_battery_max_charge_current())
        return True

    def log_active_summer_override_if_due(self, now_ts: float) -> bool:
        """Periodically log remaining time for an active summer override."""
        last_override_log = float(self.state.get("last_manual_override_log_ts", 0))
        if (now_ts - last_override_log) < STATUS_LOG_INTERVAL_SECONDS:
            return False
        remaining_h = max((float(self.state["manual_override_until_ts"]) - now_ts) / 3600.0, 0.0)
        self.dbus.log(f"Summer MinSoC override active ({remaining_h:.1f}h remaining)")
        self.state["last_manual_override_log_ts"] = now_ts
        return True

    def save_if_state_changed(self, state_changed: bool) -> None:
        """Persist RAM state when a caller reports that state changed."""
        if state_changed:
            self.save_state_to_ram()

    def track_charge_deficit(self, needs_charge: bool, now_ts: float) -> bool:
        """Track how long the controller has been below target for window escalation."""
        current_start = float(self.state.get("charge_deficit_start_ts", 0))
        if needs_charge and current_start <= 0:
            self.state["charge_deficit_start_ts"] = now_ts
            return True
        if (not needs_charge) and current_start > 0:
            self.state["charge_deficit_start_ts"] = 0
            return True
        return False

    def charge_window_hours(self, now_ts: float) -> int:
        """Return the adaptive charge-window duration after unresolved deficit nights."""
        deficit_start = float(self.state.get("charge_deficit_start_ts", 0))
        if deficit_start <= 0:
            return CHARGE_WINDOW_BASE_HOURS
        elapsed_nights = int(max(0.0, now_ts - deficit_start) // 86400)
        escalation_steps = elapsed_nights // CHARGE_WINDOW_ESCALATION_NIGHTS
        multiplier = min(2 ** escalation_steps, CHARGE_WINDOW_MAX_MULTIPLIER)
        return int(CHARGE_WINDOW_BASE_HOURS * multiplier)

    def is_charge_window_active(self, now: datetime, now_ts: float) -> bool:
        """Return True when the current hour is inside the adaptive charge window."""
        duration_h = self.charge_window_hours(now_ts)
        if duration_h >= 24:
            return True
        hours_since_start = (
            (now.hour - CHARGE_WINDOW_START_HOUR) % 24
            + (now.minute / 60.0)
            + (now.second / 3600.0)
        )
        return hours_since_start < duration_h

    def should_stage_charge_target(self, target_soc: float) -> bool:
        """Return True for reserve-raising targets that should be reached in stages."""
        return target_soc > DEFAULT_SOC

    def _build_charge_context(
        self,
        current_soc: float,
        target_soc: float,
        current_setting: float,
        now: datetime,
        now_ts: float,
    ) -> ChargeContext:
        """Collect runtime measurements and flags used by charge-control decisions."""
        needs_charge = current_soc < (target_soc - SOC_HYSTERESIS)
        charge_deficit_changed = self.track_charge_deficit(needs_charge, now_ts)
        time_ok = self.charge_time_ok(now, now_ts)
        grid_power_net = self.get_grid_power_net()
        battery_power = self.get_battery_power()
        return {
            "needs_charge": needs_charge,
            "time_ok": time_ok,
            "charge_window_hours": self.charge_window_hours(now_ts),
            "charge_deficit_changed": charge_deficit_changed,
            "stage_charge_target": self.should_stage_charge_target(target_soc),
            "grid_import": self.import_only_power(grid_power_net),
            "effective_active": self.effective_charge_control_active(
                needs_charge,
                target_soc,
                current_setting,
                now_ts,
            ),
            "battery_max_current": self.get_battery_max_charge_current(),
            "battery_voltage": self.get_battery_voltage(),
            "house_load": self.get_house_load_power(grid_power_net, battery_power),
        }

    def charge_time_ok(self, now: datetime, now_ts: float) -> bool:
        """Return True when charging is allowed by window or active balancing."""
        return self.is_charge_window_active(now, now_ts) or bool(self.state["balancing_active"])

    def import_only_power(self, grid_power_net: float) -> float:
        """Clamp net grid power to import-only watts."""
        return grid_power_net if grid_power_net > 0 else 0.0

    def effective_charge_control_active(
        self,
        needs_charge: bool,
        target_soc: float,
        current_setting: float,
        now_ts: float,
    ) -> bool:
        """Return True when an active or boot-recovered SoC raise is in progress."""
        return self.is_charge_control_active() or self.boot_recover_active(
            needs_charge,
            target_soc,
            current_setting,
            now_ts,
        )

    def boot_recover_active(
        self,
        needs_charge: bool,
        target_soc: float,
        current_setting: float,
        now_ts: float,
    ) -> bool:
        """Return True when a recently restarted controller should resume a raise."""
        if self.is_charge_control_active():
            return False
        if not self.is_boot_recovery_window(now_ts):
            return False
        return needs_charge and current_setting >= (target_soc - 0.1)

    def _handle_charge_needed(
        self,
        current_limit_path: str,
        target_soc: float,
        current_soc: float,
        current_setting: float,
        context: ChargeContext,
    ) -> None:
        """Raise MinSoC while charging is allowed, otherwise hold the reached pause SoC."""
        if context["time_ok"]:
            self.resume_soc_raise(current_limit_path, target_soc, current_setting, context)
            return

        if not context.get("stage_charge_target", True):
            self.apply_unstaged_target(current_limit_path, target_soc, current_setting, context)
            return

        if self.pause_soc_raise(current_limit_path, target_soc, current_soc, current_setting):
            self.finish_pause_soc_raise(context)

    def resume_soc_raise(
        self,
        current_limit_path: str,
        target_soc: float,
        current_setting: float,
        context: ChargeContext,
    ) -> None:
        """Apply the target and charge-current policy during an allowed charge window."""
        self.log_soc_raise_resume_if_needed(target_soc, context)
        self.set_min_soc_if_changed(current_limit_path, current_setting, target_soc)
        self.set_charge_state(True, False)
        self.apply_charge_current_policy(context)
        self.maybe_log_status(context["house_load"], context["battery_max_current"])
        self.save_state_to_ram()

    def log_soc_raise_resume_if_needed(self, target_soc: float, context: ChargeContext) -> None:
        """Log when a SoC raise starts or resumes from a paused state."""
        if context["effective_active"] and not self.state["charging_paused"]:
            return
        self.dbus.log(f"Starting/resuming SoC raise to {target_soc}% (grid: {context['grid_import']:.0f}W)")

    def set_min_soc_if_changed(
        self,
        current_limit_path: str,
        current_setting: float,
        target_soc: float,
    ) -> bool:
        """Write MinSoC when the requested value differs from the current setting."""
        if abs(current_setting - target_soc) > MIN_SOC_EPSILON:
            return self.set_min_soc(current_limit_path, target_soc)
        return False

    def apply_charge_current_policy(self, context: ChargeContext) -> None:
        """Apply or restore DVCC current based on the computed charge-current limit."""
        limit_current = self.compute_charge_current_limit(
            context["house_load"],
            context["battery_max_current"],
            context["battery_voltage"],
        )
        if limit_current is None:
            self.restore_normal_charge_current(context["battery_max_current"])
            return
        self.set_max_charge_current(limit_current, "ChargeLimit")

    def apply_unstaged_target(
        self,
        current_limit_path: str,
        target_soc: float,
        current_setting: float,
        context: ChargeContext,
    ) -> None:
        """Apply targets that are intentionally not staged through charge windows."""
        self.set_min_soc_if_changed(current_limit_path, current_setting, target_soc)
        self.clear_charge_state()
        self.restore_normal_charge_current(context["battery_max_current"])
        self.save_state_to_ram()

    def finish_pause_soc_raise(self, context: ChargeContext) -> None:
        """Restore current and persist after entering or updating a paused SoC raise."""
        self.restore_normal_charge_current(context["battery_max_current"])
        self.maybe_log_status(context["house_load"], context["battery_max_current"])
        self.dbus.log(f"SoC raise paused outside charge window (grid: {context['grid_import']:.0f}W)")
        self.save_state_to_ram()

    def pause_soc_raise(
        self,
        current_limit_path: str,
        target_soc: float,
        current_soc: float,
        current_setting: float,
    ) -> bool:
        """Hold the reached SoC outside a charge window without jumping to the target."""
        pause_soc = max(min(current_soc, target_soc), DEFAULT_SOC)
        pause_written = self.write_pause_soc_if_needed(current_limit_path, current_setting, pause_soc)
        state_changed = self.set_charge_state(True, True)
        return pause_written or state_changed

    def write_pause_soc_if_needed(
        self,
        current_limit_path: str,
        current_setting: float,
        pause_soc: float,
    ) -> bool:
        """Write the pause SoC when starting or raising a paused hold point."""
        if not self.should_write_pause_soc(current_setting, pause_soc):
            return False
        return self.set_min_soc(current_limit_path, pause_soc)

    def should_write_pause_soc(self, current_setting: float, pause_soc: float) -> bool:
        """Return True when the pause SoC should be written to ESS."""
        if abs(current_setting - pause_soc) <= MIN_SOC_EPSILON:
            return False
        return (not self.state["charging_paused"]) or pause_soc > (current_setting + MIN_SOC_EPSILON)

    def _handle_charge_not_needed(
        self,
        current_limit_path: str,
        target_soc: float,
        current_setting: float,
        battery_max_current: Optional[float],
    ) -> None:
        """Handle the branch where SoC is already at or above the target."""
        if abs(current_setting - target_soc) > MIN_SOC_EPSILON:
            self.set_min_soc(current_limit_path, target_soc)
            self.dbus.log(f"SoC limit adjusted to {target_soc}%")
        if self.clear_charge_state():
            self.save_state_to_ram()
        self.restore_normal_charge_current(battery_max_current)

    def apply_soc_logic(self, target_soc: float, current_soc: float) -> None:
        """
        Apply MinSoC and DVCC charge-current control for seasonal battery protection.

        Reserve-raising targets, including the 40% protection stage, are raised
        mainly during adaptive low-load charging windows, or while recovering an
        already active raise. Outside those windows the controller may set a
        pause SoC at the already reached level, so ESS does not discharge stored
        reserve energy while avoiding an immediate jump to the full target.
        """
        current_limit_path = '/Settings/CGwacs/BatteryLife/MinimumSocLimit'
        current_setting = self.read_valid_min_soc_setting(current_limit_path)
        if current_setting is None:
            return
        current_soc = float(current_soc)
        now = datetime.now()
        now_ts = time.time()
        if self._handle_summer_manual_override(current_setting, now_ts, self.in_control_window(now)):
            return
        current_setting = self.enforce_default_min_soc_floor(current_limit_path, current_setting, target_soc)
        context = self._build_charge_context(current_soc, target_soc, current_setting, now, now_ts)
        if context.get("charge_deficit_changed"):
            self.save_state_to_ram()
        self.dispatch_soc_logic(current_limit_path, target_soc, current_soc, current_setting, context)

    def read_valid_min_soc_setting(self, current_limit_path: str) -> Optional[float]:
        """Read and validate the Victron ESS MinimumSocLimit setting."""
        current_setting = self.dbus.get_raw_value(SERVICE_SETTINGS, current_limit_path, None)
        if current_setting is not None and 0 <= current_setting <= 100:
            return float(current_setting)
        self.log_invalid_min_soc_if_due()
        return None

    def log_invalid_min_soc_if_due(self) -> None:
        """Rate-limit logs for invalid or missing ESS MinSoC settings."""
        now = time.time()
        last_log = float(self.state.get("last_min_soc_invalid_log_ts", 0))
        if (now - last_log) < SOC_INVALID_LOG_INTERVAL_SECONDS:
            return
        self.dbus.log("MinSoC path invalid/missing; skipping cycle")
        self.state["last_min_soc_invalid_log_ts"] = now

    def in_control_window(self, now: datetime) -> bool:
        """Return True when seasonal control windows are active."""
        return self.is_winter_window(now) or self.is_pv_history_window(now)

    def enforce_default_min_soc_floor(
        self,
        current_limit_path: str,
        current_setting: float,
        target_soc: float,
    ) -> float:
        """Raise MinSoC to the script default floor before applying higher targets."""
        if current_setting >= DEFAULT_SOC or target_soc < DEFAULT_SOC:
            return current_setting
        self.set_min_soc(current_limit_path, DEFAULT_SOC)
        return DEFAULT_SOC

    def dispatch_soc_logic(
        self,
        current_limit_path: str,
        target_soc: float,
        current_soc: float,
        current_setting: float,
        context: ChargeContext,
    ) -> None:
        """Dispatch one SoC cycle to either the charge-needed or not-needed branch."""
        if context["needs_charge"]:
            self._handle_charge_needed(current_limit_path, target_soc, current_soc, current_setting, context)
            return
        self._handle_charge_not_needed(
            current_limit_path,
            target_soc,
            current_setting,
            context["battery_max_current"],
        )

    def read_current_soc(self) -> Optional[float]:
        """Read and validate the current battery SoC measurement."""
        current_soc = self.dbus.get_raw_value(SERVICE_SYSTEM, '/Dc/Battery/Soc', None)
        if current_soc is None or current_soc < 0 or current_soc > 100:
            now = time.time()
            last_log = float(self.state.get("last_soc_invalid_log_ts", 0))
            if (now - last_log) >= SOC_INVALID_LOG_INTERVAL_SECONDS:
                self.dbus.log("SoC invalid/missing; skipping cycle")
                self.state["last_soc_invalid_log_ts"] = now
            return None
        return float(current_soc)

    def log_mode_change(self, mode: str, target: float) -> None:
        """Persist and log seasonal mode changes."""
        if mode == self.state.get("last_mode"):
            return
        self.dbus.log(f"Mode: {mode} (Target {target}%)")
        self.state["last_mode"] = mode
        self.save_state_to_ram()

    def load_sd_state_window(self) -> None:
        """Load the partial SD state when entering a seasonal SD window."""
        self.refresh_sd_paths(force=True)
        sd_data = self.read_state_file(self.sd_state_file)
        if isinstance(sd_data, dict):
            if self.merge_sd_state(sd_data):
                self.save_state_to_ram()
                self.dbus.log("SD window active: newer state loaded from SD")
            else:
                self.dbus.log("SD window active: SD state found but RAM is newer")
        self.sd_window_active = True

    def merge_sd_state(self, sd_data: State) -> bool:
        """Merge only newer persisted SD subset into the current RAM state."""
        return self.merge_sd_state_if_newer(self.state, sd_data, self.state)

    def update_sd_window_state(self) -> None:
        """Refresh SD path/state when the seasonal persistence window changes."""
        sd_window_now = self.is_sd_window()
        if sd_window_now:
            self.refresh_sd_paths()
        self.apply_sd_window_transition(sd_window_now)

    def apply_sd_window_transition(self, sd_window_now: bool) -> None:
        """Apply entry or exit side effects for the seasonal SD window."""
        if sd_window_now and not self.sd_window_active:
            self.load_sd_state_window()
            return
        if (not sd_window_now) and self.sd_window_active:
            self.sd_window_active = False

    def run_once(self) -> bool:
        """Run one controller iteration; return False when SoC is invalid."""
        self.update_sd_window_state()
        self.update_pv_history()

        now_ts = time.time()
        current_soc = self.read_current_soc()
        if current_soc is None:
            return False

        if self.update_full_soc_tracking(current_soc, now_ts):
            self.save_state_to_ram()

        target, mode = self.determine_target_soc(now_ts, current_soc)
        self.log_mode_change(mode, target)
        self.apply_soc_logic(target, current_soc)
        return True

    def run(self) -> None:  # pragma: no cover
        """Main loop: update PV, compute targets, apply logic, and sleep."""
        sd_mode = "seasonal SD available" if self.sd_state_file else "RAM-only"
        self.dbus.log(f"ESS winter controller started ({sd_mode})")
        while True:
            try:
                self.run_once()
                time.sleep(LOOP_INTERVAL_SECONDS)
            except Exception as e:
                self.dbus.log(f"Main loop error: {e}")
                time.sleep(LOOP_INTERVAL_SECONDS)

if __name__ == "__main__":  # pragma: no cover
    controller = WinterController()
    controller.run()

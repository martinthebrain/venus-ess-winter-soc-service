# SPDX-License-Identifier: GPL-3.0-or-later
from __future__ import annotations

import time
from typing import Any, Optional, Sequence

import dbus

from .base import ControllerMixinBase
from .config import *  # noqa: F403
from .paths import MAX_CHARGE_CURRENT_PATH

class DvccMixin(ControllerMixinBase):
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
        max_curr_path = MAX_CHARGE_CURRENT_PATH
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
        max_curr_path = MAX_CHARGE_CURRENT_PATH
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

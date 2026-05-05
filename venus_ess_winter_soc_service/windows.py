# SPDX-License-Identifier: GPL-3.0-or-later
from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Optional

from .base import ControllerMixinBase
from .config import *  # noqa: F403

class WindowsMixin(ControllerMixinBase):
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

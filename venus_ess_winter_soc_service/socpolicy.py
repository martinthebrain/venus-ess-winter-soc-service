# SPDX-License-Identifier: GPL-3.0-or-later
from __future__ import annotations

import time
from datetime import datetime
from typing import Optional, cast

from .base import ControllerMixinBase
from .config import *  # noqa: F403
from .paths import MIN_SOC_PATH

class SocPolicyMixin(ControllerMixinBase):
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
        remaining_h = max((float(self.state["manual_override_until_ts"]) - now_ts) / float(SECONDS_PER_HOUR), 0.0)
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
        elapsed_nights = int(max(0.0, now_ts - deficit_start) // SECONDS_PER_DAY)
        escalation_steps = elapsed_nights // CHARGE_WINDOW_ESCALATION_NIGHTS
        multiplier = min(2 ** escalation_steps, CHARGE_WINDOW_MAX_MULTIPLIER)
        return int(CHARGE_WINDOW_BASE_HOURS * multiplier)

    def is_charge_window_active(self, now: datetime, now_ts: float) -> bool:
        """Return True when the current hour is inside the adaptive charge window."""
        duration_h = self.charge_window_hours(now_ts)
        if duration_h >= FULL_DAY_HOURS:
            return True
        hours_since_start = (
            (now.hour - CHARGE_WINDOW_START_HOUR) % FULL_DAY_HOURS
            + (now.minute / 60.0)
            + (now.second / float(SECONDS_PER_HOUR))
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
        return needs_charge and current_setting >= (target_soc - BOOT_RECOVERY_TARGET_MATCH_EPSILON)

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
            return cast(bool, self.set_min_soc(current_limit_path, target_soc))
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
        return cast(bool, self.set_min_soc(current_limit_path, pause_soc))

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
        current_limit_path = MIN_SOC_PATH
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
        if current_setting is not None and MIN_VALID_SOC <= current_setting <= MAX_VALID_SOC:
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
        return cast(bool, self.is_winter_window(now) or self.is_pv_history_window(now))

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

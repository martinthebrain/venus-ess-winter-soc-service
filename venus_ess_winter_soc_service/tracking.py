# SPDX-License-Identifier: GPL-3.0-or-later
from __future__ import annotations

import time
from datetime import datetime
from typing import Optional, cast

from .base import ControllerMixinBase
from .config import *  # noqa: F403

class TrackingMixin(ControllerMixinBase):
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

        if (now_ts - float(self.state["balancing_start_ts"])) > (BALANCING_MAX_HOURS * SECONDS_PER_HOUR):
            self.finish_balancing_attempt(now_ts, success=False)
            self.dbus.log("Balancing aborted (timeout without sustained near-full SoC)")
            return True

        changed = self.update_balancing_full_seconds(current_soc, delta)
        if self.state["balance_full_seconds"] >= (BALANCING_DURATION_HOURS * SECONDS_PER_HOUR):
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
            return BALANCING_BOOT_GRACE_HOURS * SECONDS_PER_HOUR
        return BALANCING_INTERVAL_DAYS * SECONDS_PER_DAY

    def is_balance_retry_cooling_down(self, now_ts: float) -> bool:
        """Return True when a recent failed/started attempt is still cooling down."""
        elapsed = now_ts - float(self.state["last_balance_attempt_ts"])
        return elapsed < (BALANCING_RETRY_COOLDOWN_HOURS * SECONDS_PER_HOUR)

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
            sample_values = cast(list[float], self.state_float_list(samples))
            return sum(sample_values) / len(sample_values)
        return None

    def is_pv_fallback_old_enough(self, now_ts: float) -> bool:
        """Return True when simple sample averaging is old enough to trust."""
        last_valid_ts = float(self.state.get("last_pv_integral_ts", 0.0))
        if last_valid_ts <= 0:
            last_valid_ts = float(self.state.get("boot_ts", now_ts))
        return (now_ts - last_valid_ts) >= (PV_FALLBACK_MIN_VALID_AGE_DAYS * SECONDS_PER_DAY)

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
            return TRANSITION_GUARD_SOC, "Pre-Winter Low PV"
        return DEFAULT_SOC, "Default"

    def determine_post_winter_target(self) -> TargetMode:
        """Return the post-winter guard target until PV has clearly recovered."""
        if self.has_transition_history_above_threshold():
            return DEFAULT_SOC, "Post-Winter PV Recovered"
        return TRANSITION_GUARD_SOC, "Post-Winter Guard"

    def determine_winter_target(
        self,
        now_ts: float,
        current_soc: Optional[float] = None,
    ) -> TargetMode:
        """Return the winter target and start a balancing cycle when it is due."""
        if self.should_start_balancing(now_ts):
            self.start_balancing(now_ts)
        if self.state["balancing_active"]:
            return BALANCING_TARGET_SOC, "Winter Balancing"
        if self.should_use_winter_40_stage(current_soc):
            return TRANSITION_GUARD_SOC, "Winter Low PV Stage"
        return WINTER_TARGET_SOC, "Winter"

    def should_use_winter_40_stage(self, current_soc: Optional[float]) -> bool:
        """Return True when low PV history should first build a 40% winter reserve."""
        if current_soc is None:
            return False
        if current_soc >= (TRANSITION_GUARD_SOC - SOC_HYSTERESIS):
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
        return cast(list[float], history[-TRANSITION_DAYS:])

    def has_transition_history_below_threshold(self) -> bool:
        """Return True when all recent transition PV values are below threshold."""
        hist = self.transition_history_ready()
        return hist is not None and all(val < PV_THRESHOLD for val in hist)

    def has_transition_history_above_threshold(self) -> bool:
        """Return True when all recent transition PV values are above threshold."""
        hist = self.transition_history_ready()
        return hist is not None and all(val > PV_THRESHOLD for val in hist)

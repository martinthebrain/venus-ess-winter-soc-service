# SPDX-License-Identifier: GPL-3.0-or-later
from __future__ import annotations

import time
from typing import Optional, cast

from .base import ControllerMixinBase
from .config import *  # noqa: F403
from .config import State

class RuntimeMixin(ControllerMixinBase):
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
            sd_state = cast(State, sd_data)
            if self.merge_sd_state(sd_state):
                self.save_state_to_ram()
                self.dbus.log("SD window active: newer state loaded from SD")
            else:
                self.dbus.log("SD window active: SD state found but RAM is newer")
        self.sd_window_active = True

    def merge_sd_state(self, sd_data: State) -> bool:
        """Merge only newer persisted SD subset into the current RAM state."""
        return cast(bool, self.merge_sd_state_if_newer(self.state, sd_data, self.state))

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

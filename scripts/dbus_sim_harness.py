# SPDX-License-Identifier: GPL-3.0-or-later
from __future__ import annotations

import threading
from datetime import datetime
from typing import Any, Optional

from dbus_sim_core import DbusWrite, FakeDbus, M, ScenarioOutcome, simulated_date


def failed_check_messages(checks: list[tuple[bool, str]]) -> list[str]:
    """Return messages for failed scenario checks."""
    return [message for ok, message in checks if not ok]


def append_recent_logs(details: list[str], dbus: Optional[FakeDbus]) -> None:
    """Append recent simulated D-Bus logs when a scenario failed."""
    if details and dbus is not None and dbus.logs:
        details.append("recent logs: " + " | ".join(dbus.logs[-5:]))

class Harness:
    """Factory and assertion helpers for controller scenario tests."""

    min_soc_path = M.MIN_SOC_PATH
    max_charge_path = M.MAX_CHARGE_CURRENT_PATH
    soc_path = M.BATTERY_SOC_PATH
    battery_power_path = M.BATTERY_POWER_PATH
    battery_voltage_path = M.BATTERY_VOLTAGE_PATH
    bms_current_path = M.BMS_MAX_CHARGE_CURRENT_PATH

    def make_dbus(
        self,
        *,
        soc: Optional[float] = 70.0,
        min_soc: float = 10.0,
        max_charge_current: float = -1.0,
        battery_max_current: Optional[float] = 200.0,
        voltage: Optional[float] = 52.0,
        battery_power: float = 0.0,
        house_load: float = 1500.0,
        pv_power: float = 0.0,
    ) -> FakeDbus:
        """Create a simulated Venus D-Bus state."""
        dbus = FakeDbus()
        self.set_raw(dbus, M.SERVICE_SETTINGS, self.min_soc_path, min_soc)
        self.set_raw(dbus, M.SERVICE_SETTINGS, self.max_charge_path, max_charge_current)
        self.set_raw(dbus, M.SERVICE_SYSTEM, self.soc_path, soc)
        self.set_value(dbus, M.SERVICE_SYSTEM, self.battery_power_path, battery_power)
        self.set_value(dbus, M.SERVICE_SYSTEM, self.battery_voltage_path, voltage)
        for phase in M.PHASES:
            self.set_value(dbus, M.SERVICE_SYSTEM, M.AC_GRID_POWER_PATH.format(phase=phase), house_load / 3.0)
            self.set_value(
                dbus,
                M.SERVICE_SYSTEM,
                M.AC_CONSUMPTION_ON_INPUT_POWER_PATH.format(phase=phase),
                house_load / 3.0,
            )
            self.set_value(dbus, M.SERVICE_SYSTEM, M.AC_PV_ON_GRID_POWER_PATH.format(phase=phase), pv_power / 3.0)
            self.set_value(dbus, M.SERVICE_SYSTEM, M.AC_PV_ON_OUTPUT_POWER_PATH.format(phase=phase), 0.0)
        self.set_value(dbus, M.SERVICE_SYSTEM, M.DC_PV_POWER_PATH, 0.0)
        if battery_max_current is not None:
            dbus.services.append(M.PREFERRED_BATTERY_SERVICE)
            self.set_value(dbus, M.PREFERRED_BATTERY_SERVICE, self.bms_current_path, battery_max_current)
        return dbus

    def set_value(
        self,
        dbus: FakeDbus,
        service: str,
        path: str,
        value: Optional[float],
    ) -> None:
        """Set one simulated measured D-Bus value."""
        dbus.values[(service, path)] = value

    def set_raw(
        self,
        dbus: FakeDbus,
        service: str,
        path: str,
        value: Optional[float],
    ) -> None:
        """Set one simulated raw D-Bus value."""
        dbus.raw_values[(service, path)] = value
        dbus.values[(service, path)] = value

    def make_controller(self, dbus: FakeDbus) -> Any:
        """Create a controller instance without touching real D-Bus or files."""
        controller = object.__new__(M.WinterController)
        controller.dbus = dbus
        controller.state = controller.default_state()
        controller.sd_last_persist_ts = 0.0
        controller.sd_error_count = 0
        controller.sd_next_try_ts = 0.0
        controller.sd_last_signature = None
        controller.sd_pending_signature = None
        controller.sd_pending_fsync = False
        controller.sd_window_active = False
        controller.sd_card_path = None
        controller.sd_state_dir = None
        controller.sd_state_file = None
        controller.sd_info = "simulated"
        controller.sd_last_lookup_ts = 0.0
        controller.last_charge_limit_set_ts = 0.0
        controller.sd_write_lock = threading.Lock()
        controller.sd_write_event = threading.Event()
        controller.sd_write_pending = None
        controller.sd_write_inflight = False
        controller.save_counter = 0

        def save_state_to_ram(force_persist: bool = False) -> None:
            controller.save_counter += 1
            controller.state["ts"] = float(controller.save_counter)
            if force_persist:
                controller.state["last_force_persist"] = True

        controller.save_state_to_ram = save_state_to_ram
        controller.refresh_sd_paths = lambda force=False: None
        controller.persist_state_to_sd = lambda force_persist=False: None
        return controller

    def run_once_at(self, controller: Any, when: datetime) -> bool:
        """Run one controller iteration at a simulated wall-clock date."""
        with simulated_date(when):
            return bool(controller.run_once())

    def raw(self, dbus: FakeDbus, service: str, path: str) -> Optional[float]:
        """Read one raw simulated value."""
        return dbus.raw_values.get((service, path))

    def writes_to(self, dbus: FakeDbus, path: str) -> list[DbusWrite]:
        """Return all simulated writes to one D-Bus path."""
        return [write for write in dbus.writes if write.path == path]

    def scenario_result(
        self,
        name: str,
        checks: list[tuple[bool, str]],
        dbus: Optional[FakeDbus] = None,
    ) -> ScenarioOutcome:
        """Build a result from boolean checks and optional log context."""
        details = failed_check_messages(checks)
        append_recent_logs(details, dbus)
        return ScenarioOutcome(name=name, passed=not details, details=details)


H = Harness()


# SPDX-License-Identifier: GPL-3.0-or-later
from __future__ import annotations

import threading
from datetime import datetime
from typing import Any, Optional

from live_dbus_core import (
    BUS_ITEM_INTERFACE,
    CONTROL_FAIL_MAX_CHARGE_PATH,
    CONTROL_FAIL_MIN_SOC_PATH,
    FAKE_BATTERY_SERVICE,
    FAKE_SETTINGS_SERVICE,
    FAKE_SYSTEM_SERVICE,
    LiveDbusStore,
    LiveWrite,
    M,
    ScenarioOutcome,
    configure_controller_services,
    import_live_dbus_modules,
    simulated_date,
)

class LiveHarness:
    """Controller factory and assertion helpers for live D-Bus scenarios."""

    min_soc_path = M.MIN_SOC_PATH
    max_charge_path = M.MAX_CHARGE_CURRENT_PATH
    soc_path = M.BATTERY_SOC_PATH
    bms_current_path = M.BMS_MAX_CHARGE_CURRENT_PATH

    def __init__(self, store: LiveDbusStore) -> None:
        """Keep access to the live D-Bus store."""
        self.store = store

    def reset_values(
        self,
        *,
        soc: Optional[float],
        min_soc: float = 10.0,
        max_charge_current: float = -1.0,
        battery_max_current: Optional[float] = 200.0,
        voltage: Optional[float] = 52.0,
        house_load: float = 1500.0,
        pv_power: float = 0.0,
    ) -> None:
        """Reset simulated D-Bus values before one scenario."""
        self.store.clear_writes()
        self.store.logs.clear()
        self.store.fail_writes_for.clear()
        self.set_value(FAKE_SETTINGS_SERVICE, self.min_soc_path, min_soc)
        self.set_value(FAKE_SETTINGS_SERVICE, self.max_charge_path, max_charge_current)
        self.set_value(FAKE_SETTINGS_SERVICE, CONTROL_FAIL_MIN_SOC_PATH, 0.0)
        self.set_value(FAKE_SETTINGS_SERVICE, CONTROL_FAIL_MAX_CHARGE_PATH, 0.0)
        self.set_value(FAKE_SYSTEM_SERVICE, self.soc_path, soc)
        self.set_value(FAKE_SYSTEM_SERVICE, M.BATTERY_POWER_PATH, 0.0)
        self.set_value(FAKE_SYSTEM_SERVICE, M.BATTERY_VOLTAGE_PATH, voltage)
        self.set_value(FAKE_SYSTEM_SERVICE, M.DC_PV_POWER_PATH, 0.0)
        self.set_value(FAKE_BATTERY_SERVICE, self.bms_current_path, battery_max_current)
        for phase in M.PHASES:
            self.set_value(FAKE_SYSTEM_SERVICE, M.AC_GRID_POWER_PATH.format(phase=phase), house_load / 3.0)
            self.set_value(FAKE_SYSTEM_SERVICE, M.AC_CONSUMPTION_ON_INPUT_POWER_PATH.format(phase=phase), house_load / 3.0)
            self.set_value(FAKE_SYSTEM_SERVICE, M.AC_CONSUMPTION_POWER_PATH.format(phase=phase), None)
            self.set_value(FAKE_SYSTEM_SERVICE, M.AC_PV_ON_GRID_POWER_PATH.format(phase=phase), pv_power / 3.0)
            self.set_value(FAKE_SYSTEM_SERVICE, M.AC_PV_ON_OUTPUT_POWER_PATH.format(phase=phase), 0.0)

    def set_value(self, service: str, path: str, value: Optional[float]) -> None:
        """Set one simulated value."""
        self.store.set(service, path, value)

    def get_value(self, service: str, path: str) -> Optional[float]:
        """Get one simulated value."""
        return self.store.get(service, path)

    def make_controller(self) -> Any:
        """Create a controller that talks to the live fake D-Bus services."""
        configure_controller_services()
        controller = object.__new__(M.WinterController)
        controller.dbus = M.DBusInterface()
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
        controller.sd_info = "live simulated"
        controller.sd_last_lookup_ts = 0.0
        controller.last_charge_limit_set_ts = 0.0
        controller.sd_write_lock = threading.Lock()
        controller.sd_write_event = threading.Event()
        controller.sd_write_pending = None
        controller.sd_write_inflight = False
        controller.refresh_sd_paths = lambda force=False: None
        controller.persist_state_to_sd = lambda force_persist=False: None
        return controller

    def run_once_at(self, controller: Any, when: datetime) -> bool:
        """Run one controller iteration at a simulated date."""
        with simulated_date(when):
            return bool(controller.run_once())

    def writes_to(self, path: str) -> list[LiveWrite]:
        """Return all live D-Bus writes to a path."""
        return [write for write in self.store.writes if write.path == path]

    def outcome(self, name: str, checks: list[tuple[bool, str]]) -> ScenarioOutcome:
        """Build one scenario result."""
        details = [message for ok, message in checks if not ok]
        if details and self.store.logs:
            details.append("store logs: " + " | ".join(self.store.logs[-5:]))
        return ScenarioOutcome(name, not details, details)


class RemoteHarness:
    """Scenario helper that talks to fake services from a separate process."""

    min_soc_path = LiveHarness.min_soc_path
    max_charge_path = LiveHarness.max_charge_path
    soc_path = LiveHarness.soc_path
    bms_current_path = LiveHarness.bms_current_path

    def __init__(self) -> None:
        """Connect to the system bus as a D-Bus client."""
        dbus, _dbus_service, _dbus_glib, _glib = import_live_dbus_modules()
        self.dbus = dbus
        self.bus = dbus.SystemBus()

    def bus_item(self, service: str, path: str) -> Any:
        """Return a proxy for one fake BusItem path."""
        return self.bus.get_object(service, path, introspect=False)

    def get_value(self, service: str, path: str) -> Optional[float]:
        """Read one value from the fake services."""
        value = self.bus_item(service, path).GetValue(
            dbus_interface=BUS_ITEM_INTERFACE,
            timeout=2.0,
        )
        try:
            return float(value)
        except Exception:
            return None

    def set_value(self, service: str, path: str, value: Optional[float]) -> None:
        """Write one value to the fake services."""
        if value is None:
            value = -1.0
        if path.endswith("/MaxChargeCurrent"):
            dbus_value = self.dbus.Int32(int(round(value)))
        else:
            dbus_value = self.dbus.Double(float(value))
        self.bus_item(service, path).SetValue(
            dbus_value,
            dbus_interface=BUS_ITEM_INTERFACE,
            timeout=2.0,
        )

    def reset_values(
        self,
        *,
        soc: Optional[float],
        min_soc: float = 10.0,
        max_charge_current: float = -1.0,
        battery_max_current: Optional[float] = 200.0,
        voltage: Optional[float] = 52.0,
        house_load: float = 1500.0,
        pv_power: float = 0.0,
    ) -> None:
        """Reset fake services through D-Bus before one client scenario."""
        self.set_value(FAKE_SETTINGS_SERVICE, self.min_soc_path, min_soc)
        self.set_value(FAKE_SETTINGS_SERVICE, self.max_charge_path, max_charge_current)
        self.set_value(FAKE_SETTINGS_SERVICE, CONTROL_FAIL_MIN_SOC_PATH, 0.0)
        self.set_value(FAKE_SETTINGS_SERVICE, CONTROL_FAIL_MAX_CHARGE_PATH, 0.0)
        self.set_value(FAKE_SYSTEM_SERVICE, self.soc_path, soc)
        self.set_value(FAKE_SYSTEM_SERVICE, M.BATTERY_POWER_PATH, 0.0)
        self.set_value(FAKE_SYSTEM_SERVICE, M.BATTERY_VOLTAGE_PATH, voltage)
        self.set_value(FAKE_SYSTEM_SERVICE, M.DC_PV_POWER_PATH, 0.0)
        self.set_value(FAKE_BATTERY_SERVICE, self.bms_current_path, battery_max_current)
        for phase in M.PHASES:
            self.set_value(FAKE_SYSTEM_SERVICE, M.AC_GRID_POWER_PATH.format(phase=phase), house_load / 3.0)
            self.set_value(FAKE_SYSTEM_SERVICE, M.AC_CONSUMPTION_ON_INPUT_POWER_PATH.format(phase=phase), house_load / 3.0)
            self.set_value(FAKE_SYSTEM_SERVICE, M.AC_CONSUMPTION_POWER_PATH.format(phase=phase), None)
            self.set_value(FAKE_SYSTEM_SERVICE, M.AC_PV_ON_GRID_POWER_PATH.format(phase=phase), pv_power / 3.0)
            self.set_value(FAKE_SYSTEM_SERVICE, M.AC_PV_ON_OUTPUT_POWER_PATH.format(phase=phase), 0.0)

    def make_controller(self) -> Any:
        """Create a controller client that reads the fake service names."""
        configure_controller_services()
        controller = object.__new__(M.WinterController)
        controller.dbus = M.DBusInterface()
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
        controller.sd_info = "remote live simulated"
        controller.sd_last_lookup_ts = 0.0
        controller.last_charge_limit_set_ts = 0.0
        controller.sd_write_lock = threading.Lock()
        controller.sd_write_event = threading.Event()
        controller.sd_write_pending = None
        controller.sd_write_inflight = False
        controller.refresh_sd_paths = lambda force=False: None
        controller.persist_state_to_sd = lambda force_persist=False: None
        return controller

    def fail_min_soc_writes(self, enabled: bool) -> None:
        """Enable or disable simulated MinSoC SetValue failures."""
        self.set_value(FAKE_SETTINGS_SERVICE, CONTROL_FAIL_MIN_SOC_PATH, 1.0 if enabled else 0.0)

    def fail_max_charge_writes(self, enabled: bool) -> None:
        """Enable or disable simulated MaxChargeCurrent SetValue failures."""
        self.set_value(FAKE_SETTINGS_SERVICE, CONTROL_FAIL_MAX_CHARGE_PATH, 1.0 if enabled else 0.0)

    def make_controller_with_missing_system_service(self) -> Any:
        """Create a controller pointing at a missing system service."""
        controller = self.make_controller()
        configure_controller_services(system_service=f"{FAKE_SYSTEM_SERVICE}.missing")
        return controller

    def make_sd_controller(self) -> Any:
        """Create a controller with local SD writer state for filesystem checks."""
        controller = self.make_controller()
        controller.sd_write_lock = threading.Lock()
        controller.sd_write_event = threading.Event()
        controller.sd_write_pending = None
        controller.sd_write_inflight = False
        return controller

    def run_once_at(self, controller: Any, when: datetime) -> bool:
        """Run one controller iteration at a simulated date."""
        with simulated_date(when):
            return bool(controller.run_once())

    def outcome(self, name: str, checks: list[tuple[bool, str]]) -> ScenarioOutcome:
        """Build one remote scenario result."""
        return ScenarioOutcome(name, all(ok for ok, _message in checks), [message for ok, message in checks if not ok])


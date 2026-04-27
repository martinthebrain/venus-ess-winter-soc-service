#!/usr/bin/env python3
"""Publish simulated Victron D-Bus services and run live controller scenarios.

This testbed uses the real system D-Bus, but it owns only service names ending
in ``.sim``. It is intended for a disposable Venus OS test device. The normal
Victron service names are not touched unless the controller is explicitly
started with the environment variables printed by ``--serve``.
"""

from __future__ import annotations

import argparse
import importlib
import importlib.util
import os
import subprocess
import sys
import threading
import time
import types
from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Protocol, cast


ROOT = Path(__file__).resolve().parents[1]
SOC_SCRIPT = ROOT / "socSteuerung.py"
FAKE_SETTINGS_SERVICE = "com.victronenergy.settings.sim"
FAKE_SYSTEM_SERVICE = "com.victronenergy.system.sim"
FAKE_BATTERY_SERVICE = "com.victronenergy.battery.sim"
BUS_ITEM_INTERFACE = "com.victronenergy.BusItem"
DBUS_REQUEST_NAME_REPLY_PRIMARY_OWNER = 1
CONTROL_FAIL_MIN_SOC_PATH = "/Sim/FailWrites/MinimumSocLimit"
CONTROL_FAIL_MAX_CHARGE_PATH = "/Sim/FailWrites/MaxChargeCurrent"


class ControllerModule(Protocol):
    """Typed subset of the dynamically loaded controller module."""

    SERVICE_SETTINGS: str
    SERVICE_SYSTEM: str
    PREFERRED_BATTERY_SERVICE: str
    GRID_SOFT_MIN_CHARGE_CURRENT_A: float
    WinterController: type[Any]
    DBusInterface: type[Any]
    datetime: Any


def install_fake_dbus_for_import_if_needed() -> None:
    """Install a tiny dbus module only so socSteuerung.py can be imported locally."""
    if "dbus" in sys.modules:
        return
    try:
        __import__("dbus")
        return
    except Exception:
        pass

    fake = types.ModuleType("dbus")
    setattr(fake, "__ess_fake_dbus__", True)
    setattr(fake, "Boolean", bool)
    setattr(fake, "Int16", int)
    setattr(fake, "UInt16", int)
    setattr(fake, "Int32", int)
    setattr(fake, "UInt32", int)
    setattr(fake, "Int64", int)
    setattr(fake, "UInt64", int)
    setattr(fake, "Double", float)
    setattr(fake, "Byte", int)
    setattr(fake, "SystemBus", object)
    sys.modules["dbus"] = fake


def load_controller_module() -> ControllerModule:
    """Load socSteuerung.py without starting its main loop."""
    install_fake_dbus_for_import_if_needed()
    module_name = "socsteuerung_live_dbus_testbed"
    if module_name in sys.modules:
        return cast(ControllerModule, sys.modules[module_name])
    spec = importlib.util.spec_from_file_location(module_name, SOC_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {SOC_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return cast(ControllerModule, module)


M = load_controller_module()


ValueKey = tuple[str, str]


@dataclass
class LiveWrite:
    """One write received by the simulated D-Bus services."""

    service: str
    path: str
    value: float


def value_map() -> dict[ValueKey, Optional[float]]:
    """Return an empty D-Bus value map."""
    return {}


def write_list() -> list[LiveWrite]:
    """Return an empty D-Bus write list."""
    return []


def log_list() -> list[str]:
    """Return an empty log list."""
    return []


def key_set() -> set[ValueKey]:
    """Return an empty path-key set."""
    return set()


def object_list() -> list[Any]:
    """Return an empty list for dynamic D-Bus objects."""
    return []


@dataclass
class LiveDbusStore:
    """Shared state behind the simulated D-Bus services."""

    values: dict[ValueKey, Optional[float]] = field(default_factory=value_map)
    writes: list[LiveWrite] = field(default_factory=write_list)
    logs: list[str] = field(default_factory=log_list)
    fail_writes_for: set[ValueKey] = field(default_factory=key_set)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def get(self, service: str, path: str) -> Optional[float]:
        """Read one simulated D-Bus value."""
        with self.lock:
            return self.values.get((service, path))

    def get_variant(self, service: str, path: str, dbus_module: Any) -> Any:
        """Read one simulated value wrapped as a D-Bus variant."""
        value = self.get(service, path)
        if path.startswith("/Sim/"):
            return dbus_module.Int32(0, variant_level=1)
        if value is None:
            return dbus_module.Array([], signature="d", variant_level=1)
        if path.endswith("/MaxChargeCurrent"):
            return dbus_module.Int32(int(round(value)), variant_level=1)
        return dbus_module.Double(float(value), variant_level=1)

    def set(self, service: str, path: str, value: Optional[float]) -> None:
        """Set one simulated D-Bus value."""
        with self.lock:
            self.values[(service, path)] = value

    def write(self, service: str, path: str, value: Any) -> int:
        """Write one simulated D-Bus value and return a Victron-style status."""
        key = (service, path)
        with self.lock:
            if self.handle_control_write(service, path, value):
                return 0
            if key in self.fail_writes_for:
                self.logs.append(f"Simulated write failure for {service}{path}")
                return 1
            numeric = float(value)
            self.values[key] = numeric
            self.writes.append(LiveWrite(service, path, numeric))
            return 0

    def handle_control_write(self, service: str, path: str, value: Any) -> bool:
        """Apply writes to testbed control paths."""
        if service != FAKE_SETTINGS_SERVICE:
            return False
        if path == CONTROL_FAIL_MIN_SOC_PATH:
            self.toggle_failure((FAKE_SETTINGS_SERVICE, LiveHarness.min_soc_path), value)
            return True
        if path == CONTROL_FAIL_MAX_CHARGE_PATH:
            self.toggle_failure((FAKE_SETTINGS_SERVICE, LiveHarness.max_charge_path), value)
            return True
        return False

    def toggle_failure(self, key: ValueKey, value: Any) -> None:
        """Enable or disable simulated SetValue failure for one path."""
        if float(value) != 0.0:
            self.fail_writes_for.add(key)
        else:
            self.fail_writes_for.discard(key)

    def clear_writes(self) -> None:
        """Clear recorded writes before a scenario step."""
        with self.lock:
            self.writes.clear()


def import_live_dbus_modules() -> tuple[Any, Any, Any, Any]:
    """Import dbus-python and GLib modules used by the live testbed."""
    try:
        dbus = importlib.import_module("dbus")
        if bool(getattr(dbus, "__ess_fake_dbus__", False)):
            raise ImportError("dbus-python is not installed")
        dbus_service = importlib.import_module("dbus.service")
        dbus_glib = importlib.import_module("dbus.mainloop.glib")
        glib = importlib.import_module("gi.repository.GLib")
    except Exception as exc:
        raise RuntimeError(
            "Live D-Bus tests require dbus-python and gi.repository.GLib on Venus OS"
        ) from exc
    return dbus, dbus_service, dbus_glib, glib


def build_bus_item_class(dbus_module: Any, dbus_service: Any) -> type[Any]:
    """Create a dbus.service.Object subclass for Victron BusItem paths."""
    bus_object = cast(type[Any], dbus_service.Object)
    bus_object_init = cast(Callable[[Any, Any, str], None], dbus_service.Object.__init__)

    class SimulatedBusItem(bus_object):  # type: ignore[misc, valid-type]
        """D-Bus object exposing GetValue and SetValue for one path."""

        def __init__(
            self,
            bus: Any,
            object_path: str,
            store: LiveDbusStore,
            service_name: str,
        ) -> None:
            """Register one object path on the system bus."""
            bus_object_init(self, bus, object_path)
            self._store = store
            self._service_name = service_name
            self._object_path = object_path

        @dbus_service.method(BUS_ITEM_INTERFACE, in_signature="", out_signature="v")  # type: ignore[untyped-decorator]
        def GetValue(self) -> Any:
            """Return the current simulated D-Bus value."""
            return self._store.get_variant(self._service_name, self._object_path, dbus_module)

        @dbus_service.method(BUS_ITEM_INTERFACE, in_signature="v", out_signature="i")  # type: ignore[untyped-decorator]
        def SetValue(self, value: Any) -> int:
            """Store a new simulated D-Bus value."""
            return self._store.write(self._service_name, self._object_path, value)

    return SimulatedBusItem


@dataclass
class LiveDbusServer:
    """Own simulated Victron service names on the real system D-Bus."""

    store: LiveDbusStore
    dbus: Any
    dbus_service: Any
    dbus_glib: Any
    glib: Any
    bus: Any = None
    loop: Any = None
    loop_thread: Optional[threading.Thread] = None
    objects: list[Any] = field(default_factory=object_list)
    bus_names: list[Any] = field(default_factory=object_list)

    @classmethod
    def create(cls, store: LiveDbusStore) -> "LiveDbusServer":
        """Create a server from runtime D-Bus imports."""
        dbus, dbus_service, dbus_glib, glib = import_live_dbus_modules()
        return cls(store, dbus, dbus_service, dbus_glib, glib)

    def start(self) -> None:
        """Start the system-bus services and GLib main loop thread."""
        print("Starting live fake D-Bus services...", flush=True)
        self.dbus_glib.DBusGMainLoop(set_as_default=True)
        try:
            self.bus = self.dbus.SystemBus()
        except Exception as exc:
            raise RuntimeError("Could not connect to the system D-Bus") from exc
        for service_name in (FAKE_SETTINGS_SERVICE, FAKE_SYSTEM_SERVICE, FAKE_BATTERY_SERVICE):
            self.claim_bus_name(service_name)
        self.create_objects()
        self.loop = self.glib.MainLoop()
        self.loop_thread = threading.Thread(target=self.loop.run, daemon=True)
        self.loop_thread.start()
        print("Live fake D-Bus services started.", flush=True)

    def claim_bus_name(self, service_name: str) -> None:
        """Claim one service name and fail immediately if another testbed owns it."""
        result = self.bus.request_name(
            service_name,
            self.dbus.bus.NAME_FLAG_DO_NOT_QUEUE,
        )
        if int(result) != DBUS_REQUEST_NAME_REPLY_PRIMARY_OWNER:
            raise RuntimeError(
                f"{service_name} is already owned. Stop any running --serve testbed first."
            )
        self.bus_names.append(self.dbus_service.BusName(service_name, self.bus))

    def create_objects(self) -> None:
        """Create all BusItem object paths used by the controller."""
        bus_item = build_bus_item_class(self.dbus, self.dbus_service)
        for service_name, path in sorted(self.store.values):
            self.objects.append(bus_item(self.bus, path, self.store, service_name))

    def stop(self) -> None:
        """Stop the GLib main loop."""
        if self.loop is not None:
            self.loop.quit()
        if self.loop_thread is not None:
            self.loop_thread.join(timeout=2)


class SimulatedDatetime:
    """datetime replacement whose now() value is controlled by scenarios."""

    now_value = datetime(2026, 1, 1, 12, 0, 0)

    @classmethod
    def now(cls, tz: Any = None) -> datetime:
        """Return the scenario timestamp."""
        if tz is not None:
            return cls.now_value.replace(tzinfo=tz)
        return cls.now_value


@contextmanager
def simulated_date(value: datetime) -> Generator[None, None, None]:
    """Temporarily replace the controller module's datetime class."""
    previous = M.datetime
    SimulatedDatetime.now_value = value
    M.datetime = SimulatedDatetime
    try:
        yield
    finally:
        M.datetime = previous


@dataclass
class ScenarioOutcome:
    """Result of one live D-Bus scenario."""

    name: str
    passed: bool
    details: list[str]


@dataclass
class Scenario:
    """One named live D-Bus scenario."""

    name: str
    run: Callable[[Any], ScenarioOutcome]


class LiveHarness:
    """Controller factory and assertion helpers for live D-Bus scenarios."""

    min_soc_path = "/Settings/CGwacs/BatteryLife/MinimumSocLimit"
    max_charge_path = "/Settings/SystemSetup/MaxChargeCurrent"
    soc_path = "/Dc/Battery/Soc"
    bms_current_path = "/Info/MaxChargeCurrent"

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
        self.set_value(FAKE_SYSTEM_SERVICE, "/Dc/Battery/Power", 0.0)
        self.set_value(FAKE_SYSTEM_SERVICE, "/Dc/Battery/Voltage", voltage)
        self.set_value(FAKE_SYSTEM_SERVICE, "/Dc/Pv/Power", 0.0)
        self.set_value(FAKE_BATTERY_SERVICE, self.bms_current_path, battery_max_current)
        for phase in ("L1", "L2", "L3"):
            self.set_value(FAKE_SYSTEM_SERVICE, f"/Ac/Grid/{phase}/Power", house_load / 3.0)
            self.set_value(FAKE_SYSTEM_SERVICE, f"/Ac/ConsumptionOnInput/{phase}/Power", house_load / 3.0)
            self.set_value(FAKE_SYSTEM_SERVICE, f"/Ac/Consumption/{phase}/Power", None)
            self.set_value(FAKE_SYSTEM_SERVICE, f"/Ac/PvOnGrid/{phase}/Power", pv_power / 3.0)
            self.set_value(FAKE_SYSTEM_SERVICE, f"/Ac/PvOnOutput/{phase}/Power", 0.0)

    def set_value(self, service: str, path: str, value: Optional[float]) -> None:
        """Set one simulated value."""
        self.store.set(service, path, value)

    def get_value(self, service: str, path: str) -> Optional[float]:
        """Get one simulated value."""
        return self.store.get(service, path)

    def make_controller(self) -> Any:
        """Create a controller that talks to the live fake D-Bus services."""
        M.SERVICE_SETTINGS = FAKE_SETTINGS_SERVICE
        M.SERVICE_SYSTEM = FAKE_SYSTEM_SERVICE
        M.PREFERRED_BATTERY_SERVICE = FAKE_BATTERY_SERVICE
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
        self.set_value(FAKE_SYSTEM_SERVICE, "/Dc/Battery/Power", 0.0)
        self.set_value(FAKE_SYSTEM_SERVICE, "/Dc/Battery/Voltage", voltage)
        self.set_value(FAKE_SYSTEM_SERVICE, "/Dc/Pv/Power", 0.0)
        self.set_value(FAKE_BATTERY_SERVICE, self.bms_current_path, battery_max_current)
        for phase in ("L1", "L2", "L3"):
            self.set_value(FAKE_SYSTEM_SERVICE, f"/Ac/Grid/{phase}/Power", house_load / 3.0)
            self.set_value(FAKE_SYSTEM_SERVICE, f"/Ac/ConsumptionOnInput/{phase}/Power", house_load / 3.0)
            self.set_value(FAKE_SYSTEM_SERVICE, f"/Ac/Consumption/{phase}/Power", None)
            self.set_value(FAKE_SYSTEM_SERVICE, f"/Ac/PvOnGrid/{phase}/Power", pv_power / 3.0)
            self.set_value(FAKE_SYSTEM_SERVICE, f"/Ac/PvOnOutput/{phase}/Power", 0.0)

    def make_controller(self) -> Any:
        """Create a controller client that reads the fake service names."""
        M.SERVICE_SETTINGS = FAKE_SETTINGS_SERVICE
        M.SERVICE_SYSTEM = FAKE_SYSTEM_SERVICE
        M.PREFERRED_BATTERY_SERVICE = FAKE_BATTERY_SERVICE
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
        M.SERVICE_SYSTEM = f"{FAKE_SYSTEM_SERVICE}.missing"
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


def remote_no_battery(harness: RemoteHarness) -> ScenarioOutcome:
    """Missing SoC should skip without changing settings."""
    harness.reset_values(soc=None)
    controller = harness.make_controller()
    ok = harness.run_once_at(controller, datetime(2026, 4, 27, 12, 0, 0))
    return harness.outcome(
        "live-no-battery-fails-safe",
        [
            (not ok, "controller should skip without SoC"),
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.min_soc_path) == 10.0, "MinSoC changed"),
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.max_charge_path) == -1.0, "MaxChargeCurrent changed"),
        ],
    )


def remote_charge_window(harness: RemoteHarness) -> ScenarioOutcome:
    """Inside the winter charge window, MinSoC and DVCC should be written."""
    harness.reset_values(soc=50.0, min_soc=10.0, max_charge_current=-1.0, house_load=1500.0)
    controller = harness.make_controller()
    ok = harness.run_once_at(controller, datetime(2026, 1, 1, 23, 30, 0))
    current = harness.get_value(FAKE_SETTINGS_SERVICE, harness.max_charge_path)
    return harness.outcome(
        "live-charge-window-writes-target-and-limit",
        [
            (ok, "controller should complete the loop"),
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.min_soc_path) == 65.0, "MinSoC was not raised to 65%"),
            (current is not None and 0 < current < 200.0, "DVCC charge limit was not written"),
            (controller.state["max_charge_current_raw"] == -1.0, "original DVCC value was not captured"),
        ],
    )


def remote_pause_outside_window(harness: RemoteHarness) -> ScenarioOutcome:
    """Outside the charge window, the controller should hold reached SoC."""
    harness.reset_values(soc=30.0, min_soc=10.0, max_charge_current=-1.0)
    controller = harness.make_controller()
    ok = harness.run_once_at(controller, datetime(2026, 1, 1, 12, 0, 0))
    return harness.outcome(
        "live-outside-window-pauses-at-current-soc",
        [
            (ok, "controller should complete the loop"),
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.min_soc_path) == 30.0, "pause SoC was not written"),
            (bool(controller.state["charging_paused"]), "controller did not enter paused state"),
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.max_charge_path) == -1.0, "DVCC changed while paused"),
        ],
    )


def remote_dvcc_restore(harness: RemoteHarness) -> ScenarioOutcome:
    """The captured DVCC setting should be restored after reaching target."""
    harness.reset_values(soc=50.0, min_soc=10.0, max_charge_current=-1.0)
    controller = harness.make_controller()
    first_ok = harness.run_once_at(controller, datetime(2026, 1, 1, 23, 30, 0))
    harness.set_value(FAKE_SYSTEM_SERVICE, harness.soc_path, 70.0)
    second_ok = harness.run_once_at(controller, datetime(2026, 1, 2, 12, 0, 0))
    return harness.outcome(
        "live-dvcc-capture-and-restore",
        [
            (first_ok and second_ok, "controller should complete both loops"),
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.max_charge_path) == -1.0, "DVCC was not restored"),
            (not bool(controller.state["max_charge_current_raw_set"]), "DVCC restore state was not cleared"),
        ],
    )


def remote_manual_dvcc_limit(harness: RemoteHarness) -> ScenarioOutcome:
    """A stricter manual MaxChargeCurrent should not be raised."""
    harness.reset_values(soc=50.0, min_soc=10.0, max_charge_current=30.0)
    controller = harness.make_controller()
    ok = harness.run_once_at(controller, datetime(2026, 1, 1, 23, 30, 0))
    return harness.outcome(
        "live-manual-dvcc-limit-is-not-raised",
        [
            (ok, "controller should complete the loop"),
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.max_charge_path) == 30.0, "manual DVCC value was raised"),
        ],
    )


def remote_dbus_write_failures(harness: RemoteHarness) -> ScenarioOutcome:
    """Simulated D-Bus SetValue failures should not crash or change values."""
    harness.reset_values(soc=50.0, min_soc=10.0, max_charge_current=-1.0)
    harness.fail_min_soc_writes(True)
    harness.fail_max_charge_writes(True)
    controller = harness.make_controller()
    ok = harness.run_once_at(controller, datetime(2026, 1, 1, 23, 30, 0))
    return harness.outcome(
        "live-dbus-write-failures-do-not-crash",
        [
            (ok, "controller should complete despite SetValue failures"),
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.min_soc_path) == 10.0, "failed MinSoC write changed value"),
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.max_charge_path) == -1.0, "failed DVCC write changed value"),
        ],
    )


def remote_missing_system_service(harness: RemoteHarness) -> ScenarioOutcome:
    """A missing system service should make SoC invalid and skip the loop."""
    harness.reset_values(soc=50.0, min_soc=10.0, max_charge_current=-1.0)
    controller = harness.make_controller_with_missing_system_service()
    ok = harness.run_once_at(controller, datetime(2026, 1, 1, 23, 30, 0))
    return harness.outcome(
        "live-missing-system-service-fails-safe",
        [
            (not ok, "controller should skip when the system service is missing"),
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.min_soc_path) == 10.0, "MinSoC changed without system service"),
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.max_charge_path) == -1.0, "DVCC changed without system service"),
        ],
    )


def remote_missing_bms_current(harness: RemoteHarness) -> ScenarioOutcome:
    """Missing BMS max current should avoid guessing a DVCC limit."""
    harness.reset_values(soc=50.0, min_soc=10.0, max_charge_current=-1.0, battery_max_current=None)
    controller = harness.make_controller()
    ok = harness.run_once_at(controller, datetime(2026, 1, 1, 23, 30, 0))
    return harness.outcome(
        "live-missing-bms-current-does-not-guess-dvcc",
        [
            (ok, "controller should complete with missing BMS current"),
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.min_soc_path) == 65.0, "MinSoC target was not applied"),
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.max_charge_path) == -1.0, "DVCC was changed without BMS current"),
        ],
    )


def remote_broken_power_sensors(harness: RemoteHarness) -> ScenarioOutcome:
    """Broken PV/grid/voltage values should fall back without crashing."""
    harness.reset_values(soc=50.0, min_soc=10.0, max_charge_current=-1.0, voltage=None)
    for phase in ("L1", "L2", "L3"):
        harness.set_value(FAKE_SYSTEM_SERVICE, f"/Ac/Grid/{phase}/Power", None)
        harness.set_value(FAKE_SYSTEM_SERVICE, f"/Ac/ConsumptionOnInput/{phase}/Power", None)
        harness.set_value(FAKE_SYSTEM_SERVICE, f"/Ac/PvOnGrid/{phase}/Power", None)
        harness.set_value(FAKE_SYSTEM_SERVICE, f"/Ac/PvOnOutput/{phase}/Power", None)
    controller = harness.make_controller()
    ok = harness.run_once_at(controller, datetime(2026, 1, 1, 23, 30, 0))
    current = harness.get_value(FAKE_SETTINGS_SERVICE, harness.max_charge_path)
    return harness.outcome(
        "live-broken-power-sensors-use-fallbacks",
        [
            (ok, "controller should complete with broken power sensors"),
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.min_soc_path) == 65.0, "MinSoC target was not applied"),
            (current is not None and current > 0.0, "safe DVCC fallback current was not applied"),
        ],
    )


def remote_sd_persist_success_and_failure(harness: RemoteHarness) -> ScenarioOutcome:
    """SD writes should succeed on a directory and fail cleanly on a bad target."""
    controller = harness.make_sd_controller()
    good_dir = Path("/tmp/ess-live-sd-good")
    good_file = good_dir / "ess_winter_logic.json"
    good_file.unlink(missing_ok=True)
    good_dir.mkdir(parents=True, exist_ok=True)
    request = {
        "sd_state_dir": good_dir,
        "sd_state_file": good_file,
        "payload_json": '{"ok": true}',
        "signature": {"ok": True},
        "fsync": True,
    }
    controller.perform_sd_write(request)
    success_ok = good_file.exists()

    bad_target = Path("/tmp/ess-live-sd-blocker")
    if bad_target.exists() and bad_target.is_dir():
        bad_target.rmdir()
    bad_target.write_text("not a directory", encoding="utf-8")
    bad_request = {
        "sd_state_dir": bad_target,
        "sd_state_file": bad_target / "ess_winter_logic.json",
        "payload_json": '{"ok": false}',
        "signature": {"ok": False},
        "fsync": True,
    }
    failed_cleanly = False
    try:
        controller.perform_sd_write(bad_request)
    except Exception:
        failed_cleanly = True
    return harness.outcome(
        "live-sd-persist-success-and-failure",
        [
            (success_ok, "SD write did not create the expected file"),
            (failed_cleanly, "bad SD target did not fail cleanly"),
        ],
    )


REMOTE_SCENARIOS: list[Scenario] = [
    Scenario("live-no-battery-fails-safe", remote_no_battery),
    Scenario("live-charge-window-writes-target-and-limit", remote_charge_window),
    Scenario("live-outside-window-pauses-at-current-soc", remote_pause_outside_window),
    Scenario("live-dvcc-capture-and-restore", remote_dvcc_restore),
    Scenario("live-manual-dvcc-limit-is-not-raised", remote_manual_dvcc_limit),
    Scenario("live-dbus-write-failures-do-not-crash", remote_dbus_write_failures),
    Scenario("live-missing-system-service-fails-safe", remote_missing_system_service),
    Scenario("live-missing-bms-current-does-not-guess-dvcc", remote_missing_bms_current),
    Scenario("live-broken-power-sensors-use-fallbacks", remote_broken_power_sensors),
    Scenario("live-sd-persist-success-and-failure", remote_sd_persist_success_and_failure),
]


def live_no_battery(harness: LiveHarness) -> ScenarioOutcome:
    """Missing SoC should skip without writes through real D-Bus calls."""
    harness.reset_values(soc=None)
    controller = harness.make_controller()
    ok = harness.run_once_at(controller, datetime(2026, 4, 27, 12, 0, 0))
    return harness.outcome(
        "live-no-battery-fails-safe",
        [
            (not ok, "controller should skip without SoC"),
            (not harness.store.writes, "controller wrote D-Bus values without SoC"),
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.min_soc_path) == 10.0, "MinSoC changed"),
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.max_charge_path) == -1.0, "MaxChargeCurrent changed"),
        ],
    )


def live_charge_window(harness: LiveHarness) -> ScenarioOutcome:
    """Inside the winter charge window, MinSoC and DVCC should be written."""
    harness.reset_values(soc=50.0, min_soc=10.0, max_charge_current=-1.0, house_load=1500.0)
    controller = harness.make_controller()
    ok = harness.run_once_at(controller, datetime(2026, 1, 1, 23, 30, 0))
    current = harness.get_value(FAKE_SETTINGS_SERVICE, harness.max_charge_path)
    return harness.outcome(
        "live-charge-window-writes-target-and-limit",
        [
            (ok, "controller should complete the loop"),
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.min_soc_path) == 65.0, "MinSoC was not raised to 65%"),
            (current is not None and 0 < current < 200.0, "DVCC charge limit was not written"),
            (controller.state["max_charge_current_raw"] == -1.0, "original DVCC value was not captured"),
        ],
    )


def live_pause_outside_window(harness: LiveHarness) -> ScenarioOutcome:
    """Outside the charge window, the controller should hold reached SoC."""
    harness.reset_values(soc=30.0, min_soc=10.0, max_charge_current=-1.0)
    controller = harness.make_controller()
    ok = harness.run_once_at(controller, datetime(2026, 1, 1, 12, 0, 0))
    return harness.outcome(
        "live-outside-window-pauses-at-current-soc",
        [
            (ok, "controller should complete the loop"),
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.min_soc_path) == 30.0, "pause SoC was not written"),
            (bool(controller.state["charging_paused"]), "controller did not enter paused state"),
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.max_charge_path) == -1.0, "DVCC changed while paused"),
        ],
    )


def live_dvcc_restore(harness: LiveHarness) -> ScenarioOutcome:
    """The captured DVCC setting should be restored after reaching target."""
    harness.reset_values(soc=50.0, min_soc=10.0, max_charge_current=-1.0)
    controller = harness.make_controller()
    first_ok = harness.run_once_at(controller, datetime(2026, 1, 1, 23, 30, 0))
    harness.set_value(FAKE_SYSTEM_SERVICE, harness.soc_path, 70.0)
    second_ok = harness.run_once_at(controller, datetime(2026, 1, 2, 12, 0, 0))
    return harness.outcome(
        "live-dvcc-capture-and-restore",
        [
            (first_ok and second_ok, "controller should complete both loops"),
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.max_charge_path) == -1.0, "DVCC was not restored"),
            (not bool(controller.state["max_charge_current_raw_set"]), "DVCC restore state was not cleared"),
        ],
    )


def live_manual_dvcc_limit(harness: LiveHarness) -> ScenarioOutcome:
    """A stricter manual MaxChargeCurrent should not be raised."""
    harness.reset_values(soc=50.0, min_soc=10.0, max_charge_current=30.0)
    controller = harness.make_controller()
    ok = harness.run_once_at(controller, datetime(2026, 1, 1, 23, 30, 0))
    return harness.outcome(
        "live-manual-dvcc-limit-is-not-raised",
        [
            (ok, "controller should complete the loop"),
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.max_charge_path) == 30.0, "manual DVCC value was raised"),
            (not harness.writes_to(harness.max_charge_path), "controller wrote MaxChargeCurrent unexpectedly"),
        ],
    )


def live_write_failures(harness: LiveHarness) -> ScenarioOutcome:
    """Simulated SetValue failures should not crash the controller loop."""
    harness.reset_values(soc=50.0, min_soc=10.0, max_charge_current=-1.0)
    harness.store.fail_writes_for.add((FAKE_SETTINGS_SERVICE, harness.min_soc_path))
    harness.store.fail_writes_for.add((FAKE_SETTINGS_SERVICE, harness.max_charge_path))
    controller = harness.make_controller()
    ok = harness.run_once_at(controller, datetime(2026, 1, 1, 23, 30, 0))
    return harness.outcome(
        "live-dbus-write-failures-do-not-crash",
        [
            (ok, "controller should complete despite SetValue failures"),
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.min_soc_path) == 10.0, "failed MinSoC write changed value"),
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.max_charge_path) == -1.0, "failed DVCC write changed value"),
        ],
    )


SCENARIOS: list[Scenario] = [
    Scenario("live-no-battery-fails-safe", live_no_battery),
    Scenario("live-charge-window-writes-target-and-limit", live_charge_window),
    Scenario("live-outside-window-pauses-at-current-soc", live_pause_outside_window),
    Scenario("live-dvcc-capture-and-restore", live_dvcc_restore),
    Scenario("live-manual-dvcc-limit-is-not-raised", live_manual_dvcc_limit),
    Scenario("live-dbus-write-failures-do-not-crash", live_write_failures),
]


def seed_all_paths(store: LiveDbusStore) -> None:
    """Create every path that may be accessed by the live scenarios."""
    harness = LiveHarness(store)
    harness.reset_values(soc=50.0)


def selected_scenarios(names: list[str]) -> list[Scenario]:
    """Return selected scenarios by name."""
    if not names or "all" in names:
        return REMOTE_SCENARIOS
    known = {scenario.name: scenario for scenario in REMOTE_SCENARIOS}
    missing = [name for name in names if name not in known]
    if missing:
        raise SystemExit(f"Unknown scenario(s): {', '.join(missing)}")
    return [known[name] for name in names]


def run_scenarios(harness: Any, scenarios: list[Scenario], verbose: bool) -> int:
    """Run live D-Bus scenarios and return an exit code."""
    failures = 0
    for scenario in scenarios:
        print(f"RUN  {scenario.name}", flush=True)
        outcome = scenario.run(harness)
        status = "PASS" if outcome.passed else "FAIL"
        print(f"{status} {outcome.name}", flush=True)
        if verbose or not outcome.passed:
            for detail in outcome.details:
                print(f"  - {detail}")
        if not outcome.passed:
            failures += 1
    print(f"\n{len(scenarios) - failures}/{len(scenarios)} live D-Bus scenarios passed", flush=True)
    return 1 if failures else 0


def run_client_scenarios(names: list[str], verbose: bool) -> int:
    """Run scenario clients against already running fake D-Bus services."""
    return run_scenarios(RemoteHarness(), selected_scenarios(names), verbose)


def run_scenarios_in_child(names: list[str], verbose: bool) -> int:
    """Run controller scenarios in a child process to avoid same-process D-Bus deadlocks."""
    cmd = [sys.executable, str(Path(__file__).resolve()), "--client"]
    if verbose:
        cmd.append("--verbose")
    cmd.extend(names)
    env = dict(os.environ)
    proc = subprocess.run(cmd, env=env, check=False)
    return int(proc.returncode)


def controller_env_command() -> str:
    """Return a command that runs the controller against the fake services."""
    return (
        f"ESS_SERVICE_SETTINGS={FAKE_SETTINGS_SERVICE} "
        f"ESS_SERVICE_SYSTEM={FAKE_SYSTEM_SERVICE} "
        f"ESS_PREFERRED_BATTERY_SERVICE={FAKE_BATTERY_SERVICE} "
        f"python3 {SOC_SCRIPT}"
    )


def serve_until_interrupted(server: LiveDbusServer) -> int:
    """Keep fake services alive for manual controller testing."""
    print("Live fake D-Bus services are running.")
    print("Quick probes:")
    print(f"dbus -y {FAKE_SYSTEM_SERVICE} /Dc/Battery/Soc GetValue")
    print(f"dbus -y {FAKE_SETTINGS_SERVICE} /Settings/CGwacs/BatteryLife/MinimumSocLimit GetValue")
    print("In another shell, run:")
    print(controller_env_command())
    print("Press Ctrl-C here to stop the fake services.")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        server.stop()
        return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse command-line options."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenarios", nargs="*", help="Scenario names to run, or 'all'.")
    parser.add_argument("--serve", action="store_true", help="Only publish fake services for manual testing.")
    parser.add_argument("--client", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("-v", "--verbose", action="store_true", help="Print details for passing scenarios too.")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    """Command-line entrypoint."""
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if bool(args.client):
        return run_client_scenarios(cast(list[str], args.scenarios), bool(args.verbose))

    store = LiveDbusStore()
    seed_all_paths(store)
    try:
        server = LiveDbusServer.create(store)
        server.start()
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    try:
        if bool(args.serve):
            return serve_until_interrupted(server)
        return run_scenarios_in_child(cast(list[str], args.scenarios), bool(args.verbose))
    finally:
        server.stop()


if __name__ == "__main__":
    raise SystemExit(main())

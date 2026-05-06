#!/usr/bin/env python3
"""Publish simulated Victron D-Bus services and run live controller scenarios.

This testbed uses the real system D-Bus, but it owns only service names ending
in ``.sim``. It is intended for a disposable Venus OS test device. The normal
Victron service names are not touched unless the controller is explicitly
started with the environment variables printed by ``--serve``.
"""

from __future__ import annotations

import importlib
import importlib.util
import sys
import threading
import types
from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Protocol, cast


ROOT = Path(__file__).resolve().parents[1]
SOC_SCRIPT = ROOT / "socSteuerung.py"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
FAKE_SETTINGS_SERVICE = "com.victronenergy.settings.sim"
FAKE_SYSTEM_SERVICE = "com.victronenergy.system.sim"
FAKE_BATTERY_SERVICE = "com.victronenergy.battery.sim"
BUS_ITEM_INTERFACE = "com.victronenergy.BusItem"
DBUS_REQUEST_NAME_REPLY_PRIMARY_OWNER = 1
CONTROL_FAIL_MIN_SOC_PATH = "/Sim/FailWrites/MinimumSocLimit"
CONTROL_FAIL_MAX_CHARGE_PATH = "/Sim/FailWrites/MaxChargeCurrent"
CONTROLLER_SERVICE_MODULES = (
    "socsteuerung_live_under_test",
    "venus_ess_winter_soc_service.config",
    "venus_ess_winter_soc_service.dvcc",
    "venus_ess_winter_soc_service.power",
    "venus_ess_winter_soc_service.runtime",
    "venus_ess_winter_soc_service.socpolicy",
    "venus_ess_winter_soc_service.windows",
)
DATETIME_MODULES = (
    "venus_ess_winter_soc_service.socpolicy",
    "venus_ess_winter_soc_service.tracking",
    "venus_ess_winter_soc_service.windows",
)


class ControllerModule(Protocol):
    """Typed subset of the dynamically loaded controller module."""

    SERVICE_SETTINGS: str
    SERVICE_SYSTEM: str
    PREFERRED_BATTERY_SERVICE: str
    GRID_SOFT_MIN_CHARGE_CURRENT_A: float
    PHASES: tuple[str, ...]
    MIN_SOC_PATH: str
    MAX_CHARGE_CURRENT_PATH: str
    BATTERY_SOC_PATH: str
    BATTERY_POWER_PATH: str
    BATTERY_VOLTAGE_PATH: str
    BMS_MAX_CHARGE_CURRENT_PATH: str
    DC_PV_POWER_PATH: str
    AC_GRID_POWER_PATH: str
    AC_PV_ON_GRID_POWER_PATH: str
    AC_PV_ON_OUTPUT_POWER_PATH: str
    AC_CONSUMPTION_ON_INPUT_POWER_PATH: str
    AC_CONSUMPTION_POWER_PATH: str
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


def configure_controller_services(system_service: str = FAKE_SYSTEM_SERVICE) -> None:
    """Point the loaded controller modules at the simulated D-Bus services."""
    for module_name in CONTROLLER_SERVICE_MODULES:
        module = sys.modules.get(module_name)
        if module is not None:
            setattr(module, "SERVICE_SETTINGS", FAKE_SETTINGS_SERVICE)
            setattr(module, "SERVICE_SYSTEM", system_service)
            setattr(module, "PREFERRED_BATTERY_SERVICE", FAKE_BATTERY_SERVICE)


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
            self.toggle_failure((FAKE_SETTINGS_SERVICE, M.MIN_SOC_PATH), value)
            return True
        if path == CONTROL_FAIL_MAX_CHARGE_PATH:
            self.toggle_failure((FAKE_SETTINGS_SERVICE, M.MAX_CHARGE_CURRENT_PATH), value)
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
    previous = controller_datetime_values()
    SimulatedDatetime.now_value = value
    set_controller_datetime(SimulatedDatetime)
    try:
        yield
    finally:
        restore_controller_datetime(previous)


def controller_datetime_values() -> dict[str, Any]:
    """Return current datetime objects from controller modules."""
    return {
        module_name: getattr(sys.modules[module_name], "datetime")
        for module_name in DATETIME_MODULES
    }


def set_controller_datetime(value: Any) -> None:
    """Set the datetime object in every date-sensitive controller module."""
    for module_name in DATETIME_MODULES:
        setattr(sys.modules[module_name], "datetime", value)


def restore_controller_datetime(previous: dict[str, Any]) -> None:
    """Restore datetime objects after a simulated scenario date."""
    for module_name, value in previous.items():
        setattr(sys.modules[module_name], "datetime", value)


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

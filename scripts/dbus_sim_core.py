#!/usr/bin/env python3
"""Run offline D-Bus scenarios against the ESS winter SoC controller.

The simulator does not publish a real D-Bus service and does not touch live
Victron settings. Instead, it imports the controller, replaces its D-Bus facade
with an in-memory model, and executes realistic control-loop scenarios. This
keeps the harness safe on a development machine and useful on a Venus OS test
device that has no battery attached.
"""

from __future__ import annotations

import importlib.util
import sys
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
    WinterController: type[Any]
    datetime: Any


def install_fake_dbus_if_needed() -> None:
    """Install a tiny fake dbus module when dbus-python is unavailable."""
    if "dbus" in sys.modules:
        return
    try:
        __import__("dbus")
        return
    except Exception:
        pass

    fake = types.ModuleType("dbus")
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
    install_fake_dbus_if_needed()
    module_name = "socsteuerung_scenario_sim"
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
class DbusWrite:
    """One simulated D-Bus write."""

    service: str
    path: str
    value: float


def value_map() -> dict[ValueKey, Optional[float]]:
    """Return an empty simulated D-Bus value map."""
    return {}


def service_list() -> list[str]:
    """Return an empty simulated D-Bus service list."""
    return []


def log_list() -> list[str]:
    """Return an empty simulated log list."""
    return []


def write_list() -> list[DbusWrite]:
    """Return an empty simulated D-Bus write list."""
    return []


def key_set() -> set[ValueKey]:
    """Return an empty simulated D-Bus path set."""
    return set()


@dataclass
class FakeDbus:
    """In-memory D-Bus facade used by the scenario runner."""

    values: dict[ValueKey, Optional[float]] = field(default_factory=value_map)
    raw_values: dict[ValueKey, Optional[float]] = field(default_factory=value_map)
    services: list[str] = field(default_factory=service_list)
    logs: list[str] = field(default_factory=log_list)
    writes: list[DbusWrite] = field(default_factory=write_list)
    fail_writes_for: set[ValueKey] = field(default_factory=key_set)
    fail_reads_for: set[ValueKey] = field(default_factory=key_set)

    def get_value(
        self,
        service: str,
        path: str,
        default: Optional[float] = 0.0,
    ) -> Optional[float]:
        """Return measured values, converting failed reads to the caller default."""
        key = (service, path)
        if key in self.fail_reads_for:
            return default
        value = self.values.get(key, default)
        return default if value is None else value

    def get_raw_value(
        self,
        service: str,
        path: str,
        default: Optional[float] = None,
    ) -> Optional[float]:
        """Return raw settings/measurements, preserving Victron -1 values."""
        key = (service, path)
        if key in self.fail_reads_for:
            return default
        return self.raw_values.get(key, self.values.get(key, default))

    def set_value(self, service: str, path: str, value: Any) -> bool:
        """Record a simulated D-Bus write or fail it when requested."""
        key = (service, path)
        if key in self.fail_writes_for:
            self.log(f"Simulated write failure for {path}")
            return False
        numeric = float(value)
        self.raw_values[key] = numeric
        self.values[key] = numeric
        self.writes.append(DbusWrite(service, path, numeric))
        return True

    def list_services(self, prefix: str) -> list[str]:
        """Return simulated services matching the requested prefix."""
        return [service for service in self.services if service.startswith(prefix)]

    def log(self, msg: str) -> None:
        """Collect controller log messages for assertions and diagnostics."""
        self.logs.append(msg)


class SimulatedDatetime:
    """datetime replacement whose now() value is controlled by the scenario."""

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
    """Result of one scenario."""

    name: str
    passed: bool
    details: list[str]


@dataclass
class Scenario:
    """One named simulation scenario."""

    name: str
    run: Callable[[], ScenarioOutcome]


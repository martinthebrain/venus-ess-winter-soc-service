from __future__ import annotations

import importlib.util
import sys
import threading
import types
from datetime import datetime
from pathlib import Path
from typing import Any


def install_fake_dbus() -> None:
    """Install a small dbus stand-in when dbus-python is unavailable in tests."""
    if "dbus" in sys.modules:
        return

    fake = types.ModuleType("dbus")
    fake.Boolean = bool
    fake.Int16 = int
    fake.UInt16 = int
    fake.Int32 = int
    fake.UInt32 = int
    fake.Int64 = int
    fake.UInt64 = int
    fake.Double = float
    fake.Byte = int

    class SystemBus:
        """SystemBus stub that should not be used by unit tests."""

        def get_object(self, *_args: object, **_kwargs: object) -> object:
            """Fail if production D-Bus access leaks into unit tests."""
            raise RuntimeError("Not used in unit tests")

        def list_names(self, *_args: object, **_kwargs: object) -> list[str]:
            """Return no services for accidental scans."""
            return []

    fake.SystemBus = SystemBus
    sys.modules["dbus"] = fake


def load_controller_module() -> Any:
    """Load the compatibility wrapper once under a stable test-only name."""
    install_fake_dbus()
    module_name = "socsteuerung_under_test"
    if module_name in sys.modules:
        return sys.modules[module_name]

    script = Path(__file__).resolve().parents[1] / "socSteuerung.py"
    spec = importlib.util.spec_from_file_location(module_name, script)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


M = load_controller_module()


def charge_context(**overrides: object) -> dict[str, object]:
    """Return a complete charge-control context with overridable defaults."""
    context = {
        "time_ok": True,
        "effective_active": False,
        "stage_charge_target": True,
        "grid_import": 100.0,
        "house_load": 2500.0,
        "battery_max_current": 100.0,
        "battery_voltage": 52.0,
    }
    context.update(overrides)
    return context


class RichDbusStub:
    """In-memory D-Bus stub shared by controller unit tests."""

    def __init__(self) -> None:
        """Initialize empty D-Bus value, log, write, and service stores."""
        self.values: dict[tuple[str, str], object] = {}
        self.raw_values: dict[tuple[str, str], object] = {}
        self.logs: list[str] = []
        self.sets: list[tuple[str, str, object]] = []
        self.services: list[str] = []

    def get_value(self, service: str, path: str, default: object = 0.0) -> object:
        """Return a measured test value or the caller default."""
        return self.values.get((service, path), default)

    def get_raw_value(self, service: str, path: str, default: object = None) -> object:
        """Return a raw test value, falling back to measured values and defaults."""
        return self.raw_values.get((service, path), self.values.get((service, path), default))

    def set_value(self, service: str, path: str, value: object) -> bool:
        """Record one setting write and update the raw value store."""
        self.sets.append((service, path, value))
        self.raw_values[(service, path)] = value
        return True

    def log(self, msg: str) -> None:
        """Collect a controller log line."""
        self.logs.append(msg)

    def list_services(self, prefix: str) -> list[str]:
        """Return test services matching a service prefix."""
        return [svc for svc in self.services if svc.startswith(prefix)]


class FixedDatetime:
    """datetime replacement with a mutable now() value for tests."""

    value = datetime(2026, 1, 1, 12, 0, 0)

    @classmethod
    def now(cls) -> datetime:
        """Return the configured datetime value."""
        return cls.value


def controller() -> Any:
    """Create a WinterController instance without running __init__."""
    c = M.WinterController.__new__(M.WinterController)
    c.dbus = RichDbusStub()
    c.state = M.WinterController.default_state(c)
    c.sd_last_persist_ts = 0.0
    c.sd_error_count = 0
    c.sd_next_try_ts = 0.0
    c.sd_last_signature = None
    c.sd_pending_signature = None
    c.sd_pending_fsync = False
    c.sd_window_active = False
    c.sd_card_path = None
    c.sd_state_dir = None
    c.sd_state_file = None
    c.sd_info = ""
    c.sd_last_lookup_ts = 0.0
    c.last_charge_limit_set_ts = 0.0
    c.sd_write_lock = threading.Lock()
    c.sd_write_event = threading.Event()
    c.sd_write_pending = None
    c.sd_write_inflight = False
    return c

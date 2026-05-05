# SPDX-License-Identifier: GPL-3.0-or-later
from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Optional, cast

import dbus

from .config import DBUS_CALL_TIMEOUT_SECONDS, LOG_FILE, LOG_MAX_BYTES, LOG_TRUNCATE_BYTES
class DBusInterface:
    """Small Victron D-Bus facade with tolerant reads, typed writes, and logging."""
    def __init__(self) -> None:
        """Connect to the system D-Bus."""
        self.bus: Any = dbus.SystemBus()

    def get_bus_item(self, service: str, path: str) -> Any:
        """Return a D-Bus BusItem proxy without slow or fragile introspection."""
        try:
            return self.bus.get_object(service, path, introspect=False)
        except TypeError:
            return self.bus.get_object(service, path)

    def get_value(self, service: str, path: str, default: Optional[float] = 0.0) -> Optional[float]:
        """Read a measured numeric D-Bus value and hide invalid None/-1 readings."""
        try:
            obj = self.get_bus_item(service, path)
            try:
                val = obj.GetValue(
                    dbus_interface='com.victronenergy.BusItem',
                    timeout=DBUS_CALL_TIMEOUT_SECONDS
                )
            except TypeError:
                # Older dbus-python bindings do not accept a typed timeout parameter.
                val = obj.GetValue(dbus_interface='com.victronenergy.BusItem')
            # Measurements use -1 or None for unavailable values on several paths.
            if val is None or val == -1:
                return default
            return float(val)
        except Exception:
            return default

    def get_raw_value(self, service: str, path: str, default: Optional[float] = None) -> Optional[float]:
        """Read a raw D-Bus value without treating -1 as invalid."""
        try:
            obj = self.get_bus_item(service, path)
            try:
                val = obj.GetValue(
                    dbus_interface='com.victronenergy.BusItem',
                    timeout=DBUS_CALL_TIMEOUT_SECONDS
                )
            except TypeError:
                val = obj.GetValue(dbus_interface='com.victronenergy.BusItem')
            if val is None:
                return default
            return float(val)
        except Exception:
            return default

    def set_value(self, service: str, path: str, value: Any) -> bool:
        """Write a D-Bus value, preserving explicit dbus.* types when provided."""
        try:
            value = self.coerce_dbus_value(value)
            obj = self.get_bus_item(service, path)
            self.set_bus_item_value(obj, value)
            return True
        except Exception as e:
            self.log(f"Error while setting {path}: {e}")
            return False

    def coerce_dbus_value(self, value: Any) -> Any:
        """Convert plain Python scalars to explicit D-Bus scalar types."""
        if isinstance(value, self.dbus_scalar_types()):
            return value
        if isinstance(value, bool):
            return dbus.Boolean(value)
        if isinstance(value, float):
            return dbus.Double(value)
        if isinstance(value, int):
            return dbus.Int32(value)
        return value

    def dbus_scalar_types(self) -> tuple[type[Any], ...]:
        """Return scalar D-Bus wrapper classes that should be preserved."""
        return (
            dbus.Boolean,
            dbus.Int16,
            dbus.UInt16,
            dbus.Int32,
            dbus.UInt32,
            dbus.Int64,
            dbus.UInt64,
            dbus.Double,
            dbus.Byte,
        )

    def set_bus_item_value(self, obj: Any, value: Any) -> None:
        """Call SetValue with timeout support and old-binding fallback."""
        try:
            obj.SetValue(
                value,
                dbus_interface='com.victronenergy.BusItem',
                timeout=DBUS_CALL_TIMEOUT_SECONDS
            )
        except TypeError:
            obj.SetValue(value, dbus_interface='com.victronenergy.BusItem')

    def log(self, msg: str) -> None:
        """Log to stdout and to the volatile RAM log with simple size rotation."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"{timestamp}: {msg}"
        print(log_entry)
        # Keep diagnostics in RAM so normal operation does not write Cerbo flash.
        try:
            with open(LOG_FILE, "a") as f:
                f.write(log_entry + "\n")
            try:
                if os.path.getsize(LOG_FILE) > LOG_MAX_BYTES:
                    with open(LOG_FILE, "rb") as f:
                        f.seek(0, os.SEEK_END)
                        size = f.tell()
                        keep = LOG_TRUNCATE_BYTES if size > LOG_TRUNCATE_BYTES else size
                        f.seek(-keep, os.SEEK_END)
                        data = f.read(keep)
                    with open(LOG_FILE, "wb") as f:
                        f.write(data)
            except Exception:
                pass
        except:
            pass

    def list_services(self, prefix: str) -> list[str]:
        """Return list of services matching a prefix."""
        try:
            # dbus-python exposes list_names without a typed timeout parameter in many stubs.
            # Keep this call simple for compatibility and static type checkers.
            names = cast(list[str], self.bus.list_names() or [])
            return [name for name in names if name.startswith(prefix)]
        except Exception:
            return []


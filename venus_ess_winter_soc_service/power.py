# SPDX-License-Identifier: GPL-3.0-or-later
from __future__ import annotations

import time
from typing import Any, Optional, Sequence

from .base import ControllerMixinBase
from .config import *  # noqa: F403

class PowerMixin(ControllerMixinBase):
    def get_total_pv_power(self) -> float:
        """Sum AC PV on grid/output sides plus DC PV power."""
        pv_ac = 0.0
        for phase in ['L1', 'L2', 'L3']:
            pv_ac += self.dbus.get_value(SERVICE_SYSTEM, f'/Ac/PvOnGrid/{phase}/Power', 0.0) or 0.0
            pv_ac += self.dbus.get_value(SERVICE_SYSTEM, f'/Ac/PvOnOutput/{phase}/Power', 0.0) or 0.0
             
        pv_dc = self.dbus.get_value(SERVICE_SYSTEM, '/Dc/Pv/Power', 0.0) or 0.0
        return pv_ac + pv_dc

    def get_grid_power_net(self) -> float:
        """Net grid power: import positive, export negative."""
        total = self.sum_phase_values('/Ac/Grid/{phase}/Power')
        return total if total is not None else 0.0

    def get_battery_service(self) -> Optional[str]:
        """Return a cached valid battery service, rescanning only when it becomes invalid."""
        cached = self.state.get("battery_service")
        if self.is_valid_battery_service(cached):
            return str(cached)
        if not self.battery_service_rescan_due():
            return None
        return self.scan_battery_service(str(cached) if cached is not None else None)

    def battery_service_rescan_due(self) -> bool:
        """Return True when enough time has passed to scan D-Bus battery services."""
        now = time.time()
        last_scan = self.state.get("battery_service_last_scan_ts", 0)
        if (now - last_scan) >= BATTERY_SERVICE_RESCAN_SECONDS:
            self.state["battery_service_last_scan_ts"] = now
            return True
        return False

    def scan_battery_service(self, cached: Optional[str]) -> Optional[str]:
        """Scan D-Bus and select the preferred or strongest battery service."""
        services = self.dbus.list_services('com.victronenergy.battery')
        if not services:
            return None
        preferred = self.select_preferred_battery_service(services)
        return preferred or self.store_best_battery_service(services, cached)

    def store_best_battery_service(
        self,
        services: Sequence[str],
        cached: Optional[str],
    ) -> Optional[str]:
        """Store and return the strongest valid battery service discovered."""
        best = self.select_best_battery_service(services)
        if best and best != cached:
            self.state["battery_service"] = best
            self.save_state_to_ram()
        return best

    def is_valid_battery_service(self, service: object) -> bool:
        """Return True when a battery service exposes a usable BMS charge current."""
        if not isinstance(service, str) or not service:
            return False
        val = self.dbus.get_value(service, '/Info/MaxChargeCurrent', None)
        return val is not None and val > 0

    def select_preferred_battery_service(self, services: Sequence[str]) -> Optional[str]:
        """Use configured preferred battery service when it is available and valid."""
        if PREFERRED_BATTERY_SERVICE not in services:
            return None
        if not self.is_valid_battery_service(PREFERRED_BATTERY_SERVICE):
            return None
        self.state["battery_service"] = PREFERRED_BATTERY_SERVICE
        self.save_state_to_ram()
        return PREFERRED_BATTERY_SERVICE

    def select_best_battery_service(self, services: Sequence[str]) -> Optional[str]:
        """Choose the battery service with the highest advertised charge current."""
        best: Optional[str] = None
        best_val = 0.0
        for svc in services:
            val = self.dbus.get_value(svc, '/Info/MaxChargeCurrent', None)
            if val is not None and val > best_val:
                best_val = val
                best = svc
        return best

    def get_battery_max_charge_current(self) -> Optional[float]:
        """Read the BMS maximum charge current, falling back to the last valid value."""
        svc = self.get_battery_service()
        live_current = self.read_live_battery_max_current(svc)
        if live_current is not None:
            return live_current
        return self.get_cached_battery_max_current()

    def read_live_battery_max_current(self, service: Optional[str]) -> Optional[float]:
        """Read and cache a live BMS maximum charge current when available."""
        if not service:
            return None
        val = self.dbus.get_value(service, '/Info/MaxChargeCurrent', None)
        if val is None or val <= 0:
            return None
        self.state["battery_max_current_last"] = val
        return val

    def get_cached_battery_max_current(self) -> Optional[float]:
        """Return the last known valid BMS maximum charge current."""
        cached = self.state.get("battery_max_current_last")
        if cached is not None and cached > 0:
            return float(cached)
        return None

    def get_battery_power(self) -> Optional[float]:
        """Read the current DC battery power from the system service."""
        return self.dbus.get_value(SERVICE_SYSTEM, '/Dc/Battery/Power', 0)

    def get_battery_voltage(self) -> Optional[float]:
        """Read the current DC battery voltage used for charge-current calculations."""
        return self.dbus.get_value(SERVICE_SYSTEM, '/Dc/Battery/Voltage', None)

    def get_house_load_power(
        self,
        grid_power_net: Optional[float] = None,
        batt_power: Optional[float] = None,
    ) -> float:
        """Compute house load from consumption paths or fallback."""
        total = self.sum_phase_values('/Ac/ConsumptionOnInput/{phase}/Power')
        if total is not None:
            return total

        total = self.sum_phase_values('/Ac/Consumption/{phase}/Power')
        if total is not None:
            return total

        return self.compute_house_load_fallback(grid_power_net, batt_power)

    def sum_phase_values(self, path_template: str) -> Optional[float]:
        """Sum L1-L3 D-Bus values, returning None when no phase exists."""
        total = 0.0
        found = False
        for phase in ['L1', 'L2', 'L3']:
            val = self.dbus.get_value(SERVICE_SYSTEM, path_template.format(phase=phase), None)
            if val is not None:
                total += val
                found = True
        return total if found else None

    def compute_house_load_fallback(
        self,
        grid_power_net: Optional[float] = None,
        batt_power: Optional[float] = None,
    ) -> float:
        """Fallback house-load estimate from grid and positive battery discharge."""
        grid_power_net = self.resolve_power_value(grid_power_net, self.get_grid_power_net)
        batt_power = self.resolve_power_value(batt_power, self.get_battery_power)
        fallback = grid_power_net + max(0.0, batt_power)
        return max(fallback, 0.0)

    def resolve_power_value(self, value: Optional[float], reader: PowerReader) -> float:
        """Return a provided power value or read one, normalizing missing data to zero."""
        if value is None:
            value = reader()
        return value if value is not None else 0.0

    def state_float(self, value: Any) -> float:
        """Convert numeric state values loaded from JSON or D-Bus to float."""
        return float(value)

    def state_float_list(self, values: Any) -> list[float]:
        """Convert a JSON-loaded numeric list into floats."""
        return [self.state_float(value) for value in values]

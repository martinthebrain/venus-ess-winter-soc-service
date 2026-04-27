#!/usr/bin/env python3
"""Run offline D-Bus scenarios against the ESS winter SoC controller.

The simulator does not publish a real D-Bus service and does not touch live
Victron settings. Instead, it imports the controller, replaces its D-Bus facade
with an in-memory model, and executes realistic control-loop scenarios. This
keeps the harness safe on a development machine and useful on a Venus OS test
device that has no battery attached.
"""

from __future__ import annotations

import argparse
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


class ControllerModule(Protocol):
    """Typed subset of the dynamically loaded controller module."""

    SERVICE_SETTINGS: str
    SERVICE_SYSTEM: str
    PREFERRED_BATTERY_SERVICE: str
    GRID_SOFT_MIN_CHARGE_CURRENT_A: float
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
    previous = M.datetime
    SimulatedDatetime.now_value = value
    M.datetime = SimulatedDatetime
    try:
        yield
    finally:
        M.datetime = previous


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


class Harness:
    """Factory and assertion helpers for controller scenario tests."""

    min_soc_path = "/Settings/CGwacs/BatteryLife/MinimumSocLimit"
    max_charge_path = "/Settings/SystemSetup/MaxChargeCurrent"
    soc_path = "/Dc/Battery/Soc"
    battery_power_path = "/Dc/Battery/Power"
    battery_voltage_path = "/Dc/Battery/Voltage"
    bms_current_path = "/Info/MaxChargeCurrent"

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
        for phase in ("L1", "L2", "L3"):
            self.set_value(dbus, M.SERVICE_SYSTEM, f"/Ac/Grid/{phase}/Power", house_load / 3.0)
            self.set_value(
                dbus,
                M.SERVICE_SYSTEM,
                f"/Ac/ConsumptionOnInput/{phase}/Power",
                house_load / 3.0,
            )
            self.set_value(dbus, M.SERVICE_SYSTEM, f"/Ac/PvOnGrid/{phase}/Power", pv_power / 3.0)
            self.set_value(dbus, M.SERVICE_SYSTEM, f"/Ac/PvOnOutput/{phase}/Power", 0.0)
        self.set_value(dbus, M.SERVICE_SYSTEM, "/Dc/Pv/Power", 0.0)
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
        details = [message for ok, message in checks if not ok]
        if details and dbus is not None and dbus.logs:
            details.append("recent logs: " + " | ".join(dbus.logs[-5:]))
        return ScenarioOutcome(name=name, passed=not details, details=details)


H = Harness()


def scenario_no_battery_fails_safe() -> ScenarioOutcome:
    """Missing SoC must stop the loop and leave settings untouched."""
    dbus = H.make_dbus(soc=None, min_soc=10.0, max_charge_current=-1.0)
    controller = H.make_controller(dbus)
    ok = H.run_once_at(controller, datetime(2026, 4, 27, 12, 0, 0))
    return H.scenario_result(
        "no-battery-fails-safe",
        [
            (not ok, "controller should skip when SoC is missing"),
            (H.raw(dbus, M.SERVICE_SETTINGS, H.min_soc_path) == 10.0, "MinSoC changed without SoC"),
            (H.raw(dbus, M.SERVICE_SETTINGS, H.max_charge_path) == -1.0, "MaxChargeCurrent changed without SoC"),
            (not dbus.writes, "controller wrote D-Bus values without SoC"),
        ],
        dbus,
    )


def scenario_summer_manual_override_is_preserved() -> ScenarioOutcome:
    """Summer manual MinSoC changes should not be reset immediately."""
    dbus = H.make_dbus(soc=80.0, min_soc=35.0, max_charge_current=-1.0)
    controller = H.make_controller(dbus)
    controller.state["min_soc_last_seen"] = 10.0
    ok = H.run_once_at(controller, datetime(2026, 7, 1, 12, 0, 0))
    return H.scenario_result(
        "summer-manual-override-is-preserved",
        [
            (ok, "controller should run with valid SoC"),
            (H.raw(dbus, M.SERVICE_SETTINGS, H.min_soc_path) == 35.0, "summer override was overwritten"),
            (controller.state["manual_override_until_ts"] > 0, "manual override window was not started"),
        ],
        dbus,
    )


def scenario_transition_40_percent_is_staged() -> ScenarioOutcome:
    """The 40% reserve target must not hard-jump outside the charge window."""
    dbus = H.make_dbus(soc=25.0, min_soc=10.0, max_charge_current=-1.0)
    controller = H.make_controller(dbus)
    controller.state["pv_history"] = [1000.0, 1200.0, 1500.0, 1600.0]
    ok = H.run_once_at(controller, datetime(2026, 11, 10, 12, 0, 0))
    return H.scenario_result(
        "transition-40-percent-is-staged",
        [
            (ok, "controller should run with valid SoC"),
            (H.raw(dbus, M.SERVICE_SETTINGS, H.min_soc_path) == 25.0, "40% target was applied immediately outside charge window"),
            (bool(controller.state["charging_paused"]), "controller did not enter paused hold state"),
            (H.raw(dbus, M.SERVICE_SETTINGS, H.max_charge_path) == -1.0, "DVCC current changed while paused"),
        ],
        dbus,
    )


def scenario_charge_window_applies_target_and_limit() -> ScenarioOutcome:
    """Inside the charge window, the controller should raise MinSoC and limit DVCC."""
    dbus = H.make_dbus(soc=25.0, min_soc=10.0, max_charge_current=-1.0, house_load=1500.0)
    controller = H.make_controller(dbus)
    controller.state["pv_history"] = [1000.0, 1200.0, 1500.0, 1600.0]
    ok = H.run_once_at(controller, datetime(2026, 11, 10, 23, 30, 0))
    written_current = H.raw(dbus, M.SERVICE_SETTINGS, H.max_charge_path)
    return H.scenario_result(
        "charge-window-applies-target-and-limit",
        [
            (ok, "controller should run with valid SoC"),
            (H.raw(dbus, M.SERVICE_SETTINGS, H.min_soc_path) == 40.0, "MinSoC was not raised to the target in charge window"),
            (written_current is not None and 0 < written_current < 200.0, "DVCC charge limit was not written"),
            (controller.state["max_charge_current_raw"] == -1.0, "original DVCC value was not captured"),
            (bool(controller.state["charge_current_owned_by_script"]), "controller did not take DVCC ownership"),
        ],
        dbus,
    )


def scenario_winter_low_soc_stages_before_65() -> ScenarioOutcome:
    """Low winter SoC with low PV history should go to 40% before 65%."""
    dbus = H.make_dbus(soc=30.0, min_soc=10.0, max_charge_current=-1.0)
    controller = H.make_controller(dbus)
    controller.state["pv_history"] = [1000.0, 1200.0, 1500.0, 1600.0]
    with simulated_date(datetime(2026, 1, 1, 12, 0, 0)):
        first_target, first_mode = controller.determine_target_soc(1000.0, 30.0)
        second_target, second_mode = controller.determine_target_soc(1000.0, 42.0)
    return H.scenario_result(
        "winter-low-soc-stages-before-65",
        [
            ((first_target, first_mode) == (40.0, "Winter Low PV Stage"), "winter low-SoC stage did not select 40%"),
            ((second_target, second_mode) == (65.0, "Winter"), "winter did not continue to 65% after 40% stage"),
        ],
        dbus,
    )


def scenario_soft_grid_target_still_progresses() -> ScenarioOutcome:
    """Very high house load should still allow the configured minimum progress current."""
    dbus = H.make_dbus(soc=50.0, min_soc=10.0, max_charge_current=-1.0, house_load=10000.0)
    controller = H.make_controller(dbus)
    ok = H.run_once_at(controller, datetime(2026, 1, 1, 23, 30, 0))
    written_current = H.raw(dbus, M.SERVICE_SETTINGS, H.max_charge_path)
    return H.scenario_result(
        "soft-grid-target-still-progresses",
        [
            (ok, "controller should run with valid SoC"),
            (written_current == M.GRID_SOFT_MIN_CHARGE_CURRENT_A, "minimum progress current was not applied"),
        ],
        dbus,
    )


def scenario_manual_lower_dvcc_is_not_raised() -> ScenarioOutcome:
    """A stricter manual DVCC limit must not be raised by the controller."""
    dbus = H.make_dbus(soc=50.0, min_soc=10.0, max_charge_current=30.0, house_load=1500.0)
    controller = H.make_controller(dbus)
    ok = H.run_once_at(controller, datetime(2026, 1, 1, 23, 30, 0))
    current_writes = H.writes_to(dbus, H.max_charge_path)
    return H.scenario_result(
        "manual-lower-dvcc-is-not-raised",
        [
            (ok, "controller should run with valid SoC"),
            (H.raw(dbus, M.SERVICE_SETTINGS, H.max_charge_path) == 30.0, "manual lower DVCC value was raised"),
            (not current_writes, "controller wrote MaxChargeCurrent despite stricter manual value"),
        ],
        dbus,
    )


def scenario_dvcc_capture_and_restore() -> ScenarioOutcome:
    """A script-owned DVCC limit should be restored after the target is reached."""
    dbus = H.make_dbus(soc=50.0, min_soc=10.0, max_charge_current=-1.0, house_load=1500.0)
    controller = H.make_controller(dbus)
    first_ok = H.run_once_at(controller, datetime(2026, 1, 1, 23, 30, 0))
    H.set_raw(dbus, M.SERVICE_SYSTEM, H.soc_path, 70.0)
    second_ok = H.run_once_at(controller, datetime(2026, 1, 2, 12, 0, 0))
    return H.scenario_result(
        "dvcc-capture-and-restore",
        [
            (first_ok and second_ok, "controller should run both iterations"),
            (H.raw(dbus, M.SERVICE_SETTINGS, H.max_charge_path) == -1.0, "DVCC value was not restored to -1"),
            (not bool(controller.state["max_charge_current_raw_set"]), "captured DVCC restore state was not cleared"),
        ],
        dbus,
    )


def scenario_external_dvcc_takeover_releases_ownership() -> ScenarioOutcome:
    """External DVCC edits should make the controller release ownership."""
    dbus = H.make_dbus(soc=50.0, min_soc=10.0, max_charge_current=-1.0, house_load=1500.0)
    controller = H.make_controller(dbus)
    first_ok = H.run_once_at(controller, datetime(2026, 1, 1, 23, 30, 0))
    H.set_raw(dbus, M.SERVICE_SETTINGS, H.max_charge_path, 20.0)
    second_ok = H.run_once_at(controller, datetime(2026, 1, 1, 23, 45, 0))
    return H.scenario_result(
        "external-dvcc-takeover-releases-ownership",
        [
            (first_ok and second_ok, "controller should run both iterations"),
            (H.raw(dbus, M.SERVICE_SETTINGS, H.max_charge_path) == 20.0, "external stricter DVCC value was overwritten"),
            (not bool(controller.state["charge_current_owned_by_script"]), "DVCC ownership was not released"),
            (any("changed externally" in log for log in dbus.logs), "external change was not logged"),
        ],
        dbus,
    )


def scenario_invalid_inputs_are_ignored() -> ScenarioOutcome:
    """Invalid SoC and MinSoC readings should be ignored safely."""
    bad_soc_dbus = H.make_dbus(soc=101.0, min_soc=10.0, max_charge_current=-1.0)
    bad_soc_controller = H.make_controller(bad_soc_dbus)
    bad_soc_ok = H.run_once_at(bad_soc_controller, datetime(2026, 1, 1, 23, 30, 0))

    bad_min_dbus = H.make_dbus(soc=50.0, min_soc=10.0, max_charge_current=-1.0)
    H.set_raw(bad_min_dbus, M.SERVICE_SETTINGS, H.min_soc_path, 150.0)
    bad_min_controller = H.make_controller(bad_min_dbus)
    bad_min_ok = H.run_once_at(bad_min_controller, datetime(2026, 1, 1, 23, 30, 0))
    return H.scenario_result(
        "invalid-inputs-are-ignored",
        [
            (not bad_soc_ok, "invalid SoC should stop run_once"),
            (not bad_soc_dbus.writes, "invalid SoC caused D-Bus writes"),
            (bad_min_ok, "run_once should return True after valid SoC even if MinSoC is invalid"),
            (not bad_min_dbus.writes, "invalid MinSoC caused D-Bus writes"),
        ],
    )


def scenario_missing_bms_does_not_guess_dvcc() -> ScenarioOutcome:
    """Missing BMS current should avoid guessing a temporary DVCC limit."""
    dbus = H.make_dbus(soc=50.0, min_soc=10.0, max_charge_current=-1.0, battery_max_current=None)
    controller = H.make_controller(dbus)
    ok = H.run_once_at(controller, datetime(2026, 1, 1, 23, 30, 0))
    return H.scenario_result(
        "missing-bms-does-not-guess-dvcc",
        [
            (ok, "controller should run with valid SoC"),
            (H.raw(dbus, M.SERVICE_SETTINGS, H.min_soc_path) == 65.0, "MinSoC target was not applied"),
            (H.raw(dbus, M.SERVICE_SETTINGS, H.max_charge_path) == -1.0, "DVCC was changed without BMS max current"),
        ],
        dbus,
    )


def scenario_dbus_write_failures_do_not_crash() -> ScenarioOutcome:
    """Simulated D-Bus write failures should be logged and should not crash."""
    dbus = H.make_dbus(soc=50.0, min_soc=10.0, max_charge_current=-1.0)
    dbus.fail_writes_for.add((M.SERVICE_SETTINGS, H.min_soc_path))
    dbus.fail_writes_for.add((M.SERVICE_SETTINGS, H.max_charge_path))
    controller = H.make_controller(dbus)
    ok = H.run_once_at(controller, datetime(2026, 1, 1, 23, 30, 0))
    return H.scenario_result(
        "dbus-write-failures-do-not-crash",
        [
            (ok, "controller should complete the iteration despite write failures"),
            (any("Simulated write failure" in log for log in dbus.logs), "write failure was not logged"),
        ],
        dbus,
    )


SCENARIOS: list[Scenario] = [
    Scenario("no-battery-fails-safe", scenario_no_battery_fails_safe),
    Scenario("summer-manual-override-is-preserved", scenario_summer_manual_override_is_preserved),
    Scenario("transition-40-percent-is-staged", scenario_transition_40_percent_is_staged),
    Scenario("charge-window-applies-target-and-limit", scenario_charge_window_applies_target_and_limit),
    Scenario("winter-low-soc-stages-before-65", scenario_winter_low_soc_stages_before_65),
    Scenario("soft-grid-target-still-progresses", scenario_soft_grid_target_still_progresses),
    Scenario("manual-lower-dvcc-is-not-raised", scenario_manual_lower_dvcc_is_not_raised),
    Scenario("dvcc-capture-and-restore", scenario_dvcc_capture_and_restore),
    Scenario("external-dvcc-takeover-releases-ownership", scenario_external_dvcc_takeover_releases_ownership),
    Scenario("invalid-inputs-are-ignored", scenario_invalid_inputs_are_ignored),
    Scenario("missing-bms-does-not-guess-dvcc", scenario_missing_bms_does_not_guess_dvcc),
    Scenario("dbus-write-failures-do-not-crash", scenario_dbus_write_failures_do_not_crash),
]


def selected_scenarios(names: list[str]) -> list[Scenario]:
    """Return scenarios selected by CLI names."""
    if not names or "all" in names:
        return SCENARIOS
    known = {scenario.name: scenario for scenario in SCENARIOS}
    missing = [name for name in names if name not in known]
    if missing:
        raise SystemExit(f"Unknown scenario(s): {', '.join(missing)}")
    return [known[name] for name in names]


def run_scenarios(scenarios: list[Scenario], verbose: bool) -> int:
    """Run scenarios and return a process exit code."""
    failures = 0
    for scenario in scenarios:
        outcome = scenario.run()
        status = "PASS" if outcome.passed else "FAIL"
        print(f"{status} {outcome.name}")
        if verbose or not outcome.passed:
            for detail in outcome.details:
                print(f"  - {detail}")
        if not outcome.passed:
            failures += 1
    print(f"\n{len(scenarios) - failures}/{len(scenarios)} scenarios passed")
    return 1 if failures else 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "scenarios",
        nargs="*",
        help="Scenario names to run, or 'all'. Omit to run all scenarios.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print details for passing scenarios too.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    """Command-line entrypoint."""
    args = parse_args(sys.argv[1:] if argv is None else argv)
    scenarios = selected_scenarios(cast(list[str], args.scenarios))
    return run_scenarios(scenarios, bool(args.verbose))


if __name__ == "__main__":
    raise SystemExit(main())

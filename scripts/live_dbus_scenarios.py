# SPDX-License-Identifier: GPL-3.0-or-later
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from live_dbus_core import FAKE_SETTINGS_SERVICE, FAKE_SYSTEM_SERVICE, M, Scenario, ScenarioOutcome
from live_dbus_harness import LiveHarness, RemoteHarness

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
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.min_soc_path) == 55.0, "MinSoC was not raised to 55%"),
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
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.min_soc_path) == 55.0, "MinSoC target was not applied"),
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.max_charge_path) == -1.0, "DVCC was changed without BMS current"),
        ],
    )


def remote_broken_power_sensors(harness: RemoteHarness) -> ScenarioOutcome:
    """Broken PV/grid/voltage values should fall back without crashing."""
    harness.reset_values(soc=50.0, min_soc=10.0, max_charge_current=-1.0, voltage=None)
    for phase in M.PHASES:
        harness.set_value(FAKE_SYSTEM_SERVICE, M.AC_GRID_POWER_PATH.format(phase=phase), None)
        harness.set_value(FAKE_SYSTEM_SERVICE, M.AC_CONSUMPTION_ON_INPUT_POWER_PATH.format(phase=phase), None)
        harness.set_value(FAKE_SYSTEM_SERVICE, M.AC_PV_ON_GRID_POWER_PATH.format(phase=phase), None)
        harness.set_value(FAKE_SYSTEM_SERVICE, M.AC_PV_ON_OUTPUT_POWER_PATH.format(phase=phase), None)
    controller = harness.make_controller()
    ok = harness.run_once_at(controller, datetime(2026, 1, 1, 23, 30, 0))
    current = harness.get_value(FAKE_SETTINGS_SERVICE, harness.max_charge_path)
    return harness.outcome(
        "live-broken-power-sensors-use-fallbacks",
        [
            (ok, "controller should complete with broken power sensors"),
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.min_soc_path) == 55.0, "MinSoC target was not applied"),
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
            (harness.get_value(FAKE_SETTINGS_SERVICE, harness.min_soc_path) == 55.0, "MinSoC was not raised to 55%"),
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



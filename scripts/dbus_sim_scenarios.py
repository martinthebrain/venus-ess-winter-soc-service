# SPDX-License-Identifier: GPL-3.0-or-later
from __future__ import annotations

from datetime import datetime

from dbus_sim_core import M, Scenario, ScenarioOutcome, simulated_date
from dbus_sim_harness import H

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


def scenario_winter_low_soc_stages_before_target() -> ScenarioOutcome:
    """Low winter SoC with low PV history should go to 40% before 55%."""
    dbus = H.make_dbus(soc=30.0, min_soc=10.0, max_charge_current=-1.0)
    controller = H.make_controller(dbus)
    controller.state["pv_history"] = [1000.0, 1200.0, 1500.0, 1600.0]
    with simulated_date(datetime(2026, 1, 1, 12, 0, 0)):
        first_target, first_mode = controller.determine_target_soc(1000.0, 30.0)
        second_target, second_mode = controller.determine_target_soc(1000.0, 42.0)
    return H.scenario_result(
        "winter-low-soc-stages-before-target",
        [
            ((first_target, first_mode) == (40.0, "Winter Low PV Stage"), "winter low-SoC stage did not select 40%"),
            ((second_target, second_mode) == (55.0, "Winter"), "winter did not continue to 55% after 40% stage"),
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
            (H.raw(dbus, M.SERVICE_SETTINGS, H.min_soc_path) == 55.0, "MinSoC target was not applied"),
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
    Scenario("winter-low-soc-stages-before-target", scenario_winter_low_soc_stages_before_target),
    Scenario("soft-grid-target-still-progresses", scenario_soft_grid_target_still_progresses),
    Scenario("manual-lower-dvcc-is-not-raised", scenario_manual_lower_dvcc_is_not_raised),
    Scenario("dvcc-capture-and-restore", scenario_dvcc_capture_and_restore),
    Scenario("external-dvcc-takeover-releases-ownership", scenario_external_dvcc_takeover_releases_ownership),
    Scenario("invalid-inputs-are-ignored", scenario_invalid_inputs_are_ignored),
    Scenario("missing-bms-does-not-guess-dvcc", scenario_missing_bms_does_not_guess_dvcc),
    Scenario("dbus-write-failures-do-not-crash", scenario_dbus_write_failures_do_not_crash),
]



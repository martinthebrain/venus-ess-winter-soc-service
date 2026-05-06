import ast
import json
import os
import signal
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

from tests.helpers import M, FixedDatetime, charge_context, controller

from venus_ess_winter_soc_service import dbus_iface as DBUS_MOD
from venus_ess_winter_soc_service import dvcc as DVCC_MOD
from venus_ess_winter_soc_service import persistence as PERSISTENCE_MOD
from venus_ess_winter_soc_service import paths as PATHS
from venus_ess_winter_soc_service import socpolicy as SOC_POLICY_MOD
from venus_ess_winter_soc_service import storage as STORAGE_MOD
from venus_ess_winter_soc_service import tracking as TRACKING_MOD
from venus_ess_winter_soc_service import windows as WINDOWS_MOD

class RuntimeLogicTests(unittest.TestCase):
    def test_power_battery_and_charge_current_helpers(self):
        c = controller()
        for phase, value in zip(PATHS.PHASES, [1, 2, 3]):
            c.dbus.values[(M.SERVICE_SYSTEM, PATHS.AC_PV_ON_GRID_POWER_PATH.format(phase=phase))] = value
            c.dbus.values[(M.SERVICE_SYSTEM, PATHS.AC_PV_ON_OUTPUT_POWER_PATH.format(phase=phase))] = value
            c.dbus.values[(M.SERVICE_SYSTEM, PATHS.AC_GRID_POWER_PATH.format(phase=phase))] = value
            c.dbus.values[(M.SERVICE_SYSTEM, PATHS.AC_CONSUMPTION_ON_INPUT_POWER_PATH.format(phase=phase))] = value
        c.dbus.values[(M.SERVICE_SYSTEM, PATHS.DC_PV_POWER_PATH)] = 4
        c.dbus.values[(M.SERVICE_SYSTEM, PATHS.BATTERY_POWER_PATH)] = 5
        c.dbus.values[(M.SERVICE_SYSTEM, PATHS.BATTERY_VOLTAGE_PATH)] = 52
        self.assertEqual(c.get_total_pv_power(), 16)
        self.assertEqual(c.get_grid_power_net(), 6)
        self.assertEqual(c.get_battery_power(), 5)
        self.assertEqual(c.get_battery_voltage(), 52)
        self.assertEqual(c.get_house_load_power(), 6)

        c.dbus.values.clear()
        for phase, value in zip(PATHS.PHASES, [4, 5, 6]):
            c.dbus.values[(M.SERVICE_SYSTEM, PATHS.AC_CONSUMPTION_POWER_PATH.format(phase=phase))] = value
        self.assertEqual(c.get_house_load_power(), 15)
        c.dbus.values.clear()
        self.assertEqual(c.get_house_load_power(grid_power_net=-10, batt_power=-5), 0)
        self.assertEqual(c.get_house_load_power(grid_power_net=10, batt_power=5), 15)
        c.get_grid_power_net = lambda: None
        c.get_battery_power = lambda: None
        self.assertEqual(c.compute_house_load_fallback(), 0)

        c.dbus.services = ["com.victronenergy.battery.bad", M.PREFERRED_BATTERY_SERVICE, "com.victronenergy.battery.good"]
        c.dbus.values[(M.PREFERRED_BATTERY_SERVICE, PATHS.BMS_MAX_CHARGE_CURRENT_PATH)] = 200
        c.save_state_to_ram = mock.Mock()
        c.state["battery_service"] = M.PREFERRED_BATTERY_SERVICE
        self.assertEqual(c.get_battery_service(), M.PREFERRED_BATTERY_SERVICE)
        c.state["battery_service"] = None
        self.assertEqual(c.get_battery_service(), M.PREFERRED_BATTERY_SERVICE)
        self.assertEqual(c.get_battery_max_charge_current(), 200)
        c.state["battery_service"] = "missing"
        c.state["battery_service_last_scan_ts"] = M.time.time()
        self.assertIsNone(c.get_battery_service())
        c.state["battery_max_current_last"] = 123
        c.get_battery_service = lambda: None
        self.assertEqual(c.get_battery_max_charge_current(), 123)
        c.state["battery_max_current_last"] = None
        self.assertIsNone(c.get_battery_max_charge_current())
        c.get_battery_service = lambda: "svc"
        c.dbus.values[("svc", PATHS.BMS_MAX_CHARGE_CURRENT_PATH)] = 0
        self.assertIsNone(c.get_battery_max_charge_current())

        c = controller()
        c.dbus.services = ["com.victronenergy.battery.a", "com.victronenergy.battery.b"]
        c.dbus.values[("com.victronenergy.battery.a", PATHS.BMS_MAX_CHARGE_CURRENT_PATH)] = 20
        c.dbus.values[("com.victronenergy.battery.b", PATHS.BMS_MAX_CHARGE_CURRENT_PATH)] = 30
        c.save_state_to_ram = mock.Mock()
        self.assertEqual(c.get_battery_service(), "com.victronenergy.battery.b")
        c.dbus.services = []
        c.state["battery_service"] = None
        c.state["battery_service_last_scan_ts"] = 0
        self.assertIsNone(c.get_battery_service())
        self.assertIsNone(c.select_preferred_battery_service(["x"]))
        c.dbus.values[(M.PREFERRED_BATTERY_SERVICE, PATHS.BMS_MAX_CHARGE_CURRENT_PATH)] = 0
        self.assertIsNone(c.select_preferred_battery_service([M.PREFERRED_BATTERY_SERVICE]))

        c.state["max_charge_current_raw"] = 80
        c.state["max_charge_current_raw_set"] = True
        c.save_state_to_ram = mock.Mock()
        self.assertEqual(c.get_max_charge_current_raw(), 80)
        self.assertEqual(c.get_normal_charge_current(200), 80)
        c.state["normal_charge_current"] = None
        with mock.patch.object(DVCC_MOD, "NORMAL_CHARGE_CURRENT", 60):
            self.assertEqual(c.get_normal_charge_current(200), 80)
        c.state["max_charge_current_raw_set"] = False
        with mock.patch.object(DVCC_MOD, "NORMAL_CHARGE_CURRENT", 60):
            self.assertEqual(c.get_normal_charge_current(200), 60)
        c.state["normal_charge_current"] = 70
        self.assertEqual(c.get_normal_charge_current(200), 70)
        c.state["normal_charge_current"] = None
        self.assertEqual(c.get_normal_charge_current(200), 200)
        self.assertIsNone(c.get_normal_charge_current(None))

        self.assertEqual(c.compute_charge_current_limit(1000, None, 52), None)
        self.assertEqual(c.compute_charge_current_limit(1500, 200, 52), 41)
        self.assertEqual(c.compute_charge_current_limit(3950, 200, 52), M.GRID_SOFT_MIN_CHARGE_CURRENT_A)
        self.assertEqual(c.compute_charge_current_limit(3950, 5, 52), 5)
        self.assertGreater(c.compute_charge_current_limit(1000, 200, None), 0)
        self.assertEqual(c.available_grid_charge_power(1500), M.GRID_LOAD_LIMIT - 1500 - M.GRID_PAUSE_HEADROOM_W)
        self.assertEqual(c.clamp_to_normal_current(300, 200), 200)
        with mock.patch.object(DVCC_MOD, "SAFE_CHARGE_CURRENT_A", None):
            self.assertIsNone(c.compute_safe_charge_current(200))

    def test_capture_update_set_restore_and_status(self):
        c = controller()
        max_path = PATHS.MAX_CHARGE_CURRENT_PATH
        c.dbus.raw_values[(M.SERVICE_SETTINGS, max_path)] = 120
        c.save_state_to_ram = mock.Mock()

        c.capture_original_dvcc_before_limit(None)
        self.assertFalse(c.state["max_charge_current_raw_set"])
        c.capture_original_dvcc_before_limit(120)
        self.assertEqual(c.state["max_charge_current_raw"], 120)
        c.state["max_charge_current_raw_set"] = True
        c.capture_original_dvcc_before_limit(80)
        self.assertEqual(c.state["max_charge_current_raw"], 120)

        c.clear_saved_max_charge_current_raw()
        self.assertFalse(c.state["max_charge_current_raw_set"])
        self.assertIsNone(c.state["max_charge_current_raw"])
        c.clear_saved_max_charge_current_raw()

        self.assertTrue(c.would_restrict_charge_current(-1, 50))
        self.assertTrue(c.would_restrict_charge_current(120, 50))
        self.assertFalse(c.would_restrict_charge_current(40, 50))
        self.assertFalse(c.would_restrict_charge_current(50, 50))
        self.assertFalse(c._same_charge_current(None, 50))

        c.state["max_charge_current_raw_set"] = False
        c.state["max_charge_current_raw"] = None

        c.set_max_charge_current(None, "none")
        c.dbus.raw_values[(M.SERVICE_SETTINGS, max_path)] = None
        c.set_max_charge_current(10, "none")
        c.dbus.raw_values[(M.SERVICE_SETTINGS, max_path)] = 20
        c.set_max_charge_current(-1, "unlimited")
        self.assertEqual(c.dbus.sets, [])
        c.dbus.raw_values[(M.SERVICE_SETTINGS, max_path)] = -1
        c.set_max_charge_current(30, "limit")
        self.assertEqual(c.dbus.sets[-1][2], 30.0)
        self.assertEqual(c.state["max_charge_current_raw"], -1)
        c.state["max_charge_current_raw_set"] = False
        c.state["max_charge_current_raw"] = None
        c.last_charge_limit_set_ts = 0
        c.dbus.raw_values[(M.SERVICE_SETTINGS, max_path)] = 100
        c.set_max_charge_current(30, "limit")
        self.assertEqual(c.dbus.sets[-1][2], 30.0)
        self.assertEqual(c.state["max_charge_current_raw"], 100)
        c.last_charge_limit_set_ts = M.time.time()
        c.dbus.raw_values[(M.SERVICE_SETTINGS, max_path)] = 10
        before = len(c.dbus.sets)
        c.set_max_charge_current(30, "limit")
        self.assertEqual(len(c.dbus.sets), before)
        self.assertTrue(any("MaxChargeCurrent unchanged" in msg for msg in c.dbus.logs))
        c.dbus.raw_values[(M.SERVICE_SETTINGS, max_path)] = 100
        before = len(c.dbus.sets)
        self.assertTrue(c.set_max_charge_current(20, "limit"))
        self.assertEqual(len(c.dbus.sets), before + 1)

        c.capture_original_dvcc_before_limit = lambda _raw: False
        c.state["charge_current_owned_by_script"] = False
        c.dbus.raw_values[(M.SERVICE_SETTINGS, max_path)] = 100
        self.assertFalse(c.set_max_charge_current(20, "limit"))
        c.capture_original_dvcc_before_limit = M.WinterController.capture_original_dvcc_before_limit.__get__(c, M.WinterController)

        c.state["max_charge_current_raw"] = 100
        c.state["max_charge_current_raw_set"] = True
        c.state["charge_current_owned_by_script"] = True
        c.state["max_charge_current_script_last_set"] = 30
        c.dbus.raw_values[(M.SERVICE_SETTINGS, max_path)] = 30
        self.assertTrue(c.set_max_charge_current(30, "limit"))
        self.assertEqual(c.state["max_charge_current_script_last_set"], 30.0)

        c.last_charge_limit_set_ts = M.time.time()
        c.dbus.raw_values[(M.SERVICE_SETTINGS, max_path)] = 30
        before = len(c.dbus.sets)
        self.assertFalse(c.set_max_charge_current(40, "limit"))
        self.assertEqual(len(c.dbus.sets), before)

        c.last_charge_limit_set_ts = 0
        c.dbus.raw_values[(M.SERVICE_SETTINGS, max_path)] = 30
        self.assertTrue(c.set_max_charge_current(150, "limit"))
        self.assertEqual(c.dbus.sets[-1][2], 100)

        original_set_value = c.dbus.set_value
        c.dbus.set_value = lambda *_args: False
        c.dbus.raw_values[(M.SERVICE_SETTINGS, max_path)] = 50
        c.state["charge_current_owned_by_script"] = True
        c.state["max_charge_current_script_last_set"] = 50
        self.assertFalse(c.set_max_charge_current(20, "limit"))
        c.dbus.set_value = original_set_value

        c.state["charge_current_owned_by_script"] = True
        c.state["max_charge_current_script_last_set"] = 30
        c.state["max_charge_current_raw"] = 120
        c.state["max_charge_current_raw_set"] = True
        c.dbus.raw_values[(M.SERVICE_SETTINGS, max_path)] = 45
        self.assertTrue(c.set_max_charge_current(20, "limit"))
        self.assertTrue(any("changed externally" in msg for msg in c.dbus.logs))

        c.state["max_charge_current_raw"] = 90
        c.state["max_charge_current_raw_set"] = True
        c.last_charge_limit_set_ts = 0
        c.restore_normal_charge_current(200)
        self.assertEqual(c.dbus.sets[-1][2], 90.0)
        self.assertFalse(c.state["max_charge_current_raw_set"])
        c.restore_normal_charge_current(200)
        with mock.patch.object(DVCC_MOD, "NORMAL_CHARGE_CURRENT", -1):
            c.restore_normal_charge_current(None)
            self.assertEqual(c.dbus.sets[-1][2], -1)
        c.restore_normal_charge_current(None)

        original_set_value = c.dbus.set_value
        c.dbus.set_value = lambda *_args: False
        c.dbus.raw_values[(M.SERVICE_SETTINGS, max_path)] = 50
        self.assertFalse(c.set_max_charge_current(-1, "Restore"))
        c.dbus.raw_values[(M.SERVICE_SETTINGS, max_path)] = -1
        self.assertTrue(c.set_max_charge_current(-1, "Restore"))
        self.assertFalse(c.set_max_charge_current(20, "Restore"))
        c.dbus.raw_values[(M.SERVICE_SETTINGS, max_path)] = 100
        c.last_charge_limit_set_ts = 0
        self.assertFalse(c.set_max_charge_current(20, "Restore"))
        c.dbus.set_value = original_set_value
        c.dbus.raw_values[(M.SERVICE_SETTINGS, max_path)] = 20
        self.assertTrue(c.set_max_charge_current(20, "Restore"))
        c.dbus.set_value = original_set_value

        c.sd_state_file = None
        c.state["last_status_log_ts"] = 0
        c.maybe_log_status(None, None)
        self.assertTrue(any("Status:" in msg for msg in c.dbus.logs))
        c.sd_state_file = Path("/tmp/sd")
        c.sd_next_try_ts = M.time.time() + 10
        c.state["last_status_log_ts"] = 0
        c.maybe_log_status(10, 20)
        c.sd_next_try_ts = 0
        c.sd_error_count = 1
        c.state["last_status_log_ts"] = 0
        c.maybe_log_status(10, 20)
        c.sd_error_count = 0
        c.sd_state_file = Path("/tmp/sd")
        c.state["last_status_log_ts"] = 0
        c.dbus.raw_values[(M.SERVICE_SETTINGS, max_path)] = None
        c.maybe_log_status(10, 20)
        before_logs = len(c.dbus.logs)
        c.maybe_log_status(10, 20)
        self.assertEqual(len(c.dbus.logs), before_logs)

    def test_balancing_and_target_logic(self):
        c = controller()
        c.save_state_to_ram = mock.Mock()
        c.is_winter_window = lambda _now=None: True
        now = 10_000.0
        self.assertEqual(c.loop_delta_seconds(now), 0)
        c.state["last_loop_ts"] = now - 60
        c.update_full_soc_tracking(M.BALANCING_FULL_SOC, now)
        self.assertEqual(c.state["last_loop_ts"], now)
        c.state["last_loop_ts"] = now - 60
        c.state["full_soc_seconds"] = M.FULL_SOC_CONFIRM_MINUTES * 60 - 1
        self.assertTrue(c.update_full_soc_tracking(M.BALANCING_FULL_SOC, now))
        self.assertEqual(c.state["last_full_ts"], now)
        c.save_state_to_ram.assert_called_with(force_persist=True)
        self.assertEqual(c.loop_delta_seconds(now - 1), 0)
        c.state["full_soc_seconds"] = 10
        c.is_winter_window = lambda _now=None: False
        self.assertTrue(c.track_full_soc_seconds(10, now, 1))
        self.assertFalse(c.track_full_soc_seconds(10, now, 1))

        c.state["balancing_active"] = False
        self.assertFalse(c.track_balancing_progress(50, now, 1))
        c.state["balancing_active"] = True
        c.state["balancing_start_ts"] = now - M.BALANCING_MAX_HOURS * 3600 - 1
        self.assertTrue(c.track_balancing_progress(50, now, 60))
        self.assertFalse(c.state["balancing_active"])
        c.state["balancing_active"] = True
        c.state["balancing_start_ts"] = now
        c.state["balance_full_seconds"] = 5
        self.assertTrue(c.update_balancing_full_seconds(10, 1))
        self.assertFalse(c.update_balancing_full_seconds(10, 1))
        c.state["balance_full_seconds"] = 5
        self.assertTrue(c.track_balancing_progress(10, now, 1))
        c.state["balance_full_seconds"] = M.BALANCING_DURATION_HOURS * 3600
        self.assertTrue(c.track_balancing_progress(M.BALANCING_FULL_SOC, now, 1))
        self.assertEqual(c.state["last_balance_ts"], now)
        c.save_state_to_ram.assert_called_with(force_persist=True)

        c.state.update(c.default_state())
        c.state["boot_ts"] = 0
        c.is_winter_window = lambda _now=None: True
        c.state["balancing_active"] = True
        self.assertFalse(c.should_start_balancing(999999))
        c.state["balancing_active"] = False
        c.state["last_balance_ts"] = 1
        self.assertTrue(c.should_start_balancing(M.BALANCING_INTERVAL_DAYS * 86400 + 2))
        c.state["last_balance_ts"] = 0
        c.state["last_full_ts"] = 1
        self.assertTrue(c.should_start_balancing(M.BALANCING_INTERVAL_DAYS * 86400 + 2))
        c.state["last_full_ts"] = 0
        self.assertFalse(c.should_start_balancing(1))
        self.assertTrue(c.should_start_balancing(M.BALANCING_BOOT_GRACE_HOURS * 3600 + 1))
        c.is_winter_window = lambda _now=None: False
        c.state["last_balance_attempt_ts"] = 0
        self.assertTrue(c.should_start_balancing(M.BALANCING_BOOT_GRACE_HOURS * 3600 + 1))
        c.is_winter_window = lambda _now=None: True
        c.state["last_balance_attempt_ts"] = M.BALANCING_BOOT_GRACE_HOURS * 3600
        self.assertFalse(c.should_start_balancing(M.BALANCING_BOOT_GRACE_HOURS * 3600 + 1))

        c.save_state_to_ram = mock.Mock()
        c.start_balancing(123)
        self.assertTrue(c.state["balancing_active"])
        with mock.patch.object(TRACKING_MOD, "datetime", FixedDatetime):
            FixedDatetime.value = datetime(2026, 1, 1, 12)
            c.should_start_balancing = lambda _ts: False
            self.assertEqual(c.determine_target_soc(123), (100.0, "Winter Balancing"))
            c.state["balancing_active"] = False
            self.assertEqual(c.determine_target_soc(123), (55.0, "Winter"))
            c.state["pv_history"] = [1000, 2000, 2500, 2800]
            self.assertEqual(c.determine_target_soc(123, current_soc=30), (40.0, "Winter Low PV Stage"))
            self.assertEqual(c.determine_target_soc(123, current_soc=40), (55.0, "Winter"))
            FixedDatetime.value = datetime(2026, 11, 10, 12)
            c.state["pv_history"] = [1, 2, 3, 4]
            self.assertEqual(c.determine_target_soc(123), (40.0, "Pre-Winter Low PV"))
            c.state["pv_history"] = [4000, 4001, 4002, 4003]
            self.assertEqual(c.determine_target_soc(123), (M.DEFAULT_SOC, "Default"))
            FixedDatetime.value = datetime(2026, 2, 10, 12)
            c.state["pv_history"] = [4000, 4001, 4002, 4003]
            self.assertEqual(c.determine_target_soc(123), (M.DEFAULT_SOC, "Post-Winter PV Recovered"))
            c.state["pv_history"] = [1000, 2000, 2500, 2800]
            self.assertEqual(c.determine_target_soc(123), (40.0, "Post-Winter Guard"))
            FixedDatetime.value = datetime(2026, 7, 1, 12)
            self.assertEqual(c.determine_target_soc(123), (M.DEFAULT_SOC, "Default"))
        c.state["pv_history"] = [1]
        self.assertIsNone(c.transition_history_ready())
        c.state["pv_history"] = "not-a-list"
        self.assertIsNone(c.transition_history_ready())
        self.assertFalse(c.has_transition_history_below_threshold())
        self.assertFalse(c.has_transition_history_above_threshold())
        c.state["balancing_active"] = False
        c.should_start_balancing = lambda _ts: True
        self.assertEqual(c.determine_winter_target(456), (100.0, "Winter Balancing"))
        c.state["balancing_active"] = False
        c.should_start_balancing = lambda _ts: False
        c.state["pv_history"] = [1000, 1000, 1000, 1000]
        self.assertTrue(c.should_use_winter_40_stage(30))
        self.assertFalse(c.should_use_winter_40_stage(None))

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

class RuntimePolicyRuntimeTests(unittest.TestCase):
    def test_pv_history_manual_override_context_and_apply(self):
        c = controller()
        c.save_state_to_ram = mock.Mock()
        self.assertFalse(c.same_min_soc(10.0, None))
        c.state["last_sample_date"] = "2026-01-01"
        c.state["pv_energy_ws"] = 600
        c.state["pv_time_s"] = 2
        c.roll_pv_day("2026-01-02", 100)
        self.assertEqual(c.state["pv_history"], [300])
        c.save_state_to_ram.assert_called_with(force_persist=True)
        c.save_state_to_ram.reset_mock()
        c.roll_pv_day("2026-01-03", 101)
        c.save_state_to_ram.assert_called_with(force_persist=False)
        c.state["current_day_samples"] = [10, 20]
        c.state["boot_ts"] = 0
        c.state["last_pv_integral_ts"] = 0
        self.assertEqual(c.compute_completed_pv_average(M.PV_FALLBACK_MIN_VALID_AGE_DAYS * 86400 + 200), 15)
        c.state["current_day_samples"] = []
        self.assertIsNone(c.compute_completed_pv_average(1))
        c.state["pv_history"] = [1, 2, 3, 4]
        c.add_pv_history_value(5)
        self.assertEqual(c.state["pv_history"], [2, 3, 4, 5])
        c.integrate_pv_sample(1, 2)
        c.state["pv_last_sample_ts"] = 100
        c.state["pv_last_sample_power"] = 10
        c.integrate_pv_sample(160, 30)
        self.assertGreater(c.state["pv_energy_ws"], 0)
        c.get_total_pv_power = lambda: 55
        c.collect_pv_sample(200)
        c.reset_pv_sample_gap()
        self.assertEqual(c.state["pv_last_sample_ts"], 0)
        with mock.patch.object(TRACKING_MOD, "datetime", FixedDatetime), mock.patch.object(M.time, "time", return_value=200):
            c.state["last_sample_date"] = "2026-01-02"
            c.roll_pv_day = mock.Mock()
            FixedDatetime.value = datetime(2026, 1, 3, 10)
            c.update_pv_history()
            c.roll_pv_day.assert_called_once_with("2026-01-03", 200)
            FixedDatetime.value = datetime(2026, 1, 3, 18)
            c.update_pv_history()

        c.state["min_soc_last_seen"] = None
        self.assertTrue(c.track_manual_min_soc_change(20, 1, False))
        c.state["min_soc_last_seen"] = 20
        self.assertFalse(c.track_manual_min_soc_change(20.05, 2, False))
        self.assertFalse(c.track_manual_min_soc_change(30, 3, True))
        c.state["min_soc_last_seen"] = 20
        self.assertTrue(c.track_manual_min_soc_change(30, 3, False))
        c.state["manual_override_until_ts"] = 1
        c._handle_summer_manual_override(30, 2, False)
        c.state["manual_override_until_ts"] = M.STATUS_LOG_INTERVAL_SECONDS + 100
        c.state["last_manual_override_log_ts"] = 0
        c.track_manual_min_soc_change = lambda *_args: False
        c.restore_normal_charge_current = mock.Mock()
        c.get_battery_max_charge_current = lambda: 50
        self.assertTrue(c._handle_summer_manual_override(30, M.STATUS_LOG_INTERVAL_SECONDS + 1, False))
        c.state["manual_override_until_ts"] = M.STATUS_LOG_INTERVAL_SECONDS + 100
        c.state["last_manual_override_log_ts"] = M.STATUS_LOG_INTERVAL_SECONDS
        self.assertTrue(c._handle_summer_manual_override(30, M.STATUS_LOG_INTERVAL_SECONDS + 1, False))

        c.state["balancing_active"] = False
        c.state["charging_mode_active"] = False
        c.state["charging_paused"] = False
        c.get_grid_power_net = lambda: -100
        c.get_battery_max_charge_current = lambda: 100
        c.get_battery_voltage = lambda: 52
        c.get_battery_power = lambda: 0
        c.get_house_load_power = lambda *_args: 1000
        c.is_boot_recovery_window = lambda _ts: False
        ctx = c._build_charge_context(50, 40, 40, datetime(2026, 1, 1, 12), 1)
        self.assertFalse(ctx["needs_charge"])
        c.state["charging_mode_active"] = True
        self.assertFalse(c.boot_recover_active(True, M.WINTER_TARGET_SOC, M.WINTER_TARGET_SOC, 1))
        c.state["charging_mode_active"] = False
        self.assertTrue(c.track_charge_deficit(True, 100))
        self.assertFalse(c.track_charge_deficit(True, 200))
        self.assertEqual(c.charge_window_hours(100 + (2 * 86400)), M.CHARGE_WINDOW_BASE_HOURS * 2)
        self.assertEqual(c.charge_window_hours(100 + (4 * 86400)), M.CHARGE_WINDOW_BASE_HOURS * 4)
        self.assertTrue(c.track_charge_deficit(False, 300))
        with mock.patch.object(SOC_POLICY_MOD, "CHARGE_WINDOW_BASE_HOURS", 24):
            self.assertTrue(c.is_charge_window_active(datetime(2026, 1, 1, 12), 400))
        self.assertFalse(c.should_stage_charge_target(10))
        self.assertTrue(c.should_stage_charge_target(40))
        self.assertTrue(c.should_stage_charge_target(M.WINTER_TARGET_SOC))
        c.is_pv_history_window = lambda _now=None: True
        c.is_winter_window = lambda _now=None: False
        self.assertTrue(c.is_sd_window())
        c.is_pv_history_window = M.WinterController.is_pv_history_window.__get__(c, M.WinterController)
        c.is_winter_window = M.WinterController.is_winter_window.__get__(c, M.WinterController)
        with mock.patch.object(WINDOWS_MOD, "datetime", FixedDatetime):
            FixedDatetime.value = datetime(2026, 11, 10, 12)
            self.assertTrue(c.is_pv_history_window())
            FixedDatetime.value = datetime(2026, 1, 1, 12)
            self.assertTrue(c.is_winter_window())
        c.state["boot_ts"] = 0
        c.is_boot_recovery_window = M.WinterController.is_boot_recovery_window.__get__(c, M.WinterController)
        self.assertTrue(c.is_boot_recovery_window(1))
        with mock.patch.object(M.time, "time", return_value=1):
            self.assertTrue(c.is_boot_recovery_window())
        self.assertTrue(c.set_min_soc("/min", 12))
        c.dbus.set_value = lambda *_args: False
        self.assertFalse(c.set_min_soc("/min", 12))

        c.dbus.raw_values[(M.SERVICE_SETTINGS, PATHS.MIN_SOC_PATH)] = None
        c.apply_soc_logic(10, 50)
        c.state["last_min_soc_invalid_log_ts"] = M.time.time()
        c.apply_soc_logic(10, 50)
        c._handle_summer_manual_override = lambda *_args: True
        c.dbus.raw_values[(M.SERVICE_SETTINGS, PATHS.MIN_SOC_PATH)] = 20
        c.apply_soc_logic(10, 50)
        c.dbus.raw_values[(M.SERVICE_SETTINGS, PATHS.MIN_SOC_PATH)] = 5
        c._handle_summer_manual_override = lambda *_args: False
        c.save_state_to_ram = mock.Mock()
        c._build_charge_context = lambda *_args: {"needs_charge": True, "charge_deficit_changed": True}
        called = {}
        c._handle_charge_needed = lambda *args: called.setdefault("needed", args)
        c.apply_soc_logic(10, 5)
        self.assertIn("needed", called)
        c.save_state_to_ram.assert_called()
        c._build_charge_context = lambda *_args: {"needs_charge": False, "battery_max_current": 77}
        c._handle_charge_not_needed = lambda *args: called.setdefault("not_needed", args)
        c.apply_soc_logic(10, 50)
        self.assertIn("not_needed", called)

    def test_charge_pause_sd_window_run_once_and_read_soc(self):
        c = controller()
        c.save_state_to_ram = mock.Mock()
        c.set_min_soc = mock.Mock()
        c.restore_normal_charge_current = mock.Mock()
        c.maybe_log_status = mock.Mock()
        c.state["charging_mode_active"] = False
        c._handle_charge_needed(
            "/min",
            target_soc=50,
            current_soc=30,
            current_setting=10,
            context=charge_context(time_ok=False, effective_active=True, house_load=3900),
        )
        self.assertTrue(c.state["charging_paused"])
        self.assertTrue(c.state["charging_mode_active"])
        c.set_min_soc.assert_called_with("/min", 30)
        c.restore_normal_charge_current.assert_called_with(100)
        c.state["charging_paused"] = False
        c.state["charging_mode_active"] = True
        c.set_min_soc.reset_mock()
        c.restore_normal_charge_current.reset_mock()
        c._handle_charge_needed(
            "/min",
            target_soc=10,
            current_soc=5,
            current_setting=0,
            context=charge_context(time_ok=False, stage_charge_target=False, house_load=3900),
        )
        c.set_min_soc.assert_called_with("/min", 10)
        self.assertFalse(c.state["charging_paused"])
        self.assertFalse(c.state["charging_mode_active"])
        c.restore_normal_charge_current.assert_called_with(100)
        c.state["charging_paused"] = False
        c.state["charging_mode_active"] = False
        c.set_min_soc.reset_mock()
        c.restore_normal_charge_current.reset_mock()
        c._handle_charge_needed(
            "/min",
            target_soc=40,
            current_soc=30,
            current_setting=10,
            context=charge_context(time_ok=False, house_load=3900),
        )
        c.set_min_soc.assert_called_with("/min", 30)
        self.assertTrue(c.state["charging_paused"])
        self.assertTrue(c.state["charging_mode_active"])
        c.restore_normal_charge_current.assert_called_with(100)
        c.compute_charge_current_limit = lambda *_args: None
        c.restore_normal_charge_current = mock.Mock()
        c._handle_charge_needed(
            "/min",
            target_soc=50,
            current_soc=30,
            current_setting=50,
            context=charge_context(house_load=1000),
        )
        c.restore_normal_charge_current.assert_called_with(100)
        c.dbus.logs.clear()
        c.state["charging_paused"] = False
        c.log_soc_raise_resume_if_needed(50, charge_context(effective_active=True))
        self.assertEqual(c.dbus.logs, [])
        self.assertFalse(c.write_pause_soc_if_needed("/min", 30, 30))
        c.state["charging_paused"] = True
        self.assertFalse(c.should_write_pause_soc(30, 30))

        c.dbus.raw_values[(M.SERVICE_SYSTEM, PATHS.BATTERY_SOC_PATH)] = 50
        self.assertEqual(c.read_current_soc(), 50)
        c.dbus.raw_values[(M.SERVICE_SYSTEM, PATHS.BATTERY_SOC_PATH)] = 101
        self.assertIsNone(c.read_current_soc())

        c.state["ts"] = 1
        sd_data = {"ts": 2, "pv_history": [1], "last_balance_ts": 2, "max_charge_current_raw": 3}
        c.read_state_file = lambda _path: sd_data
        c.init_sd_state_cache = mock.Mock()
        c.refresh_sd_paths = mock.Mock()
        c.save_state_to_ram = mock.Mock()
        c.sd_state_file = Path("/tmp/sd")
        c.is_pv_history_window = lambda _now=None: True
        c.is_winter_window = lambda _now=None: True
        c.load_sd_state_window()
        self.assertEqual(c.state["pv_history"], [1])
        c.init_sd_state_cache.assert_called_once_with(sd_data)
        stale_sd = {"ts": 1, "pv_history": [9], "last_balance_ts": 99}
        c.state["ts"] = 10
        c.sd_last_signature = {"old": "signature"}
        c.sd_last_persist_ts = 99
        c.init_sd_state_cache.reset_mock()
        c.save_state_to_ram.reset_mock()
        c.read_state_file = lambda _path: stale_sd
        c.load_sd_state_window()
        self.assertEqual(c.state["pv_history"], [1])
        c.init_sd_state_cache.assert_not_called()
        c.save_state_to_ram.assert_not_called()
        self.assertIsNone(c.sd_last_signature)
        self.assertEqual(c.sd_last_persist_ts, 0)
        c.is_sd_window = lambda _now=None: True
        c.sd_window_active = False
        c.update_sd_window_state()
        self.assertTrue(c.sd_window_active)
        c.is_sd_window = lambda _now=None: False
        c.update_sd_window_state()
        self.assertFalse(c.sd_window_active)

        c.update_pv_history = mock.Mock()
        c.read_current_soc = mock.Mock(return_value=60)
        c.update_full_soc_tracking = mock.Mock(return_value=True)
        c.determine_target_soc = mock.Mock(return_value=(10, "Default"))
        c.log_mode_change = mock.Mock()
        c.update_sd_window_state = mock.Mock()
        c.apply_soc_logic = mock.Mock()
        self.assertTrue(c.run_once())
        c.log_mode_change = M.WinterController.log_mode_change.__get__(c, M.WinterController)
        c.log_mode_change("Default", 10)
        c.log_mode_change("Default", 10)
        c.log_mode_change("Winter", M.WINTER_TARGET_SOC)
        c.read_current_soc = mock.Mock(return_value=None)
        self.assertFalse(c.run_once())

    def test_package_branch_edges(self):
        c = controller()

        class DInt32(int):
            pass

        dbi = M.DBusInterface.__new__(M.DBusInterface)
        with mock.patch.object(M.dbus, "Int32", DInt32):
            self.assertIsInstance(dbi.coerce_dbus_value(DInt32(7)), DInt32)
        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch.object(DBUS_MOD, "LOG_FILE", str(Path(tmp) / "log.txt")), \
             mock.patch.object(DBUS_MOD, "LOG_MAX_BYTES", 999999):
            dbi.log("short")

        self.assertEqual(c.first_normal_charge_current([0, None, 5]), 5)

        c.is_pv_history_window = lambda _now=None: False
        c.is_winter_window = lambda _now=None: False
        self.assertEqual(c.sd_persistent_keys(), list(M.SD_PERSISTENT_BASE_KEYS))
        c.init_sd_state_cache({"ts": 7})
        self.assertEqual(c.sd_last_signature["max_charge_current_raw_set"], False)

        c.save_state_to_ram = mock.Mock()
        c.store_best_battery_service(["svc"], "svc")
        c.dbus.values[("svc", PATHS.BMS_MAX_CHARGE_CURRENT_PATH)] = 1
        c.store_best_battery_service(["svc"], "svc")
        c.dbus.values[("svc2", PATHS.BMS_MAX_CHARGE_CURRENT_PATH)] = 0
        self.assertIsNone(c.select_best_battery_service(["svc2"]))

        c.state["last_soc_invalid_log_ts"] = M.time.time()
        c.dbus.raw_values[(M.SERVICE_SYSTEM, PATHS.BATTERY_SOC_PATH)] = None
        self.assertIsNone(c.read_current_soc())
        self.assertFalse(any("SoC invalid" in msg for msg in c.dbus.logs))

        c.read_state_file = lambda _path: None
        c.load_sd_state_window()
        c.sd_window_active = False
        c.apply_sd_window_transition(False)
        self.assertFalse(c.sd_window_active)
        c.update_full_soc_tracking = mock.Mock(return_value=False)
        c.read_current_soc = mock.Mock(return_value=60)
        c.determine_target_soc = mock.Mock(return_value=(10, "Default"))
        c.apply_soc_logic = mock.Mock()
        self.assertTrue(c.run_once())

        c.state["charging_paused"] = True
        c.state["charging_mode_active"] = True
        self.assertFalse(c.pause_soc_raise("/min", 50, 30, 30))
        c.finish_pause_soc_raise = mock.Mock()
        c._handle_charge_needed(
            "/min",
            target_soc=50,
            current_soc=30,
            current_setting=30,
            context=charge_context(time_ok=False),
        )
        c.finish_pause_soc_raise.assert_not_called()
        c.clear_charge_state = mock.Mock(return_value=False)
        c.restore_normal_charge_current = mock.Mock()
        c._handle_charge_not_needed("/min", 10, 10, None)
        c.restore_normal_charge_current.assert_called_with(None)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "not_sd").mkdir()
            self.assertEqual(M.find_auto_sd([root]), (None, ""))

        c.state["last_sample_date"] = "2026-01-01"
        FixedDatetime.value = datetime(2026, 1, 1, 20)
        with mock.patch.object(TRACKING_MOD, "datetime", FixedDatetime):
            c.update_pv_history()
        c.state["last_pv_integral_ts"] = 100
        self.assertTrue(c.is_pv_fallback_old_enough(100 + M.PV_FALLBACK_MIN_VALID_AGE_DAYS * 86400))
        c.state["pv_last_sample_ts"] = 100
        c.integrate_pv_sample(100 + (M.LOOP_INTERVAL_SECONDS * 10), 20)
        self.assertEqual(c.state["pv_energy_ws"], 0.0)


if __name__ == "__main__":
    unittest.main()

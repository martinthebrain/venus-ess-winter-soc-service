import ast
import json
import os
import signal
import tempfile
import threading
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

from tests.test_socsteuerung_logic import M, charge_context


class RichDbusStub:
    def __init__(self):
        self.values = {}
        self.raw_values = {}
        self.logs = []
        self.sets = []
        self.services = []

    def get_value(self, service, path, default=0.0):
        return self.values.get((service, path), default)

    def get_raw_value(self, service, path, default=None):
        return self.raw_values.get((service, path), self.values.get((service, path), default))

    def set_value(self, service, path, value):
        self.sets.append((service, path, value))
        self.raw_values[(service, path)] = value
        return True

    def log(self, msg):
        self.logs.append(msg)

    def list_services(self, prefix):
        return [svc for svc in self.services if svc.startswith(prefix)]


class FixedDatetime:
    value = datetime(2026, 1, 1, 12, 0, 0)

    @classmethod
    def now(cls):
        return cls.value


def controller():
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


class HelperAndDbusTests(unittest.TestCase):
    def test_every_controller_function_has_docstring(self):
        source = Path(M.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)
        missing = [
            f"{node.name}:{node.lineno}"
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and not ast.get_docstring(node)
        ]
        self.assertEqual([], missing)

    def test_path_helpers_and_atomic_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sd = root / "card"
            sd.mkdir()
            self.assertTrue(M.path_exists(sd))

            with mock.patch.dict(os.environ, {"ESS_SD_PATH": str(sd)}, clear=True):
                self.assertEqual(M.find_sd_from_env([root])[0], sd)
                self.assertEqual(M.get_sd_path()[0], sd)

            with mock.patch.dict(os.environ, {"ESS_SD_PATH": str(root / "missing")}, clear=True):
                path, info = M.find_sd_from_env([root])
                self.assertIsNone(path)
                self.assertIn("not found", info)

            with mock.patch.dict(os.environ, {"ESS_SD_LABEL": "card"}, clear=True):
                self.assertEqual(M.find_sd_from_env([root])[0], sd)

            mmc = root / "mmcblk0p1"
            mmc.mkdir()
            self.assertEqual(M.find_auto_sd([root])[0], mmc)

            target = root / "state.json"
            M.atomic_write(target, '{"ok": true}', fsync=True)
            self.assertEqual(json.loads(target.read_text(encoding="utf-8")), {"ok": True})
            with mock.patch.object(M.os, "open", side_effect=OSError("no dir fsync")):
                M.atomic_write(target, '{"ok": false}', fsync=True)
            self.assertEqual(json.loads(target.read_text(encoding="utf-8")), {"ok": False})

    def test_path_helper_negative_branches(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            missing_root = root / "missing"
            with mock.patch.dict(os.environ, {}, clear=True):
                self.assertEqual(M.find_sd_from_env([root]), (None, ""))
                self.assertEqual(M.find_auto_sd([missing_root]), (None, ""))
            with mock.patch.dict(os.environ, {"ESS_SD_LABEL": "nope"}, clear=True):
                self.assertIn("not found", M.find_sd_from_env([root])[1])
            bad_root = mock.Mock()
            bad_root.exists.return_value = True
            bad_root.iterdir.side_effect = OSError("boom")
            with mock.patch.object(M, "path_exists", return_value=True):
                self.assertEqual(M.find_auto_sd([bad_root]), (None, ""))
            self.assertIsNone(M.find_auto_sd_in_root(root))
            with mock.patch.object(M, "find_sd_from_env", return_value=(None, "")), \
                 mock.patch.object(M, "find_auto_sd", return_value=(Path("/tmp/auto"), "auto")):
                self.assertEqual(M.get_sd_path()[1], "auto")
            with mock.patch.object(M, "find_sd_from_env", return_value=(None, "")), \
                 mock.patch.object(M, "find_auto_sd", return_value=(None, "")):
                self.assertEqual(M.get_sd_path(), (None, "No SD found"))

    def test_dbus_interface_success_fallbacks_and_logging(self):
        class DBoolean(int):
            pass

        class DInt32(int):
            pass

        class DDouble(float):
            pass

        class Obj:
            def __init__(self):
                self.value = 12
                self.sets = []
                self.fail_timeout_once = False

            def GetValue(self, **kwargs):
                if self.fail_timeout_once and "timeout" in kwargs:
                    self.fail_timeout_once = False
                    raise TypeError("old binding")
                return self.value

            def SetValue(self, value, **kwargs):
                if self.fail_timeout_once and "timeout" in kwargs:
                    self.fail_timeout_once = False
                    raise TypeError("old binding")
                self.sets.append(value)

        class Bus:
            def __init__(self, obj):
                self.obj = obj

            def get_object(self, *_args):
                return self.obj

            def list_names(self):
                return ["com.victronenergy.battery.a", "x"]

        obj = Obj()
        dbi = M.DBusInterface.__new__(M.DBusInterface)
        dbi.bus = Bus(obj)

        with mock.patch.object(M.dbus, "Boolean", DBoolean), \
             mock.patch.object(M.dbus, "Int16", DInt32), \
             mock.patch.object(M.dbus, "UInt16", DInt32), \
             mock.patch.object(M.dbus, "Int32", DInt32), \
             mock.patch.object(M.dbus, "UInt32", DInt32), \
             mock.patch.object(M.dbus, "Int64", DInt32), \
             mock.patch.object(M.dbus, "UInt64", DInt32), \
             mock.patch.object(M.dbus, "Double", DDouble), \
             mock.patch.object(M.dbus, "Byte", DInt32):
            obj.fail_timeout_once = True
            self.assertEqual(dbi.get_value("svc", "/p"), 12.0)
            self.assertEqual(dbi.get_value("svc", "/p"), 12.0)
            obj.fail_timeout_once = True
            self.assertEqual(dbi.get_raw_value("svc", "/p"), 12.0)
            self.assertTrue(dbi.set_value("svc", "/p", True))
            self.assertTrue(dbi.set_value("svc", "/p", 4.5))
            obj.fail_timeout_once = True
            self.assertTrue(dbi.set_value("svc", "/p", 7))
            sentinel = object()
            self.assertIs(dbi.coerce_dbus_value(sentinel), sentinel)

        obj.value = None
        self.assertEqual(dbi.get_value("svc", "/p", 3), 3)
        self.assertIsNone(dbi.get_raw_value("svc", "/p", None))
        obj.value = -1
        self.assertEqual(dbi.get_value("svc", "/p", 8), 8)
        self.assertEqual(dbi.list_services("com.victronenergy.battery"), ["com.victronenergy.battery.a"])

        class FailingBus:
            def get_object(self, *_args):
                raise RuntimeError("boom")

            def list_names(self):
                raise RuntimeError("boom")

        dbi.bus = FailingBus()
        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch.object(M, "LOG_FILE", str(Path(tmp) / "log.txt")), \
             mock.patch.object(M, "LOG_MAX_BYTES", 1), \
             mock.patch.object(M, "LOG_TRUNCATE_BYTES", 1):
            self.assertEqual(dbi.get_value("svc", "/p", 9), 9)
            self.assertIsNone(dbi.get_raw_value("svc", "/p", None))
            self.assertFalse(dbi.set_value("svc", "/p", 1))
            dbi.log("hello")
            self.assertEqual(dbi.list_services("x"), [])

        with mock.patch.object(M.dbus, "SystemBus", return_value="bus"):
            self.assertEqual(M.DBusInterface().bus, "bus")
        with mock.patch("builtins.open", side_effect=OSError("outer")):
            dbi.log("outer fail")
        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch.object(M, "LOG_FILE", str(Path(tmp) / "log.txt")), \
             mock.patch.object(M.os.path, "getsize", side_effect=OSError("inner")):
            dbi.log("inner fail")


class StateAndSdTests(unittest.TestCase):
    def test_state_file_read_backup_and_merge(self):
        c = controller()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            good = root / "good.json"
            good.write_text('{"ts": 2, "last_mode": "X"}', encoding="utf-8")
            self.assertEqual(c.read_state_file(good)["last_mode"], "X")
            self.assertIsNone(c.read_state_file(root / "missing.json"))
            not_object = root / "list.json"
            not_object.write_text("[1, 2, 3]", encoding="utf-8")
            self.assertIsNone(c.read_state_file(not_object))
            self.assertTrue(any("does not contain a JSON object" in msg for msg in c.dbus.logs))

            bad = root / "bad.json"
            bad.write_text("{bad", encoding="utf-8")
            self.assertIsNone(c.read_state_file(bad))
            self.assertTrue(list(root.glob("bad.json.bad-*")))
            self.assertTrue(any("State file is unreadable" in msg for msg in c.dbus.logs))

            with mock.patch.object(M.os, "replace", side_effect=OSError("nope")):
                bad2 = root / "bad2.json"
                bad2.write_text("x", encoding="utf-8")
                c.backup_bad_state_file(bad2)
                self.assertTrue(any("Could not move bad state file aside" in msg for msg in c.dbus.logs))

        defaults = c.default_state()
        c.merge_state(defaults, {"last_mode": "Y", "unknown": 1})
        self.assertEqual(defaults["last_mode"], "Y")
        c.merge_state(defaults, None)
        self.assertTrue(c.sd_should_override_ram({"ts": 2}, {"ts": 1}))
        self.assertFalse(c.sd_should_override_ram({"ts": 1}, {"ts": 2}))
        self.assertFalse(c.sd_should_override_ram(None, {"ts": 2}))
        self.assertTrue(c.sd_should_override_ram({"ts": 2}, None))

    def test_load_state_and_sd_persistence_decisions(self):
        c = controller()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ram = root / "ram.json"
            sd_dir = root / "sd"
            sd_dir.mkdir()
            sd = sd_dir / "ess_winter_logic.json"
            ram.write_text(json.dumps({
                "ts": 1,
                "last_mode": "RAM",
                "last_balance_ts": 1,
                "max_charge_current_raw": None,
                "max_charge_current_raw_set": False,
                "charging_mode_active": True,
            }), encoding="utf-8")
            sd.write_text(json.dumps({
                "ts": 2,
                "last_mode": "SD",
                "last_balance_ts": 5,
                "max_charge_current_raw": -1,
                "max_charge_current_raw_set": True,
                "charging_mode_active": False,
            }), encoding="utf-8")
            c.sd_state_file = sd
            c.is_sd_window = lambda _now=None: True
            c.is_winter_window = lambda _now=None: True
            c.is_pv_history_window = lambda _now=None: False
            with mock.patch.object(M, "STATE_FILE", str(ram)):
                loaded = c.load_state()
                self.assertEqual(loaded["last_mode"], "RAM")
                self.assertEqual(loaded["last_balance_ts"], 5)
                self.assertEqual(loaded["max_charge_current_raw"], -1)
                self.assertTrue(loaded["max_charge_current_raw_set"])
                self.assertTrue(loaded["charging_mode_active"])
            ram.write_text(json.dumps({
                "ts": 3,
                "last_mode": "RAM-newer",
                "last_balance_ts": 9,
                "max_charge_current_raw": 80,
                "max_charge_current_raw_set": True,
            }), encoding="utf-8")
            c.sd_last_signature = {"from": "old-sd"}
            c.sd_last_persist_ts = 2
            with mock.patch.object(M, "STATE_FILE", str(ram)):
                loaded = c.load_state()
                self.assertEqual(loaded["last_mode"], "RAM-newer")
                self.assertEqual(loaded["last_balance_ts"], 9)
                self.assertEqual(loaded["max_charge_current_raw"], 80)
                self.assertIsNone(c.sd_last_signature)
                self.assertEqual(c.sd_last_persist_ts, 0)
            self.assertIsNone(c.read_state_file(None))
            ram.write_text(json.dumps({
                "ts": 1,
                "last_mode": "RAM",
                "last_balance_ts": 1,
                "max_charge_current_raw": None,
                "max_charge_current_raw_set": False,
                "charging_mode_active": True,
            }), encoding="utf-8")
            c.is_sd_window = lambda _now=None: False
            with mock.patch.object(M, "STATE_FILE", str(ram)):
                loaded = c.load_state()
                self.assertEqual(loaded["last_balance_ts"], 1)
                self.assertFalse(loaded["max_charge_current_raw_set"])
            c.is_sd_window = lambda _now=None: True
            sd.write_text(json.dumps({"ts": 2, "last_balance_ts": 7}), encoding="utf-8")
            ram.unlink()
            with mock.patch.object(M, "STATE_FILE", str(ram)):
                loaded = c.load_state()
                self.assertEqual(loaded["last_balance_ts"], 7)

        c.state["pv_history"] = [1, 2, 3, 4]
        c.state["last_sample_date"] = "2026-02-10"
        c.state["last_balance_ts"] = 10
        c.state["last_balance_attempt_ts"] = 11
        c.state["last_full_ts"] = 12
        c.is_sd_window = lambda _now=None: False
        c.persist_state_to_sd(force_persist=False)
        self.assertFalse(c.can_attempt_sd_write(False))
        c.sd_state_file = Path("/tmp/x")
        self.assertFalse(c.can_attempt_sd_write(True))
        c.is_sd_window = lambda _now=None: True
        c.sd_state_file = None
        c.refresh_sd_paths = lambda force=False: None
        self.assertFalse(c.can_attempt_sd_write(False))
        c.sd_state_file = Path("/tmp/x")
        c.is_pv_history_window = lambda _now=None: True
        c.is_winter_window = lambda _now=None: True
        self.assertIn("last_full_ts", c.sd_persistent_keys())
        state = {}
        c.merge_sd_persistent_state(state, {"ts": 2, "last_full_ts": 9}, {"ts": 3})
        self.assertEqual(state, {})
        sd_subset = {"ts": 4, "pv_history": [1, 2], "last_full_ts": 9}
        c.merge_sd_persistent_state(state, sd_subset, {"ts": 3})
        sd_subset["pv_history"].append(3)
        self.assertEqual(state["pv_history"], [1, 2])
        sig = c.build_sd_signature()
        self.assertIn("pv_history", sig)
        self.assertIn("last_balance_ts", sig)
        c.sd_last_signature = sig
        c.state["pv_history"].append(5)
        changed_sig = c.build_sd_signature()
        self.assertNotEqual(c.sd_last_signature, changed_sig)
        c.sd_next_try_ts = 20
        self.assertTrue(c.is_sd_backoff_active(10, False))
        self.assertFalse(c.is_sd_backoff_active(10, True))

        c.sd_last_persist_ts = 100
        self.assertTrue(c.should_skip_sd_persist(101, changed_sig, False))
        c.sd_last_persist_ts = 0
        c.sd_last_signature = changed_sig
        self.assertTrue(c.should_skip_sd_persist(M.SD_SAVE_INTERVAL_SECONDS + 1, changed_sig, False))
        c.sd_last_signature = None
        c.sd_pending_signature = changed_sig
        self.assertTrue(c.should_skip_sd_persist(M.SD_SAVE_INTERVAL_SECONDS + 1, changed_sig, False))
        self.assertFalse(c.should_skip_sd_persist(101, changed_sig, True))

        calls = []
        c.sd_pending_signature = None
        c._enqueue_sd_write = lambda **kwargs: calls.append(kwargs)
        c.persist_state_to_sd(force_persist=True)
        self.assertEqual(len(calls), 1)
        c.sd_next_try_ts = M.time.time() + 100
        c.persist_state_to_sd(force_persist=False)
        self.assertEqual(len(calls), 1)
        c.sd_next_try_ts = 0
        c.should_skip_sd_persist = lambda *_args: True
        c.persist_state_to_sd(force_persist=False)
        self.assertEqual(len(calls), 1)
        c.should_skip_sd_persist = M.WinterController.should_skip_sd_persist.__get__(c, M.WinterController)
        c._enqueue_sd_write = mock.Mock(side_effect=RuntimeError("sd"))
        c.persist_state_to_sd(force_persist=True)
        self.assertGreater(c.sd_error_count, 0)

        c.init_sd_state_cache({"ts": 9, "pv_history": [9], "last_balance_ts": 8})
        self.assertEqual(c.sd_last_persist_ts, 9)
        c.init_sd_state_cache(None)

    def test_enqueue_flush_signal_refresh_and_save(self):
        c = controller()
        c.sd_state_dir = Path("/tmp")
        c.sd_state_file = Path("/tmp/state.json")
        c._enqueue_sd_write("{}", {"a": 1}, True, c.sd_state_dir, c.sd_state_file)
        self.assertIsNotNone(c.sd_write_pending)
        c._enqueue_sd_write('{"b": 2}', {"b": 2}, False, c.sd_state_dir, c.sd_state_file)
        self.assertEqual(c.sd_write_pending["signature"], {"b": 2})

        self.assertFalse(c.flush_sd_writes(timeout_seconds=0.001))
        c.sd_write_pending = None
        c.sd_write_inflight = False
        self.assertTrue(c.flush_sd_writes(timeout_seconds=0.001))

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with mock.patch.dict(os.environ, {"ESS_SD_PATH": str(root)}, clear=True):
                c.sd_last_lookup_ts = 0
                c.refresh_sd_paths(force=True)
                self.assertEqual(c.sd_card_path, root)
                before = c.sd_last_lookup_ts
                c.refresh_sd_paths(force=False)
                self.assertEqual(c.sd_last_lookup_ts, before)
            with mock.patch.dict(os.environ, {}, clear=True), mock.patch.object(M, "get_sd_path", return_value=(None, "none")):
                c.refresh_sd_paths(force=True)
                self.assertIsNone(c.sd_state_file)

            c.state["x"] = 1
            c.persist_state_to_sd = mock.Mock()
            with mock.patch.object(M, "STATE_FILE", str(root / "ram.json")):
                c.save_state_to_ram(force_persist=True)
                self.assertTrue((root / "ram.json").exists())
                c.persist_state_to_sd.assert_called_with(force_persist=True)
            with mock.patch.object(M, "atomic_write", side_effect=OSError("fail")):
                c.save_state_to_ram()
                self.assertTrue(any("Could not save state" in msg for msg in c.dbus.logs))

        registered = []
        with mock.patch.object(M.signal, "signal", side_effect=lambda sig, handler: registered.append((sig, handler))):
            c.register_signal_handlers()
        self.assertEqual([item[0] for item in registered], [signal.SIGTERM, signal.SIGINT])
        c.save_state_to_ram = mock.Mock()
        c.flush_sd_writes = mock.Mock(return_value=True)
        with self.assertRaises(SystemExit), mock.patch.object(M.sys, "exit", side_effect=SystemExit):
            registered[0][1](signal.SIGTERM, None)


class RuntimeLogicTests(unittest.TestCase):
    def test_power_battery_and_charge_current_helpers(self):
        c = controller()
        for phase, value in zip(["L1", "L2", "L3"], [1, 2, 3]):
            c.dbus.values[(M.SERVICE_SYSTEM, f"/Ac/PvOnGrid/{phase}/Power")] = value
            c.dbus.values[(M.SERVICE_SYSTEM, f"/Ac/PvOnOutput/{phase}/Power")] = value
            c.dbus.values[(M.SERVICE_SYSTEM, f"/Ac/Grid/{phase}/Power")] = value
            c.dbus.values[(M.SERVICE_SYSTEM, f"/Ac/ConsumptionOnInput/{phase}/Power")] = value
        c.dbus.values[(M.SERVICE_SYSTEM, "/Dc/Pv/Power")] = 4
        c.dbus.values[(M.SERVICE_SYSTEM, "/Dc/Battery/Power")] = 5
        c.dbus.values[(M.SERVICE_SYSTEM, "/Dc/Battery/Voltage")] = 52
        self.assertEqual(c.get_total_pv_power(), 16)
        self.assertEqual(c.get_grid_power_net(), 6)
        self.assertEqual(c.get_battery_power(), 5)
        self.assertEqual(c.get_battery_voltage(), 52)
        self.assertEqual(c.get_house_load_power(), 6)

        c.dbus.values.clear()
        for phase, value in zip(["L1", "L2", "L3"], [4, 5, 6]):
            c.dbus.values[(M.SERVICE_SYSTEM, f"/Ac/Consumption/{phase}/Power")] = value
        self.assertEqual(c.get_house_load_power(), 15)
        c.dbus.values.clear()
        self.assertEqual(c.get_house_load_power(grid_power_net=-10, batt_power=-5), 0)
        self.assertEqual(c.get_house_load_power(grid_power_net=10, batt_power=5), 15)
        c.get_grid_power_net = lambda: None
        c.get_battery_power = lambda: None
        self.assertEqual(c.compute_house_load_fallback(), 0)

        c.dbus.services = ["com.victronenergy.battery.bad", M.PREFERRED_BATTERY_SERVICE, "com.victronenergy.battery.good"]
        c.dbus.values[(M.PREFERRED_BATTERY_SERVICE, "/Info/MaxChargeCurrent")] = 200
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
        c.dbus.values[("svc", "/Info/MaxChargeCurrent")] = 0
        self.assertIsNone(c.get_battery_max_charge_current())

        c = controller()
        c.dbus.services = ["com.victronenergy.battery.a", "com.victronenergy.battery.b"]
        c.dbus.values[("com.victronenergy.battery.a", "/Info/MaxChargeCurrent")] = 20
        c.dbus.values[("com.victronenergy.battery.b", "/Info/MaxChargeCurrent")] = 30
        c.save_state_to_ram = mock.Mock()
        self.assertEqual(c.get_battery_service(), "com.victronenergy.battery.b")
        c.dbus.services = []
        c.state["battery_service"] = None
        c.state["battery_service_last_scan_ts"] = 0
        self.assertIsNone(c.get_battery_service())
        self.assertIsNone(c.select_preferred_battery_service(["x"]))
        c.dbus.values[(M.PREFERRED_BATTERY_SERVICE, "/Info/MaxChargeCurrent")] = 0
        self.assertIsNone(c.select_preferred_battery_service([M.PREFERRED_BATTERY_SERVICE]))

        c.state["max_charge_current_raw"] = 80
        c.state["max_charge_current_raw_set"] = True
        c.save_state_to_ram = mock.Mock()
        self.assertEqual(c.get_max_charge_current_raw(), 80)
        self.assertEqual(c.get_normal_charge_current(200), 80)
        c.state["normal_charge_current"] = None
        with mock.patch.object(M, "NORMAL_CHARGE_CURRENT", 60):
            self.assertEqual(c.get_normal_charge_current(200), 80)
        c.state["max_charge_current_raw_set"] = False
        with mock.patch.object(M, "NORMAL_CHARGE_CURRENT", 60):
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
        with mock.patch.object(M, "SAFE_CHARGE_CURRENT_A", None):
            self.assertIsNone(c.compute_safe_charge_current(200))

    def test_capture_update_set_restore_and_status(self):
        c = controller()
        max_path = "/Settings/SystemSetup/MaxChargeCurrent"
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
        with mock.patch.object(M, "NORMAL_CHARGE_CURRENT", -1):
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
        with mock.patch.object(M, "datetime", FixedDatetime):
            FixedDatetime.value = datetime(2026, 1, 1, 12)
            c.should_start_balancing = lambda _ts: False
            self.assertEqual(c.determine_target_soc(123), (100.0, "Winter Balancing"))
            c.state["balancing_active"] = False
            self.assertEqual(c.determine_target_soc(123), (65.0, "Winter"))
            c.state["pv_history"] = [1000, 2000, 2500, 2800]
            self.assertEqual(c.determine_target_soc(123, current_soc=30), (40.0, "Winter Low PV Stage"))
            self.assertEqual(c.determine_target_soc(123, current_soc=40), (65.0, "Winter"))
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
        with mock.patch.object(M, "datetime", FixedDatetime), mock.patch.object(M.time, "time", return_value=200):
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
        self.assertFalse(c.boot_recover_active(True, 65, 65, 1))
        c.state["charging_mode_active"] = False
        self.assertTrue(c.track_charge_deficit(True, 100))
        self.assertFalse(c.track_charge_deficit(True, 200))
        self.assertEqual(c.charge_window_hours(100 + (2 * 86400)), M.CHARGE_WINDOW_BASE_HOURS * 2)
        self.assertEqual(c.charge_window_hours(100 + (4 * 86400)), M.CHARGE_WINDOW_BASE_HOURS * 4)
        self.assertTrue(c.track_charge_deficit(False, 300))
        with mock.patch.object(M, "CHARGE_WINDOW_BASE_HOURS", 24):
            self.assertTrue(c.is_charge_window_active(datetime(2026, 1, 1, 12), 400))
        self.assertFalse(c.should_stage_charge_target(10))
        self.assertTrue(c.should_stage_charge_target(40))
        self.assertTrue(c.should_stage_charge_target(65))
        c.is_pv_history_window = lambda _now=None: True
        c.is_winter_window = lambda _now=None: False
        self.assertTrue(c.is_sd_window())
        c.is_pv_history_window = M.WinterController.is_pv_history_window.__get__(c, M.WinterController)
        c.is_winter_window = M.WinterController.is_winter_window.__get__(c, M.WinterController)
        with mock.patch.object(M, "datetime", FixedDatetime):
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

        c.dbus.raw_values[(M.SERVICE_SETTINGS, "/Settings/CGwacs/BatteryLife/MinimumSocLimit")] = None
        c.apply_soc_logic(10, 50)
        c.state["last_min_soc_invalid_log_ts"] = M.time.time()
        c.apply_soc_logic(10, 50)
        c._handle_summer_manual_override = lambda *_args: True
        c.dbus.raw_values[(M.SERVICE_SETTINGS, "/Settings/CGwacs/BatteryLife/MinimumSocLimit")] = 20
        c.apply_soc_logic(10, 50)
        c.dbus.raw_values[(M.SERVICE_SETTINGS, "/Settings/CGwacs/BatteryLife/MinimumSocLimit")] = 5
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

        c.dbus.raw_values[(M.SERVICE_SYSTEM, "/Dc/Battery/Soc")] = 50
        self.assertEqual(c.read_current_soc(), 50)
        c.dbus.raw_values[(M.SERVICE_SYSTEM, "/Dc/Battery/Soc")] = 101
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
        c.log_mode_change("Winter", 65)
        c.read_current_soc = mock.Mock(return_value=None)
        self.assertFalse(c.run_once())


if __name__ == "__main__":
    unittest.main()

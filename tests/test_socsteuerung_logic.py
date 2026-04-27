import importlib.util
import sys
import types
import unittest
from datetime import datetime
from pathlib import Path


def _install_fake_dbus():
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

    class _SystemBus:
        def get_object(self, *_args, **_kwargs):
            raise RuntimeError("Not used in unit tests")

        def list_names(self, *_args, **_kwargs):
            return []

    fake.SystemBus = _SystemBus
    sys.modules["dbus"] = fake


def _load_module():
    _install_fake_dbus()
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


M = _load_module()


def charge_context(**overrides):
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


class _DbusStub:
    def __init__(self, current_setting):
        self.current_setting = current_setting
        self.logs = []

    def get_raw_value(self, *_args, **_kwargs):
        return self.current_setting

    def log(self, msg):
        self.logs.append(msg)


class WinterControllerLogicTests(unittest.TestCase):
    def _controller(self):
        c = M.WinterController.__new__(M.WinterController)
        c.state = {
            "balancing_active": False,
            "charging_mode_active": False,
            "charging_paused": False,
            "last_min_soc_invalid_log_ts": 0,
            "manual_override_until_ts": 0,
            "last_manual_override_log_ts": 0,
        }
        return c

    def test_apply_soc_logic_routes_to_not_needed_branch(self):
        c = self._controller()
        c.dbus = _DbusStub(current_setting=30.0)
        c.is_winter_window = lambda _now=None: False
        c.is_pv_history_window = lambda _now=None: False
        c._handle_summer_manual_override = lambda *_args, **_kwargs: False
        c._build_charge_context = lambda *_args, **_kwargs: {
            "needs_charge": False,
            "battery_max_current": 123.0,
        }

        calls = {}

        def _not_needed(path, target, current, battery_max):
            calls["args"] = (path, target, current, battery_max)

        c._handle_charge_not_needed = _not_needed
        c._handle_charge_needed = lambda *_args, **_kwargs: self.fail("unexpected charge-needed branch")
        c.set_min_soc = lambda *_args, **_kwargs: None

        c.apply_soc_logic(target_soc=10.0, current_soc=50.0)

        self.assertIn("args", calls)
        self.assertEqual(calls["args"][1], 10.0)
        self.assertEqual(calls["args"][3], 123.0)

    def test_handle_summer_manual_override_active_pauses_control(self):
        c = self._controller()
        c.dbus = _DbusStub(current_setting=40.0)
        c.state["charging_mode_active"] = True
        c.state["manual_override_until_ts"] = 50_000
        c.track_manual_min_soc_change = lambda *_args, **_kwargs: False
        c.get_battery_max_charge_current = lambda: 77.0

        calls = {"save": 0, "restore": None}
        c.save_state_to_ram = lambda *args, **kwargs: calls.__setitem__("save", calls["save"] + 1)
        c.restore_normal_charge_current = lambda v: calls.__setitem__("restore", v)

        active = c._handle_summer_manual_override(
            current_setting=40.0,
            now_ts=10_000,
            in_control_window=False,
        )

        self.assertTrue(active)
        self.assertFalse(c.state["charging_mode_active"])
        self.assertFalse(c.state["charging_paused"])
        self.assertGreaterEqual(calls["save"], 1)
        self.assertEqual(calls["restore"], 77.0)

    def test_build_charge_context_sets_boot_recovery_active(self):
        c = self._controller()
        c.get_grid_power_net = lambda: 1200.0
        c.get_battery_max_charge_current = lambda: 300.0
        c.get_battery_voltage = lambda: 52.0
        c.get_battery_power = lambda: 0.0
        c.get_house_load_power = lambda *_args, **_kwargs: 2800.0
        c.is_boot_recovery_window = lambda _ts: True

        ctx = c._build_charge_context(
            current_soc=15.0,
            target_soc=65.0,
            current_setting=65.0,
            now=datetime(2026, 1, 1, 1, 0, 0),
            now_ts=1_000.0,
        )

        self.assertTrue(ctx["needs_charge"])
        self.assertTrue(ctx["effective_active"])
        self.assertEqual(ctx["grid_import"], 1200.0)

    def test_handle_charge_needed_start_resume(self):
        c = self._controller()
        c.dbus = _DbusStub(current_setting=25.0)
        calls = {
            "set_min_soc": None,
            "set_max_charge_current": None,
            "save": 0,
        }

        c.set_min_soc = lambda path, value: calls.__setitem__("set_min_soc", (path, value))
        c.compute_charge_current_limit = lambda *_args: 44.0
        c.set_max_charge_current = lambda v, reason: calls.__setitem__("set_max_charge_current", (v, reason))
        c.restore_normal_charge_current = lambda *_args: self.fail("should not restore when limit exists")
        c.maybe_log_status = lambda *_args: None
        c.save_state_to_ram = lambda *args, **kwargs: calls.__setitem__("save", calls["save"] + 1)

        c._handle_charge_needed(
            current_limit_path="/Settings/CGwacs/BatteryLife/MinimumSocLimit",
            target_soc=40.0,
            current_soc=20.0,
            current_setting=25.0,
            context=charge_context(
                grid_import=500.0,
                battery_max_current=300.0,
            ),
        )

        self.assertEqual(calls["set_min_soc"][1], 40.0)
        self.assertEqual(calls["set_max_charge_current"], (44.0, "ChargeLimit"))
        self.assertEqual(calls["save"], 1)
        self.assertTrue(c.state["charging_mode_active"])
        self.assertFalse(c.state["charging_paused"])

    def test_handle_charge_not_needed_resets_flags_and_restores(self):
        c = self._controller()
        c.dbus = _DbusStub(current_setting=30.0)
        c.state["charging_mode_active"] = True
        calls = {"set_min_soc": None, "restore": None, "save": 0}

        c.set_min_soc = lambda path, value: calls.__setitem__("set_min_soc", (path, value))
        c.restore_normal_charge_current = lambda v: calls.__setitem__("restore", v)
        c.save_state_to_ram = lambda *args, **kwargs: calls.__setitem__("save", calls["save"] + 1)

        c._handle_charge_not_needed(
            current_limit_path="/Settings/CGwacs/BatteryLife/MinimumSocLimit",
            target_soc=10.0,
            current_setting=30.0,
            battery_max_current=250.0,
        )

        self.assertEqual(calls["set_min_soc"][1], 10.0)
        self.assertEqual(calls["restore"], 250.0)
        self.assertEqual(calls["save"], 1)
        self.assertFalse(c.state["charging_mode_active"])
        self.assertFalse(c.state["charging_paused"])


if __name__ == "__main__":
    unittest.main()

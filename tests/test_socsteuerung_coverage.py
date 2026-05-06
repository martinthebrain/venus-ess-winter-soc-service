import ast
import json
import os
import signal
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

from tests.helpers import M

from venus_ess_winter_soc_service import dbus_iface as DBUS_MOD
from venus_ess_winter_soc_service import dvcc as DVCC_MOD
from venus_ess_winter_soc_service import persistence as PERSISTENCE_MOD
from venus_ess_winter_soc_service import paths as PATHS
from venus_ess_winter_soc_service import socpolicy as SOC_POLICY_MOD
from venus_ess_winter_soc_service import storage as STORAGE_MOD
from venus_ess_winter_soc_service import tracking as TRACKING_MOD
from venus_ess_winter_soc_service import windows as WINDOWS_MOD


class HelperAndDbusTests(unittest.TestCase):
    def test_every_controller_function_has_docstring(self):
        paths = [Path(M.__file__), *Path(M.__file__).resolve().parent.joinpath("venus_ess_winter_soc_service").glob("*.py")]
        missing = []
        for path in paths:
            tree = ast.parse(path.read_text(encoding="utf-8"))
            missing.extend(
                f"{path.name}:{node.name}:{node.lineno}"
                for node in ast.walk(tree)
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and not ast.get_docstring(node)
            )
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
            with mock.patch.object(STORAGE_MOD, "path_exists", return_value=True):
                self.assertEqual(M.find_auto_sd([bad_root]), (None, ""))
            self.assertIsNone(M.find_auto_sd_in_root(root))
            with mock.patch.object(STORAGE_MOD, "find_sd_from_env", return_value=(None, "")), \
                 mock.patch.object(STORAGE_MOD, "find_auto_sd", return_value=(Path("/tmp/auto"), "auto")):
                self.assertEqual(M.get_sd_path()[1], "auto")
            with mock.patch.object(STORAGE_MOD, "find_sd_from_env", return_value=(None, "")), \
                 mock.patch.object(STORAGE_MOD, "find_auto_sd", return_value=(None, "")):
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
             mock.patch.object(DBUS_MOD, "LOG_FILE", str(Path(tmp) / "log.txt")), \
             mock.patch.object(DBUS_MOD, "LOG_MAX_BYTES", 1), \
             mock.patch.object(DBUS_MOD, "LOG_TRUNCATE_BYTES", 1):
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
             mock.patch.object(DBUS_MOD, "LOG_FILE", str(Path(tmp) / "log.txt")), \
             mock.patch.object(M.os.path, "getsize", side_effect=OSError("inner")):
            dbi.log("inner fail")

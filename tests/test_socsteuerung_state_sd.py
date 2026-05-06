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
            with mock.patch.object(PERSISTENCE_MOD, "STATE_FILE", str(ram)):
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
            with mock.patch.object(PERSISTENCE_MOD, "STATE_FILE", str(ram)):
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
            with mock.patch.object(PERSISTENCE_MOD, "STATE_FILE", str(ram)):
                loaded = c.load_state()
                self.assertEqual(loaded["last_balance_ts"], 1)
                self.assertFalse(loaded["max_charge_current_raw_set"])
            c.is_sd_window = lambda _now=None: True
            sd.write_text(json.dumps({"ts": 2, "last_balance_ts": 7}), encoding="utf-8")
            ram.unlink()
            with mock.patch.object(PERSISTENCE_MOD, "STATE_FILE", str(ram)):
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
            with mock.patch.dict(os.environ, {}, clear=True), mock.patch.object(PERSISTENCE_MOD, "get_sd_path", return_value=(None, "none")):
                c.refresh_sd_paths(force=True)
                self.assertIsNone(c.sd_state_file)

            c.state["x"] = 1
            c.persist_state_to_sd = mock.Mock()
            with mock.patch.object(PERSISTENCE_MOD, "STATE_FILE", str(root / "ram.json")):
                c.save_state_to_ram(force_persist=True)
                self.assertTrue((root / "ram.json").exists())
                c.persist_state_to_sd.assert_called_with(force_persist=True)
            with mock.patch.object(PERSISTENCE_MOD, "atomic_write", side_effect=OSError("fail")):
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

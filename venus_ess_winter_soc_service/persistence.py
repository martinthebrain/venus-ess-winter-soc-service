# SPDX-License-Identifier: GPL-3.0-or-later
from __future__ import annotations

import copy
import json
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, cast

from .base import ControllerMixinBase
from .config import *  # noqa: F403
from .storage import atomic_write, get_sd_path

class PersistenceMixin(ControllerMixinBase):
    def load_state(self) -> State:
        """Load RAM state and merge the seasonal SD subset over defaults."""
        defaults = self.default_state()
        ram_data = self.read_state_file(Path(STATE_FILE))
        sd_data = self.read_state_file(self.sd_state_file) if self.is_sd_window() else None
        self.merge_state(defaults, ram_data)
        self.merge_sd_state_if_newer(defaults, sd_data, ram_data)
        defaults["boot_ts"] = time.time()
        return defaults

    def default_state(self) -> State:
        """Return a complete fresh controller state with isolated mutable values."""
        return {
            "pv_history": [],
            "last_balance_ts": 0,
            "last_balance_attempt_ts": 0,
            "balancing_active": False,
            "balancing_start_ts": 0,
            "balance_full_seconds": 0.0,
            "full_soc_seconds": 0.0,
            "last_full_ts": 0,
            "charging_mode_active": False,
            "charging_paused": False,
            "charge_deficit_start_ts": 0,
            "battery_service": None,
            "battery_max_current_last": None,
            "battery_service_last_scan_ts": 0,
            "normal_charge_current": None,
            "max_charge_current_raw": None,
            "max_charge_current_raw_set": False,
            "charge_current_owned_by_script": False,
            "max_charge_current_script_last_set": None,
            "current_day_samples": [],
            "pv_energy_ws": 0.0,
            "pv_time_s": 0.0,
            "pv_last_sample_ts": 0.0,
            "pv_last_sample_power": 0.0,
            "last_pv_integral_ts": 0.0,
            "last_sample_date": "",
            "last_mode": "",
            "last_loop_ts": 0,
            "last_status_log_ts": 0,
            "last_soc_invalid_log_ts": 0,
            "last_min_soc_invalid_log_ts": 0,
            "manual_override_until_ts": 0,
            "min_soc_last_seen": None,
            "min_soc_last_script_set": None,
            "min_soc_last_script_set_ts": 0,
            "last_manual_override_log_ts": 0,
            "ts": 0,
            "boot_ts": time.time()
        }

    def sd_should_override_ram(self, sd_data: object, ram_data: object) -> bool:
        """Return True when the partial SD state is newer than RAM and may be merged."""
        if not isinstance(sd_data, dict):
            return False
        if not isinstance(ram_data, dict):
            return True
        sd_state = cast(State, sd_data)
        ram_state = cast(State, ram_data)
        return float(sd_state.get("ts", 0)) > float(ram_state.get("ts", 0))

    def sd_persistent_keys(self) -> list[str]:
        """Return the durable SD keys that are meaningful in the active season."""
        keys: list[str] = list(SD_PERSISTENT_BASE_KEYS)
        if self.is_pv_history_window():
            keys.extend(SD_PERSISTENT_PV_KEYS)
        if self.is_winter_window():
            keys.extend(SD_PERSISTENT_WINTER_KEYS)
        return keys

    def merge_sd_persistent_state(
        self,
        state: State,
        sd_data: object,
        ram_data: object = None,
    ) -> None:
        """Merge only explicit SD keys, never treating SD as a full controller state."""
        if not self.sd_should_override_ram(sd_data, ram_data) or not isinstance(sd_data, dict):
            return
        sd_state = cast(State, sd_data)
        for key in self.sd_persistent_keys():
            if key in sd_state:
                state[key] = copy.deepcopy(sd_state[key])

    def merge_sd_state_if_newer(self, state: State, sd_data: object, ram_data: object) -> bool:
        """Merge SD subset and seed cache only when SD is newer than the RAM state."""
        if not isinstance(sd_data, dict):
            return False
        sd_state = cast(State, sd_data)
        if self.sd_should_override_ram(sd_state, ram_data):
            self.merge_sd_persistent_state(state, sd_state, ram_data)
            self.init_sd_state_cache(sd_state)
            return True
        self.sd_last_signature = None
        self.sd_last_persist_ts = 0
        return False

    def merge_state(self, defaults: State, chosen: object) -> None:
        """Merge known keys from a persisted full state into the default structure."""
        if not isinstance(chosen, dict):
            return
        for key in defaults:
            if key in chosen:
                defaults[key] = chosen[key]

    def save_state_to_ram(self, force_persist: bool = False) -> None:
        """Persist the full state to RAM and optionally queue the SD subset."""
        self.state["ts"] = time.time()
        try:
            atomic_write(Path(STATE_FILE), json.dumps(self.state), fsync=False)
        except Exception as e:
            self.dbus.log(f"Could not save state to RAM: {e}")
        self.persist_state_to_sd(force_persist=force_persist)

    def read_state_file(self, path: Optional[Path]) -> Optional[State]:
        """Read a JSON state file and quarantine it when it cannot be parsed."""
        if not path:
            return None
        try:
            if path.exists():
                data = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    return cast(State, data)
                self.dbus.log(f"State file does not contain a JSON object: {path}")
        except Exception as e:
            self.dbus.log(f"State file is unreadable: {path}: {e}")
            self.backup_bad_state_file(path)
            return None
        return None

    def backup_bad_state_file(self, path: Path) -> None:
        """Move an unreadable state file aside so the failure can be inspected later."""
        try:
            stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            backup = path.with_name(f"{path.name}.bad-{stamp}")
            os.replace(path, backup)
            self.dbus.log(f"Bad state file moved aside: {backup}")
        except Exception as e:
            self.dbus.log(f"Could not move bad state file aside: {path}: {e}")

    def refresh_sd_paths(self, force: bool = False) -> None:
        """Resolve the SD mount lazily to tolerate card insertion/removal."""
        now = time.time()
        if not force and (now - self.sd_last_lookup_ts) < SD_LOOKUP_INTERVAL_SECONDS:
            return
        path, info = get_sd_path()
        self.sd_last_lookup_ts = now
        self.sd_info = info
        if path:
            self.sd_card_path = path
            self.sd_state_dir = path / SD_DIR_NAME
            self.sd_state_file = self.sd_state_dir / "ess_winter_logic.json"
        else:
            self.sd_card_path = None
            self.sd_state_dir = None
            self.sd_state_file = None

    def persist_state_to_sd(self, force_persist: bool = False) -> None:
        """Queue a partial SD state write with interval limiting and retry backoff."""
        if not self.can_attempt_sd_write(force_persist):
            return
        now = time.time()
        if self.is_sd_backoff_active(now, force_persist):
            return

        signature = self.build_sd_signature()
        payload = dict(signature)
        payload["ts"] = now
        if self.should_skip_sd_persist(now, signature, force_persist):
            return

        try:
            self._enqueue_sd_write(
                payload_json=json.dumps(payload),
                signature=signature,
                fsync=force_persist,
                sd_state_dir=self.sd_state_dir,
                sd_state_file=self.sd_state_file,
            )
        except Exception as e:
            self.sd_error_count += 1
            backoff = min(SD_BACKOFF_MAX_SECONDS, 2 ** min(self.sd_error_count, 8))
            self.sd_next_try_ts = now + backoff
            self.dbus.log(f"SD write error: {e} (backoff {backoff:.0f}s)")

    def can_attempt_sd_write(self, force_persist: bool) -> bool:
        """Return True when seasonal SD persistence is allowed and has a target path."""
        if not self.is_sd_window():
            return False
        if self.sd_state_file is not None:
            return True
        self.refresh_sd_paths(force=True)
        return self.sd_state_file is not None

    def is_sd_backoff_active(self, now: float, force_persist: bool) -> bool:
        """Return True when a non-forced SD write should wait for backoff."""
        return (now < self.sd_next_try_ts) and not force_persist

    def build_sd_signature(self) -> SdSignature:
        """Build the partial durable state that is worth persisting on SD."""
        return copy.deepcopy({key: self.state.get(key) for key in self.sd_persistent_keys()})

    def should_skip_sd_persist(
        self,
        now: float,
        signature: SdSignature,
        force_persist: bool,
    ) -> bool:
        """Skip unchanged or too-frequent non-forced SD writes."""
        if force_persist:
            return False
        if (now - self.sd_last_persist_ts) < SD_SAVE_INTERVAL_SECONDS:
            return True
        if signature == self.sd_last_signature:
            return True
        with self.sd_write_lock:
            return signature == self.sd_pending_signature

    def init_sd_state_cache(self, sd_data: object) -> None:
        """Seed write-deduplication metadata from an already loaded SD state."""
        if not isinstance(sd_data, dict):
            return
        sd_state = cast(State, sd_data)
        signature: SdSignature = {
            "max_charge_current_raw": sd_state.get("max_charge_current_raw"),
            "max_charge_current_raw_set": sd_state.get("max_charge_current_raw_set", False),
            "charge_current_owned_by_script": sd_state.get("charge_current_owned_by_script", False),
            "max_charge_current_script_last_set": sd_state.get("max_charge_current_script_last_set"),
        }
        if self.is_pv_history_window():
            signature["pv_history"] = sd_state.get("pv_history", [])
            signature["last_sample_date"] = sd_state.get("last_sample_date", "")
        if self.is_winter_window():
            signature["last_balance_ts"] = sd_state.get("last_balance_ts", 0)
            signature["last_balance_attempt_ts"] = sd_state.get("last_balance_attempt_ts", 0)
            signature["last_full_ts"] = sd_state.get("last_full_ts", 0)
        self.sd_last_signature = copy.deepcopy(signature)
        self.sd_last_persist_ts = float(sd_state.get("ts", 0))
        self.sd_pending_signature = None
        self.sd_pending_fsync = False

    def _enqueue_sd_write(
        self,
        payload_json: str,
        signature: SdSignature,
        fsync: bool,
        sd_state_dir: Optional[Path],
        sd_state_file: Optional[Path],
    ) -> None:
        """Queue an SD write for the background writer, keeping only the latest state."""
        signature = copy.deepcopy(signature)
        with self.sd_write_lock:
            # Last-write-wins: pending writes are coalesced into the newest payload.
            if self.sd_write_pending:
                self.sd_write_pending["payload_json"] = payload_json
                self.sd_write_pending["signature"] = signature
                # Preserve fsync if any coalesced write represented a critical event.
                self.sd_write_pending["fsync"] = self.sd_write_pending["fsync"] or fsync
                self.sd_write_pending["sd_state_dir"] = sd_state_dir
                self.sd_write_pending["sd_state_file"] = sd_state_file
            else:
                self.sd_write_pending = {
                    "payload_json": payload_json,
                    "signature": signature,
                    "fsync": fsync,
                    "sd_state_dir": sd_state_dir,
                    "sd_state_file": sd_state_file,
                }
            self.sd_pending_signature = signature
            self.sd_pending_fsync = self.sd_pending_fsync or fsync
            self.sd_write_event.set()

    def _sd_writer_loop(self) -> None:  # pragma: no cover
        """Background thread that performs SD writes and retries failed payloads."""
        while True:
            self.sd_write_event.wait()
            while self.process_next_sd_write():
                pass

    def process_next_sd_write(self) -> bool:  # pragma: no cover
        """Process one queued SD write request and return whether work was found."""
        req = self.pop_sd_write_request()
        if req is None:
            return False
        try:
            self.perform_sd_write(req)
            self.mark_sd_write_success(req, time.time())
        except Exception as e:
            self.mark_sd_write_failure(req, e, time.time())
        finally:
            self.clear_sd_write_inflight()
        return True

    def pop_sd_write_request(self) -> Optional[SdWriteRequest]:  # pragma: no cover
        """Take the newest queued SD write request from the background queue."""
        with self.sd_write_lock:
            req = self.sd_write_pending
            self.sd_write_pending = None
            if req is None:
                self.sd_write_event.clear()
                return None
            self.sd_write_inflight = True
            return req

    def perform_sd_write(self, req: SdWriteRequest) -> None:  # pragma: no cover
        """Write one SD request to disk, validating that a target path exists."""
        sd_state_dir = req.get("sd_state_dir")
        sd_state_file = req.get("sd_state_file")
        if sd_state_dir is None or sd_state_file is None:
            raise RuntimeError("SD path is not available")
        sd_state_dir.mkdir(parents=True, exist_ok=True)
        atomic_write(sd_state_file, req["payload_json"], fsync=req["fsync"])

    def mark_sd_write_success(self, req: SdWriteRequest, now: float) -> None:  # pragma: no cover
        """Update SD write metadata after a successful background write."""
        with self.sd_write_lock:
            self.sd_last_persist_ts = now
            self.sd_last_signature = copy.deepcopy(req["signature"])
            self.sd_error_count = 0
            self.sd_next_try_ts = 0.0
            self.sync_pending_sd_metadata()

    def sync_pending_sd_metadata(self) -> None:  # pragma: no cover
        """Mirror the currently queued SD request in de-duplication metadata."""
        if self.sd_write_pending is None:
            self.sd_pending_signature = None
            self.sd_pending_fsync = False
            return
        self.sd_pending_signature = copy.deepcopy(self.sd_write_pending["signature"])
        self.sd_pending_fsync = self.sd_write_pending["fsync"]

    def mark_sd_write_failure(
        self,
        req: SdWriteRequest,
        error: Exception,
        now: float,
    ) -> None:  # pragma: no cover
        """Requeue a failed SD write and sleep for the calculated backoff."""
        with self.sd_write_lock:
            self.sd_error_count += 1
            backoff = min(SD_BACKOFF_MAX_SECONDS, 2 ** min(self.sd_error_count, 8))
            self.sd_next_try_ts = now + backoff
            if self.sd_write_pending is None:
                self.sd_write_pending = req
            self.sd_pending_signature = copy.deepcopy(req["signature"])
            self.sd_pending_fsync = req["fsync"]
        self.dbus.log(f"SD write error: {error} (backoff {backoff:.0f}s)")
        time.sleep(backoff)

    def clear_sd_write_inflight(self) -> None:  # pragma: no cover
        """Mark the background writer as idle after one request attempt."""
        with self.sd_write_lock:
            self.sd_write_inflight = False

    def flush_sd_writes(self, timeout_seconds: float = 5) -> bool:
        """Wait briefly for pending SD writes before shutdown or tests continue."""
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            with self.sd_write_lock:
                pending = self.sd_write_pending is not None
                inflight = self.sd_write_inflight
            if not pending and not inflight:
                return True
            time.sleep(SD_FLUSH_POLL_INTERVAL_SECONDS)
        return False

    def register_signal_handlers(self) -> None:
        """Install SIGTERM/SIGINT handlers so RAM and critical SD state are flushed."""
        def _handler(signum: int, frame: Any) -> None:
            """Signal handler: persist state and exit."""
            self.dbus.log(f"Shutdown signal {signum}; saving state.")
            self.save_state_to_ram(force_persist=True)
            self.flush_sd_writes(timeout_seconds=5)
            sys.exit(0)

        signal.signal(signal.SIGTERM, _handler)
        signal.signal(signal.SIGINT, _handler)

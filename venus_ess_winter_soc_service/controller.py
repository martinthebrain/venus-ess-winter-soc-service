# SPDX-License-Identifier: GPL-3.0-or-later
from __future__ import annotations

import threading
from pathlib import Path
from typing import Optional

from .config import SdSignature, SdWriteRequest, State
from .dbus_iface import DBusInterface
from .dvcc import DvccMixin
from .persistence import PersistenceMixin
from .power import PowerMixin
from .runtime import RuntimeMixin
from .socpolicy import SocPolicyMixin
from .tracking import TrackingMixin
from .windows import WindowsMixin


class WinterController(
    PersistenceMixin,
    WindowsMixin,
    PowerMixin,
    DvccMixin,
    TrackingMixin,
    SocPolicyMixin,
    RuntimeMixin,
):
    """Seasonal ESS controller for MinSoC targets, balancing, and charge limiting."""

    def __init__(self) -> None:  # pragma: no cover
        """Initialize controller state, SD handling, and background writer."""
        self.dbus = DBusInterface()
        self.sd_last_persist_ts = 0.0
        self.sd_error_count = 0
        self.sd_next_try_ts = 0.0
        self.sd_last_signature: Optional[SdSignature] = None
        self.sd_pending_signature: Optional[SdSignature] = None
        self.sd_pending_fsync = False
        self.sd_window_active = False
        self.sd_card_path: Optional[Path] = None
        self.sd_state_dir: Optional[Path] = None
        self.sd_state_file: Optional[Path] = None
        self.sd_info = ""
        self.sd_last_lookup_ts = 0.0
        self.last_charge_limit_set_ts = 0.0
        self.sd_write_lock = threading.Lock()
        self.sd_write_event = threading.Event()
        self.sd_write_pending: Optional[SdWriteRequest] = None
        self.sd_write_inflight = False
        self.sd_writer_thread = threading.Thread(target=self._sd_writer_loop, daemon=True)
        self.sd_writer_thread.start()
        self.refresh_sd_paths(force=True)
        self.state: State = self.load_state()
        self.register_signal_handlers()
        self.sd_window_active = self.is_sd_window()
        if self.sd_state_file is None:
            self.dbus.log(f"SD disabled: {self.sd_info}")
        elif not self.sd_window_active:
            self.dbus.log(f"SD present but inactive outside the seasonal window: {self.sd_info}")
        else:
            self.dbus.log(f"SD enabled: {self.sd_info}")

# SPDX-License-Identifier: GPL-3.0-or-later
from __future__ import annotations

import threading
from pathlib import Path
from typing import Any, Optional, TYPE_CHECKING

from .config import SdSignature, SdWriteRequest, State
from .dbus_iface import DBusInterface


class ControllerMixinBase:
    """Typed shared surface used by controller mixins.

    The controller is split by responsibility, but each mixin still collaborates
    with methods and state owned by other mixins. These annotations keep strict
    static checkers aware of the composed object without adding runtime fallback
    behavior that could hide a genuinely missing method.
    """

    dbus: DBusInterface
    state: State
    sd_last_persist_ts: float
    sd_error_count: int
    sd_next_try_ts: float
    sd_last_signature: Optional[SdSignature]
    sd_pending_signature: Optional[SdSignature]
    sd_pending_fsync: bool
    sd_window_active: bool
    sd_card_path: Optional[Path]
    sd_state_dir: Optional[Path]
    sd_state_file: Optional[Path]
    sd_info: str
    sd_last_lookup_ts: float
    last_charge_limit_set_ts: float
    sd_write_lock: threading.Lock
    sd_write_event: threading.Event
    sd_write_pending: Optional[SdWriteRequest]
    sd_write_inflight: bool

    if TYPE_CHECKING:  # pragma: no cover
        def __getattr__(self, name: str) -> Any:
            """Expose cross-mixin methods to static type checkers only."""
            ...

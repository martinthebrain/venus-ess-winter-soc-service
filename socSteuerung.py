#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
from __future__ import annotations

"""Compatibility wrapper for the packaged ESS winter SoC controller."""

import copy
import json
import os
import signal
import sys
import threading
import time

import dbus

from venus_ess_winter_soc_service import *  # noqa: F403
from venus_ess_winter_soc_service import WinterController
from venus_ess_winter_soc_service.config import *  # noqa: F403
from venus_ess_winter_soc_service.storage import *  # noqa: F403

__all__ = [
    "WinterController",
    "copy",
    "dbus",
    "json",
    "os",
    "signal",
    "sys",
    "threading",
    "time",
]


if __name__ == "__main__":  # pragma: no cover
    controller = WinterController()
    controller.run()

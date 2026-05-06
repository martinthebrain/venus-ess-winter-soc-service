# SPDX-License-Identifier: GPL-3.0-or-later
from __future__ import annotations

"""Central Victron D-Bus paths used by the controller and test harnesses."""

PHASES = ("L1", "L2", "L3")

MIN_SOC_PATH = "/Settings/CGwacs/BatteryLife/MinimumSocLimit"
MAX_CHARGE_CURRENT_PATH = "/Settings/SystemSetup/MaxChargeCurrent"

BATTERY_SOC_PATH = "/Dc/Battery/Soc"
BATTERY_POWER_PATH = "/Dc/Battery/Power"
BATTERY_VOLTAGE_PATH = "/Dc/Battery/Voltage"
BMS_MAX_CHARGE_CURRENT_PATH = "/Info/MaxChargeCurrent"

DC_PV_POWER_PATH = "/Dc/Pv/Power"
AC_GRID_POWER_PATH = "/Ac/Grid/{phase}/Power"
AC_PV_ON_GRID_POWER_PATH = "/Ac/PvOnGrid/{phase}/Power"
AC_PV_ON_OUTPUT_POWER_PATH = "/Ac/PvOnOutput/{phase}/Power"
AC_CONSUMPTION_ON_INPUT_POWER_PATH = "/Ac/ConsumptionOnInput/{phase}/Power"
AC_CONSUMPTION_POWER_PATH = "/Ac/Consumption/{phase}/Power"

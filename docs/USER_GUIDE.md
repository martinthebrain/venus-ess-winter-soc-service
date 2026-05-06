# User Guide

This guide is for installing and operating `venus-ess-winter-soc-service` on a
Victron Venus OS device.

## What It Does

The service raises the Victron ESS minimum SoC seasonally when PV production is
unlikely to keep the battery in a healthy reserve range. It does not try to
charge immediately at all costs. Instead, it stages reserve building through
adaptive charge windows and uses a soft grid-import comfort target.

Default behaviour:

| Period | Dates | Behaviour |
|---|---:|---|
| Summer/default | outside seasonal windows | use `10%`, preserve temporary manual overrides |
| Pre-winter transition | Nov 05 - Nov 24 | if recent PV is low, stage reserve raise toward `40%` |
| Winter | Nov 25 - Feb 05 | stage reserve raise toward `55%` |
| Winter low SoC + low PV history | winter window | stage toward `40%` first, then continue to `55%` |
| Balancing | winter window, when due | temporarily target `100%` |
| Post-winter transition | Feb 06 - Feb 25 | hold `40%` until PV recovery is confirmed, then return to `10%` |

Outside allowed charge windows, the controller does not jump straight to the
full reserve target. It may hold the already reached SoC so ESS does not
discharge protected reserve energy again.

## Requirements

- Victron Venus OS
- Cerbo GX or compatible Venus OS device
- Python 3
- Victron ESS configured
- D-Bus access to Victron services
- DVCC enabled if charge-current limiting should be used
- A battery service exposing `/Info/MaxChargeCurrent`

Default D-Bus service names:

```python
SERVICE_SETTINGS = "com.victronenergy.settings"
SERVICE_SYSTEM = "com.victronenergy.system"
PREFERRED_BATTERY_SERVICE = "com.victronenergy.battery.socketcan_vecan1"
```

The preferred battery service can be changed in
`venus_ess_winter_soc_service/config.py`.

## Installation

Venus OS does not include `git` by default. The installer can therefore be run
from a single downloaded `install.sh`.

```bash
mkdir -p /data/venus-ess-winter-soc-service
cd /data/venus-ess-winter-soc-service
wget -O install.sh https://raw.githubusercontent.com/martinthebrain/venus-ess-winter-soc-service/main/install.sh
chmod +x install.sh
./install.sh
```

The installed service files live in:

```text
/data/etc/venus-ess-winter-soc-service
```

The runit service is linked as:

```text
/service/venus-ess-winter-soc-service
```

The installer also adds a persistent block to:

```text
/data/rc.local
```

This recreates the `/service/...` symlink after reboot or firmware updates.

The installer can be run again to update an existing installation. It overwrites
installed files, keeps the `rc.local` block idempotent, and starts the service
when `svc` is available.

## Uninstall

```bash
/data/etc/venus-ess-winter-soc-service/uninstall.sh
```

The uninstaller stops the runit service when possible, removes the `/service`
symlink, removes the `rc.local` block, and deletes the installed service files.

RAM logs and RAM state under `/dev/shm` are volatile and disappear on reboot.
SD-card seasonal state is not removed by the uninstaller.

## Configuration

Most configuration is done through constants in
`venus_ess_winter_soc_service/config.py`.

Important values:

```python
DEFAULT_SOC = 10.0
TRANSITION_GUARD_SOC = 40.0
WINTER_TARGET_SOC = 55.0
BALANCING_TARGET_SOC = 100.0
PV_THRESHOLD = 3000
TRANSITION_DAYS = 4
```

Charge-window behaviour:

```python
CHARGE_WINDOW_START_HOUR = 23
CHARGE_WINDOW_BASE_HOURS = 4
CHARGE_WINDOW_ESCALATION_NIGHTS = 2
CHARGE_WINDOW_MAX_MULTIPLIER = 4
```

Grid-softness behaviour:

```python
GRID_LOAD_LIMIT = 4000
GRID_PAUSE_HEADROOM_W = 100
GRID_SOFT_MIN_CHARGE_CURRENT_A = 10.0
CHARGE_EFFICIENCY = 0.9
```

Balancing:

```python
BALANCING_INTERVAL_DAYS = 14
BALANCING_DURATION_HOURS = 4
BALANCING_MAX_HOURS = 12
BALANCING_FULL_SOC = 99.0
```

The `ESS_SERVICE_*` environment variables are reserved for testbeds that point
the controller at simulated D-Bus services.

## Runtime State and Logs

The full runtime state is written to RAM:

```text
/dev/shm/ess_winter_logic.json
```

The diagnostics log is also written to RAM:

```text
/dev/shm/ess_winter_log.txt
```

Watch the log:

```bash
tail -f /dev/shm/ess_winter_log.txt
```

Healthy startup usually contains:

```text
SD disabled: No SD found
ESS winter controller started (RAM-only)
```

or:

```text
SD enabled: ...
ESS winter controller started (seasonal SD available)
```

Normal operation logs mode changes such as:

```text
Mode: Default (Target 10.0%)
Mode: Winter (Target 55.0%)
```

Important warning signs are repeated `SoC invalid/missing`, `MinSoC
path invalid/missing`, D-Bus write errors, or persistent `SD write error`
messages. With no battery attached, `SoC invalid/missing` is expected and the
controller should not write ESS or DVCC settings.

## SD Card Handling

The controller never writes regular state to Cerbo flash. It writes full runtime
state to RAM and may write a small selected subset to SD during seasonal windows.

The SD file is not a full runtime-state replacement. It stores durable seasonal
values such as:

- DVCC restore ownership state
- PV transition history during transition windows
- winter balancing timestamps

SD writes are intentionally rare. Normal SD writes are interval-limited by
`SD_SAVE_INTERVAL_SECONDS`; forced writes are reserved for important seasonal
events such as DVCC restore capture, balancing timestamps, confirmed full-SoC
timestamps, and completed PV-history days.

The SD directory is:

```text
socSteuerung/
```

SD detection order:

1. `ESS_SD_PATH`
2. `ESS_SD_LABEL`
3. first `mmcblk*` mount below `/media` or `/run/media`

Examples:

```bash
export ESS_SD_PATH=/media/mmcblk0p1
export ESS_SD_LABEL=MY_SD_CARD
```

If no SD card is found, the controller still runs with RAM-only state. After a
reboot, volatile in-progress runtime flags are lost, but the controller fails
safe: invalid or missing SoC data skips control writes, and unknown DVCC restore
state is not guessed.

## Manual Overrides

Outside seasonal control windows, manual MinSoC changes are treated as
intentional. The controller preserves such manual changes for:

```python
SUMMER_MANUAL_MINSOC_HOLD_SECONDS = FULL_DAY_HOURS * SECONDS_PER_HOUR
```

Inside seasonal control windows, the controller assumes it is responsible for
the seasonal SoC policy.

## D-Bus Paths

The controller reads these Victron paths:

| Service | Path | Purpose |
|---|---|---|
| `com.victronenergy.system` | `/Dc/Battery/Soc` | current battery SoC |
| `com.victronenergy.system` | `/Dc/Battery/Power` | house-load fallback |
| `com.victronenergy.system` | `/Dc/Battery/Voltage` | charge-current calculation |
| `com.victronenergy.system` | `/Ac/Grid/L*/Power` | grid import/export |
| `com.victronenergy.system` | `/Ac/ConsumptionOnInput/L*/Power` | preferred house-load source |
| `com.victronenergy.system` | `/Ac/Consumption/L*/Power` | fallback house-load source |
| `com.victronenergy.system` | `/Ac/PvOnGrid/L*/Power` | AC PV history |
| `com.victronenergy.system` | `/Ac/PvOnOutput/L*/Power` | AC PV behind the MultiPlus |
| `com.victronenergy.system` | `/Dc/Pv/Power` | DC PV history |
| battery service | `/Info/MaxChargeCurrent` | BMS charge-current ceiling |

The controller writes these settings only when policy requires it:

| Service | Path | Purpose |
|---|---|---|
| `com.victronenergy.settings` | `/Settings/CGwacs/BatteryLife/MinimumSocLimit` | ESS minimum SoC |
| `com.victronenergy.settings` | `/Settings/SystemSetup/MaxChargeCurrent` | temporary DVCC charge-current limit |

## Quick Diagnosis

Installed-system diagnosis:

```bash
/data/etc/venus-ess-winter-soc-service/scripts/diagnose_install.sh
```

Basic manual checks:

```bash
svstat /service/venus-ess-winter-soc-service
readlink /service/venus-ess-winter-soc-service
dbus -y com.victronenergy.settings /Settings/CGwacs/BatteryLife/MinimumSocLimit GetValue
dbus -y com.victronenergy.settings /Settings/SystemSetup/MaxChargeCurrent GetValue
```

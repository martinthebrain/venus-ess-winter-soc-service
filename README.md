# venus-ess-winter-soc-service

[![Tests](https://github.com/martinthebrain/venus-ess-winter-soc-service/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/martinthebrain/venus-ess-winter-soc-service/actions/workflows/tests.yml?query=branch%3Amain)
[![codecov](https://codecov.io/gh/martinthebrain/venus-ess-winter-soc-service/branch/main/graph/badge.svg)](https://codecov.io/gh/martinthebrain/venus-ess-winter-soc-service)

Victron Venus OS service for seasonal ESS minimum SoC control, winter battery protection, adaptive low-load charging windows, and temporary DVCC charge-current limiting with RAM-first state handling and low-wear SD persistence for Venus OS / Cerbo GX systems.

This service protects a battery during winter and transition periods by raising the ESS minimum SoC when PV production is no longer sufficient to regularly bring the battery into a healthy higher SoC range. It does **not** try to maximize comfort or avoid every bit of grid import. The primary goal is battery protection while grid-friendly charging is treated as a soft preference.

The controller can also apply a temporary DVCC `MaxChargeCurrent` limit while it is actively raising SoC. This limit is restored when the controller no longer needs it.

## Main Features

- Seasonal ESS minimum SoC control for Victron Venus OS
- Winter battery protection target
- Pre-winter and post-winter transition logic based on recent PV production
- Staged reserve building instead of immediate jumps to high SoC targets
- Adaptive low-load charging windows
- Temporary DVCC charge-current limiting
- Restore of the previous DVCC `MaxChargeCurrent` setting
- Detection and release of controller ownership after external DVCC changes
- Temporary preservation of summer manual MinSoC overrides
- RAM-first runtime state to avoid Cerbo flash wear
- Optional low-wear SD-card persistence for selected seasonal state
- Atomic SD writes with retry/backoff handling
- Balancing cycle support for occasional near-full battery time

## Why This Exists

In winter, PV production may not be sufficient to regularly charge the battery to a healthy higher SoC. Keeping a battery at low SoC for long periods can be undesirable for battery ageing and general battery care.

This controller raises the Victron ESS minimum SoC seasonally so the battery is not left at low SoC for extended periods.

The raised target does not need to be reached immediately. If the battery reaches the target one night later, that is usually acceptable. Therefore, the controller stages SoC increases through adaptive charge windows and uses a soft grid import target where possible.

## Seasonal Policy

Default configuration:

| Period | Dates | Behaviour |
|---|---:|---|
| Summer / Default | Outside seasonal windows | Use `10%` MinSoC and preserve temporary manual overrides |
| Pre-winter transition | Nov 05 – Nov 24 | If recent PV is low, stage reserve raise toward `40%` |
| Winter | Nov 25 – Feb 05 | Stage reserve raise toward `65%` |
| Winter low SoC + low PV history | Winter window | Stage toward `40%` first, then continue to `65%` |
| Balancing | Winter window, when due | Temporarily target `100%` |
| Post-winter transition | Feb 06 – Feb 25 | Hold `40%` until PV recovery is confirmed, then return to `10%` |

Outside allowed charge windows, the controller does not jump straight to the full reserve target. Instead, it may hold the already reached SoC so ESS does not discharge protected reserve energy again.

## Charging Window Behaviour

The controller starts with a base low-load charging window:

```python
CHARGE_WINDOW_START_HOUR = 23
CHARGE_WINDOW_BASE_HOURS = 4
```

By default, this means charging is allowed from 23:00 to 03:00.

If the battery remains below the target for multiple days, the window expands:

```python
CHARGE_WINDOW_ESCALATION_NIGHTS = 2
CHARGE_WINDOW_MAX_MULTIPLIER = 4
```

With the default configuration, the charging window can grow from 4 hours to 8 hours and eventually up to 16 hours.

This makes the controller gentle at first, but increasingly determined when the battery protection target is not reached.

## Grid Import Target

The grid import target is intentionally soft:

```python
GRID_LOAD_LIMIT = 4000
GRID_SOFT_MIN_CHARGE_CURRENT_A = 10.0
```

`GRID_LOAD_LIMIT` is not a safety limit. It is a comfort target used to reduce unnecessary grid stress while charging.

If the battery needs protection, the controller can still allow a minimum progress current so charging does not stall indefinitely.

## DVCC Charge Current Handling

When the controller applies a stricter temporary DVCC `MaxChargeCurrent`, it first captures the original value.

It then restores the captured value when the controller no longer needs its temporary charge-current limit.

The script tries to avoid overwriting user or system changes:

- It only takes ownership when applying a stricter limit.
- It remembers the value it wrote.
- If the value changes externally, ownership is released.
- Restore is only attempted when a captured or configured restore value exists.

## Runtime State and Persistence

The full runtime state is written to RAM:

```text
/dev/shm/ess_winter_logic.json
```

The diagnostics log is also written to RAM:

```text
/dev/shm/ess_winter_log.txt
```

This avoids regular writes to the Cerbo GX internal flash.

A small selected subset of state may be written to an SD card during seasonal windows only. The SD file is not a full runtime-state replacement. It only keeps durable seasonal values such as:

- DVCC restore ownership state
- PV transition history during transition windows
- Winter balancing timestamps

The SD directory is:

```text
socSteuerung/
```

## SD Card Detection

The controller looks for an SD card in this order:

1. `ESS_SD_PATH`
2. `ESS_SD_LABEL`
3. First `mmcblk*` mount below `/media` or `/run/media`

Examples:

```bash
export ESS_SD_PATH=/media/mmcblk0p1
```

or:

```bash
export ESS_SD_LABEL=MY_SD_CARD
```

If no SD card is found, the controller still runs with RAM-only state.

## Requirements

- Victron Venus OS
- Cerbo GX or compatible Venus OS device
- Python 3
- D-Bus access to Victron services
- Victron ESS configured
- DVCC enabled if charge-current limiting should be used
- A battery service exposing `/Info/MaxChargeCurrent`

The script expects these D-Bus services by default:

```python
SERVICE_SETTINGS = 'com.victronenergy.settings'
SERVICE_SYSTEM = 'com.victronenergy.system'
PREFERRED_BATTERY_SERVICE = 'com.victronenergy.battery.socketcan_vecan1'
```

The preferred battery service can be adjusted in the script:

```python
PREFERRED_BATTERY_SERVICE = 'com.victronenergy.battery.socketcan_vecan1'
```

## Installation

Venus OS does not include `git` by default. The installer is therefore designed
to work from either a complete checkout or a single downloaded `install.sh`.

On the Cerbo / Venus OS shell:

```bash
mkdir -p /data/venus-ess-winter-soc-service
cd /data/venus-ess-winter-soc-service
wget -O install.sh https://raw.githubusercontent.com/martinthebrain/venus-ess-winter-soc-service/main/install.sh
chmod +x install.sh
./install.sh
```

If `socSteuerung.py`, `service/run`, or `uninstall.sh` are missing from the
same directory as `install.sh`, the installer downloads them from:

```text
https://github.com/martinthebrain/venus-ess-winter-soc-service
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

Watch the RAM log:

```bash
tail -f /dev/shm/ess_winter_log.txt
```

## Uninstall

Run:

```bash
/data/etc/venus-ess-winter-soc-service/uninstall.sh
```

The uninstaller stops the runit service when possible, removes the `/service`
symlink, removes the `rc.local` block, and deletes the installed service files.

RAM logs and RAM state under `/dev/shm` are volatile and disappear on reboot.
SD-card seasonal state is not removed by the uninstaller.

## Configuration

Most configuration is done through constants at the top of the script.

Important values:

```python
DEFAULT_SOC = 10.0
PV_THRESHOLD = 3000
TRANSITION_DAYS = 4

WINTER_START_MMDD = 1125
WINTER_END_MMDD = 205

TRANS_PRE_START_MMDD = 1105
TRANS_PRE_END_MMDD = 1124

TRANS_POST_START_MMDD = 206
TRANS_POST_END_MMDD = 225
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

## Manual Overrides

Outside seasonal control windows, manual MinSoC changes are treated as intentional.

The controller preserves such manual changes for:

```python
SUMMER_MANUAL_MINSOC_HOLD_SECONDS = 24 * 3600
```

After that hold period, it returns to its default behaviour.

Inside seasonal control windows, the controller assumes it is responsible for the seasonal SoC policy.

## Safety Notes

This script changes live Victron ESS settings.

Use it only if you understand:

- Victron ESS minimum SoC behaviour
- DVCC charge-current limiting
- Battery ageing considerations
- Grid import implications
- Venus OS D-Bus service paths
- Your battery manufacturer's requirements

The grid import target in this script is a soft comfort preference.

Battery protection, inverter limits, charger limits, fuse ratings, grid-code compliance, and battery manufacturer limits must be handled by the Victron system configuration and the battery/BMS itself.

## Recommended Testing

The repository includes unit tests, 100% coverage enforcement for
`socSteuerung.py`, Radon cyclomatic-complexity checks, and strict `mypy` /
`pyright` type checks for the controller and helper scripts. GitHub Actions
runs these checks on pushes and pull requests and uploads `coverage.xml` to
Codecov.

Codecov expects a GitHub repository secret named:

```text
CODECOV_TOKEN
```

Local check:

```bash
python3 -m unittest discover -s tests
python3 -m mypy
python3 -m pyright
python3 -m coverage run -m unittest
python3 -m coverage report
python3 -m coverage xml
python3 -m radon cc socSteuerung.py -s -a
python3 scripts/check_radon_a.py
```

Before running unattended on Venus OS, test these cases:

- Summer/default mode returns to `10%`
- Manual MinSoC changes outside seasonal windows are preserved temporarily
- Pre-winter low-PV history triggers a staged `40%` target
- Winter mode targets `65%`
- Winter balancing temporarily targets `100%`
- Outside charge windows, the controller holds the reached SoC instead of jumping to the full target
- During charge windows, the controller raises MinSoC toward the target
- DVCC `MaxChargeCurrent` is captured and restored correctly
- External DVCC changes release controller ownership
- SD removal or write failures do not crash the controller
- Invalid SoC or MinSoC readings skip the loop safely

## Logging

The controller logs to stdout and to:

```text
/dev/shm/ess_winter_log.txt
```

The log is volatile and intentionally RAM-backed.

## License

GPL-3.0-or-later

See `LICENSE`.

## Disclaimer

This project is not affiliated with or endorsed by Victron Energy.

Use at your own risk. The author is not responsible for damage, misconfiguration, battery wear, data loss, grid import costs, or other consequences resulting from the use of this software.

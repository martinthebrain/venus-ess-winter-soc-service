# Validation Guide

This guide describes the automated and manual validation paths for the project.

## Local Checks

Run from the repository root:

```bash
python3 -m unittest discover -s tests
python3 scripts/dbus_scenario_simulator.py
scripts/install_selftest.sh
python3 -m mypy
python3 -m pyright
python3 -m coverage run -m unittest discover -s tests
python3 -m coverage report
python3 -m coverage xml
python3 scripts/check_radon_a.py
python3 scripts/check_package_file_lengths.py
```

The repository enforces:

- 100% line and branch coverage for `socSteuerung.py` and the runtime package
- strict `mypy`
- strict `pyright`
- Radon A for all checked Python functions
- 500 lines or fewer per maintained Python file

## Offline D-Bus Scenario Simulator

The offline simulator does not publish a real D-Bus service and does not touch
live Victron settings. It imports the controller with an in-memory D-Bus facade.

```bash
python3 scripts/dbus_scenario_simulator.py
python3 scripts/dbus_scenario_simulator.py --verbose
python3 scripts/dbus_scenario_simulator.py no-battery-fails-safe dvcc-capture-and-restore
```

Installed on Venus OS:

```bash
python3 /data/etc/venus-ess-winter-soc-service/scripts/dbus_scenario_simulator.py
```

Covered scenarios include:

- missing SoC fails safe
- summer manual override preservation
- staged transition `40%` target
- winter `55%` target
- soft grid target still allows progress
- stricter manual DVCC limits are not raised
- DVCC capture and restore
- external DVCC takeover
- invalid SoC and MinSoC readings
- missing BMS charge current
- D-Bus write failures

## Live D-Bus Testbed

The live testbed is intended for a disposable Venus OS test device. It publishes
simulated Victron services on the real system D-Bus using names ending in
`.sim`. It does not use the real `com.victronenergy.system` or
`com.victronenergy.settings` services.

Run installed live scenarios:

```bash
svc -d /service/venus-ess-winter-soc-service
python3 /data/etc/venus-ess-winter-soc-service/scripts/live_dbus_testbed.py --run-live-scenarios
```

Manual fake-service mode:

```bash
python3 /data/etc/venus-ess-winter-soc-service/scripts/live_dbus_testbed.py --serve
```

The script prints an `ESS_SERVICE_*` command that starts the controller against
the fake services in another shell.

Cleanup stale manual testbeds:

```bash
python3 /data/etc/venus-ess-winter-soc-service/scripts/live_dbus_testbed.py --cleanup-stale
```

## Installer Validation

The installer self-test exercises install, update, and uninstall in a temporary
sandbox:

```bash
scripts/install_selftest.sh
```

It checks:

- installation into an alternate data root
- service symlink creation
- `rc.local` block creation
- update over an existing installation
- uninstall cleanup

## Installed-System Diagnosis

On Venus OS:

```bash
/data/etc/venus-ess-winter-soc-service/scripts/diagnose_install.sh
```

The diagnosis checks installed files, executable bits, service link, `svstat`,
`rc.local`, package import, basic D-Bus probes, RAM log, and RAM state.

## Raspberry Pi / Venus OS Validation Pattern

For a test Venus OS image without a battery:

1. Install or update the service.
2. Restart the runit service.
3. Run `diagnose_install.sh`.
4. Run the offline simulator.
5. Run the live D-Bus testbed.
6. Verify real settings stayed unchanged:

```bash
dbus -y com.victronenergy.settings /Settings/CGwacs/BatteryLife/MinimumSocLimit GetValue
dbus -y com.victronenergy.settings /Settings/SystemSetup/MaxChargeCurrent GetValue
```

With no battery attached, `/Dc/Battery/Soc` may return `[]`. In that case the
expected service log is:

```text
SoC invalid/missing; skipping cycle
```

The controller should not write ESS MinSoC or DVCC settings in that state.

## CI

GitHub Actions runs unit tests, type checks, coverage, scenario simulator,
installer self-test, Radon, and the file-length gate. Coverage and test results
are uploaded to Codecov.

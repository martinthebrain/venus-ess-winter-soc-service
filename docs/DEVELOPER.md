# Developer Guide

This guide is for contributors who want to change the code, tests, installer,
or validation tooling.

## Project Structure

`socSteuerung.py` is kept as the executable compatibility entrypoint for Venus
OS and the runit service. The implementation lives in the package:

```text
venus_ess_winter_soc_service/
  config.py       Constants, D-Bus service names, and shared type aliases
  controller.py   WinterController composition and startup wiring
  dbus_iface.py   D-Bus adapter for tolerant reads, typed writes, and logs
  paths.py        Central Victron D-Bus path constants
  storage.py      SD-card discovery and atomic file writes
  persistence.py  RAM state, seasonal SD subset, and background SD writer
  power.py        PV, grid, house-load, and battery-service measurements
  dvcc.py         DVCC MaxChargeCurrent capture, limiting, and restore logic
  tracking.py     PV history, full-SoC tracking, balancing, and target selection
  socpolicy.py    MinSoC state machine, charge windows, pause/resume behaviour
  runtime.py      Main loop, SoC validation, mode logging, and SD-window updates
```

Simulator and testbed tooling is split by responsibility:

```text
scripts/dbus_sim_core.py        Offline fake D-Bus model and date simulation
scripts/dbus_sim_harness.py     Offline controller harness
scripts/dbus_sim_scenarios.py   Offline scenario definitions
scripts/dbus_scenario_simulator.py

scripts/live_dbus_core.py       Real system-bus fake-service publisher
scripts/live_dbus_harness.py    Live/local and remote controller harnesses
scripts/live_dbus_scenarios.py  Live D-Bus scenario definitions
scripts/live_dbus_testbed.py
```

Tests are split into focused files under `tests/`, with common fixtures in
`tests/helpers.py`.

## Quality Gates

Run:

```bash
python3 -m unittest discover -s tests
python3 scripts/dbus_scenario_simulator.py
scripts/install_selftest.sh
python3 -m coverage run -m unittest discover -s tests
python3 -m coverage report
python3 scripts/check_radon_a.py
python3 scripts/check_package_file_lengths.py
python3 -m mypy
python3 -m pyright
```

Expected:

- 100% line and branch coverage for runtime code
- no `mypy` issues
- no `pyright` issues
- all functions Radon A
- maintained Python files at or below 500 lines
- offline D-Bus scenarios pass
- installer self-test passes

## CI and Codecov

The workflow is in `.github/workflows/tests.yml`.

It runs:

- strict mypy
- strict pyright
- unit tests with XML output
- offline D-Bus simulator
- installer self-test
- coverage report and XML generation
- Radon A gate
- file-length gate
- Codecov upload

Codecov expects a repository secret named:

```text
CODECOV_TOKEN
```

## Installer Development

`install.sh` must work from:

1. a complete checkout
2. a single downloaded `install.sh` on Venus OS

Venus OS usually has `wget` but not `git`, so missing files are downloaded from
GitHub raw URLs. When adding files required at runtime or for installed
diagnostics, add them to `PACKAGE_FILES` or `SCRIPT_FILES` in `install.sh`.

Use `scripts/install_selftest.sh` after installer changes.

The installer supports environment overrides for sandboxed tests:

```text
ESS_INSTALL_DATA_ROOT
ESS_SERVICE_ROOT
ESS_INSTALL_DIR
ESS_SERVICE_LINK
ESS_RC_LOCAL
ESS_RAW_BASE_URL
ESS_START_SERVICE
ESS_ALLOW_NON_ROOT
ESS_INSTALL_USE_LOCAL_FILES
```

When `ESS_RAW_BASE_URL` is unset, managed files are fetched from the latest
GitHub release tag unless the installer is running from a Git checkout. Set it
explicitly in tests, release candidates, or forked installs when the source must
be pinned to a branch or tag.

When the installer runs from a Git checkout, existing local files are used. On a
normal Venus OS install directory without `.git`, managed files are refreshed
from the latest release when the installer is run again. Set
`ESS_INSTALL_USE_LOCAL_FILES=1` only for explicit local-source testing outside a
Git checkout.

Normal Venus OS installs should not need these variables.

## Release Notes

Before pushing a release candidate:

1. Run all local gates.
2. Run the offline simulator.
3. Run the installer self-test.
4. Install on a Venus OS test device.
5. Run `diagnose_install.sh`.
6. Run the live D-Bus testbed.
7. Reboot the test device and confirm service autostart.

When creating a GitHub release, publish it from a tested tag. The
`release-assets.yml` workflow uploads `install.sh` as a release asset after the
release is published. The public install command uses:

```text
https://github.com/martinthebrain/venus-ess-winter-soc-service/releases/latest/download/install.sh
```

Wait for the release asset workflow to finish before announcing the release.

For a public release, prefer a tagged GitHub release so users can install a
known version instead of tracking `main`.

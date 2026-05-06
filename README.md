# venus-ess-winter-soc-service

[![Tests](https://github.com/martinthebrain/venus-ess-winter-soc-service/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/martinthebrain/venus-ess-winter-soc-service/actions/workflows/tests.yml?query=branch%3Amain)
[![codecov](https://codecov.io/gh/martinthebrain/venus-ess-winter-soc-service/branch/main/graph/badge.svg)](https://codecov.io/gh/martinthebrain/venus-ess-winter-soc-service)

A small Victron Venus OS service that manages ESS minimum SoC seasonally.

It raises the battery reserve during winter and transition periods when PV
production may not reliably recover the battery. Charging is staged through
adaptive low-load windows, and temporary DVCC `MaxChargeCurrent` limits are
restored when the controller no longer needs them.

## Highlights

- Seasonal ESS MinSoC targets for Venus OS
- Winter target defaults to `55%`
- Pre-/post-winter transition logic based on recent PV history
- Staged reserve building instead of hard jumps
- Temporary Victron charge-current limits that are restored automatically
- Stores runtime state in RAM to avoid Cerbo flash wear
- Optional seasonal SD-card backup with very few writes
- Built with clean Python architecture and strong validation for maintainability

## Is This For You?

Use Victron BatteryLife if you want the standard Victron adaptive SoC behaviour.

Use Node-RED if you already run Venus OS Large, prefer visual automation flows,
or only need a simple schedule.

Use this service if you want deterministic seasonal reserve management, PV-history
based transition behaviour, adaptive low-load charging windows, and a small
standalone service without Node-RED, Signal K, or Venus OS Large.

This is a configurable reserve-management policy, not a universal battery-health
rule. Battery chemistry, temperature, warranty requirements, and BMS limits still
matter.

## Quick Install

On the Cerbo / Venus OS shell:

```bash
mkdir -p /data/venus-ess-winter-soc-service
cd /data/venus-ess-winter-soc-service
wget -O install.sh https://github.com/martinthebrain/venus-ess-winter-soc-service/releases/latest/download/install.sh
chmod +x install.sh
./install.sh
```

The installer and the service files are downloaded from the latest GitHub
release by default. Set `ESS_RAW_BASE_URL` only when you intentionally want to
install a specific tag, branch, fork, or local test source.

Check that it is running:

```bash
svstat /service/venus-ess-winter-soc-service
tail -f /dev/shm/ess_winter_log.txt
```

Uninstall:

```bash
/data/etc/venus-ess-winter-soc-service/uninstall.sh
```

## Default Policy

| Period | Default behaviour |
|---|---|
| Summer/default | `10%`, temporary manual overrides are preserved |
| Pre-winter low PV | staged raise toward `40%` |
| Winter | staged raise toward `55%` |
| Winter low SoC + low PV history | `40%` first, then `55%` |
| Winter balancing | temporary `100%` target when due |
| Post-winter | hold `40%` until PV recovery is confirmed |

Outside charge windows the controller may hold the already reached SoC, so ESS
does not immediately discharge protected reserve energy again.

## Documentation

- [User Guide](docs/USER_GUIDE.md): installation, configuration, operation, logs, SD card handling
- [Validation Guide](docs/VALIDATION.md): unit tests, offline simulator, live D-Bus testbed, Raspberry Pi/Venus OS checks
- [Developer Guide](docs/DEVELOPER.md): project structure, CI, quality gates, release/update notes
- [Technical Design](docs/TECHNICAL_DESIGN.md): rationale, trade-offs, persistence, DVCC ownership, policy choices

## Safety Notes

This service writes live Victron ESS settings:

- `/Settings/CGwacs/BatteryLife/MinimumSocLimit`
- `/Settings/SystemSetup/MaxChargeCurrent`

Use it only if you understand your Victron ESS configuration, DVCC behaviour,
battery/BMS requirements, and grid/import implications. Protection limits,
charger limits, fuses, grid-code compliance, and battery manufacturer limits
must still be handled by the Victron system and the BMS itself.

## License

GPL-3.0-or-later. See [LICENSE](LICENSE).

This project is not affiliated with or endorsed by Victron Energy.

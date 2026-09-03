# Venus ESS Winter SoC Service (Rust)

This is the native Rust implementation of the winter reserve controller. Its
current behaviour is documented in `CONTRACTS.md` and enforced by unit,
property, controller-scenario, and JSON contract tests.

The runtime keeps the established controller fields and wraps them in a
versioned, device-bound JSON state document. Runtime files and the exclusive
instance lock live in a process-private `0700` directory below `/dev/shm`.
Shadow mode uses separate files there, never writes D-Bus settings, and never
persists to removable storage:

```sh
ESS_SHADOW_MODE=1 \
ESS_DECISION_FILE=/dev/shm/venus-ess-winter-soc-service/decision-shadow.json \
./bin/venus-ess-winter-soc-service
```

Deployment-specific policy can be changed without rebuilding the service. For
example, this raises the summer minimum SoC while all other values retain their
defaults:

```sh
ESS_SUMMER_MIN_SOC=15 ./bin/venus-ess-winter-soc-service
```

`service/run` loads optional root-owned overrides from `config.env` in the
deployment root. `config.env.example` lists every supported setting and its
default. Values are parsed into a typed policy and checked individually and in
combination. Invalid configuration prevents startup with a descriptive error;
it never silently falls back to a different policy.

Temporary GUI current, discharge-power, and minimum-SoC settings retain their
original custom values across a reboot. The small restore record prefers
removable SD/USB storage and uses
`/data/venus-ess-winter-soc-service-rust/gui-restore-state.json` only when no
removable medium is mounted. Writes occur only on ownership changes and are
generation-acknowledged before the temporary D-Bus setting is changed. Cleared
ownership supersedes an older durable record through an explicit tombstone,
while standard values are still derived on restoration and are not persisted
as GUI values when no cleanup is required.

Queued removable writes are tied to the exact mounted filesystem and are
revalidated by the writer. Destinations are opened relative to the verified
mount root, so removing or replacing a card cannot redirect a delayed write to
the filesystem below its former mount point. `ESS_SD_PATH`, when set, must name
the mounted removable root itself.

The controller also protects a battery that is still discharging below 20% SoC
by limiting ESS discharge power to 40% of the discovered nominal VE.Bus power.
It preserves stricter GUI limits and restores the previous limit only after
charging has been observed and SoC exceeds 25%.

Winter balancing separates the approach to full charge from the high-SoC hold.
The approach may span up to 72 hours by default and therefore several scheduled
charging windows. Reaching 99% starts an independent twelve-hour watchdog; four
continuous hours at or above 99% complete balancing. Falling below 99% restarts
the continuous hold counter without extending that high-SoC watchdog. Both
watchdogs and the hold duration are deployment-configurable.

Routine charging stops at 90% by temporarily setting the system-wide DVCC
`/Settings/SystemSetup/MaxChargeCurrent` limit to 0 A. This covers the
controllable Victron inverter/chargers and MPPT chargers instead of merely
limiting Multi charge power. The deprecated
`/Settings/CGwacs/MaxChargePercentage` setting and the persistent ESS GUI
`MaxChargePower` setting are never changed. The controller writes only after
the ceiling is reached, restores the exact preceding current limit below the
release hysteresis, and uses durable ownership, a write-ahead record, and
readback for crash recovery. Existing external 0 A limits are left untouched;
an external change while the controller owns the limit immediately releases
ownership. After more than two calendar-day transitions
without a near-full charge, the ceiling becomes 100%; reaching at least 98%
resets the volatile counter. The counter lives only in the RAM state and
therefore starts at day zero after a GX reboot. All percentages and the
calendar interval are deployment-configurable.

One arbiter owns that shared DVCC setting for every controller mechanism. It
combines the configured current, reserve-current, routine-ceiling, and explicit
inhibit constraints into one effective target, captures one external baseline,
and uses one durable generation-bound write-ahead transaction. A routine
ceiling or inhibit contributes 0 A; otherwise the lowest applicable constraint
wins. Moving between reserve and routine control can therefore never capture a
temporary controller value as a new GUI baseline.

Service-triggered reserve charging limits the grid-supplied share of
`MaxChargeCurrent` to 40% of the lower current reported by the BMS and VE.Bus
charger. Concurrent AC and DC PV contribution is added without exceeding that
hardware maximum. A stricter GUI limit remains authoritative. If either
hardware limit is unavailable, the reserve raise continues without a
service-owned current cap; an earlier temporary cap is restored and confirmed.
When a cap is available, it is applied and read back before `MinimumSocLimit`
is raised. Failed writes or readback pause the reserve raise at the reached SoC
and are exposed in the decision snapshot rather than leaving a partially
activated charge request. Reserve-current writes use a durable write-ahead
record. Readback must match the normalized requested value, and restoration is
allowed only while the current setting still matches the controller's last
confirmed value; a newer external limit remains untouched.

The BMS and VE.Bus services are selected exclusively through systemcalc
`/ActiveBmsService` and `/VebusService`. A reported BMS limit of 0 A remains an
explicit prohibition. The optional `ESS_PREFERRED_BATTERY_SERVICE` setting is
only a compatibility fallback for systems that do not expose
`/ActiveBmsService`; no installation-specific service name is built in.
VE.Bus 0 A and explicit system, active-BMS, or VE.Bus charge-disable signals
are handled as hard inhibits. The controller neither starts a reserve raise nor
restores a positive current limit while an inhibit is active. Decision snapshots
list the inhibit causes and report `UserChargeLimited` separately because that
flag indicates an external limit, not necessarily a complete charging ban.

Build the Venus OS ARMv7 binary with the pinned toolchain container:

```sh
./scripts/build-armv7.sh
```

The production service entry point is `service/run`. The independent shadow
entry point is `service-shadow/run`; both expect the deployment root at
`/data/venus-ess-winter-soc-service-rust` unless `ESS_WINTER_RUST_ROOT` is set.
From an extracted checkout on Venus OS, `./install.sh` activates the native
service and makes the runit link persistent across reboots. Set
`ESS_WINTER_INSTALL_MODE=shadow` to run the same build without changing the
active controller. `./uninstall.sh` performs the explicit owned-setting cleanup
before removing the service integration.
`ESS_WINTER_INSTALL_MODE=uninstall deploy/venus/install.sh` first stops the
service and invokes `--restore-all-owned-settings`. It restores only settings
whose current values still prove service ownership, verifies every write, and
removes runit and `rc.local` integration only after successful cleanup. The
historical `--restore-charge-current-ceiling` command remains available for
compatibility and restores only the shared `MaxChargeCurrent` baseline.

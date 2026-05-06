# Technical Design

This document explains why the controller is built the way it is, including the
main trade-offs.

## Static Python Configuration

Configuration lives in `venus_ess_winter_soc_service/config.py`.

Why:

- Venus OS systems are often headless and should fail predictably.
- A malformed writable JSON/YAML config would introduce another runtime failure
  mode.
- Configuration changes should be deliberate and versioned.

Trade-off:

- Changing policy values requires editing code and reinstalling.
- This is less convenient than a UI or JSON file, but safer for a small service
  that writes live ESS settings.

## RAM-First State

Full runtime state is written to:

```text
/dev/shm/ess_winter_logic.json
```

Why:

- `/dev/shm` is volatile RAM.
- Regular controller loops do not write Cerbo internal flash.
- Runtime counters and flags are cheap to lose on reboot.

Trade-off:

- In-progress state is lost on reboot.
- The controller must be conservative after restart.

## Seasonal Partial SD Persistence

Only selected durable keys are written to SD, and only in seasonal windows.

Why:

- SD wear is kept low.
- SD state cannot accidentally replace a more complete RAM state.
- Important seasonal facts survive reboot, such as PV history and balancing
  timestamps.

Trade-off:

- SD is not a complete state backup.
- Some runtime progress may be recomputed or restarted after reboot.

Implementation details:

- SD payloads contain only whitelisted keys.
- SD state is merged into RAM only when timestamp rules allow it.
- Mutable signatures are deep-copied so list mutation cannot bypass change
  detection.
- Critical events can force persistence.

## DVCC Ownership and Restore

The controller may temporarily lower
`/Settings/SystemSetup/MaxChargeCurrent`.

Why ownership tracking exists:

- The previous DVCC value must be restored after the controller is done.
- Manual or external DVCC changes must not be overwritten unexpectedly.
- Victron uses `-1` to mean no explicit limit; this is logically unlimited even
  though it is numerically lower than positive current values.

Policy:

- Capture the original value once, immediately before the first stricter
  script-owned limit.
- Do not blindly learn the current value at startup.
- Do not blindly learn the current value while idle.
- If the actual D-Bus value no longer matches the script-owned value, release
  ownership.
- Restore only when a trustworthy captured or configured value exists.

Trade-off:

- If the device reboots during a script-owned limit and no durable restore value
  exists, the controller avoids guessing.
- This may leave a manual inspection case, but it avoids restoring to a wrong
  value.

## Staged MinSoC Raises

Winter targets are not always applied immediately.

Why:

- A sudden MinSoC jump can trigger immediate grid charging.
- The user wanted grid kindness as a comfort goal, not a hard safety limit.
- The battery-protection target can usually be reached over a multi-day horizon.

Behaviour:

- The controller raises reserve mainly during charge windows.
- Outside charge windows it may set a pause SoC at the already reached level.
- This prevents ESS from discharging reserve energy already charged at night.
- If the deficit persists, the charge window expands.

Trade-off:

- The target may be reached later.
- Long deficits are handled by adaptive window escalation.

## Soft Grid Target

`GRID_LOAD_LIMIT` is a comfort target, not a safety limit.

Why:

- The physical house connection, fuses, BMS, and inverter settings must handle
  safety.
- The controller only tries to avoid unnecessary grid stress while building
  reserve.

Implementation:

- Available AC headroom is converted to DC charge current with efficiency.
- A minimum progress current prevents the controller from stalling indefinitely
  at high house load.

Trade-off:

- The soft target can be exceeded.
- This is intentional when battery reserve should continue progressing.

## Winter Target 55%

The default winter target is `55%`.

Why:

- Long-term LFP storage is often recommended around the middle of the SoC range,
  commonly around 40-60%.
- `55%` gives buffer above very low SoC without keeping the pack near full.

Trade-off:

- It is a reserve-management default, not a universal chemistry rule.
- Users should check their battery manufacturer and warranty requirements.

## Balancing

Balancing can temporarily target `100%`.

Why:

- Some battery systems occasionally need near-full time for balancing or SoC
  calibration.

Trade-off:

- High SoC can increase calendar aging if held unnecessarily.
- The controller uses intervals, timeouts, and near-full confirmation rather
  than permanently holding a high target.

## D-Bus Adapter and Central Paths

D-Bus access is wrapped in `dbus_iface.py`, and paths are centralized in
`paths.py`.

Why:

- Victron D-Bus reads can return missing values, `-1`, empty arrays, or fail.
- Settings need correct D-Bus types.
- Central paths prevent drift between runtime, tests, and simulators.

Trade-off:

- There is a little indirection.
- The benefit is safer type handling and less string duplication.

## Package Split

The controller is composed from mixins rather than one large script.

Why:

- Each file has a focused responsibility.
- Radon complexity stays low.
- Tests can target smaller units.
- Maintained files are kept below 500 lines.

Trade-off:

- Following one full control loop involves multiple files.
- The top-level flow is documented in `config.py`, and the module names mirror
  the controller responsibilities.

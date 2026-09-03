# Executable Behaviour Contracts

The Rust service preserves the externally observable policy of the deployed
Python service on both validated GX installations.

The values below describe the default policy. A deployment may override the
documented fields in `config.env` without changing this contract document or
the default scenario corpus. Parsing bounds and cross-field invariants are
executable configuration contracts.

## Seasonal reserve

- The default ESS minimum SoC is 10%; deployments may configure a higher
  summer floor without changing the seasonal policy contract.
- Winter runs from 25 November through 5 February and targets 45%.
- Four consecutive daily PV averages below 3000 W enable the 40% low-PV stage
  during pre-winter.
- Winter always uses the configured winter target unless balancing is active;
  PV history neither stages nor changes the winter target.
- Post-winter keeps 40% until four consecutive averages are strictly above
  3000 W.
- A due balancing cycle targets 100% and starts in `ApproachToFull`. This phase
  has its own 72-hour watchdog by default, allowing several scheduled charging
  windows to reach 99% SoC without consuming high-SoC hold time.
- The first sample at or above 99% enters `HighSocHold` and starts a separate
  twelve-hour watchdog. Four continuous hours at or above 99% complete
  balancing. A later dip below 99% resets only the continuous four-hour hold;
  it never restarts the high-SoC watchdog.
- Active balancing has priority over the routine upper charge ceiling,
  including in its start cycle. Either phase-specific timeout ends the attempt
  and starts the configured retry cooldown.

## Staged charging

- A battery needs reserve charging only below `target - 1 percentage point`.
- Reserve targets above the configured summer floor are raised in adaptive
  windows beginning at 23:00.
- The initial window is four hours, doubles after two unresolved nights, and is
  capped at sixteen hours.
- Outside the window, the controller holds the reached SoC between the
  configured summer floor and the current seasonal target instead of jumping
  directly to that target.
- Before its first `MinimumSocLimit` change, the controller durably records the
  external value and a generation-bound write intent. Each write is confirmed
  by readback before it becomes the controller's last owned value. A later
  external change releases that ownership and becomes the next baseline if the
  active seasonal policy subsequently needs another change.

## DVCC current arbitration

- `/Settings/SystemSetup/MaxChargeCurrent` has exactly one controller-owned
  arbiter. Configured limits, reserve charging, the routine SoC ceiling, and
  explicit charge inhibits are constraints on that arbiter, never independent
  owners of the D-Bus setting.
- The arbiter captures one external baseline, records one last confirmed
  effective value, and maintains one generation-bound write-ahead transaction.
  A routine ceiling or explicit inhibit contributes 0 A; otherwise the lowest
  configured and reserve constraint wins. A stricter external baseline always
  remains authoritative.

- Service-triggered reserve charging is capped at 40% of the lower non-negative
  limit reported by the BMS `/Info/MaxChargeCurrent` and VE.Bus
  `/Dc/0/MaxChargeCurrent` for its grid-supplied share. Concurrent AC PV,
  converted with the configured charge efficiency, and DC PV are added as
  battery-equivalent current. The resulting total never exceeds the lower
  hardware limit and is rounded down, never up.
- If either hardware limit is unavailable, reserve charging continues without
  a service-owned current limit. A previously owned temporary limit is
  restored and confirmed by readback before charging continues. The decision
  reports `charge_current_limit_unavailable` and its typed causes.
- A reported 0 A hardware limit and an explicit disabled charge permission are
  hard charge inhibits. While reserve charging is needed, the controller does
  not raise `MinimumSocLimit`, requests 0 A during an active charge window
  without overriding an already stricter external setting, and retains any
  owned 0 A upper-ceiling limit.
  It resumes from the unchanged seasonal target only after the inhibit clears.
- `/SystemState/ChargeDisabled`, active-BMS `/Io/AllowToCharge`, and active
  VE.Bus `/Bms/AllowToCharge` are optional explicit inhibit signals. Missing or
  malformed optional signals are not guessed. `/SystemState/UserChargeLimited`
  is reported separately and never treated as a blanket charging prohibition.
- Before applying the first stricter `MaxChargeCurrent`, the previous raw
  Victron value is durably captured and synchronized before the D-Bus write.
  Later constraints reuse that baseline; no mechanism may capture another
  mechanism's temporary value as an external setting. A custom value is stored
  verbatim. The standard `-1` unlimited value is derived during restoration and
  is not stored as a GUI value.
- The controller never replaces a pre-existing equal or stricter limit.
- Relaxations are rate-limited; stricter reductions are immediate.
- Every temporary charge-current write is preceded by the arbiter's single,
  durably synchronized generation-bound record containing its expected and
  intended values. The normalized intended value must be confirmed exactly by
  readback; a merely stricter third-party value is not claimed as the
  controller's write.
- An external change releases controller ownership instead of being overwritten.
  Restoration is attempted only while the current setting still matches the
  controller's last confirmed value.
- When the last constraint ends, the captured baseline is restored and
  ownership is cleared only after confirming the restored value. A failed
  restore remains owned and is retried until readback succeeds.
- Reserve charging is activated transactionally: the complete measurement
  snapshot and current limit are calculated first, an applicable
  `MaxChargeCurrent` limit is written and confirmed by readback, and only then
  may `MinimumSocLimit` be raised. An already equal or stricter external limit
  satisfies this prerequisite without transferring ownership.
- A failed current write, current readback, restore, or `MinimumSocLimit`
  confirmation leaves reserve charging paused. If a raise was already active
  or its write result is uncertain, the controller attempts to hold the
  already reached SoC instead of continuing toward the seasonal target.

## Upper charge ceiling

- The deprecated Venus `/Settings/CGwacs/MaxChargePercentage` path is never
  used as a numeric SoC ceiling. The persistent GUI
  `/Settings/CGwacs/MaxChargePower` value is also never changed.
- The controller enforces the routine ceiling through the documented,
  system-wide DVCC `/Settings/SystemSetup/MaxChargeCurrent` setting. It writes
  0 A only when SoC reaches the configured ceiling. This stops the controllable
  Victron inverter/chargers and solar chargers; unrelated chargers outside
  DVCC remain outside this contract.
- Entry occurs at the ceiling. Once owned, the 0 A limit is retained until SoC
  falls below `ceiling - ESS_SOC_HYSTERESIS`, avoiding threshold oscillation.
- A pre-existing 0 A limit is already stricter and remains externally owned.
  Any other valid raw setting, including the `-1` unlimited sentinel, is
  captured exactly before the controller temporarily replaces it with 0 A.
- A routine ceiling contributes the strongest 0 A constraint to the shared
  arbiter. Entering it while a reserve constraint is active reuses the same
  external baseline and ownership; leaving it reveals the still-applicable
  reserve constraint or restores the baseline when no constraint remains.
- Every ceiling-related write uses the arbiter's single durable intent,
  generation, external baseline, and last confirmed effective value. Readback
  must confirm the intended value before the transaction is committed.
- After restart, an intended value already visible on D-Bus is committed, an
  unchanged expected value is retried, and a third value is treated as an
  external change. External control releases ownership and is not overwritten
  again until the current ceiling episode has ended.
- Normal shutdown clears all internal charge-current constraints and restores
  the one owned `MaxChargeCurrent` baseline in its original representation. It
  deliberately retains `MinimumSocLimit` and active low-SoC discharge
  protection across supervised restarts.
- Explicit installer uninstall uses `--restore-all-owned-settings`. It also
  restores an owned `MinimumSocLimit` and `MaxDischargePower`, but only while
  the live value still matches the controller's last confirmed write. A later
  external change is retained and releases ownership. Every cleanup write has
  a durable intent and exact readback; an incomplete cleanup prevents removal.
- Missing or invalid SoC or `MinimumSocLimit` telemetry prevents every control
  write in that cycle. Unavailable current telemetry, failed durable intent,
  failed writes, and failed readback are exposed as bounded diagnostics and
  retried without speculative state changes.
- A near-full sample at or above 98% resets a volatile calendar-day counter.
  The same reset occurs on first startup after the GX RAM has been cleared.
- A 100% ceiling is allowed only when more than two complete date transitions
  have elapsed. For example, a reset on Monday permits 100% again on Thursday;
  Tuesday has age one and Wednesday age two.
- Calendar dates, rather than elapsed 24-hour periods, define age. The GX uses
  its local system date, which is UTC on the validated installations.
- A backward date change or a forward discontinuity of more than one calendar
  day resets the volatile counter instead of making a full charge immediately
  due.
- The counter is retained only in the RAM state file. It is deliberately absent
  from the removable-storage subset and therefore resets after a GX reboot.
- A seasonal target above the active routine ceiling is deferred to that
  ceiling. A due or active winter balancing cycle instead raises the effective
  ceiling to its balancing target in the same controller cycle and restores a
  previously owned 0 W routine override.
- Reaching 98% during balancing still resets the volatile calendar counter but
  cannot revoke the balancing ceiling. Success, the approach timeout, or the
  high-SoC timeout restores the routine ceiling in that same controller cycle.

## Low-SoC discharge protection

- While the battery is discharging, a SoC strictly below 20% activates an
  independent discharge-power guard. It does not depend on the configured ESS
  minimum SoC.
- The guard limits `MaxDischargePower` to 40% of the positive VE.Bus
  `NominalInverterPower`. A pre-existing lower GUI limit remains unchanged.
- Nominal inverter power is selected in strict order: a valid live VE.Bus
  observation, a cache entry that is both younger than the configured maximum
  age and bound to the currently selected VE.Bus service, then an explicitly
  configured installation value. Unbound legacy cache values are never used.
- The default cache maximum age is 24 hours. A changed VE.Bus service
  invalidates the cache immediately; live cache checkpoints are coalesced to at
  most twice per maximum-age interval while the discharge guard is relevant.
- The value present before activation is retained as the restore value. The
  Venus `-1` default setting is restored exactly as `-1` unless an external
  GUI change replaces it while protection is active.
- A custom pre-existing discharge-power value is durably synchronized before
  the temporary guard is written. For the standard `-1` default, only the
  default restore intent is retained.
- Release requires both a confirmed charging period after activation and a SoC
  strictly above 25%. By default, battery power must remain at or above 100 W
  for at least 120 monotonic seconds, with no more than 90 seconds between
  consecutive valid samples. Missing, lower, non-finite, or excessively late
  samples restart confirmation. Merely crossing 25% does not release the guard.
- A GUI change made while the guard is active becomes the new restore value.
  If it permits more than the 40% cap, the cap remains enforced until release.
- Every temporary `MaxDischargePower` write is preceded by a durably flushed
  intent containing a generation, the expected current value and the intended
  value. A successful method call is committed only after immediate readback
  confirms the intended value. Failed readback retains the pending intent for
  idempotent reconciliation in the next cycle. After a restart, an already
  visible intended value is committed, an unchanged expected value is retried,
  and any third value is treated as a newer external GUI change and is never
  overwritten by recovery.
- An unchanged expected value alone is insufficient for replay. A pending
  restriction is retried only while SoC is still below the entry threshold and
  the battery is currently discharging. A pending restore is retried only while
  confirmed recharge and the release SoC still require it. If either action is
  no longer required and its target is demonstrably absent, its durable intent
  is cancelled before policy evaluation continues; no obsolete actuator write
  is issued.
- Missing SoC, battery power, setting, or nominal-power data never causes a
  speculative write or clears an already active guard. A low-SoC protection
  request that cannot currently be verified or written is exposed as
  `discharge_protection_pending_unenforced` with a bounded reason in the cycle
  decision, retried on subsequent cycles and logged with rate limiting.
- A pending write is never replayed using a stale nominal-power cache. If its
  target no longer matches the current hardware and policy basis, the old
  intent is discarded and the protection is evaluated again from current
  telemetry.

## Measurements and discovery

- AC PV on grid and output plus DC PV retain the established optional-source
  aggregation; unavailable PV sources contribute zero.
- Grid and consumption totals use the corresponding Venus
  `NumberOfPhases` value. Exactly the declared one, two, or three phase values
  must be present; an undeclared L2 or L3 is not treated as missing telemetry.
  A missing phase-count value retains the conservative legacy requirement for
  all three phases, while an invalid count is rejected. A fallback house load
  requires both valid grid and battery power. Incomplete declared-phase
  telemetry pauses reserve charging at the reached SoC. An expired BMS current
  removes only the service-owned current cap; it does not prevent the reserve
  target from being raised.
- AC PV on grid and output and DC PV are read only while their value is needed
  for transition history or an active reserve-current calculation. A value read
  twice in the same cycle is served from the cycle-local cache.
- The Venus `-1` measurement sentinel is unavailable, while raw settings retain
  `-1`.
- Battery SoC, power, and voltage come from `com.victronenergy.system`, which
  already represents the battery measurement selected by Venus OS.
- The BMS limit is read only from the service named by systemcalc
  `/ActiveBmsService`; the VE.Bus limits are read only from `/VebusService`.
  Service choice never depends on the numerical size of a discovered limit.
- BMS and VE.Bus limits of 0 A are explicit charge prohibitions, not missing
  telemetry. `None` alone represents an unknown limit. A live BMS 0 A value
  immediately replaces a previously positive cache entry. A cached BMS limit
  expires after five minutes without a successful confirmation by default.
- Cycle decisions expose `charging_inhibited`, all typed
  `charging_inhibit_reasons`, the optional diagnostic `user_charge_limited`
  state, current-limit availability, and a typed reserve-charging pause reason.
- A changed active service immediately invalidates every cache bound to the
  preceding service. `ESS_PREFERRED_BATTERY_SERVICE` is an optional legacy
  fallback used only when systemcalc does not publish a valid active BMS; no
  fallback service is compiled into the binary.

## Persistence and operation

- Full runtime state remains in RAM at
  `/dev/shm/venus-ess-winter-soc-service/state.json` inside a `0700` directory.
- State documents retain the established controller fields and add an exact
  schema version plus a stable device identity. Wrong-device, future-dated,
  structurally invalid, or semantically unsafe state is rejected before merge.
- A root-owned legacy RAM document at the former default path may be imported
  once through the same semantic validator; unbound removable state is rejected.
- Restore state for temporary `MaxChargeCurrent`, `MaxDischargePower`, and
  `MinimumSocLimit` changes is durable throughout the year. It is written to
  detected removable SD/USB storage first, or to the small configured `/data`
  fallback file when no removable medium is mounted.
- Durable restore writes are event-driven, atomic, and carry a monotonically
  increasing persistence generation. A D-Bus actuator write is permitted only
  after the exact pending generation, or a newer superseding generation, has
  been confirmed on the durable medium; queue idleness alone is insufficient.
- Every queued removable-storage write is bound to the detected mount root,
  Linux mount ID, device number, root inode, filesystem ID, filesystem root,
  type, and source. The worker reopens and verifies that exact root immediately
  before writing and resolves the destination relative to its directory
  descriptor. A removed, remounted, or replaced medium therefore makes the
  queued request stale; no directory is created below the exposed mount path.
- The `/data` restore fallback is a distinct non-removable target. Only that
  target may create a missing absolute parent directory; a stale removable
  request never silently falls back to it or counts as durably persisted.
- Cleared ownership is represented by an explicit generation-bound tombstone
  whenever an older durable restore record exists. A newer RAM tombstone wins
  over older removable or fallback state and is reconciled to that medium, so
  the old ownership cannot reappear after a later full reboot. Default GUI
  values remain derived rather than stored when no old restore record exists.
- A durability timeout leaves the write-ahead state intact and pauses the
  corresponding D-Bus action. A rejected RAM/enqueue operation may roll the
  staged state back; an asynchronously pending write never does, because its
  completion could otherwise persist a state that the controller has forgotten.
- State and log files are regular owner-controlled `0600` files. Opens use
  no-follow semantics; atomic writes use random same-directory files created
  exclusively, followed by synchronization and rename when durability is
  required.
- PV daily averages are collected and included in the removable-storage subset
  only during the pre-winter and post-winter transition windows. A day enters
  this history only after at least 75% of the 09:00-17:00 observation window,
  six integrated hours by default, has valid PV coverage. Long measurement
  gaps do not count; a fully observed day with 0 W remains valid.
- PV coverage advances only when every aggregate AC/DC channel observed in the
  current runtime state has a finite, non-negative value. A real `0 W` is a
  valid measurement; absence, a wrong type, a negative value, or a D-Bus
  transport failure breaks the integration interval. Channels that have never
  been observed remain optional, preserving AC-only and DC-only systems. The
  learned mask is part of the seasonally persisted PV subset, so with removable
  storage available a full reboot cannot turn a previously known but currently
  failed channel into an optional one.
- Removable storage inserted or replaced during an active seasonal window is
  identified and its validated newer state is imported before the first write
  to that medium. Absence never marks a seasonal import as completed.
- The small seasonal subset is written only to removable SD or USB storage, no
  more often than every six hours except for setting-ownership, balancing,
  safety, or shutdown checkpoints.
- Shadow mode never writes DBus or SD and uses separate RAM state, log, and
  decision files. Every integer and floating-point setting write passes through
  one shadow-aware actuator boundary. A preview is distinct from an applied
  write and therefore performs no readback, ownership commit, pending-write
  recovery, or shutdown restore.
- Active and shadow modes each hold a distinct exclusive advisory lock, so two
  instances of either mode cannot control or overwrite the same state.
- A missing or invalid SoC or MinSoC skips the cycle without changing settings.
- Runtime durations use monotonic system uptime; Epoch time is reserved for
  calendar decisions, generated timestamps, and durable event dates.
- A recoverable D-Bus transport fault receives one retry and then stops further
  D-Bus operations for that cycle. The next scheduled cycle starts with a fresh
  transport budget.
- Failed D-Bus reads are exposed as typed, deduplicated cycle diagnostics that
  distinguish unavailable services and paths, type mismatches, timeouts,
  transport failures and method errors. Legitimate optional `None` values are
  not errors. At most 16 distinct issues are retained; further issues are
  represented by a saturating overflow count.

The default scenario cases in `contracts/scenarios.json` are executed by
`tests/scenario_contracts.rs`. Changing a default boundary requires an explicit
contract update; selecting a valid deployment override does not.

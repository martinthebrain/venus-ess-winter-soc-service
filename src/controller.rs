//! Stateful controller that applies the pure policy through typed boundaries.

use crate::charge_ceiling::{ChargeCeilingEvaluation, evaluate as evaluate_charge_ceiling};
use crate::charge_current_control::{
    ChargeCurrentAction, PendingWriteResolution as ChargeCurrentPendingWriteResolution,
    clear_requests as clear_charge_current_requests, evaluate as evaluate_charge_current_control,
    prepare_action as prepare_charge_current_action,
    reconcile_pending_write as reconcile_pending_charge_current_write,
    routine_limit_required as charge_current_limit_required, set_configured_constraint,
    set_explicit_inhibit_requested, set_reserve_constraint, set_routine_ceiling_requested,
};
use crate::clock::{Clock, LocalDateTime};
use crate::config::{
    AC_CONSUMPTION_ON_INPUT_PHASE_COUNT_PATH, AC_CONSUMPTION_PHASE_COUNT_PATH,
    AC_GRID_PHASE_COUNT_PATH, AC_GRID_POWER_PATH, AC_PV_ON_GRID_POWER_PATH,
    AC_PV_ON_OUTPUT_POWER_PATH, ACTIVE_BMS_SERVICE_PATH, BATTERY_POWER_PATH, BATTERY_SOC_PATH,
    BATTERY_VOLTAGE_PATH, BMS_ALLOW_TO_CHARGE_PATH, BMS_MAX_CHARGE_CURRENT_PATH, DC_PV_POWER_PATH,
    MAX_CHARGE_CURRENT_PATH, MAX_DISCHARGE_POWER_PATH, MAX_VALID_SOC, MIN_SOC_PATH, MIN_VALID_SOC,
    NOMINAL_INVERTER_POWER_PATH, PHASES, PV_OBSERVATION_END_HOUR, PV_OBSERVATION_START_HOUR,
    RuntimeConfig, SYSTEM_CHARGE_DISABLED_PATH, SYSTEM_USER_CHARGE_LIMITED_PATH,
    VEBUS_ALLOW_TO_CHARGE_PATH, VEBUS_MAX_CHARGE_CURRENT_PATH, VEBUS_SERVICE_PATH,
};
use crate::discharge_protection::{
    PendingWriteResolution as DischargePendingWriteResolution, ProtectionAction,
    ProtectionEvaluation, ProtectionInput, cancel_unapplied_action, commit_action,
    discard_pending_write, evaluate as evaluate_discharge_protection, normalized_setting,
    pending_retry_is_required, prepare_action as prepare_discharge_action,
    reconcile_pending_write as reconcile_pending_discharge_write,
    release_ownership as release_discharge_ownership, setting_available,
};
use crate::domain::{
    ChargeContext, ChargeCurrentLimitUnavailableReason, ChargeCurrentWriteKind,
    ChargingInhibitReason, ControllerState, CycleDecision, CycleOutcome, DbusFailureKind,
    DbusReadIssue, MinimumSocWriteKind, PV_CHANNEL_MASK, PendingChargeCurrentWrite, PvPower,
    ReserveChargingPauseReason, TargetMode,
};
use crate::logging::LogSink;
use crate::minimum_soc_control::{
    Reconciliation as MinimumSocReconciliation, commit_pending as commit_minimum_soc_write,
    prepare_write as prepare_minimum_soc_write, reconcile as reconcile_minimum_soc_write,
    release_ownership as release_minimum_soc_ownership,
    restore_target as minimum_soc_restore_target, same as same_minimum_soc,
};
use crate::policy::{
    BalancingTimeoutPhase, collect_pv_sample, compute_charge_current_limit, determine_target,
    house_load_fallback, import_only, is_charge_window_active, is_transition_mmdd, is_winter_mmdd,
    needs_charge, pause_soc, reset_pv_gap, roll_pv_day, track_charge_deficit,
    update_full_and_balancing_tracking,
};
use crate::ports::{DbusPort, PortError, StatePort};
use std::time::Duration;

const AC_CONSUMPTION_ON_INPUT: &str = "/Ac/ConsumptionOnInput/{phase}/Power";
const AC_CONSUMPTION: &str = "/Ac/Consumption/{phase}/Power";
const PHASE_PLACEHOLDER: &str = "{phase}";
const MAX_ACCEPTED_CHARGE_CURRENT_A: f64 = 10_000.0;
const MAX_DBUS_READ_ISSUES: usize = 16;

#[derive(Clone, Copy, Debug, PartialEq)]
struct PvObservation {
    power: PvPower,
    expected_mask: u8,
    valid_mask: u8,
    complete: bool,
    topology_changed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CurrentReason {
    ChargeLimit,
    Restore,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ChargeCurrentRequestOrigin {
    RoutineCeiling,
    Reserve,
    Shutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ChargeCurrentControlFailure {
    CurrentRead,
    DurableState,
    DbusWrite,
    Readback,
    ExternalControl,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DurablePersistence {
    Durable,
    Pending,
    Rejected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SettingWriteOutcome {
    Applied,
    Preview,
    Failed,
}

#[derive(Clone, Copy, Debug)]
enum SettingValue {
    Float(f64),
    Integer(i32),
}

impl SettingValue {
    const fn as_f64(self) -> f64 {
        match self {
            Self::Float(value) => value,
            Self::Integer(value) => value as f64,
        }
    }
}

impl From<ChargeCurrentControlFailure> for ChargeCurrentCeilingUnavailableReason {
    fn from(value: ChargeCurrentControlFailure) -> Self {
        match value {
            ChargeCurrentControlFailure::CurrentRead => Self::CurrentRead,
            ChargeCurrentControlFailure::DurableState => Self::DurableState,
            ChargeCurrentControlFailure::DbusWrite => Self::DbusWrite,
            ChargeCurrentControlFailure::Readback => Self::Readback,
            ChargeCurrentControlFailure::ExternalControl => Self::ExternalControl,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NominalPowerSource {
    Live,
    Cached,
    Configured,
}

#[derive(Clone, Debug)]
struct NominalPowerReading {
    watts: f64,
    service: Option<String>,
    observed_at: f64,
    source: NominalPowerSource,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DischargeProtectionUnavailableReason {
    CurrentLimit,
    NominalPower,
    DurableState,
    DbusWrite,
    Readback,
}

impl DischargeProtectionUnavailableReason {
    const fn label(self) -> &'static str {
        match self {
            Self::CurrentLimit => "max_discharge_power_unavailable",
            Self::NominalPower => "nominal_inverter_power_unavailable",
            Self::DurableState => "durable_intent_unavailable",
            Self::DbusWrite => "dbus_write_failed",
            Self::Readback => "dbus_readback_failed",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ChargeCurrentCeilingUnavailableReason {
    CurrentRead,
    DurableState,
    DbusWrite,
    Readback,
    ExternalControl,
}

#[derive(Default)]
enum ServiceResolution {
    #[default]
    Pending,
    Resolved(Option<String>),
}

#[derive(Default)]
struct CycleTopology {
    bms_service: ServiceResolution,
    vebus_service: ServiceResolution,
}

#[derive(Clone, Debug, Default)]
struct ChargeConstraints {
    battery_max_current_a: Option<f64>,
    vebus_max_charge_current_a: Option<f64>,
    inhibit_reasons: Vec<ChargingInhibitReason>,
    user_charge_limited: Option<bool>,
}

impl ChargeConstraints {
    const fn inhibited(&self) -> bool {
        !self.inhibit_reasons.is_empty()
    }
}

impl ChargeCurrentCeilingUnavailableReason {
    const fn label(self) -> &'static str {
        match self {
            Self::CurrentRead => "max_charge_current_unavailable",
            Self::DurableState => "durable_intent_unavailable",
            Self::DbusWrite => "dbus_write_failed",
            Self::Readback => "dbus_readback_failed",
            Self::ExternalControl => "external_charge_current_control",
        }
    }
}

pub struct Controller<P, S, C, L>
where
    P: DbusPort,
    S: StatePort,
    C: Clock,
    L: LogSink,
{
    dbus: P,
    store: S,
    clock: C,
    logger: L,
    config: RuntimeConfig,
    pub state: ControllerState,
    last_charge_limit_set_ts: f64,
    requested_min_soc: Option<f64>,
    requested_max_charge_current: Option<f64>,
    requested_charge_ceiling_current_a: Option<f64>,
    requested_max_discharge_power: Option<f64>,
    charge_current_ceiling_active: bool,
    charge_current_ceiling_unenforced_reason: Option<ChargeCurrentCeilingUnavailableReason>,
    last_charge_current_ceiling_unenforced_log:
        Option<(ChargeCurrentCeilingUnavailableReason, f64)>,
    discharge_protection_unenforced_reason: Option<DischargeProtectionUnavailableReason>,
    last_discharge_protection_unenforced_log: Option<(DischargeProtectionUnavailableReason, f64)>,
    charge_ceiling_soc: Option<f64>,
    full_charge_due: Option<bool>,
    full_charge_age_days: Option<u16>,
    cycle_pv_observation: Option<PvObservation>,
    cycle_topology: CycleTopology,
    cycle_charge_constraints: ChargeConstraints,
    charge_current_limit_unavailable_reasons: Vec<ChargeCurrentLimitUnavailableReason>,
    reserve_charging_pause_reason: Option<ReserveChargingPauseReason>,
    dbus_read_issues: Vec<DbusReadIssue>,
    dbus_read_issue_overflow: u16,
    wrote_setting: bool,
}

impl<P, S, C, L> Controller<P, S, C, L>
where
    P: DbusPort,
    S: StatePort,
    C: Clock,
    L: LogSink,
{
    #[must_use]
    pub const fn new(
        dbus: P,
        store: S,
        clock: C,
        logger: L,
        config: RuntimeConfig,
        state: ControllerState,
    ) -> Self {
        Self {
            dbus,
            store,
            clock,
            logger,
            config,
            state,
            last_charge_limit_set_ts: 0.0,
            requested_min_soc: None,
            requested_max_charge_current: None,
            requested_charge_ceiling_current_a: None,
            requested_max_discharge_power: None,
            charge_current_ceiling_active: false,
            charge_current_ceiling_unenforced_reason: None,
            last_charge_current_ceiling_unenforced_log: None,
            discharge_protection_unenforced_reason: None,
            last_discharge_protection_unenforced_log: None,
            charge_ceiling_soc: None,
            full_charge_due: None,
            full_charge_age_days: None,
            cycle_pv_observation: None,
            cycle_topology: CycleTopology {
                bms_service: ServiceResolution::Pending,
                vebus_service: ServiceResolution::Pending,
            },
            cycle_charge_constraints: ChargeConstraints {
                battery_max_current_a: None,
                vebus_max_charge_current_a: None,
                inhibit_reasons: Vec::new(),
                user_charge_limited: None,
            },
            charge_current_limit_unavailable_reasons: Vec::new(),
            reserve_charging_pause_reason: None,
            dbus_read_issues: Vec::new(),
            dbus_read_issue_overflow: 0,
            wrote_setting: false,
        }
    }

    pub fn log_startup(&mut self) {
        let storage = self.store.sd_description().map_or_else(
            || "RAM-only".to_owned(),
            |description| {
                if self.config.shadow {
                    format!("shadow, {description} ignored")
                } else {
                    format!("seasonal removable storage available: {description}")
                }
            },
        );
        self.logger.log(&format!(
            "ESS winter controller started ({storage}, summer minimum SoC {:.1}%)",
            self.config.policy.summer_min_soc
        ));
    }

    pub fn run_once(&mut self) -> CycleDecision {
        self.begin_cycle();
        let now = self.clock.local_date_time();
        let generated_at = self.clock.epoch_seconds();
        let now_ts = self.clock.monotonic_seconds();
        match self
            .store
            .refresh_window(&mut self.state, now, generated_at)
        {
            Ok(true) => self
                .logger
                .log("SD window active: newer state loaded from SD"),
            Ok(false) => {}
            Err(error) => self
                .logger
                .log(&format!("SD state refresh failed: {error}")),
        }
        self.update_pv_history(now, now_ts);

        let Some(current_soc) = self.read_current_soc(now_ts) else {
            return self.decision(
                generated_at,
                &TargetMode {
                    target_soc: self.config.policy.summer_min_soc,
                    mode: "Unavailable",
                },
                None,
                None,
                CycleOutcome::MissingSoc,
            );
        };
        let current_min_soc = self.raw(&self.config.settings_service.clone(), MIN_SOC_PATH);
        let Some(current_min_soc) = current_min_soc.filter(|value| {
            *value >= MIN_VALID_SOC && *value <= MAX_VALID_SOC && value.is_finite()
        }) else {
            self.log_invalid_min_soc(now_ts);
            return self.decision(
                generated_at,
                &TargetMode {
                    target_soc: self.config.policy.summer_min_soc,
                    mode: "Unavailable",
                },
                Some(current_soc),
                current_min_soc,
                CycleOutcome::MissingMinSoc,
            );
        };
        self.reconcile_minimum_soc_control(current_min_soc, now);
        let battery_power =
            self.measurement_optional(&self.config.system_service.clone(), BATTERY_POWER_PATH);
        self.refresh_charge_constraints(now, now_ts);
        self.apply_discharge_protection(current_soc, battery_power, now, now_ts);
        let mut ceiling = self.evaluate_charge_ceiling(current_soc, battery_power, now);

        let target =
            self.track_and_determine_target(current_soc, &mut ceiling, now, generated_at, now_ts);
        self.apply_charge_current_ceiling(
            current_soc,
            ceiling,
            self.cycle_charge_constraints.inhibited(),
            now,
            now_ts,
        );

        self.apply_soc_logic(
            target.target_soc,
            current_soc,
            current_min_soc,
            battery_power,
            now,
            now_ts,
        );
        let outcome = if self.wrote_setting {
            CycleOutcome::Applied
        } else {
            CycleOutcome::NoChange
        };
        self.decision(
            generated_at,
            &target,
            Some(current_soc),
            Some(current_min_soc),
            outcome,
        )
    }

    fn begin_cycle(&mut self) {
        self.dbus.begin_cycle();
        self.requested_min_soc = None;
        self.requested_max_charge_current = None;
        self.requested_charge_ceiling_current_a = None;
        self.requested_max_discharge_power = None;
        self.charge_current_ceiling_active = false;
        self.charge_current_ceiling_unenforced_reason = None;
        self.discharge_protection_unenforced_reason = None;
        self.charge_ceiling_soc = None;
        self.full_charge_due = None;
        self.full_charge_age_days = None;
        self.cycle_pv_observation = None;
        self.cycle_topology = CycleTopology::default();
        self.cycle_charge_constraints = ChargeConstraints::default();
        self.charge_current_limit_unavailable_reasons.clear();
        self.reserve_charging_pause_reason = None;
        self.dbus_read_issues.clear();
        self.dbus_read_issue_overflow = 0;
        self.wrote_setting = false;
    }

    pub fn shutdown(&mut self) {
        let now = self.clock.local_date_time();
        let now_ts = self.clock.monotonic_seconds();
        self.logger.log(
            "Shutdown requested; restoring the owned MaxChargeCurrent baseline while retaining MinSoC and discharge protection",
        );
        self.dbus.begin_cycle();
        if self.config.shadow {
            self.logger
                .log("Shadow: MaxChargeCurrent baseline left unchanged during shutdown");
        } else {
            self.restore_owned_charge_current(now, now_ts);
        }
        self.save_state(now, true);
        if !self.store.flush(Duration::from_secs(5)) {
            self.logger
                .log("Timed out while flushing the final SD checkpoint");
        }
    }

    /// Restore every setting that is still unambiguously owned by this service.
    ///
    /// This is an explicit removal operation, not the normal supervised
    /// shutdown path. A setting changed by another actor is retained.
    ///
    /// # Errors
    ///
    /// Returns an error when any owned setting cannot be reconciled, restored,
    /// read back, or durably committed.
    pub fn restore_all_owned_settings(&mut self) -> Result<(), String> {
        if self.config.shadow {
            return Err("full owned-setting cleanup is unavailable in shadow mode".to_owned());
        }
        let now = self.clock.local_date_time();
        let now_ts = self.clock.monotonic_seconds();
        self.logger
            .log("Explicit cleanup requested; restoring all unambiguously owned settings");
        let mut failures = Vec::new();

        self.dbus.begin_cycle();
        self.restore_owned_charge_current(now, now_ts);
        if self.state.charge_current_control.owned
            || self.state.charge_current_control.pending_write.is_some()
        {
            failures.push("MaxChargeCurrent");
        }

        self.dbus.begin_cycle();
        if !self.restore_owned_discharge_power(now, now_ts) {
            failures.push("MaxDischargePower");
        }

        self.dbus.begin_cycle();
        if !self.restore_owned_minimum_soc(now, now_ts) {
            failures.push("MinimumSocLimit");
        }

        self.save_state(now, true);
        if !self.store.flush(Duration::from_secs(5)) {
            failures.push("final durable checkpoint");
        }
        if failures.is_empty() {
            self.logger
                .log("Explicit cleanup completed; all owned settings were released");
            Ok(())
        } else {
            Err(format!(
                "owned-setting cleanup incomplete: {}",
                failures.join(", ")
            ))
        }
    }

    fn restore_owned_discharge_power(&mut self, now: LocalDateTime, now_ts: f64) -> bool {
        if !self.state.discharge_protection.active
            && self.state.discharge_protection.pending_write.is_none()
        {
            return true;
        }
        let current = self.raw(
            &self.config.settings_service.clone(),
            MAX_DISCHARGE_POWER_PATH,
        );
        let Some(nominal_reading) = self.nominal_inverter_power(now, now_ts) else {
            self.logger
                .log("Cleanup could not resolve the active VE.Bus nominal power");
            return false;
        };
        self.bind_nominal_inverter_power(&nominal_reading);
        let nominal = Some(nominal_reading.watts);

        if let Some(result) = self.reconcile_discharge_cleanup_pending(current, nominal, now) {
            return result;
        }

        if !self.state.discharge_protection.active {
            return true;
        }
        let Some(normalized_current) = normalized_setting(current, nominal) else {
            self.logger
                .log("Cleanup could not read a valid MaxDischargePower value");
            return false;
        };
        let Some(last_set) = self.state.discharge_protection.last_set_power_w else {
            release_discharge_ownership(&mut self.state.discharge_protection);
            return self.persist_gui_restore_state(now) == DurablePersistence::Durable;
        };
        if !same_discharge_power(
            normalized_current,
            last_set,
            self.config.policy.discharge_power_epsilon_w,
        ) {
            release_discharge_ownership(&mut self.state.discharge_protection);
            self.logger
                .log("Cleanup retained an externally changed MaxDischargePower value");
            return self.persist_gui_restore_state(now) == DurablePersistence::Durable;
        }
        let target = if self.state.discharge_protection.restore_default {
            -1.0
        } else {
            let Some(restore) = self.state.discharge_protection.restore_power_w else {
                return false;
            };
            restore.min(nominal_reading.watts)
        };
        if self.state.discharge_protection.pending_write.is_none() {
            prepare_discharge_action(
                &mut self.state.discharge_protection,
                ProtectionAction::Restore(target),
                normalized_current,
            );
        }
        if self.persist_gui_restore_state(now) != DurablePersistence::Durable {
            return false;
        }
        if self.write_discharge_power(target) != SettingWriteOutcome::Applied
            || !self.discharge_write_readback_confirmed(nominal)
        {
            return false;
        }
        commit_action(
            &mut self.state.discharge_protection,
            ProtectionAction::Restore(target),
        );
        self.logger.log(&format!(
            "Cleanup restored the owned MaxDischargePower baseline to {target:.0}W"
        ));
        self.persist_gui_restore_state(now) == DurablePersistence::Durable
    }

    fn reconcile_discharge_cleanup_pending(
        &mut self,
        current: Option<f64>,
        nominal: Option<f64>,
        now: LocalDateTime,
    ) -> Option<bool> {
        let resolution = reconcile_pending_discharge_write(
            &self.state.discharge_protection,
            current,
            nominal,
            self.config.policy.discharge_protection_nominal_fraction,
            self.config.policy.discharge_power_epsilon_w,
        )?;
        match resolution {
            DischargePendingWriteResolution::Applied(action) => {
                commit_action(&mut self.state.discharge_protection, action);
                let durable = self.persist_gui_restore_state(now) == DurablePersistence::Durable;
                if !durable || matches!(action, ProtectionAction::Restore(_)) {
                    Some(durable)
                } else {
                    None
                }
            }
            DischargePendingWriteResolution::Retry(ProtectionAction::Restrict(_)) => {
                release_discharge_ownership(&mut self.state.discharge_protection);
                self.logger
                    .log("Cleanup discarded an unapplied MaxDischargePower restriction");
                Some(self.persist_gui_restore_state(now) == DurablePersistence::Durable)
            }
            DischargePendingWriteResolution::Retry(ProtectionAction::Restore(_)) => None,
            DischargePendingWriteResolution::ExternalChange
            | DischargePendingWriteResolution::Obsolete => {
                release_discharge_ownership(&mut self.state.discharge_protection);
                self.logger
                    .log("Cleanup retained an externally changed MaxDischargePower value");
                Some(self.persist_gui_restore_state(now) == DurablePersistence::Durable)
            }
            DischargePendingWriteResolution::Unavailable => {
                self.logger
                    .log("Cleanup could not reconcile MaxDischargePower");
                Some(false)
            }
        }
    }

    fn restore_owned_minimum_soc(&mut self, now: LocalDateTime, now_ts: f64) -> bool {
        if !self.state.minimum_soc_control.owned
            && self.state.minimum_soc_control.pending_write.is_none()
        {
            return true;
        }
        let Some(current) = self
            .raw(&self.config.settings_service.clone(), MIN_SOC_PATH)
            .filter(|value| value.is_finite() && (MIN_VALID_SOC..=MAX_VALID_SOC).contains(value))
        else {
            self.logger
                .log("Cleanup could not read a valid MinimumSocLimit value");
            return false;
        };
        match reconcile_minimum_soc_write(
            &self.state.minimum_soc_control,
            current,
            self.config.policy.min_soc_epsilon,
        ) {
            MinimumSocReconciliation::Applied(kind) => {
                commit_minimum_soc_write(&mut self.state.minimum_soc_control);
                if kind == MinimumSocWriteKind::Apply {
                    self.state.minimum_soc_control.last_set = Some(current);
                }
                if self.persist_gui_restore_state(now) != DurablePersistence::Durable {
                    return false;
                }
                if kind == MinimumSocWriteKind::Restore {
                    return true;
                }
            }
            MinimumSocReconciliation::Retry(MinimumSocWriteKind::Apply) => {
                release_minimum_soc_ownership(&mut self.state.minimum_soc_control);
                self.logger
                    .log("Cleanup discarded an unapplied MinimumSocLimit change");
                return self.persist_gui_restore_state(now) == DurablePersistence::Durable;
            }
            MinimumSocReconciliation::Retry(MinimumSocWriteKind::Restore)
            | MinimumSocReconciliation::Stable => {}
            MinimumSocReconciliation::ExternalChange => {
                release_minimum_soc_ownership(&mut self.state.minimum_soc_control);
                self.logger
                    .log("Cleanup retained an externally changed MinimumSocLimit value");
                return self.persist_gui_restore_state(now) == DurablePersistence::Durable;
            }
        }
        if !self.state.minimum_soc_control.owned {
            return true;
        }
        let Some(target) = minimum_soc_restore_target(&self.state.minimum_soc_control) else {
            return false;
        };
        if same_minimum_soc(current, target, self.config.policy.min_soc_epsilon) {
            release_minimum_soc_ownership(&mut self.state.minimum_soc_control);
            return self.persist_gui_restore_state(now) == DurablePersistence::Durable;
        }
        if self.state.minimum_soc_control.pending_write.is_none() {
            prepare_minimum_soc_write(
                &mut self.state.minimum_soc_control,
                current,
                target,
                MinimumSocWriteKind::Restore,
            );
        }
        if self.persist_gui_restore_state(now) != DurablePersistence::Durable {
            return false;
        }
        if self.write_setting(
            &self.config.settings_service.clone(),
            MIN_SOC_PATH,
            SettingValue::Float(target),
        ) != SettingWriteOutcome::Applied
        {
            return false;
        }
        let confirmed = self
            .raw(&self.config.settings_service.clone(), MIN_SOC_PATH)
            .is_some_and(|value| {
                same_minimum_soc(value, target, self.config.policy.min_soc_epsilon)
            });
        if !confirmed {
            return false;
        }
        commit_minimum_soc_write(&mut self.state.minimum_soc_control);
        self.state.min_soc_last_script_set = Some(target);
        self.state.min_soc_last_script_set_ts = now_ts;
        self.state.min_soc_last_seen = Some(target);
        self.logger.log(&format!(
            "Cleanup restored the owned MinimumSocLimit baseline to {target:.1}%"
        ));
        self.persist_gui_restore_state(now) == DurablePersistence::Durable
    }

    fn decision(
        &mut self,
        generated_at: f64,
        target: &TargetMode,
        current_soc: Option<f64>,
        current_min_soc: Option<f64>,
        mut outcome: CycleOutcome,
    ) -> CycleDecision {
        if let Some(error) = self.dbus.cycle_fault() {
            self.logger.log(&format!(
                "D-Bus cycle stopped after transport failure: {error}"
            ));
            outcome = CycleOutcome::DbusFailure;
        }
        CycleDecision {
            generated_at,
            mode: target.mode.to_owned(),
            target_soc: target.target_soc,
            current_soc,
            current_min_soc,
            requested_min_soc: self.requested_min_soc,
            requested_max_charge_current: self.requested_max_charge_current,
            requested_charge_ceiling_current_a: self.requested_charge_ceiling_current_a,
            requested_max_discharge_power: self.requested_max_discharge_power,
            charge_current_ceiling_active: self.charge_current_ceiling_active,
            charge_current_ceiling_owned: self.state.charge_current_control.owned
                && self.state.charge_current_control.routine_ceiling_requested,
            charge_current_ceiling_pending_unenforced: self
                .charge_current_ceiling_unenforced_reason
                .is_some(),
            charge_current_ceiling_unenforced_reason: self
                .charge_current_ceiling_unenforced_reason
                .map(|reason| reason.label().to_owned()),
            discharge_protection_active: self.state.discharge_protection.active,
            discharge_protection_pending_unenforced: self
                .discharge_protection_unenforced_reason
                .is_some(),
            discharge_protection_unenforced_reason: self
                .discharge_protection_unenforced_reason
                .map(|reason| reason.label().to_owned()),
            charge_ceiling_soc: self.charge_ceiling_soc,
            full_charge_due: self.full_charge_due,
            full_charge_age_days: self.full_charge_age_days,
            charging_inhibited: self.cycle_charge_constraints.inhibited(),
            charging_inhibit_reasons: self.cycle_charge_constraints.inhibit_reasons.clone(),
            user_charge_limited: self.cycle_charge_constraints.user_charge_limited,
            charge_current_limit_unavailable: !self
                .charge_current_limit_unavailable_reasons
                .is_empty(),
            charge_current_limit_unavailable_reasons: self
                .charge_current_limit_unavailable_reasons
                .clone(),
            reserve_charging_paused: self.state.charging_paused,
            reserve_charging_pause_reason: self.reserve_charging_pause_reason,
            dbus_read_issues: self.dbus_read_issues.clone(),
            dbus_read_issue_overflow: self.dbus_read_issue_overflow,
            pv_sample_valid: self
                .cycle_pv_observation
                .map(|observation| observation.complete),
            pv_expected_channels: self
                .cycle_pv_observation
                .map_or(0, |observation| observation.expected_mask.count_ones()),
            pv_valid_channels: self
                .cycle_pv_observation
                .map_or(0, |observation| observation.valid_mask.count_ones()),
            shadow: self.config.shadow,
            outcome,
        }
    }

    fn evaluate_charge_ceiling(
        &mut self,
        current_soc: f64,
        battery_power_w: Option<f64>,
        now: LocalDateTime,
    ) -> ChargeCeilingEvaluation {
        let evaluation = evaluate_charge_ceiling(
            &self.config.policy,
            &mut self.state.charge_ceiling,
            now,
            current_soc,
            battery_power_w,
        );
        if evaluation.near_full_observed {
            self.logger.log(&format!(
                "Near-full charge observed at {current_soc:.1}% SoC; calendar counter reset"
            ));
        }
        if evaluation.state_changed {
            self.save_state(now, false);
        }
        evaluation
    }

    fn apply_charge_current_ceiling(
        &mut self,
        current_soc: f64,
        evaluation: ChargeCeilingEvaluation,
        charging_inhibited: bool,
        now: LocalDateTime,
        now_ts: f64,
    ) {
        self.charge_ceiling_soc = Some(evaluation.ceiling_soc);
        self.full_charge_due = Some(evaluation.full_charge_due);
        self.full_charge_age_days = Some(evaluation.age_days);
        let was_requested = self.state.charge_current_control.routine_ceiling_requested;
        let should_limit = charge_current_limit_required(
            was_requested,
            current_soc,
            evaluation.ceiling_soc,
            self.config.policy.soc_hysteresis,
        );
        self.charge_current_ceiling_active = should_limit;
        let retain_owned_limit = charging_inhibited && was_requested;
        let effective_should_limit = should_limit || retain_owned_limit;
        set_routine_ceiling_requested(
            &mut self.state.charge_current_control,
            effective_should_limit,
        );

        if !effective_should_limit && !was_requested {
            return;
        }
        if let Err(error) =
            self.actuate_charge_current(ChargeCurrentRequestOrigin::RoutineCeiling, now, now_ts)
        {
            if effective_should_limit {
                self.mark_charge_current_ceiling_unenforced(error.into(), now_ts);
            }
        }
    }

    fn actuate_charge_current(
        &mut self,
        origin: ChargeCurrentRequestOrigin,
        now: LocalDateTime,
        now_ts: f64,
    ) -> Result<(), ChargeCurrentControlFailure> {
        if self.state.charge_current_control.pending_write.is_some()
            && !self.store.flush(Duration::from_secs(5))
        {
            self.logger
                .log("MaxChargeCurrent write remains paused until its durable intent is confirmed");
            return Err(ChargeCurrentControlFailure::DurableState);
        }
        let current_a = self
            .raw(
                &self.config.settings_service.clone(),
                MAX_CHARGE_CURRENT_PATH,
            )
            .filter(|value| valid_charge_current_setting(*value))
            .ok_or(ChargeCurrentControlFailure::CurrentRead)?;
        if self.config.shadow {
            return self.preview_charge_current_control(current_a, origin);
        }

        let generation = self
            .state
            .charge_current_control
            .pending_write
            .map_or(0, |pending| pending.generation);
        if let Some(resolution) = reconcile_pending_charge_current_write(
            &mut self.state.charge_current_control,
            current_a,
            self.config.policy.charge_ceiling_current_epsilon_a,
        ) {
            match resolution {
                ChargeCurrentPendingWriteResolution::Applied(kind) => {
                    self.logger.log(&format!(
                        "Recovered completed MaxChargeCurrent {} generation {generation}",
                        charge_current_write_kind_label(kind)
                    ));
                    self.persist_committed_charge_current(now);
                }
                ChargeCurrentPendingWriteResolution::Retry(pending) => {
                    return self.execute_charge_current_write(pending, origin, now, now_ts);
                }
                ChargeCurrentPendingWriteResolution::Cancelled => {
                    self.logger.log(&format!(
                        "Obsolete MaxChargeCurrent write generation {generation} cancelled"
                    ));
                    self.persist_committed_charge_current(now);
                }
                ChargeCurrentPendingWriteResolution::ExternalChange { satisfied } => {
                    self.logger.log(&format!(
                        "MaxChargeCurrent write generation {generation} superseded by external control"
                    ));
                    self.persist_committed_charge_current(now);
                    return if satisfied {
                        Ok(())
                    } else {
                        Err(ChargeCurrentControlFailure::ExternalControl)
                    };
                }
            }
        }

        let before = self.state.charge_current_control.clone();
        let evaluation = evaluate_charge_current_control(
            &mut self.state.charge_current_control,
            current_a,
            self.config.policy.charge_ceiling_current_epsilon_a,
        );
        if evaluation.ownership_released {
            self.logger
                .log("MaxChargeCurrent changed externally; unified ownership released");
            self.persist_committed_charge_current(now);
        } else if self.state.charge_current_control != before {
            self.save_state(now, false);
        }
        if let Some(action) = evaluation.action {
            return self.stage_charge_current_action(action, current_a, origin, now, now_ts);
        }
        if evaluation.blocked_by_external_control || !evaluation.satisfied {
            return Err(ChargeCurrentControlFailure::ExternalControl);
        }
        Ok(())
    }

    fn preview_charge_current_control(
        &mut self,
        current_a: f64,
        origin: ChargeCurrentRequestOrigin,
    ) -> Result<(), ChargeCurrentControlFailure> {
        let mut preview = self.state.charge_current_control.clone();
        let evaluation = evaluate_charge_current_control(
            &mut preview,
            current_a,
            self.config.policy.charge_ceiling_current_epsilon_a,
        );
        if let Some(action) = evaluation.action {
            self.record_charge_current_request(origin, action.intended_a());
            self.logger.log(&format!(
                "Shadow: would {} DVCC MaxChargeCurrent to {:.1}A",
                charge_current_action_label(action),
                action.intended_a()
            ));
            return Ok(());
        }
        if evaluation.blocked_by_external_control || !evaluation.satisfied {
            return Err(ChargeCurrentControlFailure::ExternalControl);
        }
        Ok(())
    }

    fn stage_charge_current_action(
        &mut self,
        action: ChargeCurrentAction,
        current_a: f64,
        origin: ChargeCurrentRequestOrigin,
        now: LocalDateTime,
        now_ts: f64,
    ) -> Result<(), ChargeCurrentControlFailure> {
        let previous_state = self.state.charge_current_control.clone();
        let pending = prepare_charge_current_action(
            &mut self.state.charge_current_control,
            action,
            current_a,
        );
        match self.persist_gui_restore_state(now) {
            DurablePersistence::Durable => {}
            DurablePersistence::Pending => {
                self.logger.log(
                    "MaxChargeCurrent left unchanged while its unified restore intent is pending durable confirmation",
                );
                return Err(ChargeCurrentControlFailure::DurableState);
            }
            DurablePersistence::Rejected => {
                self.state.charge_current_control = previous_state;
                self.save_state(now, false);
                self.logger.log(
                    "MaxChargeCurrent left unchanged because its unified restore intent could not be queued",
                );
                return Err(ChargeCurrentControlFailure::DurableState);
            }
        }
        self.execute_charge_current_write(pending, origin, now, now_ts)
    }

    fn execute_charge_current_write(
        &mut self,
        pending: PendingChargeCurrentWrite,
        origin: ChargeCurrentRequestOrigin,
        now: LocalDateTime,
        now_ts: f64,
    ) -> Result<(), ChargeCurrentControlFailure> {
        self.record_charge_current_request(origin, pending.intended_a);
        match self.write_charge_current_value(pending.intended_a) {
            SettingWriteOutcome::Applied => {}
            SettingWriteOutcome::Preview => return Ok(()),
            SettingWriteOutcome::Failed => {
                return Err(ChargeCurrentControlFailure::DbusWrite);
            }
        }
        let readback = self
            .raw(
                &self.config.settings_service.clone(),
                MAX_CHARGE_CURRENT_PATH,
            )
            .filter(|value| valid_charge_current_setting(*value))
            .ok_or(ChargeCurrentControlFailure::Readback)?;
        match reconcile_pending_charge_current_write(
            &mut self.state.charge_current_control,
            readback,
            self.config.policy.charge_ceiling_current_epsilon_a,
        ) {
            Some(ChargeCurrentPendingWriteResolution::Applied(_)) => {
                self.last_charge_limit_set_ts = now_ts;
                self.wrote_setting = true;
                self.logger.log(&format!(
                    "MaxChargeCurrent -> {:.1}A ({})",
                    pending.intended_a,
                    charge_current_origin_label(origin)
                ));
                self.persist_committed_charge_current(now);
                Ok(())
            }
            Some(ChargeCurrentPendingWriteResolution::ExternalChange { .. }) => {
                self.logger.log(&format!(
                    "MaxChargeCurrent write generation {} lost ownership during readback",
                    pending.generation
                ));
                self.persist_committed_charge_current(now);
                Err(ChargeCurrentControlFailure::ExternalControl)
            }
            _ => Err(ChargeCurrentControlFailure::Readback),
        }
    }

    fn persist_committed_charge_current(&mut self, now: LocalDateTime) {
        if self.persist_gui_restore_state(now) != DurablePersistence::Durable {
            self.logger
                .log("Unified MaxChargeCurrent state is pending durable persistence");
        }
    }

    fn restore_owned_charge_current(&mut self, now: LocalDateTime, now_ts: f64) {
        self.charge_current_ceiling_active = false;
        clear_charge_current_requests(&mut self.state.charge_current_control);
        if !self.state.charge_current_control.owned
            && self.state.charge_current_control.pending_write.is_none()
        {
            return;
        }
        if let Err(error) =
            self.actuate_charge_current(ChargeCurrentRequestOrigin::Shutdown, now, now_ts)
        {
            self.mark_charge_current_ceiling_unenforced(error.into(), now_ts);
        }
    }

    const fn record_charge_current_request(
        &mut self,
        origin: ChargeCurrentRequestOrigin,
        intended_a: f64,
    ) {
        match origin {
            ChargeCurrentRequestOrigin::RoutineCeiling => {
                self.requested_charge_ceiling_current_a = Some(intended_a);
            }
            ChargeCurrentRequestOrigin::Reserve => {
                self.requested_max_charge_current = Some(intended_a);
            }
            ChargeCurrentRequestOrigin::Shutdown => {}
        }
    }

    fn mark_charge_current_ceiling_unenforced(
        &mut self,
        reason: ChargeCurrentCeilingUnavailableReason,
        now_ts: f64,
    ) {
        self.charge_current_ceiling_unenforced_reason = Some(reason);
        let should_log =
            self.last_charge_current_ceiling_unenforced_log
                .is_none_or(|(previous, logged_at)| {
                    previous != reason
                        || now_ts < logged_at
                        || now_ts - logged_at >= self.config.invalid_log_interval.as_secs_f64()
                });
        if should_log {
            self.logger.log(&format!(
                "Routine SoC ceiling requested but not currently enforceable: {}",
                reason.label()
            ));
            self.last_charge_current_ceiling_unenforced_log = Some((reason, now_ts));
        }
    }

    fn track_and_determine_target(
        &mut self,
        current_soc: f64,
        ceiling: &mut ChargeCeilingEvaluation,
        now: LocalDateTime,
        now_epoch_ts: f64,
        now_ts: f64,
    ) -> TargetMode {
        let tracking = update_full_and_balancing_tracking(
            &self.config.policy,
            &mut self.state,
            current_soc,
            now,
            now_epoch_ts,
            now_ts,
            self.config.loop_interval.as_secs_f64(),
        );
        if tracking.balancing_completed {
            self.logger.log("Balancing completed successfully");
        }
        if tracking.entered_high_soc_hold {
            self.logger
                .log("Balancing entered high-SoC hold at the configured threshold");
        }
        if let Some(phase) = tracking.balancing_timeout_phase {
            let reason = match phase {
                BalancingTimeoutPhase::ApproachToFull => "approach-to-full watchdog expired",
                BalancingTimeoutPhase::HighSocHold => "high-SoC hold watchdog expired",
            };
            self.logger.log(&format!("Balancing aborted ({reason})"));
        }
        if tracking.changed || tracking.force_persist {
            self.save_state(now, tracking.force_persist);
        }

        let was_balancing = self.state.balancing_active;
        let target = determine_target(
            &self.config.policy,
            &mut self.state,
            now,
            now_epoch_ts,
            now_ts,
        );
        if self.state.balancing_active {
            ceiling.ceiling_soc = ceiling
                .ceiling_soc
                .max(self.config.policy.balancing_target_soc);
        }
        let target = if target.target_soc > ceiling.ceiling_soc {
            TargetMode {
                target_soc: ceiling.ceiling_soc,
                mode: "Top Charge Deferred",
            }
        } else {
            target
        };
        if !was_balancing && self.state.balancing_active {
            self.logger.log("Starting balancing cycle");
            self.save_state(now, true);
        }
        if self.state.last_mode != target.mode {
            self.logger.log(&format!(
                "Mode: {} (Target {}%)",
                target.mode, target.target_soc
            ));
            target.mode.clone_into(&mut self.state.last_mode);
            self.save_state(now, false);
        }
        target
    }

    fn update_pv_history(&mut self, now: LocalDateTime, now_ts: f64) {
        if !is_transition_mmdd(&self.config.policy, now.mmdd()) {
            let changed = !self.state.pv_history.is_empty()
                || !self.state.current_day_samples.is_empty()
                || self.state.pv_energy_ws != 0.0
                || self.state.pv_time_s != 0.0
                || self.state.pv_last_sample_ts != 0.0
                || self.state.pv_last_sample_power != 0.0
                || self.state.last_pv_integral_ts != 0.0
                || !self.state.last_sample_date.is_empty();
            self.state.pv_history.clear();
            self.state.current_day_samples.clear();
            self.state.pv_energy_ws = 0.0;
            self.state.pv_time_s = 0.0;
            self.state.last_pv_integral_ts = 0.0;
            self.state.last_sample_date.clear();
            reset_pv_gap(&mut self.state);
            if changed {
                self.save_state(now, false);
            }
            return;
        }
        let today = now.date_key();
        if self.state.last_sample_date != today {
            let average = roll_pv_day(&self.config.policy, &mut self.state, today, now_ts);
            if let Some(average) = average {
                self.logger
                    .log(&format!("PV daily average stored: {average:.2} W"));
            }
            self.save_state(now, average.is_some());
        }
        if (PV_OBSERVATION_START_HOUR..PV_OBSERVATION_END_HOUR).contains(&now.hour) {
            let observation = self.pv_observation();
            if observation.complete {
                collect_pv_sample(
                    &mut self.state,
                    now_ts,
                    observation.power.total_w(),
                    self.config.loop_interval.as_secs_f64(),
                );
            } else {
                reset_pv_gap(&mut self.state);
            }
            if observation.topology_changed {
                self.save_state(now, false);
            }
        } else {
            reset_pv_gap(&mut self.state);
        }
    }

    fn read_current_soc(&mut self, now_ts: f64) -> Option<f64> {
        let value = self.raw(&self.config.system_service.clone(), BATTERY_SOC_PATH);
        if value
            .is_some_and(|soc| soc.is_finite() && (MIN_VALID_SOC..=MAX_VALID_SOC).contains(&soc))
        {
            return value;
        }
        if now_ts - self.state.last_soc_invalid_log_ts
            >= self.config.invalid_log_interval.as_secs_f64()
        {
            self.logger.log("SoC invalid/missing; skipping cycle");
            self.state.last_soc_invalid_log_ts = now_ts;
        }
        None
    }

    fn log_invalid_min_soc(&mut self, now_ts: f64) {
        if now_ts - self.state.last_min_soc_invalid_log_ts
            >= self.config.invalid_log_interval.as_secs_f64()
        {
            self.logger
                .log("MinSoC path invalid/missing; skipping cycle");
            self.state.last_min_soc_invalid_log_ts = now_ts;
        }
    }

    fn apply_discharge_protection(
        &mut self,
        current_soc: f64,
        battery_power_w: Option<f64>,
        now: LocalDateTime,
        now_ts: f64,
    ) {
        let entering = current_soc < self.config.policy.discharge_protection_enter_soc
            && battery_power_w.is_some_and(|power| power < 0.0);
        if !self.state.discharge_protection.active && !entering {
            return;
        }
        let current_limit_w = self.raw(
            &self.config.settings_service.clone(),
            MAX_DISCHARGE_POWER_PATH,
        );
        let nominal_reading = self.nominal_inverter_power(now, now_ts);
        if self.config.shadow {
            self.preview_discharge_protection(
                current_soc,
                battery_power_w,
                current_limit_w,
                nominal_reading.as_ref().map(|reading| reading.watts),
                now_ts,
            );
            return;
        }
        if self.reconcile_discharge_write(
            current_limit_w,
            nominal_reading.as_ref(),
            current_soc,
            battery_power_w,
            now,
            now_ts,
        ) {
            return;
        }
        let nominal_inverter_power_w = nominal_reading.as_ref().map(|reading| reading.watts);
        let nominal_binding_changed = nominal_reading
            .as_ref()
            .is_some_and(|reading| self.bind_nominal_inverter_power(reading));
        let previous_state = self.state.discharge_protection.clone();
        let evaluation = evaluate_discharge_protection(
            &self.config.policy,
            &mut self.state.discharge_protection,
            ProtectionInput {
                soc: current_soc,
                battery_power_w,
                current_limit_w,
                nominal_inverter_power_w,
                monotonic_now: now_ts,
            },
        );
        let mut force_persist = self.observe_discharge_events(&evaluation);
        self.observe_discharge_telemetry(
            entering,
            current_limit_w,
            nominal_inverter_power_w,
            now_ts,
        );
        let action_committed = if let Some(action) = evaluation.action {
            let Some(committed) = self.execute_discharge_action(
                action,
                current_limit_w,
                nominal_inverter_power_w,
                previous_state,
                now,
                now_ts,
            ) else {
                return;
            };
            force_persist |= committed;
            committed
        } else {
            false
        };
        if evaluation.state_changed || action_committed || nominal_binding_changed {
            self.save_state(now, force_persist);
        }
    }

    fn preview_discharge_protection(
        &mut self,
        current_soc: f64,
        battery_power_w: Option<f64>,
        current_limit_w: Option<f64>,
        nominal_inverter_power_w: Option<f64>,
        now_ts: f64,
    ) {
        let mut preview = self.state.discharge_protection.clone();
        let evaluation = evaluate_discharge_protection(
            &self.config.policy,
            &mut preview,
            ProtectionInput {
                soc: current_soc,
                battery_power_w,
                current_limit_w,
                nominal_inverter_power_w,
                monotonic_now: now_ts,
            },
        );
        let Some(action) = evaluation.action else {
            return;
        };
        let target = match action {
            ProtectionAction::Restrict(power) | ProtectionAction::Restore(power) => power,
        };
        let outcome = self.write_discharge_power(target);
        debug_assert_eq!(outcome, SettingWriteOutcome::Preview);
    }

    fn observe_discharge_events(&mut self, evaluation: &ProtectionEvaluation) -> bool {
        if evaluation.events.activated() {
            self.logger.log("Low-SoC discharge protection activated");
        }
        if evaluation.events.recharge_seen() {
            self.logger
                .log("Low-SoC discharge protection confirmed sustained battery charging");
        }
        if evaluation.events.external_change() {
            self.logger.log(
                "MaxDischargePower changed externally; new value retained for later restoration",
            );
        }
        if evaluation.events.released() {
            self.logger
                .log("Low-SoC discharge protection released without a setting write");
        }
        evaluation.events.activated()
            || evaluation.events.external_change()
            || evaluation.events.released()
    }

    fn observe_discharge_telemetry(
        &mut self,
        entering: bool,
        current_limit_w: Option<f64>,
        nominal_inverter_power_w: Option<f64>,
        now_ts: f64,
    ) {
        if !entering && !self.state.discharge_protection.active {
            return;
        }
        let reason = if !setting_available(current_limit_w) {
            Some(DischargeProtectionUnavailableReason::CurrentLimit)
        } else if nominal_inverter_power_w.is_none() {
            Some(DischargeProtectionUnavailableReason::NominalPower)
        } else {
            None
        };
        if let Some(reason) = reason {
            self.mark_discharge_protection_unenforced(reason, now_ts);
        }
    }

    fn execute_discharge_action(
        &mut self,
        action: ProtectionAction,
        current_limit_w: Option<f64>,
        nominal_inverter_power_w: Option<f64>,
        previous_state: crate::domain::DischargeProtectionState,
        now: LocalDateTime,
        now_ts: f64,
    ) -> Option<bool> {
        let target = match action {
            ProtectionAction::Restrict(power) | ProtectionAction::Restore(power) => power,
        };
        let Some(expected_before_w) = normalized_setting(current_limit_w, nominal_inverter_power_w)
        else {
            self.state.discharge_protection = previous_state;
            self.logger.log(
                "MaxDischargePower left unchanged because its current value cannot be reconciled",
            );
            return None;
        };
        prepare_discharge_action(
            &mut self.state.discharge_protection,
            action,
            expected_before_w,
        );
        match self.persist_gui_restore_state(now) {
            DurablePersistence::Durable => {}
            DurablePersistence::Pending => {
                self.mark_discharge_protection_unenforced(
                    DischargeProtectionUnavailableReason::DurableState,
                    now_ts,
                );
                self.logger.log(
                    "MaxDischargePower left unchanged while its restore intent is pending durable confirmation",
                );
                return None;
            }
            DurablePersistence::Rejected => {
                self.state.discharge_protection = previous_state;
                self.save_state(now, false);
                self.mark_discharge_protection_unenforced(
                    DischargeProtectionUnavailableReason::DurableState,
                    now_ts,
                );
                self.logger.log(
                    "MaxDischargePower left unchanged because its restore intent could not be queued",
                );
                return None;
            }
        }
        match self.write_discharge_power(target) {
            SettingWriteOutcome::Applied => {}
            SettingWriteOutcome::Preview => return Some(false),
            SettingWriteOutcome::Failed => {
                self.mark_discharge_protection_unenforced(
                    DischargeProtectionUnavailableReason::DbusWrite,
                    now_ts,
                );
                return Some(false);
            }
        }
        if !self.discharge_write_readback_confirmed(nominal_inverter_power_w) {
            self.mark_discharge_protection_unenforced(
                DischargeProtectionUnavailableReason::Readback,
                now_ts,
            );
            return Some(false);
        }
        commit_action(&mut self.state.discharge_protection, action);
        match action {
            ProtectionAction::Restrict(power) => self.logger.log(&format!(
                "MaxDischargePower -> {power:.0}W (low-SoC protection)"
            )),
            ProtectionAction::Restore(power) => self.logger.log(&format!(
                "MaxDischargePower -> {power:.0}W (protection released)"
            )),
        }
        if self.persist_gui_restore_state(now) != DurablePersistence::Durable {
            self.logger
                .log("Updated MaxDischargePower restore state is pending durable persistence");
        }
        Some(true)
    }

    fn reconcile_discharge_write(
        &mut self,
        current_limit_w: Option<f64>,
        nominal_reading: Option<&NominalPowerReading>,
        current_soc: f64,
        battery_power_w: Option<f64>,
        now: LocalDateTime,
        now_ts: f64,
    ) -> bool {
        if self.state.discharge_protection.pending_write.is_some()
            && !self.store.flush(Duration::from_secs(5))
        {
            self.mark_discharge_protection_unenforced(
                DischargeProtectionUnavailableReason::DurableState,
                now_ts,
            );
            self.logger.log(
                "MaxDischargePower write remains paused until its durable intent is confirmed",
            );
            return true;
        }
        let nominal_inverter_power_w = nominal_reading.map(|reading| reading.watts);
        let resolution = reconcile_pending_discharge_write(
            &self.state.discharge_protection,
            current_limit_w,
            nominal_inverter_power_w,
            self.config.policy.discharge_protection_nominal_fraction,
            self.config.policy.discharge_power_epsilon_w,
        );
        let Some(resolution) = resolution else {
            return false;
        };
        let generation = self
            .state
            .discharge_protection
            .pending_write
            .map_or(0, |pending| pending.generation);
        match resolution {
            DischargePendingWriteResolution::Unavailable => {
                let reason = if setting_available(current_limit_w) {
                    DischargeProtectionUnavailableReason::NominalPower
                } else {
                    DischargeProtectionUnavailableReason::CurrentLimit
                };
                self.mark_discharge_protection_unenforced(reason, now_ts);
                true
            }
            DischargePendingWriteResolution::Obsolete => {
                discard_pending_write(&mut self.state.discharge_protection);
                self.logger.log(&format!(
                    "MaxDischargePower write generation {generation} discarded because its hardware basis changed"
                ));
                false
            }
            DischargePendingWriteResolution::ExternalChange => {
                discard_pending_write(&mut self.state.discharge_protection);
                self.logger.log(&format!(
                    "MaxDischargePower write generation {generation} superseded by an external change"
                ));
                false
            }
            DischargePendingWriteResolution::Applied(action) => {
                commit_action(&mut self.state.discharge_protection, action);
                if let Some(reading) = nominal_reading {
                    self.bind_nominal_inverter_power(reading);
                }
                self.logger.log(&format!(
                    "Recovered completed MaxDischargePower write generation {generation}"
                ));
                if self.persist_gui_restore_state(now) != DurablePersistence::Durable {
                    self.logger
                        .log("Recovered MaxDischargePower state is pending durable persistence");
                }
                self.save_state(now, true);
                false
            }
            DischargePendingWriteResolution::Retry(action) => {
                if !pending_retry_is_required(
                    &self.config.policy,
                    &self.state.discharge_protection,
                    action,
                    current_soc,
                    battery_power_w,
                ) {
                    return self.cancel_obsolete_discharge_retry(action, generation, now, now_ts);
                }
                self.retry_pending_discharge_write(action, nominal_reading, generation, now, now_ts)
            }
        }
    }

    fn cancel_obsolete_discharge_retry(
        &mut self,
        action: ProtectionAction,
        generation: u64,
        now: LocalDateTime,
        now_ts: f64,
    ) -> bool {
        let previous_state = self.state.discharge_protection.clone();
        cancel_unapplied_action(&mut self.state.discharge_protection, action);
        match self.persist_gui_restore_state(now) {
            DurablePersistence::Durable => {}
            DurablePersistence::Pending => {
                self.mark_discharge_protection_unenforced(
                    DischargeProtectionUnavailableReason::DurableState,
                    now_ts,
                );
                return true;
            }
            DurablePersistence::Rejected => {
                self.state.discharge_protection = previous_state;
                self.mark_discharge_protection_unenforced(
                    DischargeProtectionUnavailableReason::DurableState,
                    now_ts,
                );
                return true;
            }
        }
        self.logger.log(&format!(
            "Discarded obsolete pending MaxDischargePower write generation {generation} after revalidating current protection conditions"
        ));
        false
    }

    fn retry_pending_discharge_write(
        &mut self,
        action: ProtectionAction,
        nominal_reading: Option<&NominalPowerReading>,
        generation: u64,
        now: LocalDateTime,
        now_ts: f64,
    ) -> bool {
        let target = match action {
            ProtectionAction::Restrict(power) | ProtectionAction::Restore(power) => power,
        };
        match self.write_discharge_power(target) {
            SettingWriteOutcome::Applied => {}
            SettingWriteOutcome::Preview => return true,
            SettingWriteOutcome::Failed => {
                self.mark_discharge_protection_unenforced(
                    DischargeProtectionUnavailableReason::DbusWrite,
                    now_ts,
                );
                return true;
            }
        }
        let nominal_inverter_power_w = nominal_reading.map(|reading| reading.watts);
        if !self.discharge_write_readback_confirmed(nominal_inverter_power_w) {
            self.mark_discharge_protection_unenforced(
                DischargeProtectionUnavailableReason::Readback,
                now_ts,
            );
            return true;
        }
        commit_action(&mut self.state.discharge_protection, action);
        if let Some(reading) = nominal_reading {
            self.bind_nominal_inverter_power(reading);
        }
        self.logger.log(&format!(
            "Retried MaxDischargePower write generation {generation}"
        ));
        if self.persist_gui_restore_state(now) != DurablePersistence::Durable {
            self.logger
                .log("Retried MaxDischargePower state is pending durable persistence");
        }
        self.save_state(now, true);
        true
    }

    fn nominal_inverter_power(
        &mut self,
        now: LocalDateTime,
        now_ts: f64,
    ) -> Option<NominalPowerReading> {
        let now_epoch = self.clock.epoch_seconds();
        if let Some(power) = self.positive_vebus_measurement(NOMINAL_INVERTER_POWER_PATH, now) {
            let service = self.state.vebus_service.clone()?;
            self.state.nominal_inverter_power_last_seen_monotonic = Some(now_ts);
            return Some(NominalPowerReading {
                watts: power,
                service: Some(service),
                observed_at: now_epoch,
                source: NominalPowerSource::Live,
            });
        }
        if let Some(power) = self.fresh_cached_nominal_inverter_power(now_epoch, now_ts) {
            return Some(NominalPowerReading {
                watts: power,
                service: self.state.nominal_inverter_power_service.clone(),
                observed_at: self.state.nominal_inverter_power_observed_at,
                source: NominalPowerSource::Cached,
            });
        }
        self.config
            .configured_nominal_inverter_power_w
            .map(|power| NominalPowerReading {
                watts: power,
                service: None,
                observed_at: 0.0,
                source: NominalPowerSource::Configured,
            })
    }

    fn fresh_cached_nominal_inverter_power(
        &self,
        now_epoch: f64,
        now_monotonic: f64,
    ) -> Option<f64> {
        let power = self.state.nominal_inverter_power_last?;
        let service = self.state.nominal_inverter_power_service.as_ref()?;
        if self.state.vebus_service.as_ref() != Some(service) {
            return None;
        }
        let max_age = self.config.nominal_inverter_power_max_age.as_secs_f64();
        let fresh_in_process = self
            .state
            .nominal_inverter_power_last_seen_monotonic
            .is_some_and(|seen| now_monotonic >= seen && now_monotonic - seen <= max_age);
        let observed_at = self.state.nominal_inverter_power_observed_at;
        let fresh_persisted =
            observed_at > 0.0 && now_epoch >= observed_at && now_epoch - observed_at <= max_age;
        (fresh_in_process || fresh_persisted).then_some(power)
    }

    fn bind_nominal_inverter_power(&mut self, reading: &NominalPowerReading) -> bool {
        if reading.source == NominalPowerSource::Cached {
            return false;
        }
        let same_value = self.state.nominal_inverter_power_last.is_some_and(|power| {
            (power - reading.watts).abs() <= self.config.policy.discharge_power_epsilon_w
        });
        let same_service = self.state.nominal_inverter_power_service == reading.service;
        let same_source = self.state.nominal_inverter_power_configured
            == (reading.source == NominalPowerSource::Configured);
        let checkpoint_due = reading.source == NominalPowerSource::Live
            && (reading.observed_at < self.state.nominal_inverter_power_observed_at
                || reading.observed_at - self.state.nominal_inverter_power_observed_at
                    >= self.config.nominal_inverter_power_max_age.as_secs_f64() / 2.0);
        if same_value && same_service && same_source && !checkpoint_due {
            return false;
        }

        let nominal_changed = !same_value;
        self.state.nominal_inverter_power_last = Some(reading.watts);
        self.state
            .nominal_inverter_power_service
            .clone_from(&reading.service);
        self.state.nominal_inverter_power_observed_at = reading.observed_at;
        self.state.nominal_inverter_power_configured =
            reading.source == NominalPowerSource::Configured;
        if nominal_changed && self.state.discharge_protection.active {
            clamp_power_to_nominal(
                &mut self.state.discharge_protection.restore_power_w,
                reading.watts,
            );
            clamp_power_to_nominal(
                &mut self.state.discharge_protection.last_set_power_w,
                reading.watts,
            );
            clamp_power_to_nominal(
                &mut self.state.discharge_protection.last_observed_power_w,
                reading.watts,
            );
        }
        true
    }

    fn mark_discharge_protection_unenforced(
        &mut self,
        reason: DischargeProtectionUnavailableReason,
        now_ts: f64,
    ) {
        self.discharge_protection_unenforced_reason = Some(reason);
        let should_log =
            self.last_discharge_protection_unenforced_log
                .is_none_or(|(previous, logged_at)| {
                    previous != reason
                        || now_ts < logged_at
                        || now_ts - logged_at >= self.config.invalid_log_interval.as_secs_f64()
                });
        if should_log {
            self.logger.log(&format!(
                "Low-SoC discharge protection requested but not currently enforceable: {}",
                reason.label()
            ));
            self.last_discharge_protection_unenforced_log = Some((reason, now_ts));
        }
    }

    fn vebus_max_charge_current(&mut self, now: LocalDateTime) -> Option<f64> {
        let service = self.active_vebus_service(now)?;
        self.measurement_optional(&service, VEBUS_MAX_CHARGE_CURRENT_PATH)
            .filter(|value| value.is_finite() && *value >= 0.0)
    }

    fn positive_vebus_measurement(&mut self, path: &str, now: LocalDateTime) -> Option<f64> {
        let service = self.active_vebus_service(now)?;
        self.measurement_optional(&service, path)
            .filter(|value| *value > 0.0)
    }

    fn active_vebus_service(&mut self, now: LocalDateTime) -> Option<String> {
        if let ServiceResolution::Resolved(selected) = &self.cycle_topology.vebus_service {
            return selected.clone();
        }
        let system_service = self.config.system_service.clone();
        let value = match self.dbus.text(&system_service, VEBUS_SERVICE_PATH) {
            Ok(value) => value,
            Err(error) => {
                self.record_dbus_read_error(&system_service, VEBUS_SERVICE_PATH, &error);
                self.cycle_topology.vebus_service = ServiceResolution::Resolved(None);
                return None;
            }
        };
        let selected = selected_service(value, "com.victronenergy.vebus");
        self.cycle_topology.vebus_service = ServiceResolution::Resolved(selected.clone());
        if selected != self.state.vebus_service {
            self.state.vebus_service.clone_from(&selected);
            self.state.nominal_inverter_power_last = None;
            self.state.nominal_inverter_power_service = None;
            self.state.nominal_inverter_power_observed_at = 0.0;
            self.state.nominal_inverter_power_configured = false;
            self.state.nominal_inverter_power_last_seen_monotonic = None;
            self.logger.log(&format!(
                "Active VE.Bus service changed to {}",
                selected.as_deref().unwrap_or("none")
            ));
            self.save_state(now, false);
        }
        selected
    }

    fn refresh_charge_constraints(&mut self, now: LocalDateTime, now_ts: f64) {
        let battery_max_current_a = self.battery_max_charge_current(now, now_ts);
        let vebus_max_charge_current_a = self.vebus_max_charge_current(now);
        let system_service = self.config.system_service.clone();
        let system_charge_disabled =
            self.optional_binary_state(&system_service, SYSTEM_CHARGE_DISABLED_PATH);
        let user_charge_limited =
            self.optional_binary_state(&system_service, SYSTEM_USER_CHARGE_LIMITED_PATH);
        let active_bms_allows_charge = self
            .active_bms_service(now)
            .and_then(|service| self.optional_binary_state(&service, BMS_ALLOW_TO_CHARGE_PATH));
        let vebus_allows_charge = self
            .active_vebus_service(now)
            .and_then(|service| self.optional_binary_state(&service, VEBUS_ALLOW_TO_CHARGE_PATH));

        let mut inhibit_reasons = Vec::new();
        if battery_max_current_a == Some(0.0) {
            inhibit_reasons.push(ChargingInhibitReason::BmsMaxChargeCurrentZero);
        }
        if vebus_max_charge_current_a == Some(0.0) {
            inhibit_reasons.push(ChargingInhibitReason::VebusMaxChargeCurrentZero);
        }
        if system_charge_disabled == Some(true) {
            inhibit_reasons.push(ChargingInhibitReason::SystemChargeDisabled);
        }
        if active_bms_allows_charge == Some(false) {
            inhibit_reasons.push(ChargingInhibitReason::ActiveBmsDisallowsCharge);
        }
        if vebus_allows_charge == Some(false) {
            inhibit_reasons.push(ChargingInhibitReason::VebusDisallowsCharge);
        }
        self.cycle_charge_constraints = ChargeConstraints {
            battery_max_current_a,
            vebus_max_charge_current_a,
            inhibit_reasons,
            user_charge_limited,
        };
    }

    fn optional_binary_state(&mut self, service: &str, path: &str) -> Option<bool> {
        match self.measurement_optional(service, path) {
            Some(0.0) => Some(false),
            Some(1.0) => Some(true),
            _ => None,
        }
    }

    fn apply_soc_logic(
        &mut self,
        target_soc: f64,
        current_soc: f64,
        current_setting: f64,
        battery_power: Option<f64>,
        now: LocalDateTime,
        now_ts: f64,
    ) {
        let in_control_window = is_winter_mmdd(&self.config.policy, now.mmdd())
            || is_transition_mmdd(&self.config.policy, now.mmdd());
        if self.handle_summer_manual_override(current_setting, now, now_ts, in_control_window) {
            return;
        }
        let context = self.build_charge_context(
            current_soc,
            target_soc,
            current_setting,
            battery_power,
            now,
            now_ts,
        );
        if context.charge_deficit_changed {
            self.save_state(now, false);
        }
        if context.needs_charge {
            self.handle_charge_needed(
                target_soc,
                current_soc,
                current_setting,
                context,
                now,
                now_ts,
            );
        } else {
            self.handle_charge_not_needed(
                target_soc,
                current_setting,
                context.battery_max_current_a,
                now,
                now_ts,
            );
        }
    }

    fn handle_summer_manual_override(
        &mut self,
        current_setting: f64,
        now: LocalDateTime,
        now_ts: f64,
        in_control_window: bool,
    ) -> bool {
        let mut changed =
            self.track_manual_min_soc_change(current_setting, now_ts, in_control_window);
        changed |= self.expire_summer_manual_override(in_control_window, now_ts);
        let active = !in_control_window && self.state.manual_override_until_ts > now_ts;
        if !active {
            if changed {
                self.save_state(now, false);
            }
            return false;
        }
        if now_ts - self.state.last_manual_override_log_ts
            >= self.config.status_log_interval.as_secs_f64()
        {
            let remaining = ((self.state.manual_override_until_ts - now_ts) / 3_600.0).max(0.0);
            self.logger.log(&format!(
                "Summer MinSoC override active ({remaining:.1}h remaining)"
            ));
            self.state.last_manual_override_log_ts = now_ts;
            changed = true;
        }
        changed |= self.set_charge_state(false, false);
        if changed {
            self.save_state(now, false);
        }
        let battery_max = self.battery_max_charge_current(now, now_ts);
        let _ = self.restore_normal_charge_current(battery_max, now, now_ts);
        true
    }

    fn expire_summer_manual_override(&mut self, in_control_window: bool, now_ts: f64) -> bool {
        if in_control_window
            || self.state.manual_override_until_ts <= 0.0
            || now_ts < self.state.manual_override_until_ts
        {
            return false;
        }
        self.state.manual_override_until_ts = 0.0;
        self.logger
            .log("Summer MinSoC override expired; controller returns to default");
        true
    }

    fn track_manual_min_soc_change(
        &mut self,
        current_setting: f64,
        now_ts: f64,
        in_control_window: bool,
    ) -> bool {
        let Some(last_seen) = self.state.min_soc_last_seen else {
            self.state.min_soc_last_seen = Some(current_setting);
            return true;
        };
        if (current_setting - last_seen).abs() <= self.config.policy.min_soc_epsilon {
            return false;
        }
        let recent_script_write = self
            .state
            .min_soc_last_script_set
            .is_some_and(|script_value| {
                (current_setting - script_value).abs() <= self.config.policy.min_soc_epsilon
                    && now_ts - self.state.min_soc_last_script_set_ts
                        <= self.config.policy.min_soc_script_write_match_seconds
            });
        let changed = if !in_control_window && !recent_script_write {
            self.state.manual_override_until_ts =
                now_ts + self.config.policy.summer_manual_minsoc_hold_seconds;
            self.state.last_manual_override_log_ts = now_ts;
            let hold_hours = self.config.policy.summer_manual_minsoc_hold_seconds / 3_600.0;
            self.logger.log(&format!(
                "Manual MinSoC change detected; controller leaves the value unchanged for {hold_hours:.1}h"
            ));
            true
        } else {
            false
        };
        self.state.min_soc_last_seen = Some(current_setting);
        changed
    }

    fn build_charge_context(
        &mut self,
        current_soc: f64,
        target_soc: f64,
        current_setting: f64,
        battery_power: Option<f64>,
        now: LocalDateTime,
        now_ts: f64,
    ) -> ChargeContext {
        let charge_needed = needs_charge(&self.config.policy, current_soc, target_soc);
        let charge_deficit_changed = track_charge_deficit(&mut self.state, charge_needed, now_ts);
        let time_ok = is_charge_window_active(&self.config.policy, &self.state, now, now_ts)
            || self.state.balancing_active;
        let grid_net = self.grid_power_net();
        let battery_max = self.cycle_charge_constraints.battery_max_current_a;
        let vebus_max_charge_current = self.cycle_charge_constraints.vebus_max_charge_current_a;
        let voltage =
            self.measurement_optional(&self.config.system_service.clone(), BATTERY_VOLTAGE_PATH);
        let pv_power = if charge_needed
            && time_ok
            && battery_max.is_some()
            && vebus_max_charge_current.is_some()
            && voltage.is_some_and(|value| value > 1.0)
        {
            self.pv_power()
        } else {
            PvPower::default()
        };
        let house_load = self.house_load(grid_net, battery_power);
        let effective_active = self.state.charge_control_active()
            || (now_ts - self.state.boot_ts <= self.config.policy.boot_recovery_seconds
                && charge_needed
                && current_setting
                    >= target_soc - self.config.policy.boot_recovery_target_match_epsilon);
        ChargeContext {
            needs_charge: charge_needed,
            time_ok,
            charge_window_hours: crate::policy::charge_window_hours(
                &self.config.policy,
                &self.state,
                now_ts,
            ),
            charge_deficit_changed,
            stage_charge_target: target_soc > self.config.policy.summer_min_soc,
            grid_import_w: grid_net.map(import_only),
            effective_active,
            battery_max_current_a: battery_max,
            vebus_max_charge_current_a: vebus_max_charge_current,
            battery_voltage_v: voltage,
            pv_power,
            house_load_w: house_load,
            charging_inhibited: self.cycle_charge_constraints.inhibited(),
        }
    }

    fn handle_charge_needed(
        &mut self,
        target_soc: f64,
        current_soc: f64,
        current_setting: f64,
        context: ChargeContext,
        now: LocalDateTime,
        now_ts: f64,
    ) {
        if context.charging_inhibited {
            let limited = context.time_ok
                && self
                    .set_max_charge_current(0.0, CurrentReason::ChargeLimit, now, now_ts)
                    .is_ok();
            let changed = self.pause_reserve_charging(
                current_soc,
                target_soc,
                current_setting,
                ReserveChargingPauseReason::ExplicitChargeInhibit,
                now,
                now_ts,
            );
            if limited || changed {
                self.logger
                    .log("Reserve charging is paused by an explicit charge inhibit");
            }
            return;
        }
        if context.time_ok {
            if context.house_load_w.is_none() {
                if self.pause_reserve_charging(
                    current_soc,
                    target_soc,
                    current_setting,
                    ReserveChargingPauseReason::LoadTelemetryIncomplete,
                    now,
                    now_ts,
                ) {
                    self.logger
                        .log("Reserve charging paused because load telemetry is incomplete");
                }
                return;
            }
            if let Err(reason) =
                self.prepare_reserve_charge(target_soc, current_setting, &context, now, now_ts)
            {
                self.pause_reserve_charging(
                    current_soc,
                    target_soc,
                    current_setting,
                    reason,
                    now,
                    now_ts,
                );
                self.logger.log(&format!(
                    "Reserve charging paused because its actuator sequence is not confirmed ({reason:?})"
                ));
                return;
            }
            if !context.effective_active || self.state.charging_paused {
                self.logger.log(&format!(
                    "Starting/resuming SoC raise to {target_soc}% (grid: {})",
                    format_watts(context.grid_import_w)
                ));
            }
            self.set_charge_state(true, false);
            self.maybe_log_status(
                context.house_load_w,
                context.battery_max_current_a,
                context.vebus_max_charge_current_a,
                now,
                now_ts,
            );
            self.save_state(now, false);
            return;
        }
        if !context.stage_charge_target {
            if (current_setting - target_soc).abs() > self.config.policy.min_soc_epsilon {
                let _ = self.set_min_soc(target_soc, now, now_ts);
            }
            self.set_charge_state(false, false);
            let _ = self.restore_normal_charge_current(context.battery_max_current_a, now, now_ts);
            self.save_state(now, false);
            return;
        }
        self.reserve_charging_pause_reason = Some(ReserveChargingPauseReason::OutsideChargeWindow);
        let hold = pause_soc(current_soc, target_soc, self.config.policy.summer_min_soc);
        let should_write = (current_setting - hold).abs() > self.config.policy.min_soc_epsilon
            && (!self.state.charging_paused
                || hold > current_setting + self.config.policy.min_soc_epsilon);
        let wrote = should_write && self.set_min_soc(hold, now, now_ts).is_ok();
        let changed = self.set_charge_state(true, true);
        self.retry_owned_charge_current_restore(context.battery_max_current_a, now, now_ts);
        if wrote || changed {
            self.maybe_log_status(
                context.house_load_w,
                context.battery_max_current_a,
                context.vebus_max_charge_current_a,
                now,
                now_ts,
            );
            self.logger.log(&format!(
                "SoC raise paused outside charge window (grid: {})",
                format_watts(context.grid_import_w)
            ));
            self.save_state(now, false);
        }
    }

    fn retry_owned_charge_current_restore(
        &mut self,
        battery_max: Option<f64>,
        now: LocalDateTime,
        now_ts: f64,
    ) {
        if self
            .state
            .charge_current_control
            .reserve_constraint_a
            .is_some()
            || self.state.charge_current_control.explicit_inhibit_requested
            || (self.state.charge_current_control.pending_write.is_some()
                && !self.state.charge_current_control.routine_ceiling_requested)
        {
            if let Err(reason) = self.restore_normal_charge_current(battery_max, now, now_ts) {
                self.reserve_charging_pause_reason = Some(reason);
            }
        }
    }

    fn prepare_reserve_charge(
        &mut self,
        target_soc: f64,
        current_setting: f64,
        context: &ChargeContext,
        now: LocalDateTime,
        now_ts: f64,
    ) -> Result<(), ReserveChargingPauseReason> {
        let normal = self.normal_charge_current(context.battery_max_current_a);
        let limit = compute_charge_current_limit(
            &self.config.policy,
            context.house_load_w.unwrap_or_default(),
            context.battery_max_current_a,
            context.vebus_max_charge_current_a,
            context.battery_voltage_v,
            context.pv_power,
            normal,
        );
        if let Some(limit) = limit {
            self.set_max_charge_current(limit, CurrentReason::ChargeLimit, now, now_ts)?;
        } else {
            self.record_charge_current_limit_unavailable(context);
            if self
                .state
                .charge_current_control
                .reserve_constraint_a
                .is_some()
                || self.state.charge_current_control.explicit_inhibit_requested
            {
                self.restore_normal_charge_current(context.battery_max_current_a, now, now_ts)?;
            }
        }
        if (current_setting - target_soc).abs() > self.config.policy.min_soc_epsilon {
            self.set_min_soc(target_soc, now, now_ts)?;
        }
        Ok(())
    }

    fn handle_charge_not_needed(
        &mut self,
        target_soc: f64,
        current_setting: f64,
        battery_max: Option<f64>,
        now: LocalDateTime,
        now_ts: f64,
    ) {
        if (current_setting - target_soc).abs() > self.config.policy.min_soc_epsilon
            && self.set_min_soc(target_soc, now, now_ts).is_ok()
        {
            self.logger
                .log(&format!("SoC limit adjusted to {target_soc}%"));
        }
        if self.set_charge_state(false, false) {
            self.save_state(now, false);
        }
        let _ = self.restore_normal_charge_current(battery_max, now, now_ts);
    }

    fn pause_reserve_charging(
        &mut self,
        current_soc: f64,
        target_soc: f64,
        current_setting: f64,
        reason: ReserveChargingPauseReason,
        now: LocalDateTime,
        now_ts: f64,
    ) -> bool {
        self.reserve_charging_pause_reason = Some(reason);
        let hold = pause_soc(current_soc, target_soc, self.config.policy.summer_min_soc);
        let min_soc_write_may_have_applied = matches!(
            reason,
            ReserveChargingPauseReason::MinimumSocWriteFailed
                | ReserveChargingPauseReason::MinimumSocReadbackFailed
        );
        let should_hold = current_setting > hold + self.config.policy.min_soc_epsilon
            || min_soc_write_may_have_applied;
        let wrote = should_hold && self.set_min_soc(hold, now, now_ts).is_ok();
        let changed = self.set_charge_state(true, true);
        if wrote || changed {
            self.save_state(now, false);
        }
        wrote || changed
    }

    fn record_charge_current_limit_unavailable(&mut self, context: &ChargeContext) {
        if context.battery_max_current_a.is_none() {
            self.charge_current_limit_unavailable_reasons
                .push(ChargeCurrentLimitUnavailableReason::ActiveBmsLimitUnavailable);
        }
        if context.vebus_max_charge_current_a.is_none() {
            self.charge_current_limit_unavailable_reasons
                .push(ChargeCurrentLimitUnavailableReason::ActiveVebusLimitUnavailable);
        }
        if context.battery_voltage_v.is_none() && self.config.policy.safe_charge_current_a.is_none()
        {
            self.charge_current_limit_unavailable_reasons
                .push(ChargeCurrentLimitUnavailableReason::BatteryVoltageUnavailable);
        }
        if self.charge_current_limit_unavailable_reasons.is_empty() {
            self.charge_current_limit_unavailable_reasons
                .push(ChargeCurrentLimitUnavailableReason::LimitCalculationUnavailable);
        }
    }

    const fn set_charge_state(&mut self, active: bool, paused: bool) -> bool {
        let changed =
            self.state.charging_mode_active != active || self.state.charging_paused != paused;
        self.state.charging_mode_active = active;
        self.state.charging_paused = paused;
        changed
    }

    fn set_min_soc(
        &mut self,
        target: f64,
        now: LocalDateTime,
        now_ts: f64,
    ) -> Result<(), ReserveChargingPauseReason> {
        self.requested_min_soc = Some(target);
        let Some(current) = self
            .raw(&self.config.settings_service.clone(), MIN_SOC_PATH)
            .filter(|value| value.is_finite() && (MIN_VALID_SOC..=MAX_VALID_SOC).contains(value))
        else {
            return Err(ReserveChargingPauseReason::MinimumSocReadbackFailed);
        };
        self.reconcile_minimum_soc_control(current, now);
        if same_minimum_soc(current, target, self.config.policy.min_soc_epsilon) {
            self.cancel_obsolete_minimum_soc_write(current, now);
            self.state.min_soc_last_seen = Some(current);
            return Ok(());
        }
        if self.config.shadow {
            let outcome = self.write_setting(
                &self.config.settings_service.clone(),
                MIN_SOC_PATH,
                SettingValue::Float(target),
            );
            debug_assert_eq!(outcome, SettingWriteOutcome::Preview);
            return Ok(());
        }

        let previous_control = self.state.minimum_soc_control.clone();
        let pending_matches = self
            .state
            .minimum_soc_control
            .pending_write
            .is_some_and(|pending| {
                pending.kind == MinimumSocWriteKind::Apply
                    && same_minimum_soc(
                        pending.expected_before,
                        current,
                        self.config.policy.min_soc_epsilon,
                    )
                    && same_minimum_soc(
                        pending.intended,
                        target,
                        self.config.policy.min_soc_epsilon,
                    )
            });
        if !pending_matches {
            prepare_minimum_soc_write(
                &mut self.state.minimum_soc_control,
                current,
                target,
                MinimumSocWriteKind::Apply,
            );
        }
        match self.persist_gui_restore_state(now) {
            DurablePersistence::Durable => {}
            DurablePersistence::Pending => {
                return Err(ReserveChargingPauseReason::MinimumSocOwnershipUnavailable);
            }
            DurablePersistence::Rejected => {
                self.state.minimum_soc_control = previous_control;
                self.save_state(now, false);
                return Err(ReserveChargingPauseReason::MinimumSocOwnershipUnavailable);
            }
        }
        match self.write_setting(
            &self.config.settings_service.clone(),
            MIN_SOC_PATH,
            SettingValue::Float(target),
        ) {
            SettingWriteOutcome::Applied => {}
            SettingWriteOutcome::Preview => return Ok(()),
            SettingWriteOutcome::Failed => {
                return Err(ReserveChargingPauseReason::MinimumSocWriteFailed);
            }
        }
        self.wrote_setting = true;
        let confirmed = self
            .raw(&self.config.settings_service.clone(), MIN_SOC_PATH)
            .filter(|value| {
                value.is_finite() && (value - target).abs() <= self.config.policy.min_soc_epsilon
            });
        let Some(confirmed) = confirmed else {
            return Err(ReserveChargingPauseReason::MinimumSocReadbackFailed);
        };
        commit_minimum_soc_write(&mut self.state.minimum_soc_control);
        self.state.minimum_soc_control.last_set = Some(confirmed);
        self.state.min_soc_last_script_set = Some(confirmed);
        self.state.min_soc_last_script_set_ts = now_ts;
        self.state.min_soc_last_seen = Some(confirmed);
        if self.persist_gui_restore_state(now) != DurablePersistence::Durable {
            self.logger
                .log("Confirmed MinimumSocLimit ownership is pending durable persistence");
        }
        Ok(())
    }

    fn reconcile_minimum_soc_control(&mut self, current: f64, now: LocalDateTime) {
        if self.config.shadow {
            return;
        }
        let reconciliation = reconcile_minimum_soc_write(
            &self.state.minimum_soc_control,
            current,
            self.config.policy.min_soc_epsilon,
        );
        match reconciliation {
            MinimumSocReconciliation::Stable | MinimumSocReconciliation::Retry(_) => {}
            MinimumSocReconciliation::Applied(kind) => {
                commit_minimum_soc_write(&mut self.state.minimum_soc_control);
                if kind == MinimumSocWriteKind::Apply {
                    self.state.minimum_soc_control.last_set = Some(current);
                    self.state.min_soc_last_script_set = Some(current);
                    self.state.min_soc_last_seen = Some(current);
                }
                self.logger
                    .log("Recovered a confirmed MinimumSocLimit write");
                if self.persist_gui_restore_state(now) != DurablePersistence::Durable {
                    self.logger
                        .log("Recovered MinimumSocLimit state is pending durable persistence");
                }
            }
            MinimumSocReconciliation::ExternalChange => {
                release_minimum_soc_ownership(&mut self.state.minimum_soc_control);
                self.logger.log(
                    "MinimumSocLimit changed externally; the external value remains untouched",
                );
                if self.persist_gui_restore_state(now) != DurablePersistence::Durable {
                    self.logger
                        .log("MinimumSocLimit ownership release is pending durable persistence");
                }
            }
        }
    }

    fn cancel_obsolete_minimum_soc_write(&mut self, current: f64, now: LocalDateTime) {
        let Some(pending) = self.state.minimum_soc_control.pending_write else {
            return;
        };
        if !same_minimum_soc(
            current,
            pending.expected_before,
            self.config.policy.min_soc_epsilon,
        ) {
            return;
        }
        if self.state.minimum_soc_control.last_set.is_none() {
            release_minimum_soc_ownership(&mut self.state.minimum_soc_control);
        } else {
            self.state.minimum_soc_control.pending_write = None;
        }
        if self.persist_gui_restore_state(now) != DurablePersistence::Durable {
            self.logger
                .log("Cancelled MinimumSocLimit intent is pending durable persistence");
        }
    }

    fn pv_power(&mut self) -> PvPower {
        self.pv_observation().power
    }

    fn pv_observation(&mut self) -> PvObservation {
        if let Some(observation) = self.cycle_pv_observation {
            return observation;
        }
        let service = self.config.system_service.clone();
        let mut ac_w = 0.0;
        let mut valid_mask = 0_u8;
        let mut channel = 0_u8;
        for phase in PHASES {
            ac_w += self.pv_channel(
                &service,
                &AC_PV_ON_GRID_POWER_PATH.replace(PHASE_PLACEHOLDER, phase),
                channel,
                &mut valid_mask,
            );
            channel += 1;
            ac_w += self.pv_channel(
                &service,
                &AC_PV_ON_OUTPUT_POWER_PATH.replace(PHASE_PLACEHOLDER, phase),
                channel,
                &mut valid_mask,
            );
            channel += 1;
        }
        let dc_w = self.pv_channel(&service, DC_PV_POWER_PATH, channel, &mut valid_mask);
        let previous_expected = self.state.pv_expected_channels & PV_CHANNEL_MASK;
        let expected_mask = previous_expected | valid_mask;
        let topology_changed = expected_mask != previous_expected;
        self.state.pv_expected_channels = expected_mask;
        let observation = PvObservation {
            power: PvPower { ac_w, dc_w },
            expected_mask,
            valid_mask,
            complete: expected_mask != 0
                && self.dbus.cycle_fault().is_none()
                && expected_mask & !valid_mask == 0,
            topology_changed,
        };
        self.cycle_pv_observation = Some(observation);
        observation
    }

    fn pv_channel(&mut self, service: &str, path: &str, channel: u8, valid_mask: &mut u8) -> f64 {
        let Some(value) = self.measurement_optional(service, path) else {
            return 0.0;
        };
        if value < 0.0 {
            let error = PortError::classified(
                DbusFailureKind::TypeMismatch,
                "PV measurement validation",
                "negative PV power",
            );
            self.record_dbus_read_error(service, path, &error);
            return 0.0;
        }
        *valid_mask |= 1_u8 << channel;
        value
    }

    fn grid_power_net(&mut self) -> Option<f64> {
        self.sum_phases(AC_GRID_POWER_PATH, AC_GRID_PHASE_COUNT_PATH)
    }

    fn house_load(&mut self, grid_net: Option<f64>, battery_power: Option<f64>) -> Option<f64> {
        if let Some(load) = self.sum_phases(
            AC_CONSUMPTION_ON_INPUT,
            AC_CONSUMPTION_ON_INPUT_PHASE_COUNT_PATH,
        ) {
            return Some(load);
        }
        if let Some(load) = self.sum_phases(AC_CONSUMPTION, AC_CONSUMPTION_PHASE_COUNT_PATH) {
            return Some(load);
        }

        let initial_battery_power = battery_power?;
        let current_battery_power =
            self.measurement_optional(&self.config.system_service.clone(), BATTERY_POWER_PATH)?;
        if (initial_battery_power < 0.0) != (current_battery_power < 0.0) {
            return None;
        }
        Some(house_load_fallback(grid_net?, current_battery_power))
    }

    fn sum_phases(&mut self, template: &str, phase_count_path: &str) -> Option<f64> {
        let service = self.config.system_service.clone();
        let phase_count = match self.measurement_optional(&service, phase_count_path) {
            Some(1.0) => 1,
            Some(2.0) => 2,
            Some(3.0) | None => 3,
            Some(_) => return None,
        };
        let mut total = 0.0;
        for phase in PHASES.iter().take(phase_count) {
            total +=
                self.measurement_optional(&service, &template.replace(PHASE_PLACEHOLDER, phase))?;
        }
        Some(total)
    }

    fn battery_max_charge_current(&mut self, now: LocalDateTime, now_ts: f64) -> Option<f64> {
        let service = self.active_bms_service(now)?;
        if let Some(value) = self
            .measurement_optional(&service, BMS_MAX_CHARGE_CURRENT_PATH)
            .filter(|value| *value >= 0.0)
        {
            self.state.battery_max_current_last = Some(value);
            self.state.battery_max_current_last_seen_ts = now_ts;
            return Some(value);
        }
        self.fresh_battery_max_current(now_ts)
    }

    fn active_bms_service(&mut self, now: LocalDateTime) -> Option<String> {
        if let ServiceResolution::Resolved(selected) = &self.cycle_topology.bms_service {
            return selected.clone();
        }
        let system_service = self.config.system_service.clone();
        let configured_fallback = self.config.fallback_battery_service.clone();
        let selected = match self.dbus.text(&system_service, ACTIVE_BMS_SERVICE_PATH) {
            Ok(value) => selected_service(value, "com.victronenergy.battery")
                .or_else(|| configured_fallback.and_then(valid_battery_service)),
            Err(error) => {
                self.record_dbus_read_error(&system_service, ACTIVE_BMS_SERVICE_PATH, &error);
                if self.dbus.cycle_fault().is_none() {
                    configured_fallback.and_then(valid_battery_service)
                } else {
                    self.cycle_topology.bms_service = ServiceResolution::Resolved(None);
                    return None;
                }
            }
        };
        self.cycle_topology.bms_service = ServiceResolution::Resolved(selected.clone());
        if selected != self.state.battery_service {
            self.state.battery_service.clone_from(&selected);
            self.state.battery_max_current_last = None;
            self.state.battery_max_current_last_seen_ts = 0.0;
            self.logger.log(&format!(
                "Active BMS service changed to {}",
                selected.as_deref().unwrap_or("none")
            ));
            self.save_state(now, false);
        }
        selected
    }

    fn fresh_battery_max_current(&self, now_ts: f64) -> Option<f64> {
        let age = now_ts - self.state.battery_max_current_last_seen_ts;
        (self.state.battery_max_current_last_seen_ts > 0.0
            && age >= 0.0
            && age <= self.config.battery_max_current_max_age.as_secs_f64())
        .then_some(self.state.battery_max_current_last)
        .flatten()
        .filter(|value| *value >= 0.0)
    }

    fn normal_charge_current(&self, battery_max: Option<f64>) -> Option<f64> {
        [
            self.state
                .charge_current_control
                .external_baseline_a
                .filter(|value| *value >= 0.0),
            self.config.policy.normal_charge_current_a,
            battery_max,
        ]
        .into_iter()
        .flatten()
        .find(|value| value.is_finite() && *value >= 0.0)
    }

    fn set_max_charge_current(
        &mut self,
        desired: f64,
        reason: CurrentReason,
        now: LocalDateTime,
        now_ts: f64,
    ) -> Result<(), ReserveChargingPauseReason> {
        if desired != 0.0 && self.cycle_charge_constraints.inhibited() {
            return Err(ReserveChargingPauseReason::ExplicitChargeInhibit);
        }
        if matches!(reason, CurrentReason::ChargeLimit)
            && self.state.charge_current_control.routine_ceiling_requested
        {
            return Err(ReserveChargingPauseReason::ChargeCurrentControlConflict);
        }
        match reason {
            CurrentReason::ChargeLimit => {
                let desired = normalized_charge_current_setting(desired);
                if desired < 0.0 {
                    return Err(ReserveChargingPauseReason::ChargeCurrentOwnershipUnavailable);
                }
                if desired == 0.0 && self.cycle_charge_constraints.inhibited() {
                    set_explicit_inhibit_requested(&mut self.state.charge_current_control, true);
                } else {
                    set_explicit_inhibit_requested(&mut self.state.charge_current_control, false);
                    let previous = self.state.charge_current_control.reserve_constraint_a;
                    let increase_is_rate_limited = previous.is_some_and(|previous| {
                        desired > previous
                            && now_ts - self.last_charge_limit_set_ts
                                < self.config.policy.charge_limit_min_update_interval_seconds
                    });
                    if !increase_is_rate_limited {
                        set_reserve_constraint(
                            &mut self.state.charge_current_control,
                            Some(desired),
                        );
                    }
                }
            }
            CurrentReason::Restore => {
                set_explicit_inhibit_requested(&mut self.state.charge_current_control, false);
                set_reserve_constraint(&mut self.state.charge_current_control, None);
                set_configured_constraint(
                    &mut self.state.charge_current_control,
                    self.config
                        .policy
                        .normal_charge_current_a
                        .filter(|value| *value != 0.0)
                        .map(normalized_charge_current_setting),
                );
                if self.state.charge_current_control.routine_ceiling_requested {
                    return Ok(());
                }
            }
        }
        self.actuate_charge_current(ChargeCurrentRequestOrigin::Reserve, now, now_ts)
            .map_err(map_charge_current_failure)
    }

    fn restore_normal_charge_current(
        &mut self,
        _battery_max: Option<f64>,
        now: LocalDateTime,
        now_ts: f64,
    ) -> Result<(), ReserveChargingPauseReason> {
        self.set_max_charge_current(-1.0, CurrentReason::Restore, now, now_ts)
    }

    #[allow(clippy::cast_possible_truncation)]
    fn write_charge_current_value(&mut self, value: f64) -> SettingWriteOutcome {
        let integer = if value < 0.0 {
            -1
        } else {
            let rounded = value.round_ties_even();
            if rounded < f64::from(i32::MIN) || rounded > f64::from(i32::MAX) {
                return SettingWriteOutcome::Failed;
            }
            rounded as i32
        };
        let service = self.config.settings_service.clone();
        let outcome = self.write_setting(
            &service,
            MAX_CHARGE_CURRENT_PATH,
            SettingValue::Integer(integer),
        );
        if outcome == SettingWriteOutcome::Applied {
            self.wrote_setting = true;
        }
        outcome
    }

    fn write_discharge_power(&mut self, value: f64) -> SettingWriteOutcome {
        self.requested_max_discharge_power = Some(value);
        let service = self.config.settings_service.clone();
        let outcome = self.write_setting(
            &service,
            MAX_DISCHARGE_POWER_PATH,
            SettingValue::Float(value),
        );
        if outcome == SettingWriteOutcome::Applied {
            self.wrote_setting = true;
        }
        outcome
    }

    fn discharge_write_readback_confirmed(&mut self, nominal_power_w: Option<f64>) -> bool {
        if self.config.shadow {
            return true;
        }
        let current = self.raw(
            &self.config.settings_service.clone(),
            MAX_DISCHARGE_POWER_PATH,
        );
        matches!(
            reconcile_pending_discharge_write(
                &self.state.discharge_protection,
                current,
                nominal_power_w,
                self.config.policy.discharge_protection_nominal_fraction,
                self.config.policy.discharge_power_epsilon_w,
            ),
            Some(DischargePendingWriteResolution::Applied(_))
        )
    }

    fn maybe_log_status(
        &mut self,
        house_load: Option<f64>,
        battery_max: Option<f64>,
        vebus_max_charge_current: Option<f64>,
        now: LocalDateTime,
        now_ts: f64,
    ) {
        if now_ts - self.state.last_status_log_ts < self.config.status_log_interval.as_secs_f64() {
            return;
        }
        let current = self.raw(
            &self.config.settings_service.clone(),
            MAX_CHARGE_CURRENT_PATH,
        );
        self.logger.log(&format!(
            "Status: HouseLoad {}, BMS Max {}, VE.Bus Max {}, MaxChargeCurrent {}, {}",
            format_watts(house_load),
            format_amps(battery_max),
            format_amps(vebus_max_charge_current),
            format_amps(current),
            self.store.status(self.clock.epoch_seconds())
        ));
        self.state.last_status_log_ts = now_ts;
        self.save_state(now, false);
    }

    fn save_state(&mut self, now: LocalDateTime, force_sd: bool) {
        let now_ts = self.clock.epoch_seconds();
        if let Err(error) = self.store.save(&mut self.state, now, now_ts, force_sd) {
            self.logger.log(&format!("Could not save state: {error}"));
        }
    }

    fn persist_gui_restore_state(&mut self, now: LocalDateTime) -> DurablePersistence {
        let now_ts = self.clock.epoch_seconds();
        if let Err(error) = self.store.save(&mut self.state, now, now_ts, true) {
            self.logger.log(&format!(
                "Could not persist temporary GUI restore state: {error}"
            ));
            return DurablePersistence::Rejected;
        }
        if self.store.flush(Duration::from_secs(5)) {
            return DurablePersistence::Durable;
        }
        self.logger
            .log("Timed out while persisting temporary GUI restore state");
        DurablePersistence::Pending
    }

    fn raw(&mut self, service: &str, path: &str) -> Option<f64> {
        match self.dbus.raw_number(service, path) {
            Ok(value) => value,
            Err(error) => {
                self.record_dbus_read_error(service, path, &error);
                None
            }
        }
    }

    fn measurement_optional(&mut self, service: &str, path: &str) -> Option<f64> {
        match self.dbus.measurement(service, path) {
            Ok(value) => value,
            Err(error) => {
                self.record_dbus_read_error(service, path, &error);
                None
            }
        }
    }

    fn record_dbus_read_error(&mut self, service: &str, path: &str, error: &PortError) {
        let kind = error.kind();
        let operation = error.operation();
        let cycle_wide = matches!(kind, DbusFailureKind::Timeout | DbusFailureKind::Transport);
        if let Some(issue) = self.dbus_read_issues.iter_mut().find(|issue| {
            issue.kind == kind
                && issue.operation == operation
                && (cycle_wide || (issue.service == service && issue.path == path))
        }) {
            issue.occurrences = issue.occurrences.saturating_add(1);
            return;
        }
        if self.dbus_read_issues.len() == MAX_DBUS_READ_ISSUES {
            self.dbus_read_issue_overflow = self.dbus_read_issue_overflow.saturating_add(1);
            return;
        }
        self.dbus_read_issues.push(DbusReadIssue {
            kind,
            operation: operation.to_owned(),
            service: service.to_owned(),
            path: path.to_owned(),
            occurrences: 1,
        });
    }

    /// Sole actuator boundary for mutable Venus settings.
    fn write_setting(
        &mut self,
        service: &str,
        path: &str,
        value: SettingValue,
    ) -> SettingWriteOutcome {
        if self.config.shadow {
            self.logger
                .log(&format!("Shadow: would set {path} to {}", value.as_f64()));
            return SettingWriteOutcome::Preview;
        }
        let result = match value {
            SettingValue::Float(value) => self.dbus.write_float(service, path, value),
            SettingValue::Integer(value) => self.dbus.write_integer(service, path, value),
        };
        match result {
            Ok(()) => SettingWriteOutcome::Applied,
            Err(error) => {
                self.logger
                    .log(&format!("Error while setting {path}: {error}"));
                SettingWriteOutcome::Failed
            }
        }
    }
}

fn normalized_charge_current_setting(value: f64) -> f64 {
    if value < 0.0 {
        -1.0
    } else {
        value.round_ties_even()
    }
}

fn same_discharge_power(left: f64, right: f64, epsilon_w: f64) -> bool {
    left.is_finite()
        && right.is_finite()
        && epsilon_w.is_finite()
        && epsilon_w >= 0.0
        && (left - right).abs() <= epsilon_w
}

fn valid_charge_current_setting(value: f64) -> bool {
    value.is_finite() && (-1.0..=MAX_ACCEPTED_CHARGE_CURRENT_A).contains(&value)
}

fn selected_service(value: Option<String>, prefix: &str) -> Option<String> {
    value.filter(|service| {
        service
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('.'))
    })
}

fn valid_battery_service(service: String) -> Option<String> {
    selected_service(Some(service), "com.victronenergy.battery")
}

const fn charge_current_action_label(action: ChargeCurrentAction) -> &'static str {
    match action {
        ChargeCurrentAction::Apply(_) => "set",
        ChargeCurrentAction::Restore(_) => "restore",
    }
}

const fn charge_current_origin_label(origin: ChargeCurrentRequestOrigin) -> &'static str {
    match origin {
        ChargeCurrentRequestOrigin::RoutineCeiling => "routine SoC ceiling",
        ChargeCurrentRequestOrigin::Reserve => "reserve charge-current control",
        ChargeCurrentRequestOrigin::Shutdown => "shutdown restore",
    }
}

const fn map_charge_current_failure(
    failure: ChargeCurrentControlFailure,
) -> ReserveChargingPauseReason {
    match failure {
        ChargeCurrentControlFailure::CurrentRead => {
            ReserveChargingPauseReason::ChargeCurrentReadFailed
        }
        ChargeCurrentControlFailure::DurableState
        | ChargeCurrentControlFailure::ExternalControl => {
            ReserveChargingPauseReason::ChargeCurrentOwnershipUnavailable
        }
        ChargeCurrentControlFailure::DbusWrite => {
            ReserveChargingPauseReason::ChargeCurrentWriteFailed
        }
        ChargeCurrentControlFailure::Readback => {
            ReserveChargingPauseReason::ChargeCurrentReadbackFailed
        }
    }
}

const fn charge_current_write_kind_label(kind: ChargeCurrentWriteKind) -> &'static str {
    match kind {
        ChargeCurrentWriteKind::Restrict => "restriction",
        ChargeCurrentWriteKind::Restore => "restoration",
    }
}

fn clamp_power_to_nominal(value: &mut Option<f64>, nominal_power_w: f64) {
    if let Some(power) = value.as_mut().filter(|power| **power >= 0.0) {
        *power = power.min(nominal_power_w);
    }
}

fn format_amps(value: Option<f64>) -> String {
    value.map_or_else(|| "n/a".to_owned(), |value| format!("{value:.1}A"))
}

fn format_watts(value: Option<f64>) -> String {
    value.map_or_else(|| "unknown".to_owned(), |watts| format!("{watts:.0}W"))
}

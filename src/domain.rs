//! Typed controller state compatible with the established Python JSON schema.

use serde::{Deserialize, Serialize};

pub(crate) const PV_CHANNEL_MASK: u8 = 0x7f;

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct ControllerState {
    pub pv_history: Vec<f64>,
    pub pv_expected_channels: u8,
    pub last_balance_ts: f64,
    pub last_balance_attempt_ts: f64,
    pub balancing_active: bool,
    pub balancing_start_ts: f64,
    pub balancing_high_soc_start_ts: f64,
    pub balance_full_seconds: f64,
    pub full_soc_seconds: f64,
    pub last_full_ts: f64,
    pub charging_mode_active: bool,
    pub charging_paused: bool,
    pub charge_deficit_start_ts: f64,
    pub battery_service: Option<String>,
    pub battery_max_current_last: Option<f64>,
    pub battery_max_current_last_seen_ts: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub normal_charge_current: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_charge_current_raw: Option<f64>,
    #[serde(skip_serializing_if = "is_false")]
    pub max_charge_current_raw_set: bool,
    #[serde(skip_serializing_if = "is_false")]
    pub charge_current_owned_by_script: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_charge_current_script_last_set: Option<f64>,
    #[serde(skip_serializing_if = "is_zero_u64")]
    pub reserve_charge_current_write_generation: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reserve_charge_current_pending_write: Option<PendingReserveChargeCurrentWrite>,
    pub current_day_samples: Vec<f64>,
    pub pv_energy_ws: f64,
    pub pv_time_s: f64,
    pub pv_last_sample_ts: f64,
    pub pv_last_sample_power: f64,
    pub last_pv_integral_ts: f64,
    pub last_sample_date: String,
    pub last_mode: String,
    pub last_loop_ts: f64,
    pub last_status_log_ts: f64,
    pub last_soc_invalid_log_ts: f64,
    pub last_min_soc_invalid_log_ts: f64,
    pub manual_override_until_ts: f64,
    pub min_soc_last_seen: Option<f64>,
    pub min_soc_last_script_set: Option<f64>,
    pub min_soc_last_script_set_ts: f64,
    #[serde(skip_serializing_if = "MinimumSocControlState::is_default")]
    pub minimum_soc_control: MinimumSocControlState,
    pub last_manual_override_log_ts: f64,
    pub charge_ceiling: ChargeCeilingState,
    #[serde(skip_serializing_if = "ChargeCurrentCeilingState::is_default")]
    pub charge_current_ceiling: ChargeCurrentCeilingState,
    pub charge_current_control: ChargeCurrentControlState,
    pub discharge_protection: DischargeProtectionState,
    pub vebus_service: Option<String>,
    pub nominal_inverter_power_last: Option<f64>,
    pub nominal_inverter_power_service: Option<String>,
    pub nominal_inverter_power_observed_at: f64,
    pub nominal_inverter_power_configured: bool,
    #[serde(skip)]
    pub nominal_inverter_power_last_seen_monotonic: Option<f64>,
    pub ts: f64,
    pub boot_ts: f64,
}

impl Default for ControllerState {
    fn default() -> Self {
        Self {
            pv_history: Vec::new(),
            pv_expected_channels: 0,
            last_balance_ts: 0.0,
            last_balance_attempt_ts: 0.0,
            balancing_active: false,
            balancing_start_ts: 0.0,
            balancing_high_soc_start_ts: 0.0,
            balance_full_seconds: 0.0,
            full_soc_seconds: 0.0,
            last_full_ts: 0.0,
            charging_mode_active: false,
            charging_paused: false,
            charge_deficit_start_ts: 0.0,
            battery_service: None,
            battery_max_current_last: None,
            battery_max_current_last_seen_ts: 0.0,
            normal_charge_current: None,
            max_charge_current_raw: None,
            max_charge_current_raw_set: false,
            charge_current_owned_by_script: false,
            max_charge_current_script_last_set: None,
            reserve_charge_current_write_generation: 0,
            reserve_charge_current_pending_write: None,
            current_day_samples: Vec::new(),
            pv_energy_ws: 0.0,
            pv_time_s: 0.0,
            pv_last_sample_ts: 0.0,
            pv_last_sample_power: 0.0,
            last_pv_integral_ts: 0.0,
            last_sample_date: String::new(),
            last_mode: String::new(),
            last_loop_ts: 0.0,
            last_status_log_ts: 0.0,
            last_soc_invalid_log_ts: 0.0,
            last_min_soc_invalid_log_ts: 0.0,
            manual_override_until_ts: 0.0,
            min_soc_last_seen: None,
            min_soc_last_script_set: None,
            min_soc_last_script_set_ts: 0.0,
            minimum_soc_control: MinimumSocControlState::default(),
            last_manual_override_log_ts: 0.0,
            charge_ceiling: ChargeCeilingState::default(),
            charge_current_ceiling: ChargeCurrentCeilingState::default(),
            charge_current_control: ChargeCurrentControlState::default(),
            discharge_protection: DischargeProtectionState::default(),
            vebus_service: None,
            nominal_inverter_power_last: None,
            nominal_inverter_power_service: None,
            nominal_inverter_power_observed_at: 0.0,
            nominal_inverter_power_configured: false,
            nominal_inverter_power_last_seen_monotonic: None,
            ts: 0.0,
            boot_ts: 0.0,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct MinimumSocControlState {
    pub owned: bool,
    pub external_baseline: Option<f64>,
    pub last_set: Option<f64>,
    pub write_generation: u64,
    pub pending_write: Option<PendingMinimumSocWrite>,
}

impl MinimumSocControlState {
    pub(crate) const fn is_default(&self) -> bool {
        !self.owned
            && self.external_baseline.is_none()
            && self.last_set.is_none()
            && self.write_generation == 0
            && self.pending_write.is_none()
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MinimumSocWriteKind {
    Apply,
    Restore,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub struct PendingMinimumSocWrite {
    pub generation: u64,
    pub kind: MinimumSocWriteKind,
    pub expected_before: f64,
    pub intended: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ChargeCeilingState {
    pub reference_day: Option<i32>,
    pub observed_day: Option<i32>,
    pub near_full_latched: bool,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct ChargeCurrentCeilingState {
    pub active: bool,
    pub external_control_latched: bool,
    pub restore_current_a: Option<f64>,
    pub last_set_current_a: Option<f64>,
    pub write_generation: u64,
    pub pending_write: Option<PendingChargeCurrentWrite>,
}

impl ChargeCurrentCeilingState {
    const fn is_default(&self) -> bool {
        !self.active
            && !self.external_control_latched
            && self.restore_current_a.is_none()
            && self.last_set_current_a.is_none()
            && self.write_generation == 0
            && self.pending_write.is_none()
    }
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct ChargeCurrentControlState {
    pub external_baseline_a: Option<f64>,
    pub configured_constraint_a: Option<f64>,
    pub reserve_constraint_a: Option<f64>,
    pub routine_ceiling_requested: bool,
    pub explicit_inhibit_requested: bool,
    pub routine_external_control_latched: bool,
    pub owned: bool,
    pub last_effectively_written_a: Option<f64>,
    pub write_generation: u64,
    pub pending_write: Option<PendingChargeCurrentWrite>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChargeCurrentWriteKind {
    Restrict,
    Restore,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub struct PendingChargeCurrentWrite {
    pub generation: u64,
    pub kind: ChargeCurrentWriteKind,
    pub expected_before_a: f64,
    pub intended_a: f64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReserveChargeCurrentWriteKind {
    Limit,
    Restore,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub struct PendingReserveChargeCurrentWrite {
    pub generation: u64,
    pub kind: ReserveChargeCurrentWriteKind,
    pub expected_before_a: f64,
    pub intended_a: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct DischargeProtectionState {
    pub active: bool,
    pub recharge_seen: bool,
    #[serde(skip)]
    pub recharge_candidate_since_ts: Option<f64>,
    #[serde(skip)]
    pub recharge_candidate_last_sample_ts: Option<f64>,
    pub restore_power_w: Option<f64>,
    #[serde(alias = "restore_to_nominal")]
    pub restore_default: bool,
    pub last_set_power_w: Option<f64>,
    pub last_observed_power_w: Option<f64>,
    pub write_generation: u64,
    pub pending_write: Option<PendingDischargeWrite>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DischargeWriteKind {
    Restrict,
    Restore,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub struct PendingDischargeWrite {
    pub generation: u64,
    pub kind: DischargeWriteKind,
    pub expected_before_w: f64,
    pub intended_w: f64,
}

impl ControllerState {
    #[must_use]
    pub const fn charge_control_active(&self) -> bool {
        self.charging_mode_active || self.charging_paused
    }

    /// Fold both historical charge-current owners into the single arbiter.
    ///
    /// # Errors
    ///
    /// Returns an error when a current-format state is mixed with non-default
    /// legacy ownership, because that combination has no unambiguous authority.
    pub fn migrate_legacy_charge_current_state(&mut self) -> Result<bool, String> {
        let reserve_legacy = self.max_charge_current_raw_set
            || self.charge_current_owned_by_script
            || self.reserve_charge_current_pending_write.is_some();
        let ceiling_legacy = !self.charge_current_ceiling.is_default();
        if !reserve_legacy && !ceiling_legacy {
            return Ok(false);
        }
        if self.charge_current_control != ChargeCurrentControlState::default() {
            return Err("new and legacy charge-current ownership overlap".to_owned());
        }

        let reserve_baseline = self
            .max_charge_current_raw_set
            .then_some(self.max_charge_current_raw.unwrap_or(-1.0));
        let routine_pending = self.charge_current_ceiling.pending_write;
        let reserve_pending = self.reserve_charge_current_pending_write;
        let routine_requested = routine_pending
            .map_or(self.charge_current_ceiling.active, |write| {
                matches!(write.kind, ChargeCurrentWriteKind::Restrict)
            });
        let reserve_constraint_a = reserve_pending.map_or_else(
            || {
                self.charge_current_owned_by_script
                    .then_some(self.max_charge_current_script_last_set)
                    .flatten()
            },
            |write| {
                matches!(write.kind, ReserveChargeCurrentWriteKind::Limit)
                    .then_some(write.intended_a)
            },
        );
        let external_baseline_a =
            reserve_baseline.or(self.charge_current_ceiling.restore_current_a);
        let selected_pending = routine_pending.map_or_else(
            || {
                reserve_pending.map(|write| PendingChargeCurrentWrite {
                    generation: write.generation,
                    kind: match write.kind {
                        ReserveChargeCurrentWriteKind::Limit => ChargeCurrentWriteKind::Restrict,
                        ReserveChargeCurrentWriteKind::Restore => ChargeCurrentWriteKind::Restore,
                    },
                    expected_before_a: write.expected_before_a,
                    intended_a: write.intended_a,
                })
            },
            Some,
        );
        let last_effectively_written_a = if self.charge_current_ceiling.active {
            self.charge_current_ceiling.last_set_current_a.or_else(|| {
                self.charge_current_owned_by_script
                    .then_some(self.max_charge_current_script_last_set)
                    .flatten()
            })
        } else {
            self.charge_current_owned_by_script
                .then_some(self.max_charge_current_script_last_set)
                .flatten()
        };
        let owned = self.charge_current_ceiling.active
            || self.charge_current_owned_by_script
            || selected_pending.is_some();
        let mut control = ChargeCurrentControlState {
            external_baseline_a,
            configured_constraint_a: None,
            reserve_constraint_a,
            routine_ceiling_requested: routine_requested,
            explicit_inhibit_requested: false,
            routine_external_control_latched: self.charge_current_ceiling.external_control_latched,
            owned,
            last_effectively_written_a,
            write_generation: self
                .charge_current_ceiling
                .write_generation
                .max(self.reserve_charge_current_write_generation),
            pending_write: selected_pending,
        };
        if let Some(mut pending) = control.pending_write {
            let has_constraint = control.routine_ceiling_requested
                || control.explicit_inhibit_requested
                || control.reserve_constraint_a.is_some();
            pending.kind = if has_constraint {
                ChargeCurrentWriteKind::Restrict
            } else {
                ChargeCurrentWriteKind::Restore
            };
            control.pending_write = Some(pending);
        }
        self.charge_current_control = control;
        self.normal_charge_current = None;
        self.max_charge_current_raw = None;
        self.max_charge_current_raw_set = false;
        self.charge_current_owned_by_script = false;
        self.max_charge_current_script_last_set = None;
        self.reserve_charge_current_write_generation = 0;
        self.reserve_charge_current_pending_write = None;
        self.charge_current_ceiling = ChargeCurrentCeilingState::default();
        Ok(true)
    }

    pub fn reset_runtime_for_boot(&mut self, monotonic_now: f64) {
        self.boot_ts = monotonic_now;
        self.discharge_protection.recharge_candidate_since_ts = None;
        self.discharge_protection.recharge_candidate_last_sample_ts = None;
        self.nominal_inverter_power_last_seen_monotonic = None;
        let invalid_balancing_timeline = self.balancing_start_ts > monotonic_now
            || self.balancing_high_soc_start_ts > monotonic_now
            || (self.balancing_high_soc_start_ts > 0.0
                && self.balancing_high_soc_start_ts < self.balancing_start_ts);
        if !self.balancing_active || invalid_balancing_timeline {
            self.balancing_active = false;
            self.balancing_start_ts = 0.0;
            self.balancing_high_soc_start_ts = 0.0;
            self.balance_full_seconds = 0.0;
        } else if self.balancing_high_soc_start_ts <= 0.0 {
            // Older state documents have no high-SoC phase timestamp. Their
            // partial hold duration cannot be proven and is restarted safely.
            self.balance_full_seconds = 0.0;
        }
        for timestamp in [
            &mut self.last_balance_attempt_ts,
            &mut self.charge_deficit_start_ts,
            &mut self.pv_last_sample_ts,
            &mut self.last_pv_integral_ts,
            &mut self.last_loop_ts,
            &mut self.last_status_log_ts,
            &mut self.last_soc_invalid_log_ts,
            &mut self.last_min_soc_invalid_log_ts,
            &mut self.min_soc_last_script_set_ts,
            &mut self.last_manual_override_log_ts,
            &mut self.battery_max_current_last_seen_ts,
        ] {
            if *timestamp > monotonic_now {
                *timestamp = 0.0;
            }
        }
        if self.manual_override_until_ts > monotonic_now + 604_800.0 {
            self.manual_override_until_ts = 0.0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlapping_legacy_owners_migrate_to_one_original_baseline() {
        let mut state = ControllerState {
            max_charge_current_raw: None,
            max_charge_current_raw_set: true,
            charge_current_owned_by_script: true,
            max_charge_current_script_last_set: Some(14.0),
            reserve_charge_current_write_generation: 3,
            charge_current_ceiling: ChargeCurrentCeilingState {
                active: true,
                restore_current_a: Some(14.0),
                last_set_current_a: Some(0.0),
                write_generation: 4,
                ..ChargeCurrentCeilingState::default()
            },
            ..ControllerState::default()
        };

        assert_eq!(state.migrate_legacy_charge_current_state(), Ok(true));

        assert_eq!(state.charge_current_control.external_baseline_a, Some(-1.0));
        assert_eq!(
            state.charge_current_control.reserve_constraint_a,
            Some(14.0)
        );
        assert!(state.charge_current_control.routine_ceiling_requested);
        assert!(state.charge_current_control.owned);
        assert_eq!(
            state.charge_current_control.last_effectively_written_a,
            Some(0.0)
        );
        assert_eq!(state.charge_current_control.write_generation, 4);
        assert_eq!(
            state.charge_current_ceiling,
            ChargeCurrentCeilingState::default()
        );
        assert!(!state.max_charge_current_raw_set);
        assert!(!state.charge_current_owned_by_script);
    }

    #[test]
    fn mixed_legacy_and_unified_ownership_is_rejected() {
        let mut state = ControllerState {
            max_charge_current_raw_set: true,
            charge_current_control: ChargeCurrentControlState {
                reserve_constraint_a: Some(10.0),
                ..ChargeCurrentControlState::default()
            },
            ..ControllerState::default()
        };

        assert!(state.migrate_legacy_charge_current_state().is_err());
    }

    #[test]
    fn process_restart_in_the_same_boot_preserves_high_soc_progress() {
        let mut state = ControllerState {
            balancing_active: true,
            balancing_start_ts: 100.0,
            balancing_high_soc_start_ts: 200.0,
            balance_full_seconds: 3_600.0,
            ..ControllerState::default()
        };

        state.reset_runtime_for_boot(300.0);

        assert!(state.balancing_active);
        assert_eq!(state.balancing_start_ts.to_bits(), 100.0_f64.to_bits());
        assert_eq!(
            state.balancing_high_soc_start_ts.to_bits(),
            200.0_f64.to_bits()
        );
        assert_eq!(state.balance_full_seconds.to_bits(), 3_600.0_f64.to_bits());
    }

    #[test]
    fn rebooted_monotonic_clock_discards_both_balancing_phase_timers() {
        let mut state = ControllerState {
            balancing_active: true,
            balancing_start_ts: 100.0,
            balancing_high_soc_start_ts: 200.0,
            balance_full_seconds: 3_600.0,
            ..ControllerState::default()
        };

        state.reset_runtime_for_boot(10.0);

        assert!(!state.balancing_active);
        assert_eq!(state.balancing_start_ts.to_bits(), 0.0_f64.to_bits());
        assert_eq!(
            state.balancing_high_soc_start_ts.to_bits(),
            0.0_f64.to_bits()
        );
        assert_eq!(state.balance_full_seconds.to_bits(), 0.0_f64.to_bits());
    }
}

#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_false(value: &bool) -> bool {
    !*value
}

#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct CycleDecision {
    pub generated_at: f64,
    pub mode: String,
    pub target_soc: f64,
    pub current_soc: Option<f64>,
    pub current_min_soc: Option<f64>,
    pub requested_min_soc: Option<f64>,
    pub requested_max_charge_current: Option<f64>,
    pub requested_charge_ceiling_current_a: Option<f64>,
    pub requested_max_discharge_power: Option<f64>,
    pub charge_current_ceiling_active: bool,
    pub charge_current_ceiling_owned: bool,
    pub charge_current_ceiling_pending_unenforced: bool,
    pub charge_current_ceiling_unenforced_reason: Option<String>,
    pub discharge_protection_active: bool,
    pub discharge_protection_pending_unenforced: bool,
    pub discharge_protection_unenforced_reason: Option<String>,
    pub charge_ceiling_soc: Option<f64>,
    pub full_charge_due: Option<bool>,
    pub full_charge_age_days: Option<u16>,
    pub charging_inhibited: bool,
    pub charging_inhibit_reasons: Vec<ChargingInhibitReason>,
    pub user_charge_limited: Option<bool>,
    pub charge_current_limit_unavailable: bool,
    pub charge_current_limit_unavailable_reasons: Vec<ChargeCurrentLimitUnavailableReason>,
    pub reserve_charging_paused: bool,
    pub reserve_charging_pause_reason: Option<ReserveChargingPauseReason>,
    pub dbus_read_issues: Vec<DbusReadIssue>,
    pub dbus_read_issue_overflow: u16,
    pub pv_sample_valid: Option<bool>,
    pub pv_expected_channels: u32,
    pub pv_valid_channels: u32,
    pub shadow: bool,
    pub outcome: CycleOutcome,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DbusFailureKind {
    ServiceUnavailable,
    PathUnavailable,
    TypeMismatch,
    Timeout,
    Transport,
    MethodError,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct DbusReadIssue {
    pub kind: DbusFailureKind,
    pub operation: String,
    pub service: String,
    pub path: String,
    pub occurrences: u16,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChargingInhibitReason {
    ActiveBmsDisallowsCharge,
    BmsMaxChargeCurrentZero,
    SystemChargeDisabled,
    VebusDisallowsCharge,
    VebusMaxChargeCurrentZero,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChargeCurrentLimitUnavailableReason {
    ActiveBmsLimitUnavailable,
    ActiveVebusLimitUnavailable,
    BatteryVoltageUnavailable,
    LimitCalculationUnavailable,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReserveChargingPauseReason {
    ExplicitChargeInhibit,
    LoadTelemetryIncomplete,
    ChargeCurrentControlConflict,
    ChargeCurrentReadFailed,
    ChargeCurrentOwnershipUnavailable,
    ChargeCurrentWriteFailed,
    ChargeCurrentReadbackFailed,
    MinimumSocOwnershipUnavailable,
    MinimumSocWriteFailed,
    MinimumSocReadbackFailed,
    OutsideChargeWindow,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CycleOutcome {
    Applied,
    NoChange,
    DbusFailure,
    MissingSoc,
    MissingMinSoc,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TargetMode {
    pub target_soc: f64,
    pub mode: &'static str,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct PvPower {
    pub ac_w: f64,
    pub dc_w: f64,
}

impl PvPower {
    #[must_use]
    pub fn total_w(self) -> f64 {
        self.ac_w + self.dc_w
    }
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ChargeContext {
    pub needs_charge: bool,
    pub time_ok: bool,
    pub charge_window_hours: u8,
    pub charge_deficit_changed: bool,
    pub stage_charge_target: bool,
    pub grid_import_w: Option<f64>,
    pub effective_active: bool,
    pub battery_max_current_a: Option<f64>,
    pub vebus_max_charge_current_a: Option<f64>,
    pub battery_voltage_v: Option<f64>,
    pub pv_power: PvPower,
    pub house_load_w: Option<f64>,
    pub charging_inhibited: bool,
}

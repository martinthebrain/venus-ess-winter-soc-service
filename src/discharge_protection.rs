//! Pure hysteretic battery-discharge protection state machine.

use crate::config::PolicyConfig;
use crate::domain::{DischargeProtectionState, DischargeWriteKind, PendingDischargeWrite};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ProtectionInput {
    pub soc: f64,
    pub battery_power_w: Option<f64>,
    pub current_limit_w: Option<f64>,
    pub nominal_inverter_power_w: Option<f64>,
    pub monotonic_now: f64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ProtectionAction {
    Restrict(f64),
    Restore(f64),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PendingWriteResolution {
    Unavailable,
    Obsolete,
    Applied(ProtectionAction),
    Retry(ProtectionAction),
    ExternalChange,
}

const DEFAULT_DISCHARGE_POWER_W: f64 = -1.0;
const MAX_NOMINAL_INVERTER_POWER_W: f64 = 1_000_000.0;

#[derive(Clone, Copy, Debug, PartialEq)]
enum RestoreTarget {
    Default,
    ExplicitWatts(f64),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProtectionEvents(u8);

impl ProtectionEvents {
    const ACTIVATED: u8 = 1;
    const RECHARGE_SEEN: u8 = 1 << 1;
    const EXTERNAL_CHANGE: u8 = 1 << 2;
    const RELEASED: u8 = 1 << 3;

    #[must_use]
    pub const fn activated(self) -> bool {
        self.0 & Self::ACTIVATED != 0
    }

    #[must_use]
    pub const fn recharge_seen(self) -> bool {
        self.0 & Self::RECHARGE_SEEN != 0
    }

    #[must_use]
    pub const fn external_change(self) -> bool {
        self.0 & Self::EXTERNAL_CHANGE != 0
    }

    #[must_use]
    pub const fn released(self) -> bool {
        self.0 & Self::RELEASED != 0
    }

    const fn mark(&mut self, event: u8) {
        self.0 |= event;
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ProtectionEvaluation {
    pub action: Option<ProtectionAction>,
    pub events: ProtectionEvents,
    pub state_changed: bool,
}

#[must_use]
pub fn evaluate(
    policy: &PolicyConfig,
    state: &mut DischargeProtectionState,
    input: ProtectionInput,
) -> ProtectionEvaluation {
    let mut evaluation = ProtectionEvaluation::default();
    if !state.active {
        if input.soc >= policy.discharge_protection_enter_soc
            || !input.battery_power_w.is_some_and(|power| power < 0.0)
        {
            return evaluation;
        }
        let (Some(current), Some(nominal)) = (
            valid_configured_limit(input.current_limit_w),
            valid_nominal(input.nominal_inverter_power_w),
        ) else {
            return evaluation;
        };
        let Some(restore_target) = restore_target(current, nominal) else {
            return evaluation;
        };
        state.active = true;
        state.recharge_seen = false;
        reset_recharge_candidate(state);
        set_restore_target(state, restore_target);
        state.last_set_power_w = None;
        state.last_observed_power_w = Some(restore_target.as_setting());
        evaluation.events.mark(ProtectionEvents::ACTIVATED);
        evaluation.state_changed = true;
    }

    if confirm_recharge(policy, state, input.battery_power_w, input.monotonic_now) {
        evaluation.events.mark(ProtectionEvents::RECHARGE_SEEN);
        evaluation.state_changed = true;
    }

    let Some(current) = valid_configured_limit(input.current_limit_w) else {
        return evaluation;
    };
    let nominal = valid_nominal(input.nominal_inverter_power_w);
    let normalized_current = nominal
        .and_then(|power| restore_target(current, power))
        .map_or(current, RestoreTarget::as_setting);
    if state.last_observed_power_w.is_none() {
        state.last_observed_power_w = Some(normalized_current);
        evaluation.state_changed = true;
    } else if state
        .last_observed_power_w
        .is_some_and(|last| !same_power(normalized_current, last, policy.discharge_power_epsilon_w))
    {
        let Some(restore_target) = nominal.and_then(|power| restore_target(current, power)) else {
            return evaluation;
        };
        set_restore_target(state, restore_target);
        state.last_set_power_w = None;
        state.last_observed_power_w = Some(restore_target.as_setting());
        evaluation.events.mark(ProtectionEvents::EXTERNAL_CHANGE);
        evaluation.state_changed = true;
    }

    if state.recharge_seen && input.soc > policy.discharge_protection_release_soc {
        let restore = if state.restore_default {
            Some(DEFAULT_DISCHARGE_POWER_W)
        } else {
            state
                .restore_power_w
                .zip(nominal)
                .map(|(power, current_nominal)| power.min(current_nominal))
        };
        if let Some(restore) = restore {
            if same_power(current, restore, policy.discharge_power_epsilon_w) {
                reset_protection(state);
                evaluation.events.mark(ProtectionEvents::RELEASED);
                evaluation.state_changed = true;
            } else {
                evaluation.action = Some(ProtectionAction::Restore(restore));
            }
        }
        return evaluation;
    }

    let Some(nominal) = nominal else {
        return evaluation;
    };
    let cap = nominal * policy.discharge_protection_nominal_fraction;
    if current < 0.0 || current > cap + policy.discharge_power_epsilon_w {
        evaluation.action = Some(ProtectionAction::Restrict(cap));
    }
    evaluation
}

pub fn commit_action(state: &mut DischargeProtectionState, action: ProtectionAction) {
    match action {
        ProtectionAction::Restrict(power) => {
            state.last_set_power_w = Some(power);
            state.last_observed_power_w = Some(power);
            state.pending_write = None;
        }
        ProtectionAction::Restore(_) => reset_protection(state),
    }
}

#[must_use]
pub fn normalized_setting(value: Option<f64>, nominal: Option<f64>) -> Option<f64> {
    let current = valid_configured_limit(value)?;
    let nominal = valid_nominal(nominal)?;
    restore_target(current, nominal).map(RestoreTarget::as_setting)
}

#[must_use]
pub fn setting_available(value: Option<f64>) -> bool {
    valid_configured_limit(value).is_some()
}

pub fn prepare_action(
    state: &mut DischargeProtectionState,
    action: ProtectionAction,
    expected_before_w: f64,
) {
    let generation = state.write_generation.wrapping_add(1).max(1);
    state.write_generation = generation;
    state.pending_write = Some(PendingDischargeWrite {
        generation,
        kind: action_kind(action),
        expected_before_w,
        intended_w: action_target(action),
    });
}

#[must_use]
pub fn reconcile_pending_write(
    state: &DischargeProtectionState,
    current_value: Option<f64>,
    nominal_power_w: Option<f64>,
    nominal_fraction: f64,
    epsilon_w: f64,
) -> Option<PendingWriteResolution> {
    let pending = state.pending_write?;
    let Some(current) = normalized_setting(current_value, nominal_power_w) else {
        return Some(PendingWriteResolution::Unavailable);
    };
    let nominal = nominal_power_w.and_then(|power| valid_nominal(Some(power)));
    if !pending_matches_current_policy(state, pending, nominal, nominal_fraction, epsilon_w) {
        return Some(PendingWriteResolution::Obsolete);
    }
    let action = pending_action(pending);
    if same_power(current, pending.intended_w, epsilon_w) {
        Some(PendingWriteResolution::Applied(action))
    } else if same_power(current, pending.expected_before_w, epsilon_w) {
        Some(PendingWriteResolution::Retry(action))
    } else {
        Some(PendingWriteResolution::ExternalChange)
    }
}

fn pending_matches_current_policy(
    state: &DischargeProtectionState,
    pending: PendingDischargeWrite,
    nominal: Option<f64>,
    nominal_fraction: f64,
    epsilon_w: f64,
) -> bool {
    let Some(nominal) = nominal else {
        return false;
    };
    let expected_target = match pending.kind {
        DischargeWriteKind::Restrict => {
            if !nominal_fraction.is_finite() || !(0.0..=1.0).contains(&nominal_fraction) {
                return false;
            }
            nominal * nominal_fraction
        }
        DischargeWriteKind::Restore if state.restore_default => DEFAULT_DISCHARGE_POWER_W,
        DischargeWriteKind::Restore => {
            let Some(power) = state.restore_power_w else {
                return false;
            };
            power.min(nominal)
        }
    };
    same_power(pending.intended_w, expected_target, epsilon_w)
}

pub const fn discard_pending_write(state: &mut DischargeProtectionState) {
    state.pending_write = None;
}

#[must_use]
pub fn pending_retry_is_required(
    policy: &PolicyConfig,
    state: &DischargeProtectionState,
    action: ProtectionAction,
    soc: f64,
    battery_power_w: Option<f64>,
) -> bool {
    match action {
        ProtectionAction::Restrict(_) => {
            soc.is_finite()
                && soc < policy.discharge_protection_enter_soc
                && battery_power_w.is_some_and(|power| power.is_finite() && power < 0.0)
        }
        ProtectionAction::Restore(_) => {
            state.recharge_seen && soc.is_finite() && soc > policy.discharge_protection_release_soc
        }
    }
}

pub fn cancel_unapplied_action(state: &mut DischargeProtectionState, action: ProtectionAction) {
    match action {
        ProtectionAction::Restrict(_) => reset_protection(state),
        ProtectionAction::Restore(_) => discard_pending_write(state),
    }
}

const fn action_kind(action: ProtectionAction) -> DischargeWriteKind {
    match action {
        ProtectionAction::Restrict(_) => DischargeWriteKind::Restrict,
        ProtectionAction::Restore(_) => DischargeWriteKind::Restore,
    }
}

const fn action_target(action: ProtectionAction) -> f64 {
    match action {
        ProtectionAction::Restrict(power) | ProtectionAction::Restore(power) => power,
    }
}

const fn pending_action(pending: PendingDischargeWrite) -> ProtectionAction {
    match pending.kind {
        DischargeWriteKind::Restrict => ProtectionAction::Restrict(pending.intended_w),
        DischargeWriteKind::Restore => ProtectionAction::Restore(pending.intended_w),
    }
}

fn reset_protection(state: &mut DischargeProtectionState) {
    let generation = state.write_generation;
    *state = DischargeProtectionState {
        write_generation: generation,
        ..DischargeProtectionState::default()
    };
}

/// Drop discharge-setting ownership without changing the external setting.
pub fn release_ownership(state: &mut DischargeProtectionState) {
    reset_protection(state);
}

fn valid_configured_limit(value: Option<f64>) -> Option<f64> {
    value.filter(|number| {
        number.is_finite()
            && (number.to_bits() == DEFAULT_DISCHARGE_POWER_W.to_bits() || *number >= 0.0)
    })
}

fn valid_nominal(value: Option<f64>) -> Option<f64> {
    value.filter(|number| {
        number.is_finite() && *number > 0.0 && *number <= MAX_NOMINAL_INVERTER_POWER_W
    })
}

fn restore_target(current: f64, nominal: f64) -> Option<RestoreTarget> {
    if current.to_bits() == DEFAULT_DISCHARGE_POWER_W.to_bits() {
        Some(RestoreTarget::Default)
    } else if current >= 0.0 {
        Some(RestoreTarget::ExplicitWatts(current.min(nominal)))
    } else {
        None
    }
}

const fn set_restore_target(state: &mut DischargeProtectionState, target: RestoreTarget) {
    state.restore_default = matches!(target, RestoreTarget::Default);
    state.restore_power_w = match target {
        RestoreTarget::Default => None,
        RestoreTarget::ExplicitWatts(power) => Some(power),
    };
}

fn confirm_recharge(
    policy: &PolicyConfig,
    state: &mut DischargeProtectionState,
    battery_power_w: Option<f64>,
    monotonic_now: f64,
) -> bool {
    if state.recharge_seen {
        reset_recharge_candidate(state);
        return false;
    }
    let qualifies = monotonic_now.is_finite()
        && battery_power_w.is_some_and(|power| {
            power.is_finite() && power >= policy.discharge_recharge_min_power_w
        });
    if !qualifies {
        reset_recharge_candidate(state);
        return false;
    }

    let continuous = state.recharge_candidate_last_sample_ts.is_some_and(|last| {
        monotonic_now >= last
            && monotonic_now - last <= policy.discharge_recharge_max_sample_gap_seconds
    });
    if !continuous {
        state.recharge_candidate_since_ts = Some(monotonic_now);
    }
    state.recharge_candidate_last_sample_ts = Some(monotonic_now);

    if state
        .recharge_candidate_since_ts
        .is_some_and(|since| monotonic_now - since >= policy.discharge_recharge_confirm_seconds)
    {
        state.recharge_seen = true;
        reset_recharge_candidate(state);
        return true;
    }
    false
}

const fn reset_recharge_candidate(state: &mut DischargeProtectionState) {
    state.recharge_candidate_since_ts = None;
    state.recharge_candidate_last_sample_ts = None;
}

impl RestoreTarget {
    const fn as_setting(self) -> f64 {
        match self {
            Self::Default => DEFAULT_DISCHARGE_POWER_W,
            Self::ExplicitWatts(power) => power,
        }
    }
}

fn same_power(left: f64, right: f64, epsilon: f64) -> bool {
    (left - right).abs() <= epsilon
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(soc: f64, battery_power_w: f64, current: f64, nominal: f64) -> ProtectionInput {
        input_at(soc, battery_power_w, current, nominal, 0.0)
    }

    fn input_at(
        soc: f64,
        battery_power_w: f64,
        current: f64,
        nominal: f64,
        monotonic_now: f64,
    ) -> ProtectionInput {
        ProtectionInput {
            soc,
            battery_power_w: Some(battery_power_w),
            current_limit_w: Some(current),
            nominal_inverter_power_w: Some(nominal),
            monotonic_now,
        }
    }

    #[test]
    fn strict_entry_requires_low_soc_while_discharging() {
        let policy = PolicyConfig::default();
        for sample in [
            input(20.0, -100.0, -1.0, 2_500.0),
            input(19.0, 0.0, -1.0, 2_500.0),
        ] {
            let mut state = DischargeProtectionState::default();
            assert_eq!(
                evaluate(&policy, &mut state, sample),
                ProtectionEvaluation::default()
            );
            assert!(!state.active);
        }
    }

    #[test]
    fn default_setting_is_capped_and_restored_exactly() {
        let policy = PolicyConfig::default();
        let mut state = DischargeProtectionState::default();
        let entry = evaluate(&policy, &mut state, input(19.9, -500.0, -1.0, 2_500.0));
        assert_eq!(entry.action, Some(ProtectionAction::Restrict(1_000.0)));
        assert!(state.restore_default);
        assert_eq!(state.restore_power_w, None);
        commit_action(
            &mut state,
            entry.action.unwrap_or_else(|| std::process::abort()),
        );

        let first = evaluate(
            &policy,
            &mut state,
            input_at(24.9, 500.0, 1_000.0, 2_500.0, 10.0),
        );
        assert!(!first.events.recharge_seen());
        let second = evaluate(
            &policy,
            &mut state,
            input_at(24.9, 500.0, 1_000.0, 2_500.0, 70.0),
        );
        assert!(!second.events.recharge_seen());
        let release = evaluate(
            &policy,
            &mut state,
            input_at(25.1, 500.0, 1_000.0, 2_500.0, 130.0),
        );
        assert!(release.events.recharge_seen());
        assert_eq!(release.action, Some(ProtectionAction::Restore(-1.0)));
    }

    #[test]
    fn stricter_gui_limit_is_never_raised() {
        let policy = PolicyConfig::default();
        let mut state = DischargeProtectionState::default();
        let result = evaluate(&policy, &mut state, input(19.0, -500.0, 800.0, 2_500.0));
        assert!(result.events.activated());
        assert_eq!(result.action, None);
        assert_eq!(state.restore_power_w, Some(800.0));
    }

    #[test]
    fn external_change_becomes_the_new_restore_value_but_remains_capped() {
        let policy = PolicyConfig::default();
        let mut state = DischargeProtectionState {
            active: true,
            recharge_seen: false,
            recharge_candidate_since_ts: None,
            recharge_candidate_last_sample_ts: None,
            restore_power_w: Some(2_500.0),
            restore_default: false,
            last_set_power_w: Some(1_000.0),
            last_observed_power_w: Some(1_000.0),
            write_generation: 0,
            pending_write: None,
        };
        let result = evaluate(&policy, &mut state, input(19.0, -500.0, 2_000.0, 2_500.0));
        assert!(result.events.external_change());
        assert_eq!(state.restore_power_w, Some(2_000.0));
        assert_eq!(result.action, Some(ProtectionAction::Restrict(1_000.0)));
    }

    #[test]
    fn release_requires_observed_charging_and_strictly_more_than_25_percent() {
        let policy = PolicyConfig::default();
        let mut state = DischargeProtectionState {
            active: true,
            recharge_seen: false,
            recharge_candidate_since_ts: None,
            recharge_candidate_last_sample_ts: None,
            restore_power_w: Some(2_500.0),
            restore_default: false,
            last_set_power_w: Some(1_000.0),
            last_observed_power_w: Some(1_000.0),
            write_generation: 0,
            pending_write: None,
        };
        assert_eq!(
            evaluate(&policy, &mut state, input(26.0, 0.0, 1_000.0, 2_500.0)).action,
            None
        );
        assert_eq!(
            evaluate(
                &policy,
                &mut state,
                input_at(25.0, 100.0, 1_000.0, 2_500.0, 10.0)
            )
            .action,
            None
        );
        assert_eq!(
            evaluate(
                &policy,
                &mut state,
                input_at(25.0, 100.0, 1_000.0, 2_500.0, 70.0)
            )
            .action,
            None
        );
        let confirmed = evaluate(
            &policy,
            &mut state,
            input_at(25.0, 100.0, 1_000.0, 2_500.0, 130.0),
        );
        assert!(confirmed.events.recharge_seen());
        assert_eq!(confirmed.action, None);
        assert_eq!(
            evaluate(
                &policy,
                &mut state,
                input_at(25.1, 100.0, 1_000.0, 2_500.0, 190.0)
            )
            .action,
            Some(ProtectionAction::Restore(2_500.0))
        );
    }

    #[test]
    fn stricter_gui_change_during_protection_becomes_the_restore_value() {
        let policy = PolicyConfig::default();
        let mut state = DischargeProtectionState::default();
        let entry = evaluate(&policy, &mut state, input(19.0, -500.0, 800.0, 2_500.0));
        assert_eq!(entry.action, None);

        let changed = evaluate(&policy, &mut state, input(19.0, -500.0, 700.0, 2_500.0));
        assert!(changed.events.external_change());
        assert_eq!(state.restore_power_w, Some(700.0));
        assert_eq!(changed.action, None);
    }

    #[test]
    fn default_restore_is_independent_of_changed_nominal_power() {
        let policy = PolicyConfig::default();
        let mut state = DischargeProtectionState::default();
        let entry = evaluate(&policy, &mut state, input(19.0, -500.0, -1.0, 2_500.0));
        commit_action(
            &mut state,
            entry.action.unwrap_or_else(|| std::process::abort()),
        );
        for now in [10.0, 70.0] {
            let pending = evaluate(
                &policy,
                &mut state,
                input_at(25.1, 500.0, 1_000.0, 3_000.0, now),
            );
            assert_eq!(pending.action, None);
        }
        let release = evaluate(
            &policy,
            &mut state,
            input_at(25.1, 500.0, 1_000.0, 3_000.0, 130.0),
        );
        assert_eq!(release.action, Some(ProtectionAction::Restore(-1.0)));
    }

    #[test]
    fn configured_limit_rejects_values_between_default_and_zero() {
        let policy = PolicyConfig::default();
        let mut state = DischargeProtectionState::default();

        let result = evaluate(&policy, &mut state, input(19.0, -500.0, -0.5, 2_500.0));

        assert_eq!(result, ProtectionEvaluation::default());
        assert!(!state.active);
    }

    #[test]
    fn explicit_restore_value_is_bounded_by_current_hardware() {
        let policy = PolicyConfig::default();
        let mut state = DischargeProtectionState::default();

        let entry = evaluate(&policy, &mut state, input(19.0, -500.0, 1.0e100, 2_500.0));

        assert_eq!(entry.action, Some(ProtectionAction::Restrict(1_000.0)));
        assert_eq!(state.restore_power_w, Some(2_500.0));
        assert_eq!(state.last_observed_power_w, Some(2_500.0));
    }

    #[test]
    fn persisted_explicit_restore_is_rebounded_before_use() {
        let policy = PolicyConfig::default();
        let mut state = DischargeProtectionState {
            active: true,
            recharge_seen: true,
            recharge_candidate_since_ts: None,
            recharge_candidate_last_sample_ts: None,
            restore_power_w: Some(5_000.0),
            restore_default: false,
            last_set_power_w: Some(1_000.0),
            last_observed_power_w: Some(1_000.0),
            write_generation: 0,
            pending_write: None,
        };

        let release = evaluate(&policy, &mut state, input(25.1, 500.0, 1_000.0, 2_500.0));

        assert_eq!(release.action, Some(ProtectionAction::Restore(2_500.0)));
    }

    #[test]
    fn implausible_nominal_power_never_drives_a_setting_write() {
        let policy = PolicyConfig::default();
        let mut state = DischargeProtectionState::default();

        let result = evaluate(&policy, &mut state, input(19.0, -500.0, -1.0, 1.0e100));

        assert_eq!(result, ProtectionEvaluation::default());
        assert!(!state.active);
    }

    #[test]
    fn a_single_positive_sample_never_confirms_recharge() {
        let policy = PolicyConfig::default();
        let mut state = DischargeProtectionState::default();
        let entry = evaluate(&policy, &mut state, input(19.0, -500.0, -1.0, 2_500.0));
        commit_action(
            &mut state,
            entry.action.unwrap_or_else(|| std::process::abort()),
        );

        let result = evaluate(
            &policy,
            &mut state,
            input_at(26.0, 500.0, 1_000.0, 2_500.0, 10.0),
        );

        assert!(!result.events.recharge_seen());
        assert_eq!(result.action, None);
        assert!(!state.recharge_seen);
    }

    #[test]
    fn low_power_and_missing_samples_restart_confirmation() {
        let policy = PolicyConfig::default();
        let mut state = DischargeProtectionState::default();
        let entry = evaluate(&policy, &mut state, input(19.0, -500.0, -1.0, 2_500.0));
        commit_action(
            &mut state,
            entry.action.unwrap_or_else(|| std::process::abort()),
        );

        let _ = evaluate(
            &policy,
            &mut state,
            input_at(24.0, 500.0, 1_000.0, 2_500.0, 10.0),
        );
        let _ = evaluate(
            &policy,
            &mut state,
            input_at(24.0, 99.9, 1_000.0, 2_500.0, 70.0),
        );
        assert_eq!(state.recharge_candidate_since_ts, None);
        let mut missing = input_at(24.0, 0.0, 1_000.0, 2_500.0, 130.0);
        missing.battery_power_w = None;
        let _ = evaluate(&policy, &mut state, missing);
        assert_eq!(state.recharge_candidate_since_ts, None);
    }

    #[test]
    fn excessive_sample_gap_restarts_confirmation() {
        let policy = PolicyConfig::default();
        let mut state = DischargeProtectionState::default();
        let entry = evaluate(&policy, &mut state, input(19.0, -500.0, -1.0, 2_500.0));
        commit_action(
            &mut state,
            entry.action.unwrap_or_else(|| std::process::abort()),
        );

        let _ = evaluate(
            &policy,
            &mut state,
            input_at(24.0, 500.0, 1_000.0, 2_500.0, 10.0),
        );
        let delayed = evaluate(
            &policy,
            &mut state,
            input_at(26.0, 500.0, 1_000.0, 2_500.0, 101.0),
        );

        assert!(!delayed.events.recharge_seen());
        assert_eq!(state.recharge_candidate_since_ts, Some(101.0));
        assert!(!state.recharge_seen);
    }

    #[test]
    fn incomplete_confirmation_is_neither_serialized_nor_restored() {
        let state = DischargeProtectionState {
            active: true,
            recharge_seen: false,
            recharge_candidate_since_ts: Some(10.0),
            recharge_candidate_last_sample_ts: Some(70.0),
            restore_power_w: None,
            restore_default: true,
            last_set_power_w: Some(1_000.0),
            last_observed_power_w: Some(1_000.0),
            write_generation: 0,
            pending_write: None,
        };

        let encoded = serde_json::to_value(&state).unwrap_or_else(|_| std::process::abort());
        assert!(encoded.get("recharge_candidate_since_ts").is_none());
        assert!(encoded.get("recharge_candidate_last_sample_ts").is_none());
        let restored: DischargeProtectionState =
            serde_json::from_value(encoded).unwrap_or_else(|_| std::process::abort());
        assert_eq!(restored.recharge_candidate_since_ts, None);
        assert_eq!(restored.recharge_candidate_last_sample_ts, None);
        assert!(!restored.recharge_seen);
    }

    #[test]
    fn pending_restriction_distinguishes_applied_retry_and_external_values() {
        let policy = PolicyConfig::default();
        let mut pending = DischargeProtectionState::default();
        let entry = evaluate(&policy, &mut pending, input(19.0, -500.0, -1.0, 2_500.0));
        let action = entry.action.unwrap_or_else(|| std::process::abort());
        prepare_action(&mut pending, action, -1.0);

        assert_eq!(pending.write_generation, 1);
        assert_eq!(
            pending.pending_write,
            Some(PendingDischargeWrite {
                generation: 1,
                kind: DischargeWriteKind::Restrict,
                expected_before_w: -1.0,
                intended_w: 1_000.0,
            })
        );
        assert_eq!(
            reconcile_pending_write(&pending, Some(1_000.0), Some(2_500.0), 0.4, 1.0),
            Some(PendingWriteResolution::Applied(action))
        );
        assert_eq!(
            reconcile_pending_write(&pending, Some(-1.0), Some(2_500.0), 0.4, 1.0),
            Some(PendingWriteResolution::Retry(action))
        );
        assert_eq!(
            reconcile_pending_write(&pending, Some(800.0), Some(2_500.0), 0.4, 1.0),
            Some(PendingWriteResolution::ExternalChange)
        );

        commit_action(&mut pending, action);
        assert!(pending.active);
        assert!(pending.restore_default);
        assert_eq!(pending.restore_power_w, None);
        assert_eq!(pending.last_set_power_w, Some(1_000.0));
        assert_eq!(pending.pending_write, None);
        assert_eq!(pending.write_generation, 1);
    }

    #[test]
    fn superseding_user_value_becomes_the_restore_target_after_recovery() {
        let policy = PolicyConfig::default();
        let mut state = DischargeProtectionState::default();
        let entry = evaluate(&policy, &mut state, input(19.0, -500.0, -1.0, 2_500.0));
        prepare_action(
            &mut state,
            entry.action.unwrap_or_else(|| std::process::abort()),
            -1.0,
        );

        discard_pending_write(&mut state);
        let recovered = evaluate(&policy, &mut state, input(19.0, -500.0, 800.0, 2_500.0));

        assert!(recovered.events.external_change());
        assert_eq!(recovered.action, None);
        assert_eq!(state.restore_power_w, Some(800.0));
        assert!(!state.restore_default);
        assert_eq!(state.pending_write, None);
        assert_eq!(state.write_generation, 1);
    }

    #[test]
    fn recovered_restore_commit_resets_protection_but_retains_generation() {
        let mut state = DischargeProtectionState {
            active: true,
            recharge_seen: true,
            restore_power_w: None,
            restore_default: true,
            last_set_power_w: Some(1_000.0),
            last_observed_power_w: Some(1_000.0),
            ..DischargeProtectionState::default()
        };
        let action = ProtectionAction::Restore(-1.0);
        prepare_action(&mut state, action, 1_000.0);

        assert_eq!(
            reconcile_pending_write(&state, Some(-1.0), Some(2_500.0), 0.4, 1.0),
            Some(PendingWriteResolution::Applied(action))
        );
        commit_action(&mut state, action);

        assert!(!state.active);
        assert_eq!(state.pending_write, None);
        assert_eq!(state.write_generation, 1);
    }

    #[test]
    fn pending_write_from_an_old_hardware_basis_is_obsolete() {
        let policy = PolicyConfig::default();
        let mut state = DischargeProtectionState::default();
        let entry = evaluate(&policy, &mut state, input(19.0, -500.0, -1.0, 2_500.0));
        prepare_action(
            &mut state,
            entry.action.unwrap_or_else(|| std::process::abort()),
            -1.0,
        );

        assert_eq!(
            reconcile_pending_write(&state, Some(-1.0), Some(2_000.0), 0.4, 1.0),
            Some(PendingWriteResolution::Obsolete)
        );
    }

    #[test]
    fn restriction_retry_requires_current_low_soc_discharge() {
        let policy = PolicyConfig::default();
        let state = DischargeProtectionState {
            active: true,
            ..DischargeProtectionState::default()
        };
        let action = ProtectionAction::Restrict(1_000.0);

        assert!(pending_retry_is_required(
            &policy,
            &state,
            action,
            19.0,
            Some(-500.0)
        ));
        assert!(!pending_retry_is_required(
            &policy,
            &state,
            action,
            80.0,
            Some(0.0)
        ));
        assert!(!pending_retry_is_required(
            &policy, &state, action, 19.0, None
        ));
    }

    #[test]
    fn restore_retry_requires_the_current_release_condition() {
        let policy = PolicyConfig::default();
        let mut state = DischargeProtectionState {
            active: true,
            recharge_seen: true,
            ..DischargeProtectionState::default()
        };
        let action = ProtectionAction::Restore(-1.0);

        assert!(pending_retry_is_required(
            &policy,
            &state,
            action,
            25.1,
            Some(0.0)
        ));
        assert!(!pending_retry_is_required(
            &policy,
            &state,
            action,
            19.0,
            Some(-500.0)
        ));
        state.recharge_seen = false;
        assert!(!pending_retry_is_required(
            &policy,
            &state,
            action,
            80.0,
            Some(0.0)
        ));
    }

    #[test]
    fn cancelling_unapplied_actions_preserves_only_required_ownership() {
        let mut restriction = DischargeProtectionState {
            active: true,
            restore_default: true,
            write_generation: 7,
            pending_write: Some(PendingDischargeWrite {
                generation: 7,
                kind: DischargeWriteKind::Restrict,
                expected_before_w: -1.0,
                intended_w: 1_000.0,
            }),
            ..DischargeProtectionState::default()
        };
        cancel_unapplied_action(&mut restriction, ProtectionAction::Restrict(1_000.0));
        assert!(!restriction.active);
        assert_eq!(restriction.pending_write, None);
        assert_eq!(restriction.write_generation, 7);

        let mut restore = DischargeProtectionState {
            active: true,
            recharge_seen: true,
            restore_default: true,
            last_set_power_w: Some(1_000.0),
            write_generation: 8,
            pending_write: Some(PendingDischargeWrite {
                generation: 8,
                kind: DischargeWriteKind::Restore,
                expected_before_w: 1_000.0,
                intended_w: -1.0,
            }),
            ..DischargeProtectionState::default()
        };
        cancel_unapplied_action(&mut restore, ProtectionAction::Restore(-1.0));
        assert!(restore.active);
        assert!(restore.recharge_seen);
        assert_eq!(restore.last_set_power_w, Some(1_000.0));
        assert_eq!(restore.pending_write, None);
        assert_eq!(restore.write_generation, 8);
    }
}

//! Single-owner arbitration and crash recovery for DVCC charge current.

use crate::domain::{ChargeCurrentControlState, ChargeCurrentWriteKind, PendingChargeCurrentWrite};

pub const DISABLED_CHARGE_CURRENT_A: f64 = 0.0;
pub const UNLIMITED_CHARGE_CURRENT_A: f64 = -1.0;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ChargeCurrentAction {
    Apply(f64),
    Restore(f64),
}

impl ChargeCurrentAction {
    #[must_use]
    pub const fn intended_a(self) -> f64 {
        match self {
            Self::Apply(value) | Self::Restore(value) => value,
        }
    }

    #[must_use]
    pub const fn kind(self) -> ChargeCurrentWriteKind {
        match self {
            Self::Apply(_) => ChargeCurrentWriteKind::Restrict,
            Self::Restore(_) => ChargeCurrentWriteKind::Restore,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ChargeCurrentEvaluation {
    pub action: Option<ChargeCurrentAction>,
    pub ownership_released: bool,
    pub satisfied: bool,
    pub blocked_by_external_control: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PendingWriteResolution {
    Applied(ChargeCurrentWriteKind),
    Retry(PendingChargeCurrentWrite),
    Cancelled,
    ExternalChange { satisfied: bool },
}

#[must_use]
pub fn routine_limit_required(
    was_requested: bool,
    current_soc: f64,
    ceiling_soc: f64,
    hysteresis: f64,
) -> bool {
    if ceiling_soc >= 100.0 {
        return false;
    }
    if was_requested {
        current_soc >= ceiling_soc - hysteresis
    } else {
        current_soc >= ceiling_soc
    }
}

pub const fn set_configured_constraint(state: &mut ChargeCurrentControlState, value: Option<f64>) {
    state.configured_constraint_a = value;
}

pub const fn set_reserve_constraint(state: &mut ChargeCurrentControlState, value: Option<f64>) {
    state.reserve_constraint_a = value;
}

pub const fn set_routine_ceiling_requested(state: &mut ChargeCurrentControlState, requested: bool) {
    state.routine_ceiling_requested = requested;
    if !requested {
        state.routine_external_control_latched = false;
    }
}

pub const fn set_explicit_inhibit_requested(
    state: &mut ChargeCurrentControlState,
    requested: bool,
) {
    state.explicit_inhibit_requested = requested;
}

pub const fn clear_requests(state: &mut ChargeCurrentControlState) {
    state.configured_constraint_a = None;
    state.reserve_constraint_a = None;
    state.routine_ceiling_requested = false;
    state.explicit_inhibit_requested = false;
    state.routine_external_control_latched = false;
}

#[must_use]
pub const fn effective_constraint(state: &ChargeCurrentControlState) -> Option<f64> {
    if state.routine_ceiling_requested || state.explicit_inhibit_requested {
        return Some(DISABLED_CHARGE_CURRENT_A);
    }
    stricter_constraint(state.configured_constraint_a, state.reserve_constraint_a)
}

#[must_use]
pub fn effective_target(state: &ChargeCurrentControlState) -> Option<f64> {
    effective_constraint(state)
        .map(|limit| stricter_with_baseline(limit, state.external_baseline_a))
}

pub fn evaluate(
    state: &mut ChargeCurrentControlState,
    current_a: f64,
    epsilon_a: f64,
) -> ChargeCurrentEvaluation {
    let requested = effective_constraint(state);
    let satisfied = requested.is_none_or(|limit| current_at_most(current_a, limit, epsilon_a));
    if state.pending_write.is_some() {
        return ChargeCurrentEvaluation {
            action: None,
            ownership_released: false,
            satisfied,
            blocked_by_external_control: false,
        };
    }

    if state.owned {
        let owns_current = state
            .last_effectively_written_a
            .is_some_and(|last| current_matches(current_a, last, epsilon_a));
        if !owns_current {
            release_to_external_control(state);
            return ChargeCurrentEvaluation {
                action: None,
                ownership_released: true,
                satisfied,
                blocked_by_external_control: requested.is_some() && !satisfied,
            };
        }
        return match requested {
            Some(limit) => {
                let desired = stricter_with_baseline(limit, state.external_baseline_a);
                if state.external_baseline_a.is_some_and(|baseline| {
                    current_matches(current_a, baseline, epsilon_a)
                        && current_matches(desired, baseline, epsilon_a)
                }) {
                    clear_ownership(state);
                    return ChargeCurrentEvaluation {
                        action: None,
                        ownership_released: false,
                        satisfied: true,
                        blocked_by_external_control: false,
                    };
                }
                ChargeCurrentEvaluation {
                    action: (!current_matches(current_a, desired, epsilon_a))
                        .then_some(ChargeCurrentAction::Apply(desired)),
                    ownership_released: false,
                    satisfied: current_at_most(current_a, limit, epsilon_a),
                    blocked_by_external_control: false,
                }
            }
            None => {
                if state
                    .external_baseline_a
                    .is_some_and(|baseline| current_matches(current_a, baseline, epsilon_a))
                {
                    clear_ownership(state);
                    ChargeCurrentEvaluation {
                        action: None,
                        ownership_released: false,
                        satisfied: true,
                        blocked_by_external_control: false,
                    }
                } else {
                    ChargeCurrentEvaluation {
                        action: state.external_baseline_a.map(ChargeCurrentAction::Restore),
                        ownership_released: false,
                        satisfied: false,
                        blocked_by_external_control: false,
                    }
                }
            }
        };
    }

    if state.routine_external_control_latched && state.routine_ceiling_requested {
        return ChargeCurrentEvaluation {
            action: None,
            ownership_released: false,
            satisfied,
            blocked_by_external_control: !satisfied,
        };
    }
    if !state.routine_ceiling_requested {
        state.routine_external_control_latched = false;
    }
    ChargeCurrentEvaluation {
        action: requested
            .filter(|limit| !current_at_most(current_a, *limit, epsilon_a))
            .map(ChargeCurrentAction::Apply),
        ownership_released: false,
        satisfied,
        blocked_by_external_control: false,
    }
}

pub fn prepare_action(
    state: &mut ChargeCurrentControlState,
    action: ChargeCurrentAction,
    current_a: f64,
) -> PendingChargeCurrentWrite {
    if !state.owned {
        state.external_baseline_a = Some(current_a);
        state.last_effectively_written_a = None;
        state.owned = true;
    }
    state.write_generation = state.write_generation.saturating_add(1).max(1);
    let pending = PendingChargeCurrentWrite {
        generation: state.write_generation,
        kind: action.kind(),
        expected_before_a: current_a,
        intended_a: action.intended_a(),
    };
    state.pending_write = Some(pending);
    pending
}

pub fn reconcile_pending_write(
    state: &mut ChargeCurrentControlState,
    current_a: f64,
    epsilon_a: f64,
) -> Option<PendingWriteResolution> {
    let pending = state.pending_write?;
    if current_matches(current_a, pending.intended_a, epsilon_a) {
        commit_write(state, pending);
        return Some(PendingWriteResolution::Applied(pending.kind));
    }
    if current_matches(current_a, pending.expected_before_a, epsilon_a) {
        let desired = desired_action(state, current_a, epsilon_a);
        if desired.is_some_and(|action| {
            action.kind() == pending.kind
                && current_matches(action.intended_a(), pending.intended_a, epsilon_a)
        }) {
            return Some(PendingWriteResolution::Retry(pending));
        }
        cancel_obsolete_write(state, pending, current_a, epsilon_a);
        return Some(PendingWriteResolution::Cancelled);
    }

    let requested = effective_constraint(state);
    let satisfied = requested.is_none_or(|limit| current_at_most(current_a, limit, epsilon_a));
    release_to_external_control(state);
    Some(PendingWriteResolution::ExternalChange { satisfied })
}

pub fn commit_write(state: &mut ChargeCurrentControlState, pending: PendingChargeCurrentWrite) {
    if state.pending_write != Some(pending) {
        return;
    }
    state.pending_write = None;
    match pending.kind {
        ChargeCurrentWriteKind::Restrict => {
            state.owned = true;
            state.last_effectively_written_a = Some(pending.intended_a);
        }
        ChargeCurrentWriteKind::Restore => clear_ownership(state),
    }
}

pub const fn clear_ownership(state: &mut ChargeCurrentControlState) {
    state.external_baseline_a = None;
    state.owned = false;
    state.last_effectively_written_a = None;
    state.pending_write = None;
}

#[must_use]
pub fn current_matches(left: f64, right: f64, epsilon_a: f64) -> bool {
    (left - right).abs() <= epsilon_a
}

#[must_use]
pub fn current_at_most(current: f64, limit: f64, epsilon_a: f64) -> bool {
    current >= 0.0 && current <= limit + epsilon_a
}

fn desired_action(
    state: &ChargeCurrentControlState,
    current_a: f64,
    epsilon_a: f64,
) -> Option<ChargeCurrentAction> {
    effective_constraint(state).map_or_else(
        || {
            state
                .external_baseline_a
                .filter(|baseline| !current_matches(current_a, *baseline, epsilon_a))
                .map(ChargeCurrentAction::Restore)
        },
        |limit| {
            let desired = stricter_with_baseline(limit, state.external_baseline_a);
            (!current_matches(current_a, desired, epsilon_a))
                .then_some(ChargeCurrentAction::Apply(desired))
        },
    )
}

fn cancel_obsolete_write(
    state: &mut ChargeCurrentControlState,
    pending: PendingChargeCurrentWrite,
    current_a: f64,
    epsilon_a: f64,
) {
    state.pending_write = None;
    if state
        .external_baseline_a
        .is_some_and(|baseline| current_matches(current_a, baseline, epsilon_a))
        && effective_constraint(state).is_none()
    {
        clear_ownership(state);
    } else if state.owned {
        state.last_effectively_written_a = Some(pending.expected_before_a);
    }
}

const fn release_to_external_control(state: &mut ChargeCurrentControlState) {
    state.routine_external_control_latched = state.routine_ceiling_requested;
    clear_ownership(state);
}

fn stricter_with_baseline(limit: f64, baseline: Option<f64>) -> f64 {
    baseline
        .filter(|value| *value >= 0.0)
        .map_or(limit, |value| value.min(limit))
}

const fn stricter_constraint(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(if left < right { left } else { right }),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routine_limit_uses_entry_and_release_hysteresis() {
        assert!(!routine_limit_required(false, 89.9, 90.0, 1.0));
        assert!(routine_limit_required(false, 90.0, 90.0, 1.0));
        assert!(routine_limit_required(true, 89.0, 90.0, 1.0));
        assert!(!routine_limit_required(true, 88.9, 90.0, 1.0));
        assert!(!routine_limit_required(false, 100.0, 100.0, 1.0));
    }

    #[test]
    fn reserve_and_routine_share_one_original_baseline() {
        let mut state = ChargeCurrentControlState {
            reserve_constraint_a: Some(20.0),
            ..ChargeCurrentControlState::default()
        };
        let reserve = evaluate(&mut state, -1.0, 0.1)
            .action
            .unwrap_or_else(|| std::process::abort());
        let reserve = prepare_action(&mut state, reserve, -1.0);
        commit_write(&mut state, reserve);
        assert_eq!(state.external_baseline_a, Some(-1.0));

        set_routine_ceiling_requested(&mut state, true);
        let routine = evaluate(&mut state, 20.0, 0.1)
            .action
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(routine, ChargeCurrentAction::Apply(0.0));
        let routine = prepare_action(&mut state, routine, 20.0);
        commit_write(&mut state, routine);

        clear_requests(&mut state);
        let restore = evaluate(&mut state, 0.0, 0.1)
            .action
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(restore, ChargeCurrentAction::Restore(-1.0));
    }

    #[test]
    fn stricter_external_value_is_preserved_without_ownership() {
        let mut state = ChargeCurrentControlState {
            reserve_constraint_a: Some(20.0),
            ..ChargeCurrentControlState::default()
        };
        let result = evaluate(&mut state, 5.0, 0.1);
        assert!(result.satisfied);
        assert_eq!(result.action, None);
        assert!(!state.owned);
    }

    #[test]
    fn pending_write_is_applied_retried_or_rejected() {
        let mut applied = ChargeCurrentControlState {
            reserve_constraint_a: Some(20.0),
            ..ChargeCurrentControlState::default()
        };
        prepare_action(&mut applied, ChargeCurrentAction::Apply(20.0), -1.0);
        assert_eq!(
            reconcile_pending_write(&mut applied, 20.0, 0.1),
            Some(PendingWriteResolution::Applied(
                ChargeCurrentWriteKind::Restrict
            ))
        );

        let mut retry = ChargeCurrentControlState {
            reserve_constraint_a: Some(20.0),
            ..ChargeCurrentControlState::default()
        };
        let pending = prepare_action(&mut retry, ChargeCurrentAction::Apply(20.0), -1.0);
        assert_eq!(
            reconcile_pending_write(&mut retry, -1.0, 0.1),
            Some(PendingWriteResolution::Retry(pending))
        );

        let mut external = ChargeCurrentControlState {
            reserve_constraint_a: Some(20.0),
            ..ChargeCurrentControlState::default()
        };
        prepare_action(&mut external, ChargeCurrentAction::Apply(20.0), -1.0);
        assert_eq!(
            reconcile_pending_write(&mut external, 5.0, 0.1),
            Some(PendingWriteResolution::ExternalChange { satisfied: true })
        );
        assert!(!external.owned);
    }

    #[test]
    fn changed_request_cancels_obsolete_pending_write() {
        let mut state = ChargeCurrentControlState {
            reserve_constraint_a: Some(20.0),
            ..ChargeCurrentControlState::default()
        };
        prepare_action(&mut state, ChargeCurrentAction::Apply(20.0), -1.0);
        set_routine_ceiling_requested(&mut state, true);
        assert_eq!(
            reconcile_pending_write(&mut state, -1.0, 0.1),
            Some(PendingWriteResolution::Cancelled)
        );
        assert!(state.owned);
        assert_eq!(state.last_effectively_written_a, Some(-1.0));
        assert_eq!(
            evaluate(&mut state, -1.0, 0.1).action,
            Some(ChargeCurrentAction::Apply(0.0))
        );
    }
}

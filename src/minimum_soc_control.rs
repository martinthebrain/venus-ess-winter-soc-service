//! Ownership and write-ahead state for the ESS minimum-SoC setting.

use crate::domain::{MinimumSocControlState, MinimumSocWriteKind, PendingMinimumSocWrite};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reconciliation {
    Stable,
    Applied(MinimumSocWriteKind),
    Retry(MinimumSocWriteKind),
    ExternalChange,
}

#[must_use]
pub fn reconcile(state: &MinimumSocControlState, current: f64, epsilon: f64) -> Reconciliation {
    if let Some(pending) = state.pending_write {
        if same(current, pending.intended, epsilon) {
            return Reconciliation::Applied(pending.kind);
        }
        if same(current, pending.expected_before, epsilon) {
            return Reconciliation::Retry(pending.kind);
        }
        return Reconciliation::ExternalChange;
    }
    if state.owned
        && !state
            .last_set
            .is_some_and(|last_set| same(current, last_set, epsilon))
    {
        return Reconciliation::ExternalChange;
    }
    Reconciliation::Stable
}

pub fn prepare_write(
    state: &mut MinimumSocControlState,
    current: f64,
    intended: f64,
    kind: MinimumSocWriteKind,
) {
    if !state.owned {
        state.owned = true;
        state.external_baseline = Some(current);
        state.last_set = None;
    }
    let generation = state.write_generation.wrapping_add(1).max(1);
    state.write_generation = generation;
    state.pending_write = Some(PendingMinimumSocWrite {
        generation,
        kind,
        expected_before: current,
        intended,
    });
}

pub fn commit_pending(state: &mut MinimumSocControlState) {
    let Some(pending) = state.pending_write else {
        return;
    };
    match pending.kind {
        MinimumSocWriteKind::Apply => {
            state.owned = true;
            state.last_set = Some(pending.intended);
            state.pending_write = None;
        }
        MinimumSocWriteKind::Restore => release_ownership(state),
    }
}

pub fn release_ownership(state: &mut MinimumSocControlState) {
    let generation = state.write_generation;
    *state = MinimumSocControlState {
        write_generation: generation,
        ..MinimumSocControlState::default()
    };
}

#[must_use]
pub const fn restore_target(state: &MinimumSocControlState) -> Option<f64> {
    state.external_baseline
}

#[must_use]
pub fn same(left: f64, right: f64, epsilon: f64) -> bool {
    left.is_finite()
        && right.is_finite()
        && epsilon.is_finite()
        && epsilon >= 0.0
        && (left - right).abs() <= epsilon
}

#[cfg(test)]
mod tests {
    use super::{
        Reconciliation, commit_pending, prepare_write, reconcile, release_ownership, restore_target,
    };
    use crate::domain::{MinimumSocControlState, MinimumSocWriteKind};

    #[test]
    fn first_write_captures_the_external_baseline() {
        let mut state = MinimumSocControlState::default();

        prepare_write(&mut state, 15.0, 45.0, MinimumSocWriteKind::Apply);

        assert!(state.owned);
        assert_eq!(restore_target(&state), Some(15.0));
        assert_eq!(
            reconcile(&state, 15.0, 0.01),
            Reconciliation::Retry(MinimumSocWriteKind::Apply)
        );
        assert_eq!(
            reconcile(&state, 45.0, 0.01),
            Reconciliation::Applied(MinimumSocWriteKind::Apply)
        );
    }

    #[test]
    fn committed_write_detects_later_external_control() {
        let mut state = MinimumSocControlState::default();
        prepare_write(&mut state, 15.0, 45.0, MinimumSocWriteKind::Apply);
        commit_pending(&mut state);

        assert_eq!(reconcile(&state, 45.0, 0.01), Reconciliation::Stable);
        assert_eq!(
            reconcile(&state, 20.0, 0.01),
            Reconciliation::ExternalChange
        );
    }

    #[test]
    fn restore_commit_clears_ownership_but_keeps_the_generation() {
        let mut state = MinimumSocControlState::default();
        prepare_write(&mut state, 15.0, 45.0, MinimumSocWriteKind::Apply);
        commit_pending(&mut state);
        prepare_write(&mut state, 45.0, 15.0, MinimumSocWriteKind::Restore);
        let generation = state.write_generation;

        commit_pending(&mut state);

        assert!(!state.owned);
        assert_eq!(state.write_generation, generation);
        assert_eq!(restore_target(&state), None);
    }

    #[test]
    fn releasing_external_control_preserves_only_the_generation() {
        let mut state = MinimumSocControlState::default();
        prepare_write(&mut state, 15.0, 45.0, MinimumSocWriteKind::Apply);
        let generation = state.write_generation;

        release_ownership(&mut state);

        assert_eq!(state.write_generation, generation);
        assert!(!state.owned);
        assert!(state.pending_write.is_none());
    }
}

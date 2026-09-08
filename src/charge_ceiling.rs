//! Volatile calendar-day policy for routine and near-full charging ceilings.

use time::{Date, Month};

use crate::clock::LocalDateTime;
use crate::config::PolicyConfig;
use crate::domain::ChargeCeilingState;

#[derive(Clone, Copy, Debug, PartialEq)]
// Permission, calendar due status, and transition notifications are independent outputs.
#[allow(clippy::struct_excessive_bools)]
pub struct ChargeCeilingEvaluation {
    pub ceiling_soc: f64,
    pub full_charge_permitted: bool,
    pub full_charge_due: bool,
    pub age_days: u16,
    pub state_changed: bool,
    pub near_full_observed: bool,
}

#[must_use]
pub fn evaluate(
    policy: &PolicyConfig,
    state: &mut ChargeCeilingState,
    now: LocalDateTime,
    current_soc: f64,
    monotonic_seconds: f64,
) -> ChargeCeilingEvaluation {
    let Some(today) = calendar_day_number(now) else {
        interrupt_confirmation(state);
        return ChargeCeilingEvaluation {
            ceiling_soc: policy.routine_max_charge_soc,
            full_charge_permitted: false,
            full_charge_due: false,
            age_days: 0,
            state_changed: false,
            near_full_observed: false,
        };
    };

    let mut state_changed = false;
    let reset_for_clock = state.observed_day.map_or_else(
        || state.reference_day.is_some(),
        |observed| !(0..=1).contains(&(today - observed)),
    ) || state
        .reference_day
        .is_some_and(|reference| reference > today);
    if state.reference_day.is_none() || reset_for_clock {
        state.reference_day = Some(today);
        state.full_charge_completed_day = None;
        state.full_charge_in_progress = false;
        state.near_full_latched = false;
        interrupt_confirmation(state);
        state_changed = true;
    }
    if state.observed_day != Some(today) {
        state.observed_day = Some(today);
        state_changed = true;
    }
    let age_before_observation = today.saturating_sub(state.reference_day.unwrap_or(today));
    let was_due = usize::try_from(age_before_observation)
        .is_ok_and(|age| age > policy.full_charge_min_age_days);
    if state.near_full_latched
        && (state.full_charge_completed_day.is_none()
            || current_soc < policy.full_charge_reached_soc - policy.soc_hysteresis)
    {
        state.near_full_latched = false;
        state_changed = true;
    }
    if was_due || (!state.near_full_latched && current_soc >= policy.full_charge_reached_soc) {
        state.full_charge_in_progress = true;
    }
    let near_full_observed = confirm_full_charge(policy, state, current_soc, monotonic_seconds);
    if near_full_observed {
        state.reference_day = Some(today);
        state.full_charge_completed_day = Some(today);
        state.full_charge_in_progress = false;
        state.near_full_latched = true;
        state_changed = true;
    }

    let elapsed = today.saturating_sub(state.reference_day.unwrap_or(today));
    let age_days = u16::try_from(elapsed).unwrap_or(u16::MAX);
    let full_charge_due = usize::from(age_days) > policy.full_charge_min_age_days;
    let full_charge_permitted = full_charge_due
        || state.full_charge_in_progress
        || state.full_charge_completed_day == Some(today);
    ChargeCeilingEvaluation {
        ceiling_soc: if full_charge_permitted {
            policy.full_max_charge_soc
        } else {
            policy.routine_max_charge_soc
        },
        full_charge_permitted,
        full_charge_due,
        age_days,
        state_changed,
        near_full_observed,
    }
}

/// Missing telemetry interrupts the continuous hold, not an unfinished full-charge permission.
pub const fn interrupt_confirmation(state: &mut ChargeCeilingState) {
    state.near_full_since_monotonic = None;
    state.near_full_last_sample_monotonic = None;
}

fn confirm_full_charge(
    policy: &PolicyConfig,
    state: &mut ChargeCeilingState,
    soc: f64,
    monotonic: f64,
) -> bool {
    if !state.full_charge_in_progress
        || !soc.is_finite()
        || soc < policy.full_charge_reached_soc
        || !monotonic.is_finite()
    {
        interrupt_confirmation(state);
        return false;
    }
    let continuous = state.near_full_last_sample_monotonic.is_some_and(|last| {
        (0.0..=policy.full_charge_max_sample_gap_seconds).contains(&(monotonic - last))
    });
    if !continuous {
        state.near_full_since_monotonic = Some(monotonic);
    }
    state.near_full_last_sample_monotonic = Some(monotonic);
    if state
        .near_full_since_monotonic
        .is_some_and(|since| monotonic - since >= policy.full_charge_confirm_seconds)
    {
        interrupt_confirmation(state);
        return true;
    }
    false
}

#[must_use]
pub fn calendar_day_number(value: LocalDateTime) -> Option<i32> {
    let month = Month::try_from(value.month).ok()?;
    Date::from_calendar_date(value.year, month, value.day)
        .ok()
        .map(Date::to_julian_day)
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn date(day: u8) -> LocalDateTime {
        LocalDateTime {
            year: 2026,
            month: 9,
            day,
            hour: 12,
            minute: 0,
            second: 0,
        }
    }

    #[test]
    fn full_charge_is_due_only_after_more_than_two_calendar_days() {
        let policy = PolicyConfig::default();
        let mut state = ChargeCeilingState::default();

        for (day, expected_age) in [(1, 0), (2, 1), (3, 2)] {
            let result = evaluate(&policy, &mut state, date(day), 50.0, 0.0);
            assert_eq!(result.age_days, expected_age);
            assert!(!result.full_charge_due);
            assert_eq!(result.ceiling_soc.to_bits(), 90.0_f64.to_bits());
        }

        let due = evaluate(&policy, &mut state, date(4), 50.0, 0.0);
        assert_eq!(due.age_days, 3);
        assert!(due.full_charge_due);
        assert_eq!(due.ceiling_soc.to_bits(), 100.0_f64.to_bits());
    }

    #[test]
    fn two_hours_confirm_full_charge_but_keep_permission_until_tomorrow() {
        let policy = PolicyConfig::default();
        let mut state = ChargeCeilingState::default();
        for minute in 0..120 {
            let result = evaluate(&policy, &mut state, date(5), 99.0, f64::from(minute * 60));
            assert!(!result.near_full_observed);
            assert!(result.full_charge_permitted);
        }
        assert!(!evaluate(&policy, &mut state, date(5), 99.0, 7_199.0).near_full_observed);
        let result = evaluate(&policy, &mut state, date(5), 100.0, 7_200.0);
        assert!(result.near_full_observed);
        assert_eq!(result.age_days, 0);
        assert!(!result.full_charge_due);
        assert!(result.full_charge_permitted);
        let later = evaluate(&policy, &mut state, date(5), 100.0, 20_000.0);
        assert!(later.full_charge_permitted);
        assert!(!later.near_full_observed);
        let result = evaluate(&policy, &mut state, date(6), 100.0, 86_400.0);
        assert!(!result.full_charge_permitted);
        assert_eq!(result.ceiling_soc.to_bits(), 90.0_f64.to_bits());
    }

    #[test]
    fn restart_state_starts_at_calendar_day_zero() {
        let policy = PolicyConfig::default();
        let mut state = ChargeCeilingState::default();
        let result = evaluate(&policy, &mut state, date(20), 50.0, 0.0);
        assert_eq!(result.age_days, 0);
        assert!(!result.full_charge_due);
        assert!(result.state_changed);
    }

    #[test]
    fn backwards_calendar_jump_restarts_the_counter_safely() {
        let policy = PolicyConfig::default();
        let mut state = ChargeCeilingState {
            reference_day: calendar_day_number(date(20)),
            observed_day: calendar_day_number(date(20)),
            near_full_latched: false,
            ..ChargeCeilingState::default()
        };
        let result = evaluate(&policy, &mut state, date(19), 50.0, 0.0);
        assert_eq!(result.age_days, 0);
        assert!(!result.full_charge_due);
        assert!(result.state_changed);
    }

    #[test]
    fn multi_day_forward_clock_jump_restarts_the_counter_safely() {
        let policy = PolicyConfig::default();
        let mut state = ChargeCeilingState {
            reference_day: calendar_day_number(date(1)),
            observed_day: calendar_day_number(date(2)),
            near_full_latched: false,
            ..ChargeCeilingState::default()
        };
        let result = evaluate(&policy, &mut state, date(20), 50.0, 0.0);
        assert_eq!(result.age_days, 0);
        assert!(!result.full_charge_due);
        assert!(result.state_changed);
    }

    #[test]
    fn unchanged_near_full_soc_does_not_reset_the_day_each_day() {
        let policy = PolicyConfig::default();
        let mut state = ChargeCeilingState {
            reference_day: calendar_day_number(date(1)),
            observed_day: calendar_day_number(date(3)),
            near_full_latched: true,
            ..ChargeCeilingState::default()
        };
        let result = evaluate(&policy, &mut state, date(4), 99.0, 0.0);
        assert!(result.full_charge_due);
        assert_eq!(result.age_days, 3);
        assert!(!result.near_full_observed);
    }

    #[test]
    fn a_new_due_episode_also_requires_two_hours_when_soc_remains_high() {
        let policy = PolicyConfig::default();
        let mut state = ChargeCeilingState {
            reference_day: calendar_day_number(date(1)),
            observed_day: calendar_day_number(date(3)),
            near_full_latched: true,
            ..ChargeCeilingState::default()
        };
        for minute in 0..120 {
            let result = evaluate(&policy, &mut state, date(4), 99.0, f64::from(minute * 60));
            assert!(result.full_charge_due);
            assert!(!result.near_full_observed);
        }
        let result = evaluate(&policy, &mut state, date(4), 99.0, 7_200.0);
        assert!(!result.full_charge_due);
        assert_eq!(result.age_days, 0);
        assert!(result.near_full_observed);
    }

    #[test]
    fn below_99_percent_never_counts_as_full() {
        let policy = PolicyConfig::default();
        let mut state = ChargeCeilingState::default();
        for minute in 0..=240 {
            let result = evaluate(&policy, &mut state, date(1), 98.99, f64::from(minute * 60));
            assert!(!result.near_full_observed);
        }
        assert_eq!(state.full_charge_completed_day, None);
    }

    #[test]
    fn soc_dip_or_missing_data_restarts_the_continuous_hold() {
        let policy = PolicyConfig::default();
        for interruption in [Some(98.99), Some(f64::NAN), None] {
            let mut state = ChargeCeilingState::default();
            for minute in 0..120 {
                let _ = evaluate(&policy, &mut state, date(1), 99.0, f64::from(minute * 60));
            }
            if let Some(soc) = interruption {
                let _ = evaluate(&policy, &mut state, date(1), soc, 7_200.0);
            } else {
                interrupt_confirmation(&mut state);
            }
            for minute in 121..241 {
                let result = evaluate(&policy, &mut state, date(1), 99.0, f64::from(minute * 60));
                assert!(!result.near_full_observed);
            }
            assert!(evaluate(&policy, &mut state, date(1), 99.0, 14_460.0).near_full_observed);
        }
    }

    #[test]
    fn large_gap_and_backwards_monotonic_time_cannot_complete_a_hold() {
        let policy = PolicyConfig::default();
        for next in [7_200.0, -60.0, f64::NAN] {
            let mut state = ChargeCeilingState::default();
            let _ = evaluate(&policy, &mut state, date(1), 99.0, 0.0);
            assert!(!evaluate(&policy, &mut state, date(1), 99.0, next).near_full_observed);
        }
    }

    #[test]
    fn crossing_midnight_uses_the_confirmation_day_not_the_start_day() {
        let policy = PolicyConfig::default();
        let mut state = ChargeCeilingState::default();
        for minute in 0..=120 {
            let day = if minute < 60 { 1 } else { 2 };
            let result = evaluate(&policy, &mut state, date(day), 99.0, f64::from(minute * 60));
            assert_eq!(result.near_full_observed, minute == 120);
            assert!(result.full_charge_permitted);
        }
        assert_eq!(
            state.full_charge_completed_day,
            calendar_day_number(date(2))
        );
        assert!(!evaluate(&policy, &mut state, date(3), 100.0, 86_400.0).full_charge_permitted);
    }

    #[test]
    fn restarting_drops_unconfirmed_duration_but_preserves_a_confirmed_day() {
        let policy = PolicyConfig::default();
        let mut state = ChargeCeilingState::default();
        let _ = evaluate(&policy, &mut state, date(1), 99.0, 0.0);
        let encoded = serde_json::to_string(&state).unwrap_or_else(|_| std::process::abort());
        let mut restored: ChargeCeilingState =
            serde_json::from_str(&encoded).unwrap_or_else(|_| std::process::abort());
        assert_eq!(restored.near_full_since_monotonic, None);
        assert!(!evaluate(&policy, &mut restored, date(1), 99.0, 7_200.0).near_full_observed);
        restored.full_charge_completed_day = calendar_day_number(date(1));
        restored.near_full_latched = true;
        let encoded = serde_json::to_string(&restored).unwrap_or_else(|_| std::process::abort());
        let mut restored: ChargeCeilingState =
            serde_json::from_str(&encoded).unwrap_or_else(|_| std::process::abort());
        assert!(evaluate(&policy, &mut restored, date(1), 100.0, 7_260.0).full_charge_permitted);
        assert!(!evaluate(&policy, &mut restored, date(2), 100.0, 86_400.0).full_charge_permitted);
    }

    #[test]
    fn an_old_single_sample_latch_does_not_count_as_two_hours() {
        let mut state = ChargeCeilingState {
            reference_day: calendar_day_number(date(1)),
            observed_day: calendar_day_number(date(1)),
            near_full_latched: true,
            ..ChargeCeilingState::default()
        };
        let result = evaluate(&PolicyConfig::default(), &mut state, date(1), 99.0, 0.0);
        assert!(result.full_charge_permitted);
        assert!(!result.near_full_observed);
        assert!(!state.near_full_latched);
    }

    #[test]
    fn an_unconfirmed_charge_keeps_permission_across_midnight() {
        let policy = PolicyConfig::default();
        let mut state = ChargeCeilingState::default();
        let _ = evaluate(&policy, &mut state, date(1), 99.0, 0.0);
        let result = evaluate(&policy, &mut state, date(2), 98.0, 60.0);
        assert!(result.full_charge_permitted);
        assert!(!result.near_full_observed);
        assert_eq!(state.full_charge_completed_day, None);
    }
}

//! Volatile calendar-day policy for routine and near-full charging ceilings.

use time::{Date, Month};

use crate::clock::LocalDateTime;
use crate::config::PolicyConfig;
use crate::domain::ChargeCeilingState;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ChargeCeilingEvaluation {
    pub ceiling_soc: f64,
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
    battery_power_w: Option<f64>,
) -> ChargeCeilingEvaluation {
    let Some(today) = calendar_day_number(now) else {
        return ChargeCeilingEvaluation {
            ceiling_soc: policy.routine_max_charge_soc,
            full_charge_due: false,
            age_days: 0,
            state_changed: false,
            near_full_observed: false,
        };
    };

    let mut state_changed = false;
    let mut near_full_observed = false;
    let reset_for_clock = state.observed_day.map_or_else(
        || state.reference_day.is_some(),
        |observed| !(0..=1).contains(&(today - observed)),
    ) || state
        .reference_day
        .is_some_and(|reference| reference > today);
    if state.reference_day.is_none() || reset_for_clock {
        state.reference_day = Some(today);
        state_changed = true;
    }
    if state.observed_day != Some(today) {
        state.observed_day = Some(today);
        state_changed = true;
    }
    let age_before_observation = today.saturating_sub(state.reference_day.unwrap_or(today));
    let was_due = usize::try_from(age_before_observation)
        .is_ok_and(|age| age > policy.full_charge_min_age_days);
    let newly_near_full = current_soc >= policy.full_charge_reached_soc
        && (!state.near_full_latched
            || (was_due && battery_power_w.is_some_and(|power| power > 0.0)));
    if newly_near_full {
        state.reference_day = Some(today);
        state.near_full_latched = true;
        state_changed = true;
        near_full_observed = true;
    } else if state.near_full_latched
        && current_soc < policy.full_charge_reached_soc - policy.soc_hysteresis
    {
        state.near_full_latched = false;
        state_changed = true;
    }

    let elapsed = today.saturating_sub(state.reference_day.unwrap_or(today));
    let age_days = u16::try_from(elapsed).unwrap_or(u16::MAX);
    let full_charge_due = usize::from(age_days) > policy.full_charge_min_age_days;
    ChargeCeilingEvaluation {
        ceiling_soc: if full_charge_due {
            policy.full_max_charge_soc
        } else {
            policy.routine_max_charge_soc
        },
        full_charge_due,
        age_days,
        state_changed,
        near_full_observed,
    }
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
            let result = evaluate(&policy, &mut state, date(day), 50.0, None);
            assert_eq!(result.age_days, expected_age);
            assert!(!result.full_charge_due);
            assert_eq!(result.ceiling_soc.to_bits(), 90.0_f64.to_bits());
        }

        let due = evaluate(&policy, &mut state, date(4), 50.0, None);
        assert_eq!(due.age_days, 3);
        assert!(due.full_charge_due);
        assert_eq!(due.ceiling_soc.to_bits(), 100.0_f64.to_bits());
    }

    #[test]
    fn near_full_sample_resets_the_calendar_counter() {
        let policy = PolicyConfig::default();
        let mut state = ChargeCeilingState {
            reference_day: calendar_day_number(date(1)),
            observed_day: calendar_day_number(date(1)),
            near_full_latched: false,
        };
        let result = evaluate(&policy, &mut state, date(5), 98.0, Some(100.0));
        assert!(result.near_full_observed);
        assert_eq!(result.age_days, 0);
        assert!(!result.full_charge_due);
        assert_eq!(result.ceiling_soc.to_bits(), 90.0_f64.to_bits());
    }

    #[test]
    fn restart_state_starts_at_calendar_day_zero() {
        let policy = PolicyConfig::default();
        let mut state = ChargeCeilingState::default();
        let result = evaluate(&policy, &mut state, date(20), 50.0, None);
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
        };
        let result = evaluate(&policy, &mut state, date(19), 50.0, None);
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
        };
        let result = evaluate(&policy, &mut state, date(20), 50.0, None);
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
        };
        let result = evaluate(&policy, &mut state, date(4), 98.5, None);
        assert!(result.full_charge_due);
        assert_eq!(result.age_days, 3);
        assert!(!result.near_full_observed);
    }

    #[test]
    fn charging_while_due_counts_as_a_new_near_full_charge() {
        let policy = PolicyConfig::default();
        let mut state = ChargeCeilingState {
            reference_day: calendar_day_number(date(1)),
            observed_day: calendar_day_number(date(3)),
            near_full_latched: true,
        };
        let result = evaluate(&policy, &mut state, date(4), 99.0, Some(50.0));
        assert!(!result.full_charge_due);
        assert_eq!(result.age_days, 0);
        assert!(result.near_full_observed);
    }
}

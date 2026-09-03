//! Pure seasonal, timing, PV, balancing, and charge-current policy.

use crate::clock::LocalDateTime;
use crate::config::{
    PV_OBSERVATION_END_HOUR, PV_OBSERVATION_START_HOUR, PolicyConfig, SECONDS_PER_DAY,
    SECONDS_PER_HOUR,
};
use crate::domain::{ControllerState, PvPower, TargetMode};

#[must_use]
pub const fn is_winter_mmdd(policy: &PolicyConfig, mmdd: u16) -> bool {
    mmdd >= policy.winter_start_mmdd || mmdd <= policy.winter_end_mmdd
}

#[must_use]
pub const fn is_transition_mmdd(policy: &PolicyConfig, mmdd: u16) -> bool {
    (mmdd >= policy.transition_pre_start_mmdd && mmdd <= policy.transition_pre_end_mmdd)
        || (mmdd >= policy.transition_post_start_mmdd && mmdd <= policy.transition_post_end_mmdd)
}

#[must_use]
pub const fn is_sd_window(policy: &PolicyConfig, mmdd: u16) -> bool {
    is_winter_mmdd(policy, mmdd) || is_transition_mmdd(policy, mmdd)
}

#[must_use]
pub fn history_below_threshold(policy: &PolicyConfig, history: &[f64]) -> bool {
    transition_history(policy, history)
        .is_some_and(|values| values.iter().all(|value| *value < policy.pv_threshold_w))
}

#[must_use]
pub fn history_above_threshold(policy: &PolicyConfig, history: &[f64]) -> bool {
    transition_history(policy, history)
        .is_some_and(|values| values.iter().all(|value| *value > policy.pv_threshold_w))
}

fn transition_history<'a>(policy: &PolicyConfig, history: &'a [f64]) -> Option<&'a [f64]> {
    (history.len() >= policy.transition_days)
        .then(|| &history[history.len() - policy.transition_days..])
}

#[must_use]
pub fn determine_target(
    policy: &PolicyConfig,
    state: &mut ControllerState,
    now: LocalDateTime,
    now_epoch_ts: f64,
    monotonic_now: f64,
) -> TargetMode {
    let mmdd = now.mmdd();
    if is_winter_mmdd(policy, mmdd) {
        if should_start_balancing(policy, state, now_epoch_ts, monotonic_now, true) {
            start_balancing(state, monotonic_now);
        }
        if state.balancing_active {
            return TargetMode {
                target_soc: policy.balancing_target_soc,
                mode: "Winter Balancing",
            };
        }
        return TargetMode {
            target_soc: policy.winter_target_soc,
            mode: "Winter",
        };
    }
    if (policy.transition_pre_start_mmdd..=policy.transition_pre_end_mmdd).contains(&mmdd) {
        if history_below_threshold(policy, &state.pv_history) {
            return TargetMode {
                target_soc: policy.transition_guard_soc,
                mode: "Pre-Winter Low PV",
            };
        }
        return default_target(policy);
    }
    if (policy.transition_post_start_mmdd..=policy.transition_post_end_mmdd).contains(&mmdd) {
        if history_above_threshold(policy, &state.pv_history) {
            return TargetMode {
                target_soc: policy.summer_min_soc,
                mode: "Post-Winter PV Recovered",
            };
        }
        return TargetMode {
            target_soc: policy.transition_guard_soc,
            mode: "Post-Winter Guard",
        };
    }
    default_target(policy)
}

const fn default_target(policy: &PolicyConfig) -> TargetMode {
    TargetMode {
        target_soc: policy.summer_min_soc,
        mode: "Default",
    }
}

#[must_use]
pub fn bounded_loop_delta(state: &ControllerState, now_ts: f64, loop_interval_seconds: f64) -> f64 {
    if state.last_loop_ts <= 0.0 {
        return 0.0;
    }
    let delta = now_ts - state.last_loop_ts;
    if (0.0..=loop_interval_seconds * 5.0).contains(&delta) {
        delta
    } else {
        0.0
    }
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TrackingResult {
    pub changed: bool,
    pub force_persist: bool,
    pub balancing_completed: bool,
    pub balancing_timeout_phase: Option<BalancingTimeoutPhase>,
    pub entered_high_soc_hold: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BalancingPhase {
    Idle,
    ApproachToFull,
    HighSocHold,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BalancingTimeoutPhase {
    ApproachToFull,
    HighSocHold,
}

#[must_use]
pub const fn balancing_phase(state: &ControllerState) -> BalancingPhase {
    if !state.balancing_active {
        BalancingPhase::Idle
    } else if state.balancing_high_soc_start_ts <= 0.0 {
        BalancingPhase::ApproachToFull
    } else {
        BalancingPhase::HighSocHold
    }
}

pub fn update_full_and_balancing_tracking(
    policy: &PolicyConfig,
    state: &mut ControllerState,
    current_soc: f64,
    now: LocalDateTime,
    now_epoch_ts: f64,
    monotonic_now: f64,
    loop_interval_seconds: f64,
) -> TrackingResult {
    let delta = bounded_loop_delta(state, monotonic_now, loop_interval_seconds);
    state.last_loop_ts = monotonic_now;
    let mut result = TrackingResult::default();

    if is_winter_mmdd(policy, now.mmdd()) && current_soc >= policy.balancing_full_soc {
        let previous = state.full_soc_seconds;
        state.full_soc_seconds += delta;
        let threshold = policy.full_soc_confirm_minutes * 60.0;
        if previous < threshold && state.full_soc_seconds >= threshold {
            state.last_full_ts = now_epoch_ts;
            result.changed = true;
            result.force_persist = true;
        }
    } else if state.full_soc_seconds != 0.0 {
        state.full_soc_seconds = 0.0;
        result.changed = true;
    }

    if !state.balancing_active {
        return result;
    }

    if state.balancing_high_soc_start_ts <= 0.0 {
        if current_soc >= policy.balancing_full_soc {
            state.balancing_high_soc_start_ts = monotonic_now;
            state.balance_full_seconds = 0.0;
            result.changed = true;
            result.entered_high_soc_hold = true;
        } else if monotonic_now - state.balancing_start_ts
            > policy.balancing_approach_max_hours * SECONDS_PER_HOUR
        {
            finish_balancing(state, now_epoch_ts, monotonic_now, false);
            result.changed = true;
            result.force_persist = true;
            result.balancing_timeout_phase = Some(BalancingTimeoutPhase::ApproachToFull);
            return result;
        }
    }

    if state.balancing_high_soc_start_ts > 0.0
        && monotonic_now - state.balancing_high_soc_start_ts
            > policy.balancing_max_hours * SECONDS_PER_HOUR
    {
        finish_balancing(state, now_epoch_ts, monotonic_now, false);
        result.changed = true;
        result.force_persist = true;
        result.balancing_timeout_phase = Some(BalancingTimeoutPhase::HighSocHold);
        return result;
    }

    if current_soc >= policy.balancing_full_soc && !result.entered_high_soc_hold {
        state.balance_full_seconds += delta;
    } else if state.balance_full_seconds != 0.0 {
        state.balance_full_seconds = 0.0;
        result.changed = true;
    }
    if state.balance_full_seconds >= policy.balancing_duration_hours * SECONDS_PER_HOUR {
        finish_balancing(state, now_epoch_ts, monotonic_now, true);
        result.changed = true;
        result.force_persist = true;
        result.balancing_completed = true;
    }
    result
}

#[must_use]
pub fn should_start_balancing(
    policy: &PolicyConfig,
    state: &ControllerState,
    now_epoch_ts: f64,
    monotonic_now: f64,
    winter: bool,
) -> bool {
    if state.balancing_active {
        return false;
    }
    let source = if state.last_balance_ts > 0.0 {
        state.last_balance_ts
    } else if winter && state.last_full_ts > 0.0 {
        state.last_full_ts
    } else {
        0.0
    };
    let using_boot_grace = source <= 0.0;
    let required = if using_boot_grace {
        policy.balancing_boot_grace_hours * SECONDS_PER_HOUR
    } else {
        policy.balancing_interval_days * SECONDS_PER_DAY
    };
    let due = if using_boot_grace {
        monotonic_now - state.boot_ts >= required
    } else {
        now_epoch_ts - source >= required
    };
    let cooling_down = state.last_balance_attempt_ts > 0.0
        && monotonic_now - state.last_balance_attempt_ts
            < policy.balancing_retry_cooldown_hours * SECONDS_PER_HOUR;
    due && !cooling_down
}

pub const fn start_balancing(state: &mut ControllerState, now_ts: f64) {
    state.balancing_active = true;
    state.balancing_start_ts = now_ts;
    state.balancing_high_soc_start_ts = 0.0;
    state.balance_full_seconds = 0.0;
    state.last_balance_attempt_ts = now_ts;
}

const fn finish_balancing(
    state: &mut ControllerState,
    now_epoch_ts: f64,
    monotonic_now: f64,
    success: bool,
) {
    state.balancing_active = false;
    state.balancing_start_ts = 0.0;
    state.balancing_high_soc_start_ts = 0.0;
    if success {
        state.last_balance_ts = now_epoch_ts;
    }
    state.balance_full_seconds = 0.0;
    state.last_balance_attempt_ts = monotonic_now;
}

#[must_use]
pub fn needs_charge(policy: &PolicyConfig, current_soc: f64, target_soc: f64) -> bool {
    current_soc < target_soc - policy.soc_hysteresis
}

pub fn track_charge_deficit(state: &mut ControllerState, needs_charge: bool, now_ts: f64) -> bool {
    if needs_charge && state.charge_deficit_start_ts <= 0.0 {
        state.charge_deficit_start_ts = now_ts;
        true
    } else if !needs_charge && state.charge_deficit_start_ts > 0.0 {
        state.charge_deficit_start_ts = 0.0;
        true
    } else {
        false
    }
}

#[must_use]
pub fn charge_window_hours(policy: &PolicyConfig, state: &ControllerState, now_ts: f64) -> u8 {
    if state.charge_deficit_start_ts <= 0.0 {
        return policy.charge_window_base_hours;
    }
    let elapsed_nights =
        ((now_ts - state.charge_deficit_start_ts).max(0.0) / SECONDS_PER_DAY).floor();
    let mut escalation_steps = (elapsed_nights / policy.charge_window_escalation_nights).floor();
    let mut multiplier = 1_u8;
    while escalation_steps >= 1.0 && multiplier < policy.charge_window_max_multiplier {
        multiplier = multiplier
            .saturating_mul(2)
            .min(policy.charge_window_max_multiplier);
        escalation_steps -= 1.0;
    }
    policy.charge_window_base_hours.saturating_mul(multiplier)
}

#[must_use]
pub fn is_charge_window_active(
    policy: &PolicyConfig,
    state: &ControllerState,
    now: LocalDateTime,
    now_ts: f64,
) -> bool {
    let duration = charge_window_hours(policy, state, now_ts);
    if duration >= 24 {
        return true;
    }
    let hour_delta = (24 + i16::from(now.hour) - i16::from(policy.charge_window_start_hour)) % 24;
    let hours_since_start = f64::from(hour_delta)
        + f64::from(now.minute) / 60.0
        + f64::from(now.second) / SECONDS_PER_HOUR;
    hours_since_start < f64::from(duration)
}

#[must_use]
pub const fn pause_soc(current_soc: f64, target_soc: f64, summer_min_soc: f64) -> f64 {
    current_soc.min(target_soc).max(summer_min_soc)
}

#[must_use]
pub const fn import_only(grid_power_net_w: f64) -> f64 {
    grid_power_net_w.max(0.0)
}

#[must_use]
pub fn house_load_fallback(grid_power_net_w: f64, battery_power_w: f64) -> f64 {
    (grid_power_net_w + battery_power_w.max(0.0)).max(0.0)
}

#[must_use]
pub fn available_grid_charge_power(policy: &PolicyConfig, house_load_w: f64) -> f64 {
    (policy.grid_load_limit_w - house_load_w - policy.grid_pause_headroom_w).max(0.0)
}

#[must_use]
pub fn compute_charge_current_limit(
    policy: &PolicyConfig,
    house_load_w: f64,
    battery_max_current_a: Option<f64>,
    vebus_max_charge_current_a: Option<f64>,
    battery_voltage_v: Option<f64>,
    pv_power: PvPower,
    normal_current_a: Option<f64>,
) -> Option<f64> {
    let battery_max = battery_max_current_a?;
    let vebus_max = vebus_max_charge_current_a?;
    if battery_max < 0.0 || !battery_max.is_finite() || vebus_max < 0.0 || !vebus_max.is_finite() {
        return None;
    }
    let equipment_max = battery_max.min(vebus_max);
    if equipment_max == 0.0 {
        return Some(0.0);
    }
    let grid_charge_cap = (equipment_max * policy.grid_charge_max_fraction).floor();
    let mut current = if let Some(voltage) = battery_voltage_v.filter(|value| *value > 1.0) {
        let by_grid =
            available_grid_charge_power(policy, house_load_w) * policy.charge_efficiency / voltage;
        let minimum_progress = policy.grid_soft_min_charge_current_a.min(grid_charge_cap);
        let grid_current = grid_charge_cap.min(by_grid.max(minimum_progress));
        let pv_current = pv_power
            .ac_w
            .max(0.0)
            .mul_add(policy.charge_efficiency, pv_power.dc_w.max(0.0))
            / voltage;
        (grid_current + pv_current).min(equipment_max).floor()
    } else {
        policy.safe_charge_current_a?.min(grid_charge_cap)
    };
    if let Some(normal) = normal_current_a.filter(|value| value.is_finite() && *value >= 0.0) {
        if current > normal {
            current = normal;
        }
    }
    Some(current.max(0.0).floor())
}

pub fn integrate_pv_sample(
    state: &mut ControllerState,
    now_ts: f64,
    pv_total_w: f64,
    loop_interval_seconds: f64,
) {
    if state.pv_last_sample_ts <= 0.0 {
        return;
    }
    let delta = now_ts - state.pv_last_sample_ts;
    if delta > 0.0 && delta <= loop_interval_seconds * 5.0 {
        let average_power = f64::midpoint(state.pv_last_sample_power, pv_total_w);
        state.pv_energy_ws += average_power * delta;
        state.pv_time_s += delta;
    }
}

pub fn collect_pv_sample(
    state: &mut ControllerState,
    now_ts: f64,
    pv_total_w: f64,
    loop_interval_seconds: f64,
) {
    state.current_day_samples.push(pv_total_w);
    integrate_pv_sample(state, now_ts, pv_total_w, loop_interval_seconds);
    state.pv_last_sample_ts = now_ts;
    state.pv_last_sample_power = pv_total_w;
}

pub const fn reset_pv_gap(state: &mut ControllerState) {
    state.pv_last_sample_ts = 0.0;
    state.pv_last_sample_power = 0.0;
}

#[must_use]
pub fn completed_pv_average(
    policy: &PolicyConfig,
    state: &mut ControllerState,
    now_ts: f64,
) -> Option<f64> {
    let observation_hours = f64::from(PV_OBSERVATION_END_HOUR - PV_OBSERVATION_START_HOUR);
    let required_coverage_s =
        observation_hours * SECONDS_PER_HOUR * policy.pv_min_daily_coverage_fraction;
    if state.pv_time_s < required_coverage_s {
        return None;
    }
    state.last_pv_integral_ts = now_ts;
    Some(state.pv_energy_ws / state.pv_time_s)
}

pub fn roll_pv_day(
    policy: &PolicyConfig,
    state: &mut ControllerState,
    today: String,
    now_ts: f64,
) -> Option<f64> {
    let average = completed_pv_average(policy, state, now_ts);
    if let Some(value) = average {
        state.pv_history.push(value);
        if state.pv_history.len() > policy.transition_days {
            state.pv_history.remove(0);
        }
    }
    state.current_day_samples.clear();
    state.pv_energy_ws = 0.0;
    state.pv_time_s = 0.0;
    reset_pv_gap(state);
    state.last_sample_date = today;
    average
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn winter_noon() -> LocalDateTime {
        LocalDateTime {
            year: 2026,
            month: 1,
            day: 1,
            hour: 12,
            minute: 0,
            second: 0,
        }
    }

    #[test]
    fn threshold_comparisons_remain_strict() {
        let policy = PolicyConfig::default();
        assert!(!history_below_threshold(
            &policy,
            &[2_999.0, 2_999.0, 2_999.0, 3_000.0]
        ));
        assert!(!history_above_threshold(
            &policy,
            &[3_001.0, 3_001.0, 3_001.0, 3_000.0]
        ));
    }

    #[test]
    fn pv_integration_rejects_long_gaps() {
        let mut state = ControllerState {
            pv_last_sample_ts: 100.0,
            pv_last_sample_power: 10.0,
            ..ControllerState::default()
        };
        let loop_interval_seconds: f64 = 60.0;
        integrate_pv_sample(
            &mut state,
            loop_interval_seconds.mul_add(10.0, 100.0),
            20.0,
            loop_interval_seconds,
        );
        assert_eq!(state.pv_energy_ws.to_bits(), 0.0_f64.to_bits());
    }

    #[test]
    fn pv_daily_average_requires_six_integrated_hours_by_default() {
        let policy = PolicyConfig::default();
        let required_coverage_s = 6.0 * SECONDS_PER_HOUR;
        for observed_s in [60.0, required_coverage_s - 1.0] {
            let mut state = ControllerState {
                pv_energy_ws: 1_000.0 * observed_s,
                pv_time_s: observed_s,
                ..ControllerState::default()
            };
            assert_eq!(completed_pv_average(&policy, &mut state, 50_000.0), None);
        }

        let mut state = ControllerState {
            pv_energy_ws: 1_000.0 * required_coverage_s,
            pv_time_s: required_coverage_s,
            ..ControllerState::default()
        };
        assert_eq!(
            completed_pv_average(&policy, &mut state, 50_000.0),
            Some(1_000.0)
        );
        assert_eq!(state.last_pv_integral_ts.to_bits(), 50_000.0_f64.to_bits());
    }

    #[test]
    fn fully_observed_zero_pv_day_is_valid() {
        let policy = PolicyConfig::default();
        let mut state = ControllerState {
            pv_time_s: 6.0 * SECONDS_PER_HOUR,
            ..ControllerState::default()
        };

        assert_eq!(
            completed_pv_average(&policy, &mut state, 50_000.0),
            Some(0.0)
        );
    }

    #[test]
    fn legacy_samples_cannot_bypass_integrated_coverage() {
        let policy = PolicyConfig::default();
        let mut state = ControllerState {
            boot_ts: 1.0,
            current_day_samples: vec![2_000.0; 480],
            ..ControllerState::default()
        };

        assert_eq!(
            completed_pv_average(&policy, &mut state, 10.0 * SECONDS_PER_DAY),
            None
        );
    }

    #[test]
    fn epoch_jump_does_not_expire_monotonic_balancing_timeout() {
        let policy = PolicyConfig::default();
        let mut state = ControllerState {
            balancing_active: true,
            balancing_start_ts: 100.0,
            last_loop_ts: 100.0,
            ..ControllerState::default()
        };

        let result = update_full_and_balancing_tracking(
            &policy,
            &mut state,
            50.0,
            winter_noon(),
            2_000_000_000.0,
            101.0,
            60.0,
        );

        assert!(state.balancing_active);
        assert_eq!(result.balancing_timeout_phase, None);
    }

    #[test]
    fn approach_to_full_is_not_limited_by_the_high_soc_watchdog() {
        let policy = PolicyConfig::default();
        let started_at = 100.0;
        let now = (policy.balancing_max_hours + 1.0).mul_add(SECONDS_PER_HOUR, started_at);
        let mut state = ControllerState {
            balancing_active: true,
            balancing_start_ts: started_at,
            last_loop_ts: now - 60.0,
            ..ControllerState::default()
        };

        let result = update_full_and_balancing_tracking(
            &policy,
            &mut state,
            policy.balancing_full_soc - 1.0,
            winter_noon(),
            2_000_000_000.0,
            now,
            60.0,
        );

        assert_eq!(balancing_phase(&state), BalancingPhase::ApproachToFull);
        assert_eq!(result.balancing_timeout_phase, None);
    }

    #[test]
    fn approach_watchdog_aborts_only_after_its_own_deadline() {
        let policy = PolicyConfig::default();
        let started_at = 100.0;
        let now = policy
            .balancing_approach_max_hours
            .mul_add(SECONDS_PER_HOUR, started_at)
            + 1.0;
        let mut state = ControllerState {
            balancing_active: true,
            balancing_start_ts: started_at,
            last_loop_ts: now - 60.0,
            ..ControllerState::default()
        };

        let result = update_full_and_balancing_tracking(
            &policy,
            &mut state,
            policy.balancing_full_soc - 1.0,
            winter_noon(),
            2_000_000_000.0,
            now,
            60.0,
        );

        assert_eq!(
            result.balancing_timeout_phase,
            Some(BalancingTimeoutPhase::ApproachToFull)
        );
        assert_eq!(balancing_phase(&state), BalancingPhase::Idle);
    }

    #[test]
    fn reaching_full_threshold_starts_a_fresh_high_soc_phase() {
        let policy = PolicyConfig::default();
        let now = 60.0 * SECONDS_PER_HOUR;
        let mut state = ControllerState {
            balancing_active: true,
            balancing_start_ts: 100.0,
            last_loop_ts: now - 60.0,
            ..ControllerState::default()
        };

        let entered = update_full_and_balancing_tracking(
            &policy,
            &mut state,
            policy.balancing_full_soc,
            winter_noon(),
            2_000_000_000.0,
            now,
            60.0,
        );

        assert!(entered.entered_high_soc_hold);
        assert_eq!(balancing_phase(&state), BalancingPhase::HighSocHold);
        assert_eq!(state.balancing_high_soc_start_ts.to_bits(), now.to_bits());
        assert_eq!(state.balance_full_seconds.to_bits(), 0.0_f64.to_bits());

        update_full_and_balancing_tracking(
            &policy,
            &mut state,
            policy.balancing_full_soc,
            winter_noon(),
            2_000_000_060.0,
            now + 60.0,
            60.0,
        );
        assert_eq!(state.balance_full_seconds.to_bits(), 60.0_f64.to_bits());
    }

    #[test]
    fn a_soc_dip_restarts_continuous_hold_but_not_the_high_soc_watchdog() {
        let policy = PolicyConfig::default();
        let mut state = ControllerState {
            balancing_active: true,
            balancing_start_ts: 100.0,
            balancing_high_soc_start_ts: 1_000.0,
            balance_full_seconds: 3_000.0,
            last_loop_ts: 2_000.0,
            ..ControllerState::default()
        };

        let result = update_full_and_balancing_tracking(
            &policy,
            &mut state,
            policy.balancing_full_soc - 0.1,
            winter_noon(),
            2_000_000_000.0,
            2_060.0,
            60.0,
        );

        assert_eq!(result.balancing_timeout_phase, None);
        assert_eq!(balancing_phase(&state), BalancingPhase::HighSocHold);
        assert_eq!(
            state.balancing_high_soc_start_ts.to_bits(),
            1_000.0_f64.to_bits()
        );
        assert_eq!(state.balance_full_seconds.to_bits(), 0.0_f64.to_bits());
    }

    #[test]
    fn high_soc_watchdog_is_independent_of_the_approach_duration() {
        let policy = PolicyConfig::default();
        let high_soc_started_at = 200_000.0;
        let now = policy
            .balancing_max_hours
            .mul_add(SECONDS_PER_HOUR, high_soc_started_at)
            + 1.0;
        let mut state = ControllerState {
            balancing_active: true,
            balancing_start_ts: 100.0,
            balancing_high_soc_start_ts: high_soc_started_at,
            last_loop_ts: now - 60.0,
            ..ControllerState::default()
        };

        let result = update_full_and_balancing_tracking(
            &policy,
            &mut state,
            policy.balancing_full_soc - 0.1,
            winter_noon(),
            2_000_000_000.0,
            now,
            60.0,
        );

        assert_eq!(
            result.balancing_timeout_phase,
            Some(BalancingTimeoutPhase::HighSocHold)
        );
        assert_eq!(balancing_phase(&state), BalancingPhase::Idle);
    }

    #[test]
    fn boot_grace_and_retry_cooldown_use_monotonic_time() {
        let policy = PolicyConfig::default();
        let boot_state = ControllerState {
            boot_ts: 100.0,
            ..ControllerState::default()
        };
        assert!(!should_start_balancing(
            &policy,
            &boot_state,
            2_000_000_000.0,
            101.0,
            true,
        ));

        let cooldown_state = ControllerState {
            last_balance_ts: 1.0,
            last_balance_attempt_ts: 100.0,
            ..ControllerState::default()
        };
        assert!(!should_start_balancing(
            &policy,
            &cooldown_state,
            2_000_000_000.0,
            101.0,
            true,
        ));
    }

    #[test]
    fn zero_amp_hardware_limit_differs_from_an_unknown_limit() {
        let policy = PolicyConfig::default();
        assert_eq!(
            compute_charge_current_limit(
                &policy,
                500.0,
                Some(0.0),
                Some(35.0),
                Some(52.0),
                PvPower::default(),
                None,
            ),
            Some(0.0)
        );
        assert_eq!(
            compute_charge_current_limit(
                &policy,
                500.0,
                Some(200.0),
                Some(0.0),
                Some(52.0),
                PvPower::default(),
                None,
            ),
            Some(0.0)
        );
        assert_eq!(
            compute_charge_current_limit(
                &policy,
                500.0,
                Some(200.0),
                Some(35.0),
                Some(52.0),
                PvPower::default(),
                Some(0.0),
            ),
            Some(0.0)
        );
        assert_eq!(
            compute_charge_current_limit(
                &policy,
                500.0,
                None,
                Some(35.0),
                Some(52.0),
                PvPower::default(),
                None,
            ),
            None
        );
    }
}

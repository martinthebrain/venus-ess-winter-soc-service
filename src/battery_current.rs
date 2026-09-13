//! Bounded DC-current feedback; the discharge arbiter owns the ESS setting.

use crate::config::BatteryCurrentConfig;
use serde::Serialize;

#[derive(Clone, Debug, Default, Serialize, PartialEq)]
pub struct BatteryCurrentStatus {
    pub discharge_enabled: bool,
    pub charge_enabled: bool,
    pub configured_charge_current_a: Option<f64>,
    pub applied_charge_current_a: Option<f64>,
    pub charge_unenforced_reason: Option<String>,
    pub pv_export_setpoint_w: Option<f64>,
    pub pv_export_state: Option<String>,
    pub measured_charge_current_a: Option<f64>,
    pub generated_at: f64,
    pub enabled: bool,
    pub configured_current_a: Option<f64>,
    pub effective_current_a: Option<f64>,
    pub measured_discharge_current_a: Option<f64>,
    pub dc_pv_power_w: Option<f64>,
    pub power_constraint_w: Option<f64>,
    pub grid_connected: Option<bool>,
    pub applied_power_w: Option<f64>,
    pub unenforced_reason: Option<String>,
    pub reason: Option<&'static str>,
}

#[derive(Clone, Copy, Debug)]
pub struct CurrentSample {
    pub voltage_v: Option<f64>,
    pub battery_current_a: Option<f64>,
    pub dc_pv_w: Option<f64>,
    pub bms_limit_a: Option<f64>,
    pub grid_connected: Option<bool>,
    pub monotonic_now: f64,
}

#[derive(Default)]
pub struct CurrentFeedback {
    reduction_w: f64,
    last_increase: Option<f64>,
    last_sample: Option<f64>,
    last_constraint: Option<f64>,
    last_pv_w: f64,
}

impl CurrentFeedback {
    pub fn evaluate(
        &mut self,
        config: &BatteryCurrentConfig,
        sample: CurrentSample,
    ) -> BatteryCurrentStatus {
        let mut status = BatteryCurrentStatus {
            enabled: config.enabled,
            discharge_enabled: config.discharge_enabled(),
            configured_current_a: config
                .discharge_enabled()
                .then_some(config.max_discharge_current_a),
            grid_connected: sample.grid_connected,
            ..BatteryCurrentStatus::default()
        };
        if !config.discharge_enabled() {
            *self = Self::default();
            return status;
        }
        let current = bounded(sample.battery_current_a, -10_000.0, 10_000.0);
        let voltage = bounded(sample.voltage_v, 1.0, 1_000.0);
        let bms = bounded(sample.bms_limit_a, 0.0, 10_000.0);
        status.measured_discharge_current_a = current.map(|value| (-value).max(0.0));
        status.effective_current_a = bms.map(|limit| config.max_discharge_current_a.min(limit));
        status.dc_pv_power_w = bounded(sample.dc_pv_w, 0.0, 1_000_000.0);
        if sample.grid_connected != Some(true) {
            status.reason = Some("grid_not_connected_or_unknown");
        }
        // An old PV contribution must never survive a telemetry interruption.
        let (Some(voltage), Some(current), Some(limit)) =
            (voltage, current, status.effective_current_a)
        else {
            status.power_constraint_w = Some(0.0);
            status.reason = Some("battery_telemetry_unavailable");
            self.last_constraint = Some(0.0);
            self.last_pv_w = 0.0;
            return status;
        };
        let gap = self.last_sample.is_none_or(|last| {
            sample.monotonic_now < last
                || sample.monotonic_now - last > config.interval.as_secs_f64() * 3.0
        });
        if gap {
            self.reduction_w = 0.0;
            self.last_increase = Some(sample.monotonic_now);
        }
        self.last_sample = Some(sample.monotonic_now);
        let pv = status.dc_pv_power_w.unwrap_or(0.0);
        let added_pv_allowance = (pv - self.last_pv_w).max(0.0) * config.inverter_efficiency;
        self.last_pv_w = pv;
        if status.dc_pv_power_w.is_none() && status.reason.is_none() {
            status.reason = Some("dc_pv_unavailable_no_pv_allowance");
        }
        let target_current = (limit - config.headroom_a).max(0.0);
        let discharge = (-current).max(0.0);
        let increase_due = self.last_increase.is_none_or(|last| {
            sample.monotonic_now - last >= config.increase_interval.as_secs_f64()
        });
        let feedforward = target_current.mul_add(voltage, pv) * config.inverter_efficiency;
        if !gap && discharge > target_current && sample.grid_connected == Some(true) {
            // A falling PV allowance already removes this much inverter power.
            // Do not integrate the same disturbance again as a feedback error.
            let planned_reduction = self.last_constraint.map_or(0.0, |previous| {
                (previous - (feedforward - self.reduction_w).max(0.0)).max(0.0)
            });
            let excess = ((discharge - target_current) * voltage)
                .mul_add(config.inverter_efficiency, -planned_reduction)
                .max(0.0);
            self.reduction_w = (self.reduction_w + excess).min(1_000_000.0);
        } else if increase_due && discharge < (target_current - 1.0).max(0.0) {
            self.reduction_w = (self.reduction_w - config.power_step_w).max(0.0);
        }
        let mut constraint = (feedforward - self.reduction_w).clamp(0.0, 1_000_000.0);
        constraint = (constraint / config.power_step_w).floor() * config.power_step_w;
        if let Some(previous) = self.last_constraint {
            if constraint > previous && !increase_due {
                // Slow recovery applies to battery power, not to new PV power.
                let pv_only_increase =
                    (added_pv_allowance / config.power_step_w).floor() * config.power_step_w;
                constraint = constraint.min(previous + pv_only_increase);
            }
        }
        if increase_due {
            self.last_increase = Some(sample.monotonic_now);
        }
        self.last_constraint = Some(constraint);
        status.power_constraint_w = Some(constraint);
        if discharge > limit && status.reason.is_none() {
            status.reason = Some("current_above_limit_reducing_power");
        }
        status
    }
}

fn bounded(value: Option<f64>, min: f64, max: f64) -> Option<f64> {
    value.filter(|number| number.is_finite() && (min..=max).contains(number))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(current: f64, pv: Option<f64>, time: f64) -> CurrentSample {
        CurrentSample {
            voltage_v: Some(50.0),
            battery_current_a: Some(current),
            dc_pv_w: pv,
            bms_limit_a: Some(200.0),
            grid_connected: Some(true),
            monotonic_now: time,
        }
    }

    fn enabled() -> BatteryCurrentConfig {
        BatteryCurrentConfig {
            enabled: true,
            ..BatteryCurrentConfig::default()
        }
    }

    #[test]
    fn default_is_off_and_dc_pv_adds_inverter_allowance() {
        let mut feedback = CurrentFeedback::default();
        assert_eq!(
            feedback
                .evaluate(
                    &BatteryCurrentConfig::default(),
                    sample(-20.0, Some(0.0), 0.0)
                )
                .power_constraint_w,
            None
        );
        assert_eq!(
            feedback
                .evaluate(&enabled(), sample(-20.0, Some(0.0), 0.0))
                .power_constraint_w,
            Some(3250.0)
        );
        assert_eq!(
            feedback
                .evaluate(&enabled(), sample(-20.0, Some(2000.0), 5.0))
                .power_constraint_w,
            Some(5050.0)
        );
        for time in [10.0, 15.0, 20.0, 25.0] {
            feedback.evaluate(&enabled(), sample(-20.0, Some(2000.0), time));
        }
        assert_eq!(
            feedback
                .evaluate(&enabled(), sample(-20.0, Some(2000.0), 30.0))
                .power_constraint_w,
            Some(5050.0)
        );
        assert_eq!(
            feedback
                .evaluate(&enabled(), sample(-20.0, None, 35.0))
                .power_constraint_w,
            Some(3250.0)
        );
    }

    #[test]
    fn feedback_reduces_limit_and_never_uses_stale_battery_data() {
        let mut feedback = CurrentFeedback::default();
        feedback.evaluate(&enabled(), sample(-20.0, Some(0.0), 0.0));
        let first = feedback.evaluate(&enabled(), sample(-80.0, Some(0.0), 5.0));
        assert!(first.power_constraint_w.is_some_and(|power| power < 3250.0));
        let missing = CurrentSample {
            voltage_v: None,
            ..sample(-80.0, Some(5000.0), 5.0)
        };
        assert_eq!(
            feedback.evaluate(&enabled(), missing).power_constraint_w,
            Some(0.0)
        );
    }

    #[test]
    fn bms_zero_allows_only_dc_pv_and_bad_numbers_fail_closed() {
        let mut feedback = CurrentFeedback::default();
        let zero = CurrentSample {
            bms_limit_a: Some(0.0),
            ..sample(0.0, Some(2000.0), 0.0)
        };
        assert_eq!(
            feedback.evaluate(&enabled(), zero).power_constraint_w,
            Some(1800.0)
        );
        for bad in [f64::NAN, f64::INFINITY, -1.0] {
            let input = CurrentSample {
                bms_limit_a: Some(bad),
                ..zero
            };
            assert_eq!(
                feedback.evaluate(&enabled(), input).power_constraint_w,
                Some(0.0)
            );
        }
    }

    #[test]
    fn startup_and_pv_drop_do_not_double_count_the_feedforward_reduction() {
        let mut feedback = CurrentFeedback::default();
        assert_eq!(
            feedback
                .evaluate(&enabled(), sample(-100.0, Some(3000.0), 0.0))
                .power_constraint_w,
            Some(5950.0)
        );
        assert_eq!(
            feedback
                .evaluate(&enabled(), sample(-120.0, Some(0.0), 5.0))
                .power_constraint_w,
            Some(3250.0)
        );
    }

    #[test]
    fn closed_loop_accounts_for_conversion_losses_without_steady_overcurrent() {
        let config = enabled();
        let mut feedback = CurrentFeedback::default();
        let mut actual_current = -100.0;
        for tick in 0..240 {
            let status = feedback.evaluate(
                &config,
                sample(actual_current, Some(1000.0), f64::from(tick) * 5.0),
            );
            let watts = status
                .power_constraint_w
                .unwrap_or_else(|| std::process::abort());
            actual_current = -(watts / 0.85 - 1000.0) / 50.0;
            if tick > 3 {
                assert!(actual_current >= -75.0, "{tick}: {actual_current}");
            }
        }
    }
}

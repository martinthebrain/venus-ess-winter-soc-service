//! PV-preserving charge control through the volatile, mode-2 ESS grid setpoint.
//!
//! This never changes PV feed-in permissions, MPPT limits, or the user's grid
//! setpoint. Only an initially empty override may be acquired. Its write-ahead
//! state belongs in RAM, just like the actuator, and never in the SD journal.

use serde::{Deserialize, Serialize};

pub const HUB4: &str = "com.victronenergy.hub4";
pub const SETPOINT: &str = "/Overrides/Setpoint";
const MAX_POWER_W: f64 = 1_000_000.0;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ExportState {
    pub boot_id: String,
    pub service_owner: String,
    pub baseline_w: Option<f64>,
    pub last_setpoint_w: Option<f64>,
    pub pending: Option<PendingWrite>,
    pub external_latched: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct PendingWrite {
    pub expected: Option<f64>,
    pub intended: Option<f64>,
}

impl ExportState {
    #[must_use]
    pub const fn owned(&self) -> bool {
        self.last_setpoint_w.is_some() || self.pending.is_some()
    }

    #[must_use]
    pub fn valid(&self) -> bool {
        let number = |v: Option<f64>| v.is_none_or(|v| v.is_finite() && v.abs() <= MAX_POWER_W);
        number(self.baseline_w)
            && number(self.last_setpoint_w)
            && self
                .pending
                .is_none_or(|p| number(p.expected) && number(p.intended))
            && (!self.owned()
                || (self.boot_id.len() == 36
                    && self.service_owner.starts_with(':')
                    && self.service_owner.len() <= 128
                    && self.baseline_w.is_some()))
    }

    /// Reconcile only with the same boot and unique D-Bus service owner.
    /// Unapplied writes are discarded, never replayed using old PV measurements.
    pub fn reconcile(&mut self, boot: &str, owner: &str, current: Option<f64>) {
        if self.boot_id != boot || self.service_owner != owner || !self.valid() {
            *self = Self::default();
        }
        if let Some(pending) = self.pending.take() {
            if same(current, pending.intended) {
                self.last_setpoint_w = pending.intended;
            } else if !same(current, pending.expected) {
                self.last_setpoint_w = None;
                self.external_latched = true;
            }
        }
        if self.last_setpoint_w.is_some() && !same(current, self.last_setpoint_w) {
            self.last_setpoint_w = None;
            self.external_latched = true;
        }
        boot.clone_into(&mut self.boot_id);
        owner.clone_into(&mut self.service_owner);
    }

    pub const fn prepare(&mut self, current: Option<f64>, target: Option<f64>, baseline: f64) {
        self.baseline_w = Some(baseline);
        self.pending = Some(PendingWrite {
            expected: current,
            intended: target,
        });
    }

    pub const fn commit(&mut self) {
        if let Some(pending) = self.pending.take() {
            self.last_setpoint_w = pending.intended;
        }
    }
}

#[must_use]
pub fn same(a: Option<f64>, b: Option<f64>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => a.is_finite() && b.is_finite() && (a - b).abs() < 0.5,
        (None, None) => true,
        _ => false,
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ExportSample {
    pub battery_current_a: f64,
    pub voltage_v: f64,
    pub dc_pv_w: f64,
    pub grid_w: f64,
    pub baseline_w: f64,
    pub limit_a: f64,
    /// -1 means no user export-power limit.
    pub max_export_w: f64,
    pub efficiency: f64,
}

impl ExportSample {
    #[must_use]
    pub fn valid(self) -> bool {
        let bounded = |v: f64, min: f64, max: f64| v.is_finite() && (min..=max).contains(&v);
        bounded(self.battery_current_a, -10_000.0, 10_000.0)
            && bounded(self.voltage_v, 1.0, 1_000.0)
            && bounded(self.dc_pv_w, 0.0, MAX_POWER_W)
            && bounded(self.grid_w, -MAX_POWER_W, MAX_POWER_W)
            && bounded(self.baseline_w, -MAX_POWER_W, MAX_POWER_W)
            && bounded(self.limit_a, 0.0, 10_000.0)
            && (self.max_export_w.to_bits() == (-1.0_f64).to_bits()
                || bounded(self.max_export_w, 0.0, MAX_POWER_W))
            && bounded(self.efficiency, 0.5, 1.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ExportPlan {
    pub setpoint_w: Option<f64>,
    pub reason: &'static str,
}

/// A measured-current correction around native ESS, not a replacement for ESS.
///
/// No integral accumulates when export saturates; each request uses live grid
/// power. The normal setpoint is an upper bound, so this cannot request charging
/// from the grid which the user's ordinary ESS settings would not request.
#[must_use]
pub fn plan(sample: ExportSample, active: bool) -> ExportPlan {
    let s = sample;
    if !s.valid() {
        return ExportPlan {
            setpoint_w: None,
            reason: "export_telemetry_invalid",
        };
    }
    let target_a = (s.limit_a - 2.0).max(0.0);
    if s.dc_pv_w <= 0.0
        || s.battery_current_a <= 0.0
        || (!active && s.battery_current_a <= target_a)
    {
        return ExportPlan {
            setpoint_w: None,
            reason: "no_additional_pv_export_required",
        };
    }
    let export_floor = if s.max_export_w < 0.0 {
        -MAX_POWER_W
    } else {
        -s.max_export_w
    };
    // Round towards less export; the 2 A headroom covers the 25 W quantization.
    let desired =
        (((s.battery_current_a - target_a) * s.voltage_v).mul_add(-s.efficiency, s.grid_w) / 25.0)
            .ceil()
            * 25.0;
    let limited = desired.max(export_floor).min(s.baseline_w);
    if limited >= s.baseline_w - 25.0 {
        return ExportPlan {
            setpoint_w: None,
            reason: "no_additional_pv_export_required",
        };
    }
    ExportPlan {
        setpoint_w: Some(limited),
        reason: if desired < export_floor {
            "export_power_limit_reached"
        } else if active && s.battery_current_a > s.limit_a {
            "charge_current_above_limit_requesting_more_export"
        } else {
            "pv_surplus_export_control"
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ExportSample {
        ExportSample {
            battery_current_a: 100.0,
            voltage_v: 50.0,
            dc_pv_w: 6000.0,
            grid_w: 0.0,
            baseline_w: 50.0,
            limit_a: 75.0,
            max_export_w: -1.0,
            efficiency: 0.9,
        }
    }

    #[test]
    fn surplus_goes_to_grid_without_limiting_pv() {
        let result = plan(sample(), false);
        assert_eq!(result.setpoint_w, Some(-1200.0));
        let mut settled = sample();
        settled.grid_w = -1200.0;
        settled.battery_current_a -= 1200.0 / (50.0 * 0.9);
        assert!(settled.battery_current_a <= 75.0);
        assert!(
            plan(settled, true)
                .setpoint_w
                .is_some_and(|w| (w + 1200.0).abs() <= 25.0)
        );
    }

    #[test]
    fn respects_user_export_cap_and_invalid_data() {
        assert_eq!(
            plan(
                ExportSample {
                    max_export_w: 500.0,
                    ..sample()
                },
                false
            )
            .setpoint_w,
            Some(-500.0)
        );
        assert_eq!(
            plan(
                ExportSample {
                    max_export_w: 0.0,
                    ..sample()
                },
                false
            )
            .reason,
            "export_power_limit_reached"
        );
        for current in [f64::NAN, f64::INFINITY, 10_001.0] {
            assert_eq!(
                plan(
                    ExportSample {
                        battery_current_a: current,
                        ..sample()
                    },
                    true
                )
                .setpoint_w,
                None
            );
        }
    }

    #[test]
    fn falling_pv_and_battery_discharge_release_override() {
        for changed in [
            ExportSample {
                dc_pv_w: 0.0,
                ..sample()
            },
            ExportSample {
                battery_current_a: -1.0,
                ..sample()
            },
        ] {
            assert_eq!(plan(changed, true).setpoint_w, None);
        }
        assert_eq!(
            plan(
                ExportSample {
                    battery_current_a: 30.0,
                    ..sample()
                },
                false
            )
            .setpoint_w,
            None
        );
    }

    #[test]
    fn zero_charge_limit_requests_pv_export_not_a_pv_shutdown() {
        assert_eq!(
            plan(
                ExportSample {
                    limit_a: 0.0,
                    ..sample()
                },
                false
            )
            .setpoint_w,
            Some(-4500.0)
        );
    }

    fn pending() -> ExportState {
        ExportState {
            boot_id: "00000000-0000-0000-0000-000000000000".to_owned(),
            service_owner: ":1.20".to_owned(),
            baseline_w: Some(50.0),
            pending: Some(PendingWrite {
                expected: None,
                intended: Some(-1200.0),
            }),
            ..ExportState::default()
        }
    }

    #[test]
    fn recovery_distinguishes_applied_unapplied_and_external_writes() {
        for (observed, expected, external) in [
            (None, None, false),
            (Some(-1200.0), Some(-1200.0), false),
            (Some(-300.0), None, true),
        ] {
            let mut state = pending();
            state.reconcile(&state.boot_id.clone(), ":1.20", observed);
            assert_eq!(state.last_setpoint_w, expected);
            assert_eq!(state.external_latched, external);
            assert!(state.pending.is_none());
        }
    }

    #[test]
    fn service_or_boot_replacement_never_recovers_old_override() {
        let mut state = pending();
        state.reconcile(&state.boot_id.clone(), ":1.21", Some(-1200.0));
        assert!(!state.owned());
        let mut state = pending();
        state.reconcile(
            "10000000-0000-0000-0000-000000000000",
            ":1.20",
            Some(-1200.0),
        );
        assert!(!state.owned());
    }
}

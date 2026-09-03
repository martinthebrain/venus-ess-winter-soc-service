use proptest::prelude::*;
use venus_ess_winter_soc_service::config::{
    CHARGE_WINDOW_BASE_HOURS, CHARGE_WINDOW_MAX_MULTIPLIER, DEFAULT_SOC, PolicyConfig,
};
use venus_ess_winter_soc_service::discharge_protection::{
    ProtectionAction, ProtectionInput, evaluate as evaluate_discharge_protection,
};
use venus_ess_winter_soc_service::domain::{ControllerState, DischargeProtectionState, PvPower};
use venus_ess_winter_soc_service::policy::{
    charge_window_hours, compute_charge_current_limit, needs_charge, pause_soc,
};

proptest! {
    #[test]
    fn pause_target_is_bounded_by_default_and_target(
        current_soc in -1000.0_f64..1000.0,
        target_soc in DEFAULT_SOC..=100.0,
    ) {
        let pause = pause_soc(current_soc, target_soc, DEFAULT_SOC);
        prop_assert!(pause >= DEFAULT_SOC);
        prop_assert!(pause <= target_soc);
    }

    #[test]
    fn computed_current_never_exceeds_positive_boundaries(
        load in -10000.0_f64..10000.0,
        bms in 0.01_f64..1000.0,
        vebus in 0.01_f64..1000.0,
        voltage in 1.01_f64..1000.0,
        normal in 0.01_f64..1000.0,
    ) {
        let value = compute_charge_current_limit(
            &PolicyConfig::default(),
            load,
            Some(bms),
            Some(vebus),
            Some(voltage),
            PvPower::default(),
            Some(normal),
        );
        prop_assert!(value.is_some());
        let value = value.unwrap_or_default();
        prop_assert!(value >= 0.0);
        prop_assert!(value <= bms);
        prop_assert!(value <= (bms.min(vebus) * PolicyConfig::default().grid_charge_max_fraction).floor());
        prop_assert!(value <= vebus);
        prop_assert!(value <= normal);
    }

    #[test]
    fn pv_augmented_current_never_exceeds_hardware_or_gui_limits(
        load in -10_000.0_f64..10_000.0,
        bms in 0.01_f64..1_000.0,
        vebus in 0.01_f64..1_000.0,
        voltage in 1.01_f64..1_000.0,
        ac_pv_w in 0.0_f64..100_000.0,
        dc_pv_w in 0.0_f64..100_000.0,
        normal in 0.01_f64..1_000.0,
    ) {
        let value = compute_charge_current_limit(
            &PolicyConfig::default(),
            load,
            Some(bms),
            Some(vebus),
            Some(voltage),
            PvPower {
                ac_w: ac_pv_w,
                dc_w: dc_pv_w,
            },
            Some(normal),
        );
        prop_assert!(value.is_some());
        let value = value.unwrap_or_default();
        prop_assert!(value >= 0.0);
        prop_assert!(value <= bms);
        prop_assert!(value <= vebus);
        prop_assert!(value <= normal);
    }

    #[test]
    fn adaptive_window_is_bounded(
        start in 1.0_f64..10_000.0,
        elapsed in 0.0_f64..10_000_000.0,
    ) {
        let state = ControllerState {
            charge_deficit_start_ts: start,
            ..ControllerState::default()
        };
        let hours = charge_window_hours(&PolicyConfig::default(), &state, start + elapsed);
        prop_assert!(hours >= CHARGE_WINDOW_BASE_HOURS);
        prop_assert!(hours <= CHARGE_WINDOW_BASE_HOURS * CHARGE_WINDOW_MAX_MULTIPLIER);
    }

    #[test]
    fn charge_hysteresis_is_strict(current in 0.0_f64..100.0, target in 1.0_f64..100.0) {
        let expected = current < target - 1.0;
        prop_assert_eq!(needs_charge(&PolicyConfig::default(), current, target), expected);
    }

    #[test]
    fn discharge_protection_never_raises_an_explicit_gui_limit(
        current_limit_w in 0.0_f64..100_000.0,
        nominal_power_w in 1.0_f64..100_000.0,
    ) {
        let policy = PolicyConfig::default();
        let mut state = DischargeProtectionState::default();
        let result = evaluate_discharge_protection(
            &policy,
            &mut state,
            ProtectionInput {
                soc: policy.discharge_protection_enter_soc - 0.1,
                battery_power_w: Some(-1.0),
                current_limit_w: Some(current_limit_w),
                nominal_inverter_power_w: Some(nominal_power_w),
                monotonic_now: 0.0,
            },
        );
        if let Some(ProtectionAction::Restrict(limit)) = result.action {
            prop_assert!(limit <= current_limit_w);
        }
        prop_assert_eq!(state.restore_power_w, Some(current_limit_w.min(nominal_power_w)));
    }

    #[test]
    fn discharge_protection_never_releases_without_observed_charging(
        soc in 25.000_001_f64..=100.0,
        battery_power_w in -100_000.0_f64..=0.0,
        nominal_power_w in 1.0_f64..100_000.0,
    ) {
        let policy = PolicyConfig::default();
        let cap = nominal_power_w * policy.discharge_protection_nominal_fraction;
        let mut state = DischargeProtectionState {
            active: true,
            recharge_seen: false,
            recharge_candidate_since_ts: None,
            recharge_candidate_last_sample_ts: None,
            restore_power_w: None,
            restore_default: true,
            last_set_power_w: Some(cap),
            last_observed_power_w: Some(cap),
            write_generation: 0,
            pending_write: None,
        };
        let result = evaluate_discharge_protection(
            &policy,
            &mut state,
            ProtectionInput {
                soc,
                battery_power_w: Some(battery_power_w),
                current_limit_w: Some(cap),
                nominal_inverter_power_w: Some(nominal_power_w),
                monotonic_now: 0.0,
            },
        );
        prop_assert!(!matches!(result.action, Some(ProtectionAction::Restore(_))));
        prop_assert!(state.active);
    }
}

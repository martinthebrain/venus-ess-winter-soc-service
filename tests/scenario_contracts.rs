use serde::Deserialize;
use std::fs;
use venus_ess_winter_soc_service::clock::LocalDateTime;
use venus_ess_winter_soc_service::config::PolicyConfig;
use venus_ess_winter_soc_service::domain::{ControllerState, PvPower};
use venus_ess_winter_soc_service::policy::{
    charge_window_hours, compute_charge_current_limit, determine_target, is_charge_window_active,
};

#[derive(Deserialize)]
struct Contracts {
    seasonal_targets: Vec<SeasonalCase>,
    charge_windows: Vec<WindowCase>,
    charge_currents: Vec<CurrentCase>,
}

#[derive(Deserialize)]
struct SeasonalCase {
    name: String,
    date: String,
    now_ts: f64,
    history: Vec<f64>,
    target: f64,
    mode: String,
}

#[derive(Deserialize)]
struct WindowCase {
    name: String,
    date: String,
    now_ts: f64,
    deficit_start_ts: f64,
    hours: u8,
    active: bool,
}

#[derive(Deserialize)]
struct CurrentCase {
    name: String,
    house_load_w: f64,
    battery_max_current_a: Option<f64>,
    vebus_max_charge_current_a: Option<f64>,
    battery_voltage_v: Option<f64>,
    ac_pv_power_w: f64,
    dc_pv_power_w: f64,
    normal_current_a: Option<f64>,
    expected: Option<f64>,
}

fn contracts() -> Contracts {
    let path = format!("{}/contracts/scenarios.json", env!("CARGO_MANIFEST_DIR"));
    let input = fs::read(path).unwrap_or_else(|_| std::process::abort());
    serde_json::from_slice(&input).unwrap_or_else(|_| std::process::abort())
}

fn date_time(value: &str) -> LocalDateTime {
    let bytes = value.as_bytes();
    let number = |start: usize, end: usize| {
        value[start..end]
            .parse::<u8>()
            .unwrap_or_else(|_| std::process::abort())
    };
    assert!(bytes.len() >= 19);
    LocalDateTime {
        year: value[0..4]
            .parse::<i32>()
            .unwrap_or_else(|_| std::process::abort()),
        month: number(5, 7),
        day: number(8, 10),
        hour: number(11, 13),
        minute: number(14, 16),
        second: number(17, 19),
    }
}

#[test]
fn seasonal_target_contracts_are_executable() {
    let policy = PolicyConfig::default();
    for case in contracts().seasonal_targets {
        let mut state = ControllerState {
            pv_history: case.history,
            boot_ts: case.now_ts,
            ..ControllerState::default()
        };
        let actual = determine_target(
            &policy,
            &mut state,
            date_time(&case.date),
            case.now_ts,
            case.now_ts,
        );
        assert_eq!(
            actual.target_soc.to_bits(),
            case.target.to_bits(),
            "{}",
            case.name
        );
        assert_eq!(actual.mode, case.mode, "{}", case.name);
    }
}

#[test]
fn adaptive_charge_window_contracts_match_the_python_service() {
    let policy = PolicyConfig::default();
    for case in contracts().charge_windows {
        let state = ControllerState {
            charge_deficit_start_ts: case.deficit_start_ts,
            ..ControllerState::default()
        };
        assert_eq!(
            charge_window_hours(&policy, &state, case.now_ts),
            case.hours,
            "{}",
            case.name
        );
        assert_eq!(
            is_charge_window_active(&policy, &state, date_time(&case.date), case.now_ts),
            case.active,
            "{}",
            case.name
        );
    }
}

#[test]
fn charge_current_contracts_are_executable() {
    let policy = PolicyConfig::default();
    for case in contracts().charge_currents {
        let actual = compute_charge_current_limit(
            &policy,
            case.house_load_w,
            case.battery_max_current_a,
            case.vebus_max_charge_current_a,
            case.battery_voltage_v,
            PvPower {
                ac_w: case.ac_pv_power_w,
                dc_w: case.dc_pv_power_w,
            },
            case.normal_current_a,
        );
        assert_eq!(actual, case.expected, "{}", case.name);
    }
}

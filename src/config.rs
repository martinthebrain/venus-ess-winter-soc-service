//! Validated policy configuration and immutable boundary contracts.

use std::env;
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::time::Duration;

pub const BALANCING_INTERVAL_DAYS: f64 = 14.0;
pub const BALANCING_DURATION_HOURS: f64 = 4.0;
pub const BALANCING_APPROACH_MAX_HOURS: f64 = 72.0;
pub const BALANCING_MAX_HOURS: f64 = 12.0;
pub const BALANCING_RETRY_COOLDOWN_HOURS: f64 = 24.0;
pub const BALANCING_BOOT_GRACE_HOURS: f64 = 24.0;
pub const BALANCING_FULL_SOC: f64 = 99.0;
pub const FULL_SOC_CONFIRM_MINUTES: f64 = 10.0;
pub const GRID_LOAD_LIMIT_W: f64 = 4_000.0;
pub const GRID_PAUSE_HEADROOM_W: f64 = 100.0;
pub const GRID_SOFT_MIN_CHARGE_CURRENT_A: f64 = 10.0;
pub const CHARGE_WINDOW_START_HOUR: u8 = 23;
pub const CHARGE_WINDOW_BASE_HOURS: u8 = 4;
pub const CHARGE_WINDOW_ESCALATION_NIGHTS: f64 = 2.0;
pub const CHARGE_WINDOW_MAX_MULTIPLIER: u8 = 4;
pub const CHARGE_EFFICIENCY: f64 = 0.9;
pub const GRID_CHARGE_MAX_FRACTION: f64 = 0.4;
pub const CHARGE_LIMIT_UPDATE_THRESHOLD_A: f64 = 1.0;
pub const CHARGE_LIMIT_MIN_UPDATE_INTERVAL_SECONDS: f64 = 300.0;
pub const SAFE_CHARGE_CURRENT_A: Option<f64> = Some(50.0);
pub const NORMAL_CHARGE_CURRENT_A: Option<f64> = None;
pub const SOC_HYSTERESIS: f64 = 1.0;
pub const DISCHARGE_PROTECTION_ENTER_SOC: f64 = 20.0;
pub const DISCHARGE_PROTECTION_RELEASE_SOC: f64 = 25.0;
pub const DISCHARGE_PROTECTION_NOMINAL_FRACTION: f64 = 0.4;
pub const DISCHARGE_POWER_EPSILON_W: f64 = 1.0;
pub const DISCHARGE_RECHARGE_MIN_POWER_W: f64 = 100.0;
pub const DISCHARGE_RECHARGE_CONFIRM_SECONDS: f64 = 120.0;
pub const DISCHARGE_RECHARGE_MAX_SAMPLE_GAP_SECONDS: f64 = 90.0;
pub const ROUTINE_MAX_CHARGE_SOC: f64 = 90.0;
pub const FULL_MAX_CHARGE_SOC: f64 = 100.0;
pub const FULL_CHARGE_REACHED_SOC: f64 = 99.0;
pub const FULL_CHARGE_MIN_AGE_DAYS: usize = 2;
pub const FULL_CHARGE_CONFIRM_SECONDS: f64 = 7_200.0;
pub const FULL_CHARGE_MAX_SAMPLE_GAP_SECONDS: f64 = 90.0;
pub const CHARGE_CEILING_CURRENT_EPSILON_A: f64 = 0.1;
pub const STATUS_LOG_INTERVAL_SECONDS: f64 = 300.0;
pub const INVALID_LOG_INTERVAL_SECONDS: f64 = 300.0;
pub const BOOT_RECOVERY_SECONDS: f64 = 600.0;
pub const PV_THRESHOLD_W: f64 = 3_000.0;
pub const TRANSITION_DAYS: usize = 4;
pub const PV_OBSERVATION_START_HOUR: u8 = 9;
pub const PV_OBSERVATION_END_HOUR: u8 = 17;
pub const PV_MIN_DAILY_COVERAGE_FRACTION: f64 = 0.75;
pub const LOOP_INTERVAL_SECONDS: f64 = 60.0;
pub const DEFAULT_SOC: f64 = 10.0;
pub const TRANSITION_GUARD_SOC: f64 = 40.0;
pub const WINTER_TARGET_SOC: f64 = 45.0;
pub const BALANCING_TARGET_SOC: f64 = 100.0;
pub const MIN_VALID_SOC: f64 = 0.0;
pub const MAX_VALID_SOC: f64 = 100.0;
pub const MIN_SOC_EPSILON: f64 = 0.1;
pub const BOOT_RECOVERY_TARGET_MATCH_EPSILON: f64 = 0.1;
pub const SECONDS_PER_HOUR: f64 = 3_600.0;
pub const SECONDS_PER_DAY: f64 = 86_400.0;
pub const SUMMER_MANUAL_MINSOC_HOLD_SECONDS: f64 = 86_400.0;
pub const MIN_SOC_SCRIPT_WRITE_MATCH_SECONDS: f64 = 180.0;
pub const SD_SAVE_INTERVAL_SECONDS: f64 = 21_600.0;
pub const SD_BACKOFF_MAX_SECONDS: u32 = 300;
pub const SD_LOOKUP_INTERVAL_SECONDS: f64 = 3_600.0;
pub const BATTERY_MAX_CURRENT_MAX_AGE_SECONDS: f64 = 300.0;
pub const NOMINAL_INVERTER_POWER_MAX_AGE_SECONDS: f64 = 86_400.0;
pub const WINTER_START_MMDD: u16 = 1_125;
pub const WINTER_END_MMDD: u16 = 205;
pub const TRANS_PRE_START_MMDD: u16 = 1_105;
pub const TRANS_PRE_END_MMDD: u16 = 1_124;
pub const TRANS_POST_START_MMDD: u16 = 206;
pub const TRANS_POST_END_MMDD: u16 = 225;
pub const LOG_MAX_BYTES: u64 = 2_000_000;
pub const LOG_TRUNCATE_BYTES: u64 = 200_000;
pub const STATE_MAX_BYTES: u64 = 256 * 1024;
pub const SD_DIR_NAME: &str = "socSteuerung";
pub const DEFAULT_DURABLE_RESTORE_FILE: &str =
    "/data/venus-ess-winter-soc-service-rust/gui-restore-state.json";
pub const DEFAULT_RUNTIME_DIR: &str = "/dev/shm/venus-ess-winter-soc-service";

pub const MIN_SOC_PATH: &str = "/Settings/CGwacs/BatteryLife/MinimumSocLimit";
pub const MAX_CHARGE_CURRENT_PATH: &str = "/Settings/SystemSetup/MaxChargeCurrent";
pub const MAX_DISCHARGE_POWER_PATH: &str = "/Settings/CGwacs/MaxDischargePower";
pub const BATTERY_SOC_PATH: &str = "/Dc/Battery/Soc";
pub const BATTERY_POWER_PATH: &str = "/Dc/Battery/Power";
pub const BATTERY_VOLTAGE_PATH: &str = "/Dc/Battery/Voltage";
pub const BMS_MAX_CHARGE_CURRENT_PATH: &str = "/Info/MaxChargeCurrent";
pub const BMS_ALLOW_TO_CHARGE_PATH: &str = "/Io/AllowToCharge";
pub const DC_PV_POWER_PATH: &str = "/Dc/Pv/Power";
pub const AC_GRID_POWER_PATH: &str = "/Ac/Grid/{phase}/Power";
pub const AC_GRID_PHASE_COUNT_PATH: &str = "/Ac/Grid/NumberOfPhases";
pub const AC_CONSUMPTION_ON_INPUT_PHASE_COUNT_PATH: &str = "/Ac/ConsumptionOnInput/NumberOfPhases";
pub const AC_CONSUMPTION_PHASE_COUNT_PATH: &str = "/Ac/Consumption/NumberOfPhases";
pub const AC_PV_ON_GRID_POWER_PATH: &str = "/Ac/PvOnGrid/{phase}/Power";
pub const AC_PV_ON_OUTPUT_POWER_PATH: &str = "/Ac/PvOnOutput/{phase}/Power";
pub const NOMINAL_INVERTER_POWER_PATH: &str = "/Ac/Out/NominalInverterPower";
pub const VEBUS_MAX_CHARGE_CURRENT_PATH: &str = "/Dc/0/MaxChargeCurrent";
pub const VEBUS_ALLOW_TO_CHARGE_PATH: &str = "/Bms/AllowToCharge";
pub const ACTIVE_BMS_SERVICE_PATH: &str = "/ActiveBmsService";
pub const VEBUS_SERVICE_PATH: &str = "/VebusService";
pub const SYSTEM_CHARGE_DISABLED_PATH: &str = "/SystemState/ChargeDisabled";
pub const SYSTEM_USER_CHARGE_LIMITED_PATH: &str = "/SystemState/UserChargeLimited";
pub const PHASES: [&str; 3] = ["L1", "L2", "L3"];

#[derive(Clone, Debug, PartialEq)]
pub struct PolicyConfig {
    pub balancing_interval_days: f64,
    pub balancing_duration_hours: f64,
    pub balancing_approach_max_hours: f64,
    pub balancing_max_hours: f64,
    pub balancing_retry_cooldown_hours: f64,
    pub balancing_boot_grace_hours: f64,
    pub balancing_full_soc: f64,
    pub full_soc_confirm_minutes: f64,
    pub grid_load_limit_w: f64,
    pub grid_pause_headroom_w: f64,
    pub grid_soft_min_charge_current_a: f64,
    pub charge_window_start_hour: u8,
    pub charge_window_base_hours: u8,
    pub charge_window_escalation_nights: f64,
    pub charge_window_max_multiplier: u8,
    pub charge_efficiency: f64,
    pub grid_charge_max_fraction: f64,
    pub charge_limit_update_threshold_a: f64,
    pub charge_limit_min_update_interval_seconds: f64,
    pub safe_charge_current_a: Option<f64>,
    pub normal_charge_current_a: Option<f64>,
    pub soc_hysteresis: f64,
    pub discharge_protection_enter_soc: f64,
    pub discharge_protection_release_soc: f64,
    pub discharge_protection_nominal_fraction: f64,
    pub discharge_power_epsilon_w: f64,
    pub discharge_recharge_min_power_w: f64,
    pub discharge_recharge_confirm_seconds: f64,
    pub discharge_recharge_max_sample_gap_seconds: f64,
    pub routine_max_charge_soc: f64,
    pub full_max_charge_soc: f64,
    pub full_charge_reached_soc: f64,
    pub full_charge_min_age_days: usize,
    pub full_charge_confirm_seconds: f64,
    pub full_charge_max_sample_gap_seconds: f64,
    pub charge_ceiling_current_epsilon_a: f64,
    pub min_soc_epsilon: f64,
    pub boot_recovery_target_match_epsilon: f64,
    pub boot_recovery_seconds: f64,
    pub pv_threshold_w: f64,
    pub transition_days: usize,
    pub summer_min_soc: f64,
    pub transition_guard_soc: f64,
    pub winter_target_soc: f64,
    pub balancing_target_soc: f64,
    pub pv_min_daily_coverage_fraction: f64,
    pub summer_manual_minsoc_hold_seconds: f64,
    pub min_soc_script_write_match_seconds: f64,
    pub winter_start_mmdd: u16,
    pub winter_end_mmdd: u16,
    pub transition_pre_start_mmdd: u16,
    pub transition_pre_end_mmdd: u16,
    pub transition_post_start_mmdd: u16,
    pub transition_post_end_mmdd: u16,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            balancing_interval_days: BALANCING_INTERVAL_DAYS,
            balancing_duration_hours: BALANCING_DURATION_HOURS,
            balancing_approach_max_hours: BALANCING_APPROACH_MAX_HOURS,
            balancing_max_hours: BALANCING_MAX_HOURS,
            balancing_retry_cooldown_hours: BALANCING_RETRY_COOLDOWN_HOURS,
            balancing_boot_grace_hours: BALANCING_BOOT_GRACE_HOURS,
            balancing_full_soc: BALANCING_FULL_SOC,
            full_soc_confirm_minutes: FULL_SOC_CONFIRM_MINUTES,
            grid_load_limit_w: GRID_LOAD_LIMIT_W,
            grid_pause_headroom_w: GRID_PAUSE_HEADROOM_W,
            grid_soft_min_charge_current_a: GRID_SOFT_MIN_CHARGE_CURRENT_A,
            charge_window_start_hour: CHARGE_WINDOW_START_HOUR,
            charge_window_base_hours: CHARGE_WINDOW_BASE_HOURS,
            charge_window_escalation_nights: CHARGE_WINDOW_ESCALATION_NIGHTS,
            charge_window_max_multiplier: CHARGE_WINDOW_MAX_MULTIPLIER,
            charge_efficiency: CHARGE_EFFICIENCY,
            grid_charge_max_fraction: GRID_CHARGE_MAX_FRACTION,
            charge_limit_update_threshold_a: CHARGE_LIMIT_UPDATE_THRESHOLD_A,
            charge_limit_min_update_interval_seconds: CHARGE_LIMIT_MIN_UPDATE_INTERVAL_SECONDS,
            safe_charge_current_a: SAFE_CHARGE_CURRENT_A,
            normal_charge_current_a: NORMAL_CHARGE_CURRENT_A,
            soc_hysteresis: SOC_HYSTERESIS,
            discharge_protection_enter_soc: DISCHARGE_PROTECTION_ENTER_SOC,
            discharge_protection_release_soc: DISCHARGE_PROTECTION_RELEASE_SOC,
            discharge_protection_nominal_fraction: DISCHARGE_PROTECTION_NOMINAL_FRACTION,
            discharge_power_epsilon_w: DISCHARGE_POWER_EPSILON_W,
            discharge_recharge_min_power_w: DISCHARGE_RECHARGE_MIN_POWER_W,
            discharge_recharge_confirm_seconds: DISCHARGE_RECHARGE_CONFIRM_SECONDS,
            discharge_recharge_max_sample_gap_seconds: DISCHARGE_RECHARGE_MAX_SAMPLE_GAP_SECONDS,
            routine_max_charge_soc: ROUTINE_MAX_CHARGE_SOC,
            full_max_charge_soc: FULL_MAX_CHARGE_SOC,
            full_charge_reached_soc: FULL_CHARGE_REACHED_SOC,
            full_charge_min_age_days: FULL_CHARGE_MIN_AGE_DAYS,
            full_charge_confirm_seconds: FULL_CHARGE_CONFIRM_SECONDS,
            full_charge_max_sample_gap_seconds: FULL_CHARGE_MAX_SAMPLE_GAP_SECONDS,
            charge_ceiling_current_epsilon_a: CHARGE_CEILING_CURRENT_EPSILON_A,
            min_soc_epsilon: MIN_SOC_EPSILON,
            boot_recovery_target_match_epsilon: BOOT_RECOVERY_TARGET_MATCH_EPSILON,
            boot_recovery_seconds: BOOT_RECOVERY_SECONDS,
            pv_threshold_w: PV_THRESHOLD_W,
            transition_days: TRANSITION_DAYS,
            summer_min_soc: DEFAULT_SOC,
            transition_guard_soc: TRANSITION_GUARD_SOC,
            winter_target_soc: WINTER_TARGET_SOC,
            balancing_target_soc: BALANCING_TARGET_SOC,
            pv_min_daily_coverage_fraction: PV_MIN_DAILY_COVERAGE_FRACTION,
            summer_manual_minsoc_hold_seconds: SUMMER_MANUAL_MINSOC_HOLD_SECONDS,
            min_soc_script_write_match_seconds: MIN_SOC_SCRIPT_WRITE_MATCH_SECONDS,
            winter_start_mmdd: WINTER_START_MMDD,
            winter_end_mmdd: WINTER_END_MMDD,
            transition_pre_start_mmdd: TRANS_PRE_START_MMDD,
            transition_pre_end_mmdd: TRANS_PRE_END_MMDD,
            transition_post_start_mmdd: TRANS_POST_START_MMDD,
            transition_post_end_mmdd: TRANS_POST_END_MMDD,
        }
    }
}

impl PolicyConfig {
    fn from_lookup<F>(mut lookup: F) -> Result<Self, ConfigError>
    where
        F: FnMut(&str) -> Option<String>,
    {
        let mut config = Self::default();
        config.load_balancing(&mut lookup)?;
        config.load_grid(&mut lookup)?;
        config.load_charge_window(&mut lookup)?;
        config.load_charge_current(&mut lookup)?;
        config.load_soc(&mut lookup)?;
        config.load_charge_ceiling(&mut lookup)?;
        config.load_discharge_protection(&mut lookup)?;
        config.load_pv(&mut lookup)?;
        config.load_season_dates(&mut lookup)?;
        config.validate()?;
        Ok(config)
    }

    fn load_balancing<F>(&mut self, lookup: &mut F) -> Result<(), ConfigError>
    where
        F: FnMut(&str) -> Option<String>,
    {
        self.balancing_interval_days = configured_f64(
            lookup,
            "ESS_BALANCING_INTERVAL_DAYS",
            self.balancing_interval_days,
            1.0,
            365.0,
        )?;
        self.balancing_duration_hours = configured_f64(
            lookup,
            "ESS_BALANCING_DURATION_HOURS",
            self.balancing_duration_hours,
            0.1,
            48.0,
        )?;
        self.balancing_approach_max_hours = configured_f64(
            lookup,
            "ESS_BALANCING_APPROACH_MAX_HOURS",
            self.balancing_approach_max_hours,
            1.0,
            720.0,
        )?;
        self.balancing_max_hours = configured_f64(
            lookup,
            "ESS_BALANCING_MAX_HOURS",
            self.balancing_max_hours,
            0.1,
            72.0,
        )?;
        self.balancing_retry_cooldown_hours = configured_f64(
            lookup,
            "ESS_BALANCING_RETRY_COOLDOWN_HOURS",
            self.balancing_retry_cooldown_hours,
            0.0,
            720.0,
        )?;
        self.balancing_boot_grace_hours = configured_f64(
            lookup,
            "ESS_BALANCING_BOOT_GRACE_HOURS",
            self.balancing_boot_grace_hours,
            0.0,
            720.0,
        )?;
        self.balancing_full_soc = configured_f64(
            lookup,
            "ESS_BALANCING_FULL_SOC",
            self.balancing_full_soc,
            MIN_VALID_SOC,
            MAX_VALID_SOC,
        )?;
        self.full_soc_confirm_minutes = configured_f64(
            lookup,
            "ESS_FULL_SOC_CONFIRM_MINUTES",
            self.full_soc_confirm_minutes,
            0.1,
            1_440.0,
        )?;
        Ok(())
    }

    fn load_grid<F>(&mut self, lookup: &mut F) -> Result<(), ConfigError>
    where
        F: FnMut(&str) -> Option<String>,
    {
        self.grid_load_limit_w = configured_f64(
            lookup,
            "ESS_GRID_LOAD_LIMIT_W",
            self.grid_load_limit_w,
            100.0,
            100_000.0,
        )?;
        self.grid_pause_headroom_w = configured_f64(
            lookup,
            "ESS_GRID_PAUSE_HEADROOM_W",
            self.grid_pause_headroom_w,
            0.0,
            10_000.0,
        )?;
        self.grid_soft_min_charge_current_a = configured_f64(
            lookup,
            "ESS_GRID_SOFT_MIN_CHARGE_CURRENT_A",
            self.grid_soft_min_charge_current_a,
            0.0,
            1_000.0,
        )?;
        Ok(())
    }

    fn load_charge_window<F>(&mut self, lookup: &mut F) -> Result<(), ConfigError>
    where
        F: FnMut(&str) -> Option<String>,
    {
        self.charge_window_start_hour = configured_u8(
            lookup,
            "ESS_CHARGE_WINDOW_START_HOUR",
            self.charge_window_start_hour,
            0,
            23,
        )?;
        self.charge_window_base_hours = configured_u8(
            lookup,
            "ESS_CHARGE_WINDOW_BASE_HOURS",
            self.charge_window_base_hours,
            1,
            24,
        )?;
        self.charge_window_escalation_nights = configured_f64(
            lookup,
            "ESS_CHARGE_WINDOW_ESCALATION_NIGHTS",
            self.charge_window_escalation_nights,
            0.1,
            365.0,
        )?;
        self.charge_window_max_multiplier = configured_u8(
            lookup,
            "ESS_CHARGE_WINDOW_MAX_MULTIPLIER",
            self.charge_window_max_multiplier,
            1,
            24,
        )?;
        Ok(())
    }

    fn load_charge_current<F>(&mut self, lookup: &mut F) -> Result<(), ConfigError>
    where
        F: FnMut(&str) -> Option<String>,
    {
        self.charge_efficiency = configured_f64(
            lookup,
            "ESS_CHARGE_EFFICIENCY",
            self.charge_efficiency,
            0.01,
            1.0,
        )?;
        self.grid_charge_max_fraction = configured_f64(
            lookup,
            "ESS_GRID_CHARGE_MAX_FRACTION",
            self.grid_charge_max_fraction,
            0.01,
            1.0,
        )?;
        self.charge_limit_update_threshold_a = configured_f64(
            lookup,
            "ESS_CHARGE_LIMIT_UPDATE_THRESHOLD_A",
            self.charge_limit_update_threshold_a,
            0.01,
            100.0,
        )?;
        self.charge_limit_min_update_interval_seconds = configured_f64(
            lookup,
            "ESS_CHARGE_LIMIT_MIN_UPDATE_INTERVAL_SECONDS",
            self.charge_limit_min_update_interval_seconds,
            0.0,
            86_400.0,
        )?;
        self.safe_charge_current_a = configured_optional_f64(
            lookup,
            "ESS_SAFE_CHARGE_CURRENT_A",
            self.safe_charge_current_a,
            0.1,
            1_000.0,
        )?;
        self.normal_charge_current_a = configured_optional_f64(
            lookup,
            "ESS_NORMAL_CHARGE_CURRENT_A",
            self.normal_charge_current_a,
            0.1,
            1_000.0,
        )?;
        Ok(())
    }

    fn load_soc<F>(&mut self, lookup: &mut F) -> Result<(), ConfigError>
    where
        F: FnMut(&str) -> Option<String>,
    {
        self.soc_hysteresis =
            configured_f64(lookup, "ESS_SOC_HYSTERESIS", self.soc_hysteresis, 0.0, 20.0)?;
        self.min_soc_epsilon = configured_f64(
            lookup,
            "ESS_MIN_SOC_EPSILON",
            self.min_soc_epsilon,
            0.001,
            5.0,
        )?;
        self.boot_recovery_target_match_epsilon = configured_f64(
            lookup,
            "ESS_BOOT_RECOVERY_TARGET_MATCH_EPSILON",
            self.boot_recovery_target_match_epsilon,
            0.001,
            5.0,
        )?;
        self.boot_recovery_seconds = configured_f64(
            lookup,
            "ESS_BOOT_RECOVERY_SECONDS",
            self.boot_recovery_seconds,
            0.0,
            86_400.0,
        )?;
        self.summer_min_soc = configured_f64(
            lookup,
            "ESS_SUMMER_MIN_SOC",
            self.summer_min_soc,
            MIN_VALID_SOC,
            MAX_VALID_SOC,
        )?;
        self.transition_guard_soc = configured_f64(
            lookup,
            "ESS_TRANSITION_GUARD_SOC",
            self.transition_guard_soc,
            MIN_VALID_SOC,
            MAX_VALID_SOC,
        )?;
        self.winter_target_soc = configured_f64(
            lookup,
            "ESS_WINTER_TARGET_SOC",
            self.winter_target_soc,
            MIN_VALID_SOC,
            MAX_VALID_SOC,
        )?;
        self.balancing_target_soc = configured_f64(
            lookup,
            "ESS_BALANCING_TARGET_SOC",
            self.balancing_target_soc,
            MIN_VALID_SOC,
            MAX_VALID_SOC,
        )?;
        self.summer_manual_minsoc_hold_seconds = configured_f64(
            lookup,
            "ESS_SUMMER_MANUAL_MINSOC_HOLD_SECONDS",
            self.summer_manual_minsoc_hold_seconds,
            0.0,
            604_800.0,
        )?;
        self.min_soc_script_write_match_seconds = configured_f64(
            lookup,
            "ESS_MIN_SOC_SCRIPT_WRITE_MATCH_SECONDS",
            self.min_soc_script_write_match_seconds,
            0.0,
            3_600.0,
        )?;
        Ok(())
    }

    fn load_discharge_protection<F>(&mut self, lookup: &mut F) -> Result<(), ConfigError>
    where
        F: FnMut(&str) -> Option<String>,
    {
        self.discharge_protection_enter_soc = configured_f64(
            lookup,
            "ESS_DISCHARGE_PROTECTION_ENTER_SOC",
            self.discharge_protection_enter_soc,
            MIN_VALID_SOC,
            MAX_VALID_SOC,
        )?;
        self.discharge_protection_release_soc = configured_f64(
            lookup,
            "ESS_DISCHARGE_PROTECTION_RELEASE_SOC",
            self.discharge_protection_release_soc,
            MIN_VALID_SOC,
            MAX_VALID_SOC,
        )?;
        self.discharge_protection_nominal_fraction = configured_f64(
            lookup,
            "ESS_DISCHARGE_PROTECTION_NOMINAL_FRACTION",
            self.discharge_protection_nominal_fraction,
            0.01,
            1.0,
        )?;
        self.discharge_power_epsilon_w = configured_f64(
            lookup,
            "ESS_DISCHARGE_POWER_EPSILON_W",
            self.discharge_power_epsilon_w,
            0.01,
            100.0,
        )?;
        self.discharge_recharge_min_power_w = configured_f64(
            lookup,
            "ESS_DISCHARGE_RECHARGE_MIN_POWER_W",
            self.discharge_recharge_min_power_w,
            0.0,
            100_000.0,
        )?;
        self.discharge_recharge_confirm_seconds = configured_f64(
            lookup,
            "ESS_DISCHARGE_RECHARGE_CONFIRM_SECONDS",
            self.discharge_recharge_confirm_seconds,
            1.0,
            3_600.0,
        )?;
        self.discharge_recharge_max_sample_gap_seconds = configured_f64(
            lookup,
            "ESS_DISCHARGE_RECHARGE_MAX_SAMPLE_GAP_SECONDS",
            self.discharge_recharge_max_sample_gap_seconds,
            1.0,
            3_600.0,
        )?;
        Ok(())
    }

    fn load_charge_ceiling<F>(&mut self, lookup: &mut F) -> Result<(), ConfigError>
    where
        F: FnMut(&str) -> Option<String>,
    {
        self.routine_max_charge_soc = configured_f64(
            lookup,
            "ESS_ROUTINE_MAX_CHARGE_SOC",
            self.routine_max_charge_soc,
            MIN_VALID_SOC,
            MAX_VALID_SOC,
        )?;
        self.full_max_charge_soc = configured_f64(
            lookup,
            "ESS_FULL_MAX_CHARGE_SOC",
            self.full_max_charge_soc,
            MIN_VALID_SOC,
            MAX_VALID_SOC,
        )?;
        self.full_charge_reached_soc = configured_f64(
            lookup,
            "ESS_FULL_CHARGE_REACHED_SOC",
            self.full_charge_reached_soc,
            MIN_VALID_SOC,
            MAX_VALID_SOC,
        )?;
        self.full_charge_min_age_days = configured_usize(
            lookup,
            "ESS_FULL_CHARGE_MIN_AGE_DAYS",
            self.full_charge_min_age_days,
            0,
            365,
        )?;
        self.charge_ceiling_current_epsilon_a = configured_f64(
            lookup,
            "ESS_CHARGE_CEILING_CURRENT_EPSILON_A",
            self.charge_ceiling_current_epsilon_a,
            0.001,
            1.0,
        )?;
        self.full_charge_confirm_seconds = configured_f64(
            lookup,
            "ESS_FULL_CHARGE_CONFIRM_SECONDS",
            self.full_charge_confirm_seconds,
            1.0,
            86_400.0,
        )?;
        self.full_charge_max_sample_gap_seconds = configured_f64(
            lookup,
            "ESS_FULL_CHARGE_MAX_SAMPLE_GAP_SECONDS",
            self.full_charge_max_sample_gap_seconds,
            1.0,
            3_600.0,
        )?;
        Ok(())
    }

    fn load_pv<F>(&mut self, lookup: &mut F) -> Result<(), ConfigError>
    where
        F: FnMut(&str) -> Option<String>,
    {
        self.pv_threshold_w = configured_f64(
            lookup,
            "ESS_PV_THRESHOLD_W",
            self.pv_threshold_w,
            0.0,
            1_000_000.0,
        )?;
        self.transition_days =
            configured_usize(lookup, "ESS_TRANSITION_DAYS", self.transition_days, 1, 31)?;
        self.pv_min_daily_coverage_fraction = configured_f64(
            lookup,
            "ESS_PV_MIN_DAILY_COVERAGE_FRACTION",
            self.pv_min_daily_coverage_fraction,
            0.01,
            1.0,
        )?;
        Ok(())
    }

    fn load_season_dates<F>(&mut self, lookup: &mut F) -> Result<(), ConfigError>
    where
        F: FnMut(&str) -> Option<String>,
    {
        self.winter_start_mmdd =
            configured_mmdd(lookup, "ESS_WINTER_START_MMDD", self.winter_start_mmdd)?;
        self.winter_end_mmdd =
            configured_mmdd(lookup, "ESS_WINTER_END_MMDD", self.winter_end_mmdd)?;
        self.transition_pre_start_mmdd = configured_mmdd(
            lookup,
            "ESS_TRANSITION_PRE_START_MMDD",
            self.transition_pre_start_mmdd,
        )?;
        self.transition_pre_end_mmdd = configured_mmdd(
            lookup,
            "ESS_TRANSITION_PRE_END_MMDD",
            self.transition_pre_end_mmdd,
        )?;
        self.transition_post_start_mmdd = configured_mmdd(
            lookup,
            "ESS_TRANSITION_POST_START_MMDD",
            self.transition_post_start_mmdd,
        )?;
        self.transition_post_end_mmdd = configured_mmdd(
            lookup,
            "ESS_TRANSITION_POST_END_MMDD",
            self.transition_post_end_mmdd,
        )?;
        Ok(())
    }

    fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    fn validate(&self) -> Result<(), ConfigError> {
        require(
            self.summer_min_soc <= self.transition_guard_soc
                && self.transition_guard_soc <= self.winter_target_soc
                && self.winter_target_soc <= self.balancing_target_soc,
            "SoC targets must satisfy summer <= transition <= winter <= balancing",
        )?;
        require(
            self.balancing_full_soc <= self.balancing_target_soc,
            "ESS_BALANCING_FULL_SOC must not exceed ESS_BALANCING_TARGET_SOC",
        )?;
        require(
            self.discharge_protection_enter_soc < self.discharge_protection_release_soc,
            "ESS_DISCHARGE_PROTECTION_ENTER_SOC must be below ESS_DISCHARGE_PROTECTION_RELEASE_SOC",
        )?;
        require(
            self.discharge_recharge_max_sample_gap_seconds
                <= self.discharge_recharge_confirm_seconds,
            "ESS_DISCHARGE_RECHARGE_MAX_SAMPLE_GAP_SECONDS must not exceed ESS_DISCHARGE_RECHARGE_CONFIRM_SECONDS",
        )?;
        require(
            self.routine_max_charge_soc < self.full_charge_reached_soc
                && self.full_charge_reached_soc <= self.full_max_charge_soc,
            "charge ceiling values must satisfy routine < reached <= full",
        )?;
        require(
            self.full_charge_max_sample_gap_seconds <= self.full_charge_confirm_seconds,
            "ESS_FULL_CHARGE_MAX_SAMPLE_GAP_SECONDS must not exceed ESS_FULL_CHARGE_CONFIRM_SECONDS",
        )?;
        require(
            self.balancing_target_soc <= self.full_max_charge_soc,
            "ESS_BALANCING_TARGET_SOC must not exceed ESS_FULL_MAX_CHARGE_SOC",
        )?;
        require(
            self.balancing_duration_hours <= self.balancing_max_hours,
            "ESS_BALANCING_DURATION_HOURS must not exceed ESS_BALANCING_MAX_HOURS",
        )?;
        require(
            self.grid_pause_headroom_w < self.grid_load_limit_w,
            "ESS_GRID_PAUSE_HEADROOM_W must be below ESS_GRID_LOAD_LIMIT_W",
        )?;
        require(
            u16::from(self.charge_window_base_hours) * u16::from(self.charge_window_max_multiplier)
                <= 24,
            "charge-window base hours multiplied by the maximum multiplier must not exceed 24",
        )?;
        require(
            self.winter_start_mmdd > self.winter_end_mmdd,
            "winter dates must form a year-spanning window with start after end",
        )?;
        require(
            self.transition_pre_start_mmdd <= self.transition_pre_end_mmdd
                && self.transition_pre_end_mmdd < self.winter_start_mmdd,
            "pre-winter dates must be ordered and end before winter starts",
        )?;
        require(
            self.winter_end_mmdd < self.transition_post_start_mmdd
                && self.transition_post_start_mmdd <= self.transition_post_end_mmdd,
            "post-winter dates must start after winter ends and be ordered",
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigError(String);

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for ConfigError {}

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub settings_service: String,
    pub system_service: String,
    pub fallback_battery_service: Option<String>,
    pub runtime_dir: PathBuf,
    pub instance_lock_file: PathBuf,
    pub state_device_id: String,
    pub state_file: PathBuf,
    pub legacy_state_file: Option<PathBuf>,
    pub durable_restore_file: PathBuf,
    pub log_file: PathBuf,
    pub decision_file: Option<PathBuf>,
    pub sd_path: Option<PathBuf>,
    pub sd_label: Option<String>,
    pub shadow: bool,
    pub one_shot: bool,
    pub loop_interval: Duration,
    pub dbus_timeout: Duration,
    pub status_log_interval: Duration,
    pub invalid_log_interval: Duration,
    pub battery_max_current_max_age: Duration,
    pub nominal_inverter_power_max_age: Duration,
    pub configured_nominal_inverter_power_w: Option<f64>,
    pub sd_save_interval: Duration,
    pub sd_lookup_interval: Duration,
    pub sd_backoff_max: Duration,
    pub policy: PolicyConfig,
}

impl RuntimeConfig {
    /// Load and validate runtime and policy configuration from the environment.
    ///
    /// # Errors
    ///
    /// Returns a descriptive error for malformed values or inconsistent policy.
    pub fn from_env() -> Result<Self, ConfigError> {
        let shadow = env_flag("ESS_SHADOW_MODE")?;
        let paths = runtime_paths(shadow)?;
        let config = Self {
            settings_service: env_text("ESS_SERVICE_SETTINGS")
                .unwrap_or_else(|| "com.victronenergy.settings".to_owned()),
            system_service: env_text("ESS_SERVICE_SYSTEM")
                .unwrap_or_else(|| "com.victronenergy.system".to_owned()),
            fallback_battery_service: configured_battery_service_fallback()?,
            instance_lock_file: paths.instance_lock_file,
            state_device_id: state_device_id()?,
            runtime_dir: paths.runtime_dir,
            state_file: paths.state_file,
            legacy_state_file: paths.legacy_state_file,
            durable_restore_file: env_path("ESS_DURABLE_RESTORE_FILE")
                .unwrap_or_else(|| PathBuf::from(DEFAULT_DURABLE_RESTORE_FILE)),
            log_file: paths.log_file,
            decision_file: paths.decision_file,
            sd_path: env_path("ESS_SD_PATH"),
            sd_label: env_text("ESS_SD_LABEL"),
            shadow,
            one_shot: env_flag("ESS_ONESHOT")?,
            loop_interval: Duration::from_secs_f64(configured_env_f64(
                "ESS_LOOP_INTERVAL_SECONDS",
                LOOP_INTERVAL_SECONDS,
                5.0,
                3_600.0,
            )?),
            dbus_timeout: Duration::from_secs_f64(configured_env_f64(
                "ESS_DBUS_TIMEOUT_SECONDS",
                2.0,
                0.1,
                30.0,
            )?),
            status_log_interval: configured_env_duration(
                "ESS_STATUS_LOG_INTERVAL_SECONDS",
                STATUS_LOG_INTERVAL_SECONDS,
                1.0,
                86_400.0,
            )?,
            invalid_log_interval: configured_env_duration(
                "ESS_INVALID_LOG_INTERVAL_SECONDS",
                INVALID_LOG_INTERVAL_SECONDS,
                1.0,
                86_400.0,
            )?,
            battery_max_current_max_age: configured_env_duration(
                "ESS_BATTERY_MAX_CURRENT_MAX_AGE_SECONDS",
                BATTERY_MAX_CURRENT_MAX_AGE_SECONDS,
                5.0,
                86_400.0,
            )?,
            nominal_inverter_power_max_age: configured_env_duration(
                "ESS_NOMINAL_INVERTER_POWER_MAX_AGE_SECONDS",
                NOMINAL_INVERTER_POWER_MAX_AGE_SECONDS,
                60.0,
                2_592_000.0,
            )?,
            configured_nominal_inverter_power_w: configured_env_optional_f64(
                "ESS_NOMINAL_INVERTER_POWER_W",
                1.0,
                1_000_000.0,
            )?,
            sd_save_interval: configured_env_duration(
                "ESS_SD_SAVE_INTERVAL_SECONDS",
                SD_SAVE_INTERVAL_SECONDS,
                60.0,
                604_800.0,
            )?,
            sd_lookup_interval: configured_env_duration(
                "ESS_SD_LOOKUP_INTERVAL_SECONDS",
                SD_LOOKUP_INTERVAL_SECONDS,
                1.0,
                86_400.0,
            )?,
            sd_backoff_max: Duration::from_secs(configured_env_u64(
                "ESS_SD_BACKOFF_MAX_SECONDS",
                u64::from(SD_BACKOFF_MAX_SECONDS),
                1,
                3_600,
            )?),
            policy: PolicyConfig::from_env()?,
        };
        require(
            config.loop_interval.as_secs_f64()
                <= config.policy.discharge_recharge_max_sample_gap_seconds,
            "ESS_LOOP_INTERVAL_SECONDS must not exceed ESS_DISCHARGE_RECHARGE_MAX_SAMPLE_GAP_SECONDS",
        )?;
        require(
            config.loop_interval.as_secs_f64() <= config.policy.full_charge_max_sample_gap_seconds,
            "ESS_LOOP_INTERVAL_SECONDS must not exceed ESS_FULL_CHARGE_MAX_SAMPLE_GAP_SECONDS",
        )?;
        Ok(config)
    }
}

struct RuntimePaths {
    runtime_dir: PathBuf,
    instance_lock_file: PathBuf,
    state_file: PathBuf,
    legacy_state_file: Option<PathBuf>,
    log_file: PathBuf,
    decision_file: Option<PathBuf>,
}

fn runtime_paths(shadow: bool) -> Result<RuntimePaths, ConfigError> {
    let runtime_dir =
        env_path("ESS_RUNTIME_DIR").unwrap_or_else(|| PathBuf::from(DEFAULT_RUNTIME_DIR));
    let legacy_state_path = PathBuf::from(if shadow {
        "/dev/shm/ess_winter_logic.rust-shadow.json"
    } else {
        "/dev/shm/ess_winter_logic.json"
    });
    let (state_file, legacy_state_file) = match env_path("ESS_STATE_FILE") {
        Some(path) if path != legacy_state_path => (path, None),
        _ => (
            runtime_dir.join(if shadow {
                "state-shadow.json"
            } else {
                "state.json"
            }),
            Some(legacy_state_path),
        ),
    };
    let legacy_log_path = PathBuf::from(if shadow {
        "/dev/shm/ess_winter_log.rust-shadow.txt"
    } else {
        "/dev/shm/ess_winter_log.txt"
    });
    let log_file = match env_path("ESS_LOG_FILE") {
        Some(path) if path != legacy_log_path => path,
        _ => runtime_dir.join(if shadow { "log-shadow.txt" } else { "log.txt" }),
    };
    let instance_lock_file = env_path("ESS_INSTANCE_LOCK_FILE")
        .unwrap_or_else(|| runtime_dir.join(if shadow { "shadow.lock" } else { "active.lock" }));
    let decision_file = match env_path("ESS_DECISION_FILE") {
        Some(path)
            if shadow && path == PathBuf::from("/run/ess-winter-rust-shadow-decision.json") =>
        {
            Some(runtime_dir.join("decision-shadow.json"))
        }
        other => other,
    };
    validate_runtime_paths(
        &runtime_dir,
        &state_file,
        &log_file,
        &instance_lock_file,
        decision_file.as_ref(),
    )?;
    Ok(RuntimePaths {
        runtime_dir,
        instance_lock_file,
        state_file,
        legacy_state_file,
        log_file,
        decision_file,
    })
}

fn validate_runtime_paths(
    runtime_dir: &std::path::Path,
    state_file: &std::path::Path,
    log_file: &std::path::Path,
    instance_lock_file: &std::path::Path,
    decision_file: Option<&PathBuf>,
) -> Result<(), ConfigError> {
    for (name, path) in [
        ("ESS_STATE_FILE", Some(state_file)),
        ("ESS_LOG_FILE", Some(log_file)),
        ("ESS_INSTANCE_LOCK_FILE", Some(instance_lock_file)),
        ("ESS_DECISION_FILE", decision_file.map(PathBuf::as_path)),
    ] {
        if path.is_some_and(|path| path.parent() != Some(runtime_dir)) {
            return Err(ConfigError(format!(
                "{name} must be a direct child of {}",
                runtime_dir.display()
            )));
        }
    }
    Ok(())
}

fn state_device_id() -> Result<String, ConfigError> {
    if let Some(configured) = env_text("ESS_STATE_DEVICE_ID") {
        return validate_device_id(&configured, "ESS_STATE_DEVICE_ID");
    }
    for path in ["/etc/machine-id", "/var/lib/dbus/machine-id"] {
        if let Some(value) = read_small_text(path) {
            return validate_device_id(&value, path);
        }
    }
    Err(ConfigError(
        "no stable device identity found; set ESS_STATE_DEVICE_ID".to_owned(),
    ))
}

fn read_small_text(path: &str) -> Option<String> {
    let file = File::open(path).ok()?;
    let mut bytes = Vec::new();
    file.take(129).read_to_end(&mut bytes).ok()?;
    if bytes.len() > 128 {
        return None;
    }
    String::from_utf8(bytes).ok()
}

fn validate_device_id(value: &str, source: &str) -> Result<String, ConfigError> {
    let value = value.trim();
    let valid = !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'));
    if !valid {
        return Err(ConfigError(format!(
            "invalid stable device identity from {source}"
        )));
    }
    Ok(value.to_owned())
}

fn configured_env_duration(
    name: &str,
    default: f64,
    minimum: f64,
    maximum: f64,
) -> Result<Duration, ConfigError> {
    configured_env_f64(name, default, minimum, maximum).map(Duration::from_secs_f64)
}

fn configured_env_u64(
    name: &str,
    default: u64,
    minimum: u64,
    maximum: u64,
) -> Result<u64, ConfigError> {
    let Ok(value) = env::var(name) else {
        return Ok(default);
    };
    let parsed = value
        .trim()
        .parse::<u64>()
        .map_err(|_| ConfigError(format!("{name} must be an integer")))?;
    require(
        (minimum..=maximum).contains(&parsed),
        &format!("{name} must be between {minimum} and {maximum}"),
    )?;
    Ok(parsed)
}

fn configured_env_f64(
    name: &str,
    default: f64,
    minimum: f64,
    maximum: f64,
) -> Result<f64, ConfigError> {
    configured_f64(
        &mut |key| env::var(key).ok(),
        name,
        default,
        minimum,
        maximum,
    )
}

fn configured_env_optional_f64(
    name: &str,
    minimum: f64,
    maximum: f64,
) -> Result<Option<f64>, ConfigError> {
    configured_optional_f64(&mut |key| env::var(key).ok(), name, None, minimum, maximum)
}

fn configured_battery_service_fallback() -> Result<Option<String>, ConfigError> {
    let Some(service) = env_text("ESS_PREFERRED_BATTERY_SERVICE") else {
        return Ok(None);
    };
    let valid = service
        .strip_prefix("com.victronenergy.battery")
        .is_some_and(|suffix| suffix.starts_with('.'));
    if valid {
        Ok(Some(service))
    } else {
        Err(ConfigError(
            "ESS_PREFERRED_BATTERY_SERVICE must name a com.victronenergy.battery service"
                .to_owned(),
        ))
    }
}

fn configured_f64<F>(
    lookup: &mut F,
    name: &str,
    default: f64,
    minimum: f64,
    maximum: f64,
) -> Result<f64, ConfigError>
where
    F: FnMut(&str) -> Option<String>,
{
    let Some(value) = lookup(name) else {
        return Ok(default);
    };
    let parsed = value
        .trim()
        .parse::<f64>()
        .map_err(|_| ConfigError(format!("{name} must be a number")))?;
    require(
        parsed.is_finite() && (minimum..=maximum).contains(&parsed),
        &format!("{name} must be between {minimum} and {maximum}"),
    )?;
    Ok(parsed)
}

fn configured_optional_f64<F>(
    lookup: &mut F,
    name: &str,
    default: Option<f64>,
    minimum: f64,
    maximum: f64,
) -> Result<Option<f64>, ConfigError>
where
    F: FnMut(&str) -> Option<String>,
{
    let Some(value) = lookup(name) else {
        return Ok(default);
    };
    if matches!(value.trim(), "" | "none" | "off" | "-1") {
        return Ok(None);
    }
    configured_f64(
        &mut |_| Some(value.clone()),
        name,
        default.unwrap_or(minimum),
        minimum,
        maximum,
    )
    .map(Some)
}

fn configured_u8<F>(
    lookup: &mut F,
    name: &str,
    default: u8,
    minimum: u8,
    maximum: u8,
) -> Result<u8, ConfigError>
where
    F: FnMut(&str) -> Option<String>,
{
    let Some(value) = lookup(name) else {
        return Ok(default);
    };
    let parsed = value
        .trim()
        .parse::<u8>()
        .map_err(|_| ConfigError(format!("{name} must be an integer")))?;
    require(
        (minimum..=maximum).contains(&parsed),
        &format!("{name} must be between {minimum} and {maximum}"),
    )?;
    Ok(parsed)
}

fn configured_usize<F>(
    lookup: &mut F,
    name: &str,
    default: usize,
    minimum: usize,
    maximum: usize,
) -> Result<usize, ConfigError>
where
    F: FnMut(&str) -> Option<String>,
{
    let Some(value) = lookup(name) else {
        return Ok(default);
    };
    let parsed = value
        .trim()
        .parse::<usize>()
        .map_err(|_| ConfigError(format!("{name} must be an integer")))?;
    require(
        (minimum..=maximum).contains(&parsed),
        &format!("{name} must be between {minimum} and {maximum}"),
    )?;
    Ok(parsed)
}

fn configured_mmdd<F>(lookup: &mut F, name: &str, default: u16) -> Result<u16, ConfigError>
where
    F: FnMut(&str) -> Option<String>,
{
    let Some(value) = lookup(name) else {
        return Ok(default);
    };
    let parsed = value
        .trim()
        .parse::<u16>()
        .map_err(|_| ConfigError(format!("{name} must use MMDD numeric form")))?;
    require(
        valid_mmdd(parsed),
        &format!("{name} is not a valid MMDD date"),
    )?;
    Ok(parsed)
}

const fn valid_mmdd(value: u16) -> bool {
    let month = value / 100;
    let day = value % 100;
    let maximum_day = match month {
        2 => 29,
        4 | 6 | 9 | 11 => 30,
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        _ => 0,
    };
    day >= 1 && day <= maximum_day
}

fn require(condition: bool, message: &str) -> Result<(), ConfigError> {
    if condition {
        Ok(())
    } else {
        Err(ConfigError(message.to_owned()))
    }
}

fn env_flag(name: &str) -> Result<bool, ConfigError> {
    match env::var(name) {
        Ok(value) => parse_flag(name, &value),
        Err(env::VarError::NotPresent) => Ok(false),
        Err(env::VarError::NotUnicode(_)) => Err(invalid_flag_error(name)),
    }
}

fn parse_flag(name: &str, value: &str) -> Result<bool, ConfigError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(invalid_flag_error(name)),
    }
}

fn invalid_flag_error(name: &str) -> ConfigError {
    ConfigError(format!(
        "{name} must be one of 1, true, yes, on, 0, false, no, or off"
    ))
}

fn env_text(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn env_path(name: &str) -> Option<PathBuf> {
    env_text(name).map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::{
        CHARGE_CEILING_CURRENT_EPSILON_A, FULL_CHARGE_CONFIRM_SECONDS,
        FULL_CHARGE_MAX_SAMPLE_GAP_SECONDS, FULL_CHARGE_MIN_AGE_DAYS, FULL_CHARGE_REACHED_SOC,
        FULL_MAX_CHARGE_SOC, PolicyConfig, ROUTINE_MAX_CHARGE_SOC, parse_flag, valid_mmdd,
    };
    use std::collections::HashMap;

    #[test]
    fn policy_defaults_are_preserved_without_overrides() {
        assert_eq!(
            PolicyConfig::from_lookup(|_| None),
            Ok(PolicyConfig::default())
        );
    }

    #[test]
    fn policy_overrides_are_typed_and_cross_validated() {
        let values = policy_override_values();
        let policy =
            PolicyConfig::from_lookup(|name| values.get(name).map(|value| (*value).to_owned()))
                .unwrap_or_else(|_| std::process::abort());
        assert_eq!(policy, expected_overridden_policy());
    }

    fn policy_override_values() -> HashMap<&'static str, &'static str> {
        HashMap::from([
            ("ESS_BALANCING_INTERVAL_DAYS", "15"),
            ("ESS_BALANCING_DURATION_HOURS", "5"),
            ("ESS_BALANCING_APPROACH_MAX_HOURS", "72"),
            ("ESS_BALANCING_MAX_HOURS", "13"),
            ("ESS_BALANCING_RETRY_COOLDOWN_HOURS", "25"),
            ("ESS_BALANCING_BOOT_GRACE_HOURS", "25"),
            ("ESS_BALANCING_FULL_SOC", "98"),
            ("ESS_FULL_SOC_CONFIRM_MINUTES", "11"),
            ("ESS_GRID_LOAD_LIMIT_W", "5000"),
            ("ESS_GRID_PAUSE_HEADROOM_W", "200"),
            ("ESS_GRID_SOFT_MIN_CHARGE_CURRENT_A", "11"),
            ("ESS_CHARGE_WINDOW_BASE_HOURS", "3"),
            ("ESS_CHARGE_WINDOW_ESCALATION_NIGHTS", "3"),
            ("ESS_CHARGE_WINDOW_MAX_MULTIPLIER", "4"),
            ("ESS_CHARGE_EFFICIENCY", "0.85"),
            ("ESS_GRID_CHARGE_MAX_FRACTION", "0.35"),
            ("ESS_CHARGE_LIMIT_UPDATE_THRESHOLD_A", "2"),
            ("ESS_CHARGE_LIMIT_MIN_UPDATE_INTERVAL_SECONDS", "301"),
            ("ESS_SAFE_CHARGE_CURRENT_A", "60"),
            ("ESS_NORMAL_CHARGE_CURRENT_A", "70"),
            ("ESS_SOC_HYSTERESIS", "2"),
            ("ESS_DISCHARGE_PROTECTION_ENTER_SOC", "21"),
            ("ESS_DISCHARGE_PROTECTION_RELEASE_SOC", "26"),
            ("ESS_DISCHARGE_PROTECTION_NOMINAL_FRACTION", "0.35"),
            ("ESS_DISCHARGE_POWER_EPSILON_W", "2"),
            ("ESS_DISCHARGE_RECHARGE_MIN_POWER_W", "150"),
            ("ESS_DISCHARGE_RECHARGE_CONFIRM_SECONDS", "180"),
            ("ESS_DISCHARGE_RECHARGE_MAX_SAMPLE_GAP_SECONDS", "80"),
            ("ESS_MIN_SOC_EPSILON", "0.2"),
            ("ESS_BOOT_RECOVERY_TARGET_MATCH_EPSILON", "0.2"),
            ("ESS_BOOT_RECOVERY_SECONDS", "601"),
            ("ESS_PV_THRESHOLD_W", "3100"),
            ("ESS_TRANSITION_DAYS", "5"),
            ("ESS_SUMMER_MIN_SOC", "15"),
            ("ESS_TRANSITION_GUARD_SOC", "45"),
            ("ESS_WINTER_TARGET_SOC", "60"),
            ("ESS_BALANCING_TARGET_SOC", "100"),
            ("ESS_PV_MIN_DAILY_COVERAGE_FRACTION", "0.8"),
            ("ESS_SUMMER_MANUAL_MINSOC_HOLD_SECONDS", "90000"),
            ("ESS_MIN_SOC_SCRIPT_WRITE_MATCH_SECONDS", "200"),
            ("ESS_CHARGE_WINDOW_START_HOUR", "22"),
            ("ESS_WINTER_START_MMDD", "1126"),
            ("ESS_WINTER_END_MMDD", "0204"),
            ("ESS_TRANSITION_PRE_START_MMDD", "1104"),
            ("ESS_TRANSITION_PRE_END_MMDD", "1125"),
            ("ESS_TRANSITION_POST_START_MMDD", "0205"),
            ("ESS_TRANSITION_POST_END_MMDD", "0224"),
        ])
    }

    fn expected_overridden_policy() -> PolicyConfig {
        PolicyConfig {
            balancing_interval_days: 15.0,
            balancing_duration_hours: 5.0,
            balancing_approach_max_hours: 72.0,
            balancing_max_hours: 13.0,
            balancing_retry_cooldown_hours: 25.0,
            balancing_boot_grace_hours: 25.0,
            balancing_full_soc: 98.0,
            full_soc_confirm_minutes: 11.0,
            grid_load_limit_w: 5_000.0,
            grid_pause_headroom_w: 200.0,
            grid_soft_min_charge_current_a: 11.0,
            charge_window_start_hour: 22,
            charge_window_base_hours: 3,
            charge_window_escalation_nights: 3.0,
            charge_window_max_multiplier: 4,
            charge_efficiency: 0.85,
            grid_charge_max_fraction: 0.35,
            charge_limit_update_threshold_a: 2.0,
            charge_limit_min_update_interval_seconds: 301.0,
            safe_charge_current_a: Some(60.0),
            normal_charge_current_a: Some(70.0),
            soc_hysteresis: 2.0,
            discharge_protection_enter_soc: 21.0,
            discharge_protection_release_soc: 26.0,
            discharge_protection_nominal_fraction: 0.35,
            discharge_power_epsilon_w: 2.0,
            discharge_recharge_min_power_w: 150.0,
            discharge_recharge_confirm_seconds: 180.0,
            discharge_recharge_max_sample_gap_seconds: 80.0,
            routine_max_charge_soc: ROUTINE_MAX_CHARGE_SOC,
            full_max_charge_soc: FULL_MAX_CHARGE_SOC,
            full_charge_reached_soc: FULL_CHARGE_REACHED_SOC,
            full_charge_min_age_days: FULL_CHARGE_MIN_AGE_DAYS,
            charge_ceiling_current_epsilon_a: CHARGE_CEILING_CURRENT_EPSILON_A,
            full_charge_confirm_seconds: FULL_CHARGE_CONFIRM_SECONDS,
            full_charge_max_sample_gap_seconds: FULL_CHARGE_MAX_SAMPLE_GAP_SECONDS,
            min_soc_epsilon: 0.2,
            boot_recovery_target_match_epsilon: 0.2,
            boot_recovery_seconds: 601.0,
            pv_threshold_w: 3_100.0,
            transition_days: 5,
            summer_min_soc: 15.0,
            transition_guard_soc: 45.0,
            winter_target_soc: 60.0,
            balancing_target_soc: 100.0,
            pv_min_daily_coverage_fraction: 0.8,
            summer_manual_minsoc_hold_seconds: 90_000.0,
            min_soc_script_write_match_seconds: 200.0,
            winter_start_mmdd: 1_126,
            winter_end_mmdd: 204,
            transition_pre_start_mmdd: 1_104,
            transition_pre_end_mmdd: 1_125,
            transition_post_start_mmdd: 205,
            transition_post_end_mmdd: 224,
        }
    }

    #[test]
    fn charge_ceiling_overrides_are_typed() {
        let values = HashMap::from([
            ("ESS_ROUTINE_MAX_CHARGE_SOC", "89"),
            ("ESS_FULL_MAX_CHARGE_SOC", "100"),
            ("ESS_FULL_CHARGE_REACHED_SOC", "97"),
            ("ESS_FULL_CHARGE_MIN_AGE_DAYS", "3"),
            ("ESS_FULL_CHARGE_CONFIRM_SECONDS", "10800"),
            ("ESS_FULL_CHARGE_MAX_SAMPLE_GAP_SECONDS", "120"),
            ("ESS_CHARGE_CEILING_CURRENT_EPSILON_A", "0.2"),
        ]);
        let policy =
            PolicyConfig::from_lookup(|name| values.get(name).map(|value| (*value).to_owned()))
                .unwrap_or_else(|_| std::process::abort());
        assert_eq!(policy.routine_max_charge_soc.to_bits(), 89.0_f64.to_bits());
        assert_eq!(policy.full_max_charge_soc.to_bits(), 100.0_f64.to_bits());
        assert_eq!(policy.full_charge_reached_soc.to_bits(), 97.0_f64.to_bits());
        assert_eq!(policy.full_charge_min_age_days, 3);
        assert_eq!(
            policy.full_charge_confirm_seconds.to_bits(),
            10_800.0_f64.to_bits()
        );
        assert_eq!(
            policy.full_charge_max_sample_gap_seconds.to_bits(),
            120.0_f64.to_bits()
        );
        assert_eq!(
            policy.charge_ceiling_current_epsilon_a.to_bits(),
            0.2_f64.to_bits()
        );
    }

    #[test]
    fn inconsistent_policy_is_rejected() {
        let values = HashMap::from([
            ("ESS_SUMMER_MIN_SOC", "45"),
            ("ESS_TRANSITION_GUARD_SOC", "40"),
        ]);
        let error =
            PolicyConfig::from_lookup(|name| values.get(name).map(|value| (*value).to_owned()))
                .err()
                .unwrap_or_else(|| std::process::abort());
        assert!(error.to_string().contains("summer <= transition"));

        let discharge_values = HashMap::from([
            ("ESS_DISCHARGE_PROTECTION_ENTER_SOC", "25"),
            ("ESS_DISCHARGE_PROTECTION_RELEASE_SOC", "25"),
        ]);
        let discharge_error = PolicyConfig::from_lookup(|name| {
            discharge_values.get(name).map(|value| (*value).to_owned())
        })
        .err()
        .unwrap_or_else(|| std::process::abort());
        assert!(discharge_error.to_string().contains("must be below"));

        let recharge_values = HashMap::from([
            ("ESS_DISCHARGE_RECHARGE_CONFIRM_SECONDS", "60"),
            ("ESS_DISCHARGE_RECHARGE_MAX_SAMPLE_GAP_SECONDS", "61"),
        ]);
        let recharge_error = PolicyConfig::from_lookup(|name| {
            recharge_values.get(name).map(|value| (*value).to_owned())
        })
        .err()
        .unwrap_or_else(|| std::process::abort());
        assert!(
            recharge_error
                .to_string()
                .contains("MAX_SAMPLE_GAP_SECONDS must not exceed")
        );

        let ceiling_values = HashMap::from([
            ("ESS_ROUTINE_MAX_CHARGE_SOC", "98"),
            ("ESS_FULL_CHARGE_REACHED_SOC", "98"),
        ]);
        let ceiling_error = PolicyConfig::from_lookup(|name| {
            ceiling_values.get(name).map(|value| (*value).to_owned())
        })
        .err()
        .unwrap_or_else(|| std::process::abort());
        assert!(ceiling_error.to_string().contains("routine < reached"));

        let non_spanning_winter = HashMap::from([
            ("ESS_WINTER_START_MMDD", "0401"),
            ("ESS_WINTER_END_MMDD", "0930"),
            ("ESS_TRANSITION_PRE_START_MMDD", "0320"),
            ("ESS_TRANSITION_PRE_END_MMDD", "0331"),
            ("ESS_TRANSITION_POST_START_MMDD", "1001"),
            ("ESS_TRANSITION_POST_END_MMDD", "1010"),
        ]);
        let winter_error = PolicyConfig::from_lookup(|name| {
            non_spanning_winter
                .get(name)
                .map(|value| (*value).to_owned())
        })
        .err()
        .unwrap_or_else(|| std::process::abort());
        assert!(winter_error.to_string().contains("year-spanning"));
    }

    #[test]
    fn invalid_full_charge_confirmation_settings_are_rejected() {
        for (name, value) in [
            ("ESS_FULL_CHARGE_CONFIRM_SECONDS", "0"),
            ("ESS_FULL_CHARGE_CONFIRM_SECONDS", "NaN"),
            ("ESS_FULL_CHARGE_MAX_SAMPLE_GAP_SECONDS", "0"),
            ("ESS_FULL_CHARGE_MAX_SAMPLE_GAP_SECONDS", "inf"),
            ("ESS_FULL_CHARGE_CONFIRM_SECONDS", "30"),
        ] {
            assert!(
                PolicyConfig::from_lookup(|key| (key == name).then(|| value.to_owned())).is_err()
            );
        }
    }

    #[test]
    fn malformed_policy_values_and_dates_are_rejected() {
        assert!(
            PolicyConfig::from_lookup(|name| {
                (name == "ESS_CHARGE_WINDOW_START_HOUR").then(|| "24".to_owned())
            })
            .is_err()
        );
        assert!(
            PolicyConfig::from_lookup(|name| {
                (name == "ESS_PV_THRESHOLD_W").then(|| "NaN".to_owned())
            })
            .is_err()
        );
        assert!(
            PolicyConfig::from_lookup(|name| {
                (name == "ESS_BALANCING_APPROACH_MAX_HOURS").then(|| "0".to_owned())
            })
            .is_err()
        );
        for value in ["0", "1.01", "invalid"] {
            assert!(
                PolicyConfig::from_lookup(|name| {
                    (name == "ESS_PV_MIN_DAILY_COVERAGE_FRACTION").then(|| value.to_owned())
                })
                .is_err()
            );
        }
        assert!(valid_mmdd(229));
        assert!(!valid_mmdd(230));
        assert!(!valid_mmdd(1301));
    }

    #[test]
    fn boolean_flags_accept_documented_values_case_insensitively() {
        for value in ["1", "true", "TRUE", "True", "yes", "Yes", "ON", " on "] {
            assert_eq!(parse_flag("ESS_SHADOW_MODE", value), Ok(true));
        }

        for value in ["0", "false", "FALSE", "False", "no", "No", "OFF", " off "] {
            assert_eq!(parse_flag("ESS_SHADOW_MODE", value), Ok(false));
        }
    }

    #[test]
    fn boolean_flags_reject_empty_and_unknown_values() {
        for value in ["", " ", "tru", "2", "enabled", "disabled"] {
            let error = parse_flag("ESS_SHADOW_MODE", value)
                .err()
                .unwrap_or_else(|| std::process::abort());
            assert!(
                error
                    .to_string()
                    .starts_with("ESS_SHADOW_MODE must be one of")
            );
        }
    }
}

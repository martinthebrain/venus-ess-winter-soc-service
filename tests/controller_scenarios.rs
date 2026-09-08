use std::collections::{HashMap, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use venus_ess_winter_soc_service::charge_ceiling::calendar_day_number;
use venus_ess_winter_soc_service::clock::{Clock, LocalDateTime};
use venus_ess_winter_soc_service::config::{
    AC_CONSUMPTION_ON_INPUT_PHASE_COUNT_PATH, AC_GRID_PHASE_COUNT_PATH, ACTIVE_BMS_SERVICE_PATH,
    BATTERY_POWER_PATH, BATTERY_SOC_PATH, BATTERY_VOLTAGE_PATH, BMS_ALLOW_TO_CHARGE_PATH,
    BMS_MAX_CHARGE_CURRENT_PATH, DC_PV_POWER_PATH, MAX_CHARGE_CURRENT_PATH,
    MAX_DISCHARGE_POWER_PATH, MIN_SOC_PATH, NOMINAL_INVERTER_POWER_PATH, RuntimeConfig,
    SYSTEM_CHARGE_DISABLED_PATH, SYSTEM_USER_CHARGE_LIMITED_PATH, VEBUS_ALLOW_TO_CHARGE_PATH,
    VEBUS_MAX_CHARGE_CURRENT_PATH, VEBUS_SERVICE_PATH,
};
use venus_ess_winter_soc_service::controller::Controller;
use venus_ess_winter_soc_service::domain::{
    ChargeCurrentControlState, ChargeCurrentLimitUnavailableReason, ChargeCurrentWriteKind,
    ChargingInhibitReason, ControllerState, CycleOutcome, DbusFailureKind,
    DischargeProtectionState, DischargeWriteKind, MinimumSocControlState,
    PendingChargeCurrentWrite, PendingDischargeWrite, ReserveChargingPauseReason,
};
use venus_ess_winter_soc_service::logging::LogSink;
use venus_ess_winter_soc_service::ports::{DbusPort, PortError, StatePort};

#[derive(Clone)]
struct FixedClock {
    local: LocalDateTime,
    epoch: f64,
    monotonic: f64,
}

impl Clock for FixedClock {
    fn epoch_seconds(&self) -> f64 {
        self.epoch
    }

    fn monotonic_seconds(&self) -> f64 {
        self.monotonic
    }

    fn local_date_time(&self) -> LocalDateTime {
        self.local
    }
}

#[derive(Clone)]
struct AdvancingClock(Arc<Mutex<FixedClock>>);

impl Clock for AdvancingClock {
    fn epoch_seconds(&self) -> f64 {
        self.0
            .lock()
            .unwrap_or_else(|_| std::process::abort())
            .epoch
    }

    fn monotonic_seconds(&self) -> f64 {
        self.0
            .lock()
            .unwrap_or_else(|_| std::process::abort())
            .monotonic
    }

    fn local_date_time(&self) -> LocalDateTime {
        self.0
            .lock()
            .unwrap_or_else(|_| std::process::abort())
            .local
    }
}

impl AdvancingClock {
    fn set(&self, time: FixedClock) {
        *self.0.lock().unwrap_or_else(|_| std::process::abort()) = time;
    }
}

#[derive(Clone, Default)]
struct FakeDbus {
    shared: Arc<Mutex<FakeDbusState>>,
}

#[derive(Clone, Copy, Default, Eq, PartialEq)]
enum NextWriteBehavior {
    #[default]
    Normal,
    FailChargeCurrent,
    IgnoreChargeCurrent,
    IgnoreMinSoc,
    IgnoreDischargePower,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FakeDbusOperation {
    ReadNumber,
    ReadText,
    WriteFloat,
    WriteInteger,
}

#[derive(Clone, Debug)]
enum ScriptEffect {
    FailTransport,
    SetNumber {
        service: String,
        path: String,
        value: f64,
    },
    CrashBefore,
    CrashAfter,
}

#[derive(Clone, Debug)]
struct ScriptStep {
    operation: FakeDbusOperation,
    service: String,
    path: String,
    effect: ScriptEffect,
}

impl ScriptStep {
    fn new(operation: FakeDbusOperation, service: &str, path: &str, effect: ScriptEffect) -> Self {
        Self {
            operation,
            service: service.to_owned(),
            path: path.to_owned(),
            effect,
        }
    }

    fn matches(&self, operation: FakeDbusOperation, service: &str, path: &str) -> bool {
        self.operation == operation && self.service == service && self.path == path
    }
}

#[derive(Default)]
struct FakeDbusState {
    values: HashMap<(String, String), f64>,
    text_values: HashMap<(String, String), String>,
    read_errors: HashMap<(String, String), PortError>,
    error_on_missing: Option<DbusFailureKind>,
    writes: Vec<(String, String, f64)>,
    reads: Vec<(String, String)>,
    cycle_fault: Option<PortError>,
    fail_next_transport: bool,
    next_write_behavior: NextWriteBehavior,
    transport_operations: usize,
    script: VecDeque<ScriptStep>,
    charge_current_after_write: Option<f64>,
}

impl FakeDbus {
    fn value(&self, service: &str, path: &str, value: f64) {
        if let Ok(mut state) = self.shared.lock() {
            state
                .values
                .insert((service.to_owned(), path.to_owned()), value);
        }
    }

    fn text_value(&self, service: &str, path: &str, value: &str) {
        if let Ok(mut state) = self.shared.lock() {
            state
                .text_values
                .insert((service.to_owned(), path.to_owned()), value.to_owned());
        }
    }

    fn remove_text_value(&self, service: &str, path: &str) {
        if let Ok(mut state) = self.shared.lock() {
            state
                .text_values
                .remove(&(service.to_owned(), path.to_owned()));
        }
    }

    fn writes(&self) -> Vec<(String, String, f64)> {
        self.shared
            .lock()
            .map(|state| state.writes.clone())
            .unwrap_or_default()
    }

    fn reads(&self) -> Vec<(String, String)> {
        self.shared
            .lock()
            .map(|state| state.reads.clone())
            .unwrap_or_default()
    }

    fn number(&self, service: &str, path: &str) -> Option<f64> {
        self.shared.lock().ok().and_then(|state| {
            state
                .values
                .get(&(service.to_owned(), path.to_owned()))
                .copied()
        })
    }

    fn fail_next_transport(&self) {
        if let Ok(mut state) = self.shared.lock() {
            state.fail_next_transport = true;
        }
    }

    fn read_error(&self, service: &str, path: &str, kind: DbusFailureKind) {
        if let Ok(mut state) = self.shared.lock() {
            state.read_errors.insert(
                (service.to_owned(), path.to_owned()),
                PortError::classified(kind, "fake DBus read", "injected failure"),
            );
        }
    }

    fn error_on_missing(&self, kind: DbusFailureKind) {
        if let Ok(mut state) = self.shared.lock() {
            state.error_on_missing = Some(kind);
        }
    }

    fn transport_operations(&self) -> usize {
        self.shared
            .lock()
            .map_or(0, |state| state.transport_operations)
    }

    fn script(&self, steps: impl IntoIterator<Item = ScriptStep>) {
        if let Ok(mut state) = self.shared.lock() {
            state.script.extend(steps);
        }
    }

    fn assert_script_complete(&self) {
        let remaining = self
            .shared
            .lock()
            .map(|state| state.script.clone())
            .unwrap_or_default();
        assert!(
            remaining.is_empty(),
            "unreached DBus script steps: {remaining:?}"
        );
    }

    fn take_script_effect(
        state: &mut FakeDbusState,
        operation: FakeDbusOperation,
        service: &str,
        path: &str,
    ) -> Option<ScriptEffect> {
        if state
            .script
            .front()
            .is_some_and(|step| step.matches(operation, service, path))
        {
            return state.script.pop_front().map(|step| step.effect);
        }
        None
    }

    fn apply_pre_operation_effect(
        state: &mut FakeDbusState,
        effect: Option<&ScriptEffect>,
    ) -> Result<(), PortError> {
        match effect {
            Some(ScriptEffect::FailTransport) => {
                let error = PortError::classified(
                    DbusFailureKind::Timeout,
                    "fake DBus",
                    "scripted transport timeout",
                );
                state.cycle_fault = Some(error.clone());
                Err(error)
            }
            Some(ScriptEffect::SetNumber {
                service,
                path,
                value,
            }) => {
                state.values.insert((service.clone(), path.clone()), *value);
                Ok(())
            }
            Some(ScriptEffect::CrashBefore | ScriptEffect::CrashAfter) | None => Ok(()),
        }
    }

    fn ignore_next_charge_current_write(&self) {
        if let Ok(mut state) = self.shared.lock() {
            state.next_write_behavior = NextWriteBehavior::IgnoreChargeCurrent;
        }
    }

    fn fail_next_charge_current_write(&self) {
        if let Ok(mut state) = self.shared.lock() {
            state.next_write_behavior = NextWriteBehavior::FailChargeCurrent;
        }
    }

    fn set_charge_current_after_next_write(&self, value: f64) {
        if let Ok(mut state) = self.shared.lock() {
            state.charge_current_after_write = Some(value);
        }
    }

    fn ignore_next_min_soc_write(&self) {
        if let Ok(mut state) = self.shared.lock() {
            state.next_write_behavior = NextWriteBehavior::IgnoreMinSoc;
        }
    }

    fn ignore_next_discharge_power_write(&self) {
        if let Ok(mut state) = self.shared.lock() {
            state.next_write_behavior = NextWriteBehavior::IgnoreDischargePower;
        }
    }
}

impl DbusPort for FakeDbus {
    fn begin_cycle(&mut self) {
        if let Ok(mut state) = self.shared.lock() {
            state.cycle_fault = None;
        }
    }

    fn cycle_fault(&self) -> Option<PortError> {
        self.shared
            .lock()
            .ok()
            .and_then(|state| state.cycle_fault.clone())
    }

    fn measurement(&mut self, service: &str, path: &str) -> Result<Option<f64>, PortError> {
        self.raw_number(service, path)
            .map(|value| value.filter(|number| number.to_bits() != (-1.0_f64).to_bits()))
    }

    fn raw_number(&mut self, service: &str, path: &str) -> Result<Option<f64>, PortError> {
        let mut state = self
            .shared
            .lock()
            .map_err(|_| PortError::new("fake DBus", "poisoned lock"))?;
        if let Some(error) = &state.cycle_fault {
            return Err(error.clone());
        }
        state.transport_operations += 1;
        state.reads.push((service.to_owned(), path.to_owned()));
        let effect =
            Self::take_script_effect(&mut state, FakeDbusOperation::ReadNumber, service, path);
        if matches!(effect, Some(ScriptEffect::CrashBefore)) {
            drop(state);
            scripted_crash("scripted crash before number read");
        }
        Self::apply_pre_operation_effect(&mut state, effect.as_ref())?;
        if state.fail_next_transport {
            state.fail_next_transport = false;
            let error =
                PortError::classified(DbusFailureKind::Timeout, "fake DBus", "transport timeout");
            state.cycle_fault = Some(error.clone());
            return Err(error);
        }
        if let Some(error) = state
            .read_errors
            .get(&(service.to_owned(), path.to_owned()))
        {
            return Err(error.clone());
        }
        let result = state
            .values
            .get(&(service.to_owned(), path.to_owned()))
            .copied()
            .map_or_else(
                || {
                    state.error_on_missing.map_or_else(
                        || Ok(None),
                        |kind| {
                            Err(PortError::classified(
                                kind,
                                "fake DBus read",
                                "injected missing-value failure",
                            ))
                        },
                    )
                },
                |value| Ok(Some(value)),
            );
        drop(state);
        if matches!(effect, Some(ScriptEffect::CrashAfter)) {
            scripted_crash("scripted crash after number read");
        }
        result
    }

    fn text(&mut self, service: &str, path: &str) -> Result<Option<String>, PortError> {
        let mut state = self
            .shared
            .lock()
            .map_err(|_| PortError::new("fake DBus", "poisoned lock"))?;
        if let Some(error) = &state.cycle_fault {
            return Err(error.clone());
        }
        state.transport_operations += 1;
        state.reads.push((service.to_owned(), path.to_owned()));
        let effect =
            Self::take_script_effect(&mut state, FakeDbusOperation::ReadText, service, path);
        if matches!(effect, Some(ScriptEffect::CrashBefore)) {
            drop(state);
            scripted_crash("scripted crash before text read");
        }
        Self::apply_pre_operation_effect(&mut state, effect.as_ref())?;
        if state.fail_next_transport {
            state.fail_next_transport = false;
            let error =
                PortError::classified(DbusFailureKind::Timeout, "fake DBus", "transport timeout");
            state.cycle_fault = Some(error.clone());
            return Err(error);
        }
        if let Some(error) = state
            .read_errors
            .get(&(service.to_owned(), path.to_owned()))
        {
            return Err(error.clone());
        }
        let result = state
            .text_values
            .get(&(service.to_owned(), path.to_owned()))
            .cloned()
            .map_or_else(
                || {
                    state.error_on_missing.map_or_else(
                        || Ok(None),
                        |kind| {
                            Err(PortError::classified(
                                kind,
                                "fake DBus read",
                                "injected missing-value failure",
                            ))
                        },
                    )
                },
                |value| Ok(Some(value)),
            );
        drop(state);
        if matches!(effect, Some(ScriptEffect::CrashAfter)) {
            scripted_crash("scripted crash after text read");
        }
        result
    }

    fn write_float(&mut self, service: &str, path: &str, value: f64) -> Result<(), PortError> {
        let mut state = self
            .shared
            .lock()
            .map_err(|_| PortError::new("fake DBus", "poisoned lock"))?;
        if let Some(error) = &state.cycle_fault {
            return Err(error.clone());
        }
        state.transport_operations += 1;
        let effect =
            Self::take_script_effect(&mut state, FakeDbusOperation::WriteFloat, service, path);
        if matches!(effect, Some(ScriptEffect::CrashBefore)) {
            drop(state);
            scripted_crash("scripted crash before float write");
        }
        Self::apply_pre_operation_effect(&mut state, effect.as_ref())?;
        state
            .writes
            .push((service.to_owned(), path.to_owned(), value));
        if state.next_write_behavior == NextWriteBehavior::IgnoreMinSoc && path == MIN_SOC_PATH
            || state.next_write_behavior == NextWriteBehavior::IgnoreDischargePower
                && path == MAX_DISCHARGE_POWER_PATH
        {
            state.next_write_behavior = NextWriteBehavior::Normal;
        } else {
            state
                .values
                .insert((service.to_owned(), path.to_owned()), value);
        }
        drop(state);
        if matches!(effect, Some(ScriptEffect::CrashAfter)) {
            scripted_crash("scripted crash after float write");
        }
        Ok(())
    }

    fn write_integer(&mut self, service: &str, path: &str, value: i32) -> Result<(), PortError> {
        let mut state = self
            .shared
            .lock()
            .map_err(|_| PortError::new("fake DBus", "poisoned lock"))?;
        if let Some(error) = &state.cycle_fault {
            return Err(error.clone());
        }
        state.transport_operations += 1;
        let effect =
            Self::take_script_effect(&mut state, FakeDbusOperation::WriteInteger, service, path);
        if matches!(effect, Some(ScriptEffect::CrashBefore)) {
            drop(state);
            scripted_crash("scripted crash before integer write");
        }
        Self::apply_pre_operation_effect(&mut state, effect.as_ref())?;
        if state.next_write_behavior == NextWriteBehavior::FailChargeCurrent
            && path == MAX_CHARGE_CURRENT_PATH
        {
            state.next_write_behavior = NextWriteBehavior::Normal;
            let error = PortError::new("fake DBus", "charge-current write failed");
            state.cycle_fault = Some(error.clone());
            return Err(error);
        }
        state
            .writes
            .push((service.to_owned(), path.to_owned(), f64::from(value)));
        if state.next_write_behavior == NextWriteBehavior::IgnoreChargeCurrent
            && path == MAX_CHARGE_CURRENT_PATH
        {
            state.next_write_behavior = NextWriteBehavior::Normal;
        } else {
            state
                .values
                .insert((service.to_owned(), path.to_owned()), f64::from(value));
            if path == MAX_CHARGE_CURRENT_PATH {
                if let Some(external) = state.charge_current_after_write.take() {
                    state
                        .values
                        .insert((service.to_owned(), path.to_owned()), external);
                }
            }
        }
        drop(state);
        if matches!(effect, Some(ScriptEffect::CrashAfter)) {
            scripted_crash("scripted crash after integer write");
        }
        Ok(())
    }
}

fn scripted_crash(message: &'static str) -> ! {
    std::panic::resume_unwind(Box::new(message))
}

#[derive(Clone, Default)]
struct FakeStore {
    shared: Arc<Mutex<FakeStoreState>>,
}

#[derive(Default)]
struct FakeStoreState {
    saves: Vec<bool>,
    saved_states: Vec<ControllerState>,
    fail_forced_save: bool,
    flush_results: VecDeque<bool>,
}

impl FakeStore {
    fn fail_forced_save(&self) {
        if let Ok(mut state) = self.shared.lock() {
            state.fail_forced_save = true;
        }
    }

    fn set_flush_results(&self, results: impl IntoIterator<Item = bool>) {
        if let Ok(mut state) = self.shared.lock() {
            state.flush_results = results.into_iter().collect();
        }
    }

    fn latest_state(&self) -> ControllerState {
        self.shared
            .lock()
            .ok()
            .and_then(|state| state.saved_states.last().cloned())
            .unwrap_or_default()
    }
}

impl StatePort for FakeStore {
    fn refresh_window(
        &mut self,
        _state: &mut ControllerState,
        _now: LocalDateTime,
        _now_ts: f64,
    ) -> Result<bool, String> {
        Ok(false)
    }

    fn save(
        &mut self,
        state: &mut ControllerState,
        _now: LocalDateTime,
        now_ts: f64,
        force_sd: bool,
    ) -> Result<(), String> {
        state.ts = now_ts;
        if let Ok(mut shared) = self.shared.lock() {
            if force_sd && shared.fail_forced_save {
                return Err("forced persistence failed".to_owned());
            }
            shared.saves.push(force_sd);
            shared.saved_states.push(state.clone());
        }
        Ok(())
    }

    fn flush(&self, _timeout: Duration) -> bool {
        self.shared
            .lock()
            .ok()
            .and_then(|mut state| state.flush_results.pop_front())
            .unwrap_or(true)
    }

    fn status(&self, _now_ts: f64) -> String {
        "SD ok".to_owned()
    }

    fn sd_description(&self) -> Option<&str> {
        None
    }
}

#[derive(Clone, Default)]
struct FakeLog {
    messages: Arc<Mutex<Vec<String>>>,
}

impl LogSink for FakeLog {
    fn log(&mut self, message: &str) {
        if let Ok(mut messages) = self.messages.lock() {
            messages.push(message.to_owned());
        }
    }
}

fn config(shadow: bool) -> RuntimeConfig {
    RuntimeConfig {
        settings_service: "settings".to_owned(),
        system_service: "system".to_owned(),
        fallback_battery_service: None,
        runtime_dir: "/tmp/unused-runtime".into(),
        instance_lock_file: "/tmp/unused-runtime/service.lock".into(),
        state_device_id: "controller-scenario-device".to_owned(),
        state_file: "/tmp/unused-state".into(),
        legacy_state_file: None,
        durable_restore_file: "/tmp/unused-gui-restore-state".into(),
        log_file: "/tmp/unused-log".into(),
        decision_file: None,
        sd_path: None,
        sd_label: None,
        shadow,
        one_shot: false,
        loop_interval: Duration::from_secs(60),
        dbus_timeout: Duration::from_secs(2),
        status_log_interval: Duration::from_secs(300),
        invalid_log_interval: Duration::from_secs(300),
        battery_max_current_max_age: Duration::from_secs(300),
        nominal_inverter_power_max_age: Duration::from_secs(86_400),
        configured_nominal_inverter_power_w: None,
        sd_save_interval: Duration::from_secs(21_600),
        sd_lookup_interval: Duration::from_secs(3_600),
        sd_backoff_max: Duration::from_secs(300),
        policy: venus_ess_winter_soc_service::config::PolicyConfig::default(),
    }
}

const fn clock(month: u8, day: u8, hour: u8, epoch: f64) -> FixedClock {
    clock_times(month, day, hour, epoch, epoch)
}

const fn clock_times(month: u8, day: u8, hour: u8, epoch: f64, monotonic: f64) -> FixedClock {
    FixedClock {
        local: LocalDateTime {
            year: 2026,
            month,
            day,
            hour,
            minute: 0,
            second: 0,
        },
        epoch,
        monotonic,
    }
}

fn base_bus() -> FakeDbus {
    let bus = FakeDbus::default();
    bus.text_value(
        "system",
        ACTIVE_BMS_SERVICE_PATH,
        "com.victronenergy.battery.preferred",
    );
    bus.text_value("system", VEBUS_SERVICE_PATH, VEBUS_SERVICE);
    bus.value("system", BATTERY_SOC_PATH, 50.0);
    bus.value("system", BATTERY_VOLTAGE_PATH, 52.0);
    bus.value("system", BATTERY_POWER_PATH, 0.0);
    bus.value("settings", MIN_SOC_PATH, 10.0);
    bus.value("settings", MAX_CHARGE_CURRENT_PATH, -1.0);
    bus.value("settings", MAX_DISCHARGE_POWER_PATH, -1.0);
    bus
}

fn configure_reserve_charge(bus: &FakeDbus) {
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value(ACTIVE_BMS_SERVICE, BMS_MAX_CHARGE_CURRENT_PATH, 200.0);
    bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 35.0);
    for phase in ["L1", "L2", "L3"] {
        bus.value(
            "system",
            &format!("/Ac/ConsumptionOnInput/{phase}/Power"),
            500.0,
        );
    }
}

#[test]
fn non_transport_dbus_read_errors_are_typed_without_treating_absence_as_failure() {
    let bus = base_bus();
    bus.read_error(
        "com.victronenergy.battery.preferred",
        BMS_MAX_CHARGE_CURRENT_PATH,
        DbusFailureKind::TypeMismatch,
    );
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock(11, 10, 12, 2_000.0),
        FakeLog::default(),
        config(false),
        ControllerState {
            last_sample_date: "2026-11-10".to_owned(),
            ..ControllerState::default()
        },
    );

    let decision = controller.run_once();

    assert_eq!(decision.dbus_read_issue_overflow, 0);
    assert_eq!(decision.dbus_read_issues.len(), 1);
    let issue = &decision.dbus_read_issues[0];
    assert_eq!(issue.kind, DbusFailureKind::TypeMismatch);
    assert_eq!(issue.operation, "fake DBus read");
    assert_eq!(issue.service, "com.victronenergy.battery.preferred");
    assert_eq!(issue.path, BMS_MAX_CHARGE_CURRENT_PATH);
    assert_eq!(issue.occurrences, 1);
}

#[test]
fn cycle_wide_transport_failures_are_deduplicated() {
    let bus = base_bus();
    bus.fail_next_transport();
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock(11, 10, 12, 2_000.0),
        FakeLog::default(),
        config(false),
        ControllerState {
            last_sample_date: "2026-11-10".to_owned(),
            ..ControllerState::default()
        },
    );

    let decision = controller.run_once();

    assert_eq!(decision.outcome, CycleOutcome::DbusFailure);
    assert_eq!(decision.dbus_read_issues.len(), 1);
    assert_eq!(decision.dbus_read_issues[0].kind, DbusFailureKind::Timeout);
    assert!(decision.dbus_read_issues[0].occurrences > 1);
}

#[test]
fn dbus_read_diagnostics_are_hard_bounded() {
    let bus = base_bus();
    bus.error_on_missing(DbusFailureKind::PathUnavailable);
    bus.value("system", BATTERY_SOC_PATH, 20.0);
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock(11, 10, 12, 2_000.0),
        FakeLog::default(),
        config(false),
        ControllerState {
            last_sample_date: "2026-11-10".to_owned(),
            ..ControllerState::default()
        },
    );

    let decision = controller.run_once();

    assert_eq!(decision.dbus_read_issues.len(), 16);
    assert!(decision.dbus_read_issue_overflow > 0);
}

#[test]
fn first_ram_day_arms_the_routine_ceiling_without_writing_below_it() {
    let bus = base_bus();
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(9, 1, 12, 2_000.0),
        FakeLog::default(),
        config(false),
        ControllerState::default(),
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_charge_ceiling_current_a, None);
    assert!(!decision.charge_current_ceiling_active);
    assert!(!decision.charge_current_ceiling_owned);
    assert_eq!(decision.charge_ceiling_soc, Some(90.0));
    assert_eq!(decision.full_charge_due, Some(false));
    assert_eq!(decision.full_charge_age_days, Some(0));
    assert_eq!(decision.outcome, CycleOutcome::NoChange);
    assert!(bus.writes().is_empty());
    assert_eq!(
        controller.state.charge_ceiling.reference_day,
        calendar_day_number(clock(9, 1, 12, 0.0).local)
    );
}

#[test]
fn full_ceiling_is_enabled_on_the_fourth_calendar_date() {
    let bus = base_bus();
    let state = ControllerState {
        charge_ceiling: venus_ess_winter_soc_service::domain::ChargeCeilingState {
            reference_day: calendar_day_number(clock(9, 1, 12, 0.0).local),
            observed_day: calendar_day_number(clock(9, 3, 12, 0.0).local),
            near_full_latched: false,
            ..Default::default()
        },
        last_sample_date: "2026-09-04".to_owned(),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock(9, 4, 12, 2_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_charge_ceiling_current_a, None);
    assert_eq!(decision.charge_ceiling_soc, Some(100.0));
    assert_eq!(decision.full_charge_due, Some(true));
    assert_eq!(decision.full_charge_age_days, Some(3));
}

#[test]
fn a_single_near_full_sample_does_not_end_the_full_charge_permission() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 99.0);
    let state = ControllerState {
        charge_ceiling: venus_ess_winter_soc_service::domain::ChargeCeilingState {
            reference_day: calendar_day_number(clock(9, 1, 12, 0.0).local),
            observed_day: calendar_day_number(clock(9, 3, 12, 0.0).local),
            near_full_latched: false,
            ..Default::default()
        },
        last_sample_date: "2026-09-04".to_owned(),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock(9, 4, 12, 2_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_charge_ceiling_current_a, None);
    assert!(!decision.charge_current_ceiling_active);
    assert!(!decision.charge_current_ceiling_owned);
    assert_eq!(decision.full_charge_due, Some(true));
    assert_eq!(decision.full_charge_age_days, Some(3));
}

#[test]
fn due_winter_balancing_enables_the_full_ceiling_in_the_start_cycle() {
    let bus = base_bus();
    let state = ControllerState {
        boot_ts: 1_000.0,
        last_sample_date: "2026-01-01".to_owned(),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock(1, 1, 12, 100_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert!(controller.state.balancing_active);
    assert_eq!(decision.mode, "Winter Balancing");
    assert_eq!(decision.requested_charge_ceiling_current_a, None);
    assert_eq!(decision.charge_ceiling_soc, Some(100.0));
    assert_eq!(decision.full_charge_due, Some(false));
}

#[test]
fn confirmed_full_charge_sets_zero_current_only_on_the_following_utc_date() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 99.0);
    bus.value("settings", MAX_CHARGE_CURRENT_PATH, 17.0);
    let time = AdvancingClock(Arc::new(Mutex::new(clock(9, 4, 20, 2_000.0))));
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        time.clone(),
        FakeLog::default(),
        config(false),
        ControllerState::default(),
    );

    for minute in 0..=120 {
        time.set(clock(9, 4, 22, 2_000.0 + f64::from(minute * 60)));
        if minute == 60 {
            bus.value("system", BATTERY_SOC_PATH, 100.0);
        }
        let decision = controller.run_once();
        assert!(!decision.charge_current_ceiling_active);
        assert_eq!(decision.charge_ceiling_soc, Some(100.0));
        assert_eq!(
            controller
                .state
                .charge_ceiling
                .full_charge_completed_day
                .is_some(),
            minute == 120
        );
    }
    time.set(clock(9, 4, 23, 12_800.0));
    assert!(!controller.run_once().charge_current_ceiling_active);
    assert!(bus.writes().is_empty());
    time.set(clock(9, 5, 0, 16_400.0));
    let decision = controller.run_once();
    assert!(decision.charge_current_ceiling_active);
    assert_eq!(decision.requested_charge_ceiling_current_a, Some(0.0));
    assert_eq!(
        controller.state.charge_current_control.external_baseline_a,
        Some(17.0)
    );
}

#[test]
fn missing_soc_interrupts_full_confirmation_in_the_controller() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 99.0);
    let time = AdvancingClock(Arc::new(Mutex::new(clock(9, 4, 20, 2_000.0))));
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        time.clone(),
        FakeLog::default(),
        config(false),
        ControllerState::default(),
    );
    let _ = controller.run_once();
    assert!(
        controller
            .state
            .charge_ceiling
            .near_full_since_monotonic
            .is_some()
    );
    bus.value("system", BATTERY_SOC_PATH, f64::NAN);
    time.set(clock(9, 4, 20, 2_060.0));
    assert_eq!(controller.run_once().outcome, CycleOutcome::MissingSoc);
    assert_eq!(
        controller.state.charge_ceiling.near_full_since_monotonic,
        None
    );
    bus.value("system", BATTERY_SOC_PATH, 99.0);
    time.set(clock(9, 4, 20, 2_080.0));
    let _ = controller.run_once();
    assert_eq!(
        controller.state.charge_ceiling.near_full_since_monotonic,
        Some(2_080.0)
    );
}

#[test]
fn active_winter_balancing_does_not_impose_zero_at_100_percent_after_confirmation_day() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 100.0);
    let mut state = ControllerState {
        balancing_active: true,
        balancing_start_ts: 1_000.0,
        last_balance_attempt_ts: 1_000.0,
        last_sample_date: "2026-01-02".to_owned(),
        ..ControllerState::default()
    };
    state.charge_ceiling.reference_day = calendar_day_number(clock(1, 1, 0, 0.0).local);
    state.charge_ceiling.observed_day = state.charge_ceiling.reference_day;
    state.charge_ceiling.full_charge_completed_day = state.charge_ceiling.reference_day;
    state.charge_ceiling.near_full_latched = true;
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(1, 2, 0, 2_000.0),
        FakeLog::default(),
        config(false),
        state,
    );
    let decision = controller.run_once();
    assert!(controller.state.balancing_active);
    assert!(!decision.charge_current_ceiling_active);
    assert_eq!(decision.requested_charge_ceiling_current_a, None);
    assert!(
        bus.writes()
            .iter()
            .all(|(_, path, value)| path != MAX_CHARGE_CURRENT_PATH || *value != 0.0)
    );
}

#[test]
fn active_balancing_keeps_the_full_ceiling_while_99_percent_is_unconfirmed() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 99.0);
    let state = ControllerState {
        balancing_active: true,
        balancing_start_ts: 1_000.0,
        last_balance_attempt_ts: 1_000.0,
        last_sample_date: "2026-01-01".to_owned(),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock(1, 1, 12, 2_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert!(controller.state.balancing_active);
    assert_eq!(decision.mode, "Winter Balancing");
    assert_eq!(decision.requested_charge_ceiling_current_a, None);
    assert_eq!(decision.charge_ceiling_soc, Some(100.0));
    assert_eq!(decision.full_charge_due, Some(false));
    assert_eq!(decision.full_charge_age_days, Some(0));
}

#[test]
fn reboot_during_balancing_discards_old_monotonic_progress_and_restarts_after_boot_grace() {
    let bus = base_bus();
    let mut state = ControllerState {
        balancing_active: true,
        balancing_start_ts: 50_000.0,
        last_balance_attempt_ts: 50_000.0,
        balance_full_seconds: 7_200.0,
        last_sample_date: "2026-01-01".to_owned(),
        ..ControllerState::default()
    };

    state.reset_runtime_for_boot(10.0);

    assert!(!state.balancing_active);
    assert_eq!(state.balancing_start_ts.to_bits(), 0.0_f64.to_bits());
    assert_eq!(
        state.balancing_high_soc_start_ts.to_bits(),
        0.0_f64.to_bits()
    );
    assert_eq!(state.balance_full_seconds.to_bits(), 0.0_f64.to_bits());

    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock_times(1, 1, 12, 100_000.0, 100_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert!(controller.state.balancing_active);
    assert_eq!(decision.mode, "Winter Balancing");
    assert_eq!(decision.charge_ceiling_soc, Some(100.0));
    assert_eq!(
        controller.state.balance_full_seconds.to_bits(),
        0.0_f64.to_bits()
    );
}

#[test]
fn completed_balancing_restores_the_routine_ceiling_in_the_same_cycle() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 99.0);
    let state = ControllerState {
        charge_ceiling: venus_ess_winter_soc_service::domain::ChargeCeilingState {
            reference_day: calendar_day_number(clock(1, 1, 0, 0.0).local).map(|day| day - 1),
            observed_day: calendar_day_number(clock(1, 1, 0, 0.0).local),
            full_charge_completed_day: calendar_day_number(clock(1, 1, 0, 0.0).local)
                .map(|day| day - 1),
            near_full_latched: true,
            ..Default::default()
        },
        balancing_active: true,
        balancing_start_ts: 1_000.0,
        balancing_high_soc_start_ts: 1_000.0,
        balance_full_seconds: 4.0_f64.mul_add(3_600.0, -60.0),
        last_loop_ts: 1_940.0,
        last_sample_date: "2026-01-01".to_owned(),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock(1, 1, 12, 2_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert!(!controller.state.balancing_active);
    assert_eq!(decision.mode, "Winter");
    assert_eq!(decision.requested_charge_ceiling_current_a, Some(0.0));
    assert_eq!(decision.charge_ceiling_soc, Some(90.0));
    assert_eq!(
        controller.state.last_balance_ts.to_bits(),
        2_000.0_f64.to_bits()
    );
}

#[test]
fn timed_out_high_soc_hold_restores_the_routine_ceiling_in_the_same_cycle() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 95.0);
    let state = ControllerState {
        balancing_active: true,
        balancing_start_ts: 100.0,
        balancing_high_soc_start_ts: 100.0,
        last_balance_attempt_ts: 100.0,
        last_sample_date: "2026-01-01".to_owned(),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock(1, 1, 12, 50_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert!(!controller.state.balancing_active);
    assert_eq!(decision.mode, "Winter");
    assert_eq!(decision.requested_charge_ceiling_current_a, Some(0.0));
    assert_eq!(decision.charge_ceiling_soc, Some(90.0));
}

#[test]
fn routine_ceiling_restores_the_exact_unlimited_current_below_hysteresis() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 90.0);
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(9, 1, 12, 2_000.0),
        FakeLog::default(),
        config(false),
        ControllerState::default(),
    );

    let restricted = controller.run_once();
    assert_eq!(restricted.requested_charge_ceiling_current_a, Some(0.0));
    assert!(controller.state.charge_current_control.owned);
    assert!(
        controller
            .state
            .charge_current_control
            .routine_ceiling_requested
    );

    bus.value("system", BATTERY_SOC_PATH, 88.9);
    let restored = controller.run_once();
    assert_eq!(restored.requested_charge_ceiling_current_a, Some(-1.0));
    assert!(!controller.state.charge_current_control.owned);
    assert_eq!(
        bus.writes()
            .into_iter()
            .filter(|(_, path, _)| path == MAX_CHARGE_CURRENT_PATH)
            .collect::<Vec<_>>(),
        vec![
            (
                "settings".to_owned(),
                MAX_CHARGE_CURRENT_PATH.to_owned(),
                0.0
            ),
            (
                "settings".to_owned(),
                MAX_CHARGE_CURRENT_PATH.to_owned(),
                -1.0
            ),
        ],
    );
}

#[test]
fn routine_ceiling_restores_a_pre_existing_positive_current_exactly() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 95.0);
    bus.value("settings", MAX_CHARGE_CURRENT_PATH, 17.0);
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(9, 1, 12, 2_000.0),
        FakeLog::default(),
        config(false),
        ControllerState::default(),
    );

    controller.run_once();
    assert_eq!(
        controller.state.charge_current_control.external_baseline_a,
        Some(17.0)
    );
    bus.value("system", BATTERY_SOC_PATH, 88.9);
    let restored = controller.run_once();

    assert_eq!(restored.requested_charge_ceiling_current_a, Some(17.0));
    assert_eq!(
        bus.writes()
            .iter()
            .rev()
            .find(|(_, path, _)| path == MAX_CHARGE_CURRENT_PATH)
            .map(|write| write.2),
        Some(17.0)
    );
}

#[test]
fn a_due_full_charge_releases_an_owned_routine_current_ceiling() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 95.0);
    bus.value("settings", MAX_CHARGE_CURRENT_PATH, 0.0);
    let state = ControllerState {
        charge_ceiling: venus_ess_winter_soc_service::domain::ChargeCeilingState {
            reference_day: calendar_day_number(clock(9, 1, 12, 0.0).local),
            observed_day: calendar_day_number(clock(9, 3, 12, 0.0).local),
            near_full_latched: false,
            ..Default::default()
        },
        charge_current_control: ChargeCurrentControlState {
            external_baseline_a: Some(-1.0),
            routine_ceiling_requested: true,
            owned: true,
            last_effectively_written_a: Some(0.0),
            write_generation: 1,
            pending_write: None,
            ..ChargeCurrentControlState::default()
        },
        last_sample_date: "2026-09-04".to_owned(),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock(9, 4, 12, 2_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.charge_ceiling_soc, Some(100.0));
    assert_eq!(decision.requested_charge_ceiling_current_a, Some(-1.0));
    assert!(
        !controller
            .state
            .charge_current_control
            .routine_ceiling_requested
    );
}

#[test]
fn stricter_external_current_and_persistent_gui_power_limit_are_never_raised() {
    const GUI_MAX_CHARGE_POWER_PATH: &str = "/Settings/CGwacs/MaxChargePower";
    const DEPRECATED_MAX_CHARGE_PERCENTAGE_PATH: &str = "/Settings/CGwacs/MaxChargePercentage";
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 95.0);
    bus.value("settings", GUI_MAX_CHARGE_POWER_PATH, 800.0);
    bus.value("settings", DEPRECATED_MAX_CHARGE_PERCENTAGE_PATH, 80.0);
    bus.value("settings", MAX_CHARGE_CURRENT_PATH, 0.0);
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(9, 1, 12, 2_000.0),
        FakeLog::default(),
        config(false),
        ControllerState::default(),
    );

    let decision = controller.run_once();

    assert!(decision.charge_current_ceiling_active);
    assert!(!decision.charge_current_ceiling_owned);
    assert!(!decision.charge_current_ceiling_pending_unenforced);
    assert!(bus.writes().is_empty());
    assert!(
        !bus.writes()
            .iter()
            .any(|(_, path, _)| path == GUI_MAX_CHARGE_POWER_PATH)
    );
    assert!(
        !bus.writes()
            .iter()
            .any(|(_, path, _)| path == DEPRECATED_MAX_CHARGE_PERCENTAGE_PATH)
    );
}

#[test]
fn external_change_releases_ownership_until_the_ceiling_episode_ends() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 95.0);
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(9, 1, 12, 2_000.0),
        FakeLog::default(),
        config(false),
        ControllerState::default(),
    );
    controller.run_once();
    assert_eq!(bus.writes().len(), 1);

    bus.value("settings", MAX_CHARGE_CURRENT_PATH, 5.0);
    let external = controller.run_once();
    assert!(!external.charge_current_ceiling_owned);
    assert!(external.charge_current_ceiling_pending_unenforced);
    assert_eq!(
        external.charge_current_ceiling_unenforced_reason.as_deref(),
        Some("external_charge_current_control")
    );
    controller.run_once();
    assert_eq!(bus.writes().len(), 1);

    bus.value("system", BATTERY_SOC_PATH, 88.9);
    controller.run_once();
    assert!(
        !controller
            .state
            .charge_current_control
            .routine_external_control_latched
    );
    bus.value("system", BATTERY_SOC_PATH, 90.0);
    controller.run_once();
    assert_eq!(bus.writes().len(), 2);
}

fn pending_charge_current_restriction_state() -> ControllerState {
    ControllerState {
        charge_current_control: ChargeCurrentControlState {
            external_baseline_a: Some(-1.0),
            routine_ceiling_requested: true,
            owned: true,
            last_effectively_written_a: None,
            write_generation: 1,
            pending_write: Some(PendingChargeCurrentWrite {
                generation: 1,
                kind: ChargeCurrentWriteKind::Restrict,
                expected_before_a: -1.0,
                intended_a: 0.0,
            }),
            ..ChargeCurrentControlState::default()
        },
        ..ControllerState::default()
    }
}

#[test]
fn restart_retries_or_commits_the_charge_current_write_from_readback() {
    let before_write_bus = base_bus();
    before_write_bus.value("system", BATTERY_SOC_PATH, 95.0);
    let mut before_write = Controller::new(
        before_write_bus.clone(),
        FakeStore::default(),
        clock(9, 1, 12, 2_000.0),
        FakeLog::default(),
        config(false),
        pending_charge_current_restriction_state(),
    );
    before_write.run_once();
    assert_eq!(before_write_bus.writes().len(), 1);
    assert_eq!(
        before_write
            .state
            .charge_current_control
            .last_effectively_written_a,
        Some(0.0)
    );
    assert_eq!(
        before_write.state.charge_current_control.pending_write,
        None
    );

    let after_write_bus = base_bus();
    after_write_bus.value("system", BATTERY_SOC_PATH, 95.0);
    after_write_bus.value("settings", MAX_CHARGE_CURRENT_PATH, 0.0);
    let mut after_write = Controller::new(
        after_write_bus.clone(),
        FakeStore::default(),
        clock(9, 1, 12, 2_000.0),
        FakeLog::default(),
        config(false),
        pending_charge_current_restriction_state(),
    );
    after_write.run_once();
    assert!(after_write_bus.writes().is_empty());
    assert_eq!(
        after_write
            .state
            .charge_current_control
            .last_effectively_written_a,
        Some(0.0)
    );
    assert_eq!(after_write.state.charge_current_control.pending_write, None);
}

#[test]
fn durable_intent_failure_prevents_the_charge_current_write() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 95.0);
    let store = FakeStore::default();
    store.fail_forced_save();
    let mut controller = Controller::new(
        bus.clone(),
        store,
        clock(9, 1, 12, 2_000.0),
        FakeLog::default(),
        config(false),
        ControllerState::default(),
    );

    let decision = controller.run_once();

    assert!(bus.writes().is_empty());
    assert!(!controller.state.charge_current_control.owned);
    assert_eq!(
        decision.charge_current_ceiling_unenforced_reason.as_deref(),
        Some("durable_intent_unavailable")
    );
}

#[test]
fn durable_intent_timeout_keeps_charge_wal_and_retries_only_after_confirmation() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 95.0);
    let store = FakeStore::default();
    store.set_flush_results([false, true]);
    let mut controller = Controller::new(
        bus.clone(),
        store,
        clock(9, 1, 12, 2_000.0),
        FakeLog::default(),
        config(false),
        ControllerState::default(),
    );

    let timed_out = controller.run_once();

    assert!(bus.writes().is_empty());
    assert!(controller.state.charge_current_control.owned);
    assert!(
        controller
            .state
            .charge_current_control
            .pending_write
            .is_some()
    );
    assert_eq!(
        timed_out
            .charge_current_ceiling_unenforced_reason
            .as_deref(),
        Some("durable_intent_unavailable")
    );

    controller.run_once();

    assert_eq!(bus.number("settings", MAX_CHARGE_CURRENT_PATH), Some(0.0));
    assert!(
        controller
            .state
            .charge_current_control
            .pending_write
            .is_none()
    );
    assert_eq!(bus.writes().len(), 1);
}

#[test]
fn failed_readback_retains_the_write_ahead_record() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 95.0);
    bus.ignore_next_charge_current_write();
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(9, 1, 12, 2_000.0),
        FakeLog::default(),
        config(false),
        ControllerState::default(),
    );

    let decision = controller.run_once();

    assert_eq!(bus.writes().len(), 1);
    assert!(
        controller
            .state
            .charge_current_control
            .pending_write
            .is_some()
    );
    assert_eq!(
        decision.charge_current_ceiling_unenforced_reason.as_deref(),
        Some("dbus_readback_failed")
    );
}

#[test]
fn charge_ceiling_crash_matrix_recovers_before_and_after_the_dbus_write() {
    for effect in [ScriptEffect::CrashBefore, ScriptEffect::CrashAfter] {
        let bus = base_bus();
        bus.value("system", BATTERY_SOC_PATH, 95.0);
        bus.script([ScriptStep::new(
            FakeDbusOperation::WriteInteger,
            "settings",
            MAX_CHARGE_CURRENT_PATH,
            effect.clone(),
        )]);
        let store = FakeStore::default();
        let mut controller = Controller::new(
            bus.clone(),
            store.clone(),
            clock(9, 1, 12, 2_000.0),
            FakeLog::default(),
            config(false),
            ControllerState::default(),
        );

        assert!(catch_unwind(AssertUnwindSafe(|| controller.run_once())).is_err());
        bus.assert_script_complete();

        let durable_state = store.latest_state();
        assert!(durable_state.charge_current_control.pending_write.is_some());
        let mut restarted = Controller::new(
            bus.clone(),
            store,
            clock(9, 1, 12, 2_001.0),
            FakeLog::default(),
            config(false),
            durable_state,
        );

        restarted.run_once();

        assert_eq!(bus.number("settings", MAX_CHARGE_CURRENT_PATH), Some(0.0));
        assert!(restarted.state.charge_current_control.owned);
        assert_eq!(restarted.state.charge_current_control.pending_write, None);
    }
}

#[test]
fn routine_ceiling_reuses_the_reserve_owner_without_losing_the_external_baseline() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 95.0);
    bus.value("settings", MAX_CHARGE_CURRENT_PATH, 14.0);
    let state = ControllerState {
        charge_current_control: ChargeCurrentControlState {
            external_baseline_a: Some(-1.0),
            reserve_constraint_a: Some(14.0),
            owned: true,
            last_effectively_written_a: Some(14.0),
            write_generation: 1,
            ..ChargeCurrentControlState::default()
        },
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(9, 1, 12, 2_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_charge_ceiling_current_a, Some(0.0));
    assert_eq!(bus.number("settings", MAX_CHARGE_CURRENT_PATH), Some(0.0));
    assert_eq!(
        controller.state.charge_current_control.external_baseline_a,
        Some(-1.0)
    );
    assert_eq!(
        controller
            .state
            .charge_current_control
            .last_effectively_written_a,
        Some(0.0)
    );
    assert!(controller.state.charge_current_control.owned);

    controller.shutdown();

    assert_eq!(bus.number("settings", MAX_CHARGE_CURRENT_PATH), Some(-1.0));
    assert!(!controller.state.charge_current_control.owned);
}

#[test]
fn routine_takeover_of_a_reserve_limit_is_crash_safe_with_one_baseline() {
    for effect in [ScriptEffect::CrashBefore, ScriptEffect::CrashAfter] {
        let bus = base_bus();
        bus.value("system", BATTERY_SOC_PATH, 95.0);
        bus.value("settings", MAX_CHARGE_CURRENT_PATH, 14.0);
        bus.script([ScriptStep::new(
            FakeDbusOperation::WriteInteger,
            "settings",
            MAX_CHARGE_CURRENT_PATH,
            effect.clone(),
        )]);
        let store = FakeStore::default();
        let state = ControllerState {
            charge_current_control: ChargeCurrentControlState {
                external_baseline_a: Some(-1.0),
                reserve_constraint_a: Some(14.0),
                owned: true,
                last_effectively_written_a: Some(14.0),
                write_generation: 1,
                ..ChargeCurrentControlState::default()
            },
            ..ControllerState::default()
        };
        let mut interrupted = Controller::new(
            bus.clone(),
            store.clone(),
            clock(9, 1, 12, 2_000.0),
            FakeLog::default(),
            config(false),
            state,
        );

        assert!(catch_unwind(AssertUnwindSafe(|| interrupted.run_once())).is_err());
        let durable = store.latest_state();
        let pending = durable
            .charge_current_control
            .pending_write
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(pending.expected_before_a.to_bits(), 14.0_f64.to_bits());
        assert_eq!(pending.intended_a.to_bits(), 0.0_f64.to_bits());
        assert_eq!(
            durable.charge_current_control.external_baseline_a,
            Some(-1.0)
        );

        let mut restarted = Controller::new(
            bus.clone(),
            store,
            clock(9, 1, 12, 2_001.0),
            FakeLog::default(),
            config(false),
            durable,
        );
        restarted.run_once();

        assert_eq!(bus.number("settings", MAX_CHARGE_CURRENT_PATH), Some(0.0));
        assert_eq!(
            restarted.state.charge_current_control.external_baseline_a,
            Some(-1.0)
        );
        assert!(
            restarted
                .state
                .charge_current_control
                .pending_write
                .is_none()
        );
        restarted.shutdown();
        assert_eq!(bus.number("settings", MAX_CHARGE_CURRENT_PATH), Some(-1.0));
    }
}

#[test]
fn reserve_charge_crash_matrix_converges_before_and_after_the_min_soc_write() {
    for effect in [ScriptEffect::CrashBefore, ScriptEffect::CrashAfter] {
        let bus = base_bus();
        bus.value("system", BATTERY_SOC_PATH, 30.0);
        bus.value(ACTIVE_BMS_SERVICE, BMS_MAX_CHARGE_CURRENT_PATH, 200.0);
        bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 35.0);
        for phase in ["L1", "L2", "L3"] {
            bus.value(
                "system",
                &format!("/Ac/ConsumptionOnInput/{phase}/Power"),
                500.0,
            );
        }
        bus.script([ScriptStep::new(
            FakeDbusOperation::WriteFloat,
            "settings",
            MIN_SOC_PATH,
            effect.clone(),
        )]);
        let store = FakeStore::default();
        let mut controller = Controller::new(
            bus.clone(),
            store.clone(),
            clock(1, 1, 1, 100_000.0),
            FakeLog::default(),
            config(false),
            ControllerState {
                boot_ts: 100_000.0,
                min_soc_last_seen: Some(10.0),
                ..ControllerState::default()
            },
        );

        assert!(catch_unwind(AssertUnwindSafe(|| controller.run_once())).is_err());
        bus.assert_script_complete();

        let mut restarted = Controller::new(
            bus.clone(),
            store.clone(),
            clock(1, 1, 1, 100_001.0),
            FakeLog::default(),
            config(false),
            store.latest_state(),
        );
        restarted.run_once();

        assert_eq!(bus.number("settings", MAX_CHARGE_CURRENT_PATH), Some(14.0));
        assert_eq!(bus.number("settings", MIN_SOC_PATH), Some(45.0));
        assert!(restarted.state.charging_mode_active);
        assert!(!restarted.state.charging_paused);
        assert!(restarted.state.minimum_soc_control.owned);
        assert_eq!(
            restarted.state.minimum_soc_control.external_baseline,
            Some(10.0)
        );
        assert_eq!(restarted.state.minimum_soc_control.last_set, Some(45.0));
        assert!(restarted.state.minimum_soc_control.pending_write.is_none());
    }
}

#[test]
fn reserve_current_crash_matrix_converges_before_and_after_the_dbus_write() {
    for effect in [ScriptEffect::CrashBefore, ScriptEffect::CrashAfter] {
        let bus = base_bus();
        bus.value("system", BATTERY_SOC_PATH, 30.0);
        bus.value(ACTIVE_BMS_SERVICE, BMS_MAX_CHARGE_CURRENT_PATH, 200.0);
        bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 35.0);
        for phase in ["L1", "L2", "L3"] {
            bus.value(
                "system",
                &format!("/Ac/ConsumptionOnInput/{phase}/Power"),
                500.0,
            );
        }
        bus.script([ScriptStep::new(
            FakeDbusOperation::WriteInteger,
            "settings",
            MAX_CHARGE_CURRENT_PATH,
            effect.clone(),
        )]);
        let store = FakeStore::default();
        let mut controller = Controller::new(
            bus.clone(),
            store.clone(),
            clock(1, 1, 1, 100_000.0),
            FakeLog::default(),
            config(false),
            ControllerState {
                boot_ts: 100_000.0,
                min_soc_last_seen: Some(10.0),
                ..ControllerState::default()
            },
        );

        assert!(catch_unwind(AssertUnwindSafe(|| controller.run_once())).is_err());
        bus.assert_script_complete();

        let interrupted = store.latest_state();
        let pending = interrupted
            .charge_current_control
            .pending_write
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(pending.kind, ChargeCurrentWriteKind::Restrict);
        assert_eq!(pending.expected_before_a.to_bits(), (-1.0_f64).to_bits());
        assert_eq!(pending.intended_a.to_bits(), 14.0_f64.to_bits());
        assert!(interrupted.charge_current_control.owned);

        let mut restarted = Controller::new(
            bus.clone(),
            store.clone(),
            clock(1, 1, 1, 100_001.0),
            FakeLog::default(),
            config(false),
            store.latest_state(),
        );
        restarted.run_once();

        assert_eq!(bus.number("settings", MAX_CHARGE_CURRENT_PATH), Some(14.0));
        assert_eq!(bus.number("settings", MIN_SOC_PATH), Some(45.0));
        assert!(restarted.state.charge_current_control.owned);
        assert_eq!(
            restarted
                .state
                .charge_current_control
                .last_effectively_written_a,
            Some(14.0)
        );
        assert!(
            restarted
                .state
                .charge_current_control
                .pending_write
                .is_none()
        );
        assert!(restarted.state.charging_mode_active);
        assert!(!restarted.state.charging_paused);
    }
}

#[test]
fn external_gui_limit_is_preserved_when_reserve_charging_ends() {
    let bus = base_bus();
    configure_reserve_charge(&bus);
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        config(false),
        ControllerState {
            boot_ts: 100_000.0,
            min_soc_last_seen: Some(10.0),
            ..ControllerState::default()
        },
    );

    controller.run_once();
    assert_eq!(bus.number("settings", MAX_CHARGE_CURRENT_PATH), Some(14.0));
    assert!(controller.state.charge_current_control.owned);

    bus.value("settings", MAX_CHARGE_CURRENT_PATH, 5.0);
    bus.value("system", BATTERY_SOC_PATH, 45.0);
    let writes_before_restore = bus.writes().len();
    controller.run_once();

    assert_eq!(bus.number("settings", MAX_CHARGE_CURRENT_PATH), Some(5.0));
    assert_eq!(bus.writes().len(), writes_before_restore);
    assert!(!controller.state.charge_current_control.owned);
    assert!(
        controller
            .state
            .charge_current_control
            .pending_write
            .is_none()
    );
}

#[test]
fn stricter_external_value_during_readback_is_never_claimed_or_restored() {
    let bus = base_bus();
    configure_reserve_charge(&bus);
    bus.set_charge_current_after_next_write(5.0);
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        config(false),
        ControllerState {
            boot_ts: 100_000.0,
            min_soc_last_seen: Some(10.0),
            ..ControllerState::default()
        },
    );

    let decision = controller.run_once();

    assert_eq!(bus.number("settings", MAX_CHARGE_CURRENT_PATH), Some(5.0));
    assert_eq!(bus.number("settings", MIN_SOC_PATH), Some(10.0));
    assert_eq!(
        decision.reserve_charging_pause_reason,
        Some(ReserveChargingPauseReason::ChargeCurrentOwnershipUnavailable)
    );
    assert!(!controller.state.charge_current_control.owned);
    assert!(
        controller
            .state
            .charge_current_control
            .pending_write
            .is_none()
    );

    bus.value("system", BATTERY_SOC_PATH, 45.0);
    controller.run_once();
    assert_eq!(bus.number("settings", MAX_CHARGE_CURRENT_PATH), Some(5.0));
}

#[test]
fn reserve_restore_crash_matrix_converges_without_losing_the_original_value() {
    for effect in [ScriptEffect::CrashBefore, ScriptEffect::CrashAfter] {
        let bus = base_bus();
        bus.value("system", BATTERY_SOC_PATH, 45.0);
        bus.value("settings", MIN_SOC_PATH, 45.0);
        bus.value("settings", MAX_CHARGE_CURRENT_PATH, 14.0);
        bus.script([ScriptStep::new(
            FakeDbusOperation::WriteInteger,
            "settings",
            MAX_CHARGE_CURRENT_PATH,
            effect.clone(),
        )]);
        let store = FakeStore::default();
        let mut controller = Controller::new(
            bus.clone(),
            store.clone(),
            clock(1, 1, 1, 100_000.0),
            FakeLog::default(),
            config(false),
            ControllerState {
                boot_ts: 100_000.0,
                min_soc_last_seen: Some(45.0),
                charge_current_control: ChargeCurrentControlState {
                    external_baseline_a: Some(-1.0),
                    reserve_constraint_a: Some(14.0),
                    owned: true,
                    last_effectively_written_a: Some(14.0),
                    ..ChargeCurrentControlState::default()
                },
                ..ControllerState::default()
            },
        );

        assert!(catch_unwind(AssertUnwindSafe(|| controller.run_once())).is_err());
        bus.assert_script_complete();
        let interrupted = store.latest_state();
        let pending = interrupted
            .charge_current_control
            .pending_write
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(pending.kind, ChargeCurrentWriteKind::Restore);
        assert_eq!(pending.expected_before_a.to_bits(), 14.0_f64.to_bits());
        assert_eq!(pending.intended_a.to_bits(), (-1.0_f64).to_bits());

        let mut restarted = Controller::new(
            bus.clone(),
            store.clone(),
            clock(1, 1, 1, 100_001.0),
            FakeLog::default(),
            config(false),
            interrupted,
        );
        restarted.run_once();

        assert_eq!(bus.number("settings", MAX_CHARGE_CURRENT_PATH), Some(-1.0));
        assert!(!restarted.state.charge_current_control.owned);
        assert!(
            restarted
                .state
                .charge_current_control
                .pending_write
                .is_none()
        );
    }
}

#[test]
fn external_value_supersedes_an_interrupted_reserve_restore() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 45.0);
    bus.value("settings", MIN_SOC_PATH, 45.0);
    bus.value("settings", MAX_CHARGE_CURRENT_PATH, 14.0);
    bus.script([ScriptStep::new(
        FakeDbusOperation::WriteInteger,
        "settings",
        MAX_CHARGE_CURRENT_PATH,
        ScriptEffect::CrashBefore,
    )]);
    let store = FakeStore::default();
    let mut controller = Controller::new(
        bus.clone(),
        store.clone(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        config(false),
        ControllerState {
            boot_ts: 100_000.0,
            min_soc_last_seen: Some(45.0),
            charge_current_control: ChargeCurrentControlState {
                external_baseline_a: Some(-1.0),
                reserve_constraint_a: Some(14.0),
                owned: true,
                last_effectively_written_a: Some(14.0),
                ..ChargeCurrentControlState::default()
            },
            ..ControllerState::default()
        },
    );

    assert!(catch_unwind(AssertUnwindSafe(|| controller.run_once())).is_err());
    bus.value("settings", MAX_CHARGE_CURRENT_PATH, 5.0);
    let mut restarted = Controller::new(
        bus.clone(),
        store.clone(),
        clock(1, 1, 1, 100_001.0),
        FakeLog::default(),
        config(false),
        store.latest_state(),
    );
    restarted.run_once();

    assert_eq!(bus.number("settings", MAX_CHARGE_CURRENT_PATH), Some(5.0));
    assert!(!restarted.state.charge_current_control.owned);
    assert!(
        restarted
            .state
            .charge_current_control
            .pending_write
            .is_none()
    );
}

#[test]
fn shutdown_restores_an_owned_charge_current_ceiling() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 95.0);
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(9, 1, 12, 2_000.0),
        FakeLog::default(),
        config(false),
        ControllerState::default(),
    );
    controller.run_once();

    controller.shutdown();

    assert_eq!(bus.writes().last().map(|write| write.2), Some(-1.0));
    assert!(!controller.state.charge_current_control.owned);
    assert_eq!(controller.state.charge_current_control.pending_write, None);
}

fn state_with_all_owned_settings() -> ControllerState {
    ControllerState {
        min_soc_last_seen: Some(45.0),
        min_soc_last_script_set: Some(45.0),
        minimum_soc_control: MinimumSocControlState {
            owned: true,
            external_baseline: Some(10.0),
            last_set: Some(45.0),
            ..MinimumSocControlState::default()
        },
        charge_current_control: ChargeCurrentControlState {
            external_baseline_a: Some(-1.0),
            reserve_constraint_a: Some(14.0),
            owned: true,
            last_effectively_written_a: Some(14.0),
            ..ChargeCurrentControlState::default()
        },
        discharge_protection: DischargeProtectionState {
            active: true,
            restore_default: true,
            last_set_power_w: Some(1_000.0),
            last_observed_power_w: Some(1_000.0),
            ..DischargeProtectionState::default()
        },
        ..ControllerState::default()
    }
}

fn bus_with_all_owned_settings() -> FakeDbus {
    let bus = base_bus();
    bus.value("settings", MIN_SOC_PATH, 45.0);
    bus.value("settings", MAX_CHARGE_CURRENT_PATH, 14.0);
    bus.value("settings", MAX_DISCHARGE_POWER_PATH, 1_000.0);
    configure_nominal_inverter_power(&bus, 2_500.0);
    bus
}

#[test]
fn normal_shutdown_restores_only_max_charge_current() {
    let bus = bus_with_all_owned_settings();
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(1, 1, 1, 2_000.0),
        FakeLog::default(),
        config(false),
        state_with_all_owned_settings(),
    );

    controller.shutdown();

    assert_eq!(bus.number("settings", MAX_CHARGE_CURRENT_PATH), Some(-1.0));
    assert_eq!(
        bus.number("settings", MAX_DISCHARGE_POWER_PATH),
        Some(1_000.0)
    );
    assert_eq!(bus.number("settings", MIN_SOC_PATH), Some(45.0));
    assert!(controller.state.discharge_protection.active);
    assert!(controller.state.minimum_soc_control.owned);
}

#[test]
fn explicit_cleanup_restores_every_unambiguously_owned_setting() {
    let bus = bus_with_all_owned_settings();
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(1, 1, 1, 2_000.0),
        FakeLog::default(),
        config(false),
        state_with_all_owned_settings(),
    );

    let result = controller.restore_all_owned_settings();

    assert!(result.is_ok());
    assert_eq!(bus.number("settings", MAX_CHARGE_CURRENT_PATH), Some(-1.0));
    assert_eq!(bus.number("settings", MAX_DISCHARGE_POWER_PATH), Some(-1.0));
    assert_eq!(bus.number("settings", MIN_SOC_PATH), Some(10.0));
    assert!(!controller.state.charge_current_control.owned);
    assert!(!controller.state.discharge_protection.active);
    assert!(!controller.state.minimum_soc_control.owned);
}

#[test]
fn explicit_cleanup_retains_settings_changed_by_an_external_actor() {
    let bus = bus_with_all_owned_settings();
    bus.value("settings", MAX_CHARGE_CURRENT_PATH, 5.0);
    bus.value("settings", MAX_DISCHARGE_POWER_PATH, 500.0);
    bus.value("settings", MIN_SOC_PATH, 20.0);
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(1, 1, 1, 2_000.0),
        FakeLog::default(),
        config(false),
        state_with_all_owned_settings(),
    );

    let result = controller.restore_all_owned_settings();

    assert!(result.is_ok());
    assert_eq!(bus.number("settings", MAX_CHARGE_CURRENT_PATH), Some(5.0));
    assert_eq!(
        bus.number("settings", MAX_DISCHARGE_POWER_PATH),
        Some(500.0)
    );
    assert_eq!(bus.number("settings", MIN_SOC_PATH), Some(20.0));
    assert!(!controller.state.discharge_protection.active);
    assert!(!controller.state.minimum_soc_control.owned);
}

#[test]
fn explicit_cleanup_keeps_discharge_ownership_when_readback_fails() {
    let bus = base_bus();
    bus.value("settings", MAX_DISCHARGE_POWER_PATH, 1_000.0);
    configure_nominal_inverter_power(&bus, 2_500.0);
    bus.ignore_next_discharge_power_write();
    let state = ControllerState {
        discharge_protection: DischargeProtectionState {
            active: true,
            restore_default: true,
            last_set_power_w: Some(1_000.0),
            last_observed_power_w: Some(1_000.0),
            ..DischargeProtectionState::default()
        },
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(1, 1, 1, 2_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let result = controller.restore_all_owned_settings();

    assert!(result.is_err());
    assert_eq!(
        bus.number("settings", MAX_DISCHARGE_POWER_PATH),
        Some(1_000.0)
    );
    assert!(controller.state.discharge_protection.active);
    assert!(
        controller
            .state
            .discharge_protection
            .pending_write
            .is_some()
    );
}

#[test]
fn shadow_mode_refuses_explicit_cleanup_without_writing() {
    let bus = bus_with_all_owned_settings();
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(1, 1, 1, 2_000.0),
        FakeLog::default(),
        config(true),
        state_with_all_owned_settings(),
    );

    let result = controller.restore_all_owned_settings();

    assert!(result.is_err());
    assert!(bus.writes().is_empty());
}

#[test]
fn explicit_cleanup_keeps_minimum_soc_ownership_when_readback_fails() {
    let bus = base_bus();
    bus.value("settings", MIN_SOC_PATH, 45.0);
    bus.ignore_next_min_soc_write();
    let state = ControllerState {
        minimum_soc_control: MinimumSocControlState {
            owned: true,
            external_baseline: Some(10.0),
            last_set: Some(45.0),
            ..MinimumSocControlState::default()
        },
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(1, 1, 1, 2_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let result = controller.restore_all_owned_settings();

    assert!(result.is_err());
    assert_eq!(bus.number("settings", MIN_SOC_PATH), Some(45.0));
    assert!(controller.state.minimum_soc_control.owned);
    assert!(controller.state.minimum_soc_control.pending_write.is_some());
}

#[test]
fn minimum_soc_cleanup_recovers_crashes_before_and_after_the_write() {
    for effect in [ScriptEffect::CrashBefore, ScriptEffect::CrashAfter] {
        let bus = base_bus();
        bus.value("settings", MIN_SOC_PATH, 45.0);
        bus.script([ScriptStep::new(
            FakeDbusOperation::WriteFloat,
            "settings",
            MIN_SOC_PATH,
            effect,
        )]);
        let store = FakeStore::default();
        let state = ControllerState {
            minimum_soc_control: MinimumSocControlState {
                owned: true,
                external_baseline: Some(10.0),
                last_set: Some(45.0),
                ..MinimumSocControlState::default()
            },
            ..ControllerState::default()
        };
        let mut controller = Controller::new(
            bus.clone(),
            store.clone(),
            clock(1, 1, 1, 2_000.0),
            FakeLog::default(),
            config(false),
            state,
        );

        assert!(
            catch_unwind(AssertUnwindSafe(|| controller.restore_all_owned_settings())).is_err()
        );
        bus.assert_script_complete();
        let mut restarted = Controller::new(
            bus.clone(),
            store.clone(),
            clock(1, 1, 1, 2_001.0),
            FakeLog::default(),
            config(false),
            store.latest_state(),
        );

        assert!(restarted.restore_all_owned_settings().is_ok());
        assert_eq!(bus.number("settings", MIN_SOC_PATH), Some(10.0));
        assert!(!restarted.state.minimum_soc_control.owned);
        assert!(restarted.state.minimum_soc_control.pending_write.is_none());
    }
}

const VEBUS_SERVICE: &str = "com.victronenergy.vebus.test";
const ACTIVE_BMS_SERVICE: &str = "com.victronenergy.battery.preferred";
const OTHER_BMS_SERVICE: &str = "com.victronenergy.battery.unrelated";
const OTHER_VEBUS_SERVICE: &str = "com.victronenergy.vebus.unrelated";
const AC_GRID_L1_PV_CHANNEL: u8 = 1;
const DC_PV_CHANNEL: u8 = 1 << 6;

fn configure_nominal_inverter_power(bus: &FakeDbus, power_w: f64) {
    bus.value(VEBUS_SERVICE, NOMINAL_INVERTER_POWER_PATH, power_w);
}

#[test]
fn pv_history_accepts_real_zero_from_the_only_expected_channel() {
    let bus = base_bus();
    bus.value("system", DC_PV_POWER_PATH, 0.0);
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock_times(11, 10, 10, 1_000.0, 1_000.0),
        FakeLog::default(),
        config(false),
        ControllerState {
            pv_expected_channels: DC_PV_CHANNEL,
            pv_time_s: 60.0,
            pv_last_sample_ts: 940.0,
            last_sample_date: "2026-11-10".to_owned(),
            ..ControllerState::default()
        },
    );

    let decision = controller.run_once();

    assert_eq!(controller.state.current_day_samples, vec![0.0]);
    assert_eq!(controller.state.pv_time_s.to_bits(), 120.0_f64.to_bits());
    assert_eq!(decision.pv_sample_valid, Some(true));
    assert_eq!(decision.pv_expected_channels, 1);
    assert_eq!(decision.pv_valid_channels, 1);
}

#[test]
fn pv_history_rejects_a_missing_expected_channel_and_breaks_the_integral() {
    let bus = base_bus();
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock_times(11, 10, 10, 1_000.0, 1_000.0),
        FakeLog::default(),
        config(false),
        ControllerState {
            pv_expected_channels: DC_PV_CHANNEL,
            pv_energy_ws: 30_000.0,
            pv_time_s: 60.0,
            pv_last_sample_ts: 940.0,
            pv_last_sample_power: 500.0,
            last_sample_date: "2026-11-10".to_owned(),
            ..ControllerState::default()
        },
    );

    let decision = controller.run_once();

    assert!(controller.state.current_day_samples.is_empty());
    assert_eq!(
        controller.state.pv_energy_ws.to_bits(),
        30_000.0_f64.to_bits()
    );
    assert_eq!(controller.state.pv_time_s.to_bits(), 60.0_f64.to_bits());
    assert_eq!(
        controller.state.pv_last_sample_ts.to_bits(),
        0.0_f64.to_bits()
    );
    assert_eq!(decision.pv_sample_valid, Some(false));
    assert_eq!(decision.pv_expected_channels, 1);
    assert_eq!(decision.pv_valid_channels, 0);
}

#[test]
fn pv_history_rejects_partial_ac_dc_telemetry() {
    let bus = base_bus();
    bus.value("system", DC_PV_POWER_PATH, 500.0);
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock_times(11, 10, 10, 1_000.0, 1_000.0),
        FakeLog::default(),
        config(false),
        ControllerState {
            pv_expected_channels: AC_GRID_L1_PV_CHANNEL | DC_PV_CHANNEL,
            pv_time_s: 60.0,
            pv_last_sample_ts: 940.0,
            pv_last_sample_power: 500.0,
            last_sample_date: "2026-11-10".to_owned(),
            ..ControllerState::default()
        },
    );

    let decision = controller.run_once();

    assert_eq!(controller.state.pv_time_s.to_bits(), 60.0_f64.to_bits());
    assert_eq!(decision.pv_sample_valid, Some(false));
    assert_eq!(decision.pv_expected_channels, 2);
    assert_eq!(decision.pv_valid_channels, 1);
}

#[test]
fn pv_history_rejects_wrongly_typed_expected_telemetry() {
    let bus = base_bus();
    bus.read_error("system", DC_PV_POWER_PATH, DbusFailureKind::TypeMismatch);
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock_times(11, 10, 10, 1_000.0, 1_000.0),
        FakeLog::default(),
        config(false),
        ControllerState {
            pv_expected_channels: DC_PV_CHANNEL,
            pv_time_s: 60.0,
            pv_last_sample_ts: 940.0,
            last_sample_date: "2026-11-10".to_owned(),
            ..ControllerState::default()
        },
    );

    let decision = controller.run_once();

    assert_eq!(controller.state.pv_time_s.to_bits(), 60.0_f64.to_bits());
    assert_eq!(decision.pv_sample_valid, Some(false));
    assert!(decision.dbus_read_issues.iter().any(|issue| {
        issue.kind == DbusFailureKind::TypeMismatch && issue.path == DC_PV_POWER_PATH
    }));
}

#[test]
fn pv_history_rejects_a_transport_failed_observation() {
    let bus = base_bus();
    bus.fail_next_transport();
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock_times(11, 10, 10, 1_000.0, 1_000.0),
        FakeLog::default(),
        config(false),
        ControllerState {
            pv_expected_channels: DC_PV_CHANNEL,
            pv_energy_ws: 30_000.0,
            pv_time_s: 60.0,
            pv_last_sample_ts: 940.0,
            pv_last_sample_power: 500.0,
            last_sample_date: "2026-11-10".to_owned(),
            ..ControllerState::default()
        },
    );

    let decision = controller.run_once();

    assert_eq!(
        controller.state.pv_energy_ws.to_bits(),
        30_000.0_f64.to_bits()
    );
    assert_eq!(controller.state.pv_time_s.to_bits(), 60.0_f64.to_bits());
    assert_eq!(
        controller.state.pv_last_sample_ts.to_bits(),
        0.0_f64.to_bits()
    );
    assert_eq!(decision.pv_sample_valid, Some(false));
    assert!(
        decision
            .dbus_read_issues
            .iter()
            .any(|issue| issue.kind == DbusFailureKind::Timeout)
    );
}

#[test]
fn pv_history_does_not_invent_a_sample_without_any_configured_channel() {
    let bus = base_bus();
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock_times(11, 10, 10, 1_000.0, 1_000.0),
        FakeLog::default(),
        config(false),
        ControllerState {
            last_sample_date: "2026-11-10".to_owned(),
            ..ControllerState::default()
        },
    );

    let decision = controller.run_once();

    assert!(controller.state.current_day_samples.is_empty());
    assert_eq!(controller.state.pv_time_s.to_bits(), 0.0_f64.to_bits());
    assert_eq!(decision.pv_sample_valid, Some(false));
    assert_eq!(decision.pv_expected_channels, 0);
    assert_eq!(decision.pv_valid_channels, 0);
}

#[test]
fn summer_external_min_soc_change_is_preserved_for_24_hours() {
    let bus = base_bus();
    bus.value("settings", MIN_SOC_PATH, 30.0);
    let state = ControllerState {
        boot_ts: 1_000.0,
        min_soc_last_seen: Some(10.0),
        last_sample_date: "2026-07-01".to_owned(),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(7, 1, 12, 2_000.0),
        FakeLog::default(),
        config(false),
        state,
    );
    let decision = controller.run_once();
    assert_eq!(decision.outcome, CycleOutcome::NoChange);
    assert_eq!(
        controller.state.manual_override_until_ts.to_bits(),
        88_400.0_f64.to_bits()
    );
    assert!(bus.writes().is_empty());
}

#[test]
fn winter_ignores_pv_history_and_applies_grid_soft_current() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value(
        "com.victronenergy.battery.preferred",
        BMS_MAX_CHARGE_CURRENT_PATH,
        200.0,
    );
    bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 35.0);
    for phase in ["L1", "L2", "L3"] {
        bus.value(
            "system",
            &format!("/Ac/ConsumptionOnInput/{phase}/Power"),
            500.0,
        );
    }
    let state = ControllerState {
        boot_ts: 100_000.0,
        pv_history: vec![1_000.0, 2_000.0, 2_500.0, 2_800.0],
        min_soc_last_seen: Some(10.0),
        last_sample_date: "2026-01-01".to_owned(),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        config(false),
        state,
    );
    let decision = controller.run_once();
    assert_eq!(decision.mode, "Winter");
    assert_eq!(decision.requested_min_soc, Some(45.0));
    assert_eq!(
        decision.requested_max_charge_current,
        Some(14.0),
        "decision={decision:?}, state={:?}, writes={:?}",
        controller.state,
        bus.writes()
    );
    assert!(controller.state.charge_current_control.owned);
    assert_eq!(
        controller.state.charge_current_control.external_baseline_a,
        Some(-1.0)
    );
    assert!(controller.state.pv_history.is_empty());
    assert!(controller.state.current_day_samples.is_empty());
    assert!(bus.reads().iter().any(|(_, path)| path == DC_PV_POWER_PATH));
    assert!(
        bus.writes()
            .iter()
            .any(|(_, path, value)| path == MIN_SOC_PATH && value.to_bits() == 45.0_f64.to_bits())
    );
    let writes = bus.writes();
    assert_eq!(writes[0].1, MAX_CHARGE_CURRENT_PATH);
    assert_eq!(writes[1].1, MIN_SOC_PATH);
}

#[test]
fn reserve_charge_never_raises_min_soc_after_a_current_write_failure() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value(ACTIVE_BMS_SERVICE, BMS_MAX_CHARGE_CURRENT_PATH, 200.0);
    bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 35.0);
    for phase in ["L1", "L2", "L3"] {
        bus.value(
            "system",
            &format!("/Ac/ConsumptionOnInput/{phase}/Power"),
            500.0,
        );
    }
    bus.fail_next_charge_current_write();
    let state = ControllerState {
        boot_ts: 100_000.0,
        min_soc_last_seen: Some(10.0),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.outcome, CycleOutcome::DbusFailure);
    assert_eq!(decision.requested_min_soc, None);
    assert_eq!(
        decision.reserve_charging_pause_reason,
        Some(ReserveChargingPauseReason::ChargeCurrentWriteFailed)
    );
    assert!(decision.reserve_charging_paused);
    assert!(bus.writes().iter().all(|(_, path, _)| path != MIN_SOC_PATH));
}

#[test]
fn reserve_charge_never_raises_min_soc_without_current_readback() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value(ACTIVE_BMS_SERVICE, BMS_MAX_CHARGE_CURRENT_PATH, 200.0);
    bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 35.0);
    for phase in ["L1", "L2", "L3"] {
        bus.value(
            "system",
            &format!("/Ac/ConsumptionOnInput/{phase}/Power"),
            500.0,
        );
    }
    bus.ignore_next_charge_current_write();
    let state = ControllerState {
        boot_ts: 100_000.0,
        min_soc_last_seen: Some(10.0),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_min_soc, None);
    assert_eq!(
        decision.reserve_charging_pause_reason,
        Some(ReserveChargingPauseReason::ChargeCurrentReadbackFailed)
    );
    assert!(decision.reserve_charging_paused);
    assert!(bus.writes().iter().all(|(_, path, _)| path != MIN_SOC_PATH));
}

#[test]
fn reserve_charge_pauses_when_the_min_soc_readback_does_not_confirm_the_write() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value(ACTIVE_BMS_SERVICE, BMS_MAX_CHARGE_CURRENT_PATH, 200.0);
    bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 35.0);
    for phase in ["L1", "L2", "L3"] {
        bus.value(
            "system",
            &format!("/Ac/ConsumptionOnInput/{phase}/Power"),
            500.0,
        );
    }
    bus.ignore_next_min_soc_write();
    let state = ControllerState {
        boot_ts: 100_000.0,
        min_soc_last_seen: Some(10.0),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_max_charge_current, Some(14.0));
    assert_eq!(decision.requested_min_soc, Some(30.0));
    assert_eq!(
        decision.reserve_charging_pause_reason,
        Some(ReserveChargingPauseReason::MinimumSocReadbackFailed)
    );
    assert!(decision.reserve_charging_paused);
    let min_soc_writes: Vec<f64> = bus
        .writes()
        .into_iter()
        .filter_map(|(_, path, value)| (path == MIN_SOC_PATH).then_some(value))
        .collect();
    assert_eq!(min_soc_writes, vec![45.0, 30.0]);
}

#[test]
fn transport_failure_between_confirmed_current_limit_and_min_soc_never_raises_min_soc() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value(ACTIVE_BMS_SERVICE, BMS_MAX_CHARGE_CURRENT_PATH, 200.0);
    bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 35.0);
    for phase in ["L1", "L2", "L3"] {
        bus.value(
            "system",
            &format!("/Ac/ConsumptionOnInput/{phase}/Power"),
            500.0,
        );
    }
    bus.script([ScriptStep::new(
        FakeDbusOperation::WriteFloat,
        "settings",
        MIN_SOC_PATH,
        ScriptEffect::FailTransport,
    )]);
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        config(false),
        ControllerState {
            boot_ts: 100_000.0,
            min_soc_last_seen: Some(10.0),
            ..ControllerState::default()
        },
    );

    let decision = controller.run_once();

    bus.assert_script_complete();
    assert_eq!(decision.outcome, CycleOutcome::DbusFailure);
    assert_eq!(decision.requested_max_charge_current, Some(14.0));
    assert_eq!(decision.requested_min_soc, Some(30.0));
    assert_eq!(bus.number("settings", MAX_CHARGE_CURRENT_PATH), Some(14.0));
    assert_eq!(bus.number("settings", MIN_SOC_PATH), Some(10.0));
    assert!(controller.state.charging_mode_active);
    assert!(controller.state.charging_paused);
}

#[test]
fn grid_charge_limit_adds_ac_and_dc_pv_current() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value(
        "com.victronenergy.battery.preferred",
        BMS_MAX_CHARGE_CURRENT_PATH,
        200.0,
    );
    bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 35.0);
    bus.value("system", "/Ac/PvOnGrid/L1/Power", 520.0);
    bus.value("system", DC_PV_POWER_PATH, 520.0);
    for phase in ["L1", "L2", "L3"] {
        bus.value(
            "system",
            &format!("/Ac/ConsumptionOnInput/{phase}/Power"),
            500.0,
        );
    }
    let state = ControllerState {
        boot_ts: 100_000.0,
        min_soc_last_seen: Some(10.0),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_max_charge_current, Some(33.0));
    assert_eq!(decision.requested_min_soc, Some(45.0));
    assert!(controller.state.charge_current_control.owned);
}

#[test]
fn active_bms_service_is_authoritative_over_a_larger_unrelated_limit() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value(ACTIVE_BMS_SERVICE, BMS_MAX_CHARGE_CURRENT_PATH, 40.0);
    bus.value(OTHER_BMS_SERVICE, BMS_MAX_CHARGE_CURRENT_PATH, 400.0);
    bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 210.0);
    for phase in ["L1", "L2", "L3"] {
        bus.value(
            "system",
            &format!("/Ac/ConsumptionOnInput/{phase}/Power"),
            500.0,
        );
    }
    let mut runtime_config = config(false);
    runtime_config.fallback_battery_service = Some(OTHER_BMS_SERVICE.to_owned());
    let state = ControllerState {
        boot_ts: 100_000.0,
        min_soc_last_seen: Some(10.0),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        runtime_config,
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_max_charge_current, Some(16.0));
    assert_eq!(
        controller.state.battery_service.as_deref(),
        Some(ACTIVE_BMS_SERVICE)
    );
    assert!(bus.reads().iter().any(|(service, path)| {
        service == ACTIVE_BMS_SERVICE && path == BMS_MAX_CHARGE_CURRENT_PATH
    }));
    assert!(
        !bus.reads()
            .iter()
            .any(|(service, _)| service == OTHER_BMS_SERVICE)
    );
}

#[test]
fn active_vebus_service_is_authoritative_over_a_larger_unrelated_limit() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value(ACTIVE_BMS_SERVICE, BMS_MAX_CHARGE_CURRENT_PATH, 200.0);
    bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 35.0);
    bus.value(OTHER_VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 350.0);
    for phase in ["L1", "L2", "L3"] {
        bus.value(
            "system",
            &format!("/Ac/ConsumptionOnInput/{phase}/Power"),
            500.0,
        );
    }
    let state = ControllerState {
        boot_ts: 100_000.0,
        min_soc_last_seen: Some(10.0),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_max_charge_current, Some(14.0));
    assert_eq!(
        controller.state.vebus_service.as_deref(),
        Some(VEBUS_SERVICE)
    );
    assert!(bus.reads().iter().any(|(service, path)| {
        service == VEBUS_SERVICE && path == VEBUS_MAX_CHARGE_CURRENT_PATH
    }));
    assert!(
        !bus.reads()
            .iter()
            .any(|(service, _)| service == OTHER_VEBUS_SERVICE)
    );
}

#[test]
fn vebus_service_change_rebinds_charge_limits_within_the_same_controller() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value(ACTIVE_BMS_SERVICE, BMS_MAX_CHARGE_CURRENT_PATH, 200.0);
    bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 35.0);
    bus.value(OTHER_VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 0.0);
    for phase in ["L1", "L2", "L3"] {
        bus.value(
            "system",
            &format!("/Ac/ConsumptionOnInput/{phase}/Power"),
            0.0,
        );
    }
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        config(false),
        ControllerState {
            boot_ts: 100_000.0,
            min_soc_last_seen: Some(10.0),
            ..ControllerState::default()
        },
    );

    let first = controller.run_once();
    assert_eq!(first.requested_max_charge_current, Some(14.0));
    assert_eq!(
        controller.state.vebus_service.as_deref(),
        Some(VEBUS_SERVICE)
    );

    bus.text_value("system", VEBUS_SERVICE_PATH, OTHER_VEBUS_SERVICE);
    let second = controller.run_once();

    assert_eq!(second.requested_max_charge_current, Some(0.0));
    assert!(second.charging_inhibited);
    assert_eq!(
        second.charging_inhibit_reasons,
        vec![ChargingInhibitReason::VebusMaxChargeCurrentZero]
    );
    assert_eq!(
        controller.state.vebus_service.as_deref(),
        Some(OTHER_VEBUS_SERVICE)
    );
    assert_eq!(bus.number("settings", MAX_CHARGE_CURRENT_PATH), Some(0.0));
}

#[test]
fn zero_amp_active_bms_limit_is_an_explicit_charge_prohibition() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value(ACTIVE_BMS_SERVICE, BMS_MAX_CHARGE_CURRENT_PATH, 0.0);
    bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 35.0);
    for phase in ["L1", "L2", "L3"] {
        bus.value(
            "system",
            &format!("/Ac/ConsumptionOnInput/{phase}/Power"),
            500.0,
        );
    }
    let state = ControllerState {
        boot_ts: 100_000.0,
        min_soc_last_seen: Some(10.0),
        battery_service: Some(ACTIVE_BMS_SERVICE.to_owned()),
        battery_max_current_last: Some(200.0),
        battery_max_current_last_seen_ts: 99_999.0,
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_max_charge_current, Some(0.0));
    assert_eq!(controller.state.battery_max_current_last, Some(0.0));
    assert!(controller.state.charge_current_control.owned);
    assert!(decision.charging_inhibited);
    assert_eq!(
        decision.charging_inhibit_reasons,
        vec![ChargingInhibitReason::BmsMaxChargeCurrentZero]
    );
    assert_eq!(decision.requested_min_soc, None);
    assert!(controller.state.charging_paused);
}

#[test]
fn zero_amp_vebus_limit_is_an_explicit_charge_prohibition() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value(ACTIVE_BMS_SERVICE, BMS_MAX_CHARGE_CURRENT_PATH, 200.0);
    bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 0.0);
    let state = ControllerState {
        boot_ts: 100_000.0,
        min_soc_last_seen: Some(10.0),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_max_charge_current, Some(0.0));
    assert_eq!(decision.requested_min_soc, None);
    assert!(decision.charging_inhibited);
    assert_eq!(
        decision.charging_inhibit_reasons,
        vec![ChargingInhibitReason::VebusMaxChargeCurrentZero]
    );
    assert!(controller.state.charging_paused);
}

#[test]
fn explicit_system_and_device_prohibitions_are_reported_and_enforced() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value(ACTIVE_BMS_SERVICE, BMS_MAX_CHARGE_CURRENT_PATH, 200.0);
    bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 35.0);
    bus.value("system", SYSTEM_CHARGE_DISABLED_PATH, 1.0);
    bus.value("system", SYSTEM_USER_CHARGE_LIMITED_PATH, 0.0);
    bus.value(ACTIVE_BMS_SERVICE, BMS_ALLOW_TO_CHARGE_PATH, 0.0);
    bus.value(VEBUS_SERVICE, VEBUS_ALLOW_TO_CHARGE_PATH, 0.0);
    let state = ControllerState {
        boot_ts: 100_000.0,
        min_soc_last_seen: Some(10.0),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_max_charge_current, Some(0.0));
    assert_eq!(decision.requested_min_soc, None);
    assert_eq!(decision.user_charge_limited, Some(false));
    assert_eq!(
        decision.charging_inhibit_reasons,
        vec![
            ChargingInhibitReason::SystemChargeDisabled,
            ChargingInhibitReason::ActiveBmsDisallowsCharge,
            ChargingInhibitReason::VebusDisallowsCharge,
        ]
    );
}

#[test]
fn user_charge_limited_is_diagnostic_and_not_a_blanket_prohibition() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value(ACTIVE_BMS_SERVICE, BMS_MAX_CHARGE_CURRENT_PATH, 200.0);
    bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 35.0);
    bus.value("system", SYSTEM_CHARGE_DISABLED_PATH, 0.0);
    bus.value("system", SYSTEM_USER_CHARGE_LIMITED_PATH, 1.0);
    bus.value(ACTIVE_BMS_SERVICE, BMS_ALLOW_TO_CHARGE_PATH, 1.0);
    bus.value(VEBUS_SERVICE, VEBUS_ALLOW_TO_CHARGE_PATH, 1.0);
    for phase in ["L1", "L2", "L3"] {
        bus.value(
            "system",
            &format!("/Ac/ConsumptionOnInput/{phase}/Power"),
            500.0,
        );
    }
    let state = ControllerState {
        boot_ts: 100_000.0,
        min_soc_last_seen: Some(10.0),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert!(!decision.charging_inhibited);
    assert!(decision.charging_inhibit_reasons.is_empty());
    assert_eq!(decision.user_charge_limited, Some(true));
    assert_eq!(decision.requested_min_soc, Some(45.0));
    assert_eq!(decision.requested_max_charge_current, Some(14.0));
}

#[test]
fn external_inhibit_prevents_restoring_an_owned_zero_amp_ceiling() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 88.0);
    bus.value(ACTIVE_BMS_SERVICE, BMS_MAX_CHARGE_CURRENT_PATH, 200.0);
    bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 35.0);
    bus.value(ACTIVE_BMS_SERVICE, BMS_ALLOW_TO_CHARGE_PATH, 0.0);
    bus.value("settings", MAX_CHARGE_CURRENT_PATH, 0.0);
    let state = ControllerState {
        boot_ts: 100_000.0,
        min_soc_last_seen: Some(10.0),
        charge_current_control: ChargeCurrentControlState {
            external_baseline_a: Some(-1.0),
            routine_ceiling_requested: true,
            owned: true,
            last_effectively_written_a: Some(0.0),
            ..ChargeCurrentControlState::default()
        },
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(9, 1, 12, 100_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert!(decision.charging_inhibited);
    assert!(controller.state.charge_current_control.owned);
    assert!(
        controller
            .state
            .charge_current_control
            .routine_ceiling_requested
    );
    assert_eq!(decision.requested_charge_ceiling_current_a, None);
    assert!(
        bus.writes()
            .iter()
            .all(|(_, path, value)| path != MAX_CHARGE_CURRENT_PATH || *value == 0.0)
    );
}

#[test]
fn explicitly_configured_bms_fallback_is_used_only_without_an_active_bms() {
    let bus = base_bus();
    bus.remove_text_value("system", ACTIVE_BMS_SERVICE_PATH);
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value(OTHER_BMS_SERVICE, BMS_MAX_CHARGE_CURRENT_PATH, 40.0);
    bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 210.0);
    for phase in ["L1", "L2", "L3"] {
        bus.value(
            "system",
            &format!("/Ac/ConsumptionOnInput/{phase}/Power"),
            500.0,
        );
    }
    let mut runtime_config = config(false);
    runtime_config.fallback_battery_service = Some(OTHER_BMS_SERVICE.to_owned());
    let state = ControllerState {
        boot_ts: 100_000.0,
        min_soc_last_seen: Some(10.0),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        runtime_config,
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_max_charge_current, Some(16.0));
    assert_eq!(
        controller.state.battery_service.as_deref(),
        Some(OTHER_BMS_SERVICE)
    );
}

#[test]
fn grid_charge_never_raises_a_stricter_gui_current_limit() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value("settings", MAX_CHARGE_CURRENT_PATH, 10.0);
    bus.value(
        "com.victronenergy.battery.preferred",
        BMS_MAX_CHARGE_CURRENT_PATH,
        200.0,
    );
    bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 35.0);
    for phase in ["L1", "L2", "L3"] {
        bus.value(
            "system",
            &format!("/Ac/ConsumptionOnInput/{phase}/Power"),
            500.0,
        );
    }
    let state = ControllerState {
        boot_ts: 100_000.0,
        min_soc_last_seen: Some(10.0),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_max_charge_current, None);
    assert!(!controller.state.charge_current_control.owned);
    assert!(
        bus.writes()
            .iter()
            .all(|(_, path, _)| path != MAX_CHARGE_CURRENT_PATH)
    );
}

#[test]
fn declared_single_phase_load_requires_only_l1() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value("system", AC_CONSUMPTION_ON_INPUT_PHASE_COUNT_PATH, 1.0);
    bus.value("system", "/Ac/ConsumptionOnInput/L1/Power", 3_800.0);
    bus.value(
        "com.victronenergy.battery.preferred",
        BMS_MAX_CHARGE_CURRENT_PATH,
        200.0,
    );
    bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 35.0);
    let state = ControllerState {
        boot_ts: 100_000.0,
        min_soc_last_seen: Some(10.0),
        last_sample_date: "2026-01-01".to_owned(),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_min_soc, Some(45.0));
    assert_eq!(decision.requested_max_charge_current, Some(10.0));
    assert!(!controller.state.charging_paused);
    assert!(bus.reads().iter().all(|(_, path)| {
        path != "/Ac/ConsumptionOnInput/L2/Power" && path != "/Ac/ConsumptionOnInput/L3/Power"
    }));
}

#[test]
fn declared_two_phase_load_requires_l1_and_l2_only() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value("system", AC_CONSUMPTION_ON_INPUT_PHASE_COUNT_PATH, 2.0);
    bus.value("system", "/Ac/ConsumptionOnInput/L1/Power", 1_900.0);
    bus.value("system", "/Ac/ConsumptionOnInput/L2/Power", 1_900.0);
    bus.value(
        "com.victronenergy.battery.preferred",
        BMS_MAX_CHARGE_CURRENT_PATH,
        200.0,
    );
    bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 35.0);
    let state = ControllerState {
        boot_ts: 100_000.0,
        min_soc_last_seen: Some(10.0),
        last_sample_date: "2026-01-01".to_owned(),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_min_soc, Some(45.0));
    assert_eq!(decision.requested_max_charge_current, Some(10.0));
    assert!(!controller.state.charging_paused);
    assert!(
        bus.reads()
            .iter()
            .all(|(_, path)| path != "/Ac/ConsumptionOnInput/L3/Power")
    );
}

#[test]
fn declared_single_phase_grid_supports_the_house_load_fallback() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value("system", AC_GRID_PHASE_COUNT_PATH, 1.0);
    bus.value("system", "/Ac/Grid/L1/Power", 3_500.0);
    bus.value(
        "com.victronenergy.battery.preferred",
        BMS_MAX_CHARGE_CURRENT_PATH,
        200.0,
    );
    bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 35.0);
    let state = ControllerState {
        boot_ts: 100_000.0,
        min_soc_last_seen: Some(10.0),
        last_sample_date: "2026-01-01".to_owned(),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_min_soc, Some(45.0));
    assert_eq!(decision.requested_max_charge_current, Some(10.0));
    assert!(!controller.state.charging_paused);
    assert!(
        bus.reads()
            .iter()
            .all(|(_, path)| { path != "/Ac/Grid/L2/Power" && path != "/Ac/Grid/L3/Power" })
    );
}

#[test]
fn grid_battery_fallback_pauses_when_battery_flow_changes_direction_during_sampling() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value("system", BATTERY_POWER_PATH, 500.0);
    bus.value("system", AC_GRID_PHASE_COUNT_PATH, 1.0);
    bus.value("system", "/Ac/Grid/L1/Power", 2_000.0);
    bus.value(ACTIVE_BMS_SERVICE, BMS_MAX_CHARGE_CURRENT_PATH, 200.0);
    bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 35.0);
    bus.script([ScriptStep::new(
        FakeDbusOperation::ReadNumber,
        "system",
        "/Ac/Grid/L1/Power",
        ScriptEffect::SetNumber {
            service: "system".to_owned(),
            path: BATTERY_POWER_PATH.to_owned(),
            value: -500.0,
        },
    )]);
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        config(false),
        ControllerState {
            boot_ts: 100_000.0,
            min_soc_last_seen: Some(10.0),
            ..ControllerState::default()
        },
    );

    let decision = controller.run_once();

    bus.assert_script_complete();
    assert_eq!(decision.requested_max_charge_current, None);
    assert_eq!(decision.requested_min_soc, None);
    assert_eq!(
        decision.reserve_charging_pause_reason,
        Some(ReserveChargingPauseReason::LoadTelemetryIncomplete)
    );
    assert!(decision.reserve_charging_paused);
}

#[test]
fn invalid_declared_phase_counts_are_never_guessed() {
    for invalid_count in [0.0, 1.5, 4.0] {
        let bus = base_bus();
        bus.value("system", BATTERY_SOC_PATH, 30.0);
        bus.value(
            "system",
            AC_CONSUMPTION_ON_INPUT_PHASE_COUNT_PATH,
            invalid_count,
        );
        for phase in ["L1", "L2", "L3"] {
            bus.value(
                "system",
                &format!("/Ac/ConsumptionOnInput/{phase}/Power"),
                500.0,
            );
        }
        let state = ControllerState {
            boot_ts: 100_000.0,
            min_soc_last_seen: Some(10.0),
            last_sample_date: "2026-01-01".to_owned(),
            ..ControllerState::default()
        };
        let mut controller = Controller::new(
            bus.clone(),
            FakeStore::default(),
            clock(1, 1, 1, 100_000.0),
            FakeLog::default(),
            config(false),
            state,
        );

        let decision = controller.run_once();

        assert_eq!(decision.requested_min_soc, None);
        assert!(controller.state.charging_paused);
        assert_eq!(
            decision.reserve_charging_pause_reason,
            Some(ReserveChargingPauseReason::LoadTelemetryIncomplete)
        );
        assert!(
            bus.reads()
                .iter()
                .all(|(_, path)| !path.starts_with("/Ac/ConsumptionOnInput/L"))
        );
    }
}

#[test]
fn incomplete_three_phase_load_never_changes_the_charge_current() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value("system", AC_CONSUMPTION_ON_INPUT_PHASE_COUNT_PATH, 3.0);
    bus.value(
        "com.victronenergy.battery.preferred",
        BMS_MAX_CHARGE_CURRENT_PATH,
        200.0,
    );
    for phase in ["L1", "L2"] {
        bus.value(
            "system",
            &format!("/Ac/ConsumptionOnInput/{phase}/Power"),
            500.0,
        );
    }
    let state = ControllerState {
        boot_ts: 100_000.0,
        pv_history: vec![1_000.0, 2_000.0, 2_500.0, 2_800.0],
        min_soc_last_seen: Some(10.0),
        last_sample_date: "2026-01-01".to_owned(),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_max_charge_current, None);
    assert_eq!(decision.requested_min_soc, None);
    assert!(controller.state.charging_paused);
    assert_eq!(
        decision.reserve_charging_pause_reason,
        Some(ReserveChargingPauseReason::LoadTelemetryIncomplete)
    );
    assert!(!controller.state.charge_current_control.owned);
}

#[test]
fn every_declared_phase_is_required_but_unused_phases_are_not_read() {
    for (phase_count, missing_phase) in [
        (1_u8, "L1"),
        (2_u8, "L1"),
        (2_u8, "L2"),
        (3_u8, "L1"),
        (3_u8, "L2"),
        (3_u8, "L3"),
    ] {
        let bus = base_bus();
        bus.value("system", BATTERY_SOC_PATH, 30.0);
        bus.value(
            "system",
            AC_CONSUMPTION_ON_INPUT_PHASE_COUNT_PATH,
            f64::from(phase_count),
        );
        bus.value(ACTIVE_BMS_SERVICE, BMS_MAX_CHARGE_CURRENT_PATH, 200.0);
        bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 35.0);
        for phase in ["L1", "L2", "L3"].iter().take(usize::from(phase_count)) {
            if *phase != missing_phase {
                bus.value(
                    "system",
                    &format!("/Ac/ConsumptionOnInput/{phase}/Power"),
                    500.0,
                );
            }
        }
        let mut controller = Controller::new(
            bus.clone(),
            FakeStore::default(),
            clock(1, 1, 1, 100_000.0),
            FakeLog::default(),
            config(false),
            ControllerState {
                boot_ts: 100_000.0,
                min_soc_last_seen: Some(10.0),
                ..ControllerState::default()
            },
        );

        let decision = controller.run_once();

        assert_eq!(
            decision.reserve_charging_pause_reason,
            Some(ReserveChargingPauseReason::LoadTelemetryIncomplete),
            "phase_count={phase_count}, missing_phase={missing_phase}"
        );
        assert_eq!(decision.requested_max_charge_current, None);
        assert_eq!(decision.requested_min_soc, None);
        for unused_phase in ["L1", "L2", "L3"].iter().skip(usize::from(phase_count)) {
            assert!(bus.reads().iter().all(|(_, path)| {
                path != &format!("/Ac/ConsumptionOnInput/{unused_phase}/Power")
            }));
        }
    }
}

#[test]
fn active_reserve_charge_is_paused_at_the_reached_soc_when_load_telemetry_disappears() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value("settings", MIN_SOC_PATH, 45.0);
    bus.value("settings", MAX_CHARGE_CURRENT_PATH, 14.0);
    bus.value("system", AC_CONSUMPTION_ON_INPUT_PHASE_COUNT_PATH, 3.0);
    bus.value(ACTIVE_BMS_SERVICE, BMS_MAX_CHARGE_CURRENT_PATH, 200.0);
    bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 35.0);
    for phase in ["L1", "L2"] {
        bus.value(
            "system",
            &format!("/Ac/ConsumptionOnInput/{phase}/Power"),
            500.0,
        );
    }
    let state = ControllerState {
        boot_ts: 100_000.0,
        min_soc_last_seen: Some(45.0),
        charging_mode_active: true,
        charge_current_control: ChargeCurrentControlState {
            external_baseline_a: Some(-1.0),
            reserve_constraint_a: Some(14.0),
            owned: true,
            last_effectively_written_a: Some(14.0),
            ..ChargeCurrentControlState::default()
        },
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_min_soc, Some(30.0));
    assert_eq!(
        decision.reserve_charging_pause_reason,
        Some(ReserveChargingPauseReason::LoadTelemetryIncomplete)
    );
    assert!(decision.reserve_charging_paused);
    assert!(
        bus.writes().iter().any(|(_, path, value)| {
            path == MIN_SOC_PATH && value.to_bits() == 30.0_f64.to_bits()
        })
    );
}

#[test]
fn missing_vebus_max_current_continues_without_current_limit() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value(
        "com.victronenergy.battery.preferred",
        BMS_MAX_CHARGE_CURRENT_PATH,
        200.0,
    );
    for phase in ["L1", "L2", "L3"] {
        bus.value(
            "system",
            &format!("/Ac/ConsumptionOnInput/{phase}/Power"),
            500.0,
        );
    }
    let state = ControllerState {
        boot_ts: 100_000.0,
        min_soc_last_seen: Some(10.0),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_max_charge_current, None);
    assert_eq!(decision.requested_min_soc, Some(45.0));
    assert!(!controller.state.charging_paused);
    assert!(!controller.state.charge_current_control.owned);
    assert!(decision.charge_current_limit_unavailable);
    assert_eq!(
        decision.charge_current_limit_unavailable_reasons,
        vec![ChargeCurrentLimitUnavailableReason::ActiveVebusLimitUnavailable]
    );
}

#[test]
fn expired_bms_charge_current_is_not_reused() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 35.0);
    for phase in ["L1", "L2", "L3"] {
        bus.value(
            "system",
            &format!("/Ac/ConsumptionOnInput/{phase}/Power"),
            500.0,
        );
    }
    let state = ControllerState {
        boot_ts: 100.0,
        pv_history: vec![1_000.0, 2_000.0, 2_500.0, 2_800.0],
        min_soc_last_seen: Some(10.0),
        last_sample_date: "2026-01-01".to_owned(),
        battery_max_current_last: Some(200.0),
        battery_max_current_last_seen_ts: 500.0,
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock(1, 1, 1, 1_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_max_charge_current, None);
    assert_eq!(decision.requested_min_soc, Some(45.0));
    assert!(!controller.state.charging_paused);
}

#[test]
fn bms_service_change_invalidates_a_still_fresh_bound_limit() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value(VEBUS_SERVICE, VEBUS_MAX_CHARGE_CURRENT_PATH, 35.0);
    for phase in ["L1", "L2", "L3"] {
        bus.value(
            "system",
            &format!("/Ac/ConsumptionOnInput/{phase}/Power"),
            500.0,
        );
    }
    let state = ControllerState {
        boot_ts: 100.0,
        min_soc_last_seen: Some(10.0),
        last_sample_date: "2026-01-01".to_owned(),
        battery_service: Some(OTHER_BMS_SERVICE.to_owned()),
        battery_max_current_last: Some(200.0),
        battery_max_current_last_seen_ts: 999.0,
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock(1, 1, 1, 1_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_max_charge_current, None);
    assert_eq!(
        controller.state.battery_service.as_deref(),
        Some(ACTIVE_BMS_SERVICE)
    );
    assert_eq!(controller.state.battery_max_current_last, None);
    assert_eq!(
        controller.state.battery_max_current_last_seen_ts.to_bits(),
        0.0_f64.to_bits()
    );
}

#[test]
fn missing_hardware_limit_releases_an_owned_charge_current_cap() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value("settings", MAX_CHARGE_CURRENT_PATH, 14.0);
    bus.value(
        "com.victronenergy.battery.preferred",
        BMS_MAX_CHARGE_CURRENT_PATH,
        200.0,
    );
    for phase in ["L1", "L2", "L3"] {
        bus.value(
            "system",
            &format!("/Ac/ConsumptionOnInput/{phase}/Power"),
            500.0,
        );
    }
    let state = ControllerState {
        boot_ts: 100_000.0,
        min_soc_last_seen: Some(10.0),
        charge_current_control: ChargeCurrentControlState {
            external_baseline_a: Some(-1.0),
            reserve_constraint_a: Some(14.0),
            owned: true,
            last_effectively_written_a: Some(14.0),
            ..ChargeCurrentControlState::default()
        },
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_max_charge_current, Some(-1.0));
    assert_eq!(decision.requested_min_soc, Some(45.0));
    assert!(!controller.state.charging_paused);
    assert!(!controller.state.charge_current_control.owned);
}

#[test]
fn winter_outside_window_holds_reached_soc_without_dvcc_limit() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    let state = ControllerState {
        boot_ts: 100_000.0,
        pv_history: vec![1_000.0, 2_000.0, 2_500.0, 2_800.0],
        min_soc_last_seen: Some(10.0),
        last_sample_date: "2026-01-01".to_owned(),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock(1, 1, 12, 100_000.0),
        FakeLog::default(),
        config(false),
        state,
    );
    let decision = controller.run_once();
    assert_eq!(decision.requested_min_soc, Some(30.0));
    assert_eq!(decision.requested_max_charge_current, None);
    assert!(controller.state.charging_paused);
    assert!(controller.state.charging_mode_active);
}

#[test]
fn paused_reserve_charge_retries_restore_until_readback_confirms_it() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 30.0);
    bus.value("settings", MIN_SOC_PATH, 30.0);
    bus.value("settings", MAX_CHARGE_CURRENT_PATH, 14.0);
    bus.ignore_next_charge_current_write();
    let state = ControllerState {
        boot_ts: 100_000.0,
        min_soc_last_seen: Some(30.0),
        charging_mode_active: true,
        charging_paused: true,
        charge_current_control: ChargeCurrentControlState {
            external_baseline_a: Some(-1.0),
            reserve_constraint_a: Some(14.0),
            owned: true,
            last_effectively_written_a: Some(14.0),
            ..ChargeCurrentControlState::default()
        },
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(1, 1, 12, 100_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let first = controller.run_once();

    assert_eq!(
        first.reserve_charging_pause_reason,
        Some(ReserveChargingPauseReason::ChargeCurrentReadbackFailed)
    );
    assert!(controller.state.charge_current_control.owned);

    let second = controller.run_once();

    assert_eq!(
        second.reserve_charging_pause_reason,
        Some(ReserveChargingPauseReason::OutsideChargeWindow)
    );
    assert!(!controller.state.charge_current_control.owned);
    let restore_writes = bus
        .writes()
        .into_iter()
        .filter(|(_, path, value)| {
            path == MAX_CHARGE_CURRENT_PATH && value.to_bits() == (-1.0_f64).to_bits()
        })
        .count();
    assert_eq!(restore_writes, 2);
}

#[test]
fn missing_soc_never_writes_settings() {
    let bus = base_bus();
    if let Ok(mut state) = bus.shared.lock() {
        state
            .values
            .remove(&("system".to_owned(), BATTERY_SOC_PATH.to_owned()));
    }
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        config(false),
        ControllerState::default(),
    );
    assert_eq!(controller.run_once().outcome, CycleOutcome::MissingSoc);
    assert!(bus.writes().is_empty());
}

#[test]
fn transport_fault_stops_the_dbus_cycle_after_one_operation() {
    let bus = base_bus();
    bus.fail_next_transport();
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(1, 1, 1, 100_000.0),
        FakeLog::default(),
        config(false),
        ControllerState::default(),
    );

    let decision = controller.run_once();

    assert_eq!(decision.outcome, CycleOutcome::DbusFailure);
    assert_eq!(bus.transport_operations(), 1);
    assert!(bus.writes().is_empty());
}

#[test]
fn epoch_jump_does_not_extend_the_manual_override() {
    let bus = base_bus();
    bus.value("settings", MIN_SOC_PATH, 30.0);
    let state = ControllerState {
        boot_ts: 1_000.0,
        min_soc_last_seen: Some(10.0),
        last_sample_date: "2026-07-01".to_owned(),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock_times(7, 1, 12, 2_000_000_000.0, 2_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(
        decision.generated_at.to_bits(),
        2_000_000_000.0_f64.to_bits()
    );
    assert_eq!(
        controller.state.manual_override_until_ts.to_bits(),
        88_400.0_f64.to_bits()
    );
}

#[test]
fn shadow_mode_records_actions_without_touching_dbus() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 5.0);
    bus.value("settings", MIN_SOC_PATH, 5.0);
    let state = ControllerState {
        min_soc_last_seen: Some(5.0),
        last_sample_date: "2026-07-01".to_owned(),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        config(true),
        state,
    );
    let decision = controller.run_once();
    assert_eq!(decision.requested_min_soc, Some(10.0));
    assert_eq!(decision.outcome, CycleOutcome::NoChange);
    assert_eq!(controller.state.min_soc_last_script_set, None);
    assert!(!controller.state.minimum_soc_control.owned);
    assert!(controller.state.minimum_soc_control.pending_write.is_none());
    assert!(!controller.state.charge_current_control.owned);
    assert!(
        controller
            .state
            .charge_current_control
            .pending_write
            .is_none()
    );
    assert!(bus.writes().is_empty());
}

#[test]
fn shadow_routine_ceiling_previews_without_committing_ownership() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 95.0);
    bus.value("settings", MAX_CHARGE_CURRENT_PATH, -1.0);
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        config(true),
        ControllerState {
            last_sample_date: "2026-07-01".to_owned(),
            ..ControllerState::default()
        },
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_charge_ceiling_current_a, Some(0.0));
    assert!(!controller.state.charge_current_control.owned);
    assert!(
        controller
            .state
            .charge_current_control
            .pending_write
            .is_none()
    );
    assert!(bus.writes().is_empty());
}

#[test]
fn shadow_recovery_does_not_commit_loaded_charge_current_pending_write() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 95.0);
    bus.value("settings", MAX_CHARGE_CURRENT_PATH, 20.0);
    let pending = PendingChargeCurrentWrite {
        generation: 7,
        kind: ChargeCurrentWriteKind::Restrict,
        expected_before_a: 20.0,
        intended_a: 0.0,
    };
    let state = ControllerState {
        last_sample_date: "2026-07-01".to_owned(),
        charge_current_control: ChargeCurrentControlState {
            external_baseline_a: Some(-1.0),
            routine_ceiling_requested: true,
            owned: true,
            last_effectively_written_a: Some(20.0),
            write_generation: 7,
            pending_write: Some(pending),
            ..ChargeCurrentControlState::default()
        },
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        config(true),
        state,
    );

    controller.run_once();

    assert_eq!(
        controller.state.charge_current_control.pending_write,
        Some(pending)
    );
    assert_eq!(
        controller
            .state
            .charge_current_control
            .last_effectively_written_a,
        Some(20.0)
    );
    assert!(controller.state.charge_current_control.owned);
    assert!(bus.writes().is_empty());
}

#[test]
fn shadow_recovery_does_not_commit_loaded_discharge_pending_write() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 19.0);
    bus.value("system", BATTERY_POWER_PATH, -500.0);
    configure_nominal_inverter_power(&bus, 2_500.0);
    let pending = PendingDischargeWrite {
        generation: 5,
        kind: DischargeWriteKind::Restrict,
        expected_before_w: 2_500.0,
        intended_w: 1_000.0,
    };
    let discharge_state = DischargeProtectionState {
        active: true,
        restore_default: true,
        write_generation: 5,
        pending_write: Some(pending),
        ..DischargeProtectionState::default()
    };
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        config(true),
        ControllerState {
            last_sample_date: "2026-07-01".to_owned(),
            discharge_protection: discharge_state.clone(),
            ..ControllerState::default()
        },
    );

    controller.run_once();

    assert_eq!(controller.state.discharge_protection, discharge_state);
    assert!(bus.writes().is_empty());
}

#[test]
fn shadow_signal_shutdown_does_not_restore_loaded_active_state() {
    let bus = base_bus();
    bus.value("settings", MAX_CHARGE_CURRENT_PATH, 20.0);
    let charge_current_control = ChargeCurrentControlState {
        external_baseline_a: Some(-1.0),
        reserve_constraint_a: Some(20.0),
        owned: true,
        last_effectively_written_a: Some(20.0),
        write_generation: 3,
        ..ChargeCurrentControlState::default()
    };
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        config(true),
        ControllerState {
            charge_current_control: charge_current_control.clone(),
            ..ControllerState::default()
        },
    );

    controller.shutdown();

    assert_eq!(
        controller.state.charge_current_control,
        charge_current_control
    );
    assert_eq!(bus.number("settings", MAX_CHARGE_CURRENT_PATH), Some(20.0));
    assert!(bus.writes().is_empty());
}

#[test]
fn configured_summer_min_soc_is_applied_without_changing_policy_defaults() {
    let bus = base_bus();
    let state = ControllerState {
        min_soc_last_seen: Some(10.0),
        last_sample_date: "2026-07-01".to_owned(),
        ..ControllerState::default()
    };
    let mut runtime_config = config(false);
    runtime_config.policy.summer_min_soc = 15.0;
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        runtime_config,
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.target_soc.to_bits(), 15.0_f64.to_bits());
    assert_eq!(decision.requested_min_soc, Some(15.0));
    assert!(
        bus.writes().iter().any(|(_, path, value)| {
            path == MIN_SOC_PATH && value.to_bits() == 15.0_f64.to_bits()
        })
    );
}

#[test]
fn low_soc_discharge_is_limited_to_40_percent_of_nominal_power() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 19.9);
    bus.value("system", BATTERY_POWER_PATH, -500.0);
    configure_nominal_inverter_power(&bus, 2_500.0);
    let state = ControllerState {
        last_sample_date: "2026-07-01".to_owned(),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_max_discharge_power, Some(1_000.0));
    assert_eq!(decision.outcome, CycleOutcome::Applied);
    assert!(controller.state.discharge_protection.active);
    assert_eq!(controller.state.discharge_protection.restore_power_w, None);
    assert!(bus.writes().iter().any(|(_, path, value)| {
        path == MAX_DISCHARGE_POWER_PATH && value.to_bits() == 1_000.0_f64.to_bits()
    }));
}

#[test]
fn discharge_write_is_committed_only_after_readback_and_retried_if_unconfirmed() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 19.9);
    bus.value("system", BATTERY_POWER_PATH, -500.0);
    configure_nominal_inverter_power(&bus, 2_500.0);
    bus.ignore_next_discharge_power_write();
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        config(false),
        ControllerState {
            last_sample_date: "2026-07-01".to_owned(),
            ..ControllerState::default()
        },
    );

    let unconfirmed = controller.run_once();

    assert!(unconfirmed.discharge_protection_pending_unenforced);
    assert_eq!(
        unconfirmed
            .discharge_protection_unenforced_reason
            .as_deref(),
        Some("dbus_readback_failed")
    );
    assert!(
        controller
            .state
            .discharge_protection
            .pending_write
            .is_some()
    );
    assert_eq!(controller.state.discharge_protection.last_set_power_w, None);

    let confirmed = controller.run_once();

    assert!(!confirmed.discharge_protection_pending_unenforced);
    assert_eq!(controller.state.discharge_protection.pending_write, None);
    assert_eq!(
        controller.state.discharge_protection.last_set_power_w,
        Some(1_000.0)
    );
    assert_eq!(
        bus.writes()
            .iter()
            .filter(|(_, path, value)| {
                path == MAX_DISCHARGE_POWER_PATH && value.to_bits() == 1_000.0_f64.to_bits()
            })
            .count(),
        2
    );
}

#[test]
fn discharge_protection_crash_matrix_recovers_before_and_after_the_dbus_write() {
    for effect in [ScriptEffect::CrashBefore, ScriptEffect::CrashAfter] {
        let bus = base_bus();
        bus.value("system", BATTERY_SOC_PATH, 19.0);
        bus.value("system", BATTERY_POWER_PATH, -500.0);
        configure_nominal_inverter_power(&bus, 2_500.0);
        bus.script([ScriptStep::new(
            FakeDbusOperation::WriteFloat,
            "settings",
            MAX_DISCHARGE_POWER_PATH,
            effect.clone(),
        )]);
        let store = FakeStore::default();
        let mut controller = Controller::new(
            bus.clone(),
            store.clone(),
            clock(7, 1, 12, 1_000.0),
            FakeLog::default(),
            config(false),
            ControllerState {
                last_sample_date: "2026-07-01".to_owned(),
                ..ControllerState::default()
            },
        );

        assert!(catch_unwind(AssertUnwindSafe(|| controller.run_once())).is_err());
        bus.assert_script_complete();

        let durable_state = store.latest_state();
        assert!(durable_state.discharge_protection.pending_write.is_some());
        let mut restarted = Controller::new(
            bus.clone(),
            store,
            clock(7, 1, 12, 1_001.0),
            FakeLog::default(),
            config(false),
            durable_state,
        );
        restarted.run_once();

        assert_eq!(
            bus.number("settings", MAX_DISCHARGE_POWER_PATH),
            Some(1_000.0)
        );
        assert!(restarted.state.discharge_protection.active);
        assert_eq!(restarted.state.discharge_protection.pending_write, None);
    }
}

#[test]
fn crash_before_restriction_does_not_replay_it_after_soc_recovery() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 19.0);
    bus.value("system", BATTERY_POWER_PATH, -500.0);
    configure_nominal_inverter_power(&bus, 2_500.0);
    bus.script([ScriptStep::new(
        FakeDbusOperation::WriteFloat,
        "settings",
        MAX_DISCHARGE_POWER_PATH,
        ScriptEffect::CrashBefore,
    )]);
    let store = FakeStore::default();
    let mut controller = Controller::new(
        bus.clone(),
        store.clone(),
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        config(false),
        ControllerState {
            last_sample_date: "2026-07-01".to_owned(),
            ..ControllerState::default()
        },
    );

    assert!(catch_unwind(AssertUnwindSafe(|| controller.run_once())).is_err());
    bus.assert_script_complete();
    let durable_state = store.latest_state();
    assert!(durable_state.discharge_protection.pending_write.is_some());
    assert_eq!(bus.number("settings", MAX_DISCHARGE_POWER_PATH), Some(-1.0));

    bus.value("system", BATTERY_SOC_PATH, 80.0);
    bus.value("system", BATTERY_POWER_PATH, 0.0);
    let mut restarted = Controller::new(
        bus.clone(),
        store.clone(),
        clock(7, 1, 12, 1_001.0),
        FakeLog::default(),
        config(false),
        durable_state,
    );

    let decision = restarted.run_once();

    assert_eq!(decision.requested_max_discharge_power, None);
    assert!(!decision.discharge_protection_active);
    assert_eq!(restarted.state.discharge_protection.pending_write, None);
    assert_eq!(bus.number("settings", MAX_DISCHARGE_POWER_PATH), Some(-1.0));
    assert!(
        !bus.writes()
            .iter()
            .any(|(_, path, _)| path == MAX_DISCHARGE_POWER_PATH)
    );
    let cancelled_state = store.latest_state();
    assert!(!cancelled_state.discharge_protection.active);
    assert_eq!(cancelled_state.discharge_protection.pending_write, None);
}

fn cached_nominal_state(
    selected_service: &str,
    observed_service: &str,
    observed_at: f64,
) -> ControllerState {
    ControllerState {
        last_sample_date: "2026-07-01".to_owned(),
        vebus_service: Some(selected_service.to_owned()),
        nominal_inverter_power_last: Some(2_500.0),
        nominal_inverter_power_service: Some(observed_service.to_owned()),
        nominal_inverter_power_observed_at: observed_at,
        ..ControllerState::default()
    }
}

#[test]
fn low_soc_protection_uses_a_fresh_service_bound_nominal_cache() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 19.0);
    bus.value("system", BATTERY_POWER_PATH, -500.0);
    let mut runtime_config = config(false);
    runtime_config.nominal_inverter_power_max_age = Duration::from_secs(100);
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        runtime_config,
        cached_nominal_state(VEBUS_SERVICE, VEBUS_SERVICE, 950.0),
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_max_discharge_power, Some(1_000.0));
    assert!(decision.discharge_protection_active);
    assert!(!decision.discharge_protection_pending_unenforced);
}

#[test]
fn stale_nominal_cache_leaves_low_soc_protection_explicitly_pending() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 19.0);
    bus.value("system", BATTERY_POWER_PATH, -500.0);
    let mut runtime_config = config(false);
    runtime_config.nominal_inverter_power_max_age = Duration::from_secs(100);
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        runtime_config,
        cached_nominal_state(VEBUS_SERVICE, VEBUS_SERVICE, 899.0),
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_max_discharge_power, None);
    assert!(!decision.discharge_protection_active);
    assert!(decision.discharge_protection_pending_unenforced);
    assert_eq!(
        decision.discharge_protection_unenforced_reason.as_deref(),
        Some("nominal_inverter_power_unavailable")
    );
    assert!(
        !bus.writes()
            .iter()
            .any(|(_, path, _)| path == MAX_DISCHARGE_POWER_PATH)
    );

    bus.value(VEBUS_SERVICE, NOMINAL_INVERTER_POWER_PATH, 2_500.0);
    let recovered = controller.run_once();
    assert_eq!(recovered.requested_max_discharge_power, Some(1_000.0));
    assert!(recovered.discharge_protection_active);
    assert!(!recovered.discharge_protection_pending_unenforced);
}

#[test]
fn nominal_cache_is_invalidated_when_the_selected_vebus_service_changes() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 19.0);
    bus.value("system", BATTERY_POWER_PATH, -500.0);
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        config(false),
        cached_nominal_state(
            "com.victronenergy.vebus.replacement",
            "com.victronenergy.vebus.replacement",
            999.0,
        ),
    );

    let decision = controller.run_once();

    assert!(decision.discharge_protection_pending_unenforced);
    assert_eq!(
        decision.discharge_protection_unenforced_reason.as_deref(),
        Some("nominal_inverter_power_unavailable")
    );
    assert!(
        !bus.writes()
            .iter()
            .any(|(_, path, _)| path == MAX_DISCHARGE_POWER_PATH)
    );
    assert_eq!(
        controller.state.vebus_service.as_deref(),
        Some(VEBUS_SERVICE)
    );
    assert_eq!(controller.state.nominal_inverter_power_last, None);
    assert_eq!(controller.state.nominal_inverter_power_service, None);
}

#[test]
fn configured_nominal_power_is_used_only_as_an_explicit_fallback() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 19.0);
    bus.value("system", BATTERY_POWER_PATH, -500.0);
    let mut runtime_config = config(false);
    runtime_config.configured_nominal_inverter_power_w = Some(2_000.0);
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        runtime_config,
        ControllerState {
            last_sample_date: "2026-07-01".to_owned(),
            ..ControllerState::default()
        },
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_max_discharge_power, Some(800.0));
    assert!(decision.discharge_protection_active);
    assert!(controller.state.nominal_inverter_power_configured);
    assert_eq!(controller.state.nominal_inverter_power_service, None);
}

#[test]
fn live_nominal_power_has_priority_over_the_configured_fallback() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 19.0);
    bus.value("system", BATTERY_POWER_PATH, -500.0);
    configure_nominal_inverter_power(&bus, 2_500.0);
    let mut runtime_config = config(false);
    runtime_config.configured_nominal_inverter_power_w = Some(2_000.0);
    let mut controller = Controller::new(
        bus,
        FakeStore::default(),
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        runtime_config,
        ControllerState {
            last_sample_date: "2026-07-01".to_owned(),
            ..ControllerState::default()
        },
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_max_discharge_power, Some(1_000.0));
    assert!(!controller.state.nominal_inverter_power_configured);
    assert_eq!(
        controller.state.nominal_inverter_power_service.as_deref(),
        Some(VEBUS_SERVICE)
    );
}

#[test]
fn missing_max_discharge_setting_is_reported_without_a_speculative_write() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 19.0);
    bus.value("system", BATTERY_POWER_PATH, -500.0);
    configure_nominal_inverter_power(&bus, 2_500.0);
    if let Ok(mut state) = bus.shared.lock() {
        state
            .values
            .remove(&("settings".to_owned(), MAX_DISCHARGE_POWER_PATH.to_owned()));
    }
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        config(false),
        ControllerState {
            last_sample_date: "2026-07-01".to_owned(),
            ..ControllerState::default()
        },
    );

    let decision = controller.run_once();

    assert!(decision.discharge_protection_pending_unenforced);
    assert_eq!(
        decision.discharge_protection_unenforced_reason.as_deref(),
        Some("max_discharge_power_unavailable")
    );
    assert!(
        !bus.writes()
            .iter()
            .any(|(_, path, _)| path == MAX_DISCHARGE_POWER_PATH)
    );

    bus.value("settings", MAX_DISCHARGE_POWER_PATH, -1.0);
    let recovered = controller.run_once();
    assert_eq!(recovered.requested_max_discharge_power, Some(1_000.0));
    assert!(recovered.discharge_protection_active);
    assert!(!recovered.discharge_protection_pending_unenforced);
}

fn pending_restriction_state() -> ControllerState {
    ControllerState {
        last_sample_date: "2026-07-01".to_owned(),
        vebus_service: Some(VEBUS_SERVICE.to_owned()),
        nominal_inverter_power_last: Some(2_500.0),
        nominal_inverter_power_service: Some(VEBUS_SERVICE.to_owned()),
        nominal_inverter_power_observed_at: 1_000.0,
        discharge_protection: DischargeProtectionState {
            active: true,
            recharge_seen: false,
            restore_power_w: None,
            restore_default: true,
            last_set_power_w: None,
            last_observed_power_w: Some(-1.0),
            write_generation: 7,
            pending_write: Some(PendingDischargeWrite {
                generation: 7,
                kind: DischargeWriteKind::Restrict,
                expected_before_w: -1.0,
                intended_w: 1_000.0,
            }),
            ..DischargeProtectionState::default()
        },
        ..ControllerState::default()
    }
}

fn pending_restore_state() -> ControllerState {
    ControllerState {
        last_sample_date: "2026-07-01".to_owned(),
        vebus_service: Some(VEBUS_SERVICE.to_owned()),
        nominal_inverter_power_last: Some(2_500.0),
        nominal_inverter_power_service: Some(VEBUS_SERVICE.to_owned()),
        nominal_inverter_power_observed_at: 1_000.0,
        discharge_protection: DischargeProtectionState {
            active: true,
            recharge_seen: true,
            restore_power_w: None,
            restore_default: true,
            last_set_power_w: Some(1_000.0),
            last_observed_power_w: Some(1_000.0),
            write_generation: 8,
            pending_write: Some(PendingDischargeWrite {
                generation: 8,
                kind: DischargeWriteKind::Restore,
                expected_before_w: 1_000.0,
                intended_w: -1.0,
            }),
            ..DischargeProtectionState::default()
        },
        ..ControllerState::default()
    }
}

#[test]
fn stale_hardware_basis_never_retries_a_pending_discharge_write() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 19.0);
    bus.value("system", BATTERY_POWER_PATH, -500.0);
    let mut state = pending_restriction_state();
    state.nominal_inverter_power_observed_at = 899.0;
    let mut runtime_config = config(false);
    runtime_config.nominal_inverter_power_max_age = Duration::from_secs(100);
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        runtime_config,
        state,
    );

    let decision = controller.run_once();

    assert!(decision.discharge_protection_pending_unenforced);
    assert_eq!(
        decision.discharge_protection_unenforced_reason.as_deref(),
        Some("nominal_inverter_power_unavailable")
    );
    assert!(
        controller
            .state
            .discharge_protection
            .pending_write
            .is_some()
    );
    assert!(
        !bus.writes()
            .iter()
            .any(|(_, path, _)| path == MAX_DISCHARGE_POWER_PATH)
    );
}

#[test]
fn durable_intent_timeout_keeps_discharge_wal_and_retries_only_after_confirmation() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 19.0);
    bus.value("system", BATTERY_POWER_PATH, -500.0);
    configure_nominal_inverter_power(&bus, 2_500.0);
    let store = FakeStore::default();
    store.set_flush_results([false, true]);
    let mut controller = Controller::new(
        bus.clone(),
        store,
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        config(false),
        ControllerState {
            last_sample_date: "2026-07-01".to_owned(),
            ..ControllerState::default()
        },
    );

    let timed_out = controller.run_once();

    assert!(timed_out.discharge_protection_pending_unenforced);
    assert_eq!(
        timed_out.discharge_protection_unenforced_reason.as_deref(),
        Some("durable_intent_unavailable")
    );
    assert!(controller.state.discharge_protection.active);
    assert!(
        controller
            .state
            .discharge_protection
            .pending_write
            .is_some()
    );
    assert!(
        !bus.writes()
            .iter()
            .any(|(_, path, _)| path == MAX_DISCHARGE_POWER_PATH)
    );

    controller.run_once();

    assert_eq!(
        bus.number("settings", MAX_DISCHARGE_POWER_PATH),
        Some(1_000.0)
    );
    assert!(
        controller
            .state
            .discharge_protection
            .pending_write
            .is_none()
    );
}

#[test]
fn obsolete_restriction_is_retained_for_revalidation_when_wal_cleanup_cannot_persist() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 80.0);
    bus.value("system", BATTERY_POWER_PATH, 0.0);
    configure_nominal_inverter_power(&bus, 2_500.0);
    let store = FakeStore::default();
    store.fail_forced_save();
    let mut controller = Controller::new(
        bus.clone(),
        store,
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        config(false),
        pending_restriction_state(),
    );

    let decision = controller.run_once();

    assert!(decision.discharge_protection_pending_unenforced);
    assert_eq!(
        decision.discharge_protection_unenforced_reason.as_deref(),
        Some("durable_intent_unavailable")
    );
    assert!(controller.state.discharge_protection.active);
    assert!(
        controller
            .state
            .discharge_protection
            .pending_write
            .is_some()
    );
    assert_eq!(bus.number("settings", MAX_DISCHARGE_POWER_PATH), Some(-1.0));
    assert!(
        !bus.writes()
            .iter()
            .any(|(_, path, _)| path == MAX_DISCHARGE_POWER_PATH)
    );
}

#[test]
fn obsolete_restriction_timeout_keeps_the_newer_tombstone_in_memory() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 80.0);
    bus.value("system", BATTERY_POWER_PATH, 0.0);
    configure_nominal_inverter_power(&bus, 2_500.0);
    let store = FakeStore::default();
    store.set_flush_results([true, false]);
    let mut controller = Controller::new(
        bus.clone(),
        store,
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        config(false),
        pending_restriction_state(),
    );

    let decision = controller.run_once();

    assert!(decision.discharge_protection_pending_unenforced);
    assert_eq!(
        decision.discharge_protection_unenforced_reason.as_deref(),
        Some("durable_intent_unavailable")
    );
    assert!(!controller.state.discharge_protection.active);
    assert!(
        controller
            .state
            .discharge_protection
            .pending_write
            .is_none()
    );
    assert_eq!(bus.number("settings", MAX_DISCHARGE_POWER_PATH), Some(-1.0));
    assert!(
        !bus.writes()
            .iter()
            .any(|(_, path, _)| path == MAX_DISCHARGE_POWER_PATH)
    );
}

#[test]
fn changed_hardware_basis_replaces_an_old_pending_target_before_writing() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 19.0);
    bus.value("system", BATTERY_POWER_PATH, -500.0);
    configure_nominal_inverter_power(&bus, 2_000.0);
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        config(false),
        pending_restriction_state(),
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_max_discharge_power, Some(800.0));
    assert_eq!(controller.state.nominal_inverter_power_last, Some(2_000.0));
    assert_eq!(controller.state.discharge_protection.pending_write, None);
    assert!(bus.writes().iter().any(|(_, path, value)| {
        path == MAX_DISCHARGE_POWER_PATH && value.to_bits() == 800.0_f64.to_bits()
    }));
    assert!(!bus.writes().iter().any(|(_, path, value)| {
        path == MAX_DISCHARGE_POWER_PATH && value.to_bits() == 1_000.0_f64.to_bits()
    }));
}

#[test]
fn restart_commits_a_discharge_write_already_visible_on_dbus() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 19.0);
    bus.value("system", BATTERY_POWER_PATH, -500.0);
    bus.value("settings", MAX_DISCHARGE_POWER_PATH, 1_000.0);
    configure_nominal_inverter_power(&bus, 2_500.0);
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        config(false),
        pending_restriction_state(),
    );

    controller.run_once();

    assert!(controller.state.discharge_protection.active);
    assert!(controller.state.discharge_protection.restore_default);
    assert_eq!(
        controller.state.discharge_protection.last_set_power_w,
        Some(1_000.0)
    );
    assert_eq!(controller.state.discharge_protection.pending_write, None);
    assert!(
        !bus.writes()
            .iter()
            .any(|(_, path, _)| path == MAX_DISCHARGE_POWER_PATH)
    );
}

#[test]
fn restart_retries_a_discharge_write_not_yet_visible_on_dbus() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 19.0);
    bus.value("system", BATTERY_POWER_PATH, -500.0);
    configure_nominal_inverter_power(&bus, 2_500.0);
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        config(false),
        pending_restriction_state(),
    );

    controller.run_once();

    assert_eq!(
        controller.state.discharge_protection.last_set_power_w,
        Some(1_000.0)
    );
    assert_eq!(controller.state.discharge_protection.pending_write, None);
    assert_eq!(
        bus.writes()
            .iter()
            .filter(|(_, path, value)| {
                path == MAX_DISCHARGE_POWER_PATH && value.to_bits() == 1_000.0_f64.to_bits()
            })
            .count(),
        1
    );
}

#[test]
fn restart_does_not_replay_an_obsolete_restore_while_low_soc_is_discharging() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 19.0);
    bus.value("system", BATTERY_POWER_PATH, -500.0);
    bus.value("settings", MAX_DISCHARGE_POWER_PATH, 1_000.0);
    configure_nominal_inverter_power(&bus, 2_500.0);
    let store = FakeStore::default();
    let mut controller = Controller::new(
        bus.clone(),
        store.clone(),
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        config(false),
        pending_restore_state(),
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_max_discharge_power, None);
    assert!(decision.discharge_protection_active);
    assert_eq!(controller.state.discharge_protection.pending_write, None);
    assert_eq!(
        bus.number("settings", MAX_DISCHARGE_POWER_PATH),
        Some(1_000.0)
    );
    assert!(
        !bus.writes()
            .iter()
            .any(|(_, path, _)| path == MAX_DISCHARGE_POWER_PATH)
    );
    assert_eq!(
        store.latest_state().discharge_protection.pending_write,
        None
    );
}

#[test]
fn applied_restore_is_reconciled_before_current_low_soc_policy_is_reapplied() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 19.0);
    bus.value("system", BATTERY_POWER_PATH, -500.0);
    configure_nominal_inverter_power(&bus, 2_500.0);
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        config(false),
        pending_restore_state(),
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_max_discharge_power, Some(1_000.0));
    assert!(decision.discharge_protection_active);
    assert_eq!(controller.state.discharge_protection.pending_write, None);
    assert_eq!(
        bus.number("settings", MAX_DISCHARGE_POWER_PATH),
        Some(1_000.0)
    );
    assert_eq!(
        bus.writes()
            .iter()
            .filter(|(_, path, value)| {
                path == MAX_DISCHARGE_POWER_PATH && value.to_bits() == 1_000.0_f64.to_bits()
            })
            .count(),
        1
    );
}

#[test]
fn restart_never_overwrites_a_user_value_that_superseded_a_pending_write() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 19.0);
    bus.value("system", BATTERY_POWER_PATH, -500.0);
    bus.value("settings", MAX_DISCHARGE_POWER_PATH, 800.0);
    configure_nominal_inverter_power(&bus, 2_500.0);
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        config(false),
        pending_restriction_state(),
    );

    controller.run_once();

    assert_eq!(
        controller.state.discharge_protection.restore_power_w,
        Some(800.0)
    );
    assert!(!controller.state.discharge_protection.restore_default);
    assert_eq!(controller.state.discharge_protection.pending_write, None);
    assert!(
        !bus.writes()
            .iter()
            .any(|(_, path, _)| path == MAX_DISCHARGE_POWER_PATH)
    );
}

#[test]
fn existing_stricter_gui_discharge_limit_is_preserved() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 19.0);
    bus.value("system", BATTERY_POWER_PATH, -500.0);
    bus.value("settings", MAX_DISCHARGE_POWER_PATH, 800.0);
    configure_nominal_inverter_power(&bus, 2_500.0);
    let state = ControllerState {
        last_sample_date: "2026-07-01".to_owned(),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_max_discharge_power, None);
    assert_eq!(decision.outcome, CycleOutcome::NoChange);
    assert!(controller.state.discharge_protection.active);
    assert_eq!(
        controller.state.discharge_protection.restore_power_w,
        Some(800.0)
    );
    assert!(bus.writes().is_empty());
}

#[test]
fn discharge_limit_is_restored_only_after_charging_above_25_percent() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 19.0);
    bus.value("system", BATTERY_POWER_PATH, -500.0);
    configure_nominal_inverter_power(&bus, 2_500.0);
    let state = ControllerState {
        last_sample_date: "2026-07-01".to_owned(),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        config(false),
        state,
    );
    assert_eq!(
        controller.run_once().requested_max_discharge_power,
        Some(1_000.0)
    );

    bus.value("system", BATTERY_SOC_PATH, 25.0);
    bus.value("system", BATTERY_POWER_PATH, 500.0);
    controller
        .state
        .discharge_protection
        .recharge_candidate_since_ts = Some(880.0);
    controller
        .state
        .discharge_protection
        .recharge_candidate_last_sample_ts = Some(940.0);
    assert_eq!(controller.run_once().requested_max_discharge_power, None);
    assert!(controller.state.discharge_protection.recharge_seen);

    bus.value("system", BATTERY_SOC_PATH, 25.1);
    let release = controller.run_once();
    assert_eq!(release.requested_max_discharge_power, Some(-1.0));
    assert!(!controller.state.discharge_protection.active);
    assert!(bus.writes().iter().any(|(_, path, value)| {
        path == MAX_DISCHARGE_POWER_PATH && value.to_bits() == (-1.0_f64).to_bits()
    }));
}

#[test]
fn invalid_min_soc_blocks_every_control_write_in_the_cycle() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 19.0);
    bus.value("system", BATTERY_POWER_PATH, -500.0);
    configure_nominal_inverter_power(&bus, 2_500.0);
    if let Ok(mut state) = bus.shared.lock() {
        state
            .values
            .remove(&("settings".to_owned(), MIN_SOC_PATH.to_owned()));
    }
    let state = ControllerState {
        last_sample_date: "2026-07-01".to_owned(),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        config(false),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.outcome, CycleOutcome::MissingMinSoc);
    assert_eq!(decision.requested_max_discharge_power, None);
    assert!(bus.writes().is_empty());
}

#[test]
fn discharge_protection_shadow_mode_reports_but_does_not_write() {
    let bus = base_bus();
    bus.value("system", BATTERY_SOC_PATH, 19.0);
    bus.value("system", BATTERY_POWER_PATH, -500.0);
    configure_nominal_inverter_power(&bus, 2_500.0);
    let state = ControllerState {
        last_sample_date: "2026-07-01".to_owned(),
        ..ControllerState::default()
    };
    let mut controller = Controller::new(
        bus.clone(),
        FakeStore::default(),
        clock(7, 1, 12, 1_000.0),
        FakeLog::default(),
        config(true),
        state,
    );

    let decision = controller.run_once();

    assert_eq!(decision.requested_max_discharge_power, Some(1_000.0));
    assert!(bus.writes().is_empty());
}

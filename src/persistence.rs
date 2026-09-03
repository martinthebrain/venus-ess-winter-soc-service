//! RAM-first state persistence with a bounded coalescing SD writer.

use crate::clock::{LocalDateTime, system_epoch_seconds};
use crate::config::{RuntimeConfig, SD_DIR_NAME};
use crate::domain::{ControllerState, PV_CHANNEL_MASK};
use crate::policy::{is_sd_window, is_transition_mmdd, is_winter_mmdd};
use crate::ports::StatePort;
use crate::storage::{
    RemovableMediumIdentity, SdLocation, atomic_write, atomic_write_removable, locate_sd,
    read_bounded,
};
use serde_json::{Map, Value};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const BASE_KEYS: [&str; 15] = [
    "max_charge_current_raw",
    "max_charge_current_raw_set",
    "charge_current_owned_by_script",
    "max_charge_current_script_last_set",
    "reserve_charge_current_write_generation",
    "reserve_charge_current_pending_write",
    "charge_current_ceiling",
    "charge_current_control",
    "minimum_soc_control",
    "discharge_protection",
    "vebus_service",
    "nominal_inverter_power_last",
    "nominal_inverter_power_service",
    "nominal_inverter_power_observed_at",
    "nominal_inverter_power_configured",
];
const PV_KEYS: [&str; 3] = ["pv_history", "last_sample_date", "pv_expected_channels"];
const WINTER_KEYS: [&str; 2] = ["last_balance_ts", "last_full_ts"];
const STATE_SCHEMA_VERSION: u64 = 1;
const STATE_SCHEMA_KEY: &str = "schema_version";
const STATE_DEVICE_KEY: &str = "device_id";
const STATE_DURABLE_GENERATION_KEY: &str = "durable_generation";
const MAX_DURABLE_GENERATION: u64 = 1_000_000_000_000;
const STATE_FUTURE_TOLERANCE_SECONDS: f64 = 300.0;
const MAX_PERSISTED_CURRENT_A: f64 = 10_000.0;
const MAX_PERSISTED_POWER_W: f64 = 1_000_000.0;
const MAX_PERSISTED_PV_POWER_W: f64 = 1_000_000.0;
const MAX_PERSISTED_PV_ENERGY_WS: f64 = 100_000_000_000_000.0;
const MAX_PERSISTED_DURATION_SECONDS: f64 = 1_000_000_000.0;

#[derive(Debug)]
pub struct StateRepository {
    config: RuntimeConfig,
    sd_location: Option<SdLocation>,
    sd_medium_identity: Option<RemovableMediumIdentity>,
    sd_state_file: Option<PathBuf>,
    sd_last_lookup_ts: f64,
    sd_imported_medium: Option<RemovableMediumIdentity>,
    durable_generation: u64,
    writer: SdWriter,
    recovery_warnings: Vec<String>,
}

impl StateRepository {
    /// Create the RAM-first repository and its bounded SD writer.
    ///
    /// # Errors
    ///
    /// Returns an error when the operating system cannot create the writer
    /// thread. The service must not silently claim durable checkpoints in that
    /// condition.
    pub fn new(config: RuntimeConfig) -> Result<Self, String> {
        let writer = SdWriter::spawn(config.sd_save_interval, config.sd_backoff_max)?;
        Ok(Self {
            config,
            sd_location: None,
            sd_medium_identity: None,
            sd_state_file: None,
            sd_last_lookup_ts: 0.0,
            sd_imported_medium: None,
            durable_generation: 0,
            writer,
            recovery_warnings: Vec::new(),
        })
    }

    /// Restore a validated, versioned controller state from RAM and, seasonally, SD.
    ///
    /// Corruption in one source is quarantined independently so a valid source
    /// remains usable.
    ///
    /// # Errors
    ///
    /// Returns an error only when an otherwise valid merged state cannot be
    /// represented by the typed controller schema.
    pub fn initialize(
        &mut self,
        now: LocalDateTime,
        now_ts: f64,
        monotonic_now: f64,
    ) -> Result<ControllerState, String> {
        self.refresh_sd(now_ts, true);
        let state_file = self.config.state_file.clone();
        let mut ram_value = self.read_source(&state_file, now_ts, false);
        if ram_value.is_none() {
            ram_value = self.read_legacy_ram_state(now_ts);
        }
        let mut merged = ram_value
            .clone()
            .unwrap_or_else(|| Value::Object(Map::new()));
        let mut durable_value = None;
        let mut include_seasonal = false;
        let mut durable_file = None;
        if !self.config.shadow {
            include_seasonal =
                self.sd_state_file.is_some() && is_sd_window(&self.config.policy, now.mmdd());
            let path = self.durable_state_file();
            durable_value = self.read_source(&path, now_ts, false);
            durable_file = Some(path.clone());
            self.durable_generation = ram_value
                .as_ref()
                .map_or(0, value_generation)
                .max(durable_value.as_ref().map_or(0, value_generation));
            if let Some(durable) = durable_value.as_ref() {
                let medium_identity = self.medium_identity_for_path(&path);
                self.writer.seed(
                    path,
                    medium_identity,
                    sd_signature(
                        &self.config.policy,
                        durable,
                        now.mmdd(),
                        include_seasonal,
                        &self.config.state_device_id,
                    ),
                    value_ts(durable),
                    value_generation(durable),
                );
            }
            if should_sd_override(durable_value.as_ref(), ram_value.as_ref()) {
                merge_sd_subset(
                    &self.config.policy,
                    &mut merged,
                    durable_value.as_ref(),
                    now.mmdd(),
                    include_seasonal,
                );
            }
        }
        self.sd_imported_medium = (!self.config.shadow
            && self.sd_state_file.is_some()
            && is_sd_window(&self.config.policy, now.mmdd()))
        .then(|| self.sd_medium_identity.clone())
        .flatten();
        let mut state = match serde_json::from_value::<ControllerState>(merged) {
            Ok(state) => state,
            Err(merged_error) => {
                self.recovery_warnings.push(format!(
                    "merged state is incompatible; retaining RAM state: {merged_error}"
                ));
                ram_value.as_ref().map_or_else(
                    || Ok(ControllerState::default()),
                    |ram| {
                        serde_json::from_value(ram.clone())
                            .map_err(|ram_error| ram_error.to_string())
                    },
                )?
            }
        };
        if state.migrate_legacy_charge_current_state()? {
            self.recovery_warnings.push(
                "legacy MaxChargeCurrent ownership migrated to the unified arbiter".to_owned(),
            );
        }
        state.reset_runtime_for_boot(monotonic_now);
        if let (Some(path), Some(durable)) = (durable_file, durable_value.as_ref()) {
            self.reconcile_newer_ram_restore_state(
                &mut state,
                ram_value.as_ref(),
                durable,
                &path,
                (now, now_ts),
                include_seasonal,
            )?;
        }
        Ok(state)
    }

    fn reconcile_newer_ram_restore_state(
        &mut self,
        state: &mut ControllerState,
        ram: Option<&Value>,
        durable: &Value,
        path: &Path,
        time: (LocalDateTime, f64),
        include_seasonal: bool,
    ) -> Result<(), String> {
        let (now, now_ts) = time;
        let durable_signature = sd_signature(
            &self.config.policy,
            durable,
            now.mmdd(),
            include_seasonal,
            &self.config.state_device_id,
        );
        let current_signature = state_signature(
            &self.config.policy,
            state,
            now.mmdd(),
            include_seasonal,
            &self.config.state_device_id,
        )?;
        let restore_state_differs = durable_signature != current_signature
            && (signature_has_restore_state(&durable_signature)
                || signature_has_restore_state(&current_signature));
        if !restore_state_differs || should_sd_override(Some(durable), ram) {
            return Ok(());
        }
        if let Err(error) = self.save(state, now, now_ts, true) {
            self.recovery_warnings.push(format!(
                "newer RAM restore state could not supersede {}: {error}",
                path.display()
            ));
        } else if !self.flush(Duration::from_secs(5)) {
            self.recovery_warnings.push(format!(
                "newer RAM restore state is still pending for {}",
                path.display()
            ));
        }
        Ok(())
    }

    pub fn take_recovery_warnings(&mut self) -> Vec<String> {
        std::mem::take(&mut self.recovery_warnings)
    }

    fn refresh_window_inner(
        &mut self,
        state: &mut ControllerState,
        now: LocalDateTime,
        now_ts: f64,
    ) -> Result<bool, String> {
        let active_now = !self.config.shadow && is_sd_window(&self.config.policy, now.mmdd());
        if !active_now {
            self.sd_imported_medium = None;
            return Ok(false);
        }
        self.refresh_sd(now_ts, false);
        let Some(medium) = self.sd_medium_identity.clone() else {
            self.sd_imported_medium = None;
            return Ok(false);
        };
        if self.sd_imported_medium.as_ref() == Some(&medium) {
            return Ok(false);
        }
        let Some(path) = self.sd_state_file.clone() else {
            return Ok(false);
        };
        let Some(sd) = self.read_source(&path, now_ts, false) else {
            self.writer.reset_seed();
            self.sd_imported_medium = Some(medium);
            return Ok(false);
        };
        let ram_generation = self.durable_generation;
        let sd_generation = value_generation(&sd);
        let medium_identity = self.medium_identity_for_path(&path);
        self.writer.seed(
            path.clone(),
            medium_identity,
            sd_signature(
                &self.config.policy,
                &sd,
                now.mmdd(),
                true,
                &self.config.state_device_id,
            ),
            value_ts(&sd),
            sd_generation,
        );
        let ram = state_value_with_generation(state, &self.config.state_device_id, ram_generation)?;
        self.durable_generation = ram_generation.max(sd_generation);
        if !should_sd_override(Some(&sd), Some(&ram)) {
            self.sd_imported_medium = Some(medium);
            let durable_signature = sd_signature(
                &self.config.policy,
                &sd,
                now.mmdd(),
                true,
                &self.config.state_device_id,
            );
            let current_signature = state_signature(
                &self.config.policy,
                state,
                now.mmdd(),
                true,
                &self.config.state_device_id,
            )?;
            if durable_signature != current_signature
                && (signature_has_restore_state(&durable_signature)
                    || signature_has_restore_state(&current_signature))
            {
                self.save(state, now, now_ts, true)?;
                if !self.flush(Duration::from_secs(5)) {
                    self.recovery_warnings.push(format!(
                        "newer RAM restore state is still pending for {}",
                        path.display()
                    ));
                }
            }
            return Ok(false);
        }
        let mut merged = ram;
        merge_sd_subset(
            &self.config.policy,
            &mut merged,
            Some(&sd),
            now.mmdd(),
            true,
        );
        match serde_json::from_value::<ControllerState>(merged) {
            Ok(mut loaded) => {
                if loaded.migrate_legacy_charge_current_state()? {
                    self.recovery_warnings.push(
                        "legacy MaxChargeCurrent ownership migrated from removable storage"
                            .to_owned(),
                    );
                }
                *state = loaded;
            }
            Err(error) => {
                self.recovery_warnings
                    .push(format!("SD state is incompatible and was ignored: {error}"));
                self.writer.reset_seed();
                self.sd_imported_medium = Some(medium);
                return Ok(false);
            }
        }
        self.sd_imported_medium = Some(medium);
        Ok(true)
    }

    /// Save full state to RAM and enqueue the bounded durable subset.
    ///
    /// # Errors
    ///
    /// Returns an error when state encoding or the atomic RAM write fails.
    pub fn save(
        &mut self,
        state: &mut ControllerState,
        now: LocalDateTime,
        now_ts: f64,
        force_sd: bool,
    ) -> Result<(), String> {
        if !self.config.shadow && is_sd_window(&self.config.policy, now.mmdd()) {
            self.refresh_window_inner(state, now, now_ts)?;
        }
        state.ts = now_ts;
        if !self.config.shadow {
            self.refresh_sd(now_ts, false);
        }
        let include_seasonal = !self.config.shadow
            && self.sd_state_file.is_some()
            && is_sd_window(&self.config.policy, now.mmdd());
        let mut durable_write = None;
        if !self.config.shadow {
            let path = self.durable_state_file();
            let medium_identity = self.medium_identity_for_path(&path);
            let restore_active = has_gui_restore_state(state);
            let clears_restore = self
                .writer
                .has_restore_record(&path, medium_identity.as_ref())
                && !restore_active;
            if include_seasonal || restore_active || clears_restore {
                let signature = state_signature(
                    &self.config.policy,
                    state,
                    now.mmdd(),
                    include_seasonal,
                    &self.config.state_device_id,
                )?;
                let urgent = force_sd || clears_restore;
                if !self.writer.should_skip(
                    &path,
                    medium_identity.as_ref(),
                    &signature,
                    now_ts,
                    urgent,
                ) {
                    let generation = self
                        .durable_generation
                        .checked_add(1)
                        .filter(|generation| *generation <= MAX_DURABLE_GENERATION)
                        .ok_or_else(|| "durable state generation exhausted".to_owned())?;
                    durable_write = Some((path, medium_identity, signature, generation, urgent));
                }
            }
        }

        let generation = durable_write
            .as_ref()
            .map_or(self.durable_generation, |(_, _, _, generation, _)| {
                *generation
            });
        let value = state_value_with_generation(state, &self.config.state_device_id, generation)?;
        let bytes = serde_json::to_vec(&value).map_err(|error| error.to_string())?;
        atomic_write(&self.config.state_file, &bytes, false).map_err(|error| error.to_string())?;

        if let Some((path, medium_identity, signature, generation, urgent)) = durable_write {
            let mut payload = signature.clone();
            payload.insert("ts".to_owned(), Value::from(now_ts));
            payload.insert(
                STATE_DURABLE_GENERATION_KEY.to_owned(),
                Value::from(generation),
            );
            let payload =
                serde_json::to_vec(&Value::Object(payload)).map_err(|error| error.to_string())?;
            self.durable_generation = generation;
            self.writer.enqueue(WriteRequest {
                directory: path
                    .parent()
                    .unwrap_or_else(|| Path::new("."))
                    .to_path_buf(),
                path,
                payload,
                signature,
                medium_identity,
                generation,
                fsync: urgent,
            });
        }
        Ok(())
    }

    #[must_use]
    pub fn flush(&self, timeout: Duration) -> bool {
        self.writer.flush(timeout)
    }

    #[must_use]
    pub fn status(&self, now_ts: f64) -> String {
        if self.sd_state_file.is_none() {
            return "SD missing".to_owned();
        }
        let snapshot = self.writer.snapshot();
        if now_ts < snapshot.next_try_ts {
            return format!("SD backoff {:.0}s", snapshot.next_try_ts - now_ts);
        }
        if snapshot.error_count > 0 {
            return format!("SD errors {}", snapshot.error_count);
        }
        "SD ok".to_owned()
    }

    #[must_use]
    pub fn sd_description(&self) -> Option<&str> {
        self.sd_location
            .as_ref()
            .map(|location| location.description.as_str())
    }

    fn refresh_sd(&mut self, now_ts: f64, force: bool) {
        if !force && now_ts - self.sd_last_lookup_ts < self.config.sd_lookup_interval.as_secs_f64()
        {
            return;
        }
        self.sd_last_lookup_ts = now_ts;
        let location = locate_sd(&self.config);
        let medium_identity = location.as_ref().map(|location| location.identity.clone());
        if medium_identity != self.sd_medium_identity {
            self.sd_imported_medium = None;
            self.writer.reset_seed();
        }
        self.sd_location = location;
        self.sd_medium_identity = medium_identity;
        self.sd_state_file = self.sd_location.as_ref().map(|location| {
            location
                .path
                .join(SD_DIR_NAME)
                .join("ess_winter_logic.json")
        });
    }

    fn durable_state_file(&self) -> PathBuf {
        self.sd_state_file
            .clone()
            .unwrap_or_else(|| self.config.durable_restore_file.clone())
    }

    fn medium_identity_for_path(&self, path: &Path) -> Option<RemovableMediumIdentity> {
        (self.sd_state_file.as_deref() == Some(path))
            .then(|| self.sd_medium_identity.clone())
            .flatten()
    }

    fn read_source(&mut self, path: &Path, now_ts: f64, allow_legacy: bool) -> Option<Value> {
        match read_state_value(path, &self.config.state_device_id, now_ts, allow_legacy) {
            Ok(value) => value,
            Err(error) => {
                self.recovery_warnings.push(error);
                None
            }
        }
    }

    fn read_legacy_ram_state(&mut self, now_ts: f64) -> Option<Value> {
        let legacy_path = self.config.legacy_state_file.clone()?;
        let legacy = self.read_source(&legacy_path, now_ts, true)?;
        self.recovery_warnings.push(format!(
            "validated legacy RAM state imported from {}",
            legacy_path.display()
        ));
        Some(legacy)
    }
}

impl StatePort for StateRepository {
    fn refresh_window(
        &mut self,
        state: &mut ControllerState,
        now: LocalDateTime,
        now_ts: f64,
    ) -> Result<bool, String> {
        self.refresh_window_inner(state, now, now_ts)
    }

    fn save(
        &mut self,
        state: &mut ControllerState,
        now: LocalDateTime,
        now_ts: f64,
        force_sd: bool,
    ) -> Result<(), String> {
        Self::save(self, state, now, now_ts, force_sd)
    }

    fn flush(&self, timeout: Duration) -> bool {
        Self::flush(self, timeout)
    }

    fn status(&self, now_ts: f64) -> String {
        Self::status(self, now_ts)
    }

    fn sd_description(&self) -> Option<&str> {
        Self::sd_description(self)
    }
}

fn read_json_value(path: &Path) -> Result<Option<Value>, String> {
    let bytes = read_bounded(path).map_err(|error| error.to_string())?;
    let Some(bytes) = bytes else {
        return Ok(None);
    };
    match serde_json::from_slice::<Value>(&bytes) {
        Ok(Value::Object(map)) => Ok(Some(Value::Object(map))),
        Ok(_) => {
            quarantine_bad_file(path);
            Err(format!(
                "state file is not a JSON object: {}",
                path.display()
            ))
        }
        Err(error) => {
            quarantine_bad_file(path);
            Err(format!(
                "state file is unreadable: {}: {error}",
                path.display()
            ))
        }
    }
}

fn read_state_value(
    path: &Path,
    expected_device_id: &str,
    now_ts: f64,
    allow_legacy: bool,
) -> Result<Option<Value>, String> {
    let Some(value) = read_json_value(path)? else {
        return Ok(None);
    };
    validate_state_value(&value, expected_device_id, now_ts, allow_legacy)
        .map_err(|error| format!("state file rejected: {}: {error}", path.display()))?;
    Ok(Some(value))
}

fn validate_state_value(
    value: &Value,
    expected_device_id: &str,
    now_ts: f64,
    allow_legacy: bool,
) -> Result<(), String> {
    let object = value
        .as_object()
        .ok_or_else(|| "state must be a JSON object".to_owned())?;
    let schema = object.get(STATE_SCHEMA_KEY).and_then(Value::as_u64);
    let device = object.get(STATE_DEVICE_KEY).and_then(Value::as_str);
    if schema.is_none() && device.is_none() && allow_legacy {
        // Legacy import is restricted by secure owner/type checks and the same
        // semantic validation as the current schema.
    } else {
        if schema != Some(STATE_SCHEMA_VERSION) {
            return Err(format!(
                "unsupported schema version; expected {STATE_SCHEMA_VERSION}"
            ));
        }
        if device != Some(expected_device_id) {
            return Err("state belongs to another device".to_owned());
        }
    }
    if let Some(generation) = object.get(STATE_DURABLE_GENERATION_KEY) {
        if generation
            .as_u64()
            .is_none_or(|generation| generation > MAX_DURABLE_GENERATION)
        {
            return Err("durable state generation is invalid".to_owned());
        }
    }
    let timestamp = object
        .get("ts")
        .and_then(Value::as_f64)
        .ok_or_else(|| "state timestamp is missing or invalid".to_owned())?;
    validate_epoch_timestamp("ts", timestamp, now_ts)?;
    let mut state = serde_json::from_value::<ControllerState>(value.clone())
        .map_err(|error| format!("state schema is invalid: {error}"))?;
    validate_charge_state(&state)?;
    validate_charge_current_ceiling_state(&state)?;
    state.migrate_legacy_charge_current_state()?;
    validate_controller_state(&state, now_ts)
}

fn validate_controller_state(state: &ControllerState, now_ts: f64) -> Result<(), String> {
    validate_persisted_times(state, now_ts)?;
    validate_balancing_state(state)?;
    validate_charge_state(state)?;
    validate_pv_state(state)?;
    validate_identity_state(state)?;
    validate_charge_current_control_state(state)?;
    validate_minimum_soc_control_state(state)?;
    validate_discharge_state(state)
}

fn validate_minimum_soc_control_state(state: &ControllerState) -> Result<(), String> {
    let control = &state.minimum_soc_control;
    validate_optional_range(
        "minimum_soc_control.external_baseline",
        control.external_baseline,
        0.0,
        100.0,
    )?;
    validate_optional_range("minimum_soc_control.last_set", control.last_set, 0.0, 100.0)?;
    if !control.owned {
        if control.external_baseline.is_some()
            || control.last_set.is_some()
            || control.pending_write.is_some()
        {
            return Err("unowned minimum-SoC control retains ownership state".to_owned());
        }
        return Ok(());
    }
    let baseline = control
        .external_baseline
        .ok_or_else(|| "owned minimum-SoC control has no external baseline".to_owned())?;
    if let Some(pending) = control.pending_write {
        if pending.generation == 0 || pending.generation != control.write_generation {
            return Err("pending minimum-SoC write has an invalid generation binding".to_owned());
        }
        validate_range(
            "minimum_soc_control.pending_write.expected_before",
            pending.expected_before,
            0.0,
            100.0,
        )?;
        validate_range(
            "minimum_soc_control.pending_write.intended",
            pending.intended,
            0.0,
            100.0,
        )?;
        if (pending.expected_before - pending.intended).abs() <= f64::EPSILON {
            return Err("pending minimum-SoC write changes no value".to_owned());
        }
        let expected_before = control.last_set.unwrap_or(baseline);
        if (pending.expected_before - expected_before).abs() > f64::EPSILON {
            return Err("pending minimum-SoC write does not follow the owned value".to_owned());
        }
        if matches!(pending.kind, crate::domain::MinimumSocWriteKind::Restore)
            && (pending.intended - baseline).abs() > f64::EPSILON
        {
            return Err("pending minimum-SoC restore differs from its baseline".to_owned());
        }
    } else if control.last_set.is_none() {
        return Err("owned minimum-SoC control has no confirmed write".to_owned());
    }
    Ok(())
}

fn validate_balancing_state(state: &ControllerState) -> Result<(), String> {
    if state.balancing_active && state.balancing_start_ts <= 0.0 {
        return Err("active balancing has no approach start timestamp".to_owned());
    }
    if state.balancing_high_soc_start_ts > 0.0
        && (!state.balancing_active
            || state.balancing_start_ts <= 0.0
            || state.balancing_high_soc_start_ts < state.balancing_start_ts)
    {
        return Err("high-SoC balancing timestamp has no valid active approach".to_owned());
    }
    Ok(())
}

fn validate_charge_current_control_state(state: &ControllerState) -> Result<(), String> {
    let control = &state.charge_current_control;
    validate_optional_integer_current(
        "charge_current_control.external_baseline_a",
        control.external_baseline_a,
    )?;
    for (name, value) in [
        (
            "charge_current_control.configured_constraint_a",
            control.configured_constraint_a,
        ),
        (
            "charge_current_control.reserve_constraint_a",
            control.reserve_constraint_a,
        ),
    ] {
        validate_optional_nonnegative_integer_current(name, value)?;
    }
    validate_optional_integer_current(
        "charge_current_control.last_effectively_written_a",
        control.last_effectively_written_a,
    )?;

    if !control.owned {
        if control.external_baseline_a.is_some()
            || control.last_effectively_written_a.is_some()
            || control.pending_write.is_some()
        {
            return Err("unowned charge-current control retains ownership state".to_owned());
        }
        return Ok(());
    }
    if control.external_baseline_a.is_none() {
        return Err("owned charge-current control has no external baseline".to_owned());
    }
    if control.routine_external_control_latched {
        return Err("owned charge-current control cannot be externally latched".to_owned());
    }
    if let Some(pending) = control.pending_write {
        if pending.generation == 0 || pending.generation != control.write_generation {
            return Err(
                "pending unified charge-current write has an invalid generation binding".to_owned(),
            );
        }
        validate_integer_current(
            "charge_current_control.pending_write.expected_before_a",
            pending.expected_before_a,
        )?;
        validate_integer_current(
            "charge_current_control.pending_write.intended_a",
            pending.intended_a,
        )?;
        if current_equal(pending.expected_before_a, pending.intended_a) {
            return Err("pending unified charge-current write changes no value".to_owned());
        }
        if let Some(last) = control.last_effectively_written_a {
            if !current_equal(last, pending.expected_before_a) {
                return Err(
                    "pending unified charge-current write does not follow the last confirmed value"
                        .to_owned(),
                );
            }
        } else if control.external_baseline_a != Some(pending.expected_before_a) {
            return Err(
                "first pending unified charge-current write does not follow its baseline"
                    .to_owned(),
            );
        }
        match pending.kind {
            crate::domain::ChargeCurrentWriteKind::Restrict => {
                let target =
                    crate::charge_current_control::effective_target(control).ok_or_else(|| {
                        "pending unified restriction has no active constraint".to_owned()
                    })?;
                if pending.intended_a < 0.0 || !current_equal(pending.intended_a, target) {
                    return Err(
                        "pending unified restriction differs from its effective target".to_owned(),
                    );
                }
            }
            crate::domain::ChargeCurrentWriteKind::Restore => {
                if crate::charge_current_control::effective_constraint(control).is_some()
                    || control.external_baseline_a != Some(pending.intended_a)
                {
                    return Err(
                        "pending unified restoration conflicts with active control".to_owned()
                    );
                }
            }
        }
    } else if control.last_effectively_written_a.is_none() {
        return Err("owned charge-current control has no confirmed write".to_owned());
    }
    Ok(())
}

fn validate_charge_current_ceiling_state(state: &ControllerState) -> Result<(), String> {
    let ceiling = &state.charge_current_ceiling;
    validate_optional_integer_current(
        "charge_current_ceiling.restore_current_a",
        ceiling.restore_current_a,
    )?;
    validate_optional_integer_current(
        "charge_current_ceiling.last_set_current_a",
        ceiling.last_set_current_a,
    )?;
    if !ceiling.active {
        if ceiling.restore_current_a.is_some()
            || ceiling.last_set_current_a.is_some()
            || ceiling.pending_write.is_some()
        {
            return Err(
                "inactive charge-current ceiling cannot retain owned restore state".to_owned(),
            );
        }
        return Ok(());
    }
    if ceiling.external_control_latched {
        return Err("owned charge-current ceiling cannot be externally latched".to_owned());
    }
    if ceiling.restore_current_a.is_none() {
        return Err("owned charge-current ceiling has no restore value".to_owned());
    }
    if let Some(pending) = ceiling.pending_write {
        if pending.generation == 0 || pending.generation != ceiling.write_generation {
            return Err(
                "pending charge-current write has an invalid generation binding".to_owned(),
            );
        }
        validate_integer_current(
            "charge_current_ceiling.pending_write.expected_before_a",
            pending.expected_before_a,
        )?;
        validate_integer_current(
            "charge_current_ceiling.pending_write.intended_a",
            pending.intended_a,
        )?;
        if current_equal(pending.expected_before_a, pending.intended_a) {
            return Err("pending charge-current write does not change the setting".to_owned());
        }
        match pending.kind {
            crate::domain::ChargeCurrentWriteKind::Restrict => {
                if pending.intended_a != 0.0 || ceiling.last_set_current_a.is_some() {
                    return Err("pending charge restriction has inconsistent ownership".to_owned());
                }
            }
            crate::domain::ChargeCurrentWriteKind::Restore => {
                if Some(pending.intended_a) != ceiling.restore_current_a
                    || ceiling.last_set_current_a != Some(0.0)
                {
                    return Err("pending charge restoration has inconsistent ownership".to_owned());
                }
            }
        }
    } else if ceiling.last_set_current_a != Some(0.0) {
        return Err("owned charge-current ceiling has no confirmed 0A write".to_owned());
    }
    Ok(())
}

const fn current_equal(left: f64, right: f64) -> bool {
    left.to_bits() == right.to_bits()
}

fn validate_optional_integer_current(name: &str, value: Option<f64>) -> Result<(), String> {
    value.map_or(Ok(()), |current| validate_integer_current(name, current))
}

fn validate_optional_nonnegative_integer_current(
    name: &str,
    value: Option<f64>,
) -> Result<(), String> {
    value.map_or(Ok(()), |current| {
        validate_range(name, current, 0.0, MAX_PERSISTED_CURRENT_A)?;
        if current.fract() == 0.0 {
            Ok(())
        } else {
            Err(format!("{name} must be a whole-amp setting"))
        }
    })
}

fn validate_integer_current(name: &str, value: f64) -> Result<(), String> {
    validate_range(name, value, -1.0, MAX_PERSISTED_CURRENT_A)?;
    if value.fract() == 0.0 {
        Ok(())
    } else {
        Err(format!("{name} must be a whole-amp setting"))
    }
}

fn validate_persisted_times(state: &ControllerState, now_ts: f64) -> Result<(), String> {
    validate_epoch_timestamp("last_balance_ts", state.last_balance_ts, now_ts)?;
    validate_epoch_timestamp("last_full_ts", state.last_full_ts, now_ts)?;
    for (name, value) in [
        ("last_balance_attempt_ts", state.last_balance_attempt_ts),
        ("balancing_start_ts", state.balancing_start_ts),
        (
            "balancing_high_soc_start_ts",
            state.balancing_high_soc_start_ts,
        ),
        ("balance_full_seconds", state.balance_full_seconds),
        ("full_soc_seconds", state.full_soc_seconds),
        ("charge_deficit_start_ts", state.charge_deficit_start_ts),
        (
            "battery_max_current_last_seen_ts",
            state.battery_max_current_last_seen_ts,
        ),
        ("pv_time_s", state.pv_time_s),
        ("pv_last_sample_ts", state.pv_last_sample_ts),
        ("last_pv_integral_ts", state.last_pv_integral_ts),
        ("last_loop_ts", state.last_loop_ts),
        ("last_status_log_ts", state.last_status_log_ts),
        ("last_soc_invalid_log_ts", state.last_soc_invalid_log_ts),
        (
            "last_min_soc_invalid_log_ts",
            state.last_min_soc_invalid_log_ts,
        ),
        ("manual_override_until_ts", state.manual_override_until_ts),
        (
            "min_soc_last_script_set_ts",
            state.min_soc_last_script_set_ts,
        ),
        (
            "last_manual_override_log_ts",
            state.last_manual_override_log_ts,
        ),
        ("boot_ts", state.boot_ts),
    ] {
        validate_range(name, value, 0.0, MAX_PERSISTED_DURATION_SECONDS)?;
    }
    Ok(())
}

fn validate_charge_state(state: &ControllerState) -> Result<(), String> {
    validate_optional_range(
        "battery_max_current_last",
        state.battery_max_current_last,
        0.0,
        MAX_PERSISTED_CURRENT_A,
    )?;
    validate_optional_range(
        "normal_charge_current",
        state.normal_charge_current,
        0.0,
        MAX_PERSISTED_CURRENT_A,
    )?;
    validate_optional_range(
        "max_charge_current_raw",
        state.max_charge_current_raw,
        0.0,
        MAX_PERSISTED_CURRENT_A,
    )?;
    validate_optional_range(
        "max_charge_current_script_last_set",
        state.max_charge_current_script_last_set,
        0.0,
        MAX_PERSISTED_CURRENT_A,
    )?;
    for (name, value) in [
        ("min_soc_last_seen", state.min_soc_last_seen),
        ("min_soc_last_script_set", state.min_soc_last_script_set),
    ] {
        validate_optional_range(name, value, 0.0, 100.0)?;
    }
    validate_optional_range(
        "nominal_inverter_power_last",
        state.nominal_inverter_power_last,
        f64::EPSILON,
        MAX_PERSISTED_POWER_W,
    )?;
    if state.charge_current_owned_by_script && state.max_charge_current_script_last_set.is_none() {
        return Err("owned charge-current state has no last written value".to_owned());
    }
    if let Some(pending) = state.reserve_charge_current_pending_write {
        if pending.generation == 0
            || pending.generation != state.reserve_charge_current_write_generation
        {
            return Err(
                "pending reserve charge-current write has an invalid generation binding".to_owned(),
            );
        }
        validate_integer_current(
            "reserve_charge_current_pending_write.expected_before_a",
            pending.expected_before_a,
        )?;
        validate_integer_current(
            "reserve_charge_current_pending_write.intended_a",
            pending.intended_a,
        )?;
        if current_equal(pending.expected_before_a, pending.intended_a) {
            return Err(
                "pending reserve charge-current write does not change the setting".to_owned(),
            );
        }
        match pending.kind {
            crate::domain::ReserveChargeCurrentWriteKind::Limit => {
                if !state.max_charge_current_raw_set {
                    return Err(
                        "pending reserve charge-current limit has no restore value".to_owned()
                    );
                }
                if pending.intended_a < 0.0 {
                    return Err(
                        "pending reserve charge-current limit cannot request the default setting"
                            .to_owned(),
                    );
                }
                if state.charge_current_owned_by_script
                    && state.max_charge_current_script_last_set != Some(pending.expected_before_a)
                {
                    return Err(
                        "pending reserve charge-current update has inconsistent ownership"
                            .to_owned(),
                    );
                }
            }
            crate::domain::ReserveChargeCurrentWriteKind::Restore => {
                if !state.max_charge_current_raw_set
                    || !state.charge_current_owned_by_script
                    || state.max_charge_current_script_last_set != Some(pending.expected_before_a)
                    || !current_equal(
                        pending.intended_a,
                        state.max_charge_current_raw.unwrap_or(-1.0),
                    )
                {
                    return Err(
                        "pending reserve charge-current restoration has inconsistent ownership"
                            .to_owned(),
                    );
                }
            }
        }
    }
    Ok(())
}

fn validate_pv_state(state: &ControllerState) -> Result<(), String> {
    if state.pv_expected_channels & !PV_CHANNEL_MASK != 0 {
        return Err("PV channel topology contains unknown channels".to_owned());
    }
    for value in state.pv_history.iter().chain(&state.current_day_samples) {
        validate_range("PV sample", *value, 0.0, MAX_PERSISTED_PV_POWER_W)?;
    }
    validate_range(
        "pv_last_sample_power",
        state.pv_last_sample_power,
        0.0,
        MAX_PERSISTED_PV_POWER_W,
    )?;
    validate_range(
        "pv_energy_ws",
        state.pv_energy_ws,
        0.0,
        MAX_PERSISTED_PV_ENERGY_WS,
    )?;
    Ok(())
}

fn validate_identity_state(state: &ControllerState) -> Result<(), String> {
    validate_service_name("battery_service", state.battery_service.as_deref())?;
    validate_service_name("vebus_service", state.vebus_service.as_deref())?;
    validate_service_name(
        "nominal_inverter_power_service",
        state.nominal_inverter_power_service.as_deref(),
    )?;
    validate_epoch_timestamp(
        "nominal_inverter_power_observed_at",
        state.nominal_inverter_power_observed_at,
        state.ts,
    )?;
    let has_live_nominal_binding = state.nominal_inverter_power_service.is_some()
        && state.nominal_inverter_power_observed_at > 0.0;
    if state.nominal_inverter_power_service.is_some()
        != (state.nominal_inverter_power_observed_at > 0.0)
    {
        return Err(
            "nominal inverter power service and observation time must be present together"
                .to_owned(),
        );
    }
    if state.nominal_inverter_power_service.is_some() && state.nominal_inverter_power_last.is_none()
    {
        return Err("nominal inverter power provenance has no value".to_owned());
    }
    if state.nominal_inverter_power_configured
        && (has_live_nominal_binding || state.nominal_inverter_power_last.is_none())
    {
        return Err("configured nominal inverter power has inconsistent provenance".to_owned());
    }
    for (name, day) in [
        (
            "charge_ceiling.reference_day",
            state.charge_ceiling.reference_day,
        ),
        (
            "charge_ceiling.observed_day",
            state.charge_ceiling.observed_day,
        ),
    ] {
        if day.is_some_and(|value| !(0..=4_000_000).contains(&value)) {
            return Err(format!("{name} is outside the accepted range"));
        }
    }
    if !state.last_sample_date.is_empty()
        && (state.last_sample_date.len() != 10
            || !state
                .last_sample_date
                .bytes()
                .enumerate()
                .all(|(index, byte)| {
                    if matches!(index, 4 | 7) {
                        byte == b'-'
                    } else {
                        byte.is_ascii_digit()
                    }
                }))
    {
        return Err("last_sample_date is not YYYY-MM-DD".to_owned());
    }
    Ok(())
}

fn validate_discharge_state(state: &ControllerState) -> Result<(), String> {
    let protection = &state.discharge_protection;
    validate_optional_range(
        "discharge_protection.restore_power_w",
        protection.restore_power_w,
        0.0,
        MAX_PERSISTED_POWER_W,
    )?;
    validate_optional_range(
        "discharge_protection.last_set_power_w",
        protection.last_set_power_w,
        0.0,
        MAX_PERSISTED_POWER_W,
    )?;
    validate_optional_range(
        "discharge_protection.last_observed_power_w",
        protection.last_observed_power_w,
        -1.0,
        MAX_PERSISTED_POWER_W,
    )?;
    if protection.active && (protection.restore_default == protection.restore_power_w.is_some()) {
        return Err(
            "active discharge protection must have exactly one restore strategy".to_owned(),
        );
    }
    if protection.active {
        let nominal = state.nominal_inverter_power_last.ok_or_else(|| {
            "active discharge protection has no nominal inverter power binding".to_owned()
        })?;
        for (name, value) in [
            ("restore_power_w", protection.restore_power_w),
            ("last_set_power_w", protection.last_set_power_w),
            ("last_observed_power_w", protection.last_observed_power_w),
        ] {
            if value.is_some_and(|power| power >= 0.0 && power > nominal) {
                return Err(format!(
                    "discharge_protection.{name} exceeds the bound nominal inverter power"
                ));
            }
        }
        if let Some(pending) = protection.pending_write {
            if pending.generation == 0 || pending.generation != protection.write_generation {
                return Err("pending discharge write has an invalid generation binding".to_owned());
            }
            validate_discharge_setting(
                "pending_write.expected_before_w",
                pending.expected_before_w,
                nominal,
            )?;
            validate_discharge_setting("pending_write.intended_w", pending.intended_w, nominal)?;
            if matches!(pending.kind, crate::domain::DischargeWriteKind::Restrict)
                && pending.intended_w < 0.0
            {
                return Err("pending restriction cannot target the default sentinel".to_owned());
            }
            if pending.expected_before_w.to_bits() == pending.intended_w.to_bits() {
                return Err("pending discharge write does not change the setting".to_owned());
            }
        }
    } else if protection.pending_write.is_some() {
        return Err("inactive discharge protection cannot have a pending write".to_owned());
    }
    Ok(())
}

fn validate_discharge_setting(name: &str, value: f64, nominal: f64) -> Result<(), String> {
    if !value.is_finite()
        || !(value.to_bits() == (-1.0_f64).to_bits() || (0.0..=nominal).contains(&value))
    {
        return Err(format!(
            "discharge_protection.{name} is outside the hardware-bound setting range"
        ));
    }
    Ok(())
}

fn validate_service_name(name: &str, value: Option<&str>) -> Result<(), String> {
    if value.is_some_and(|service| {
        service.is_empty()
            || service.len() > 255
            || !service
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    }) {
        return Err(format!("{name} is not a valid bounded service name"));
    }
    Ok(())
}

fn validate_epoch_timestamp(name: &str, value: f64, now_ts: f64) -> Result<(), String> {
    validate_range(name, value, 0.0, now_ts + STATE_FUTURE_TOLERANCE_SECONDS)
}

fn validate_optional_range(
    name: &str,
    value: Option<f64>,
    minimum: f64,
    maximum: f64,
) -> Result<(), String> {
    value.map_or(Ok(()), |number| {
        validate_range(name, number, minimum, maximum)
    })
}

fn validate_range(name: &str, value: f64, minimum: f64, maximum: f64) -> Result<(), String> {
    if value.is_finite() && (minimum..=maximum).contains(&value) {
        Ok(())
    } else {
        Err(format!("{name} is outside the accepted range"))
    }
}

fn quarantine_bad_file(path: &Path) {
    let backup = path.with_extension(format!("bad-{}", std::process::id()));
    let _ = fs::rename(path, backup);
}

fn should_sd_override(sd: Option<&Value>, ram: Option<&Value>) -> bool {
    let Some(sd) = sd else {
        return false;
    };
    ram.is_none_or(|ram| {
        let sd_generation = value_generation(sd);
        let ram_generation = value_generation(ram);
        if sd_generation != ram_generation && (sd_generation > 0 || ram_generation > 0) {
            sd_generation > ram_generation
        } else {
            value_ts(sd) > value_ts(ram)
        }
    })
}

fn value_ts(value: &Value) -> f64 {
    value.get("ts").and_then(Value::as_f64).unwrap_or(0.0)
}

fn value_generation(value: &Value) -> u64 {
    value
        .get(STATE_DURABLE_GENERATION_KEY)
        .and_then(Value::as_u64)
        .unwrap_or(0)
}

fn merge_sd_subset(
    policy: &crate::config::PolicyConfig,
    target: &mut Value,
    sd: Option<&Value>,
    mmdd: u16,
    include_seasonal: bool,
) {
    let (Some(target), Some(sd)) = (target.as_object_mut(), sd.and_then(Value::as_object)) else {
        return;
    };
    for key in persistent_keys(policy, mmdd, include_seasonal) {
        if let Some(value) = sd.get(key) {
            target.insert(key.to_owned(), value.clone());
        }
    }
}

fn persistent_keys(
    policy: &crate::config::PolicyConfig,
    mmdd: u16,
    include_seasonal: bool,
) -> Vec<&'static str> {
    let mut keys = BASE_KEYS.to_vec();
    if include_seasonal && is_transition_mmdd(policy, mmdd) {
        keys.extend(PV_KEYS);
    }
    if include_seasonal && is_winter_mmdd(policy, mmdd) {
        keys.extend(WINTER_KEYS);
    }
    keys
}

fn state_signature(
    policy: &crate::config::PolicyConfig,
    state: &ControllerState,
    mmdd: u16,
    include_seasonal: bool,
    device_id: &str,
) -> Result<Map<String, Value>, String> {
    let value = state_value(state, device_id)?;
    Ok(sd_signature(
        policy,
        &value,
        mmdd,
        include_seasonal,
        device_id,
    ))
}

fn sd_signature(
    policy: &crate::config::PolicyConfig,
    value: &Value,
    mmdd: u16,
    include_seasonal: bool,
    device_id: &str,
) -> Map<String, Value> {
    let mut signature = persistent_keys(policy, mmdd, include_seasonal)
        .into_iter()
        .filter_map(|key| value.get(key).cloned().map(|value| (key.to_owned(), value)))
        .collect::<Map<_, _>>();
    add_state_metadata(&mut signature, device_id);
    signature
}

fn state_value(state: &ControllerState, device_id: &str) -> Result<Value, String> {
    state_value_with_generation(state, device_id, 0)
}

fn state_value_with_generation(
    state: &ControllerState,
    device_id: &str,
    generation: u64,
) -> Result<Value, String> {
    let mut value = serde_json::to_value(state).map_err(|error| error.to_string())?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| "controller state did not encode as an object".to_owned())?;
    add_state_metadata(object, device_id);
    if generation > 0 {
        object.insert(
            STATE_DURABLE_GENERATION_KEY.to_owned(),
            Value::from(generation),
        );
    }
    Ok(value)
}

fn add_state_metadata(object: &mut Map<String, Value>, device_id: &str) {
    object.insert(
        STATE_SCHEMA_KEY.to_owned(),
        Value::from(STATE_SCHEMA_VERSION),
    );
    object.insert(STATE_DEVICE_KEY.to_owned(), Value::from(device_id));
}

const fn has_gui_restore_state(state: &ControllerState) -> bool {
    state.max_charge_current_raw_set
        || state.charge_current_owned_by_script
        || state.reserve_charge_current_pending_write.is_some()
        || state.charge_current_ceiling.active
        || state.charge_current_ceiling.pending_write.is_some()
        || state.charge_current_control.owned
        || state.charge_current_control.pending_write.is_some()
        || state.minimum_soc_control.owned
        || state.minimum_soc_control.pending_write.is_some()
        || state.discharge_protection.active
}

fn signature_has_restore_state(signature: &Map<String, Value>) -> bool {
    signature
        .get("max_charge_current_raw_set")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || signature
            .get("charge_current_owned_by_script")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        || signature
            .get("reserve_charge_current_pending_write")
            .is_some_and(|value| !value.is_null())
        || signature
            .get("charge_current_ceiling")
            .and_then(Value::as_object)
            .is_some_and(|state| {
                state
                    .get("active")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                    || state
                        .get("pending_write")
                        .is_some_and(|value| !value.is_null())
            })
        || signature
            .get("charge_current_control")
            .and_then(Value::as_object)
            .is_some_and(|state| {
                state.get("owned").and_then(Value::as_bool).unwrap_or(false)
                    || state
                        .get("pending_write")
                        .is_some_and(|value| !value.is_null())
            })
        || signature
            .get("discharge_protection")
            .and_then(Value::as_object)
            .and_then(|state| state.get("active"))
            .and_then(Value::as_bool)
            .unwrap_or(false)
        || signature
            .get("minimum_soc_control")
            .and_then(Value::as_object)
            .is_some_and(|state| {
                state.get("owned").and_then(Value::as_bool).unwrap_or(false)
                    || state
                        .get("pending_write")
                        .is_some_and(|value| !value.is_null())
            })
}

#[derive(Clone, Debug)]
struct WriteRequest {
    directory: PathBuf,
    path: PathBuf,
    payload: Vec<u8>,
    signature: Map<String, Value>,
    medium_identity: Option<RemovableMediumIdentity>,
    generation: u64,
    fsync: bool,
}

#[derive(Clone, Debug)]
struct InflightWrite {
    path: PathBuf,
    signature: Map<String, Value>,
    medium_identity: Option<RemovableMediumIdentity>,
}

#[derive(Debug, Default)]
struct WriterState {
    pending: Option<WriteRequest>,
    inflight: Option<InflightWrite>,
    last_path: Option<PathBuf>,
    last_signature: Option<Map<String, Value>>,
    last_medium_identity: Option<RemovableMediumIdentity>,
    last_persist_ts: f64,
    latest_enqueued_generation: u64,
    persisted_generation: u64,
    error_count: u32,
    next_try_ts: f64,
}

#[derive(Clone, Copy, Debug, Default)]
struct WriterSnapshot {
    error_count: u32,
    next_try_ts: f64,
}

#[derive(Debug)]
struct SdWriter {
    shared: Arc<(Mutex<WriterState>, Condvar)>,
    save_interval: Duration,
}

impl SdWriter {
    fn spawn(save_interval: Duration, backoff_max: Duration) -> Result<Self, String> {
        let shared = Arc::new((Mutex::new(WriterState::default()), Condvar::new()));
        let worker = Arc::clone(&shared);
        thread::Builder::new()
            .name("ess-sd-writer".to_owned())
            .stack_size(128 * 1024)
            .spawn(move || writer_loop(&worker, backoff_max))
            .map_err(|error| format!("cannot start SD writer: {error}"))?;
        Ok(Self {
            shared,
            save_interval,
        })
    }

    fn seed(
        &self,
        path: PathBuf,
        medium_identity: Option<RemovableMediumIdentity>,
        signature: Map<String, Value>,
        persisted_at: f64,
        generation: u64,
    ) {
        let (lock, _) = &*self.shared;
        if let Ok(mut state) = lock.lock() {
            state.last_path = Some(path);
            state.last_signature = Some(signature);
            state.last_medium_identity = medium_identity;
            state.last_persist_ts = persisted_at;
            state.persisted_generation = state.persisted_generation.max(generation);
            state.latest_enqueued_generation = state.latest_enqueued_generation.max(generation);
        }
    }

    fn reset_seed(&self) {
        let (lock, _) = &*self.shared;
        if let Ok(mut state) = lock.lock() {
            state.last_path = None;
            state.last_signature = None;
            state.last_medium_identity = None;
            state.last_persist_ts = 0.0;
        }
    }

    fn has_restore_record(
        &self,
        path: &Path,
        medium_identity: Option<&RemovableMediumIdentity>,
    ) -> bool {
        let (lock, _) = &*self.shared;
        let Ok(state) = lock.lock() else {
            return false;
        };
        (state.last_path.as_deref() == Some(path)
            && state.last_medium_identity.as_ref() == medium_identity
            && state
                .last_signature
                .as_ref()
                .is_some_and(signature_has_restore_state))
            || state.pending.as_ref().is_some_and(|request| {
                request.path == path
                    && request.medium_identity.as_ref() == medium_identity
                    && signature_has_restore_state(&request.signature)
            })
            || state.inflight.as_ref().is_some_and(|request| {
                request.path == path
                    && request.medium_identity.as_ref() == medium_identity
                    && signature_has_restore_state(&request.signature)
            })
    }

    fn should_skip(
        &self,
        path: &Path,
        medium_identity: Option<&RemovableMediumIdentity>,
        signature: &Map<String, Value>,
        now_ts: f64,
        force: bool,
    ) -> bool {
        let (lock, _) = &*self.shared;
        let Ok(state) = lock.lock() else {
            return true;
        };
        if state.pending.as_ref().is_some_and(|pending| {
            pending.path == path
                && pending.medium_identity.as_ref() == medium_identity
                && pending.signature == *signature
        }) || state.inflight.as_ref().is_some_and(|inflight| {
            inflight.path == path
                && inflight.medium_identity.as_ref() == medium_identity
                && inflight.signature == *signature
        }) {
            return true;
        }
        if force {
            return false;
        }
        if now_ts < state.next_try_ts
            || now_ts - state.last_persist_ts < self.save_interval.as_secs_f64()
        {
            return true;
        }
        state.last_path.as_deref() == Some(path)
            && state.last_medium_identity.as_ref() == medium_identity
            && state.last_signature.as_ref() == Some(signature)
    }

    fn enqueue(&self, request: WriteRequest) {
        let (lock, ready) = &*self.shared;
        let Ok(mut state) = lock.lock() else {
            return;
        };
        state.latest_enqueued_generation = state.latest_enqueued_generation.max(request.generation);
        if let Some(pending) = &mut state.pending {
            if pending.generation > request.generation {
                return;
            }
            let fsync = pending.fsync || request.fsync;
            *pending = request;
            pending.fsync = fsync;
        } else {
            state.pending = Some(request);
        }
        ready.notify_all();
    }

    fn flush(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let (lock, ready) = &*self.shared;
        let Ok(mut state) = lock.lock() else {
            return false;
        };
        let target_generation = state.latest_enqueued_generation;
        loop {
            if state.persisted_generation >= target_generation {
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let Ok((next, _)) = ready.wait_timeout(state, deadline - now) else {
                return false;
            };
            state = next;
        }
    }

    fn snapshot(&self) -> WriterSnapshot {
        let (lock, _) = &*self.shared;
        lock.lock().map_or_else(
            |_| WriterSnapshot::default(),
            |state| WriterSnapshot {
                error_count: state.error_count,
                next_try_ts: state.next_try_ts,
            },
        )
    }
}

fn writer_loop(shared: &Arc<(Mutex<WriterState>, Condvar)>, backoff_max: Duration) {
    loop {
        let request = {
            let (lock, ready) = &**shared;
            let Ok(mut state) = lock.lock() else {
                return;
            };
            while state.pending.is_none() {
                let Ok(next) = ready.wait(state) else {
                    return;
                };
                state = next;
            }
            let request = state.pending.take();
            state.inflight = request.as_ref().map(|request| InflightWrite {
                path: request.path.clone(),
                signature: request.signature.clone(),
                medium_identity: request.medium_identity.clone(),
            });
            request
        };
        let Some(request) = request else {
            continue;
        };
        let result = request.medium_identity.as_ref().map_or_else(
            || {
                fs::create_dir_all(&request.directory)
                    .and_then(|()| atomic_write(&request.path, &request.payload, request.fsync))
            },
            |identity| {
                atomic_write_removable(identity, &request.path, &request.payload, request.fsync)
            },
        );
        let (lock, ready) = &**shared;
        let Ok(mut state) = lock.lock() else {
            return;
        };
        state.inflight = None;
        match result {
            Ok(()) => {
                state.last_path = Some(request.path);
                state.last_signature = Some(request.signature);
                state.last_medium_identity = request.medium_identity;
                state.last_persist_ts = system_epoch_seconds();
                state.persisted_generation = state.persisted_generation.max(request.generation);
                state.error_count = 0;
                state.next_try_ts = 0.0;
            }
            Err(error) => {
                state.error_count = state.error_count.saturating_add(1);
                let exponent = state.error_count.min(8);
                let backoff = 2_u64.pow(exponent).min(backoff_max.as_secs());
                state.next_try_ts =
                    system_epoch_seconds() + Duration::from_secs(backoff).as_secs_f64();
                if state.pending.is_none() {
                    state.pending = Some(request);
                }
                eprintln!(
                    "venus-ess-winter-soc-service: durable state write error: {error} (backoff {backoff}s)"
                );
            }
        }
        ready.notify_all();
        let retry_delay = (state.next_try_ts - system_epoch_seconds()).max(0.0);
        drop(state);
        if retry_delay > 0.0 {
            thread::sleep(Duration::from_secs_f64(retry_delay));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_DURABLE_GENERATION, STATE_DURABLE_GENERATION_KEY, SdWriter, StateRepository,
        WriteRequest, add_state_metadata, merge_sd_subset, sd_signature, should_sd_override,
        state_value, state_value_with_generation, validate_state_value, value_generation, value_ts,
    };
    use crate::clock::LocalDateTime;
    use crate::config::{PolicyConfig, RuntimeConfig};
    use crate::domain::ControllerState;
    use serde_json::{Map, Value, json};
    use std::fs;
    use std::time::Duration;

    fn repository_config(root: &std::path::Path) -> RuntimeConfig {
        RuntimeConfig {
            settings_service: "settings".to_owned(),
            system_service: "system".to_owned(),
            fallback_battery_service: None,
            runtime_dir: root.join("runtime"),
            instance_lock_file: root.join("runtime/service.lock"),
            state_device_id: "persistence-test-device".to_owned(),
            state_file: root.join("ram.json"),
            legacy_state_file: None,
            durable_restore_file: root.join("gui-restore-state.json"),
            log_file: root.join("log.txt"),
            decision_file: None,
            sd_path: Some(root.join("sd")),
            sd_label: None,
            shadow: false,
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
            policy: PolicyConfig::default(),
        }
    }

    fn state_document(config: &RuntimeConfig, mut value: Value) -> Vec<u8> {
        let object = value
            .as_object_mut()
            .unwrap_or_else(|| std::process::abort());
        add_state_metadata(object, &config.state_device_id);
        serde_json::to_vec(&value).unwrap_or_else(|_| std::process::abort())
    }

    fn valid_state_value(config: &RuntimeConfig, timestamp: f64) -> Value {
        let state = ControllerState {
            ts: timestamp,
            ..ControllerState::default()
        };
        state_value(&state, &config.state_device_id).unwrap_or_else(|_| std::process::abort())
    }

    const fn winter_date() -> LocalDateTime {
        LocalDateTime {
            year: 2026,
            month: 1,
            day: 1,
            hour: 12,
            minute: 0,
            second: 0,
        }
    }

    const fn summer_date() -> LocalDateTime {
        LocalDateTime {
            year: 2026,
            month: 7,
            day: 1,
            hour: 12,
            minute: 0,
            second: 0,
        }
    }

    const fn transition_date() -> LocalDateTime {
        LocalDateTime {
            year: 2026,
            month: 11,
            day: 10,
            hour: 12,
            minute: 0,
            second: 0,
        }
    }

    #[test]
    fn late_inserted_and_replaced_media_is_imported_in_the_active_window() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let mut config = repository_config(root.path());
        config.sd_lookup_interval = Duration::from_secs(1);
        let sd_root = config
            .sd_path
            .clone()
            .unwrap_or_else(|| std::process::abort());
        let sd_directory = sd_root.join("socSteuerung");
        let sd_state_file = sd_directory.join("ess_winter_logic.json");
        let mut repository =
            StateRepository::new(config.clone()).unwrap_or_else(|_| std::process::abort());
        let mut state = repository
            .initialize(transition_date(), 100.0, 100.0)
            .unwrap_or_else(|_| std::process::abort());

        assert!(repository.sd_imported_medium.is_none());
        assert!(
            !repository
                .refresh_window_inner(&mut state, transition_date(), 101.0)
                .unwrap_or_else(|_| std::process::abort())
        );
        assert!(repository.sd_imported_medium.is_none());

        fs::create_dir_all(&sd_directory).unwrap_or_else(|_| std::process::abort());
        fs::write(
            &sd_state_file,
            state_document(&config, json!({"ts": 102.0, "pv_history": [1_500.0]})),
        )
        .unwrap_or_else(|_| std::process::abort());

        assert!(
            repository
                .refresh_window_inner(&mut state, transition_date(), 103.0)
                .unwrap_or_else(|_| std::process::abort())
        );
        assert_eq!(state.pv_history, vec![1_500.0]);

        // Keep both directory objects alive at once so the filesystem cannot
        // immediately reuse the old inode for the simulated replacement.
        let replacement_root = root.path().join("replacement-sd");
        let replacement_directory = replacement_root.join("socSteuerung");
        let replacement_state_file = replacement_directory.join("ess_winter_logic.json");
        fs::create_dir_all(&replacement_directory).unwrap_or_else(|_| std::process::abort());
        fs::write(
            &replacement_state_file,
            state_document(&config, json!({"ts": 104.0, "pv_history": [2_500.0]})),
        )
        .unwrap_or_else(|_| std::process::abort());
        fs::remove_dir_all(&sd_root).unwrap_or_else(|_| std::process::abort());
        fs::rename(&replacement_root, &sd_root).unwrap_or_else(|_| std::process::abort());

        assert!(
            repository
                .refresh_window_inner(&mut state, transition_date(), 105.0)
                .unwrap_or_else(|_| std::process::abort())
        );
        assert_eq!(state.pv_history, vec![2_500.0]);
    }

    #[test]
    fn save_imports_a_late_medium_before_writing_it() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let mut config = repository_config(root.path());
        config.sd_lookup_interval = Duration::from_secs(1);
        let sd_directory = config
            .sd_path
            .as_ref()
            .map_or_else(|| std::process::abort(), |path| path.join("socSteuerung"));
        let sd_state_file = sd_directory.join("ess_winter_logic.json");
        let mut repository =
            StateRepository::new(config.clone()).unwrap_or_else(|_| std::process::abort());
        let mut state = repository
            .initialize(transition_date(), 100.0, 100.0)
            .unwrap_or_else(|_| std::process::abort());
        state.pv_history = vec![500.0];

        fs::create_dir_all(&sd_directory).unwrap_or_else(|_| std::process::abort());
        fs::write(
            &sd_state_file,
            state_document(&config, json!({"ts": 102.0, "pv_history": [3_000.0]})),
        )
        .unwrap_or_else(|_| std::process::abort());

        repository
            .save(&mut state, transition_date(), 103.0, false)
            .unwrap_or_else(|_| std::process::abort());

        assert_eq!(state.pv_history, vec![3_000.0]);
    }

    #[test]
    fn late_medium_cannot_reintroduce_an_older_restore_record() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let mut config = repository_config(root.path());
        config.sd_lookup_interval = Duration::from_secs(1);
        let sd_directory = config
            .sd_path
            .as_ref()
            .map_or_else(|| std::process::abort(), |path| path.join("socSteuerung"));
        let sd_state_file = sd_directory.join("ess_winter_logic.json");
        let mut repository =
            StateRepository::new(config.clone()).unwrap_or_else(|_| std::process::abort());
        let mut state = repository
            .initialize(transition_date(), 100.0, 100.0)
            .unwrap_or_else(|_| std::process::abort());
        repository.durable_generation = 2;
        repository
            .save(&mut state, transition_date(), 101.0, false)
            .unwrap_or_else(|_| std::process::abort());

        let old_owned = ControllerState {
            ts: 99.0,
            charge_current_control: crate::domain::ChargeCurrentControlState {
                external_baseline_a: Some(-1.0),
                reserve_constraint_a: Some(20.0),
                owned: true,
                last_effectively_written_a: Some(20.0),
                ..crate::domain::ChargeCurrentControlState::default()
            },
            ..ControllerState::default()
        };
        let old_value = state_value_with_generation(&old_owned, &config.state_device_id, 1)
            .unwrap_or_else(|_| std::process::abort());
        fs::create_dir_all(&sd_directory).unwrap_or_else(|_| std::process::abort());
        fs::write(
            &sd_state_file,
            serde_json::to_vec(&old_value).unwrap_or_else(|_| std::process::abort()),
        )
        .unwrap_or_else(|_| std::process::abort());

        assert!(
            !repository
                .refresh_window_inner(&mut state, transition_date(), 103.0)
                .unwrap_or_else(|_| std::process::abort())
        );
        assert!(!state.charge_current_control.owned);
        let tombstone = fs::read(&sd_state_file)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(value_generation(&tombstone), 3);
        assert_eq!(
            tombstone["charge_current_control"]["owned"],
            Value::Bool(false)
        );
    }

    #[test]
    fn custom_gui_restore_state_survives_a_summer_reboot_without_removable_storage() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let mut config = repository_config(root.path());
        config.sd_path = Some(root.path().join("missing-removable"));
        let mut repository =
            StateRepository::new(config.clone()).unwrap_or_else(|_| std::process::abort());
        let mut state = ControllerState {
            charge_current_control: crate::domain::ChargeCurrentControlState {
                external_baseline_a: Some(17.0),
                reserve_constraint_a: Some(14.0),
                routine_ceiling_requested: true,
                owned: true,
                last_effectively_written_a: Some(0.0),
                write_generation: 3,
                pending_write: None,
                ..crate::domain::ChargeCurrentControlState::default()
            },
            ..ControllerState::default()
        };

        repository
            .save(&mut state, summer_date(), 100.0, true)
            .unwrap_or_else(|_| std::process::abort());
        assert!(repository.flush(Duration::from_secs(2)));
        assert!(config.durable_restore_file.exists());
        fs::remove_file(&config.state_file).unwrap_or_else(|_| std::process::abort());

        let mut restarted = StateRepository::new(config).unwrap_or_else(|_| std::process::abort());
        let restored = restarted
            .initialize(summer_date(), 101.0, 101.0)
            .unwrap_or_else(|_| std::process::abort());

        assert_eq!(restored.max_charge_current_raw, None);
        assert!(!restored.max_charge_current_raw_set);
        assert!(!restored.charge_current_owned_by_script);
        assert_eq!(restored.max_charge_current_script_last_set, None);
        assert_eq!(
            restored.charge_current_control.external_baseline_a,
            Some(17.0)
        );
        assert_eq!(
            restored.charge_current_control.reserve_constraint_a,
            Some(14.0)
        );
        assert!(restored.charge_current_control.routine_ceiling_requested);
        assert!(restored.charge_current_control.owned);
        assert_eq!(
            restored.charge_current_control.last_effectively_written_a,
            Some(0.0)
        );
        assert_eq!(
            restored.charge_current_ceiling,
            crate::domain::ChargeCurrentCeilingState::default()
        );
    }

    #[test]
    fn minimum_soc_ownership_survives_a_reboot_without_removable_storage() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let mut config = repository_config(root.path());
        config.sd_path = Some(root.path().join("missing-removable"));
        let mut repository =
            StateRepository::new(config.clone()).unwrap_or_else(|_| std::process::abort());
        let mut state = ControllerState {
            minimum_soc_control: crate::domain::MinimumSocControlState {
                owned: true,
                external_baseline: Some(15.0),
                last_set: Some(45.0),
                write_generation: 2,
                pending_write: None,
            },
            ..ControllerState::default()
        };

        repository
            .save(&mut state, summer_date(), 100.0, true)
            .unwrap_or_else(|_| std::process::abort());
        assert!(repository.flush(Duration::from_secs(2)));
        fs::remove_file(&config.state_file).unwrap_or_else(|_| std::process::abort());

        let mut restarted = StateRepository::new(config).unwrap_or_else(|_| std::process::abort());
        let restored = restarted
            .initialize(summer_date(), 101.0, 101.0)
            .unwrap_or_else(|_| std::process::abort());

        assert!(restored.minimum_soc_control.owned);
        assert_eq!(restored.minimum_soc_control.external_baseline, Some(15.0));
        assert_eq!(restored.minimum_soc_control.last_set, Some(45.0));
    }

    #[test]
    fn default_state_does_not_create_the_flash_restore_file() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let mut config = repository_config(root.path());
        config.sd_path = Some(root.path().join("missing-removable"));
        let mut repository =
            StateRepository::new(config.clone()).unwrap_or_else(|_| std::process::abort());
        let mut state = ControllerState::default();

        repository
            .save(&mut state, summer_date(), 100.0, true)
            .unwrap_or_else(|_| std::process::abort());
        assert!(repository.flush(Duration::from_secs(2)));

        assert!(!config.durable_restore_file.exists());
    }

    #[test]
    fn newer_ram_tombstone_prevents_old_durable_ownership_from_resurrecting() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let mut config = repository_config(root.path());
        config.sd_path = Some(root.path().join("missing-removable"));
        let mut first =
            StateRepository::new(config.clone()).unwrap_or_else(|_| std::process::abort());
        let mut owned = ControllerState {
            charge_current_control: crate::domain::ChargeCurrentControlState {
                external_baseline_a: Some(-1.0),
                reserve_constraint_a: Some(20.0),
                owned: true,
                last_effectively_written_a: Some(20.0),
                ..crate::domain::ChargeCurrentControlState::default()
            },
            ..ControllerState::default()
        };
        first
            .save(&mut owned, summer_date(), 100.0, true)
            .unwrap_or_else(|_| std::process::abort());
        assert!(first.flush(Duration::from_secs(2)));
        let durable = fs::read(&config.durable_restore_file)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(value_generation(&durable), 1);

        let mut clear = ControllerState {
            ts: 101.0,
            ..ControllerState::default()
        };
        let newer_ram = state_value_with_generation(&clear, &config.state_device_id, 2)
            .unwrap_or_else(|_| std::process::abort());
        fs::write(
            &config.state_file,
            serde_json::to_vec(&newer_ram).unwrap_or_else(|_| std::process::abort()),
        )
        .unwrap_or_else(|_| std::process::abort());

        let mut second =
            StateRepository::new(config.clone()).unwrap_or_else(|_| std::process::abort());
        clear = second
            .initialize(summer_date(), 102.0, 102.0)
            .unwrap_or_else(|_| std::process::abort());
        assert!(!clear.charge_current_control.owned);
        let tombstone = fs::read(&config.durable_restore_file)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(value_generation(&tombstone), 3);
        assert_eq!(
            tombstone["charge_current_control"]["owned"],
            Value::Bool(false)
        );

        fs::remove_file(&config.state_file).unwrap_or_else(|_| std::process::abort());
        let mut third = StateRepository::new(config).unwrap_or_else(|_| std::process::abort());
        let after_full_reboot = third
            .initialize(summer_date(), 103.0, 103.0)
            .unwrap_or_else(|_| std::process::abort());
        assert!(!after_full_reboot.charge_current_control.owned);
        assert!(
            after_full_reboot
                .charge_current_control
                .pending_write
                .is_none()
        );
    }

    #[test]
    fn writer_flush_waits_for_the_latest_superseding_generation() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let path = root.path().join("state.json");
        let writer = SdWriter::spawn(Duration::ZERO, Duration::from_secs(1))
            .unwrap_or_else(|_| std::process::abort());
        let signature_one = serde_json::from_value::<std::collections::BTreeMap<String, Value>>(
            json!({"marker": 1}),
        )
        .unwrap_or_else(|_| std::process::abort())
        .into_iter()
        .collect();
        let signature_two = serde_json::from_value::<std::collections::BTreeMap<String, Value>>(
            json!({"marker": 2}),
        )
        .unwrap_or_else(|_| std::process::abort())
        .into_iter()
        .collect();
        for (generation, payload, signature) in [
            (1, b"one".to_vec(), signature_one),
            (2, b"two".to_vec(), signature_two),
        ] {
            writer.enqueue(WriteRequest {
                directory: root.path().to_path_buf(),
                path: path.clone(),
                payload,
                signature,
                medium_identity: None,
                generation,
                fsync: false,
            });
        }

        assert!(writer.flush(Duration::from_secs(2)));
        assert_eq!(fs::read(path).ok().as_deref(), Some(b"two".as_slice()));
    }

    #[test]
    fn fallback_writer_may_create_its_non_removable_directory() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let directory = root.path().join("data-fallback");
        let path = directory.join("state.json");
        let signature = serde_json::from_value::<std::collections::BTreeMap<String, Value>>(
            json!({"marker": 1}),
        )
        .unwrap_or_else(|_| std::process::abort())
        .into_iter()
        .collect();
        let writer = SdWriter::spawn(Duration::ZERO, Duration::from_secs(1))
            .unwrap_or_else(|_| std::process::abort());

        writer.enqueue(WriteRequest {
            directory,
            path: path.clone(),
            payload: b"fallback".to_vec(),
            signature,
            medium_identity: None,
            generation: 1,
            fsync: true,
        });

        assert!(writer.flush(Duration::from_secs(2)));
        assert_eq!(fs::read(path).ok().as_deref(), Some(b"fallback".as_slice()));
    }

    #[test]
    fn writer_does_not_coalesce_requests_across_removable_media_changes() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let medium = root.path().join("medium");
        fs::create_dir(&medium).unwrap_or_else(|_| std::process::abort());
        let replacement = root.path().join("replacement-medium");
        fs::create_dir(&replacement).unwrap_or_else(|_| std::process::abort());
        let mut config = repository_config(root.path());
        config.sd_path = Some(medium.clone());
        let old_identity = crate::storage::locate_sd(&config)
            .map_or_else(|| std::process::abort(), |location| location.identity);
        fs::remove_dir(&medium).unwrap_or_else(|_| std::process::abort());
        fs::rename(&replacement, &medium).unwrap_or_else(|_| std::process::abort());
        let new_identity = crate::storage::locate_sd(&config)
            .map_or_else(|| std::process::abort(), |location| location.identity);
        assert_ne!(old_identity, new_identity);

        let path = medium.join("socSteuerung/ess_winter_logic.json");
        let signature = serde_json::from_value::<std::collections::BTreeMap<String, Value>>(
            json!({"marker": 1}),
        )
        .unwrap_or_else(|_| std::process::abort())
        .into_iter()
        .collect::<Map<String, Value>>();
        let writer = SdWriter::spawn(Duration::ZERO, Duration::from_secs(1))
            .unwrap_or_else(|_| std::process::abort());
        let (lock, _) = &*writer.shared;
        if let Ok(mut state) = lock.lock() {
            state.pending = Some(WriteRequest {
                directory: path
                    .parent()
                    .unwrap_or_else(|| std::process::abort())
                    .to_path_buf(),
                path: path.clone(),
                payload: b"old".to_vec(),
                signature: signature.clone(),
                medium_identity: Some(old_identity),
                generation: 1,
                fsync: false,
            });
        } else {
            std::process::abort();
        }

        assert!(!writer.should_skip(&path, Some(&new_identity), &signature, 100.0, false,));
    }

    #[test]
    fn idle_writer_is_not_durable_without_the_requested_generation() {
        let writer = SdWriter::spawn(Duration::ZERO, Duration::from_secs(1))
            .unwrap_or_else(|_| std::process::abort());
        let (lock, _) = &*writer.shared;
        if let Ok(mut state) = lock.lock() {
            state.latest_enqueued_generation = 2;
            state.persisted_generation = 1;
        } else {
            std::process::abort();
        }

        assert!(!writer.flush(Duration::ZERO));
    }

    #[test]
    fn unified_external_charge_current_latch_is_durable() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let config = repository_config(root.path());
        let state = ControllerState {
            charge_current_control: crate::domain::ChargeCurrentControlState {
                routine_ceiling_requested: true,
                routine_external_control_latched: true,
                ..crate::domain::ChargeCurrentControlState::default()
            },
            ..ControllerState::default()
        };

        let signature = super::state_signature(
            &config.policy,
            &state,
            summer_date().mmdd(),
            false,
            &config.state_device_id,
        )
        .unwrap_or_else(|_| std::process::abort());

        assert_eq!(
            signature["charge_current_control"]["routine_external_control_latched"],
            json!(true)
        );
        assert!(signature.get("charge_current_ceiling").is_none());
    }

    #[test]
    fn saved_ram_state_contains_schema_and_device_binding() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let mut config = repository_config(root.path());
        config.sd_path = Some(root.path().join("missing-removable"));
        let mut repository =
            StateRepository::new(config.clone()).unwrap_or_else(|_| std::process::abort());
        let mut state = ControllerState::default();

        repository
            .save(&mut state, summer_date(), 100.0, false)
            .unwrap_or_else(|_| std::process::abort());
        let saved = fs::read(&config.state_file)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .unwrap_or_else(|| std::process::abort());

        assert_eq!(saved["schema_version"], json!(1));
        assert_eq!(saved["device_id"], json!(config.state_device_id));
        assert!(validate_state_value(&saved, "persistence-test-device", 100.0, false).is_ok());
    }

    #[test]
    fn wrong_device_schema_and_future_timestamp_are_rejected() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let config = repository_config(root.path());
        let mut wrong_device = valid_state_value(&config, 100.0);
        wrong_device["device_id"] = json!("another-device");
        assert!(
            validate_state_value(&wrong_device, &config.state_device_id, 100.0, false).is_err()
        );

        let mut wrong_schema = valid_state_value(&config, 100.0);
        wrong_schema["schema_version"] = json!(2);
        assert!(
            validate_state_value(&wrong_schema, &config.state_device_id, 100.0, false).is_err()
        );

        let future = valid_state_value(&config, 401.0);
        assert!(validate_state_value(&future, &config.state_device_id, 100.0, false).is_err());

        let mut excessive_generation = valid_state_value(&config, 100.0);
        excessive_generation[STATE_DURABLE_GENERATION_KEY] = json!(MAX_DURABLE_GENERATION + 1);
        assert!(
            validate_state_value(&excessive_generation, &config.state_device_id, 100.0, false)
                .is_err()
        );
    }

    #[test]
    fn unknown_pv_channel_topology_is_rejected() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let config = repository_config(root.path());
        let mut value = valid_state_value(&config, 100.0);
        value["pv_expected_channels"] = json!(0x80);

        let result = validate_state_value(&value, &config.state_device_id, 100.0, false);

        assert!(matches!(
            result,
            Err(error) if error.contains("PV channel topology contains unknown channels")
        ));
    }

    #[test]
    fn balancing_phase_timestamps_are_semantically_validated() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let config = repository_config(root.path());
        let mut high_soc_hold = valid_state_value(&config, 100.0);
        high_soc_hold["balancing_active"] = json!(true);
        high_soc_hold["balancing_start_ts"] = json!(10.0);
        high_soc_hold["balancing_high_soc_start_ts"] = json!(20.0);
        assert!(
            validate_state_value(&high_soc_hold, &config.state_device_id, 100.0, false).is_ok()
        );

        let mut backwards = high_soc_hold.clone();
        backwards["balancing_high_soc_start_ts"] = json!(9.0);
        assert!(validate_state_value(&backwards, &config.state_device_id, 100.0, false).is_err());

        let mut inactive = high_soc_hold;
        inactive["balancing_active"] = json!(false);
        assert!(validate_state_value(&inactive, &config.state_device_id, 100.0, false).is_err());
    }

    #[test]
    fn dangerous_restore_values_are_rejected_before_merge() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let config = repository_config(root.path());
        let mut state = valid_state_value(&config, 100.0);
        state["discharge_protection"] = json!({
            "active": true,
            "recharge_seen": false,
            "restore_power_w": 2_000_000.0,
            "restore_to_nominal": false,
            "last_set_power_w": 1_000.0,
            "last_observed_power_w": 1_000.0
        });

        assert!(validate_state_value(&state, &config.state_device_id, 100.0, false).is_err());
    }

    #[test]
    fn restore_state_is_bound_to_nominal_power_and_accepts_the_previous_field_name() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let config = repository_config(root.path());
        let mut legacy_default = valid_state_value(&config, 100.0);
        legacy_default["vebus_service"] = json!("com.victronenergy.vebus.ttyO1");
        legacy_default["nominal_inverter_power_last"] = json!(2_500.0);
        legacy_default["discharge_protection"] = json!({
            "active": true,
            "recharge_seen": false,
            "restore_power_w": null,
            "restore_to_nominal": true,
            "last_set_power_w": 1_000.0,
            "last_observed_power_w": 1_000.0
        });

        assert!(
            validate_state_value(&legacy_default, &config.state_device_id, 100.0, false).is_ok()
        );
        let decoded: ControllerState =
            serde_json::from_value(legacy_default).unwrap_or_else(|_| std::process::abort());
        assert!(decoded.discharge_protection.restore_default);

        let mut above_hardware = valid_state_value(&config, 100.0);
        above_hardware["vebus_service"] = json!("com.victronenergy.vebus.ttyO1");
        above_hardware["nominal_inverter_power_last"] = json!(2_500.0);
        above_hardware["discharge_protection"] = json!({
            "active": true,
            "recharge_seen": true,
            "restore_power_w": 3_000.0,
            "restore_default": false,
            "last_set_power_w": 1_000.0,
            "last_observed_power_w": 1_000.0
        });

        assert!(
            validate_state_value(&above_hardware, &config.state_device_id, 100.0, false).is_err()
        );
    }

    #[test]
    fn pending_discharge_write_is_validated_and_round_trips() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let config = repository_config(root.path());
        let mut pending = valid_state_value(&config, 100.0);
        pending["vebus_service"] = json!("com.victronenergy.vebus.ttyO1");
        pending["nominal_inverter_power_last"] = json!(2_500.0);
        pending["discharge_protection"] = json!({
            "active": true,
            "recharge_seen": false,
            "restore_power_w": null,
            "restore_default": true,
            "last_set_power_w": null,
            "last_observed_power_w": -1.0,
            "write_generation": 7,
            "pending_write": {
                "generation": 7,
                "kind": "restrict",
                "expected_before_w": -1.0,
                "intended_w": 1_000.0
            }
        });

        assert!(validate_state_value(&pending, &config.state_device_id, 100.0, false).is_ok());
        let decoded: ControllerState =
            serde_json::from_value(pending.clone()).unwrap_or_else(|_| std::process::abort());
        let encoded = state_value(&decoded, &config.state_device_id)
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            encoded["discharge_protection"]["pending_write"],
            pending["discharge_protection"]["pending_write"]
        );

        let mut wrong_generation = pending.clone();
        wrong_generation["discharge_protection"]["pending_write"]["generation"] = json!(6);
        assert!(
            validate_state_value(&wrong_generation, &config.state_device_id, 100.0, false).is_err()
        );

        let mut above_hardware = pending;
        above_hardware["discharge_protection"]["pending_write"]["intended_w"] = json!(3_000.0);
        assert!(
            validate_state_value(&above_hardware, &config.state_device_id, 100.0, false).is_err()
        );
    }

    #[test]
    fn pending_charge_current_write_is_validated_and_round_trips() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let config = repository_config(root.path());
        let mut pending = valid_state_value(&config, 100.0);
        pending["charge_current_ceiling"] = json!({
            "active": true,
            "external_control_latched": false,
            "restore_current_a": -1.0,
            "last_set_current_a": null,
            "write_generation": 7,
            "pending_write": {
                "generation": 7,
                "kind": "restrict",
                "expected_before_a": -1.0,
                "intended_a": 0.0
            }
        });

        assert!(validate_state_value(&pending, &config.state_device_id, 100.0, false).is_ok());
        let decoded: ControllerState =
            serde_json::from_value(pending.clone()).unwrap_or_else(|_| std::process::abort());
        let encoded = state_value(&decoded, &config.state_device_id)
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            encoded["charge_current_ceiling"]["pending_write"],
            pending["charge_current_ceiling"]["pending_write"]
        );

        let mut wrong_generation = pending.clone();
        wrong_generation["charge_current_ceiling"]["pending_write"]["generation"] = json!(6);
        assert!(
            validate_state_value(&wrong_generation, &config.state_device_id, 100.0, false).is_err()
        );

        let mut unsafe_target = pending.clone();
        unsafe_target["charge_current_ceiling"]["pending_write"]["intended_a"] = json!(5.0);
        assert!(
            validate_state_value(&unsafe_target, &config.state_device_id, 100.0, false).is_err()
        );

        let mut fractional_restore = pending.clone();
        fractional_restore["charge_current_ceiling"]["restore_current_a"] = json!(17.5);
        assert!(
            validate_state_value(&fractional_restore, &config.state_device_id, 100.0, false)
                .is_err()
        );

        let mut impossible_inactive = pending;
        impossible_inactive["charge_current_ceiling"]["active"] = json!(false);
        assert!(
            validate_state_value(&impossible_inactive, &config.state_device_id, 100.0, false)
                .is_err()
        );
    }

    #[test]
    fn pending_minimum_soc_write_is_validated_and_round_trips() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let config = repository_config(root.path());
        let mut pending = valid_state_value(&config, 100.0);
        pending["minimum_soc_control"] = json!({
            "owned": true,
            "external_baseline": 10.0,
            "last_set": null,
            "write_generation": 7,
            "pending_write": {
                "generation": 7,
                "kind": "apply",
                "expected_before": 10.0,
                "intended": 45.0
            }
        });

        assert!(validate_state_value(&pending, &config.state_device_id, 100.0, false).is_ok());
        let decoded: ControllerState =
            serde_json::from_value(pending.clone()).unwrap_or_else(|_| std::process::abort());
        let encoded = state_value(&decoded, &config.state_device_id)
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            encoded["minimum_soc_control"]["pending_write"],
            pending["minimum_soc_control"]["pending_write"]
        );

        let mut wrong_generation = pending.clone();
        wrong_generation["minimum_soc_control"]["pending_write"]["generation"] = json!(6);
        assert!(
            validate_state_value(&wrong_generation, &config.state_device_id, 100.0, false).is_err()
        );

        let mut wrong_restore = pending.clone();
        wrong_restore["minimum_soc_control"]["pending_write"]["kind"] = json!("restore");
        assert!(
            validate_state_value(&wrong_restore, &config.state_device_id, 100.0, false).is_err()
        );

        let mut unowned = pending;
        unowned["minimum_soc_control"]["owned"] = json!(false);
        assert!(validate_state_value(&unowned, &config.state_device_id, 100.0, false).is_err());
    }

    #[test]
    fn unified_charge_current_write_is_validated_and_round_trips() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let config = repository_config(root.path());
        let mut pending = valid_state_value(&config, 100.0);
        pending["charge_current_control"] = json!({
            "external_baseline_a": -1.0,
            "reserve_constraint_a": 14.0,
            "routine_ceiling_requested": true,
            "owned": true,
            "write_generation": 7,
            "pending_write": {
                "generation": 7,
                "kind": "restrict",
                "expected_before_a": -1.0,
                "intended_a": 0.0
            }
        });

        assert!(validate_state_value(&pending, &config.state_device_id, 100.0, false).is_ok());
        let decoded: ControllerState =
            serde_json::from_value(pending.clone()).unwrap_or_else(|_| std::process::abort());
        let encoded = state_value(&decoded, &config.state_device_id)
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            encoded["charge_current_control"]["pending_write"],
            pending["charge_current_control"]["pending_write"]
        );

        let mut wrong_generation = pending.clone();
        wrong_generation["charge_current_control"]["pending_write"]["generation"] = json!(6);
        assert!(
            validate_state_value(&wrong_generation, &config.state_device_id, 100.0, false).is_err()
        );

        let mut wrong_target = pending.clone();
        wrong_target["charge_current_control"]["pending_write"]["intended_a"] = json!(14.0);
        assert!(
            validate_state_value(&wrong_target, &config.state_device_id, 100.0, false).is_err()
        );

        let mut latched_owner = pending.clone();
        latched_owner["charge_current_control"]["routine_external_control_latched"] = json!(true);
        assert!(
            validate_state_value(&latched_owner, &config.state_device_id, 100.0, false).is_err()
        );

        let mut mixed_legacy = pending;
        mixed_legacy["max_charge_current_raw_set"] = json!(true);
        assert!(
            validate_state_value(&mixed_legacy, &config.state_device_id, 100.0, false).is_err()
        );
    }

    #[test]
    fn pending_reserve_charge_current_write_is_validated_and_round_trips() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let config = repository_config(root.path());
        let mut pending = valid_state_value(&config, 100.0);
        pending["max_charge_current_raw"] = Value::Null;
        pending["max_charge_current_raw_set"] = json!(true);
        pending["reserve_charge_current_write_generation"] = json!(7);
        pending["reserve_charge_current_pending_write"] = json!({
            "generation": 7,
            "kind": "limit",
            "expected_before_a": -1.0,
            "intended_a": 14.0
        });

        assert!(validate_state_value(&pending, &config.state_device_id, 100.0, false).is_ok());
        let decoded: ControllerState =
            serde_json::from_value(pending.clone()).unwrap_or_else(|_| std::process::abort());
        let encoded = state_value(&decoded, &config.state_device_id)
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            encoded["reserve_charge_current_pending_write"],
            pending["reserve_charge_current_pending_write"]
        );

        let mut wrong_generation = pending.clone();
        wrong_generation["reserve_charge_current_pending_write"]["generation"] = json!(6);
        assert!(
            validate_state_value(&wrong_generation, &config.state_device_id, 100.0, false).is_err()
        );

        let mut invalid_limit = pending.clone();
        invalid_limit["reserve_charge_current_pending_write"]["intended_a"] = json!(-1.0);
        assert!(
            validate_state_value(&invalid_limit, &config.state_device_id, 100.0, false).is_err()
        );

        let mut restore = pending;
        restore["charge_current_owned_by_script"] = json!(true);
        restore["max_charge_current_script_last_set"] = json!(14.0);
        restore["reserve_charge_current_write_generation"] = json!(8);
        restore["reserve_charge_current_pending_write"] = json!({
            "generation": 8,
            "kind": "restore",
            "expected_before_a": 14.0,
            "intended_a": -1.0
        });
        assert!(validate_state_value(&restore, &config.state_device_id, 100.0, false).is_ok());

        restore["reserve_charge_current_pending_write"]["intended_a"] = json!(17.0);
        assert!(validate_state_value(&restore, &config.state_device_id, 100.0, false).is_err());
    }

    #[test]
    fn nominal_power_provenance_is_semantically_validated() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let config = repository_config(root.path());
        let mut live = valid_state_value(&config, 100.0);
        live["nominal_inverter_power_last"] = json!(2_500.0);
        live["nominal_inverter_power_service"] = json!("com.victronenergy.vebus.ttyO1");
        live["nominal_inverter_power_observed_at"] = json!(99.0);
        assert!(validate_state_value(&live, &config.state_device_id, 100.0, false).is_ok());

        let mut missing_time = live.clone();
        missing_time["nominal_inverter_power_observed_at"] = json!(0.0);
        assert!(
            validate_state_value(&missing_time, &config.state_device_id, 100.0, false).is_err()
        );

        let mut future = live.clone();
        future["nominal_inverter_power_observed_at"] = json!(401.0);
        assert!(validate_state_value(&future, &config.state_device_id, 100.0, false).is_err());

        let mut configured = valid_state_value(&config, 100.0);
        configured["nominal_inverter_power_last"] = json!(2_000.0);
        configured["nominal_inverter_power_configured"] = json!(true);
        assert!(validate_state_value(&configured, &config.state_device_id, 100.0, false).is_ok());
        configured["nominal_inverter_power_service"] = json!("com.victronenergy.vebus.ttyO1");
        configured["nominal_inverter_power_observed_at"] = json!(99.0);
        assert!(validate_state_value(&configured, &config.state_device_id, 100.0, false).is_err());
    }

    #[test]
    fn legacy_state_requires_an_explicit_owned_migration_path() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let config = repository_config(root.path());
        let legacy = json!({"ts": 100.0, "min_soc_last_seen": 42.0});

        assert!(validate_state_value(&legacy, &config.state_device_id, 100.0, false).is_err());
        assert!(validate_state_value(&legacy, &config.state_device_id, 100.0, true).is_ok());
    }

    #[test]
    fn default_gui_value_is_derived_instead_of_persisted() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let mut config = repository_config(root.path());
        config.sd_path = Some(root.path().join("missing-removable"));
        let mut repository =
            StateRepository::new(config.clone()).unwrap_or_else(|_| std::process::abort());
        let mut state = ControllerState {
            max_charge_current_raw: None,
            max_charge_current_raw_set: true,
            charge_current_owned_by_script: true,
            max_charge_current_script_last_set: Some(14.0),
            ..ControllerState::default()
        };

        repository
            .save(&mut state, summer_date(), 100.0, true)
            .unwrap_or_else(|_| std::process::abort());
        assert!(repository.flush(Duration::from_secs(2)));
        let persisted = fs::read(&config.durable_restore_file)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .unwrap_or_else(|| std::process::abort());

        assert_eq!(persisted["max_charge_current_raw"], Value::Null);
        assert_eq!(persisted["max_charge_current_raw_set"], json!(true));
    }

    #[test]
    fn newer_sd_only_merges_the_explicit_seasonal_subset() {
        let mut ram = json!({
            "ts": 1.0,
            "pv_history": [1.0],
            "pv_expected_channels": 1,
            "charging_paused": true,
            "charge_ceiling": {"reference_day": 100},
            "discharge_protection": {"active": false}
        });
        let sd = json!({
            "ts": 2.0,
            "pv_history": [9.0],
            "pv_expected_channels": 65,
            "charging_paused": false,
            "charge_ceiling": {"reference_day": 200},
            "discharge_protection": {"active": true, "restore_power_w": 2500.0}
        });
        assert!(should_sd_override(Some(&sd), Some(&ram)));
        merge_sd_subset(&PolicyConfig::default(), &mut ram, Some(&sd), 1110, true);
        assert_eq!(ram["pv_history"], json!([9.0]));
        assert_eq!(ram["pv_expected_channels"], json!(65));
        assert_eq!(ram["charging_paused"], json!(true));
        assert_eq!(ram["charge_ceiling"]["reference_day"], json!(100));
        assert_eq!(ram["discharge_protection"]["active"], json!(true));
        assert_eq!(
            ram["discharge_protection"]["restore_power_w"],
            json!(2500.0)
        );
        assert_eq!(value_ts(&sd).to_bits(), 2.0_f64.to_bits());
    }

    #[test]
    fn durable_generation_takes_precedence_over_legacy_epoch_ordering() {
        let newer_generation = json!({
            "ts": 100.0,
            "durable_generation": 2
        });
        let newer_timestamp = json!({
            "ts": 200.0,
            "durable_generation": 1
        });

        assert!(should_sd_override(
            Some(&newer_generation),
            Some(&newer_timestamp)
        ));
        assert!(!should_sd_override(
            Some(&newer_timestamp),
            Some(&newer_generation)
        ));
    }

    #[test]
    fn winter_restore_excludes_transition_pv_history() {
        let mut ram = json!({
            "ts": 1.0,
            "pv_history": [9.0],
            "last_sample_date": "ram-only",
            "pv_expected_channels": 1
        });
        let sd = json!({
            "ts": 2.0,
            "pv_history": [1000.0, 1500.0, 2000.0, 2500.0],
            "last_sample_date": "2026-01-01",
            "pv_expected_channels": 65
        });

        merge_sd_subset(&PolicyConfig::default(), &mut ram, Some(&sd), 101, true);

        assert_eq!(ram["pv_history"], json!([9.0]));
        assert_eq!(ram["last_sample_date"], json!("ram-only"));
        assert_eq!(ram["pv_expected_channels"], json!(1));
        let signature = sd_signature(
            &PolicyConfig::default(),
            &sd,
            101,
            true,
            "persistence-test-device",
        );
        assert!(!signature.contains_key("pv_history"));
        assert!(!signature.contains_key("last_sample_date"));
        assert!(!signature.contains_key("pv_expected_channels"));
    }

    #[test]
    fn corrupt_sd_never_discards_valid_ram_state() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let config = repository_config(root.path());
        let sd_directory = config
            .sd_path
            .as_ref()
            .map_or_else(|| std::process::abort(), |path| path.join("socSteuerung"));
        fs::create_dir_all(&sd_directory).unwrap_or_else(|_| std::process::abort());
        fs::write(
            &config.state_file,
            state_document(
                &config,
                json!({"ts":10.0,"last_balance_ts":7.0,"min_soc_last_seen":42.0}),
            ),
        )
        .unwrap_or_else(|_| std::process::abort());
        fs::write(sd_directory.join("ess_winter_logic.json"), b"broken")
            .unwrap_or_else(|_| std::process::abort());

        let mut repository = StateRepository::new(config).unwrap_or_else(|_| std::process::abort());
        let state = repository
            .initialize(winter_date(), 20.0, 20.0)
            .unwrap_or_else(|_| std::process::abort());

        assert_eq!(state.last_balance_ts.to_bits(), 7.0_f64.to_bits());
        assert_eq!(state.min_soc_last_seen, Some(42.0));
        assert_eq!(repository.take_recovery_warnings().len(), 1);
    }

    #[test]
    fn incompatible_newer_sd_subset_falls_back_to_valid_ram_schema() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let config = repository_config(root.path());
        let sd_directory = config
            .sd_path
            .as_ref()
            .map_or_else(|| std::process::abort(), |path| path.join("socSteuerung"));
        fs::create_dir_all(&sd_directory).unwrap_or_else(|_| std::process::abort());
        fs::write(
            &config.state_file,
            state_document(
                &config,
                json!({"ts":10.0,"last_balance_ts":7.0,"min_soc_last_seen":42.0}),
            ),
        )
        .unwrap_or_else(|_| std::process::abort());
        fs::write(
            sd_directory.join("ess_winter_logic.json"),
            state_document(&config, json!({"ts":11.0,"last_balance_ts":"invalid"})),
        )
        .unwrap_or_else(|_| std::process::abort());

        let mut repository = StateRepository::new(config).unwrap_or_else(|_| std::process::abort());
        let state = repository
            .initialize(winter_date(), 20.0, 20.0)
            .unwrap_or_else(|_| std::process::abort());

        assert_eq!(state.last_balance_ts.to_bits(), 7.0_f64.to_bits());
        assert_eq!(state.min_soc_last_seen, Some(42.0));
        assert_eq!(repository.take_recovery_warnings().len(), 1);
    }

    #[test]
    fn semantically_unsafe_newer_sd_state_cannot_override_ram() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let config = repository_config(root.path());
        let sd_directory = config
            .sd_path
            .as_ref()
            .map_or_else(|| std::process::abort(), |path| path.join("socSteuerung"));
        fs::create_dir_all(&sd_directory).unwrap_or_else(|_| std::process::abort());
        fs::write(
            &config.state_file,
            state_document(
                &config,
                json!({"ts":10.0,"last_balance_ts":7.0,"min_soc_last_seen":42.0}),
            ),
        )
        .unwrap_or_else(|_| std::process::abort());
        fs::write(
            sd_directory.join("ess_winter_logic.json"),
            state_document(
                &config,
                json!({
                    "ts": 11.0,
                    "discharge_protection": {
                        "active": true,
                        "restore_power_w": 2_000_000.0,
                        "restore_to_nominal": false
                    }
                }),
            ),
        )
        .unwrap_or_else(|_| std::process::abort());

        let mut repository = StateRepository::new(config).unwrap_or_else(|_| std::process::abort());
        let state = repository
            .initialize(winter_date(), 20.0, 20.0)
            .unwrap_or_else(|_| std::process::abort());

        assert_eq!(state.last_balance_ts.to_bits(), 7.0_f64.to_bits());
        assert_eq!(state.min_soc_last_seen, Some(42.0));
        assert_eq!(repository.take_recovery_warnings().len(), 1);
    }
}

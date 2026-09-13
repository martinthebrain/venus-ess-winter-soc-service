//! Controller integration for the single volatile PV-export actuator.

use super::{
    BATTERY_SOC_PATH, BATTERY_VOLTAGE_PATH, BMS_MAX_CHARGE_CURRENT_PATH, Clock, Controller,
    DC_PV_POWER_PATH, DbusPort, LocalDateTime, LogSink, MAX_CHARGE_CURRENT_PATH, MIN_SOC_PATH,
    StatePort,
};
use crate::pv_charge_export::{self as export, ExportSample};

impl<P: DbusPort, S: StatePort, C: Clock, L: LogSink> Controller<P, S, C, L> {
    pub(super) fn update_pv_charge_export(&mut self) {
        if !self.config.battery_current.charge_enabled() && !self.state.pv_charge_export.owned() {
            return;
        }
        let now = self.clock.local_date_time();
        let result = self.update_pv_export_inner(now);
        if let Err(reason) = result {
            self.discharge_limit_status.pv_export_state = Some(reason.to_owned());
            if self.config.battery_current.charge_enabled() {
                self.discharge_limit_status.charge_unenforced_reason = Some(reason.to_owned());
            }
        }
        self.discharge_limit_status.pv_export_setpoint_w =
            self.state.pv_charge_export.last_setpoint_w;
    }

    fn update_pv_export_inner(&mut self, now: LocalDateTime) -> Result<(), &'static str> {
        let settings = self.config.settings_service.clone();
        let feed_in = self.raw(&settings, "/Settings/CGwacs/OvervoltageFeedIn");
        if feed_in == Some(0.0) || !self.config.battery_current.charge_enabled() {
            return self.release_pv_export_override();
        }
        if feed_in != Some(1.0) {
            self.release_pv_export_override()?;
            return Err("pv_feed_in_setting_unavailable");
        }
        if !self.state.pv_charge_export.owned() && !self.pv_export_recovery_pending {
            let system = self.config.system_service.clone();
            let pv = self.raw(&system, DC_PV_POWER_PATH);
            let current = self.raw(&system, "/Dc/Battery/Current");
            if pv == Some(0.0)
                && current.is_some_and(|v| {
                    (-10_000.0..=self.config.battery_current.max_charge_current_a).contains(&v)
                })
            {
                self.discharge_limit_status.measured_charge_current_a = current.map(|v| v.max(0.0));
                self.discharge_limit_status.pv_export_state =
                    Some("no_dc_pv_export_required".to_owned());
                if self
                    .discharge_limit_status
                    .charge_unenforced_reason
                    .as_deref()
                    == Some("dc_pv_feed_in_enabled_or_unknown_bypasses_dvcc_limit")
                {
                    self.discharge_limit_status.charge_unenforced_reason = None;
                }
                return Ok(());
            }
        }
        let sample = match self.pv_export_sample(now) {
            Ok(sample) => sample,
            Err(error) => {
                self.release_pv_export_override()?;
                return Err(error);
            }
        };
        self.discharge_limit_status.measured_charge_current_a =
            Some(sample.battery_current_a.max(0.0));
        if self.config.shadow {
            let plan = export::plan(sample, false);
            self.discharge_limit_status.pv_export_state = Some(format!("shadow:{}", plan.reason));
            return Ok(());
        }
        let (owner, current) = self.read_pv_export_override()?;
        if self.pv_export_recovery_pending {
            self.write_pv_export_override(&owner, current, None, sample.baseline_w)?;
            self.pv_export_recovery_pending = false;
            return Err("pv_export_recovered_before_fresh_control");
        }
        let state = &mut self.state.pv_charge_export;
        let active = state.last_setpoint_w.is_some();
        let baseline_changed = active && !export::same(state.baseline_w, Some(sample.baseline_w));
        if baseline_changed {
            state.external_latched = true;
            self.write_pv_export_override(&owner, current, None, sample.baseline_w)?;
            return Err("external_grid_setpoint_changed");
        }
        if !active && current.is_some() {
            return Err("grid_override_owned_externally");
        }
        if state.external_latched {
            if sample.battery_current_a > (sample.limit_a - 2.0).max(0.0) {
                return Err("external_grid_control_latched");
            }
            state.external_latched = false;
        }
        let plan = export::plan(sample, active);
        self.write_pv_export_override(&owner, current, plan.setpoint_w, sample.baseline_w)?;
        self.discharge_limit_status.pv_export_state = Some(plan.reason.to_owned());
        self.discharge_limit_status.charge_unenforced_reason =
            if sample.battery_current_a > sample.limit_a {
                Some(plan.reason.to_owned())
            } else {
                None
            };
        Ok(())
    }

    fn pv_export_sample(&mut self, now: LocalDateTime) -> Result<ExportSample, &'static str> {
        let settings = self.config.settings_service.clone();
        let system = self.config.system_service.clone();
        let dvcc = self.raw(&settings, "/Settings/Services/Bol");
        if !matches!(dvcc, Some(1.0 | 3.0)) {
            return Err("dvcc_not_enabled_or_unknown");
        }
        if self.raw(&settings, "/Settings/DynamicEss/Mode") != Some(0.0)
            || !matches!(
                self.raw(&settings, "/Settings/CGwacs/Hub4Mode"),
                Some(1.0 | 2.0)
            )
            || self.raw(&system, "/Control/ScheduledCharge") != Some(0.0)
            || self.raw(export::HUB4, "/Overrides/ForceCharge") != Some(0.0)
            || self.raw(export::HUB4, "/Overrides/FeedInExcess") != Some(0.0)
        {
            return Err("conflicting_or_unknown_ess_control");
        }
        let soc = self
            .raw(&system, BATTERY_SOC_PATH)
            .filter(|v| (0.0..=100.0).contains(v));
        let min_soc = self
            .raw(&settings, MIN_SOC_PATH)
            .filter(|v| (0.0..=100.0).contains(v));
        if soc.zip(min_soc).is_none_or(|(soc, min)| soc <= min) {
            return Err("pv_export_soc_reserve_unavailable");
        }
        let vebus = self.active_vebus_service(now).ok_or("vebus_unavailable")?;
        if self.optional_binary_state(&vebus, "/Ac/ActiveIn/Connected") != Some(true) {
            return Err("grid_not_connected_or_unknown");
        }
        let input = self.raw(&vebus, "/Ac/ActiveIn/ActiveInput");
        let input_type = match input {
            Some(0.0) => self.raw(&settings, "/Settings/SystemSetup/AcInput1"),
            Some(1.0) => self.raw(&settings, "/Settings/SystemSetup/AcInput2"),
            _ => None,
        };
        if input_type != Some(1.0) {
            return Err("active_input_is_not_grid");
        }
        let bms = self
            .active_bms_service(now)
            .ok_or("active_bms_unavailable")?;
        let bms_limit = self
            .raw(&bms, BMS_MAX_CHARGE_CURRENT_PATH)
            .filter(|v| (0.0..=10_000.0).contains(v))
            .ok_or("bms_charge_limit_unavailable")?;
        let gui_limit = self
            .raw(&settings, MAX_CHARGE_CURRENT_PATH)
            .filter(|v| v.to_bits() == (-1.0_f64).to_bits() || (0.0..=10_000.0).contains(v))
            .ok_or("charge_current_setting_unavailable")?;
        let limit = self
            .config
            .battery_current
            .max_charge_current_a
            .min(bms_limit);
        Ok(ExportSample {
            battery_current_a: self
                .raw(&system, "/Dc/Battery/Current")
                .ok_or("battery_current_unavailable")?,
            voltage_v: self
                .raw(&system, BATTERY_VOLTAGE_PATH)
                .ok_or("battery_voltage_unavailable")?,
            dc_pv_w: self
                .raw(&system, DC_PV_POWER_PATH)
                .ok_or("dc_pv_power_unavailable")?,
            grid_w: self
                .pv_export_grid_power()
                .ok_or("grid_power_unavailable")?,
            baseline_w: self
                .raw(&settings, "/Settings/CGwacs/AcPowerSetPoint")
                .ok_or("grid_setpoint_unavailable")?,
            max_export_w: self
                .raw(&settings, "/Settings/CGwacs/MaxFeedInPower")
                .ok_or("grid_export_limit_unavailable")?,
            limit_a: if gui_limit >= 0.0 {
                limit.min(gui_limit)
            } else {
                limit
            },
            efficiency: self.config.battery_current.inverter_efficiency,
        })
    }

    fn pv_export_grid_power(&mut self) -> Option<f64> {
        let system = self.config.system_service.clone();
        let count = match self.raw(&system, super::AC_GRID_PHASE_COUNT_PATH)? {
            1.0 => 1,
            2.0 => 2,
            3.0 => 3,
            _ => return None,
        };
        let mut watts = 0.0;
        for phase in super::PHASES.iter().take(count) {
            // -1 W is valid grid export, not a measurement-unavailable sentinel.
            watts += self.raw(
                &system,
                &super::AC_GRID_POWER_PATH.replace(super::PHASE_PLACEHOLDER, phase),
            )?;
        }
        Some(watts)
    }

    fn read_pv_export_override(&mut self) -> Result<(String, Option<f64>), &'static str> {
        if self.pv_export_boot_id.len() != 36 {
            return Err("boot_identity_unavailable");
        }
        let owner = self
            .dbus
            .service_owner(export::HUB4)
            .map_err(|_| "ess_owner_unavailable")?;
        // Address the unique owner for read, write, and readback, not a replacement service.
        let current = self
            .dbus
            .nullable_number(&owner, export::SETPOINT)
            .map_err(|_| "grid_override_unavailable")?;
        if current.is_some_and(|v| !v.is_finite() || v.abs() > 1_000_000.0) {
            return Err("grid_override_invalid");
        }
        let previous = self.state.pv_charge_export.clone();
        self.state
            .pv_charge_export
            .reconcile(&self.pv_export_boot_id, &owner, current);
        if previous != self.state.pv_charge_export {
            self.store
                .save_volatile(&self.state)
                .map_err(|_| "grid_override_ram_journal_failed")?;
        }
        Ok((owner, current))
    }

    fn write_pv_export_override(
        &mut self,
        owner: &str,
        current: Option<f64>,
        target: Option<f64>,
        baseline: f64,
    ) -> Result<(), &'static str> {
        if self.config.shadow || export::same(current, target) {
            return Ok(());
        }
        if current.is_some() && self.state.pv_charge_export.last_setpoint_w.is_none() {
            return Ok(()); // Never clear or replace another controller's override.
        }
        let previous = self.state.pv_charge_export.clone();
        self.state
            .pv_charge_export
            .prepare(current, target, baseline);
        if self.store.save_volatile(&self.state).is_err() {
            self.state.pv_charge_export = previous;
            return Err("grid_override_ram_journal_failed");
        }
        // A second comparison closes the ordinary read/plan window. D-Bus SetValue
        // itself has no CAS operation; post-write conflicts remain observable.
        let before = self
            .dbus
            .nullable_number(owner, export::SETPOINT)
            .map_err(|_| "grid_override_readback_failed")?;
        if !export::same(before, current) {
            self.state
                .pv_charge_export
                .reconcile(&self.pv_export_boot_id, owner, before);
            self.store
                .save_volatile(&self.state)
                .map_err(|_| "grid_override_ram_journal_failed")?;
            return Err("grid_override_changed_externally");
        }
        match target {
            Some(value) => self.dbus.write_float(owner, export::SETPOINT, value),
            None => self.dbus.clear_value(owner, export::SETPOINT),
        }
        .map_err(|_| "grid_override_write_failed")?;
        let observed = self
            .dbus
            .nullable_number(owner, export::SETPOINT)
            .map_err(|_| "grid_override_readback_failed")?;
        if !export::same(observed, target) {
            self.state
                .pv_charge_export
                .reconcile(&self.pv_export_boot_id, owner, observed);
            self.store
                .save_volatile(&self.state)
                .map_err(|_| "grid_override_ram_journal_failed")?;
            return Err("grid_override_readback_mismatch");
        }
        self.state.pv_charge_export.commit();
        self.wrote_setting = true;
        self.store
            .save_volatile(&self.state)
            .map_err(|_| "grid_override_ram_journal_failed")
    }

    pub(super) fn release_pv_export_override(&mut self) -> Result<(), &'static str> {
        if self.config.shadow || !self.state.pv_charge_export.owned() {
            return Ok(());
        }
        let (owner, current) = self.read_pv_export_override()?;
        let baseline = self.state.pv_charge_export.baseline_w.unwrap_or(0.0);
        self.write_pv_export_override(&owner, current, None, baseline)
    }
}

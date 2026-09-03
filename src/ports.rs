//! Boundary contracts used by the controller and deterministic test doubles.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::time::Duration;

use crate::clock::LocalDateTime;
use crate::domain::{ControllerState, DbusFailureKind};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PortError {
    kind: DbusFailureKind,
    operation: &'static str,
    detail: String,
}

impl PortError {
    #[must_use]
    pub fn new(operation: &'static str, detail: impl Into<String>) -> Self {
        Self::classified(DbusFailureKind::MethodError, operation, detail)
    }

    #[must_use]
    pub fn classified(
        kind: DbusFailureKind,
        operation: &'static str,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            operation,
            detail: detail.into(),
        }
    }

    #[must_use]
    pub const fn kind(&self) -> DbusFailureKind {
        self.kind
    }

    #[must_use]
    pub const fn operation(&self) -> &'static str {
        self.operation
    }
}

impl Display for PortError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.operation, self.detail)
    }
}

impl Error for PortError {}

pub trait DbusPort {
    /// Start a controller cycle and clear any transport fault latched by the
    /// preceding cycle.
    fn begin_cycle(&mut self) {}
    /// Return the transport fault that stopped further D-Bus work this cycle.
    fn cycle_fault(&self) -> Option<PortError> {
        None
    }
    /// Read a measurement, treating the Venus unavailable sentinel as absent.
    ///
    /// # Errors
    ///
    /// Returns a boundary error when the bus request or conversion fails.
    fn measurement(&mut self, service: &str, path: &str) -> Result<Option<f64>, PortError>;
    /// Read a raw number, preserving setting sentinels such as `-1`.
    ///
    /// # Errors
    ///
    /// Returns a boundary error when the bus request or conversion fails.
    fn raw_number(&mut self, service: &str, path: &str) -> Result<Option<f64>, PortError>;
    /// Read a textual Venus value, treating an empty value as absent.
    ///
    /// # Errors
    ///
    /// Returns a boundary error when the bus request or conversion fails.
    fn text(&mut self, service: &str, path: &str) -> Result<Option<String>, PortError>;
    /// Write a floating-point Venus value.
    ///
    /// # Errors
    ///
    /// Returns a boundary error when validation or the bus write fails.
    fn write_float(&mut self, service: &str, path: &str, value: f64) -> Result<(), PortError>;
    /// Write an integer Venus value.
    ///
    /// # Errors
    ///
    /// Returns a boundary error when the bus write fails.
    fn write_integer(&mut self, service: &str, path: &str, value: i32) -> Result<(), PortError>;
}

pub trait StatePort {
    /// Re-evaluate the seasonal SD window and optionally restore newer state.
    ///
    /// # Errors
    ///
    /// Returns an error when state encoding or recovery fails.
    fn refresh_window(
        &mut self,
        state: &mut ControllerState,
        now: LocalDateTime,
        now_ts: f64,
    ) -> Result<bool, String>;
    /// Save volatile state and enqueue a permitted durable subset.
    ///
    /// # Errors
    ///
    /// Returns an error when state encoding or the RAM write fails.
    fn save(
        &mut self,
        state: &mut ControllerState,
        now: LocalDateTime,
        now_ts: f64,
        force_sd: bool,
    ) -> Result<(), String>;
    /// Wait until the newest durable generation known at call time is confirmed.
    ///
    /// A newer superseding generation also satisfies the wait. Queue idleness
    /// alone is not a durability acknowledgement.
    fn flush(&self, timeout: Duration) -> bool;
    fn status(&self, now_ts: f64) -> String;
    fn sd_description(&self) -> Option<&str>;
}

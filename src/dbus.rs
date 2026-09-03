//! Sole Venus OS D-Bus boundary for the standalone winter controller.

use crate::domain::DbusFailureKind;
use crate::ports::{DbusPort, PortError};
use std::io::ErrorKind;
use std::thread;
use std::time::Duration;
use zbus::blocking::connection::Builder;
use zbus::blocking::{Connection, Proxy};
use zbus::zvariant::{Array, OwnedValue};

const BUS_ITEM_INTERFACE: &str = "com.victronenergy.BusItem";
const RETRY_DELAY: Duration = Duration::from_millis(100);

pub struct VenusDbus {
    connection: Connection,
    method_timeout: Duration,
    cycle_fault: Option<PortError>,
}

impl VenusDbus {
    /// Connect to the system bus with the same two-second method bound used by Python.
    ///
    /// # Errors
    ///
    /// Returns an error when the Venus OS system bus is unavailable.
    pub fn connect(method_timeout: Duration) -> Result<Self, PortError> {
        build_connection(method_timeout)
            .map(|connection| Self {
                connection,
                method_timeout,
                cycle_fault: None,
            })
            .map_err(|error| {
                PortError::classified(
                    DbusFailureKind::Transport,
                    "DBus connect",
                    error.to_string(),
                )
            })
    }

    fn value(&mut self, service: &str, path: &str) -> Result<OwnedValue, PortError> {
        self.call("DBus GetValue", |connection| {
            let proxy = Proxy::new(connection, service, path, BUS_ITEM_INTERFACE)?;
            proxy.call("GetValue", &())
        })
    }

    fn number(&mut self, service: &str, path: &str) -> Result<Option<f64>, PortError> {
        let value = self.value(service, path)?;
        let number = owned_number(&value).ok_or_else(|| {
            PortError::classified(
                DbusFailureKind::TypeMismatch,
                "DBus numeric conversion",
                path.to_owned(),
            )
        })?;
        Ok(number.is_finite().then_some(number))
    }

    fn set_value(
        &mut self,
        service: &str,
        path: &str,
        input: &OwnedValue,
    ) -> Result<(), PortError> {
        let result: i32 = self.call("DBus SetValue", |connection| {
            let proxy = Proxy::new(connection, service, path, BUS_ITEM_INTERFACE)?;
            proxy.call("SetValue", &(input.try_clone()?,))
        })?;
        if result == 0 {
            Ok(())
        } else {
            Err(PortError::classified(
                DbusFailureKind::MethodError,
                "DBus SetValue",
                format!("service returned {result} for {path}"),
            ))
        }
    }

    fn call<T, F>(&mut self, operation: &'static str, mut function: F) -> Result<T, PortError>
    where
        F: FnMut(&Connection) -> Result<T, zbus::Error>,
    {
        if let Some(error) = &self.cycle_fault {
            return Err(error.clone());
        }
        if self.connection.is_closed() {
            if let Err(error) = self.reconnect(operation, None) {
                self.cycle_fault = Some(error.clone());
                return Err(error);
            }
        }
        let first_error = match function(&self.connection) {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        let action = recovery_action(&first_error, self.connection.is_closed());
        if action == RecoveryAction::Fail {
            return Err(PortError::classified(
                classify_error(&first_error),
                operation,
                first_error.to_string(),
            ));
        }
        thread::sleep(RETRY_DELAY);
        if action == RecoveryAction::Reconnect || self.connection.is_closed() {
            if let Err(error) = self.reconnect(operation, Some(&first_error)) {
                self.cycle_fault = Some(error.clone());
                return Err(error);
            }
        }
        match function(&self.connection) {
            Ok(value) => {
                self.cycle_fault = Some(PortError::classified(
                    classify_error(&first_error),
                    "DBus cycle",
                    format!("{operation} required transport recovery: {first_error}"),
                ));
                Ok(value)
            }
            Err(retry_error) => {
                let error = PortError::classified(
                    classify_error(&first_error),
                    operation,
                    format!("first attempt failed: {first_error}; retry failed: {retry_error}"),
                );
                self.cycle_fault = Some(error.clone());
                Err(error)
            }
        }
    }

    fn reconnect(
        &mut self,
        operation: &'static str,
        first_error: Option<&zbus::Error>,
    ) -> Result<(), PortError> {
        self.connection = build_connection(self.method_timeout).map_err(|error| {
            let reason = first_error.map_or_else(
                || "system-bus connection was closed".to_owned(),
                |first| format!("first attempt failed: {first}"),
            );
            PortError::classified(
                DbusFailureKind::Transport,
                operation,
                format!("{reason}; reconnect failed: {error}"),
            )
        })?;
        Ok(())
    }
}

fn build_connection(method_timeout: Duration) -> zbus::Result<Connection> {
    Builder::system()?.method_timeout(method_timeout).build()
}

impl DbusPort for VenusDbus {
    fn begin_cycle(&mut self) {
        self.cycle_fault = None;
    }

    fn cycle_fault(&self) -> Option<PortError> {
        self.cycle_fault.clone()
    }

    fn measurement(&mut self, service: &str, path: &str) -> Result<Option<f64>, PortError> {
        self.number(service, path)
            .map(|value| value.filter(|number| number.to_bits() != (-1.0_f64).to_bits()))
    }

    fn raw_number(&mut self, service: &str, path: &str) -> Result<Option<f64>, PortError> {
        self.number(service, path)
    }

    fn text(&mut self, service: &str, path: &str) -> Result<Option<String>, PortError> {
        let value = self.value(service, path)?;
        owned_text(&value).map_err(|()| {
            PortError::classified(
                DbusFailureKind::TypeMismatch,
                "DBus text conversion",
                path.to_owned(),
            )
        })
    }

    fn write_float(&mut self, service: &str, path: &str, value: f64) -> Result<(), PortError> {
        if !value.is_finite() {
            return Err(PortError::classified(
                DbusFailureKind::TypeMismatch,
                "DBus float validation",
                path.to_owned(),
            ));
        }
        self.set_value(service, path, &OwnedValue::from(value))
    }

    fn write_integer(&mut self, service: &str, path: &str, value: i32) -> Result<(), PortError> {
        self.set_value(service, path, &OwnedValue::from(value))
    }
}

fn owned_number(value: &OwnedValue) -> Option<f64> {
    f64::try_from(value)
        .ok()
        .or_else(|| i64::try_from(value).ok().and_then(i64_to_exact_f64))
        .or_else(|| u64::try_from(value).ok().and_then(u64_to_exact_f64))
        .or_else(|| i32::try_from(value).ok().map(f64::from))
        .or_else(|| u32::try_from(value).ok().map(f64::from))
}

fn owned_text(value: &OwnedValue) -> Result<Option<String>, ()> {
    if let Ok(text) = <&str>::try_from(value) {
        let text = text.trim();
        return Ok((!text.is_empty()).then(|| text.to_owned()));
    }
    if <&Array<'_>>::try_from(value).is_ok_and(Array::is_empty) {
        return Ok(None);
    }
    Err(())
}

#[allow(clippy::cast_precision_loss)]
fn i64_to_exact_f64(value: i64) -> Option<f64> {
    const MAX_EXACT_INTEGER: i64 = 1_i64 << 53;
    (-MAX_EXACT_INTEGER..=MAX_EXACT_INTEGER)
        .contains(&value)
        .then_some(value as f64)
}

#[allow(clippy::cast_precision_loss)]
fn u64_to_exact_f64(value: u64) -> Option<f64> {
    const MAX_EXACT_INTEGER: u64 = 1_u64 << 53;
    (value <= MAX_EXACT_INTEGER).then_some(value as f64)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecoveryAction {
    Fail,
    Retry,
    Reconnect,
}

fn classify_error(error: &zbus::Error) -> DbusFailureKind {
    match error {
        zbus::Error::InputOutput(error) if error.kind() == ErrorKind::TimedOut => {
            DbusFailureKind::Timeout
        }
        zbus::Error::InputOutput(_) => DbusFailureKind::Transport,
        zbus::Error::FDO(error) => match &**error {
            zbus::fdo::Error::ServiceUnknown(_) | zbus::fdo::Error::NameHasNoOwner(_) => {
                DbusFailureKind::ServiceUnavailable
            }
            zbus::fdo::Error::UnknownObject(_) => DbusFailureKind::PathUnavailable,
            zbus::fdo::Error::NoReply(_)
            | zbus::fdo::Error::Timeout(_)
            | zbus::fdo::Error::TimedOut(_) => DbusFailureKind::Timeout,
            zbus::fdo::Error::Disconnected(_)
            | zbus::fdo::Error::IOError(_)
            | zbus::fdo::Error::NoServer(_)
            | zbus::fdo::Error::NoNetwork(_) => DbusFailureKind::Transport,
            zbus::fdo::Error::InvalidArgs(_) => DbusFailureKind::TypeMismatch,
            zbus::fdo::Error::ZBus(error) => classify_error(error),
            _ => DbusFailureKind::MethodError,
        },
        zbus::Error::MethodError(name, _, _) => classify_error_name(name.as_str()),
        zbus::Error::InterfaceNotFound => DbusFailureKind::PathUnavailable,
        zbus::Error::Variant(_) => DbusFailureKind::TypeMismatch,
        _ => DbusFailureKind::MethodError,
    }
}

fn classify_error_name(name: &str) -> DbusFailureKind {
    match name {
        "org.freedesktop.DBus.Error.ServiceUnknown"
        | "org.freedesktop.DBus.Error.NameHasNoOwner" => DbusFailureKind::ServiceUnavailable,
        "org.freedesktop.DBus.Error.UnknownObject" => DbusFailureKind::PathUnavailable,
        "org.freedesktop.DBus.Error.NoReply"
        | "org.freedesktop.DBus.Error.Timeout"
        | "org.freedesktop.DBus.Error.TimedOut" => DbusFailureKind::Timeout,
        "org.freedesktop.DBus.Error.Disconnected"
        | "org.freedesktop.DBus.Error.IOError"
        | "org.freedesktop.DBus.Error.NoServer"
        | "org.freedesktop.DBus.Error.NoNetwork" => DbusFailureKind::Transport,
        "org.freedesktop.DBus.Error.InvalidArgs" => DbusFailureKind::TypeMismatch,
        _ => DbusFailureKind::MethodError,
    }
}

fn recovery_action(error: &zbus::Error, connection_closed: bool) -> RecoveryAction {
    if connection_closed {
        return RecoveryAction::Reconnect;
    }
    match error {
        zbus::Error::InputOutput(error) => match error.kind() {
            ErrorKind::BrokenPipe
            | ErrorKind::ConnectionAborted
            | ErrorKind::ConnectionReset
            | ErrorKind::NotConnected
            | ErrorKind::UnexpectedEof
            | ErrorKind::WriteZero => RecoveryAction::Reconnect,
            ErrorKind::TimedOut => RecoveryAction::Retry,
            _ => RecoveryAction::Fail,
        },
        zbus::Error::FDO(error) => match &**error {
            zbus::fdo::Error::Disconnected(_) | zbus::fdo::Error::IOError(_) => {
                RecoveryAction::Reconnect
            }
            zbus::fdo::Error::NoReply(_)
            | zbus::fdo::Error::Timeout(_)
            | zbus::fdo::Error::TimedOut(_) => RecoveryAction::Retry,
            _ => RecoveryAction::Fail,
        },
        zbus::Error::MethodError(name, _, _) => match name.as_str() {
            "org.freedesktop.DBus.Error.Disconnected" | "org.freedesktop.DBus.Error.IOError" => {
                RecoveryAction::Reconnect
            }
            "org.freedesktop.DBus.Error.NoReply"
            | "org.freedesktop.DBus.Error.Timeout"
            | "org.freedesktop.DBus.Error.TimedOut" => RecoveryAction::Retry,
            _ => RecoveryAction::Fail,
        },
        _ => RecoveryAction::Fail,
    }
}

#[cfg(test)]
mod tests {
    use super::{classify_error, owned_number, owned_text};
    use crate::domain::DbusFailureKind;
    use zbus::zvariant::{OwnedValue, Str};

    #[test]
    fn all_numeric_venus_scalars_are_decoded() {
        assert_eq!(owned_number(&OwnedValue::from(12.5_f64)), Some(12.5));
        assert_eq!(owned_number(&OwnedValue::from(-1_i32)), Some(-1.0));
        assert_eq!(owned_number(&OwnedValue::from(42_u32)), Some(42.0));
    }

    #[test]
    fn textual_venus_values_are_trimmed_and_empty_values_are_absent() {
        assert_eq!(
            owned_text(&OwnedValue::from(Str::from(
                " com.victronenergy.vebus.ttyS4 ",
            ))),
            Ok(Some("com.victronenergy.vebus.ttyS4".to_owned()))
        );
        assert_eq!(owned_text(&OwnedValue::from(Str::from("  "))), Ok(None));
        assert_eq!(owned_text(&OwnedValue::from(42_i32)), Err(()));
    }

    #[test]
    fn fdo_read_failures_are_classified_by_boundary_meaning() {
        let cases = [
            (
                zbus::fdo::Error::ServiceUnknown("missing service".to_owned()),
                DbusFailureKind::ServiceUnavailable,
            ),
            (
                zbus::fdo::Error::UnknownObject("missing object".to_owned()),
                DbusFailureKind::PathUnavailable,
            ),
            (
                zbus::fdo::Error::InvalidArgs("wrong type".to_owned()),
                DbusFailureKind::TypeMismatch,
            ),
            (
                zbus::fdo::Error::NoReply("timed out".to_owned()),
                DbusFailureKind::Timeout,
            ),
            (
                zbus::fdo::Error::Disconnected("closed".to_owned()),
                DbusFailureKind::Transport,
            ),
            (
                zbus::fdo::Error::AccessDenied("denied".to_owned()),
                DbusFailureKind::MethodError,
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(classify_error(&zbus::Error::FDO(Box::new(error))), expected);
        }
    }
}

//! Exercise optional-path caching against an isolated bus, never the system bus.

use super::{BmsOptionalPath, VenusDbus};
use crate::config::{BMS_ALLOW_TO_CHARGE_PATH, BMS_MAX_CHARGE_CURRENT_PATH};
use crate::domain::DbusFailureKind;
use crate::ports::{DbusPort, PortError};
use std::error::Error;
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use zbus::blocking::Connection;
use zbus::blocking::connection::Builder;
use zbus::zvariant::{OwnedValue, Str};

type TestResult = Result<(), Box<dyn Error>>;
const SERVICE: &str = "com.victronenergy.battery.test";
const OTHER_SERVICE: &str = "com.victronenergy.battery.other";

struct TestBus {
    daemon: Child,
    address: String,
    _directory: tempfile::TempDir,
}

impl TestBus {
    fn start() -> Result<Self, Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let address = format!("unix:path={}", directory.path().join("bus").display());
        let daemon = Command::new("dbus-daemon")
            .args(["--session", "--nofork", "--print-address=1", "--address"])
            .arg(&address)
            .stdout(Stdio::piped())
            .spawn()?;
        let mut bus = Self {
            daemon,
            address: String::new(),
            _directory: directory,
        };
        let stdout = bus.daemon.stdout.take().ok_or("missing bus stdout")?;
        BufReader::new(stdout).read_line(&mut bus.address)?;
        bus.address = bus.address.trim().to_owned();
        if bus.address.is_empty() {
            return Err("isolated D-Bus daemon failed to start".into());
        }
        Ok(bus)
    }

    fn client(&self) -> zbus::Result<VenusDbus> {
        let method_timeout = Duration::from_secs(1);
        Ok(VenusDbus {
            connection: Builder::address(self.address.as_str())?
                .method_timeout(method_timeout)
                .build()?,
            method_timeout,
            cycle_fault: None,
            bms_optional_path: BmsOptionalPath::default(),
        })
    }

    fn battery(
        &self,
        name: &str,
        ccl: TestValue,
        permission: TestValue,
    ) -> zbus::Result<Connection> {
        Builder::address(self.address.as_str())?
            .name(name)?
            .serve_at(BMS_MAX_CHARGE_CURRENT_PATH, ccl)?
            .serve_at(BMS_ALLOW_TO_CHARGE_PATH, permission)?
            .build()
    }
}

impl Drop for TestBus {
    fn drop(&mut self) {
        let _ = self.daemon.kill();
        let _ = self.daemon.wait();
    }
}

#[derive(Clone, Copy)]
enum Response {
    Number(i32),
    Missing,
    Timeout,
    AccessDenied,
    WrongType,
    InvalidNumber,
    ServiceUnavailable,
}

#[derive(Clone)]
struct TestValue {
    response: Arc<Mutex<Response>>,
    reads: Arc<AtomicUsize>,
}

impl TestValue {
    fn new(response: Response) -> Self {
        Self {
            response: Arc::new(Mutex::new(response)),
            reads: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn change(&self, response: Response) {
        *self
            .response
            .lock()
            .unwrap_or_else(|_| std::process::abort()) = response;
    }

    fn read_count(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }
}

#[zbus::interface(name = "com.victronenergy.BusItem")]
impl TestValue {
    fn get_value(&self) -> zbus::fdo::Result<OwnedValue> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let response = *self
            .response
            .lock()
            .unwrap_or_else(|_| std::process::abort());
        match response {
            Response::Number(value) => Ok(OwnedValue::from(value)),
            Response::Missing => Err(zbus::fdo::Error::UnknownObject("absent".to_owned())),
            Response::Timeout => Err(zbus::fdo::Error::NoReply("timeout".to_owned())),
            Response::AccessDenied => Err(zbus::fdo::Error::AccessDenied("denied".to_owned())),
            Response::WrongType => Ok(OwnedValue::from(Str::from("not a number"))),
            Response::InvalidNumber => Ok(OwnedValue::from(f64::NAN)),
            Response::ServiceUnavailable => {
                Err(zbus::fdo::Error::ServiceUnknown("unavailable".to_owned()))
            }
        }
    }
}

fn read_cycle(dbus: &mut VenusDbus, service: &str) -> Result<Option<f64>, PortError> {
    dbus.begin_cycle();
    dbus.measurement(service, BMS_MAX_CHARGE_CURRENT_PATH)?;
    dbus.measurement(service, BMS_ALLOW_TO_CHARGE_PATH)
}

#[test]
fn missing_optional_path_is_only_probed_once_per_verified_owner() -> TestResult {
    let bus = TestBus::start()?;
    let ccl = TestValue::new(Response::Number(80));
    let permission = TestValue::new(Response::Missing);
    let _battery = bus.battery(SERVICE, ccl.clone(), permission.clone())?;
    let mut dbus = bus.client()?;

    assert!(
        read_cycle(&mut dbus, SERVICE)
            .is_err_and(|error| error.kind() == DbusFailureKind::PathUnavailable)
    );
    for _ in 0..4 {
        assert_eq!(read_cycle(&mut dbus, SERVICE)?, None);
        assert!(dbus.cycle_fault().is_none());
    }
    assert_eq!(permission.read_count(), 1);
    assert_eq!(ccl.read_count(), 5);

    // Without this cycle's owner proof, even a remembered absence must be rechecked.
    dbus.begin_cycle();
    assert!(dbus.measurement(SERVICE, BMS_ALLOW_TO_CHARGE_PATH).is_err());
    assert_eq!(permission.read_count(), 2);
    Ok(())
}

#[test]
fn existing_permission_is_read_each_cycle_including_zero() -> TestResult {
    let bus = TestBus::start()?;
    let permission = TestValue::new(Response::Number(1));
    let _battery = bus.battery(
        SERVICE,
        TestValue::new(Response::Number(80)),
        permission.clone(),
    )?;
    let mut dbus = bus.client()?;

    for value in [1, 0, 1] {
        permission.change(Response::Number(value));
        assert_eq!(read_cycle(&mut dbus, SERVICE)?, Some(f64::from(value)));
    }
    assert_eq!(permission.read_count(), 3);
    Ok(())
}

#[test]
fn timeout_conversion_and_service_errors_never_mean_path_absence() -> TestResult {
    let bus = TestBus::start()?;
    let permission = TestValue::new(Response::Timeout);
    let _battery = bus.battery(
        SERVICE,
        TestValue::new(Response::Number(80)),
        permission.clone(),
    )?;
    let mut dbus = bus.client()?;

    for response in [
        Response::Timeout,
        Response::AccessDenied,
        Response::WrongType,
        Response::InvalidNumber,
        Response::ServiceUnavailable,
    ] {
        permission.change(response);
        assert!(!matches!(read_cycle(&mut dbus, SERVICE), Ok(Some(_))));
        permission.change(Response::Number(0));
        let reads = permission.read_count();
        assert_eq!(read_cycle(&mut dbus, SERVICE)?, Some(0.0));
        assert_eq!(permission.read_count(), reads + 1);
    }
    Ok(())
}

#[test]
fn restarted_bms_with_the_same_name_reprobes_the_optional_path() -> TestResult {
    let bus = TestBus::start()?;
    let missing = TestValue::new(Response::Missing);
    let original = bus.battery(
        SERVICE,
        TestValue::new(Response::Number(80)),
        missing.clone(),
    )?;
    let mut dbus = bus.client()?;
    assert!(read_cycle(&mut dbus, SERVICE).is_err());
    assert_eq!(read_cycle(&mut dbus, SERVICE)?, None);
    let old_owner = original.unique_name().cloned();
    original.close()?;

    let permission = TestValue::new(Response::Number(0));
    let replacement = bus.battery(
        SERVICE,
        TestValue::new(Response::Number(80)),
        permission.clone(),
    )?;
    assert_ne!(old_owner.as_ref(), replacement.unique_name());
    assert_eq!(read_cycle(&mut dbus, SERVICE)?, Some(0.0));
    assert_eq!(missing.read_count(), 1);
    assert_eq!(permission.read_count(), 1);
    Ok(())
}

#[test]
fn bms_selection_changes_discard_the_previous_missing_path() -> TestResult {
    let bus = TestBus::start()?;
    let first = TestValue::new(Response::Missing);
    let second = TestValue::new(Response::Number(0));
    let _first = bus.battery(SERVICE, TestValue::new(Response::Number(80)), first.clone())?;
    let _second = bus.battery(OTHER_SERVICE, TestValue::new(Response::Number(80)), second)?;
    let mut dbus = bus.client()?;
    assert!(read_cycle(&mut dbus, SERVICE).is_err());
    assert_eq!(read_cycle(&mut dbus, SERVICE)?, None);
    assert_eq!(read_cycle(&mut dbus, OTHER_SERVICE)?, Some(0.0));
    assert!(read_cycle(&mut dbus, SERVICE).is_err());
    assert_eq!(first.read_count(), 2);
    Ok(())
}

#[test]
fn failed_owner_verification_and_transport_fault_do_not_use_cached_absence() -> TestResult {
    let bus = TestBus::start()?;
    let ccl = TestValue::new(Response::Number(80));
    let permission = TestValue::new(Response::Missing);
    let _battery = bus.battery(SERVICE, ccl.clone(), permission.clone())?;
    let mut dbus = bus.client()?;
    assert!(read_cycle(&mut dbus, SERVICE).is_err());
    assert_eq!(read_cycle(&mut dbus, SERVICE)?, None);

    // An already latched transport fault must not become a successful cached read.
    dbus.cycle_fault = Some(PortError::classified(
        DbusFailureKind::Transport,
        "test",
        "closed",
    ));
    assert!(
        dbus.measurement(SERVICE, BMS_ALLOW_TO_CHARGE_PATH)
            .is_err_and(|error| error.kind() == DbusFailureKind::Transport)
    );

    permission.change(Response::Number(0));
    ccl.change(Response::AccessDenied);
    dbus.begin_cycle();
    assert!(
        dbus.measurement(SERVICE, BMS_MAX_CHARGE_CURRENT_PATH)
            .is_err()
    );
    assert_eq!(
        dbus.measurement(SERVICE, BMS_ALLOW_TO_CHARGE_PATH)?,
        Some(0.0)
    );
    assert_eq!(permission.read_count(), 2);
    Ok(())
}

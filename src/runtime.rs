//! Process lifecycle, signals, decision snapshots, and the bounded main loop.

use crate::clock::{Clock, SystemClock};
use crate::config::RuntimeConfig;
use crate::controller::Controller;
use crate::dbus::VenusDbus;
use crate::domain::ControllerState;
use crate::instance_lock::InstanceLock;
use crate::logging::{LogSink, RamLogger};
use crate::persistence::StateRepository;
use crate::storage::{atomic_write, prepare_private_directory};
use signal_hook::consts::signal::{SIGINT, SIGTERM};
use signal_hook::flag;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

type RuntimeController = Controller<VenusDbus, StateRepository, SystemClock, RamLogger>;

struct RuntimeContext {
    _instance_lock: InstanceLock,
    controller: RuntimeController,
}

/// Run the service until a terminating signal arrives.
///
/// # Errors
///
/// Returns an error when the private runtime boundary, instance lock, signals,
/// state repository, or initial system-bus connection cannot be established.
pub fn run(config: RuntimeConfig) -> Result<(), Box<dyn Error>> {
    let decision_file = config.decision_file.clone();
    let one_shot = config.one_shot;
    let interval = config.loop_interval;
    let mut context = initialize(config)?;
    let controller = &mut context.controller;
    controller.log_startup();

    let terminating = Arc::new(AtomicBool::new(false));
    flag::register(SIGTERM, Arc::clone(&terminating))?;
    flag::register(SIGINT, Arc::clone(&terminating))?;

    loop {
        let started = Instant::now();
        let decision = controller.run_once();
        if let Some(path) = &decision_file {
            let encoded = serde_json::to_vec(&decision)?;
            if let Err(error) = atomic_write(path, &encoded, false) {
                eprintln!("venus-ess-winter-soc-service: decision snapshot failed: {error}");
            }
        }
        if one_shot || terminating.load(Ordering::Acquire) {
            break;
        }
        let remaining = interval.saturating_sub(started.elapsed());
        sleep_interruptibly(remaining, &terminating);
        if terminating.load(Ordering::Acquire) {
            break;
        }
    }
    controller.shutdown();
    Ok(())
}

/// Restore a DVCC charge-current baseline still owned by a previous process.
///
/// This command is intended for installers and uninstallers after the supervised
/// process has stopped. The historical command name is retained for installer
/// compatibility, but this clears every unified-arbiter constraint and uses the
/// same instance lock and persisted ownership contract as the normal runtime.
///
/// # Errors
///
/// Returns an error when state recovery, locking, or the D-Bus boundary cannot
/// be established.
pub fn restore_owned_charge_current(config: RuntimeConfig) -> Result<(), Box<dyn Error>> {
    let mut context = initialize(config)?;
    context.controller.shutdown();
    Ok(())
}

/// Restore all still-owned settings before permanently removing the service.
///
/// # Errors
///
/// Returns an error when state recovery, locking, D-Bus access, readback, or
/// durable ownership cleanup fails.
pub fn restore_all_owned_settings(config: RuntimeConfig) -> Result<(), Box<dyn Error>> {
    let mut context = initialize(config)?;
    context
        .controller
        .restore_all_owned_settings()
        .map_err(Into::into)
}

fn initialize(config: RuntimeConfig) -> Result<RuntimeContext, Box<dyn Error>> {
    prepare_private_directory(&config.runtime_dir)?;
    let instance_lock = InstanceLock::acquire(&config.instance_lock_file)?;
    let clock = SystemClock;
    let mut logger = RamLogger::new(config.log_file.clone());
    let mut store = StateRepository::new(config.clone())?;
    let now = clock.local_date_time();
    let now_ts = clock.epoch_seconds();
    let monotonic_now = clock.monotonic_seconds();
    let state = match store.initialize(now, now_ts, monotonic_now) {
        Ok(state) => state,
        Err(error) => {
            logger.log(&format!("State recovery failed; using defaults: {error}"));
            ControllerState {
                boot_ts: monotonic_now,
                ..ControllerState::default()
            }
        }
    };
    for warning in store.take_recovery_warnings() {
        logger.log(&format!("State recovery warning: {warning}"));
    }
    let dbus = VenusDbus::connect(config.dbus_timeout)?;
    Ok(RuntimeContext {
        _instance_lock: instance_lock,
        controller: Controller::new(dbus, store, clock, logger, config, state),
    })
}

fn sleep_interruptibly(duration: Duration, terminating: &AtomicBool) {
    let deadline = Instant::now() + duration;
    while !terminating.load(Ordering::Acquire) {
        let now = Instant::now();
        if now >= deadline {
            return;
        }
        thread::sleep((deadline - now).min(Duration::from_millis(250)));
    }
}

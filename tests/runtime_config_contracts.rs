use std::process::Command;

#[test]
fn invalid_runtime_tuning_fails_before_connecting_to_dbus() {
    let cases = [
        ("ESS_LOOP_INTERVAL_SECONDS", "4"),
        ("ESS_LOOP_INTERVAL_SECONDS", "91"),
        ("ESS_DBUS_TIMEOUT_SECONDS", "0"),
        ("ESS_STATUS_LOG_INTERVAL_SECONDS", "0"),
        ("ESS_INVALID_LOG_INTERVAL_SECONDS", "0"),
        ("ESS_NOMINAL_INVERTER_POWER_MAX_AGE_SECONDS", "59"),
        ("ESS_NOMINAL_INVERTER_POWER_W", "0"),
        ("ESS_NOMINAL_INVERTER_POWER_W", "not-a-number"),
        ("ESS_PREFERRED_BATTERY_SERVICE", "not-a-battery-service"),
        ("ESS_SHADOW_MODE", "tru"),
        ("ESS_ONESHOT", "enabled"),
        ("ESS_SD_SAVE_INTERVAL_SECONDS", "59"),
        ("ESS_SD_LOOKUP_INTERVAL_SECONDS", "0"),
        ("ESS_SD_BACKOFF_MAX_SECONDS", "0"),
        ("ESS_STATE_FILE", "/tmp/insecure-state.json"),
        ("ESS_LOG_FILE", "/tmp/insecure-log.txt"),
        ("ESS_INSTANCE_LOCK_FILE", "/tmp/insecure.lock"),
        ("ESS_DECISION_FILE", "/tmp/insecure-decision.json"),
        ("ESS_STATE_DEVICE_ID", "invalid device identity"),
    ];
    for (name, value) in cases {
        let output = Command::new(env!("CARGO_BIN_EXE_venus-ess-winter-soc-service"))
            .env(name, value)
            .output()
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(output.status.code(), Some(2), "{name}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(name),
            "{name}"
        );
    }
}

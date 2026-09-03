use venus_ess_winter_soc_service::config::RuntimeConfig;
use venus_ess_winter_soc_service::runtime;

fn main() {
    let command = match parse_command() {
        Ok(value) => value,
        Err(error) => {
            eprintln!("venus-ess-winter-soc-service: {error}");
            std::process::exit(2);
        }
    };
    let config = match RuntimeConfig::from_env() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("venus-ess-winter-soc-service: invalid configuration: {error}");
            std::process::exit(2);
        }
    };
    let result = match command {
        Command::Run => runtime::run(config),
        Command::RestoreChargeCurrent => runtime::restore_owned_charge_current(config),
        Command::RestoreAllOwnedSettings => runtime::restore_all_owned_settings(config),
    };
    if let Err(error) = result {
        eprintln!("venus-ess-winter-soc-service: {error}");
        std::process::exit(1);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Command {
    Run,
    RestoreChargeCurrent,
    RestoreAllOwnedSettings,
}

fn parse_command() -> Result<Command, &'static str> {
    let mut arguments = std::env::args_os().skip(1);
    match (arguments.next(), arguments.next()) {
        (None, None) => Ok(Command::Run),
        (Some(argument), None) if argument == "--restore-charge-current-ceiling" => {
            Ok(Command::RestoreChargeCurrent)
        }
        (Some(argument), None) if argument == "--restore-all-owned-settings" => {
            Ok(Command::RestoreAllOwnedSettings)
        }
        _ => Err(
            "expected no arguments, --restore-charge-current-ceiling, or --restore-all-owned-settings",
        ),
    }
}

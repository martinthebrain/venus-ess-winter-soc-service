#!/usr/bin/env python3
"""Publish simulated Victron D-Bus services and run live controller scenarios."""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Optional, cast

from live_dbus_core import (
    FAKE_BATTERY_SERVICE,
    FAKE_SETTINGS_SERVICE,
    FAKE_SYSTEM_SERVICE,
    LiveDbusServer,
    LiveDbusStore,
    M,
    SOC_SCRIPT,
    Scenario,
    ScenarioOutcome,
)
from live_dbus_harness import LiveHarness, RemoteHarness
from live_dbus_scenarios import REMOTE_SCENARIOS


def seed_all_paths(store: LiveDbusStore) -> None:
    """Create every path that may be accessed by the live scenarios."""
    harness = LiveHarness(store)
    harness.reset_values(soc=50.0)


def selected_scenarios(names: list[str]) -> list[Scenario]:
    """Return selected scenarios by name."""
    return select_scenarios(names, REMOTE_SCENARIOS)


def run_scenarios(harness: Any, scenarios: list[Scenario], verbose: bool) -> int:
    """Run live D-Bus scenarios and return an exit code."""
    outcomes: list[ScenarioOutcome] = []
    for scenario in scenarios:
        print(f"RUN  {scenario.name}", flush=True)
        outcome = scenario.run(harness)
        outcomes.append(outcome)
        print_scenario_outcome(outcome, verbose)
    failures = count_failures(outcomes)
    print(f"\n{len(scenarios) - failures}/{len(scenarios)} live D-Bus scenarios passed", flush=True)
    return 1 if failures else 0


def select_scenarios(names: list[str], scenarios: list[Scenario]) -> list[Scenario]:
    """Return all scenarios or a validated name subset."""
    if not names or "all" in names:
        return scenarios
    known = {scenario.name: scenario for scenario in scenarios}
    ensure_scenario_names_exist(names, known)
    return [known[name] for name in names]


def ensure_scenario_names_exist(names: list[str], known: dict[str, Scenario]) -> None:
    """Raise when a requested scenario name is unknown."""
    missing = [name for name in names if name not in known]
    if missing:
        raise SystemExit(f"Unknown scenario(s): {', '.join(missing)}")


def print_scenario_outcome(outcome: ScenarioOutcome, verbose: bool) -> None:
    """Print one scenario outcome and optional details."""
    status = "PASS" if outcome.passed else "FAIL"
    print(f"{status} {outcome.name}", flush=True)
    print_outcome_details(outcome, verbose)


def print_outcome_details(outcome: ScenarioOutcome, verbose: bool) -> None:
    """Print outcome details when requested or when the scenario failed."""
    if verbose or not outcome.passed:
        for detail in outcome.details:
            print(f"  - {detail}")


def count_failures(outcomes: list[ScenarioOutcome]) -> int:
    """Return the number of failed scenario outcomes."""
    return sum(1 for outcome in outcomes if not outcome.passed)


def run_client_scenarios(names: list[str], verbose: bool) -> int:
    """Run scenario clients against already running fake D-Bus services."""
    try:
        return run_scenarios(RemoteHarness(), selected_scenarios(names), verbose)
    except Exception as exc:
        print(
            "ERROR: Could not talk to the fake D-Bus services. "
            "Start them with 'live_dbus_testbed.py --serve' or run without --client.",
            file=sys.stderr,
        )
        print(f"DETAIL: {exc}", file=sys.stderr)
        return 2


def run_scenarios_in_child(names: list[str], verbose: bool) -> int:
    """Run controller scenarios in a child process to avoid same-process D-Bus deadlocks."""
    cmd = [sys.executable, str(Path(__file__).resolve()), "--client"]
    if verbose:
        cmd.append("--verbose")
    cmd.extend(names)
    proc = subprocess.run(cmd, env=dict(os.environ), check=False)
    return int(proc.returncode)


def controller_env_command() -> str:
    """Return a command that runs the controller against the fake services."""
    return (
        f"ESS_SERVICE_SETTINGS={FAKE_SETTINGS_SERVICE} "
        f"ESS_SERVICE_SYSTEM={FAKE_SYSTEM_SERVICE} "
        f"ESS_PREFERRED_BATTERY_SERVICE={FAKE_BATTERY_SERVICE} "
        f"python3 {SOC_SCRIPT}"
    )


def serve_until_interrupted(server: LiveDbusServer) -> int:
    """Keep fake services alive for manual controller testing."""
    print("Live fake D-Bus services are running.")
    print("Quick probes:")
    print(f"dbus -y {FAKE_SYSTEM_SERVICE} {M.BATTERY_SOC_PATH} GetValue")
    print(f"dbus -y {FAKE_SETTINGS_SERVICE} {M.MIN_SOC_PATH} GetValue")
    print("In another shell, run:")
    print(controller_env_command())
    print("Press Ctrl-C here to stop the fake services.")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        server.stop()
        return 0


def cleanup_stale_testbeds() -> int:
    """Stop old --serve testbed processes without touching the production service."""
    proc = subprocess.run(
        ["pgrep", "-f", "live_dbus_testbed.py --serve"],
        capture_output=True,
        text=True,
        check=False,
    )
    killed = 0
    own_pid = os.getpid()
    for raw_pid in proc.stdout.split():
        try:
            pid = int(raw_pid)
        except ValueError:
            continue
        if pid == own_pid:
            continue
        try:
            os.kill(pid, signal.SIGTERM)
            killed += 1
        except OSError:
            pass
    print(f"Stopped {killed} stale live D-Bus testbed process(es).")
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse command-line options."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenarios", nargs="*", help="Scenario names to run, or 'all'.")
    parser.add_argument("--serve", action="store_true", help="Only publish fake services for manual testing.")
    parser.add_argument("--run-live-scenarios", action="store_true", help="Explicitly run live scenarios; this is also the default.")
    parser.add_argument("--cleanup-stale", action="store_true", help="Stop old --serve testbed processes and exit.")
    parser.add_argument("--client", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("-v", "--verbose", action="store_true", help="Print details for passing scenarios too.")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    """Command-line entrypoint."""
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if bool(args.cleanup_stale):
        return cleanup_stale_testbeds()
    if bool(args.client):
        return run_client_scenarios(cast(list[str], args.scenarios), bool(args.verbose))
    return run_server_command(args)


def run_server_command(args: argparse.Namespace) -> int:
    """Start fake services and either serve manually or run child scenarios."""
    store = LiveDbusStore()
    seed_all_paths(store)
    try:
        server = LiveDbusServer.create(store)
        server.start()
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    try:
        if bool(args.serve):
            return serve_until_interrupted(server)
        return run_scenarios_in_child(cast(list[str], args.scenarios), bool(args.verbose))
    finally:
        server.stop()


if __name__ == "__main__":
    raise SystemExit(main())

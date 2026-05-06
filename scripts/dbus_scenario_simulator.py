#!/usr/bin/env python3
"""Run offline D-Bus scenarios against the ESS winter SoC controller."""

from __future__ import annotations

import argparse
import sys
from typing import Optional, cast

from dbus_sim_core import Scenario, ScenarioOutcome
from dbus_sim_scenarios import SCENARIOS

def selected_scenarios(names: list[str]) -> list[Scenario]:
    """Return scenarios selected by CLI names."""
    return select_scenarios(names, SCENARIOS)


def run_scenarios(scenarios: list[Scenario], verbose: bool) -> int:
    """Run scenarios and return a process exit code."""
    outcomes: list[ScenarioOutcome] = []
    for scenario in scenarios:
        outcome = scenario.run()
        outcomes.append(outcome)
        print_scenario_outcome(outcome, verbose)
    failures = count_failures(outcomes)
    print(f"\n{len(scenarios) - failures}/{len(scenarios)} scenarios passed")
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
    print(f"{status} {outcome.name}")
    print_outcome_details(outcome, verbose)


def print_outcome_details(outcome: ScenarioOutcome, verbose: bool) -> None:
    """Print outcome details when requested or when the scenario failed."""
    if verbose or not outcome.passed:
        for detail in outcome.details:
            print(f"  - {detail}")


def count_failures(outcomes: list[ScenarioOutcome]) -> int:
    """Return the number of failed scenario outcomes."""
    return sum(1 for outcome in outcomes if not outcome.passed)


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "scenarios",
        nargs="*",
        help="Scenario names to run, or 'all'. Omit to run all scenarios.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print details for passing scenarios too.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    """Command-line entrypoint."""
    args = parse_args(sys.argv[1:] if argv is None else argv)
    scenarios = selected_scenarios(cast(list[str], args.scenarios))
    return run_scenarios(scenarios, bool(args.verbose))


if __name__ == "__main__":
    raise SystemExit(main())

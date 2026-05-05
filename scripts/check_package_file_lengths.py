#!/usr/bin/env python3
"""Fail when production package files exceed the configured line limit."""

from __future__ import annotations

from pathlib import Path


MAX_LINES = 500
CHECKED_PATHS = (
    Path("socSteuerung.py"),
    Path("venus_ess_winter_soc_service"),
)


def checked_python_files() -> list[Path]:
    """Return production Python files covered by the package size gate."""
    files = [path for path in CHECKED_PATHS if path.is_file()]
    files.extend(Path("venus_ess_winter_soc_service").glob("*.py"))
    return sorted(files)


def count_lines(path: Path) -> int:
    """Return the physical line count of one file."""
    return len(path.read_text(encoding="utf-8").splitlines())


def oversized_files(files: list[Path]) -> list[tuple[Path, int]]:
    """Return files exceeding the package line-count limit."""
    return [(path, count_lines(path)) for path in files if count_lines(path) > MAX_LINES]


def print_oversized(offenders: list[tuple[Path, int]]) -> None:
    """Print all package files above the line-count limit."""
    print(f"Package file length gate failed; max is {MAX_LINES} lines:")
    for path, lines in offenders:
        print(f"- {path}: {lines} lines")


def main() -> int:
    """Run the package line-count gate."""
    files = checked_python_files()
    offenders = oversized_files(files)
    if offenders:
        print_oversized(offenders)
        return 1
    print(f"Package file length gate passed: {len(files)} files <= {MAX_LINES} lines.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Fail when any checked Python block is above Radon rank A."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

from radon.complexity import cc_rank, cc_visit
from radon.visitors import Function


CHECKED_PATHS = (
    Path("socSteuerung.py"),
    Path("venus_ess_winter_soc_service"),
    Path("scripts"),
)


def iter_python_files(paths: Iterable[Path]) -> list[Path]:
    """Return checked Python files below files or directories."""
    files: list[Path] = []
    for path in paths:
        files.extend(python_files_for_path(path))
    return sorted(files)


def python_files_for_path(path: Path) -> list[Path]:
    """Return Python files represented by one path."""
    if path.is_dir():
        return python_files_in_dir(path)
    return python_file(path)


def python_file(path: Path) -> list[Path]:
    """Return a single Python file path when the path points to one."""
    if path.is_file() and path.suffix == ".py":
        return [path]
    return []


def python_files_in_dir(path: Path) -> list[Path]:
    """Return Python files below one directory."""
    return [candidate for candidate in path.rglob("*.py") if candidate.is_file()]


def rank_offenders(path: Path) -> list[tuple[Path, Function]]:
    """Return functions in one file whose cyclomatic complexity exceeds A."""
    blocks = cc_visit(path.read_text(encoding="utf-8"))
    return [
        (path, block)
        for block in blocks
        if isinstance(block, Function) and cc_rank(block.complexity) != "A"
    ]


def collect_offenders(files: Iterable[Path]) -> list[tuple[Path, Function]]:
    """Return all non-A functions across checked files."""
    offenders: list[tuple[Path, Function]] = []
    for path in files:
        offenders.extend(rank_offenders(path))
    return offenders


def print_success(files: list[Path]) -> None:
    """Print a short success message for the complexity gate."""
    print(f"Radon complexity gate passed: {len(files)} files, all functions are A.")


def print_failure(offenders: list[tuple[Path, Function]]) -> None:
    """Print all functions that violate the complexity gate."""
    print("Radon complexity gate failed:")
    for path, block in offenders:
        print(f"- {path}:{block.fullname}: {cc_rank(block.complexity)} ({block.complexity})")


def main() -> int:
    """Run the Radon A-only complexity gate."""
    files = iter_python_files(CHECKED_PATHS)
    offenders = collect_offenders(files)
    if offenders:
        print_failure(offenders)
        return 1
    print_success(files)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

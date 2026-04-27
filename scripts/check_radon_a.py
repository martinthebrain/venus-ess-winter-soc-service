#!/usr/bin/env python3
"""Fail when any Python block in socSteuerung.py is above Radon rank A."""

from pathlib import Path

from radon.complexity import cc_rank, cc_visit
from radon.visitors import Function


def main() -> int:
    """Run the Radon A-only complexity gate."""
    source_path = Path("socSteuerung.py")
    blocks = cc_visit(source_path.read_text(encoding="utf-8"))
    offenders = [
        block
        for block in blocks
        if isinstance(block, Function) and cc_rank(block.complexity) != "A"
    ]
    if not offenders:
        print("Radon complexity gate passed: all functions are A.")
        return 0

    print("Radon complexity gate failed:")
    for block in offenders:
        print(f"- {block.fullname}: {cc_rank(block.complexity)} ({block.complexity})")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

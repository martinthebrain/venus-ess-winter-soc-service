# SPDX-License-Identifier: GPL-3.0-or-later
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, Sequence

from .config import SdPathResult
def path_exists(path: Path) -> bool:
    """Return whether a path exists, isolated so SD probing can be unit-tested."""
    return path.exists()

def find_sd_by_path_env() -> SdPathResult:
    """Resolve ESS_SD_PATH when it points to an existing path."""
    sd_path = os.getenv("ESS_SD_PATH", "").strip()
    if not sd_path:
        return None, ""
    p = Path(sd_path)
    if path_exists(p):
        return p, f"SD path: {p}"
    return None, f"SD path not found: {p}"

def find_sd_by_label_env(media_roots: Sequence[Path]) -> SdPathResult:
    """Resolve ESS_SD_LABEL below removable-media roots."""
    sd_label = os.getenv("ESS_SD_LABEL", "").strip()
    if not sd_label:
        return None, ""
    for root in media_roots:
        p = root / sd_label
        if path_exists(p):
            return p, f"SD label: {p}"
    return None, f"SD label not found: {sd_label}"

def find_sd_from_env(media_roots: Sequence[Path]) -> SdPathResult:
    """Resolve an SD card from explicit environment configuration."""
    env_path, env_info = find_sd_by_path_env()
    if env_path or env_info:
        return env_path, env_info
    return find_sd_by_label_env(media_roots)

def find_auto_sd_in_root(root: Path) -> Optional[Path]:
    """Return the first mmcblk-style folder below one media root."""
    if not path_exists(root):
        return None
    for folder in root.iterdir():
        if "mmcblk" in folder.name:
            return folder
    return None

def find_auto_sd(media_roots: Sequence[Path]) -> SdPathResult:
    """Find the first mmcblk-style mount below the usual removable-media roots."""
    try:
        for root in media_roots:
            folder = find_auto_sd_in_root(root)
            if folder:
                return folder, f"SD auto: {folder}"
    except Exception:
        pass
    return None, ""

def get_sd_path() -> SdPathResult:
    """Locate an SD mount via env var, label, or removable-media mountpoints."""
    # Lookup order:
    # 1. ESS_SD_PATH, when set to an existing mount path.
    # 2. ESS_SD_LABEL, resolved below /media or /run/media.
    # 3. First mmcblk* mount below /media or /run/media.
    media_roots = [Path("/media"), Path("/run/media")]
    env_path, env_info = find_sd_from_env(media_roots)
    if env_path:
        return env_path, env_info

    auto_path, auto_info = find_auto_sd(media_roots)
    if auto_path:
        return auto_path, auto_info

    return None, (env_info or "No SD found")

def atomic_write(path: Path, data: str, fsync: bool = False) -> None:
    """Write data through a temporary file and atomically replace the target."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        f.write(data)
        f.flush()
        if fsync:
            os.fsync(f.fileno())
    os.replace(tmp, path)
    if fsync:
        try:
            dir_fd = os.open(str(path.parent), os.O_DIRECTORY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except Exception:
            pass


"""Normalize PDF roots from Windows, WSL, or POSIX paths."""
from __future__ import annotations

import re
from pathlib import Path


_WIN_DRIVE_RE = re.compile(r"^([a-zA-Z]):[/\\](.*)$")


def normalize_host_path(raw: str) -> Path:
    """Turn a user-supplied path into a :class:`Path` this machine can open.

    - ``C:\\Users\\...`` or ``C:/Users/...`` → ``/mnt/c/Users/...`` (WSL)
    - Already ``/mnt/c/...`` or POSIX → unchanged
    """
    s = raw.strip().strip('"').strip("'")
    s = s.replace("\\", "/")
    m = _WIN_DRIVE_RE.match(s)
    if m:
        drive = m.group(1).lower()
        rest = m.group(2).strip("/")
        return Path(f"/mnt/{drive}") / rest
    return Path(s)


def to_display_path(p: Path | str) -> str:
    """Best-effort short display string."""
    try:
        return str(Path(p).expanduser().resolve())
    except Exception:
        return str(p)

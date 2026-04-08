"""Normalize metadata strings from pandas/CSV (NaN, empty, literal 'nan')."""

from __future__ import annotations

from typing import Any

import pandas as pd


def clean_meta_str(v: Any) -> str:
    """Stable string for identity/metadata fields.

    pandas empty cells become ``float('nan')``. In Python, ``bool(np.nan)`` is True, so
    ``np.nan or ""`` incorrectly preserves NaN — later ``str`` becomes ``"nan"`` in CSV.
    """
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    s = str(v).strip()
    if s.lower() in ("nan", "none", "<na>", "null", "nat"):
        return ""
    return s

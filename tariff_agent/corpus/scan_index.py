"""Scan a directory tree for PDFs and write a filings-style index CSV.

Each row is compatible with :func:`run_docling_on_filings` / :func:`run_chunking`:
``filing_id``, ``local_path`` (absolute), plus optional identity columns.
"""
from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any

import pandas as pd

from tariff_agent.corpus.paths import normalize_host_path

_YEAR_RE = re.compile(r"(20[12][0-9])")


def _filing_id(p: Path) -> str:
    return hashlib.md5(str(p.resolve()).encode()).hexdigest()


def _issuer_from_path(p: Path, root: Path) -> str:
    try:
        rel = p.relative_to(root)
        parts = rel.parts
        if len(parts) >= 2:
            return str(parts[0]).replace("_", " ").replace("-", " ")
        stem = p.stem.replace("_", " ")
        return stem[:120]
    except ValueError:
        return p.stem.replace("_", " ")[:120]


def _ticker_slug(name: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", name).strip("_").upper()
    return (slug[:24] + "_ESG") if slug else "UNKNOWN_ESG"


def _year_hint(p: Path, root: Path) -> str:
    for part in list(root.parts) + list(p.parts):
        m = _YEAR_RE.search(part)
        if m:
            return m.group(1) + "-01-01"
    return ""


def scan_pdf_directory(
    docs_dir: str | Path,
    *,
    filing_type: str = "ESG_REPORT",
    min_bytes: int = 100,
) -> list[dict[str, Any]]:
    root = Path(docs_dir).expanduser() if isinstance(docs_dir, Path) else normalize_host_path(str(docs_dir))
    if not root.is_dir():
        raise FileNotFoundError(f"Not a directory: {root}")

    rows: list[dict[str, Any]] = []
    for p in sorted(root.rglob("*.pdf")):
        try:
            if p.stat().st_size < min_bytes:
                continue
        except OSError:
            continue
        issuer = _issuer_from_path(p, root)
        rows.append({
            "filing_id": _filing_id(p),
            "local_path": str(p.resolve()),
            "profile_id": "",
            "ticker": _ticker_slug(issuer),
            "issuer_name": issuer,
            "filing_type": filing_type,
            "filing_date": _year_hint(p, root),
            "naics": "",
            "profile_number": "",
        })
    return rows


def write_corpus_index(
    docs_dir: str | Path,
    out_csv: str | Path,
    *,
    filing_type: str = "ESG_REPORT",
) -> int:
    rows = scan_pdf_directory(docs_dir, filing_type=filing_type)
    out = Path(out_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out, index=False)
    return len(rows)

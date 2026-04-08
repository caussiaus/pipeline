"""Load per-mechanism criteria text for Pass-2 LLM prompt injection.

Criteria text is built by ``scripts/build_sector_profiles.py`` from scraped
proclamation pages and stored as ``raw_data/criteria/{mechanism}.txt``.

If a criteria file is absent (documents not yet fetched), ``load_criteria()``
returns an empty string — the Pass-2 prompt still runs, just without
instrument-specific grounding text.
"""
from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
_DEFAULT_CRITERIA_DIR = ROOT / "raw_data" / "criteria"

# Maximum characters of criteria text to inject per prompt — keeps prompt
# within context budget while providing meaningful legal grounding.
_CRITERIA_MAX_CHARS = 4_000


def load_criteria(mechanism: str, criteria_dir: Path | None = None) -> str:
    """Return criteria text for ``mechanism`` to inject into the Pass-2 system prompt.

    Returns empty string if the file has not been fetched yet.
    Caller is responsible for inserting into the prompt with a section header.
    """
    d = criteria_dir or _DEFAULT_CRITERIA_DIR
    path = Path(d) / f"{mechanism}.txt"
    if not path.is_file():
        return ""
    try:
        text = path.read_text(encoding="utf-8").strip()
        if len(text) > _CRITERIA_MAX_CHARS:
            text = text[:_CRITERIA_MAX_CHARS] + "\n[... truncated ...]"
        return text
    except Exception as exc:
        logger.warning("reference_docs: could not read %s: %s", path, exc)
        return ""


def criteria_header(mechanism: str, text: str) -> str:
    """Wrap criteria text in a labelled prompt section."""
    if not text:
        return ""
    return (
        f"\n\n## ACTIVE TARIFF INSTRUMENTS FOR THIS SECTOR [{mechanism}]\n"
        f"[Source: scraped from authoritative government documents — "
        f"evaluate disclosure accuracy against the instrument text below]\n\n"
        f"{text}\n"
    )


def load_criteria_block(mechanism: str, criteria_dir: Path | None = None) -> str:
    """Return a fully formatted criteria block for prompt injection, or empty string."""
    text = load_criteria(mechanism, criteria_dir)
    return criteria_header(mechanism, text)

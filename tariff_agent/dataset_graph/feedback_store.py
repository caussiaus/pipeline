"""Three-level feedback store for the interactive dataset generation pipeline.

Stores human corrections and schema decisions as JSONL so they can be:
  1. Replayed to enforce schema definitions on future runs
  2. Used as fine-tuning signal (SFT pairs) by build_sft_dataset.py
  3. Audited to understand how the schema evolved across iterations

Three levels — each written to a separate JSONL file:

  schema     output/feedback/{run_id}/schema.jsonl
             One record per schema iteration: the proposed columns, user feedback,
             and whether the user approved or rejected.

  extraction output/feedback/{run_id}/extraction.jsonl
             One record per cell where the user made a correction: the original
             proposed value, evidence, and the override value + reason.

  merge      output/feedback/{run_id}/merge.jsonl
             One record per row where the user resolved a conflict between
             values extracted from different chunks of the same filing.
"""
from __future__ import annotations

import datetime
import json
import uuid
from pathlib import Path
from typing import Any

from tariff_agent.utils.config import get_settings


# ---------------------------------------------------------------------------
# Run identity
# ---------------------------------------------------------------------------

def new_run_id() -> str:
    return str(uuid.uuid4())[:8]


def _feedback_dir(run_id: str) -> Path:
    cfg = get_settings()
    d = cfg.resolve("output/feedback") / run_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def _append(path: Path, record: dict[str, Any]) -> None:
    with open(path, "a") as f:
        f.write(json.dumps(record, default=str, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------------------
# Schema-level feedback
# ---------------------------------------------------------------------------

def log_schema_iteration(
    run_id: str,
    *,
    iteration: int,
    dataset_name: str,
    user_query: str,
    proposed_columns: list[dict],
    user_feedback: str,
    approved: bool,
) -> None:
    """Record what the schema looked like and what the user said about it."""
    record = {
        "level": "schema",
        "run_id": run_id,
        "ts": datetime.datetime.utcnow().isoformat(),
        "iteration": iteration,
        "dataset_name": dataset_name,
        "user_query": user_query,
        "proposed_columns": proposed_columns,
        "user_feedback": user_feedback,
        "approved": approved,
    }
    _append(_feedback_dir(run_id) / "schema.jsonl", record)


# ---------------------------------------------------------------------------
# Extraction-level feedback (cell corrections)
# ---------------------------------------------------------------------------

def log_cell_correction(
    run_id: str,
    *,
    filing_id: str,
    ticker: str,
    field_name: str,
    proposed_value: Any,
    evidence_quote: str | None,
    evidence_pages: str | None,
    evidence_section: str | None,
    override_value: Any,
    override_reason: str,
    reviewer: str = "user",
) -> None:
    """Record when a user overrides a cell value."""
    record = {
        "level": "extraction",
        "run_id": run_id,
        "ts": datetime.datetime.utcnow().isoformat(),
        "filing_id": filing_id,
        "ticker": ticker,
        "field_name": field_name,
        "proposed_value": proposed_value,
        "evidence": {
            "quote": evidence_quote,
            "pages": evidence_pages,
            "section": evidence_section,
        },
        "override_value": override_value,
        "override_reason": override_reason,
        "reviewer": reviewer,
    }
    _append(_feedback_dir(run_id) / "extraction.jsonl", record)


# ---------------------------------------------------------------------------
# Merge-level feedback (conflict resolution)
# ---------------------------------------------------------------------------

def log_merge_decision(
    run_id: str,
    *,
    filing_id: str,
    ticker: str,
    field_name: str,
    conflicting_values: list[dict],  # list of {chunk_id, value, evidence_quote}
    chosen_value: Any,
    choice_reason: str,
    reviewer: str = "user",
) -> None:
    """Record when a user resolves a conflict between values from different chunks."""
    record = {
        "level": "merge",
        "run_id": run_id,
        "ts": datetime.datetime.utcnow().isoformat(),
        "filing_id": filing_id,
        "ticker": ticker,
        "field_name": field_name,
        "conflicting_values": conflicting_values,
        "chosen_value": chosen_value,
        "choice_reason": choice_reason,
        "reviewer": reviewer,
    }
    _append(_feedback_dir(run_id) / "merge.jsonl", record)


# ---------------------------------------------------------------------------
# Read back
# ---------------------------------------------------------------------------

def load_feedback(run_id: str, level: str = "extraction") -> list[dict]:
    path = _feedback_dir(run_id) / f"{level}.jsonl"
    if not path.exists():
        return []
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return records


def list_runs() -> list[dict[str, Any]]:
    """Return summary of all feedback runs (sorted by newest first)."""
    cfg = get_settings()
    base = cfg.resolve("output/feedback")
    if not base.exists():
        return []
    runs = []
    for d in sorted(base.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
        if not d.is_dir():
            continue
        schema_records = load_feedback(d.name, "schema")
        extraction_records = load_feedback(d.name, "extraction")
        merge_records = load_feedback(d.name, "merge")
        runs.append({
            "run_id": d.name,
            "schema_iters": len([r for r in schema_records if r.get("level") == "schema"]),
            "cell_corrections": len(extraction_records),
            "merge_decisions": len(merge_records),
            "dataset_name": schema_records[-1].get("dataset_name", "") if schema_records else "",
            "last_modified": datetime.datetime.fromtimestamp(d.stat().st_mtime).isoformat(),
        })
    return runs


# ---------------------------------------------------------------------------
# SFT export — convert extraction feedback into training pairs
# ---------------------------------------------------------------------------

def export_sft_pairs(run_id: str) -> list[dict[str, Any]]:
    """Convert extraction-level corrections into (system, user, assistant) SFT pairs.

    Each correction where the user changed a cell value becomes:
    - system: "You extract field X from financial filings..."
    - user: the evidence quote (or 'no evidence found')
    - assistant: the correct (override) value with reason

    These pairs can be added to the Pass-2 SFT dataset.
    """
    records = load_feedback(run_id, "extraction")
    pairs = []
    for r in records:
        if r.get("override_value") == r.get("proposed_value"):
            continue  # no change, skip
        field = r.get("field_name", "")
        evidence = r.get("evidence", {}) or {}
        quote = evidence.get("quote") or "No direct evidence found in filing."
        pages = evidence.get("pages") or "unknown"
        proposed = r.get("proposed_value")
        override = r.get("override_value")
        reason = r.get("override_reason", "")

        pairs.append({
            "system": (
                f"You extract the field '{field}' from a financial regulatory filing. "
                "Return only the field value, no explanation."
            ),
            "user": (
                f"Evidence (pages {pages}):\n\"{quote}\"\n\n"
                f"What is the value for '{field}'?"
            ),
            "assistant": str(override),
            "metadata": {
                "run_id": run_id,
                "filing_id": r.get("filing_id"),
                "field_name": field,
                "proposed_value": proposed,
                "override_reason": reason,
            },
        })
    return pairs

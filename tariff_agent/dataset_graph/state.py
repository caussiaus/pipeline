"""LangGraph state types for the interactive dataset generation pipeline."""
from __future__ import annotations

from typing import Any, Literal, TypedDict


# ---------------------------------------------------------------------------
# Schema column definition
# ---------------------------------------------------------------------------

class SchemaColumn(TypedDict, total=False):
    name: str
    type: str           # "boolean", "string", "string|null", "integer", "number|null", …
    description: str
    extraction_instruction: str
    default: Any        # null, false, 0, or ""
    mode: str           # "direct" | "evidence" — see ExtractionMode below


# ---------------------------------------------------------------------------
# Cell-level evidence — one span per field per row
# ---------------------------------------------------------------------------

class EvidenceSpan(TypedDict, total=False):
    chunk_id: str
    quote: str          # verbatim excerpt (≤80 words) that supports the cell value
    page_start: int
    page_end: int
    section_path: str
    relevance: str      # "direct" | "adjacent" | "indirect"


class CellRecord(TypedDict, total=False):
    row_id: str         # == filing_id (canonical row anchor)
    field_name: str
    proposed_value: Any
    evidence: EvidenceSpan | None
    decision: str       # "proposed" | "approved" | "rejected" | "overridden"
    override_value: Any
    override_reason: str


# ---------------------------------------------------------------------------
# Extraction mode
# ---------------------------------------------------------------------------

ExtractionMode = Literal["direct", "evidence"]
# direct   — "find field value in likely source chunks, return it"
# evidence — "collect candidate quotes first, then decide value from evidence"
#            Used for ambiguous / hard-to-find fields where absence ≠ certainty.


# ---------------------------------------------------------------------------
# Pipeline state
# ---------------------------------------------------------------------------

class DatasetState(TypedDict, total=False):
    # ── User intent ──────────────────────────────────────────────────────
    user_query: str
    # ── Corpus scope ─────────────────────────────────────────────────────
    sample_tickers: list[str]   # if set, restrict to these tickers for design validation
    use_sample: bool            # True during interactive design, False for full-corpus run
    # ── Schema design loop ───────────────────────────────────────────────
    schema_iteration: int
    dataset_name: str
    dataset_description: str
    proposed_columns: list[SchemaColumn]
    schema_approved: bool
    schema_feedback: str
    # ── Extraction ───────────────────────────────────────────────────────
    extraction_mode: ExtractionMode     # "direct" or "evidence"
    extraction_done: bool
    rows: list[dict[str, Any]]          # flat dicts: identity + field values + evidence cols
    cells: list[CellRecord]             # structured cell records (parallel to rows)
    dataset_path: str
    # ── Critique loop ────────────────────────────────────────────────────
    critique_text: str
    critique_suggestions: list[str]
    critique_quality: str               # "good" | "ok" | "needs_work"
    export_approved: bool
    # ── Feedback / versioning ────────────────────────────────────────────
    feedback_run_id: str                # UUID written to feedback store per session
    # ── Active corpus (Streamlit / multi-corpus) ──────────────────────────
    corpus_index_csv: str               # override for index; default = Settings.filings_index_path
    corpus_chunks_parquet: str
    corpus_chunks_llm_parquet: str
    datasets_export_dir: str            # where custom dataset CSVs are written
    # ── Internal ─────────────────────────────────────────────────────────
    error: str

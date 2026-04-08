"""Dynamic prompt builders for the interactive dataset generation pipeline.

The dataset pipeline lets a user describe in plain language what they want to
find across the 602-filing corpus, then designs and runs targeted extraction.
"""
from __future__ import annotations

import json
from typing import Any


# ---------------------------------------------------------------------------
# Schema designer — converts a free-text user request into column definitions
# ---------------------------------------------------------------------------

SCHEMA_DESIGNER_SYSTEM_PROMPT = """\
You are a financial data analyst designing structured extraction schemas for \
Canadian company regulatory filings (MD&A, AIF, annual reports).

The user tells you what they want to find across ~600 filings from TSX/NYSE-listed \
Canadian companies (fiscal years 2023-2025). You convert their request into a \
structured extraction schema of 5-8 columns that an LLM can fill from each filing.

RULES:
- Every column must be extractable from raw disclosure text alone — no inference beyond \
what is explicitly stated
- Prefer: boolean, string|null, integer (0-3 scale), or short string-enum fields
- ALWAYS include one "evidence_quote" column (type string|null) to capture verbatim text
- ALWAYS include a "not_found_reason" column (type string|null) that the extractor fills \
when absence is the answer — this is the proof-of-absence record
- Use snake_case column names, ≤30 chars
- "description": ≤20 words, what the dashboard user sees
- "extraction_instruction": verbatim instruction for the extraction LLM — specific keywords, \
phrases, section names to look for
- "default": null, false, 0, or "" as appropriate if evidence is absent

Return ONLY a JSON object:
{
  "dataset_name": "snake_case_slug",
  "description": "one sentence about what this dataset captures",
  "columns": [
    {
      "name": "column_name",
      "type": "boolean|string|string|null|integer|number|number|null",
      "description": "dashboard-facing label",
      "extraction_instruction": "exact LLM instruction — what keywords/phrases/sections to look for",
      "default": null
    }
  ]
}

Limit: 5-8 columns total."""


def build_schema_design_user_prompt(
    user_query: str,
    naics_sectors: list[str],
    schema_feedback: str = "",
) -> str:
    sectors_str = ", ".join(sorted(set(naics_sectors)))[:300]
    feedback_block = ""
    if schema_feedback:
        feedback_block = f"\n\nUser feedback on previous schema:\n{schema_feedback}\n\nRevise accordingly."
    return (
        f"User request: {user_query}\n\n"
        f"Corpus context:\n"
        f"- 602 Canadian regulatory filings (MD&A, AIF), fiscal years 2023-2025\n"
        f"- NAICS sectors present: {sectors_str}\n"
        f"- Documents already passed through a tariff keyword filter + Pass-1 chunk classification\n"
        f"  (each filing has a count of tariff-positive chunks available as context)"
        f"{feedback_block}\n\n"
        "Design the extraction schema. Return JSON only."
    )


# ---------------------------------------------------------------------------
# Extraction prompt — runs per-filing against the approved schema
# ---------------------------------------------------------------------------

EXTRACTION_SYSTEM_PROMPT = """\
You extract specific information from a Canadian company's regulatory filing.

You ONLY see structured evidence blocks — NOT the full PDF. Evidence blocks \
include the chunk text, page numbers, and section path from the original document.

Rules:
- If information is NOT present in the evidence blocks: use the specified default value
- DO NOT invent, infer, or hallucinate beyond what is explicitly stated
- For each field, populate BOTH the value AND its companion _evidence object:
    - _evidence.quote: exact verbatim excerpt from one block (≤80 words) that most
      directly supports your answer; null if absent
    - _evidence.chunk_id: the chunk_id header from the relevant block; null if absent
    - _evidence.page_start / page_end: page numbers from that block's header
    - _evidence.section_path: section from that block's header
- For not_found_reason: if all values are defaults, briefly explain why
  (e.g. "no tariff-relevant passages", "only forward-looking risk boilerplate")
- Return ONE JSON object with exactly the specified keys. No markdown, no explanation."""


def build_extraction_user_prompt(
    columns: list[dict[str, Any]],
    filing_meta: dict[str, Any],
    evidence_blocks: list[dict[str, Any]],
    *,
    all_chunks_count: int,
    keyword_hit_count: int,
    pass1_positive_count: int,
    extraction_mode: str = "direct",
) -> str:
    mode_note = (
        "\nEXTRACTION MODE — evidence-first: collect all candidate quotes first, "
        "then decide the field value based on the weight of evidence.\n"
        if extraction_mode == "evidence"
        else ""
    )

    col_lines = "\n".join(
        f'  "{c["name"]}" ({c["type"]}): {c["description"]}\n'
        f'    Look for: {c["extraction_instruction"]}\n'
        f'    Default if absent: {json.dumps(c.get("default"))}'
        for c in columns
    )

    # Build field list including evidence companions
    field_names_with_evidence: list[str] = []
    for c in columns:
        field_names_with_evidence.append(c["name"])
        field_names_with_evidence.append(f"{c['name']}_evidence")

    if evidence_blocks:
        blocks_text = "\n\n".join(
            (
                f"[Block {i + 1} | chunk_id={b.get('chunk_id', '')} "
                f"| pages {b.get('page_start', '?')}-{b.get('page_end', '?')} "
                f"| section: {b.get('section_path', '')}]\n"
                f"{str(b.get('text', b.get('quote', '')))[:700]}"
            )
            for i, b in enumerate(evidence_blocks[:12])
        )
    else:
        blocks_text = (
            "NO TARIFF-RELEVANT EVIDENCE BLOCKS FOUND.\n"
            "This filing had no chunks flagged by Pass-1. "
            "All fields should default unless general text below contradicts that."
        )

    return (
        f"Filing: {filing_meta.get('ticker', '?')} | "
        f"{filing_meta.get('issuer_name', '')} | "
        f"{filing_meta.get('filing_date', '')} | "
        f"NAICS sector: {filing_meta.get('naics_sector', 'unknown')}\n"
        f"Search coverage: {all_chunks_count} total chunks | "
        f"{keyword_hit_count} keyword hits | "
        f"{pass1_positive_count} tariff-positive (Pass-1)"
        f"{mode_note}\n\n"
        f"Evidence blocks:\n{blocks_text}\n\n"
        f"Fields to extract:\n{col_lines}\n\n"
        f"For EACH field also return a companion {{field}}_evidence object with: "
        f"quote, chunk_id, page_start, page_end, section_path (all null if absent).\n\n"
        f"Return JSON with keys: {json.dumps(field_names_with_evidence)}"
    )


def build_dynamic_json_schema(columns: list[dict[str, Any]], *, with_evidence: bool = True) -> dict[str, Any]:
    """Build an OpenAI/vLLM guided-decoding JSON schema from column definitions.

    When ``with_evidence=True`` (default), each field gets a companion
    ``{name}_evidence`` object containing chunk provenance.
    """
    _type_map: dict[str, Any] = {
        "boolean": {"type": "boolean"},
        "string": {"type": "string"},
        "string|null": {"type": ["string", "null"]},
        "integer": {"type": "integer"},
        "integer|null": {"type": ["integer", "null"]},
        "number": {"type": "number"},
        "number|null": {"type": ["number", "null"]},
    }

    _evidence_schema = {
        "type": ["object", "null"],
        "properties": {
            "quote": {"type": ["string", "null"]},
            "chunk_id": {"type": ["string", "null"]},
            "page_start": {"type": ["integer", "null"]},
            "page_end": {"type": ["integer", "null"]},
            "section_path": {"type": ["string", "null"]},
        },
    }

    properties: dict[str, Any] = {}
    required: list[str] = []
    for col in columns:
        t = col.get("type", "string|null").strip()
        properties[col["name"]] = _type_map.get(t, {"type": ["string", "null"]})
        required.append(col["name"])
        if with_evidence:
            ev_key = f"{col['name']}_evidence"
            properties[ev_key] = _evidence_schema
            required.append(ev_key)

    return {
        "type": "object",
        "additionalProperties": False,
        "properties": properties,
        "required": required,
    }


# ---------------------------------------------------------------------------
# Critique prompt — samples rows and asks if extraction looks correct
# ---------------------------------------------------------------------------

CRITIQUE_SYSTEM_PROMPT = """\
You are a data quality reviewer for a financial NLP pipeline. You receive a \
sample of extracted rows from a 600-filing corpus and assess whether the \
extraction schema and LLM outputs are working correctly.

Identify:
1. Columns that are almost always null/false/empty — might be too specific or poorly worded
2. Columns that are almost always filled — might be too broad (catching boilerplate)
3. Any evidence_quote examples that look like hallucinations or generic boilerplate
4. Schema improvements: renamed columns, split/merged fields, better extraction instructions

Return a brief critique (≤200 words) as plain text, then end with a JSON block:
{"suggested_changes": ["change 1", "change 2", ...], "overall_quality": "good|ok|needs_work"}"""


def build_critique_user_prompt(
    dataset_name: str,
    columns: list[dict[str, Any]],
    sample_rows: list[dict[str, Any]],
) -> str:
    col_summary = json.dumps(
        [{"name": c["name"], "type": c["type"], "description": c["description"]} for c in columns],
        indent=2,
    )
    rows_text = json.dumps(sample_rows[:15], indent=2, default=str)
    return (
        f"Dataset: {dataset_name}\n\n"
        f"Schema columns:\n{col_summary}\n\n"
        f"Sample extracted rows (up to 15):\n{rows_text}\n\n"
        "Critique this extraction. Are the columns well-defined? "
        "Is the data populated correctly based on what you'd expect from regulatory filings?"
    )

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tariff_agent.utils.reference_docs import load_criteria_block

_KEY_QUOTE_ITEM = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "section_path": {"type": "string"},
        "quote": {"type": "string"},
        "signal_type": {"type": "string"},
        "chunk_id": {"type": "string"},
        "page_start": {"type": "integer"},
        "page_end": {"type": "integer"},
    },
    "required": ["section_path", "quote", "signal_type", "chunk_id", "page_start", "page_end"],
}

DOC_SYSTEM_PROMPT = """You consolidate Pass-1 tariff evidence for ONE securities filing. You only see structured extracts — not full PDFs.

## RATE TIMELINE — VERIFY AGAINST FILING DATE
Tariff rates changed multiple times. Use the filing_date to determine which rates were current:
- §232 steel/aluminum: 25% eff. Mar 12 2025 → 50% eff. June 4 2025 (Proc. 10947)
- §232 copper: 50% eff. Aug 1 2025 (copper content only; scrap/ores excluded)
- §232 auto: 25% vehicles eff. Apr 3 2025; 25% parts eff. May 3 2025; medium/heavy added Oct 22 2025
- CVD/AD softwood lumber: AR6 AD rates (Canfor 35.53%, WF 9.65%) + new §232 10% layer eff. Oct 14 2025
- Canadian retaliatory: Phase 1 goods REMOVED Sept 1 2025; steel/aluminum/auto RETAINED
- Canadian steel derivative surtax: 25% global eff. Dec 26 2025 (doors, windows, wire, fasteners, etc.)
- IEEPA tariffs (Mar 4 – Feb 20 2026): struck down by US Supreme Court Feb 20 2026
  Replaced by §122 10% global tariff Feb 24 2026 (CUSMA-compliant goods exempt — net effect same)
A disclosure citing "25% steel tariff" in a filing dated after June 2025 is citing the wrong rate.
A disclosure citing "ongoing Phase 1 retaliatory tariffs" after September 2025 is citing expired measures.

## DISCLOSURE_QUALITY scoring rubric

BOILERPLATE — generic language, no entity-specific content (common in financial services, holding companies):
  "We monitor developments in international trade relations and potential protective measures."
  "Geopolitical events and international tensions may disrupt our supply chains."
  "Trade policy changes may have an adverse effect on our business and results of operations."
  → Score BOILERPLATE even if 'tariff' appears nearby, unless a named program or company-specific impact follows.

SPECIFIC_QUALITATIVE — entity-specific but unquantified:
  "Our copper concentrate shipments to US smelters are subject to Section 232 steel/aluminum duties."
  "We have assessed our exposure to retaliatory Canadian tariffs on US steel inputs used at our Timmins mill."
  "USMCA content rules for light vehicles affect our Ontario stamping plant's eligibility for preferential rates."
  → Score SPECIFIC_QUALITATIVE when a named tariff is tied to this company's named products, facilities, or customers.

SPECIFIC_QUANTITATIVE — dollar amount, margin %, volume, or cost-per-unit explicitly linked to a named tariff:
  "Section 232 duties added approximately CAD 2.1M to our processing costs in Q3 2025."
  "Our AISC increased by USD 18/oz attributable to US aluminum tariffs on plant equipment."
  "We estimate a 150 bps margin impact from retaliatory 25% tariffs on our CAD-denominated steel exports."
  → Score SPECIFIC_QUANTITATIVE when a number ($ / % / $/unit) is directly tied to a named duty or program.

MIXED (use SPECIFIC_QUALITATIVE in output) — some qualitative entity-specific + some boilerplate in same filing:
  → Score SPECIFIC_QUALITATIVE, not BOILERPLATE and not SPECIFIC_QUANTITATIVE.

## SCORING RULES (0–3)
- 0: absent or pure noise/boilerplate
- 1: boilerplate language mentioning tariffs — real risk cannot be assessed
- 2: specific qualitative (named tariff + company-specific input/segment/facility)
- 3: quantified (dollar amount, %, volume explicitly tied to named tariff)

## OUTPUT RULES
- Synthesise from the numbered evidence blocks; do not invent quotes; you may paraphrase only in doc_summary_sentence.
- tariff_direction must reflect net disclosure stance: COST_INCREASE, REVENUE_DECREASE, PASS_THROUGH, MIXED, MINIMAL, NONE.
  If MD&A says costs will be "passed through" but risk factors describe "significant margin pressure", prefer MIXED.
- pass_through_flag / mitigation_flag: true if filing clearly discusses those themes in the evidence.
- mitigation_summary: short phrase if mitigation_flag, else null.
- quantified_impact true if any numeric/quantified tariff cost or benefit appears; set quantified_impact_text to a short fragment or null.
- specific_tariff_programs: deduplicated named programs (e.g. "Section 232", "USMCA").
- key_quotes: 2–4 items: verbatim quote, signal_type, section_path, and chunk_id + page_start + page_end from the matching evidence block. Use chunk_id "" and pages 0 only if impossible.
- first_tariff_section_path: best single section path where tariff discussion starts, or null.

## COUNTER-TARIFF STATUS (counter_tariff_status)
Canada updated its retaliatory tariff schedule on September 1 2025, removing ~CAD 44.2B of goods
from counter-tariff exposure while explicitly retaining steel, aluminum, and auto counter-tariffs.
A further 25% global tariff on steel derivative products took effect December 26 2025.

Set counter_tariff_status based on what the filing discloses about the company's product exposure:
- "active": product/sector is subject to Canadian counter-tariffs as of September 1 2025
  (auto parts HS 8708, steel/aluminum, selected agri-food still on list)
- "removed_sept_2025": product/sector was on the retaliatory list before September 1 2025 but
  was removed in that update (e.g. most consumer goods, some industrial goods, plastics, apparel)
- "steel_aluminum_retained": product is steel or aluminum and the filing notes the counter-tariff
  was explicitly retained (Finance Canada confirmed retention for NAICS 331/332/336 inputs)
- null: filing does not mention Canadian counter-tariffs, or information is insufficient to classify

## CUSMA OFFSET CREDIT (cusma_offset_credit_mentioned) — NAICS 336 ONLY
Proclamation 10925 (90 FR 23768, May 2 2025) amended the §232 auto tariff to allow a CUSMA offset
credit: the dutiable value of a vehicle is reduced by the US-origin content value, so a vehicle with
60% US-origin parts effectively pays §232 on only the remaining 40%. This mechanism is
operationally significant and its absence from a NAICS 336 disclosure is an analytical gap.

Set cusma_offset_credit_mentioned:
- true: filing explicitly discusses the CUSMA content offset / US-origin value credit against §232
- false: filing discusses §232 auto tariffs but does NOT mention the offset credit mechanism
- null: filing is not from a NAICS 336 issuer, or §232 auto tariffs are not discussed at all

- Output one JSON object only. No markdown.
"""


DOC_OUTPUT_JSON_SCHEMA: dict = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "has_tariff_discussion": {"type": "boolean"},
        "tariff_direction": {
            "type": "string",
            "enum": [
                "COST_INCREASE",
                "REVENUE_DECREASE",
                "MIXED",
                "PASS_THROUGH",
                "MINIMAL",
                "NONE",
            ],
        },
        "earnings_tariff_score": {"type": "integer", "minimum": 0, "maximum": 3},
        "supply_chain_tariff_score": {"type": "integer", "minimum": 0, "maximum": 3},
        "macro_tariff_score": {"type": "integer", "minimum": 0, "maximum": 3},
        "pass_through_flag": {"type": "boolean"},
        "mitigation_flag": {"type": "boolean"},
        "mitigation_summary": {"type": ["string", "null"]},
        "quantified_impact": {"type": "boolean"},
        "quantified_impact_text": {"type": ["string", "null"]},
        "specific_tariff_programs": {"type": "array", "items": {"type": "string"}},
        "disclosure_quality": {
            "type": "string",
            "enum": ["BOILERPLATE", "SPECIFIC_QUALITATIVE", "SPECIFIC_QUANTITATIVE"],
        },
        "doc_summary_sentence": {"type": "string"},
        "key_quotes": {
            "type": "array",
            "maxItems": 4,
            "items": _KEY_QUOTE_ITEM,
        },
        "first_tariff_section_path": {"type": ["string", "null"]},
        "counter_tariff_status": {
            "type": ["string", "null"],
            "enum": ["active", "removed_sept_2025", "steel_aluminum_retained", None],
        },
        "cusma_offset_credit_mentioned": {"type": ["boolean", "null"]},
    },
    "required": [
        "has_tariff_discussion",
        "tariff_direction",
        "earnings_tariff_score",
        "supply_chain_tariff_score",
        "macro_tariff_score",
        "pass_through_flag",
        "mitigation_flag",
        "mitigation_summary",
        "quantified_impact",
        "quantified_impact_text",
        "specific_tariff_programs",
        "disclosure_quality",
        "doc_summary_sentence",
        "key_quotes",
        "first_tariff_section_path",
        "counter_tariff_status",
        "cusma_offset_credit_mentioned",
    ],
}


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, default=str)
    except TypeError:
        return str(obj)


def _format_chunk_block(idx: int, r: dict[str, Any]) -> str:
    sec = r.get("section_path", "")
    p0, p1 = r.get("page_start", ""), r.get("page_end", "")
    lines = [
        f"### Evidence block {idx}",
        f"chunk_id: {r.get('chunk_id', '')}",
        f"pages: {p0}-{p1}",
        f"section_path: {sec}",
        f"pass_through_mentioned: {r.get('pass_through_mentioned', False)}",
        f"mitigation_mentioned: {r.get('mitigation_mentioned', False)}",
        f"uncertainty_language: {r.get('uncertainty_language', False)}",
        f"specific_tariff_programs (chunk): {_safe_json(r.get('specific_tariff_programs', []))}",
        f"earnings_evidence: {_safe_json(r.get('earnings_evidence', []))}",
        f"supply_chain_evidence: {_safe_json(r.get('supply_chain_evidence', []))}",
        f"macro_evidence: {_safe_json(r.get('macro_evidence', []))}",
        f"other_tariff_mentions: {_safe_json(r.get('other_tariff_mentions', []))}",
    ]
    return "\n".join(lines)


def _evidence_richness(r: dict[str, Any]) -> int:
    """Score a chunk row by evidence density — used to rank before capping."""
    score = 0
    for k in ("earnings_evidence", "supply_chain_evidence", "macro_evidence"):
        v = r.get(k)
        if isinstance(v, list):
            score += len(v) * 3
    for k in ("other_tariff_mentions", "specific_tariff_programs"):
        v = r.get(k)
        if isinstance(v, list):
            score += len(v)
    for k in ("pass_through_mentioned", "mitigation_mentioned", "uncertainty_language"):
        if r.get(k):
            score += 1
    return score


def build_doc_system_prompt(mechanism: str | None = None, criteria_dir: Path | None = None) -> str:
    """Return the Pass-2 system prompt, optionally appended with scraped criteria text.

    ``mechanism`` is the trade law instrument for this issuer's sector (e.g. 'section_232_auto').
    If ``raw_data/criteria/{mechanism}.txt`` exists (built by ``scripts/build_sector_profiles.py``),
    the relevant proclamation/tariff text is appended so the LLM can evaluate disclosure accuracy
    against the actual legal instrument rather than inferring from training data.
    """
    criteria_block = load_criteria_block(mechanism or "", criteria_dir) if mechanism else ""
    return DOC_SYSTEM_PROMPT + criteria_block


def build_doc_user_prompt(
    filing_meta: dict[str, Any],
    chunk_rows: list[dict[str, Any]],
    max_evidence_blocks: int = 12,
) -> str:
    # Sort richest evidence first, then cap — preserves cross-section diversity for Pass-2.
    rows = sorted(chunk_rows, key=_evidence_richness, reverse=True)
    if len(rows) > max_evidence_blocks:
        rows = rows[:max_evidence_blocks]
    blocks = "\n\n".join(_format_chunk_block(i + 1, r) for i, r in enumerate(rows))
    issuer = filing_meta.get("issuer_name") or filing_meta.get("issuer") or ""
    return f"""Filing meta:
filing_id={filing_meta.get("filing_id")}
ticker={filing_meta.get("ticker")}
issuer_name={issuer}
filing_type={filing_meta.get("filing_type")}
filing_date={filing_meta.get("filing_date")}

Pass-1 structured evidence (only tariff-positive chunks):

{blocks}

Return JSON:
has_tariff_discussion, tariff_direction, earnings_tariff_score, supply_chain_tariff_score, macro_tariff_score,
pass_through_flag, mitigation_flag, mitigation_summary, quantified_impact, quantified_impact_text,
specific_tariff_programs, disclosure_quality, doc_summary_sentence (<=40 words),
key_quotes (2–4 objects: section_path, quote, signal_type, chunk_id, page_start, page_end from evidence headers),
first_tariff_section_path, counter_tariff_status, cusma_offset_credit_mentioned.
"""

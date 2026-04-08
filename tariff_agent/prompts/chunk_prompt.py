from __future__ import annotations

import re

from tariff_agent.state import ChunkRecord

# ---------------------------------------------------------------------------
# Stage-0 keyword gate (Pass 1 pre-filter).  Case-insensitive.
# Organised by sector group so gaps are easy to spot.
# ---------------------------------------------------------------------------
KEYWORD_RULES: list[tuple[re.Pattern[str], str]] = [
    # ── Core trade-policy terms (all sectors) ─────────────────────────────
    (re.compile(r"\btariff(s)?\b", re.I), "tariff"),
    (re.compile(r"\bdut(y|ies)\b", re.I), "duty"),
    (re.compile(r"\bcustoms?\b", re.I), "customs"),
    (re.compile(r"\bimport\s+tax(es)?\b", re.I), "import_tax"),
    (re.compile(r"\bborder\s+(tax|taxes|adjustment)s?\b", re.I), "border_tax"),
    (re.compile(r"\btrade\s+(war|barrier|restriction)s?\b", re.I), "trade_barrier"),
    (re.compile(r"\bUSMCA\b|\bCUSMA\b", re.I), "USMCA_CUSMA"),
    (re.compile(r"\bsection\s*232\b", re.I), "Section_232"),
    (re.compile(r"\bsection\s*301\b", re.I), "Section_301"),
    (re.compile(r"\b232\b.*\b(tariff|duty|steel|aluminum|aluminium)\b", re.I), "232_context"),
    (re.compile(r"\b301\b.*\b(tariff|duty|china)\b", re.I), "301_context"),
    (re.compile(r"\bretaliat(ory|ion)\b", re.I), "retaliatory"),
    (re.compile(r"\b(countervail|anti[- ]?dump)\w*\b", re.I), "trade_remedy_ad_cvd"),
    (re.compile(r"\bIEEPA\b|\bexecutive\s+order\b.*\b(tariff|duty|import)\b", re.I), "IEEPA_exec_order"),
    (re.compile(r"\bsafeguard(s)?\b", re.I), "safeguard"),
    (re.compile(r"\bpreferential\s+(tariff|duty)\b", re.I), "preferential_tariff"),
    (re.compile(r"\brules?\s+of\s+origin\b|\bROO\b", re.I), "rules_of_origin"),
    (re.compile(r"\bGSP\b|\bgeneralized\s+system\s+of\s+preferences\b", re.I), "GSP"),
    (re.compile(r"\bpass[-\s]?through\b", re.I), "pass_through"),
    (re.compile(r"\bsupply[-\s]?chain\s+disruption", re.I), "supply_chain_disruption"),
    (re.compile(r"\bcountry[-\s]?of[-\s]?origin\b", re.I), "country_of_origin"),
    (re.compile(r"\breciprocal\s+(tariff|duty)\b", re.I), "reciprocal_tariff"),
    (re.compile(r"\btrade\s+remed(y|ies)\b", re.I), "trade_remedy"),
    (re.compile(r"\b(steel|aluminum|aluminium)\s+tariff", re.I), "steel_aluminum_tariff"),
    (re.compile(r"\bLiberation\s+Day\b", re.I), "Liberation_Day"),
    (re.compile(r"\bWTO\b.*\b(dispute|panel|tariff)\b|\btariff\b.*\bWTO\b", re.I), "WTO_trade"),
    (re.compile(r"\bcustoms\s+dut", re.I), "customs_duty"),
    (re.compile(r"\bde\s+minimis\b", re.I), "de_minimis"),
    (re.compile(r"\bCETA\b|\bCPTPP\b", re.I), "CETA_CPTPP"),

    # ── Forestry / lumber ─────────────────────────────────────────────────
    (re.compile(r"\bsoftwood\s+lumber\b", re.I), "softwood_lumber"),
    (re.compile(r"\bCVD\b", re.I), "CVD"),
    (re.compile(r"\blumber\s+(duty|dut|tariff)\b|\b(duty|tariff)\b.*\blumber\b", re.I), "lumber_duty"),

    # ── Auto / transportation equipment ───────────────────────────────────
    (re.compile(r"\bfinished\s+vehicle\b.*tariff|tariff.*\bfinished\s+vehicle\b", re.I), "finished_vehicle_tariff"),
    (re.compile(r"\bauto\s+parts?\b.*\bdut|\bdut.*\bauto\s+parts?\b", re.I), "auto_parts_duty"),

    # ── Agriculture / food ────────────────────────────────────────────────
    (re.compile(r"\bdairy.{0,30}quota|quota.{0,30}dairy", re.I), "dairy_quota"),
    (re.compile(r"\bTRQ\b|\btariff[- ]rate\s+quota\b", re.I), "TRQ"),

    # ── Energy / infrastructure ───────────────────────────────────────────
    (re.compile(r"\bBuy\s+American\b", re.I), "Buy_American"),
    (re.compile(r"\bdomestic\s+content\b.*\brequir", re.I), "domestic_content"),

    # ── Retail / consumer goods ───────────────────────────────────────────
    (re.compile(r"\bimport\s+cost|\bcost\s+of\s+import", re.I), "import_cost"),
]


def keyword_terms(text: str) -> list[str]:
    """Return a sorted, deduplicated list of matched keyword labels.

    Uses a ``set`` to accumulate matches, so multiple rules sharing a label or
    two rules both firing on the same text never produce duplicates.
    The sort guarantees deterministic order for ``groupby`` aggregations and
    downstream deduplication checks.
    """
    if not text or not text.strip():
        return []
    found: set[str] = set()
    for pat, label in KEYWORD_RULES:
        if pat.search(text):
            found.add(label)
    return sorted(found)


def keyword_hit(text: str) -> bool:
    return bool(keyword_terms(text))


# ---------------------------------------------------------------------------
# JSON schema for structured Pass-1 output
# ---------------------------------------------------------------------------

_EVID_ITEM_E = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "quote": {"type": "string", "maxLength": 300},
        "signal_type": {
            "type": "string",
            "enum": [
                "COST_INCREASE",
                "MARGIN_COMPRESSION",
                "REVENUE_DECREASE",
                "PASS_THROUGH_CLAIM",
                "VOLUME_IMPACT",
                "BENEFIT_OR_TAILWIND",
                "UNCLEAR",
                "NONE",
            ],
        },
        "magnitude_text": {"type": ["string", "null"]},
    },
    "required": ["quote", "signal_type", "magnitude_text"],
}

_EVID_ITEM_SC = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "quote": {"type": "string", "maxLength": 300},
        "chain_type": {
            "type": "string",
            "enum": [
                "INPUT_SOURCING",
                "LOGISTICS",
                "EQUIPMENT_OR_CAPEX",
                "SUPPLIER_OR_ALT_SOURCE",
                "CUSTOMS_OR_BORDER",
                "OTHER",
                "NONE",
            ],
        },
        "magnitude_text": {"type": ["string", "null"]},
    },
    "required": ["quote", "chain_type", "magnitude_text"],
}

_EVID_ITEM_M = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "quote": {"type": "string", "maxLength": 300},
        "macro_type": {
            "type": "string",
            "enum": [
                "TRADE_POLICY_ESCALATION",
                "REGULATORY_OR_LEGAL",
                "FX_OR_MACRO",
                "DEMAND_DESTRUCTION",
                "OTHER",
                "NONE",
            ],
        },
        "magnitude_text": {"type": ["string", "null"]},
    },
    "required": ["quote", "macro_type", "magnitude_text"],
}

# ---------------------------------------------------------------------------
# System prompt — includes negative anchor rejection rubric
# ---------------------------------------------------------------------------

CHUNK_SYSTEM_PROMPT = """You are a financial analyst extracting tariff-specific evidence from Canadian securities filings (MD&A, AIF, material change reports, news releases).

## POSITIVE SIGNALS — extract these
Extract and return mentions_tariffs=True when the text contains:
- Named tariff programs: Section 232, Section 301, IEEPA, retaliatory tariffs, anti-dumping (AD) / countervailing duties (CVD), softwood lumber duties
- Named trade agreements with operational impact: USMCA/CUSMA content rules, CETA, CPTPP quota obligations, Buy American requirements
- Quantified tariff impacts: dollar amounts, margin %, AISC $/oz, cost-per-unit explicitly attributed to duties
- Named goods subject to specific duties: steel, aluminum, copper concentrate, softwood lumber, auto parts, agricultural TRQ goods
- Explicit pass-through pricing or mitigation strategies tied to a named tariff or duty
- Tariff-driven supply chain changes: sourcing shifts, supplier diversification, border rerouting attributed to duties

## REJECTION RULES — set mentions_tariffs=False for these
Return mentions_tariffs=False unless the chunk ALSO contains a positive signal above:

Generic boilerplate (could appear in any company's disclosure):
- "global trade developments", "trade uncertainty", "geopolitical events"
- "changes in trade policy", "evolving trade dynamics", "trade tensions"
- "supply chain disruptions", "input cost pressures", "market volatility"
- "economic instability", "macroeconomic conditions", "broader market pressures"
- "known trends or uncertainties", "forward-looking statements", "material adverse effect"
- "international trade relations", "protective measures", "trade environment"
- "tariff-related risks", "trade policy changes may affect us" (no named program or quantified impact)
- Any sentence that could appear unchanged in a disclosure from an unrelated industry (e.g., a software company)

Indirect-only language (for financial/services sectors):
- "tariff slowdown could reduce loan quality in affected sectors"
- "macro environment including trade tensions may impact our portfolio"
- These are NOT tariff exposure — they are second-order credit/macro risks

## DECISION RULE
Ask: could this exact sentence appear word-for-word in a disclosure from a company in a completely unrelated industry? If yes → mentions_tariffs=False.
Only return mentions_tariffs=True if the passage is specific to THIS company's tariff exposure.

## OUTPUT RULES
- Set mentions_tariffs true only if the chunk substantiates tariff/trade-policy exposure (not incidental one-word mentions with no substance).
- Every evidence item's quote must be copied verbatim from the chunk, ≤300 characters, a short phrase or single sentence.
- Use evidence arrays: only include items that match that dimension; use NONE / empty arrays where appropriate.
- Output one JSON object matching the schema exactly. No markdown fences, no commentary.
"""

CHUNK_OUTPUT_JSON_SCHEMA: dict = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "mentions_tariffs": {"type": "boolean"},
        "earnings_impact_present": {"type": "boolean"},
        "earnings_evidence": {"type": "array", "items": _EVID_ITEM_E},
        "macro_risk_present": {"type": "boolean"},
        "macro_evidence": {"type": "array", "items": _EVID_ITEM_M},
        "supply_chain_risk_present": {"type": "boolean"},
        "supply_chain_evidence": {"type": "array", "items": _EVID_ITEM_SC},
        "other_tariff_mentions": {"type": "array", "items": {"type": "string"}},
        "pass_through_mentioned": {"type": "boolean"},
        "mitigation_mentioned": {"type": "boolean"},
        "uncertainty_language": {"type": "boolean"},
        "specific_tariff_programs": {"type": "array", "items": {"type": "string"}},
    },
    "required": [
        "mentions_tariffs",
        "earnings_impact_present",
        "earnings_evidence",
        "macro_risk_present",
        "macro_evidence",
        "supply_chain_risk_present",
        "supply_chain_evidence",
        "other_tariff_mentions",
        "pass_through_mentioned",
        "mitigation_mentioned",
        "uncertainty_language",
        "specific_tariff_programs",
    ],
}


# ---------------------------------------------------------------------------
# Sector note injection — mechanism-based, grounded in exposure_vector
# ---------------------------------------------------------------------------

# Mechanisms where generic signal suppression should be applied
_STRICT_REJECTION_MECHANISMS = frozenset({
    "minimal_no_vector",
    "cusma_exempt_services",
    "holding_subsidiary_dependent",
    "pharma_232_pending",
})

_STRICT_NOTE = (
    "NOTE: {sector} ({mechanism}). {exposure_vector} "
    "Apply STRICT rejection — return mentions_tariffs=False unless the text contains "
    "a named tariff program with a direct, company-specific cost or revenue link. "
    "Generic trade risk language must be rejected."
)

_EXPOSURE_NOTE = (
    "SECTOR CONTEXT: {sector} — primary tariff mechanism: {mechanism}.\n"
    "{exposure_vector}\n"
    "When this issuer mentions tariffs, the above instrument is the most likely legal basis. "
    "Flag mentions_tariffs=True when text references this mechanism or its effects."
)


def build_chunk_user_prompt(chunk: ChunkRecord) -> str:
    sector_note = ""
    mechanism = getattr(chunk, "mechanism", "minimal_no_vector") or "minimal_no_vector"
    exposure_vector = getattr(chunk, "exposure_vector", "") or ""
    naics_sector = chunk.naics_sector or "unknown"

    if mechanism in _STRICT_REJECTION_MECHANISMS:
        sector_note = _STRICT_NOTE.format(
            sector=naics_sector,
            mechanism=mechanism,
            exposure_vector=exposure_vector or "No direct tariff transmission pathway.",
        ) + "\n\n"
    elif exposure_vector:
        sector_note = _EXPOSURE_NOTE.format(
            sector=naics_sector,
            mechanism=mechanism,
            exposure_vector=exposure_vector,
        ) + "\n\n"

    terms = chunk.keyword_hit_terms if chunk.keyword_hit_terms else keyword_terms(chunk.text)
    terms_s = ", ".join(terms) if terms else "(none)"

    return (
        f"{sector_note}"
        f"Filing: {chunk.filing_id} | ticker {chunk.ticker} | type {chunk.filing_type} | date {chunk.filing_date}\n"
        f"Section: {chunk.section_path} | pages {chunk.page_start}-{chunk.page_end}\n"
        f"Sector: {naics_sector} | mechanism: {mechanism}\n"
        f"\nStage-0 keyword labels that matched this chunk (context only — apply explicit-language rules): {terms_s}\n"
        f"\nText:\n\"\"\"{chunk.text}\"\"\"\n"
        f"\nReturn JSON with keys:\n"
        f"mentions_tariffs, earnings_impact_present, earnings_evidence,\n"
        f"macro_risk_present, macro_evidence, supply_chain_risk_present, supply_chain_evidence,\n"
        f"other_tariff_mentions, pass_through_mentioned, mitigation_mentioned, uncertainty_language,\n"
        f'specific_tariff_programs (named programs only, e.g. "Section 232", "USMCA Article 32.10", "Liberation Day").\n'
    )

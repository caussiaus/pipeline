from __future__ import annotations

from operator import add
from typing import Annotated, Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator
from typing_extensions import TypedDict

FilingType = Literal[
    "MDA_ANNUAL",
    "MDA_INTERIM",
    "AIF",
    "MATERIAL_CHANGE_REPORT",
    "NEWS_RELEASE",
    "OTHER",
]

_FILING_TYPE_SET = frozenset(
    {
        "MDA_ANNUAL",
        "MDA_INTERIM",
        "AIF",
        "MATERIAL_CHANGE_REPORT",
        "NEWS_RELEASE",
        "OTHER",
    }
)


def normalize_filing_type(raw: object) -> FilingType:
    s = str(raw).strip()
    if s in _FILING_TYPE_SET:
        return s  # type: ignore[return-value]
    return "OTHER"


# Pass 1 — per-span earnings classification
EarningsSignalType = Literal[
    "COST_INCREASE",
    "MARGIN_COMPRESSION",
    "REVENUE_DECREASE",
    "PASS_THROUGH_CLAIM",
    "VOLUME_IMPACT",
    "BENEFIT_OR_TAILWIND",
    "UNCLEAR",
    "NONE",
]
_EARN_SIG = frozenset(
    {
        "COST_INCREASE",
        "MARGIN_COMPRESSION",
        "REVENUE_DECREASE",
        "PASS_THROUGH_CLAIM",
        "VOLUME_IMPACT",
        "BENEFIT_OR_TAILWIND",
        "UNCLEAR",
        "NONE",
    }
)


def normalize_earnings_signal_type(raw: object) -> EarningsSignalType:
    s = str(raw).strip().upper().replace(" ", "_")
    if s in _EARN_SIG:
        return s  # type: ignore[return-value]
    return "UNCLEAR"


SupplyChainSignalType = Literal[
    "INPUT_SOURCING",
    "LOGISTICS",
    "EQUIPMENT_OR_CAPEX",
    "SUPPLIER_OR_ALT_SOURCE",
    "CUSTOMS_OR_BORDER",
    "OTHER",
    "NONE",
]
_SC_SIG = frozenset(
    {
        "INPUT_SOURCING",
        "LOGISTICS",
        "EQUIPMENT_OR_CAPEX",
        "SUPPLIER_OR_ALT_SOURCE",
        "CUSTOMS_OR_BORDER",
        "OTHER",
        "NONE",
    }
)


def normalize_supply_chain_signal_type(raw: object) -> SupplyChainSignalType:
    s = str(raw).strip().upper().replace(" ", "_")
    if s in _SC_SIG:
        return s  # type: ignore[return-value]
    return "OTHER"


MacroSignalType = Literal[
    "TRADE_POLICY_ESCALATION",
    "REGULATORY_OR_LEGAL",
    "FX_OR_MACRO",
    "DEMAND_DESTRUCTION",
    "OTHER",
    "NONE",
]
_MACRO_SIG = frozenset(
    {
        "TRADE_POLICY_ESCALATION",
        "REGULATORY_OR_LEGAL",
        "FX_OR_MACRO",
        "DEMAND_DESTRUCTION",
        "OTHER",
        "NONE",
    }
)


def normalize_macro_signal_type(raw: object) -> MacroSignalType:
    s = str(raw).strip().upper().replace(" ", "_")
    if s in _MACRO_SIG:
        return s  # type: ignore[return-value]
    return "OTHER"


# Pass 2 — filing-level direction (distinct from legacy chunk enums)
FilingTariffDirection = Literal[
    "COST_INCREASE",
    "REVENUE_DECREASE",
    "MIXED",
    "PASS_THROUGH",
    "MINIMAL",
    "NONE",
]
_FT_DIR = frozenset(
    {
        "COST_INCREASE",
        "REVENUE_DECREASE",
        "MIXED",
        "PASS_THROUGH",
        "MINIMAL",
        "NONE",
    }
)


def normalize_filing_tariff_direction(raw: object) -> FilingTariffDirection:
    s = str(raw).strip().upper().replace(" ", "_")
    if s in _FT_DIR:
        return s  # type: ignore[return-value]
    return "NONE"


DisclosureQuality = Literal[
    "BOILERPLATE",
    "SPECIFIC_QUALITATIVE",
    "SPECIFIC_QUANTITATIVE",
]
_DQ = frozenset({"BOILERPLATE", "SPECIFIC_QUALITATIVE", "SPECIFIC_QUANTITATIVE"})

# Status of Canadian counter-tariff exposure for the product(s) discussed in a filing.
# Derived from Finance Canada's September 1 2025 retaliatory list update and the
# December 26 2025 steel derivative tariff announcement.
CounterTariffStatus = Literal[
    "active",               # still subject to Canadian counter-tariffs as of Sept 1 2025
    "removed_sept_2025",    # removed from counter-tariff list on Sept 1 2025
    "steel_aluminum_retained",  # steel/aluminum product; counter-tariffs explicitly retained
]
_CT_STATUS = frozenset({"active", "removed_sept_2025", "steel_aluminum_retained"})


def normalize_disclosure_quality(raw: object) -> DisclosureQuality:
    s = str(raw).strip().upper().replace(" ", "_")
    if s in _DQ:
        return s  # type: ignore[return-value]
    return "BOILERPLATE"


def _clip_quote(s: str, max_len: int = 300) -> str:
    t = str(s).strip()
    return t if len(t) <= max_len else t[: max_len - 1] + "…"


class EarningsEvidenceItem(BaseModel):
    model_config = ConfigDict(extra="ignore")

    quote: str
    signal_type: EarningsSignalType = "UNCLEAR"
    magnitude_text: Optional[str] = None

    @field_validator("quote", mode="before")
    @classmethod
    def _clip(cls, v: object) -> str:
        return _clip_quote(str(v or ""), 300)


class SupplyChainEvidenceItem(BaseModel):
    model_config = ConfigDict(extra="ignore")

    quote: str
    chain_type: SupplyChainSignalType = "OTHER"
    magnitude_text: Optional[str] = None

    @field_validator("quote", mode="before")
    @classmethod
    def _clip(cls, v: object) -> str:
        return _clip_quote(str(v or ""), 300)


class MacroEvidenceItem(BaseModel):
    model_config = ConfigDict(extra="ignore")

    quote: str
    macro_type: MacroSignalType = "OTHER"
    magnitude_text: Optional[str] = None

    @field_validator("quote", mode="before")
    @classmethod
    def _clip(cls, v: object) -> str:
        return _clip_quote(str(v or ""), 300)


class ChunkRecord(BaseModel):
    chunk_id: str
    filing_id: str
    profile_id: str
    ticker: str
    filing_type: FilingType
    filing_date: str
    section_path: str
    page_start: int
    page_end: int
    text: str
    num_tokens: int
    keyword_hit: bool = False
    keyword_hit_terms: list[str] = Field(default_factory=list)
    # JSON list of Docling ProvenanceItem boxes: page_no, l,t,r,b, coord_origin — for PDF evidence highlights.
    source_bboxes_json: str = ""
    naics_sector: str = "unknown"       # Statistics Canada NAICS sector label
    mechanism: str = "minimal_no_vector"  # trade law instrument; replaces sector_tariff_relevance
    exposure_vector: str = ""           # plain-language exposure description for prompts
    cap_earnings: int = 3               # fsQCA cap — earnings dimension
    cap_supply_chain: int = 3           # fsQCA cap — supply chain dimension
    cap_macro: int = 3                  # fsQCA cap — macro dimension


class ChunkLLMOutput(BaseModel):
    model_config = ConfigDict(extra="ignore")

    chunk_id: str
    filing_id: str
    mentions_tariffs: bool = False
    earnings_impact_present: bool = False
    earnings_evidence: list[EarningsEvidenceItem] = Field(default_factory=list)
    macro_risk_present: bool = False
    macro_evidence: list[MacroEvidenceItem] = Field(default_factory=list)
    supply_chain_risk_present: bool = False
    supply_chain_evidence: list[SupplyChainEvidenceItem] = Field(default_factory=list)
    other_tariff_mentions: list[str] = Field(default_factory=list)
    pass_through_mentioned: bool = False
    mitigation_mentioned: bool = False
    uncertainty_language: bool = False
    specific_tariff_programs: list[str] = Field(default_factory=list)
    model_version: str
    inference_timestamp: str
    llm_skipped: bool = False


class KeyQuoteItem(BaseModel):
    """Filing-level cite: links a verbatim quote back to Pass-1 chunk + PDF pages for dashboards."""

    model_config = ConfigDict(extra="ignore")

    section_path: str = ""
    quote: str = ""
    signal_type: str = ""
    chunk_id: str = ""
    page_start: int = 0
    page_end: int = 0


class FilingLLMOutput(BaseModel):
    model_config = ConfigDict(extra="ignore")

    filing_id: str
    profile_id: str
    profile_number: str = ""   # 9-digit SEDAR identifier — universal key (present for all issuers)
    ticker: str
    issuer_name: str = ""
    filing_type: FilingType
    filing_date: str
    fiscal_year: Optional[int] = None
    has_tariff_discussion: bool
    tariff_direction: FilingTariffDirection
    earnings_tariff_score: int = Field(ge=0, le=3)
    supply_chain_tariff_score: int = Field(ge=0, le=3)
    macro_tariff_score: int = Field(ge=0, le=3)
    pass_through_flag: bool = False
    mitigation_flag: bool = False
    mitigation_summary: Optional[str] = None
    quantified_impact: bool = False
    quantified_impact_text: Optional[str] = None
    specific_tariff_programs: list[str] = Field(default_factory=list)
    disclosure_quality: DisclosureQuality = "BOILERPLATE"
    doc_summary_sentence: str
    key_quotes: list[KeyQuoteItem] = Field(default_factory=list)
    first_tariff_section_path: Optional[str] = None
    # Counter-tariff status — classified from Finance Canada Sept 1 2025 and Dec 26 2025 updates
    counter_tariff_status: Optional[CounterTariffStatus] = None
    # NAICS 336 only: does the filing explicitly mention the CUSMA offset credit from
    # Proclamation 10925, which reduces §232 tariff exposure on qualifying US-content vehicles?
    cusma_offset_credit_mentioned: Optional[bool] = None


class IssuerYearRecord(BaseModel):
    ticker: str
    profile_id: str
    fiscal_year: int
    has_tariff_discussion: bool
    max_earnings_tariff_score: int
    max_supply_chain_tariff_score: int
    max_macro_tariff_score: int
    first_tariff_filing_date: str


class HumanReviewRow(BaseModel):
    chunk_id: str
    filing_id: str
    ticker: str
    filing_type: str
    filing_date: str
    section_path: str
    page_start: int
    page_end: int
    label_category: str
    signal_type: str = ""
    supporting_quote: str
    model_prediction: str
    human_label_correct: str = ""
    corrected_label_value: str = ""
    human_comment: str = ""
    confirmed: str = ""


class PipelineState(TypedDict, total=False):
    """LangGraph state: artifacts are mostly on disk; this tracks stage messages."""

    messages: Annotated[list[str], add]
    stage_errors: dict[str, str]
    meta: dict[str, Any]

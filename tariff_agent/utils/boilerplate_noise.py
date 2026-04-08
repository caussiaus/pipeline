"""Curated NI 51-102–style boilerplate for false-positive / robustness testing.

Phrases are generic macro, trade, supply-chain, and risk-factor language that regulators
often flag as non-entity-specific. Use to inject noise into chunks and check that Pass 1
keeps ``mentions_tariffs`` false when no explicit tariff/trade-policy signal exists.

Note: Some phrases contain terms that match ``keyword_hit`` (e.g. “supply chain disruption”).
Those chunks still reach the LLM—useful for testing model precision. Others do not hit the
pre-filter; pair them with a tariff-containing base chunk, or use a forced-LLM eval path.
"""
from __future__ import annotations

import random
from typing import Literal

from tariff_agent.prompts.chunk_prompt import keyword_hit

BoilerplateCategory = Literal[
    "generic_macro",
    "generic_trade",
    "generic_supply",
    "risk_factor",
]

BOILERPLATE_BY_CATEGORY: dict[BoilerplateCategory, list[str]] = {
    "generic_macro": [
        "We continue to monitor economic uncertainties, including potential cost increases from global trade developments.",
        "Global economic conditions and geopolitical events create uncertainty that may affect our operations.",
        "Known trends or uncertainties could have a material adverse effect on our financial condition and results of operations.",
        "Economic instability, inflationary pressures, and market volatility continue to create challenges.",
        "We face risks from changes in macroeconomic conditions, including interest rates and inflation.",
        "Broader market pressures and regulatory changes may impact our business.",
        "The rapidly evolving global political and economic environment contributes to significant market uncertainty.",
        "Trade uncertainty and geopolitical shifts may materially affect future revenue, expenses or projects.",
        "We are exposed to risks arising from international tensions and changing economic conditions.",
        "Uncertainties in the global economy could adversely impact our liquidity and capital resources.",
    ],
    "generic_trade": [
        "Geopolitical events and international tensions may disrupt our operations and supply chains.",
        "Changes in trade policy or government actions could have a material impact on our business.",
        "We monitor developments in international trade relations and potential protective measures.",
        "Evolving trade policies and regulatory changes pose risks to our cross-border activities.",
        "Potential shifts in global trade dynamics may affect demand for our products.",
        "Risks associated with changes in trade agreements or government policies remain a concern.",
        "International trade tensions could lead to increased costs or reduced market access.",
        "We are subject to risks from political and regulatory developments in key markets.",
        "Ongoing geopolitical risks may result in supply chain disruptions or higher input costs.",
        "The imposition of new measures or restrictions by governments could adversely affect our operations.",
    ],
    "generic_supply": [
        "Supply chain disruptions remain a risk due to broader market pressures and external events.",
        "We continue to face challenges related to supply chain constraints and logistics issues.",
        "Increased input costs from global sourcing may pressure our margins.",
        "Access to raw materials and components could be affected by external factors beyond our control.",
        "Potential disruptions in our supply chain may impact production and delivery timelines.",
        "Rising costs of goods and services due to market conditions continue to be monitored.",
        "We are exposed to risks of volatility in commodity prices and sourcing challenges.",
        "Operational changes may be required in response to evolving supply chain conditions.",
        "Supply chain realignment or diversification efforts are ongoing to mitigate risks.",
        "External events could lead to delays or increased costs in our procurement activities.",
    ],
    "risk_factor": [
        "There can be no assurance that these risks will not materialize or have a material adverse effect.",
        "Known and expected trends, demands, events or uncertainties may materially affect future results.",
        "We regularly review and update our risk factors to reflect material developments.",
        "The discussion of risks is not exhaustive and additional risks may emerge.",
        "Management believes these factors could have a material impact on our financial position.",
        "Forward-looking statements are subject to risks and uncertainties that may cause actual results to differ materially.",
        "We face a number of risks that could adversely affect our business, financial condition and results.",
        "Operational measures are being taken to address potential impacts from external uncertainties.",
        "Liquidity and capital resources may be affected by unforeseen events and market conditions.",
        "We assess the broader impact of external factors on our operations and financial condition.",
    ],
}


def all_boilerplate_phrases() -> list[str]:
    """Flat list of all phrases (40), stable order: macro, trade, supply, risk_factor."""
    out: list[str] = []
    for cat in ("generic_macro", "generic_trade", "generic_supply", "risk_factor"):
        out.extend(BOILERPLATE_BY_CATEGORY[cat])  # type: ignore[index]
    return out


def categories_for_section_path(section_path: str) -> list[BoilerplateCategory]:
    """Bias phrase categories from chunk section headings (MD&A-style paths)."""
    s = section_path.upper()
    macro_kw = (
        "MACRO",
        "GEOPOLITICAL",
        "ECONOMIC",
        "POLITICAL",
        "TRADE",
        "GLOBAL",
        "RISK FACTOR",
        "UNCERTAINT",
    )
    supply_kw = ("SUPPLY", "CHAIN", "PROCUREMENT", "LOGISTICS", "INPUT", "SOURCING", "OPERATIONS")
    cats: list[BoilerplateCategory] = []
    if any(k in s for k in macro_kw):
        cats.extend(["generic_macro", "generic_trade", "risk_factor"])
    if any(k in s for k in supply_kw):
        cats.append("generic_supply")
    if not cats:
        return ["generic_macro", "generic_trade", "generic_supply", "risk_factor"]
    seen: set[BoilerplateCategory] = set()
    out: list[BoilerplateCategory] = []
    for c in cats:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def sample_phrases(
    n: int,
    *,
    rng: random.Random | None = None,
    categories: list[BoilerplateCategory] | None = None,
) -> list[str]:
    """Sample up to ``n`` distinct phrases from selected categories."""
    r = rng or random.Random()
    pool: list[str] = []
    cats = categories or list(BOILERPLATE_BY_CATEGORY.keys())
    for c in cats:
        pool.extend(BOILERPLATE_BY_CATEGORY[c])
    if n <= 0 or not pool:
        return []
    n = min(n, len(pool))
    return r.sample(pool, n)


def inject_boilerplate(
    text: str,
    *,
    section_path: str = "",
    n_phrases: int = 2,
    mode: Literal["append", "prepend", "interleave"] = "append",
    rng: random.Random | None = None,
    categories: list[BoilerplateCategory] | None = None,
) -> tuple[str, list[str]]:
    """Return (new_text, phrases_used). Does not modify ``text`` in place.

    * **append** / **prepend**: add sentences as a block.
    * **interleave**: insert one phrase after each paragraph break in ``text`` when possible;
      if there is only one block, behaves like append.
    """
    r = rng or random.Random()
    cat_pool = categories if categories is not None else categories_for_section_path(section_path)
    phrases = sample_phrases(n_phrases, rng=r, categories=cat_pool)
    if not phrases:
        return (text, [])

    glue = "\n\n"
    noise_block = glue.join(phrases)
    base = text.strip()
    if not base:
        return (noise_block, phrases)

    if mode == "prepend":
        return (f"{noise_block}{glue}{text}", phrases)
    if mode == "append":
        return (f"{text}{glue}{noise_block}", phrases)

    # interleave
    parts = [p.strip() for p in text.split("\n\n") if p.strip()]
    if len(parts) <= 1:
        return (f"{text}{glue}{noise_block}", phrases)
    out_parts: list[str] = []
    for i, para in enumerate(parts):
        out_parts.append(para)
        if i < len(phrases):
            out_parts.append(phrases[i])
    if len(phrases) > len(parts) - 1:
        out_parts.extend(phrases[len(parts) - 1 :])
    return (glue.join(out_parts), phrases)


def injection_triggers_keyword_gate(text_after_injection: str) -> bool:
    """True if the pipeline Stage-0 regex gate would send this chunk to the LLM."""
    return keyword_hit(text_after_injection)

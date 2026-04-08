#!/usr/bin/env python3
"""Write distilled criteria injection files for every tariff mechanism.

These are the authoritative, sub-600-token fact blocks that get prepended to
the Pass-2 system prompt for each NAICS-routed filing. They contain ONLY the
operational facts the LLM needs to evaluate disclosure quality:
  - Current rate + effective date
  - Scope (what products/HS chapters)
  - CUSMA treatment (exempt or not)
  - Key mechanisms the company should be discussing
  - What a complete vs. incomplete disclosure looks like

NOT included: Federal Register boilerplate, website chrome, historical recitals,
legal definitions sections, contact information, or proclamation preambles.
Those stay in the raw source files as audit reference only.

Sources:
  - Blakes law firm timeline (raw_data/criteria/__Blakes.pdf)
  - Federal Register proclamations (raw_data/section_232_proclamations/*.html)
  - CFIB live rate table (raw_data/rate_sources/cfib_tariff_summary.html)
  - Official press releases cited in user-provided source table

Usage:
  .venv/bin/python3 scripts/build_distilled_criteria.py
  .venv/bin/python3 scripts/build_distilled_criteria.py --check-tokens
"""
from __future__ import annotations

import argparse
import logging
import textwrap
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
CRITERIA_DIR = ROOT / "raw_data" / "criteria"

# ---------------------------------------------------------------------------
# Distilled criteria — one block per mechanism, sourced from Blakes PDF +
# official tables.  Keep each block under 600 tokens (~2,400 characters).
# Update these when rates or scope change; the raw HTML/PDF files are the
# audit trail.
# ---------------------------------------------------------------------------

_CRITERIA: dict[str, str] = {

    # ------------------------------------------------------------------
    # section_232_auto
    # Sources: Proc. 10908 (FR 2025-05930), Proc. 10925 (FR 2025-07833),
    #          Blakes timeline, medium/heavy (FR 2025-19639),
    #          parts inclusions (FR 2026-05681)
    # ------------------------------------------------------------------
    "section_232_auto": textwrap.dedent("""\
        ACTIVE TARIFF INSTRUMENT: section_232_auto
        LEGAL BASIS: Trade Expansion Act 1962 §232; Proclamation 10908 (Mar 26, 2025)

        RATES & SCOPE:
        - Rate: 25% ad valorem on passenger vehicles, SUVs, minivans, light trucks (HS 8703)
        - Rate: 25% on auto parts (HS 8407, 8408, 8409, 8706–8708) eff. May 3, 2025
        - Medium/heavy duty trucks, buses, and their parts added to scope Oct 22, 2025
        - Parts inclusions window: open Apr 1–14, 2026 — additional parts categories may be added

        CUSMA TREATMENT:
        - CUSMA exemption: NONE — Art. 32.2 national security carve-out explicitly overrides CUSMA
        - CUSMA-compliant auto PARTS (not vehicles): tariff-free eff. May 3, 2025
          Exception: knock-down kits and parts compilations — NOT eligible, full value applies
        - US-content offset credit (Proc. 10925): tariff applies only to the non-US-origin content
          value of the vehicle; US-origin portion is deducted from dutiable basis
          - Documentation required per model year; must be certified to CBP
          - Overstatement penalty: retroactive full-value tariff from Apr 3, 2025

        CANADIAN RETALIATORY:
        - 25% on US motor vehicles (non-CUSMA) and on US-content of CUSMA-compliant US vehicles
          eff. Apr 9, 2025 — RETAINED after Sept 1, 2025 (not removed)

        EVALUATION FOCUS:
        Complete disclosure names the specific tariff, identifies affected vehicle platforms or
        plants, quantifies non-US content, and addresses the CUSMA offset credit. Incomplete
        disclosure uses generic "trade policy uncertainty" language without asset-level detail.
        cusma_offset_credit_mentioned = true only if the filing explicitly discusses the
        US-origin content deduction mechanism from Proclamation 10925.
    """),

    # ------------------------------------------------------------------
    # section_232_steel_aluminum
    # Sources: BIS §232 page, Proc. 10947 (June 4 2025), Proc. 10962 (Apr 2026),
    #          Blakes timeline, BIS 407-category press release
    # ------------------------------------------------------------------
    "section_232_steel_aluminum": textwrap.dedent("""\
        ACTIVE TARIFF INSTRUMENT: section_232_steel_aluminum
        LEGAL BASIS: Trade Expansion Act 1962 §232; Proclamations 9704/9980 (steel/Al),
                     Proc. 10947 (rate increase June 4, 2025), Proc. 10962 (Apr 2026)

        CURRENT RATES:
        - Steel and aluminum: 25% eff. Mar 12, 2025 → INCREASED to 50% eff. June 4, 2025
        - Copper (semi-finished products, intensive copper derivatives): 50% eff. Aug 1, 2025
          Rate on copper content only; ores, concentrates, cathodes, anodes, scrap excluded
        - 407 additional steel/aluminum derivative product categories added Aug 2025
        - Full customs value basis confirmed eff. Apr 6, 2026 (Proc. 10962)
          No partial-content deductions for steel/aluminum — full invoice value is dutiable basis

        CUSMA TREATMENT:
        - CUSMA exemption: NONE — §232 applies regardless of CUSMA origin status

        CANADIAN RETALIATORY:
        - 25% on US steel and aluminum (eff. Mar 13, 2025)
        - RETAINED after Sept 1, 2025 — NOT removed in the cleanup that eliminated Phase 1 goods

        EVALUATION FOCUS:
        Complete disclosure identifies specific metal types, HS classifications, and export or
        import volumes affected. Rate should be cited as 50% (not 25%) for any period after
        June 4, 2025. Disclosures still citing 25% after June 2025 are factually incomplete.
        If company exports steel/aluminum to US: supply chain score ≥ 2 is warranted.
        If company imports US steel/aluminum inputs: check Dec 26 2025 Canadian surtax exposure.
    """),

    # ------------------------------------------------------------------
    # cvd_ad_softwood_lumber
    # Sources: trade.gov CVD AR5, FR 2025-17453 (AD AR6), Global Affairs CA table,
    #          Blakes timeline (Oct 14 2025 §232 layer), FR Oct 14 2025 EO
    # ------------------------------------------------------------------
    "cvd_ad_softwood_lumber": textwrap.dedent("""\
        ACTIVE TARIFF INSTRUMENT: cvd_ad_softwood_lumber
        LEGAL BASIS: CVD case C-122-858; AD case A-122-857 (access.trade.gov);
                     Trade Expansion Act §232 (Oct 14, 2025 overlay)

        CVD RATES — AR5 Final (current cash deposit rates):
        - All-others rate: 14.63%
        - Range by producer: 12.12%–16.82%
        - Producer-specific rates binding for all shipments from that company

        AD RATES — AR6 Amended Final (FR 2025-17453, Sept 11, 2025):
        - Canfor Corporation: 35.53%
        - West Fraser Timber: 9.65%
        - All-others: varies — see access.trade.gov case A-122-857 for full producer list

        §232 OVERLAY — NEW, eff. Oct 14, 2025 (no CUSMA exemption):
        - 10% additional tariff on all softwood timber and lumber imports
        - 25% on upholstered wooden furniture; 25% on kitchen cabinets and vanities
          (increasing to 30% furniture / 50% cabinets+vanities eff. Jan 1, 2026)
        - This §232 layer is additive on top of CVD/AD orders — not stacked with IEEPA

        COMBINED EXPOSURE (all-others producer, softwood lumber):
        CVD 14.63% + AD ~20% + §232 10% ≈ 44–50% total tariff load

        EVALUATION FOCUS:
        Complete disclosure names the specific CVD/AD case and their producer-specific rate.
        Incomplete disclosure cites generic "trade tariffs" without distinguishing CVD, AD,
        and §232 layers. The Oct 14, 2025 §232 addition is a material new event — any filing
        dated after Oct 2025 should reference it. Producer-specific rate disclosure scores higher
        than all-others rate disclosure on disclosure_quality.
    """),

    # ------------------------------------------------------------------
    # energy_differential
    # Sources: Blakes timeline, CFIB table, Mar 7 2025 EO (CUSMA exemption)
    # Note: IEEPA basis struck down Feb 20, 2026 by SCOTUS; §122 replacement Feb 24, 2026
    # ------------------------------------------------------------------
    "energy_differential": textwrap.dedent("""\
        ACTIVE TARIFF INSTRUMENT: energy_differential
        LEGAL BASIS: Originally IEEPA; struck down by US Supreme Court Feb 20, 2026.
                     Replaced by §122 Trade Act of 1974 (10% global tariff) eff. Feb 24, 2026.

        CURRENT RATE: 10% on non-CUSMA-compliant energy products and potash
        - In scope: crude oil, natural gas, LNG, refined products, coal, uranium
        - Potash: 10% (reduced from 25% effective Mar 7, 2025)
        - CUSMA-compliant Canadian energy: EXEMPT under both IEEPA and §122 replacement
          Net exposure for CUSMA-qualifying Canadian energy issuers: effectively 0%

        RATE CONTINUITY NOTE:
        IEEPA tariff struck down Feb 20, 2026. §122 replacement imposes same 10% rate.
        Valid for up to 150 days pending Congressional approval. For filings dated after
        Feb 24, 2026: the applicable legal authority changed but the economic exposure is identical.

        EVALUATION FOCUS:
        Does the issuer disclose whether their product qualifies for CUSMA-origin treatment?
        Pipeline operators: address Buy American requirements on cross-border infrastructure.
        Refiners with US operations: distinguish between upstream (10% energy exposure) and
        downstream (CUSMA-protected refined products). Rate quantification as $/boe or $/mcf
        or netback reduction is the standard for SPECIFIC_QUANTITATIVE scoring.
    """),

    # ------------------------------------------------------------------
    # input_cost_steel_derivative
    # Sources: Finance Canada Nov 26/Dec 12/Dec 26 2025 announcements (Blakes timeline),
    #          Canada Gazette amendments
    # ------------------------------------------------------------------
    "input_cost_steel_derivative": textwrap.dedent("""\
        ACTIVE TARIFF INSTRUMENT: input_cost_steel_derivative
        LEGAL BASIS: Canadian Customs Tariff; Order-in-Council (Dec 26, 2025)

        CANADIAN SURTAX ON STEEL DERIVATIVE IMPORTS, eff. Dec 26, 2025:
        - 25% Canadian tariff on ALL global imports of steel derivative products
        - Products in scope (C$10B+): doors, windows, wire, fasteners, bridges, wind towers,
          structural steel components, rebar derivatives, boilers, radiators
        - CUSMA carve-out maintained for US steel used in exempt manufacturing categories

        TRQ REDUCTIONS (eff. Dec 26, 2025):
        - Non-FTA partners: quotas reduced from 50% to 20% of 2024 import levels
          Over-quota volumes: 50% surtax
        - Non-CUSMA FTA partners: quotas reduced from 100% to 75% of 2024 import levels

        REMISSION EXPIRY:
        - Canadian retaliatory tariff remission on US steel for manufacturing expired Jan 31, 2026
          EXCEPT: steel for auto/aerospace manufacturing and ALL aluminum → extended to Jun 30, 2026

        US EXPOSURE (for Canadian manufacturers importing US steel inputs):
        - §232 50% on US-origin steel still applies to US exporters; buying from US sources
          is no cheaper than elsewhere — domestic sourcing pressure increases
        - 407 additional derivative categories added to US §232 scope Aug 2025

        EVALUATION FOCUS:
        The Dec 26, 2025 steel derivative tariff is a NEW event distinct from earlier §232 exposure.
        Filings after Jan 2026 should address it separately. If management only references
        historical §232 steel exposure without the Dec 26 Canadian surtax, the disclosure is
        incomplete for post-Jan 2026 periods.
    """),

    # ------------------------------------------------------------------
    # cusma_agri_conditional
    # Sources: Blakes timeline, Finance Canada Sept 1 2025 announcement
    # ------------------------------------------------------------------
    "cusma_agri_conditional": textwrap.dedent("""\
        ACTIVE TARIFF INSTRUMENT: cusma_agri_conditional
        LEGAL BASIS: CUSMA Chapter 2 (market access); Annex 2-B (tariff schedule)

        CUSMA PROTECTION:
        - Most Canadian agricultural and food products qualify for CUSMA preferential treatment
        - CUSMA-origin goods enter US tariff-free; Rules of Origin (CUSMA Ch. 4) apply
        - Products must meet regional value content thresholds to qualify

        CANADIAN RETALIATORY TARIFFS — CURRENT STATUS (post Sept 1, 2025):
        - Phase 1 retaliatory list (Mar 4 – Sept 1, 2025): US agri-food, OJ, bourbon, processed
          foods — ALL REMOVED effective September 1, 2025
        - After Sept 1, 2025: NO Canadian retaliatory tariffs apply to US agri-food imports
          (only steel, aluminum, and autos retained)

        RESIDUAL EXPOSURE:
        - US inputs (packaging, machinery) subject to §232 steel/aluminum duties if not CUSMA
        - Cannabis, specialty foods: check for US market access vs. retaliatory tariff impacts

        EVALUATION FOCUS:
        Any filing that discloses ongoing exposure to "Canadian retaliatory tariffs on US food
        products" for a period after September 1, 2025, is citing expired measures.
        Flag these as potentially misleading or stale disclosures. CUSMA-origin food producers
        should have minimal US tariff exposure — high scores require specific evidence of
        Rules-of-Origin failure or non-qualifying input sourcing.
    """),

    # ------------------------------------------------------------------
    # cusma_exempt_services
    # Sources: CUSMA Ch. 15 (financial services), Ch. 16 (business persons)
    # ------------------------------------------------------------------
    "cusma_exempt_services": textwrap.dedent("""\
        ACTIVE TARIFF INSTRUMENT: cusma_exempt_services
        LEGAL BASIS: CUSMA Chapters 15 (financial services), 16 (temporary entry of business persons)

        SERVICES EXEMPTION:
        - Professional services, financial services, IT/software, digital content: NO product tariff
        - CUSMA Ch. 15 explicitly covers cross-border financial services
        - Ch. 16 covers temporary entry of business persons — not affected by goods tariffs

        SUBSIDIARY/HOLDING EXPOSURE:
        - If issuer holds subsidiaries in manufacturing, mining, or resource extraction, tariff
          exposure exists at the subsidiary level and should flow through to consolidated financials
        - Services companies that also manufacture products (e.g., software + hardware) face
          tariff exposure on the goods component only

        EVALUATION FOCUS:
        For a pure services issuer: has_tariff_discussion should be false unless they explicitly
        note demand-side impact (reduced client capex, client cost pressures) or subsidiary exposure.
        If a services company scores high (2-3) without naming a specific downstream mechanism,
        the disclosure is likely BOILERPLATE — downgrade to BOILERPLATE regardless of language
        specificity. A services firm citing "global trade uncertainty" with no client-specific
        or subsidiary-specific content is the canonical boilerplate pattern for this sector.
    """),

    # ------------------------------------------------------------------
    # holding_subsidiary_dependent
    # Sources: General sector logic
    # ------------------------------------------------------------------
    "holding_subsidiary_dependent": textwrap.dedent("""\
        ACTIVE TARIFF INSTRUMENT: holding_subsidiary_dependent
        SCOPE: Management companies and holding entities with no direct product tariff exposure

        EVALUATION APPROACH:
        - No direct tariff exposure applies to the issuer's own operations
        - Tariff risk exists ONLY through consolidated subsidiary operations
        - If subsidiary operates in manufacturing (NAICS 33x), mining (21x), energy (211),
          or forestry (113/321), the holding company inherits exposure through earnings consolidation

        WHAT TO LOOK FOR:
        - Segment reporting that breaks out subsidiary tariff impact by operating unit
        - Management commentary on subsidiary-level cost or revenue impacts
        - Any discussion of altering subsidiary operations in response to tariffs

        EVALUATION FOCUS:
        If the filing discusses only the issuer entity and contains no subsidiary-level tariff
        disclosure: return has_tariff_discussion = false.
        If subsidiary exposure is disclosed: rate the evidence quality based on the subsidiary's
        sector, not the holding company's classification. A holding company that names a
        manufacturing subsidiary and quantifies its §232 exposure earns SPECIFIC_QUANTITATIVE.
    """),

    # ------------------------------------------------------------------
    # demand_compression
    # Sources: General macro transmission, Blakes timeline
    # ------------------------------------------------------------------
    "demand_compression": textwrap.dedent("""\
        ACTIVE TARIFF INSTRUMENT: demand_compression
        MECHANISM: Indirect tariff transmission via reduced consumer/business demand

        CURRENT CONTEXT:
        - US baseline 10% "reciprocal" tariff eff. Apr 5, 2025 (does not apply to Canada/Mexico)
        - §122 replacement 10% global tariff eff. Feb 24, 2026 (CUSMA-compliant goods exempt)
        - Domestic Canadian inflation from retaliatory tariffs (steel, aluminum, autos retained)
        - Cross-border trade volume reduction reduces demand for transportation, logistics, retail

        TYPICAL EXPOSURE CHANNELS:
        - Retail: US consumer spending reduction → lower discretionary demand for Canadian goods
        - Transportation/logistics: cross-border freight volume decline
        - Accommodation/food services: US visitor decline from travel sentiment, exchange rate
        - Wholesale: margin compression on US-sourced goods re-sold in Canada

        EVALUATION FOCUS:
        Demand compression is a macro-level, indirect effect. Score BOILERPLATE unless the
        issuer specifically names a tariff mechanism that directly affects their cost base or
        revenue line. Generic statements about "trade uncertainty reducing demand" without
        naming a specific tariff program are BOILERPLATE regardless of language sophistication.
        Score earnings_tariff_score ≥ 2 only if a specific trade mechanism is named and its
        effect on this company's revenue is described with entity-specific evidence.
    """),

    # ------------------------------------------------------------------
    # minimal_no_vector
    # ------------------------------------------------------------------
    "minimal_no_vector": textwrap.dedent("""\
        ACTIVE TARIFF INSTRUMENT: minimal_no_vector
        SECTOR: No credible direct tariff transmission pathway identified for this NAICS code

        EVALUATION APPROACH:
        - Apply strict rejection criteria for tariff relevance
        - Return has_tariff_discussion = false unless the company explicitly names a specific tariff
          program and directly links it to their own costs or revenues
        - Generic language about "trade policy", "geopolitical risk", or "supply chain uncertainty"
          without naming a specific duty instrument should NOT score as tariff disclosure

        WHAT COUNTS AS REAL DISCLOSURE (for any sector):
        - Named tariff program (Section 232, CVD/AD, CUSMA retaliatory) AND
        - Company-specific impact (named product, facility, customer, or cost line) AND
        - At minimum one directional claim (cost increase, revenue decrease, margin impact)

        BOILERPLATE PATTERNS TO REJECT:
        "We monitor developments in international trade" — not a disclosure
        "Trade policy changes may affect our business" — not a disclosure
        "Geopolitical tensions may disrupt supply chains" — not a disclosure
        Only escalate to true if all three conditions above are met explicitly.
    """),
}

# ---------------------------------------------------------------------------
# Token estimation — rough approximation (GPT-2 tokenizer basis, ×1.1 safety)
# ---------------------------------------------------------------------------

def _estimate_tokens(text: str) -> int:
    words = len(text.split())
    return int(words / 0.75 * 1.1)


def write_distilled_criteria(criteria_dir: Path, check_tokens: bool = False) -> None:
    criteria_dir.mkdir(parents=True, exist_ok=True)
    all_ok = True
    for mechanism, text in _CRITERIA.items():
        out = criteria_dir / f"{mechanism}.txt"
        out.write_text(text.strip() + "\n")
        tokens = _estimate_tokens(text)
        status = "OK" if tokens <= 600 else "OVER_BUDGET"
        if tokens > 600:
            all_ok = False
        log.info(
            "%s %s → %s (~%d tokens, %d chars)",
            status,
            mechanism,
            out.name,
            tokens,
            len(text),
        )
    if check_tokens and not all_ok:
        raise SystemExit("One or more criteria files exceed 600-token budget — trim before next run.")
    if all_ok:
        log.info("All criteria within 600-token budget.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--check-tokens",
        action="store_true",
        help="Exit non-zero if any criteria file exceeds 600 estimated tokens",
    )
    parser.add_argument(
        "--criteria-dir",
        default=str(CRITERIA_DIR),
        help=f"Output directory for criteria .txt files (default: {CRITERIA_DIR})",
    )
    args = parser.parse_args()
    write_distilled_criteria(Path(args.criteria_dir), check_tokens=args.check_tokens)


if __name__ == "__main__":
    main()

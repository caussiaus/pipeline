#!/usr/bin/env python3
"""Download raw tariff reference documents from authoritative sources.

Output layout (all relative to repo root):
  raw_data/retaliatory_tariffs.xlsx          — Finance Canada counter-tariff schedule
  raw_data/retaliatory_tariffs_page.html     — full HTML backup of the schedule page
  raw_data/hs_naics_concordance_meta.json    — StatCan concordance manifest (see note below)
  raw_data/section_232_proclamations/        — BIS §232 proclamation summary pages
  raw_data/cusma/chapter_02.html             — CUSMA Chapter 2 (tariff treatment) HTML

StatCan concordance note:
  The HS-NAICS concordance (catalogue 12-501-X) is a 3.2 MB PDF; pdfplumber is not
  in this env's dependencies. Run --concordance-pdf to download the PDF for manual
  extraction, or supply your own CSV at raw_data/hs_naics_concordance.csv with columns:
    hs_code (string, 4–6 digits) | naics_code (string, 6 digits) | description

Usage:
  .venv/bin/python3 scripts/fetch_tariff_reference_docs.py
  .venv/bin/python3 scripts/fetch_tariff_reference_docs.py --concordance-pdf
  .venv/bin/python3 scripts/fetch_tariff_reference_docs.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "raw_data"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; tariff-sedar-pipeline/1.0; "
        "+https://github.com/your-org/tariff-sedar-pipeline)"
    ),
}

# ---------------------------------------------------------------------------
# Source manifest — edit these if official URLs change
# ---------------------------------------------------------------------------
SOURCES = {
    # Finance Canada — always fetch; the page was updated Sept 1 2025 when $44.2B of goods
    # were removed while steel/aluminum/auto counter-tariffs were retained.
    "retaliatory_tariffs_html": {
        "url": (
            "https://www.canada.ca/en/department-finance/programs/international-trade-finance-policy"
            "/canadas-response-us-tariffs/complete-list-us-products-subject-to-counter-tariffs.html"
        ),
        "dest": RAW / "retaliatory_tariffs_page.html",
        "label": "Finance Canada retaliatory tariff list (HTML — Sept 1 2025 revision)",
    },
    # Steel/aluminum §232
    "s232_steel_aluminum": {
        "url": (
            "https://www.bis.gov/about-bis/bis-leadership-and-offices/SIES"
            "/section-232-investigations/section-232-steel-aluminum"
        ),
        "dest": RAW / "section_232_proclamations" / "steel_aluminum.html",
        "label": "BIS §232 steel and aluminum proclamation page",
    },
    # Copper + Al/steel expansion (April 2026)
    "s232_copper": {
        "url": (
            "https://www.whitehouse.gov/presidential-actions/2026/04/"
            "strengthening-actions-taken-to-adjust-imports-of-aluminum-steel-and-copper-into-the-united-states/"
        ),
        "dest": RAW / "section_232_proclamations" / "copper_expansion.html",
        "label": "White House §232 copper / Al/steel expansion (Apr 2026)",
    },
    # Auto §232 amendment — Proclamation 10925 (90 FR 23768, May 2 2025)
    # Defines the CUSMA offset credit: US-origin content reduces the §232 tariff basis.
    # Any NAICS 336 disclosure that omits this mechanism is analytically incomplete.
    "s232_auto_amendment_10925": {
        "url": (
            "https://www.federalregister.gov/documents/2025/05/02/2025-07833/"
            "amendments-to-adjusting-imports-of-automobiles-and-automobile-parts-into-the-united-states"
        ),
        "dest": RAW / "section_232_proclamations" / "auto_amendment_10925.html",
        "label": "§232 auto amendment — Proclamation 10925 (CUSMA offset credit mechanism)",
    },
    # Auto parts inclusions window — opens additional parts categories for §232, eff. 2026
    "s232_auto_parts_inclusions_2026": {
        "url": (
            "https://www.federalregister.gov/documents/2026/03/24/2026-05681/"
            "notice-of-the-opening-of-the-inclusions-window-for-the-section-232-automobile-parts-tariff"
        ),
        "dest": RAW / "section_232_proclamations" / "auto_parts_inclusions_2026.html",
        "label": "§232 auto parts inclusions window (2026 — expands parts scope)",
    },
    # Medium/heavy duty vehicles §232 extension (October 2025)
    "s232_medium_heavy_vehicles": {
        "url": (
            "https://www.federalregister.gov/documents/2025/10/22/2025-19639/"
            "adjusting-imports-of-medium--and-heavy-duty-vehicles-medium--and-heavy-duty-vehicle-parts-and-buses"
        ),
        "dest": RAW / "section_232_proclamations" / "medium_heavy_vehicles.html",
        "label": "§232 medium/heavy duty vehicles and buses (Oct 2025)",
    },
    # Finance Canada December 26 2025 steel derivative tariffs — new 25% global tariff on
    # steel derivative products (NAICS 332 / downstream fabricators).
    # Note: if this 404s, build_sector_profiles.py falls back to section_232_steel_aluminum
    # text for the input_cost_steel_derivative criteria — pipeline still runs correctly.
    "ca_steel_derivatives_dec2025": {
        "url": (
            "https://www.canada.ca/en/department-finance/news/2025/12/"
            "canada-imposes-tariffs-on-steel-derivative-products.html"
        ),
        "dest": RAW / "section_232_proclamations" / "ca_steel_derivatives_dec2025.html",
        "label": "Finance Canada Dec 26 2025 steel derivative tariffs (25% global)",
    },
    # CUSMA Chapter 2
    "cusma_chapter_02": {
        "url": (
            "https://www.international.gc.ca/trade-commerce/trade-agreements-accords-commerciaux/"
            "agr-acc/cusma-aceum/text-texte/02.aspx?lang=eng"
        ),
        "dest": RAW / "cusma" / "chapter_02.html",
        "label": "CUSMA Chapter 2 (National Treatment and Market Access for Goods)",
    },
    "cusma_tariff_schedule": {
        "url": (
            "https://www.international.gc.ca/trade-commerce/trade-agreements-accords-commerciaux/"
            "agr-acc/cusma-aceum/text-texte/tariff-schedule-liste-canada.aspx?lang=eng"
        ),
        "dest": RAW / "cusma" / "tariff_schedule_page.html",
        "label": "CUSMA Annex 2-B Canada Tariff Schedule (index page)",
    },

    # ---------------------------------------------------------------------------
    # Rate-confirmation sources — authoritative tables for current rates
    # ---------------------------------------------------------------------------

    # CFIB live table — cleanest single summary of all current rates and dates
    "cfib_tariff_summary": {
        "url": "https://www.cfib-fcei.ca/en/site/us-tariffs",
        "dest": RAW / "rate_sources" / "cfib_tariff_summary.html",
        "label": "CFIB live tariff rate summary table",
    },

    # §232 auto — original proclamation as published in Federal Register (FR 2025-05930)
    # Different from the WH page; includes the formal HTSUS scope table
    "fr_s232_auto_original": {
        "url": (
            "https://www.federalregister.gov/documents/2025/04/03/2025-05930/"
            "adjusting-imports-of-automobiles-and-automobile-parts-into-the-united-states"
        ),
        "dest": RAW / "section_232_proclamations" / "auto_fr_2025-05930.html",
        "label": "FR §232 auto original proclamation (FR 2025-05930, HTSUS scope table)",
    },

    # §232 steel/aluminum 50% rate — Proclamation 10947 implementation (June 4, 2025)
    "fr_s232_50pct_proc10947": {
        "url": (
            "https://www.federalregister.gov/documents/2025/06/09/2025-10524/"
            "adjusting-imports-of-aluminum-and-steel-into-the-united-states"
        ),
        "dest": RAW / "section_232_proclamations" / "steel_aluminum_50pct_proc10947.html",
        "label": "FR §232 steel/aluminum 50% rate — Proclamation 10947 (June 4, 2025)",
    },

    # BIS press release: 407 additional steel/aluminum derivative product categories (Aug 2025)
    "bis_s232_407_derivatives": {
        "url": (
            "https://www.bis.gov/press-release/"
            "department-commerce-adds-407-product-categories-steel-aluminum-tariffs"
        ),
        "dest": RAW / "section_232_proclamations" / "bis_407_derivatives_aug2025.html",
        "label": "BIS §232 407 additional derivative categories (Aug 2025)",
    },

    # Softwood lumber — AR6 anti-dumping final results (FR 2025-17453, Sept 11, 2025)
    # Canfor: 35.53%, West Fraser: 9.65%; all-others corrected
    "fr_lumber_ad_ar6": {
        "url": (
            "https://www.federalregister.gov/documents/2025/09/11/2025-17453/"
            "certain-softwood-lumber-products-from-canada-amended-final-results-of-antidumping-duty"
        ),
        "dest": RAW / "softwood_lumber" / "lumber_ad_ar6_fr_2025-17453.html",
        "label": "FR softwood lumber AD AR6 final rates (Canfor 35.53%, WF 9.65%)",
    },

    # Global Affairs Canada combined CVD+AD rates table — clearest per-producer summary
    "ca_softwood_lumber_combined": {
        "url": "https://www.international.gc.ca/controls-controles/softwood-bois_oeuvre/recent.aspx?lang=eng",
        "dest": RAW / "softwood_lumber" / "ca_softwood_combined_rates.html",
        "label": "Global Affairs Canada combined CVD+AD softwood lumber rates",
    },

    # Canadian Gazette: Phase 1 surtax order (original legal instrument, Mar 4, 2025)
    "ca_gazette_phase1_surtax": {
        "url": "https://gazette.gc.ca/rp-pr/p2/2025/2025-03-12/html/sor-dors66-eng.html",
        "dest": RAW / "retaliatory_legal" / "gazette_phase1_surtax_sor66.html",
        "label": "Canada Gazette SOR/2025-66 Phase 1 surtax order (Mar 4, 2025)",
    },

    # Blakes law firm live timeline — curated index with exact dates, legal instruments, and links
    # for every tariff event in both directions. NOTE: PDF copy is at raw_data/criteria/__Blakes.pdf
    "blakes_timeline": {
        "url": "https://www.blakes.com/insights/us-canada-tariffs-timeline-of-key-dates-and-documents/",
        "dest": RAW / "rate_sources" / "blakes_timeline.html",
        "label": "Blakes law firm US-Canada tariff timeline (live HTML)",
    },
}

# Sources requiring URL fallback — tried in order until one succeeds
SOURCES_WITH_FALLBACK: list[dict] = [
    {
        "label": "Finance Canada retaliatory tariff list (XLSX)",
        "dest": RAW / "retaliatory_tariffs.xlsx",
        "candidates": [
            # Current and historical locations — Finance Canada moves these periodically
            "https://www.canada.ca/content/dam/fin/migration/consultations/complete-list-us-products-subject-counter-tariffs-en.xlsx",
            "https://www.canada.ca/content/dam/canada/finance/documents/complete-list-us-products-subject-counter-tariffs-en.xlsx",
            "https://www.fin.gc.ca/n-nr/2025/docs/complete-list-us-products-subject-counter-tariffs-en.xlsx",
        ],
    },
    {
        "label": "White House §232 auto proclamation 10908",
        "dest": RAW / "section_232_proclamations" / "auto_proclamation_10908.html",
        "candidates": [
            # Live URL — note "autombile" typo is present in the actual WH slug
            "https://www.whitehouse.gov/presidential-actions/2025/03/adjusting-imports-of-automobiles-and-autombile-parts-into-the-united-states/",
            # Corrected spelling as alternate WH path
            "https://www.whitehouse.gov/presidential-actions/2025/03/adjusting-imports-of-automobiles-and-automobile-parts-into-the-united-states/",
            # Federal Register authoritative fallback (90 FR 14705)
            "https://www.federalregister.gov/documents/2025/03/27/2025-05423/adjusting-imports-of-automobiles-and-automobile-parts-into-the-united-states",
        ],
    },
]

CONCORDANCE_PDF_URL = (
    "https://publications.gc.ca/collections/collection_2024/statcan/12-501-x-2022001-eng.pdf"
)
CONCORDANCE_PDF_DEST = RAW / "hs_naics_concordance.pdf"


def _fetch(url: str, dest: Path, label: str, *, dry_run: bool, delay: float = 1.5) -> bool:
    if dest.exists():
        log.info("SKIP (already fetched): %s → %s", label, dest.name)
        return True
    if dry_run:
        log.info("DRY-RUN: would fetch %s → %s", label, dest.name)
        return True
    log.info("Fetching %s …", label)
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        dest.write_bytes(resp.content)
        log.info("  → saved %s (%.1f kB)", dest.name, len(resp.content) / 1024)
        time.sleep(delay)
        return True
    except Exception as exc:
        log.warning("  FAILED %s: %s", label, exc)
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="Print what would be fetched without downloading")
    parser.add_argument("--concordance-pdf", action="store_true", help="Also download StatCan HS-NAICS concordance PDF (~3 MB)")
    parser.add_argument("--force", action="store_true", help="Re-fetch even if destination already exists")
    args = parser.parse_args()

    if args.force:
        for entry in SOURCES.values():
            entry["dest"].unlink(missing_ok=True)
        for entry in SOURCES_WITH_FALLBACK:
            entry["dest"].unlink(missing_ok=True)
        if args.concordance_pdf:
            CONCORDANCE_PDF_DEST.unlink(missing_ok=True)

    results = {}
    for key, entry in SOURCES.items():
        ok = _fetch(entry["url"], entry["dest"], entry["label"], dry_run=args.dry_run)
        results[key] = ok

    for entry in SOURCES_WITH_FALLBACK:
        dest: Path = entry["dest"]
        label: str = entry["label"]
        if dest.exists() and not args.force:
            log.info("SKIP (already fetched): %s → %s", label, dest.name)
            results[dest.stem] = True
            continue
        ok = False
        for url in entry["candidates"]:
            ok = _fetch(url, dest, f"{label} [{url.split('/')[2]}]", dry_run=args.dry_run, delay=1.0)
            if ok and dest.exists():
                break
            if args.dry_run:
                break
        results[dest.stem] = ok

    if args.concordance_pdf:
        ok = _fetch(CONCORDANCE_PDF_URL, CONCORDANCE_PDF_DEST, "StatCan HS-NAICS concordance (PDF)", dry_run=args.dry_run)
        results["concordance_pdf"] = ok

    # Write manifest so build_sector_profiles.py knows what was fetched
    manifest = {
        "fetched": {k: str(SOURCES[k]["dest"]) for k, v in results.items() if v and k in SOURCES},
        "concordance_pdf": str(CONCORDANCE_PDF_DEST) if results.get("concordance_pdf") else None,
        "concordance_csv": str(RAW / "hs_naics_concordance.csv") if (RAW / "hs_naics_concordance.csv").is_file() else None,
    }
    manifest_path = RAW / "fetch_manifest.json"
    if not args.dry_run:
        manifest_path.write_text(json.dumps(manifest, indent=2))
        log.info("Manifest written to %s", manifest_path)

    failed = [k for k, v in results.items() if not v]
    # The HTML page is sufficient for build_sector_profiles.py — XLSX is a nice-to-have
    # These sources have fallback coverage in build_sector_profiles.py and are not blockers
    # These have fallbacks in build_distilled_criteria.py (rates are hardcoded from Blakes PDF)
    _OPTIONAL = {
        "retaliatory_tariffs", "auto", "auto_proclamation_10908",
        "ca_steel_derivatives_dec2025",   # Finance Canada URL keeps changing — covered in criteria
        "fr_s232_50pct_proc10947",        # FR server error — 50% rate already in distilled criteria
    }
    critical_failed = [k for k in failed if k not in _OPTIONAL]
    if critical_failed:
        log.warning("Critical sources failed: %s", critical_failed)
        sys.exit(1)
    elif failed:
        log.warning(
            "Optional sources not fetched: %s — build_sector_profiles.py will use available fallbacks. "
            "For XLSX: download manually from canada.ca/en/department-finance → counter-tariffs → complete-list. "
            "For auto proclamation 10908: check federalregister.gov, search '90 FR 14705'.",
            failed,
        )
        log.info("Continuing. Run scripts/build_sector_profiles.py next.")
    else:
        log.info("All sources fetched. Run scripts/build_sector_profiles.py next.")


if __name__ == "__main__":
    main()

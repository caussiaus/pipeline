#!/usr/bin/env python3
"""Build sector_profiles.json and per-mechanism criteria text from scraped reference docs.

Reads:
  raw_data/retaliatory_tariffs.xlsx          — Finance Canada counter-tariff schedule
  raw_data/retaliatory_tariffs_page.html     — fallback if XLSX unavailable
  raw_data/hs_naics_concordance.csv          — HS code → NAICS crosswalk (user-supplied or generated)
  raw_data/section_232_proclamations/*.html  — BIS / White House §232 proclamation pages
  raw_data/cusma/chapter_02.html             — CUSMA Chapter 2 text

Writes:
  raw_data/affected_naics.csv       — NAICS codes affected by tariffs + rates + mechanism
  raw_data/sector_profiles.json     — machine-readable caps for tariff_agent/utils/sector_meta.py
  raw_data/criteria/{mechanism}.txt — LLM injection text per mechanism for Pass-2 prompts

Usage:
  .venv/bin/python3 scripts/build_sector_profiles.py
  .venv/bin/python3 scripts/build_sector_profiles.py --no-concordance  # skip HS join, use HS chapter heuristic
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import textwrap
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "raw_data"
CRITERIA_DIR = RAW / "criteria"

# ---------------------------------------------------------------------------
# HS chapter → (mechanism, earnings_cap, supply_chain_cap, macro_cap)
# Derived from the legal instruments; used when concordance CSV is absent.
# HS chapters are the first 2 digits of the HS code (padded to 4+ digits).
#
# Source: Trade Expansion Act §232, Finance Canada retaliatory schedule,
#         CUSMA Ch. 2 national security carve-out (Art. 32.2)
# ---------------------------------------------------------------------------
_HS_CHAPTER_RULES: list[tuple[str | tuple[str, ...], str, int, int, int]] = [
    # (hs_prefix_or_prefixes, mechanism, e_cap, sc_cap, m_cap)
    # Steel (HS 72, 73) and aluminum (HS 76) — §232, no CUSMA exemption
    (("72", "73", "76"), "section_232_steel_aluminum",   3, 3, 3),
    # Auto and parts (HS 87) — §232 auto proclamation
    (("87",),            "section_232_auto",              3, 3, 3),
    # Wood / lumber / paper (HS 44, 47, 48, 94 furniture) — CVD/AD orders
    (("44", "47", "48"), "cvd_ad_softwood_lumber",        3, 3, 2),
    (("94",),            "cvd_ad_softwood_lumber",        2, 3, 2),
    # Energy products (HS 27) — §122/§232 energy differential
    (("27",),            "energy_differential",           2, 3, 2),
    # Agricultural / food (HS 01-24) — CUSMA conditional + retaliatory
    (tuple(f"{i:02d}" for i in range(1, 25)),
                         "cusma_agri_conditional",        2, 2, 2),
    # Chemicals, plastics (HS 28-40) — mostly CUSMA-protected
    (tuple(f"{i:02d}" for i in range(28, 41)),
                         "cusma_agri_conditional",        2, 2, 2),
    # Machinery (HS 84, 85) — steel derivative / Buy American
    (("84", "85"),       "input_cost_steel_derivative",   2, 3, 2),
    # Textiles / apparel (HS 50-63) — CUSMA-qualified for most
    (tuple(f"{i:02d}" for i in range(50, 64)),
                         "cusma_agri_conditional",        1, 2, 1),
]

# Flat lookup built from rules above
_HS_CHAPTER_MAP: dict[str, tuple[str, int, int, int]] = {}
for _prefixes, _mech, _ec, _sc, _mc in _HS_CHAPTER_RULES:
    if isinstance(_prefixes, str):
        _prefixes = (_prefixes,)
    for _p in _prefixes:
        _HS_CHAPTER_MAP[_p] = (_mech, _ec, _sc, _mc)


def mechanism_from_hs(hs_code: str) -> tuple[str, int, int, int]:
    """Return (mechanism, e_cap, sc_cap, m_cap) for a given HS code string."""
    code = re.sub(r"\D", "", str(hs_code)).zfill(4)
    hit = _HS_CHAPTER_MAP.get(code[:2])
    if hit:
        return hit
    return "demand_compression", 2, 2, 2


# ---------------------------------------------------------------------------
# Retaliatory tariff schedule parsers
# ---------------------------------------------------------------------------

def _rate_from_str(raw: str) -> float:
    """Extract numeric rate from strings like '25%', '25 %', '10.5 per cent'."""
    m = re.search(r"(\d+(?:\.\d+)?)\s*%?", str(raw))
    return float(m.group(1)) if m else 0.0


def load_retaliatory_tariffs_xlsx(path: Path) -> pd.DataFrame:
    """Parse Finance Canada retaliatory tariff XLSX into a clean DataFrame.

    Expected columns (Finance Canada format as of 2025):
      - HS code or tariff item
      - Description
      - Rate (%)
      - Effective date
    """
    log.info("Parsing retaliatory tariffs XLSX: %s", path)
    # Finance Canada workbook may have multiple sheets; try all, use first non-empty one
    xl = pd.ExcelFile(path, engine="openpyxl")
    df = None
    for sheet in xl.sheet_names:
        candidate = xl.parse(sheet, dtype=str).dropna(how="all")
        if len(candidate) > 10:
            df = candidate
            log.info("  Using sheet '%s' (%d rows)", sheet, len(df))
            break
    if df is None or df.empty:
        raise ValueError(f"No usable sheet found in {path}")

    # Normalise column names: strip whitespace, lowercase
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    log.info("  Columns: %s", list(df.columns))

    # Best-effort column identification
    hs_col = next((c for c in df.columns if any(x in c for x in ("hs", "tariff_item", "item", "code"))), df.columns[0])
    rate_col = next((c for c in df.columns if any(x in c for x in ("rate", "%", "percent", "duty"))), None)
    desc_col = next((c for c in df.columns if any(x in c for x in ("descri", "product", "good"))), None)
    date_col = next((c for c in df.columns if any(x in c for x in ("date", "effective", "eff"))), None)

    out = pd.DataFrame()
    out["hs_code"] = df[hs_col].astype(str).str.strip().str.replace(r"[\.\s]", "", regex=True)
    out["description"] = df[desc_col].astype(str).str.strip() if desc_col else ""
    out["rate"] = df[rate_col].apply(_rate_from_str) if rate_col else 25.0
    out["effective_date"] = df[date_col].astype(str).str.strip() if date_col else ""
    out = out[out["hs_code"].str.match(r"^\d{4,8}$")].copy()
    log.info("  %d clean HS rows after filtering", len(out))
    return out


def load_retaliatory_tariffs_html(path: Path) -> pd.DataFrame:
    """Fallback: scrape tariff table from the Finance Canada HTML page."""
    log.info("Parsing retaliatory tariffs HTML: %s", path)
    soup = BeautifulSoup(path.read_bytes(), "lxml")
    tables = soup.find_all("table")
    if not tables:
        raise ValueError(f"No <table> elements found in {path}")
    # Use the largest table
    best = max(tables, key=lambda t: len(t.find_all("tr")))
    rows = []
    for tr in best.find_all("tr"):
        cells = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
        if cells:
            rows.append(cells)
    if not rows:
        raise ValueError("Table found but no rows extracted")
    headers = [c.strip().lower().replace(" ", "_") for c in rows[0]]
    df_raw = pd.DataFrame(rows[1:], columns=headers if len(headers) == len(rows[1]) else None)
    # Same column heuristic as XLSX path
    df_raw.columns = [str(c) for c in df_raw.columns]
    hs_col = next((c for c in df_raw.columns if any(x in c for x in ("hs", "tariff", "item", "code", "0"))), df_raw.columns[0])
    rate_col = next((c for c in df_raw.columns if any(x in c for x in ("rate", "%", "duty", "percent"))), None)
    desc_col = next((c for c in df_raw.columns if any(x in c for x in ("descri", "product", "good"))), None)
    out = pd.DataFrame()
    out["hs_code"] = df_raw[hs_col].astype(str).str.strip().str.replace(r"[\.\s\-]", "", regex=True)
    out["description"] = df_raw[desc_col].astype(str).str.strip() if desc_col else ""
    out["rate"] = df_raw[rate_col].apply(_rate_from_str) if rate_col else 25.0
    out["effective_date"] = ""
    out = out[out["hs_code"].str.match(r"^\d{4,8}$")].copy()
    log.info("  %d rows extracted from HTML table", len(out))
    return out


def load_retaliatory_tariffs(raw_dir: Path) -> pd.DataFrame:
    xlsx = raw_dir / "retaliatory_tariffs.xlsx"
    html = raw_dir / "retaliatory_tariffs_page.html"
    if xlsx.is_file():
        return load_retaliatory_tariffs_xlsx(xlsx)
    if html.is_file():
        return load_retaliatory_tariffs_html(html)
    raise FileNotFoundError(
        "Neither retaliatory_tariffs.xlsx nor retaliatory_tariffs_page.html found in raw_data/. "
        "Run scripts/fetch_tariff_reference_docs.py first."
    )


# ---------------------------------------------------------------------------
# HS-NAICS concordance loader
# ---------------------------------------------------------------------------

def load_hs_naics_concordance(raw_dir: Path) -> pd.DataFrame | None:
    """Load user-supplied or previously built HS-NAICS concordance CSV.

    Expected columns: hs_code (str), naics_code (str), description (optional).
    Returns None if file absent (caller falls back to chapter heuristic).
    """
    path = raw_dir / "hs_naics_concordance.csv"
    if not path.is_file():
        log.info("hs_naics_concordance.csv not found — will use HS chapter heuristic")
        return None
    log.info("Loading HS-NAICS concordance: %s", path)
    df = pd.read_csv(path, dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]
    if "hs_code" not in df.columns or "naics_code" not in df.columns:
        raise ValueError(f"Concordance CSV must have 'hs_code' and 'naics_code' columns; got {list(df.columns)}")
    df["hs_prefix"] = df["hs_code"].str.replace(r"\D", "", regex=True).str.zfill(4).str[:2]
    df["naics_3digit"] = df["naics_code"].str.replace(r"\D", "", regex=True).str.zfill(6).str[:3]
    log.info("  %d HS-NAICS concordance rows", len(df))
    return df


# ---------------------------------------------------------------------------
# Proclamation text extractor
# ---------------------------------------------------------------------------

def _extract_text_from_html(path: Path, max_chars: int = 12_000) -> str:
    """Extract clean plain text from a saved HTML page, capped at max_chars."""
    if not path.is_file():
        return ""
    soup = BeautifulSoup(path.read_bytes(), "lxml")
    # Remove nav, footer, scripts
    for tag in soup(["nav", "footer", "script", "style", "header"]):
        tag.decompose()
    text = soup.get_text("\n", strip=True)
    # Collapse runs of blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text[:max_chars]


def build_criteria_texts(raw_dir: Path) -> dict[str, str]:
    """Extract per-mechanism criteria text from saved proclamation pages.

    Criteria files are injected into the Pass-2 system prompt for the relevant
    mechanism so the LLM evaluates disclosures against the actual legal instrument
    rather than training-data inference.
    """
    proclamation_dir = raw_dir / "section_232_proclamations"
    cusma_dir = raw_dir / "cusma"

    texts: dict[str, str] = {}

    # §232 steel/aluminum (BIS proclamation page + April 2026 copper/Al/steel expansion)
    sa_text = _extract_text_from_html(proclamation_dir / "steel_aluminum.html")
    copper_text = _extract_text_from_html(proclamation_dir / "copper_expansion.html")
    if sa_text or copper_text:
        texts["section_232_steel_aluminum"] = "\n\n---\n\n".join(filter(None, [sa_text, copper_text]))

    # §232 auto — Proclamation 10908 (original 25% auto duty)
    # + Proclamation 10925 (CUSMA offset credit: dutiable value reduced by US-origin content)
    # + parts inclusions window (2026 expansion) + medium/heavy duty vehicles (Oct 2025)
    # Original proclamation slug had "autombile" typo; try both filenames
    auto_10908 = _extract_text_from_html(proclamation_dir / "auto_proclamation_10908.html")
    if not auto_10908:
        # Fall back to old filename if an earlier fetch used it
        auto_10908 = _extract_text_from_html(proclamation_dir / "auto.html")
    auto_10925 = _extract_text_from_html(proclamation_dir / "auto_amendment_10925.html")
    auto_inclusions = _extract_text_from_html(proclamation_dir / "auto_parts_inclusions_2026.html")
    medium_heavy = _extract_text_from_html(proclamation_dir / "medium_heavy_vehicles.html")
    auto_parts = [p for p in [auto_10908, auto_10925, auto_inclusions, medium_heavy] if p]
    if auto_parts:
        texts["section_232_auto"] = "\n\n---\n\n".join(auto_parts)

    # Steel derivative tariffs — Finance Canada Dec 26 2025 (25% global on steel derivatives)
    # Relevant for NAICS 332 (fabricated metals), 333 (machinery), 335 (electrical equipment)
    steel_deriv = _extract_text_from_html(proclamation_dir / "ca_steel_derivatives_dec2025.html")
    if steel_deriv:
        texts["input_cost_steel_derivative"] = steel_deriv
    elif sa_text:
        # Minimal fallback: use the BIS page to at least describe the mechanism
        texts["input_cost_steel_derivative"] = sa_text

    # CUSMA Chapter 2 — used for both agri-conditional and services exemption mechanisms
    cusma_text = _extract_text_from_html(cusma_dir / "chapter_02.html")
    if cusma_text:
        texts["cusma_agri_conditional"] = cusma_text
        texts["cusma_exempt_services"] = cusma_text  # same legal basis

    return texts


# ---------------------------------------------------------------------------
# Core build logic
# ---------------------------------------------------------------------------

def build_affected_naics(
    tariffs_df: pd.DataFrame,
    concordance_df: pd.DataFrame | None,
) -> pd.DataFrame:
    """Join retaliatory tariff schedule to NAICS codes.

    If concordance_df is None, falls back to HS chapter heuristic.
    Returns DataFrame: naics_3digit | mechanism | max_rate | hs_codes | effective_date
    """
    records = []
    for _, row in tariffs_df.iterrows():
        hs = str(row["hs_code"]).zfill(4)
        rate = float(row.get("rate", 25))
        mech, e_cap, sc_cap, m_cap = mechanism_from_hs(hs)
        effective_date = str(row.get("effective_date", ""))

        if concordance_df is not None:
            matches = concordance_df[concordance_df["hs_prefix"] == hs[:2]]["naics_3digit"].unique()
            for naics_3 in matches:
                records.append({
                    "naics_3digit": naics_3,
                    "hs_code": hs,
                    "mechanism": mech,
                    "rate": rate,
                    "cap_earnings": e_cap,
                    "cap_supply_chain": sc_cap,
                    "cap_macro": m_cap,
                    "effective_date": effective_date,
                })
        else:
            # No concordance: record by HS chapter only
            records.append({
                "naics_3digit": f"HS_{hs[:2]}",  # placeholder
                "hs_code": hs,
                "mechanism": mech,
                "rate": rate,
                "cap_earnings": e_cap,
                "cap_supply_chain": sc_cap,
                "cap_macro": m_cap,
                "effective_date": effective_date,
            })

    if not records:
        return pd.DataFrame(columns=["naics_3digit", "hs_code", "mechanism", "rate",
                                     "cap_earnings", "cap_supply_chain", "cap_macro", "effective_date"])
    df = pd.DataFrame(records)
    # Aggregate: worst-case caps + max rate per NAICS 3-digit
    agg = (
        df.groupby(["naics_3digit", "mechanism"], as_index=False)
        .agg(
            max_rate=("rate", "max"),
            cap_earnings=("cap_earnings", "max"),
            cap_supply_chain=("cap_supply_chain", "max"),
            cap_macro=("cap_macro", "max"),
            hs_codes=("hs_code", lambda x: ",".join(sorted(set(x))[:20])),
            effective_date=("effective_date", "first"),
        )
    )
    # Where multiple mechanisms map to same NAICS, keep highest-cap one
    agg = agg.sort_values("cap_earnings", ascending=False).drop_duplicates("naics_3digit")
    log.info("Built affected_naics: %d NAICS 3-digit codes", len(agg))
    return agg


def build_sector_profiles_json(affected_df: pd.DataFrame) -> dict:
    """Convert affected_naics DataFrame to the sector_profiles.json schema."""
    profiles = {}
    for _, row in affected_df.iterrows():
        naics_key = str(row["naics_3digit"])
        profiles[naics_key] = {
            "mechanism": row["mechanism"],
            "cap_earnings": int(row["cap_earnings"]),
            "cap_supply_chain": int(row["cap_supply_chain"]),
            "cap_macro": int(row["cap_macro"]),
            "max_observed_rate": float(row.get("max_rate", 0)),
            "hs_codes_sample": str(row.get("hs_codes", "")),
        }
    return profiles


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--no-concordance", action="store_true",
                        help="Skip HS-NAICS concordance join; use HS chapter heuristic only")
    parser.add_argument("--raw-criteria", action="store_true",
                        help="Write raw scraped HTML text to criteria/ instead of distilled blocks "
                             "(useful for auditing what the source documents say)")
    args = parser.parse_args()

    tariffs_df = load_retaliatory_tariffs(RAW)
    concordance_df = None if args.no_concordance else load_hs_naics_concordance(RAW)

    affected_df = build_affected_naics(tariffs_df, concordance_df)
    affected_path = RAW / "affected_naics.csv"
    affected_df.to_csv(affected_path, index=False)
    log.info("Wrote %s (%d rows)", affected_path, len(affected_df))

    profiles = build_sector_profiles_json(affected_df)
    profiles_path = RAW / "sector_profiles.json"
    profiles_path.write_text(json.dumps(profiles, indent=2))
    log.info("Wrote %s (%d profiles)", profiles_path, len(profiles))

    if args.raw_criteria:
        # Audit mode: write raw scraped source text so you can check what the
        # documents actually say before updating the distilled blocks
        criteria = build_criteria_texts(RAW)
        raw_audit_dir = CRITERIA_DIR / "raw_source_archive"
        raw_audit_dir.mkdir(parents=True, exist_ok=True)
        for mech, text in criteria.items():
            out = raw_audit_dir / f"{mech}_raw.txt"
            out.write_text(textwrap.dedent(text))
            log.info("Wrote raw source archive: %s (%d chars)", out.name, len(text))
        log.info("Raw source archive written to %s", raw_audit_dir)
    else:
        # Default: write distilled sub-600-token fact blocks used in Pass-2 prompts
        import sys
        sys.path.insert(0, str(ROOT / "scripts"))
        from build_distilled_criteria import write_distilled_criteria
        write_distilled_criteria(CRITERIA_DIR)

    log.info(
        "Done. Next: run pipeline stages. sector_meta.py will load %s at runtime.",
        profiles_path,
    )


if __name__ == "__main__":
    main()

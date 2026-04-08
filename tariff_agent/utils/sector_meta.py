"""Sector metadata: NAICS → SectorProfile lookup and filings enrichment.

Primary source of truth:
  raw_data/sector_profiles.json  — built by scripts/build_sector_profiles.py
  from Finance Canada retaliatory tariff schedule + HS-NAICS concordance.

Bootstrap fallback (used only when sector_profiles.json is absent):
  BOOTSTRAP_PROFILES below — covers the highest-exposure sectors so the
  pipeline can run before the reference documents have been fetched.

``enrich_with_sector()`` adds to any DataFrame with a ``profile_id`` column:
  naics, naics_sector, mechanism, exposure_vector, cap_earnings, cap_supply_chain, cap_macro

For fsQCA: use ``fuzzy_from_score()`` / ``ragin_fuzzy_from_score()`` for
cross-sector normalised membership values.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
_DEFAULT_PROFILES_PATH = ROOT / "raw_data" / "sector_profiles.json"


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SectorProfile:
    naics_label: str            # Official Statistics Canada sector name
    mechanism: str              # Trade law instrument driving primary exposure
    exposure_vector: str        # Plain-language description injected into LLM prompts
    cap_earnings: int           # fsQCA cap: earnings/revenue dimension (0–3)
    cap_supply_chain: int       # fsQCA cap: supply chain dimension (0–3)
    cap_macro: int              # fsQCA cap: macroeconomic dimension (0–3)

    def get_cap(self, dimension: str) -> int:
        return getattr(self, f"cap_{dimension}", 3)


_UNKNOWN_PROFILE = SectorProfile(
    naics_label="unknown",
    mechanism="minimal_no_vector",
    exposure_vector="",
    cap_earnings=3,
    cap_supply_chain=3,
    cap_macro=3,
)

# ---------------------------------------------------------------------------
# Bootstrap: high-exposure sectors — used when sector_profiles.json absent.
# Keys are 3-digit NAICS prefixes; 2-digit keys are fallbacks.
# ---------------------------------------------------------------------------
BOOTSTRAP_PROFILES: dict[str, SectorProfile] = {
    # 3-digit high-certainty overrides
    "336": SectorProfile(
        naics_label="Transportation equipment manufacturing",
        mechanism="section_232_auto",
        exposure_vector=(
            "Subject to 25% Section 232 tariff on non-CUSMA-qualifying vehicles and parts "
            "(HS 8703–8708). CUSMA national security carve-out (Art. 32.2) overrides preferential "
            "treatment. Accounts for ~29% of US duties on Canadian imports."
        ),
        cap_earnings=3, cap_supply_chain=3, cap_macro=3,
    ),
    "331": SectorProfile(
        naics_label="Primary metal manufacturing",
        mechanism="section_232_steel_aluminum",
        exposure_vector=(
            "50% Section 232 tariff on steel (HS 72–73) and aluminum (HS 76) with no CUSMA "
            "exemption. CUSMA Art. 32.2 national security carve-out applies. Accounts for ~23% "
            "of US duties on Canadian imports."
        ),
        cap_earnings=3, cap_supply_chain=3, cap_macro=3,
    ),
    "321": SectorProfile(
        naics_label="Wood product manufacturing",
        mechanism="cvd_ad_softwood_lumber",
        exposure_vector=(
            "Subject to US CVD/AD orders on softwood lumber (HS 4407–4409); combined duties up "
            "to 45% as of 2025. Panel and OSB producers face the same duty deposit regime as "
            "NAICS 113 forestry operations."
        ),
        cap_earnings=3, cap_supply_chain=3, cap_macro=2,
    ),
    "113": SectorProfile(
        naics_label="Forestry and logging",
        mechanism="cvd_ad_softwood_lumber",
        exposure_vector=(
            "Longstanding US CVD/AD orders on softwood lumber escalated 2025; up to 45% total "
            "anti-dumping and countervailing duties. Stumpage dispute and cash deposit requirements "
            "directly affect cash flow."
        ),
        cap_earnings=3, cap_supply_chain=3, cap_macro=2,
    ),
    "212": SectorProfile(
        naics_label="Mining and quarrying (except oil and gas)",
        mechanism="section_232_steel_aluminum",
        exposure_vector=(
            "Metal ore production (gold, copper, nickel, iron) subject to Section 232 tariffs "
            "on processed metal exports; copper covered by July 2025 §232 expansion. "
            "No CUSMA exemption for §232 actions."
        ),
        cap_earnings=3, cap_supply_chain=3, cap_macro=2,
    ),
    "211": SectorProfile(
        naics_label="Oil and gas extraction",
        mechanism="energy_differential",
        exposure_vector=(
            "10% tariff on Canadian crude oil, natural gas, and refined products under "
            "§122/§232 energy finding. Lower income shock than manufacturing sectors but "
            "direct supply-chain exposure through Buy American pipeline equipment requirements."
        ),
        cap_earnings=2, cap_supply_chain=3, cap_macro=2,
    ),
    "213": SectorProfile(
        naics_label="Support activities for mining and oil and gas",
        mechanism="energy_differential",
        exposure_vector=(
            "Services to oil/gas sector; indirect tariff exposure through client industry "
            "capex reductions and Buy American procurement restrictions on equipment."
        ),
        cap_earnings=1, cap_supply_chain=2, cap_macro=2,
    ),
    "332": SectorProfile(
        naics_label="Fabricated metal product manufacturing",
        mechanism="input_cost_steel_derivative",
        exposure_vector=(
            "Canadian retaliatory 25% tariff on US-origin steel inputs under Finance Canada "
            "derivative products schedule (effective Dec 26 2025). Direct input cost impact "
            "for fabricators sourcing US steel."
        ),
        cap_earnings=3, cap_supply_chain=3, cap_macro=2,
    ),
    # 2-digit fallbacks
    "11": SectorProfile(
        naics_label="Agriculture, forestry, fishing and hunting",
        mechanism="cusma_agri_conditional",
        exposure_vector=(
            "Most goods covered under CUSMA Annex 2-B agricultural exemptions. "
            "Some retaliatory Canadian tariff exposure on US agricultural inputs. "
            "Only flag if a named tariff with direct cost/revenue link is present."
        ),
        cap_earnings=2, cap_supply_chain=2, cap_macro=2,
    ),
    "21": SectorProfile(
        naics_label="Mining, quarrying, and oil and gas extraction",
        mechanism="energy_differential",
        exposure_vector=(
            "10% energy differential tariff on crude/NG exports; §232 steel/aluminum for "
            "mining equipment. Exposure varies significantly by sub-sector — see 3-digit override."
        ),
        cap_earnings=2, cap_supply_chain=3, cap_macro=2,
    ),
    "22": SectorProfile(
        naics_label="Utilities",
        mechanism="energy_differential",
        exposure_vector=(
            "Energy price transmission through 10% Canadian energy tariff. "
            "Indirect earnings exposure; supply chain less affected than extraction sector."
        ),
        cap_earnings=2, cap_supply_chain=2, cap_macro=1,
    ),
    "23": SectorProfile(
        naics_label="Construction",
        mechanism="input_cost_steel_derivative",
        exposure_vector=(
            "Steel and lumber input cost inflation from §232 and CVD/AD orders. "
            "Supply chain exposure through steel rebar, structural steel, and OSB panel pricing."
        ),
        cap_earnings=2, cap_supply_chain=3, cap_macro=2,
    ),
    "41": SectorProfile(
        naics_label="Wholesale trade",
        mechanism="demand_compression",
        exposure_vector=(
            "Margin compression on traded goods from tariff-induced price increases. "
            "Only flag if company names specific products or programs affected."
        ),
        cap_earnings=2, cap_supply_chain=2, cap_macro=2,
    ),
    "44": SectorProfile(
        naics_label="Retail trade",
        mechanism="demand_compression",
        exposure_vector=(
            "Consumer price inflation from tariffs reduces discretionary spending. "
            "Apply strict rejection — only flag if company explicitly links tariffs to margins."
        ),
        cap_earnings=1, cap_supply_chain=2, cap_macro=2,
    ),
    "45": SectorProfile(
        naics_label="Retail trade",
        mechanism="demand_compression",
        exposure_vector=(
            "Consumer price inflation from tariffs reduces discretionary spending. "
            "Apply strict rejection — only flag if company explicitly links tariffs to margins."
        ),
        cap_earnings=1, cap_supply_chain=2, cap_macro=2,
    ),
    "48": SectorProfile(
        naics_label="Transportation and warehousing",
        mechanism="demand_compression",
        exposure_vector=(
            "Cross-border trade volume reduction from tariff-induced trade diversion. "
            "Only flag if named tariff program directly reduces shipment volumes."
        ),
        cap_earnings=2, cap_supply_chain=2, cap_macro=2,
    ),
    "49": SectorProfile(
        naics_label="Warehousing and storage",
        mechanism="demand_compression",
        exposure_vector=(
            "Cross-border trade volume reduction from tariff-induced trade diversion. "
            "Only flag if named tariff program directly reduces throughput."
        ),
        cap_earnings=2, cap_supply_chain=2, cap_macro=2,
    ),
    "51": SectorProfile(
        naics_label="Information and cultural industries",
        mechanism="cusma_exempt_services",
        exposure_vector=(
            "Services covered by CUSMA Chapter 19 cultural industries exemption. "
            "Return mentions_tariffs=False unless a named duty with a direct cost link is present. "
            "Generic 'trade uncertainty' language is REJECTION."
        ),
        cap_earnings=1, cap_supply_chain=1, cap_macro=1,
    ),
    "52": SectorProfile(
        naics_label="Finance and insurance",
        mechanism="minimal_no_vector",
        exposure_vector=(
            "No direct tariff exposure. Transmission is indirect via client portfolio credit risk. "
            "Only flag if company explicitly quantifies tariff-related credit losses or reserves."
        ),
        cap_earnings=1, cap_supply_chain=1, cap_macro=2,
    ),
    "53": SectorProfile(
        naics_label="Real estate and rental and leasing",
        mechanism="input_cost_steel_derivative",
        exposure_vector=(
            "Construction cost inflation from steel and lumber duties affects development projects. "
            "Flag only if named tariff is linked to a specific project or margin impact."
        ),
        cap_earnings=1, cap_supply_chain=2, cap_macro=1,
    ),
    "54": SectorProfile(
        naics_label="Professional, scientific and technical services",
        mechanism="cusma_exempt_services",
        exposure_vector=(
            "Services covered by CUSMA Chapter 15 (cross-border services). "
            "No product duty. Apply strict rejection criteria."
        ),
        cap_earnings=1, cap_supply_chain=1, cap_macro=2,
    ),
    "55": SectorProfile(
        naics_label="Management of companies and enterprises",
        mechanism="holding_subsidiary_dependent",
        exposure_vector=(
            "Holding company: tariff exposure determined entirely by subsidiary sector mix. "
            "Score based on subsidiary disclosures; apply parent-level caps only if consolidated "
            "financials explicitly quantify tariff impacts."
        ),
        cap_earnings=1, cap_supply_chain=1, cap_macro=1,
    ),
    "56": SectorProfile(
        naics_label="Administrative and support, waste management and remediation services",
        mechanism="minimal_no_vector",
        exposure_vector="Indirect exposure through client industries. Apply strict rejection criteria.",
        cap_earnings=1, cap_supply_chain=1, cap_macro=1,
    ),
    "61": SectorProfile(
        naics_label="Educational services",
        mechanism="minimal_no_vector",
        exposure_vector="No credible tariff transmission pathway. Return mentions_tariffs=False.",
        cap_earnings=0, cap_supply_chain=0, cap_macro=1,
    ),
    "62": SectorProfile(
        naics_label="Health care and social assistance",
        mechanism="pharma_232_pending",
        exposure_vector=(
            "Section 232 investigation on pharmaceuticals active (announced Feb 2025, no duty yet). "
            "Steel/aluminum remission to Jun 2026 for medical devices. "
            "Flag ONLY if company names §232 pharma investigation with a company-specific impact."
        ),
        cap_earnings=1, cap_supply_chain=2, cap_macro=1,
    ),
    "71": SectorProfile(
        naics_label="Arts, entertainment and recreation",
        mechanism="minimal_no_vector",
        exposure_vector="Tourism volume reduction; no direct duty. Apply strict rejection criteria.",
        cap_earnings=1, cap_supply_chain=1, cap_macro=1,
    ),
    "72": SectorProfile(
        naics_label="Accommodation and food services",
        mechanism="demand_compression",
        exposure_vector=(
            "Cross-border tourism reduction and food input cost increases from agri tariffs. "
            "Flag only if explicitly quantified or linked to named program."
        ),
        cap_earnings=1, cap_supply_chain=1, cap_macro=2,
    ),
    "81": SectorProfile(
        naics_label="Other services (except public administration)",
        mechanism="minimal_no_vector",
        exposure_vector="No credible tariff transmission pathway.",
        cap_earnings=0, cap_supply_chain=0, cap_macro=1,
    ),
    # Manufacturing 2-digit catch-alls — only reached when no 3-digit override matched.
    # Covers NAICS 313-329 (textiles, printing, plastics, etc.) and 338-339 (misc).
    # Caps are moderate because input cost exposure (steel, energy, materials) is real
    # but varies by sub-sector. 3-digit profiles above take priority for known sub-sectors.
    "31": SectorProfile(
        naics_label="Manufacturing (light — food, textiles, apparel)",
        mechanism="cusma_agri_conditional",
        exposure_vector=(
            "Light manufacturing mostly covered by CUSMA Annex 2-B. Some input cost exposure "
            "from agri/textile tariffs. Flag only if named tariff directly linked to products."
        ),
        cap_earnings=2, cap_supply_chain=2, cap_macro=2,
    ),
    "32": SectorProfile(
        naics_label="Manufacturing (materials — plastics, paper, petroleum, minerals)",
        mechanism="input_cost_steel_derivative",
        exposure_vector=(
            "Materials manufacturing: input cost exposure through steel derivative tariffs, "
            "energy differential on refined products, and softwood-related paper input costs. "
            "Cap varies by sub-sector — see 3-digit profiles for 321/322/324/325/327."
        ),
        cap_earnings=2, cap_supply_chain=3, cap_macro=2,
    ),
    "33": SectorProfile(
        naics_label="Manufacturing (industrial/durable — machinery, metals, electronics, transport)",
        mechanism="input_cost_steel_derivative",
        exposure_vector=(
            "Durable goods manufacturing: steel and aluminum input cost exposure through "
            "Section 232 derivative products schedule. Specific sub-sectors (331, 332, 336) "
            "have higher-cap 3-digit profiles. Miscellaneous manufacturers (339) may have "
            "Buy American procurement exposure."
        ),
        cap_earnings=2, cap_supply_chain=3, cap_macro=2,
    ),
}

# 3-digit manufacturing overrides not already in bootstrap
_MFG_3DIGIT: dict[str, SectorProfile] = {
    "311": SectorProfile("Food manufacturing", "cusma_agri_conditional",
                         "CUSMA-protected; some retaliatory input cost exposure on US agri ingredients.",
                         2, 2, 2),
    "312": SectorProfile("Beverage and tobacco product manufacturing", "cusma_agri_conditional",
                         "OJ and bourbon targeted in Canadian retaliatory schedule; otherwise CUSMA-protected.",
                         2, 2, 2),
    "322": SectorProfile("Paper manufacturing", "cvd_ad_softwood_lumber",
                         "Softwood pulp input cost affected; some CVD orders on newsprint.",
                         2, 3, 2),
    "324": SectorProfile("Petroleum and coal product manufacturing", "energy_differential",
                         "Refined product exports subject to 10% energy differential duty.",
                         2, 3, 2),
    "325": SectorProfile("Chemical manufacturing", "cusma_agri_conditional",
                         "Mostly CUSMA-protected; some specialty chemicals in retaliatory schedule.",
                         2, 2, 2),
    "327": SectorProfile("Non-metallic mineral product manufacturing", "input_cost_steel_derivative",
                         "Steel rebar derivative tariffs affect cement and concrete product costs.",
                         2, 2, 2),
    "333": SectorProfile("Machinery manufacturing", "input_cost_steel_derivative",
                         "Buy American procurement restrictions and steel/aluminum input cost inflation.",
                         2, 3, 2),
    "334": SectorProfile("Computer and electronic product manufacturing", "cusma_exempt_services",
                         "Mostly CUSMA-qualifying; some Section 301 exposure via Chinese-origin components.",
                         1, 2, 2),
    "335": SectorProfile("Electrical equipment, appliance and component manufacturing", "input_cost_steel_derivative",
                         "Transformer and motor steel content in Section 232 derivative products list.",
                         2, 2, 2),
    "337": SectorProfile("Furniture and related product manufacturing", "cvd_ad_softwood_lumber",
                         "Softwood lumber and panel input cost pass-through from CVD/AD orders.",
                         2, 3, 2),
}
BOOTSTRAP_PROFILES.update(_MFG_3DIGIT)


# ---------------------------------------------------------------------------
# Profile loading
# ---------------------------------------------------------------------------

def load_sector_profiles(profiles_path: Path | None = None) -> dict[str, SectorProfile]:
    """Load sector profiles from sector_profiles.json if available.

    Falls back to ``BOOTSTRAP_PROFILES`` with a warning if the file is absent.
    The JSON format matches the output of ``scripts/build_sector_profiles.py``.
    """
    path = Path(profiles_path) if profiles_path else _DEFAULT_PROFILES_PATH
    if not path.is_file():
        logger.info(
            "sector_meta: %s not found — using bootstrap profiles. "
            "Run scripts/fetch_tariff_reference_docs.py && scripts/build_sector_profiles.py "
            "to build from source documents.",
            path,
        )
        return dict(BOOTSTRAP_PROFILES)

    try:
        raw = json.loads(path.read_text())
    except Exception as exc:
        logger.warning("sector_meta: could not parse %s: %s — using bootstrap", path, exc)
        return dict(BOOTSTRAP_PROFILES)

    profiles: dict[str, SectorProfile] = dict(BOOTSTRAP_PROFILES)  # start with bootstrap
    loaded = 0
    for key, entry in raw.items():
        try:
            # JSON has numeric caps; merge with bootstrap naics_label/exposure_vector if known
            bootstrap = BOOTSTRAP_PROFILES.get(key)
            profiles[key] = SectorProfile(
                naics_label=entry.get("naics_label", bootstrap.naics_label if bootstrap else key),
                mechanism=entry["mechanism"],
                exposure_vector=entry.get("exposure_vector", bootstrap.exposure_vector if bootstrap else ""),
                cap_earnings=int(entry["cap_earnings"]),
                cap_supply_chain=int(entry["cap_supply_chain"]),
                cap_macro=int(entry["cap_macro"]),
            )
            loaded += 1
        except (KeyError, TypeError) as exc:
            logger.warning("sector_meta: skipping malformed profile '%s': %s", key, exc)

    logger.info("sector_meta: loaded %d profiles from %s (bootstrap has %d)", loaded, path.name, len(BOOTSTRAP_PROFILES))
    return profiles


# ---------------------------------------------------------------------------
# Profile lookup
# ---------------------------------------------------------------------------

def get_profile(naics_code: str, profiles: dict[str, SectorProfile]) -> SectorProfile:
    """Return the best SectorProfile for a 4–6 digit NAICS code.

    Resolution order: 3-digit prefix → 2-digit prefix → unknown fallback.
    """
    code = re.sub(r"\D", "", str(naics_code)).zfill(4)
    p3 = profiles.get(code[:3])
    if p3:
        return p3
    p2 = profiles.get(code[:2])
    if p2:
        return p2
    return _UNKNOWN_PROFILE


# ---------------------------------------------------------------------------
# NAICS code parsing
# ---------------------------------------------------------------------------

def _parse_naics_code(raw: Any) -> str:
    """Extract leading numeric digits from strings like '212220 - Gold and silver ore mining'."""
    if not raw or isinstance(raw, float):
        return ""
    s = str(raw).strip()
    if s.startswith("000000"):
        return ""
    m = re.match(r"(\d{4,6})", s)
    return m.group(1) if m else ""


# ---------------------------------------------------------------------------
# Ragin / fuzzy calibration
# ---------------------------------------------------------------------------

_RAGIN_ANCHORS: dict[int, float] = {0: 0.05, 1: 0.25, 2: 0.75, 3: 0.95}


def fuzzy_from_score(score_adj: int | float, profile: SectorProfile, dimension: str) -> float:
    """Sector-normalised linear fuzzy membership in [0, 1].

    Divides sector-capped score by the profile's ceiling for that dimension,
    making scores comparable across sectors in fsQCA cross-case analysis.
    """
    cap = profile.get_cap(dimension)
    if cap <= 0:
        return 0.0
    return round(min(1.0, max(0.0, score_adj / cap)), 4)


def ragin_fuzzy_from_score(score_adj: int | float, profile: SectorProfile, dimension: str) -> float:
    """Ragin 4-point calibration after sector normalisation.

    1. Normalise: score_adj / sector_cap → [0, 1]
    2. Scale back to [0, 3] integer
    3. Apply Ragin anchors {0→0.05, 1→0.25, 2→0.75, 3→0.95}
    Crossover (0.5) sits between levels 1 and 2 — aligned with BOILERPLATE/SPECIFIC_QUALITATIVE.
    """
    cap = profile.get_cap(dimension)
    if cap <= 0:
        return 0.05
    normalized = round(min(3.0, max(0.0, (score_adj / cap) * 3)))
    return _RAGIN_ANCHORS.get(int(normalized), 0.05)


# ---------------------------------------------------------------------------
# DataFrame enrichment
# ---------------------------------------------------------------------------

def _clean_sedar_name(s: Any) -> str:
    """Normalise a company name for fuzzy matching against the SEDAR master."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s = str(s).split("/")[0].strip()
    s = re.split(r"\bOperating name\b", s, flags=re.I)[0].strip()
    s = re.sub(r"[^a-z0-9 ]", " ", s.lower())
    s = re.sub(r"\s+", " ", s).strip()
    for sfx in ["inc", "corp", "ltd", "limited", "lp", "llp", "co", "trust", "fund", "plc"]:
        s = re.sub(rf"\b{sfx}\b", "", s).strip()
    return re.sub(r"\s+", " ", s).strip()


def build_master_name_lookup(metadata_path: str | Path) -> pd.DataFrame:
    """Return a DataFrame indexed by cleaned sedar_name with profile_number, profile_id, naics.

    Used to backfill identity columns for issuers that lack a long-form ``profile_id``
    in the master CSV (i.e. pre-SEDAR+ registrants still on the legacy system).
    """
    meta_path = Path(metadata_path)
    if not meta_path.is_file():
        return pd.DataFrame(columns=["_name_key", "profile_number", "profile_id", "naics"])
    try:
        raw = pd.read_csv(meta_path, usecols=["sedar_name", "profile_number", "profile_id", "naics"], dtype=str)
    except Exception as exc:
        logger.warning("sector_meta: could not read master for name lookup: %s", exc)
        return pd.DataFrame(columns=["_name_key", "profile_number", "profile_id", "naics"])
    raw["_name_key"] = raw["sedar_name"].map(_clean_sedar_name)
    return raw.drop_duplicates(subset=["_name_key"], keep="first").set_index("_name_key")


def enrich_filings_with_master_ids(
    df: pd.DataFrame,
    metadata_path: str | Path,
) -> pd.DataFrame:
    """Add ``profile_number`` and backfill ``profile_id`` from the SEDAR master CSV.

    Joins first on ``profile_id`` (exact, for SEDAR+ issuers), then falls back to a
    fuzzy ``issuer_name`` match for legacy issuers that have a ``profile_number`` but
    no long-form hash.  Always adds ``profile_number`` as a stable short SEDAR ID.
    """
    lookup = build_master_name_lookup(metadata_path)
    if lookup.empty:
        if "profile_number" not in df.columns:
            df = df.copy()
            df["profile_number"] = ""
        return df

    out = df.copy()

    # 1 — exact profile_id join (fast path for SEDAR+ issuers)
    pid_map = lookup.reset_index().dropna(subset=["profile_id"]).drop_duplicates("profile_id")
    pid_map = pid_map.set_index("profile_id")[["profile_number"]].rename(
        columns={"profile_number": "_pnum_pid"}
    )

    # 2 — name-based fallback
    if "issuer_name" in out.columns:
        out["_name_key"] = out["issuer_name"].map(_clean_sedar_name)
        out["_pnum_name"] = out["_name_key"].map(
            lookup["profile_number"] if "profile_number" in lookup.columns else pd.Series(dtype=str)
        )
        out["_pid_name"] = out["_name_key"].map(
            lookup["profile_id"] if "profile_id" in lookup.columns else pd.Series(dtype=str)
        )
        out["_naics_name"] = out["_name_key"].map(
            lookup["naics"] if "naics" in lookup.columns else pd.Series(dtype=str)
        )
        out.drop(columns=["_name_key"], inplace=True)
    else:
        out["_pnum_name"] = pd.NA
        out["_pid_name"] = pd.NA
        out["_naics_name"] = pd.NA

    # Resolve profile_number: pid-join → name-join
    if "profile_id" in out.columns:
        out["_pnum_pid_resolved"] = out["profile_id"].map(
            lambda v: pid_map.loc[v, "_pnum_pid"] if (v and v in pid_map.index) else pd.NA
        )
    else:
        out["_pnum_pid_resolved"] = pd.NA

    def _coalesce(*vals: Any) -> str:
        for v in vals:
            if v and not (isinstance(v, float) and pd.isna(v)) and str(v).strip() not in ("", "nan", "NaN", "<NA>"):
                return str(v).strip()
        return ""

    out["profile_number"] = out.apply(
        lambda r: _coalesce(r.get("_pnum_pid_resolved"), r.get("_pnum_name")), axis=1
    )

    # Backfill profile_id for legacy issuers where master has no long hash but we can infer nothing
    # (leave as-is — empty string is correct; do not invent IDs)
    if "profile_id" in out.columns:
        missing_pid = out["profile_id"].isna() | (out["profile_id"].astype(str).str.strip().isin(["", "nan"]))
        out.loc[missing_pid, "profile_id"] = ""

    # Naics backfill for rows where sector enrichment hasn't run yet
    if "naics" not in out.columns:
        out["naics"] = out["_naics_name"].fillna("")

    out.drop(
        columns=[c for c in ["_pnum_pid_resolved", "_pnum_name", "_pid_name", "_naics_name"] if c in out.columns],
        inplace=True,
    )

    n_pnum = (out["profile_number"] != "").sum()
    logger.info("enrich_filings_with_master_ids: resolved profile_number for %s/%s rows", n_pnum, len(out))
    return out


def enrich_with_sector(
    df: pd.DataFrame,
    metadata_path: str | Path,
    profiles_path: str | Path | None = None,
) -> pd.DataFrame:
    """Left-join sector metadata onto ``df``.

    Join priority (first match wins):
      1. ``profile_number`` — 9-digit SEDAR ID, present for ALL 10k+ issuers.
      2. ``profile_id``     — long 48-char hash (SEDAR+ migrants only, ~66% of master).
      3. normalised ``issuer_name`` match against ``sedar_name``.

    Adds columns: profile_number, naics, naics_sector, mechanism,
                  exposure_vector, cap_earnings, cap_supply_chain, cap_macro.
    """
    profiles = load_sector_profiles(Path(profiles_path) if profiles_path else None)

    _defaults: dict[str, Any] = {
        "naics": "",
        "naics_sector": "unknown",
        "mechanism": "minimal_no_vector",
        "exposure_vector": "",
        "cap_earnings": 3,
        "cap_supply_chain": 3,
        "cap_macro": 3,
    }
    meta_path = Path(metadata_path)

    if not meta_path.is_file():
        logger.warning("sector_meta: metadata file not found: %s — skipping enrichment", meta_path)
        df = df.copy()
        for col, default in _defaults.items():
            if col not in df.columns:
                df[col] = default
        if "profile_number" not in df.columns:
            df["profile_number"] = ""
        return df

    try:
        raw_meta = pd.read_csv(
            meta_path,
            usecols=["sedar_name", "profile_number", "profile_id", "naics"],
            dtype=str,
        )
    except Exception as exc:
        logger.warning("sector_meta: could not read %s: %s — skipping enrichment", meta_path, exc)
        df = df.copy()
        for col, default in _defaults.items():
            if col not in df.columns:
                df[col] = default
        if "profile_number" not in df.columns:
            df["profile_number"] = ""
        return df

    # ── Resolve NAICS sector profile for every master row ──────────────────
    raw_meta["_naics_code"] = raw_meta["naics"].fillna("").apply(_parse_naics_code)
    raw_meta["_name_key"]   = raw_meta["sedar_name"].map(_clean_sedar_name)

    def _sector_cols(code: str) -> dict[str, Any]:
        p = get_profile(code, profiles)
        return {
            "naics_sector":    p.naics_label,
            "mechanism":       p.mechanism,
            "exposure_vector": p.exposure_vector,
            "cap_earnings":    p.cap_earnings,
            "cap_supply_chain": p.cap_supply_chain,
            "cap_macro":       p.cap_macro,
        }

    sec = raw_meta["_naics_code"].apply(lambda c: pd.Series(_sector_cols(c)))
    meta_full = pd.concat(
        [raw_meta[["profile_number", "profile_id", "_name_key", "naics"]], sec],
        axis=1,
    ).rename(columns={"naics": "naics_raw"})

    # ── Three vectorised lookup tables ─────────────────────────────────────
    # 1. by profile_number (9-digit, universal key)
    by_pnum = (
        meta_full[meta_full["profile_number"].notna() & (meta_full["profile_number"].str.strip() != "")]
        .drop_duplicates("profile_number")
        .set_index("profile_number")
    )
    # 2. by profile_id (long hash, subset only)
    by_pid = (
        meta_full[meta_full["profile_id"].notna() & (meta_full["profile_id"].str.strip().isin(["", "NaN", "nan"]) == False)]  # noqa: E712
        .drop_duplicates("profile_id")
        .set_index("profile_id")
    )
    # 3. by cleaned name
    by_name = meta_full.drop_duplicates("_name_key").set_index("_name_key")

    _result_cols = ["naics_raw", "profile_number"] + list(_defaults.keys())

    def _resolve(r: pd.Series) -> pd.Series:
        def _from(row: Any) -> pd.Series:
            return pd.Series({
                "naics":           row.get("naics_raw", ""),
                "profile_number":  row.get("profile_number", ""),
                "naics_sector":    row.get("naics_sector", "unknown"),
                "mechanism":       row.get("mechanism", "minimal_no_vector"),
                "exposure_vector": row.get("exposure_vector", ""),
                "cap_earnings":    row.get("cap_earnings", 3),
                "cap_supply_chain": row.get("cap_supply_chain", 3),
                "cap_macro":       row.get("cap_macro", 3),
            })

        pnum = str(r.get("profile_number", "") or "").strip()
        if pnum and pnum in by_pnum.index:
            return _from(by_pnum.loc[pnum])

        pid = str(r.get("profile_id", "") or "").strip()
        if pid and pid in by_pid.index:
            return _from(by_pid.loc[pid])

        nk = _clean_sedar_name(r.get("issuer_name", ""))
        if nk and nk in by_name.index:
            return _from(by_name.loc[nk])

        return pd.Series({"naics": "", "profile_number": "", **_defaults})

    out = df.copy()
    out = out.drop(
        columns=[c for c in [*_defaults.keys(), "profile_number"] if c in out.columns],
        errors="ignore",
    )

    resolved = out.apply(_resolve, axis=1)
    out = pd.concat([out, resolved], axis=1)

    for col, default in _defaults.items():
        out[col] = out[col].fillna(default)
    out["profile_number"] = out["profile_number"].fillna("")

    n_pnum = (out["profile_number"] != "").sum()
    n_sector = (out["naics_sector"] != "unknown").sum()
    logger.info(
        "sector_meta: profile_number resolved %s/%s; NAICS sector enriched %s/%s",
        n_pnum, len(out), n_sector, len(out),
    )
    return out

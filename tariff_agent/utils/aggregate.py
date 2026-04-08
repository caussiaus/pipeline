from __future__ import annotations

import pandas as pd

from tariff_agent.utils.config import Settings, get_settings
from tariff_agent.utils.sector_meta import (
    enrich_with_sector,
    fuzzy_from_score,
    ragin_fuzzy_from_score,
    get_profile,
    load_sector_profiles,
    _UNKNOWN_PROFILE,
)

_SCORE_DIMS = [
    ("earnings_tariff_score",     "cap_earnings",     "earnings"),
    ("supply_chain_tariff_score", "cap_supply_chain", "supply_chain"),
    ("macro_tariff_score",        "cap_macro",        "macro"),
]


def _apply_caps(df: pd.DataFrame) -> pd.DataFrame:
    """Clamp raw scores to per-row sector caps; add fuzzy and Ragin columns."""
    profiles = load_sector_profiles()

    for score_col, cap_col, dim in _SCORE_DIMS:
        adj_col   = f"{score_col}_adj"
        fuzzy_col = f"{dim}_fuzzy"
        ragin_col = f"{dim}_ragin_fuzzy"

        cap_series = df.get(cap_col, pd.Series(3, index=df.index)).fillna(3).astype(int)
        raw_series = df.get(score_col, pd.Series(0, index=df.index)).fillna(0).astype(int)
        df[adj_col] = raw_series.clip(upper=cap_series)

        def _fuzzy(row: pd.Series, _dim: str = dim, _ac: str = adj_col) -> float:
            naics = str(row.get("naics", "") or "")
            profile = get_profile(naics, profiles) if naics else _UNKNOWN_PROFILE
            return fuzzy_from_score(row[_ac], profile, _dim)

        def _ragin(row: pd.Series, _dim: str = dim, _ac: str = adj_col) -> float:
            naics = str(row.get("naics", "") or "")
            profile = get_profile(naics, profiles) if naics else _UNKNOWN_PROFILE
            return ragin_fuzzy_from_score(row[_ac], profile, _dim)

        df[fuzzy_col] = df.apply(_fuzzy, axis=1)
        df[ragin_col] = df.apply(_ragin, axis=1)

    return df


def build_issuer_year_table(settings: Settings | None = None, *, force: bool = False) -> pd.DataFrame:
    settings = settings or get_settings()
    out_path = settings.resolve(settings.issuer_year_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not force and settings.skip_aggregate_if_exists and out_path.is_file():
        return pd.read_csv(out_path)

    filings = pd.read_csv(settings.resolve(settings.filings_index_path))
    if settings.sedar_master_issuers_path.strip():
        filings = enrich_with_sector(filings, settings.sedar_master_issuers_path)
    doc_llm = pd.read_csv(settings.resolve(settings.filings_llm_csv))

    # Pass-2 CSV repeats filing metadata (ticker, profile_id, etc.). Merging both
    # sides without dropping duplicates yields ticker_x / ticker_y and breaks groupby.
    _from_index = {"profile_id", "ticker", "issuer_name", "filing_type", "filing_date"}
    _dup = [c for c in _from_index if c in doc_llm.columns]
    df = filings.merge(doc_llm.drop(columns=_dup, errors="ignore"), on="filing_id", how="inner")
    if "fiscal_year" not in df.columns:
        df["fiscal_year"] = pd.to_datetime(df["filing_date"], errors="coerce").dt.year
    else:
        miss = df["fiscal_year"].isna()
        if miss.any():
            df.loc[miss, "fiscal_year"] = pd.to_datetime(
                df.loc[miss, "filing_date"], errors="coerce"
            ).dt.year
    df = df[df["fiscal_year"].notna()]

    # Sector columns default to unknown/uncapped if enrichment was skipped
    for col, default in [
        ("naics_sector", "unknown"),
        ("mechanism", "minimal_no_vector"),
        ("cap_earnings", 3),
        ("cap_supply_chain", 3),
        ("cap_macro", 3),
    ]:
        if col not in df.columns:
            df[col] = default

    df = _apply_caps(df)

    group_cols = ["ticker", "profile_id", "fiscal_year"]
    sector_first = df.groupby(group_cols, as_index=False).agg(
        naics_sector=("naics_sector", "first"),
        mechanism=("mechanism", "first"),
    )

    agg_spec: dict[str, tuple[str, str]] = {
        "has_tariff_discussion":             ("has_tariff_discussion",           "max"),
        "max_earnings_tariff_score":         ("earnings_tariff_score",           "max"),
        "max_supply_chain_tariff_score":     ("supply_chain_tariff_score",       "max"),
        "max_macro_tariff_score":            ("macro_tariff_score",              "max"),
        "max_earnings_tariff_score_adj":     ("earnings_tariff_score_adj",       "max"),
        "max_supply_chain_tariff_score_adj": ("supply_chain_tariff_score_adj",   "max"),
        "max_macro_tariff_score_adj":        ("macro_tariff_score_adj",          "max"),
        "max_earnings_fuzzy":                ("earnings_fuzzy",                  "max"),
        "max_supply_chain_fuzzy":            ("supply_chain_fuzzy",              "max"),
        "max_macro_fuzzy":                   ("macro_fuzzy",                     "max"),
        "max_earnings_ragin_fuzzy":          ("earnings_ragin_fuzzy",            "max"),
        "max_supply_chain_ragin_fuzzy":      ("supply_chain_ragin_fuzzy",        "max"),
        "max_macro_ragin_fuzzy":             ("macro_ragin_fuzzy",               "max"),
    }
    agg = df.groupby(group_cols, as_index=False).agg(**agg_spec)
    agg = agg.merge(sector_first, on=group_cols, how="left")

    mask = df["has_tariff_discussion"] == True  # noqa: E712
    first_dates = (
        df.loc[mask]
        .groupby(group_cols, as_index=False)["filing_date"]
        .min()
        .rename(columns={"filing_date": "first_tariff_filing_date"})
    )
    agg = agg.merge(first_dates, on=group_cols, how="left")
    agg["first_tariff_filing_date"] = agg["first_tariff_filing_date"].fillna("")

    # Audit flag: rows where sector profile was not enriched from NAICS metadata
    agg["sector_unknown"] = agg["naics_sector"] == "unknown"

    agg.to_csv(out_path, index=False)
    return agg

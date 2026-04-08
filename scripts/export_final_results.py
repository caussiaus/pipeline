#!/usr/bin/env python3
"""Join index + Pass-2 + sector columns into one CSV for spreadsheets / review.

Writes:
  output/csv/final_filings_overview.csv
  output/csv/FINAL_RUN_SUMMARY.txt  (counts + artifact paths)
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd

from tariff_agent.utils.config import get_settings
from tariff_agent.utils.consistency_audit import (
    apply_index_identity,
    load_pass1_any_by_filing,
    write_consistency_report,
)
from tariff_agent.utils.sector_meta import enrich_with_sector


def main() -> None:
    s = get_settings()
    idx = pd.read_csv(s.resolve(s.filings_index_path), dtype=str)
    doc = pd.read_csv(s.resolve(s.filings_llm_csv), dtype=str)

    # enrich_with_sector adds profile_number (9-digit, universal SEDAR key) via
    # profile_number → profile_id → issuer_name priority join against master CSV.
    if s.sedar_master_issuers_path.strip():
        idx = enrich_with_sector(idx, s.sedar_master_issuers_path)

    # Persist enriched filings_index (adds profile_number + sector cols)
    idx.to_csv(s.resolve(s.filings_index_path), index=False)

    # Propagate identity + profile_number from index into filings_llm.csv
    apply_cols = ["profile_id", "profile_number", "ticker", "issuer_name", "filing_type", "filing_date"]
    doc = apply_index_identity(doc, idx, id_cols=apply_cols)
    doc.to_csv(s.resolve(s.filings_llm_csv), index=False)

    # final_filings_overview = index (with sector cols) + doc Pass-2 columns
    ddup = [c for c in apply_cols if c in doc.columns]
    doc_slim = doc.drop(columns=ddup, errors="ignore")
    m = idx.merge(doc_slim, on="filing_id", how="left")
    out1 = s.resolve(s.filings_llm_csv).parent / "final_filings_overview.csv"
    m.to_csv(out1, index=False)

    pass1 = load_pass1_any_by_filing(s)
    qc_path = write_consistency_report(doc, s, pass1_any=pass1)
    qc = pd.read_csv(qc_path)
    n_err = int((qc["qc_error_count"] > 0).sum())
    n_warn = int((qc["qc_warn_count"] > 0).sum())

    iy_path = s.resolve(s.issuer_year_csv)
    lines = [
        f"filings_index: {len(idx)}",
        f"final_filings_overview: {len(m)} rows -> {out1}",
        f"filings_llm has_tariff_discussion true: {(doc['has_tariff_discussion'].str.lower()=='true').sum()}",
        f"consistency_report: {qc_path} ({len(qc)} rows, {n_err} with errors, {n_warn} with warnings)",
        f"issuer_year: {iy_path} ({sum(1 for _ in open(iy_path)) - 1 if iy_path.is_file() else 0} data rows)",
        f"review_ready: {s.resolve(s.review_csv)}",
        f"chunks: {s.resolve(s.chunks_parquet)}",
    ]
    out2 = s.resolve(s.filings_llm_csv).parent / "FINAL_RUN_SUMMARY.txt"
    out2.write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()

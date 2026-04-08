from __future__ import annotations

from tariff_agent.state import PipelineState
from tariff_agent.utils.config import get_settings
from tariff_agent.utils.consistency_audit import (
    apply_index_identity,
    load_pass1_any_by_filing,
    write_consistency_report,
)
from tariff_agent.utils.sector_meta import enrich_with_sector

import pandas as pd


def export_node(state: PipelineState) -> dict:
    """Join index + Pass-2 + sector → final_filings_overview; write consistency report."""
    s = get_settings()
    idx = pd.read_csv(s.resolve(s.filings_index_path), dtype=str)
    doc = pd.read_csv(s.resolve(s.filings_llm_csv), dtype=str)

    if s.sedar_master_issuers_path.strip():
        idx = enrich_with_sector(idx, s.sedar_master_issuers_path)
    idx.to_csv(s.resolve(s.filings_index_path), index=False)

    apply_cols = ["profile_id", "profile_number", "ticker", "issuer_name", "filing_type", "filing_date"]
    doc = apply_index_identity(doc, idx, id_cols=apply_cols)
    doc.to_csv(s.resolve(s.filings_llm_csv), index=False)

    ddup = [c for c in apply_cols if c in doc.columns]
    m = idx.merge(doc.drop(columns=ddup, errors="ignore"), on="filing_id", how="left")
    out1 = s.resolve(s.filings_llm_csv).parent / "final_filings_overview.csv"
    m.to_csv(out1, index=False)

    pass1 = load_pass1_any_by_filing(s)
    qc_path = write_consistency_report(doc, s, pass1_any=pass1)
    qc = pd.read_csv(qc_path)
    n_err = int((qc["qc_error_count"] > 0).sum())
    n_warn = int((qc["qc_warn_count"] > 0).sum())

    return {"messages": [
        f"stage=export complete: {len(m)} filings, {n_err} QC errors, {n_warn} warnings → {out1}"
    ]}

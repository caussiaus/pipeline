"""Refinement node: targeted Pass-2 re-run on QC-flagged filings.

Reads ``filings_llm_consistency.csv``, selects filing IDs whose QC severity
meets the configured threshold, then calls ``run_llm_on_docs`` with only those
IDs so the rest of the parquet is preserved untouched.  After the targeted re-run
it rebuilds the issuer-year table and re-exports.

Graph meta keys consumed
------------------------
``refine_rules``   — ``|``-separated rule codes to target (default: all errors).
                     Example: "fls_only_tariff_signals|has_tariff_false_but_scores_positive"
``refine_severity`` — minimum severity to include: "error" | "warn" | "info" (default "error").
``refine_max``     — cap on number of filings to re-run in one pass (default 0 = no cap).
``refine_iter``    — iteration counter, auto-incremented (set to 0 by caller to start).
``max_refine_iters`` — abort loop after this many iterations (default 3).
"""
from __future__ import annotations

import logging

import pandas as pd

from tariff_agent.state import PipelineState
from tariff_agent.utils.aggregate import build_issuer_year_table
from tariff_agent.utils.config import get_settings
from tariff_agent.utils.doc_level import run_doc_level

logger = logging.getLogger(__name__)

_SEV_ORDER = {"error": 3, "warn": 2, "info": 1, "none": 0}

# Rules that require a fresh LLM call to fix (vs pure post-processing)
_LLM_FIXABLE = frozenset({
    "fls_only_tariff_signals",
    "fls_majority_tariff_signals",
    "has_tariff_false_but_scores_positive",
    "has_tariff_true_but_all_scores_zero",
    "tariff_direction_none_but_earnings_score_positive",
    "pass2_discussion_without_pass1_chunk_flag",
    "default_no_tariff_summary_but_key_quotes",
})


def _select_targets(
    qc: pd.DataFrame,
    *,
    rules: set[str] | None,
    min_severity: str,
    max_count: int,
) -> set[str]:
    min_s = _SEV_ORDER.get(min_severity, 3)

    mask = qc["qc_max_severity"].map(lambda s: _SEV_ORDER.get(str(s), 0) >= min_s)
    if rules:
        rule_mask = qc["qc_rules"].fillna("").apply(
            lambda cell: bool(set(str(cell).split("|")) & rules)
        )
        mask = mask & rule_mask

    targets = set(qc.loc[mask, "filing_id"].astype(str).tolist())

    # Only keep rules that a fresh LLM pass can actually fix
    llm_targets: set[str] = set()
    for _, row in qc[qc["filing_id"].isin(targets)].iterrows():
        row_rules = set(str(row.get("qc_rules", "")).split("|"))
        if row_rules & _LLM_FIXABLE:
            llm_targets.add(str(row["filing_id"]))

    if max_count > 0:
        llm_targets = set(list(llm_targets)[:max_count])

    return llm_targets


def refine_node(state: PipelineState) -> dict:
    meta = state.get("meta", {})
    s = get_settings()

    qc_path = s.resolve(s.consistency_report_csv)
    if not qc_path.is_file():
        return {"messages": ["stage=refine: no consistency report found — run export first"]}

    qc = pd.read_csv(qc_path, dtype=str)

    rules_raw = str(meta.get("refine_rules", "") or "")
    rules = set(r.strip() for r in rules_raw.split("|") if r.strip()) or None
    min_sev = str(meta.get("refine_severity", "error"))
    max_count = int(meta.get("refine_max", 0) or 0)
    iteration = int(meta.get("refine_iter", 0) or 0)
    max_iters = int(meta.get("max_refine_iters", 3) or 3)

    if iteration >= max_iters:
        return {"messages": [f"stage=refine: max iterations ({max_iters}) reached — stopping"]}

    targets = _select_targets(qc, rules=rules, min_severity=min_sev, max_count=max_count)
    if not targets:
        return {"messages": ["stage=refine: no LLM-fixable QC targets — nothing to re-run"]}

    logger.info("refine iter %s/%s: re-running Pass-2 on %s filings", iteration + 1, max_iters, len(targets))
    run_doc_level(update_filing_ids=targets)
    build_issuer_year_table(force=True)

    return {
        "messages": [f"stage=refine iter={iteration + 1}: re-ran Pass-2 on {len(targets)} filings"],
        "meta": {**meta, "refine_iter": iteration + 1},
    }

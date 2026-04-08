#!/usr/bin/env python3
"""Tariff / SEDAR analysis pipeline.

Stages (run individually with --stage, or chain them):
  parse       PDF → Docling JSON               [slow, one-time]
  chunk       Docling JSON → chunks.parquet    [fast, one-time]
  llm_chunk   Pass-1 chunk LLM extraction      [GPU, one-time]
  llm_doc     Pass-2 filing-level synthesis    [GPU, repeatable]
  aggregate   Issuer×year rollup               [instant]
  export      Final CSVs + QC consistency      [instant]
  refine      Re-run Pass-2 on QC-error rows   [GPU, targeted]
  all         Full graph: parse→…→export        (+ --refine for loop)

Quick iteration after data is on disk:
  python run_pipeline.py --stage refine            # fix QC errors, GPU required
  python run_pipeline.py --stage export            # rebuild CSVs, no GPU
  python run_pipeline.py --stage aggregate         # rebuild issuer-year, no GPU
"""
from __future__ import annotations

import argparse
import os

from tariff_agent.utils.config import ensure_hf_hub_env_for_process

ensure_hf_hub_env_for_process()

from tariff_agent.graph import run_full_pipeline
from tariff_agent.utils.aggregate import build_issuer_year_table
from tariff_agent.utils.chunking import run_chunking
from tariff_agent.utils.doc_level import run_doc_level
from tariff_agent.utils.docling_pipeline import run_docling_on_filings
from tariff_agent.utils.llm_client import run_llm_on_chunks
from tariff_agent.utils.vllm_lifecycle import maybe_start_vllm_after_parse


_SKIP_VARS = (
    "SKIP_PARSE_IF_EXISTS",
    "SKIP_CHUNK_IF_EXISTS",
    "SKIP_LLM_CHUNK_IF_EXISTS",
    "SKIP_LLM_DOC_IF_EXISTS",
    "SKIP_AGGREGATE_IF_EXISTS",
)


def _apply_no_skip_env() -> None:
    for k in _SKIP_VARS:
        os.environ[k] = "0"


def _run_export() -> None:
    """Rebuild final CSVs and QC consistency report — no GPU required."""
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from scripts.export_final_results import main as _export_main
    _export_main()


def _run_refine(
    *,
    rules: str = "",
    severity: str = "error",
    max_count: int = 0,
    max_iters: int = 1,
) -> None:
    """Re-run Pass-2 on QC-flagged filings, then rebuild aggregate + export."""
    import logging
    import pandas as pd
    from tariff_agent.utils.config import get_settings
    from tariff_agent.nodes.refine_node import _select_targets

    logger = logging.getLogger(__name__)
    s = get_settings()

    qc_path = s.resolve(s.consistency_report_csv)
    if not qc_path.is_file():
        print("No consistency report found — run 'export' first.")
        return

    for iteration in range(max_iters):
        qc = pd.read_csv(qc_path, dtype=str)
        rule_set = set(r.strip() for r in rules.split("|") if r.strip()) or None
        targets = _select_targets(qc, rules=rule_set, min_severity=severity, max_count=max_count)

        if not targets:
            print(f"Refine iter {iteration+1}: no LLM-fixable targets — done.")
            break

        print(f"Refine iter {iteration+1}/{max_iters}: re-running Pass-2 on {len(targets)} filings …")
        run_doc_level(update_filing_ids=targets)
        build_issuer_year_table(force=True)
        _run_export()
        qc_path = s.resolve(s.consistency_report_csv)

    print("Refinement complete.")


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--stage",
        choices=("all", "parse", "chunk", "llm_chunk", "llm_doc",
                 "aggregate", "export", "refine"),
        default="all",
        help="Stage to run (default: all).",
    )
    p.add_argument("--no-skip",  action="store_true",
                   help="Recompute stages even when outputs already exist.")
    p.add_argument("--refine",   action="store_true",
                   help="(--stage all only) Run refine loop after export.")
    p.add_argument("--refine-rules", default="",
                   help="Pipe-separated QC rule codes to target (default: all LLM-fixable errors).")
    p.add_argument("--refine-severity", default="error",
                   choices=("error", "warn", "info"),
                   help="Minimum QC severity to include in refine targets (default: error).")
    p.add_argument("--refine-max", type=int, default=0,
                   help="Max filings per refine pass (0 = no cap).")
    p.add_argument("--refine-iters", type=int, default=1,
                   help="Max refine iterations (default: 1 for --stage refine; 3 for --stage all --refine).")
    p.add_argument("--thread-id", default="default",
                   help="LangGraph checkpoint thread id.")
    args = p.parse_args()

    force = bool(args.no_skip)
    if force:
        _apply_no_skip_env()

    if args.stage == "all":
        run_full_pipeline(
            thread_id=args.thread_id,
            force=force,
            refine=args.refine,
            max_refine_iters=args.refine_iters if args.refine else 0,
            refine_rules=args.refine_rules,
            refine_severity=args.refine_severity,
        )
        return

    if args.stage == "parse":
        run_docling_on_filings(force=force)
        maybe_start_vllm_after_parse()
    elif args.stage == "chunk":
        run_chunking(force=force)
    elif args.stage == "llm_chunk":
        run_llm_on_chunks(force=force)
    elif args.stage == "llm_doc":
        run_doc_level(force=force)
    elif args.stage == "aggregate":
        build_issuer_year_table(force=force)
    elif args.stage == "export":
        _run_export()
    elif args.stage == "refine":
        _run_refine(
            rules=args.refine_rules,
            severity=args.refine_severity,
            max_count=args.refine_max,
            max_iters=args.refine_iters,
        )


if __name__ == "__main__":
    main()

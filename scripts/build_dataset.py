#!/usr/bin/env python3
"""Interactive dataset generation CLI.

Two-phase workflow:

  PHASE 1 — DESIGN (sample mode)
    Work on a small set of tickers (3-5 companies) to validate fields
    before running on the full corpus. Interactive vLLM profile is used
    (lower concurrency, tunable temperature). All schema iterations and
    any human corrections are stored in output/feedback/{run_id}/.

  PHASE 2 — PRODUCTION (full corpus)
    Once schema is approved from sample, run on all 602 filings using
    the batch vLLM profile (zero temperature, max throughput).

vLLM is NOT relaunched between phases — request parameters are routed
per-profile at the client level.

Usage examples:

  # Design phase — 3-company sample, interactive loop
  python scripts/build_dataset.py \\
      --query "find all mentions of steel tariff cost impacts" \\
      --sample-tickers TSX:ASTL TSX:CFP NYSE:WCN

  # Skip sample, go straight to full corpus
  python scripts/build_dataset.py \\
      --query "find all mentions of Liberation Day tariffs" \\
      --no-sample

  # Evidence-first mode (slower, more auditable)
  python scripts/build_dataset.py \\
      --query "..." --sample-tickers TSX:ASTL TSX:GIL --mode evidence

  # Auto-approve (scripting / CI)
  python scripts/build_dataset.py --query "..." --auto-approve --no-sample
"""
from __future__ import annotations

import argparse
import json
import sys
import textwrap
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tariff_agent.dataset_graph.critique_node import critique_node
from tariff_agent.dataset_graph.extraction_node import extraction_node
from tariff_agent.dataset_graph.feedback_store import (
    list_runs,
    log_schema_iteration,
    new_run_id,
)
from tariff_agent.dataset_graph.graph import export_node
from tariff_agent.dataset_graph.schema_node import schema_node
from tariff_agent.dataset_graph.state import DatasetState


# ---------------------------------------------------------------------------
# Terminal helpers
# ---------------------------------------------------------------------------

def _hr(char: str = "─", width: int = 72) -> str:
    return char * width


def _section(title: str) -> None:
    print(f"\n{_hr('═')}")
    print(f"  {title}")
    print(_hr("─"))


def _print_schema(state: DatasetState) -> None:
    cols = state.get("proposed_columns", [])
    name = state.get("dataset_name", "")
    desc = state.get("dataset_description", "")
    print(f"\n  Dataset: {name}")
    print(f"  {desc}\n")
    for i, col in enumerate(cols, 1):
        mode_tag = f"[{col.get('mode', 'direct')}]" if col.get("mode") else ""
        print(f"  [{i}] {col['name']}  ({col['type']}) {mode_tag}")
        print(f"       {col['description']}")
        instr = textwrap.fill(
            col.get("extraction_instruction", ""), width=60,
            initial_indent="       → ", subsequent_indent="         ",
        )
        print(instr)
        print(f"       default: {json.dumps(col.get('default'))}")
    print()


def _ask(prompt: str, default: str = "") -> str:
    try:
        val = input(prompt).strip()
        return val if val else default
    except (EOFError, KeyboardInterrupt):
        return default


def _print_sample_note(tickers: list[str]) -> None:
    print(f"\n  DESIGN MODE — running on {len(tickers)} tickers: {', '.join(tickers)}")
    print("  Once schema is approved here, use --no-sample to run on all 602 filings.\n")


# ---------------------------------------------------------------------------
# Schema approval loop
# ---------------------------------------------------------------------------

def _schema_loop(
    state: DatasetState,
    run_id: str,
    *,
    auto_approve: bool = False,
) -> DatasetState:
    while True:
        _section(f"SCHEMA PROPOSAL  (iteration {state.get('schema_iteration', 1)})")
        _print_schema(state)

        if auto_approve:
            print("  [auto-approve] Schema approved.")
            log_schema_iteration(
                run_id,
                iteration=state.get("schema_iteration", 1),
                dataset_name=state.get("dataset_name", ""),
                user_query=state.get("user_query", ""),
                proposed_columns=state.get("proposed_columns", []),
                user_feedback="",
                approved=True,
            )
            return {**state, "schema_approved": True}

        print("  Each field will also emit _evidence_quote / _evidence_pages / _evidence_section")
        print("  columns so every cell is traceable to its source span.\n")

        choice = _ask(
            "  Approve schema? [y] yes / [n] feedback / [m] set field mode / [q] quit: ",
            default="y",
        ).lower()

        if choice in ("q", "quit", "exit"):
            sys.exit(0)

        if choice in ("m", "mode"):
            _set_field_modes(state)
            continue

        if choice in ("y", "yes", ""):
            log_schema_iteration(
                run_id,
                iteration=state.get("schema_iteration", 1),
                dataset_name=state.get("dataset_name", ""),
                user_query=state.get("user_query", ""),
                proposed_columns=state.get("proposed_columns", []),
                user_feedback="",
                approved=True,
            )
            return {**state, "schema_approved": True, "schema_feedback": ""}

        feedback = _ask(
            "  Describe changes (add/remove columns, rewording, data type changes): ",
        )
        if not feedback:
            return {**state, "schema_approved": True}

        log_schema_iteration(
            run_id,
            iteration=state.get("schema_iteration", 1),
            dataset_name=state.get("dataset_name", ""),
            user_query=state.get("user_query", ""),
            proposed_columns=state.get("proposed_columns", []),
            user_feedback=feedback,
            approved=False,
        )

        print("  Refining schema…")
        state = {**state, "schema_approved": False, "schema_feedback": feedback}
        state = schema_node(state)
        if state.get("error"):
            print(f"  Error: {state['error']}")
            sys.exit(1)


def _set_field_modes(state: DatasetState) -> None:
    """Interactively toggle field extraction mode (direct vs evidence-first)."""
    cols = state.get("proposed_columns", [])
    print("\n  Field modes (direct = fast, evidence = collect quotes first then decide):")
    for i, col in enumerate(cols, 1):
        print(f"  [{i}] {col['name']} — {col.get('mode', 'direct')}")
    idx_str = _ask("  Enter field number to toggle (or Enter to skip): ")
    if idx_str.isdigit():
        i = int(idx_str) - 1
        if 0 <= i < len(cols):
            current = cols[i].get("mode", "direct")
            cols[i]["mode"] = "evidence" if current == "direct" else "direct"
            print(f"  → {cols[i]['name']} mode: {cols[i]['mode']}")


# ---------------------------------------------------------------------------
# Post-extraction critique
# ---------------------------------------------------------------------------

def _critique_loop(
    state: DatasetState,
    *,
    auto_approve: bool,
    no_critique: bool,
) -> DatasetState:
    if no_critique or auto_approve:
        return {**state, "export_approved": True}

    _section("DATASET QUALITY CRITIQUE")
    state = critique_node(state)

    rows = state.get("rows", [])
    cols = state.get("proposed_columns", [])

    # Fill-rate + evidence coverage
    print("\n  Fill-rate & evidence coverage:")
    for col in cols:
        n_filled = sum(1 for r in rows if r.get(col["name"]) not in (None, "", False, 0))
        n_evidence = sum(1 for r in rows if r.get(f"{col['name']}_evidence_quote"))
        pct = 100 * n_filled / max(len(rows), 1)
        ev_pct = 100 * n_evidence / max(n_filled, 1)
        bar = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
        print(f"    {col['name']:<32} {bar} {pct:5.1f}% filled  {ev_pct:5.1f}% with evidence")

    no_ev_rows = sum(1 for r in rows if r.get("_pass1_positive", 0) in (0, "0"))
    print(f"\n  Proof-of-absence: {no_ev_rows} filings searched with no evidence found")

    print(f"\n  LLM critique:")
    for line in state.get("critique_text", "").splitlines():
        print(f"    {line}")

    for s in state.get("critique_suggestions", []):
        print(f"    • {s}")

    quality = state.get("critique_quality", "ok")
    icon = {"good": "✓", "ok": "~", "needs_work": "✗"}.get(quality, "~")
    print(f"\n  Quality: {icon} {quality.upper()}")

    choice = _ask(
        "\n  Export? [y] yes / [r] revise schema / [q] quit: ",
        default="y",
    ).lower()

    if choice in ("q", "quit"):
        sys.exit(0)
    if choice in ("r", "revise"):
        return {**state, "export_approved": False}
    return {**state, "export_approved": True}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Interactive corpus-to-CSV dataset generation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--query", required=True, help="What to find across the filing corpus")
    ap.add_argument("--sample-tickers", nargs="+", metavar="TICKER",
                    help="Tickers for design phase (e.g. TSX:ASTL TSX:CFP). Default: first 3 tariff-positive")
    ap.add_argument("--no-sample", action="store_true",
                    help="Skip design phase, run directly on full corpus")
    ap.add_argument("--mode", choices=["direct", "evidence"], default="direct",
                    help="Extraction mode: direct (faster) or evidence-first (more auditable)")
    ap.add_argument("--auto-approve", action="store_true")
    ap.add_argument("--no-critique", action="store_true")
    ap.add_argument("--resume-run", metavar="RUN_ID",
                    help="Resume a previous session's schema (prints previous runs if omitted with --list-runs)")
    ap.add_argument("--list-runs", action="store_true", help="List previous feedback runs and exit")
    ap.add_argument("--max-schema-iters", type=int, default=8)
    args = ap.parse_args()

    if args.list_runs:
        runs = list_runs()
        if not runs:
            print("No feedback runs found.")
        else:
            print(f"{'RUN ID':<10} {'DATASET':<30} {'SCHEMA ITERS':>12} {'CORRECTIONS':>12}  LAST MODIFIED")
            for r in runs:
                print(f"{r['run_id']:<10} {r['dataset_name']:<30} {r['schema_iters']:>12} {r['cell_corrections']:>12}  {r['last_modified']}")
        sys.exit(0)

    run_id = args.resume_run or new_run_id()
    print(f"\n{'═' * 72}")
    print("  INTERACTIVE DATASET GENERATOR")
    print(f"  Query  : {args.query}")
    print(f"  Mode   : {args.mode}")
    print(f"  Run ID : {run_id}  (feedback → output/feedback/{run_id}/)")
    print(f"{'═' * 72}")

    # ── Auto-select sample tickers if not provided ────────────────────────
    sample_tickers: list[str] = args.sample_tickers or []
    if not args.no_sample and not sample_tickers:
        import pandas as pd
        from tariff_agent.utils.config import get_settings
        cfg = get_settings()
        llm = pd.read_csv(cfg.resolve(cfg.filings_llm_csv), dtype=str)
        idx = pd.read_csv(cfg.resolve(cfg.filings_index_path), dtype=str)
        tariff_tickers = llm[llm["has_tariff_discussion"].str.lower() == "true"]["ticker"].unique().tolist()
        # Pick diverse sectors: try to get steel/lumber/manufacturing
        preferred = ["TSX:ASTL", "TSX:CFP", "NYSE:WCN", "TSX:GIL", "TSX:MX"]
        sample_tickers = [t for t in preferred if t in tariff_tickers][:3]
        if len(sample_tickers) < 3:
            sample_tickers += [t for t in tariff_tickers if t not in sample_tickers][:3 - len(sample_tickers)]

    use_sample = not args.no_sample and bool(sample_tickers)

    state: DatasetState = {
        "user_query": args.query,
        "schema_iteration": 0,
        "schema_approved": False,
        "extraction_done": False,
        "export_approved": False,
        "rows": [],
        "cells": [],
        "sample_tickers": sample_tickers,
        "use_sample": use_sample,
        "extraction_mode": args.mode,
        "feedback_run_id": run_id,
    }

    # ── Phase 1: Schema design ────────────────────────────────────────────
    print("\n  Designing extraction schema…")
    state = schema_node(state)
    if state.get("error"):
        print(f"  Error: {state['error']}")
        sys.exit(1)

    state = _schema_loop(state, run_id, auto_approve=args.auto_approve)

    # ── Phase 2: Sample extraction ────────────────────────────────────────
    if use_sample:
        _print_sample_note(sample_tickers)
        _section(f"SAMPLE EXTRACTION ({len(sample_tickers)} tickers, interactive profile)")
        print(f"  Fields: {len(state.get('proposed_columns', []))} columns (+ evidence spans per field)\n")
        state = extraction_node(state)
        if state.get("error"):
            print(f"  Extraction error: {state['error']}")
            sys.exit(1)

        sample_rows = state.get("rows", [])
        print(f"\n  Sample rows extracted: {len(sample_rows)}")
        _print_sample_results(state)

        choice = _ask(
            "\n  [y] approve schema & run full corpus  "
            "[r] revise schema  [q] quit: ",
            default="y",
        ).lower()

        if choice in ("q", "quit"):
            sys.exit(0)

        iters = 0
        while choice in ("r", "revise") and iters < args.max_schema_iters:
            iters += 1
            feedback = _ask("  Describe changes: ")
            if feedback:
                state = {**state, "schema_feedback": feedback, "schema_approved": False}
                state = schema_node(state)
            state = _schema_loop(state, run_id, auto_approve=False)
            state = {**state, "use_sample": True}
            state = extraction_node(state)
            _print_sample_results(state)
            choice = _ask(
                "\n  [y] approve & run full corpus  [r] revise  [q] quit: ",
                default="y",
            ).lower()

        if choice in ("q", "quit"):
            sys.exit(0)

        # Switch to full-corpus batch mode
        state = {**state, "use_sample": False}

    # ── Phase 3: Full-corpus extraction ──────────────────────────────────
    _section("FULL CORPUS EXTRACTION (batch profile)")
    n = 602  # approximate
    print(f"  Running on ~{n} filings, batch profile, mode={args.mode}")
    print("  Evidence spans included per field — every cell is traceable.\n")
    state = extraction_node(state)
    if state.get("error"):
        print(f"  Extraction error: {state['error']}")
        sys.exit(1)

    rows = state.get("rows", [])
    print(f"\n  Extracted {len(rows)} rows")
    _print_absence_proof(rows)

    # ── Phase 4: Critique ─────────────────────────────────────────────────
    state = _critique_loop(
        state,
        auto_approve=args.auto_approve,
        no_critique=args.no_critique,
    )

    iters = 0
    while not state.get("export_approved") and iters < args.max_schema_iters:
        iters += 1
        state = schema_node(state)
        state = _schema_loop(state, run_id, auto_approve=args.auto_approve)
        state = extraction_node(state)
        state = _critique_loop(
            state,
            auto_approve=args.auto_approve,
            no_critique=args.no_critique,
        )

    # ── Phase 5: Export ───────────────────────────────────────────────────
    state = export_node(state)
    if state.get("error"):
        print(f"  Export error: {state['error']}")
        sys.exit(1)

    path = state.get("dataset_path", "")
    _section("DONE")
    print(f"  Dataset  : {path}")
    print(f"  Run ID   : {run_id}")
    print(f"  Rows     : {len(state.get('rows', []))}")
    n_schema_cols = len(state.get("proposed_columns", []))
    print(f"  Columns  : {n_schema_cols} schema × 4 (value + 3 evidence) + 7 identity + 4 provenance")
    print(
        f"\n  Feedback stored: output/feedback/{run_id}/\n"
        f"    schema.jsonl     — schema iteration history\n"
        f"    extraction.jsonl — cell corrections (add via review_app.py)\n"
        f"    merge.jsonl      — conflict resolutions\n"
        f"\n  SFT pairs: python scripts/build_sft_dataset.py (picks up feedback automatically)\n"
        f"\n  Review app:\n"
        f"    streamlit run scripts/review_app.py\n"
    )


def _print_sample_results(state: DatasetState) -> None:
    rows = state.get("rows", [])
    cols = state.get("proposed_columns", [])
    if not rows:
        return
    print("\n  Sample fill-rate:")
    for col in cols:
        n_filled = sum(1 for r in rows if r.get(col["name"]) not in (None, "", False, 0))
        n_ev = sum(1 for r in rows if r.get(f"{col['name']}_evidence_quote"))
        print(
            f"    {col['name']:<30} {n_filled}/{len(rows)} filled, "
            f"{n_ev} with evidence"
        )
    print()
    print("  Sample values (first 3 rows):")
    for row in rows[:3]:
        print(f"    [{row.get('ticker','?')} | {row.get('filing_date','?')}]")
        for col in cols:
            val = row.get(col["name"])
            ev = row.get(f"{col['name']}_evidence_quote")
            ev_pages = row.get(f"{col['name']}_evidence_pages", "")
            if val not in (None, "", False, 0) or ev:
                print(f"      {col['name']}: {val}")
                if ev:
                    print(f"        evidence (pp{ev_pages}): \"{str(ev)[:120]}\"")


def _print_absence_proof(rows: list[dict]) -> None:
    no_ev = [r for r in rows if str(r.get("_pass1_positive", 0)) == "0"]
    print(
        f"  Proof of absence: {len(no_ev)}/{len(rows)} filings had no Pass-1 tariff evidence.\n"
        f"  For each: all chunks were parsed → keyword-filtered → Pass-1 LLM scored → 0 positive.\n"
        f"  Columns _all_chunks, _keyword_hits, _pass1_positive record search breadth per row."
    )


if __name__ == "__main__":
    # Fix syntax error in while loop for sample revision
    main()

"""Page 5 — Feedback & SFT Export.

View correction history across all runs, export SFT training pairs,
and see which schema decisions were made.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tariff_agent.dataset_graph.feedback_store import (
    export_sft_pairs,
    list_runs,
    load_feedback,
)


def page() -> None:
    st.header("Feedback History & SFT Export")
    st.caption(
        "Every schema iteration, cell correction, and merge decision is stored here. "
        "Use the SFT export to build training pairs for fine-tuning your extraction model."
    )

    runs = list_runs()
    if not runs:
        st.info("No feedback runs yet. Use the Build Dataset page to start a session.")
        return

    # ── Run selector ──────────────────────────────────────────────────────
    run_options = [
        f"{r['run_id']} — {r['dataset_name']} ({r['schema_iters']} schema iters, {r['cell_corrections']} corrections)"
        for r in runs
    ]
    sel_i = st.selectbox("Select run", range(len(run_options)), format_func=lambda i: run_options[i])
    run = runs[sel_i]
    run_id = run["run_id"]

    c1, c2, c3 = st.columns(3)
    c1.metric("Schema iterations", run["schema_iters"])
    c2.metric("Cell corrections", run["cell_corrections"])
    c3.metric("Merge decisions", run["merge_decisions"])

    # ── Three-level view ──────────────────────────────────────────────────
    tab_schema, tab_extraction, tab_merge, tab_sft = st.tabs([
        "📐 Schema history", "✏️ Cell corrections", "🔀 Merge decisions", "🧪 SFT export"
    ])

    with tab_schema:
        records = load_feedback(run_id, "schema")
        if not records:
            st.info("No schema feedback recorded for this run.")
        else:
            for r in records:
                with st.expander(
                    f"Iteration {r.get('iteration', '?')} — "
                    f"{'✅ approved' if r.get('approved') else '↩ rejected'}"
                ):
                    st.markdown(f"**User feedback:** {r.get('user_feedback', '(none)')}")
                    cols_df = pd.DataFrame(r.get("proposed_columns", []))
                    if not cols_df.empty:
                        st.dataframe(cols_df[["name", "type", "description"]].astype(str),
                                     use_container_width=True)

    with tab_extraction:
        records = load_feedback(run_id, "extraction")
        if not records:
            st.info("No cell corrections recorded. Use the Browse page to annotate documents.")
        else:
            df = pd.DataFrame([{
                "field": r.get("field_name"),
                "ticker": r.get("ticker"),
                "proposed": r.get("proposed_value"),
                "override": r.get("override_value"),
                "reason": r.get("override_reason"),
                "evidence_quote": (r.get("evidence") or {}).get("quote", ""),
                "pages": (r.get("evidence") or {}).get("pages", ""),
            } for r in records])
            st.dataframe(df, use_container_width=True)

    with tab_merge:
        records = load_feedback(run_id, "merge")
        if not records:
            st.info("No merge decisions recorded.")
        else:
            st.dataframe(pd.DataFrame(records), use_container_width=True)

    with tab_sft:
        st.markdown(
            "Convert cell corrections into **(system, user, assistant)** SFT training pairs. "
            "Each correction where a cell value was changed becomes a training example."
        )
        pairs = export_sft_pairs(run_id)
        st.metric("Training pairs from this run", len(pairs))

        if pairs:
            preview_df = pd.DataFrame([{
                "field": p["metadata"]["field_name"],
                "ticker": p["metadata"].get("filing_id", "")[:12],
                "proposed": p["metadata"]["proposed_value"],
                "correct": p["assistant"],
                "reason": p["metadata"]["override_reason"],
            } for p in pairs])
            st.dataframe(preview_df, use_container_width=True)

            sft_jsonl = "\n".join(
                __import__("json").dumps({"system": p["system"], "user": p["user"], "assistant": p["assistant"]})
                for p in pairs
            )
            st.download_button(
                "⬇ Download SFT pairs (JSONL)",
                data=sft_jsonl.encode(),
                file_name=f"sft_{run_id}.jsonl",
                mime="application/jsonl",
            )

        # Aggregate all runs
        st.divider()
        if st.button("Export ALL runs as SFT dataset"):
            all_pairs = []
            for r in list_runs():
                all_pairs.extend(export_sft_pairs(r["run_id"]))
            st.metric("Total SFT pairs across all runs", len(all_pairs))
            if all_pairs:
                import json
                sft_all = "\n".join(
                    json.dumps({"system": p["system"], "user": p["user"], "assistant": p["assistant"]})
                    for p in all_pairs
                )
                st.download_button(
                    "⬇ Download all SFT pairs",
                    data=sft_all.encode(),
                    file_name="sft_all_runs.jsonl",
                    mime="application/jsonl",
                )

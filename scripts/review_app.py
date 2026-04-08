"""Streamlit human-review application for the tariff pipeline.

Two tabs:
  1. Review  — browse filings, see LLM predictions, flag/correct, export corrections
  2. Build   — interactive dataset generation (schema propose → approve → extract → CSV)

Run:
    streamlit run scripts/review_app.py
    streamlit run scripts/review_app.py -- --dataset output/datasets/my_dataset.csv
"""
from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tariff_agent.utils.config import get_settings

# ── Page config ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Tariff Pipeline Review",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

cfg = get_settings()


# ──────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def _load_filings_llm() -> pd.DataFrame:
    p = cfg.resolve(cfg.filings_llm_csv)
    if not p.exists():
        return pd.DataFrame()
    return pd.read_csv(p, dtype=str)


@st.cache_data(show_spinner=False)
def _load_index() -> pd.DataFrame:
    p = cfg.resolve(cfg.filings_index_path)
    if not p.exists():
        return pd.DataFrame()
    return pd.read_csv(p, dtype=str)


@st.cache_data(show_spinner=False)
def _load_chunks() -> pd.DataFrame:
    p = cfg.resolve(cfg.chunks_parquet)
    if not p.exists():
        return pd.DataFrame()
    return pd.read_parquet(p)


@st.cache_data(show_spinner=False)
def _load_chunks_llm() -> pd.DataFrame:
    p = cfg.resolve(cfg.chunks_llm_parquet)
    if not p.exists():
        return pd.DataFrame()
    return pd.read_parquet(p)


@st.cache_data(show_spinner=False)
def _load_consistency() -> pd.DataFrame:
    p = cfg.resolve(cfg.consistency_report_csv)
    if not p.exists():
        return pd.DataFrame()
    return pd.read_csv(p, dtype=str)


def _load_dataset(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    return pd.read_csv(p, dtype=str)


def _parse_key_quotes(raw: str) -> list[dict]:
    if not raw or str(raw).strip() in ("", "nan", "[]"):
        return []
    try:
        v = json.loads(raw)
        if isinstance(v, list):
            return v
    except Exception:
        pass
    return []


def _save_correction(row_data: dict, corrections_path: Path) -> None:
    corrections_path.parent.mkdir(parents=True, exist_ok=True)
    import csv
    write_header = not corrections_path.exists()
    with open(corrections_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row_data.keys()))
        if write_header:
            writer.writeheader()
        writer.writerow(row_data)


# ──────────────────────────────────────────────────────────────────────────
# Tab 1 — Review
# ──────────────────────────────────────────────────────────────────────────

def _tab_review() -> None:
    st.subheader("Filing Review & Correction")

    filings = _load_filings_llm()
    if filings.empty:
        st.warning("No filings_llm.csv found. Run the pipeline first.")
        return

    idx = _load_index()
    consistency = _load_consistency()
    chunks = _load_chunks()
    chunks_llm = _load_chunks_llm()

    corrections_path = cfg.resolve("output/human_review/corrections.csv")

    # ── Sidebar filters ───────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("### Filters")
        only_tariff = st.checkbox("Only tariff-positive filings", value=True)
        only_flagged = st.checkbox("Only QC-flagged filings", value=False)
        only_fls = st.checkbox("Only FLS-only flagged", value=False)

    df = filings.copy()

    if only_tariff:
        df = df[df["has_tariff_discussion"].astype(str).str.lower() == "true"]
    if only_flagged and not consistency.empty and "filing_id" in consistency.columns:
        flagged_ids = consistency[consistency["severity"].astype(str).str.lower() == "error"]["filing_id"].unique()
        df = df[df["filing_id"].isin(flagged_ids)]
    if only_fls and not consistency.empty and "fls_only" in consistency.columns:
        fls_ids = consistency[consistency["fls_only"].astype(str).str.lower() == "true"]["filing_id"].unique()
        df = df[df["filing_id"].isin(fls_ids)]

    # ── Filing selector ───────────────────────────────────────────────────
    if df.empty:
        st.info("No filings match current filters.")
        return

    display_options = [
        f"{row.get('ticker','?')} | {row.get('filing_date','?')} | {row.get('filing_id','')[:12]}…"
        for _, row in df.iterrows()
    ]
    selected_idx = st.selectbox("Select filing", range(len(display_options)),
                                format_func=lambda i: display_options[i])

    sel_row = df.iloc[selected_idx]
    filing_id = str(sel_row.get("filing_id", ""))

    # ── Two columns: filing info + chunk evidence ─────────────────────────
    left, right = st.columns([1, 1])

    with left:
        st.markdown("#### Filing summary")
        st.markdown(f"**Company:** {sel_row.get('issuer_name', '')}  `{sel_row.get('ticker', '')}`")
        st.markdown(f"**Profile #:** `{sel_row.get('profile_number', '')}`")
        st.markdown(f"**Date:** {sel_row.get('filing_date', '')}  |  **Type:** {sel_row.get('filing_type', '')}")
        st.markdown(f"**NAICS sector:** {sel_row.get('naics_sector', '')}  |  **Mechanism:** {sel_row.get('mechanism', '')}")

        ht = str(sel_row.get("has_tariff_discussion", "")).lower() == "true"
        st.markdown(f"**Has tariff discussion:** {'✅ Yes' if ht else '❌ No'}")
        st.markdown(f"**Disclosure quality:** `{sel_row.get('disclosure_quality', '')}`")
        st.markdown(f"**Tariff direction:** `{sel_row.get('tariff_direction', '')}`")

        # Scores
        scols = ["earnings_tariff_score", "supply_chain_tariff_score", "macro_tariff_score"]
        score_vals = {c: sel_row.get(c, "0") for c in scols}
        s1, s2, s3 = st.columns(3)
        s1.metric("Earnings", score_vals["earnings_tariff_score"])
        s2.metric("Supply Chain", score_vals["supply_chain_tariff_score"])
        s3.metric("Macro", score_vals["macro_tariff_score"])

        st.markdown("**Summary:**")
        st.info(sel_row.get("doc_summary_sentence", ""))

        # Key quotes with page citations
        kq = _parse_key_quotes(str(sel_row.get("key_quotes", "[]")))
        if kq:
            st.markdown("**Key quotes (span-level citations):**")
            for i, q in enumerate(kq, 1):
                p0, p1 = q.get("page_start", 0), q.get("page_end", 0)
                sec = q.get("section_path", "")
                sig = q.get("signal_type", "")
                st.markdown(
                    f"*{i}. [{sig}] pp.{p0}–{p1} — `{sec}`*\n\n"
                    f"> {q.get('quote', '')[:300]}"
                )

        # QC flags
        if not consistency.empty and "filing_id" in consistency.columns:
            qc_rows = consistency[consistency["filing_id"] == filing_id]
            if not qc_rows.empty:
                st.markdown("**QC flags:**")
                for _, qr in qc_rows.iterrows():
                    sev = str(qr.get("severity", ""))
                    rule = str(qr.get("rule", ""))
                    color = "🔴" if sev == "error" else "🟡"
                    st.markdown(f"{color} `{rule}` ({sev})")

    with right:
        st.markdown("#### Evidence chunks (Pass-1 positive)")

        if not chunks.empty and "filing_id" in chunks.columns:
            f_chunks = chunks[chunks["filing_id"] == filing_id]
        else:
            f_chunks = pd.DataFrame()

        if not chunks_llm.empty and "filing_id" in chunks_llm.columns:
            f_llm = chunks_llm[chunks_llm["filing_id"] == filing_id]
            if "mentions_tariffs" in f_llm.columns:
                pos_ids = set(
                    f_llm[f_llm["mentions_tariffs"].astype(str).str.lower().isin(["true", "1"])]["chunk_id"].astype(str).tolist()
                )
            else:
                pos_ids = set()
        else:
            f_llm = pd.DataFrame()
            pos_ids = set()

        all_count = len(f_chunks)
        pos_count = len(pos_ids)
        # keyword_hit lives in chunks.parquet, not chunks_llm
        kw_count = 0
        if not f_chunks.empty and "keyword_hit" in f_chunks.columns:
            kw_count = int(f_chunks["keyword_hit"].astype(str).str.lower().isin(["true", "1"]).sum())

        st.caption(
            f"Search coverage: **{all_count}** total chunks | "
            f"**{kw_count}** keyword hits | "
            f"**{pos_count}** tariff-positive"
        )

        if pos_count == 0:
            st.info(
                f"**Proof of absence:** {all_count} chunks searched, {kw_count} keyword hits, "
                f"0 tariff-positive. No tariff signal found in this filing."
            )
        elif not f_chunks.empty:
            pos_chunks = f_chunks[f_chunks["chunk_id"].astype(str).isin(pos_ids)]
            for _, cr in pos_chunks.iterrows():
                with st.expander(
                    f"pp.{cr.get('page_start','?')}–{cr.get('page_end','?')} "
                    f"| {str(cr.get('section_path',''))[:60]}"
                ):
                    st.text(str(cr.get("text", ""))[:1500])

    # ── Correction form ───────────────────────────────────────────────────
    st.divider()
    st.markdown("#### Annotate this filing")

    with st.form(key=f"correction_{filing_id}"):
        c1, c2, c3 = st.columns(3)
        correct_ht = c1.selectbox("has_tariff_discussion",
                                  options=["(unchanged)", "True", "False"],
                                  index=0)
        correct_dq = c2.selectbox("disclosure_quality",
                                  options=["(unchanged)", "BOILERPLATE", "SPECIFIC_QUALITATIVE", "SPECIFIC_QUANTITATIVE"],
                                  index=0)
        correct_fls = c3.selectbox("fls_only (override)",
                                   options=["(unchanged)", "True", "False"],
                                   index=0)
        notes = st.text_area("Reviewer notes", placeholder="Why are you overriding? (required if changing)")
        submitted = st.form_submit_button("Save correction")

    if submitted:
        changes = {k: v for k, v in {
            "has_tariff_discussion": correct_ht,
            "disclosure_quality": correct_dq,
            "fls_only": correct_fls,
        }.items() if v != "(unchanged)"}

        if changes:
            correction = {
                "filing_id": filing_id,
                "ticker": sel_row.get("ticker", ""),
                "reviewer_timestamp": pd.Timestamp.now().isoformat(),
                "notes": notes,
                **changes,
            }
            _save_correction(correction, corrections_path)
            st.success(f"Saved correction for {filing_id}")
        else:
            st.warning("No changes selected.")

    # ── Export corrections ────────────────────────────────────────────────
    if corrections_path.exists():
        corr_df = pd.read_csv(corrections_path)
        st.caption(f"{len(corr_df)} corrections saved to `{corrections_path}`")
        with st.expander("Download corrections CSV"):
            st.download_button(
                "Download corrections.csv",
                data=corr_df.to_csv(index=False).encode(),
                file_name="corrections.csv",
                mime="text/csv",
            )


# ──────────────────────────────────────────────────────────────────────────
# Tab 2 — Dataset Builder
# ──────────────────────────────────────────────────────────────────────────

def _tab_build() -> None:
    st.subheader("Interactive Dataset Builder")
    st.markdown(
        "Describe what you want to extract from the 602-filing corpus. "
        "The agent will propose a schema, you refine it, then it runs extraction."
    )

    # ── Session state for the multi-step flow ─────────────────────────────
    if "ds_state" not in st.session_state:
        st.session_state.ds_state = {}
    if "ds_phase" not in st.session_state:
        st.session_state.ds_phase = "query"  # query → schema → extract → critique → done

    phase = st.session_state.ds_phase
    ds = st.session_state.ds_state

    # ── Phase: Query ──────────────────────────────────────────────────────
    if phase == "query":
        query = st.text_area(
            "What do you want to find?",
            placeholder=(
                "Examples:\n"
                "  - Find all mentions of steel tariff cost impacts and any dollar amounts\n"
                "  - Identify companies that discuss supply chain diversification in response to tariffs\n"
                "  - Which filings mention 'Liberation Day' or April 2025 tariff announcements?\n"
                "  - Extract mitigation strategies by sector"
            ),
            height=120,
        )
        if st.button("Design schema →", type="primary", disabled=not query.strip()):
            with st.spinner("Designing schema…"):
                from tariff_agent.dataset_graph.schema_node import schema_node
                state: dict = {
                    "user_query": query,
                    "schema_iteration": 0,
                    "schema_approved": False,
                }
                state = schema_node(state)
            if state.get("error"):
                st.error(state["error"])
            else:
                st.session_state.ds_state = state
                st.session_state.ds_phase = "schema"
                st.rerun()

    # ── Phase: Schema review ──────────────────────────────────────────────
    elif phase == "schema":
        cols = ds.get("proposed_columns", [])
        st.markdown(f"**Dataset:** `{ds.get('dataset_name', '')}` — {ds.get('dataset_description', '')}")
        st.markdown(f"*Schema iteration {ds.get('schema_iteration', 1)}*")

        # Editable schema table
        schema_df = pd.DataFrame([
            {
                "name": c.get("name", ""),
                "type": c.get("type", "string|null"),
                "description": c.get("description", ""),
                "extraction_instruction": c.get("extraction_instruction", ""),
                "default": json.dumps(c.get("default")),
            }
            for c in cols
        ])
        edited_df = st.data_editor(
            schema_df,
            use_container_width=True,
            num_rows="dynamic",
            key="schema_editor",
        )

        feedback = st.text_input("Feedback for refinement (leave blank to approve as-is):",
                                 placeholder="e.g. 'add a column for specific dollar amounts', 'remove mitigation column'")

        b1, b2, b3 = st.columns(3)
        approve = b1.button("✓ Approve & Extract", type="primary")
        refine = b2.button("↩ Refine schema")
        restart = b3.button("⟳ Start over")

        if restart:
            st.session_state.ds_phase = "query"
            st.session_state.ds_state = {}
            st.rerun()

        if refine:
            if not feedback.strip():
                st.warning("Enter feedback before refining.")
            else:
                with st.spinner("Refining schema…"):
                    from tariff_agent.dataset_graph.schema_node import schema_node
                    state = {**ds, "schema_feedback": feedback, "schema_approved": False}
                    state = schema_node(state)
                if state.get("error"):
                    st.error(state["error"])
                else:
                    st.session_state.ds_state = state
                    st.rerun()

        if approve:
            # Apply any edits the user made in the data editor
            updated_cols = []
            for _, row in edited_df.iterrows():
                try:
                    default = json.loads(str(row.get("default", "null")))
                except Exception:
                    default = None
                updated_cols.append({
                    "name": str(row["name"]),
                    "type": str(row["type"]),
                    "description": str(row["description"]),
                    "extraction_instruction": str(row["extraction_instruction"]),
                    "default": default,
                })
            state = {**ds, "proposed_columns": updated_cols, "schema_approved": True}
            st.session_state.ds_state = state
            st.session_state.ds_phase = "extract"
            st.rerun()

    # ── Phase: Extraction ─────────────────────────────────────────────────
    elif phase == "extract":
        st.info("Running extraction on all 602 filings. This takes several minutes.")
        progress = st.progress(0, text="Initialising…")

        with st.spinner("Extracting…"):
            from tariff_agent.dataset_graph.extraction_node import extraction_node

            start = time.time()
            state = extraction_node(ds)
            elapsed = time.time() - start

        progress.progress(100, text="Done")

        if state.get("error"):
            st.error(state["error"])
        else:
            rows = state.get("rows", [])
            st.success(f"Extracted {len(rows)} rows in {elapsed:.0f}s")
            st.session_state.ds_state = state
            st.session_state.ds_phase = "critique"
            st.rerun()

    # ── Phase: Critique ───────────────────────────────────────────────────
    elif phase == "critique":
        rows = ds.get("rows", [])
        cols = ds.get("proposed_columns", [])

        # Fill-rate
        st.markdown("#### Fill-rate by column")
        fill_data = []
        for col in cols:
            n = sum(1 for r in rows if r.get(col["name"]) not in (None, "", False, 0))
            fill_data.append({"column": col["name"], "filled": n, "pct": round(100 * n / max(len(rows), 1), 1)})
        st.dataframe(pd.DataFrame(fill_data), use_container_width=True)

        # Proof of absence
        no_ev = sum(1 for r in rows if r.get("_pass1_positive", 0) in (0, "0"))
        st.metric("Filings with no tariff evidence (negative / proof of absence)", no_ev,
                  help="For these filings, the pipeline searched every chunk and found nothing.")

        # LLM critique
        if st.button("Run LLM quality critique"):
            with st.spinner("Critiquing…"):
                from tariff_agent.dataset_graph.critique_node import critique_node
                state = critique_node(ds)
            st.session_state.ds_state = state
            st.markdown("**LLM critique:**")
            st.write(state.get("critique_text", ""))
            if state.get("critique_suggestions"):
                st.markdown("**Suggestions:**")
                for s in state["critique_suggestions"]:
                    st.markdown(f"- {s}")

        # Sample preview
        st.markdown("#### Sample rows (10)")
        preview_cols = [c["name"] for c in cols] + ["ticker", "filing_date"]
        preview_df = pd.DataFrame(rows)[
            [c for c in preview_cols if c in pd.DataFrame(rows).columns]
        ].head(10)
        st.dataframe(preview_df, use_container_width=True)

        b1, b2, b3 = st.columns(3)
        export_btn = b1.button("💾 Export CSV", type="primary")
        revise_btn = b2.button("↩ Revise schema")
        restart_btn = b3.button("⟳ Start over")

        if restart_btn:
            st.session_state.ds_phase = "query"
            st.session_state.ds_state = {}
            st.rerun()

        if revise_btn:
            st.session_state.ds_phase = "schema"
            st.session_state.ds_state = {**ds, "schema_approved": False}
            st.rerun()

        if export_btn:
            from tariff_agent.dataset_graph.graph import export_node
            state = export_node({**ds, "export_approved": True})
            if state.get("error"):
                st.error(state["error"])
            else:
                path = state["dataset_path"]
                df = pd.read_csv(path)
                st.success(f"Saved to `{path}`")
                st.download_button(
                    "⬇ Download CSV",
                    data=df.to_csv(index=False).encode(),
                    file_name=Path(path).name,
                    mime="text/csv",
                )
                st.session_state.ds_phase = "done"
                st.session_state.ds_state = state
                st.rerun()

    # ── Phase: Done ───────────────────────────────────────────────────────
    elif phase == "done":
        path = ds.get("dataset_path", "")
        st.success(f"Dataset exported: `{path}`")

        if path and Path(path).exists():
            df = pd.read_csv(path)
            st.markdown(f"**{len(df)} rows × {len(df.columns)} columns**")
            st.dataframe(df, use_container_width=True)
            st.download_button(
                "⬇ Download CSV",
                data=df.to_csv(index=False).encode(),
                file_name=Path(path).name,
                mime="text/csv",
            )

        if st.button("Build another dataset"):
            st.session_state.ds_phase = "query"
            st.session_state.ds_state = {}
            st.rerun()


# ──────────────────────────────────────────────────────────────────────────
# Tab 3 — Corpus overview (pre-built datasets)
# ──────────────────────────────────────────────────────────────────────────

def _tab_datasets() -> None:
    st.subheader("Saved Datasets")
    datasets_dir = cfg.resolve(getattr(cfg, "datasets_dir", "output/datasets"))
    if not datasets_dir.exists():
        st.info("No datasets generated yet. Use the Build tab.")
        return

    csvs = sorted(datasets_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not csvs:
        st.info("No datasets found in output/datasets/")
        return

    selected = st.selectbox("Select dataset", [p.name for p in csvs])
    path = datasets_dir / selected

    df = _load_dataset(str(path))
    if df.empty:
        st.warning("Empty or unreadable dataset.")
        return

    # Stats
    c1, c2, c3 = st.columns(3)
    c1.metric("Rows", len(df))
    c2.metric("Columns", len(df.columns))
    no_ev = 0
    if "_pass1_positive" in df.columns:
        no_ev = int((df["_pass1_positive"].fillna(0).astype(str) == "0").sum())
    c3.metric("Negative filings", no_ev)

    # Full grid
    st.dataframe(df, use_container_width=True, height=500)

    st.download_button(
        "⬇ Download",
        data=df.to_csv(index=False).encode(),
        file_name=selected,
        mime="text/csv",
    )


# ──────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────

def main() -> None:
    st.title("Tariff Pipeline — Human Review & Dataset Builder")

    tab_review, tab_build, tab_datasets = st.tabs([
        "📋 Review filings",
        "🔬 Build dataset",
        "📁 Saved datasets",
    ])

    with tab_review:
        _tab_review()
    with tab_build:
        _tab_build()
    with tab_datasets:
        _tab_datasets()


if __name__ == "__main__":
    main()

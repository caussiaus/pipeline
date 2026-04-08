"""Build Dataset — n8n-inspired layout.

┌─ LEFT PANEL (field list) ──┬─ CENTER (frozen-header table + chat log) ──┬─ RIGHT (field metadata / chat) ─┐
│  schema fields             │  extraction results table                  │  field detail + chat with agent  │
│  click to inspect          │  frozen column headers                     │  context, evidence, refinement   │
└────────────────────────────┴────────────────────────────────────────────┴──────────────────────────────────┘
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tariff_agent.corpus.config import CorpusConfig


def _cfg() -> CorpusConfig | None:
    return st.session_state.get("corpus_cfg")


def _corpus_overlay() -> dict:
    cfg = _cfg()
    if not cfg:
        return {}
    root = Path(__file__).resolve().parents[1]
    return {
        "corpus_index_csv":          str(cfg.resolve(cfg.index_csv, root)),
        "corpus_chunks_parquet":     str(cfg.resolve(cfg.chunks_parquet, root)),
        "corpus_chunks_llm_parquet": str(cfg.resolve(cfg.chunks_llm_parquet, root)),
        "datasets_export_dir":       str(cfg.resolve(cfg.datasets_dir, root)),
    }


def _with_corpus(state: dict) -> dict:
    return {**_corpus_overlay(), **state}


# ── Helpers ────────────────────────────────────────────────────────────────

def _phase_badge(label: str, active: bool = False, done: bool = False) -> str:
    cls = "phase-pill" + (" active" if active else " done" if done else "")
    return f"<span class='{cls}'>{label}</span>"


def _field_type_badge(t: str) -> str:
    color = {"boolean": "#7A9E7E", "integer": "#7A7A9E", "number": "#7A7A9E"}.get(
        t.split("|")[0], "#9E7A7A"
    )
    return (
        f"<span style='display:inline-block;font-size:0.68rem;padding:1px 6px;"
        f"border-radius:99px;background:{color}22;color:{color};border:1px solid {color}44'>"
        f"{t}</span>"
    )


def _schema_table(columns: list[dict]) -> list[dict]:
    """Editable compact schema editor — returns updated columns."""
    df = pd.DataFrame([{
        "name":         c.get("name", ""),
        "type":         c.get("type", "string|null"),
        "description":  c.get("description", ""),
        "instruction":  c.get("extraction_instruction", ""),
        "mode":         c.get("mode", "direct"),
    } for c in columns])

    edited = st.data_editor(
        df,
        use_container_width=True,
        num_rows="dynamic",
        height=min(38 + 36 * max(len(columns), 2), 340),
        column_config={
            "name":        st.column_config.TextColumn("Field", width="small"),
            "type":        st.column_config.SelectboxColumn("Type", options=[
                "boolean", "string", "string|null", "integer", "integer|null", "number|null",
            ], width="small"),
            "description": st.column_config.TextColumn("Description"),
            "instruction": st.column_config.TextColumn("Extraction instruction"),
            "mode":        st.column_config.SelectboxColumn("Mode", options=["direct", "evidence"], width="small"),
        },
        key="schema_data_editor",
        hide_index=True,
    )

    out = []
    for _, row in edited.iterrows():
        orig = next((c for c in columns if c.get("name") == str(row["name"])), {})
        out.append({
            "name": str(row["name"]),
            "type": str(row["type"]),
            "description": str(row["description"]),
            "extraction_instruction": str(row["instruction"]),
            "mode": str(row.get("mode", "direct")),
            "default": orig.get("default"),
        })
    return out


def _results_table_html(rows: list[dict], columns: list[dict], identity_fields: list[str]) -> str:
    """Render a frozen-header scrollable HTML table styled to the design system."""
    col_names = [c["name"] for c in columns]
    id_cols = [f for f in identity_fields if any(f in r for r in rows)]
    all_cols = id_cols + col_names

    header_cells = "".join(
        f"<th onclick=\"window._fieldClick('{c}')\" title='Click to inspect field'>{c}</th>"
        for c in all_cols
    )

    def _cell(v):
        if v is None or v == "" or str(v) in ("None", "nan"):
            return "<td class='null'>—</td>"
        sv = str(v)
        if sv.lower() == "true":
            return "<td class='bool-t'>✓</td>"
        if sv.lower() == "false":
            return "<td class='bool-f'>✗</td>"
        return f"<td title='{sv[:200]}'>{sv[:80]}</td>"

    body_rows = "".join(
        f"<tr>{''.join(_cell(r.get(c)) for c in all_cols)}</tr>"
        for r in rows[:500]
    )

    return f"""
<div class='frozen-table-wrap'>
<table class='frozen-table'>
  <thead><tr>{header_cells}</tr></thead>
  <tbody>{body_rows}</tbody>
</table>
</div>
<style>
.frozen-table-wrap {{
  overflow: auto;
  max-height: 420px;
  border: 1px solid var(--cream-dark);
  border-radius: 4px;
  background: #FFFCF6;
}}
.frozen-table {{
  border-collapse: collapse;
  width: max-content;
  min-width: 100%;
  font-size: 0.77rem;
  font-family: var(--font);
}}
.frozen-table thead {{
  position: sticky;
  top: 0;
  z-index: 10;
  background: var(--cream-mid);
}}
.frozen-table th {{
  padding: 7px 12px;
  text-align: left;
  font-weight: 600;
  font-size: 0.74rem;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--brown-mid);
  border-bottom: 1px solid var(--cream-dark);
  cursor: pointer;
  white-space: nowrap;
  transition: background 0.1s;
}}
.frozen-table th:hover {{ background: var(--cream-dark); }}
.frozen-table td {{
  padding: 5px 12px;
  border-bottom: 1px solid #EDE7D9;
  color: var(--brown);
  white-space: nowrap;
  max-width: 260px;
  overflow: hidden;
  text-overflow: ellipsis;
}}
.frozen-table tr:hover td {{ background: #FAF6EE; }}
.frozen-table .null {{ color: #C8BBA8; }}
.frozen-table .bool-t {{ color: #5A7A4A; font-weight: 600; }}
.frozen-table .bool-f {{ color: #9E5A5A; font-weight: 600; }}
</style>
"""


def _fill_stats(rows: list[dict], columns: list[dict]) -> pd.DataFrame:
    out = []
    n = max(len(rows), 1)
    for col in columns:
        nm = col["name"]
        filled = sum(1 for r in rows if r.get(nm) not in (None, "", False, 0))
        evidenced = sum(1 for r in rows if r.get(f"{nm}_evidence_quote"))
        out.append({
            "field":     nm,
            "type":      col.get("type", ""),
            "filled":    filled,
            "fill %":    round(100 * filled / n, 1),
            "evidenced": evidenced,
        })
    return pd.DataFrame(out)


# ── Main page ───────────────────────────────────────────────────────────────
def page() -> None:
    cfg = _cfg()
    if cfg is None:
        st.info("Configure a corpus first on the **Setup & Ingest** page.")
        return

    # ── Session init ───────────────────────────────────────────────────────
    if "ds_phase" not in st.session_state:
        st.session_state.ds_phase = "query"
    if "ds" not in st.session_state:
        st.session_state.ds = {}
    if "run_id" not in st.session_state:
        from tariff_agent.dataset_graph.feedback_store import new_run_id
        st.session_state.run_id = new_run_id()
    if "active_field" not in st.session_state:
        st.session_state.active_field = None
    if "field_chat" not in st.session_state:
        st.session_state.field_chat = []

    phase = st.session_state.ds_phase
    ds = st.session_state.ds

    # ── Page header ────────────────────────────────────────────────────────
    st.markdown(
        f"<h1 style='margin-bottom:4px'>Build Dataset"
        f"<span style='font-size:0.85rem;font-weight:400;color:var(--text-muted);margin-left:10px'>"
        f"{cfg.name[:40]}</span></h1>",
        unsafe_allow_html=True,
    )
    phases = ["query", "schema", "sample_extract", "sample_review", "full_extract", "critique", "done"]
    badges = " ".join(
        _phase_badge(p, active=(p == phase), done=(phases.index(p) < phases.index(phase)))
        for p in phases
    )
    st.markdown(badges, unsafe_allow_html=True)
    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

    # ── Three-column shell ─────────────────────────────────────────────────
    left, center, right = st.columns([0.85, 2.2, 1.2], gap="medium")

    cols: list[dict] = ds.get("proposed_columns", [])
    rows: list[dict] = ds.get("rows", [])

    # ────────────────────────────── LEFT ────────────────────────────────────
    with left:
        st.markdown(
            "<div style='font-size:0.78rem;font-weight:600;letter-spacing:0.08em;"
            "text-transform:uppercase;color:var(--text-muted);margin-bottom:8px'>Fields</div>",
            unsafe_allow_html=True,
        )
        if not cols:
            st.markdown(
                "<div style='font-size:0.8rem;color:var(--text-muted);padding:10px 0'>"
                "No schema yet. Enter a query →</div>",
                unsafe_allow_html=True,
            )
        else:
            for col in cols:
                nm = col["name"]
                active = st.session_state.active_field == nm
                fill_n = sum(1 for r in rows if r.get(nm) not in (None, "", False, 0))
                fill_pct = round(100 * fill_n / max(len(rows), 1)) if rows else 0
                badge_html = _field_type_badge(col.get("type", "string"))
                card_cls = "field-card active" if active else "field-card"
                clicked = st.button(
                    f"{nm}",
                    key=f"field_btn_{nm}",
                    help=col.get("description", ""),
                    use_container_width=True,
                )
                if clicked:
                    st.session_state.active_field = nm
                    st.session_state.field_chat = []
                    st.rerun()

        if cols and phase not in ("query",):
            st.markdown("---")
            if st.button("+ Add field", key="add_field_btn", use_container_width=True):
                cols.append({"name": "new_field", "type": "string|null", "description": "", "extraction_instruction": "", "mode": "direct"})
                ds["proposed_columns"] = cols
                st.session_state.ds = ds
                st.rerun()

        st.markdown("---")
        st.markdown(
            f"<div style='font-size:0.74rem;color:var(--text-muted)'>Run: "
            f"<code style='font-size:0.72rem'>{st.session_state.run_id}</code></div>",
            unsafe_allow_html=True,
        )
        if st.button("⟳ Reset", key="reset_btn", use_container_width=True):
            st.session_state.ds_phase = "query"
            st.session_state.ds = {}
            st.session_state.active_field = None
            st.session_state.field_chat = []
            st.rerun()

    # ─────────────────────────── CENTER ─────────────────────────────────────
    with center:

        # ── QUERY phase ──────────────────────────────────────────────────────
        if phase == "query":
            st.markdown("### What do you want to extract?")
            st.caption(f"Corpus: {cfg.topic[:100]}")
            query = st.text_area(
                "",
                placeholder=(
                    "Describe the dataset you want to build from these documents.\n\n"
                    "Examples:\n"
                    "  • Extract Scope 1, 2, 3 GHG emissions with targets and baseline year\n"
                    "  • Find all mentions of steel tariff cost impacts with dollar amounts\n"
                    "  • Identify board gender diversity ratios and ESG committee existence"
                ),
                height=150,
                label_visibility="collapsed",
                key="query_input",
            )
            sample_tickers_raw = st.text_input(
                "Sample companies (for schema design)",
                value=", ".join(ds.get("sample_tickers", [])) or "",
                help="Comma-separated tickers or issuer names. Leave blank for auto-selection.",
                key="sample_tickers_input",
            )
            sample_tickers = [t.strip() for t in sample_tickers_raw.split(",") if t.strip()]
            extraction_mode = st.radio(
                "Extraction mode",
                ["direct", "evidence"],
                horizontal=True,
                help="direct = fast extraction  |  evidence = collect quotes first then decide",
                key="extraction_mode_radio",
            )

            if st.button("Design schema →", type="primary", disabled=not query.strip(), key="design_btn"):
                with st.spinner("Designing schema…"):
                    from tariff_agent.dataset_graph.schema_node import schema_node
                    state = _with_corpus({
                        "user_query": query,
                        "schema_iteration": 0,
                        "schema_approved": False,
                        "sample_tickers": sample_tickers,
                        "use_sample": True,
                        "extraction_mode": extraction_mode,
                    })
                    state = schema_node(state)
                if state.get("error"):
                    st.error(state["error"])
                else:
                    st.session_state.ds = state
                    st.session_state.ds_phase = "schema"
                    st.rerun()

        # ── SCHEMA phase ──────────────────────────────────────────────────────
        elif phase == "schema":
            st.markdown(
                f"### Schema — `{ds.get('dataset_name', 'untitled')}`"
                f"  <span style='font-size:0.8rem;color:var(--text-muted)'>iteration {ds.get('schema_iteration', 1)}</span>",
                unsafe_allow_html=True,
            )
            st.caption(ds.get("dataset_description", ""))
            updated_cols = _schema_table(cols)

            feedback = st.text_input(
                "Refine schema (leave blank to proceed)",
                placeholder="e.g. add a dollar-amount column, remove the mitigation field, split X into two columns",
                key="schema_feedback_input",
            )
            b1, b2 = st.columns(2)
            run_sample = b1.button("▶ Run on sample", type="primary", key="run_sample_btn")
            refine_btn = b2.button("↩ Refine with LLM", key="refine_btn", disabled=not feedback.strip())

            if run_sample:
                from tariff_agent.dataset_graph.feedback_store import log_schema_iteration
                log_schema_iteration(st.session_state.run_id,
                    iteration=ds.get("schema_iteration", 1),
                    dataset_name=ds.get("dataset_name", ""),
                    user_query=ds.get("user_query", ""),
                    proposed_columns=updated_cols,
                    user_feedback="", approved=True)
                sample_tickers = [t.strip() for t in (st.session_state.get("sample_tickers_input") or "").split(",") if t.strip()]
                st.session_state.ds = _with_corpus({**ds, "proposed_columns": updated_cols,
                    "schema_approved": True, "use_sample": True,
                    "sample_tickers": sample_tickers or ds.get("sample_tickers", []),
                    "extraction_mode": ds.get("extraction_mode", "direct")})
                st.session_state.ds_phase = "sample_extract"
                st.rerun()

            if refine_btn:
                from tariff_agent.dataset_graph.feedback_store import log_schema_iteration
                from tariff_agent.dataset_graph.schema_node import schema_node
                log_schema_iteration(st.session_state.run_id,
                    iteration=ds.get("schema_iteration", 1),
                    dataset_name=ds.get("dataset_name", ""),
                    user_query=ds.get("user_query", ""),
                    proposed_columns=updated_cols,
                    user_feedback=feedback, approved=False)
                with st.spinner("Refining…"):
                    state = _with_corpus({**ds, "proposed_columns": updated_cols,
                                         "schema_feedback": feedback, "schema_approved": False})
                    state = schema_node(state)
                st.session_state.ds = state
                st.rerun()

        # ── SAMPLE EXTRACT ────────────────────────────────────────────────────
        elif phase == "sample_extract":
            sample_tickers = ds.get("sample_tickers", [])
            st.info(f"Extracting sample: `{', '.join(sample_tickers) or 'auto-selected'}`")
            with st.spinner("Running extraction on sample…"):
                from tariff_agent.dataset_graph.extraction_node import extraction_node
                t0 = time.time()
                state = extraction_node(_with_corpus(ds))
                elapsed = time.time() - t0
            if state.get("error"):
                st.error(state["error"])
                st.session_state.ds_phase = "schema"
            else:
                st.success(f"{len(state.get('rows', []))} rows in {elapsed:.1f}s")
                st.session_state.ds = state
                st.session_state.ds_phase = "sample_review"
                st.rerun()

        # ── SAMPLE REVIEW ─────────────────────────────────────────────────────
        elif phase == "sample_review":
            st.markdown(f"### Sample results — {len(rows)} rows")
            fill_df = _fill_stats(rows, cols)
            st.dataframe(fill_df, use_container_width=True, height=180, hide_index=True)

            if rows:
                st.markdown(
                    _results_table_html(rows, cols, list(cfg.identity_fields)),
                    unsafe_allow_html=True,
                )

            st.markdown("---")
            b1, b2, b3 = st.columns(3)
            approve = b1.button("✓ Approve — run full corpus", type="primary", key="approve_sample_btn")
            revise  = b2.button("↩ Revise schema", key="revise_sample_btn")
            restart = b3.button("⟳ Start over", key="restart_sample_btn")
            if restart:
                st.session_state.ds_phase = "query"; st.session_state.ds = {}; st.rerun()
            if revise:
                st.session_state.ds_phase = "schema"; st.session_state.ds = {**ds, "schema_approved": False}; st.rerun()
            if approve:
                st.session_state.ds = _with_corpus({**ds, "use_sample": False})
                st.session_state.ds_phase = "full_extract"; st.rerun()

        # ── FULL EXTRACT ──────────────────────────────────────────────────────
        elif phase == "full_extract":
            st.subheader("Full corpus extraction")
            st.info(f"Running extraction on all {len(cols)} fields × all documents.")
            prog = st.progress(0, "Starting…")
            with st.spinner("Extracting…"):
                from tariff_agent.dataset_graph.extraction_node import extraction_node
                t0 = time.time()
                state = extraction_node(_with_corpus(ds))
                elapsed = time.time() - t0
            prog.progress(100)
            if state.get("error"):
                st.error(state["error"])
            else:
                st.success(f"{len(state.get('rows', []))} rows extracted in {elapsed:.0f}s")
                st.session_state.ds = state
                st.session_state.ds_phase = "critique"; st.rerun()

        # ── CRITIQUE + EXPORT ─────────────────────────────────────────────────
        elif phase == "critique":
            st.markdown(f"### Full results — {len(rows)} rows")
            c1, c2, c3 = st.columns(3)
            no_ev = sum(1 for r in rows if str(r.get("_pass1_positive", 0)) == "0")
            c1.metric("Total rows", len(rows))
            c2.metric("With evidence", len(rows) - no_ev)
            c3.metric("Negative (absence)", no_ev)

            st.dataframe(_fill_stats(rows, cols), use_container_width=True, height=200, hide_index=True)

            if rows:
                st.markdown(
                    _results_table_html(rows, cols, list(cfg.identity_fields)),
                    unsafe_allow_html=True,
                )

            if st.button("Run LLM critique", key="critique_btn"):
                with st.spinner("Critiquing…"):
                    from tariff_agent.dataset_graph.critique_node import critique_node
                    state = critique_node(ds)
                st.session_state.ds = state
                if state.get("critique_text"):
                    st.markdown("**Critique:**")
                    st.write(state["critique_text"])

            st.markdown("---")
            e1, e2, e3 = st.columns(3)
            export  = e1.button("💾 Export CSV", type="primary", key="export_btn")
            revise2 = e2.button("↩ Revise schema", key="revise2_btn")
            restart2= e3.button("⟳ Start over", key="restart2_btn")
            if restart2:
                st.session_state.ds_phase = "query"; st.session_state.ds = {}; st.rerun()
            if revise2:
                st.session_state.ds_phase = "schema"; st.session_state.ds = {**ds, "schema_approved": False}; st.rerun()
            if export:
                from tariff_agent.dataset_graph.graph import export_node
                state = export_node(_with_corpus({**ds, "export_approved": True}))
                if state.get("error"):
                    st.error(state["error"])
                else:
                    path = state["dataset_path"]
                    df_exp = pd.read_csv(path)
                    st.success(f"Saved: `{path}`")
                    st.download_button("⬇ Download CSV", data=df_exp.to_csv(index=False).encode(),
                                       file_name=Path(path).name, mime="text/csv")
                    st.session_state.ds = state; st.session_state.ds_phase = "done"; st.rerun()

        # ── DONE ──────────────────────────────────────────────────────────────
        elif phase == "done":
            path = ds.get("dataset_path", "")
            st.success(f"Dataset exported: `{path}`")
            if path and Path(path).exists():
                df_done = pd.read_csv(path)
                st.markdown(
                    _results_table_html(df_done.to_dict("records"), cols, list(cfg.identity_fields)),
                    unsafe_allow_html=True,
                )
                st.download_button("⬇ Download", data=df_done.to_csv(index=False).encode(),
                                   file_name=Path(path).name, mime="text/csv")
            if st.button("Build another dataset", key="restart_done_btn"):
                st.session_state.ds_phase = "query"; st.session_state.ds = {}; st.rerun()

    # ──────────────────────────── RIGHT ─────────────────────────────────────
    with right:
        active_field = st.session_state.get("active_field")
        field_meta = next((c for c in cols if c.get("name") == active_field), None) if active_field else None

        if field_meta:
            # Field metadata card
            st.markdown(
                f"<div style='font-size:0.8rem;font-weight:600;color:var(--brown);margin-bottom:4px'>"
                f"{field_meta['name']}</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                _field_type_badge(field_meta.get("type", "string")),
                unsafe_allow_html=True,
            )
            st.markdown(
                f"<div style='font-size:0.8rem;color:var(--text-muted);margin:6px 0'>"
                f"{field_meta.get('description', '—')}</div>",
                unsafe_allow_html=True,
            )
            st.caption(f"Instruction: {field_meta.get('extraction_instruction', '—')[:140]}")
            st.caption(f"Mode: **{field_meta.get('mode', 'direct')}**")

            # Evidence sample
            ev_rows = [r for r in rows if r.get(f"{active_field}_evidence_quote")]
            if ev_rows:
                st.markdown("---")
                st.markdown(f"<div style='font-size:0.74rem;font-weight:600;text-transform:uppercase;letter-spacing:0.07em;color:var(--text-muted);margin-bottom:6px'>Evidence samples</div>", unsafe_allow_html=True)
                for r in ev_rows[:4]:
                    ev_q = str(r.get(f"{active_field}_evidence_quote", ""))[:240]
                    ev_p = r.get(f"{active_field}_evidence_pages", "?")
                    ticker = r.get("ticker") or r.get("issuer_name") or r.get("filing_id", "")
                    val = r.get(active_field)
                    st.markdown(
                        f"<div style='background:#FFFCF6;border:1px solid var(--cream-dark);border-radius:4px;"
                        f"padding:8px 10px;margin-bottom:6px;font-size:0.78rem'>"
                        f"<span style='font-weight:600'>{ticker}</span>"
                        f"<span style='color:var(--text-muted);font-size:0.72rem'>&nbsp;pp.{ev_p}</span>"
                        f"<div style='color:var(--text-muted);margin-top:4px'>Value: <code style='font-size:0.74rem'>{val}</code></div>"
                        f"<div style='font-style:italic;color:var(--brown-mid);margin-top:3px'>\"{ev_q}\"</div>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )

            # Chat with agent about this field
            st.markdown("---")
            st.markdown(
                f"<div style='font-size:0.74rem;font-weight:600;text-transform:uppercase;letter-spacing:0.07em;color:var(--text-muted);margin-bottom:6px'>Chat — {active_field}</div>",
                unsafe_allow_html=True,
            )
            chat_history = st.session_state.field_chat
            for msg in chat_history[-10:]:
                with st.chat_message(msg["role"]):
                    st.markdown(msg["content"])

            prompt = st.chat_input(f"Ask about {active_field}…", key="field_chat_input")
            if prompt:
                chat_history.append({"role": "user", "content": prompt})
                # Field-scoped context → schema node refinement
                with st.spinner("Thinking…"):
                    try:
                        from tariff_agent.dataset_graph.schema_node import schema_node
                        ctx = ds.get("user_query", cfg.topic)
                        field_feedback = (
                            f"Regarding only the field `{active_field}` "
                            f"(type: {field_meta.get('type')}, current instruction: '{field_meta.get('extraction_instruction', '')}'): "
                            f"{prompt}"
                        )
                        state = _with_corpus({
                            **ds,
                            "schema_feedback": field_feedback,
                            "schema_approved": False,
                        })
                        state = schema_node(state)
                        # Diff: find updated field
                        updated = next(
                            (c for c in state.get("proposed_columns", []) if c.get("name") == active_field),
                            None,
                        )
                        if updated:
                            reply = (
                                f"Updated field `{active_field}`:\n"
                                f"- Type: `{updated.get('type')}`\n"
                                f"- Instruction: {updated.get('extraction_instruction', '')}\n\n"
                                f"Apply by clicking **Refine** or **Run on sample** in the center panel."
                            )
                            # Apply update locally
                            new_cols = [updated if c.get("name") == active_field else c
                                        for c in state.get("proposed_columns", cols)]
                            ds["proposed_columns"] = new_cols
                            st.session_state.ds = {**st.session_state.ds, "proposed_columns": new_cols}
                        else:
                            reply = state.get("error") or "No change proposed."
                    except Exception as e:
                        reply = f"Error: {e}"
                chat_history.append({"role": "assistant", "content": reply})
                st.session_state.field_chat = chat_history
                st.rerun()

        else:
            # No field selected
            st.markdown(
                "<div style='padding:20px 0;color:var(--text-muted);font-size:0.82rem'>"
                "Select a field from the left panel to inspect evidence and chat with the agent about that specific column."
                "</div>",
                unsafe_allow_html=True,
            )
            st.markdown("---")
            # Global chat — dataset-level
            st.markdown(
                "<div style='font-size:0.74rem;font-weight:600;text-transform:uppercase;letter-spacing:0.07em;color:var(--text-muted);margin-bottom:6px'>Dataset chat</div>",
                unsafe_allow_html=True,
            )
            if "ds_chat" not in st.session_state:
                st.session_state.ds_chat = []

            for msg in st.session_state.ds_chat[-10:]:
                with st.chat_message(msg["role"]):
                    st.markdown(msg["content"])

            global_prompt = st.chat_input("Ask about the dataset…", key="global_chat_input")
            if global_prompt and phase not in ("query",):
                st.session_state.ds_chat.append({"role": "user", "content": global_prompt})
                with st.spinner("Thinking…"):
                    try:
                        from tariff_agent.dataset_graph.schema_node import schema_node
                        state = _with_corpus({
                            **ds,
                            "schema_feedback": global_prompt,
                            "schema_approved": False,
                        })
                        state = schema_node(state)
                        new_cols = state.get("proposed_columns", cols)
                        n_changed = sum(
                            1 for a, b in zip(cols, new_cols)
                            if a.get("extraction_instruction") != b.get("extraction_instruction")
                        )
                        reply = (
                            f"Schema updated ({n_changed} field(s) changed). "
                            f"Review the Fields panel on the left and run the sample when ready."
                        )
                        st.session_state.ds = {**st.session_state.ds, "proposed_columns": new_cols}
                    except Exception as e:
                        reply = f"Error: {e}"
                st.session_state.ds_chat.append({"role": "assistant", "content": reply})
                st.rerun()

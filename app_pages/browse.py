"""Page 2 — Browse Corpus.

Browse documents with chunk-level evidence viewer.
For each document shows:
  - Identity metadata
  - Pass-2 LLM summary (if available)
  - Key quotes with span-level page citations
  - All Pass-1 positive chunks with full text + page provenance
  - Proof-of-absence stats for negative documents
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tariff_agent.corpus.config import CorpusConfig
from tariff_agent.utils.pdf_evidence import (
    bboxes_on_page,
    parse_source_bboxes_json,
    render_page_highlight_png,
)


def _get_cfg() -> CorpusConfig | None:
    return st.session_state.get("corpus_cfg")


@st.cache_data(show_spinner=False)
def _load_csv(path: str) -> pd.DataFrame:
    p = Path(path)
    return pd.read_csv(p, dtype=str) if p.exists() else pd.DataFrame()


@st.cache_data(show_spinner=False)
def _load_parquet(path: str) -> pd.DataFrame:
    p = Path(path)
    return pd.read_parquet(p) if p.exists() else pd.DataFrame()


def _parse_json_list(raw) -> list:
    if not raw or str(raw).strip() in ("", "nan", "[]"):
        return []
    try:
        v = json.loads(str(raw))
        return v if isinstance(v, list) else []
    except Exception:
        return []


def _resolve_pdf_path(local_path: str, cfg: CorpusConfig, project_root: Path) -> Path:
    raw = (local_path or "").strip().replace("\\", "/")
    p = Path(raw)
    if p.is_file():
        return p
    pdf_root = (getattr(cfg, "filings_pdf_root_env", None) or "").strip()
    base = Path(pdf_root) if pdf_root else project_root
    return (base / raw).resolve()


def page() -> None:
    cfg = _get_cfg()
    if cfg is None:
        st.info("Configure a corpus first on the **Corpus Setup** page.")
        return

    root = Path(__file__).resolve().parents[1]
    st.header(f"Browse — {cfg.name}")
    st.caption(
        "Chunk rows can store Docling **`source_bboxes_json`** for red boxes on the PDF. "
        "Re-run **chunk** after upgrading if your `chunks.parquet` predates that column."
    )

    # Load data
    docs_llm = _load_csv(str(root / cfg.docs_llm_csv) if not Path(cfg.docs_llm_csv).is_absolute() else cfg.docs_llm_csv)
    index = _load_csv(str(root / cfg.index_csv) if not Path(cfg.index_csv).is_absolute() else cfg.index_csv)
    chunks = _load_parquet(str(root / cfg.chunks_parquet) if not Path(cfg.chunks_parquet).is_absolute() else cfg.chunks_parquet)
    chunks_llm = _load_parquet(str(root / cfg.chunks_llm_parquet) if not Path(cfg.chunks_llm_parquet).is_absolute() else cfg.chunks_llm_parquet)

    if index.empty:
        st.warning("No document index found. Run the ingestion pipeline on the Corpus Setup page.")
        return

    doc_id_col = cfg.doc_id_field

    # ── Filters ───────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("### Filters")
        only_positive = st.checkbox("Tariff-positive only", value=False)
        search_company = st.text_input("Search company / filename", "")

    df = index.copy()
    if not docs_llm.empty and "has_tariff_discussion" in docs_llm.columns:
        df = df.merge(
            docs_llm[[doc_id_col, "has_tariff_discussion", "disclosure_quality",
                       "doc_summary_sentence", "key_quotes", "tariff_direction"]].rename(
                columns={doc_id_col: doc_id_col}
            ),
            on=doc_id_col, how="left",
        )
    if only_positive and "has_tariff_discussion" in df.columns:
        df = df[df["has_tariff_discussion"].astype(str).str.lower() == "true"]
    if search_company:
        mask = df.apply(lambda r: search_company.lower() in str(r.values).lower(), axis=1)
        df = df[mask]

    if df.empty:
        st.info("No documents match current filters.")
        return

    # ── Document selector ─────────────────────────────────────────────────
    id_col1 = cfg.identity_fields[0] if cfg.identity_fields else doc_id_col
    id_col2 = cfg.identity_fields[1] if len(cfg.identity_fields) > 1 else ""
    id_col3 = cfg.identity_fields[2] if len(cfg.identity_fields) > 2 else ""

    def _label(row):
        parts = [str(row.get(id_col1, ""))[:20]]
        if id_col2 and row.get(id_col2):
            parts.append(str(row.get(id_col2, ""))[:15])
        if id_col3 and row.get(id_col3):
            parts.append(str(row.get(id_col3, ""))[:12])
        return " | ".join(p for p in parts if p)

    options = [_label(row) for _, row in df.iterrows()]
    sel_i = st.selectbox("Select document", range(len(options)), format_func=lambda i: options[i])
    sel = df.iloc[sel_i]
    doc_id = str(sel.get(doc_id_col, ""))
    pdf_path = _resolve_pdf_path(str(sel.get("local_path", "")), cfg, root)

    # ── Two-column layout ─────────────────────────────────────────────────
    left, right = st.columns([1, 1])

    with left:
        st.markdown("#### Document metadata")

        # Identity fields
        for f in cfg.identity_fields:
            if sel.get(f):
                st.markdown(f"**{f}:** `{sel.get(f)}`")

        # Extra context
        for f in cfg.extra_context_fields:
            if sel.get(f):
                st.markdown(f"**{f}:** {sel.get(f)}")

        # LLM summary
        if "has_tariff_discussion" in sel:
            ht = str(sel.get("has_tariff_discussion", "")).lower() == "true"
            st.markdown(f"**Discussion detected:** {'✅' if ht else '❌'}")
        if "disclosure_quality" in sel:
            st.markdown(f"**Disclosure quality:** `{sel.get('disclosure_quality','')}`")
        if "tariff_direction" in sel:
            st.markdown(f"**Direction:** `{sel.get('tariff_direction','')}`")
        if "doc_summary_sentence" in sel:
            st.info(str(sel.get("doc_summary_sentence", "")))

        # Key quotes with span citations
        kq = _parse_json_list(sel.get("key_quotes", ""))
        if kq:
            st.markdown("**Key quotes — span citations:**")
            for i, q in enumerate(kq, 1):
                p0 = q.get("page_start", "?")
                p1 = q.get("page_end", "?")
                sec = q.get("section_path", "")
                sig = q.get("signal_type", "")
                st.markdown(
                    f"*{i}. [{sig}] pp.{p0}–{p1}*\n\n"
                    f"*`{sec}`*\n\n"
                    f"> {q.get('quote', '')[:400]}"
                )

    with right:
        st.markdown("#### Evidence chunks")

        join_col = "filing_id"
        if not chunks.empty and join_col not in chunks.columns and doc_id_col in chunks.columns:
            join_col = doc_id_col
        f_chunks = (
            chunks[chunks[join_col].astype(str) == doc_id]
            if not chunks.empty and join_col in chunks.columns
            else pd.DataFrame()
        )
        if not chunks_llm.empty and join_col not in chunks_llm.columns and doc_id_col in chunks_llm.columns:
            join_col_llm = doc_id_col
        else:
            join_col_llm = join_col
        f_llm = (
            chunks_llm[chunks_llm[join_col_llm].astype(str) == doc_id]
            if not chunks_llm.empty and join_col_llm in chunks_llm.columns
            else pd.DataFrame()
        )

        total = len(f_chunks)
        kw_hits = 0
        if not f_chunks.empty and "keyword_hit" in f_chunks.columns:
            kw_hits = int(f_chunks["keyword_hit"].astype(str).str.lower().isin(["true","1"]).sum())

        pos_ids: set[str] = set()
        if not f_llm.empty and "mentions_tariffs" in f_llm.columns:
            pos_mask = f_llm["mentions_tariffs"].astype(str).str.lower().isin(["true","1"])
            pos_ids = set(f_llm.loc[pos_mask, "chunk_id"].astype(str).tolist())

        st.caption(
            f"**{total}** chunks total | **{kw_hits}** keyword hits | **{len(pos_ids)}** Pass-1 positive"
        )

        if not pos_ids:
            st.info(
                f"**Proof of absence:** {total} chunks parsed, {kw_hits} keyword hits, "
                f"0 tariff-positive. No tariff signal found in this document."
            )
        elif not f_chunks.empty:
            pos_df = f_chunks[f_chunks["chunk_id"].astype(str).isin(pos_ids)]
            for _, cr in pos_df.iterrows():
                with st.expander(
                    f"pp.{cr.get('page_start','?')}–{cr.get('page_end','?')} "
                    f"| {str(cr.get('section_path',''))[:55]}"
                ):
                    st.text(str(cr.get("text", ""))[:2000])
                    p0 = int(cr.get("page_start") or 1)
                    p1 = int(cr.get("page_end") or p0)
                    page_choices = list(range(p0, p1 + 1)) if p1 >= p0 else [p0]
                    if len(page_choices) > 1:
                        pg_ev = st.selectbox(
                            "Evidence page",
                            page_choices,
                            key=f"evpage_{cr.get('chunk_id', '')}",
                        )
                    else:
                        pg_ev = page_choices[0]
                    boxes_raw = str(cr.get("source_bboxes_json", "") or "")
                    boxes = parse_source_bboxes_json(boxes_raw)
                    if pdf_path.is_file():
                        rects: list[tuple[float, float, float, float]] = []
                        try:
                            import fitz

                            doc = fitz.open(str(pdf_path))
                            try:
                                pi = max(0, pg_ev - 1)
                                if pi < len(doc):
                                    ph = float(doc[pi].rect.height)
                                    rects = bboxes_on_page(boxes, pg_ev, ph)
                            finally:
                                doc.close()
                            snippet = str(cr.get("text", ""))[:900]
                            png = render_page_highlight_png(
                                pdf_path,
                                pg_ev,
                                highlight_rects=rects or None,
                                text_snippet="" if rects else snippet,
                            )
                            st.image(png, caption=f"PDF evidence — page {pg_ev} (red = Docling layout boxes or text search)")
                        except ImportError:
                            st.info("Install **pymupdf** for PDF highlights: `pip install 'pymupdf>=1.24,<2'`")
                        except Exception as e:
                            st.warning(f"Could not render PDF: {e}")
                    else:
                        st.caption(f"PDF path not found: `{pdf_path}`")

    # ── Correction form ───────────────────────────────────────────────────
    st.divider()
    st.markdown("#### Annotate")

    run_id = st.session_state.get("review_run_id", "manual")

    with st.form(f"annotate_{doc_id}"):
        notes = st.text_area("Reviewer notes", placeholder="Why is this annotation being added?")
        c1, c2 = st.columns(2)
        correct_ht = c1.selectbox("has_tariff_discussion",
                                  ["(unchanged)", "True", "False"])
        correct_dq = c2.selectbox("disclosure_quality",
                                  ["(unchanged)", "BOILERPLATE", "SPECIFIC_QUALITATIVE", "SPECIFIC_QUANTITATIVE"])
        submitted = st.form_submit_button("Save annotation")

    if submitted:
        changes = {k: v for k, v in {
            "has_tariff_discussion": correct_ht,
            "disclosure_quality": correct_dq,
        }.items() if v != "(unchanged)"}

        if changes and notes:
            from tariff_agent.dataset_graph.feedback_store import log_cell_correction
            for field_name, new_val in changes.items():
                log_cell_correction(
                    run_id,
                    filing_id=doc_id,
                    ticker=str(sel.get("ticker", sel.get(cfg.identity_fields[0] if cfg.identity_fields else "doc_id", ""))),
                    field_name=field_name,
                    proposed_value=str(sel.get(field_name, "")),
                    evidence_quote=None,
                    evidence_pages=None,
                    evidence_section=None,
                    override_value=new_val,
                    override_reason=notes,
                )
            st.success(f"Annotation saved (run: {run_id})")
        else:
            st.warning("Select a change and add notes.")

"""Saved Datasets — frozen-header viewer with field metadata panel."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tariff_agent.corpus.config import CorpusConfig


def _cfg() -> CorpusConfig | None:
    return st.session_state.get("corpus_cfg")


def _field_type_badge(t: str) -> str:
    color = {"boolean": "#7A9E7E", "integer": "#7A7A9E", "number": "#7A7A9E"}.get(t.split("|")[0], "#9E7A7A")
    return (
        f"<span style='display:inline-block;font-size:0.68rem;padding:1px 6px;"
        f"border-radius:99px;background:{color}22;color:{color};border:1px solid {color}44'>{t}</span>"
    )


def _frozen_table_html(df: pd.DataFrame, highlight_col: str | None = None) -> str:
    header_cells = "".join(
        f"<th onclick=\"top._colClick && top._colClick('{c}')\" "
        f"style='{'border-bottom:2px solid var(--brown)' if c == highlight_col else ''}' "
        f"title='Click to inspect column'>{c}</th>"
        for c in df.columns
    )

    def _cell(v):
        sv = "" if v is None else str(v)
        if sv in ("", "nan", "None"):
            return "<td class='null'>—</td>"
        if sv.lower() == "true":
            return "<td class='bool-t'>✓</td>"
        if sv.lower() == "false":
            return "<td class='bool-f'>✗</td>"
        return f"<td title='{sv[:200]}'>{sv[:80]}</td>"

    body = "".join(
        f"<tr>{''.join(_cell(row[c]) for c in df.columns)}</tr>"
        for _, row in df.head(500).iterrows()
    )

    return f"""
<div class='frozen-table-wrap'>
<table class='frozen-table'>
  <thead><tr>{header_cells}</tr></thead>
  <tbody>{body}</tbody>
</table>
</div>
<style>
.frozen-table-wrap {{
  overflow:auto; max-height:480px;
  border:1px solid var(--cream-dark); border-radius:4px; background:#FFFCF6;
}}
.frozen-table {{
  border-collapse:collapse; width:max-content; min-width:100%;
  font-size:0.77rem; font-family:var(--font);
}}
.frozen-table thead {{
  position:sticky; top:0; z-index:10; background:var(--cream-mid);
}}
.frozen-table th {{
  padding:7px 12px; text-align:left; font-weight:600;
  font-size:0.73rem; text-transform:uppercase; letter-spacing:0.06em;
  color:var(--brown-mid); border-bottom:1px solid var(--cream-dark);
  cursor:pointer; white-space:nowrap;
}}
.frozen-table th:hover {{ background:var(--cream-dark); }}
.frozen-table td {{
  padding:5px 12px; border-bottom:1px solid #EDE7D9;
  color:var(--brown); white-space:nowrap; max-width:240px;
  overflow:hidden; text-overflow:ellipsis;
}}
.frozen-table tr:hover td {{ background:#FAF6EE; }}
.frozen-table .null {{ color:#C8BBA8; }}
.frozen-table .bool-t {{ color:#5A7A4A; font-weight:600; }}
.frozen-table .bool-f {{ color:#9E5A5A; font-weight:600; }}
</style>
"""


def page() -> None:
    cfg = _cfg()
    if cfg is None:
        st.info("Configure a corpus first on the **Setup & Ingest** page.")
        return

    root = Path(__file__).resolve().parents[1]
    datasets_dir = (
        Path(cfg.datasets_dir)
        if Path(cfg.datasets_dir).is_absolute()
        else root / cfg.datasets_dir
    )

    st.markdown(
        f"<h1 style='margin-bottom:4px'>Saved Datasets"
        f"<span style='font-size:0.85rem;font-weight:400;color:var(--text-muted);margin-left:10px'>{cfg.name[:40]}</span></h1>",
        unsafe_allow_html=True,
    )

    if not datasets_dir.exists() or not list(datasets_dir.glob("*.csv")):
        st.info("No datasets yet. Use **Build Dataset** to create one.")
        return

    csvs = sorted(datasets_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)

    # ── Two-column layout: left = file list, right = field panel ───────────
    list_col, detail_col = st.columns([3.2, 1.2], gap="medium")

    with list_col:
        # Dataset selector
        sel_name = st.selectbox(
            "",
            [p.name for p in csvs],
            label_visibility="collapsed",
            key="ds_file_selector",
        )
        path = datasets_dir / sel_name
        df = pd.read_csv(path, dtype=str)

        # Column buckets
        provenance_cols = [c for c in df.columns if c.startswith("_")]
        evidence_cols   = [c for c in df.columns if c.endswith(("_evidence_quote", "_evidence_pages", "_evidence_section"))]
        id_cols         = [c for c in df.columns if c in (cfg.identity_fields or [])]
        schema_cols     = [
            c for c in df.columns
            if c not in provenance_cols and c not in evidence_cols and c not in id_cols
        ]

        # Stats bar
        no_ev = 0
        if "_pass1_positive" in df.columns:
            no_ev = int((df["_pass1_positive"].fillna("0").astype(str) == "0").sum())

        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Rows", len(df))
        m2.metric("Schema fields", len(schema_cols))
        m3.metric("Evidence cols", len(evidence_cols))
        m4.metric("Negative", no_ev)
        m5.metric("KB", f"{path.stat().st_size // 1024}")

        # View toggle
        tab_schema, tab_full, tab_evidence, tab_download = st.tabs(
            ["Schema only", "Full table", "Evidence", "Download"]
        )

        active_col = st.session_state.get("active_ds_col")

        with tab_schema:
            display = df[[c for c in id_cols + schema_cols if c in df.columns]]
            st.markdown(
                _frozen_table_html(display, highlight_col=active_col),
                unsafe_allow_html=True,
            )
            # Column click listener: user types column name in a compact input
            clicked_col = st.text_input(
                "Inspect column (type name or click header above)",
                value=active_col or "",
                key="col_inspect_input",
                label_visibility="visible",
                placeholder="column name…",
            )
            if clicked_col and clicked_col != active_col:
                st.session_state.active_ds_col = clicked_col
                st.rerun()

        with tab_full:
            st.markdown(
                _frozen_table_html(df[[c for c in df.columns if c not in provenance_cols]], highlight_col=active_col),
                unsafe_allow_html=True,
            )

        with tab_evidence:
            quote_cols = [c for c in evidence_cols if c.endswith("_evidence_quote")]
            if quote_cols:
                has_ev = df[quote_cols].notna().any(axis=1) & (df[quote_cols] != "").any(axis=1)
                ev_df = df[[c for c in id_cols + quote_cols if c in df.columns]][has_ev]
                st.dataframe(ev_df, use_container_width=True, height=400, hide_index=True)
            else:
                st.info("No evidence columns found in this dataset.")

        with tab_download:
            st.download_button(
                "⬇ Full CSV (schema + evidence + provenance)",
                data=df.to_csv(index=False).encode(),
                file_name=sel_name,
                mime="text/csv",
            )
            schema_only = df[[c for c in df.columns if c not in evidence_cols and c not in provenance_cols]]
            st.download_button(
                "⬇ Schema-only CSV",
                data=schema_only.to_csv(index=False).encode(),
                file_name=f"schema_{sel_name}",
                mime="text/csv",
            )

    # ── Right: field detail panel ───────────────────────────────────────────
    with detail_col:
        active_col = st.session_state.get("active_ds_col")

        if active_col and active_col in df.columns:
            st.markdown(
                f"<div style='font-size:0.9rem;font-weight:600;color:var(--brown);margin-bottom:2px'>{active_col}</div>",
                unsafe_allow_html=True,
            )

            # Infer type from data
            non_null = df[active_col].dropna()
            non_null = non_null[non_null.astype(str) != ""]
            if non_null.empty:
                dtype_hint = "no values"
            elif non_null.astype(str).str.lower().isin(["true", "false"]).all():
                dtype_hint = "boolean"
            else:
                try:
                    non_null.astype(float)
                    dtype_hint = "numeric"
                except ValueError:
                    dtype_hint = "string"
            st.markdown(_field_type_badge(dtype_hint), unsafe_allow_html=True)

            # Fill stats
            n = len(df)
            filled = (df[active_col].notna() & (df[active_col].astype(str) != "") & (df[active_col].astype(str) != "None")).sum()
            ev_col = f"{active_col}_evidence_quote"
            n_ev = (df[ev_col].notna() & (df[ev_col].astype(str) != "")).sum() if ev_col in df.columns else 0
            st.markdown(
                f"<div style='font-size:0.78rem;color:var(--text-muted);margin:8px 0'>"
                f"Fill rate: <strong>{round(100*filled/max(n,1),1)}%</strong> ({filled}/{n})<br>"
                f"Evidence coverage: <strong>{round(100*n_ev/max(int(filled),1),1)}%</strong> ({n_ev} rows)</div>",
                unsafe_allow_html=True,
            )

            # Value distribution
            st.markdown(
                "<div style='font-size:0.72rem;font-weight:600;text-transform:uppercase;letter-spacing:0.07em;color:var(--text-muted);margin:10px 0 4px'>Distribution</div>",
                unsafe_allow_html=True,
            )
            vc = df[active_col].astype(str).replace("nan", "—").value_counts().head(8)
            for val, cnt in vc.items():
                pct = round(100 * cnt / max(n, 1))
                bar_w = max(2, pct)
                st.markdown(
                    f"<div style='display:flex;align-items:center;gap:8px;margin-bottom:3px;font-size:0.75rem'>"
                    f"<div style='flex:0 0 90px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--brown)'>{val}</div>"
                    f"<div style='flex:1;background:var(--cream-mid);border-radius:2px;height:8px'>"
                    f"<div style='width:{bar_w}%;background:var(--brown);height:8px;border-radius:2px'></div>"
                    f"</div>"
                    f"<div style='flex:0 0 28px;color:var(--text-muted);text-align:right'>{cnt}</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

            # Evidence sample
            if ev_col in df.columns:
                ev_rows = df[df[ev_col].notna() & (df[ev_col].astype(str) != "")].head(3)
                if not ev_rows.empty:
                    st.markdown(
                        "<div style='font-size:0.72rem;font-weight:600;text-transform:uppercase;letter-spacing:0.07em;color:var(--text-muted);margin:12px 0 6px'>Evidence samples</div>",
                        unsafe_allow_html=True,
                    )
                    for _, r in ev_rows.iterrows():
                        pages_col = f"{active_col}_evidence_pages"
                        pg = r.get(pages_col, "?") if pages_col in df.columns else "?"
                        ticker = next((str(r.get(f)) for f in (cfg.identity_fields or []) if r.get(f)), "")
                        quote = str(r.get(ev_col, ""))[:200]
                        val = str(r.get(active_col, ""))
                        st.markdown(
                            f"<div style='background:#FFFCF6;border:1px solid var(--cream-dark);border-radius:4px;"
                            f"padding:8px 10px;margin-bottom:6px;font-size:0.76rem'>"
                            f"<span style='font-weight:600'>{ticker}</span>"
                            f"<span style='color:var(--text-muted);margin-left:6px;font-size:0.71rem'>pp.{pg}</span>"
                            f"<div style='color:var(--brown-mid);margin-top:3px'>→ <code style='font-size:0.72rem'>{val}</code></div>"
                            f"<div style='font-style:italic;color:var(--text-muted);margin-top:3px'>\"{quote}\"</div>"
                            f"</div>",
                            unsafe_allow_html=True,
                        )

            if st.button("✕ Clear selection", key="clear_col_btn"):
                st.session_state.active_ds_col = None
                st.rerun()

        else:
            # Field list / schema overview
            st.markdown(
                "<div style='font-size:0.78rem;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;color:var(--text-muted);margin-bottom:8px'>Schema fields</div>",
                unsafe_allow_html=True,
            )
            fill_rows = []
            for sc in schema_cols:
                if sc not in df.columns:
                    continue
                filled = (df[sc].notna() & (df[sc].astype(str) != "") & (df[sc].astype(str) != "None")).sum()
                fill_rows.append({"field": sc, "fill %": round(100 * filled / max(len(df), 1), 1)})

            for fr in fill_rows:
                nm = fr["field"]
                pct = fr["fill %"]
                bar_w = max(2, int(pct))
                clicked = st.button(nm, key=f"ds_field_btn_{nm}", use_container_width=True)
                if clicked:
                    st.session_state.active_ds_col = nm
                    st.rerun()
                st.markdown(
                    f"<div style='margin:-6px 0 6px;display:flex;align-items:center;gap:6px'>"
                    f"<div style='flex:1;background:var(--cream-mid);border-radius:2px;height:4px'>"
                    f"<div style='width:{bar_w}%;background:var(--brown-light);height:4px;border-radius:2px'></div>"
                    f"</div><div style='font-size:0.7rem;color:var(--text-muted);flex:0 0 36px'>{pct}%</div></div>",
                    unsafe_allow_html=True,
                )

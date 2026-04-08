"""Workspace — IDE-style single page.

Layout per step:
  main_col (wide)  = table / schema / landing
  right_col (280px) = field inspector + agent log
  terminal row (full width, bottom) = dark terminal with history

Thread memory: schema_cols + rows persisted to thread JSON and
restored whenever the active thread changes.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from queue import Empty, Queue
from threading import Thread as PThread

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app_pages.thread_store import Thread, delete_thread, load_thread, save_thread

_CONFIG_DIR = ROOT / "output" / "corpus_configs"
_UPLOADS_DIR = ROOT / "output" / "uploads"
_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
_UPLOADS_DIR.mkdir(parents=True, exist_ok=True)


# ── Subprocess helpers ───────────────────────────────────────────────────────

def _pipeline_cmd(t: Thread, trial_n: int = 0) -> list[str]:
    yaml = _CONFIG_DIR / f"{t.corpus_id}.yaml"
    script = str(ROOT / "scripts" / "run_corpus_pipeline.py")
    cmd = [sys.executable, script]
    cmd += ["--config", str(yaml)] if yaml.exists() else ["--corpus", t.corpus_id]
    cmd += ["--stage", "all"]
    if trial_n > 0:
        cmd += ["--trial-n", str(trial_n)]
    return cmd


def _start_proc(cmd: list[str]) -> subprocess.Popen:
    return subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1, cwd=str(ROOT),
    )


def _drain(proc: subprocess.Popen, q: Queue) -> None:
    assert proc.stdout
    for line in proc.stdout:
        q.put(line.rstrip())
    proc.wait()
    q.put(None)


def _launch_ingest(t: Thread, trial_n: int = 0) -> None:
    cmd = _pipeline_cmd(t, trial_n=trial_n)
    t.add_log(f"<span class='log-step'>$ {' '.join(Path(c).name if os.sep in c else c for c in cmd)}</span>")
    proc = _start_proc(cmd)
    q: Queue = Queue()
    PThread(target=_drain, args=(proc, q), daemon=True).start()
    st.session_state["ws_proc"] = proc
    st.session_state["ws_queue"] = q
    st.session_state["ws_proc_done"] = False
    st.session_state["ws_proc_rc"] = 0


def _poll_proc(t: Thread) -> bool:
    q: Queue | None = st.session_state.get("ws_queue")
    if q is None:
        return True
    finished = False
    lines: list[str] = []
    try:
        while True:
            item = q.get_nowait()
            if item is None:
                proc = st.session_state.get("ws_proc")
                rc = proc.returncode if proc else 0
                st.session_state["ws_proc_done"] = True
                st.session_state["ws_proc_rc"] = rc
                finished = True
                break
            lines.append(item)
    except Empty:
        pass
    for ln in lines:
        lo = ln.lower()
        cls = ("log-error" if any(x in lo for x in ("error", "traceback"))
               else "log-warn" if any(x in lo for x in ("warn", "skip"))
               else "log-info" if any(x in lo for x in ("done", "ok", "complete", "✓"))
               else "log-dim")
        t.add_log(f"<span class='{cls}'>{ln}</span>")
    return finished


# ── Thread memory ────────────────────────────────────────────────────────────

def _sync_from_thread(t: Thread) -> None:
    """Restore ws_ds and annotations from persisted thread state."""
    ds = {}
    if t.schema_cols:
        ds["proposed_columns"] = list(t.schema_cols)
    if t.rows:
        ds["rows"] = list(t.rows)
    if t.chat:
        pass  # chat is read directly from t.chat
    st.session_state["ws_ds"] = ds
    st.session_state["ws_active_field"] = None
    st.session_state["ws_terminal_draft"] = ""
    st.session_state["ws_cell_ann"] = {}
    st.session_state["ws_proc_done"] = True
    st.session_state["ws_proc_rc"] = 0
    # Load cell annotations from thread if stored
    if hasattr(t, "cell_annotations"):
        st.session_state["ws_cell_ann"] = dict(t.cell_annotations or {})


def _save_schema(t: Thread, ds: dict) -> None:
    """Persist schema cols and rows back to thread JSON."""
    t.schema_cols = list(ds.get("proposed_columns", []))
    rows = ds.get("rows", [])
    if rows:
        t.rows = list(rows)
    save_thread(t)


# ── Corpus helpers ────────────────────────────────────────────────────────────

def _make_corpus_config(t: Thread):
    from tariff_agent.corpus.config import CorpusConfig
    from tariff_agent.corpus.paths import normalize_host_path
    from tariff_agent.corpus.scan_index import write_corpus_index

    norm = normalize_host_path(t.docs_dir)
    idx_path = ROOT / "data" / "metadata" / f"corpus_{t.corpus_id}_index.csv"
    n = write_corpus_index(norm, idx_path)
    rel = str(idx_path.relative_to(ROOT))

    cfg = CorpusConfig(
        name=t.corpus_name, corpus_id=t.corpus_id, topic=t.topic,
        docs_dir=str(norm), file_pattern="csv_manifest", metadata_csv=rel,
        doc_id_field="filing_id", doc_path_field="local_path",
        identity_fields=["filing_id", "ticker", "issuer_name", "filing_type", "filing_date"],
        extra_context_fields=[], output_base_dir=str(ROOT / "output"), index_csv=rel,
    )
    cfg.to_yaml(_CONFIG_DIR / f"{t.corpus_id}.yaml")
    return cfg, n


def _corpus_overlay(t: Thread) -> dict:
    from tariff_agent.corpus.config import CorpusConfig
    yaml = _CONFIG_DIR / f"{t.corpus_id}.yaml"
    if not yaml.exists():
        return {}
    cfg = CorpusConfig.from_yaml(yaml)
    return {
        "corpus_index_csv":          str(cfg.resolve(cfg.index_csv, ROOT)),
        "corpus_chunks_parquet":     str(cfg.resolve(cfg.chunks_parquet, ROOT)),
        "corpus_chunks_llm_parquet": str(cfg.resolve(cfg.chunks_llm_parquet, ROOT)),
        "datasets_export_dir":       str(cfg.resolve(cfg.datasets_dir, ROOT)),
    }


def _total_docs(t: Thread) -> int:
    try:
        from tariff_agent.corpus.config import CorpusConfig
        yaml = _CONFIG_DIR / f"{t.corpus_id}.yaml"
        if not yaml.exists():
            return 0
        cfg = CorpusConfig.from_yaml(yaml)
        idx = cfg.resolve(cfg.index_csv, ROOT)
        return len(pd.read_csv(idx)) if idx.is_file() else 0
    except Exception:
        return 0


def _parse_progress_pct(t: Thread) -> int:
    try:
        from tariff_agent.corpus.config import CorpusConfig
        yaml = _CONFIG_DIR / f"{t.corpus_id}.yaml"
        if not yaml.exists():
            return 5
        cfg = CorpusConfig.from_yaml(yaml)
        p = cfg.resolve(cfg.parse_index_csv, ROOT)
        if not p.is_file():
            return 5
        df = pd.read_csv(p)
        ok = len(df[df["parse_status"].str.startswith("OK", na=False)])
        return min(int(100 * ok / max(t.trial_n, 1)), 99)
    except Exception:
        return 5


# ── JSON schema parsing ──────────────────────────────────────────────────────

def _parse_json_schema(content: bytes | str) -> list[dict] | None:
    try:
        if isinstance(content, bytes):
            content = content.decode("utf-8-sig")
        data = json.loads(content)
        if isinstance(data, list):
            out = [x for x in data if isinstance(x, dict)]
            return out or None
        if isinstance(data, dict):
            props = data.get("properties")
            if isinstance(props, dict):
                return [{"name": k, "type": str(v.get("type", "string")),
                         "description": str(v.get("description", "") or v.get("title", ""))}
                        for k, v in props.items() if isinstance(v, dict)]
            for key in ("fields", "columns", "schema"):
                if key in data and isinstance(data[key], list):
                    out = [x for x in data[key] if isinstance(x, dict)]
                    if out:
                        return out
            skip = {"$schema", "type", "title", "description", "properties", "fields"}
            cols = [{"name": k, **v} for k, v in data.items()
                    if k not in skip and isinstance(v, dict)
                    and ("type" in v or "description" in v)]
            return cols or None
    except Exception:
        pass
    return None


# ── Log rendering ────────────────────────────────────────────────────────────

def _render_log(lines: list[str], placeholder) -> None:
    body = "\n".join(lines[-200:]) if lines else "<span class='log-dim'>waiting…</span>"
    placeholder.markdown(
        f"<div class='agent-log'>{body}</div>"
        "<script>var e=document.querySelector('.agent-log');if(e)e.scrollTop=e.scrollHeight;</script>",
        unsafe_allow_html=True,
    )


# ── Grid table ───────────────────────────────────────────────────────────────

def _grid_html(df: pd.DataFrame, cols: list, active_field: str = "",
               cell_ann: dict | None = None) -> str:
    ann = cell_ann or {}
    col_names = [c.get("name", "") for c in cols] if cols else list(df.columns)

    def _th(nm: str) -> str:
        typ = next((c.get("type", "") for c in cols if c.get("name") == nm), "") if cols else ""
        cls = "th-active" if nm == active_field else ""
        return (f"<th class='{cls}'>{nm}"
                + (f"<span style='margin-left:5px;font-size:0.68rem;color:#999;font-weight:400;font-family:var(--mono)'>{typ}</span>" if typ else "")
                + "</th>")

    def _td(v, row_idx: int, col: str) -> str:
        sv = "" if v is None else str(v)
        has_ann = bool(ann.get(f"{row_idx}:{col}"))
        ann_cls = " annotated" if has_ann else ""
        ann_tip = f" title='{ann[f'{row_idx}:{col}'][:80]}'" if has_ann else ""
        if sv in ("", "nan", "None"):
            return f"<td class='null{ann_cls}'{ann_tip}>—</td>"
        if sv.lower() == "true":
            return f"<td class='bool-t{ann_cls}'{ann_tip}>✓</td>"
        if sv.lower() == "false":
            return f"<td class='bool-f{ann_cls}'{ann_tip}>✗</td>"
        return f"<td class='{ann_cls.strip()}'{ann_tip} title='{sv[:300]}'>{sv[:100]}</td>"

    heads = "<th class='row-num'>#</th>" + "".join(_th(c) for c in col_names)
    rows_html = ""
    for i, (_, row) in enumerate(df.iterrows()):
        cells = f"<td class='row-num'>{i+1}</td>"
        cells += "".join(_td(row.get(c, ""), i, c) for c in col_names)
        rows_html += f"<tr>{cells}</tr>"

    return (
        "<div class='grid-wrap'>"
        "<table class='grid-table'>"
        f"<thead><tr>{heads}</tr></thead>"
        f"<tbody>{rows_html}</tbody>"
        "</table></div>"
    )


def _empty_header_grid(cols: list) -> str:
    """Render table with headers only (no data rows) for schema step."""
    if not cols:
        return ""
    heads = "<th class='row-num'>#</th>"
    for fc in cols:
        nm = fc.get("name", "")
        typ = fc.get("type", "")
        desc = fc.get("description", "")[:60]
        heads += (
            f"<th class='header-only' title='{desc}'>{nm}"
            f"<span style='margin-left:5px;font-size:0.68rem;color:#888;font-weight:400;font-family:var(--mono)'>{typ}</span>"
            f"</th>"
        )
    empty_row = (
        "<tr><td class='row-num' style='color:#ccc'>—</td>"
        + "".join(f"<td class='null'>extraction pending</td>" for _ in cols)
        + "</tr>"
    ) * 2
    return (
        "<div class='grid-wrap'>"
        "<table class='grid-table'>"
        f"<thead><tr>{heads}</tr></thead>"
        f"<tbody>{empty_row}</tbody>"
        "</table></div>"
    )


# ── Field strip (clickable header chips) ─────────────────────────────────────

def _render_field_strip(cols: list) -> None:
    """Render clickable header chips. Click sets active field + prefills terminal."""
    if not cols:
        return
    active = st.session_state.get("ws_active_field", "")
    # Build HTML chip strip (visual only)
    chips = "".join(
        f"<span class='field-chip {'active' if c.get('name') == active else ''}'>"
        f"{c.get('name','')}"
        f"<span class='chip-type'>{c.get('type','')}</span>"
        f"</span>"
        for c in cols
    )
    st.markdown(f"<div class='field-strip'>{chips}</div>", unsafe_allow_html=True)

    # Actual buttons in rows of 8
    chunk = 8
    for i in range(0, len(cols), chunk):
        grp = cols[i:i+chunk]
        btns = st.columns(len(grp))
        for j, fc in enumerate(grp):
            nm = fc.get("name", "")
            is_active = nm == active
            with btns[j]:
                if st.button(
                    nm, key=f"_fhdr_{nm}",
                    type="primary" if is_active else "secondary",
                    use_container_width=True,
                    help=fc.get("description", "")[:120],
                ):
                    st.session_state["ws_active_field"] = nm
                    st.session_state["ws_terminal_draft"] = f"@{nm} "
                    st.rerun()


# ── Terminal ──────────────────────────────────────────────────────────────────

def _render_terminal(t: Thread, ds: dict) -> None:
    st.markdown(
        "<div style='font-size:0.7rem;font-weight:700;letter-spacing:0.12em;"
        "text-transform:uppercase;color:#888;margin-bottom:4px;font-family:var(--mono)'>"
        "TERMINAL</div>",
        unsafe_allow_html=True,
    )

    # History
    if t.chat:
        lines = []
        for msg in t.chat[-8:]:
            role = msg.get("role", "user")
            txt = msg["content"][:140].replace("<", "&lt;").replace(">", "&gt;")
            if role == "user":
                lines.append(
                    f"<div class='t-line'><span class='t-prompt'>▶ </span>{txt}</div>"
                )
            else:
                lines.append(
                    f"<div class='t-line'><span class='t-assistant'>◀ </span>{txt}</div>"
                )
        st.markdown(
            "<div class='terminal-wrap'><div class='terminal-history'>"
            + "".join(lines)
            + "</div></div>",
            unsafe_allow_html=True,
        )

    # Input form
    draft = st.session_state.get("ws_terminal_draft", "")
    with st.form("ws_terminal", clear_on_submit=True):
        c1, c2 = st.columns([9, 1])
        with c1:
            st.markdown("<div class='terminal-input'>", unsafe_allow_html=True)
            msg = st.text_area(
                "> ",
                value=draft,
                placeholder=(
                    "Type a message — double-click any column header above to paste its name here.\n"
                    "e.g. @company_name should only include parent companies, not subsidiaries"
                ),
                height=72,
                key="ws_terminal_input",
            )
            st.markdown("</div>", unsafe_allow_html=True)
        with c2:
            st.write(" ")
            st.write(" ")
            submitted = st.form_submit_button("Send", use_container_width=True)

    if submitted and msg.strip():
        st.session_state["ws_terminal_draft"] = ""
        _handle_terminal(t, ds, msg.strip())


def _handle_terminal(t: Thread, ds: dict, msg: str) -> None:
    t.add_chat("user", msg)
    _save_schema(t, ds)
    with st.spinner("Processing…"):
        _run_schema_node(t, ds, prompt=msg)
    # If we have data, re-extract after schema update
    if t.step in ("preview", "done") and ds.get("proposed_columns"):
        t.status = "extracting"
        t.step = "extracting"
        save_thread(t)
    st.rerun()


# ── Schema node ───────────────────────────────────────────────────────────────

def _run_schema_node(t: Thread, ds: dict, prompt: str) -> None:
    try:
        from tariff_agent.dataset_graph.schema_node import schema_node
        state = {
            **_corpus_overlay(t), **ds,
            "user_query": ds.get("user_query") or prompt,
            "schema_feedback": prompt if ds.get("proposed_columns") else "",
            "schema_iteration": ds.get("schema_iteration", 0),
            "schema_approved": False,
            "use_sample": True, "sample_tickers": [], "extraction_mode": "direct",
        }
        state = schema_node(state)
        new_cols = state.get("proposed_columns", [])
        name = state.get("dataset_name", t.corpus_name)
        reply = (
            f"Schema updated — **{len(new_cols)} fields** for `{name}`:\n\n"
            + "\n".join(f"- **{c['name']}** `{c.get('type','')}` — {c.get('description','')[:80]}"
                        for c in new_cols)
        )
        st.session_state["ws_ds"] = state
        t.add_chat("assistant", reply)
        t.add_log(f"<span class='log-info'>Schema: {len(new_cols)} fields · {name}</span>")
        _save_schema(t, state)
    except Exception as e:
        import traceback
        reply = f"Schema failed: {e}"
        t.add_chat("assistant", reply)
        t.add_log(f"<span class='log-error'>{traceback.format_exc()[-400:]}</span>")
        save_thread(t)


# ── Inspector panel ───────────────────────────────────────────────────────────

def _render_inspector(t: Thread, ds: dict) -> None:
    cols = ds.get("proposed_columns", [])
    rows = t.rows or ds.get("rows", [])
    active = st.session_state.get("ws_active_field")
    cell_ann: dict = st.session_state.get("ws_cell_ann", {})

    st.markdown("<div class='inspector-label'>Field Inspector</div>", unsafe_allow_html=True)

    if cols:
        names = [c["name"] for c in cols]
        idx = names.index(active) if active in names else 0
        chosen = st.selectbox("field", names, index=idx,
                               key="_insp_sel", label_visibility="collapsed")
        if chosen != active:
            st.session_state["ws_active_field"] = chosen

        fc = next((c for c in cols if c.get("name") == chosen), {})
        st.markdown(
            f"<div class='inspector-field'>"
            f"<span class='insp-name'>{chosen}</span>"
            f"<span class='insp-type'>{fc.get('type','')}</span>"
            f"<div class='insp-desc'>{fc.get('description','—')}</div>"
            + (f"<div class='insp-instr'>{fc.get('extraction_instruction','')[:200]}</div>"
               if fc.get('extraction_instruction') else "")
            + "</div>",
            unsafe_allow_html=True,
        )

        note = t.field_notes.get(chosen, "")
        new_note = st.text_area("Notes", value=note, height=70, key=f"_note_{chosen}",
                                placeholder="Add context, edge-cases, instructions…",
                                label_visibility="visible")
        if new_note != note:
            t.field_notes[chosen] = new_note
            save_thread(t)

        # Evidence samples
        ev_col = f"{chosen}_evidence_quote"
        samples = [r for r in rows if r.get(ev_col)][:3]
        if samples:
            st.markdown("<div class='inspector-label' style='margin-top:8px'>Evidence</div>",
                        unsafe_allow_html=True)
            for r in samples:
                ident = next((str(r.get(f, "")) for f in ["ticker", "issuer_name"] if r.get(f)), "")
                val = r.get(chosen, "")
                ev = str(r[ev_col])[:240]
                st.markdown(
                    f"<div class='evidence-quote'>"
                    + (f"<strong style='font-style:normal;color:#000'>{ident}</strong> "
                       f"→ <code style='font-style:normal'>{str(val)[:60]}</code><br>" if ident else "")
                    + f'"{ev}"</div>',
                    unsafe_allow_html=True,
                )

    st.markdown("<hr>", unsafe_allow_html=True)
    st.markdown("<div class='inspector-label'>Agent Log</div>", unsafe_allow_html=True)
    _render_log(t.log, st.empty())


# ── Cell annotation panel ─────────────────────────────────────────────────────

def _render_cell_annotation(t: Thread, ds: dict) -> None:
    cols = ds.get("proposed_columns", [])
    rows = t.rows or ds.get("rows", [])
    if not rows or not cols:
        return

    cell_ann: dict = st.session_state.get("ws_cell_ann", {})

    with st.expander("🔍  Annotate a cell — click to leave feedback on extracted values", expanded=False):
        st.markdown(
            "<p style='color:#555;font-size:0.88rem;margin-bottom:8px'>"
            "Select a row and column to see the extracted value, its evidence, "
            "and leave a comment. Annotations are shown as highlighted cells in the table "
            "and applied as refinement instructions.</p>",
            unsafe_allow_html=True,
        )
        ac1, ac2 = st.columns(2)
        row_idx = ac1.number_input("Row", min_value=1, max_value=len(rows), value=1, step=1,
                                    key="_ann_row") - 1
        col_names = [c["name"] for c in cols]
        col_sel = ac2.selectbox("Column", col_names, key="_ann_col")

        row = rows[row_idx]
        val = row.get(col_sel, "")
        ev = row.get(f"{col_sel}_evidence_quote", "")
        ev_pages = row.get(f"{col_sel}_evidence_pages", "")
        ident = next((str(row.get(f, "")) for f in ["ticker", "issuer_name", "filing_id"] if row.get(f)), f"Row {row_idx+1}")

        st.markdown(
            f"<div class='cell-ann-panel'>"
            f"<div style='font-size:0.8rem;color:#666;margin-bottom:4px'>"
            f"<strong>{ident}</strong> · column <code>{col_sel}</code></div>"
            f"<div class='cell-val-display'>{str(val)[:200] if val not in (None, '', 'nan') else '— no value extracted'}</div>"
            + (f"<div class='evidence-quote' style='margin-top:4px'>"
               f"<span style='font-size:0.74rem;color:#888;font-style:normal'>Evidence"
               + (f" pp.{ev_pages}" if ev_pages else "") + ":</span><br>"
               f'"{str(ev)[:300]}"</div>' if ev else
               "<div style='font-size:0.82rem;color:#aaa;margin-top:4px'>No evidence quote available.</div>")
            + "</div>",
            unsafe_allow_html=True,
        )

        ann_key = f"{row_idx}:{col_sel}"
        existing = cell_ann.get(ann_key, "")
        new_ann = st.text_area(
            "Your feedback on this value",
            value=existing, height=72,
            key=f"_celann_{ann_key}",
            placeholder=(
                f"e.g. 'This should be {col_sel} for the parent company only' "
                "or 'Missing — check table on page 12' or 'Value is in tonnes not kg'"
            ),
            label_visibility="collapsed",
        )

        b1, b2, b3 = st.columns(3)
        if b1.button("Save note", key="_save_ann"):
            if new_ann.strip():
                cell_ann[ann_key] = new_ann.strip()
                st.session_state["ws_cell_ann"] = cell_ann
                # Persist to thread (as field_notes with special key)
                t.field_notes[f"__cell__{ann_key}"] = new_ann.strip()
                save_thread(t)
                st.success("Saved.")
        if b2.button("Push to terminal", key="_push_ann"):
            context = (
                f"@{col_sel} row {row_idx+1} ({ident}): "
                f"extracted `{str(val)[:60]}` — {new_ann or existing}"
            )
            st.session_state["ws_terminal_draft"] = context
            st.rerun()
        if ann and b3.button("Apply all feedback →", key="_apply_all_ann", type="primary"):
            blocks = [f"Row {k.split(':')[0]}, col {k.split(':')[1]}: {v}"
                      for k, v in cell_ann.items()]
            combined = (
                "Please update extraction instructions to address these reviewer notes:\n\n"
                + "\n".join(blocks)
            )
            _handle_terminal(t, ds, combined)


# ═══════════════════════════════════════════════════════════════════════════
# RENDER — main entry point
# ═══════════════════════════════════════════════════════════════════════════

def render(active_thread_id: str | None = None) -> None:
    # Init session defaults
    for k, v in {"ws_proc_done": True, "ws_proc_rc": 0, "ws_ds": {},
                 "ws_active_field": None, "ws_cell_ann": {},
                 "ws_terminal_draft": ""}.items():
        st.session_state.setdefault(k, v)

    if active_thread_id is None:
        _render_landing()
        return

    t = load_thread(active_thread_id)
    if t is None:
        _render_landing()
        return

    # Thread memory: restore state when switching threads
    loaded_id = st.session_state.get("_ws_loaded_id")
    if loaded_id != active_thread_id:
        _sync_from_thread(t)
        st.session_state["_ws_loaded_id"] = active_thread_id

    # Poll subprocess
    if not st.session_state.get("ws_proc_done", True):
        finished = _poll_proc(t)
        if finished:
            rc = st.session_state.get("ws_proc_rc", 0)
            if rc == 0:
                if t.status == "ingesting":
                    t.status = "schema"
                    t.step = "schema"
                elif t.status == "full_ingesting":
                    t.status = "full_extracting"
                    t.step = "full_extracting"
            else:
                t.status = "failed"
                t.step = "failed"
                t.error_msg = f"Process exited with code {rc}."
            save_thread(t)

    dispatch = {
        "ingesting":       _render_ingesting,
        "schema":          _render_schema,
        "extracting":      _render_extracting,
        "preview":         _render_preview,
        "approve":         _render_approve,
        "full_ingesting":  _render_full_run,
        "full_extracting": _render_full_run,
        "done":            _render_preview,
        "failed":          _render_failed,
    }
    fn = dispatch.get(t.step)
    if fn:
        fn(t)
    else:
        _render_landing()


# ── LANDING ───────────────────────────────────────────────────────────────────

def _render_landing() -> None:
    st.markdown(
        "<div style='max-width:620px;margin:48px auto 0;padding:0 16px'>"
        "<h1 style='text-align:center;margin-bottom:6px'>Dataset Builder</h1>"
        "<p style='text-align:center;color:#666;margin-bottom:36px'>"
        "Point at a folder of PDFs, describe what to extract, and we build a tabular dataset.<br>"
        "We process a small trial batch first so you can review the schema before the full run.</p>"
        "</div>",
        unsafe_allow_html=True,
    )

    col = st.columns([1, 3, 1])[1]
    with col:
        st.markdown(
            "<div style='font-size:0.74rem;font-weight:700;letter-spacing:0.1em;"
            "text-transform:uppercase;color:#888;margin-bottom:8px'>1 — Documents</div>",
            unsafe_allow_html=True,
        )

        path_tab, upload_tab = st.tabs(["Folder path  (recommended)", "Upload files"])

        with path_tab:
            docs_dir_path = st.text_input(
                "Path",
                placeholder=r"C:\Users\casey\ESGReports   or   /mnt/c/Users/casey/ESGReports",
                key="landing_docs_dir", label_visibility="collapsed",
            )
            st.caption(
                "Enter the folder path as seen by this machine (WSL/Linux path). "
                "Scans all PDFs recursively."
            )

        with upload_tab:
            uploaded = st.file_uploader(
                "Select PDFs",
                type="pdf", accept_multiple_files=True,
                key="landing_upload", label_visibility="collapsed",
            )
            st.caption(
                "Select individual PDF files. **Folders cannot be uploaded via browser** — "
                "use the 'Folder path' tab to point at a whole directory."
            )
            if uploaded:
                st.success(f"{len(uploaded)} PDF{'s' if len(uploaded) != 1 else ''} selected.")

        st.markdown(
            "<div style='font-size:0.74rem;font-weight:700;letter-spacing:0.1em;"
            "text-transform:uppercase;color:#888;margin:20px 0 8px'>2 — Dataset name</div>",
            unsafe_allow_html=True,
        )
        corpus_name = st.text_input(
            "Name", placeholder="TSX ESG Reports 2024",
            key="landing_corpus_name", label_visibility="collapsed",
        )

        st.markdown(
            "<div style='font-size:0.74rem;font-weight:700;letter-spacing:0.1em;"
            "text-transform:uppercase;color:#888;margin:20px 0 8px'>3 — What to extract</div>",
            unsafe_allow_html=True,
        )

        desc_tab, json_tab = st.tabs(["Plain text description", "JSON schema spec"])
        with desc_tab:
            topic = st.text_area(
                "Description",
                placeholder=(
                    "Describe the fields you need in plain language:\n\n"
                    "  •  Scope 1, 2, 3 GHG emissions with targets and baseline year\n"
                    "  •  Tariff exposure — dollar impact, affected products, NAICS codes\n"
                    "  •  Board diversity: gender breakdown, independent directors"
                ),
                height=140, key="landing_topic", label_visibility="collapsed",
            )

        with json_tab:
            schema_file = st.file_uploader(
                "JSON schema", type=["json"], key="landing_schema_json",
                label_visibility="collapsed",
                help='Array [{name,type,description}], {"fields":[...]}, or JSON Schema properties.',
            )
            if schema_file:
                sig = f"{schema_file.name}:{getattr(schema_file, 'size', 0)}"
                if st.session_state.get("_json_sig") != sig:
                    try:
                        schema_file.seek(0)
                    except Exception:
                        pass
                    parsed = _parse_json_schema(schema_file.read())
                    if parsed:
                        st.session_state["_json_sig"] = sig
                        st.session_state["landing_parsed_schema"] = parsed
                        auto = "Extract fields per uploaded schema: " + ", ".join(
                            c.get("name", "?") for c in parsed[:8])
                        st.session_state["landing_topic"] = auto
                        st.success(f"Schema loaded — {len(parsed)} fields.")
                    else:
                        st.error("Couldn't parse JSON schema.")
                elif st.session_state.get("landing_parsed_schema"):
                    st.success(f"Schema ready — {len(st.session_state['landing_parsed_schema'])} fields.")

        st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

        # Validation
        path_val = (st.session_state.get("landing_docs_dir") or "").strip()
        name_val = (st.session_state.get("landing_corpus_name") or "").strip()
        topic_val = (st.session_state.get("landing_topic") or "").strip()
        pre_schema = st.session_state.get("landing_parsed_schema")

        has_docs  = bool(uploaded) or bool(path_val)
        has_name  = bool(name_val)
        has_topic = bool(topic_val) or bool(pre_schema)

        if st.button("Start analysis →", type="primary", use_container_width=True,
                     key="landing_start", disabled=not (has_docs and has_name and has_topic)):
            _do_start(uploaded, path_val, name_val, topic_val)


def _do_start(uploaded_files, docs_dir_path: str, corpus_name: str, topic: str) -> None:
    from app_pages.thread_store import Thread as TThread
    import traceback as tb

    pre_schema = st.session_state.get("landing_parsed_schema")
    topic_final = topic.strip()
    if not topic_final and pre_schema:
        names = ", ".join(c.get("name", "?") for c in pre_schema[:10])
        topic_final = f"Extract data per uploaded JSON schema ({len(pre_schema)} fields): {names}"
    if not topic_final:
        topic_final = "Extract structured data"

    t = TThread.create(docs_dir="", corpus_name=corpus_name.strip(),
                       topic=topic_final, trial_n=7)
    t.status = "ingesting"
    t.step = "ingesting"

    with st.spinner("Preparing documents…"):
        try:
            if uploaded_files:
                dest = _UPLOADS_DIR / t.thread_id
                dest.mkdir(parents=True, exist_ok=True)
                for f in uploaded_files:
                    (dest / f.name).write_bytes(f.read())
                t.docs_dir = str(dest)
                t.add_log(f"<span class='log-info'>Saved {len(uploaded_files)} PDFs → {dest}</span>")
            else:
                from tariff_agent.corpus.paths import normalize_host_path
                t.docs_dir = str(normalize_host_path(docs_dir_path))
                t.add_log(f"<span class='log-info'>Folder: {t.docs_dir}</span>")

            if pre_schema:
                ds = {"proposed_columns": list(pre_schema), "_schema_preloaded": True}
                st.session_state["ws_ds"] = ds
                t.schema_cols = list(pre_schema)
                t.add_log(f"<span class='log-info'>Pre-loaded schema: {len(pre_schema)} fields</span>")

            cfg, n = _make_corpus_config(t)
            if n == 0:
                st.warning("No PDFs found — check the path.")
                return
            t.add_log(f"<span class='log-info'>Found {n} PDFs. Trial batch: {t.trial_n} docs.</span>")
        except Exception as e:
            st.error(f"Setup error: {e}")
            t.add_log(f"<span class='log-error'>{tb.format_exc()[-400:]}</span>")
            t.status = "failed"; t.step = "failed"; t.error_msg = str(e)
            save_thread(t)
            return

    save_thread(t)
    _launch_ingest(t, trial_n=t.trial_n)
    st.session_state["active_thread_id"] = t.thread_id
    st.session_state["_ws_loaded_id"] = t.thread_id
    for k in ("landing_parsed_schema", "_json_sig"):
        st.session_state.pop(k, None)
    st.rerun()


# ── INGESTING ────────────────────────────────────────────────────────────────

def _render_ingesting(t: Thread) -> None:
    main, right = st.columns([4, 1.2], gap="large")
    with main:
        st.markdown(f"## {t.title}")
        pct = _parse_progress_pct(t)
        done = max(1, int(pct * t.trial_n / 100))
        st.progress(min(pct, 99) / 100, f"Parsing {done}/{t.trial_n} documents…")
        st.info(
            f"Processing the first **{t.trial_n} documents** with Docling + LLM chunking. "
            "Schema design starts automatically when done."
        )
        if not st.session_state.get("ws_proc_done", True):
            _poll_proc(t)
            save_thread(t)
            time.sleep(1.2)
            st.rerun()
        else:
            save_thread(t)
            st.rerun()
    with right:
        _render_inspector(t, {})


# ── SCHEMA DESIGN ────────────────────────────────────────────────────────────

def _render_schema(t: Thread) -> None:
    ds: dict = st.session_state.get("ws_ds", {})

    # Auto-propose schema on first visit
    if not ds.get("proposed_columns") and not ds.get("_schema_requested") \
            and not ds.get("_schema_preloaded"):
        ds["_schema_requested"] = True
        st.session_state["ws_ds"] = ds
        with st.spinner("Reading your documents and proposing a schema…"):
            _run_schema_node(t, ds, prompt=t.topic)
        st.rerun()

    cols = ds.get("proposed_columns", [])
    main, right = st.columns([4, 1.2], gap="large")

    with main:
        st.markdown(f"## {t.title}")
        if cols:
            st.markdown(
                f"<p style='color:#555;margin-bottom:12px'>"
                f"Agent proposed <strong>{len(cols)} fields</strong>. "
                f"Click a column header below to refine it via the terminal, "
                f"or click <strong>Run extraction</strong> to see results.</p>",
                unsafe_allow_html=True,
            )
            _render_field_strip(cols)
            st.markdown(_empty_header_grid(cols), unsafe_allow_html=True)
            st.markdown("---")
            b1, b2 = st.columns(2)
            if b1.button("▶  Run extraction on trial batch", type="primary", key="_run_ex"):
                t.status = "extracting"; t.step = "extracting"
                save_thread(t); st.rerun()
            if b2.button("Clear schema", key="_clear_sc"):
                ds = {}; st.session_state["ws_ds"] = ds
                t.chat = []; t.schema_cols = []; save_thread(t); st.rerun()
        else:
            st.info("Designing schema from your documents…")

    with right:
        _render_inspector(t, ds)

    st.markdown("---")
    _render_terminal(t, ds)


# ── EXTRACTING ───────────────────────────────────────────────────────────────

def _render_extracting(t: Thread) -> None:
    ds: dict = st.session_state.get("ws_ds", {})
    main, right = st.columns([4, 1.2], gap="large")
    with main:
        st.markdown(f"## {t.title}")
        st.info(f"Extracting {len(ds.get('proposed_columns',[]))} fields from trial batch…")
        with st.spinner("Running extraction…"):
            try:
                from tariff_agent.dataset_graph.extraction_node import extraction_node
                state = extraction_node({**_corpus_overlay(t), **ds})
                rows = state.get("rows", [])
                st.session_state["ws_ds"] = state
                t.rows = rows
                t.status = "preview"; t.step = "preview"
                t.add_log(f"<span class='log-info'>Extracted {len(rows)} rows</span>")
                _save_schema(t, state)
            except Exception as e:
                t.status = "failed"; t.step = "failed"; t.error_msg = str(e)
                t.add_log(f"<span class='log-error'>{e}</span>")
                save_thread(t)
        st.rerun()
    with right:
        _render_inspector(t, ds)


# ── PREVIEW / DONE ────────────────────────────────────────────────────────────

def _render_preview(t: Thread) -> None:
    ds:   dict = st.session_state.get("ws_ds", {})
    cols: list = ds.get("proposed_columns", [])
    rows: list = t.rows or ds.get("rows", [])
    cell_ann: dict = st.session_state.get("ws_cell_ann", {})
    is_full = t.step == "done"

    main, right = st.columns([4, 1.2], gap="large")

    with main:
        # Header row
        h1, h2, h3 = st.columns([3, 1.1, 1])
        h1.markdown(
            f"<h2 style='margin:0'>{t.title}</h2>"
            f"<span style='font-size:0.84rem;color:#666'>"
            f"{'Full corpus' if is_full else 'Preview · trial batch'} · "
            f"{len(rows)} row{'s' if len(rows)!=1 else ''} · {len(cols)} fields</span>",
            unsafe_allow_html=True,
        )
        if t.step == "preview":
            if h2.button("Approve & run full corpus →", type="primary", key="_approve"):
                t.status = "approve"; t.step = "approve"
                save_thread(t); st.rerun()
        if rows:
            df_out = pd.DataFrame(rows)
            h3.download_button(
                "⬇ CSV",
                data=df_out.to_csv(index=False).encode(),
                file_name=f"{t.corpus_id}_{'full' if is_full else 'preview'}.csv",
                mime="text/csv", key="_dl",
            )

        # Metrics
        if rows and cols:
            m1, m2, m3 = st.columns(3)
            filled = sum(1 for r in rows if any(r.get(c["name"]) for c in cols))
            evidence = sum(1 for r in rows for c in cols if r.get(f"{c['name']}_evidence_quote"))
            m1.metric("Rows", len(rows))
            m2.metric("With data", filled)
            m3.metric("Evidence quotes", evidence)

        # Field strip + table
        if cols:
            _render_field_strip(cols)
        if rows:
            df = pd.DataFrame(rows)
            from tariff_agent.corpus.config import CorpusConfig
            yaml = _CONFIG_DIR / f"{t.corpus_id}.yaml"
            id_cols = []
            if yaml.exists():
                try:
                    id_cols = list(CorpusConfig.from_yaml(yaml).identity_fields)
                except Exception:
                    pass
            col_names = [c["name"] for c in cols]
            disp_cols = [c for c in id_cols + col_names if c in df.columns]
            df_disp = df[disp_cols] if disp_cols else df
            active_field = st.session_state.get("ws_active_field", "")
            st.markdown(
                _grid_html(df_disp, cols, active_field=active_field, cell_ann=cell_ann),
                unsafe_allow_html=True,
            )
        else:
            st.info("No rows yet — run extraction to populate the table.")

        # Cell annotation
        _render_cell_annotation(t, ds)

    with right:
        _render_inspector(t, ds)

    st.markdown("---")
    _render_terminal(t, ds)


# ── APPROVAL GATE ─────────────────────────────────────────────────────────────

def _render_approve(t: Thread) -> None:
    ds:   dict = st.session_state.get("ws_ds", {})
    cols: list = ds.get("proposed_columns", [])
    rows: list = t.rows or ds.get("rows", [])
    total = _total_docs(t)

    main, right = st.columns([4, 1.2], gap="large")
    with main:
        st.markdown("## Approve schema & run full corpus")
        st.markdown(
            f"<p style='color:#555'>You've reviewed the <strong>{t.trial_n}-document trial</strong>. "
            f"Approving will process all <strong>{total} documents</strong>.</p>",
            unsafe_allow_html=True,
        )

        # Schema quality table
        if cols and rows:
            st.markdown("### Schema quality (trial)")
            qdata = []
            for c in cols:
                nm = c["name"]
                filled = sum(1 for r in rows if r.get(nm) not in (None, "", "nan", False))
                ev = sum(1 for r in rows if r.get(f"{nm}_evidence_quote"))
                qdata.append({"Field": nm, "Type": c.get("type",""),
                               "Fill %": f"{round(100*filled/max(len(rows),1))}%",
                               "Evidence": ev})
            st.dataframe(pd.DataFrame(qdata), use_container_width=True, hide_index=True,
                         height=min(220, 40 + 35 * len(qdata)))

        m1, m2, m3 = st.columns(3)
        m1.metric("Fields", len(cols))
        m2.metric("Trial rows", len(rows))
        m3.metric("Full corpus", f"{total} docs")

        st.markdown("### Final instructions _(optional)_")
        notes = st.text_area(
            "Notes",
            placeholder=(
                "Any last instructions before running all documents:\n"
                "  •  'Prioritize table data over prose for all numeric fields'\n"
                "  •  'If value spans years, take most recent'\n"
                "  •  'Parent company only — ignore subsidiary filings'"
            ),
            height=100, key="_approve_notes", label_visibility="collapsed",
        )

        st.markdown("---")
        c1, c2 = st.columns([2, 1])
        if c1.button(f"✓  Approve & run all {total} documents →",
                     type="primary", key="_final_approve", use_container_width=True):
            if notes.strip():
                t.add_chat("user", notes.strip())
                with st.spinner("Applying final instructions…"):
                    _run_schema_node(t, ds, prompt=notes.strip())
            t.status = "full_ingesting"; t.step = "full_ingesting"
            save_thread(t)
            _launch_ingest(t, trial_n=0)
            st.rerun()
        if c2.button("← Back to preview", key="_back_approve"):
            t.status = "preview"; t.step = "preview"
            save_thread(t); st.rerun()

    with right:
        _render_inspector(t, ds)

    st.markdown("---")
    _render_terminal(t, ds)


# ── FULL RUN ─────────────────────────────────────────────────────────────────

def _render_full_run(t: Thread) -> None:
    ds: dict = st.session_state.get("ws_ds", {})
    total = _total_docs(t)
    main, right = st.columns([4, 1.2], gap="large")
    with main:
        verb = "Ingesting" if t.step == "full_ingesting" else "Extracting"
        st.markdown(f"## {t.title}")
        pct = _parse_progress_pct(t) if t.step == "full_ingesting" else 50
        st.progress(min(pct, 99) / 100, f"{verb} {total} documents…")
        st.info(f"**{verb} full corpus.** This runs in the background — the log updates live.")

        if not st.session_state.get("ws_proc_done", True):
            _poll_proc(t)
            save_thread(t)
            time.sleep(1.5)
            st.rerun()
        elif t.step == "full_extracting":
            with st.spinner("Running full extraction…"):
                try:
                    from tariff_agent.dataset_graph.extraction_node import extraction_node
                    state = extraction_node({**_corpus_overlay(t), **ds, "use_sample": False})
                    rows = state.get("rows", [])
                    st.session_state["ws_ds"] = state
                    t.rows = rows; t.status = "done"; t.step = "done"
                    t.add_log(f"<span class='log-info'>Full extraction: {len(rows)} rows</span>")
                    _save_schema(t, state)
                except Exception as e:
                    t.status = "failed"; t.step = "failed"; t.error_msg = str(e)
            save_thread(t); st.rerun()
        else:
            save_thread(t); st.rerun()
    with right:
        _render_inspector(t, ds)


# ── FAILED ────────────────────────────────────────────────────────────────────

def _render_failed(t: Thread) -> None:
    st.error(f"**Pipeline failed:** {t.error_msg or 'Check the log below.'}")
    _render_log(t.log, st.empty())
    c1, c2 = st.columns(2)
    if c1.button("↩  Back to schema", key="_fail_back"):
        t.status = "schema"; t.step = "schema"; t.error_msg = ""
        save_thread(t); st.rerun()
    if c2.button("🗑  Delete thread", key="_fail_del"):
        delete_thread(t.thread_id)
        st.session_state.pop("active_thread_id", None)
        st.rerun()


# Keep backwards-compat entry point
def page() -> None:
    render(active_thread_id=st.session_state.get("active_thread_id"))

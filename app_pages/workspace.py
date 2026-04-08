"""Workspace renderer — called by app.py for the main content area.

render(active_thread_id) is the single entry point.

Flow per thread:
  new          → landing (folder path + description → send)
  ingesting    → log view (live subprocess drain)
  schema       → schema chat + field cards
  extracting   → spinner + log
  preview      → big frozen table + chat to refine
  full_ingesting / full_extracting → log + progress
  done         → frozen table + export
  failed       → error + retry
"""
from __future__ import annotations

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
_CONFIG_DIR.mkdir(parents=True, exist_ok=True)


# ── Subprocess helpers ───────────────────────────────────────────────────────

def _pipeline_cmd(t: Thread, trial_n: int = 0) -> list[str]:
    script = str(ROOT / "scripts" / "run_corpus_pipeline.py")
    yaml   = _CONFIG_DIR / f"{t.corpus_id}.yaml"
    cmd    = [sys.executable, script]
    cmd   += ["--config", str(yaml)] if yaml.exists() else ["--corpus", t.corpus_id]
    cmd   += ["--stage", "all"]
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
    t.add_log(f"<span class='log-step'>$ {' '.join(Path(c).name if '/' in c else c for c in cmd)}</span>")
    proc  = _start_proc(cmd)
    q: Queue = Queue()
    PThread(target=_drain, args=(proc, q), daemon=True).start()
    st.session_state.ws_proc      = proc
    st.session_state.ws_queue     = q
    st.session_state.ws_proc_done = False
    st.session_state.ws_proc_rc   = 0


def _poll_proc(t: Thread) -> bool:
    """Drain the queue into t.log. Return True if process finished this poll."""
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
                rc   = proc.returncode if proc else 0
                st.session_state.ws_proc_done = True
                st.session_state.ws_proc_rc   = rc
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


# ── Log rendering ────────────────────────────────────────────────────────────

def _render_log(lines: list[str], placeholder) -> None:
    body = "\n".join(lines[-180:]) if lines else "<span class='log-dim'>Waiting…</span>"
    placeholder.markdown(
        f"<div class='agent-log'>{body}</div>"
        "<script>var e=document.querySelector('.agent-log');if(e)e.scrollTop=e.scrollHeight;</script>",
        unsafe_allow_html=True,
    )


# ── Frozen table ─────────────────────────────────────────────────────────────

def _table_html(df: pd.DataFrame, active_col: str = "", max_rows: int = 400) -> str:
    def _th(c: str) -> str:
        cls = "th-active" if c == active_col else ""
        return f"<th class='{cls}' title='{c}'>{c}</th>"

    def _td(v) -> str:
        sv = "" if v is None else str(v)
        if sv in ("", "nan", "None"):
            return "<td class='null'>—</td>"
        if sv.lower() == "true":
            return "<td class='bool-t'>✓</td>"
        if sv.lower() == "false":
            return "<td class='bool-f'>✗</td>"
        return f"<td title='{sv[:240]}'>{sv[:80]}</td>"

    heads = "".join(_th(c) for c in df.columns)
    body  = "".join(
        f"<tr>{''.join(_td(row[c]) for c in df.columns)}</tr>"
        for _, row in df.head(max_rows).iterrows()
    )
    return (
        f"<div class='frozen-table-wrap'>"
        f"<table class='frozen-table'>"
        f"<thead><tr>{heads}</tr></thead>"
        f"<tbody>{body}</tbody>"
        f"</table></div>"
    )


# ── Corpus config helpers ────────────────────────────────────────────────────

def _make_corpus_config(t: Thread):
    from tariff_agent.corpus.config import CorpusConfig
    from tariff_agent.corpus.paths import normalize_host_path
    from tariff_agent.corpus.scan_index import write_corpus_index

    norm      = normalize_host_path(t.docs_dir)
    idx_path  = ROOT / "data" / "metadata" / f"corpus_{t.corpus_id}_index.csv"
    n         = write_corpus_index(norm, idx_path)
    rel       = str(idx_path.relative_to(ROOT))

    cfg = CorpusConfig(
        name=t.corpus_name,
        corpus_id=t.corpus_id,
        topic=t.topic,
        docs_dir=str(norm),
        file_pattern="csv_manifest",
        metadata_csv=rel,
        doc_id_field="filing_id",
        doc_path_field="local_path",
        identity_fields=["filing_id", "ticker", "issuer_name", "filing_type", "filing_date"],
        extra_context_fields=[],
        output_base_dir=str(ROOT / "output"),
        index_csv=rel,
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


# ── Session init ─────────────────────────────────────────────────────────────

def _init_ws():
    defaults = {
        "ws_proc_done":    True,
        "ws_proc_rc":      0,
        "ws_ds":           {},
        "ws_active_field": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


# ═══════════════════════════════════════════════════════════════════════════
# RENDER — main entry point called from app.py
# ═══════════════════════════════════════════════════════════════════════════

def render(active_thread_id: str | None = None) -> None:
    _init_ws()

    if active_thread_id is None:
        _render_landing()
        return

    t = load_thread(active_thread_id)
    if t is None:
        st.warning("Thread not found.")
        _render_landing()
        return

    # Sync subprocess state back from session
    if not st.session_state.ws_proc_done:
        finished = _poll_proc(t)
        if finished:
            rc = st.session_state.ws_proc_rc
            if rc == 0:
                # Auto-advance after successful ingest
                if t.status in ("ingesting",):
                    t.status = "schema"
                    t.step   = "schema"
                elif t.status == "full_ingesting":
                    t.status = "full_extracting"
                    t.step   = "full_extracting"
                elif t.status == "full_extracting":
                    t.status = "done"
                    t.step   = "done"
            else:
                t.status   = "failed"
                t.step     = "failed"
                t.error_msg = f"Process exited with code {rc}."
            save_thread(t)

    if   t.step in ("new",):              _render_landing()
    elif t.step == "ingesting":           _render_ingesting(t)
    elif t.step == "schema":              _render_schema(t)
    elif t.step in ("extracting",):       _render_extracting(t)
    elif t.step in ("preview", "done"):   _render_table(t)
    elif t.step in ("full_ingesting", "full_extracting"): _render_full_ingest(t)
    elif t.step == "failed":              _render_failed(t)
    else:
        _render_landing()


# ── LANDING ──────────────────────────────────────────────────────────────────

def _render_landing() -> None:
    st.markdown("""
<div style='max-width:660px;margin:60px auto 0;padding:0 16px'>
  <h1 style='text-align:center;margin-bottom:6px'>Dataset Builder</h1>
  <p style='text-align:center;color:var(--text-muted);margin-bottom:36px'>
    Point at a folder of PDFs. Describe what you want to extract.<br>
    We process a small batch first so you can refine before running the full corpus.
  </p>
</div>""", unsafe_allow_html=True)

    col = st.columns([1, 3, 1])[1]
    with col:
        docs_dir = st.text_input(
            "PDF folder",
            placeholder=r"C:\Users\casey\Reports  or  /mnt/c/Users/casey/Reports",
            key="landing_docs_dir",
            label_visibility="visible",
        )
        corpus_name = st.text_input(
            "Dataset name",
            placeholder="TSX ESG Reports 2024",
            key="landing_corpus_name",
        )
        topic = st.text_area(
            "What do you want to extract?",
            placeholder=(
                "Describe the fields you need in plain language.\n\n"
                "Examples:\n"
                "  • Scope 1, 2, 3 GHG emissions with targets and baseline year\n"
                "  • Tariff exposure — dollar impact, affected products, NAICS codes\n"
                "  • Board diversity: gender breakdown, independent directors, committee roles"
            ),
            height=140,
            key="landing_topic",
        )

        can_start = bool(docs_dir.strip() and corpus_name.strip() and topic.strip())
        if st.button("Start →", type="primary", disabled=not can_start,
                     use_container_width=True, key="landing_start"):
            _start_new_thread(docs_dir.strip(), corpus_name.strip(), topic.strip())


def _start_new_thread(docs_dir: str, corpus_name: str, topic: str) -> None:
    from app_pages.thread_store import Thread as TThread

    t = TThread.create(docs_dir=docs_dir, corpus_name=corpus_name, topic=topic, trial_n=7)
    t.add_log(f"<span class='log-info'>Scanning {docs_dir} …</span>")
    t.status = "ingesting"
    t.step   = "ingesting"
    save_thread(t)

    # Build corpus config + index
    with st.spinner("Scanning PDF folder…"):
        try:
            cfg, n = _make_corpus_config(t)
            t.add_log(f"<span class='log-info'>Found {n} PDFs. Starting trial ingest ({t.trial_n} docs)…</span>")
        except Exception as e:
            t.status    = "failed"
            t.step      = "failed"
            t.error_msg = str(e)
            save_thread(t)
            st.error(str(e))
            return

    if n == 0:
        t.status    = "failed"
        t.step      = "failed"
        t.error_msg = "No PDFs found in that folder."
        save_thread(t)
        st.warning("No PDFs found — check the path and try again.")
        return

    save_thread(t)
    _launch_ingest(t, trial_n=t.trial_n)
    st.session_state["active_thread_id"] = t.thread_id
    st.rerun()


# ── INGESTING ────────────────────────────────────────────────────────────────

def _render_ingesting(t: Thread) -> None:
    main, right = st.columns([3, 1.1], gap="medium")

    with main:
        st.markdown(
            f"<h2 style='margin-bottom:4px'>{t.title}</h2>"
            f"<div style='font-size:0.8rem;color:var(--text-muted);margin-bottom:16px'>"
            f"Processing {t.trial_n} documents — schema design starts automatically when done.</div>",
            unsafe_allow_html=True,
        )
        progress_ph = st.empty()
        st.info("Parsing and chunking your trial batch. This usually takes 1–3 minutes per document.")

        if not st.session_state.ws_proc_done:
            # Still running — drain and rerender
            _poll_proc(t)
            save_thread(t)

            # Crude progress from parse index
            pct = _parse_progress_pct(t)
            progress_ph.progress(pct, f"Parsed {int(pct * t.trial_n / 100)}/{t.trial_n} docs…")
            time.sleep(1.0)
            st.rerun()
        else:
            # Finished — transition in render() handles auto-advance
            save_thread(t)
            st.rerun()

    with right:
        _log_panel(t)


# ── SCHEMA DESIGN ────────────────────────────────────────────────────────────

def _render_schema(t: Thread) -> None:
    ds: dict = st.session_state.ws_ds

    # Auto-generate schema on first visit if topic exists
    if not ds.get("proposed_columns") and not ds.get("_schema_requested"):
        ds["_schema_requested"] = True
        st.session_state.ws_ds = ds
        with st.spinner("Designing schema from your documents…"):
            _run_schema_node(t, ds, prompt=t.topic)
        st.rerun()

    cols = ds.get("proposed_columns", [])

    main, right = st.columns([3, 1.1], gap="medium")

    with main:
        st.markdown(
            f"<h2 style='margin-bottom:2px'>{t.title}</h2>"
            f"<div style='font-size:0.79rem;color:var(--text-muted);margin-bottom:12px'>"
            f"Schema ready — {len(cols)} fields proposed. Refine via chat below, then build the preview.</div>",
            unsafe_allow_html=True,
        )

        if cols:
            # Field cards grid
            c1, c2 = st.columns(2)
            half = (len(cols) + 1) // 2
            for i, fc in enumerate(cols):
                nm   = fc.get("name", "")
                typ  = fc.get("type", "string")
                desc = fc.get("description", "")[:90]
                note = t.field_notes.get(nm, "")
                tgt  = c1 if i < half else c2
                with tgt:
                    is_active = st.session_state.ws_active_field == nm
                    border = "border-color:var(--brown)" if is_active else "border-color:var(--cream-dark)"
                    st.markdown(
                        f"<div class='field-card' style='{border}'>"
                        f"<strong>{nm}</strong> "
                        f"<span style='font-size:0.7rem;color:var(--text-muted)'>{typ}</span>"
                        f"<div style='font-size:0.74rem;color:var(--text-muted);margin-top:2px'>{desc}</div>"
                        + (f"<div class='field-note'>{note}</div>" if note else "")
                        + "</div>",
                        unsafe_allow_html=True,
                    )
                    if st.button("Inspect", key=f"_fc_{nm}", use_container_width=False,
                                 help="View and annotate this field"):
                        st.session_state.ws_active_field = nm
                        st.rerun()

            st.markdown("---")
            b1, b2 = st.columns(2)
            if b1.button("▶  Build preview table", type="primary", key="_run_extract"):
                t.status = "extracting"
                t.step   = "extracting"
                save_thread(t)
                st.rerun()
            if b2.button("Clear & redesign", key="_schema_clear"):
                st.session_state.ws_ds = {}
                t.chat = []
                save_thread(t)
                st.rerun()

        # Chat history
        if t.chat:
            st.markdown("---")
            for msg in t.chat[-12:]:
                with st.chat_message(msg["role"]):
                    st.markdown(msg["content"])

        prompt = st.chat_input("Refine schema — e.g. 'add a target year field', 'split X into two columns'…",
                               key="schema_chat")
        if prompt:
            t.add_chat("user", prompt)
            save_thread(t)
            with st.spinner("Updating schema…"):
                _run_schema_node(t, ds, prompt=prompt)
            st.rerun()

    with right:
        _log_panel(t)
        _field_inspector(t)


def _run_schema_node(t: Thread, ds: dict, prompt: str) -> None:
    try:
        from tariff_agent.dataset_graph.schema_node import schema_node
        overlay = _corpus_overlay(t)
        state = {
            **overlay,
            **ds,
            "user_query":       ds.get("user_query") or prompt,
            "schema_feedback":  prompt if ds.get("proposed_columns") else "",
            "schema_iteration": ds.get("schema_iteration", 0),
            "schema_approved":  False,
            "use_sample":       True,
            "sample_tickers":   [],
            "extraction_mode":  "direct",
        }
        state   = schema_node(state)
        new_cols = state.get("proposed_columns", [])
        name     = state.get("dataset_name", t.corpus_name)
        reply    = (
            f"Proposed **{len(new_cols)} fields** for `{name}`:\n\n"
            + "\n".join(f"- **{c['name']}** `{c.get('type','')}` — {c.get('description','')[:80]}"
                        for c in new_cols)
            + "\n\nClick **Build preview table** to extract, or keep refining."
        )
        st.session_state.ws_ds = state
        t.add_chat("assistant", reply)
        t.add_log(f"<span class='log-info'>Schema: {len(new_cols)} fields for '{name}'</span>")
    except Exception as e:
        reply = f"Schema design failed: {e}"
        t.add_chat("assistant", reply)
        t.add_log(f"<span class='log-error'>{e}</span>")
    save_thread(t)


# ── EXTRACTING ───────────────────────────────────────────────────────────────

def _render_extracting(t: Thread) -> None:
    ds: dict = st.session_state.ws_ds

    main, right = st.columns([3, 1.1], gap="medium")
    with main:
        st.markdown(f"<h2>{t.title}</h2>", unsafe_allow_html=True)
        st.info("Running extraction on trial batch…")
        with st.spinner(f"Extracting {len(ds.get('proposed_columns',[]))} fields from {t.trial_n} docs…"):
            try:
                from tariff_agent.dataset_graph.extraction_node import extraction_node
                state = extraction_node({**_corpus_overlay(t), **ds})
                rows  = state.get("rows", [])
                st.session_state.ws_ds = state
                t.rows   = rows
                t.status = "preview"
                t.step   = "preview"
                t.add_log(f"<span class='log-info'>Extracted {len(rows)} rows</span>")
            except Exception as e:
                t.status    = "failed"
                t.step      = "failed"
                t.error_msg = str(e)
                t.add_log(f"<span class='log-error'>{e}</span>")
        save_thread(t)
        st.rerun()
    with right:
        _log_panel(t)


# ── PREVIEW / DONE TABLE ─────────────────────────────────────────────────────

def _render_table(t: Thread) -> None:
    ds:   dict = st.session_state.ws_ds
    cols: list = ds.get("proposed_columns", [])
    rows: list = t.rows or ds.get("rows", [])

    main, right = st.columns([3, 1.1], gap="medium")

    with main:
        # ── Header bar ──────────────────────────────────────────────────────
        h1, h2, h3 = st.columns([3, 1, 1])
        h1.markdown(
            f"<h2 style='margin:0'>{t.title}</h2>"
            f"<span style='font-size:0.76rem;color:var(--text-muted)'>"
            f"{len(rows)} rows · {len(cols)} fields · trial batch ({t.trial_n} docs)</span>",
            unsafe_allow_html=True,
        )
        if t.step == "preview":
            if h2.button("Run full corpus →", type="primary", key="_full_corpus"):
                t.status = "full_ingesting"
                t.step   = "full_ingesting"
                save_thread(t)
                _launch_ingest(t, trial_n=0)
                st.rerun()
        if rows:
            df_out = pd.DataFrame(rows)
            h3.download_button(
                "⬇ CSV",
                data=df_out.to_csv(index=False).encode(),
                file_name=f"{t.corpus_id}_preview.csv",
                mime="text/csv",
                key="_dl_preview",
            )

        # ── Metrics ──────────────────────────────────────────────────────────
        if rows and cols:
            m1, m2, m3 = st.columns(3)
            filled = sum(1 for r in rows if any(r.get(c["name"]) for c in cols))
            evidence = sum(1 for r in rows for c in cols if r.get(f"{c['name']}_evidence_quote"))
            m1.metric("Rows", len(rows))
            m2.metric("With data", filled)
            m3.metric("Evidence quotes", evidence)

        # ── Table ────────────────────────────────────────────────────────────
        if rows:
            from tariff_agent.corpus.config import CorpusConfig
            yaml = _CONFIG_DIR / f"{t.corpus_id}.yaml"
            id_cols = []
            if yaml.exists():
                try:
                    id_cols = list(CorpusConfig.from_yaml(yaml).identity_fields)
                except Exception:
                    pass

            col_names  = [c["name"] for c in cols]
            df         = pd.DataFrame(rows)
            disp       = [c for c in id_cols + col_names if c in df.columns]
            active_col = st.session_state.ws_active_field or ""
            st.markdown(_table_html(df[disp] if disp else df, active_col=active_col),
                        unsafe_allow_html=True)
        else:
            st.info("No rows extracted — try refining the schema and re-extracting.")

        # ── Chat ─────────────────────────────────────────────────────────────
        st.markdown("---")
        if t.chat:
            for msg in t.chat[-8:]:
                with st.chat_message(msg["role"]):
                    st.markdown(msg["content"])

        chat_prompt = st.chat_input(
            "Refine — e.g. 'add a net-zero target year column', 'I need the currency for all amounts'…",
            key="table_chat",
        )
        if chat_prompt:
            t.add_chat("user", chat_prompt)
            save_thread(t)
            with st.spinner("Updating schema…"):
                _run_schema_node(t, ds, prompt=chat_prompt)
            t.status = "extracting"
            t.step   = "extracting"
            save_thread(t)
            st.rerun()

    with right:
        _log_panel(t)
        _field_inspector(t)


# ── FULL INGEST / EXTRACT ────────────────────────────────────────────────────

def _render_full_ingest(t: Thread) -> None:
    main, right = st.columns([3, 1.1], gap="medium")
    with main:
        verb  = "Ingesting" if t.step == "full_ingesting" else "Extracting"
        st.markdown(f"<h2>{t.title}</h2>", unsafe_allow_html=True)
        st.info(f"**{verb} full corpus** — check the log for progress.")
        pct = _parse_progress_pct(t) if t.step == "full_ingesting" else 50
        st.progress(min(pct, 99), f"{verb} all documents…")

        if not st.session_state.ws_proc_done:
            _poll_proc(t)
            save_thread(t)
            time.sleep(1.2)
            st.rerun()
        elif t.step == "full_extracting":
            # Auto-run full extraction
            ds: dict = st.session_state.ws_ds
            with st.spinner("Extracting full corpus…"):
                try:
                    from tariff_agent.dataset_graph.extraction_node import extraction_node
                    state = extraction_node({**_corpus_overlay(t), **ds, "use_sample": False})
                    rows  = state.get("rows", [])
                    st.session_state.ws_ds = state
                    t.rows   = rows
                    t.status = "done"
                    t.step   = "done"
                    t.add_log(f"<span class='log-info'>Full extraction: {len(rows)} rows</span>")
                except Exception as e:
                    t.status    = "failed"
                    t.step      = "failed"
                    t.error_msg = str(e)
            save_thread(t)
            st.rerun()
        else:
            save_thread(t)
            st.rerun()
    with right:
        _log_panel(t)


# ── FAILED ───────────────────────────────────────────────────────────────────

def _render_failed(t: Thread) -> None:
    st.error(f"**Pipeline failed:** {t.error_msg or 'Unknown error'}")
    st.markdown("Check the agent log for details.")
    _render_log(t.log, st.empty())
    c1, c2 = st.columns(2)
    if c1.button("↩  Go back to schema", key="_fail_back"):
        t.status = "schema"
        t.step   = "schema"
        t.error_msg = ""
        save_thread(t)
        st.rerun()
    if c2.button("🗑  Delete thread", key="_fail_del"):
        delete_thread(t.thread_id)
        st.session_state.pop("active_thread_id", None)
        st.rerun()


# ── Right-panel helpers ──────────────────────────────────────────────────────

def _log_panel(t: Thread) -> None:
    st.markdown(
        "<div style='font-size:0.68rem;font-weight:600;letter-spacing:0.09em;"
        "text-transform:uppercase;color:var(--text-muted);margin-bottom:5px'>Agent Log</div>",
        unsafe_allow_html=True,
    )
    ph = st.empty()
    _render_log(t.log, ph)


def _field_inspector(t: Thread) -> None:
    ds:   dict = st.session_state.ws_ds
    cols: list = ds.get("proposed_columns", [])
    if not cols:
        return

    st.markdown("<hr style='margin:10px 0'>", unsafe_allow_html=True)
    st.markdown(
        "<div style='font-size:0.68rem;font-weight:600;letter-spacing:0.09em;"
        "text-transform:uppercase;color:var(--text-muted);margin-bottom:6px'>Field Inspector</div>",
        unsafe_allow_html=True,
    )

    field_names = [c["name"] for c in cols]
    current     = st.session_state.get("ws_active_field")
    idx         = field_names.index(current) if current in field_names else 0
    chosen      = st.selectbox("Select field", field_names, index=idx,
                               key="_field_sel", label_visibility="collapsed")
    st.session_state.ws_active_field = chosen

    fc = next((c for c in cols if c.get("name") == chosen), {})
    st.markdown(
        f"<div class='field-card'>"
        f"<strong>{chosen}</strong> "
        f"<span style='font-size:0.7rem;color:var(--text-muted)'>{fc.get('type','')}</span>"
        f"<div style='font-size:0.76rem;color:var(--text-muted);margin-top:3px'>{fc.get('description','')}</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    note_key = f"_note_{chosen}"
    existing  = t.field_notes.get(chosen, "")
    new_note  = st.text_area("Notes / comments", value=existing, height=80,
                              key=note_key, label_visibility="visible",
                              placeholder="Add context, edge-cases, known issues…")
    if new_note != existing:
        t.field_notes[chosen] = new_note
        save_thread(t)

    # Evidence sample
    rows = t.rows or ds.get("rows", [])
    ev_col = f"{chosen}_evidence_quote"
    samples = [r for r in rows if r.get(ev_col)][:3]
    if samples:
        st.markdown(
            "<div style='font-size:0.7rem;font-weight:600;text-transform:uppercase;"
            "letter-spacing:0.07em;color:var(--text-muted);margin-top:8px'>Evidence</div>",
            unsafe_allow_html=True,
        )
        for r in samples:
            st.markdown(
                f"<div style='font-size:0.76rem;background:#FFFCF6;border:1px solid var(--cream-dark);"
                f"border-radius:3px;padding:6px 9px;margin-top:4px;color:var(--brown-mid)'>"
                f"<em>\"{str(r[ev_col])[:200]}\"</em></div>",
                unsafe_allow_html=True,
            )


# ── Misc helpers ─────────────────────────────────────────────────────────────

def _parse_progress_pct(t: Thread) -> int:
    """Estimate % of trial batch parsed from the parse index CSV."""
    try:
        from tariff_agent.corpus.config import CorpusConfig
        yaml = _CONFIG_DIR / f"{t.corpus_id}.yaml"
        if not yaml.exists():
            return 5
        cfg      = CorpusConfig.from_yaml(yaml)
        parse_csv = cfg.resolve(cfg.parse_index_csv, ROOT)
        if not parse_csv.is_file():
            return 5
        df  = pd.read_csv(parse_csv)
        ok  = len(df[df["parse_status"].str.startswith("OK", na=False)])
        tot = max(t.trial_n, 1)
        return min(int(100 * ok / tot), 99)
    except Exception:
        return 5


# Keep old page() signature so legacy imports don't break
def page() -> None:
    render(active_thread_id=st.session_state.get("active_thread_id"))

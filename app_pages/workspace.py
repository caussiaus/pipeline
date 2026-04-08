"""Workspace renderer — single entry point for the main content area.

render(active_thread_id) is called by app.py.

Thread lifecycle:
  new / (none)     → landing  (upload PDFs or paste path + description)
  ingesting        → live log, progress bar
  schema           → field cards + chat to refine (accepts JSON spec upload)
  extracting       → spinner
  preview          → frozen table + row annotations + chat refinement
  approve          → approval gate before full corpus run
  full_ingesting   → full log + progress
  full_extracting  → spinner
  done             → final frozen table + export
  failed           → error + retry
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
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
_UPLOADS_DIR = ROOT / "output" / "uploads"
_UPLOADS_DIR.mkdir(parents=True, exist_ok=True)


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
    t.add_log(f"<span class='log-step'>$ {' '.join(Path(c).name if os.sep in c else c for c in cmd)}</span>")
    proc  = _start_proc(cmd)
    q: Queue = Queue()
    PThread(target=_drain, args=(proc, q), daemon=True).start()
    st.session_state["ws_proc"]      = proc
    st.session_state["ws_queue"]     = q
    st.session_state["ws_proc_done"] = False
    st.session_state["ws_proc_rc"]   = 0


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
                rc   = proc.returncode if proc else 0
                st.session_state["ws_proc_done"] = True
                st.session_state["ws_proc_rc"]   = rc
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
    body = "\n".join(lines[-200:]) if lines else "<span class='log-dim'>Waiting for output…</span>"
    placeholder.markdown(
        f"<div class='agent-log'>{body}</div>"
        "<script>var e=document.querySelector('.agent-log');if(e)e.scrollTop=e.scrollHeight;</script>",
        unsafe_allow_html=True,
    )


# ── Frozen table ─────────────────────────────────────────────────────────────

def _table_html(df: pd.DataFrame, active_col: str = "", annotations: dict | None = None,
                max_rows: int = 400) -> str:
    ann = annotations or {}

    def _th(c: str) -> str:
        cls = "th-active" if c == active_col else ""
        return f"<th class='{cls}'>{c}</th>"

    def _td(v, row_key: str = "") -> str:
        sv = "" if v is None else str(v)
        if sv in ("", "nan", "None"):
            return "<td class='null'>—</td>"
        if sv.lower() == "true":
            return "<td class='bool-t'>✓</td>"
        if sv.lower() == "false":
            return "<td class='bool-f'>✗</td>"
        return f"<td title='{sv[:300]}'>{sv[:90]}</td>"

    heads = "".join(_th(c) for c in df.columns)
    if ann:
        heads += "<th>Notes</th>"

    rows_html = ""
    for i, (_, row) in enumerate(df.head(max_rows).iterrows()):
        cells = "".join(_td(row[c]) for c in df.columns)
        if ann:
            note = ann.get(str(i), "")
            note_html = (
                f"<td style='color:var(--brown-light);font-style:italic;font-size:0.82rem'>{note}</td>"
                if note else "<td class='null'>—</td>"
            )
            cells += note_html
        rows_html += f"<tr>{cells}</tr>"

    return (
        "<div class='frozen-table-wrap'>"
        "<table class='frozen-table'>"
        f"<thead><tr>{heads}</tr></thead>"
        f"<tbody>{rows_html}</tbody>"
        "</table></div>"
    )


# ── Corpus config helpers ────────────────────────────────────────────────────

def _make_corpus_config(t: Thread):
    from tariff_agent.corpus.config import CorpusConfig
    from tariff_agent.corpus.paths import normalize_host_path
    from tariff_agent.corpus.scan_index import write_corpus_index

    norm     = normalize_host_path(t.docs_dir)
    idx_path = ROOT / "data" / "metadata" / f"corpus_{t.corpus_id}_index.csv"
    n        = write_corpus_index(norm, idx_path)
    rel      = str(idx_path.relative_to(ROOT))

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


def _total_corpus_docs(t: Thread) -> int:
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


# ── Session init ─────────────────────────────────────────────────────────────

def _init_ws() -> None:
    for k, v in {"ws_proc_done": True, "ws_proc_rc": 0, "ws_ds": {},
                 "ws_active_field": None, "ws_row_annotations": {}}.items():
        if k not in st.session_state:
            st.session_state[k] = v


# ── File upload helpers ───────────────────────────────────────────────────────

def _save_uploaded_pdfs(files, thread_id: str) -> str:
    """Save browser-uploaded PDF files to a local dir; return that dir path."""
    dest = _UPLOADS_DIR / thread_id
    dest.mkdir(parents=True, exist_ok=True)
    for f in files:
        (dest / f.name).write_bytes(f.read())
    return str(dest)


def _parse_json_schema(content: bytes | str) -> list[dict] | None:
    """Parse a JSON schema spec file into a list of column dicts."""
    try:
        if isinstance(content, bytes):
            content = content.decode("utf-8-sig")
        data = json.loads(content)
        if isinstance(data, list):
            out = [x for x in data if isinstance(x, dict)]
            return out or None
        if isinstance(data, dict):
            # JSON Schema: {"type":"object","properties":{"col":{"type":"string","description":"..."}}}
            props = data.get("properties")
            if isinstance(props, dict):
                cols = []
                for name, spec in props.items():
                    if isinstance(spec, dict):
                        cols.append({
                            "name": name,
                            "type": str(spec.get("type", "string")),
                            "description": str(spec.get("description", "") or spec.get("title", "")),
                        })
                if cols:
                    return cols
            # Accept {"fields": [...]} or {"columns": [...]} or {"schema": [...]}
            for key in ("fields", "columns", "schema"):
                if key in data and isinstance(data[key], list):
                    out = [x for x in data[key] if isinstance(x, dict)]
                    if out:
                        return out
            # Accept {"field_name": {"type": ..., "description": ...}} style (flat map)
            cols = []
            skip = {"$schema", "type", "title", "description", "properties", "fields"}
            for name, meta in data.items():
                if name in skip:
                    continue
                if isinstance(meta, dict) and ("type" in meta or "description" in meta):
                    row = {"name": name, **meta}
                    cols.append(row)
            if cols:
                return cols
    except Exception:
        pass
    return None


# ═══════════════════════════════════════════════════════════════════════════
# RENDER — main entry point
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

    # Sync subprocess state
    if not st.session_state.get("ws_proc_done", True):
        finished = _poll_proc(t)
        if finished:
            rc = st.session_state.get("ws_proc_rc", 0)
            if rc == 0:
                if t.status == "ingesting":
                    t.status = "schema"
                    t.step   = "schema"
                elif t.status == "full_ingesting":
                    t.status = "full_extracting"
                    t.step   = "full_extracting"
            else:
                t.status    = "failed"
                t.step      = "failed"
                t.error_msg = f"Process exited with code {rc}."
            save_thread(t)

    dispatch = {
        "ingesting":       _render_ingesting,
        "schema":          _render_schema,
        "extracting":      _render_extracting,
        "preview":         _render_table,
        "approve":         _render_approve,
        "full_ingesting":  _render_full_run,
        "full_extracting": _render_full_run,
        "done":            _render_table,
        "failed":          _render_failed,
    }
    fn = dispatch.get(t.step)
    if fn:
        fn(t)
    else:
        _render_landing()


# ── LANDING ──────────────────────────────────────────────────────────────────

def _render_landing() -> None:
    st.markdown("""
<div style='max-width:680px;margin:48px auto 0;padding:0 20px'>
  <h1 style='text-align:center;margin-bottom:8px'>Dataset Builder</h1>
  <p style='text-align:center;color:var(--text-muted);margin-bottom:40px;font-size:1rem'>
    Point at a folder of PDFs, describe what to extract, and we build a structured dataset.<br>
    We start with a small preview batch so you can refine before running the full corpus.
  </p>
</div>""", unsafe_allow_html=True)

    col = st.columns([1, 4, 1])[1]
    with col:
        # ── Step 1: Documents ────────────────────────────────────────────────
        st.markdown(
            "<div style='font-size:0.8rem;font-weight:700;letter-spacing:0.1em;"
            "text-transform:uppercase;color:var(--text-muted);margin-bottom:10px'>"
            "Step 1 — Your documents</div>",
            unsafe_allow_html=True,
        )

        upload_tab, path_tab = st.tabs(["Upload PDFs", "Folder path (server)"])

        with upload_tab:
            uploaded = st.file_uploader(
                "Select PDFs from your computer",
                type="pdf",
                accept_multiple_files=True,
                key="landing_upload",
                label_visibility="collapsed",
                help="Select multiple PDFs — hold Ctrl/Cmd to select more than one",
            )
            if uploaded:
                st.success(f"{len(uploaded)} PDF{'s' if len(uploaded)!=1 else ''} ready to upload.")

        with path_tab:
            docs_dir_path = st.text_input(
                "PDF folder path",
                placeholder=r"C:\Users\casey\Reports  or  /mnt/c/Users/casey/Reports",
                key="landing_docs_dir",
                label_visibility="collapsed",
            )
            st.caption("Use the path as it appears on the server running this app (WSL/Linux path).")

        st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

        # ── Step 2: Name ─────────────────────────────────────────────────────
        st.markdown(
            "<div style='font-size:0.8rem;font-weight:700;letter-spacing:0.1em;"
            "text-transform:uppercase;color:var(--text-muted);margin-bottom:10px'>"
            "Step 2 — Name your dataset</div>",
            unsafe_allow_html=True,
        )
        corpus_name = st.text_input(
            "Dataset name",
            placeholder="TSX ESG Reports 2024",
            key="landing_corpus_name",
            label_visibility="collapsed",
        )

        st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

        # ── Step 3: Description or schema spec ───────────────────────────────
        st.markdown(
            "<div style='font-size:0.8rem;font-weight:700;letter-spacing:0.1em;"
            "text-transform:uppercase;color:var(--text-muted);margin-bottom:10px'>"
            "Step 3 — What to extract</div>",
            unsafe_allow_html=True,
        )

        desc_tab, json_tab = st.tabs(["Describe in plain text", "Upload JSON schema spec"])

        with desc_tab:
            topic = st.text_area(
                "Description",
                placeholder=(
                    "Describe the data you need in plain language.\n\n"
                    "Examples:\n"
                    "  •  Scope 1, 2, 3 GHG emissions with targets and baseline year\n"
                    "  •  Tariff exposure — dollar impact, affected products, NAICS codes\n"
                    "  •  Board diversity: gender breakdown, independent directors, committee roles"
                ),
                height=160,
                key="landing_topic",
                label_visibility="collapsed",
            )

        with json_tab:
            schema_file = st.file_uploader(
                "Upload schema JSON",
                type=["json"],
                key="landing_schema_json",
                label_visibility="collapsed",
                help=(
                    "Array of fields, or {\"fields\": [...]}, or JSON Schema "
                    "{\"properties\": {\"col\": {\"type\": \"string\", \"description\": \"...\"}}}"
                ),
            )
            if schema_file:
                sig = f"{schema_file.name}:{getattr(schema_file, 'size', 0)}"
                if st.session_state.get("_landing_json_sig") != sig:
                    try:
                        schema_file.seek(0)
                    except Exception:
                        pass
                    raw = schema_file.read()
                    parsed = _parse_json_schema(raw)
                    if parsed:
                        st.session_state["_landing_json_sig"] = sig
                        st.success(f"Schema spec loaded — {len(parsed)} fields defined.")
                        st.session_state["landing_parsed_schema"] = parsed
                        names = ", ".join(c.get("name", "?") for c in parsed[:8])
                        auto = f"Extract fields per uploaded schema: {names}"
                        if len(parsed) > 8:
                            auto += " …"
                        st.session_state["landing_topic"] = auto
                    else:
                        st.error(
                            "Couldn't parse that JSON. Use a field array, "
                            '{"fields": [...]}, or JSON Schema "properties".'
                        )
                elif st.session_state.get("landing_parsed_schema"):
                    st.success(
                        f"Schema ready — {len(st.session_state['landing_parsed_schema'])} fields."
                    )

        st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)

        # ── Start button ─────────────────────────────────────────────────────
        topic_ss = (st.session_state.get("landing_topic") or "").strip()
        path_ss  = (st.session_state.get("landing_docs_dir") or "").strip()
        name_ss  = (st.session_state.get("landing_corpus_name") or "").strip()
        parsed_ss = st.session_state.get("landing_parsed_schema")

        has_docs  = bool(uploaded) or bool(path_ss)
        has_name  = bool(name_ss)
        has_topic = bool(topic_ss) or bool(parsed_ss)

        if st.button(
            "Start analysis →",
            type="primary",
            use_container_width=True,
            key="landing_start",
            disabled=not (has_docs and has_name and has_topic),
        ):
            _do_start(
                uploaded,
                path_ss,
                name_ss,
                topic_ss,
            )


def _do_start(uploaded_files, docs_dir_path: str, corpus_name: str, topic: str) -> None:
    from app_pages.thread_store import Thread as TThread
    import traceback as tb

    pre_schema = st.session_state.get("landing_parsed_schema")
    topic_final = (topic or "").strip()
    if not topic_final and pre_schema:
        names = ", ".join(c.get("name", "?") for c in pre_schema[:10])
        topic_final = f"Extract data per uploaded JSON schema ({len(pre_schema)} fields): {names}"
    if not topic_final:
        topic_final = "Extract structured data"

    t = TThread.create(
        docs_dir="",
        corpus_name=corpus_name.strip(),
        topic=topic_final,
        trial_n=7,
    )
    t.status = "ingesting"
    t.step   = "ingesting"

    with st.spinner("Preparing your documents…"):
        try:
            if uploaded_files:
                docs_dir = _save_uploaded_pdfs(uploaded_files, t.thread_id)
                t.docs_dir = docs_dir
                t.add_log(f"<span class='log-info'>Saved {len(uploaded_files)} uploaded PDFs → {docs_dir}</span>")
            else:
                from tariff_agent.corpus.paths import normalize_host_path
                t.docs_dir = str(normalize_host_path(docs_dir_path.strip()))
                t.add_log(f"<span class='log-info'>Using folder: {t.docs_dir}</span>")

            # Inject pre-parsed schema if provided
            if pre_schema:
                st.session_state["ws_ds"] = {
                    "proposed_columns": list(pre_schema),
                    "_schema_preloaded": True,
                }
                t.add_log(
                    f"<span class='log-info'>Pre-loaded schema: {len(pre_schema)} fields</span>"
                )

            cfg, n = _make_corpus_config(t)
            if n == 0:
                st.warning("No PDFs found — check the folder and try again.")
                return
            t.add_log(f"<span class='log-info'>Found {n} PDFs. Starting trial ingest ({t.trial_n} docs)…</span>")
        except Exception as e:
            st.error(f"Setup error: {e}")
            t.add_log(f"<span class='log-error'>{tb.format_exc()[-300:]}</span>")
            t.status = "failed"; t.step = "failed"; t.error_msg = str(e)
            save_thread(t)
            return

    save_thread(t)
    _launch_ingest(t, trial_n=t.trial_n)
    st.session_state["active_thread_id"] = t.thread_id
    st.session_state.pop("landing_parsed_schema", None)
    st.session_state.pop("_landing_json_sig", None)
    st.rerun()


# ── INGESTING ────────────────────────────────────────────────────────────────

def _render_ingesting(t: Thread) -> None:
    main, right = st.columns([3, 1.1], gap="medium")
    with main:
        st.markdown(f"## {t.title}")
        pct = _parse_progress_pct(t)
        done_count = max(1, int(pct * t.trial_n / 100))
        st.progress(min(pct, 99) / 100,
                    f"Parsing document {done_count} of {t.trial_n}…")
        st.markdown(
            f"<p style='color:var(--text-muted);margin-top:8px'>"
            f"Processing your first <strong>{t.trial_n} documents</strong>. "
            f"Schema design starts automatically when done — usually 1–3 min per doc.</p>",
            unsafe_allow_html=True,
        )
        st.info("The agent is reading, parsing, and chunking your PDFs. "
                "You'll be prompted to review the proposed fields once ready.")

        if not st.session_state.get("ws_proc_done", True):
            _poll_proc(t)
            save_thread(t)
            time.sleep(1.2)
            st.rerun()
        else:
            save_thread(t)
            st.rerun()
    with right:
        _log_panel(t)


# ── SCHEMA DESIGN ────────────────────────────────────────────────────────────

def _render_schema(t: Thread) -> None:
    ds: dict = st.session_state.get("ws_ds", {})

    # Auto-generate schema on first visit
    if not ds.get("proposed_columns") and not ds.get("_schema_requested") \
            and not ds.get("_schema_preloaded"):
        ds["_schema_requested"] = True
        st.session_state["ws_ds"] = ds
        with st.spinner("Reading your documents and designing a schema…"):
            _run_schema_node(t, ds, prompt=t.topic)
        st.rerun()

    cols = ds.get("proposed_columns", [])
    main, right = st.columns([3, 1.1], gap="medium")

    with main:
        st.markdown(f"## {t.title}")
        if cols:
            st.markdown(
                f"<p style='color:var(--text-muted);margin-bottom:16px'>"
                f"The agent proposed <strong>{len(cols)} fields</strong> based on your documents. "
                f"Refine below, then hit <strong>Build preview table</strong> to see extracted data.</p>",
                unsafe_allow_html=True,
            )

            # Field cards grid
            c1, c2 = st.columns(2)
            half = (len(cols) + 1) // 2
            for i, fc in enumerate(cols):
                nm   = fc.get("name", "")
                typ  = fc.get("type", "string")
                desc = fc.get("description", "")[:100]
                note = t.field_notes.get(nm, "")
                tgt  = c1 if i < half else c2
                with tgt:
                    is_active = st.session_state.get("ws_active_field") == nm
                    border_col = "var(--brown)" if is_active else "var(--cream-dark)"
                    st.markdown(
                        f"<div class='field-card' style='border-color:{border_col}'>"
                        f"<strong style='font-size:0.95rem'>{nm}</strong> "
                        f"<span style='font-size:0.78rem;color:var(--text-muted);background:var(--cream-mid);"
                        f"padding:1px 7px;border-radius:99px'>{typ}</span>"
                        f"<div style='font-size:0.86rem;color:var(--text-muted);margin-top:4px'>{desc}</div>"
                        + (f"<div class='field-note'>{note}</div>" if note else "")
                        + "</div>",
                        unsafe_allow_html=True,
                    )
                    if st.button("Inspect →", key=f"_fc_{nm}", help="View & annotate this field"):
                        st.session_state["ws_active_field"] = nm
                        st.rerun()

            st.markdown("---")
            b1, b2 = st.columns(2)
            if b1.button("▶  Build preview table", type="primary", key="_run_extract"):
                t.status = "extracting"; t.step = "extracting"
                save_thread(t)
                st.rerun()
            if b2.button("Clear & redesign", key="_schema_clear"):
                st.session_state["ws_ds"] = {}
                t.chat = []; save_thread(t); st.rerun()
        else:
            st.info("Designing schema from your documents… one moment.")

        # Chat history
        if t.chat:
            st.markdown("---")
            for msg in t.chat[-10:]:
                with st.chat_message(msg["role"]):
                    st.markdown(msg["content"])

        # Chat input + file upload side-by-side
        _schema_chat_area(t, ds)

    with right:
        _log_panel(t)
        _field_inspector(t, ds)


def _schema_chat_area(t: Thread, ds: dict) -> None:
    """Chat input plus optional file upload for schema refinement."""
    st.markdown("---")
    up_col, _ = st.columns([1, 3])
    with up_col:
        extra_file = st.file_uploader(
            "Attach JSON or PDF",
            type=["json", "pdf"],
            key=f"_schema_upload_{t.thread_id}",
            label_visibility="visible",
            help="Upload a JSON schema spec to replace/merge fields, or a PDF to add to context.",
        )

    if extra_file:
        sig = f"{extra_file.name}:{getattr(extra_file, 'size', 0)}"
        done_key = f"_schema_attach_sig_{t.thread_id}"
        if st.session_state.get(done_key) != sig:
            try:
                extra_file.seek(0)
            except Exception:
                pass
            if extra_file.name.lower().endswith(".json"):
                parsed = _parse_json_schema(extra_file.read())
                if parsed:
                    ds["proposed_columns"] = parsed
                    st.session_state["ws_ds"] = ds
                    st.session_state[done_key] = sig
                    t.add_chat(
                        "assistant",
                        f"Loaded schema spec from `{extra_file.name}` — "
                        f"{len(parsed)} fields applied.",
                    )
                    save_thread(t)
                    st.rerun()
                else:
                    st.error("Couldn't parse JSON schema.")
            elif extra_file.name.lower().endswith(".pdf"):
                dest = _UPLOADS_DIR / t.thread_id / extra_file.name
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(extra_file.read())
                st.session_state[done_key] = sig
                t.add_chat(
                    "assistant",
                    f"Saved `{extra_file.name}` for reference. "
                    f"Mention it in your chat if you want the model to consider it.",
                )
                save_thread(t)

    prompt = st.chat_input(
        "Refine schema — or describe changes… e.g. 'add a target year field', 'merge X and Y'",
        key="schema_chat",
    )
    if prompt:
        t.add_chat("user", prompt)
        save_thread(t)
        with st.spinner("Updating schema…"):
            _run_schema_node(t, ds, prompt=prompt)
        st.rerun()


def _run_schema_node(t: Thread, ds: dict, prompt: str) -> None:
    try:
        from tariff_agent.dataset_graph.schema_node import schema_node
        state = {
            **_corpus_overlay(t), **ds,
            "user_query":       ds.get("user_query") or prompt,
            "schema_feedback":  prompt if ds.get("proposed_columns") else "",
            "schema_iteration": ds.get("schema_iteration", 0),
            "schema_approved":  False,
            "use_sample": True, "sample_tickers": [], "extraction_mode": "direct",
        }
        state    = schema_node(state)
        new_cols = state.get("proposed_columns", [])
        name     = state.get("dataset_name", t.corpus_name)
        reply = (
            f"Proposed **{len(new_cols)} fields** for `{name}`:\n\n"
            + "\n".join(f"- **{c['name']}** `{c.get('type','')}` — {c.get('description','')[:80]}"
                        for c in new_cols)
            + "\n\n_Click **Build preview table** to extract, or keep refining._"
        )
        st.session_state["ws_ds"] = state
        t.add_chat("assistant", reply)
        t.add_log(f"<span class='log-info'>Schema: {len(new_cols)} fields for '{name}'</span>")
    except Exception as e:
        import traceback
        reply = f"Schema design failed: {e}"
        t.add_chat("assistant", reply)
        t.add_log(f"<span class='log-error'>{traceback.format_exc()[-300:]}</span>")
    save_thread(t)


# ── EXTRACTING ───────────────────────────────────────────────────────────────

def _render_extracting(t: Thread) -> None:
    ds: dict = st.session_state.get("ws_ds", {})
    main, right = st.columns([3, 1.1], gap="medium")
    with main:
        st.markdown(f"## {t.title}")
        st.info(f"Running extraction on trial batch ({t.trial_n} docs)…")
        with st.spinner(f"Extracting {len(ds.get('proposed_columns',[]))} fields…"):
            try:
                from tariff_agent.dataset_graph.extraction_node import extraction_node
                state = extraction_node({**_corpus_overlay(t), **ds})
                rows  = state.get("rows", [])
                st.session_state["ws_ds"] = state
                t.rows   = rows
                t.status = "preview"; t.step = "preview"
                t.add_log(f"<span class='log-info'>Extracted {len(rows)} rows from trial batch</span>")
            except Exception as e:
                t.status = "failed"; t.step = "failed"; t.error_msg = str(e)
                t.add_log(f"<span class='log-error'>{e}</span>")
        save_thread(t)
        st.rerun()
    with right:
        _log_panel(t)


# ── PREVIEW / DONE TABLE ─────────────────────────────────────────────────────

def _render_table(t: Thread) -> None:
    ds:   dict = st.session_state.get("ws_ds", {})
    cols: list = ds.get("proposed_columns", [])
    rows: list = t.rows or ds.get("rows", [])
    ann:  dict = st.session_state.get("ws_row_annotations", {})

    main, right = st.columns([3, 1.1], gap="medium")

    with main:
        # ── Header ──────────────────────────────────────────────────────────
        h1, h2, h3 = st.columns([3, 1.1, 1])
        is_full = t.step == "done"
        h1.markdown(
            f"<h2 style='margin:0'>{t.title}</h2>"
            f"<span style='font-size:0.88rem;color:var(--text-muted)'>"
            f"{'Full corpus' if is_full else 'Preview'} · "
            f"{len(rows)} rows · {len(cols)} fields</span>",
            unsafe_allow_html=True,
        )
        if t.step == "preview":
            if h2.button("Approve & run full corpus →", type="primary", key="_to_approve"):
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

        # ── Quality metrics ──────────────────────────────────────────────────
        if rows and cols:
            m1, m2, m3 = st.columns(3)
            filled   = sum(1 for r in rows if any(r.get(c["name"]) for c in cols))
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
            col_names = [c["name"] for c in cols]
            df        = pd.DataFrame(rows)
            disp      = [c for c in id_cols + col_names if c in df.columns]
            active_col = st.session_state.get("ws_active_field", "")
            st.markdown(
                _table_html(df[disp] if disp else df, active_col=active_col, annotations=ann),
                unsafe_allow_html=True,
            )
        else:
            st.info("No rows extracted — try refining the schema via chat below.")

        # ── Row annotation ───────────────────────────────────────────────────
        if rows:
            with st.expander("Annotate a row (feedback as prompt engineering)", expanded=False):
                st.markdown(
                    "<p style='color:var(--text-muted);margin-bottom:8px'>"
                    "Leave a note on any row — these become refinement prompts "
                    "and improve extraction quality.</p>",
                    unsafe_allow_html=True,
                )
                row_idx = st.number_input(
                    "Row number", min_value=0, max_value=len(rows)-1,
                    value=0, step=1, key="_ann_row_idx",
                )
                if rows:
                    preview_row = rows[row_idx]
                    id_val = next((str(preview_row.get(f, ""))
                                   for f in ["ticker","issuer_name","filing_id"] if preview_row.get(f)), f"Row {row_idx}")
                    st.markdown(
                        f"<div style='font-size:0.87rem;color:var(--text-muted);margin-bottom:6px'>"
                        f"Selected: <strong>{id_val}</strong></div>",
                        unsafe_allow_html=True,
                    )
                note_val = ann.get(str(row_idx), "")
                new_note = st.text_area(
                    "Note / feedback",
                    value=note_val, height=80,
                    placeholder="e.g. 'This row is missing Scope 2 — the value is on page 34 under emissions table'",
                    key=f"_ann_note_{row_idx}",
                    label_visibility="collapsed",
                )
                c1, c2 = st.columns(2)
                if c1.button("Save note", key="_save_ann"):
                    if new_note.strip():
                        ann[str(row_idx)] = new_note.strip()
                        st.session_state["ws_row_annotations"] = ann
                        st.success("Note saved — use 'Apply feedback' to refine extraction.")
                if ann and c2.button("Apply feedback to schema →", key="_apply_ann"):
                    feedback_block = "\n".join(
                        f"Row {i}: {note}" for i, note in ann.items()
                    )
                    combined = (
                        f"The following rows have feedback notes from the reviewer. "
                        f"Update extraction instructions to address these issues:\n\n{feedback_block}"
                    )
                    t.add_chat("user", combined)
                    save_thread(t)
                    with st.spinner("Applying feedback…"):
                        _run_schema_node(t, ds, prompt=combined)
                    t.status = "extracting"; t.step = "extracting"
                    save_thread(t); st.rerun()

        # ── Refinement chat ──────────────────────────────────────────────────
        st.markdown("---")
        if t.chat:
            for msg in t.chat[-6:]:
                with st.chat_message(msg["role"]):
                    st.markdown(msg["content"])

        chat_prompt = st.chat_input(
            "Refine — e.g. 'add a net-zero target year column', 'split X into Scope 1 and 2'…",
            key="table_chat",
        )
        if chat_prompt:
            t.add_chat("user", chat_prompt)
            save_thread(t)
            with st.spinner("Updating schema…"):
                _run_schema_node(t, ds, prompt=chat_prompt)
            t.status = "extracting"; t.step = "extracting"
            save_thread(t); st.rerun()

    with right:
        _log_panel(t)
        _field_inspector(t, ds)


# ── APPROVAL GATE ─────────────────────────────────────────────────────────────

def _render_approve(t: Thread) -> None:
    ds:   dict = st.session_state.get("ws_ds", {})
    cols: list = ds.get("proposed_columns", [])
    rows: list = t.rows or ds.get("rows", [])
    total = _total_corpus_docs(t)
    done  = t.trial_n

    main, right = st.columns([3, 1.1], gap="medium")
    with main:
        st.markdown(f"## Approve schema for full corpus")
        st.markdown(
            f"<p style='color:var(--text-muted)'>"
            f"You've reviewed the <strong>{done}-document preview</strong>. "
            f"Approving will run the full corpus of "
            f"<strong>{total} documents</strong> through the pipeline.</p>",
            unsafe_allow_html=True,
        )

        # Schema summary
        st.markdown("### Schema summary")
        if cols:
            summary_data = []
            for c in cols:
                filled = sum(1 for r in rows if r.get(c["name"]) not in (None, "", False))
                ev     = sum(1 for r in rows if r.get(f"{c['name']}_evidence_quote"))
                summary_data.append({
                    "Field": c["name"],
                    "Type": c.get("type", ""),
                    "Fill % (preview)": f"{round(100*filled/max(len(rows),1))}%",
                    "Evidence quotes": ev,
                })
            st.dataframe(
                pd.DataFrame(summary_data), use_container_width=True,
                hide_index=True, height=min(200, 40 + 35*len(summary_data)),
            )

        # Preview metrics
        m1, m2, m3 = st.columns(3)
        m1.metric("Schema fields", len(cols))
        m2.metric("Preview rows", len(rows))
        m3.metric("Full corpus", f"{total} docs")

        st.markdown("---")

        # Optional final notes before run
        st.markdown("### Any final notes before running? _(optional)_")
        pre_run_notes = st.text_area(
            "Notes",
            placeholder=(
                "Any last-minute instructions for the agent before it processes all documents.\n\n"
                "Examples:\n"
                "  •  'Prioritize table data over prose for all numeric fields'\n"
                "  •  'If a value spans multiple years, take the most recent'\n"
                "  •  'Ignore subsidiary filings — parent company only'"
            ),
            height=120,
            key="_approve_notes",
            label_visibility="collapsed",
        )

        st.markdown("---")
        c1, c2 = st.columns([2, 1])
        if c1.button(
            f"✓  Approve & run full corpus ({total} docs) →",
            type="primary", key="_final_approve", use_container_width=True,
        ):
            if pre_run_notes.strip():
                # Bake notes into schema instructions
                t.add_chat("user", pre_run_notes.strip())
                with st.spinner("Applying final notes to schema…"):
                    _run_schema_node(t, st.session_state.get("ws_ds", {}),
                                     prompt=pre_run_notes.strip())
            t.status = "full_ingesting"; t.step = "full_ingesting"
            save_thread(t)
            _launch_ingest(t, trial_n=0)
            st.rerun()

        if c2.button("← Back to preview", key="_back_from_approve"):
            t.status = "preview"; t.step = "preview"
            save_thread(t); st.rerun()

    with right:
        _log_panel(t)
        _field_inspector(t, ds)


# ── FULL RUN ─────────────────────────────────────────────────────────────────

def _render_full_run(t: Thread) -> None:
    ds: dict = st.session_state.get("ws_ds", {})
    main, right = st.columns([3, 1.1], gap="medium")
    with main:
        verb  = "Ingesting" if t.step == "full_ingesting" else "Extracting"
        total = _total_corpus_docs(t)
        st.markdown(f"## {t.title}")
        st.markdown(
            f"<p style='color:var(--text-muted)'>"
            f"<strong>{verb}</strong> all {total} documents. "
            f"This runs in the background — you can leave this tab open.</p>",
            unsafe_allow_html=True,
        )

        pct = _parse_progress_pct(t) if t.step == "full_ingesting" else 50
        st.progress(min(pct, 99) / 100, f"{verb} corpus…")

        if not st.session_state.get("ws_proc_done", True):
            _poll_proc(t)
            save_thread(t)
            time.sleep(1.5)
            st.rerun()
        elif t.step == "full_extracting":
            st.info("Ingestion complete. Running full extraction…")
            with st.spinner("Extracting all documents…"):
                try:
                    from tariff_agent.dataset_graph.extraction_node import extraction_node
                    state = extraction_node({**_corpus_overlay(t), **ds, "use_sample": False})
                    rows  = state.get("rows", [])
                    st.session_state["ws_ds"] = state
                    t.rows = rows; t.status = "done"; t.step = "done"
                    t.add_log(f"<span class='log-info'>Full extraction complete: {len(rows)} rows</span>")
                except Exception as e:
                    t.status = "failed"; t.step = "failed"; t.error_msg = str(e)
            save_thread(t); st.rerun()
        else:
            save_thread(t); st.rerun()
    with right:
        _log_panel(t)


# ── FAILED ───────────────────────────────────────────────────────────────────

def _render_failed(t: Thread) -> None:
    st.error(f"**Pipeline failed:** {t.error_msg or 'Unknown error — check the log.'}")
    ph = st.empty()
    _render_log(t.log, ph)
    c1, c2 = st.columns(2)
    if c1.button("↩  Back to schema", key="_fail_back"):
        t.status = "schema"; t.step = "schema"; t.error_msg = ""
        save_thread(t); st.rerun()
    if c2.button("🗑  Delete thread", key="_fail_del"):
        delete_thread(t.thread_id)
        st.session_state.pop("active_thread_id", None)
        st.rerun()


# ── Right-panel helpers ──────────────────────────────────────────────────────

def _log_panel(t: Thread) -> None:
    st.markdown(
        "<div style='font-size:0.76rem;font-weight:700;letter-spacing:0.09em;"
        "text-transform:uppercase;color:var(--text-muted);margin-bottom:6px'>Agent Log</div>",
        unsafe_allow_html=True,
    )
    _render_log(t.log, st.empty())


def _field_inspector(t: Thread, ds: dict) -> None:
    cols = ds.get("proposed_columns", [])
    if not cols:
        return

    st.markdown("<hr style='margin:12px 0'>", unsafe_allow_html=True)
    st.markdown(
        "<div style='font-size:0.76rem;font-weight:700;letter-spacing:0.09em;"
        "text-transform:uppercase;color:var(--text-muted);margin-bottom:8px'>Field Inspector</div>",
        unsafe_allow_html=True,
    )

    field_names = [c["name"] for c in cols]
    current     = st.session_state.get("ws_active_field")
    idx         = field_names.index(current) if current in field_names else 0
    chosen      = st.selectbox("Field", field_names, index=idx,
                               key="_field_sel", label_visibility="collapsed")
    if chosen != current:
        st.session_state["ws_active_field"] = chosen

    fc = next((c for c in cols if c.get("name") == chosen), {})
    st.markdown(
        f"<div class='field-card'>"
        f"<strong style='font-size:0.97rem'>{chosen}</strong> "
        f"<span style='font-size:0.78rem;color:var(--text-muted);background:var(--cream-mid);"
        f"padding:2px 8px;border-radius:99px'>{fc.get('type','')}</span>"
        f"<div style='font-size:0.86rem;color:var(--text-muted);margin-top:5px'>"
        f"{fc.get('description','')}</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    # Extraction instruction (read-only display)
    instr = fc.get("extraction_instruction", "")
    if instr:
        st.markdown(
            f"<div style='font-size:0.82rem;background:var(--cream-mid);border-radius:3px;"
            f"padding:7px 10px;color:var(--brown-mid);margin-bottom:6px'>"
            f"<strong>Instruction:</strong> {instr[:200]}</div>",
            unsafe_allow_html=True,
        )

    # Notes (editable)
    existing = t.field_notes.get(chosen, "")
    new_note = st.text_area(
        "Notes",
        value=existing, height=88,
        key=f"_note_{chosen}",
        placeholder="Add context, edge-cases, known issues, or instructions for this field…",
        label_visibility="visible",
    )
    if new_note != existing:
        t.field_notes[chosen] = new_note
        save_thread(t)

    # Evidence samples
    rows = t.rows or ds.get("rows", [])
    ev_col = f"{chosen}_evidence_quote"
    samples = [r for r in rows if r.get(ev_col)][:3]
    if samples:
        st.markdown(
            "<div style='font-size:0.74rem;font-weight:700;text-transform:uppercase;"
            "letter-spacing:0.07em;color:var(--text-muted);margin-top:10px;margin-bottom:4px'>"
            "Evidence samples</div>",
            unsafe_allow_html=True,
        )
        for r in samples:
            id_val = next((str(r.get(f,"")) for f in ["ticker","issuer_name"] if r.get(f)), "")
            st.markdown(
                f"<div style='font-size:0.83rem;background:#FFFCF6;border:1px solid var(--cream-dark);"
                f"border-radius:3px;padding:7px 10px;margin-top:4px;color:var(--brown-mid)'>"
                + (f"<strong>{id_val}</strong><br>" if id_val else "")
                + f"<em>\"{str(r[ev_col])[:220]}\"</em></div>",
                unsafe_allow_html=True,
            )


# ── Misc helpers ─────────────────────────────────────────────────────────────

def _parse_progress_pct(t: Thread) -> int:
    try:
        from tariff_agent.corpus.config import CorpusConfig
        yaml = _CONFIG_DIR / f"{t.corpus_id}.yaml"
        if not yaml.exists():
            return 5
        cfg       = CorpusConfig.from_yaml(yaml)
        parse_csv = cfg.resolve(cfg.parse_index_csv, ROOT)
        if not parse_csv.is_file():
            return 5
        df  = pd.read_csv(parse_csv)
        ok  = len(df[df["parse_status"].str.startswith("OK", na=False)])
        tot = max(t.trial_n, 1)
        return min(int(100 * ok / tot), 99)
    except Exception:
        return 5


# Backwards-compatible entry point
def page() -> None:
    render(active_thread_id=st.session_state.get("active_thread_id"))

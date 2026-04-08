"""Workspace — unified researcher experience.

Single-page flow:
  1. LOAD       → specify folder (or pick preset), scan PDFs, write index
  2. TRIAL_INGEST → parse + chunk N smallest PDFs (background subprocess, live log)
  3. SCHEMA     → chat-based schema design against trial chunks
  4. TRIAL_EXTRACT → run extraction on trial batch, review in frozen table
  5. APPROVE    → user approves schema
  6. FULL_INGEST → parse + chunk remaining PDFs (background, live log)
  7. FULL_EXTRACT → extraction on full corpus
  8. EXPORT     → download CSV

Left   = step stepper + corpus status
Center = active work area (varies by step)
Right  = live agent log (always)
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from queue import Empty, Queue
from threading import Thread

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tariff_agent.corpus.config import CorpusConfig, _slugify
from tariff_agent.corpus.paths import normalize_host_path
from tariff_agent.corpus.scan_index import write_corpus_index

_CONFIG_DIR = ROOT / "output" / "corpus_configs"
_CONFIG_DIR.mkdir(parents=True, exist_ok=True)

# ── Step definitions ────────────────────────────────────────────────────────
STEPS = [
    ("load",          "1  Load PDFs"),
    ("trial_ingest",  "2  Trial ingest"),
    ("schema",        "3  Design schema"),
    ("trial_extract", "4  Trial extraction"),
    ("approve",       "5  Approve schema"),
    ("full_ingest",   "6  Full ingest"),
    ("full_extract",  "7  Full extraction"),
    ("export",        "8  Export"),
]
STEP_KEYS = [k for k, _ in STEPS]


def _step_idx(key: str) -> int:
    try:
        return STEP_KEYS.index(key)
    except ValueError:
        return 0


# ── Subprocess helpers ───────────────────────────────────────────────────────

def _run_pipeline_cmd(cfg: CorpusConfig, stage: str, trial_n: int = 0, no_skip: bool = False) -> list[str]:
    script = str(ROOT / "scripts" / "run_corpus_pipeline.py")
    yaml_path = _CONFIG_DIR / f"{cfg.corpus_id}.yaml"
    base = [sys.executable, script]
    if yaml_path.exists():
        base += ["--config", str(yaml_path)]
    else:
        base += ["--corpus", cfg.corpus_id]
    base += ["--stage", stage]
    if trial_n > 0:
        base += ["--trial-n", str(trial_n)]
    if no_skip:
        base.append("--no-skip")
    return base


def _start_subprocess(cmd: list[str]) -> subprocess.Popen:
    return subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        cwd=str(ROOT),
    )


def _drain_proc(proc: subprocess.Popen, q: Queue) -> None:
    """Background thread: push lines from proc.stdout into q, then sentinel None."""
    assert proc.stdout
    for line in proc.stdout:
        q.put(line.rstrip())
    proc.wait()
    q.put(None)  # sentinel


# ── Log rendering ────────────────────────────────────────────────────────────

def _log_class(line: str) -> str:
    lo = line.lower()
    if any(x in lo for x in ("error", "traceback", "exit -", "exit 1")):
        return "log-error"
    if any(x in lo for x in ("warning", "warn", "skip")):
        return "log-warn"
    if any(x in lo for x in ("done", "ok", "complete", "success", "✓")):
        return "log-info"
    return "log-dim"


def _render_log(lines: list[str], placeholder) -> None:
    if not lines:
        body = "<span class='log-dim'>Waiting for output…</span>"
    else:
        body = "\n".join(
            f"<span class='{_log_class(l)}'>{l}</span>"
            for l in lines[-160:]
        )
    placeholder.markdown(
        f"<div class='agent-log' id='agent-log-bottom'>{body}</div>"
        "<script>var el=document.getElementById('agent-log-bottom');if(el)el.scrollTop=el.scrollHeight;</script>",
        unsafe_allow_html=True,
    )


# ── Frozen-header table ──────────────────────────────────────────────────────

def _table_html(df: pd.DataFrame, max_rows: int = 300) -> str:
    heads = "".join(f"<th>{c}</th>" for c in df.columns)

    def _cell(v):
        sv = "" if v is None else str(v)
        if sv in ("", "nan", "None"):
            return "<td class='null'>—</td>"
        if sv.lower() == "true":
            return "<td class='bool-t'>✓</td>"
        if sv.lower() == "false":
            return "<td class='bool-f'>✗</td>"
        return f"<td title='{sv[:200]}'>{sv[:72]}</td>"

    body = "".join(
        f"<tr>{''.join(_cell(row[c]) for c in df.columns)}</tr>"
        for _, row in df.head(max_rows).iterrows()
    )

    return f"""
<div class='frozen-table-wrap'>
<table class='frozen-table'>
  <thead><tr>{heads}</tr></thead>
  <tbody>{body}</tbody>
</table>
</div>
<style>
.frozen-table-wrap{{overflow:auto;max-height:400px;border:1px solid var(--cream-dark);
  border-radius:4px;background:#FFFCF6;margin-top:8px}}
.frozen-table{{border-collapse:collapse;width:max-content;min-width:100%;
  font-size:0.76rem;font-family:var(--font)}}
.frozen-table thead{{position:sticky;top:0;z-index:10;background:var(--cream-mid)}}
.frozen-table th{{padding:6px 12px;text-align:left;font-weight:600;font-size:0.72rem;
  text-transform:uppercase;letter-spacing:0.06em;color:var(--brown-mid);
  border-bottom:1px solid var(--cream-dark);white-space:nowrap}}
.frozen-table td{{padding:5px 12px;border-bottom:1px solid #EDE7D9;color:var(--brown);
  white-space:nowrap;max-width:220px;overflow:hidden;text-overflow:ellipsis}}
.frozen-table tr:hover td{{background:#FAF6EE}}
.frozen-table .null{{color:#C8BBA8}}
.frozen-table .bool-t{{color:#5A7A4A;font-weight:600}}
.frozen-table .bool-f{{color:#9E5A5A;font-weight:600}}
</style>"""


# ── Session init ────────────────────────────────────────────────────────────

def _init_session():
    defaults = {
        "ws_step":       "load",
        "ws_cfg":        None,
        "ws_log":        [],
        "ws_proc_done":  True,
        "ws_proc_rc":    0,
        "ws_ds":         {},
        "ws_ds_phase":   "query",
        "ws_run_id":     None,
        "ws_trial_n":    8,
        "ws_chat":       [],
        "ws_field_chat": [],
        "ws_active_field": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


# ── Stepper sidebar ──────────────────────────────────────────────────────────

def _stepper(current_step: str) -> None:
    cur_idx = _step_idx(current_step)
    st.markdown(
        "<div style='font-size:0.72rem;font-weight:600;letter-spacing:0.1em;"
        "text-transform:uppercase;color:var(--text-muted);margin-bottom:10px'>Workflow</div>",
        unsafe_allow_html=True,
    )
    for i, (key, label) in enumerate(STEPS):
        if i < cur_idx:
            color = "#9DC8A0"; prefix = "✓ "
        elif i == cur_idx:
            color = "var(--cream)"; prefix = "▶ "
        else:
            color = "#7A6652"; prefix = "  "
        st.markdown(
            f"<div style='font-size:0.82rem;color:{color};padding:3px 0;white-space:nowrap'>"
            f"{prefix}{label}</div>",
            unsafe_allow_html=True,
        )

    cfg: CorpusConfig | None = st.session_state.ws_cfg
    if cfg:
        st.markdown("---")
        st.markdown(
            f"<div style='font-size:0.72rem;color:#9DC8A0;font-weight:600;margin-bottom:4px'>Corpus</div>"
            f"<div style='font-size:0.78rem;color:var(--cream);word-break:break-word'>{cfg.name}</div>"
            f"<div style='font-size:0.7rem;color:#7A6652;margin-top:2px'>{cfg.corpus_id}</div>",
            unsafe_allow_html=True,
        )
        try:
            from tariff_agent.corpus.ingest import corpus_status
            st = st  # avoid shadowing
            status = corpus_status(cfg, ROOT)
            rows_data = [
                ("Index",   status["index_exists"],      str(status["n_documents"])),
                ("Chunks",  status["chunks_exist"],      f"{status['n_chunks']:,}" if status["n_chunks"] else "—"),
                ("Pass-1",  status["llm_chunks_exist"],  f"{status['n_llm_chunks']:,}" if status["n_llm_chunks"] else "—"),
            ]
            for lbl, ok, val in rows_data:
                dot = "#9DC8A0" if ok else "#7A6652"
                st.markdown(
                    f"<div style='font-size:0.74rem;color:var(--cream);display:flex;gap:6px;align-items:center'>"
                    f"<span style='color:{dot}'>●</span>{lbl}: {val}</div>",
                    unsafe_allow_html=True,
                )
        except Exception:
            pass


# ── Main page ────────────────────────────────────────────────────────────────

def page() -> None:
    _init_session()

    step:    str                  = st.session_state.ws_step
    cfg:     CorpusConfig | None  = st.session_state.ws_cfg
    log:     list[str]            = st.session_state.ws_log
    ds:      dict                 = st.session_state.ws_ds
    trial_n: int                  = st.session_state.ws_trial_n

    # ── Page header ──────────────────────────────────────────────────────────
    st.markdown(
        "<h1 style='margin-bottom:2px'>Workspace</h1>"
        "<p style='color:var(--text-muted);font-size:0.82rem;margin-bottom:1rem'>"
        "Load a corpus of PDFs → design a schema through conversation → extract a structured dataset.</p>",
        unsafe_allow_html=True,
    )

    # ── Three-panel layout ───────────────────────────────────────────────────
    left, center, right = st.columns([0.85, 2.4, 1.1], gap="medium")

    with left:
        _stepper(step)
        st.markdown("---")
        if st.button("↺ Restart", key="ws_restart", use_container_width=True):
            for k in list(st.session_state.keys()):
                if k.startswith("ws_"):
                    del st.session_state[k]
            st.rerun()

    with right:
        st.markdown(
            "<div style='font-size:0.74rem;font-weight:600;letter-spacing:0.08em;"
            "text-transform:uppercase;color:var(--text-muted);margin-bottom:6px'>Agent log</div>",
            unsafe_allow_html=True,
        )
        log_placeholder = st.empty()
        _render_log(log, log_placeholder)

        # Field chat (when a field is active)
        if step in ("schema", "trial_extract", "approve") and st.session_state.ws_active_field:
            st.markdown("---")
            fname = st.session_state.ws_active_field
            st.markdown(
                f"<div style='font-size:0.74rem;font-weight:600;letter-spacing:0.07em;"
                f"text-transform:uppercase;color:var(--text-muted);margin-bottom:4px'>"
                f"Chat — {fname}</div>",
                unsafe_allow_html=True,
            )
            for msg in st.session_state.ws_field_chat[-8:]:
                with st.chat_message(msg["role"]):
                    st.markdown(msg["content"])
            fc_prompt = st.chat_input(f"Refine {fname}…", key="ws_field_chat_input")
            if fc_prompt and cfg:
                st.session_state.ws_field_chat.append({"role": "user", "content": fc_prompt})
                cols = ds.get("proposed_columns", [])
                fmeta = next((c for c in cols if c.get("name") == fname), {})
                with st.spinner("Thinking…"):
                    try:
                        from tariff_agent.dataset_graph.schema_node import schema_node
                        state = {
                            **_corpus_overlay(cfg),
                            **ds,
                            "schema_feedback": (
                                f"Regarding only the field `{fname}` "
                                f"(type: {fmeta.get('type')}, instruction: '{fmeta.get('extraction_instruction','')}': "
                                f"{fc_prompt}"
                            ),
                            "schema_approved": False,
                        }
                        updated_state = schema_node(state)
                        updated_col = next(
                            (c for c in updated_state.get("proposed_columns", []) if c.get("name") == fname),
                            None,
                        )
                        if updated_col:
                            new_cols = [updated_col if c.get("name") == fname else c for c in cols]
                            ds["proposed_columns"] = new_cols
                            st.session_state.ws_ds = ds
                            reply = (
                                f"Updated `{fname}`:\n"
                                f"- Instruction: _{updated_col.get('extraction_instruction','')}_\n\n"
                                "Changes reflected in schema editor."
                            )
                        else:
                            reply = updated_state.get("error", "No change proposed.")
                    except Exception as e:
                        reply = f"Error: {e}"
                st.session_state.ws_field_chat.append({"role": "assistant", "content": reply})
                st.rerun()

    # ── STEP: LOAD ───────────────────────────────────────────────────────────
    with center:

        if step == "load":
            st.markdown("### Load your PDFs")
            st.caption(
                "Paste the path to a folder of PDF files. We'll scan it recursively, "
                "deduplicate by content, and prepare an index. You can also pick a preset corpus."
            )
            st.markdown("---")

            # Quick presets
            presets = {
                "—":                     None,
                "SEDAR Tariff 2023–2025": "sedar_tariff",
                "TSX ESG 2023":          "tsx_esg_2023",
                "TSX ESG 2024":          "tsx_esg_2024",
                "SEDAR prateek root":    "sedar_prateek_filings",
            }
            preset_sel = st.selectbox("Quick-load preset corpus", list(presets.keys()), key="ws_preset_sel")
            if preset_sel != "—":
                preset_key = presets[preset_sel]
                preset_cfg = _load_preset(preset_key, ROOT)
                if preset_cfg and st.button("Load preset →", key="ws_load_preset"):
                    preset_cfg.to_yaml(_CONFIG_DIR / f"{preset_cfg.corpus_id}.yaml")
                    st.session_state.ws_cfg = preset_cfg
                    st.session_state.ws_step = "trial_ingest"
                    st.session_state.ws_log = [f"<span class='log-info'>Loaded preset: {preset_cfg.name}</span>"]
                    st.rerun()

            st.markdown("**— or —**")

            # Custom folder
            c1, c2 = st.columns(2)
            docs_dir_raw = c1.text_input(
                "PDF folder path",
                placeholder=r"C:\Users\…  or  /mnt/c/…",
                key="ws_docs_dir",
            )
            corpus_name_inp = c2.text_input(
                "Dataset name",
                placeholder="My ESG Corpus 2024",
                key="ws_corpus_name",
            )
            topic_inp = st.text_area(
                "What will you extract from these documents?",
                placeholder=(
                    "Describe in plain language what you want to find — this guides schema design.\n\n"
                    "Examples:\n"
                    "  • Scope 1, 2, 3 GHG emissions with year and reduction targets\n"
                    "  • Steel and aluminium tariff cost impacts with dollar amounts\n"
                    "  • Board gender diversity, committee structure, ESG KPIs"
                ),
                height=130,
                key="ws_topic_inp",
            )
            trial_n_inp = st.slider(
                "Trial batch size (PDFs for schema design)",
                min_value=3, max_value=30, value=trial_n, step=1,
                key="ws_trial_n_slider",
            )
            st.session_state.ws_trial_n = trial_n_inp

            load_btn = st.button(
                "Scan folder & begin →",
                type="primary",
                disabled=not (docs_dir_raw.strip() and corpus_name_inp.strip()),
                key="ws_load_btn",
            )
            if load_btn:
                norm = normalize_host_path(docs_dir_raw.strip())
                cid = _slugify(corpus_name_inp.strip())
                index_path = ROOT / "data" / "metadata" / f"corpus_{cid}_index.csv"
                with st.spinner(f"Scanning {norm} …"):
                    try:
                        n = write_corpus_index(norm, index_path)
                    except Exception as e:
                        st.error(str(e))
                        n = 0
                if n == 0:
                    st.warning("No PDFs found at that path. Check the folder and try again.")
                else:
                    rel = str(index_path.relative_to(ROOT))
                    new_cfg = CorpusConfig(
                        name=corpus_name_inp.strip(),
                        corpus_id=cid,
                        topic=(topic_inp.strip() or corpus_name_inp.strip()),
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
                    new_cfg.to_yaml(_CONFIG_DIR / f"{cid}.yaml")
                    st.session_state.ws_cfg = new_cfg
                    st.session_state.ws_step = "trial_ingest"
                    st.session_state.ws_log = [
                        f"<span class='log-info'>Indexed {n} PDFs → {rel}</span>",
                        f"<span class='log-dim'>Corpus: {cid}  |  Trial batch: {trial_n_inp}</span>",
                    ]
                    st.rerun()

        # ── STEP: TRIAL INGEST ────────────────────────────────────────────────
        elif step == "trial_ingest":
            assert cfg is not None
            st.markdown(
                f"### Trial ingest — {cfg.name}",
            )
            st.caption(
                f"Parsing and chunking the {trial_n} smallest PDFs so you can design "
                "your schema against real content. Remaining documents will be processed "
                "after you approve the schema."
            )

            # Show trial-index info
            idx_path = _resolve_index(cfg)
            total_docs = 0
            if idx_path and idx_path.is_file():
                total_docs = len(pd.read_csv(idx_path))
                st.markdown(
                    f"<div style='font-size:0.82rem;color:var(--text-muted)'>Total PDFs in corpus: "
                    f"<strong>{total_docs}</strong> &nbsp;|&nbsp; Trial batch: <strong>{trial_n}</strong></div>",
                    unsafe_allow_html=True,
                )

            proc_done = st.session_state.ws_proc_done
            proc_rc   = st.session_state.ws_proc_rc

            if proc_done and proc_rc == 0 and _trial_chunks_ready(cfg, trial_n):
                st.success(f"Trial batch parsed and chunked. Ready to design schema.")
                if st.button("Continue → Design schema", type="primary", key="ws_to_schema"):
                    st.session_state.ws_step = "schema"
                    st.session_state.ws_ds = {}
                    st.session_state.ws_ds_phase = "query"
                    st.session_state.ws_chat = []
                    st.rerun()
            elif proc_done and proc_rc != 0:
                st.error(f"Ingestion process exited with code {proc_rc}. Check the agent log →")
                if st.button("Retry", key="ws_retry_trial"):
                    st.session_state.ws_proc_done = True
                    st.session_state.ws_proc_rc   = 0
                    _launch_trial_ingest(cfg, trial_n)
            elif proc_done:
                # Not started yet
                if st.button("Start trial ingest ▶", type="primary", key="ws_start_trial"):
                    _launch_trial_ingest(cfg, trial_n)
                    st.rerun()
            else:
                # Running — drain subprocess and stream to log
                q: Queue = st.session_state.get("ws_queue")
                if q is not None:
                    new_lines: list[str] = []
                    try:
                        while True:
                            item = q.get_nowait()
                            if item is None:
                                proc = st.session_state.get("ws_proc")
                                rc = proc.returncode if proc else 0
                                st.session_state.ws_proc_done = True
                                st.session_state.ws_proc_rc   = rc
                                break
                            new_lines.append(item)
                    except Empty:
                        pass
                    if new_lines:
                        st.session_state.ws_log.extend(
                            f"<span class='{_log_class(l)}'>{l}</span>" for l in new_lines
                        )
                    _render_log(st.session_state.ws_log, log_placeholder)
                    st.markdown(
                        "<div style='font-size:0.82rem;color:var(--text-muted)'>Ingesting trial batch…</div>",
                        unsafe_allow_html=True,
                    )
                    time.sleep(0.8)
                    st.rerun()

        # ── STEP: SCHEMA DESIGN ───────────────────────────────────────────────
        elif step == "schema":
            assert cfg is not None
            st.markdown("### Design your extraction schema")
            st.caption(
                "Describe what you want to extract. The agent reads the trial documents and proposes fields. "
                "Refine through chat until you're satisfied, then run a trial extraction to preview results."
            )

            cols = ds.get("proposed_columns", [])
            phase = st.session_state.ws_ds_phase

            # ── Chat history above query ──────────────────────────────────────
            if st.session_state.ws_chat:
                st.markdown("---")
                for msg in st.session_state.ws_chat:
                    with st.chat_message(msg["role"]):
                        st.markdown(msg["content"])
                st.markdown("---")

            # ── Schema editor if we have columns ─────────────────────────────
            if cols:
                st.markdown(
                    f"<div style='font-size:0.84rem;font-weight:600;margin-bottom:4px'>"
                    f"Schema — <code>{ds.get('dataset_name', 'untitled')}</code>  "
                    f"<span style='font-weight:400;color:var(--text-muted);font-size:0.78rem'>"
                    f"({len(cols)} fields, iteration {ds.get('schema_iteration',1)})</span></div>",
                    unsafe_allow_html=True,
                )

                # Compact field cards with click-to-select
                field_col1, field_col2 = st.columns(2)
                col_halves = [cols[:len(cols)//2+len(cols)%2], cols[len(cols)//2+len(cols)%2:]]
                for ci, half in enumerate(col_halves):
                    tgt = field_col1 if ci == 0 else field_col2
                    with tgt:
                        for fc in half:
                            nm = fc.get("name", "")
                            t  = fc.get("type", "string")
                            desc = fc.get("description", "")[:80]
                            is_active = st.session_state.ws_active_field == nm
                            btn_label = f"{'▶ ' if is_active else ''}{nm}"
                            if st.button(btn_label, key=f"ws_fc_{nm}", use_container_width=True, help=desc):
                                st.session_state.ws_active_field = nm
                                st.session_state.ws_field_chat = []
                                st.rerun()
                            st.markdown(
                                f"<div style='font-size:0.72rem;color:var(--text-muted);margin:-4px 0 6px 2px'>"
                                f"<code style='font-size:0.7rem'>{t}</code> · {desc}</div>",
                                unsafe_allow_html=True,
                            )

                st.markdown("---")
                b1, b2 = st.columns(2)
                if b1.button("▶ Trial extraction →", type="primary", key="ws_run_trial_extract"):
                    from tariff_agent.dataset_graph.feedback_store import log_schema_iteration
                    log_schema_iteration(
                        _run_id(), iteration=ds.get("schema_iteration", 1),
                        dataset_name=ds.get("dataset_name", ""), user_query=ds.get("user_query", ""),
                        proposed_columns=cols, user_feedback="", approved=True,
                    )
                    st.session_state.ws_ds = {**_corpus_overlay(cfg), **ds,
                        "schema_approved": True, "use_sample": True,
                        "sample_tickers": [], "extraction_mode": "direct"}
                    st.session_state.ws_step = "trial_extract"
                    st.rerun()
                if b2.button("Start over", key="ws_schema_reset"):
                    st.session_state.ws_ds = {}
                    st.session_state.ws_chat = []
                    st.session_state.ws_ds_phase = "query"
                    st.rerun()

            # ── Chat input ────────────────────────────────────────────────────
            placeholder = (
                f"Describe what to extract…\n\n"
                f"Corpus: {cfg.topic[:80]}\n\n"
                "e.g. 'Extract Scope 1, 2, 3 emissions with targets and baseline year'"
                if not cols else
                "Refine the schema — e.g. 'add a field for emission reduction target year', 'split X into two fields'…"
            )
            prompt = st.chat_input(placeholder, key="ws_schema_chat")
            if prompt:
                st.session_state.ws_chat.append({"role": "user", "content": prompt})
                with st.spinner("Designing schema from trial documents…"):
                    try:
                        from tariff_agent.dataset_graph.schema_node import schema_node
                        overlay = _corpus_overlay(cfg)
                        state = {
                            **overlay,
                            **ds,
                            "user_query": ds.get("user_query") or prompt,
                            "schema_feedback": prompt if cols else "",
                            "schema_iteration": ds.get("schema_iteration", 0),
                            "schema_approved": False,
                            "use_sample": True,
                            "sample_tickers": [],
                            "extraction_mode": "direct",
                        }
                        state = schema_node(state)
                        new_cols = state.get("proposed_columns", [])
                        dataset_name = state.get("dataset_name", "")
                        reply = (
                            f"Proposed **{len(new_cols)} fields** for dataset `{dataset_name}`:\n\n"
                            + "\n".join(
                                f"- **{c['name']}** `{c.get('type','')}`  — {c.get('description','')[:80]}"
                                for c in new_cols
                            )
                            + "\n\nYou can edit any field above, then click **Trial extraction** "
                            "or refine further via chat."
                        )
                        st.session_state.ws_ds = state
                        st.session_state.ws_log.append(
                            f"<span class='log-info'>Schema: {len(new_cols)} fields for '{dataset_name}'</span>"
                        )
                    except Exception as e:
                        reply = f"Schema design failed: {e}"
                        st.session_state.ws_log.append(f"<span class='log-error'>{e}</span>")
                st.session_state.ws_chat.append({"role": "assistant", "content": reply})
                st.rerun()

        # ── STEP: TRIAL EXTRACT ───────────────────────────────────────────────
        elif step == "trial_extract":
            assert cfg is not None
            cols = ds.get("proposed_columns", [])

            if not ds.get("rows"):
                # First visit — run extraction
                st.info(f"Running extraction on trial batch ({trial_n} docs)…")
                with st.spinner("Extracting…"):
                    from tariff_agent.dataset_graph.extraction_node import extraction_node
                    t0 = time.time()
                    state = extraction_node({**_corpus_overlay(cfg), **ds})
                    elapsed = time.time() - t0
                if state.get("error"):
                    st.error(state["error"])
                    if st.button("← Back to schema", key="ws_back_schema"):
                        st.session_state.ws_step = "schema"
                        st.rerun()
                else:
                    st.session_state.ws_ds = state
                    st.session_state.ws_log.append(
                        f"<span class='log-info'>Trial extraction: {len(state.get('rows',[]))} rows in {elapsed:.1f}s</span>"
                    )
                    st.rerun()
            else:
                rows = ds.get("rows", [])
                st.markdown(f"### Trial results — {len(rows)} rows")

                # Fill-rate summary
                fill_rows = []
                for fc in cols:
                    nm = fc["name"]
                    filled = sum(1 for r in rows if r.get(nm) not in (None, "", False, 0))
                    ev_n   = sum(1 for r in rows if r.get(f"{nm}_evidence_quote"))
                    fill_rows.append({"field": nm, "fill %": round(100*filled/max(len(rows),1),1), "evidence": ev_n})
                st.dataframe(pd.DataFrame(fill_rows), use_container_width=True, height=160, hide_index=True)

                # Frozen results table
                id_cols = list(cfg.identity_fields)
                col_names = [c["name"] for c in cols]
                display_cols = [c for c in id_cols + col_names if c in rows[0]] if rows else []
                df_rows = pd.DataFrame(rows)
                if display_cols:
                    st.markdown(_table_html(df_rows[display_cols]), unsafe_allow_html=True)

                # Evidence quick view
                with st.expander("Evidence quotes (first 3 rows)", expanded=False):
                    for row in rows[:3]:
                        ticker = next((row.get(f) for f in id_cols if row.get(f)), "?")
                        st.markdown(f"**{ticker}**")
                        for fc in cols[:6]:
                            nm = fc["name"]
                            ev_q = row.get(f"{nm}_evidence_quote")
                            val  = row.get(nm)
                            if ev_q:
                                pg = row.get(f"{nm}_evidence_pages", "?")
                                st.markdown(f"  - **{nm}**: `{val}` &nbsp; _pp.{pg}: \"{str(ev_q)[:200]}\"_")

                st.markdown("---")
                b1, b2, b3 = st.columns(3)
                if b1.button("✓ Approve — full corpus →", type="primary", key="ws_approve_btn"):
                    st.session_state.ws_step = "approve"
                    st.rerun()
                if b2.button("↩ Refine schema", key="ws_refine_btn"):
                    st.session_state.ws_step = "schema"
                    st.session_state.ws_ds = {**ds, "rows": [], "schema_approved": False}
                    st.rerun()
                if b3.button("⟳ Re-run trial", key="ws_rerun_trial"):
                    st.session_state.ws_ds = {**ds, "rows": []}
                    st.rerun()

        # ── STEP: APPROVE ─────────────────────────────────────────────────────
        elif step == "approve":
            assert cfg is not None
            cols = ds.get("proposed_columns", [])
            rows = ds.get("rows", [])
            st.markdown("### Approve schema & launch full ingestion")
            st.caption(
                "You're about to process all documents in the corpus. "
                "Full ingestion (Docling parse + chunk + Pass-1 LLM) runs in the background."
            )

            idx_path = _resolve_index(cfg)
            total_docs = len(pd.read_csv(idx_path)) if idx_path and idx_path.is_file() else 0
            trial_done = trial_n

            st.markdown(
                f"<div style='background:#FFFCF6;border:1px solid var(--cream-dark);border-radius:4px;"
                f"padding:14px 18px;margin-bottom:16px'>"
                f"<div style='font-size:0.82rem;font-weight:600;margin-bottom:8px'>Summary</div>"
                f"<div style='font-size:0.8rem;color:var(--text-muted)'>"
                f"Schema: <strong>{len(cols)} fields</strong> &nbsp;|&nbsp; "
                f"Trial: <strong>{trial_done} docs, {len(rows)} rows</strong> &nbsp;|&nbsp; "
                f"Full corpus: <strong>{total_docs} docs</strong>"
                f"</div></div>",
                unsafe_allow_html=True,
            )

            if st.button(
                f"✓ Approve schema & start full ingest ({total_docs - trial_done} remaining PDFs) →",
                type="primary",
                key="ws_final_approve",
            ):
                st.session_state.ws_step = "full_ingest"
                st.session_state.ws_proc_done = False
                _launch_full_ingest(cfg)
                st.rerun()

            if st.button("← Back and revise", key="ws_back_from_approve"):
                st.session_state.ws_step = "schema"
                st.session_state.ws_ds = {**ds, "rows": [], "schema_approved": False}
                st.rerun()

        # ── STEP: FULL INGEST ─────────────────────────────────────────────────
        elif step == "full_ingest":
            assert cfg is not None
            st.markdown("### Full corpus ingestion")
            st.caption("Parsing and chunking all documents. This can take a while — the log on the right shows progress.")

            proc_done = st.session_state.ws_proc_done
            proc_rc   = st.session_state.ws_proc_rc

            if proc_done and proc_rc == 0:
                st.success("Full ingestion complete.")
                if st.button("Continue → Full extraction →", type="primary", key="ws_to_full_extract"):
                    st.session_state.ws_step = "full_extract"
                    st.session_state.ws_ds = {
                        **_corpus_overlay(cfg),
                        **ds,
                        "rows": [],
                        "use_sample": False,
                        "schema_approved": True,
                    }
                    st.rerun()
            elif proc_done and proc_rc != 0:
                st.error(f"Ingestion exited with code {proc_rc}. Check the log.")
                if st.button("Retry", key="ws_retry_full"):
                    _launch_full_ingest(cfg)
                    st.rerun()
            else:
                q: Queue = st.session_state.get("ws_queue")
                if q is not None:
                    new_lines = []
                    try:
                        while True:
                            item = q.get_nowait()
                            if item is None:
                                proc = st.session_state.get("ws_proc")
                                rc = proc.returncode if proc else 0
                                st.session_state.ws_proc_done = True
                                st.session_state.ws_proc_rc   = rc
                                break
                            new_lines.append(item)
                    except Empty:
                        pass
                    if new_lines:
                        st.session_state.ws_log.extend(
                            f"<span class='{_log_class(l)}'>{l}</span>" for l in new_lines
                        )
                    _render_log(st.session_state.ws_log, log_placeholder)
                    st.markdown(
                        "<div style='font-size:0.82rem;color:var(--text-muted)'>Full ingestion running…</div>",
                        unsafe_allow_html=True,
                    )
                    time.sleep(0.8)
                    st.rerun()

        # ── STEP: FULL EXTRACT ────────────────────────────────────────────────
        elif step == "full_extract":
            assert cfg is not None
            cols = ds.get("proposed_columns", [])
            rows = ds.get("rows", [])

            if not rows:
                st.info(f"Running extraction on all documents ({len(cols)} fields)…")
                prog = st.progress(0, "Starting…")
                with st.spinner("Extracting full corpus…"):
                    from tariff_agent.dataset_graph.extraction_node import extraction_node
                    t0 = time.time()
                    state = extraction_node({**_corpus_overlay(cfg), **ds})
                    elapsed = time.time() - t0
                prog.progress(100)
                if state.get("error"):
                    st.error(state["error"])
                else:
                    st.session_state.ws_ds = state
                    st.session_state.ws_log.append(
                        f"<span class='log-info'>Full extraction: {len(state.get('rows',[]))} rows in {elapsed:.0f}s</span>"
                    )
                    st.rerun()
            else:
                st.markdown(f"### Full results — {len(rows)} rows")
                no_ev = sum(1 for r in rows if str(r.get("_pass1_positive", 0)) == "0")
                m1, m2, m3 = st.columns(3)
                m1.metric("Rows", len(rows))
                m2.metric("With evidence", len(rows) - no_ev)
                m3.metric("Negative", no_ev)

                id_cols   = list(cfg.identity_fields)
                col_names = [c["name"] for c in cols]
                df_rows   = pd.DataFrame(rows)
                disp_cols = [c for c in id_cols + col_names if c in df_rows.columns]
                st.markdown(_table_html(df_rows[disp_cols] if disp_cols else df_rows), unsafe_allow_html=True)

                st.markdown("---")
                if st.button("💾 Export CSV →", type="primary", key="ws_export_btn"):
                    st.session_state.ws_step = "export"
                    st.rerun()

        # ── STEP: EXPORT ──────────────────────────────────────────────────────
        elif step == "export":
            assert cfg is not None
            st.markdown("### Export")

            if not ds.get("dataset_path"):
                with st.spinner("Exporting…"):
                    from tariff_agent.dataset_graph.graph import export_node
                    state = export_node({**_corpus_overlay(cfg), **ds, "export_approved": True})
                if state.get("error"):
                    st.error(state["error"])
                else:
                    st.session_state.ws_ds = state

            path = st.session_state.ws_ds.get("dataset_path", "")
            if path and Path(path).exists():
                df_out = pd.read_csv(path)
                st.success(f"Dataset saved: `{path}`")
                st.markdown(_table_html(df_out), unsafe_allow_html=True)
                st.download_button(
                    "⬇ Download CSV",
                    data=df_out.to_csv(index=False).encode(),
                    file_name=Path(path).name,
                    mime="text/csv",
                )
                st.session_state.ws_log.append(
                    f"<span class='log-info'>Exported: {Path(path).name}  ({len(df_out)} rows)</span>"
                )
            if st.button("Build another dataset from this corpus", key="ws_again"):
                st.session_state.ws_step = "schema"
                st.session_state.ws_ds = {}
                st.session_state.ws_chat = []
                st.rerun()


# ── Helpers ──────────────────────────────────────────────────────────────────

def _run_id() -> str:
    if not st.session_state.ws_run_id:
        from tariff_agent.dataset_graph.feedback_store import new_run_id
        st.session_state.ws_run_id = new_run_id()
    return st.session_state.ws_run_id


def _load_preset(key: str | None, root: Path) -> CorpusConfig | None:
    if not key:
        return None
    m = {
        "sedar_tariff":          lambda: CorpusConfig.sedar_default(root),
        "sedar_prateek_filings": lambda: CorpusConfig.sedar_prateek_filings(root),
        "tsx_esg_2023":          lambda: CorpusConfig.tsx_esg_2023(root),
        "tsx_esg_2024":          lambda: CorpusConfig.tsx_esg_2024(root),
        "pdf_agents_esg":        lambda: CorpusConfig.pdf_agents_esg_default(root),
    }
    fn = m.get(key)
    return fn() if fn else None


def _corpus_overlay(cfg: CorpusConfig) -> dict:
    return {
        "corpus_index_csv":          str(cfg.resolve(cfg.index_csv, ROOT)),
        "corpus_chunks_parquet":     str(cfg.resolve(cfg.chunks_parquet, ROOT)),
        "corpus_chunks_llm_parquet": str(cfg.resolve(cfg.chunks_llm_parquet, ROOT)),
        "datasets_export_dir":       str(cfg.resolve(cfg.datasets_dir, ROOT)),
    }


def _resolve_index(cfg: CorpusConfig) -> Path | None:
    p = cfg.resolve(cfg.index_csv, ROOT)
    return p if p.is_file() else None


def _trial_chunks_ready(cfg: CorpusConfig, trial_n: int) -> bool:
    """True if we have at least trial_n rows in the parse index (any OK status)."""
    parse_csv = cfg.resolve(cfg.parse_index_csv, ROOT)
    if not parse_csv.is_file():
        return False
    try:
        df = pd.read_csv(parse_csv)
        ok = df[df["parse_status"].str.startswith("OK", na=False)]
        return len(ok) >= min(trial_n, 1)
    except Exception:
        return False


def _launch_trial_ingest(cfg: CorpusConfig, trial_n: int) -> None:
    cmd = _run_pipeline_cmd(cfg, "all", trial_n=trial_n)
    st.session_state.ws_log.append(
        f"<span class='log-step'>$ {' '.join(cmd[-6:])}</span>"
    )
    proc = _start_subprocess(cmd)
    q: Queue = Queue()
    t = Thread(target=_drain_proc, args=(proc, q), daemon=True)
    t.start()
    st.session_state.ws_proc      = proc
    st.session_state.ws_queue     = q
    st.session_state.ws_proc_done = False
    st.session_state.ws_proc_rc   = 0


def _launch_full_ingest(cfg: CorpusConfig) -> None:
    cmd = _run_pipeline_cmd(cfg, "all", trial_n=0)
    st.session_state.ws_log.append(
        f"<span class='log-step'>$ {' '.join(cmd[-6:])}</span>"
    )
    proc = _start_subprocess(cmd)
    q: Queue = Queue()
    t = Thread(target=_drain_proc, args=(proc, q), daemon=True)
    t.start()
    st.session_state.ws_proc      = proc
    st.session_state.ws_queue     = q
    st.session_state.ws_proc_done = False
    st.session_state.ws_proc_rc   = 0

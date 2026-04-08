"""Corpus Setup — landing page.

Three-column layout:
  LEFT   (240px)  — corpus selector / saved configs
  CENTER (flex)   — pipeline status + agent log stream
  RIGHT  (320px)  — folder loader / config form + run controls
"""
from __future__ import annotations

import subprocess
import sys
import threading
from pathlib import Path
from typing import Iterator

import streamlit as st
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tariff_agent.corpus.config import FILE_PATTERNS, CorpusConfig, _slugify
from tariff_agent.corpus.ingest import corpus_status
from tariff_agent.corpus.paths import normalize_host_path
from tariff_agent.corpus.scan_index import write_corpus_index

# ── Constants ──────────────────────────────────────────────────────────────
_CONFIG_DIR = Path(__file__).resolve().parents[1] / "output" / "corpus_configs"
_CONFIG_DIR.mkdir(parents=True, exist_ok=True)

_PRESETS: dict[str, str] = {
    "sedar_tariff": "SEDAR Tariff 2023–2025",
    "sedar_prateek_filings": "SEDAR (prateek portable root)",
    "tsx_esg_2023": "TSX ESG 2023  (185 PDFs)",
    "tsx_esg_2024": "TSX ESG 2024  (178 PDFs)",
    "pdf_agents_esg": "pdf-agents ESG sample",
}

_KNOWN_PRESETS = frozenset(_PRESETS.keys())


def _pipeline_cmd(root: Path, cfg: CorpusConfig) -> list[str]:
    exe = sys.executable
    script = str(root / "scripts" / "run_corpus_pipeline.py")
    yaml_path = _CONFIG_DIR / f"{cfg.corpus_id}.yaml"
    if yaml_path.exists():
        return [exe, script, "--config", str(yaml_path)]
    return [exe, script, "--corpus", cfg.corpus_id]


def _load_saved() -> dict[str, Path]:
    return {p.stem: p for p in sorted(_CONFIG_DIR.glob("*.yaml"))}


def _status_icon(ok: bool) -> str:
    return "●" if ok else "○"


def _load_preset(key: str, root: Path) -> CorpusConfig | None:
    m = {
        "sedar_tariff":          lambda: CorpusConfig.sedar_default(root),
        "sedar_prateek_filings": lambda: CorpusConfig.sedar_prateek_filings(root),
        "tsx_esg_2023":          lambda: CorpusConfig.tsx_esg_2023(root),
        "tsx_esg_2024":          lambda: CorpusConfig.tsx_esg_2024(root),
        "pdf_agents_esg":        lambda: CorpusConfig.pdf_agents_esg_default(root),
    }
    fn = m.get(key)
    return fn() if fn else None


# ── Streaming subprocess helper ─────────────────────────────────────────────
def _stream_lines(cmd: list[str], cwd: str) -> Iterator[str]:
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1, cwd=cwd,
    )
    assert proc.stdout
    for line in proc.stdout:
        yield line.rstrip()
    proc.wait()
    yield f"[exit {proc.returncode}]"


def page() -> None:
    root = Path(__file__).resolve().parents[1]

    st.markdown("<h1 style='margin-bottom:4px'>Corpus Dataset Builder</h1>", unsafe_allow_html=True)
    st.markdown(
        "<p style='color:var(--text-muted);font-size:0.82rem;margin-bottom:1.4rem'>"
        "Point at a folder of PDFs, run ingestion, then build structured tabular datasets interactively.</p>",
        unsafe_allow_html=True,
    )

    # ── Three-column shell ────────────────────────────────────────────────
    left, center, right = st.columns([1.1, 2.4, 1.5], gap="medium")

    # ───────────────────────────── LEFT ────────────────────────────────────
    with left:
        st.markdown("### Saved corpora")
        saved = _load_saved()

        options: list[tuple[str, str]] = [("__new__", "+ New corpus from folder")]
        for key, label in _PRESETS.items():
            options.append((key, label))
        for stem, _ in saved.items():
            if stem not in _KNOWN_PRESETS:
                options.append((stem, f"↳ {stem}"))

        labels = [lbl for _, lbl in options]
        keys   = [k   for k, _ in options]

        cur_key = (st.session_state.get("corpus_cfg") or {}) and getattr(
            st.session_state.get("corpus_cfg"), "corpus_id", None
        )
        default_idx = keys.index(cur_key) if cur_key in keys else 0

        sel_idx = st.radio(
            "",
            range(len(labels)),
            format_func=lambda i: labels[i],
            index=default_idx,
            label_visibility="collapsed",
            key="corpus_selector_radio",
        )
        sel_key = keys[sel_idx]

        if sel_key != "__new__":
            cfg: CorpusConfig | None = None
            if sel_key in _KNOWN_PRESETS:
                cfg = _load_preset(sel_key, root)
            elif sel_key in saved:
                try:
                    cfg = CorpusConfig.from_yaml(saved[sel_key])
                except Exception as e:
                    st.error(f"Load error: {e}")
            if cfg:
                st.session_state["corpus_cfg"] = cfg

        # Pipeline status for active corpus
        cfg_active: CorpusConfig | None = st.session_state.get("corpus_cfg")
        if cfg_active:
            st.markdown("---")
            st.markdown(f"<div style='font-size:0.78rem;font-weight:500;margin-bottom:6px'>Pipeline — {cfg_active.corpus_id}</div>", unsafe_allow_html=True)
            try:
                status = corpus_status(cfg_active, root)
                rows = [
                    (_status_icon(status["index_exists"]),  "Index",    f"{status['n_documents']} docs"),
                    (_status_icon(status["chunks_exist"]),  "Chunks",   f"{status['n_chunks']:,}" if status["n_chunks"] else "—"),
                    (_status_icon(status["llm_chunks_exist"]), "Pass-1", f"{status['n_llm_chunks']:,}" if status["n_llm_chunks"] else "—"),
                    (_status_icon(status["docs_llm_exists"]),  "Pass-2", f"{status['n_docs_llm']:,}" if status["n_docs_llm"] else "—"),
                    (_status_icon(status["n_datasets"] > 0),   "Datasets", str(status["n_datasets"])),
                ]
                for icon, label, val in rows:
                    col_a, col_b = st.columns([0.22, 0.78])
                    col_a.markdown(f"<span style='color:{'#9DC8A0' if icon=='●' else '#7A6652'}'>{icon}</span>", unsafe_allow_html=True)
                    col_b.markdown(f"<span style='font-size:0.78rem'>{label}</span> <span style='font-size:0.74rem;color:var(--text-muted)'>{val}</span>", unsafe_allow_html=True)
            except Exception:
                st.caption("Status unavailable")

    # ─────────────────────────── CENTER ────────────────────────────────────
    with center:
        cfg_active = st.session_state.get("corpus_cfg")

        if cfg_active:
            st.markdown(f"### {cfg_active.name}")
            st.markdown(
                f"<code style='font-size:0.74rem;background:var(--cream-mid);padding:2px 7px;border-radius:3px'>"
                f"id: {cfg_active.corpus_id}</code>&nbsp;&nbsp;"
                f"<span style='font-size:0.76rem;color:var(--text-muted)'>{cfg_active.topic[:80]}</span>",
                unsafe_allow_html=True,
            )
            st.markdown("---")

        # ── Agent / pipeline log ─────────────────────────────────────────
        st.markdown(
            "<div style='font-size:0.8rem;font-weight:500;margin-bottom:6px;color:var(--brown-mid)'>"
            "Agent log</div>",
            unsafe_allow_html=True,
        )

        if "agent_log" not in st.session_state:
            st.session_state.agent_log = []

        log_placeholder = st.empty()

        def _render_log(lines: list[str]) -> None:
            body = "\n".join(lines[-120:]) or "<span class='log-dim'>No output yet. Run a pipeline stage.</span>"
            log_placeholder.markdown(
                f"<div class='agent-log'>{body}</div>",
                unsafe_allow_html=True,
            )

        _render_log(st.session_state.agent_log)

        # ── Run controls (below log) ──────────────────────────────────────
        if cfg_active:
            st.markdown("---")
            rc1, rc2, rc3 = st.columns([2, 1, 1])
            stage = rc1.selectbox(
                "Stage",
                ["all", "parse", "chunk", "llm_chunk", "llm_doc"],
                key="pipeline_stage_center",
                label_visibility="collapsed",
            )
            no_skip = rc2.checkbox("Force re-run", value=False, key="force_rerun_center")
            run_btn = rc3.button("Run ▶", key="run_pipeline_center")

            if run_btn:
                base_cmd = _pipeline_cmd(root, cfg_active)
                cmd = base_cmd + ["--stage", stage] + (["--no-skip"] if no_skip else [])
                st.session_state.agent_log = [
                    f"<span class='log-step'>$ {' '.join(cmd[-5:])}</span>",
                    "<span class='log-dim'>Starting…</span>",
                ]
                _render_log(st.session_state.agent_log)

                with st.spinner("Running pipeline…"):
                    for line in _stream_lines(cmd, str(root)):
                        tag = "log-info" if not line.startswith("[exit") else ("log-error" if "[exit 1" in line or "[exit -" in line else "log-info")
                        if "ERROR" in line or "error" in line.lower():
                            tag = "log-error"
                        elif "WARNING" in line or "warn" in line.lower():
                            tag = "log-warn"
                        st.session_state.agent_log.append(f"<span class='{tag}'>{line}</span>")
                        _render_log(st.session_state.agent_log)

                st.rerun()

    # ───────────────────────────── RIGHT ────────────────────────────────────
    with right:
        # ── New corpus form ─────────────────────────────────────────────
        if sel_key == "__new__":
            st.markdown("### New corpus")
            st.caption("Paste a folder path (Windows or Linux) or type one in. The scanner will find all PDFs recursively.")

            docs_dir_raw = st.text_input(
                "PDF folder path",
                placeholder=r"C:\Users\...\ESG Reports 2024  or  /mnt/c/...",
                key="nc_docs_dir",
            )
            corpus_name = st.text_input("Name", placeholder="TSX ESG 2024", key="nc_name")
            corpus_topic = st.text_area(
                "Topic (natural language)",
                placeholder="Scope 1–3 emissions, board diversity, TCFD risk disclosures…",
                height=90,
                key="nc_topic",
            )
            filing_type_lbl = st.text_input("Filing type label", value="ESG_REPORT", key="nc_ftype")

            create_btn = st.button("Scan & create ▶", key="nc_create", disabled=not (docs_dir_raw.strip() and corpus_name.strip()))
            if create_btn:
                norm = normalize_host_path(docs_dir_raw.strip())
                cid = _slugify(corpus_name)
                index_path = root / "data" / "metadata" / f"corpus_{cid}_index.csv"
                with st.spinner(f"Scanning {norm}…"):
                    try:
                        n = write_corpus_index(norm, index_path, filing_type=(filing_type_lbl.strip() or "ESG_REPORT"))
                    except Exception as e:
                        st.error(str(e))
                        n = 0
                if n == 0:
                    st.warning("No PDFs found at that path.")
                else:
                    rel_index = str(index_path.relative_to(root))
                    cfg_new = CorpusConfig(
                        name=corpus_name.strip(),
                        corpus_id=cid,
                        topic=(corpus_topic.strip() or corpus_name.strip()),
                        docs_dir=str(norm),
                        file_pattern="csv_manifest",
                        metadata_csv=rel_index,
                        doc_id_field="filing_id",
                        doc_path_field="local_path",
                        identity_fields=["filing_id", "ticker", "issuer_name", "filing_type", "filing_date"],
                        extra_context_fields=[],
                        output_base_dir=str(root / "output"),
                        index_csv=rel_index,
                    )
                    yaml_out = _CONFIG_DIR / f"{cid}.yaml"
                    cfg_new.to_yaml(yaml_out)
                    st.session_state["corpus_cfg"] = cfg_new
                    st.success(f"Indexed **{n}** PDFs → config saved as `{cid}.yaml`")
                    st.rerun()

        # ── Active corpus controls ───────────────────────────────────────
        elif cfg_active := st.session_state.get("corpus_cfg"):
            st.markdown("### Corpus config")
            st.caption(f"`{cfg_active.corpus_id}` · {cfg_active.file_pattern}")

            # Docs dir
            st.markdown(f"<div style='font-size:0.78rem;color:var(--text-muted);margin-top:8px'>PDF root</div>", unsafe_allow_html=True)
            st.code(cfg_active.docs_dir[:72], language=None)

            # Topic edit
            new_topic = st.text_area(
                "Topic",
                value=cfg_active.topic,
                height=70,
                key="edit_topic",
                label_visibility="visible",
            )
            if new_topic.strip() != cfg_active.topic:
                import dataclasses
                d = dataclasses.asdict(cfg_active)
                d["topic"] = new_topic.strip()
                cfg_active2 = CorpusConfig.from_dict(d)
                yaml_out = _CONFIG_DIR / f"{cfg_active2.corpus_id}.yaml"
                cfg_active2.to_yaml(yaml_out)
                st.session_state["corpus_cfg"] = cfg_active2
                st.caption("Topic updated.")

            st.markdown("---")

            # Prep / flatten corpus
            with st.expander("Prep & flatten PDFs", expanded=False):
                st.caption(
                    "Copy all PDFs to a flat folder, deduplicate by MD5, "
                    "and skip non-English files. Useful before first ingestion."
                )
                prep_out = st.text_input("Output flat folder", value=f"output/corpus_flat/{cfg_active.corpus_id}", key="prep_out")
                skip_lang = st.checkbox("Skip language detection (faster)", value=False, key="prep_lang")
                if st.button("Run prep script ▶", key="prep_run"):
                    cmd = [
                        sys.executable,
                        str(root / "scripts" / "prep_corpus_folder.py"),
                        "--src", cfg_active.docs_dir,
                        "--out-dir", str(root / prep_out),
                        "--workers", "4",
                    ]
                    if skip_lang:
                        cmd.append("--skip-lang")
                    with st.spinner("Prepping…"):
                        r = subprocess.run(cmd, capture_output=True, text=True, cwd=str(root))
                    out_text = (r.stdout or "") + (r.stderr or "")
                    st.code(out_text[-4000:], language=None)

            # YAML view / edit
            with st.expander("View / edit raw YAML", expanded=False):
                import dataclasses
                raw = yaml.dump(dataclasses.asdict(cfg_active), default_flow_style=False, sort_keys=False)
                edited = st.text_area("config.yaml", value=raw, height=260, key="yaml_edit")
                if st.button("Apply YAML edits", key="yaml_apply"):
                    try:
                        data = yaml.safe_load(edited)
                        cfg2 = CorpusConfig.from_dict(data)
                        yaml_out2 = _CONFIG_DIR / f"{cfg2.corpus_id}.yaml"
                        cfg2.to_yaml(yaml_out2)
                        st.session_state["corpus_cfg"] = cfg2
                        st.success("Config updated.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"YAML parse error: {e}")

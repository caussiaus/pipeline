"""Corpus Dataset Builder — production Streamlit app.

Point at any directory of PDFs, configure metadata, and interactively
build structured tabular datasets from your document corpus.

Run:
    streamlit run app.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[0]))

st.set_page_config(
    page_title="Corpus Dataset Builder",
    page_icon="◆",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Global design system (cream + dark-brown, minimal, thin) ───────────────
st.markdown(
    """
<style>
/* ── Typeface & tokens ─────────────────────────────────────────────────── */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');

:root {
  --cream:       #F5F0E8;
  --cream-mid:   #EDE7D9;
  --cream-dark:  #DDD4C2;
  --brown:       #1A120B;
  --brown-mid:   #3D2B1F;
  --brown-light: #6B4F3A;
  --rule:        #C8BBA8;
  --accent:      #1A120B;
  --text-main:   #1A120B;
  --text-muted:  #7A6652;
  --radius:      4px;
  --font:        'Inter', sans-serif;
  --mono:        'JetBrains Mono', monospace;
}

/* ── Base ──────────────────────────────────────────────────────────────── */
html, body, [data-testid="stAppViewContainer"] {
  background: var(--cream) !important;
  font-family: var(--font);
  color: var(--text-main);
}
[data-testid="stMain"] {
  background: var(--cream) !important;
}

/* ── Sidebar ───────────────────────────────────────────────────────────── */
[data-testid="stSidebar"] {
  background: var(--brown) !important;
  border-right: 1px solid var(--brown-mid);
  min-width: 220px !important;
  max-width: 240px !important;
}
[data-testid="stSidebar"] * { color: var(--cream) !important; }
[data-testid="stSidebar"] .stMarkdown h2,
[data-testid="stSidebar"] .stMarkdown h3 {
  color: var(--cream-dark) !important;
  font-weight: 500;
  font-size: 0.78rem;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  margin: 1.4rem 0 0.4rem;
}
[data-testid="stSidebar"] hr { border-color: var(--brown-mid) !important; }
[data-testid="stSidebar"] [data-testid="stSidebarNavLink"] {
  border-radius: var(--radius);
  margin: 1px 0;
  font-size: 0.85rem;
  font-weight: 400;
  padding: 7px 12px;
  transition: background 0.12s;
}
[data-testid="stSidebar"] [data-testid="stSidebarNavLink"]:hover {
  background: var(--brown-mid) !important;
}
[data-testid="stSidebar"] [data-testid="stSidebarNavLink"][aria-current="page"] {
  background: var(--brown-mid) !important;
  font-weight: 600;
}
[data-testid="stSidebar"] [data-testid="stSidebarNavSectionHeader"] {
  font-size: 0.7rem;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: var(--brown-light) !important;
  margin: 1.2rem 0 0.2rem;
}

/* ── Top nav / toolbar hide ────────────────────────────────────────────── */
[data-testid="stToolbar"] { display: none; }
#MainMenu { display: none; }
footer { display: none; }
header { background: transparent !important; }

/* ── Typography ───────────────────────────────────────────────────────── */
h1 { font-size: 1.4rem; font-weight: 600; letter-spacing: -0.02em; color: var(--brown); }
h2 { font-size: 1.1rem; font-weight: 500; letter-spacing: -0.01em; color: var(--brown); }
h3 { font-size: 0.95rem; font-weight: 500; color: var(--brown-mid); }
p, li { font-size: 0.88rem; line-height: 1.65; color: var(--text-main); }
.stCaption > p { font-size: 0.78rem; color: var(--text-muted); }

/* ── Inputs ────────────────────────────────────────────────────────────── */
input[type="text"],
textarea,
[data-baseweb="input"] input,
[data-baseweb="textarea"] textarea,
[data-baseweb="select"] {
  background: #FFFCF6 !important;
  border: 1px solid var(--rule) !important;
  border-radius: var(--radius) !important;
  font-family: var(--font) !important;
  font-size: 0.86rem !important;
  color: var(--brown) !important;
  transition: border-color 0.12s;
}
input[type="text"]:focus,
textarea:focus {
  border-color: var(--brown) !important;
  outline: none !important;
  box-shadow: 0 0 0 2px rgba(26,18,11,0.06) !important;
}
label { font-size: 0.82rem; font-weight: 500; color: var(--brown-mid); margin-bottom: 3px; }

/* ── Buttons ───────────────────────────────────────────────────────────── */
.stButton > button {
  background: var(--brown) !important;
  color: var(--cream) !important;
  border: none !important;
  border-radius: var(--radius) !important;
  font-size: 0.82rem !important;
  font-weight: 500 !important;
  padding: 7px 18px !important;
  letter-spacing: 0.03em;
  transition: background 0.12s, transform 0.06s;
}
.stButton > button:hover {
  background: var(--brown-mid) !important;
  transform: translateY(-1px);
}
.stButton > button[kind="secondary"] {
  background: transparent !important;
  color: var(--brown) !important;
  border: 1px solid var(--rule) !important;
}
.stButton > button[kind="secondary"]:hover {
  border-color: var(--brown) !important;
  background: var(--cream-mid) !important;
}

/* ── Cards / containers ────────────────────────────────────────────────── */
[data-testid="stExpander"] {
  border: 1px solid var(--cream-dark) !important;
  border-radius: var(--radius) !important;
  background: #FFFCF6 !important;
}
[data-testid="stExpander"] summary {
  font-size: 0.84rem;
  font-weight: 500;
  color: var(--brown-mid);
}
.stInfo, [data-testid="stAlert"][data-baseweb="notification"] {
  background: #F0EBE1 !important;
  border-left: 3px solid var(--brown-light) !important;
  border-radius: var(--radius) !important;
  color: var(--brown) !important;
}
.stSuccess [data-testid="stAlert"] {
  background: #EAF0E6 !important;
  border-left-color: #5A7A4A !important;
}
.stWarning [data-testid="stAlert"] {
  background: #F5EDD8 !important;
  border-left-color: #B08030 !important;
}

/* ── Dividers ──────────────────────────────────────────────────────────── */
hr { border: none; border-top: 1px solid var(--cream-dark); margin: 1.2rem 0; }

/* ── Metrics ───────────────────────────────────────────────────────────── */
[data-testid="stMetric"] {
  background: #FFFCF6;
  border: 1px solid var(--cream-dark);
  border-radius: var(--radius);
  padding: 10px 14px;
}
[data-testid="stMetricLabel"] { font-size: 0.74rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.08em; }
[data-testid="stMetricValue"] { font-size: 1.2rem; font-weight: 600; color: var(--brown); }

/* ── Dataframe ─────────────────────────────────────────────────────────── */
[data-testid="stDataFrame"] {
  border: 1px solid var(--cream-dark) !important;
  border-radius: var(--radius) !important;
  overflow: hidden;
}
.dvn-scroller { background: #FFFCF6 !important; }

/* ── Code / mono ───────────────────────────────────────────────────────── */
code, pre, .stCode {
  font-family: var(--mono) !important;
  font-size: 0.78rem !important;
  background: var(--cream-mid) !important;
  color: var(--brown) !important;
  border-radius: var(--radius);
  border: 1px solid var(--cream-dark);
}

/* ── Chat messages ─────────────────────────────────────────────────────── */
[data-testid="stChatMessage"] {
  background: #FFFCF6 !important;
  border: 1px solid var(--cream-dark) !important;
  border-radius: var(--radius) !important;
  margin-bottom: 6px !important;
  padding: 8px 12px !important;
}
[data-testid="stChatMessage"][data-testid*="user"] {
  background: var(--cream-mid) !important;
}
[data-testid="stChatInputContainer"] {
  background: #FFFCF6 !important;
  border-top: 1px solid var(--cream-dark) !important;
}

/* ── Log panel (scrolling agent output) ────────────────────────────────── */
.agent-log {
  font-family: var(--mono);
  font-size: 0.76rem;
  line-height: 1.7;
  background: var(--brown);
  color: var(--cream-dark);
  padding: 14px 18px;
  border-radius: var(--radius);
  height: 340px;
  overflow-y: auto;
  white-space: pre-wrap;
  word-break: break-all;
}
.agent-log .log-info  { color: #9DC8A0; }
.agent-log .log-warn  { color: #E6C97A; }
.agent-log .log-error { color: #E88080; }
.agent-log .log-dim   { color: #7A6652; }
.agent-log .log-step  { color: var(--cream); font-weight: 600; }

/* ── Schema / field card ───────────────────────────────────────────────── */
.field-card {
  background: #FFFCF6;
  border: 1px solid var(--cream-dark);
  border-radius: var(--radius);
  padding: 10px 14px;
  margin-bottom: 6px;
  cursor: pointer;
  transition: border-color 0.12s, background 0.12s;
}
.field-card:hover {
  border-color: var(--brown-light);
  background: var(--cream-mid);
}
.field-card.active {
  border-color: var(--brown);
  background: var(--cream-mid);
}
.field-badge {
  display: inline-block;
  font-size: 0.7rem;
  font-weight: 500;
  padding: 2px 7px;
  border-radius: 99px;
  background: var(--cream-dark);
  color: var(--brown-light);
  margin-left: 6px;
}

/* ── Progress bar ─────────────────────────────────────────────────────── */
[data-testid="stProgress"] > div > div {
  background: var(--brown) !important;
}

/* ── Select / radio ────────────────────────────────────────────────────── */
[data-baseweb="select"] { border-color: var(--rule) !important; }
[data-testid="stRadio"] label { font-size: 0.84rem; color: var(--brown-mid); }

/* ── Phase pill (status badge) ─────────────────────────────────────────── */
.phase-pill {
  display: inline-block;
  padding: 2px 10px;
  border-radius: 99px;
  font-size: 0.72rem;
  font-weight: 600;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  background: var(--cream-dark);
  color: var(--brown-light);
  margin-left: 8px;
}
.phase-pill.active { background: var(--brown); color: var(--cream); }
.phase-pill.done   { background: #5A7A4A; color: var(--cream); }
</style>
""",
    unsafe_allow_html=True,
)

# ── Page imports ────────────────────────────────────────────────────────────
from app_pages import browse, build_dataset, corpus_setup, datasets, feedback, workspace

# ── Navigation ─────────────────────────────────────────────────────────────
pages = {
    "": [
        st.Page(workspace.page, title="Workspace", icon="○", url_path="workspace", default=True),
    ],
    "Advanced": [
        st.Page(corpus_setup.page,  title="Corpus Setup",   icon="○", url_path="corpus_setup"),
        st.Page(browse.page,        title="Browse & Evidence", icon="○", url_path="browse"),
        st.Page(build_dataset.page, title="Build Dataset",  icon="○", url_path="build_dataset"),
        st.Page(datasets.page,      title="Saved Datasets", icon="○", url_path="datasets"),
        st.Page(feedback.page,      title="Feedback & SFT", icon="○", url_path="feedback"),
    ],
}

pg = st.navigation(pages, position="sidebar", expanded=True)

with st.sidebar:
    cfg = st.session_state.get("corpus_cfg")
    st.markdown("## Corpus Dataset Builder")
    st.markdown("---")
    if cfg:
        st.markdown(f"**Active corpus**\n\n`{cfg.name[:32]}`")
        st.markdown(f"<span style='font-size:0.75rem;color:#9DC8A0'>● {cfg.corpus_id}</span>", unsafe_allow_html=True)
    else:
        st.markdown("<span style='font-size:0.8rem;color:#9DC8A0'>No corpus — configure one to begin.</span>", unsafe_allow_html=True)
    st.markdown("---")

pg.run()

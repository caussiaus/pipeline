"""Dataset Builder — single-page IDE-style Streamlit app.

Layout:
  Left sidebar   = dark thread list
  Main area      = table + schema + chat
  Right panel    = field inspector
  Bottom         = terminal (dark, monospace)

Auth: APP_PASSWORD in .streamlit/secrets.toml
"""
from __future__ import annotations
import sys
from pathlib import Path
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[0]))

st.set_page_config(
    page_title="Dataset Builder",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');

:root {
  --bg:          #FFFFFF;
  --bg2:         #F7F7F7;
  --sidebar:     #111111;
  --border:      #DDDDDD;
  --border2:     #BBBBBB;
  --text:        #000000;
  --text2:       #444444;
  --text3:       #888888;
  --accent:      #000000;
  --green:       #1A8A3F;
  --red:         #CC2222;
  --yellow:      #996600;
  --term-bg:     #111111;
  --term-text:   #CCCCCC;
  --term-green:  #4EC994;
  --term-blue:   #9CDCFE;
  --term-dim:    #555555;
  --font:        'Inter', -apple-system, sans-serif;
  --mono:        'JetBrains Mono', 'Fira Code', 'Consolas', monospace;
  --r:           2px;
}

/* ── Reset ── */
html, body, [data-testid="stAppViewContainer"],
[data-testid="stMain"] {
  background: var(--bg) !important;
  font-family: var(--font) !important;
  color: var(--text) !important;
}
[data-testid="stToolbar"], #MainMenu, footer, header { display: none !important; }

/* ── Sidebar ── */
[data-testid="stSidebar"] {
  background: var(--sidebar) !important;
  border-right: 1px solid #222 !important;
  min-width: 210px !important;
  max-width: 230px !important;
}
[data-testid="stSidebar"] * { color: #CCCCCC !important; font-family: var(--font) !important; }
[data-testid="stSidebar"] hr { border-color: #2A2A2A !important; }
[data-testid="stSidebar"] .stButton > button {
  background: #1C1C1C !important;
  color: #CCCCCC !important;
  border: 1px solid #2E2E2E !important;
  font-size: 0.86rem !important;
  font-family: var(--mono) !important;
  text-align: left !important;
  padding: 7px 11px !important;
  border-radius: var(--r) !important;
  width: 100% !important;
}
[data-testid="stSidebar"] .stButton > button:hover {
  background: #252525 !important;
  border-color: #4A4A4A !important;
  color: #FFFFFF !important;
}

/* ── Typography ── */
h1 { font-size: 1.35rem !important; font-weight: 600 !important; color: #000 !important; letter-spacing: -0.02em; margin-bottom: 2px; }
h2 { font-size: 1.1rem !important; font-weight: 600 !important; color: #000 !important; }
h3 { font-size: 0.96rem !important; font-weight: 500 !important; color: #222 !important; }
p, li { font-size: 0.93rem !important; line-height: 1.6; color: var(--text) !important; }
label { font-size: 0.88rem !important; font-weight: 500 !important; color: #333 !important; }
.stCaption > p { font-size: 0.78rem !important; color: var(--text3) !important; }
code { font-family: var(--mono) !important; font-size: 0.82rem !important; background: var(--bg2) !important; color: #000 !important; padding: 1px 5px; border-radius: 2px; border: 1px solid var(--border); }

/* ── Inputs ── */
input[type="text"], input[type="password"],
[data-baseweb="input"] input,
[data-baseweb="textarea"] textarea {
  background: var(--bg) !important;
  border: 1px solid var(--border2) !important;
  border-radius: var(--r) !important;
  font-family: var(--font) !important;
  font-size: 0.93rem !important;
  color: var(--text) !important;
}
input:focus, textarea:focus {
  border-color: #000 !important;
  box-shadow: 0 0 0 2px rgba(0,0,0,0.06) !important;
  outline: none !important;
}

/* ── Buttons ── */
.stButton > button {
  background: #000 !important;
  color: #fff !important;
  border: none !important;
  border-radius: var(--r) !important;
  font-size: 0.88rem !important;
  font-weight: 500 !important;
  padding: 8px 20px !important;
}
.stButton > button:hover { background: #222 !important; }
.stButton > button[kind="secondary"] {
  background: #fff !important;
  color: #000 !important;
  border: 1px solid var(--border2) !important;
}
.stButton > button[kind="secondary"]:hover {
  background: var(--bg2) !important;
  border-color: #888 !important;
}

/* ── Tabs ── */
[data-testid="stTabs"] [role="tab"] { font-size: 0.88rem !important; color: var(--text2) !important; }
[data-testid="stTabs"] [role="tab"][aria-selected="true"] { color: #000 !important; font-weight: 600 !important; }
[data-testid="stTabs"] [data-baseweb="tab-border"] { background: #000 !important; }

/* ── Expander ── */
[data-testid="stExpander"] {
  border: 1px solid var(--border) !important;
  border-radius: var(--r) !important;
  background: var(--bg) !important;
}
[data-testid="stExpander"] summary { font-size: 0.88rem !important; color: #333 !important; }

/* ── Alerts ── */
.stInfo, .stSuccess, .stWarning, .stError {
  border-radius: var(--r) !important;
  font-size: 0.88rem !important;
}
.stInfo    { background: #F0F4FF !important; border-left: 3px solid #4466CC !important; }
.stSuccess { background: #F0F9F3 !important; border-left: 3px solid var(--green) !important; }
.stWarning { background: #FFF9F0 !important; border-left: 3px solid var(--yellow) !important; }
.stError   { background: #FFF0F0 !important; border-left: 3px solid var(--red) !important; }

/* ── Metrics ── */
[data-testid="stMetric"] {
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: var(--r);
  padding: 8px 12px;
}
[data-testid="stMetricLabel"] { font-size: 0.72rem !important; color: var(--text3) !important; text-transform: uppercase; letter-spacing: 0.06em; }
[data-testid="stMetricValue"] { font-size: 1.1rem !important; font-weight: 600 !important; color: #000 !important; }

/* ── Progress ── */
[data-testid="stProgress"] > div > div { background: #000 !important; }

/* ── Dataframe ── */
[data-testid="stDataFrame"] { border: 1px solid var(--border) !important; border-radius: var(--r) !important; }

/* ── Chat ── */
[data-testid="stChatMessage"] {
  background: var(--bg) !important;
  border: 1px solid var(--border) !important;
  border-radius: var(--r) !important;
  margin-bottom: 4px !important;
  padding: 8px 12px !important;
}

/* ── Grid table (main data view) ── */
.grid-wrap {
  overflow: auto;
  max-height: 50vh;
  border: 1px solid var(--border2);
  background: var(--bg);
  border-radius: var(--r);
}
.grid-table {
  border-collapse: collapse;
  width: max-content;
  min-width: 100%;
  font-size: 0.88rem;
  font-family: var(--font);
}
.grid-table thead { position: sticky; top: 0; z-index: 10; background: var(--bg2); }
.grid-table th {
  padding: 8px 14px;
  font-weight: 600;
  font-size: 0.76rem;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: #333;
  border-bottom: 2px solid var(--border2);
  border-right: 1px solid var(--border);
  white-space: nowrap;
  text-align: left;
  user-select: none;
}
.grid-table th.th-active { background: #000; color: #fff; }
.grid-table th:last-child { border-right: none; }
.grid-table td {
  padding: 7px 14px;
  border-bottom: 1px solid var(--border);
  border-right: 1px solid var(--border);
  color: var(--text);
  white-space: nowrap;
  max-width: 240px;
  overflow: hidden;
  text-overflow: ellipsis;
  vertical-align: top;
}
.grid-table td.row-num { color: var(--text3); font-family: var(--mono); font-size: 0.8rem; background: var(--bg2); border-right: 2px solid var(--border2) !important; }
.grid-table td:last-child { border-right: none; }
.grid-table tr:hover td { background: #F5F5F5 !important; }
.grid-table tr:hover td.row-num { background: #EBEBEB !important; }
.grid-table .null { color: #CCC; font-style: italic; }
.grid-table .bool-t { color: var(--green); font-family: var(--mono); font-weight: 600; }
.grid-table .bool-f { color: var(--red); font-family: var(--mono); }
.grid-table .annotated { border-left: 3px solid var(--yellow) !important; }
.grid-table th.header-only { color: #000; background: #F0F0F0; font-size: 0.82rem; padding: 10px 14px; }

/* ── Field chip strip (above table) ── */
.field-strip {
  display: flex;
  flex-wrap: nowrap;
  overflow-x: auto;
  gap: 4px;
  padding: 6px 0 10px;
  scrollbar-width: thin;
}
.field-chip {
  display: inline-block;
  font-family: var(--mono);
  font-size: 0.78rem;
  padding: 4px 10px;
  border: 1px solid var(--border2);
  border-radius: var(--r);
  background: var(--bg);
  color: #333;
  white-space: nowrap;
  cursor: pointer;
  transition: all 0.08s;
}
.field-chip:hover, .field-chip.active { background: #000; color: #fff; border-color: #000; }
.field-chip .chip-type { font-size: 0.68rem; color: #888; margin-left: 5px; }
.field-chip.active .chip-type { color: #888; }

/* ── Agent log ── */
.agent-log {
  font-family: var(--mono);
  font-size: 0.8rem;
  line-height: 1.7;
  background: var(--term-bg);
  color: var(--term-text);
  padding: 12px 14px;
  border-radius: var(--r);
  height: 200px;
  overflow-y: auto;
  white-space: pre-wrap;
  word-break: break-word;
}
.log-info  { color: var(--term-green); }
.log-warn  { color: #DDB555; }
.log-error { color: #F14C4C; }
.log-dim   { color: var(--term-dim); }
.log-step  { color: var(--term-blue); font-weight: 600; }

/* ── Terminal area ── */
.terminal-wrap {
  background: var(--term-bg);
  border: 1px solid #2A2A2A;
  border-radius: var(--r);
  padding: 10px 14px 6px;
  margin-top: 4px;
}
.terminal-history {
  max-height: 100px;
  overflow-y: auto;
  margin-bottom: 6px;
  font-family: var(--mono);
  font-size: 0.82rem;
}
.t-line { line-height: 1.6; color: var(--term-text); }
.t-prompt { color: var(--term-green); }
.t-assistant { color: var(--term-blue); }
.t-system { color: var(--term-dim); }

/* Terminal textarea overrides */
.terminal-input textarea {
  background: var(--term-bg) !important;
  color: var(--term-text) !important;
  border: 1px solid #333 !important;
  font-family: var(--mono) !important;
  font-size: 0.88rem !important;
  border-radius: var(--r) !important;
  caret-color: var(--term-green) !important;
}
.terminal-input textarea:focus {
  border-color: #555 !important;
  box-shadow: none !important;
}
.terminal-input label { color: var(--term-dim) !important; font-family: var(--mono) !important; font-size: 0.76rem !important; }

/* ── Inspector panel ── */
.inspector-label {
  font-size: 0.7rem;
  font-weight: 700;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: var(--text3);
  margin-bottom: 6px;
  margin-top: 2px;
}
.inspector-field {
  border: 1px solid var(--border);
  border-radius: var(--r);
  padding: 9px 12px;
  margin-bottom: 6px;
  background: var(--bg);
}
.insp-name { font-family: var(--mono); font-size: 0.9rem; font-weight: 600; color: #000; }
.insp-type { font-size: 0.72rem; color: #888; background: var(--bg2); border: 1px solid var(--border); padding: 1px 7px; border-radius: 2px; margin-left: 6px; font-family: var(--mono); }
.insp-desc { font-size: 0.84rem; color: #444; margin-top: 4px; line-height: 1.5; }
.insp-instr { font-size: 0.8rem; color: #555; background: var(--bg2); border-radius: var(--r); padding: 6px 9px; margin-top: 6px; border-left: 3px solid var(--border2); }
.evidence-quote {
  font-size: 0.83rem;
  background: #FFFBF0;
  border: 1px solid #E8DFC8;
  border-radius: var(--r);
  padding: 7px 10px;
  color: #444;
  margin-top: 4px;
  font-style: italic;
}

/* ── Cell annotation ── */
.cell-ann-panel {
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: var(--r);
  padding: 10px 14px;
  margin-top: 8px;
}
.cell-val-display {
  font-family: var(--mono);
  font-size: 0.88rem;
  background: #fff;
  border: 1px solid var(--border);
  padding: 5px 9px;
  border-radius: var(--r);
  color: #000;
  margin-bottom: 6px;
}

hr { border: none; border-top: 1px solid var(--border); margin: 10px 0; }
</style>
""", unsafe_allow_html=True)


# ── Auth gate ────────────────────────────────────────────────────────────────

def _auth() -> bool:
    pw = st.secrets.get("APP_PASSWORD", "")
    if not pw or st.session_state.get("_authed"):
        return True

    c = st.columns([1, 2, 1])[1]
    with c:
        st.markdown("<br><br>", unsafe_allow_html=True)
        st.markdown("## Dataset Builder")
        st.caption("Enter your team password to continue.")
        with st.form("_login"):
            entered = st.text_input("Password", type="password",
                                    label_visibility="collapsed",
                                    placeholder="Team password…")
            if st.form_submit_button("Sign in →", use_container_width=True, type="primary"):
                if entered == pw:
                    st.session_state["_authed"] = True
                    st.rerun()
                else:
                    st.error("Incorrect password.")
    return False


if not _auth():
    st.stop()


# ── Thread sidebar ────────────────────────────────────────────────────────────

from app_pages.thread_store import Thread, delete_thread, list_threads, load_thread

with st.sidebar:
    st.markdown(
        "<div style='font-family:var(--mono);font-size:0.9rem;font-weight:600;"
        "padding:6px 2px 12px;color:#EEE;letter-spacing:-0.01em'>📄 Dataset Builder</div>",
        unsafe_allow_html=True,
    )
    if st.button("＋  New analysis", key="_new", use_container_width=True):
        for k in [k for k in st.session_state if k.startswith("ws_") or k.startswith("_ws")]:
            del st.session_state[k]
        st.session_state.pop("active_thread_id", None)
        st.rerun()

    st.markdown("<hr>", unsafe_allow_html=True)
    st.markdown(
        "<div style='font-size:0.68rem;letter-spacing:0.12em;text-transform:uppercase;"
        "color:#555;margin-bottom:6px;font-family:var(--mono)'>THREADS</div>",
        unsafe_allow_html=True,
    )

    threads = list_threads()
    active_id = st.session_state.get("active_thread_id")

    if not threads:
        st.markdown(
            "<div style='font-size:0.8rem;color:#555;padding:4px 2px'>"
            "No threads yet.</div>", unsafe_allow_html=True,
        )

    STATUS_ICON = {
        "new": "○", "ingesting": "◑", "schema": "◈",
        "extracting": "◑", "preview": "◆", "approve": "◆",
        "full_ingesting": "◑", "full_extracting": "◑", "done": "●", "failed": "✗",
    }
    STATUS_COLOR = {
        "ingesting": "#DDB555", "extracting": "#DDB555",
        "full_ingesting": "#DDB555", "full_extracting": "#DDB555",
        "preview": "#4EC994", "done": "#4EC994",
        "failed": "#F14C4C", "schema": "#9CDCFE",
    }

    for t in threads:
        is_active = active_id == t.thread_id
        icon = STATUS_ICON.get(t.status, "○")
        dot_color = STATUS_COLOR.get(t.status, "#555")
        label = ("▶  " if is_active else "   ") + t.title[:22]
        if st.button(label, key=f"_th_{t.thread_id}", use_container_width=True,
                     help=f"{t.status} · {t.topic[:60]}"):
            for k in [k for k in st.session_state if k.startswith("ws_") or k.startswith("_ws")]:
                del st.session_state[k]
            st.session_state["active_thread_id"] = t.thread_id
            st.rerun()
        st.markdown(
            f"<div style='font-family:var(--mono);font-size:0.72rem;color:{dot_color};"
            f"margin:-8px 0 6px 4px'>{icon} {t.status} · {t.age_label}</div>",
            unsafe_allow_html=True,
        )

    if st.secrets.get("APP_PASSWORD", ""):
        st.markdown("<hr>", unsafe_allow_html=True)
        if st.button("Sign out", key="_so", use_container_width=True):
            st.session_state.pop("_authed", None)
            st.rerun()


# ── Main workspace ────────────────────────────────────────────────────────────

from app_pages import workspace
workspace.render(active_thread_id=st.session_state.get("active_thread_id"))

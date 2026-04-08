"""Dataset Builder — single-page Streamlit app.

Thread-based UX:
  • Left sidebar   = thread history
  • Main area      = active thread (landing → ingest → schema → table)
  • Right panel    = live agent log + field inspector
  • Bottom         = chat input (always available)

Auth: set APP_PASSWORD in .streamlit/secrets.toml to gate the app.
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

# ── Global CSS ───────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');

:root {
  --cream:       #F5F0E8;
  --cream-mid:   #EDE7D9;
  --cream-dark:  #DDD4C2;
  --brown:       #1A120B;
  --brown-mid:   #3D2B1F;
  --brown-light: #6B4F3A;
  --rule:        #C8BBA8;
  --text-main:   #1A120B;
  --text-muted:  #7A6652;
  --radius:      4px;
  --font:        'Inter', sans-serif;
  --mono:        'JetBrains Mono', monospace;
}

html, body, [data-testid="stAppViewContainer"],
[data-testid="stMain"] {
  background: var(--cream) !important;
  font-family: var(--font);
  color: var(--text-main);
}

/* ── Sidebar ── */
[data-testid="stSidebar"] {
  background: var(--brown) !important;
  border-right: 1px solid var(--brown-mid);
  min-width: 220px !important;
  max-width: 240px !important;
}
[data-testid="stSidebar"] * { color: var(--cream) !important; }
[data-testid="stSidebar"] hr { border-color: var(--brown-mid) !important; }
[data-testid="stSidebar"] .stButton > button {
  background: var(--brown-mid) !important;
  color: var(--cream) !important;
  border: 1px solid var(--brown-light) !important;
  font-size: 0.9rem !important;
  text-align: left !important;
  padding: 8px 12px !important;
  transition: background 0.12s;
}
[data-testid="stSidebar"] .stButton > button:hover {
  background: var(--brown-light) !important;
}
[data-testid="stSidebar"] .thread-active > .stButton > button {
  border-color: #9DC8A0 !important;
}

/* ── Hide Streamlit chrome ── */
[data-testid="stToolbar"], #MainMenu, footer, header { display: none !important; }

/* ── Typography ── */
h1 { font-size: 1.7rem; font-weight: 600; letter-spacing: -0.02em; color: var(--brown); }
h2 { font-size: 1.25rem; font-weight: 500; color: var(--brown); }
h3 { font-size: 1.05rem; font-weight: 500; color: var(--brown-mid); }
p, li { font-size: 0.97rem; line-height: 1.7; }
label { font-size: 0.93rem; font-weight: 500; color: var(--brown-mid); }

/* ── Inputs ── */
input[type="text"], textarea,
[data-baseweb="input"] input,
[data-baseweb="textarea"] textarea {
  background: #FFFCF6 !important;
  border: 1px solid var(--rule) !important;
  border-radius: var(--radius) !important;
  font-family: var(--font) !important;
  font-size: 0.97rem !important;
  color: var(--brown) !important;
}
input[type="text"]:focus, textarea:focus {
  border-color: var(--brown) !important;
  box-shadow: 0 0 0 2px rgba(26,18,11,0.06) !important;
}

/* ── Primary button ── */
.stButton > button {
  background: var(--brown) !important;
  color: var(--cream) !important;
  border: none !important;
  border-radius: var(--radius) !important;
  font-size: 0.95rem !important;
  font-weight: 500 !important;
  padding: 9px 22px !important;
  letter-spacing: 0.02em;
  transition: background 0.12s;
}
.stButton > button:hover { background: var(--brown-mid) !important; }
.stButton > button[kind="secondary"] {
  background: transparent !important;
  color: var(--brown) !important;
  border: 1px solid var(--rule) !important;
}
.stButton > button[kind="secondary"]:hover {
  border-color: var(--brown) !important;
  background: var(--cream-mid) !important;
}

/* ── Cards / expanders ── */
[data-testid="stExpander"] {
  border: 1px solid var(--cream-dark) !important;
  border-radius: var(--radius) !important;
  background: #FFFCF6 !important;
}
.stInfo, .stSuccess, .stWarning, .stError {
  border-radius: var(--radius) !important;
  font-size: 0.84rem !important;
}

/* ── Chat ── */
[data-testid="stChatMessage"] {
  background: #FFFCF6 !important;
  border: 1px solid var(--cream-dark) !important;
  border-radius: var(--radius) !important;
  margin-bottom: 5px !important;
  padding: 8px 12px !important;
}
[data-testid="stChatInputContainer"] {
  background: #FFFCF6 !important;
  border-top: 1px solid var(--cream-dark) !important;
}

/* ── Agent log ── */
.agent-log {
  font-family: var(--mono);
  font-size: 0.83rem;
  line-height: 1.75;
  background: var(--brown);
  color: var(--cream-dark);
  padding: 14px 16px;
  border-radius: var(--radius);
  height: 280px;
  overflow-y: auto;
  white-space: pre-wrap;
  word-break: break-all;
}
.log-info  { color: #9DC8A0; }
.log-warn  { color: #E6C97A; }
.log-error { color: #E88080; }
.log-dim   { color: #7A6652; }
.log-step  { color: #F5F0E8; font-weight: 600; }

/* ── Frozen table ── */
.frozen-table-wrap {
  overflow: auto;
  max-height: 52vh;
  border: 1px solid var(--cream-dark);
  border-radius: 4px;
  background: #FFFCF6;
}
.frozen-table {
  border-collapse: collapse;
  width: max-content;
  min-width: 100%;
  font-size: 0.88rem;
  font-family: var(--font);
}
.frozen-table thead {
  position: sticky;
  top: 0;
  z-index: 10;
  background: var(--cream-mid);
}
.frozen-table th {
  padding: 8px 14px;
  text-align: left;
  font-weight: 600;
  font-size: 0.8rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--brown-mid);
  border-bottom: 2px solid var(--cream-dark);
  white-space: nowrap;
  cursor: pointer;
  user-select: none;
}
.frozen-table th:hover { background: var(--cream-dark); color: var(--brown); }
.frozen-table th.th-active { background: var(--brown); color: var(--cream) !important; }
.frozen-table td {
  padding: 7px 14px;
  border-bottom: 1px solid #EDE7D9;
  color: var(--brown);
  white-space: nowrap;
  max-width: 240px;
  overflow: hidden;
  text-overflow: ellipsis;
  font-size: 0.88rem;
}
.frozen-table tr:hover td { background: #FAF6EE; }
.frozen-table .null { color: #C8BBA8; }
.frozen-table .bool-t { color: #5A7A4A; font-weight: 600; }
.frozen-table .bool-f { color: #9E5A5A; }

/* ── Field card ── */
.field-card {
  background: #FFFCF6;
  border: 1px solid var(--cream-dark);
  border-radius: var(--radius);
  padding: 9px 12px;
  margin-bottom: 5px;
  font-size: 0.82rem;
}
.field-note {
  background: var(--cream-mid);
  border-left: 3px solid var(--brown-light);
  border-radius: 0 var(--radius) var(--radius) 0;
  padding: 6px 10px;
  font-size: 0.78rem;
  color: var(--brown-mid);
  margin-top: 4px;
}

/* ── Metrics ── */
[data-testid="stMetric"] {
  background: #FFFCF6;
  border: 1px solid var(--cream-dark);
  border-radius: var(--radius);
  padding: 8px 12px;
}
[data-testid="stMetricLabel"] { font-size: 0.72rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.07em; }
[data-testid="stMetricValue"] { font-size: 1.15rem; font-weight: 600; color: var(--brown); }

/* ── Progress ── */
[data-testid="stProgress"] > div > div { background: var(--brown) !important; }

hr { border: none; border-top: 1px solid var(--cream-dark); margin: 1rem 0; }
</style>
""", unsafe_allow_html=True)


# ── Auth gate ────────────────────────────────────────────────────────────────

def _auth_gate() -> bool:
    """Return True if user is allowed in. Shows login form if password is set."""
    required = st.secrets.get("APP_PASSWORD", "")
    if not required:
        return True
    if st.session_state.get("_authed"):
        return True

    st.markdown("""
<div style='max-width:380px;margin:90px auto 0;padding:0 16px'>
  <h1 style='text-align:center;margin-bottom:4px'>Dataset Builder</h1>
  <p style='text-align:center;color:var(--text-muted);margin-bottom:28px;font-size:0.87rem'>
    Enter your team password to continue
  </p>
</div>""", unsafe_allow_html=True)

    col = st.columns([1, 2, 1])[1]
    with col:
        with st.form("_login_form", clear_on_submit=True):
            pw = st.text_input("Password", type="password", label_visibility="collapsed",
                               placeholder="Team password…")
            if st.form_submit_button("Sign in →", use_container_width=True, type="primary"):
                if pw == required:
                    st.session_state["_authed"] = True
                    st.rerun()
                else:
                    st.error("Incorrect password.")
    return False


if not _auth_gate():
    st.stop()


# ── Thread sidebar ───────────────────────────────────────────────────────────

from app_pages.thread_store import Thread, delete_thread, list_threads, load_thread

with st.sidebar:
    st.markdown(
        "<div style='font-size:1.05rem;font-weight:600;letter-spacing:-0.01em;"
        "padding:4px 0 10px'>📄 Dataset Builder</div>",
        unsafe_allow_html=True,
    )

    if st.button("＋  New analysis", key="_new_thread", use_container_width=True):
        for k in [k for k in st.session_state if k.startswith("ws_")]:
            del st.session_state[k]
        st.session_state.pop("active_thread_id", None)
        st.rerun()

    st.markdown("<hr style='margin:10px 0'>", unsafe_allow_html=True)
    st.markdown(
        "<div style='font-size:0.68rem;letter-spacing:0.1em;text-transform:uppercase;"
        "color:#6B4F3A;margin-bottom:6px'>Threads</div>",
        unsafe_allow_html=True,
    )

    threads = list_threads()
    active_id = st.session_state.get("active_thread_id")

    if not threads:
        st.markdown(
            "<div style='font-size:0.78rem;color:#6B4F3A;padding:4px 2px'>"
            "No threads yet — start a new analysis.</div>",
            unsafe_allow_html=True,
        )

    for t in threads:
        is_active = active_id == t.thread_id
        label = ("▶  " if is_active else "    ") + t.title[:26]
        if st.button(label, key=f"_th_{t.thread_id}", use_container_width=True,
                     help=f"{t.status} · {t.topic[:60]}"):
            st.session_state["active_thread_id"] = t.thread_id
            # clear in-flight subprocess refs so they don't bleed across threads
            for k in ("ws_proc", "ws_queue"):
                st.session_state.pop(k, None)
            st.rerun()
        st.markdown(
            f"<div style='font-size:0.78rem;color:{t.status_color};"
            f"margin:-8px 0 8px 4px'>● {t.status} · {t.age_label}</div>",
            unsafe_allow_html=True,
        )

    # Sign-out (if auth is active)
    if st.secrets.get("APP_PASSWORD", ""):
        st.markdown("<hr style='margin:10px 0'>", unsafe_allow_html=True)
        if st.button("Sign out", key="_signout", use_container_width=True):
            st.session_state.pop("_authed", None)
            st.rerun()


# ── Main workspace ───────────────────────────────────────────────────────────

from app_pages import workspace

workspace.render(active_thread_id=st.session_state.get("active_thread_id"))

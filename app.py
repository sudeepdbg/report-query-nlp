"""
Foundry Vantage-  v5.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Pages  : Rights Explorer · Title Catalog · Do-Not-Air · Sales · Deals
         Vendors · Work Orders · Gap Analysis · Compare · Alerts
         Title 360 · Chat / Query · Custom Dashboard
Pipeline: 3-stage NL→SQL (query_pipeline.py)
UX     : Interactive SQL chips (query_chips_ui.py)
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time
import uuid
import html
import numpy as np
from datetime import datetime
import logging
from typing import Optional

from utils.database import (
    init_database, execute_sql, get_table_stats,
    save_alert, dismiss_alert, get_alerts,
    log_query, log_feedback, update_session,
)
# ── New pipeline imports (replace old query_parser) ───────────────────────────
from utils.query_pipeline import parse_query, generate, validate, ollama_is_available
from utils.query_chips_ui  import render_chips, chips_query_block

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE CONFIG
# ══════════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Foundry Vantage — Rights Explorer",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ══════════════════════════════════════════════════════════════════════════════
# GLOBAL CSS
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
html,body,*{font-family:'Inter',sans-serif!important;box-sizing:border-box}
#MainMenu,footer,[data-testid="stStatusWidget"],[data-testid="stDecoration"]{display:none!important}

/* ── Layout ─────────────────────────────────────────────────────────────── */
.stApp,[data-testid="stAppViewContainer"],[data-testid="stMain"]{background:#f0f2f5!important}
[data-testid="block-container"]{background:#f0f2f5!important;padding:1.5rem 2rem 5rem!important;max-width:1700px!important}

/* ── Sidebar ────────────────────────────────────────────────────────────── */
[data-testid="stSidebar"]>div:first-child{background:#08101f!important;border-right:1px solid rgba(255,255,255,.05)!important}
[data-testid="stSidebar"] *{color:#94a3b8!important}
[data-testid="stSidebar"] .stButton>button{width:100%!important;text-align:left!important;
  justify-content:flex-start!important;background:transparent!important;border:none!important;
  border-radius:8px!important;color:#94a3b8!important;font-size:.86rem!important;
  font-weight:500!important;padding:.6rem 1rem!important;box-shadow:none!important;transition:all .15s!important}
[data-testid="stSidebar"] .stButton>button:hover{background:rgba(255,255,255,.07)!important;color:#e2e8f0!important}
[data-testid="stSidebar"] .stButton>button:disabled{display:none!important}
[data-testid="stSidebar"] [data-baseweb="select"]>div{background:#111827!important;border:1px solid #1e293b!important;border-radius:8px!important}
[data-testid="stSidebar"] [data-baseweb="select"] span{color:#e2e8f0!important}
[data-testid="stSidebar"] input{background:#111827!important;border:1px solid #1e293b!important;border-radius:8px!important;color:#e2e8f0!important}

/* ── Buttons ────────────────────────────────────────────────────────────── */
section.main .stButton>button{background:#7c3aed!important;color:#fff!important;border:none!important;
  border-radius:8px!important;font-weight:600!important;padding:.5rem 1.2rem!important;transition:all .15s!important}
section.main .stButton>button:hover{background:#6d28d9!important;transform:translateY(-1px)!important}
section.main .stButton>button:disabled{background:#e2e8f0!important;color:#94a3b8!important;transform:none!important}

/* ── Tabs ───────────────────────────────────────────────────────────────── */
[data-testid="stTabs"] [data-baseweb="tab-list"]{background:#e2e8f0!important;border-radius:10px!important;padding:3px!important;border:none!important;gap:2px!important}
[data-testid="stTabs"] [data-baseweb="tab"]{background:transparent!important;color:#64748b!important;border-radius:7px!important;font-size:.82rem!important;font-weight:500!important;border:none!important;padding:6px 14px!important}
[data-testid="stTabs"] [aria-selected="true"]{background:#7c3aed!important;color:#fff!important;font-weight:700!important}

/* ── Metrics ────────────────────────────────────────────────────────────── */
section.main [data-testid="stMetric"]{background:#fff!important;border:1px solid #e2e8f0!important;border-radius:12px!important;padding:.85rem 1rem!important;box-shadow:0 1px 3px rgba(0,0,0,.04)!important}
section.main [data-testid="stMetricLabel"]>div{font-size:.62rem!important;font-weight:700!important;letter-spacing:.07em!important;text-transform:uppercase!important;color:#94a3b8!important}
section.main [data-testid="stMetricValue"]>div{font-size:1.45rem!important;font-weight:800!important;color:#0f172a!important}

/* ── Containers / Inputs ────────────────────────────────────────────────── */
[data-testid="stVerticalBlockBorderWrapper"]{background:#fff!important;border:1px solid #e2e8f0!important;border-radius:12px!important;box-shadow:0 1px 3px rgba(0,0,0,.04)!important}
section.main input,section.main textarea{background:#fff!important;border:1.5px solid #e2e8f0!important;border-radius:8px!important;color:#111827!important}
section.main input:focus,section.main textarea:focus{border-color:#7c3aed!important}
section.main [data-baseweb="select"]>div{background:#fff!important;border:1.5px solid #e2e8f0!important;border-radius:8px!important}
[data-baseweb="popover"],[data-baseweb="menu"]{background:#fff!important;border:1px solid #e2e8f0!important;border-radius:10px!important;box-shadow:0 10px 30px rgba(0,0,0,.1)!important}
[role="option"]{background:#fff!important;color:#111827!important}
[role="option"]:hover{background:#f5f3ff!important}
[role="option"][aria-selected="true"]{background:#ede9fe!important;color:#4c1d95!important}
[data-testid="stDataFrame"]{border:1px solid #e2e8f0!important;border-radius:10px!important;overflow:hidden!important}
[data-testid="stPlotlyChart"]{border:1px solid #e2e8f0!important;border-radius:12px!important;overflow:hidden!important;background:#fff!important}
[data-testid="stExpander"] summary{background:#f8fafc!important;border:1px solid #e2e8f0!important;border-radius:8px!important;font-weight:600!important;color:#374151!important}
[data-testid="stExpander"]>div:last-child{border:1px solid #e2e8f0!important;border-top:none!important;border-radius:0 0 8px 8px!important;background:#fff!important}
hr{border:none!important;border-top:1px solid #e2e8f0!important;margin:.8rem 0!important}
[data-testid="stProgress"]>div{background:#e2e8f0!important}
[data-testid="stProgress"]>div>div{background:#7c3aed!important}

/* ── SQL box ────────────────────────────────────────────────────────────── */
.sql-box{background:#0f172a;color:#e2e8f0;border-radius:8px;padding:14px 18px;
  font-family:'Courier New',monospace;font-size:12.5px;border-left:4px solid #7c3aed;
  margin:8px 0;overflow-x:auto;white-space:pre-wrap}

/* ── Badges ─────────────────────────────────────────────────────────────── */
.badge{display:inline-block;padding:2px 9px;border-radius:20px;font-size:11px;font-weight:600;margin:2px}
.badge-green{background:#dcfce7;color:#166534}.badge-red{background:#fee2e2;color:#991b1b}
.badge-amber{background:#fef3c7;color:#92400e}.badge-blue{background:#dbeafe;color:#1e40af}
.badge-purple{background:#ede9fe;color:#5b21b6}.badge-gray{background:#f1f5f9;color:#475569}

/* ── Page header ────────────────────────────────────────────────────────── */
.page-header{font-size:1.75rem;font-weight:800;color:#0f172a;line-height:1.2;margin-bottom:4px}
.page-sub{font-size:.85rem;color:#64748b;margin-bottom:1rem}

/* ── Stat tile ──────────────────────────────────────────────────────────── */
.stat-tile{background:#fff;border:1px solid #e2e8f0;border-radius:12px;padding:14px 16px;text-align:center;box-shadow:0 1px 3px rgba(0,0,0,.04)}
.stat-tile .val{font-size:1.6rem;font-weight:800;color:#0f172a;line-height:1.1}
.stat-tile .lbl{font-size:.6rem;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:.07em;margin-top:4px}

/* ── Expiry urgency ─────────────────────────────────────────────────────── */
.exp-critical{background:#fee2e2;color:#991b1b;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:700}
.exp-warn{background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:700}
.exp-ok{background:#dcfce7;color:#166534;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:700}

/* ── Dashboard cards ────────────────────────────────────────────────────── */
.db-card-header{background:linear-gradient(135deg,#7c3aed 0%,#4f46e5 100%);padding:10px 14px;
  display:flex;align-items:center;justify-content:space-between}
.db-card-title{font-size:.82rem;font-weight:700;color:#fff;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:85%}
.db-card-ts{font-size:.65rem;color:rgba(255,255,255,.65)}
.db-empty{background:#f8faff;border:2px dashed #c7d2fe;border-radius:14px;padding:36px 24px;text-align:center}
.db-empty-icon{font-size:2.4rem;margin-bottom:8px}
.db-empty-title{font-size:1rem;font-weight:700;color:#4f46e5;margin-bottom:6px}
.db-empty-body{font-size:.82rem;color:#64748b;line-height:1.6}
.db-metric-card{background:linear-gradient(135deg,#7c3aed 0%,#4f46e5 100%);border-radius:14px;padding:20px 24px;text-align:center;color:#fff}
.db-metric-value{font-size:2.6rem;font-weight:800;line-height:1;color:#fff}
.db-metric-label{font-size:.75rem;font-weight:600;color:rgba(255,255,255,.7);text-transform:uppercase;letter-spacing:.08em;margin-top:6px}
.db-metric-sub{font-size:.7rem;color:rgba(255,255,255,.5);margin-top:3px}
.db-query-pill{display:inline-flex;align-items:center;gap:6px;background:#ede9fe;border-radius:20px;
  padding:3px 10px;font-size:.72rem;font-weight:600;color:#5b21b6;margin-right:4px}

/* ── SQL Chips (Enhancement 4) ──────────────────────────────────────────── */
.chip-row{display:flex;flex-wrap:wrap;gap:6px;align-items:center;padding:10px 14px;
  background:#f8faff;border:1px solid #ddd6fe;border-radius:10px;margin-bottom:4px}
.chip-label{font-size:.6rem;font-weight:700;color:#7c3aed;text-transform:uppercase;letter-spacing:.08em;margin-right:3px}
.chip{display:inline-flex;align-items:center;gap:5px;background:#ede9fe;border:1px solid #c4b5fd;
  border-radius:20px;padding:3px 10px;font-size:.78rem;font-weight:600;color:#4c1d95;white-space:nowrap}
.chip-domain{background:#0f172a;border-color:#0f172a;color:#e2e8f0}
.chip-cross{background:#1e3a5f;border-color:#1e3a5f;color:#bfdbfe}
.chip-region{background:#dbeafe;border-color:#93c5fd;color:#1e40af}
.chip-platform{background:#dcfce7;border-color:#86efac;color:#166534}
.chip-date{background:#fef3c7;border-color:#fcd34d;color:#92400e}
.chip-expiry{background:#fee2e2;border-color:#fca5a5;color:#991b1b}
.chip-status{background:#f1f5f9;border-color:#cbd5e1;color:#475569}
.chip-title{background:#fdf4ff;border-color:#e879f9;color:#86198f}
.chip-category{background:#fff7ed;border-color:#fdba74;color:#9a3412}
.chip-hint{font-size:.72rem;color:#64748b;font-style:italic;padding:3px 6px}

/* ── Hybrid intent-method badges ─────────────────────────────────────────── */
.chip-match-llm{display:inline-flex;align-items:center;gap:5px;background:#d1fae5;
  border:1px solid #6ee7b7;color:#065f46;border-radius:20px;padding:3px 11px;
  font-size:.78rem;font-weight:700;white-space:nowrap}
.chip-match-rule{display:inline-flex;align-items:center;gap:5px;background:#ede9fe;
  border:1px solid #c4b5fd;color:#4c1d95;border-radius:20px;padding:3px 11px;
  font-size:.78rem;font-weight:700;white-space:nowrap}
/* ── Sidebar Ollama status pills ─────────────────────────────────────────── */
.ollama-online{display:block;background:rgba(16,185,129,.15);border:1px solid rgba(16,185,129,.35);
  border-radius:8px;padding:5px 10px;font-size:.72rem;color:#6ee7b7;margin:4px 8px}
.ollama-offline{display:block;background:rgba(239,68,68,.12);border:1px solid rgba(239,68,68,.3);
  border-radius:8px;padding:5px 10px;font-size:.72rem;color:#fca5a5;margin:4px 8px}
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# DB + SESSION STATE
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_resource(ttl=3600, show_spinner="Connecting to Rights database…")
def init_db():
    for i in range(3):
        try:
            conn = init_database()
            conn.cursor().execute("SELECT 1")
            return conn
        except Exception as e:
            if i == 2: st.error(str(e)); raise
            time.sleep(2 ** i)

_SS_DEFAULTS = {
    "page":             "rights",
    "chat_history":     [],
    "current_region":   "NA",
    "persona":          "Business Affairs",
    "user_prefs":       {"show_sql": True, "raw_sql_mode": False},
    "db_stats":         {},
    "pending_prompt":   None,
    "title_360":        None,
    "alerts_count":     0,
    "dashboard_pins":   [],
    "dashboard_last_df":   None,
    "dashboard_last_meta": {},
    "session_id":       None,
}
for _k, _v in _SS_DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

# Ensure session_id is always a real UUID (handles None default above)
if not st.session_state.session_id:
    st.session_state.session_id = str(uuid.uuid4())

try:
    DB_CONN = init_db()
    if not st.session_state.db_stats:
        st.session_state.db_stats = get_table_stats(DB_CONN)
    # Bootstrap analytics session (upsert — safe to call every page load)
    update_session(
        DB_CONN,
        session_id = st.session_state.session_id,
        region     = st.session_state.current_region,
        persona    = st.session_state.persona,
    )
except Exception as e:
    st.error(f"⚠️ {e}"); st.stop()

# ══════════════════════════════════════════════════════════════════════════════
# NAVIGATION
# ══════════════════════════════════════════════════════════════════════════════
PAGES = [
    ("rights",       "🔑", "Rights Explorer"),
    ("titles",       "🎬", "Title Catalog"),
    ("dna",          "🚫", "Do-Not-Air"),
    ("sales",        "💸", "Sales Deals"),
    ("deals",        "💼", "Deals"),
    ("vendors",      "🏢", "Vendors"),
    ("work_orders",  "⚙️",  "Work Orders"),
    ("gap_analysis", "🔍", "Gap Analysis"),
    ("compare",      "⚖️",  "Compare Regions"),
    ("alerts",       "🔔", "Alerts"),
    ("title_360",    "🎯", "Title 360"),
    ("chat",         "💬", "Chat / Query"),
    ("dashboard",    "📐", "Custom Dashboard"),
    ("analytics",    "📊", "Analytics"),
]

# ══════════════════════════════════════════════════════════════════════════════
# SHARED HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def run(sql: str) -> pd.DataFrame:
    df, err = execute_sql(sql, DB_CONN)
    if err: st.error(f"SQL error: {err}")
    return df if df is not None else pd.DataFrame()

def run_with_logging(
    sql: str,
    question: str,
    intent: str,
    chart_type: str,
    region_ctx: str,
    cross_intent: bool = False,
) -> tuple:
    """Wraps execute_sql with latency measurement + query_log insertion.
    Returns (df, db_err, log_id)."""
    t0 = time.time()
    df, db_err = execute_sql(sql, DB_CONN)
    latency_ms = (time.time() - t0) * 1000
    log_id = log_query(
        conn          = DB_CONN,
        session_id    = st.session_state.get("session_id", "unknown"),
        user_id       = st.session_state.get("user_id", None),
        region        = region_ctx or st.session_state.get("current_region", "NA"),
        persona       = st.session_state.get("persona", "General"),
        question      = question,
        generated_sql = sql,
        intent_domain = intent or "",
        cross_intent  = cross_intent,
        latency_ms    = latency_ms,
        success       = db_err is None,
        error_message = db_err or "",
        rows_returned = len(df) if df is not None else 0,
        chart_type    = chart_type or "",
    )
    return df, db_err, log_id

def fmt_m(x) -> str:
    try:
        v = float(x)
        if v >= 1e9: return f"${v/1e9:.1f}B"
        if v >= 1e6: return f"${v/1e6:.1f}M"
        if v >= 1e3: return f"${v/1e3:.0f}K"
        return f"${v:.0f}"
    except: return str(x)

def exp_tag(days) -> str:
    try:
        d = int(days)
        if d < 0:   return "🔴 Expired"
        if d <= 30: return f"🔴 {d}d"
        if d <= 60: return f"🟡 {d}d"
        return f"🟢 {d}d"
    except: return "—"

def bool_icon(v) -> str:
    return "✅" if v in (1, "1", True, "Yes", "yes") else "❌"

def stat_tiles(items: list):
    for col, (val, lbl, color) in zip(st.columns(len(items)), items):
        col.markdown(
            f'<div class="stat-tile"><div class="val" style="color:{color}">{val}</div>'
            f'<div class="lbl">{lbl}</div></div>', unsafe_allow_html=True)

# Shared Plotly theme
PT = dict(
    plot_bgcolor="#ffffff", paper_bgcolor="#ffffff",
    font=dict(family="Inter,sans-serif", color="#6b7280", size=11),
    margin=dict(l=20, r=20, t=44, b=20),
    colorway=["#7c3aed","#f59e0b","#10b981","#ef4444","#3b82f6","#a78bfa","#fb923c"],
)

def bar(df, x, y, title="", h=300, horiz=False, color=None):
    kwargs = dict(title=title, color=color, color_discrete_sequence=["#7c3aed"])
    fig = (px.bar(df, x=y, y=x, orientation="h", **kwargs) if horiz
           else px.bar(df, x=x, y=y, **kwargs))
    fig.update_layout(**PT, height=h); fig.update_xaxes(tickangle=-30)
    return fig

def pie(df, names, values, title="", h=300):
    fig = px.pie(df, names=names, values=values, title=title, hole=0.42)
    fig.update_layout(**PT, height=h)
    return fig

# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("""
    <div style="padding:20px 16px 14px;border-bottom:1px solid rgba(255,255,255,.05)">
      <div style="display:flex;align-items:center;gap:10px">
        <div style="width:36px;height:36px;background:#7c3aed;border-radius:9px;
          display:flex;align-items:center;justify-content:center;font-size:18px;flex-shrink:0">🔑</div>
        <div>
          <div style="font-size:1.05rem;font-weight:800;color:#f1f5f9;letter-spacing:-.02em">Foundry Vantage</div>
          <div style="font-size:.6rem;color:#475569;letter-spacing:.1em;text-transform:uppercase">· v5.0</div>
        </div>
      </div>
    </div>""", unsafe_allow_html=True)

    for pid, icon, label in PAGES:
        if st.session_state.page == pid:
            st.markdown(
                f'<div style="display:flex;align-items:center;gap:10px;background:rgba(124,58,237,.2);'
                f'border:1px solid rgba(124,58,237,.5);border-radius:8px;padding:9px 14px;margin:2px 8px">'
                f'<span style="font-size:14px">{icon}</span>'
                f'<span style="font-size:.86rem;font-weight:700;color:#c4b5fd">{label}</span></div>',
                unsafe_allow_html=True)
        else:
            if st.button(f"{icon}  {label}", key=f"nav_{pid}", use_container_width=True):
                st.session_state.page = pid; st.rerun()

    st.markdown('<hr style="border:none;border-top:1px solid rgba(255,255,255,.05);margin:8px 16px">', unsafe_allow_html=True)

    st.markdown('<div style="padding:0 8px">', unsafe_allow_html=True)
    _regions = ["NA","APAC","EMEA","LATAM"]
    def _on_region(): st.session_state.current_region = st.session_state._reg_sel
    st.selectbox("Region / Market", _regions, index=_regions.index(st.session_state.current_region),
                 key="_reg_sel", on_change=_on_region)
    _personas = ["Business Affairs","Strategy","Legal","Operations","Analytics"]
    def _on_persona(): st.session_state.persona = st.session_state._per_sel
    st.selectbox("Persona", _personas,
                 index=_personas.index(st.session_state.persona) if st.session_state.persona in _personas else 0,
                 key="_per_sel", on_change=_on_persona)
    st.markdown('</div>', unsafe_allow_html=True)

    stats = st.session_state.db_stats
    alerts_live, _ = get_alerts(DB_CONN, region=st.session_state.current_region)
    st.session_state.alerts_count = len(alerts_live) if alerts_live is not None else 0

    _pairs = [("title","Titles"),("movie","Movies"),("media_rights","Rights"),("do_not_air","DNA")]
    _sh = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:5px;margin:10px 8px">'
    for k, l in _pairs:
        _sh += (f'<div style="background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.07);'
                f'border-radius:8px;padding:8px;text-align:center">'
                f'<div style="font-size:1.1rem;font-weight:800;color:#e2e8f0">{stats.get(k,0):,}</div>'
                f'<div style="font-size:.58rem;color:#475569;text-transform:uppercase;letter-spacing:.06em">{l}</div></div>')
    st.markdown(_sh + '</div>', unsafe_allow_html=True)

    ac = st.session_state.alerts_count
    if ac > 0:
        st.markdown(f'<div style="margin:0 8px 4px;background:rgba(239,68,68,.12);border:1px solid rgba(239,68,68,.3);'
                    f'border-radius:8px;padding:6px 10px;font-size:.75rem;color:#fca5a5">'
                    f'🔔 <b>{ac} alert{"s" if ac!=1 else ""}</b> — click Alerts</div>', unsafe_allow_html=True)

    pc = len(st.session_state.dashboard_pins)
    if pc > 0:
        st.markdown(f'<div style="margin:0 8px 4px;background:rgba(124,58,237,.12);border:1px solid rgba(124,58,237,.3);'
                    f'border-radius:8px;padding:6px 10px;font-size:.75rem;color:#c4b5fd">'
                    f'📐 <b>{pc} pinned chart{"s" if pc!=1 else ""}</b></div>', unsafe_allow_html=True)

    st.markdown(f'<div style="margin:0 8px 8px;background:rgba(124,58,237,.1);border:1px solid rgba(124,58,237,.2);'
                f'border-radius:8px;padding:8px 10px;font-size:.75rem;color:#c4b5fd">'
                f'📍 <b>{st.session_state.current_region}</b> · {st.session_state.persona}<br>'
                f'<span style="color:#64748b;font-size:.68rem">HBO/Cinemax/HBO Max</span></div>',
                unsafe_allow_html=True)

    # ── Ollama / LLM status ────────────────────────────────────────────────────
    _ollama_ok = ollama_is_available()
    if _ollama_ok:
        st.markdown(
            '<span class="ollama-online">🤖 <b>Ollama online</b> · LLM intent active</span>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<span class="ollama-offline">📐 <b>Ollama offline</b> · rule-based fallback</span>',
            unsafe_allow_html=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# RIGHTS EXPLORER
# ══════════════════════════════════════════════════════════════════════════════
def page_rights():
    reg = st.session_state.current_region
    st.markdown('<div class="page-header">🔑 Rights Explorer</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">Content rights licensed-in for <b>{reg}</b> — windows, territories, exclusivity &amp; expiry</div>', unsafe_allow_html=True)

    kpi = run(f"""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN status='Active' THEN 1 ELSE 0 END) AS active,
               SUM(CASE WHEN status='Expired' THEN 1 ELSE 0 END) AS expired,
               SUM(CASE WHEN term_to<=DATE('now','+30 days') AND term_to>=DATE('now') AND status='Active' THEN 1 ELSE 0 END) AS exp30,
               SUM(CASE WHEN term_to<=DATE('now','+90 days') AND term_to>=DATE('now') AND status='Active' THEN 1 ELSE 0 END) AS exp90,
               COUNT(DISTINCT title_id) AS titles_covered,
               SUM(exclusivity) AS exclusive_count
        FROM media_rights WHERE UPPER(region)='{reg}'
    """)
    if not kpi.empty:
        r = kpi.iloc[0]
        stat_tiles([
            (f"{int(r.get('total',0)):,}",         "Total Rights",     "#0f172a"),
            (f"{int(r.get('active',0)):,}",         "Active",           "#166534"),
            (f"{int(r.get('exp30',0)):,}",          "⚠ Expiring 30d",  "#991b1b"),
            (f"{int(r.get('exp90',0)):,}",          "Expiring 90d",     "#92400e"),
            (f"{int(r.get('expired',0)):,}",        "Expired",          "#64748b"),
            (f"{int(r.get('titles_covered',0)):,}", "Titles Covered",   "#1e40af"),
            (f"{int(r.get('exclusive_count',0)):,}","Exclusive",        "#5b21b6"),
        ])
    st.divider()

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "⏰ Expiry Alerts","📺 Windows & Platforms","🌍 Territories",
        "🔒 Holdbacks","⭐ Exclusivity","📄 Rights Table"])

    with tab1:
        st.markdown("#### Rights Expiring — Upcoming Windows")
        c1, c2, c3 = st.columns([2,1,1])
        days_sel = c1.slider("Expiring within (days)", 7, 180, 90, key="exp_days")
        plat_sel = c2.multiselect("Platform", ["PayTV","STB-VOD","SVOD","FAST"], key="exp_plat")
        rt_sel   = c3.selectbox("Rights Type", ["All","Exhibition","Exhibition & Distribution"], key="exp_rt")
        plat_f = ("AND ("+  " OR ".join(f"media_platform_primary LIKE '%{p}%'" for p in plat_sel)+")" if plat_sel else "")
        rt_f   = f"AND rights_type='{rt_sel}'" if rt_sel != "All" else ""

        exp_df = run(f"""
            SELECT mr.rights_id, mr.title_name, t.series_id, cd.deal_source, cd.deal_name,
                   mr.rights_type, mr.media_platform_primary, mr.territories, mr.language,
                   mr.exclusivity, mr.holdback, mr.holdback_days,
                   mr.term_to AS expiry_date,
                   CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_remaining,
                   mr.notes_restrictive, mr.status
            FROM media_rights mr
            JOIN content_deal cd ON mr.deal_id=cd.deal_id
            JOIN title t ON mr.title_id=t.title_id
            WHERE UPPER(mr.region)='{reg}' AND mr.status='Active'
              AND mr.term_to<=DATE('now','+{days_sel} days') AND mr.term_to>=DATE('now')
              {plat_f} {rt_f}
            ORDER BY mr.term_to ASC""")

        if exp_df.empty:
            st.success(f"✅ No rights expiring within {days_sel} days in {reg}.")
        else:
            c1, c2 = st.columns([3,1])
            with c1:
                fig = go.Figure()
                clrs = {"PayTV":"#3b82f6","STB-VOD":"#f59e0b","SVOD":"#7c3aed","FAST":"#10b981"}
                for plat in exp_df["media_platform_primary"].unique():
                    sub = exp_df[exp_df["media_platform_primary"]==plat]
                    fig.add_bar(y=sub["title_name"], x=sub["days_remaining"], name=plat,
                                orientation="h", marker_color=clrs.get(plat,"#94a3b8"))
                fig.update_layout(**PT, height=max(300,len(exp_df)*18),
                                  title=f"Days to Expiry — {len(exp_df)} rights",
                                  xaxis_title="Days Remaining", barmode="group")
                st.plotly_chart(fig, use_container_width=True)
            with c2:
                st.markdown("**By Platform**")
                for p, cnt in exp_df.groupby("media_platform_primary").size().items():
                    st.markdown(f"- **{p}**: {cnt}")
                st.markdown(f"**Total: {len(exp_df)}** · **Exclusive: {int(exp_df['exclusivity'].sum())}**")

            show = exp_df.copy()
            show["Days Left"]    = show["days_remaining"].apply(exp_tag)
            show["Exclusive"]    = show["exclusivity"].apply(bool_icon)
            show["Holdback"]     = show["holdback"].apply(bool_icon)
            show["Restrictions"] = show["notes_restrictive"].fillna("—")
            st.dataframe(show[["title_name","series_id","deal_source","media_platform_primary",
                                "territories","language","expiry_date","Days Left","Exclusive","Holdback","holdback_days","Restrictions"]],
                use_container_width=True, hide_index=True)
            ca, cb = st.columns([3,1])
            ca.download_button("📥 Export", exp_df.to_csv(index=False), f"expiry_{reg}_{days_sel}d.csv","text/csv")
            with cb:
                pl = f"{', '.join(plat_sel) if plat_sel else 'All'}"
                if st.button(f"🔔 Set Alert — {days_sel}d / {pl}", key="set_exp_alert"):
                    _, err = save_alert(DB_CONN,"Expiry",f"Rights expiring {days_sel}d ({pl}) [{reg}]",
                                        region=reg,platform=pl,days_threshold=days_sel,persona=st.session_state.persona)
                    if err: st.error(err)
                    else:   st.success("🔔 Alert saved!"); st.session_state.alerts_count += 1

    with tab2:
        c1, c2 = st.columns(2)
        with c1:
            df = run(f"SELECT media_platform_primary AS platform, status, COUNT(*) AS count, COUNT(DISTINCT title_id) AS titles FROM media_rights WHERE UPPER(region)='{reg}' GROUP BY platform,status ORDER BY count DESC")
            if not df.empty:
                fig = px.bar(df, x="platform", y="titles", color="status", title="Titles by Platform & Status",
                             color_discrete_map={"Active":"#10b981","Expired":"#ef4444","Pending":"#f59e0b","Suspended":"#94a3b8"})
                fig.update_layout(**PT, height=320, barmode="stack"); st.plotly_chart(fig, use_container_width=True)
        with c2:
            df = run(f"SELECT media_platform_primary AS platform, rights_type, COUNT(*) AS count FROM media_rights WHERE UPPER(region)='{reg}' GROUP BY platform,rights_type ORDER BY count DESC")
            if not df.empty: st.plotly_chart(pie(df,"platform","count","Rights Mix by Platform"), use_container_width=True)

        st.markdown("#### Ancillary Platform Coverage")
        df = run(f"SELECT media_platform_ancillary, COUNT(*) AS count FROM media_rights WHERE UPPER(region)='{reg}' AND media_platform_ancillary!='' GROUP BY media_platform_ancillary ORDER BY count DESC")
        if not df.empty:
            rows = []
            for _, row in df.iterrows():
                for p in str(row["media_platform_ancillary"]).split(","):
                    p = p.strip()
                    if p: rows.append({"ancillary_platform":p,"count":row["count"]})
            if rows:
                df2 = pd.DataFrame(rows).groupby("ancillary_platform")["count"].sum().reset_index().sort_values("count",ascending=False)
                st.plotly_chart(bar(df2,"ancillary_platform","count","Ancillary Rights Count",h=280), use_container_width=True)

        st.markdown("#### Rights by Deal Source (TRL / C2 / FRL)")
        df = run(f"SELECT cd.deal_source, COUNT(DISTINCT mr.title_id) AS titles, COUNT(*) AS rights, SUM(mr.exclusivity) AS exclusive, SUM(CASE WHEN mr.status='Active' THEN 1 ELSE 0 END) AS active FROM media_rights mr JOIN content_deal cd ON mr.deal_id=cd.deal_id WHERE UPPER(mr.region)='{reg}' GROUP BY cd.deal_source ORDER BY titles DESC")
        if not df.empty:
            c1, c2 = st.columns(2)
            with c1: st.plotly_chart(bar(df,"deal_source","titles","Titles by Deal Source",h=280), use_container_width=True)
            with c2: st.dataframe(df, use_container_width=True, hide_index=True)

    with tab3:
        raw = run(f"SELECT territories, COUNT(*) AS rights_count, COUNT(DISTINCT title_id) AS titles, SUM(exclusivity) AS exclusive, SUM(CASE WHEN status='Active' THEN 1 ELSE 0 END) AS active FROM media_rights WHERE UPPER(region)='{reg}' GROUP BY territories")
        if not raw.empty:
            rows = []
            for _, row in raw.iterrows():
                for t in str(row["territories"]).split(","):
                    t = t.strip()
                    if t: rows.append({"territory":t,"rights_count":row["rights_count"],"titles":row["titles"],"exclusive":row["exclusive"],"active":row["active"]})
            if rows:
                tdf = (pd.DataFrame(rows).groupby("territory").agg(rights_count=("rights_count","sum"),titles=("titles","sum"),exclusive=("exclusive","sum"),active=("active","sum")).reset_index().sort_values("rights_count",ascending=False))
                c1, c2 = st.columns(2)
                with c1: st.plotly_chart(bar(tdf.head(15),"territory","rights_count","Rights Count by Territory",h=320,horiz=True), use_container_width=True)
                with c2: st.plotly_chart(bar(tdf.head(15),"territory","titles","Titles by Territory",h=320,horiz=True), use_container_width=True)
                st.dataframe(tdf, use_container_width=True, hide_index=True)

    with tab4:
        df = run(f"SELECT media_platform_primary AS platform, COUNT(*) AS with_holdback, AVG(holdback_days) AS avg_holdback_days, MAX(holdback_days) AS max_holdback_days FROM media_rights WHERE UPPER(region)='{reg}' AND holdback=1 AND holdback_days>0 GROUP BY platform ORDER BY avg_holdback_days DESC")
        if not df.empty:
            c1, c2 = st.columns(2)
            with c1: st.plotly_chart(bar(df,"platform","avg_holdback_days","Avg Holdback Days",h=300), use_container_width=True)
            with c2: st.dataframe(df, use_container_width=True, hide_index=True)
        df2 = run(f"SELECT media_platform_primary AS platform, SUM(CASE WHEN holdback=1 THEN 1 ELSE 0 END) AS has_holdback, SUM(CASE WHEN holdback=0 THEN 1 ELSE 0 END) AS no_holdback FROM media_rights WHERE UPPER(region)='{reg}' GROUP BY platform ORDER BY has_holdback DESC")
        if not df2.empty:
            fig = go.Figure()
            fig.add_bar(x=df2["platform"], y=df2["has_holdback"], name="Has Holdback", marker_color="#ef4444")
            fig.add_bar(x=df2["platform"], y=df2["no_holdback"],  name="No Holdback",  marker_color="#10b981")
            fig.update_layout(**PT, height=300, barmode="group", title="Holdback vs No-Holdback")
            st.plotly_chart(fig, use_container_width=True)

    with tab5:
        df = run(f"SELECT media_platform_primary AS platform, SUM(exclusivity) AS exclusive, COUNT(*)-SUM(exclusivity) AS non_exclusive, COUNT(*) AS total FROM media_rights WHERE UPPER(region)='{reg}' GROUP BY platform ORDER BY exclusive DESC")
        if not df.empty:
            c1, c2 = st.columns(2)
            with c1:
                fig = go.Figure()
                fig.add_bar(x=df["platform"], y=df["exclusive"],    name="Exclusive",     marker_color="#7c3aed")
                fig.add_bar(x=df["platform"], y=df["non_exclusive"], name="Non-Exclusive", marker_color="#c4b5fd")
                fig.update_layout(**PT, height=300, barmode="stack", title="Exclusivity by Platform")
                st.plotly_chart(fig, use_container_width=True)
            with c2:
                df["excl_pct"] = (df["exclusive"]/df["total"]*100).round(1)
                st.dataframe(df[["platform","total","exclusive","non_exclusive","excl_pct"]], use_container_width=True, hide_index=True)
        df3 = run(f"SELECT rights_type, SUM(exclusivity) AS exclusive, COUNT(*)-SUM(exclusivity) AS non_exclusive FROM media_rights WHERE UPPER(region)='{reg}' GROUP BY rights_type")
        if not df3.empty: st.plotly_chart(pie(df3,"rights_type","exclusive","Exclusivity by Rights Type",h=280), use_container_width=True)

    with tab6:
        f1,f2,f3,f4 = st.columns(4)
        pf  = f1.selectbox("Platform",    ["All","PayTV","STB-VOD","SVOD","FAST"], key="rt2_plat")
        sf  = f2.selectbox("Status",      ["All","Active","Expired","Pending"],    key="rt2_stat")
        ef  = f3.selectbox("Exclusivity", ["All","Exclusive","Non-Exclusive"],     key="rt2_excl")
        srf = f4.selectbox("Deal Source", ["All","TRL","C2","FRL"],                key="rt2_src")
        ex = ""
        if pf  != "All": ex += f" AND mr.media_platform_primary LIKE '%{pf}%'"
        if sf  != "All": ex += f" AND mr.status='{sf}'"
        if ef  == "Exclusive":     ex += " AND mr.exclusivity=1"
        if ef  == "Non-Exclusive": ex += " AND mr.exclusivity=0"
        if srf != "All":           ex += f" AND cd.deal_source='{srf}'"
        df = run(f"""
            SELECT mr.rights_id, mr.title_name, t.series_id, cd.deal_source, mr.rights_type,
                   mr.media_platform_primary, mr.territories, mr.language, mr.brand,
                   mr.term_from, mr.term_to,
                   CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_remaining,
                   mr.status
            FROM media_rights mr JOIN content_deal cd ON mr.deal_id=cd.deal_id
            JOIN title t ON mr.title_id=t.title_id
            WHERE UPPER(mr.region)='{reg}' {ex} ORDER BY mr.term_to ASC LIMIT 400""")
        if not df.empty:
            df["Days Left"] = df["days_remaining"].apply(exp_tag)
            st.caption(f"{len(df)} rights records")
            st.dataframe(df[["title_name","series_id","deal_source","rights_type","media_platform_primary",
                              "territories","language","brand","term_from","term_to","Days Left","status"]],
                use_container_width=True, hide_index=True)
            st.download_button("📥 Export CSV", df.to_csv(index=False), f"rights_{reg}.csv","text/csv")


# ══════════════════════════════════════════════════════════════════════════════
# TITLE CATALOG
# ══════════════════════════════════════════════════════════════════════════════
def page_titles():
    reg = st.session_state.current_region
    st.markdown('<div class="page-header">🎬 Title Catalog</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">Series · Movies · Episodes · {reg}</div>', unsafe_allow_html=True)

    kpi = run(f"SELECT COUNT(*) AS total, COUNT(DISTINCT series_id) AS series_count, SUM(CASE WHEN title_type='Episode' THEN 1 ELSE 0 END) AS episodes, SUM(CASE WHEN title_type='Movie' THEN 1 ELSE 0 END) AS movies, SUM(CASE WHEN title_type='Special' THEN 1 ELSE 0 END) AS specials FROM title WHERE UPPER(region)='{reg}'")
    mv_kpi = run("SELECT COUNT(*) AS total, SUM(box_office_gross_usd_m) AS total_bo FROM movie")
    if not kpi.empty:
        r = kpi.iloc[0]; mr = mv_kpi.iloc[0] if not mv_kpi.empty else {}
        stat_tiles([(f"{int(r.get('total',0)):,}","Total Titles","#0f172a"),
                    (f"{int(mr.get('total',0)):,}","Films in Slate","#1e40af"),
                    (f"{int(r.get('series_count',0)):,}","Series","#5b21b6"),
                    (f"{int(r.get('episodes',0)):,}","Episodes","#166534"),
                    (f"${mr.get('total_bo',0)/1000:.1f}B","Total Box Office","#92400e"),
                    (f"{int(r.get('specials',0)):,}","Specials","#64748b")])
    st.divider()

    tab1, tab2, tab3, tab4 = st.tabs(["📁 TV Hierarchy","🎥 Movies","🎭 Genre & Metadata","🔍 Search All"])

    with tab1:
        df = run("SELECT s.series_id, s.series_title, s.series_source, s.controlling_entity, s.genre, COUNT(DISTINCT se.season_id) AS seasons, COUNT(DISTINCT t.title_id) AS total_titles FROM series s LEFT JOIN season se ON s.series_id=se.series_id LEFT JOIN title t ON se.season_id=t.season_id GROUP BY s.series_id ORDER BY total_titles DESC")
        if not df.empty:
            sel = st.selectbox("Drill into series", ["—"]+df["series_title"].tolist(), key="sel_series")
            c1, c2 = st.columns(2)
            with c1: st.plotly_chart(bar(df.head(15),"series_title","total_titles","Titles per Series",h=320,horiz=True), use_container_width=True)
            with c2: st.plotly_chart(pie(df,"genre","total_titles","Genre Mix",h=320), use_container_width=True)
            if sel != "—":
                s_row = df[df["series_title"]==sel].iloc[0]
                st.markdown(f"#### {sel} — Detail")
                sea_df = run(f"SELECT se.season_number, se.episode_count, se.release_year, COUNT(t.title_id) AS titles_in_db, SUM(CASE WHEN mr.status='Active' THEN 1 ELSE 0 END) AS active_rights FROM season se LEFT JOIN title t ON se.season_id=t.season_id LEFT JOIN media_rights mr ON t.title_id=mr.title_id WHERE se.series_id='{s_row['series_id']}' GROUP BY se.season_number ORDER BY se.season_number")
                if not sea_df.empty: st.dataframe(sea_df, use_container_width=True, hide_index=True)

    with tab2:
        c1, c2, c3 = st.columns(3)
        cat_f  = c1.selectbox("Category",["All","Theatrical","Library","HBO Original"],key="mv_cat")
        gen_f  = c2.selectbox("Genre",   ["All","Action","Drama","Comedy","Sci-Fi","Fantasy","Thriller","Historical","Animation"],key="mv_genre")
        ronly  = c3.checkbox("Active rights only",key="mv_rights")
        ex = ""
        if cat_f != "All": ex += f" AND m.content_category='{cat_f}'"
        if gen_f != "All": ex += f" AND m.genre='{gen_f}'"
        mv = run(f"SELECT m.movie_id, m.movie_title, m.content_category, m.genre, m.franchise, m.box_office_gross_usd_m AS box_office, m.age_rating, m.release_year, COUNT(DISTINCT mr.rights_id) AS total_rights, SUM(CASE WHEN mr.status='Active' THEN 1 ELSE 0 END) AS active_rights, SUM(CASE WHEN mr.status='Active' AND mr.term_to<=DATE('now','+90 days') THEN 1 ELSE 0 END) AS expiring_90d FROM movie m LEFT JOIN title t ON t.movie_id=m.movie_id LEFT JOIN media_rights mr ON mr.title_id=t.title_id WHERE 1=1 {ex} GROUP BY m.movie_id {'HAVING active_rights>0' if ronly else ''} ORDER BY m.box_office_gross_usd_m DESC")
        if not mv.empty:
            stat_tiles([(f"{len(mv)}","Films","#0f172a"),(f"${mv['box_office'].sum()/1000:.1f}B","Total Box Office","#1e40af"),(f"{int(mv['active_rights'].sum())}","Active Rights","#166534"),(f"{int(mv['expiring_90d'].sum())}","Expiring 90d","#991b1b")])
            c1, c2 = st.columns(2)
            with c1: st.plotly_chart(bar(mv.head(15),"movie_title","box_office","Box Office Gross ($M)",h=400,horiz=True), use_container_width=True)
            with c2:
                cg = mv.groupby("content_category")["box_office"].sum().reset_index()
                st.plotly_chart(pie(cg,"content_category","box_office","Value by Category",h=400), use_container_width=True)
            fr = mv[mv["franchise"].notna()]
            if not fr.empty:
                fg = fr.groupby("franchise").agg(movies=("movie_id","count"),box_office=("box_office","sum")).reset_index()
                st.plotly_chart(bar(fg,"franchise","box_office","Franchise Box Office ($M)",h=260), use_container_width=True)
            mv["⚠ Expiring"] = mv["expiring_90d"].apply(lambda x: f"🔴 {int(x)}" if x and int(x)>0 else "—")
            st.dataframe(mv[["movie_title","content_category","genre","franchise","box_office","age_rating","release_year","total_rights","active_rights","⚠ Expiring"]], use_container_width=True, hide_index=True)
            st.download_button("📥 Export", mv.to_csv(index=False),"movies.csv","text/csv")

    with tab3:
        c1, c2 = st.columns(2)
        with c1:
            df = run(f"SELECT genre, COUNT(*) AS count FROM title WHERE UPPER(region)='{reg}' GROUP BY genre ORDER BY count DESC")
            if not df.empty: st.plotly_chart(pie(df,"genre","count","Titles by Genre"), use_container_width=True)
        with c2:
            df = run(f"SELECT age_rating, COUNT(*) AS count FROM title WHERE UPPER(region)='{reg}' GROUP BY age_rating ORDER BY count DESC")
            if not df.empty: st.plotly_chart(pie(df,"age_rating","count","Age Ratings"), use_container_width=True)
        df = run(f"SELECT t.title_name, t.genre, t.controlling_entity, COUNT(DISTINCT mr.rights_id) AS rights_count, GROUP_CONCAT(DISTINCT mr.media_platform_primary) AS platforms, SUM(mr.exclusivity) AS exclusive, SUM(CASE WHEN mr.status='Active' THEN 1 ELSE 0 END) AS active_rights, CASE WHEN d.dna_id IS NOT NULL THEN '🚫' ELSE '✅' END AS dna_status FROM title t LEFT JOIN media_rights mr ON t.title_id=mr.title_id LEFT JOIN do_not_air d ON t.title_id=d.title_id AND d.active=1 WHERE UPPER(t.region)='{reg}' GROUP BY t.title_id ORDER BY rights_count DESC NULLS LAST LIMIT 100")
        if not df.empty: st.dataframe(df, use_container_width=True, hide_index=True)

    with tab4:
        sq = st.text_input("Search title name", placeholder="e.g. House of the Dragon…", key="title_search")
        if sq:
            safe = sq.replace("'","''").replace(";","").replace("--","")[:200]
            df = run(f"SELECT t.title_id, t.title_name, t.title_type, t.genre, t.release_year, t.controlling_entity, s.series_title, se.season_number, t.episode_number, COUNT(DISTINCT mr.rights_id) AS rights_count FROM title t LEFT JOIN season se ON t.season_id=se.season_id LEFT JOIN series s ON t.series_id=s.series_id LEFT JOIN media_rights mr ON t.title_id=mr.title_id WHERE LOWER(t.title_name) LIKE '%{safe.lower()}%' GROUP BY t.title_id ORDER BY s.series_title, se.season_number, t.episode_number LIMIT 100")
            if not df.empty:
                st.caption(f"{len(df)} results")
                sel = st.selectbox("Select for Title 360", ["—"]+df["title_name"].tolist(), key="search_360_sel")
                if sel != "—" and st.button(f"🔍 View Title 360 — {sel}", key="search_360_btn"):
                    st.session_state.title_360 = sel; st.session_state.page = "title_360"; st.rerun()
                st.dataframe(df, use_container_width=True, hide_index=True)
            else:
                st.warning("No titles found.")


# ══════════════════════════════════════════════════════════════════════════════
# DO-NOT-AIR
# ══════════════════════════════════════════════════════════════════════════════
def page_dna():
    reg = st.session_state.current_region
    st.markdown('<div class="page-header">🚫 Do-Not-Air Restrictions</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">Active DNA flags · {reg}</div>', unsafe_allow_html=True)
    kpi = run(f"SELECT COUNT(*) AS total, COUNT(DISTINCT title_id) AS titles, COUNT(DISTINCT reason_category) AS categories FROM do_not_air WHERE UPPER(region)='{reg}' AND active=1")
    if not kpi.empty:
        r = kpi.iloc[0]
        stat_tiles([(f"{int(r.get('total',0)):,}","Active DNA Records","#991b1b"),(f"{int(r.get('titles',0)):,}","Affected Titles","#92400e"),(f"{int(r.get('categories',0)):,}","Restriction Types","#64748b")])
    st.divider()
    tab1, tab2 = st.tabs(["📊 Analysis","📄 DNA Table"])
    with tab1:
        c1, c2 = st.columns(2)
        with c1:
            df = run(f"SELECT reason_category, COUNT(*) AS count FROM do_not_air WHERE UPPER(region)='{reg}' AND active=1 GROUP BY reason_category ORDER BY count DESC")
            if not df.empty: st.plotly_chart(pie(df,"reason_category","count","DNA by Reason Category"), use_container_width=True)
        with c2:
            df = run(f"SELECT reason_subcategory, COUNT(*) AS count FROM do_not_air WHERE UPPER(region)='{reg}' AND active=1 GROUP BY reason_subcategory ORDER BY count DESC LIMIT 12")
            if not df.empty: st.plotly_chart(bar(df,"reason_subcategory","count","DNA by Sub-Category",h=300,horiz=True), use_container_width=True)
        df = run(f"SELECT territory, COUNT(*) AS count FROM do_not_air WHERE UPPER(region)='{reg}' AND active=1 GROUP BY territory ORDER BY count DESC")
        if not df.empty:
            rows = []
            for _, row in df.iterrows():
                for t in str(row["territory"]).split(","):
                    t = t.strip()
                    if t: rows.append({"territory":t,"count":row["count"]})
            if rows:
                tdf = pd.DataFrame(rows).groupby("territory")["count"].sum().reset_index().sort_values("count",ascending=False)
                st.plotly_chart(bar(tdf.head(12),"territory","count","DNA by Territory",h=280), use_container_width=True)
    with tab2:
        df = run(f"SELECT dna.dna_id, dna.title_name, t.series_id, dna.reason_category, dna.reason_subcategory, dna.territory, dna.media_type, dna.term_from, dna.term_to, dna.additional_notes FROM do_not_air dna JOIN title t ON dna.title_id=t.title_id WHERE UPPER(dna.region)='{reg}' AND dna.active=1 ORDER BY dna.reason_category, dna.title_name")
        if not df.empty:
            st.caption(f"{len(df)} active DNA records")
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.download_button("📥 Export DNA List", df.to_csv(index=False), f"dna_{reg}.csv","text/csv")


# ══════════════════════════════════════════════════════════════════════════════
# SALES DEALS
# ══════════════════════════════════════════════════════════════════════════════
def page_sales():
    reg = st.session_state.current_region
    st.markdown('<div class="page-header">💸 Sales Deals — Rights Out</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">Affiliate &amp; 3rd-party sales · {reg}</div>', unsafe_allow_html=True)
    kpi = run(f"SELECT COUNT(*) AS total, SUM(CASE WHEN status='Active' THEN 1 ELSE 0 END) AS active, SUM(deal_value) AS total_value, COUNT(DISTINCT buyer) AS buyers, COUNT(DISTINCT title_id) AS titles FROM sales_deal WHERE UPPER(region)='{reg}'")
    if not kpi.empty:
        r = kpi.iloc[0]
        stat_tiles([(f"{int(r.get('total',0)):,}","Total Sales Deals","#0f172a"),(f"{int(r.get('active',0)):,}","Active","#166534"),(fmt_m(r.get("total_value",0)),"Total Value","#1e40af"),(f"{int(r.get('buyers',0)):,}","Buyers","#5b21b6"),(f"{int(r.get('titles',0)):,}","Titles Sold","#92400e")])
    st.divider()
    tab1, tab2 = st.tabs(["📊 Analytics","📄 Deal Table"])
    with tab1:
        c1, c2 = st.columns(2)
        with c1:
            df = run(f"SELECT buyer, COUNT(*) AS deals, SUM(deal_value) AS total_value FROM sales_deal WHERE UPPER(region)='{reg}' AND status='Active' GROUP BY buyer ORDER BY total_value DESC LIMIT 12")
            if not df.empty: st.plotly_chart(bar(df,"buyer","total_value","Active Deal Value by Buyer",h=320,horiz=True), use_container_width=True)
        with c2:
            df = run(f"SELECT deal_type, COUNT(*) AS count, SUM(deal_value) AS value FROM sales_deal WHERE UPPER(region)='{reg}' GROUP BY deal_type ORDER BY value DESC")
            if not df.empty: st.plotly_chart(pie(df,"deal_type","count","Sales by Deal Type"), use_container_width=True)
    with tab2:
        f1, f2 = st.columns(2)
        sf = f1.selectbox("Status",["All","Active","Expired"],key="sd_st_f")
        df_f = f2.selectbox("Type",["All","Affiliate Sales","3rd Party Sales"],key="sd_dt_f")
        ex = ("" + (f" AND status='{sf}'" if sf!="All" else "") + (f" AND deal_type='{df_f}'" if df_f!="All" else ""))
        df = run(f"SELECT sd.sales_deal_id, sd.deal_type, sd.title_name, sd.buyer, sd.territory, sd.media_platform, sd.term_from, sd.term_to, sd.deal_value, sd.currency, sd.status FROM sales_deal sd WHERE UPPER(region)='{reg}' {ex} ORDER BY sd.deal_value DESC LIMIT 300")
        if not df.empty:
            st.caption(f"{len(df)} records")
            st.dataframe(df, use_container_width=True, hide_index=True, column_config={"deal_value":st.column_config.NumberColumn("Value",format="$%,.0f")})


# ══════════════════════════════════════════════════════════════════════════════
# WORK ORDERS
# ══════════════════════════════════════════════════════════════════════════════
def page_work_orders():
    reg = st.session_state.current_region
    st.markdown('<div class="page-header">⚙️ Work Orders</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">Operational pipeline — vendor tasks, QC, localization · {reg}</div>', unsafe_allow_html=True)
    kpi = run(f"SELECT COUNT(*) AS total, SUM(CASE WHEN status='In Progress' THEN 1 ELSE 0 END) AS in_progress, SUM(CASE WHEN status='Delayed' THEN 1 ELSE 0 END) AS delayed, SUM(CASE WHEN status='Completed' THEN 1 ELSE 0 END) AS completed, AVG(quality_score) AS avg_quality, SUM(cost) AS total_cost FROM work_orders WHERE UPPER(region)='{reg}'")
    if not kpi.empty:
        r = kpi.iloc[0]
        stat_tiles([(f"{int(r.get('total',0)):,}","Total Orders","#0f172a"),(f"{int(r.get('in_progress',0)):,}","In Progress","#1e40af"),(f"{int(r.get('delayed',0)):,}","Delayed","#991b1b"),(f"{int(r.get('completed',0)):,}","Completed","#166534"),(f"{r.get('avg_quality',0):.1f}","Avg Quality","#5b21b6"),(fmt_m(r.get("total_cost",0)),"Total Cost","#92400e")])
    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        df = run(f"SELECT status, COUNT(*) AS count FROM work_orders WHERE UPPER(region)='{reg}' GROUP BY status ORDER BY count DESC")
        if not df.empty: st.plotly_chart(pie(df,"status","count","Work Order Status"), use_container_width=True)
    with c2:
        df = run(f"SELECT vendor_name, COUNT(*) AS orders, AVG(quality_score) AS avg_quality FROM work_orders WHERE UPPER(region)='{reg}' GROUP BY vendor_name ORDER BY orders DESC LIMIT 10")
        if not df.empty: st.plotly_chart(bar(df,"vendor_name","orders","Work Orders by Vendor"), use_container_width=True)
    df = run(f"SELECT work_order_id, title_name, vendor_name, work_type, status, priority, due_date, quality_score, cost, billing_status FROM work_orders WHERE UPPER(region)='{reg}' ORDER BY due_date ASC LIMIT 200")
    if not df.empty:
        st.dataframe(df, use_container_width=True, hide_index=True,
                     column_config={"quality_score":st.column_config.ProgressColumn("Quality",min_value=0,max_value=100,format="%.1f"),"cost":st.column_config.NumberColumn("Cost",format="$%,.0f")})


# ══════════════════════════════════════════════════════════════════════════════
# CHAT / QUERY  ◄── integrated with 3-stage pipeline + SQL chips
# ══════════════════════════════════════════════════════════════════════════════
def page_chat():
    reg = st.session_state.current_region
    st.markdown('<div class="page-header">💬 Chat Query</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">Natural language rights interrogation · {reg} · Hybrid LLM + Rule pipeline with interactive SQL chips</div>', unsafe_allow_html=True)

    # ── Sample queries (empty state) ─────────────────────────────────────────
    if not st.session_state.chat_history:
        st.markdown('<div style="background:#fff;border:1px solid #e2e8f0;border-radius:12px;padding:14px 16px;margin-bottom:12px">', unsafe_allow_html=True)
        st.markdown('<div style="font-size:.8rem;font-weight:700;color:#6366f1;text-transform:uppercase;letter-spacing:.06em;margin-bottom:10px">💡 Sample queries</div>', unsafe_allow_html=True)
        groups = {
            "🔗 Cross-table": ["Movies with DNA flags","Titles with rights, DNA and sales deals","Rights expiring in 60 days with active sales deals","Work orders linked to expiring rights"],
            "📋 Rights":      ["What titles do we have SVOD rights to","Show SVOD rights expiring in 30 days",'What rights do we hold for "Succession"',"Show titles with exclusive PayTV rights"],
            "🎬 Movies":      ["Show all movies in the slate","Movies by box office revenue","Franchise box office breakdown","Theatrical movies with active rights"],
            "🚫 DNA/Sales":   ["Show do-not-air restrictions","Active deals by vendor","Sales deals by buyer","Deal source breakdown TRL C2 FRL"],
        }
        for grp, qs in groups.items():
            st.markdown(f'<div style="font-size:.72rem;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:.05em;margin:8px 0 4px">{grp}</div>', unsafe_allow_html=True)
            for col, q in zip(st.columns(len(qs)), qs):
                if col.button(q, key=f"sug_{hash(q)}", use_container_width=True):
                    st.session_state.pending_prompt = q
        st.markdown('</div>', unsafe_allow_html=True)

    col_a, col_b = st.columns([3,1])
    with col_a:
        show_sql = st.toggle("Show SQL", value=st.session_state.user_prefs.get("show_sql",True), key="sql_tog")
        st.session_state.user_prefs["show_sql"] = show_sql
    with col_b:
        raw_sql_mode = st.toggle("⚡ Raw SQL", value=st.session_state.user_prefs.get("raw_sql_mode",False), key="raw_sql_tog")
        st.session_state.user_prefs["raw_sql_mode"] = raw_sql_mode

    # ── Raw SQL mode ─────────────────────────────────────────────────────────
    if raw_sql_mode:
        st.markdown('<div style="background:#f8faff;border:1px solid #c7d2fe;border-radius:10px;padding:12px 16px;margin-bottom:10px"><div style="font-size:.78rem;font-weight:700;color:#4f46e5;margin-bottom:6px">⚡ Raw SQL Mode</div><div style="font-size:.72rem;color:#64748b">Tables: <code>movie · title · series · season · media_rights · content_deal · exhibition_restrictions · elemental_rights · elemental_deal · do_not_air · sales_deal · deals · vendors · work_orders</code></div></div>', unsafe_allow_html=True)
        RAW_TEMPLATES = {
            "— Pick a template —":"",
            "Title health (rights + DNA + sales)":"""SELECT t.title_name, t.title_type, t.content_category, COUNT(DISTINCT mr.rights_id) AS active_rights, SUM(CASE WHEN mr.term_to<=DATE('now','+90 days') AND mr.status='Active' THEN 1 ELSE 0 END) AS expiring_90d, COUNT(DISTINCT dna.dna_id) AS dna_flags, GROUP_CONCAT(DISTINCT dna.reason_category) AS dna_reasons, COUNT(DISTINCT sd.sales_deal_id) AS sales_deals, GROUP_CONCAT(DISTINCT sd.buyer) AS buyers FROM title t LEFT JOIN media_rights mr ON t.title_id=mr.title_id AND mr.status='Active' LEFT JOIN do_not_air dna ON t.title_id=dna.title_id AND dna.active=1 LEFT JOIN sales_deal sd ON t.title_id=sd.title_id WHERE UPPER(t.region)='NA' GROUP BY t.title_id ORDER BY dna_flags DESC, expiring_90d DESC LIMIT 100""",
            "Expiry + sales overlap":"""SELECT mr.title_name, mr.media_platform_primary AS rights_platform, mr.term_to AS rights_expiry, CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_left, mr.exclusivity, sd.buyer, sd.deal_value, sd.status AS sale_status, CASE WHEN sd.sales_deal_id IS NOT NULL THEN '⚠ Active Sale' ELSE '— No Sale' END AS flag FROM media_rights mr JOIN content_deal cd ON mr.deal_id=cd.deal_id LEFT JOIN sales_deal sd ON mr.title_id=sd.title_id AND sd.status='Active' WHERE UPPER(mr.region)='NA' AND mr.status='Active' AND mr.term_to<=DATE('now','+90 days') ORDER BY days_left ASC LIMIT 100""",
            "Movies + DNA flags":"""SELECT m.movie_title, m.content_category, m.genre, m.franchise, m.box_office_gross_usd_m, COUNT(DISTINCT mr.rights_id) AS active_rights, COUNT(DISTINCT dna.dna_id) AS dna_flags, GROUP_CONCAT(DISTINCT dna.reason_category) AS dna_reasons, CASE WHEN COUNT(dna.dna_id)>0 THEN '🚫 Flagged' ELSE '✅ Clean' END AS dna_status FROM movie m LEFT JOIN title t ON t.movie_id=m.movie_id LEFT JOIN media_rights mr ON mr.title_id=t.title_id AND mr.status='Active' LEFT JOIN do_not_air dna ON dna.title_id=t.title_id AND dna.active=1 GROUP BY m.movie_id ORDER BY dna_flags DESC, m.box_office_gross_usd_m DESC""",
        }
        sel_tpl = st.selectbox("Quick-start template", list(RAW_TEMPLATES.keys()), key="raw_tpl")
        default_sql = RAW_TEMPLATES[sel_tpl] if sel_tpl != "— Pick a template —" else st.session_state.get("last_raw_sql","SELECT * FROM movie LIMIT 10")
        raw_sql_in = st.text_area("SQL Editor", value=default_sql, height=220, key="raw_sql_input")
        if st.button("▶ Run SQL", type="primary", key="run_raw_btn") and raw_sql_in.strip():
            st.session_state["last_raw_sql"] = raw_sql_in.strip()
            with st.spinner("Running…"):
                res_df, db_err = execute_sql(raw_sql_in.strip(), DB_CONN)
            if db_err:
                st.error(f"❌ SQL Error: {db_err}")
            elif res_df is not None and not res_df.empty:
                st.success(f"✅ {len(res_df):,} rows")
                if show_sql: st.markdown(f'<div class="sql-box">{html.escape(raw_sql_in.strip())}</div>', unsafe_allow_html=True)
                num_cols = [c for c in res_df.columns if pd.api.types.is_numeric_dtype(res_df[c])]
                if num_cols:
                    mc = st.columns(min(4,len(num_cols)))
                    for i, nc in enumerate(num_cols[:4]): mc[i].metric(nc, f"{res_df[nc].sum():,.0f}")
                if len(res_df.columns) >= 2:
                    fn = next((c for c in res_df.columns if pd.api.types.is_numeric_dtype(res_df[c])), None)
                    if fn and res_df.columns[0] != fn and len(res_df) <= 50:
                        st.plotly_chart(bar(res_df.head(30), res_df.columns[0], fn, "Query Result"), use_container_width=True)
                st.dataframe(res_df, use_container_width=True, hide_index=True, height=380)
                st.download_button("📥 CSV", res_df.to_csv(index=False),"raw_sql_result.csv","text/csv",key="dl_raw")
                st.session_state.chat_history.append({"question":f"[SQL] {raw_sql_in.strip()[:80]}…","answer":f"📊 **{len(res_df):,} records** — raw SQL","data":res_df.copy(),"chart":None,"metrics":[],"sql":raw_sql_in.strip(),"region":reg,"chips":[]})
            else:
                st.warning("No records returned.")
        st.divider()

    # ── Chat history ─────────────────────────────────────────────────────────
    for i, msg in enumerate(st.session_state.chat_history):
        with st.chat_message("user"):
            st.markdown(f"**{msg['question']}**  `{msg.get('region','')}`")
        with st.chat_message("assistant", avatar="🔑"):
            # Show chips snapshot (non-interactive — historical view)
            chips = msg.get("chips", [])
            if chips:
                from utils.query_chips_ui import _KIND_CLASS
                parts = ['<div class="chip-row"><span class="chip-hint">🔍</span>']
                for chip in chips:
                    if chip["kind"] == "match_method":
                        continue  # rendered separately below as badge
                    cls = _KIND_CLASS.get(chip["kind"], "chip")
                    parts.append(f'<span class="{cls}"><span class="chip-label">{chip["label"]}</span>{html.escape(str(chip["value"]))}</span>')
                st.markdown("".join(parts) + "</div>", unsafe_allow_html=True)
                # Method badge
                mm = msg.get("match_method", "rule")
                _bcls = "chip-match-llm" if mm == "llm" else "chip-match-rule"
                _blbl = "🤖 LLM Intent" if mm == "llm" else "📐 Rule-based"
                st.markdown(f'<span class="{_bcls}">{_blbl}</span>', unsafe_allow_html=True)
            if msg.get("sql") and show_sql:
                st.markdown(f'<div class="sql-box">{html.escape(msg["sql"])}</div>', unsafe_allow_html=True)
            if msg.get("metrics"):
                mc = st.columns(len(msg["metrics"]))
                for c, m in zip(mc, msg["metrics"]): c.metric(m["label"], m["value"])
            if msg.get("chart"):
                st.plotly_chart(msg["chart"], use_container_width=True, key=f"hchart_{i}")
            st.markdown(msg.get("answer","Here are the results:"))
            if msg.get("data") is not None and not msg["data"].empty:
                st.dataframe(msg["data"], use_container_width=True, hide_index=True, height=280)
                st.download_button("📥 CSV", msg["data"].to_csv(index=False), f"query_{i}.csv","text/csv",key=f"dl_h_{i}")
            # ── Feedback buttons for history entries ──────────────────────────
            msg_log_id = msg.get("log_id")
            if msg_log_id:
                fb_key = f"fb_{msg_log_id}"
                if fb_key not in st.session_state:
                    hfc1, hfc2, _ = st.columns([1, 1, 8])
                    with hfc1:
                        if st.button("👍", key=f"up_h_{i}_{msg_log_id}", help="Good answer"):
                            log_feedback(DB_CONN, msg_log_id, "thumbs_up")
                            st.session_state[fb_key] = "thumbs_up"
                            st.rerun()
                    with hfc2:
                        if st.button("👎", key=f"dn_h_{i}_{msg_log_id}", help="Bad answer"):
                            log_feedback(DB_CONN, msg_log_id, "thumbs_down")
                            st.session_state[fb_key] = "thumbs_down"
                            st.rerun()
                else:
                    icon = "👍" if st.session_state[fb_key] == "thumbs_up" else "👎"
                    st.caption(f"Feedback recorded {icon}")

    # ── Live input ────────────────────────────────────────────────────────────
    user_input = st.chat_input(f"Ask about rights, titles, expiry, DNA… [{reg}]")
    active_prompt = None
    if st.session_state.get("pending_prompt"):
        active_prompt = st.session_state.pending_prompt; st.session_state.pending_prompt = None
    elif user_input:
        active_prompt = user_input

    if active_prompt:
        with st.chat_message("user"):
            st.markdown(f"**{active_prompt}**  `{reg}`")

        with st.chat_message("assistant", avatar="🔑"):
            with st.spinner("Analysing…"):
                # ── STAGE 1+2+3 PIPELINE ────────────────────────────────────
                sql, error, chart_type, region_ctx, intent, match_method = chips_query_block(
                    question=active_prompt,
                    selected_region=reg,
                    key_prefix=f"chat_{abs(hash(active_prompt)) % 65536}",
                    show_sql=show_sql,
                )
                # chips_query_block renders chip row + method badge + SQL box

                if error:
                    st.error(f"❌ {error}")
                else:
                    res_df, db_err, log_id = run_with_logging(
                        sql, active_prompt,
                        intent.domain if hasattr(intent, "domain") else str(intent),
                        chart_type, region_ctx,
                    )
                    if db_err:
                        st.error(f"DB error: {db_err}")
                    elif res_df is not None and not res_df.empty:
                        # Metric strip
                        metrics_data = []
                        val_cols = [c for c in res_df.columns if any(x in c.lower() for x in ["count","total","value","fee","days"])]
                        if val_cols:
                            vc = val_cols[0]
                            try:
                                res_df[vc] = pd.to_numeric(res_df[vc], errors="coerce")
                                mc = st.columns(4)
                                mc[0].metric("Total / Sum", f"{res_df[vc].sum():,.0f}")
                                mc[1].metric("Avg",         f"{res_df[vc].mean():,.1f}")
                                mc[2].metric("Records",     f"{len(res_df):,}")
                                mc[3].metric("Max",         f"{res_df[vc].max():,.0f}")
                                metrics_data = [{"label":"Total","value":f"{res_df[vc].sum():,.0f}"},{"label":"Avg","value":f"{res_df[vc].mean():,.1f}"},{"label":"Records","value":f"{len(res_df):,}"},{"label":"Max","value":f"{res_df[vc].max():,.0f}"}]
                            except (ValueError, TypeError): pass

                        # Chart
                        fig = None
                        if chart_type == "bar" and len(res_df.columns) >= 2:
                            x_col = res_df.columns[0]
                            y_col = next((c for c in res_df.columns[1:] if pd.api.types.is_numeric_dtype(res_df[c])), res_df.columns[1])
                            try: res_df[y_col] = pd.to_numeric(res_df[y_col], errors="coerce")
                            except: pass
                            fig = bar(res_df.head(30), x_col, y_col, active_prompt[:60])
                        elif chart_type == "pie" and len(res_df.columns) >= 2:
                            fig = pie(res_df, res_df.columns[0], res_df.columns[1], active_prompt[:60])
                        if fig: st.plotly_chart(fig, use_container_width=True)

                        answer_txt = f"📊 **{len(res_df):,} records** for **{region_ctx}**." + (" Sorted by expiry." if "expir" in active_prompt.lower() else "")
                        st.markdown(answer_txt)
                        st.dataframe(res_df, use_container_width=True, hide_index=True, height=300)
                        st.download_button("📥 Download CSV", res_df.to_csv(index=False), f"rights_query_{region_ctx}.csv","text/csv",key="dl_live")

                        # ── Feedback buttons ──────────────────────────────────
                        if log_id:
                            fb_key = f"fb_{log_id}"
                            if fb_key not in st.session_state:
                                fc1, fc2, _ = st.columns([1, 1, 8])
                                with fc1:
                                    if st.button("👍", key=f"up_live_{log_id}", help="Good answer"):
                                        log_feedback(DB_CONN, log_id, "thumbs_up")
                                        st.session_state[fb_key] = "thumbs_up"
                                        st.rerun()
                                with fc2:
                                    if st.button("👎", key=f"dn_live_{log_id}", help="Bad answer"):
                                        log_feedback(DB_CONN, log_id, "thumbs_down")
                                        st.session_state[fb_key] = "thumbs_down"
                                        st.rerun()

                        st.session_state.chat_history.append({
                            "question": active_prompt, "answer": answer_txt,
                            "data": res_df.copy(), "chart": fig,
                            "metrics": metrics_data, "sql": sql,
                            "region": region_ctx, "chips": intent.chips,
                            "log_id": log_id,
                            "match_method": match_method,
                        })
                    else:
                        st.warning("No records returned — try removing a chip filter or rewording your query.")

    if st.session_state.chat_history:
        if st.button("🗑 Clear Chat", key="clear_chat"):
            st.session_state.chat_history = []; st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# DEALS PAGE
# ══════════════════════════════════════════════════════════════════════════════
def page_deals():
    reg = st.session_state.current_region
    st.markdown('<div class="page-header">💼 Deals</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">Vendor licensing &amp; distribution deals · {reg}</div>', unsafe_allow_html=True)
    kpi = run(f"SELECT COUNT(*) AS total, SUM(deal_value) AS total_value, AVG(deal_value) AS avg_value, SUM(CASE WHEN status='Active' THEN 1 ELSE 0 END) AS active, SUM(CASE WHEN status='Expired' THEN 1 ELSE 0 END) AS expired, SUM(CASE WHEN status='Pending' OR status='Under Negotiation' THEN 1 ELSE 0 END) AS pending, SUM(CASE WHEN payment_status='Overdue' THEN 1 ELSE 0 END) AS overdue_payments FROM deals WHERE UPPER(region)='{reg}'")
    if not kpi.empty:
        r = kpi.iloc[0]
        stat_tiles([(f"{int(r.get('total',0)):,}","Total Deals","#0f172a"),(fmt_m(r.get("total_value",0)),"Total Value","#1e40af"),(fmt_m(r.get("avg_value",0)),"Avg Deal Value","#5b21b6"),(f"{int(r.get('active',0)):,}","Active","#166534"),(f"{int(r.get('expired',0)):,}","Expired","#64748b"),(f"{int(r.get('pending',0)):,}","Pending / Neg.","#92400e"),(f"{int(r.get('overdue_payments',0)):,}","Overdue Payments","#991b1b")])
    st.divider()
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview","🏢 By Vendor","📋 Deal Types","📄 Deal Table"])
    with tab1:
        c1, c2 = st.columns(2)
        with c1:
            df = run(f"SELECT status, COUNT(*) AS count, SUM(deal_value) AS total_value FROM deals WHERE UPPER(region)='{reg}' GROUP BY status ORDER BY count DESC")
            if not df.empty: st.plotly_chart(pie(df,"status","count","Deals by Status"), use_container_width=True)
        with c2:
            df = run(f"SELECT rights_scope, COUNT(*) AS count, SUM(deal_value) AS total_value FROM deals WHERE UPPER(region)='{reg}' GROUP BY rights_scope ORDER BY total_value DESC")
            if not df.empty: st.plotly_chart(bar(df,"rights_scope","total_value","Deal Value by Rights Scope",h=300,horiz=True), use_container_width=True)
        df = run(f"SELECT STRFTIME('%Y-%m',deal_date) AS month, COUNT(*) AS count, SUM(deal_value) AS value FROM deals WHERE UPPER(region)='{reg}' GROUP BY month ORDER BY month")
        if not df.empty:
            fig = go.Figure()
            fig.add_bar(x=df["month"], y=df["count"], name="Deal Count", marker_color="#7c3aed", yaxis="y")
            fig.add_scatter(x=df["month"], y=df["value"], name="Deal Value ($)", mode="lines+markers", line=dict(color="#f59e0b",width=2), yaxis="y2")
            fig.update_layout(**PT, height=300, title="Monthly Deal Activity", yaxis=dict(title="Count"), yaxis2=dict(title="Value ($)",overlaying="y",side="right"), legend=dict(orientation="h",y=1.1))
            st.plotly_chart(fig, use_container_width=True)
    with tab2:
        df = run(f"SELECT vendor_name, COUNT(*) AS deals, SUM(deal_value) AS total_value, AVG(deal_value) AS avg_value, SUM(CASE WHEN status='Active' THEN 1 ELSE 0 END) AS active, SUM(CASE WHEN payment_status='Overdue' THEN 1 ELSE 0 END) AS overdue FROM deals WHERE UPPER(region)='{reg}' GROUP BY vendor_name ORDER BY total_value DESC LIMIT 10")
        if not df.empty:
            c1, c2 = st.columns(2)
            with c1: st.plotly_chart(bar(df,"vendor_name","total_value","Total Deal Value by Vendor",h=320,horiz=True), use_container_width=True)
            with c2: st.plotly_chart(bar(df,"vendor_name","deals","Deal Count by Vendor",h=320,horiz=True), use_container_width=True)
            st.dataframe(df, use_container_width=True, hide_index=True, column_config={"total_value":st.column_config.NumberColumn("Total Value",format="$%,.0f"),"avg_value":st.column_config.NumberColumn("Avg Value",format="$%,.0f")})
    with tab3:
        df = run(f"SELECT deal_type, COUNT(*) AS count, SUM(deal_value) AS total_value, AVG(deal_value) AS avg_value, SUM(CASE WHEN status='Active' THEN 1 ELSE 0 END) AS active FROM deals WHERE UPPER(region)='{reg}' GROUP BY deal_type ORDER BY total_value DESC")
        if not df.empty:
            c1, c2 = st.columns(2)
            with c1: st.plotly_chart(pie(df,"deal_type","count","Deals by Type"), use_container_width=True)
            with c2: st.plotly_chart(bar(df,"deal_type","total_value","Value by Deal Type",h=300,horiz=True), use_container_width=True)
    with tab4:
        f1, f2, f3 = st.columns(3)
        sf  = f1.selectbox("Status",   ["All","Active","Expired","Pending","Under Negotiation","Terminated"],key="dl_st")
        dtf = f2.selectbox("Deal Type",["All","Output Deal","Library Buy","First-Look Deal","Co-Production","Licensing Agreement","Distribution Deal","Volume Deal","Format Deal"],key="dl_dt")
        pyf = f3.selectbox("Payment",  ["All","Paid","Pending","Invoiced","Overdue","Partially Paid"],key="dl_pay")
        ex = (("" + (f" AND status='{sf}'" if sf!="All" else "") + (f" AND deal_type='{dtf}'" if dtf!="All" else "") + (f" AND payment_status='{pyf}'" if pyf!="All" else "")))
        df = run(f"SELECT deal_id, deal_name, vendor_name, deal_type, deal_value, currency, deal_date, expiry_date, rights_scope, territory, status, payment_status FROM deals WHERE UPPER(region)='{reg}' {ex} ORDER BY deal_value DESC LIMIT 300")
        if not df.empty:
            def _se(d):
                try: return exp_tag((datetime.strptime(str(d),"%Y-%m-%d")-datetime.now()).days)
                except: return "—"
            df["⏰ Expiry"] = df["expiry_date"].apply(_se)
            st.caption(f"{len(df)} deals")
            st.dataframe(df[["deal_id","deal_name","vendor_name","deal_type","deal_value","deal_date","expiry_date","⏰ Expiry","rights_scope","territory","status","payment_status"]], use_container_width=True, hide_index=True, column_config={"deal_value":st.column_config.NumberColumn("Value",format="$%,.0f")})
            st.download_button("📥 Export CSV", df.to_csv(index=False), f"deals_{reg}.csv","text/csv")


# ══════════════════════════════════════════════════════════════════════════════
# VENDORS
# ══════════════════════════════════════════════════════════════════════════════
def page_vendors():
    reg = st.session_state.current_region
    st.markdown('<div class="page-header">🏢 Vendors</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">Vendor performance, spend &amp; quality · {reg}</div>', unsafe_allow_html=True)
    kpi = run(f"SELECT COUNT(*) AS total, AVG(rating) AS avg_rating, SUM(total_spend) AS total_spend, COUNT(DISTINCT vendor_type) AS vendor_types, SUM(CASE WHEN active=1 THEN 1 ELSE 0 END) AS active_vendors FROM vendors WHERE UPPER(region)='{reg}'")
    if not kpi.empty:
        r = kpi.iloc[0]
        stat_tiles([(f"{int(r.get('total',0)):,}","Total Vendors","#0f172a"),(f"{r.get('avg_rating',0):.2f} ⭐","Avg Rating","#92400e"),(fmt_m(r.get("total_spend",0)),"Total Spend","#1e40af"),(f"{int(r.get('active_vendors',0)):,}","Active","#166534"),(f"{int(r.get('vendor_types',0)):,}","Vendor Types","#5b21b6")])
    st.divider()
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Performance","💰 Spend","⭐ Quality","📄 Vendor List"])
    with tab1:
        df = run(f"SELECT v.vendor_name, v.vendor_type, v.rating, COUNT(d.deal_id) AS deal_count, SUM(d.deal_value) AS total_deal_value, v.total_spend FROM vendors v LEFT JOIN deals d ON v.vendor_id=d.vendor_id WHERE UPPER(v.region)='{reg}' GROUP BY v.vendor_id ORDER BY total_deal_value DESC NULLS LAST LIMIT 10")
        if not df.empty:
            fig = go.Figure()
            fig.add_bar(x=df["vendor_name"], y=df["deal_count"], name="Deal Count", marker_color="#7c3aed", yaxis="y")
            fig.add_scatter(x=df["vendor_name"], y=df["total_deal_value"], name="Deal Value ($)", mode="lines+markers", line=dict(color="#f59e0b",width=2), yaxis="y2")
            fig.update_layout(**PT, height=340, title="Vendor Deal Count vs Deal Value", yaxis=dict(title="Deal Count"), yaxis2=dict(title="Deal Value ($)",overlaying="y",side="right"), legend=dict(orientation="h",y=1.1), xaxis=dict(tickangle=-30))
            st.plotly_chart(fig, use_container_width=True)
    with tab2:
        c1, c2 = st.columns(2)
        with c1:
            df = run(f"SELECT vendor_type, SUM(total_spend) AS spend, COUNT(*) AS vendors FROM vendors WHERE UPPER(region)='{reg}' GROUP BY vendor_type ORDER BY spend DESC")
            if not df.empty: st.plotly_chart(pie(df,"vendor_type","spend","Spend by Vendor Type"), use_container_width=True)
        with c2:
            df = run(f"SELECT vendor_name, total_spend FROM vendors WHERE UPPER(region)='{reg}' ORDER BY total_spend DESC LIMIT 10")
            if not df.empty: st.plotly_chart(bar(df,"vendor_name","total_spend","Total Spend by Vendor",h=300,horiz=True), use_container_width=True)
        df = run(f"SELECT payment_terms, COUNT(*) AS vendors, SUM(total_spend) AS spend FROM vendors WHERE UPPER(region)='{reg}' GROUP BY payment_terms ORDER BY spend DESC")
        if not df.empty:
            c1, c2 = st.columns(2)
            with c1: st.plotly_chart(pie(df,"payment_terms","vendors","Vendors by Payment Terms",h=260), use_container_width=True)
            with c2: st.plotly_chart(pie(df,"payment_terms","spend","Spend by Payment Terms",h=260), use_container_width=True)
    with tab3:
        df = run(f"SELECT v.vendor_name, v.vendor_type, v.rating, COUNT(wo.work_order_id) AS work_orders, AVG(wo.quality_score) AS avg_quality, SUM(wo.rework_count) AS total_rework, SUM(wo.cost) AS wo_cost FROM vendors v LEFT JOIN work_orders wo ON v.vendor_id=wo.vendor_id WHERE UPPER(v.region)='{reg}' GROUP BY v.vendor_id ORDER BY avg_quality DESC NULLS LAST")
        if not df.empty:
            fig = px.scatter(df,x="work_orders",y="avg_quality",size="wo_cost",color="vendor_type",hover_name="vendor_name",title="Work Volume vs Quality Score (bubble=cost)",labels={"work_orders":"Work Orders","avg_quality":"Avg Quality"})
            fig.update_layout(**PT, height=340); st.plotly_chart(fig, use_container_width=True)
            c1, c2 = st.columns(2)
            with c1: st.plotly_chart(bar(df,"vendor_name","rating","Vendor Rating",h=280), use_container_width=True)
            with c2: st.plotly_chart(bar(df,"vendor_name","total_rework","Total Rework Count",h=280), use_container_width=True)
    with tab4:
        df = run(f"SELECT v.vendor_id, v.vendor_name, v.vendor_type, v.rating, v.certification_level, v.contact_email, v.payment_terms, v.total_spend, COUNT(d.deal_id) AS deals, COUNT(wo.work_order_id) AS work_orders, CASE WHEN v.active=1 THEN 'Active' ELSE 'Inactive' END AS status FROM vendors v LEFT JOIN deals d ON v.vendor_id=d.vendor_id LEFT JOIN work_orders wo ON v.vendor_id=wo.vendor_id WHERE UPPER(v.region)='{reg}' GROUP BY v.vendor_id ORDER BY v.rating DESC")
        if not df.empty:
            st.dataframe(df, use_container_width=True, hide_index=True, column_config={"rating":st.column_config.ProgressColumn("Rating",min_value=0,max_value=5,format="%.1f ⭐"),"total_spend":st.column_config.NumberColumn("Total Spend",format="$%,.0f")})
            st.download_button("📥 Export CSV", df.to_csv(index=False), f"vendors_{reg}.csv","text/csv")


# ══════════════════════════════════════════════════════════════════════════════
# GAP ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
def page_gap_analysis():
    reg = st.session_state.current_region
    st.markdown('<div class="page-header">🔍 Rights Gap Analysis</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">Titles with missing or expired rights · {reg}</div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    gp = c1.selectbox("Platform",   ["All","PayTV","STB-VOD","SVOD","FAST"],key="gap_plat")
    gt = c2.selectbox("Title Type", ["All","Episode","Movie","Special"],    key="gap_type")
    gg = c3.selectbox("Genre",      ["All","Drama","Thriller","Fantasy","Sci-Fi","Comedy","Action","Historical","Crime","Animation"],key="gap_genre")
    pc = f"AND mr.media_platform_primary='{gp}'" if gp!="All" else ""
    tc = f"AND t.title_type='{gt}'"              if gt!="All" else ""
    gc = f"AND t.genre='{gg}'"                   if gg!="All" else ""
    st.divider()
    tab_a, tab_b, tab_c = st.tabs(["❌ No Active Rights","⏰ Expiring — No Renewal","📊 Coverage Heatmap"])

    with tab_a:
        df = run(f"SELECT t.title_name, t.title_type, t.genre, t.controlling_entity, t.content_category, COUNT(mr.rights_id) AS total_rights, SUM(CASE WHEN mr.status='Active' THEN 1 ELSE 0 END) AS active_rights, SUM(CASE WHEN mr.status='Expired' THEN 1 ELSE 0 END) AS expired_rights, MAX(mr.term_to) AS last_rights_end, s.series_title FROM title t LEFT JOIN media_rights mr ON t.title_id=mr.title_id AND UPPER(mr.region)='{reg}' {pc} LEFT JOIN season se ON t.season_id=se.season_id LEFT JOIN series s ON t.series_id=s.series_id WHERE UPPER(t.region)='{reg}' {tc} {gc} GROUP BY t.title_id HAVING active_rights=0 ORDER BY expired_rights DESC, t.title_name LIMIT 200")
        if df.empty:
            st.success(f"✅ All titles have active rights in {reg}" + (f" on {gp}" if gp!="All" else "") + ".")
        else:
            stat_tiles([(f"{len(df):,}",f"Titles with no active rights{' on '+gp if gp!='All' else ''}","#991b1b"),(f"{int(df['expired_rights'].sum()):,}","Previously Had Rights (expired)","#92400e"),(f"{int((df['total_rights']==0).sum()):,}","Never Had Rights","#64748b")])
            sg = df.dropna(subset=["series_title"]).groupby("series_title").size().reset_index(name="gap_titles").sort_values("gap_titles",ascending=False).head(15)
            if not sg.empty: st.plotly_chart(bar(sg,"series_title","gap_titles","Series with Most Rights Gaps",h=280,horiz=True), use_container_width=True)
            df["Last Rights"] = df["last_rights_end"].fillna("Never")
            df["Status"] = df["total_rights"].apply(lambda x: "🆕 Never Licensed" if x==0 else "🔴 Expired / Lapsed")
            st.dataframe(df[["title_name","title_type","genre","controlling_entity","content_category","total_rights","expired_rights","Last Rights","Status","series_title"]], use_container_width=True, hide_index=True)
            st.download_button("📥 Export Gap Report", df.to_csv(index=False), f"gap_{reg}_{gp}.csv","text/csv")
            if st.button("🔔 Alert me when rights are added", key="gap_alert_btn"):
                _, err = save_alert(DB_CONN,"Gap",f"Rights gap — {len(df)} titles, {gp} [{reg}]",region=reg,platform=gp,persona=st.session_state.persona)
                if not err: st.success("🔔 Gap alert saved!")

    with tab_b:
        df = run(f"SELECT mr.title_name, mr.media_platform_primary AS platform, mr.term_to AS rights_expiry, CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_left, mr.rights_type, mr.exclusivity, sd.sales_deal_id AS has_sales_deal, sd.buyer, sd.status AS sales_status, CASE WHEN sd.sales_deal_id IS NULL THEN '🆘 No Renewal' ELSE '⚠ Check Sale' END AS renewal_risk FROM media_rights mr LEFT JOIN sales_deal sd ON mr.title_id=sd.title_id AND UPPER(sd.region)='{reg}' AND sd.status='Active' WHERE UPPER(mr.region)='{reg}' AND mr.status='Active' AND mr.term_to<=DATE('now','+180 days') AND mr.term_to>=DATE('now') {pc} ORDER BY days_left ASC LIMIT 200")
        if not df.empty:
            ns = df[df["has_sales_deal"].isna()]
            stat_tiles([(f"{len(df):,}","Rights Expiring in 180 Days","#92400e"),(f"{len(ns):,}","No Active Sales Deal","#991b1b"),(f"{len(df)-len(ns):,}","Has Active Sale","#166534")])
            st.plotly_chart(bar(df.groupby("platform").size().reset_index(name="count"),"platform","count","Expiring Rights by Platform",h=260), use_container_width=True)
            st.dataframe(df[["title_name","platform","rights_expiry","days_left","rights_type","exclusivity","buyer","sales_status","renewal_risk"]], use_container_width=True, hide_index=True)

    with tab_c:
        hdf = run(f"SELECT t.genre, mr.media_platform_primary AS platform, COUNT(DISTINCT t.title_id) AS titles_covered, SUM(CASE WHEN mr.status='Active' THEN 1 ELSE 0 END) AS active_rights FROM title t LEFT JOIN media_rights mr ON t.title_id=mr.title_id AND UPPER(mr.region)='{reg}' WHERE UPPER(t.region)='{reg}' AND t.genre IS NOT NULL AND mr.media_platform_primary IS NOT NULL GROUP BY t.genre, mr.media_platform_primary")
        if not hdf.empty:
            pivot = hdf.pivot_table(index="genre",columns="platform",values="active_rights",aggfunc="sum",fill_value=0)
            fig = go.Figure(data=go.Heatmap(z=pivot.values.tolist(),x=pivot.columns.tolist(),y=pivot.index.tolist(),colorscale="Purples",text=pivot.values.tolist(),texttemplate="%{text}",hoverongaps=False,showscale=True))
            fig.update_layout(**PT, height=400, title=f"Active Rights — Genre × Platform [{reg}]")
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Zero = no active rights for that genre/platform combination.")


# ══════════════════════════════════════════════════════════════════════════════
# COMPARE REGIONS
# ══════════════════════════════════════════════════════════════════════════════
def page_compare():
    st.markdown('<div class="page-header">⚖️ Compare Regions</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Side-by-side rights, DNA, sales and content across two markets</div>', unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    ra = c1.selectbox("Region A",["NA","APAC","EMEA","LATAM"],index=0,key="cmp_a")
    rb = c2.selectbox("Region B",["NA","APAC","EMEA","LATAM"],index=2,key="cmp_b")
    if ra == rb: st.warning("Select two different regions."); return
    st.divider()
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Rights Overview","🚫 DNA","💸 Sales","🎬 Content"])

    def _kpi(r):
        return run(f"SELECT COUNT(*) AS total, SUM(CASE WHEN status='Active' THEN 1 ELSE 0 END) AS active, SUM(CASE WHEN status='Expired' THEN 1 ELSE 0 END) AS expired, SUM(CASE WHEN term_to<=DATE('now','+30 days') AND status='Active' THEN 1 ELSE 0 END) AS exp30, SUM(CASE WHEN term_to<=DATE('now','+90 days') AND status='Active' THEN 1 ELSE 0 END) AS exp90, SUM(exclusivity) AS exclusive, COUNT(DISTINCT title_id) AS titles_covered FROM media_rights WHERE UPPER(region)='{r}'")

    with tab1:
        ka, kb = _kpi(ra), _kpi(rb)
        if ka.empty or kb.empty: st.warning("No data"); return
        rva, rvb = ka.iloc[0], kb.iloc[0]
        metrics = [("Total Rights","total"),("Active","active"),("⚠ Expiring 30d","exp30"),("Expiring 90d","exp90"),("Exclusive","exclusive"),("Titles Covered","titles_covered")]
        cols = st.columns(len(metrics))
        for col,(lbl,key) in zip(cols,metrics):
            va, vb = int(rva.get(key,0)), int(rvb.get(key,0))
            col.metric(lbl,f"{ra}: {va:,}",delta=f"vs {rb}: {vb:,}",delta_color="off")
        dfs = []
        for r, clr in [(ra,"#7c3aed"),(rb,"#f59e0b")]:
            df = run(f"SELECT media_platform_primary AS platform, COUNT(*) AS rights FROM media_rights WHERE UPPER(region)='{r}' AND status='Active' GROUP BY platform")
            df["region"] = r; dfs.append(df)
        if all(not d.empty for d in dfs):
            fig = px.bar(pd.concat(dfs),x="platform",y="rights",color="region",barmode="group",title=f"Active Rights by Platform — {ra} vs {rb}",color_discrete_map={ra:"#7c3aed",rb:"#f59e0b"})
            fig.update_layout(**PT, height=320); st.plotly_chart(fig, use_container_width=True)

    with tab2:
        c1, c2 = st.columns(2)
        for r, col in [(ra,c1),(rb,c2)]:
            df = run(f"SELECT reason_category AS cat, COUNT(*) AS n FROM do_not_air WHERE UPPER(region)='{r}' AND active=1 GROUP BY cat")
            with col:
                st.markdown(f"#### 🚫 {r} DNA")
                if not df.empty: st.metric("Total DNA Records",f"{df['n'].sum():,}"); st.plotly_chart(pie(df,"cat","n",f"DNA — {r}",h=280), use_container_width=True)

    with tab3:
        c1, c2 = st.columns(2)
        for r, col in [(ra,c1),(rb,c2)]:
            df = run(f"SELECT buyer, SUM(deal_value) AS val, COUNT(*) AS n FROM sales_deal WHERE UPPER(region)='{r}' AND status='Active' GROUP BY buyer ORDER BY val DESC LIMIT 10")
            with col:
                st.markdown(f"#### 💸 {r} — Top Buyers")
                if not df.empty: st.metric("Active Deal Value",fmt_m(df["val"].sum())); st.plotly_chart(bar(df,"buyer","val",f"Sales — {r}",h=300,horiz=True), use_container_width=True)

    with tab4:
        dfs = []
        for r in [ra,rb]:
            df = run(f"SELECT genre, COUNT(*) AS titles FROM title WHERE UPPER(region)='{r}' GROUP BY genre ORDER BY titles DESC")
            df["region"] = r; dfs.append(df)
        if all(not d.empty for d in dfs):
            fig = px.bar(pd.concat(dfs),x="genre",y="titles",color="region",barmode="group",title=f"Content by Genre — {ra} vs {rb}",color_discrete_map={ra:"#7c3aed",rb:"#f59e0b"})
            fig.update_layout(**PT, height=320); st.plotly_chart(fig, use_container_width=True)
        c1, c2 = st.columns(2)
        for r, col in [(ra,c1),(rb,c2)]:
            df = run(f"SELECT title_type, COUNT(*) AS n FROM title WHERE UPPER(region)='{r}' GROUP BY title_type")
            with col:
                if not df.empty: st.plotly_chart(pie(df,"title_type","n",f"Types — {r}",h=260), use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# ALERTS
# ══════════════════════════════════════════════════════════════════════════════
def page_alerts():
    reg = st.session_state.current_region
    st.markdown('<div class="page-header">🔔 Alerts &amp; Saved Filters</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">Rights expiry alerts and gap notifications · {reg}</div>', unsafe_allow_html=True)
    alerts_df, err = get_alerts(DB_CONN, region=reg)
    if err: st.error(f"Error loading alerts: {err}"); return
    total = len(alerts_df); active = len(alerts_df[alerts_df["dismissed"]==0]) if not alerts_df.empty else 0
    stat_tiles([(f"{active:,}","Active Alerts","#991b1b"),(f"{total-active:,}","Dismissed","#64748b"),(f"{total:,}","Total Saved","#0f172a")])
    st.divider()
    with st.expander("➕ Create New Alert Manually"):
        a1,a2,a3,a4 = st.columns(4)
        nt  = a1.selectbox("Alert Type",["Expiry","Gap","DNA","Sales"],key="new_al_type")
        nl  = a2.text_input("Label",placeholder="e.g. SVOD rights expiring Q2",key="new_al_label")
        np_ = a3.selectbox("Platform",["All","PayTV","STB-VOD","SVOD","FAST"],key="new_al_plat")
        nd  = a4.number_input("Days threshold",7,365,90,key="new_al_days")
        nn  = st.text_area("Notes (optional)",key="new_al_notes",height=60)
        if st.button("💾 Save Alert",key="save_new_alert",type="primary"):
            if nl.strip():
                _,err2 = save_alert(DB_CONN,nt,nl.strip(),region=reg,platform=np_,days_threshold=int(nd),persona=st.session_state.persona,notes=nn.strip() or None)
                if err2: st.error(f"Failed: {err2}")
                else: st.success("✅ Alert saved!"); st.rerun()
            else: st.warning("Please enter a label.")
    st.divider()
    show_d = st.checkbox("Show dismissed alerts",key="show_dismissed")
    alerts_df, _ = get_alerts(DB_CONN, region=reg, include_dismissed=show_d)
    if alerts_df.empty: st.info("No alerts yet. Use 🔔 Set Alert buttons on the Rights Explorer page."); return
    for atype in alerts_df["alert_type"].unique():
        grp = alerts_df[alerts_df["alert_type"]==atype]
        st.markdown(f"### {'⏰' if atype=='Expiry' else '🔍' if atype=='Gap' else '🚫' if atype=='DNA' else '💸'} {atype} Alerts ({len(grp)})")
        for _, row in grp.iterrows():
            ds = "opacity:0.45;" if row["dismissed"] else ""
            ca, cb, cc = st.columns([5,2,1])
            with ca:
                st.markdown(f'<div style="background:#fff;border:1px solid {"#fca5a5" if not row["dismissed"] else "#e2e8f0"};border-left:4px solid {"#ef4444" if not row["dismissed"] else "#94a3b8"};border-radius:8px;padding:10px 14px;{ds}"><div style="font-weight:700;color:#0f172a;font-size:.9rem">{row["label"]}</div><div style="font-size:.75rem;color:#64748b;margin-top:4px">Region: <b>{row["region"]}</b> · Platform: <b>{row["platform"] or "All"}</b> · Threshold: <b>{row["days_threshold"]}d</b> · {row["persona"]}</div><div style="font-size:.68rem;color:#94a3b8;margin-top:2px">Created: {str(row["created_at"])[:16]}</div></div>', unsafe_allow_html=True)
            with cb:
                if not row["dismissed"]:
                    if row["alert_type"]=="Expiry" and st.button("▶ View Expiry",key=f"al_exp_{row['alert_id']}"): st.session_state.page="rights"; st.rerun()
                    elif row["alert_type"]=="Gap" and st.button("▶ View Gap",key=f"al_gap_{row['alert_id']}"): st.session_state.page="gap_analysis"; st.rerun()
            with cc:
                if not row["dismissed"] and st.button("✕",key=f"al_dis_{row['alert_id']}",help="Dismiss"):
                    dismiss_alert(DB_CONN, int(row["alert_id"])); st.rerun()
    st.divider()
    if st.button("🗑 Dismiss All Active Alerts",key="dismiss_all"):
        for _, row in alerts_df[alerts_df["dismissed"]==0].iterrows(): dismiss_alert(DB_CONN, int(row["alert_id"]))
        st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# TITLE 360
# ══════════════════════════════════════════════════════════════════════════════
def page_title_360():
    reg = st.session_state.current_region
    st.markdown('<div class="page-header">🎯 Title 360</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Complete per-title view — rights, DNA, sales, work orders &amp; elemental</div>', unsafe_allow_html=True)
    pre = st.session_state.get("title_360") or ""
    all_titles = run("SELECT title_name FROM title UNION SELECT movie_title AS title_name FROM movie ORDER BY title_name")
    tlist = all_titles["title_name"].tolist() if not all_titles.empty else []
    try: pi = tlist.index(pre)+1 if pre in tlist else 0
    except: pi = 0
    chosen = st.selectbox("Select a title", ["— choose a title —"]+tlist, index=pi, key="t360_sel")
    if chosen == "— choose a title —": st.info("👆 Select a title to see its full 360° profile."); return
    st.session_state.title_360 = chosen
    _safe_chosen = chosen.lower().replace("'", "''")
    t_ids_df = run(f"SELECT title_id FROM title WHERE LOWER(title_name) LIKE '%{_safe_chosen}%' LIMIT 20")
    t_ids = t_ids_df["title_id"].tolist() if not t_ids_df.empty else []
    id_list = "','".join(t_ids)
    if not id_list: st.warning("No title records found."); return
    summary = run(f"SELECT t.title_name, t.title_type, t.genre, t.age_rating, t.release_year, s.series_title, se.season_number, t.episode_number, m.movie_title, m.franchise, m.box_office_gross_usd_m FROM title t LEFT JOIN season se ON t.season_id=se.season_id LEFT JOIN series s ON t.series_id=s.series_id LEFT JOIN movie m ON t.movie_id=m.movie_id WHERE t.title_id IN ('{id_list}') LIMIT 1")
    if not summary.empty:
        r = summary.iloc[0]
        h1,h2,h3,h4 = st.columns(4)
        h1.metric("Type",str(r.get("title_type","—"))); h2.metric("Genre",str(r.get("genre","—")))
        h3.metric("Rating",str(r.get("age_rating","—"))); h4.metric("Year",str(r.get("release_year","—")))
        if r.get("series_title"): st.markdown(f"📺 **Series:** {r['series_title']} · S{r.get('season_number','?')} E{r.get('episode_number','?')}")
        if r.get("franchise"):    st.markdown(f"🎬 **Franchise:** {r['franchise']} · Box Office: ${r.get('box_office_gross_usd_m',0):.0f}M")
    st.divider()
    t1,t2,t3,t4,t5 = st.tabs(["📋 Rights","🚫 DNA","💸 Sales","⚙️ Work Orders","🎞 Elemental"])
    with t1:
        rdf = run(f"SELECT mr.rights_id, cd.deal_source, mr.rights_type, mr.media_platform_primary AS platform, mr.territories, mr.language, mr.term_from, mr.term_to, mr.exclusivity, mr.holdback, mr.holdback_days, mr.status, CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_left FROM media_rights mr JOIN content_deal cd ON mr.deal_id=cd.deal_id WHERE mr.title_id IN ('{id_list}') ORDER BY mr.status, mr.term_to ASC")
        if rdf.empty: st.info("No rights records.")
        else:
            ar = int((rdf["status"]=="Active").sum()); e30 = int((rdf["days_left"].fillna(999)<=30).sum())
            stat_tiles([(f"{len(rdf)}","Total Rights","#0f172a"),(f"{ar}","Active","#166534"),(f"{len(rdf)-ar}","Expired/Pending","#64748b"),(f"{e30}","Expiring 30d","#991b1b")])
            rdf["⏰ Days"] = rdf["days_left"].apply(exp_tag); rdf["Excl"] = rdf["exclusivity"].apply(bool_icon)
            st.dataframe(rdf[["deal_source","rights_type","platform","territories","language","term_from","term_to","⏰ Days","Excl","status"]], use_container_width=True, hide_index=True)
            if st.button("🔔 Alert on expiry",key="t360_rights_alert"):
                _,err2 = save_alert(DB_CONN,"Expiry",f"Rights alert — {chosen}",region=reg,persona=st.session_state.persona)
                if not err2: st.success("Alert saved!")
    with t2:
        dna = run(f"SELECT dna_id, reason_category, reason_subcategory, territory, media_type, term_from, term_to, additional_notes, active FROM do_not_air WHERE title_id IN ('{id_list}') ORDER BY active DESC, reason_category")
        if dna.empty: st.success("✅ No Do-Not-Air restrictions.")
        else:
            ad = int(dna["active"].sum())
            stat_tiles([(f"{ad}","Active DNA Flags","#991b1b"),(f"{len(dna)-ad}","Inactive/Historical","#64748b")])
            st.dataframe(dna, use_container_width=True, hide_index=True)
    with t3:
        sdf = run(f"SELECT sales_deal_id, deal_type, buyer, territory, region, media_platform, term_from, term_to, deal_value, currency, status FROM sales_deal WHERE title_id IN ('{id_list}') ORDER BY deal_value DESC")
        if sdf.empty: st.info("No outbound sales deals.")
        else:
            stat_tiles([(f"{len(sdf)}","Sales Deals","#0f172a"),(fmt_m(sdf["deal_value"].sum()),"Total Value","#1e40af"),(f"{sdf['buyer'].nunique()}","Unique Buyers","#5b21b6")])
            st.dataframe(sdf, use_container_width=True, hide_index=True, column_config={"deal_value":st.column_config.NumberColumn("Value",format="$%,.0f")})
    with t4:
        wo = run(f"SELECT work_order_id, vendor_name, work_type, status, priority, due_date, quality_score, cost, billing_status FROM work_orders WHERE title_id IN ('{id_list}') ORDER BY due_date ASC")
        if wo.empty: st.info("No work orders.")
        else:
            stat_tiles([(f"{len(wo)}","Work Orders","#0f172a"),(f"{int((wo['status']=='In Progress').sum())}","In Progress","#1e40af"),(f"{int((wo['status']=='Delayed').sum())}","Delayed","#991b1b"),(fmt_m(wo['cost'].sum()),"Total Cost","#92400e")])
            st.dataframe(wo, use_container_width=True, hide_index=True, column_config={"quality_score":st.column_config.ProgressColumn("Quality",min_value=0,max_value=100,format="%.1f"),"cost":st.column_config.NumberColumn("Cost",format="$%,.0f")})
    with t5:
        el = run(f"SELECT er.elemental_rights_id, ed.deal_source, er.media_platform_primary, er.territories, er.language, er.term_from, er.term_to, er.status FROM elemental_rights er JOIN elemental_deal ed ON er.elemental_deal_id=ed.elemental_deal_id WHERE er.title_id IN ('{id_list}') ORDER BY er.status, er.term_to")
        if el.empty: st.info("No elemental rights (promos, trailers, raw assets).")
        else: st.dataframe(el, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# CUSTOM DASHBOARD BUILDER  ◄── integrated with 3-stage pipeline + chips
# ══════════════════════════════════════════════════════════════════════════════

def _db_save_pin(df: pd.DataFrame, fig, query_text: str, chart_type: str, key_prefix: str):
    if st.button("📌 Save to Dashboard", key=f"save_dash_{key_prefix}", help="Pin this chart"):
        st.session_state.dashboard_pins.insert(0, {
            "query": query_text, "chart_type": chart_type, "df": df.copy(), "fig": fig,
            "ts": datetime.now().strftime("%H:%M · %d %b %Y"),
            "title": query_text[:55] + ("…" if len(query_text)>55 else ""),
            "region": st.session_state.get("current_region",""),
        })
        st.success("✅ Pinned! Visit 📐 Custom Dashboard in the sidebar.")


def _db_suggested_queries(key_prefix: str = "sug"):
    suggs = [("⏰","Rights expiring in 30 days","SVOD rights expiring in 30 days"),("🌍","Distribution by Territory","Distribution breakdown by territory"),("📺","Rights by Platform","Rights distribution by media platform"),("🚫","Movies with DNA flags","Movies with do-not-air restrictions"),("⭐","Exclusive rights count","How many exclusive rights do we hold"),("💸","Top buyers by deal value","Sales deals by buyer sorted by deal value"),("💼","Rights by deal source","Deal source breakdown TRL C2 FRL"),("🔗","Expiring rights + active sales","Rights expiring in 60 days with active sales deals")]
    st.markdown('<div style="background:#faf5ff;border:1px solid #ddd6fe;border-radius:12px;padding:16px 18px;margin-top:12px"><div style="font-size:.78rem;font-weight:700;color:#5b21b6;text-transform:uppercase;letter-spacing:.06em;margin-bottom:12px">💡 Suggested queries — Rights Explorer schema</div>', unsafe_allow_html=True)
    cols = st.columns(4)
    for i,(icon,label,query) in enumerate(suggs):
        with cols[i%4]:
            if st.button(f"{icon} {label}", key=f"{key_prefix}_{i}_{hash(query)}", use_container_width=True):
                st.session_state["_db_prefill"] = query; st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)


def render_dynamic_dashboard(df: pd.DataFrame, chart_type: str, query_text: str, *, key_prefix: str="dyn", show_save_button: bool=True) -> Optional[go.Figure]:
    """Route df+chart_type to the best Plotly visualisation. chart_type='table' always renders table (no auto-chart)."""
    q_lower  = query_text.lower()
    is_trend = any(kw in q_lower for kw in ["trend","over time","monthly","weekly","by month","by year"])
    # Exclude ID-like columns from chart candidates
    _ID_COLS = {"deal_id","rights_id","sales_deal_id","work_order_id","title_id","dna_id","id"}
    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and c.lower() not in _ID_COLS]
    str_cols = [c for c in df.columns if not pd.api.types.is_numeric_dtype(df[c])]
    time_cols= [c for c in df.columns if any(kw in c.lower() for kw in ["term_to","term_from","date","month","year","timestamp","expiry"])]
    title_txt= query_text[:70] + ("…" if len(query_text)>70 else "")
    fig: Optional[go.Figure] = None

    # Coerce numeric-looking strings
    for c in list(str_cols):
        try:
            df = df.copy(); df[c] = pd.to_numeric(df[c], errors="raise")
            num_cols.append(c); str_cols.remove(c)
        except: pass

    # 1. Single-value metric card
    if len(df)==1 and len(df.columns)<=2:
        vc = num_cols[0] if num_cols else df.columns[-1]
        lc = str_cols[0] if str_cols else df.columns[0]
        val = df[vc].iloc[0]
        lbl = str(df[lc].iloc[0]) if lc!=vc else vc.replace("_"," ").title()
        try:
            v = float(val)
            fmt = (f"${v/1e9:.1f}B" if v>=1e9 else f"${v/1e6:.1f}M" if v>=1e6 else f"{v:,.0f}" if v==int(v) else f"{v:,.2f}")
        except: fmt = str(val)
        st.markdown(f'<div class="db-metric-card"><div class="db-metric-value">{html.escape(fmt)}</div><div class="db-metric-label">{html.escape(lbl)}</div><div class="db-metric-sub">{html.escape(title_txt)}</div></div>', unsafe_allow_html=True)
        if show_save_button: _db_save_pin(df, None, query_text, chart_type, key_prefix)
        return None

    # 2. Table — always raw table, never auto-chart (fixes screenshot bug)
    if chart_type == "table":
        if num_cols:
            mc = st.columns(min(4,len(num_cols)))
            for i,nc in enumerate(num_cols[:4]):
                try: mc[i].metric(nc.replace("_"," ").title(), f"{df[nc].sum():,.0f}")
                except: pass
        st.dataframe(df, use_container_width=True, hide_index=True, height=360)
        st.download_button("📥 CSV", df.to_csv(index=False), "export.csv","text/csv", key=f"{key_prefix}_dl_tbl")
        if show_save_button: _db_save_pin(df, None, query_text, chart_type, key_prefix)
        return None

    # 3. Trend / line chart
    if (is_trend or chart_type=="line") and time_cols and num_cols:
        tc = time_cols[0]; yc = num_cols[0]
        try: df=df.copy(); df[tc]=pd.to_datetime(df[tc],errors="coerce"); df=df.dropna(subset=[tc]).sort_values(tc)
        except: pass
        color_col = "region" if "region" in df.columns and df["region"].nunique()>1 else (str_cols[1] if len(str_cols)>1 else None)
        if color_col:
            fig = px.line(df,x=tc,y=yc,color=color_col,title=title_txt,markers=True,color_discrete_sequence=PT["colorway"])
        else:
            fig = px.line(df,x=tc,y=yc,title=title_txt,markers=True)
            fig.update_traces(line_color="#7c3aed",line_width=2.5,marker=dict(color="#7c3aed",size=6))
        fig.update_layout(**PT,height=360)
        _dash_tabbed(fig,df,num_cols,key_prefix,"line",query_text,show_save_button)
        return fig

    # 4. Bar chart — horizontal, top-10, gradient, region-coloured
    if chart_type=="bar" and str_cols and num_cols:
        xc=str_cols[0]; yc=num_cols[0]
        top = df.nlargest(10,yc) if len(df)>10 else df.sort_values(yc,ascending=False)
        color_col = "region" if "region" in top.columns and top["region"].nunique()>1 else None
        if color_col:
            fig = px.bar(top,x=yc,y=xc,color=color_col,orientation="h",title=title_txt,barmode="group",
                         color_discrete_sequence=PT["colorway"],text_auto=True)
        else:
            n = len(top)
            clrs = [f"rgba(124,58,237,{0.35+0.65*(i/max(n-1,1)):.2f})" for i in range(n)]
            fig = go.Figure(go.Bar(y=top[xc].astype(str),x=top[yc],orientation="h",
                                   marker_color=list(reversed(clrs)),
                                   text=top[yc].apply(lambda v: f"{int(v):,}" if float(v)==int(float(v)) else f"{float(v):,.1f}"),
                                   textposition="auto"))
            fig.update_layout(yaxis=dict(autorange="reversed"),xaxis_title=yc.replace("_"," ").title())
        fig.update_layout(**PT,height=max(280,len(top)*38+80),title=title_txt)
        _dash_tabbed(fig,df,num_cols,key_prefix,"bar",query_text,show_save_button,len(df)>10)
        return fig

    # 5. Pie / donut
    if chart_type=="pie" and str_cols and num_cols:
        pdf = df.head(10) if len(df)>10 else df
        fig = px.pie(pdf,names=str_cols[0],values=num_cols[0],title=title_txt,hole=0.42,color_discrete_sequence=PT["colorway"])
        fig.update_layout(**PT,height=340); fig.update_traces(textposition="inside",textinfo="percent+label")
        _dash_tabbed(fig,df,num_cols,key_prefix,"pie",query_text,show_save_button)
        return fig

    # 6. Multi-column aggregation fallback
    if len(df.columns)>=2 and str_cols and num_cols:
        color_col = "region" if "region" in df.columns and df["region"].nunique()>1 else None
        top = df.nlargest(10,num_cols[0]) if len(df)>10 else df.sort_values(num_cols[0],ascending=False)
        if color_col:
            fig = px.bar(top,x=str_cols[0],y=num_cols[0],color=color_col,title=title_txt,barmode="group",color_discrete_sequence=PT["colorway"])
        else:
            fig = px.bar(top,x=str_cols[0],y=num_cols[0],title=title_txt,color_discrete_sequence=["#7c3aed"])
        fig.update_layout(**PT,height=340); fig.update_xaxes(tickangle=-30)
        _dash_tabbed(fig,df,num_cols,key_prefix,"multi",query_text,show_save_button,len(df)>10)
        return fig

    # 7. Pure table fallback
    st.dataframe(df, use_container_width=True, hide_index=True, height=300)
    if show_save_button: _db_save_pin(df, None, query_text, chart_type, key_prefix)
    return None


def _dash_tabbed(fig, df, num_cols, key_prefix, suffix, query_text, show_save, overflow=False):
    """Shared Chart View | Data View tab renderer."""
    if len(df.columns) > 2:
        tc, td = st.tabs(["📊 Chart View","📋 Data View"])
        with tc:
            st.plotly_chart(fig, use_container_width=True, key=f"{key_prefix}_{suffix}")
            if overflow: st.caption(f"Top 10 of {len(df):,} rows — see Data View for all.")
        with td:
            if num_cols:
                mc = st.columns(min(4,len(num_cols)))
                for i,nc in enumerate(num_cols[:4]):
                    try: mc[i].metric(nc.replace("_"," ").title(), f"{df[nc].sum():,.0f}")
                    except: pass
            st.dataframe(df, use_container_width=True, hide_index=True, height=320)
            st.download_button("📥 CSV", df.to_csv(index=False),"export.csv","text/csv",key=f"{key_prefix}_dl_{suffix}")
    else:
        st.plotly_chart(fig, use_container_width=True, key=f"{key_prefix}_{suffix}_s")
        if overflow: st.caption(f"Top 10 of {len(df):,} rows.")
    if show_save: _db_save_pin(df, fig, query_text, "bar", key_prefix)


def page_custom_dashboard():
    reg = st.session_state.current_region
    pin_count = len(st.session_state.dashboard_pins)
    st.markdown('<div class="page-header">📐 Custom Dashboard Builder</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">Natural language → dynamic visualisation · <b>{reg}</b> · <span class="db-query-pill">📌 {pin_count} pinned</span></div>', unsafe_allow_html=True)

    # ── Query toolbar ─────────────────────────────────────────────────────────
    with st.container(border=True):
        ci, cr = st.columns([5,1])
        with ci:
            prefill = st.session_state.pop("_db_prefill","")
            query_text = st.text_input("NL query", value=prefill, placeholder='e.g. "Rights expiring in 30 days" or "EMEA deals last 60 days"', label_visibility="collapsed", key="db_nl_query")
        with cr:
            run_clicked = st.button("▶ Run", type="primary", use_container_width=True, key="db_run")
        ta, tb, tc_ = st.columns(3)
        show_sql_db = ta.toggle("Show SQL", value=st.session_state.user_prefs.get("show_sql",True), key="db_show_sql")
        export_on   = tb.toggle("Auto CSV export", value=True, key="db_export")
        tc_.markdown(f'<div style="font-size:.78rem;color:#64748b;padding-top:6px">📍 <b>{reg}</b> · <b>{st.session_state.persona}</b></div>', unsafe_allow_html=True)

    st.markdown("---")

    # ── Execute ───────────────────────────────────────────────────────────────
    if run_clicked and query_text.strip():
        with st.spinner("Parsing and fetching…"):
            # ── 3-STAGE PIPELINE + CHIPS ─────────────────────────────────────
            sql, error, chart_type, region_ctx, intent, _match_method = chips_query_block(
                question=query_text.strip(),
                selected_region=reg,
                key_prefix="db_live",
                show_sql=show_sql_db,
            )

        if error:
            st.error(f"❌ {error}")
        elif not sql:
            st.warning("Could not generate SQL."); _db_suggested_queries("fail")
        else:
            res_df, db_err, _log_id = run_with_logging(
                sql, query_text.strip(),
                intent or "",
                chart_type, region_ctx,
            )
            if db_err:
                st.error(f"❌ Database error: {db_err}")
            elif res_df is None or res_df.empty:
                st.warning(f"No records for **{region_ctx}**. Try removing a chip or try a suggested query below.")
                _db_suggested_queries("no_results")
            else:
                st.session_state.dashboard_last_df   = res_df.copy()
                st.session_state.dashboard_last_meta = {"query":query_text.strip(),"sql":sql,"chart_type":chart_type,"region":region_ctx,"intent":intent}
                st.markdown(f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:10px"><span style="font-size:.85rem;font-weight:700;color:#0f172a">📊 {len(res_df):,} records · {region_ctx}</span><span class="db-query-pill">chart: {chart_type}</span></div>', unsafe_allow_html=True)
                render_dynamic_dashboard(res_df.copy(), chart_type, query_text.strip(), key_prefix="live", show_save_button=True)
                if export_on:
                    st.download_button("📥 Download full CSV", res_df.to_csv(index=False), f"dashboard_{region_ctx}.csv","text/csv",key="db_dl_full")

    elif not query_text.strip():
        st.markdown("""
        <div class="db-empty">
          <div class="db-empty-icon">📐</div>
          <div class="db-empty-title">Build a chart with natural language</div>
          <div class="db-empty-body">
            Type a question above and click <b>▶ Run</b>.<br>
            Interactive chips let you remove/edit region, platform, date and title filters — the query reruns instantly.<br><br>
            <b>Chart routing:</b> Single value → metric card · "trend"/time → line chart · category+number → horizontal bar top-10 · status breakdown → donut pie · multi-column → tabbed Chart + Data view
          </div>
        </div>""", unsafe_allow_html=True)
        _db_suggested_queries("empty")

    # ── Pinned dashboard grid ─────────────────────────────────────────────────
    if st.session_state.dashboard_pins:
        st.markdown("---")
        hc, hl, hx = st.columns([4,2,2])
        hc.markdown(f'<div style="font-size:1.05rem;font-weight:800;color:#0f172a">📌 Pinned Dashboard <span style="font-size:.8rem;color:#7c3aed;font-weight:600">({pin_count} cards)</span></div>', unsafe_allow_html=True)
        two_col = hl.toggle("2-column layout", value=True, key="db_2col")
        if hx.button("🗑 Clear all pins", key="db_clear"):
            st.session_state.dashboard_pins = []; st.rerun()
        pins = st.session_state.dashboard_pins; nc = 2 if two_col else 1
        for rs in range(0, len(pins), nc):
            gcols = st.columns(nc)
            for ci2 in range(nc):
                pi = rs + ci2
                if pi >= len(pins): break
                pin = pins[pi]
                with gcols[ci2]:
                    st.markdown(f'<div class="db-card-header"><span class="db-card-title">{html.escape(pin["title"])}</span><span class="db-card-ts">{pin["ts"]}</span></div>', unsafe_allow_html=True)
                    with st.container(border=True):
                        st.markdown(f'<div style="margin-bottom:6px"><span class="db-query-pill">📍 {pin["region"]}</span><span class="db-query-pill">chart: {pin["chart_type"]}</span></div>', unsafe_allow_html=True)
                        if pin["fig"] is not None:
                            st.plotly_chart(pin["fig"], use_container_width=True, key=f"pin_{pi}_{hash(pin['ts'])}")
                        else:
                            dfp = pin["df"]
                            if not dfp.empty:
                                nc_ = [c for c in dfp.columns if pd.api.types.is_numeric_dtype(dfp[c])]
                                raw = dfp[nc_[0]].iloc[0] if nc_ else dfp.iloc[0,-1]
                                try: v=float(raw); disp=f"{v:,.0f}" if v==int(v) else f"{v:,.2f}"
                                except: disp=str(raw)
                                st.markdown(f'<div class="db-metric-card" style="padding:14px 18px"><div class="db-metric-value" style="font-size:2rem">{disp}</div><div class="db-metric-sub">{html.escape(pin["title"])}</div></div>', unsafe_allow_html=True)
                        pa, pb = st.columns(2)
                        with pa: st.download_button("📥 CSV", pin["df"].to_csv(index=False), f"pin_{pi}.csv","text/csv",key=f"pin_dl_{pi}",use_container_width=True)
                        with pb:
                            if st.button("✕ Unpin",key=f"pin_rm_{pi}",use_container_width=True):
                                st.session_state.dashboard_pins.pop(pi); st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# ANALYTICS
# ══════════════════════════════════════════════════════════════════════════════
def page_analytics():
    st.markdown('<div class="page-header">📊 Analytics</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">System health · performance · quality · adoption · KPI tracker</div>', unsafe_allow_html=True)

    tabs = st.tabs(["Overview", "Performance", "Quality", "Adoption", "KPI Tracker"])

    # ── Safe SQL helper (read directly from DB_CONN, bypass execute_sql guard) ─
    def _sql(query: str) -> pd.DataFrame:
        try:
            return pd.read_sql_query(query, DB_CONN)
        except Exception:
            return pd.DataFrame()

    today     = pd.Timestamp.now().normalize()
    d7_start  = (today - pd.Timedelta(days=6)).strftime("%Y-%m-%d")
    d14_start = (today - pd.Timedelta(days=13)).strftime("%Y-%m-%d")
    d30_start = (today - pd.Timedelta(days=29)).strftime("%Y-%m-%d")
    d10_start = (today - pd.Timedelta(days=9)).strftime("%Y-%m-%d")

    # ── Pre-fetch shared datasets ─────────────────────────────────────────────
    df_7d = _sql(f"""
        SELECT log_id, timestamp, session_id, success, error_message,
               latency_ms, intent_domain, rows_returned, chart_type
        FROM query_log
        WHERE DATE(timestamp) >= '{d7_start}'
    """)
    df_14d = _sql(f"""
        SELECT DATE(timestamp) AS day, success, error_message
        FROM query_log
        WHERE DATE(timestamp) >= '{d14_start}'
    """)
    df_fb30 = _sql(f"""
        SELECT feedback_type, COUNT(*) AS cnt
        FROM feedback
        WHERE DATE(timestamp) >= '{d30_start}'
        GROUP BY feedback_type
    """)

    # ── Compute headline KPIs ─────────────────────────────────────────────────
    total_q   = len(df_7d)
    latencies = df_7d["latency_ms"].dropna().values if not df_7d.empty else np.array([])
    p95_s     = float(np.percentile(latencies, 95)) / 1000 if len(latencies) else 0.0
    p50_s     = float(np.percentile(latencies, 50)) / 1000 if len(latencies) else 0.0
    p99_s     = float(np.percentile(latencies, 99)) / 1000 if len(latencies) else 0.0
    success_r = (int(df_7d["success"].sum()) / total_q * 100) if total_q else 0.0

    if total_q and not df_7d.empty:
        succ_df = df_7d[df_7d["success"] == 1]
        intent_match = (
            (succ_df["intent_domain"].fillna("") != "").sum() / len(succ_df) * 100
            if len(succ_df) else 100.0
        )
    else:
        intent_match = 0.0

    wau  = int(df_7d["session_id"].nunique()) if not df_7d.empty else 0
    ups  = int(df_fb30[df_fb30["feedback_type"] == "thumbs_up"]["cnt"].sum())  if not df_fb30.empty else 0
    downs= int(df_fb30[df_fb30["feedback_type"] == "thumbs_down"]["cnt"].sum()) if not df_fb30.empty else 0
    total_fb   = ups + downs
    thumbs_pct = ups / total_fb * 100 if total_fb else 0.0
    ratio_str  = f"{ups/downs:.1f}× upvotes/downvotes" if downs else "No downvotes yet"

    # ── Prior-week deltas ─────────────────────────────────────────────────────
    df_prev = _sql(f"""
        SELECT log_id, session_id, success
        FROM query_log
        WHERE DATE(timestamp) >= '{(today - pd.Timedelta(days=13)).strftime('%Y-%m-%d')}'
          AND DATE(timestamp) <  '{d7_start}'
    """)
    prev_total = len(df_prev)
    prev_wau   = int(df_prev["session_id"].nunique()) if not df_prev.empty else 0
    prev_sr    = float(df_prev["success"].mean() * 100) if not df_prev.empty and len(df_prev) else 0.0

    delta_q_val = total_q - prev_total
    delta_q     = f"{delta_q_val:+,} vs last week" if prev_total else "—"
    delta_wau   = f"{wau - prev_wau:+} vs last week" if prev_wau else "—"
    delta_sr    = f"{success_r - prev_sr:+.0f}pp vs last week" if prev_sr else "—"

    # ── Outcome classifier (reused across tabs) ───────────────────────────────
    def _classify_outcome(row):
        if row["success"] == 1:
            return "Success"
        em = str(row.get("error_message", "") or "").lower()
        if any(k in em for k in ("block", "unsupport", "refuse", "not permit", "only select")):
            return "Refused"
        return "Failed"

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 0 — OVERVIEW
    # ══════════════════════════════════════════════════════════════════════════
    with tabs[0]:
        st.markdown("#### SYSTEM HEALTH — AT A GLANCE")
        st.markdown("---")

        r1c1, r1c2, r1c3, r1c4 = st.columns(4)
        with r1c1:
            st.metric("Total queries (7d)", f"{total_q:,}", delta_q)
        with r1c2:
            p95_label = "🔴 Above target" if p95_s > 10 else "🟢 On target"
            st.metric("P95 response time (7d)", f"{p95_s:.1f}s", f"Target: <10s  {p95_label}")
        with r1c3:
            st.metric("Query success rate (7d)", f"{success_r:.0f}%", delta_sr)
        with r1c4:
            st.metric("Intent match rate (7d)", f"{intent_match:.0f}%", "Target: ≥95%")

        r2c1, r2c2, _, __ = st.columns(4)
        with r2c1:
            st.metric("Weekly active users", str(wau), delta_wau)
        with r2c2:
            st.metric("Thumbs-up ratio (30d)", f"{thumbs_pct:.0f}%", ratio_str)

        st.markdown("---")

        left, right = st.columns([6, 4], gap="large")

        with left:
            st.markdown("**Queries per day (last 14 days)**")
            st.caption("Usage volume trend")
            if not df_14d.empty:
                daily_cnt = df_14d.groupby("day").size().reset_index(name="queries")
                fig_daily = px.bar(
                    daily_cnt, x="day", y="queries",
                    color_discrete_sequence=["#5B5FC7"],
                    template="plotly_white",
                )
                fig_daily.update_layout(**PT, height=320, xaxis_title="", yaxis_title="")
                st.plotly_chart(fig_daily, use_container_width=True)
            else:
                st.info("No query data yet — run some queries in the Chat page to populate this chart.")

        with right:
            st.markdown("**Query outcome breakdown**")
            st.caption("Success vs failed vs refused")
            if not df_14d.empty:
                df_14d_out = df_14d.copy()
                df_14d_out["outcome"] = df_14d_out.apply(_classify_outcome, axis=1)
                outcome_cnt = df_14d_out["outcome"].value_counts().reset_index()
                outcome_cnt.columns = ["outcome", "count"]
                color_map = {"Success": "#2ecc71", "Failed": "#e74c3c", "Refused": "#95a5a6"}
                fig_pie = px.pie(
                    outcome_cnt, names="outcome", values="count",
                    color="outcome", color_discrete_map=color_map,
                    hole=0.55, template="plotly_white",
                )
                fig_pie.update_layout(**PT, height=320, showlegend=True)
                fig_pie.update_traces(textposition="inside", textinfo="percent")
                st.plotly_chart(fig_pie, use_container_width=True)
            else:
                st.info("No outcome data yet.")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 1 — PERFORMANCE
    # ══════════════════════════════════════════════════════════════════════════
    with tabs[1]:
        st.markdown("#### Performance")

        pc1, pc2, pc3, pc4 = st.columns(4)
        pc1.metric("P50 latency (7d)", f"{p50_s:.2f}s")
        pc2.metric("P95 latency (7d)", f"{p95_s:.2f}s",
                   "🔴 Above target" if p95_s > 10 else "🟢 On target")
        pc3.metric("P99 latency (7d)", f"{p99_s:.2f}s")
        pc4.metric("Samples (7d)", f"{len(latencies):,}")

        st.markdown("---")
        perf_l, perf_r = st.columns(2, gap="large")

        with perf_l:
            st.markdown("**Latency distribution (ms)**")
            if len(latencies):
                fig_hist = px.histogram(
                    x=latencies, nbins=30,
                    labels={"x": "Latency (ms)", "y": "Queries"},
                    color_discrete_sequence=["#7c3aed"],
                    template="plotly_white",
                )
                fig_hist.update_layout(**PT, height=300, xaxis_title="Latency (ms)", yaxis_title="Count")
                st.plotly_chart(fig_hist, use_container_width=True)
            else:
                st.info("No latency data for last 7 days.")

        with perf_r:
            st.markdown("**Failed queries by reason**")
            df_fail = df_7d[df_7d["success"] == 0].copy() if not df_7d.empty else pd.DataFrame()
            if not df_fail.empty:
                def _fail_reason(em):
                    em = str(em or "").lower()
                    if "timeout"  in em: return "DB timeout"
                    if "syntax"   in em or "sql" in em: return "SQL error"
                    if any(k in em for k in ("unsupport", "refuse", "not permit", "only select")):
                        return "Unsupported intent"
                    return "Other"
                df_fail["reason"] = df_fail["error_message"].apply(_fail_reason)
                reason_cnt = df_fail["reason"].value_counts().reset_index()
                reason_cnt.columns = ["reason", "count"]
                fig_fail = px.pie(
                    reason_cnt, names="reason", values="count",
                    hole=0.42, template="plotly_white",
                    color_discrete_sequence=["#ef4444","#f59e0b","#94a3b8","#3b82f6"],
                )
                fig_fail.update_layout(**PT, height=300)
                st.plotly_chart(fig_fail, use_container_width=True)
            else:
                st.info("No failures in last 7 days 🎉")

        st.markdown("---")
        st.markdown("**P95 latency trend (last 10 days)**")
        df_10d = _sql(f"""
            SELECT DATE(timestamp) AS day, latency_ms
            FROM query_log
            WHERE DATE(timestamp) >= '{d10_start}' AND latency_ms IS NOT NULL
        """)
        if not df_10d.empty:
            daily_p95 = (
                df_10d.groupby("day")["latency_ms"]
                .apply(lambda x: float(np.percentile(x, 95)) / 1000)
                .reset_index(name="p95_s")
            )
            fig_trend = go.Figure()
            fig_trend.add_trace(go.Scatter(
                x=daily_p95["day"], y=daily_p95["p95_s"],
                mode="lines+markers", name="P95",
                line=dict(color="#7c3aed", width=2.5),
                marker=dict(size=7, color="#7c3aed"),
            ))
            fig_trend.add_hline(
                y=10, line_dash="dash", line_color="#ef4444",
                annotation_text="Target 10s",
                annotation_position="top right",
            )
            fig_trend.update_layout(
                **PT, height=300,
                xaxis_title="", yaxis_title="Seconds",
                yaxis=dict(rangemode="tozero"),
            )
            st.plotly_chart(fig_trend, use_container_width=True)
        else:
            st.info("Not enough data for trend chart yet — run some queries to populate it.")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 2 — QUALITY
    # ══════════════════════════════════════════════════════════════════════════
    with tabs[2]:
        st.markdown("#### Quality")

        if not df_14d.empty:
            df_14d_q = df_14d.copy()
            df_14d_q["outcome"] = df_14d_q.apply(_classify_outcome, axis=1)
            stacked = df_14d_q.groupby(["day", "outcome"]).size().reset_index(name="count")
            color_map = {"Success": "#2ecc71", "Failed": "#e74c3c", "Refused": "#95a5a6"}
            fig_stack = px.bar(
                stacked, x="day", y="count", color="outcome",
                title="Query outcome breakdown per day (last 14 days)",
                color_discrete_map=color_map,
                barmode="stack", template="plotly_white",
            )
            fig_stack.update_layout(**PT, height=340, xaxis_title="", yaxis_title="Queries")
            st.plotly_chart(fig_stack, use_container_width=True)
        else:
            st.info("No data yet.")

        st.markdown("---")
        st.markdown("#### User Feedback (30 days)")
        fb_c1, fb_c2, fb_c3 = st.columns(3)
        fb_c1.metric("👍 Thumbs up",   str(ups))
        fb_c2.metric("👎 Thumbs down", str(downs))
        fb_c3.metric("Ratio",          ratio_str)

        if not df_fb30.empty and total_fb:
            fig_fb = px.pie(
                df_fb30, names="feedback_type", values="cnt",
                color="feedback_type",
                color_discrete_map={"thumbs_up":"#2ecc71","thumbs_down":"#ef4444"},
                hole=0.5, template="plotly_white",
                title="Feedback split",
            )
            fig_fb.update_layout(**PT, height=280)
            st.plotly_chart(fig_fb, use_container_width=True)

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 3 — ADOPTION
    # ══════════════════════════════════════════════════════════════════════════
    with tabs[3]:
        st.markdown("#### Adoption")

        adp_c1, adp_c2, adp_c3 = st.columns(3)
        adp_c1.metric("Weekly active users (7d)", str(wau))
        adp_c2.metric("Total queries (7d)", f"{total_q:,}")
        qpu = round(total_q / wau, 1) if wau else 0.0
        adp_c3.metric("Avg queries / user (7d)", str(qpu))

        st.markdown("---")

        df_sess = _sql(f"""
            SELECT DATE(start_time) AS day, COUNT(*) AS sessions
            FROM user_session
            WHERE DATE(start_time) >= '{d14_start}'
            GROUP BY day ORDER BY day
        """)
        if not df_sess.empty:
            fig_sess = px.bar(
                df_sess, x="day", y="sessions",
                title="New sessions per day (last 14 days)",
                color_discrete_sequence=["#7c3aed"],
                template="plotly_white",
            )
            fig_sess.update_layout(**PT, height=300, xaxis_title="", yaxis_title="Sessions")
            st.plotly_chart(fig_sess, use_container_width=True)
        else:
            st.info("No session data yet.")

        st.markdown("---")
        st.markdown("**Top intent domains (7d)**")
        if not df_7d.empty:
            top_intent = (
                df_7d[df_7d["intent_domain"].fillna("") != ""]
                ["intent_domain"].value_counts().head(10).reset_index()
            )
            top_intent.columns = ["intent", "count"]
            if not top_intent.empty:
                fig_intent = px.bar(
                    top_intent, x="count", y="intent",
                    orientation="h",
                    title="",
                    color_discrete_sequence=["#7c3aed"],
                    template="plotly_white",
                )
                fig_intent.update_layout(
                    **PT, height=max(260, len(top_intent) * 36 + 60),
                    yaxis=dict(autorange="reversed"),
                    xaxis_title="Queries", yaxis_title="",
                )
                st.plotly_chart(fig_intent, use_container_width=True)
            else:
                st.info("No intent data yet.")

        st.markdown("**Top chart types requested (7d)**")
        if not df_7d.empty and "chart_type" in df_7d.columns:
            ct_cnt = (
                df_7d[df_7d["chart_type"].fillna("") != ""]
                ["chart_type"].value_counts().reset_index()
            )
            ct_cnt.columns = ["chart_type", "count"]
            if not ct_cnt.empty:
                fig_ct = px.pie(
                    ct_cnt, names="chart_type", values="count",
                    hole=0.42, template="plotly_white",
                    color_discrete_sequence=PT["colorway"],
                )
                fig_ct.update_layout(**PT, height=280)
                st.plotly_chart(fig_ct, use_container_width=True)

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 4 — KPI TRACKER
    # ══════════════════════════════════════════════════════════════════════════
    with tabs[4]:
        st.markdown("#### KPI Tracker")

        failed_rate = 100.0 - success_r
        qpu_val     = round(total_q / wau, 1) if wau else 0.0

        def _status(actual, target, higher_is_better=True):
            ok = actual >= target if higher_is_better else actual <= target
            return "🟢 On track" if ok else "🔴 Off track"

        p0_rows = [
            {"Metric": "Total queries (7d)",   "Target": "≥ 500",  "Actual": f"{total_q:,}",       "Status": _status(total_q,   500),       "Deadline": "30 Apr"},
            {"Metric": "P95 response time",    "Target": "< 10s",  "Actual": f"{p95_s:.1f}s",      "Status": _status(p95_s,     10,  False), "Deadline": "30 Apr"},
            {"Metric": "Query success rate",   "Target": "≥ 90%",  "Actual": f"{success_r:.0f}%",  "Status": _status(success_r, 90),         "Deadline": "30 Apr"},
            {"Metric": "Weekly active users",  "Target": "≥ 20",   "Actual": str(wau),              "Status": _status(wau,       20),         "Deadline": "30 Apr"},
        ]
        p1_rows = [
            {"Metric": "Intent match rate",    "Target": "≥ 95%",  "Actual": f"{intent_match:.0f}%","Status": _status(intent_match,  95),        "Deadline": "30 Jun"},
            {"Metric": "Thumbs-up ratio",      "Target": "≥ 80%",  "Actual": f"{thumbs_pct:.0f}%", "Status": _status(thumbs_pct,    80),         "Deadline": "30 Jun"},
            {"Metric": "Failed query rate",    "Target": "< 10%",  "Actual": f"{failed_rate:.0f}%","Status": _status(failed_rate,   10,  False), "Deadline": "30 Jun"},
            {"Metric": "Queries / user (7d)",  "Target": "≥ 5",    "Actual": str(qpu_val),          "Status": _status(qpu_val,       5),          "Deadline": "30 Jun"},
        ]

        st.markdown("##### P0 Metrics — deadline 30 April")
        st.dataframe(pd.DataFrame(p0_rows), use_container_width=True, hide_index=True)

        st.markdown("##### P1 Metrics — deadline 30 June")
        st.dataframe(pd.DataFrame(p1_rows), use_container_width=True, hide_index=True)

        st.markdown("---")
        st.caption(f"All metrics computed live from `query_log` · `feedback` · `user_session` tables · refreshed on each page load · last updated {datetime.now().strftime('%H:%M · %d %b %Y')}")


# ══════════════════════════════════════════════════════════════════════════════
# ROUTER
# ══════════════════════════════════════════════════════════════════════════════
{
    "rights":       page_rights,
    "titles":       page_titles,
    "dna":          page_dna,
    "sales":        page_sales,
    "deals":        page_deals,
    "vendors":      page_vendors,
    "work_orders":  page_work_orders,
    "gap_analysis": page_gap_analysis,
    "compare":      page_compare,
    "alerts":       page_alerts,
    "title_360":    page_title_360,
    "chat":         page_chat,
    "dashboard":    page_custom_dashboard,
    "analytics":    page_analytics,
}.get(st.session_state.page, page_rights)()

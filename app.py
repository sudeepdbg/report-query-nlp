"""
Foundry Vantage — Rights Explorer
Persona: Business Affairs & Strategy | HBO/Cinemax/HBO Max
Pages: Rights Explorer · Title Catalog · Do-Not-Air · Sales · Work Orders · Chat · Custom Dashboard
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time
import re
import html
from datetime import datetime, timedelta
import logging
from typing import Optional
from utils.database import init_database, execute_sql, get_table_stats, save_alert, dismiss_alert, get_alerts
from utils.query_parser import parse_query

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="Foundry Vantage — Rights Explorer",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ═══════════════════════════════════════════════════════════════════════════════
# CSS — Enhanced with Dashboard Styles
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown("""
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
html,body,*{font-family:'Inter',sans-serif!important;box-sizing:border-box}
#MainMenu,footer,[data-testid="stStatusWidget"],[data-testid="stDecoration"]{display:none!important}

.stApp,[data-testid="stAppViewContainer"],[data-testid="stMain"]{background:#f0f2f5!important}
[data-testid="block-container"]{background:#f0f2f5!important;padding:1.5rem 2rem 5rem!important;max-width:1700px!important}

/* Sidebar */
[data-testid="stSidebar"]>div:first-child{background:#08101f!important;border-right:1px solid rgba(255,255,255,.05)!important}
[data-testid="stSidebar"] *{color:#94a3b8!important}
[data-testid="stSidebar"] .stButton>button{width:100%!important;text-align:left!important;
  justify-content:flex-start!important;background:transparent!important;border:none!important;
  border-radius:8px!important;color:#94a3b8!important;font-size:.86rem!important;
  font-weight:500!important;padding:.6rem 1rem!important;box-shadow:none!important;transition:all .15s!important}
[data-testid="stSidebar"] .stButton>button:hover{background:rgba(255,255,255,.07)!important;color:#e2e8f0!important}
[data-testid="stSidebar"] .stButton>button:disabled{display:none!important}
[data-testid="stSidebar"] [data-baseweb="select"]>div{background:#111827!important;
  border:1px solid #1e293b!important;border-radius:8px!important}
[data-testid="stSidebar"] [data-baseweb="select"] span{color:#e2e8f0!important}
[data-testid="stSidebar"] input{background:#111827!important;border:1px solid #1e293b!important;
  border-radius:8px!important;color:#e2e8f0!important}

/* Main buttons */
section.main .stButton>button{background:#7c3aed!important;color:#fff!important;border:none!important;
  border-radius:8px!important;font-weight:600!important;padding:.5rem 1.2rem!important;transition:all .15s!important}
section.main .stButton>button:hover{background:#6d28d9!important;transform:translateY(-1px)!important}
section.main .stButton>button:disabled{background:#e2e8f0!important;color:#94a3b8!important;transform:none!important}

/* Tabs */
[data-testid="stTabs"] [data-baseweb="tab-list"]{background:#e2e8f0!important;border-radius:10px!important;
  padding:3px!important;border:none!important;gap:2px!important}
[data-testid="stTabs"] [data-baseweb="tab"]{background:transparent!important;color:#64748b!important;
  border-radius:7px!important;font-size:.82rem!important;font-weight:500!important;
  border:none!important;padding:6px 14px!important}
[data-testid="stTabs"] [aria-selected="true"]{background:#7c3aed!important;color:#fff!important;font-weight:700!important}

/* Metrics */
section.main [data-testid="stMetric"]{background:#fff!important;border:1px solid #e2e8f0!important;
  border-radius:12px!important;padding:.85rem 1rem!important;
  box-shadow:0 1px 3px rgba(0,0,0,.04)!important}
section.main [data-testid="stMetricLabel"]>div{font-size:.62rem!important;font-weight:700!important;
  letter-spacing:.07em!important;text-transform:uppercase!important;color:#94a3b8!important}
section.main [data-testid="stMetricValue"]>div{font-size:1.45rem!important;font-weight:800!important;color:#0f172a!important}

/* Containers */
[data-testid="stVerticalBlockBorderWrapper"]{background:#fff!important;border:1px solid #e2e8f0!important;
  border-radius:12px!important;box-shadow:0 1px 3px rgba(0,0,0,.04)!important}
section.main input,section.main textarea{background:#fff!important;border:1.5px solid #e2e8f0!important;
  border-radius:8px!important;color:#111827!important}
section.main input:focus,section.main textarea:focus{border-color:#7c3aed!important}
section.main [data-baseweb="select"]>div{background:#fff!important;border:1.5px solid #e2e8f0!important;border-radius:8px!important}
[data-baseweb="popover"],[data-baseweb="menu"]{background:#fff!important;border:1px solid #e2e8f0!important;
  border-radius:10px!important;box-shadow:0 10px 30px rgba(0,0,0,.1)!important}
[role="option"]{background:#fff!important;color:#111827!important}
[role="option"]:hover{background:#f5f3ff!important}
[role="option"][aria-selected="true"]{background:#ede9fe!important;color:#4c1d95!important}
[data-testid="stDataFrame"]{border:1px solid #e2e8f0!important;border-radius:10px!important;overflow:hidden!important}
[data-testid="stPlotlyChart"]{border:1px solid #e2e8f0!important;border-radius:12px!important;
  overflow:hidden!important;background:#fff!important}
[data-testid="stExpander"] summary{background:#f8fafc!important;border:1px solid #e2e8f0!important;
  border-radius:8px!important;font-weight:600!important;color:#374151!important}
[data-testid="stExpander"]>div:last-child{border:1px solid #e2e8f0!important;border-top:none!important;
  border-radius:0 0 8px 8px!important;background:#fff!important}
hr{border:none!important;border-top:1px solid #e2e8f0!important;margin:.8rem 0!important}
[data-testid="stProgress"]>div{background:#e2e8f0!important}
[data-testid="stProgress"]>div>div{background:#7c3aed!important}

/* SQL box */
.sql-box{background:#0f172a;color:#e2e8f0;border-radius:8px;padding:14px 18px;
  font-family:'Courier New',monospace;font-size:12.5px;border-left:4px solid #7c3aed;
  margin:8px 0;overflow-x:auto;white-space:pre-wrap}

/* Badge chips */
.badge{display:inline-block;padding:2px 9px;border-radius:20px;font-size:11px;font-weight:600;margin:2px}
.badge-green{background:#dcfce7;color:#166534}
.badge-red{background:#fee2e2;color:#991b1b}
.badge-amber{background:#fef3c7;color:#92400e}
.badge-blue{background:#dbeafe;color:#1e40af}
.badge-purple{background:#ede9fe;color:#5b21b6}
.badge-gray{background:#f1f5f9;color:#475569}

/* Page header */
.page-header{font-size:1.75rem;font-weight:800;color:#0f172a;line-height:1.2;margin-bottom:4px}
.page-sub{font-size:.85rem;color:#64748b;margin-bottom:1rem}

/* Rights window pill */
.window-pill{display:inline-block;padding:2px 10px;border-radius:20px;font-size:11px;font-weight:700;margin:2px}
.window-SVOD{background:#ede9fe;color:#5b21b6}
.window-PayTV{background:#dbeafe;color:#1e40af}
.window-STB-VOD{background:#fef3c7;color:#92400e}
.window-FAST{background:#dcfce7;color:#166534}
.window-CatchUp{background:#ffedd5;color:#9a3412}
.window-other{background:#f1f5f9;color:#475569}

/* Expiry urgency */
.exp-critical{background:#fee2e2;color:#991b1b;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:700}
.exp-warn{background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:700}
.exp-ok{background:#dcfce7;color:#166534;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:700}

/* Stat tile */
.stat-tile{background:#fff;border:1px solid #e2e8f0;border-radius:12px;padding:14px 16px;
  text-align:center;box-shadow:0 1px 3px rgba(0,0,0,.04)}
.stat-tile .val{font-size:1.6rem;font-weight:800;color:#0f172a;line-height:1.1}
.stat-tile .lbl{font-size:.6rem;font-weight:700;color:#94a3b8;text-transform:uppercase;
  letter-spacing:.07em;margin-top:4px}

/* ── Custom Dashboard Styles ─────────────────────────────────────────────────────── */
.db-card{background:#fff;border:1px solid #e2e8f0;border-radius:14px;
padding:0;overflow:hidden; box-shadow:0 2px 8px rgba(0,0,0,.05);
transition:box-shadow .2s}
.db-card:hover{box-shadow:0 4px 18px rgba(124,58,237,.12)}
.db-card-header{background:linear-gradient(135deg,#7c3aed 0%,#4f46e5 100%);
padding:10px 14px;display:flex;align-items:center;justify-content:space-between}
.db-card-title{font-size:.82rem;font-weight:700;color:#fff;
white-space:nowrap; overflow:hidden;text-overflow:ellipsis;max-width:85%}
.db-card-ts{font-size:.65rem;color:rgba(255,255,255,.65)}
.db-card-body{padding:10px 12px}
.db-empty{background:#f8faff;border:2px dashed #c7d2fe;border-radius:14px;
padding:36px 24px;text-align:center}
.db-empty-icon{font-size:2.4rem;margin-bottom:8px}
.db-empty-title{font-size:1rem;font-weight:700;color:#4f46e5;margin-bottom:6px}
.db-empty-body{font-size:.82rem;color:#64748b;line-height:1.6}
.db-suggested{background:#fff;border:1px solid #e2e8f0;border-radius:10px;
padding:12px 14px;cursor:pointer;transition:all .15s}
.db-suggested:hover{border-color:#7c3aed;background:#faf5ff}
.db-suggested-label{font-size:.78rem;font-weight:600;color:#0f172a}
.db-suggested-desc{font-size:.7rem;color:#64748b;margin-top:2px}
.db-metric-card{background:linear-gradient(135deg,#7c3aed 0%,#4f46e5 100%);
border-radius:14px;padding:20px 24px;text-align:center;color:#fff}
.db-metric-value{font-size:2.6rem;font-weight:800;line-height:1;color:#fff}
.db-metric-label{font-size:.75rem;font-weight:600;color:rgba(255,255,255,.7);
text-transform:uppercase;letter-spacing:.08em;margin-top:6px}
.db-metric-sub{font-size:.7rem;color:rgba(255,255,255,.5);margin-top:3px}
.db-query-pill{display:inline-flex;align-items:center;gap:6px;
background:#ede9fe;border-radius:20px;padding:3px 10px;
font-size:.72rem;font-weight:600;color:#5b21b6;margin-right:4px}
.db-toolbar{background:#fff;border:1px solid #e2e8f0;border-radius:10px;
padding:10px 14px;display:flex;align-items:center;gap:10px;flex-wrap:wrap;
margin-bottom:14px}
.suggested-queries-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));
gap:10px;margin-top:12px}
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
# DB init
# ═══════════════════════════════════════════════════════════════════════════════
@st.cache_resource(ttl=3600, show_spinner="Connecting to Rights database…")
def init_db():
    for i in range(3):
        try:
            conn = init_database()
            conn.cursor().execute("SELECT 1")
            return conn
        except Exception as e:
            if i == 2: st.error(str(e)); raise
            time.sleep(2**i)

# ═══════════════════════════════════════════════════════════════════════════════
# Session state — ENHANCED with Dashboard keys
# ═══════════════════════════════════════════════════════════════════════════════
for k, v in {
    'page': 'rights',
    'chat_history': [],
    'current_region': 'NA',
    'persona': 'Business Affairs',
    'user_prefs': {'show_sql': True, 'raw_sql_mode': False},
    'db_stats': {},
    'pending_prompt': None,
    'title_360': None,
    'compare_region': None,
    'alerts_count': 0,
    # ── Custom Dashboard ──────────────────────────────────────────────────────
    'dashboard_pins': [],        # list of pinned chart cards
    'dashboard_last_df': None,   # last NL query result dataframe
    'dashboard_last_meta': {},   # {query, sql, chart_type, region}
}.items():
    if k not in st.session_state:
        st.session_state[k] = v

try:
    DB_CONN = init_db()
    if not st.session_state.db_stats:
        st.session_state.db_stats = get_table_stats(DB_CONN)
except Exception as e:
    st.error(f"⚠️ {e}")
    st.stop()

# ═══════════════════════════════════════════════════════════════════════════════
# Navigation — ENHANCED with Dashboard page
# ═══════════════════════════════════════════════════════════════════════════════
PAGES = [
    ("rights",       "🔑",  "Rights Explorer"),
    ("titles",       "🎬",  "Title Catalog"),
    ("dna",          "🚫",  "Do-Not-Air"),
    ("sales",        "💸",  "Sales Deals"),
    ("deals",        "💼",  "Deals"),
    ("vendors",      "🏢",  "Vendors"),
    ("work_orders",  "⚙️",   "Work Orders"),
    ("gap_analysis", "🔍",  "Gap Analysis"),
    ("compare",      "⚖️",   "Compare Regions"),
    ("alerts",       "🔔",  "Alerts"),
    ("title_360",    "🎯",  "Title 360"),
    ("chat",         "💬",  "Chat / Query"),
    ("dashboard",    "📐",  "Custom Dashboard"),
]

# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════
def run(sql):
    df, err = execute_sql(sql, DB_CONN)
    if err: st.error(f"SQL error: {err}")
    return df if df is not None else pd.DataFrame()

def fmt_m(x):
    try:
        v = float(x)
        if v >= 1e9: return f"${v/1e9:.1f}B"
        if v >= 1e6: return f"${v/1e6:.1f}M"
        if v >= 1e3: return f"${v/1e3:.0f}K"
        return f"${v:.0f}"
    except: return str(x)

def exp_tag(days):
    try:
        d = int(days)
        if d < 0:  return "🔴 Expired"
        if d <= 30: return f"🔴 {d}d"
        if d <= 60: return f"🟡 {d}d"
        return f"🟢 {d}d"
    except: return "—"

def status_badge(s):
    m = {'Active':'badge-green','Approved':'badge-green','Completed':'badge-green',
         'Expired':'badge-red','Delayed':'badge-red','Pending':'badge-amber',
         'Suspended':'badge-gray','In Progress':'badge-blue'}
    c = m.get(str(s), 'badge-gray')
    return f'{s}'

def bool_icon(v):
    return "✅" if v in (1, "1", True, "Yes", "yes") else "❌"

PT = dict(plot_bgcolor='#ffffff', paper_bgcolor='#ffffff',
          font=dict(family='Inter,sans-serif', color='#6b7280', size=11),
          margin=dict(l=20,r=20,t=44,b=20),
          colorway=['#7c3aed','#f59e0b','#10b981','#ef4444','#3b82f6','#a78bfa','#fb923c'])

def bar(df, x, y, title="", h=300, horiz=False, color=None):
    if horiz:
        fig = px.bar(df, x=y, y=x, orientation='h', title=title, color=color,
                     color_discrete_sequence=['#7c3aed'])
    else:
        fig = px.bar(df, x=x, y=y, title=title, color=color,
                     color_discrete_sequence=['#7c3aed'])
    fig.update_layout(**PT, height=h); fig.update_xaxes(tickangle=-30)
    return fig

def pie(df, names, values, title="", h=300):
    fig = px.pie(df, names=names, values=values, title=title, hole=0.42)
    fig.update_layout(**PT, height=h)
    return fig

def stat_tiles(items):
    cols = st.columns(len(items))
    for col, (val, lbl, color) in zip(cols, items):
        col.markdown(
            f'''
            <div class="stat-tile" style="border-left:4px solid {color}">
                <div class="val">{val}</div>
                <div class="lbl">{lbl}</div>
            </div>
            ''', unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
# Sidebar
# ═══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("""
    <div style="text-align:center;padding:20px 0">
        <div style="font-size:2rem;margin-bottom:8px">🔑</div>
        <div style="font-size:1.1rem;font-weight:800;color:#e2e8f0">Foundry Vantage</div>
        <div style="font-size:.7rem;color:#64748b;margin-top:4px">Rights Explorer · MVP 2026</div>
    </div>
    """, unsafe_allow_html=True)
    
    # Navigation — emoji only in HTML div, plain text in st.button to avoid _arrow_right artifacts
    for pid, icon, label in PAGES:
        active = st.session_state.page == pid
        if active:
            st.markdown(
                f'<div style="display:flex;align-items:center;gap:10px;background:rgba(124,58,237,.2);'
                f'border:1px solid rgba(124,58,237,.5);border-radius:8px;padding:9px 14px;margin:2px 8px 2px 8px">'
                f'<span style="font-size:14px">{icon}</span>'
                f'<span style="font-size:.86rem;font-weight:700;color:#c4b5fd">{label}</span></div>',
                unsafe_allow_html=True)
        else:
            if st.button(f"{icon}  {label}", key=f"nav_{pid}", use_container_width=True):
                st.session_state.page = pid
                st.rerun()

    st.markdown('<hr style="border:none;border-top:1px solid rgba(255,255,255,.05);margin:8px 16px">', unsafe_allow_html=True)

    # Context panel
    st.markdown('<div style="padding:0 8px">', unsafe_allow_html=True)
    regions = ["NA", "APAC", "EMEA", "LATAM"]
    def _on_region():
        st.session_state.current_region = st.session_state._reg_sel
    st.selectbox("Region / Market", regions,
                 index=regions.index(st.session_state.current_region),
                 key="_reg_sel", on_change=_on_region)
    personas = ["Business Affairs", "Strategy", "Legal", "Operations", "Analytics"]
    def _on_persona():
        st.session_state.persona = st.session_state._per_sel
    st.selectbox("Persona", personas,
                 index=personas.index(st.session_state.persona)
                 if st.session_state.persona in personas else 0,
                 key="_per_sel", on_change=_on_persona)
    st.markdown('</div>', unsafe_allow_html=True)

    # DB stats
    stats = st.session_state.db_stats
    # Refresh alerts count
    alerts_live, _ = get_alerts(DB_CONN, region=st.session_state.current_region)
    st.session_state.alerts_count = len(alerts_live) if alerts_live is not None else 0

    stat_pairs = [
        ("title",         "Titles"),
        ("movie",         "Movies"),
        ("media_rights",  "Rights"),
        ("do_not_air",    "DNA"),
    ]
    _sidebar_html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:5px;margin:10px 8px">'
    for key, lbl in stat_pairs:
        _sidebar_html += (f'<div style="background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.07);'
                 f'border-radius:8px;padding:8px;text-align:center">'
                 f'<div style="font-size:1.1rem;font-weight:800;color:#e2e8f0">{stats.get(key,0):,}</div>'
                 f'<div style="font-size:.58rem;color:#475569;text-transform:uppercase;letter-spacing:.06em">{lbl}</div>'
                 f'</div>')
    _sidebar_html += '</div>'
    st.markdown(_sidebar_html, unsafe_allow_html=True)

    # Alerts badge
    ac = st.session_state.alerts_count
    if ac > 0:
        st.markdown(
            f'<div style="margin:0 8px 4px;background:rgba(239,68,68,.12);border:1px solid rgba(239,68,68,.3);'
            f'border-radius:8px;padding:6px 10px;font-size:.75rem;color:#fca5a5;cursor:pointer">'
            f'🔔  <b>{ac} active alert{"s" if ac!=1 else ""}</b> — click Alerts in nav</div>',
            unsafe_allow_html=True)

    # Context note
    st.markdown(
        f'<div style="margin:0 8px 8px;background:rgba(124,58,237,.1);border:1px solid rgba(124,58,237,.2);'
        f'border-radius:8px;padding:8px 10px;font-size:.75rem;color:#c4b5fd">'
        f'📍  <b>{st.session_state.current_region}</b> · {st.session_state.persona}<br>'
        f'<span style="color:#64748b;font-size:.68rem">HBO/Cinemax/HBO Max · U.S. default context</span>'
        f'</div>', unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
# CUSTOM DASHBOARD FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════
def _render_save_button(df, fig, query_text, chart_type, key_prefix):
    """Inline 'Save to Dashboard' button — persists card in session state."""
    btn_key = f"save_dash_{key_prefix}"
    if st.button("📌 Save to Dashboard", key=btn_key,
                 help="Pin this chart to your Custom Dashboard"):
        pin = {
            "query":      query_text,
            "chart_type": chart_type,
            "df":         df.copy(),
            "fig":        fig,
            "ts":         datetime.now().strftime("%H:%M · %d %b %Y"),
            "title":      query_text[:55] + ("…" if len(query_text) > 55 else ""),
            "region":     st.session_state.get("current_region", ""),
        }
        st.session_state.dashboard_pins.insert(0, pin)
        st.success("✅ Pinned to Dashboard — visit 📐 Custom Dashboard in the sidebar.")

def _suggested_queries_panel(key_prefix: str = "sug"):
    """
    Renders a grid of suggested queries based on the media_rights schema.
    Shown when an NL query returns no results.
    """
    suggestions = [
        {
            "label":  "Rights expiring in 30 days",
            "query":  "SVOD rights expiring in 30 days",
            "icon":   "⏰",
            "desc":   "Active rights · SVOD · 30-day window",
        },
        {
            "label":  "Distribution by Territory",
            "query":  "Distribution breakdown by territory",
            "icon":   "🌍",
            "desc":   "Active rights grouped by territory",
        },
        {
            "label":  "Rights by Platform",
            "query":  "Rights distribution by media platform",
            "icon":   "📺",
            "desc":   "PayTV · SVOD · FAST · STB-VOD mix",
        },
        {
            "label":  "Movies with DNA flags",
            "query":  "Movies with do-not-air restrictions",
            "icon":   "🚫",
            "desc":   "Cross-table: movie + DNA",
        },
        {
            "label":  "Exclusive rights count",
            "query":  "How many exclusive rights do we hold",
            "icon":   "⭐",
            "desc":   "Total exclusivity count by region",
        },
        {
            "label":  "Top buyers by deal value",
            "query":  "Sales deals by buyer sorted by deal value",
            "icon":   "💸",
            "desc":   "Outbound sales · buyer ranking",
        },
        {
            "label":  "Rights by deal source",
            "query":  "Deal source breakdown TRL C2 FRL",
            "icon":   "💼",
            "desc":   "TRL / C2 / FRL rights mix",
        },
        {
            "label":  "Expiring rights + active sales",
            "query":  "Rights expiring in 60 days with active sales deals",
            "icon":   "🔗",
            "desc":   "Cross-table: expiry + sales overlap",
        },
    ]
    st.markdown("""
    <div style="background:#faf5ff;border:1px solid #ddd6fe;border-radius:12px;
        padding:16px 18px;margin-top:12px">
      <div style="font-size:.78rem;font-weight:700;color:#5b21b6;
          text-transform:uppercase;letter-spacing:.06em;margin-bottom:12px">
       💡 Suggested queries based on the Rights Explorer schema
      </div>
    """, unsafe_allow_html=True)

    cols = st.columns(4)
    for i, s in enumerate(suggestions):
        with cols[i % 4]:
            if st.button(
                f"{s['icon']} {s['label']}",
                key=f"{key_prefix}_{i}_{hash(s['query'])}",
                use_container_width=True,
                help=s["desc"],
            ):
                st.session_state["dashboard_nl_input"] = s["query"]
                st.rerun()

    st.markdown("</div>", unsafe_allow_html=True)

def render_dynamic_dashboard(
    df: pd.DataFrame,
    chart_type: str,
    query_text: str,
    *,
    key_prefix: str = "dyn",
    show_save_button: bool = True,
) -> Optional[go.Figure]:
    """
    Renders the best visualisation for `df` given `chart_type` and `query_text`.
    Priority order
    ─────────────
    1. Single-value result  →  st.metric card
    2. Trend / time query   →  line chart  (columns: term_to / timestamp / date)
    3. chart_type == 'bar'  →  horizontal bar, top-10
    4. chart_type == 'pie'  →  donut chart
    5. Multi-column result  →  st.tabs(Chart View | Data View)
    6. Fallback             →  vertical bar + data table

    Returns the Plotly figure (or None for metric-only results) so the caller
    can store it in session state via the "Save to Dashboard" button.
    """
    # ── Shared plotly theme (mirrors app.py PT) ────────────────────────────
    _PT = dict(
        plot_bgcolor='#ffffff',
        paper_bgcolor='#ffffff',
        font=dict(family='Inter,sans-serif', color='#6b7280', size=11),
        margin=dict(l=20, r=20, t=44, b=20),
        colorway=['#7c3aed','#f59e0b','#10b981','#ef4444',
                  '#3b82f6','#a78bfa','#fb923c'],
    )
    q_lower   = query_text.lower()
    is_trend  = any(kw in q_lower for kw in ["trend", "time", "over time", "monthly", "weekly", "by month", "by year"])
    time_cols = [c for c in df.columns
                 if any(kw in c.lower() for kw in ["term_to", "term_from", "timestamp", "date", "month", "year"])]
    num_cols  = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    str_cols  = [c for c in df.columns if not pd.api.types.is_numeric_dtype(df[c])]
    title_txt = query_text[:70] + ("…" if len(query_text) > 70 else "")
    fig: Optional[go.Figure] = None

    # ── Coerce numeric-looking columns (fixed indentation) ─────────────────
    for c in df.columns:
        if not pd.api.types.is_numeric_dtype(df[c]):
            try:
                df[c] = pd.to_numeric(df[c], errors='ignore')
                if pd.api.types.is_numeric_dtype(df[c]):
                    num_cols = list(dict.fromkeys(num_cols + [c]))
                    str_cols = [x for x in str_cols if x != c]
            except Exception:
                pass

    # ── 1. Single-value / count result → metric card ──────────────────────
    if len(df) == 1 and len(df.columns) <= 2:
        val_col = num_cols[0] if num_cols else df.columns[-1]
        lbl_col = str_cols[0] if str_cols else df.columns[0]
        val     = df[val_col].iloc[0]
        lbl     = df[lbl_col].iloc[0] if lbl_col != val_col else val_col.replace("_", " ").title()

        # Format value
        try:
            v_float = float(val)
            if v_float >= 1e9:   fmt_val = f"${v_float/1e9:.1f}B"
            elif v_float >= 1e6: fmt_val = f"${v_float/1e6:.1f}M"
            elif v_float >= 1e3: fmt_val = f"{v_float:,.0f}"
            else:                fmt_val = f"{v_float:,.1f}" if v_float != int(v_float) else f"{int(v_float):,}"
        except (ValueError, TypeError):
            fmt_val = str(val)

        st.markdown(f"""
         <div class="db-metric-card">
           <div class="db-metric-value">{html.escape(fmt_val)}</div>
           <div class="db-metric-label">{html.escape(str(lbl))}</div>
           <div class="db-metric-sub">{html.escape(title_txt)}</div>
         </div>""", unsafe_allow_html=True)

        if show_save_button:
            _render_save_button(df, None, query_text, chart_type, key_prefix)
        return None   # no Plotly figure for a metric card

    # ── 2. Trend / time chart → line chart ───────────────────────────────
    if (is_trend or time_cols) and num_cols:
        t_col = time_cols[0] if time_cols else str_cols[0]
        y_col  = num_cols[0]

        # Try to parse dates so they sort correctly
        try:
            df = df.copy()
            df[t_col] = pd.to_datetime(df[t_col], errors='coerce')
            df = df.dropna(subset=[t_col]).sort_values(t_col)
        except Exception:
            pass

        color_col = str_cols[1] if len(str_cols) > 1 else None
        if color_col:
            fig = px.line(df, x=t_col, y=y_col, color=color_col, title=title_txt,
                          markers=True, color_discrete_sequence=_PT['colorway'])
        else:
            fig = px.line(df, x=t_col, y=y_col, title=title_txt,
                          markers=True, color_discrete_sequence=_PT['colorway'])
            fig.update_traces(line_color='#7c3aed', line_width=2.5,
                              marker=dict(color='#7c3aed', size=6))

        fig.update_layout(**_PT, height=360)
        fig.update_xaxes(title_text=t_col.replace("_", " ").title())
        fig.update_yaxes(title_text=y_col.replace("_", " ").title())

        if len(df.columns) > 2:
            # Multi-column → tabbed view
            tab_chart, tab_data = st.tabs(["📈 Chart View", "📋 Data View"])
            with tab_chart:
                st.plotly_chart(fig, use_container_width=True, key=f"{key_prefix}_line")
            with tab_data:
                st.dataframe(df, use_container_width=True, hide_index=True, height=320)
                st.download_button("📥 CSV", df.to_csv(index=False),
                                   "dashboard_export.csv", "text/csv",
                                   key=f"{key_prefix}_csv_line")
        else:
            st.plotly_chart(fig, use_container_width=True, key=f"{key_prefix}_line_s")

        if show_save_button:
            _render_save_button(df, fig, query_text, chart_type, key_prefix)
        return fig

    # ── 3. Bar chart → horizontal bar, top-10 ────────────────────────────
    if chart_type == 'bar' and str_cols and num_cols:
        x_col = str_cols[0]
        y_col = num_cols[0]
        top   = df.nlargest(10, y_col) if len(df) > 10 else df.sort_values(y_col, ascending=False)

        # Colour gradient — darker = bigger value
        n = len(top)
        colours = [f"rgba(124,58,237,{0.4 + 0.6*(i/(max(n-1,1))):.2f})" for i in range(n)]

        fig = go.Figure(go.Bar(
            y=top[x_col].astype(str),
            x=top[y_col],
            orientation='h',
            marker_color=list(reversed(colours)),
            text=top[y_col].apply(lambda v: f"{int(v):,}" if float(v)==int(float(v)) else f"{v:,.1f}"),
            textposition='auto',
        ))
        fig.update_layout(**_PT, height=max(280, n * 36 + 80),
                          title=title_txt,
                          xaxis_title=y_col.replace("_", " ").title(),
                          yaxis=dict(autorange='reversed'))

        if len(df.columns) > 2:
            tab_chart, tab_data = st.tabs(["📊 Chart View", "📋 Data View"])
            with tab_chart:
                st.plotly_chart(fig, use_container_width=True, key=f"{key_prefix}_bar")
                if len(df) > 10:
                    st.caption(f"Showing top 10 of {len(df):,} results. Full data in Data View.")
            with tab_data:
                st.dataframe(df, use_container_width=True, hide_index=True, height=320)
                st.download_button("📥 CSV", df.to_csv(index=False),
                                    "dashboard_export.csv", "text/csv",
                                   key=f"{key_prefix}_csv_bar")
        else:
            st.plotly_chart(fig, use_container_width=True, key=f"{key_prefix}_bar_s")
            if len(df) > 10:
                st.caption(f"Showing top 10 of {len(df):,} results.")

        if show_save_button:
            _render_save_button(df, fig, query_text, chart_type, key_prefix)
        return fig

    # ── 4. Pie chart ─────────────────────────────────────────────────────
    if chart_type == 'pie' and str_cols and num_cols:
        names_col  = str_cols[0]
        values_col = num_cols[0]
        plot_df    = df.head(10) if len(df) > 10 else df

        fig = px.pie(plot_df, names=names_col, values=values_col,
                     title=title_txt, hole=0.42,
                     color_discrete_sequence=_PT['colorway'])
        fig.update_layout(**_PT, height=340)
        fig.update_traces(textposition='inside', textinfo='percent+label')

        if len(df.columns) > 2:
            tab_chart, tab_data = st.tabs(["🥧 Chart View", "📋 Data View"])
            with tab_chart:
                st.plotly_chart(fig, use_container_width=True, key=f"{key_prefix}_pie")
            with tab_data:
                st.dataframe(df, use_container_width=True, hide_index=True, height=320)
                st.download_button("📥 CSV", df.to_csv(index=False),
                                    "dashboard_export.csv", "text/csv",
                                   key=f"{key_prefix}_csv_pie")
        else:
            st.plotly_chart(fig, use_container_width=True, key=f"{key_prefix}_pie_s")

        if show_save_button:
            _render_save_button(df, fig, query_text, chart_type, key_prefix)
        return fig

    # ── 5. Multi-column table result — smart chart + tabbed view  ────────
    if len(df.columns) >= 2:
        tab_chart, tab_data = st.tabs(["📊 Chart View", "📋 Data View"])

        with tab_chart:
            if str_cols and num_cols:
                x_col = str_cols[0]
                y_col = num_cols[0]
                top   = df.nlargest(10, y_col) if len(df) > 10 else df.sort_values(y_col, ascending=False)
                fig = px.bar(top, x=x_col, y=y_col, title=title_txt,
                             color_discrete_sequence=['#7c3aed'])
                fig.update_layout(**_PT, height=340)
                fig.update_xaxes(tickangle=-30)
                st.plotly_chart(fig, use_container_width=True, key=f"{key_prefix}_multi")
                if len(df) > 10:
                    st.caption(f"Chart shows top 10 of {len(df):,} rows.")
            else:
                st.info("No numeric column detected — showing data table only.")

        with tab_data:
            # Quick column-level KPIs for all numeric cols
            if num_cols:
                m_cols = st.columns(min(4, len(num_cols)))
                for i, nc in enumerate(num_cols[:4]):
                    try:
                        m_cols[i].metric(nc.replace("_", " ").title(),
                                         f"{df[nc].sum():,.0f}",
                                         help=f"Sum of {nc}")
                    except Exception:
                        pass
            st.dataframe(df, use_container_width=True, hide_index=True, height=340)
            st.download_button("📥 CSV", df.to_csv(index=False),
                                "dashboard_export.csv", "text/csv",
                               key=f"{key_prefix}_csv_multi")

        if show_save_button:
            _render_save_button(df, fig, query_text, chart_type, key_prefix)
        return fig

    # ── 6. Fallback — bare table ─────────────────────────────────────────
    st.dataframe(df, use_container_width=True, hide_index=True, height=300)
    if show_save_button:
        _render_save_button(df, None, query_text, chart_type, key_prefix)
    return None

def page_custom_dashboard():
    """
    Custom Dashboard Builder — NL → SQL → dynamic chart, with pinning.
    Layout
    ──────
    ┌─ header ────────────────────────────────────────────────────────────┐
    │  page title + sub                                                   │
    ├─ toolbar ───────────────────────────────────────────────────────────┤
    │  NL input · Run · Show SQL toggle · Region pill                     │
    ├─ live result ───────────────────────────────────────────────────────┤
    │  render_dynamic_dashboard(df, chart_type, query_text)               │
    │  ↳ includes "Save to Dashboard" button                              │
    ├─ pinned dashboard ──────────────────────────────────────────────────┤
    │  2-column card grid of saved charts                                  │
    └─────────────────────────────────────────────────────────────────────┘
    """
    reg = st.session_state.current_region

    # ── Page header ────────────────────────────────────────────────────────
    pin_count = len(st.session_state.dashboard_pins)
    st.markdown(
        '<div class="page-header">📐 Custom Dashboard Builder</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<div class="page-sub">'
        f'Generate visualisations on the fly with natural language · '
        f'<b>{reg}</b> · '
        f'<span class="db-query-pill">📌 {pin_count} pinned</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── Toolbar ────────────────────────────────────────────────────────────
    with st.container():
        col_inp, col_run = st.columns([5, 1])
        with col_inp:
            # Pre-fill from suggestion button if set
            default_q = st.session_state.pop("dashboard_nl_input", "")
            query_text = st.text_input(
                 "Natural language query",
                value=default_q,
                placeholder='e.g. "Rights expiring in 30 days" or "Distribution by territory"',
                label_visibility="collapsed",
                key="db_nl_query",
            )
        with col_run:
            run_clicked = st.button(
                 "▶ Run", type="primary", use_container_width=True, key="db_run"
            )

        t1, t2, t3 = st.columns(3)
        show_sql_dash = t1.toggle(
             "Show SQL",
            value=st.session_state.user_prefs.get("show_sql", True),
            key="db_show_sql",
        )
        export_enabled = t2.toggle("Auto CSV export", value=True, key="db_export")
        t3.markdown(
            f'<div style="font-size:.78rem;color:#64748b;padding-top:6px">'
            f'📍 Region:  <b>{reg}</b> · Persona:  <b>{st.session_state.persona}</b>'
            f'</div>',
            unsafe_allow_html=True,
        )

    st.markdown("---")

    # ── Query execution ────────────────────────────────────────────────────
    if run_clicked and query_text.strip():
        with st.spinner("Parsing query and fetching data…"):
            sql, parse_err, chart_type, region_ctx = parse_query(
                query_text.strip(), reg
            )

        if parse_err:
            st.error(f"❌ Parser error: {parse_err}")

        elif not sql:
            st.warning("Could not generate SQL for that query.")
            _suggested_queries_panel("run_fail")

        else:
            # ── Show SQL ────────────────────────────────────────────────
            if show_sql_dash:
                st.markdown(
                    f'<div class="sql-box">{html.escape(sql)}</div>',
                    unsafe_allow_html=True,
                )

            # ── Execute ────────────────────────────────────────────────
            res_df, db_err = execute_sql(sql, DB_CONN) 

            if db_err:
                st.error(f"❌ Database error: {db_err}")

            elif res_df is None or res_df.empty:
                # ── No results → fallback suggestions ───────────────────
                st.warning(
                    f"No records returned for **{region_ctx}**.  "
                    f"Try one of the suggested queries below."
                )
                _suggested_queries_panel("no_results")

            else:
                # ── Persist metadata for the Save button ─────────────
                st.session_state.dashboard_last_df   = res_df.copy()
                st.session_state.dashboard_last_meta = {
                     "query":      query_text.strip(),
                     "sql":        sql,
                     "chart_type": chart_type,
                     "region":     region_ctx,
                }

                # ── Result header ────────────────────────────────────
                st.markdown(
                    f'<div style="display:flex;align-items:center;gap:8px;'
                    f'margin-bottom:10px">'
                    f'<span style="font-size:.85rem;font-weight:700;color:#0f172a">'
                    f'📊 {len(res_df):,} records · {region_ctx}</span>'
                    f'<span class="db-query-pill">chart: {chart_type}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

                # ── Dynamic visualisation ────────────────────────────
                fig = render_dynamic_dashboard(
                     res_df.copy(),
                    chart_type,
                    query_text.strip(),
                    key_prefix="live",
                    show_save_button=True,
                )

                # ── Optional CSV download below chart ────────────────
                if export_enabled:
                    st.download_button(
                         "📥 Download full CSV",
                        res_df.to_csv(index=False),
                        f"dashboard_{region_ctx}_{chart_type}.csv",
                         "text/csv",
                        key="db_dl_full",
                    )

    elif not query_text.strip():
        # ── Empty state — show onboarding hints ───────────────────────────
        st.markdown("""
         <div class="db-empty">
           <div class="db-empty-icon">📐</div>
           <div class="db-empty-title">Build a chart with natural language</div>
           <div class="db-empty-body">
            Type a question above and click <b>▶ Run</b> to generate a dynamic visualisation.<br>
            Results are automatically mapped to the best chart type based on your query and data shape.<br><br>
             <b>Chart logic:</b>
            Single-value count → metric card  &nbsp;· &nbsp;
             "trend" / "time" keyword → line chart  &nbsp;· &nbsp;
            category + number → horizontal bar (top 10)  &nbsp;· &nbsp;
            status / type breakdown → donut pie  &nbsp;· &nbsp;
            multi-column → tabbed Chart + Data view
           </div>
         </div>
         """, unsafe_allow_html=True)
        _suggested_queries_panel("empty_state")

    # ══════════════════════════════════════════════════════════════════════
    #  PINNED DASHBOARD
    # ══════════════════════════════════════════════════════════════════════
    if st.session_state.dashboard_pins:
        st.markdown("---")

        # Dashboard controls
        dc1, dc2, dc3 = st.columns([4, 2, 2])
        dc1.markdown(
            f'<div style="font-size:1.05rem;font-weight:800;color:#0f172a;">'
            f'📌 Pinned Dashboard  <span style="font-size:.8rem;color:#7c3aed;'
            f'font-weight:600">({pin_count} cards)</span></div>',
            unsafe_allow_html=True,
        )
        layout_2col = dc2.toggle("2-column layout", value=True, key="db_2col")
        if dc3.button("🗑 Clear all pins", key="db_clear_pins"):
            st.session_state.dashboard_pins = []
            st.rerun()

        pins = st.session_state.dashboard_pins

        # Render grid
        n_cols = 2 if layout_2col else 1
        for row_start in range(0, len(pins), n_cols):
            cols = st.columns(n_cols)
            for col_idx in range(n_cols):
                pin_idx = row_start + col_idx
                if pin_idx >= len(pins):
                    break
                pin = pins[pin_idx]
                with cols[col_idx]:
                    # Card header
                    st.markdown(
                        f'<div class="db-card-header">'
                        f'<span class="db-card-title">{html.escape(pin["title"])}</span>'
                        f'<span class="db-card-ts">{pin["ts"]}</span>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )

                    # Card body — re-render chart or metric
                    with st.container():
                         # Region + chart type pill
                        st.markdown(
                            f'<div style="margin-bottom:6px">'
                            f'<span class="db-query-pill">📍 {pin["region"]}</span>'
                            f'<span class="db-query-pill">chart: {pin["chart_type"]}</span>'
                            f'</div>',
                            unsafe_allow_html=True,
                        )

                        if pin["fig"] is not None:
                            # Re-use the stored figure; give it a unique key
                            st.plotly_chart(
                                pin["fig"],
                                use_container_width=True,
                                key=f"pin_fig_{pin_idx}_{hash(pin['ts'])}",
                            )
                        else:
                            # Was a metric card — re-render it compactly
                            df_pin = pin["df"]
                            if not df_pin.empty:
                                num_c = [c for c in df_pin.columns
                                         if pd.api.types.is_numeric_dtype(df_pin[c])]
                                val = df_pin[num_c[0]].iloc[0] if num_c else df_pin.iloc[0, -1]
                                try:
                                    v = float(val)
                                    disp = f"{v:,.0f}" if v == int(v) else f"{v:,.2f}"
                                except (ValueError, TypeError):
                                    disp = str(val)
                                st.markdown(
                                     f'<div class="db-metric-card" style="padding:14px 18px">'
                                    f'<div class="db-metric-value" style="font-size:2rem">{disp}</div>'
                                    f'<div class="db-metric-sub">{html.escape(pin["title"])}</div>'
                                    f'</div>',
                                    unsafe_allow_html=True,
                                )

                        # Per-card actions
                        pa, pb = st.columns(2)
                         with pa:
                            st.download_button(
                                 "📥 CSV",
                                pin["df"].to_csv(index=False),
                                f"pin_{pin_idx}.csv",
                                 "text/csv",
                                key=f"pin_dl_{pin_idx}",
                                use_container_width=True,
                            )
                        with pb:
                            if st.button(
                                 "✕ Unpin",
                                key=f"pin_rm_{pin_idx}",
                                use_container_width=True,
                            ):
                                st.session_state.dashboard_pins.pop(pin_idx)
                                st.rerun()

# ═══════════════════════════════════════════════════════════════════════════════
# RIGHTS EXPLORER PAGE
# ═══════════════════════════════════════════════════════════════════════════════
def page_rights():
    reg = st.session_state.current_region
    st.markdown(f'''
    <div class="page-header">🔑 Rights Explorer</div>
    <div class="page-sub">Content rights licensed-in for <b>{reg}</b> — media rights, windows, territories, exclusivity & expiry</div>
    ''', unsafe_allow_html=True)
    # ── Top KPIs ─────────────────────────────────────────────────────────────
    kpi = run(f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status='Active' THEN 1 ELSE 0 END) AS active,
            SUM(CASE WHEN status='Expired' THEN 1 ELSE 0 END) AS expired,
             SUM(CASE WHEN status='Pending' THEN 1 ELSE 0 END) AS pending,
            SUM(CASE WHEN term_to  <= DATE('now','+30 days')
                      AND term_to  >= DATE('now') AND status='Active' THEN 1 ELSE 0 END) AS exp30,
            SUM(CASE WHEN term_to  <= DATE('now','+90 days')
                      AND term_to  >= DATE('now') AND status='Active' THEN 1 ELSE 0 END) AS exp90,
            COUNT(DISTINCT title_id) AS titles_covered,
            SUM(exclusivity) AS exclusive_count
        FROM media_rights WHERE UPPER(region)='{reg}'
        """)
    if not kpi.empty:
        r = kpi.iloc[0]
        items = [
            (f"{int(r.get('total',0)):,}",         "Total Rights",       "#0f172a"),
            (f"{int(r.get('active',0)):,}",        "Active",             "#166534"),
            (f"{int(r.get('exp30',0)):,}",          "⚠ Expiring 30d",    "#991b1b"),
            (f"{int(r.get('exp90',0)):,}",          "Expiring 90d",       "#92400e"),
            (f"{int(r.get('expired',0)):,}",        "Expired",            "#64748b"),
            (f"{int(r.get('titles_covered',0)):,}", "Titles Covered",     "#1e40af"),
            (f"{int(r.get('exclusive_count',0)):,}", "Exclusive",         "#5b21b6"),
        ]
        stat_tiles(items)

    st.divider()

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
         "⏰ Expiry Alerts",  "📺 Windows & Platforms",
         "🌍 Territories",  "🔒 Holdbacks",
         "⭐ Exclusivity",  "📄 Rights Table"
    ])

    # ── Expiry Alerts ─────────────────────────────────────────────────────────
    with tab1:
        st.markdown("#### Rights Expiring — Upcoming Windows")
        c1, c2, c3 = st.columns([2,1,1])
        days_sel  = c1.slider("Show expiring within (days)", 7, 180, 90, key="exp_days")
        plat_sel  = c2.multiselect("Platform", ["PayTV", "STB-VOD", "SVOD", "FAST"], key="exp_plat")
        rights_sel= c3.selectbox("Rights Type", ["All", "Exhibition", "Exhibition & Distribution"], key="exp_rt")

        plat_f  = ("AND (" + " OR ".join(f"media_platform_primary LIKE '%{p}%'" for p in plat_sel) + ") "
                   if plat_sel else "")
        rt_f    = f"AND rights_type='{rights_sel}'" if rights_sel != "All" else ""

        exp_df = run(f"""
            SELECT mr.rights_id, mr.title_name, t.series_id,
                   cd.deal_source, cd.deal_name, cd.deal_type,
                   mr.rights_type, mr.media_platform_primary, mr.media_platform_ancillary,
                   mr.territories, mr.language, mr.brand,
                   mr.exclusivity, mr.holdback, mr.holdback_days,
                   mr.term_to AS expiry_date,
                   CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_remaining,
                   mr.notes_restrictive, mr.status
            FROM media_rights mr
            JOIN content_deal cd ON mr.deal_id = cd.deal_id
            JOIN title t ON mr.title_id = t.title_id
            WHERE UPPER(mr.region)='{reg}' AND mr.status='Active'
              AND mr.term_to <= DATE('now','+{days_sel} days')
              AND mr.term_to  >= DATE('now')
              {plat_f} {rt_f}
            ORDER BY mr.term_to ASC
         """)

        if exp_df.empty:
            st.success(f"✅ No rights expiring within {days_sel} days in {reg}.")
        else:
            c1, c2 = st.columns([3,1])
            with c1:
                fig = go.Figure()
                colors = {'PayTV':'#3b82f6','STB-VOD':'#f59e0b','SVOD':'#7c3aed','FAST':'#10b981'}
                for plat in exp_df['media_platform_primary'].unique():
                    sub = exp_df[exp_df['media_platform_primary']==plat]
                    fig.add_bar(y=sub['title_name'], x=sub['days_remaining'],
                                name=plat, orientation='h',
                                marker_color=colors.get(plat,'#94a3b8'))
                fig.update_layout(**PT, height=max(300, len(exp_df)*18),
                                  title=f"Days to Expiry — {len(exp_df)} rights",
                                  xaxis_title="Days Remaining", barmode='group')
                st.plotly_chart(fig, use_container_width=True)
            with c2:
                st.markdown("**By Platform**")
                for p, cnt in exp_df.groupby('media_platform_primary').size().items():
                    st.markdown(f"- **{p}**: {cnt}")
                st.markdown(f"**Total: {len(exp_df)}**")
                st.markdown("**Exclusive:** " + str(int(exp_df['exclusivity'].sum())))

            # Formatted table — clean readable column labels
            show = exp_df.copy()
            show['Days Left']    = show['days_remaining'].apply(exp_tag)
            show['Exclusive']    = show['exclusivity'].apply(bool_icon)
            show['Holdback']     = show['holdback'].apply(bool_icon)
            show['Restrictions']  = show['notes_restrictive'].fillna('—')
            st.dataframe(
                show[['title_name','series_id','deal_source','media_platform_primary',
                       'territories','language','expiry_date','Days Left',
                       'Exclusive','Holdback','holdback_days','Restrictions']],
                use_container_width=True, hide_index=True,
                 column_config={
                     "title_name":             st.column_config.TextColumn("Title"),
                     "series_id":              st.column_config.TextColumn("Series"),
                     "deal_source":            st.column_config.TextColumn("Source"),
                     "media_platform_primary": st.column_config.TextColumn("Platform"),
                     "territories":            st.column_config.TextColumn("Territories"),
                     "language":               st.column_config.TextColumn("Language"),
                     "expiry_date":            st.column_config.TextColumn("Expiry Date"),
                     "Days Left":              st.column_config.TextColumn("⏰ Days Left"),
                     "Exclusive":              st.column_config.TextColumn("Exclusive ⭐"),
                     "Holdback":               st.column_config.TextColumn("Holdback 🔒"),
                     "holdback_days":          st.column_config.NumberColumn("Holdback Days"),
                     "Restrictions":           st.column_config.TextColumn("Restrictions"),
                })
            csv = exp_df.to_csv(index=False)
            ca, cb = st.columns([3,1])
            ca.download_button("📥 Export Expiry Report", csv, f"expiry_{reg}_{days_sel}d.csv", "text/csv")
            with cb:
                plat_label = f"{', '.join(plat_sel) if plat_sel else 'All'}"
                if st.button(f"🔔 Set Alert — {days_sel}d / {plat_label}", key="set_exp_alert", type="secondary"):
                    _, err = save_alert(
                        DB_CONN,
                        alert_type  = "Expiry",
                        label       = f"Rights expiring within {days_sel} days ({plat_label}) [{reg}]",
                        region      = reg,
                        platform    = plat_label,
                        days_threshold = days_sel,
                        persona     = st.session_state.persona,
                    )
                    if err: st.error(f"Alert save failed: {err}")
                    else:
                        st.success("🔔 Alert saved! View on the Alerts page.")
                        st.session_state.alerts_count += 1

    # ── Windows & Platforms ───────────────────────────────────────────────────
    with tab2:
        c1, c2 = st.columns(2)
        with c1:
            df = run(f"""
                SELECT media_platform_primary AS platform, status,
                       COUNT(*) AS count, COUNT(DISTINCT title_id) AS titles
                FROM media_rights WHERE UPPER(region)='{reg}'
                GROUP BY platform, status ORDER BY count DESC
             """)
            if not df.empty:
                fig = px.bar(df, x='platform', y='titles', color='status',
                             title="Titles Covered by Platform & Status",
                             color_discrete_map={'Active':'#10b981','Expired':'#ef4444',
                                                 'Pending':'#f59e0b','Suspended':'#94a3b8'})
                fig.update_layout(**PT, height=320, barmode='stack')
                st.plotly_chart(fig, use_container_width=True)
        with c2:
            df = run(f"""
                SELECT media_platform_primary AS platform, rights_type,
                       COUNT(*) AS count
                FROM media_rights WHERE UPPER(region)='{reg}'
                GROUP BY platform, rights_type ORDER BY count DESC
             """)
            if not df.empty:
                st.plotly_chart(pie(df,'platform','count','Rights Mix by Platform'), use_container_width=True)

        # Ancillary platforms breakdown
        st.markdown("#### Ancillary Platform Coverage")
        df = run(f"""
            SELECT media_platform_ancillary, COUNT(*) AS count,
                   COUNT(DISTINCT title_id) AS titles
            FROM media_rights
            WHERE UPPER(region)='{reg}' AND media_platform_ancillary != ''
            GROUP BY media_platform_ancillary ORDER BY count DESC
         """)
        if not df.empty:
            # Split comma-separated ancillary fields
            rows_exp = []
            for _, row in df.iterrows():
                for p in str(row['media_platform_ancillary']).split(','):
                    p = p.strip()
                    if p:
                        rows_exp.append({'ancillary_platform': p, 'count': row['count']})
            if rows_exp: 
                df_exp = pd.DataFrame(rows_exp).groupby('ancillary_platform')['count'].sum().reset_index()
                df_exp = df_exp.sort_values('count', ascending=False)
                st.plotly_chart(bar(df_exp,'ancillary_platform','count',
                                    "Ancillary Platform Rights Count", h=280), use_container_width=True)

        # Deal source breakdown
        st.markdown("#### Rights by Deal Source (TRL / C2 / FRL)")
        df = run(f"""
            SELECT cd.deal_source, COUNT(DISTINCT mr.title_id) AS titles,
                   COUNT(*) AS rights,
                   SUM(mr.exclusivity) AS exclusive,
                   SUM(mr.holdback) AS holdback_count,
                   SUM(CASE WHEN mr.status='Active' THEN 1 ELSE 0 END) AS active
            FROM media_rights mr
            JOIN content_deal cd ON mr.deal_id = cd.deal_id
            WHERE UPPER(mr.region)='{reg}'
            GROUP BY cd.deal_source ORDER BY titles DESC
         """)
        if not df.empty:
            c1, c2 = st.columns(2)
            with c1: st.plotly_chart(bar(df,'deal_source','titles','Titles by Deal Source',h=280), use_container_width=True)
            with c2: st.dataframe(df, use_container_width=True, hide_index=True)

    # ── Territories ───────────────────────────────────────────────────────────
    with tab3:
        st.markdown("#### Rights Coverage by Territory")
        # Explode comma-separated territories
        raw = run(f"""
            SELECT territories, COUNT(*) AS rights_count,
                   COUNT(DISTINCT title_id) AS titles,
                   SUM(exclusivity) AS exclusive,
                   SUM(CASE WHEN status='Active' THEN 1 ELSE 0 END) AS active
            FROM media_rights WHERE UPPER(region)='{reg}'
            GROUP BY territories
         """)
        if not raw.empty:
            rows_t = []
            for _, row in raw.iterrows():
                for t in str(row['territories']).split(','):
                    t = t.strip()
                    if t:
                        rows_t.append({'territory':t,'rights_count':row['rights_count'],
                                       'titles':row['titles'],'exclusive':row['exclusive'],
                                       'active':row['active']})
            if rows_t:
                terr_df = (pd.DataFrame(rows_t)
                           .groupby('territory')
                            .agg(rights_count=('rights_count','sum'),
                                titles=('titles','sum'),
                                exclusive=('exclusive','sum'),
                                 active=('active','sum'))
                           .reset_index()
                           .sort_values('rights_count', ascending=False))
                c1, c2 = st.columns(2)
                with c1: st.plotly_chart(bar(terr_df.head(15),'territory','rights_count','Rights Count by Territory',h=320,horiz=True), use_container_width=True)
                with c2: st.plotly_chart(bar(terr_df.head(15),'territory','titles','Titles Covered by Territory',h=320,horiz=True), use_container_width=True)
                st.dataframe(terr_df, use_container_width=True, hide_index=True)

    # ── Holdbacks ──────────────────────────────────────────────────────────────
    with tab4:
        st.markdown("#### Holdback Analysis")
        st.caption("Holdback = a waiting period before a rights window opens, typically after theatrical or pay TV.")
        df = run(f"""
            SELECT media_platform_primary AS platform,
                   COUNT(*) AS with_holdback,
                   AVG(holdback_days) AS avg_holdback_days,
                   MAX(holdback_days) AS max_holdback_days,
                   MIN(holdback_days) AS min_holdback_days
            FROM media_rights
            WHERE UPPER(region)='{reg}' AND holdback=1 AND holdback_days > 0
            GROUP BY platform ORDER BY avg_holdback_days DESC
         """)
        if not df.empty:
            c1, c2 = st.columns(2)
            with c1: st.plotly_chart(bar(df,'platform','avg_holdback_days','Avg Holdback Days by Platform',h=300), use_container_width=True)
            with c2: st.dataframe(df, use_container_width=True, hide_index=True)

        # Holdback vs non-holdback title count
        df2 = run(f"""
            SELECT media_platform_primary AS platform,
                   SUM(CASE WHEN holdback=1 THEN 1 ELSE 0 END) AS has_holdback,
                   SUM(CASE WHEN holdback=0 THEN 1 ELSE 0 END) AS no_holdback
            FROM media_rights WHERE UPPER(region)='{reg}'
            GROUP BY platform ORDER BY has_holdback DESC
         """)
        if not df2.empty:
            fig = go.Figure()
            fig.add_bar(x=df2['platform'], y=df2['has_holdback'], name='Has Holdback', marker_color='#ef4444')
            fig.add_bar(x=df2['platform'], y=df2['no_holdback'],  name='No Holdback',  marker_color='#10b981')
            fig.update_layout(**PT, height=300, barmode='group', title="Holdback vs No-Holdback by Platform")
            st.plotly_chart(fig, use_container_width=True)

    # ── Exclusivity ────────────────────────────────────────────────────────────
    with tab5:
        df = run(f"""
            SELECT media_platform_primary AS platform,
                   SUM(exclusivity) AS exclusive,
                   COUNT(*) - SUM(exclusivity) AS non_exclusive,
                   COUNT(*) AS total
            FROM media_rights WHERE UPPER(region)='{reg}'
            GROUP BY platform ORDER BY exclusive DESC
         """)
        if not df.empty:
            c1, c2 = st.columns(2)
            with c1:
                fig = go.Figure()
                fig.add_bar(x=df['platform'], y=df['exclusive'],     name='Exclusive',     marker_color='#7c3aed')
                fig.add_bar(x=df['platform'], y=df['non_exclusive'],  name='Non-Exclusive', marker_color='#c4b5fd')
                fig.update_layout(**PT, height=300, barmode='stack', title="Exclusivity by Platform")
                st.plotly_chart(fig, use_container_width=True)
            with c2:
                df['excl_pct'] = (df['exclusive']/df['total']*100).round(1)
                st.dataframe(df[['platform','total','exclusive','non_exclusive','excl_pct']],
                             use_container_width=True, hide_index=True)

        # Exclusive by rights type
        df3 = run(f"""
            SELECT rights_type,
                   SUM(exclusivity) AS exclusive,
                   COUNT(*)-SUM(exclusivity) AS non_exclusive
            FROM media_rights WHERE UPPER(region)='{reg}'
            GROUP BY rights_type
         """)
        if not df3.empty:
            st.plotly_chart(pie(df3,'rights_type','exclusive','Exclusivity Mix by Rights Type',h=280), use_container_width=True)

    # ── Rights Table ────────────────────────────────────────────────────────
    with tab6:
        st.markdown("#### Browse All Rights")
        f1, f2, f3, f4 = st.columns(4)
        plat_f2  = f1.selectbox("Platform",    ["All", "PayTV", "STB-VOD", "SVOD", "FAST"], key="rt2_plat")
        stat_f2  = f2.selectbox("Status",      ["All", "Active", "Expired", "Pending"],    key="rt2_stat")
        excl_f2  = f3.selectbox("Exclusivity", ["All", "Exclusive", "Non-Exclusive"],     key="rt2_excl")
        src_f2   = f4.selectbox("Deal Source", ["All", "TRL", "C2", "FRL"],                key="rt2_src")

        extras = ""
        if plat_f2 != "All": extras += f" AND mr.media_platform_primary LIKE '%{plat_f2}%'"
        if stat_f2 != "All": extras += f" AND mr.status='{stat_f2}'"
        if excl_f2 == "Exclusive":     extras += " AND mr.exclusivity=1"
        if excl_f2 == "Non-Exclusive": extras += " AND mr.exclusivity=0"
        if src_f2  != "All":           extras += f" AND cd.deal_source='{src_f2}'"

        df = run(f"""
            SELECT mr.rights_id, mr.title_name, t.series_id,
                   cd.deal_source, cd.deal_type, cd.deal_name,
                   mr.rights_type, mr.media_platform_primary, mr.media_platform_ancillary,
                   mr.territories, mr.language, mr.brand,
                   mr.exclusivity, mr.holdback, mr.holdback_days,
                   mr.term_from, mr.term_to,
                   CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_remaining,
                   mr.status
            FROM media_rights mr
            JOIN content_deal cd ON mr.deal_id = cd.deal_id
            JOIN title t ON mr.title_id = t.title_id
            WHERE UPPER(mr.region)='{reg}' {extras}
            ORDER BY mr.term_to ASC LIMIT 400
         """)
        if not df.empty:
            show = df.copy()
            show['Days Left']  = show['days_remaining'].apply(exp_tag)
            show['Exclusive']  = show['exclusivity'].apply(bool_icon)
            show['Holdback']   = show['holdback'].apply(bool_icon)
            st.caption(f"{len(df)} rights records")
            st.dataframe(
                show[['title_name','series_id','deal_source','rights_type',
                       'media_platform_primary','media_platform_ancillary',
                        'territories','language','brand',
                       'term_from','term_to','Days Left','Exclusive','Holdback','status']],
                use_container_width=True, hide_index=True,
                column_config={
                     "title_name":              st.column_config.TextColumn("Title"),
                     "series_id":               st.column_config.TextColumn("Series"),
                     "deal_source":             st.column_config.TextColumn("Source"),
                     "rights_type":             st.column_config.TextColumn("Rights Type"),
                     "media_platform_primary":  st.column_config.TextColumn("Platform"),
                     "media_platform_ancillary":st.column_config.TextColumn("Ancillary"),
                     "territories":             st.column_config.TextColumn("Territories"),
                     "language":                st.column_config.TextColumn("Languages"),
                     "brand":                   st.column_config.TextColumn("Brand"),
                     "term_from":               st.column_config.TextColumn("Start Date"),
                     "term_to":                 st.column_config.TextColumn("End Date"),
                     "Days Left":               st.column_config.TextColumn("⏰ Days Left"),
                     "Exclusive":               st.column_config.TextColumn("Exclusive ⭐"),
                     "Holdback":                st.column_config.TextColumn("Holdback 🔒"),
                     "status":                  st.column_config.TextColumn("Status"),
                })
            st.download_button("📥 Export CSV", df.to_csv(index=False), f"rights_{reg}.csv", "text/csv")

# ═══════════════════════════════════════════════════════════════════════════════
# TITLE CATALOG
# ═══════════════════════════════════════════════════════════════════════════════
def page_titles():
    reg = st.session_state.current_region
    st.markdown('''
    <div class="page-header">🎬 Title Catalog</div>
    <div class="page-sub">Series · Movies · Episodes — Full WBD content registry · ''' + reg + '''</div>
    ''', unsafe_allow_html=True)
    kpi = run(f"""
        SELECT COUNT(*) AS total,
               COUNT(DISTINCT series_id) AS series_count,
               COUNT(DISTINCT season_id) AS season_count,
               SUM(CASE WHEN title_type='Episode'  THEN 1 ELSE 0 END) AS episodes,
               SUM(CASE WHEN title_type='Movie'    THEN 1 ELSE 0 END) AS movies,
               SUM(CASE WHEN title_type='Special'  THEN 1 ELSE 0 END) AS specials
        FROM title WHERE UPPER(region)='{reg}'
        """)
    movie_kpi = run("SELECT COUNT(*) AS total, SUM(box_office_gross_usd_m) AS total_bo FROM movie")
    if not kpi.empty:
        r = kpi.iloc[0]
        mr = movie_kpi.iloc[0] if not movie_kpi.empty else {}
        stat_tiles([
            (f"{int(r.get('total',0)):,}",         "Total Titles",     "#0f172a"),
            (f"{int(mr.get('total',0)):,}",         "Films in Slate",   "#1e40af"),
            (f"{int(r.get('series_count',0)):,}",   "Series",           "#5b21b6"),
            (f"{int(r.get('episodes',0)):,}",       "Episodes",         "#166534"),
            (f"${mr.get('total_bo',0)/1000:.1f}B",  "Total Box Office", "#92400e"),
            (f"{int(r.get('specials',0)):,}",       "Specials",         "#64748b"),
        ])

    st.divider()
    tab1, tab2, tab3, tab4 = st.tabs(["📁 TV Hierarchy",  "🎥 Movies",  "🎭 Genre & Metadata",  "🔍 Search All"])

    with tab1:
        df = run(f"""
            SELECT s.series_id, s.series_title, s.series_source, s.controlling_entity, s.genre,
                   COUNT(DISTINCT se.season_id) AS seasons,
                   COUNT(DISTINCT t.title_id) AS total_titles
            FROM series s
            LEFT JOIN season se ON s.series_id = se.series_id
            LEFT JOIN title t ON se.season_id = t.season_id
            GROUP BY s.series_id ORDER BY total_titles DESC
         """)
        if not df.empty:
            selected_series = st.selectbox("Drill into series", ["—"] + df['series_title'].tolist(), key="sel_series")
            c1, c2 = st.columns(2)
            with c1: st.plotly_chart(bar(df.head(15),'series_title','total_titles','Titles per Series',h=320,horiz=True), use_container_width=True)
            with c2: st.plotly_chart(pie(df,'genre','total_titles','Genre Mix',h=320), use_container_width=True)

            if selected_series != "—":
                s_row = df[df['series_title']==selected_series].iloc[0]
                st.markdown(f"#### {selected_series} — Detail")
                st.markdown(f"Source: `{s_row['series_source']}` · Entity: `{s_row['controlling_entity']}` · Genre: `{s_row['genre']}`")
                sea_df = run(f"""
                    SELECT se.season_number, se.episode_count, se.release_year,
                           COUNT(t.title_id) AS titles_in_db,
                           SUM(CASE WHEN mr.status='Active' THEN 1 ELSE 0 END) AS active_rights
                    FROM season se
                    LEFT JOIN title t ON se.season_id = t.season_id
                    LEFT JOIN media_rights mr ON t.title_id = mr.title_id
                    WHERE se.series_id='{s_row['series_id']}'
                    GROUP BY se.season_number ORDER BY se.season_number
                 """)
                if not sea_df.empty:
                    st.dataframe(sea_df, use_container_width=True, hide_index=True)


    # ── Movies Tab ────────────────────────────────────────────────────────────
    with tab2:
        st.markdown("#### WBD Film Slate — 25 Titles")
        c1, c2, c3 = st.columns(3)
        cat_f    = c1.selectbox("Category", ["All", "Theatrical", "Library", "HBO Original"], key="mv_cat")
        genre_f  = c2.selectbox("Genre",    ["All", "Action", "Drama", "Comedy", "Sci-Fi", "Fantasy", "Thriller", "Historical", "Animation"], key="mv_genre")
        rights_only = c3.checkbox("Active rights only", key="mv_rights")

        extras_mv = ""
        if cat_f   != "All": extras_mv += f" AND m.content_category='{cat_f}'"
        if genre_f != "All": extras_mv += f" AND m.genre='{genre_f}'"

        mv_df = run(f"""
            SELECT m.movie_id, m.movie_title, m.content_category, m.genre,
                   m.franchise, m.box_office_gross_usd_m,
                   m.age_rating, m.release_year,
                    COUNT(DISTINCT mr.rights_id) AS total_rights,
                   SUM(CASE WHEN mr.status='Active' THEN 1 ELSE 0 END) AS active_rights,
                   SUM(CASE WHEN mr.status='Active'
                       AND mr.term_to  <= DATE('now','+90 days') THEN 1 ELSE 0 END) AS expiring_90d
            FROM movie m
            LEFT JOIN title t ON t.movie_id = m.movie_id
            LEFT JOIN media_rights mr ON mr.title_id = t.title_id
            WHERE 1=1 {extras_mv}
            GROUP BY m.movie_id
            {"HAVING active_rights > 0" if rights_only else ""}
            ORDER BY m.box_office_gross_usd_m DESC
         """)
        if not mv_df.empty:
            stat_tiles([
                (f"{len(mv_df)}",                                        "Films",            "#0f172a"),
                (f"${mv_df['box_office_gross_usd_m'].sum()/1000:.1f}B", "Total Box Office", "#1e40af"),
                (f"{int(mv_df['active_rights'].sum())}",                "Active Rights",    "#166534"),
                (f"{int(mv_df['expiring_90d'].sum())}",                 "Expiring 90d",     "#991b1b"),
            ])
            c1, c2 = st.columns(2)
            with c1:
                st.plotly_chart(bar(mv_df.head(15),'movie_title','box_office_gross_usd_m',
                    'Box Office Gross (USD M)', h=400, horiz=True), use_container_width=True)
            with c2:
                cat_grp = mv_df.groupby('content_category')['box_office_gross_usd_m'].sum().reset_index()
                st.plotly_chart(pie(cat_grp,'content_category','box_office_gross_usd_m',
                    'Value by Category', h=400), use_container_width=True)

            # Franchise analysis
            fr_df = mv_df[mv_df['franchise'].notna()]
            if not fr_df.empty:
                fr_grp = fr_df.groupby('franchise').agg(
                    movies=('movie_id','count'),  box_office=('box_office_gross_usd_m','sum')
                ).reset_index()
                st.markdown("#### Franchise Box Office")
                st.plotly_chart(bar(fr_grp,'franchise','box_office','Franchise Box Office ($M)',h=260), use_container_width=True)

            st.markdown("#### Film Slate")
            mv_show = mv_df.copy()
            mv_show['⚠ Expiring'] = mv_show['expiring_90d'].apply(lambda x: f"🔴 {int(x)}" if x and int(x) >0 else "—")
            mv_show['Rights']     = mv_show['active_rights'].apply(lambda x: "✅" if x and int(x) >0 else "❌")
            st.dataframe(
                mv_show[['movie_title','content_category','genre','franchise',
                         'box_office_gross_usd_m','age_rating','release_year',
                          'total_rights','active_rights','Rights','⚠ Expiring']],
                use_container_width=True, hide_index=True,
                column_config={
                     "movie_title":            st.column_config.TextColumn("Film"),
                     "content_category":       st.column_config.TextColumn("Category"),
                     "genre":                  st.column_config.TextColumn("Genre"),
                     "franchise":              st.column_config.TextColumn("Franchise"),
                     "box_office_gross_usd_m": st.column_config.NumberColumn("Box Office ($M)", format="$%.0f M"),
                     "age_rating":             st.column_config.TextColumn("Rating"),
                     "release_year":           st.column_config.NumberColumn("Year", format="%d"),
                     "total_rights":           st.column_config.NumberColumn("Total Rights"),
                     "active_rights":          st.column_config.NumberColumn("Active Rights"),
                })
            st.download_button("📥 Export Movie Slate", mv_df.to_csv(index=False), "movies.csv", "text/csv")

    with tab3:
        c1, c2 = st.columns(2)
        with c1:
            df = run(f"""
                SELECT genre, COUNT(*) AS count FROM title
                WHERE UPPER(region)='{reg}' GROUP BY genre ORDER BY count DESC
             """)
            if not df.empty: st.plotly_chart(pie(df,'genre','count','Titles by Genre'), use_container_width=True)
        with c2:
            df = run(f"""
                SELECT age_rating, COUNT(*) AS count FROM title
                WHERE UPPER(region)='{reg}' GROUP BY age_rating ORDER BY count DESC
             """)
            if not df.empty: st.plotly_chart(pie(df,'age_rating','count','Age Ratings'), use_container_width=True)

        # Rights coverage per title
        st.markdown("#### Title → Rights Coverage")
        df = run(f"""
            SELECT t.title_name, t.genre, t.controlling_entity,
                   COUNT(DISTINCT mr.rights_id) AS rights_count,
                   GROUP_CONCAT(DISTINCT mr.media_platform_primary) AS platforms,
                   SUM(mr.exclusivity) AS exclusive,
                   SUM(CASE WHEN mr.status='Active' THEN 1 ELSE 0 END) AS active_rights,
                   SUM(CASE WHEN mr.term_to  <= DATE('now','+90 days') AND mr.status='Active' THEN 1 ELSE 0 END) AS expiring_soon,
                   CASE WHEN d.dna_id IS NOT NULL THEN '🚫' ELSE '✅' END AS dna_status
            FROM title t
            LEFT JOIN media_rights mr ON t.title_id = mr.title_id
            LEFT JOIN do_not_air d ON t.title_id = d.title_id AND d.active=1
            WHERE UPPER(t.region)='{reg}'
            GROUP BY t.title_id ORDER BY rights_count DESC NULLS LAST LIMIT 100
         """)
        if not df.empty:
            df['⚠ Expiring'] = df['expiring_soon'].apply(lambda x: f"🔴 {int(x)}" if x and int(x) >0 else "—")
            st.dataframe(
                df[['title_name','genre','controlling_entity','rights_count',
                    'platforms','exclusive','active_rights','⚠ Expiring','dna_status']],
                use_container_width=True, hide_index=True)

    with tab4:
        search_q = st.text_input("Search title name", placeholder="e.g. House of the Dragon, Succession…", key="title_search")
        # Use parameterised LIKE via Python-side sanitisation (no f-string user input in SQL)
        if search_q:
            safe_q = search_q.replace("'", "''").replace(";", " ").replace("--", " ")[:200]
            df = run(f"""
                SELECT t.title_id, t.title_name, t.title_type, t.genre,
                       t.release_year, t.controlling_entity, t.age_rating,
                       s.series_title, se.season_number, t.episode_number,
                       COUNT(DISTINCT mr.rights_id) AS rights_count
                FROM title t
                LEFT JOIN season se ON t.season_id = se.season_id
                LEFT JOIN series s  ON t.series_id = s.series_id
                LEFT JOIN media_rights mr ON t.title_id = mr.title_id
                WHERE LOWER(t.title_name) LIKE '%{safe_q.lower()}%'
                GROUP BY t.title_id ORDER BY s.series_title, se.season_number, t.episode_number
                LIMIT 100
             """)
            if not df.empty:
                st.caption(f"{len(df)} results")
                # Title 360 drilldown button
                st.markdown("**💡 Click a title below, then press** **View Title 360 ▶** to see full detail.")
                selected_title = st.selectbox("Select title to view", ["—"] + df['title_name'].tolist(), key="search_360_sel")
                if selected_title != "—":
                    if st.button(f"🔍 View Title 360 — {selected_title}", key="search_360_btn"):
                        st.session_state.title_360 = selected_title
                        st.session_state.page = "title_360"
                        st.rerun()
                st.dataframe(df, use_container_width=True, hide_index=True)
            else:
                st.warning("No titles found.")

# ═══════════════════════════════════════════════════════════════════════════════
# DO-NOT-AIR
# ═══════════════════════════════════════════════════════════════════════════════
def page_dna():
    reg = st.session_state.current_region
    st.markdown('''
    <div class="page-header">🚫 Do-Not-Air Restrictions</div>
    <div class="page-sub">Active DNA flags — titles that cannot be aired in certain territories/media due to content or rights reasons · ''' + reg + '''</div>
    ''', unsafe_allow_html=True)
    kpi = run(f"""
        SELECT COUNT(*) AS total, COUNT(DISTINCT title_id) AS titles,
               COUNT(DISTINCT reason_category) AS categories
        FROM do_not_air WHERE UPPER(region)='{reg}' AND active=1
        """)
    if not kpi.empty:
        r = kpi.iloc[0]
        stat_tiles([
            (f"{int(r.get('total',0)):,}",       "Active DNA Records",  "#991b1b"),
            (f"{int(r.get('titles',0)):,}",      "Affected Titles",     "#92400e"),
            (f"{int(r.get('categories',0)):,}",  "Restriction Types",   "#64748b"),
        ])

    st.divider()
    tab1, tab2 = st.tabs(["📊 Analysis",  "📄 DNA Table"])

    with tab1:
        c1, c2 = st.columns(2)
        with c1:
            df = run(f"""
                SELECT reason_category, COUNT(*) AS count
                FROM do_not_air WHERE UPPER(region)='{reg}' AND active=1
                GROUP BY reason_category ORDER BY count DESC
             """)
            if not df.empty: st.plotly_chart(pie(df,'reason_category','count','DNA by Reason Category'), use_container_width=True)
        with c2:
            df = run(f"""
                SELECT reason_subcategory, COUNT(*) AS count
                FROM do_not_air WHERE UPPER(region)='{reg}' AND active=1
                GROUP BY reason_subcategory ORDER BY count DESC LIMIT 12
             """)
            if not df.empty: st.plotly_chart(bar(df,'reason_subcategory','count','DNA by Sub-Category',h=300,horiz=True), use_container_width=True)

        df = run(f"""
            SELECT territory, COUNT(*) AS count
            FROM do_not_air WHERE UPPER(region)='{reg}' AND active=1
            GROUP BY territory ORDER BY count DESC
         """)
        if not df.empty:
            rows_t = []
            for _, row in df.iterrows():
                for t in str(row['territory']).split(','):
                    t = t.strip()
                    if t: rows_t.append({'territory':t,'count':row['count']})
            if rows_t:
                terr_df = (pd.DataFrame(rows_t).groupby('territory')['count'].sum()
                            .reset_index().sort_values('count',ascending=False))
                st.plotly_chart(bar(terr_df.head(12),'territory','count','DNA Flags by Territory',h=280), use_container_width=True)

    with tab2:
        df = run(f"""
            SELECT dna.dna_id, dna.title_name, t.series_id,
                   dna.reason_category, dna.reason_subcategory,
                   dna.territory, dna.media_type,
                   dna.term_from, dna.term_to, dna.additional_notes
            FROM do_not_air dna
            JOIN title t ON dna.title_id = t.title_id
            WHERE UPPER(dna.region)='{reg}' AND dna.active=1
            ORDER BY dna.reason_category, dna.title_name
         """)
        if not df.empty:
            st.caption(f"{len(df)} active DNA records")
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.download_button("📥 Export DNA List", df.to_csv(index=False), f"dna_{reg}.csv", "text/csv")

# ═══════════════════════════════════════════════════════════════════════════════
# SALES DEALS (rights-out)
# ═══════════════════════════════════════════════════════════════════════════════
def page_sales():
    reg = st.session_state.current_region
    st.markdown('''
    <div class="page-header">💸 Sales Deals — Rights Out</div>
    <div class="page-sub">Affiliate & 3rd-party sales of content rights · ''' + reg + '''</div>
    ''', unsafe_allow_html=True)
    kpi = run(f"""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN status='Active' THEN 1 ELSE 0 END) AS active,
               SUM(deal_value) AS total_value,
               COUNT(DISTINCT buyer) AS buyers,
               COUNT(DISTINCT title_id) AS titles
        FROM sales_deal WHERE UPPER(region)='{reg}'
        """)
    if not kpi.empty:
        r = kpi.iloc[0]
        stat_tiles([
            (f"{int(r.get('total',0)):,}",   "Total Sales Deals",  "#0f172a"),
            (f"{int(r.get('active',0)):,}",  "Active",             "#166534"),
            (fmt_m(r.get('total_value',0)),  "Total Value",        "#1e40af"),
            (f"{int(r.get('buyers',0)):,}",  "Buyers",             "#5b21b6"),
            (f"{int(r.get('titles',0)):,}",  "Titles Sold",        "#92400e"),
        ])

    st.divider()
    tab1, tab2 = st.tabs(["📊 Analytics",  "📄 Deal Table"])

    with tab1:
        c1, c2 = st.columns(2)
        with c1:
            df = run(f"""
                SELECT buyer, COUNT(*) AS deals, SUM(deal_value) AS total_value
                FROM sales_deal WHERE UPPER(region)='{reg}' AND status='Active'
                GROUP BY buyer  ORDER BY total_value DESC LIMIT 12
             """)
            if not df.empty: st.plotly_chart(bar(df,'buyer','total_value','Active Deal Value by Buyer',h=320,horiz=True), use_container_width=True)
        with c2:
            df = run(f"""
                SELECT deal_type, COUNT(*) AS count, SUM(deal_value) AS value
                FROM sales_deal WHERE UPPER(region)='{reg}'
                GROUP BY deal_type ORDER BY value DESC
             """)
            if not df.empty: st.plotly_chart(pie(df,'deal_type','count','Sales by Deal Type'), use_container_width=True)

    with tab2:
        f1, f2 = st.columns(2)
        st_f   = f1.selectbox("Status", ["All", "Active", "Expired"], key="sd_st_f")
        dt_f   = f2.selectbox("Type",   ["All", "Affiliate Sales", "3rd Party Sales"], key="sd_dt_f")
        extras = ""
        if st_f != "All": extras += f" AND status='{st_f}'"
        if dt_f != "All": extras += f" AND deal_type='{dt_f}'"
        df = run(f"""
            SELECT sd.sales_deal_id, sd.deal_type, sd.title_name, sd.buyer,
                   sd.territory, sd.media_platform, sd.term_from, sd.term_to,
                   sd.deal_value, sd.currency, sd.status
            FROM sales_deal sd
            WHERE UPPER(region)='{reg}' {extras}
            ORDER BY sd.deal_value DESC LIMIT 300
         """)
        if not df.empty:
            st.caption(f"{len(df)} records")
            st.dataframe(df, use_container_width=True, hide_index=True,
                         column_config={"deal_value": st.column_config.NumberColumn("Value", format="$%,.0f")})

# ═══════════════════════════════════════════════════════════════════════════════
# WORK ORDERS
# ═══════════════════════════════════════════════════════════════════════════════
def page_work_orders():
    reg = st.session_state.current_region
    st.markdown('''
    <div class="page-header">⚙️ Work Orders</div>
    <div class="page-sub">Operational pipeline — vendor tasks, QC, localization, encoding · ''' + reg + '''</div>
    ''', unsafe_allow_html=True)
    kpi = run(f"""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN status='In Progress' THEN 1 ELSE 0 END) AS in_progress,
               SUM(CASE WHEN status='Delayed'     THEN 1 ELSE 0 END) AS delayed,
               SUM(CASE WHEN status='Completed'   THEN 1 ELSE 0 END) AS completed,
               AVG(quality_score) AS avg_quality, SUM(cost) AS total_cost
        FROM work_orders WHERE UPPER(region)='{reg}'
        """)
    if not kpi.empty:
        r = kpi.iloc[0]
        stat_tiles([
            (f"{int(r.get('total',0)):,}",        "Total Orders",   "#0f172a"),
            (f"{int(r.get('in_progress',0)):,}",   "In Progress",    "#1e40af"),
            (f"{int(r.get('delayed',0)):,}",       "Delayed",        "#991b1b"),
            (f"{int(r.get('completed',0)):,}",     "Completed",      "#166534"),
            (f"{r.get('avg_quality',0):.1f}",      "Avg Quality",    "#5b21b6"),
            (fmt_m(r.get('total_cost',0)),         "Total Cost",     "#92400e"),
        ])

    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        df = run(f"SELECT status, COUNT(*) AS count FROM work_orders WHERE UPPER(region)='{reg}' GROUP BY status ORDER BY count DESC")
        if not df.empty: st.plotly_chart(pie(df,'status','count','Work Order Status'), use_container_width=True)
    with c2:
        df = run(f"""
            SELECT vendor_name, COUNT(*) AS orders, AVG(quality_score) AS avg_quality
            FROM work_orders WHERE UPPER(region)='{reg}'
            GROUP BY vendor_name ORDER BY orders DESC LIMIT 10
         """)
        if not df.empty: st.plotly_chart(bar(df,'vendor_name','orders','Work Orders by Vendor'), use_container_width=True)

    df = run(f"""
        SELECT work_order_id, title_name, vendor_name, work_type, status,
               priority, due_date, quality_score, cost, billing_status
        FROM work_orders WHERE UPPER(region)='{reg}'
        ORDER BY due_date ASC LIMIT 200
     """)
    if not df.empty:
        st.dataframe(df, use_container_width=True, hide_index=True,
                     column_config={
                          "quality_score": st.column_config.ProgressColumn("Quality",min_value=0,max_value=100,format="%.1f"),
                          "cost": st.column_config.NumberColumn("Cost",format="$%,.0f"),
                     })

# ═══════════════════════════════════════════════════════════════════════════════
# CHAT / QUERY
# ═══════════════════════════════════════════════════════════════════════════════
def page_chat():
    reg = st.session_state.current_region
    st.markdown('''
    <div class="page-header">💬 Chat Query</div>
    <div class="page-sub">Natural language rights interrogation · ''' + reg + ''' · Ask about titles, rights windows, expiry, DNA, territories, exclusivity</div>
    ''', unsafe_allow_html=True)
    # Suggested queries — using st.container instead of expander to avoid _arrow artifact
    if not st.session_state.chat_history:
        st.markdown("""
         <div style="background:#fff;border:1px solid #e2e8f0;border-radius:12px;
             padding:14px 16px;margin-bottom:12px">
           <div style="font-size:.8rem;font-weight:700;color:#6366f1;
               text-transform:uppercase;letter-spacing:.06em;margin-bottom:10px">
            💡 Sample queries by intent type
           </div>
        """, unsafe_allow_html=True)

        groups = {
             "🔗 Cross-table joins": [
                 "Movies with DNA flags",
                 "Titles with rights, DNA and sales deals",
                 "Rights expiring in 60 days with active sales deals",
                 "Work orders linked to expiring rights",
                 "Movies sold to Netflix or Amazon",
            ],
             "📋 Rights & Titles": [
                 "What titles do we have SVOD rights to",
                 "Show titles with exclusive PayTV rights",
                'What rights do we hold for "Succession"',
                 "Show SVOD rights expiring in 30 days",
            ],
             "🎬 Movies": [
                 "Show all movies in the slate",
                 "Movies by box office revenue",
                 "Theatrical movies with active rights",
                 "Franchise box office breakdown",
            ],
             "🚫 DNA / Sales / Deals": [
                 "Show do-not-air restrictions",
                 "Active deals by vendor",
                 "Sales deals by buyer",
                 "Deal source breakdown TRL C2 FRL",
            ],
        }
        for grp, qs in groups.items():
            st.markdown(f'<div style="font-size:.72rem;font-weight:600;color:#94a3b8;'
                        f'text-transform:uppercase;letter-spacing:.05em;margin:8px 0 4px">{grp}</div>',
                        unsafe_allow_html=True)
            cols = st.columns(len(qs))
            for col, q in zip(cols, qs):
                if col.button(q, key=f"sug_{hash(q)}", use_container_width=True):
                    st.session_state.pending_prompt = q

        st.markdown('</div>', unsafe_allow_html=True)

    col_a, col_b = st.columns([3, 1])
    with col_a:
        show_sql = st.toggle("Show SQL", value=st.session_state.user_prefs.get('show_sql', True), key="sql_tog")
        st.session_state.user_prefs['show_sql'] = show_sql
    with col_b:
        raw_sql_mode = st.toggle("⚡ Raw SQL", value=st.session_state.user_prefs.get('raw_sql_mode', False), key="raw_sql_tog")
        st.session_state.user_prefs['raw_sql_mode'] = raw_sql_mode

    # ── Raw SQL editor ──────────────────────────────────────────────────────────
    if raw_sql_mode:
        st.markdown("""
         <div style="background:#f8faff;border:1px solid #c7d2fe;border-radius:10px;
             padding:12px 16px;margin-bottom:10px">
           <div style="font-size:.78rem;font-weight:700;color:#4f46e5;margin-bottom:6px">
            ⚡ Raw SQL Mode — type any SQL query directly against the Rights Explorer database
           </div>
           <div style="font-size:.72rem;color:#64748b">
            Tables:  <code>movie</code> ·  <code>title</code> ·  <code>series</code> ·  <code>season</code> ·
             <code>media_rights</code> ·  <code>content_deal</code> ·  <code>exhibition_restrictions</code> ·
             <code>elemental_rights</code> ·  <code>elemental_deal</code> ·  <code>do_not_air</code> ·
             <code>sales_deal</code> ·  <code>deals</code> ·  <code>vendors</code> ·  <code>work_orders</code>
           </div>
         </div>
        """, unsafe_allow_html=True)

        # Quick-start templates
        raw_templates = {
             "— Pick a template —":  "",
             "Title health check (rights + DNA + sales)":  """SELECT
t.title_name, t.title_type, t.content_category,
COUNT(DISTINCT mr.rights_id)                                    AS active_rights,
SUM(CASE WHEN mr.term_to  <= DATE('now','+90 days') AND mr.status='Active' THEN 1 ELSE 0 END) AS expiring_90d,
COUNT(DISTINCT dna.dna_id)                                      AS dna_flags,
GROUP_CONCAT(DISTINCT dna.reason_category)                      AS dna_reasons,
COUNT(DISTINCT sd.sales_deal_id)                                AS sales_deals,
GROUP_CONCAT(DISTINCT sd.buyer)                                  AS buyers
FROM title t
LEFT JOIN media_rights mr ON t.title_id = mr.title_id AND mr.status='Active'
LEFT JOIN do_not_air dna  ON t.title_id = dna.title_id AND dna.active=1
LEFT JOIN sales_deal sd   ON t.title_id = sd.title_id
WHERE UPPER(t.region) = 'NA'
GROUP BY t.title_id
ORDER BY dna_flags DESC, expiring_90d DESC
LIMIT 100""",
             "Expiry + sales overlap (renewal priority)":  """SELECT
mr.title_name,
mr.media_platform_primary                                       AS rights_platform,
mr.term_to                                                      AS rights_expiry,
CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER)         AS days_left,
mr.exclusivity,
sd.buyer, sd.deal_value, sd.status                              AS sale_status,
CASE WHEN sd.sales_deal_id IS NOT NULL THEN '⚠ Active Sale' ELSE '— No Sale' END AS flag
FROM media_rights mr
JOIN content_deal cd ON mr.deal_id = cd.deal_id
LEFT JOIN sales_deal sd ON mr.title_id = sd.title_id AND sd.status='Active'
WHERE UPPER(mr.region)='NA'
AND mr.status='Active'
AND mr.term_to  <= DATE('now','+90 days')
ORDER BY days_left ASC
LIMIT 100""",
             "Work orders + rights expiry overlap":  """SELECT
wo.title_name, wo.work_type, wo.status AS wo_status,
wo.priority, wo.due_date, wo.vendor_name,
COUNT(DISTINCT mr.rights_id)                                    AS active_rights,
MIN(mr.term_to)                                                 AS earliest_rights_expiry,
SUM(CASE WHEN mr.term_to  <= DATE('now','+90 days') THEN 1 ELSE 0 END) AS rights_expiring_90d
FROM work_orders wo
LEFT JOIN title t      ON wo.title_id = t.title_id
LEFT JOIN media_rights mr ON t.title_id = mr.title_id AND mr.status='Active'
WHERE UPPER(wo.region)='NA'
GROUP BY wo.work_order_id
ORDER BY rights_expiring_90d DESC, wo.due_date ASC
LIMIT 100""",
             "Movies + DNA flags":  """SELECT
m.movie_title, m.content_category, m.genre, m.franchise,
m.box_office_gross_usd_m,
COUNT(DISTINCT mr.rights_id)                                    AS active_rights,
COUNT(DISTINCT dna.dna_id)                                      AS dna_flags,
GROUP_CONCAT(DISTINCT dna.reason_category)                      AS dna_reasons,
GROUP_CONCAT(DISTINCT dna.territory)                            AS restricted_territories,
CASE WHEN COUNT(dna.dna_id) > 0 THEN '🚫 Flagged' ELSE '✅ Clean' END AS dna_status
FROM movie m
LEFT JOIN title t      ON t.movie_id = m.movie_id
LEFT JOIN media_rights mr ON mr.title_id = t.title_id AND mr.status='Active'
LEFT JOIN do_not_air dna  ON dna.title_id = t.title_id AND dna.active=1
GROUP BY m.movie_id
ORDER BY dna_flags DESC, m.box_office_gross_usd_m DESC""",
             "Movies sold to external buyers":  """SELECT
m.movie_title, m.content_category, m.genre,
m.box_office_gross_usd_m,
sd.buyer, sd.deal_type, sd.media_platform,
sd.deal_value, sd.currency,
sd.term_from, sd.term_to, sd.status
FROM movie m
JOIN title t      ON t.movie_id = m.movie_id
JOIN sales_deal sd ON sd.title_id = t.title_id
ORDER BY sd.deal_value DESC
LIMIT 100""",
             "Full content deal detail (all joins)":  """SELECT
t.title_name, t.title_type, t.genre,
cd.deal_source, cd.deal_type, cd.deal_name,
mr.rights_type, mr.media_platform_primary, mr.media_platform_ancillary,
mr.territories, mr.language, mr.brand,
mr.term_from, mr.term_to, mr.exclusivity, mr.holdback, mr.holdback_days,
mr.status,
CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_remaining,
er.max_plays, er.max_plays_per_day
FROM media_rights mr
JOIN content_deal cd             ON mr.deal_id   = cd.deal_id
JOIN title t                     ON mr.title_id  = t.title_id
LEFT JOIN exhibition_restrictions er ON er.rights_id = mr.rights_id
WHERE UPPER(mr.region)='NA' AND mr.status='Active'
ORDER BY mr.term_to ASC
LIMIT 100""",
        }
        sel_template = st.selectbox("Quick-start template", list(raw_templates.keys()), key="raw_tpl")
        default_sql  = raw_templates[sel_template] if sel_template != "— Pick a template —" else                        st.session_state.get("last_raw_sql", "SELECT * FROM movie LIMIT 10")

        raw_sql_input = st.text_area("SQL Editor", value=default_sql, height=220, key="raw_sql_input",
                                     help="Full SQLite syntax. All 14 tables available.")
        run_raw = st.button("▶ Run SQL", type="primary", key="run_raw_btn")

        if run_raw and raw_sql_input.strip():
            st.session_state["last_raw_sql"] = raw_sql_input.strip()
            with st.spinner("Running…"):
                res_df, db_err = execute_sql(raw_sql_input.strip(), DB_CONN)
            if db_err:
                st.error(f"❌ SQL Error: {db_err}")
            elif res_df is not None and not res_df.empty:
                st.success(f"✅ {len(res_df):,} rows returned")
                if show_sql:
                    st.markdown(f'<div class="sql-box">{html.escape(raw_sql_input.strip())}</div>',
                                unsafe_allow_html=True)
                # Quick metrics
                num_cols = [c for c in res_df.columns if pd.api.types.is_numeric_dtype(res_df[c])]
                if num_cols:
                    mc = st.columns(min(4, len(num_cols)))
                    for i, nc in enumerate(num_cols[:4]):
                        mc[i].metric(nc, f"{res_df[nc].sum():,.0f}")
                # Chart auto-detect
                if len(res_df.columns) >= 2:
                    first_num = next((c for c in res_df.columns if pd.api.types.is_numeric_dtype(res_df[c])), None)
                    if first_num and res_df.columns[0] != first_num and len(res_df) <= 50:
                        fig = bar(res_df.head(30), res_df.columns[0], first_num, "Query Result")
                        st.plotly_chart(fig, use_container_width=True)
                st.dataframe(res_df, use_container_width=True, hide_index=True, height=380)
                st.download_button("📥 Download CSV", res_df.to_csv(index=False),
                                    "raw_sql_result.csv", "text/csv", key="dl_raw")
                # Save to history
                st.session_state.chat_history.append({
                     "question": f"[SQL] {raw_sql_input.strip()[:80]}…",
                     "answer":   f"📊 **{len(res_df):,} records** — raw SQL query",
                     "data":     res_df.copy(),
                     "chart":    None,
                     "metrics":  [],
                     "sql":      raw_sql_input.strip(),
                     "region":   reg,
                })
            else:
                st.warning("No records returned.")
        st.divider()

    # Chat history
    for i, msg in enumerate(st.session_state.chat_history):
        with st.chat_message("user"):
            st.markdown(f"**{msg['question']}**  `{msg.get('region','')}`")
        with st.chat_message("assistant", avatar="🔑"):
            if msg.get("sql") and show_sql:
                st.markdown(f'<div class="sql-box">{html.escape(msg["sql"])}</div>', unsafe_allow_html=True)
            if msg.get("metrics"):
                mc = st.columns(len(msg["metrics"]))
                for c, m in zip(mc, msg["metrics"]):
                    c.metric(m["label"], m["value"])
            if msg.get("chart"):
                st.plotly_chart(msg["chart"], use_container_width=True, key=f"hchart_{i}")
            st.markdown(msg.get("answer", "Here are the results:"))
            if msg.get("data") is not None and not msg["data"].empty:
                st.dataframe(msg["data"], use_container_width=True, hide_index=True, height=280)
                st.download_button("📥 CSV", msg["data"].to_csv(index=False),
                                   f"query_{i}.csv", "text/csv", key=f"dl_h_{i}")

    # Input
    user_input    = st.chat_input(f"Ask about rights, titles, expiry, DNA… [{reg}]")
    active_prompt = None
    if 'pending_prompt' in st.session_state and st.session_state.pending_prompt:
        active_prompt = st.session_state.pending_prompt
        st.session_state.pending_prompt = None
    elif user_input:
        active_prompt = user_input

    if active_prompt:
        with st.chat_message("user"):
            st.markdown(f"**{active_prompt}**  `{reg}`")

        with st.chat_message("assistant", avatar="🔑"):
            with st.spinner("Analysing…"):
                # Always use sidebar region as base; query text may refine it
                sql, error, chart_type, region_ctx = parse_query(active_prompt, reg)

                if error:
                    st.error(f"❌ {error}")
                else:
                    # Show SQL inline immediately — no expander, no rerun needed
                    if show_sql:
                        st.markdown(f'<div class="sql-box">{html.escape(sql)}</div>', unsafe_allow_html=True)

                    res_df, db_err = execute_sql(sql, DB_CONN)

                    if db_err:
                        st.error(f"DB error: {db_err}")
                    elif res_df is not None and not res_df.empty:
                        # Metrics strip
                        metrics_data = []
                        val_cols = [c for c in res_df.columns
                                    if any(x in c.lower() for x in ['count','total','value','fee','days'])]
                        if val_cols:
                            vc  = val_cols[0]
                            try:
                                res_df[vc] = pd.to_numeric(res_df[vc], errors='coerce')
                                mc = st.columns(4)
                                mc[0].metric("Total / Sum", f"{res_df[vc].sum():,.0f}")
                                mc[1].metric("Avg",         f"{res_df[vc].mean():,.1f}")
                                mc[2].metric("Records",     f"{len(res_df):,}")
                                mc[3].metric("Max",         f"{res_df[vc].max():,.0f}")
                                metrics_data = [
                                    {"label": "Total",    "value":f"{res_df[vc].sum():,.0f}"},
                                    {"label": "Avg",      "value":f"{res_df[vc].mean():,.1f}"},
                                    {"label": "Records",  "value":f"{len(res_df):,}"},
                                    {"label": "Max",      "value":f"{res_df[vc].max():,.0f}"},
                                ]
                            except (ValueError, TypeError) as _metric_err:
                                logger.debug(f"Metrics render skipped: {_metric_err}")

                        # Chart
                        fig = None
                        if chart_type == 'bar' and len(res_df.columns) >= 2:
                            x_col = res_df.columns[0]
                            y_col = next((c for c in res_df.columns[1:]
                                          if pd.api.types.is_numeric_dtype(res_df[c])), res_df.columns[1])
                            try: res_df[y_col] = pd.to_numeric(res_df[y_col], errors='coerce')
                            except (ValueError, TypeError): pass
                            fig = bar(res_df.head(30), x_col, y_col, active_prompt[:60])
                        elif chart_type == 'pie' and len(res_df.columns) >= 2:
                            fig = pie(res_df, res_df.columns[0], res_df.columns[1], active_prompt[:60])

                        if fig:
                            st.plotly_chart(fig, use_container_width=True)

                        answer_txt = (f"📊 **{len(res_df):,} records** for **{region_ctx}**. "
                                      + (" Sorted by expiry date." if 'expir' in active_prompt.lower() else ""))
                        st.markdown(answer_txt)
                        st.dataframe(res_df, use_container_width=True, hide_index=True, height=300)
                        st.download_button("📥 Download CSV", res_df.to_csv(index=False),
                                           f"rights_query_{region_ctx}.csv", "text/csv",
                                           key="dl_live")

                        # Save to history — no rerun, stays visible
                        st.session_state.chat_history.append({
                             "question": active_prompt,
                             "answer":   answer_txt,
                             "data":     res_df.copy(),
                             "chart":    fig,
                             "metrics":  metrics_data,
                             "sql":      sql,
                             "region":   region_ctx,
                        })
                    else:
                        st.warning("No records returned — try adjusting your query or region filter.")

    if st.session_state.chat_history:
        if st.button("🗑 Clear Chat", key="clear_chat"):
            st.session_state.chat_history = []
            st.rerun()

# ═══════════════════════════════════════════════════════════════════════════════
# DEALS PAGE (original deals table)
# ═══════════════════════════════════════════════════════════════════════════════
def page_deals():
    reg = st.session_state.current_region
    st.markdown('''
    <div class="page-header">💼 Deals</div>
    <div class="page-sub">Vendor licensing & distribution deals · ''' + reg + '''</div>
    ''', unsafe_allow_html=True)
    kpi = run(f"""
        SELECT COUNT(*) AS total,
               SUM(deal_value) AS total_value,
               AVG(deal_value) AS avg_value,
               SUM(CASE WHEN status='Active' THEN 1 ELSE 0 END) AS active,
               SUM(CASE WHEN status='Expired' THEN 1 ELSE 0 END) AS expired,
               SUM(CASE WHEN status='Pending' OR status='Under Negotiation' THEN 1 ELSE 0 END) AS pending,
               SUM(CASE WHEN payment_status='Overdue' THEN 1 ELSE 0 END) AS overdue_payments
        FROM deals WHERE UPPER(region)='{reg}'
        """)
    if not kpi.empty:
        r = kpi.iloc[0]
        stat_tiles([
            (f"{int(r.get('total',0)):,}",           "Total Deals",       "#0f172a"),
            (fmt_m(r.get('total_value',0)),          "Total Value",       "#1e40af"),
            (fmt_m(r.get('avg_value',0)),            "Avg Deal Value",    "#5b21b6"),
            (f"{int(r.get('active',0)):,}",           "Active",            "#166534"),
            (f"{int(r.get('expired',0)):,}",          "Expired",           "#64748b"),
            (f"{int(r.get('pending',0)):,}",          "Pending / Neg.",    "#92400e"),
            (f"{int(r.get('overdue_payments',0)):,}", "Overdue Payments",  "#991b1b"),
        ])

    st.divider()
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview",  "🏢 By Vendor",  "📋 Deal Types",  "📄 Deal Table"])

    with tab1:
        c1, c2 = st.columns(2)
        with c1:
            df = run(f"""
                SELECT status, COUNT(*) AS count, SUM(deal_value) AS total_value
                FROM deals WHERE UPPER(region)='{reg}'
                GROUP BY status ORDER BY count DESC
           """)
            if not df.empty:
                st.plotly_chart(pie(df, 'status', 'count', 'Deals by Status'), use_container_width=True)
        with c2:
            df = run(f"""
                SELECT rights_scope, COUNT(*) AS count, SUM(deal_value) AS total_value
                FROM deals WHERE UPPER(region)='{reg}'
                GROUP BY rights_scope ORDER BY total_value DESC
             """)
            if not df.empty:
                st.plotly_chart(bar(df, 'rights_scope', 'total_value',
                                    'Deal Value by Rights Scope', h=300, horiz=True), use_container_width=True)

        # Monthly deal trend
        df = run(f"""
            SELECT STRFTIME('%Y-%m', deal_date) AS month,
                   COUNT(*) AS count, SUM(deal_value) AS value
            FROM deals WHERE UPPER(region)='{reg}'
            GROUP BY month ORDER BY month
         """)
        if not df.empty:
            fig = go.Figure()
            fig.add_bar(x=df['month'], y=df['count'], name='Deal Count',
                        marker_color='#7c3aed', yaxis='y')
            fig.add_scatter(x=df['month'], y=df['value'], name='Deal Value ($)',
                           mode='lines+markers', line=dict(color='#f59e0b', width=2),
                           yaxis='y2')
            fig.update_layout(**PT, height=300, title='Monthly Deal Activity',
                              yaxis=dict(title='Count'),
                              yaxis2=dict(title='Value ($)', overlaying='y', side='right'),
                              legend=dict(orientation='h', y=1.1))
            st.plotly_chart(fig, use_container_width=True)

    with tab2:
         df = run(f"""
            SELECT vendor_name, COUNT(*) AS deals,
                   SUM(deal_value) AS total_value,
                   AVG(deal_value) AS avg_value,
                   SUM(CASE WHEN status='Active' THEN 1 ELSE 0 END) AS active,
                   SUM(CASE WHEN payment_status='Overdue' THEN 1 ELSE 0 END) AS overdue
            FROM deals WHERE UPPER(region)='{reg}'
            GROUP BY vendor_name ORDER BY total_value DESC LIMIT 10
         """)
        if not df.empty:
            c1, c2 = st.columns(2)
            with c1:
                st.plotly_chart(bar(df, 'vendor_name', 'total_value',
                                    'Total Deal Value by Vendor', h=320, horiz=True), use_container_width=True)
            with c2:
                st.plotly_chart(bar(df, 'vendor_name', 'deals',
                                    'Deal Count by Vendor', h=320, horiz=True), use_container_width=True)
            st.dataframe(df, use_container_width=True, hide_index=True,
                         column_config={
                            "total_value": st.column_config.NumberColumn("Total Value", format="$%,.0f"),
                            "avg_value":   st.column_config.NumberColumn("Avg Value",   format="$%,.0f"),
                         })

    with tab3:
        df = run(f"""
            SELECT deal_type, COUNT(*) AS count,
                   SUM(deal_value) AS total_value,
                   AVG(deal_value) AS avg_value,
                   SUM(CASE WHEN status='Active' THEN 1 ELSE 0 END) AS active
            FROM deals WHERE UPPER(region)='{reg}'
            GROUP BY deal_type ORDER BY total_value DESC
         """)
        if not df.empty:
            c1, c2 = st.columns(2)
            with c1:
                st.plotly_chart(pie(df, 'deal_type', 'count', 'Deals by Type'), use_container_width=True)
            with c2:
                st.plotly_chart(bar(df, 'deal_type', 'total_value',
                                    'Value by Deal Type', h=300, horiz=True), use_container_width=True)

    with tab4:
        f1, f2, f3 = st.columns(3)
        st_f  = f1.selectbox("Status",       ["All", "Active", "Expired", "Pending", "Under Negotiation", "Terminated"], key="dl_st")
        dt_f  = f2.selectbox("Deal Type",    ["All", "Output Deal", "Library Buy", "First-Look Deal",
                                                "Co-Production", "Licensing Agreement", "Distribution Deal",
                                                "Volume Deal", "Format Deal"], key="dl_dt")
        pay_f = f3.selectbox("Payment",      ["All", "Paid", "Pending", "Invoiced", "Overdue", "Partially Paid"], key="dl_pay")

        extras = ""
        if st_f  != "All": extras += f" AND status='{st_f}'"
        if dt_f  != "All": extras += f" AND deal_type='{dt_f}'"
        if pay_f != "All": extras += f" AND payment_status='{pay_f}'"

        df = run(f"""
            SELECT deal_id, deal_name, vendor_name, deal_type,
                   deal_value, currency, deal_date, expiry_date,
                   rights_scope, territory, status, payment_status
            FROM deals WHERE UPPER(region)='{reg}' {extras}
            ORDER BY deal_value DESC LIMIT 300
         """)
        if not df.empty:
            # Colour-code expiry
            today_str = datetime.now().strftime("%Y-%m-%d")
            def _safe_exp(d):
                try:
                    return exp_tag((datetime.strptime(str(d), "%Y-%m-%d") - datetime.now()).days)
                except (ValueError, TypeError):
                    return "—"
            df['⏰ Expiry'] = df['expiry_date'].apply(_safe_exp)
            st.caption(f"{len(df)} deals")
            st.dataframe(
                df[['deal_id','deal_name','vendor_name','deal_type',
                    'deal_value','deal_date','expiry_date','⏰ Expiry',
                    'rights_scope','territory','status','payment_status']],
                use_container_width=True, hide_index=True,
                column_config={
                     "deal_value": st.column_config.NumberColumn("Value", format="$%,.0f"),
                })
            st.download_button("📥 Export CSV", df.to_csv(index=False),
                               f"deals_{reg}.csv", "text/csv")

# ═══════════════════════════════════════════════════════════════════════════════
# VENDORS PAGE (original vendors table — restored columns)
# ═══════════════════════════════════════════════════════════════════════════════
def page_vendors():
    reg = st.session_state.current_region
    st.markdown('''
    <div class="page-header">🏢 Vendors</div>
    <div class="page-sub">Vendor performance, spend & quality · ''' + reg + '''</div>
    ''', unsafe_allow_html=True)
    kpi = run(f"""
        SELECT COUNT(*) AS total,
               AVG(rating) AS avg_rating,
               SUM(total_spend) AS total_spend,
               COUNT(DISTINCT vendor_type) AS vendor_types,
                SUM(CASE WHEN active=1 THEN 1 ELSE 0 END) AS active_vendors
        FROM vendors WHERE UPPER(region)='{reg}'
        """)
    if not kpi.empty:
        r = kpi.iloc[0]
        stat_tiles([
            (f"{int(r.get('total',0)):,}",          "Total Vendors",    "#0f172a"),
            (f"{r.get('avg_rating',0):.2f} ⭐",     "Avg Rating",       "#92400e"),
            (fmt_m(r.get('total_spend',0)),         "Total Spend",      "#1e40af"),
            (f"{int(r.get('active_vendors',0)):,}",  "Active",           "#166534"),
            (f"{int(r.get('vendor_types',0)):,}",   "Vendor Types",     "#5b21b6"),
        ])

    st.divider()
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Performance",  "💰 Spend",  "⭐ Quality",  "📄 Vendor List"])

    with tab1:
        # Deals + spend dual axis
        df = run(f"""
            SELECT v.vendor_name, v.vendor_type, v.rating,
                   COUNT(d.deal_id) AS deal_count,
                   SUM(d.deal_value) AS total_deal_value,
                   v.total_spend
            FROM vendors v
            LEFT JOIN deals d ON v.vendor_id = d.vendor_id
            WHERE UPPER(v.region)='{reg}'
            GROUP BY v.vendor_id ORDER BY total_deal_value DESC NULLS LAST LIMIT 10
         """)
        if not df.empty:
            fig = go.Figure()
            fig.add_bar(x=df['vendor_name'], y=df['deal_count'],
                        name='Deal Count', marker_color='#7c3aed', yaxis='y')
            fig.add_scatter(x=df['vendor_name'], y=df['total_deal_value'],
                            name='Deal Value ($)', mode='lines+markers',
                            line=dict(color='#f59e0b', width=2), yaxis='y2')
            fig.update_layout(**PT, height=340, title='Vendor Deal Count vs Deal Value',
                              yaxis=dict(title='Deal Count'), 
                              yaxis2=dict(title='Deal Value ($)', overlaying='y', side='right'),
                              legend=dict(orientation='h', y=1.1),
                               xaxis=dict(tickangle=-30))
            st.plotly_chart(fig, use_container_width=True)

    with tab2:
        c1, c2 = st.columns(2)
        with c1:
            df = run(f"""
                SELECT vendor_type, SUM(total_spend) AS spend, COUNT(*) AS vendors
                FROM vendors WHERE UPPER(region)='{reg}'
                GROUP BY vendor_type ORDER BY spend DESC
             """)
            if not df.empty:
                st.plotly_chart(pie(df, 'vendor_type', 'spend', 'Spend by Vendor Type'), use_container_width=True)
        with c2:
            df = run(f"""
                SELECT vendor_name, total_spend, payment_terms, certification_level
                FROM vendors WHERE UPPER(region)='{reg}'
                ORDER BY total_spend DESC LIMIT 10
             """)
            if not df.empty:
                st.plotly_chart(bar(df, 'vendor_name', 'total_spend',
                                    'Total Spend by Vendor', h=300, horiz=True), use_container_width=True)

        # Payment terms breakdown
        df = run(f"""
            SELECT payment_terms, COUNT(*) AS vendors, SUM(total_spend) AS spend
            FROM vendors WHERE UPPER(region)='{reg}'
            GROUP BY payment_terms ORDER BY spend DESC
          """)
        if not df.empty:
            c1, c2 = st.columns(2)
            with c1: st.plotly_chart(pie(df, 'payment_terms', 'vendors', 'Vendors by Payment Terms', h=260), use_container_width=True)
            with c2: st.plotly_chart(pie(df, 'payment_terms', 'spend',   'Spend by Payment Terms', h=260), use_container_width=True)

    with tab3:
        df = run(f"""
            SELECT v.vendor_name, v.vendor_type, v.rating,
                   COUNT(wo.work_order_id) AS work_orders,
                   AVG(wo.quality_score)   AS avg_quality,
                    SUM(wo.rework_count)    AS total_rework,
                   SUM(wo.cost)            AS wo_cost
            FROM vendors v
            LEFT JOIN work_orders wo ON v.vendor_id = wo.vendor_id
             WHERE UPPER(v.region)='{reg}'
            GROUP BY v.vendor_id ORDER BY avg_quality DESC NULLS LAST
         """)
        if not df.empty:
            # Scatter: work volume vs quality
            fig = px.scatter(df, x='work_orders', y='avg_quality',
                             size='wo_cost', color='vendor_type',
                             hover_name='vendor_name',
                             title='Work Volume vs Quality Score (bubble = cost)',
                             labels={'work_orders':'Work Orders','avg_quality':'Avg Quality Score'})
            fig.update_layout(**PT, height=340)
            st.plotly_chart(fig, use_container_width=True)

            # Rating vs rework bar
            c1, c2 = st.columns(2)
            with c1: st.plotly_chart(bar(df, 'vendor_name', 'rating',       'Vendor Rating', h=280), use_container_width=True)
            with c2:  st.plotly_chart(bar(df, 'vendor_name', 'total_rework', 'Total Rework Count', h=280), use_container_width=True)

    with tab4:
        df = run(f"""
            SELECT v.vendor_id, v.vendor_name, v.vendor_type, v.rating,
                   v.certification_level, v.contact_email, v.phone,
                   v.payment_terms, v.total_spend, 
                   COUNT(d.deal_id) AS deals,
                   COUNT(wo.work_order_id) AS work_orders,
                   CASE WHEN v.active=1 THEN 'Active' ELSE 'Inactive' END AS status
             FROM vendors v
            LEFT JOIN deals      d  ON v.vendor_id = d.vendor_id
            LEFT JOIN work_orders wo ON v.vendor_id = wo.vendor_id
            WHERE UPPER(v.region)='{reg}'
            GROUP BY v.vendor_id ORDER BY v.rating DESC
         """)
        if not df.empty:
            st.dataframe(
                df,
                use_container_width=True, hide_index=True,
                column_config={
                     "rating":      st.column_config.ProgressColumn("Rating", min_value=0, max_value=5, format="%.1f ⭐"),
                     "total_spend": st.column_config.NumberColumn("Total Spend", format="$%,.0f"),
                })
            st.download_button("📥 Export CSV", df.to_csv(index=False),
                               f"vendors_{reg}.csv", "text/csv")

# ═══════════════════════════════════════════════════════════════════════════════
# RIGHTS GAP ANALYSIS — Feature 17
# ═══════════════════════════════════════════════════════════════════════════════
def page_gap_analysis():
    reg = st.session_state.current_region
    st.markdown('''
    <div class="page-header">🔍 Rights Gap Analysis</div>
    <div class="page-sub">Titles with missing or expired rights — identify coverage gaps for licensing decisions · ''' + reg + '''</div>
    ''', unsafe_allow_html=True)
    # ── Filters ──────────────────────────────────────────────────────────────
    c1, c2, c3 = st.columns(3)
    gap_plat  = c1.selectbox("Platform", ["All", "PayTV", "STB-VOD", "SVOD", "FAST"], key="gap_plat")
    gap_type  = c2.selectbox("Title Type", ["All", "Episode", "Movie", "Special"],   key="gap_type")
    gap_genre = c3.selectbox("Genre",      ["All"] + ["Drama", "Thriller", "Fantasy", "Sci-Fi",
                               "Comedy", "Action", "Historical", "Crime", "Animation"], key="gap_genre")

    plat_cond  = f"AND mr.media_platform_primary = '{gap_plat}'" if gap_plat != "All" else ""
    type_cond  = f"AND t.title_type = '{gap_type}'"              if gap_type != "All" else ""
    genre_cond = f"AND t.genre = '{gap_genre}'"                  if gap_genre != "All" else ""

    st.divider()
    tab_a, tab_b, tab_c = st.tabs(["❌ No Active Rights",  "⏰ Expiring — No Renewal",  "📊 Coverage Heatmap"])

    with tab_a:
        st.markdown("#### Titles with zero active rights in this region/platform")
        no_rights_df = run(f"""
            SELECT t.title_name, t.title_type, t.genre, t.controlling_entity,
                   t.content_category,
                   COUNT(mr.rights_id)                                          AS total_rights,
                   SUM(CASE WHEN mr.status='Active'   THEN 1 ELSE 0 END)      AS active_rights,
                   SUM(CASE WHEN mr.status='Expired'  THEN 1 ELSE 0 END)      AS expired_rights,
                   MAX(mr.term_to)                                              AS last_rights_end,
                   s.series_title
            FROM title t
             LEFT JOIN media_rights mr ON t.title_id = mr.title_id
              AND UPPER(mr.region) = '{reg}' {plat_cond}
            LEFT JOIN season se ON t.season_id = se.season_id
            LEFT  JOIN series  s ON t.series_id = s.series_id
            WHERE UPPER(t.region) = '{reg}' {type_cond} {genre_cond}
            GROUP BY t.title_id
            HAVING active_rights = 0
            ORDER BY expired_rights DESC, t.title_name
            LIMIT 200
         """)
        if no_rights_df.empty:
            st.success(f"✅ All titles have active rights in {reg}" +
                       (f" on {gap_plat}" if gap_plat != "All" else "") + ".")
        else:
            stat_tiles([
                (f"{len(no_rights_df):,}",
                 f"Titles with no active rights{' on '+gap_plat if gap_plat!='All' else ''}",
                  "#991b1b"),
                (f"{int(no_rights_df['expired_rights'].sum()):,}",
                  "Previously Had Rights (now expired)",  "#92400e"),
                (f"{int((no_rights_df['total_rights']==0).sum()):,}",
                  "Never Had Rights",  "#64748b"),
            ])
            # Bar: top series with most gap titles
            series_gap = (no_rights_df.dropna(subset=['series_title'])
                          .groupby('series_title').size().reset_index(name='gap_titles')
                          .sort_values('gap_titles', ascending=False).head(15))
            if not series_gap.empty:
                st.plotly_chart(bar(series_gap,'series_title','gap_titles',
                    'Series with Most Rights Gaps',h=280,horiz=True), use_container_width=True)
            no_rights_df['Last Rights'] = no_rights_df['last_rights_end'].fillna('Never')
            no_rights_df['Status'] = no_rights_df['total_rights'].apply(
                lambda x: '🆕 Never Licensed' if x == 0 else '🔴 Expired / Lapsed')
            st.dataframe(
                no_rights_df[['title_name','title_type','genre','controlling_entity',
                              'content_category','total_rights','expired_rights',
                              'Last Rights','Status','series_title']],
                use_container_width=True, hide_index=True,
                column_config={
                     "title_name":       st.column_config.TextColumn("Title"),
                     "title_type":       st.column_config.TextColumn("Type"),
                     "genre":            st.column_config.TextColumn("Genre"),
                     "controlling_entity":st.column_config.TextColumn("Entity"),
                     "content_category": st.column_config.TextColumn("Category"),
                     "total_rights":     st.column_config.NumberColumn("Total Rights"),
                     "expired_rights":   st.column_config.NumberColumn("Expired"),
                     "Last Rights":      st.column_config.TextColumn("Last Rights End"),
                     "Status":           st.column_config.TextColumn("Gap Status"),
                     "series_title":     st.column_config.TextColumn("Series"),
                })
            st.download_button("📥 Export Gap Report", no_rights_df.to_csv(index=False),
                               f"gap_{reg}_{gap_plat}.csv", "text/csv")

            # Set Alert on gap
            if st.button("🔔 Alert me when rights are added for these titles", key="gap_alert_btn"):
                _, err = save_alert(DB_CONN, "Gap", f"Rights gap — {len(no_rights_df)} titles, {gap_plat} [{reg}]",
                                    region=reg, platform=gap_plat, persona=st.session_state.persona)
                if not err: st.success("🔔 Gap alert saved!")

    with tab_b:
        st.markdown("#### Active rights expiring soon with no sales renewal in place")
        no_renewal_df = run(f"""
            SELECT mr.title_name,
                   mr.media_platform_primary                                   AS platform,
                   mr.term_to                                                   AS rights_expiry,
                   CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER)     AS days_left,
                   mr.rights_type, mr.exclusivity,
                    sd.sales_deal_id                                            AS has_sales_deal,
                   sd.buyer, sd.status                                         AS sales_status,
                    CASE WHEN sd.sales_deal_id IS NULL THEN '🆘 No Renewal'
                        ELSE '⚠ Check Sale' END                               AS renewal_risk
            FROM media_rights mr
            LEFT JOIN sales_deal sd ON mr.title_id = sd.title_id
              AND UPPER(sd.region) = '{reg}' AND sd.status = 'Active'
            WHERE UPPER(mr.region) = '{reg}' AND mr.status = 'Active'
              AND mr.term_to  <= DATE('now', '+180 days')
              AND mr.term_to  >= DATE('now')
              {plat_cond}
            ORDER BY days_left ASC
            LIMIT 200
         """)
        if not no_renewal_df.empty:
            no_sale = no_renewal_df[no_renewal_df['has_sales_deal'].isna()]
            stat_tiles([
                (f"{len(no_renewal_df):,}",  "Rights Expiring in 180 Days",   "#92400e"),
                (f"{len(no_sale):,}",         "No Active Sales Deal in Place", "#991b1b"),
                (f"{len(no_renewal_df)-len(no_sale):,}", "Has Active Sale",   "#166534"),
            ])
            st.plotly_chart(
                bar(no_renewal_df.groupby('platform').size().reset_index(name='count'),
                    'platform','count','Expiring Rights by Platform',h=260),
                use_container_width=True)
            st.dataframe(no_renewal_df[['title_name','platform','rights_expiry','days_left',
                                         'rights_type','exclusivity','buyer','sales_status','renewal_risk']],
                use_container_width=True, hide_index=True,
                column_config={
                     "title_name":   st.column_config.TextColumn("Title"),
                     "platform":     st.column_config.TextColumn("Platform"),
                     "rights_expiry":st.column_config.TextColumn("Expiry"),
                     "days_left":    st.column_config.NumberColumn("Days Left"),
                     "exclusivity":  st.column_config.CheckboxColumn("Exclusive"),
                     "buyer":        st.column_config.TextColumn("Buyer (if any)"),
                     "sales_status": st.column_config.TextColumn("Sale Status"),
                     "renewal_risk": st.column_config.TextColumn("Risk"),
                })

    with tab_c:
        st.markdown("#### Rights coverage by genre × platform — spot missing intersections")
        heat_df = run(f"""
            SELECT t.genre,
                   mr.media_platform_primary AS platform,
                   COUNT(DISTINCT t.title_id) AS titles_covered,
                   SUM(CASE WHEN mr.status='Active' THEN 1 ELSE 0 END) AS active_rights
            FROM title t
            LEFT JOIN media_rights mr ON t.title_id = mr.title_id
              AND UPPER(mr.region) = '{reg}'
            WHERE UPPER(t.region) = '{reg}' AND t.genre IS NOT NULL
              AND mr.media_platform_primary IS NOT NULL
            GROUP BY t.genre, mr.media_platform_primary
         """)
        if not heat_df.empty:
            pivot = heat_df.pivot_table(index='genre', columns='platform',
                                        values='active_rights', aggfunc='sum', fill_value=0)
            fig = go.Figure(data=go.Heatmap(
                z=pivot.values.tolist(),
                x=pivot.columns.tolist(),
                y=pivot.index.tolist(),
                colorscale='Purples',
                text=pivot.values.tolist(),
                texttemplate="%{text}",
                hoverongaps=False,
                showscale=True,
            ))
            fig.update_layout(**PT, height=400, title=f"Active Rights — Genre × Platform [{reg}]",
                              xaxis_title="Platform", yaxis_title="Genre")
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Zero (blank/dark) cells = no active rights for that genre/platform combination in this region.")

# ═══════════════════════════════════════════════════════════════════════════════
# COMPARE REGIONS — Feature 18
# ═══════════════════════════════════════════════════════════════════════════════
def page_compare():
    st.markdown('''
    <div class="page-header">⚖️ Compare Regions</div>
    <div class="page-sub">Side-by-side rights, DNA, sales and content coverage across two markets</div>
    ''', unsafe_allow_html=True)
    regions_all = ["NA", "APAC", "EMEA", "LATAM"]
    c1, c2 = st.columns(2)
    reg_a = c1.selectbox("Region A", regions_all, index=0, key="cmp_a")
    reg_b = c2.selectbox("Region B", regions_all, index=2, key="cmp_b")

    if reg_a == reg_b:
        st.warning("Select two different regions to compare.")
        return

    st.divider()
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Rights Overview", "🚫 DNA", "💸 Sales", "🎬 Content"])

    def _kpi(reg):
        return run(f"""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN status='Active'  THEN 1 ELSE 0 END) AS active,
                   SUM(CASE WHEN status='Expired' THEN 1 ELSE 0 END) AS expired,
                   SUM(CASE WHEN term_to  <= DATE('now','+30 days')
                       AND status='Active' THEN 1 ELSE 0 END)        AS exp30,
                   SUM(CASE WHEN term_to  <= DATE('now','+90 days')
                       AND status='Active' THEN 1 ELSE 0 END)        AS exp90,
                   SUM(exclusivity)                                   AS exclusive, 
                   COUNT(DISTINCT title_id)                          AS titles_covered
            FROM media_rights WHERE UPPER(region)='{reg}'
         """)

    with tab1:
        ka, kb = _kpi(reg_a), _kpi(reg_b)
        if ka.empty or kb.empty:
            st.warning("No data"); return
        ra, rb = ka.iloc[0], kb.iloc[0]

        # KPI comparison grid
        metrics = [
            ("Total Rights",     "total"),
            ("Active",           "active"),
            ("⚠ Expiring 30d",  "exp30"),
            ("Expiring 90d",     "exp90"),
            ("Exclusive",        "exclusive"),
            ("Titles Covered",   "titles_covered"),
        ]
        cols = st.columns(len(metrics))
        for col, (lbl, key) in zip(cols, metrics):
            va, vb = int(ra.get(key,0)), int(rb.get(key,0))
            delta = va - vb
            col.metric(f"{lbl}", f"{reg_a}: {va:,}", delta=f"vs {reg_b}: {vb:,}",
                      delta_color="off")

        # Grouped bar: platform distribution
        for reg, clr in [(reg_a,'#7c3aed'),(reg_b,'#f59e0b')]:
            df = run(f"""
                SELECT media_platform_primary AS platform, COUNT(*) AS rights
                FROM media_rights WHERE UPPER(region)='{reg}' AND status='Active'
                GROUP BY platform
             """)
            df['region'] = reg
            if reg == reg_a: df_a = df
            else: df_b = df

        if not df_a.empty and not df_b.empty:
            combined = pd.concat([df_a, df_b])
            fig = px.bar(combined, x='platform', y='rights', color='region', barmode='group',
                        title=f"Active Rights by Platform — {reg_a} vs {reg_b}",
                        color_discrete_map={reg_a:'#7c3aed', reg_b:'#f59e0b'})
            fig.update_layout(**PT, height=320)
            st.plotly_chart(fig, use_container_width=True)

         # Exclusivity compare
        for reg in [reg_a, reg_b]:
            df = run(f"""
                SELECT media_platform_primary AS platform,
                       SUM(exclusivity) AS exclusive, COUNT(*)-SUM(exclusivity) AS non_exclusive
                FROM media_rights  WHERE UPPER(region)='{reg}'
                GROUP BY platform
             """)
            df['region'] = reg
            if reg == reg_a: excl_a = df
            else: excl_b = df

        if not excl_a.empty and not excl_b.empty:
            excl_all = pd.concat([excl_a, excl_b])
            fig2 = px.bar(excl_all, x='platform', y='exclusive', color='region', barmode='group',
                          title=f"Exclusive Rights by Platform — {reg_a} vs {reg_b}",
                          color_discrete_map={reg_a:'#7c3aed', reg_b:'#f59e0b'})
            fig2.update_layout(**PT, height=300)
            st.plotly_chart(fig2, use_container_width=True) 

    with tab2:
        da = run(f"SELECT reason_category AS cat, COUNT(*) AS n FROM do_not_air WHERE UPPER(region)='{reg_a}' AND active=1 GROUP BY cat")
        db = run(f"SELECT reason_category AS cat, COUNT(*) AS n FROM do_not_air WHERE UPPER(region)='{reg_b}' AND active=1 GROUP BY cat")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown(f"#### 🚫 {reg_a} DNA")
            if not da.empty:
                st.metric("Total DNA Records", f"{da['n'].sum():,}")
                st.plotly_chart(pie(da,'cat','n',f'DNA — {reg_a}',h=280), use_container_width=True)
        with c2:
            st.markdown(f"#### 🚫 {reg_b} DNA")
            if not db.empty:
                st.metric("Total DNA Records", f"{db['n'].sum():,}")
                st.plotly_chart(pie(db,'cat','n',f'DNA — {reg_b}',h=280), use_container_width=True)

        # Overlap: titles with DNA in BOTH regions
        overlap = run(f"""
            SELECT DISTINCT a.title_name
            FROM do_not_air a
            JOIN do_not_air b ON a.title_id = b.title_id
            WHERE UPPER(a.region)='{reg_a}' AND UPPER(b.region)='{reg_b}'
              AND a.active=1 AND b.active=1
         """)
        if not overlap.empty:
            st.markdown(f"**{len(overlap)} titles flagged in BOTH {reg_a} and {reg_b}:**")
            st.dataframe(overlap, use_container_width=True, hide_index=True)

    with tab3:
        sa = run(f"SELECT buyer, SUM(deal_value) AS val, COUNT(*) AS n FROM sales_deal WHERE UPPER(region)='{reg_a}' AND status='Active' GROUP BY buyer ORDER BY val DESC LIMIT 10")
        sb = run(f"SELECT buyer, SUM(deal_value) AS val, COUNT(*) AS n FROM sales_deal WHERE UPPER(region)='{reg_b}' AND status='Active' GROUP BY buyer ORDER BY val DESC LIMIT 10")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown(f"#### 💸 {reg_a} — Top Buyers")
            if not sa.empty:
                st.metric("Active Deal Value", fmt_m(sa['val'].sum()))
                st.plotly_chart(bar(sa,'buyer','val',f'Sales — {reg_a}',h=300,horiz=True), use_container_width=True)
        with c2:
            st.markdown(f"#### 💸 {reg_b} — Top Buyers")
            if not sb.empty:
                st.metric("Active Deal Value", fmt_m(sb['val'].sum()))
                st.plotly_chart(bar(sb,'buyer','val',f'Sales — {reg_b}',h=300,horiz=True), use_container_width=True)

    with tab4:
        ta = run(f"SELECT genre, COUNT(*) AS titles FROM title WHERE UPPER(region)='{reg_a}' GROUP BY genre ORDER BY titles DESC")
        tb = run(f"SELECT genre, COUNT(*) AS titles FROM title WHERE UPPER(region)='{reg_b}' GROUP BY genre ORDER BY titles DESC")
        if not ta.empty and not tb.empty:
            ta['region'] = reg_a; tb['region'] = reg_b
            combined = pd.concat([ta, tb])
            fig = px.bar(combined, x='genre', y='titles', color='region', barmode='group',
                         title=f"Content by Genre — {reg_a} vs {reg_b}",
                         color_discrete_map={reg_a:'#7c3aed', reg_b:'#f59e0b'})
            fig.update_layout(**PT, height=320)
            st.plotly_chart(fig, use_container_width=True)

         # Title type breakdown
        tta = run(f"SELECT title_type, COUNT(*) AS n FROM title WHERE UPPER(region)='{reg_a}' GROUP BY title_type")
        ttb = run(f"SELECT title_type, COUNT(*) AS n FROM title WHERE UPPER(region)='{reg_b}' GROUP BY title_type")
        c1, c2 = st.columns(2)
        with c1:
            if not tta.empty: st.plotly_chart(pie(tta,'title_type','n',f'Types — {reg_a}',h=260), use_container_width=True)
        with c2:
             if not ttb.empty: st.plotly_chart(pie(ttb,'title_type','n',f'Types — {reg_b}',h=260), use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# ALERTS PAGE — Feature 15
# ═══════════════════════════════════════════════════════════════════════════════
def page_alerts():
    reg = st.session_state.current_region
    st.markdown('''
    <div class="page-header">🔔 Alerts & Saved Filters</div>
    <div class="page-sub">Rights expiry alerts and gap notifications · ''' + reg + '''</div>
    ''', unsafe_allow_html=True)
    alerts_df, err = get_alerts(DB_CONN, region=reg)
    if err:
        st.error(f"Error loading alerts: {err}")
        return

    # KPIs
    total     = len(alerts_df)
    active    = len(alerts_df[alerts_df['dismissed'] == 0]) if not alerts_df.empty else 0
    dismissed = total - active
    stat_tiles([
        (f"{active:,}",     "Active Alerts",     "#991b1b"),
        (f"{dismissed:,}",  "Dismissed",          "#64748b"),
        (f"{total:,}",      "Total Saved",        "#0f172a"),
    ])

    st.divider()

    # ── Create new alert manually ──────────────────────────────────────────────
    with st.expander("➕ Create New Alert Manually"):
        a1, a2, a3, a4 = st.columns(4)
        new_type   = a1.selectbox("Alert Type",  ["Expiry", "Gap", "DNA", "Sales"], key="new_al_type")
        new_label  = a2.text_input("Label",       placeholder="e.g. SVOD rights expiring Q2", key="new_al_label")
        new_plat   = a3.selectbox("Platform",     ["All", "PayTV", "STB-VOD", "SVOD", "FAST"], key="new_al_plat")
        new_days   = a4.number_input("Days threshold", 7, 365, 90, key="new_al_days")
        new_notes  = st.text_area("Notes (optional)", key="new_al_notes", height=60)
        if st.button("💾 Save Alert", key="save_new_alert", type="primary"):
            if new_label.strip():
                _, err = save_alert(DB_CONN, new_type, new_label.strip(),
                                    region=reg, platform=new_plat,
                                     days_threshold=int(new_days),
                                    persona=st.session_state.persona,
                                    notes=new_notes.strip() or None)
                if err: st.error(f"Failed: {err}")
                else:   st.success("✅ Alert saved!"); st.rerun()
            else:
                st.warning("Please enter a label.")

    st.divider()

    # ── Alert list ─────────────────────────────────────────────────────────────
    show_dismissed = st.checkbox("Show dismissed alerts", key="show_dismissed")
    alerts_df, _ = get_alerts(DB_CONN, region=reg, include_dismissed=show_dismissed)

    if alerts_df.empty:
        st.info("No alerts yet. Use the '🔔 Set Alert' buttons on the Rights Explorer Expiry tab, or create one above.")
        return

    # Group by type
    for atype in alerts_df['alert_type'].unique():
        grp = alerts_df[alerts_df['alert_type'] == atype]
        _icons = {'Expiry':'⏰','Gap':'🔍','DNA':'🚫','Sales':'💸'}
        _icon  = _icons.get(atype, '🔔')
        st.markdown(f"### {_icon} {atype} Alerts ({len(grp)})")

        for _, row in grp.iterrows():
            dismissed_style = "opacity:0.45;" if row['dismissed'] else ""
            with st.container():
                ca, cb, cc = st.columns([5, 2, 1])
                with ca:
                    st.markdown(
                        f'<div style="background:#fff;border:1px solid {"#fca5a5" if not row["dismissed"] else "#e2e8f0"};'
                        f'border-left:4px solid {"#ef4444" if not row["dismissed"] else "#94a3b8"};'
                        f'border-radius:8px;padding:10px 14px;{dismissed_style}">'
                        f'<div style="font-weight:700;color:#0f172a;font-size:.9rem">{row["label"]}</div>'
                        f'<div style="font-size:.75rem;color:#64748b;margin-top:4px">'
                        f'Region:  <b>{row["region"]}</b> · Platform:  <b>{row["platform"] or "All"}</b> · '
                        f'Threshold:  <b>{row["days_threshold"]}d</b> · Persona: {row["persona"]}'
                        f'{"<br>📝 " + str(row["notes"]) if row["notes"] else ""}'
                        f'</div>'
                        f'<div style="font-size:.68rem;color:#94a3b8;margin-top:2px">Created: {str(row["created_at"])[:16]}</div>'
                        f'</div>', unsafe_allow_html=True)
                with cb:
                    # Jump to relevant page
                    if not row['dismissed']:
                        if row['alert_type'] == 'Expiry':
                            if st.button("▶ View Expiry", key=f"al_exp_{row['alert_id']}"):
                                st.session_state.page = 'rights'; st.rerun()
                        elif row['alert_type'] == 'Gap':
                            if st.button("▶ View Gap", key=f"al_gap_{row['alert_id']}"):
                                st.session_state.page = 'gap_analysis'; st.rerun()
                with cc:
                    if not row['dismissed']:
                        if st.button("✕", key=f"al_dis_{row['alert_id']}", help="Dismiss alert"):
                            dismiss_alert(DB_CONN, int(row['alert_id']))
                            st.rerun()

    # Bulk clear
    st.divider()
    if st.button("🗑 Dismiss All Active Alerts", key="dismiss_all"):
        for _, row in alerts_df[alerts_df['dismissed']==0].iterrows():
            dismiss_alert(DB_CONN, int(row['alert_id']))
        st.rerun()

# ═══════════════════════════════════════════════════════════════════════════════
# TITLE 360 — Feature 16
# ═══════════════════════════════════════════════════════════════════════════════
def page_title_360():
    reg = st.session_state.current_region
    st.markdown('''
    <div class="page-header">🎯 Title 360</div>
    <div class="page-sub">Complete per-title view — rights, DNA, sales, work orders & elemental</div>
    ''', unsafe_allow_html=True)
    # Title selector
    # Quick-select from session (set by drilldown buttons elsewhere)
    preselect = st.session_state.get('title_360') or ""

    all_titles = run("""
        SELECT title_name FROM title UNION SELECT movie_title AS title_name FROM movie
        ORDER BY title_name
     """)
    title_list = all_titles['title_name'].tolist() if not all_titles.empty else []

    # Find preselect index
    try:
        preselect_idx = title_list.index(preselect) + 1 if preselect in title_list else 0
    except ValueError:
        preselect_idx = 0

    chosen = st.selectbox("Select a title", ["— choose a title —"] + title_list,
                          index=preselect_idx, key="t360_sel")

    if chosen == "— choose a title —":
        st.info("👆 Select a title above to see its full 360° profile.")
        return

    # Persist selection
    st.session_state.title_360 = chosen

    # ── Resolve title_id(s) — handles series titles AND movie titles ──────────
    t_ids_df = run(f"""
        SELECT title_id FROM title
        WHERE LOWER(title_name) LIKE '%{chosen.lower().replace("'", "''")}%'
        LIMIT 20
     """)
    t_ids = t_ids_df['title_id'].tolist() if not t_ids_df.empty else []
    id_list = "','".join(t_ids)
    if not id_list:
        st.warning("No title records found for that selection.")
        return

    # ── Top summary ────────────────────────────────────────────────────────────
    summary = run(f"""
        SELECT t.title_name, t.title_type, t.genre, t.content_category,
               t.controlling_entity, t.age_rating, t.release_year,
               t.runtime_minutes, t.region,
                s.series_title, se.season_number, t.episode_number,
               m.movie_title, m.franchise, m.box_office_gross_usd_m
        FROM title t
        LEFT JOIN season se ON t.season_id = se.season_id
        LEFT JOIN series  s ON t.series_id = s.series_id
        LEFT JOIN movie   m ON t.movie_id  = m.movie_id
        WHERE t.title_id IN ('{id_list}')
        LIMIT 1
     """)
    if not summary.empty:
        r = summary.iloc[0]
        h1, h2, h3, h4 = st.columns(4)
        h1.metric("Type",       str(r.get('title_type','—')))
        h2.metric("Genre",      str(r.get('genre','—')))
        h3.metric("Rating",     str(r.get('age_rating','—')))
        h4.metric("Year",       str(r.get('release_year','—')))
        if r.get('series_title'):
            st.markdown(f"📺 **Series:** {r['series_title']} · Season {r.get('season_number','?')} · Ep {r.get('episode_number','?')}")
        if r.get('movie_title') and r.get('franchise'):
            st.markdown(f"🎬 **Franchise:** {r['franchise']} · Box Office: ${r.get('box_office_gross_usd_m',0):.0f}M")

    st.divider()
    t1, t2, t3, t4, t5 = st.tabs(["📋 Rights", "🚫 DNA", "💸 Sales", "⚙️ Work Orders", "🎞 Elemental"])

    with t1:
        rights_df = run(f"""
            SELECT mr.rights_id, cd.deal_source, cd.deal_name,
                   mr.rights_type, mr.media_platform_primary AS platform,
                   mr.media_platform_ancillary AS ancillary,
                   mr.territories, mr.language, mr.brand,
                   mr.term_from, mr.term_to, mr.exclusivity, mr.holdback,
                   mr.holdback_days, mr.status,
                    CAST(JULIANDAY(mr.term_to)-JULIANDAY('now') AS INTEGER) AS days_left,
                   mr.notes_restrictive
            FROM media_rights mr
            JOIN content_deal cd ON mr.deal_id = cd.deal_id
            WHERE mr.title_id IN ('{id_list}')
            ORDER BY mr.status, mr.term_to ASC
         """)
        if rights_df.empty:
            st.info("No rights records found for this title.")
        else:
            active_r = int((rights_df['status']=='Active').sum())
            exp30    = int((rights_df['days_left'].fillna(999) <= 30).sum())
            stat_tiles([
                (f"{len(rights_df)}",   "Total Rights",    "#0f172a"),
                (f"{active_r}",         "Active",           "#166534"),
                (f"{len(rights_df)-active_r}",  "Expired/Pending",  "#64748b"),
                (f"{exp30}",            "Expiring 30d",     "#991b1b"),
            ])
            rights_df['⏰ Days'] = rights_df['days_left'].apply(exp_tag)
            rights_df['Excl']   = rights_df['exclusivity'].apply(bool_icon)
            st.dataframe(
                 rights_df[['deal_source','rights_type','platform','ancillary',
                           'territories','language','term_from','term_to','⏰ Days','Excl','status']],
                 use_container_width=True, hide_index=True,
                column_config={
                     "deal_source": st.column_config.TextColumn("Source"),
                     "rights_type": st.column_config.TextColumn("Type"),
                     "platform":    st.column_config.TextColumn("Platform"),
                     "ancillary":   st.column_config.TextColumn("Ancillary"),
                     "territories": st.column_config.TextColumn("Territories"),
                     "language":    st.column_config.TextColumn("Language"),
                     "term_from":   st.column_config.TextColumn("Start"),
                     "term_to":     st.column_config.TextColumn("End"),
                     "⏰ Days":     st.column_config.TextColumn("Days Left"),
                     "Excl":        st.column_config.TextColumn("Exclusive"),
                     "status":      st.column_config.TextColumn("Status"),
                })
            # Alert button
            if st.button("🔔 Alert on expiry", key="t360_rights_alert"):
                _, err = save_alert(DB_CONN, "Expiry",f"Rights alert — {chosen}",
                                    title_name=chosen, region=reg,
                                    persona=st.session_state.persona)
                if not err: st.success("Alert saved!")

    with t2:
        dna_df = run(f"""
            SELECT dna_id, reason_category, reason_subcategory,
                   territory, media_type, term_from, term_to, additional_notes, active
            FROM do_not_air
            WHERE title_id IN ('{id_list}')
            ORDER BY active DESC, reason_category
         """)
        if dna_df.empty:
            st.success("✅ No Do-Not-Air restrictions for this title.")
        else:
            active_dna = int(dna_df['active'].sum())
            stat_tiles([
                (f"{active_dna}",  "Active DNA Flags",  "#991b1b"),
                (f"{len(dna_df)-active_dna}",  "Inactive/Historical",  "#64748b"),
            ])
            st.dataframe(dna_df, use_container_width=True, hide_index=True)

    with t3:
        sales_df = run(f"""
            SELECT sales_deal_id, deal_type, buyer, territory, region,
                   media_platform, term_from, term_to,
                   deal_value, currency, status
            FROM sales_deal
            WHERE title_id IN ('{id_list}')
            ORDER BY deal_value DESC
         """)
        if sales_df.empty:
            st.info("No outbound sales deals for this title.")
        else:
            stat_tiles([
                (f"{len(sales_df)}",                   "Sales Deals",    "#0f172a"),
                (fmt_m(sales_df['deal_value'].sum()),   "Total Value",    "#1e40af"),
                (f"{sales_df['buyer'].nunique()}",      "Unique Buyers",  "#5b21b6"),
            ])
            st.dataframe(sales_df, use_container_width=True, hide_index=True,
                column_config={"deal_value": st.column_config.NumberColumn("Value", format="$%,.0f")})

    with t4:
        wo_df = run(f"""
            SELECT work_order_id, vendor_name, work_type, status,
                   priority, due_date, quality_score, cost, billing_status
            FROM work_orders
            WHERE title_id IN ('{id_list}')
            ORDER BY due_date ASC
         """)
        if wo_df.empty:
            st.info("No work orders for this title.")
        else:
            stat_tiles([
                (f"{len(wo_df)}",               "Work Orders",   "#0f172a"),
                (f"{int((wo_df['status']=='In Progress').sum())}",  "In Progress",  "#1e40af"),
                (f"{int((wo_df['status']=='Delayed').sum())}",      "Delayed",      "#991b1b"),
                (fmt_m(wo_df['cost'].sum()),    "Total Cost",    "#92400e"),
            ])
            st.dataframe(wo_df, use_container_width=True, hide_index=True,
                column_config={
                     "quality_score": st.column_config.ProgressColumn("Quality",min_value=0,max_value=100,format="%.1f"),
                     "cost": st.column_config.NumberColumn("Cost", format="$%,.0f"),
                })

    with t5:
        el_df = run(f"""
            SELECT er.elemental_rights_id, ed.deal_source, er.media_platform_primary,
                   er.territories, er.language, er.term_from, er.term_to, er.status
            FROM elemental_rights er
            JOIN elemental_deal ed ON er.elemental_deal_id = ed.elemental_deal_id
            WHERE er.title_id IN ('{id_list}')
            ORDER BY er.status, er.term_to
        """)
        if el_df.empty:
            st.info("No elemental rights (promos, trailers, raw assets) for this title.")
        else:
            st.dataframe(el_df, use_container_width=True, hide_index=True)

# ═══════════════════════════════════════════════════════════════════════════════
# Router — ENHANCED with Dashboard
# ═══════════════════════════════════════════════════════════════════════════════
{
    "rights":      page_rights,
    "titles":      page_titles,
    "dna":         page_dna,
    "sales":       page_sales,
    "deals":       page_deals,
    "vendors":     page_vendors,
    "work_orders": page_work_orders,
    "gap_analysis":page_gap_analysis,
    "compare":     page_compare,
    "alerts":      page_alerts,
    "title_360":   page_title_360,
    "chat":        page_chat,
    "dashboard":   page_custom_dashboard,
}.get(st.session_state.page, page_rights)()

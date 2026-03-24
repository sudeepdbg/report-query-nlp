"""
Foundry Vantage — Rights Explorer
Persona: Business Affairs & Strategy | HBO/Cinemax/HBO Max
Pages: Rights Explorer · Title Catalog · Do-Not-Air · Sales · Work Orders · Chat
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

# ── CSS ────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
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
.badge-green {background:#dcfce7;color:#166534}
.badge-red   {background:#fee2e2;color:#991b1b}
.badge-amber {background:#fef3c7;color:#92400e}
.badge-blue  {background:#dbeafe;color:#1e40af}
.badge-purple{background:#ede9fe;color:#5b21b6}
.badge-gray  {background:#f1f5f9;color:#475569}
/* Page header */
.page-header{font-size:1.75rem;font-weight:800;color:#0f172a;line-height:1.2;margin-bottom:4px}
.page-sub{font-size:.85rem;color:#64748b;margin-bottom:1rem}
/* Rights window pill */
.window-pill{display:inline-block;padding:2px 10px;border-radius:20px;font-size:11px;font-weight:700;margin:2px}
.window-SVOD   {background:#ede9fe;color:#5b21b6}
.window-PayTV  {background:#dbeafe;color:#1e40af}
.window-STB-VOD{background:#fef3c7;color:#92400e}
.window-FAST   {background:#dcfce7;color:#166534}
.window-CatchUp{background:#ffedd5;color:#9a3412}
.window-other  {background:#f1f5f9;color:#475569}
/* Expiry urgency */
.exp-critical{background:#fee2e2;color:#991b1b;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:700}
.exp-warn    {background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:700}
.exp-ok      {background:#dcfce7;color:#166534;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:700}
/* Stat tile */
.stat-tile{background:#fff;border:1px solid #e2e8f0;border-radius:12px;padding:14px 16px;
text-align:center;box-shadow:0 1px 3px rgba(0,0,0,.04)}
.stat-tile .val{font-size:1.6rem;font-weight:800;color:#0f172a;line-height:1.1}
.stat-tile .lbl{font-size:.6rem;font-weight:700;color:#94a3b8;text-transform:uppercase;
letter-spacing:.07em;margin-top:4px}
</style>
""", unsafe_allow_html=True)

# ── DB init ───────────────────────────────────────────────────────────────────
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

# ── Session state ─────────────────────────────────────────────────────────────
for k, v in {
    'page': 'rights',
    'chat_history': [],
    'current_region': 'NA',
    'persona': 'Business Affairs',
    'user_prefs': {'show_sql': True, 'raw_sql_mode': False, 'auto_viz': True},
    'db_stats': {},
    'pending_prompt': None,
    'title_360': None,
    'compare_region': None,
    'alerts_count': 0,
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

# ── Navigation ─────────────────────────────────────────────────────────────────
PAGES = [
    ("rights",      "🔑", "Rights Explorer"),
    ("titles",      "🎬", "Title Catalog"),
    ("dna",         "🚫", "Do-Not-Air"),
    ("sales",       "💸", "Sales Deals"),
    ("deals",       "💼", "Deals"),
    ("vendors",     "🏢", "Vendors"),
    ("work_orders", "⚙️",  "Work Orders"),
    ("gap_analysis","🔍", "Gap Analysis"),
    ("compare",     "⚖️",  "Compare Regions"),
    ("alerts",      "🔔", "Alerts"),
    ("title_360",   "🎯", "Title 360"),
    ("chat",        "💬", "Chat / Query"),
]

# ── Helpers ───────────────────────────────────────────────────────────────────
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
    return f'<span class="badge {c}">{s}</span>'

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

# ── NEW: Enhanced Chart Rendering for Dynamic Report Generation ───────────────
def _render_line_chart(df, x_col, y_col, title=""):
    """Render line chart for time-series data."""
    fig = px.line(df.sort_values(x_col), x=x_col, y=y_col,
                  title=title, markers=True, color_discrete_sequence=['#7c3aed'])
    fig.update_layout(**PT, height=350)
    fig.update_xaxes(tickangle=-30)
    return fig

def _detect_chart_from_data(df, parser_hint):
    """Detect optimal chart type based on data shape if parser hint is generic."""
    if df.empty: return 'table', {}
    
    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    cat_cols = [c for c in df.columns if pd.api.types.is_object_dtype(df[c])]
    date_cols = [c for c in df.columns if 'date' in c.lower() or 'time' in c.lower() or 'year' in c.lower()]
    
    meta = {'x_column': None, 'y_column': None, 'reason': ''}
    
    # Line chart for time-series
    if parser_hint == 'line' or (len(date_cols) > 0 and len(num_cols) > 0):
        meta['x_column'] = date_cols[0] if date_cols else df.columns[0]
        meta['y_column'] = num_cols[0]
        meta['reason'] = 'Time-series data detected — line chart shows trends'
        return 'line', meta
    
    # Pie chart for distribution
    if parser_hint == 'pie' and len(cat_cols) >= 1 and len(num_cols) >= 1:
        meta['x_column'] = cat_cols[0]
        meta['y_column'] = num_cols[0]
        meta['reason'] = 'Category distribution — pie chart shows proportions'
        return 'pie', meta
    
    # Bar chart for comparison
    if len(cat_cols) >= 1 and len(num_cols) >= 1:
        meta['x_column'] = cat_cols[0]
        meta['y_column'] = num_cols[0]
        meta['reason'] = 'Category comparison — bar chart shows relative values'
        return 'bar', meta
    
    return 'table', {'reason': 'No clear pattern detected — table shows all data'}

def stat_tiles(items):
    cols = st.columns(len(items))
    for col, (val, lbl, color) in zip(cols, items):
        col.markdown(
            f'<div class="stat-tile">'
            f'<div class="val" style="color:{color}">{val}</div>'
            f'<div class="lbl">{lbl}</div></div>', unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
<div style="padding:20px 16px 14px;border-bottom:1px solid rgba(255,255,255,.05)">
<div style="display:flex;align-items:center;gap:10px">
<div style="width:36px;height:36px;background:#7c3aed;border-radius:9px;
display:flex;align-items:center;justify-content:center;font-size:18px;flex-shrink:0">🔑</div>
<div>
<div style="font-size:1.05rem;font-weight:800;color:#f1f5f9;letter-spacing:-.02em">Foundry Vantage</div>
<div style="font-size:.6rem;color:#475569;letter-spacing:.1em;text-transform:uppercase">Rights Explorer · MVP 2026</div>
</div>
</div>
</div>""", unsafe_allow_html=True)

# Navigation
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
regions = ["NA","APAC","EMEA","LATAM"]
def _on_region():
    st.session_state.current_region = st.session_state._reg_sel
st.selectbox("Region / Market", regions,
             index=regions.index(st.session_state.current_region),
             key="_reg_sel", on_change=_on_region)

personas = ["Business Affairs","Strategy","Legal","Operations","Analytics"]
def _on_persona():
    st.session_state.persona = st.session_state._per_sel
st.selectbox("Persona", personas,
             index=personas.index(st.session_state.persona)
             if st.session_state.persona in personas else 0,
             key="_per_sel", on_change=_on_persona)
st.markdown('</div>', unsafe_allow_html=True)

# DB stats
stats = st.session_state.db_stats
alerts_live, _ = get_alerts(DB_CONN, region=st.session_state.current_region)
st.session_state.alerts_count = len(alerts_live) if alerts_live is not None else 0

stat_pairs = [
    ("title",        "Titles"),
    ("movie",        "Movies"),
    ("media_rights", "Rights"),
    ("do_not_air",   "DNA"),
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

ac = st.session_state.alerts_count
if ac > 0:
    st.markdown(
        f'<div style="margin:0 8px 4px;background:rgba(239,68,68,.12);border:1px solid rgba(239,68,68,.3);'
        f'border-radius:8px;padding:6px 10px;font-size:.75rem;color:#fca5a5;cursor:pointer">'
        f'🔔 <b>{ac} active alert{"s" if ac!=1 else ""}</b> — click Alerts in nav</div>',
        unsafe_allow_html=True)

st.markdown(
    f'<div style="margin:0 8px 8px;background:rgba(124,58,237,.1);border:1px solid rgba(124,58,237,.2);'
    f'border-radius:8px;padding:8px 10px;font-size:.75rem;color:#c4b5fd">'
    f'📍 <b>{st.session_state.current_region}</b> · {st.session_state.persona}<br>'
    f'<span style="color:#64748b;font-size:.68rem">HBO/Cinemax/HBO Max · U.S. default context</span>'
    f'</div>', unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════════════════════════
# RIGHTS EXPLORER PAGE
# ════════════════════════════════════════════════════════════════════════════════
def page_rights():
    reg = st.session_state.current_region
    st.markdown(f'<div class="page-header">🔑 Rights Explorer</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">Content rights licensed-in for <b>{reg}</b> — media rights, windows, territories, exclusivity &amp; expiry</div>', unsafe_allow_html=True)

    # ── Top KPIs ─────────────────────────────────────────────────────────────
    kpi = run(f"""
    SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN status='Active' THEN 1 ELSE 0 END) AS active,
    SUM(CASE WHEN status='Expired' THEN 1 ELSE 0 END) AS expired,
    SUM(CASE WHEN status='Pending' THEN 1 ELSE 0 END) AS pending,
    SUM(CASE WHEN term_to <= DATE('now','+30 days')
    AND term_to >= DATE('now') AND status='Active' THEN 1 ELSE 0 END) AS exp30,
    SUM(CASE WHEN term_to <= DATE('now','+90 days')
    AND term_to >= DATE('now') AND status='Active' THEN 1 ELSE 0 END) AS exp90,
    COUNT(DISTINCT title_id) AS titles_covered,
    SUM(exclusivity) AS exclusive_count
    FROM media_rights WHERE UPPER(region)='{reg}'
    """)
    if not kpi.empty:
        r = kpi.iloc[0]
        items = [
            (f"{int(r.get('total',0)):,}",        "Total Rights",      "#0f172a"),
            (f"{int(r.get('active',0)):,}",       "Active",            "#166534"),
            (f"{int(r.get('exp30',0)):,}",         "⚠ Expiring 30d",   "#991b1b"),
            (f"{int(r.get('exp90',0)):,}",         "Expiring 90d",      "#92400e"),
            (f"{int(r.get('expired',0)):,}",       "Expired",           "#64748b"),
            (f"{int(r.get('titles_covered',0)):,}","Titles Covered",    "#1e40af"),
            (f"{int(r.get('exclusive_count',0)):,}","Exclusive",        "#5b21b6"),
        ]
        stat_tiles(items)
    st.divider()

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "⏰ Expiry Alerts", "📺 Windows & Platforms",
        "🌍 Territories", "🔒 Holdbacks",
        "⭐ Exclusivity", "📄 Rights Table"
    ])

    # ── Expiry Alerts ─────────────────────────────────────────────────────────
    with tab1:
        st.markdown("#### Rights Expiring — Upcoming Windows")
        c1, c2, c3 = st.columns([2,1,1])
        days_sel  = c1.slider("Show expiring within (days)", 7, 180, 90, key="exp_days")
        plat_sel  = c2.multiselect("Platform", ["PayTV","STB-VOD","SVOD","FAST"], key="exp_plat")
        rights_sel= c3.selectbox("Rights Type", ["All","Exhibition","Exhibition & Distribution"], key="exp_rt")
        plat_f  = ("AND (" + " OR ".join(f"media_platform_primary LIKE '%{p}%'" for p in plat_sel) + ")"
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
        AND mr.term_to >= DATE('now')
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

            show = exp_df.copy()
            show['Days Left']    = show['days_remaining'].apply(exp_tag)
            show['Exclusive']    = show['exclusivity'].apply(bool_icon)
            show['Holdback']     = show['holdback'].apply(bool_icon)
            show['Restrictions'] = show['notes_restrictive'].fillna('—')
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

        st.markdown("#### Ancillary Platform Coverage")
        df = run(f"""
        SELECT media_platform_ancillary, COUNT(*) AS count,
        COUNT(DISTINCT title_id) AS titles
        FROM media_rights
        WHERE UPPER(region)='{reg}' AND media_platform_ancillary != ''
        GROUP BY media_platform_ancillary ORDER BY count DESC
        """)
        if not df.empty:
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

    # ── Territories ────────────────────────────────────────────────────────────
    with tab3:
        st.markdown("#### Rights Coverage by Territory")
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

        df3 = run(f"""
        SELECT rights_type,
        SUM(exclusivity) AS exclusive,
        COUNT(*)-SUM(exclusivity) AS non_exclusive
        FROM media_rights WHERE UPPER(region)='{reg}'
        GROUP BY rights_type
        """)
        if not df3.empty:
            st.plotly_chart(pie(df3,'rights_type','exclusive','Exclusivity Mix by Rights Type',h=280), use_container_width=True)

    # ── Rights Table ───────────────────────────────────────────────────────────
    with tab6:
        st.markdown("#### Browse All Rights")
        f1, f2, f3, f4 = st.columns(4)
        plat_f2  = f1.selectbox("Platform",    ["All","PayTV","STB-VOD","SVOD","FAST"], key="rt2_plat")
        stat_f2  = f2.selectbox("Status",      ["All","Active","Expired","Pending"],    key="rt2_stat")
        excl_f2  = f3.selectbox("Exclusivity", ["All","Exclusive","Non-Exclusive"],     key="rt2_excl")
        src_f2   = f4.selectbox("Deal Source", ["All","TRL","C2","FRL"],                key="rt2_src")
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

# ════════════════════════════════════════════════════════════════════════════════
# CHAT / QUERY PAGE (ENHANCED WITH DYNAMIC REPORT GENERATION)
# ════════════════════════════════════════════════════════════════════════════════
def page_chat():
    reg = st.session_state.current_region
    st.markdown('<div class="page-header">💬 Chat Query </div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">Natural language rights interrogation · {reg} · Ask about titles, rights windows, expiry, DNA, territories, exclusivity</div>', unsafe_allow_html=True)

    if not st.session_state.chat_history:
        st.markdown("""
<div style="background:#fff;border:1px solid #e2e8f0;border-radius:12px;
padding:14px 16px;margin-bottom:12px">
<div style="font-size:.8rem;font-weight:700;color:#6366f1;
text-transform:uppercase;letter-spacing:.06em;margin-bottom:10px">
💡 Sample queries by intent type
</div>""", unsafe_allow_html=True)
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

    col_a, col_b, col_c = st.columns([3, 1, 1])
    with col_a:
        show_sql = st.toggle("Show SQL", value=st.session_state.user_prefs.get('show_sql', True), key="sql_tog")
        st.session_state.user_prefs['show_sql'] = show_sql
    with col_b:
        raw_sql_mode = st.toggle("⚡ Raw SQL", value=st.session_state.user_prefs.get('raw_sql_mode', False), key="raw_sql_tog")
        st.session_state.user_prefs['raw_sql_mode'] = raw_sql_mode
    with col_c:
        auto_viz = st.toggle("🎨 Auto Chart", value=st.session_state.user_prefs.get('auto_viz', True), key="auto_viz_tog")
        st.session_state.user_prefs['auto_viz'] = auto_viz

    # ── Raw SQL editor ──────────────────────────────────────────────────────────
    if raw_sql_mode:
        st.markdown("""
<div style="background:#f8faff;border:1px solid #c7d2fe;border-radius:10px;
padding:12px 16px;margin-bottom:10px">
<div style="font-size:.78rem;font-weight:700;color:#4f46e5;margin-bottom:6px">
⚡ Raw SQL Mode — type any SQL query directly against the Rights Explorer database
</div>
<div style="font-size:.72rem;color:#64748b">
Tables: <code>movie</code> · <code>title</code> · <code>series</code> · <code>season</code> ·
<code>media_rights</code> · <code>content_deal</code> · <code>exhibition_restrictions</code> ·
<code>elemental_rights</code> · <code>elemental_deal</code> · <code>do_not_air</code> ·
<code>sales_deal</code> · <code>deals</code> · <code>vendors</code> · <code>work_orders</code>
</div>
</div>""", unsafe_allow_html=True)
        raw_templates = {
            "— Pick a template —": "",
            "Title health check (rights + DNA + sales)": """SELECT
t.title_name, t.title_type, t.content_category,
COUNT(DISTINCT mr.rights_id)                                    AS active_rights,
SUM(CASE WHEN mr.term_to <= DATE('now','+90 days') AND mr.status='Active' THEN 1 ELSE 0 END) AS expiring_90d,
COUNT(DISTINCT dna.dna_id)                                      AS dna_flags,
GROUP_CONCAT(DISTINCT dna.reason_category)                      AS dna_reasons,
COUNT(DISTINCT sd.sales_deal_id)                                AS sales_deals,
GROUP_CONCAT(DISTINCT sd.buyer)                                 AS buyers
FROM title t
LEFT JOIN media_rights mr ON t.title_id = mr.title_id AND mr.status='Active'
LEFT JOIN do_not_air dna  ON t.title_id = dna.title_id AND dna.active=1
LEFT JOIN sales_deal sd   ON t.title_id = sd.title_id
WHERE UPPER(t.region) = 'NA'
GROUP BY t.title_id
ORDER BY dna_flags DESC, expiring_90d DESC
LIMIT 100""",
            "Expiry + sales overlap (renewal priority)": """SELECT
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
AND mr.term_to <= DATE('now','+90 days')
ORDER BY days_left ASC
LIMIT 100""",
            "Work orders + rights expiry overlap": """SELECT
wo.title_name, wo.work_type, wo.status AS wo_status,
wo.priority, wo.due_date, wo.vendor_name,
COUNT(DISTINCT mr.rights_id)                                    AS active_rights,
MIN(mr.term_to)                                                 AS earliest_rights_expiry,
SUM(CASE WHEN mr.term_to <= DATE('now','+90 days') THEN 1 ELSE 0 END) AS rights_expiring_90d
FROM work_orders wo
LEFT JOIN title t      ON wo.title_id = t.title_id
LEFT JOIN media_rights mr ON t.title_id = mr.title_id AND mr.status='Active'
WHERE UPPER(wo.region)='NA'
GROUP BY wo.work_order_id
ORDER BY rights_expiring_90d DESC, wo.due_date ASC
LIMIT 100""",
            "Movies + DNA flags": """SELECT
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
            "Movies sold to external buyers": """SELECT
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
            "Full content deal detail (all joins)": """SELECT
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
        default_sql  = raw_templates[sel_template] if sel_template != "— Pick a template —" else st.session_state.get("last_raw_sql", "SELECT * FROM movie LIMIT 10")
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
                    num_cols = [c for c in res_df.columns if pd.api.types.is_numeric_dtype(res_df[c])]
                    if num_cols:
                        mc = st.columns(min(4, len(num_cols)))
                        for i, nc in enumerate(num_cols[:4]):
                            mc[i].metric(nc, f"{res_df[nc].sum():,.0f}")
                    if len(res_df.columns) >= 2:
                        first_num = next((c for c in res_df.columns if pd.api.types.is_numeric_dtype(res_df[c])), None)
                        if first_num and res_df.columns[0] != first_num and len(res_df) <= 50:
                            fig = bar(res_df.head(30), res_df.columns[0], first_num, "Query Result")
                            st.plotly_chart(fig, use_container_width=True)
                    st.dataframe(res_df, use_container_width=True, hide_index=True, height=380)
                    st.download_button("📥 Download CSV", res_df.to_csv(index=False),
                                       "raw_sql_result.csv", "text/csv", key="dl_raw")
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
                sql, error, chart_hint, region_ctx = parse_query(active_prompt, reg)
                if error:
                    st.error(f"❌ {error}")
                else:
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
                            vc = val_cols[0]
                            try:
                                res_df[vc] = pd.to_numeric(res_df[vc], errors='coerce')
                                mc = st.columns(4)
                                mc[0].metric("Total / Sum", f"{res_df[vc].sum():,.0f}")
                                mc[1].metric("Avg",         f"{res_df[vc].mean():,.1f}")
                                mc[2].metric("Records",     f"{len(res_df):,}")
                                mc[3].metric("Max",         f"{res_df[vc].max():,.0f}")
                                metrics_data = [
                                    {"label":"Total",   "value":f"{res_df[vc].sum():,.0f}"},
                                    {"label":"Avg",     "value":f"{res_df[vc].mean():,.1f}"},
                                    {"label":"Records", "value":f"{len(res_df):,}"},
                                    {"label":"Max",     "value":f"{res_df[vc].max():,.0f}"},
                                ]
                            except (ValueError, TypeError) as _metric_err:
                                logger.debug(f"Metrics render skipped: {_metric_err}")

                        # ── DYNAMIC CHART GENERATION ─────────────────────────────
                        fig = None
                        chart_meta = {}
                        
                        # 1. Get chart type from parser hint
                        parser_chart = chart_hint
                        
                        # 2. If auto_viz is enabled, refine based on data shape
                        if auto_viz and parser_chart == 'auto':
                            final_chart, chart_meta = _detect_chart_from_data(res_df, parser_chart)
                        else:
                            final_chart = parser_chart
                            chart_meta = {'reason': 'User preference or parser hint'}
                        
                        # 3. User override for chart type
                        if auto_viz:
                            chart_options = ['auto', 'table', 'bar', 'line', 'pie']
                            user_chart_override = st.selectbox(
                                "📊 Visualization",
                                options=chart_options,
                                index=chart_options.index(final_chart) if final_chart in chart_options else 0,
                                key=f"chart_override_{len(st.session_state.chat_history)}",
                                help="Change chart type if auto-selection isn't right"
                            )
                            if user_chart_override != 'auto':
                                final_chart = user_chart_override
                        
                        # 4. Show chart reasoning (transparency)
                        if auto_viz and final_chart != 'table':
                            st.caption(f"💡 {chart_meta.get('reason', 'Auto-selected visualization')}")
                        
                        # 5. Render Chart
                        if final_chart == 'bar' and len(res_df.columns) >= 2:
                            x_col = chart_meta.get('x_column') or res_df.columns[0]
                            y_col = chart_meta.get('y_column')
                            if not y_col:
                                y_col = next((c for c in res_df.columns[1:] if pd.api.types.is_numeric_dtype(res_df[c])), res_df.columns[1])
                            try: res_df[y_col] = pd.to_numeric(res_df[y_col], errors='coerce')
                            except (ValueError, TypeError): pass
                            fig = bar(res_df.head(30), x_col, y_col, active_prompt[:60])
                        elif final_chart == 'pie' and len(res_df.columns) >= 2:
                            x_col = chart_meta.get('x_column') or res_df.columns[0]
                            y_col = chart_meta.get('y_column')
                            if not y_col:
                                y_col = next((c for c in res_df.columns[1:] if pd.api.types.is_numeric_dtype(res_df[c])), res_df.columns[1])
                            fig = pie(res_df.head(15), x_col, y_col, active_prompt[:60])
                        elif final_chart == 'line' and len(res_df.columns) >= 2:
                            x_col = chart_meta.get('x_column')
                            y_col = chart_meta.get('y_column')
                            if not x_col:
                                for c in res_df.columns:
                                    if 'date' in c.lower() or 'time' in c.lower() or 'month' in c.lower():
                                        x_col = c
                                        break
                                if not x_col: x_col = res_df.columns[0]
                            if not y_col:
                                y_col = next((c for c in res_df.columns[1:] if pd.api.types.is_numeric_dtype(res_df[c])), res_df.columns[1])
                            fig = _render_line_chart(res_df.sort_values(x_col), x_col, y_col, active_prompt[:60])
                        
                        if fig:
                            st.plotly_chart(fig, use_container_width=True)
                        
                        answer_txt = (f"📊 **{len(res_df):,} records** for **{region_ctx}**."
                                      + (" Sorted by expiry date." if 'expir' in active_prompt.lower() else ""))
                        st.markdown(answer_txt)
                        st.dataframe(res_df, use_container_width=True, hide_index=True, height=300)
                        st.download_button("📥 Download CSV", res_df.to_csv(index=False),
                                           f"rights_query_{region_ctx}.csv", "text/csv",
                                           key="dl_live")
                        
                        # Save to history
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

# ── Router ─────────────────────────────────────────────────────────────────────
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
}.get(st.session_state.page, page_rights)()

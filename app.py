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

from utils.database import init_database, execute_sql, get_table_stats
from utils.query_parser import parse_query

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="Foundry Vantage — Rights Explorer",
    page_icon="🔑",
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
    'user_prefs': {'show_sql': True},
    'db_stats': {},
    'pending_prompt': None,
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
    stat_pairs = [
        ("title",        "Titles"),
        ("movie",        "Movies"),
        ("media_rights", "Rights"),
        ("do_not_air",   "DNA"),
    ]
    html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:5px;margin:10px 8px">'
    for key, lbl in stat_pairs:
        html += (f'<div style="background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.07);'
                 f'border-radius:8px;padding:8px;text-align:center">'
                 f'<div style="font-size:1.1rem;font-weight:800;color:#e2e8f0">{stats.get(key,0):,}</div>'
                 f'<div style="font-size:.58rem;color:#475569;text-transform:uppercase;letter-spacing:.06em">{lbl}</div>'
                 f'</div>')
    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)

    # Context note
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

            # Formatted table — clean readable column labels
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
            st.download_button("📥 Export Expiry Report", csv, f"expiry_{reg}_{days_sel}d.csv", "text/csv")

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

    # ── Territories ────────────────────────────────────────────────────────────
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
# TITLE CATALOG
# ════════════════════════════════════════════════════════════════════════════════
def page_titles():
    reg = st.session_state.current_region
    st.markdown('<div class="page-header">🎬 Title Catalog</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">Series · Movies · Episodes — Full WBD content registry · {reg}</div>', unsafe_allow_html=True)

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
            (f"{int(r.get('total',0)):,}",        "Total Titles",    "#0f172a"),
            (f"{int(mr.get('total',0)):,}",        "Films in Slate",  "#1e40af"),
            (f"{int(r.get('series_count',0)):,}",  "Series",          "#5b21b6"),
            (f"{int(r.get('episodes',0)):,}",      "Episodes",        "#166534"),
            (f"${mr.get('total_bo',0)/1000:.1f}B", "Total Box Office","#92400e"),
            (f"{int(r.get('specials',0)):,}",      "Specials",        "#64748b"),
        ])

    st.divider()
    tab1, tab2, tab3, tab4 = st.tabs(["📁 TV Hierarchy", "🎥 Movies", "🎭 Genre & Metadata", "🔍 Search All"])

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


    # ── Movies Tab ──────────────────────────────────────────────────────────────
    with tab2:
        st.markdown("#### WBD Film Slate — 25 Titles")
        c1, c2, c3 = st.columns(3)
        cat_f    = c1.selectbox("Category", ["All","Theatrical","Library","HBO Original"], key="mv_cat")
        genre_f  = c2.selectbox("Genre",    ["All","Action","Drama","Comedy","Sci-Fi","Fantasy","Thriller","Historical","Animation"], key="mv_genre")
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
                       AND mr.term_to <= DATE('now','+90 days') THEN 1 ELSE 0 END) AS expiring_90d
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
                (f"{len(mv_df)}",                                       "Films",           "#0f172a"),
                (f"${mv_df['box_office_gross_usd_m'].sum()/1000:.1f}B","Total Box Office","#1e40af"),
                (f"{int(mv_df['active_rights'].sum())}",               "Active Rights",   "#166534"),
                (f"{int(mv_df['expiring_90d'].sum())}",                "Expiring 90d",    "#991b1b"),
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
                    movies=('movie_id','count'), box_office=('box_office_gross_usd_m','sum')
                ).reset_index()
                st.markdown("#### Franchise Box Office")
                st.plotly_chart(bar(fr_grp,'franchise','box_office','Franchise Box Office ($M)',h=260), use_container_width=True)

            st.markdown("#### Film Slate")
            mv_show = mv_df.copy()
            mv_show['⚠ Expiring'] = mv_show['expiring_90d'].apply(lambda x: f"🔴 {int(x)}" if x and int(x)>0 else "—")
            mv_show['Rights']     = mv_show['active_rights'].apply(lambda x: "✅" if x and int(x)>0 else "❌")
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
                   SUM(CASE WHEN mr.term_to <= DATE('now','+90 days') AND mr.status='Active' THEN 1 ELSE 0 END) AS expiring_soon,
                   CASE WHEN d.dna_id IS NOT NULL THEN '🚫' ELSE '✅' END AS dna_status
            FROM title t
            LEFT JOIN media_rights mr ON t.title_id = mr.title_id
            LEFT JOIN do_not_air d ON t.title_id = d.title_id AND d.active=1
            WHERE UPPER(t.region)='{reg}'
            GROUP BY t.title_id ORDER BY rights_count DESC NULLS LAST LIMIT 100
        """)
        if not df.empty:
            df['⚠ Expiring'] = df['expiring_soon'].apply(lambda x: f"🔴 {int(x)}" if x and int(x)>0 else "—")
            st.dataframe(
                df[['title_name','genre','controlling_entity','rights_count',
                    'platforms','exclusive','active_rights','⚠ Expiring','dna_status']],
                use_container_width=True, hide_index=True)

    with tab3:
        search_q = st.text_input("Search title name", placeholder="e.g. House of the Dragon, Succession…", key="title_search")
        if search_q:
            df = run(f"""
                SELECT t.title_id, t.title_name, t.title_type, t.genre,
                       t.release_year, t.controlling_entity, t.age_rating,
                       s.series_title, se.season_number, t.episode_number,
                       COUNT(DISTINCT mr.rights_id) AS rights_count
                FROM title t
                LEFT JOIN season se ON t.season_id = se.season_id
                LEFT JOIN series s  ON t.series_id = s.series_id
                LEFT JOIN media_rights mr ON t.title_id = mr.title_id
                WHERE LOWER(t.title_name) LIKE '%{search_q.lower()}%'
                GROUP BY t.title_id ORDER BY s.series_title, se.season_number, t.episode_number
                LIMIT 100
            """)
            if not df.empty:
                st.caption(f"{len(df)} results")
                st.dataframe(df, use_container_width=True, hide_index=True)
            else:
                st.warning("No titles found.")


# ════════════════════════════════════════════════════════════════════════════════
# DO-NOT-AIR
# ════════════════════════════════════════════════════════════════════════════════
def page_dna():
    reg = st.session_state.current_region
    st.markdown('<div class="page-header">🚫 Do-Not-Air Restrictions</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">Active DNA flags — titles that cannot be aired in certain territories/media due to content or rights reasons · {reg}</div>', unsafe_allow_html=True)

    kpi = run(f"""
        SELECT COUNT(*) AS total, COUNT(DISTINCT title_id) AS titles,
               COUNT(DISTINCT reason_category) AS categories
        FROM do_not_air WHERE UPPER(region)='{reg}' AND active=1
    """)
    if not kpi.empty:
        r = kpi.iloc[0]
        stat_tiles([
            (f"{int(r.get('total',0)):,}",      "Active DNA Records", "#991b1b"),
            (f"{int(r.get('titles',0)):,}",     "Affected Titles",    "#92400e"),
            (f"{int(r.get('categories',0)):,}", "Restriction Types",  "#64748b"),
        ])

    st.divider()
    tab1, tab2 = st.tabs(["📊 Analysis", "📄 DNA Table"])

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

    with tab3:
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


# ════════════════════════════════════════════════════════════════════════════════
# SALES DEALS (rights-out)
# ════════════════════════════════════════════════════════════════════════════════
def page_sales():
    reg = st.session_state.current_region
    st.markdown('<div class="page-header">💸 Sales Deals — Rights Out</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">Affiliate &amp; 3rd-party sales of content rights · {reg}</div>', unsafe_allow_html=True)

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
            (f"{int(r.get('total',0)):,}",  "Total Sales Deals", "#0f172a"),
            (f"{int(r.get('active',0)):,}", "Active",            "#166534"),
            (fmt_m(r.get('total_value',0)), "Total Value",       "#1e40af"),
            (f"{int(r.get('buyers',0)):,}", "Buyers",            "#5b21b6"),
            (f"{int(r.get('titles',0)):,}", "Titles Sold",       "#92400e"),
        ])

    st.divider()
    tab1, tab2 = st.tabs(["📊 Analytics", "📄 Deal Table"])

    with tab1:
        c1, c2 = st.columns(2)
        with c1:
            df = run(f"""
                SELECT buyer, COUNT(*) AS deals, SUM(deal_value) AS total_value
                FROM sales_deal WHERE UPPER(region)='{reg}' AND status='Active'
                GROUP BY buyer ORDER BY total_value DESC LIMIT 12
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
        st_f   = f1.selectbox("Status", ["All","Active","Expired"], key="sd_st_f")
        dt_f   = f2.selectbox("Type",   ["All","Affiliate Sales","3rd Party Sales"], key="sd_dt_f")
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


# ════════════════════════════════════════════════════════════════════════════════
# WORK ORDERS
# ════════════════════════════════════════════════════════════════════════════════
def page_work_orders():
    reg = st.session_state.current_region
    st.markdown('<div class="page-header">⚙️ Work Orders</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">Operational pipeline — vendor tasks, QC, localization, encoding · {reg}</div>', unsafe_allow_html=True)

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
            (f"{int(r.get('total',0)):,}",       "Total Orders",  "#0f172a"),
            (f"{int(r.get('in_progress',0)):,}",  "In Progress",   "#1e40af"),
            (f"{int(r.get('delayed',0)):,}",      "Delayed",       "#991b1b"),
            (f"{int(r.get('completed',0)):,}",    "Completed",     "#166534"),
            (f"{r.get('avg_quality',0):.1f}",     "Avg Quality",   "#5b21b6"),
            (fmt_m(r.get('total_cost',0)),        "Total Cost",    "#92400e"),
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


# ════════════════════════════════════════════════════════════════════════════════
# CHAT / QUERY — Rights Explorer conversational interface
# ════════════════════════════════════════════════════════════════════════════════
def page_chat():
    reg = st.session_state.current_region
    st.markdown('<div class="page-header">💬 Chat Query — Rights Explorer</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">Natural language rights interrogation · {reg} · Ask about titles, rights windows, expiry, DNA, territories, exclusivity</div>', unsafe_allow_html=True)


    # Suggested queries — using st.container instead of expander to avoid _arrow artifact
    if not st.session_state.chat_history:
        st.markdown("""
        <div style="background:#fff;border:1px solid #e2e8f0;border-radius:12px;
             padding:14px 16px;margin-bottom:12px">
          <div style="font-size:.8rem;font-weight:700;color:#6366f1;
               text-transform:uppercase;letter-spacing:.06em;margin-bottom:10px">
            💡 Sample queries by intent type
          </div>""", unsafe_allow_html=True)

        groups = {
            "What titles do we have": [
                "What titles do we have in APAC",
                "Count titles by genre",
                "Show me episodes of House of the Dragon",
            ],
            "Titles we have rights to": [
                "What titles do we have SVOD rights to",
                "Show titles with exclusive PayTV rights",
                "Count titles with active STB-VOD rights",
                "List titles by season with rights",
            ],
            "Rights for specific title": [
                'What rights do we hold for "Succession"',
                'Show all rights for "The Last of Us"',
            ],
            "Expiry alerts": [
                "Show SVOD rights expiring in 30 days",
                "Rights expiring in next 90 days",
                "PayTV rights expiring soon",
            ],
            "Movies & Films": [
                "Show all movies in the slate",
                "Movies by box office revenue",
                "Theatrical movies with active rights",
                "Movies breakdown by genre",
            ],
            "Do-Not-Air / Deals": [
                "Show do-not-air restrictions",
                "Active deals by vendor",
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
                    st.rerun()

        st.markdown('</div>', unsafe_allow_html=True)

    show_sql = st.toggle("Show SQL", value=st.session_state.user_prefs.get('show_sql', True), key="sql_tog")
    st.session_state.user_prefs['show_sql'] = show_sql

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
                            except: pass

                        # Chart
                        fig = None
                        if chart_type == 'bar' and len(res_df.columns) >= 2:
                            x_col = res_df.columns[0]
                            y_col = next((c for c in res_df.columns[1:]
                                          if pd.api.types.is_numeric_dtype(res_df[c])), res_df.columns[1])
                            try: res_df[y_col] = pd.to_numeric(res_df[y_col], errors='coerce')
                            except: pass
                            fig = bar(res_df.head(30), x_col, y_col, active_prompt[:60])
                        elif chart_type == 'pie' and len(res_df.columns) >= 2:
                            fig = pie(res_df, res_df.columns[0], res_df.columns[1], active_prompt[:60])

                        if fig:
                            st.plotly_chart(fig, use_container_width=True)

                        answer_txt = (f"📊 **{len(res_df):,} records** for **{region_ctx}**."
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


# ════════════════════════════════════════════════════════════════════════════════
# DEALS PAGE (original deals table)
# ════════════════════════════════════════════════════════════════════════════════
def page_deals():
    reg = st.session_state.current_region
    st.markdown('<div class="page-header">💼 Deals</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">Vendor licensing &amp; distribution deals · {reg}</div>', unsafe_allow_html=True)

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
            (f"{int(r.get('total',0)):,}",          "Total Deals",      "#0f172a"),
            (fmt_m(r.get('total_value', 0)),         "Total Value",      "#1e40af"),
            (fmt_m(r.get('avg_value', 0)),           "Avg Deal Value",   "#5b21b6"),
            (f"{int(r.get('active',0)):,}",          "Active",           "#166534"),
            (f"{int(r.get('expired',0)):,}",         "Expired",          "#64748b"),
            (f"{int(r.get('pending',0)):,}",         "Pending / Neg.",   "#92400e"),
            (f"{int(r.get('overdue_payments',0)):,}","Overdue Payments", "#991b1b"),
        ])

    st.divider()
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "🏢 By Vendor", "📋 Deal Types", "📄 Deal Table"])

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
        st_f  = f1.selectbox("Status",       ["All","Active","Expired","Pending","Under Negotiation","Terminated"], key="dl_st")
        dt_f  = f2.selectbox("Deal Type",    ["All","Output Deal","Library Buy","First-Look Deal",
                                               "Co-Production","Licensing Agreement","Distribution Deal",
                                               "Volume Deal","Format Deal"], key="dl_dt")
        pay_f = f3.selectbox("Payment",      ["All","Paid","Pending","Invoiced","Overdue","Partially Paid"], key="dl_pay")

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
            df['⏰ Expiry'] = df['expiry_date'].apply(
                lambda d: exp_tag((datetime.strptime(d, "%Y-%m-%d") - datetime.now()).days)
                if d else "—")
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


# ════════════════════════════════════════════════════════════════════════════════
# VENDORS PAGE (original vendors table — restored columns)
# ════════════════════════════════════════════════════════════════════════════════
def page_vendors():
    reg = st.session_state.current_region
    st.markdown('<div class="page-header">🏢 Vendors</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="page-sub">Vendor performance, spend &amp; quality · {reg}</div>', unsafe_allow_html=True)

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
            (f"{int(r.get('total',0)):,}",         "Total Vendors",   "#0f172a"),
            (f"{r.get('avg_rating',0):.2f} ⭐",    "Avg Rating",      "#92400e"),
            (fmt_m(r.get('total_spend', 0)),        "Total Spend",     "#1e40af"),
            (f"{int(r.get('active_vendors',0)):,}", "Active",          "#166534"),
            (f"{int(r.get('vendor_types',0)):,}",  "Vendor Types",    "#5b21b6"),
        ])

    st.divider()
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Performance", "💰 Spend", "⭐ Quality", "📄 Vendor List"])

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
            with c2: st.plotly_chart(bar(df, 'vendor_name', 'total_rework', 'Total Rework Count', h=280), use_container_width=True)

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


# ── Router ─────────────────────────────────────────────────────────────────────
{
    "rights":      page_rights,
    "titles":      page_titles,
    "dna":         page_dna,
    "sales":       page_sales,
    "deals":       page_deals,
    "vendors":     page_vendors,
    "work_orders": page_work_orders,
    "chat":        page_chat,
}.get(st.session_state.page, page_rights)()

"""
Foundry Vantage — Media Supply Chain Intelligence
Streamlit frontend, fixed edition.

Fixes applied vs original:
  1.  Removed unconditional st.rerun() after history append — Streamlit reruns automatically
  2.  Stable widget keys — no more time.time() or dynamic region key
  3.  Tableau live-response keys use history length so they never collide with history items
  4.  Region from selectbox only updates session state when value actually changes
  5.  Chat-detected region stored before selectbox renders, selectbox reads from state
  6.  DB init wrapped in try/except — error shown in UI, not silently cached
  7.  Chat history stores capped DataFrame (500 rows) not the full result set
  8.  Metrics detect any numeric column, not just deal_value/total_value
  9.  Chart types extended: bar, line, pie, scatter
 10.  pending_prompt / active_prompt pattern simplified with on_click callback
 11.  HTML/Jinja2 file removed — it was dead code incompatible with Streamlit
 12.  SQL shown in expander for transparency; parse_query contract documented
"""

import time
import traceback

import pandas as pd
import plotly.express as px
import streamlit as st

# ── These three utils must exist in your project. Contracts documented below. ──
from utils.database import init_database, execute_sql  # type: ignore
from utils.query_parser import parse_query              # type: ignore
from utils.tableau_sync import trigger_tableau_report   # type: ignore

# ─────────────────────────────────────────────────────────────────────────────
# parse_query(prompt: str, region: str) -> tuple[str, str | None, str]
#   Returns (sql, error_message_or_None, chart_type)
#   chart_type must be one of: "bar" | "line" | "pie" | "scatter"
#   sql MUST use parameterised placeholders, never f-string user input directly.
#
# execute_sql(sql: str, conn) -> tuple[pd.DataFrame | None, str | None]
#   Returns (dataframe_or_None, error_message_or_None)
#
# trigger_tableau_report(df: pd.DataFrame, name: str) -> tuple[bool, str]
#   Returns (success, info_message)
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Foundry Vantage",
    page_icon="🎥",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS — keep Streamlit's base, layer Vantage identity on top ─────────
st.markdown("""
<style>
    /* Brand colours */
    :root {
        --vantage-navy:  #091E42;
        --vantage-blue:  #0052CC;
        --vantage-teal:  #00B8D9;
        --vantage-bg:    #F4F5F7;
        --vantage-text:  #172B4D;
    }

    /* Sidebar */
    [data-testid="stSidebar"] { background: var(--vantage-navy); }
    [data-testid="stSidebar"] * { color: white !important; }
    [data-testid="stSidebar"] .stSelectbox label { color: #8993A4 !important; font-size: 0.75rem; }

    /* Suggestion chips */
    .stButton > button {
        border-radius: 20px !important;
        border: 1px solid var(--vantage-blue) !important;
        color: var(--vantage-blue) !important;
        background: white !important;
        font-size: 0.82rem !important;
        padding: 4px 14px !important;
        margin: 2px 0 !important;
        transition: all 0.15s !important;
    }
    .stButton > button:hover {
        background: var(--vantage-blue) !important;
        color: white !important;
    }

    /* Chat input */
    [data-testid="stChatInput"] textarea {
        border-radius: 24px !important;
        border: 1px solid #DFE1E6 !important;
        padding: 12px 20px !important;
    }

    /* Page header */
    h1 { color: var(--vantage-text); font-size: 1.5rem !important; font-weight: 600 !important; }

    /* Metric cards */
    [data-testid="stMetric"] {
        background: white;
        border: 1px solid #DFE1E6;
        border-radius: 8px;
        padding: 12px 16px;
    }

    /* Dataframe */
    [data-testid="stDataFrame"] { border-radius: 8px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# 1. SESSION STATE INITIALISATION
# ══════════════════════════════════════════════════════════════════════════════
REGIONS  = ["NA", "APAC", "EMEA", "LATAM"]
PERSONAS = ["Leadership", "Product", "Operations", "Finance"]
MAX_HISTORY_ROWS = 500   # cap stored per message to avoid memory growth

def _init_state():
    defaults = {
        "chat_history":     [],
        "current_region":   "APAC",
        "persona":          "Product",
        "db_conn":          None,
        "db_error":         None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

_init_state()


# ══════════════════════════════════════════════════════════════════════════════
# 2. DATABASE CONNECTION  (cached, with visible error — fix #6)
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_resource(show_spinner="Connecting to database…")
def _get_db():
    """
    Cache the connection object once per process.
    Errors are surfaced to the UI rather than silently cached.
    """
    try:
        return init_database(), None
    except Exception as exc:
        return None, str(exc)

_db_conn, _db_err = _get_db()
if _db_err:
    # Store error in session state so it can be shown in main area too
    st.session_state.db_error = _db_err
else:
    st.session_state.db_conn = _db_conn


# ══════════════════════════════════════════════════════════════════════════════
# 3. SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
PERSONA_PROMPTS = {
    "Leadership":  lambda r: [f"Top vendors in {r}", f"Market value overview for {r}"],
    "Product":     lambda r: [f"Show SVOD rights in {r}", f"Rights scope breakdown {r}"],
    "Operations":  lambda r: [f"Work order status {r}", f"Delayed tasks {r}"],
    "Finance":     lambda r: [f"Total spend per vendor in {r}", f"Highest cost deals {r}"],
}

def _on_suggestion_click(prompt: str):
    """on_click callback — sets prompt directly, no pending_prompt needed (fix #10)."""
    st.session_state["_active_prompt"] = prompt

with st.sidebar:
    st.markdown("## 🎥 Foundry Vantage")
    st.divider()

    # Region selector — fixed key, only writes to state on actual change (fix #3, #4, #5)
    selected_region = st.selectbox(
        "Market Region",
        REGIONS,
        index=REGIONS.index(st.session_state.current_region),
        key="region_select",          # FIXED key — never changes
    )
    if selected_region != st.session_state.current_region:
        st.session_state.current_region = selected_region

    # Persona selector — fixed key
    selected_persona = st.selectbox(
        "View Persona",
        PERSONAS,
        index=PERSONAS.index(st.session_state.persona),
        key="persona_select",
    )
    if selected_persona != st.session_state.persona:
        st.session_state.persona = selected_persona

    st.divider()
    st.subheader(f"💡 {st.session_state.persona} Queries")

    suggestions = PERSONA_PROMPTS[st.session_state.persona](st.session_state.current_region)
    for idx, sug in enumerate(suggestions):
        st.button(
            sug,
            key=f"sug_{idx}_{st.session_state.persona}_{st.session_state.current_region}",
            on_click=_on_suggestion_click,
            args=(sug,),
            use_container_width=True,
        )

    st.divider()
    # DB status indicator
    if st.session_state.db_error:
        st.error(f"DB Offline: {st.session_state.db_error}")
    else:
        st.success("● Snowflake Connected", icon=None)


# ══════════════════════════════════════════════════════════════════════════════
# 4. RESOLVE ACTIVE PROMPT
#    Priority: suggestion button click > chat input
#    No more pending_prompt bounce — callbacks write directly (fix #10)
# ══════════════════════════════════════════════════════════════════════════════
chat_input = st.chat_input(
    "Ask about deals, vendors, or work orders…",
    disabled=bool(st.session_state.db_error),
)

# Drain the active prompt set by suggestion button callback
active_prompt: str | None = st.session_state.pop("_active_prompt", None) or chat_input

# Auto-detect region from prompt text — runs BEFORE any widget re-render (fix #5)
if active_prompt:
    for r in REGIONS:
        if r.lower() in active_prompt.lower():
            if r != st.session_state.current_region:
                st.session_state.current_region = r
            break


# ══════════════════════════════════════════════════════════════════════════════
# 5. HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def _build_chart(df: pd.DataFrame, chart_type: str, region: str):
    """
    Build a Plotly figure. Supports bar, line, pie, scatter (fix #9).
    Falls back to bar if chart_type is unrecognised.
    """
    x_col = df.columns[0]
    y_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
    title = f"{chart_type.capitalize()} — {region}"

    if chart_type == "pie":
        return px.pie(df, names=x_col, values=y_col, title=title, hole=0.4)
    if chart_type == "line":
        return px.line(df, x=x_col, y=y_col, title=title, markers=True)
    if chart_type == "scatter":
        return px.scatter(df, x=x_col, y=y_col, title=title, color=x_col)
    # default — bar
    return px.bar(df, x=x_col, y=y_col, title=title, color=x_col)


def _find_numeric_cols(df: pd.DataFrame) -> list[str]:
    """Return names of numeric columns — used for flexible metric detection (fix #8)."""
    return [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]


def _render_metrics(df: pd.DataFrame):
    """
    Render summary metric cards for any numeric column found in df (fix #8).
    Original only checked for 'deal_value' / 'total_value' by name.
    """
    num_cols = _find_numeric_cols(df)
    if not num_cols:
        return None

    # Show at most 4 metric columns
    display_cols = num_cols[:4]
    metric_data  = []
    cols         = st.columns(len(display_cols))

    for col_widget, col_name in zip(cols, display_cols):
        total = df[col_name].sum()
        avg   = df[col_name].mean()
        label = col_name.replace("_", " ").title()

        # Format: use $ prefix for likely financial columns
        financial_keywords = {"value", "spend", "cost", "budget", "revenue", "deal", "amount"}
        is_financial = any(kw in col_name.lower() for kw in financial_keywords)
        fmt = lambda v: f"${v:,.0f}" if is_financial else f"{v:,.2f}"  # noqa: E731

        col_widget.metric(f"Total {label}", fmt(total), delta=f"avg {fmt(avg)}")
        metric_data.append({"label": f"Total {label}", "value": fmt(total), "avg": fmt(avg)})

    return metric_data


def _render_tableau_actions(df: pd.DataFrame, key_suffix: str, region: str):
    """Render Tableau push UI. key_suffix must be unique per call site (fix #4)."""
    st.markdown("---")
    st.caption("📊 Enterprise Actions")
    tc1, tc2 = st.columns([3, 1])
    tab_name = tc1.text_input(
        "Tableau Report Name",
        value=f"Foundry_{region}_{key_suffix}",
        key=f"tab_name_{key_suffix}",
        label_visibility="collapsed",
    )
    if tc2.button("🚀 Push to Tableau", key=f"tab_push_{key_suffix}", use_container_width=True):
        success, info = trigger_tableau_report(df, tab_name)
        st.success("Pushed successfully!") if success else st.error(info)


def _safe_store_df(df: pd.DataFrame) -> pd.DataFrame:
    """Cap stored DataFrame to MAX_HISTORY_ROWS to prevent memory growth (fix #7)."""
    return df.head(MAX_HISTORY_ROWS).copy()


# ══════════════════════════════════════════════════════════════════════════════
# 6. PAGE HEADER
# ══════════════════════════════════════════════════════════════════════════════
region_badge = f"**Region: {st.session_state.current_region}**"
persona_badge = f"**Persona: {st.session_state.persona}**"
header_col1, header_col2 = st.columns([3, 1])
header_col1.title(f"🔍 {st.session_state.persona} Insights")
header_col2.markdown(f"<div style='text-align:right;padding-top:14px;color:#64748b;font-size:0.85rem'>{region_badge} · {persona_badge}</div>", unsafe_allow_html=True)

# Show DB error banner prominently if connection failed
if st.session_state.db_error:
    st.error(f"⚠️ Database connection failed: {st.session_state.db_error}\n\nCheck your `utils/database.py` configuration.")
    st.stop()


# ══════════════════════════════════════════════════════════════════════════════
# 7. RENDER CHAT HISTORY  (fix #2 — all keys are stable and indexed)
# ══════════════════════════════════════════════════════════════════════════════
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"):
        st.write(msg["question"])

    with st.chat_message("assistant", avatar="🎥"):
        st.write(msg["answer"])

        # Re-render stored metrics
        if msg.get("metrics"):
            metric_cols = st.columns(min(len(msg["metrics"]), 4))
            for mc, m in zip(metric_cols, msg["metrics"]):
                mc.metric(m["label"], m["value"], delta=f"avg {m.get('avg', '')}")

        # Re-render chart stored in history
        if msg.get("chart") is not None:
            st.plotly_chart(msg["chart"], use_container_width=True, key=f"hist_chart_{i}")

        # Tableau actions — history item uses index as suffix (fix #4)
        _render_tableau_actions(msg["data"], key_suffix=f"hist_{i}", region=st.session_state.current_region)

        with st.expander("View Records", expanded=False):
            st.dataframe(msg["data"], use_container_width=True, key=f"hist_df_{i}")

        if msg.get("sql"):
            with st.expander("View SQL", expanded=False):
                st.code(msg["sql"], language="sql")


# ══════════════════════════════════════════════════════════════════════════════
# 8. PROCESS NEW QUERY
# ══════════════════════════════════════════════════════════════════════════════
if active_prompt:
    active_region = st.session_state.current_region  # snapshot — stable for this run

    with st.chat_message("user"):
        st.write(active_prompt)

    with st.chat_message("assistant", avatar="🎥"):
        with st.spinner(f"Querying {active_region}…"):
            try:
                sql, parse_error, chart_type = parse_query(active_prompt, active_region)
            except Exception:
                sql, parse_error, chart_type = None, traceback.format_exc(), "bar"

        if parse_error:
            st.error(f"Could not generate query: {parse_error}")

        elif sql:
            res_df, db_err = execute_sql(sql, st.session_state.db_conn)

            if db_err:
                st.error(f"Database error: {db_err}")

            elif res_df is None or res_df.empty:
                st.warning(f"No records found for **{active_prompt}** in **{active_region}**.")

            else:
                # Answer header
                answer_text = f"Displaying **{active_region}** data for: *{active_prompt}*"
                st.write(answer_text)

                # Metrics — any numeric column (fix #8)
                metric_data = _render_metrics(res_df)

                # Chart — supports bar/line/pie/scatter (fix #9)
                fig = _build_chart(res_df, chart_type or "bar", active_region)

                # Stable key using history length — not time.time() (fix #2)
                chart_key = f"new_chart_{len(st.session_state.chat_history)}"
                st.plotly_chart(fig, use_container_width=True, key=chart_key)

                # SQL transparency
                with st.expander("View SQL", expanded=False):
                    st.code(sql, language="sql")

                # Tableau — live response uses history length as suffix (fix #4)
                live_suffix = f"live_{len(st.session_state.chat_history)}"
                _render_tableau_actions(res_df, key_suffix=live_suffix, region=active_region)

                # Dataset explorer
                with st.expander("Explore Dataset", expanded=False):
                    st.dataframe(res_df, use_container_width=True)

                # Append to history — cap stored df (fix #7)
                st.session_state.chat_history.append({
                    "question": active_prompt,
                    "answer":   answer_text,
                    "data":     _safe_store_df(res_df),   # capped copy
                    "chart":    fig,
                    "metrics":  metric_data,
                    "sql":      sql,
                })

                # ── NO st.rerun() here (fix #1) ──
                # Streamlit automatically reruns after state mutation.
                # Adding st.rerun() would cause a double-render loop.

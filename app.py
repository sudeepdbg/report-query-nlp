import streamlit as st
import pandas as pd
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

# Import specialized modules
from utils.database import init_database, execute_sql
from utils.query_parser import parse_query

# 1. Page Configuration
st.set_page_config(
    page_title="Foundry Vantage",
    page_icon="🎥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 2. Database Connection (Cached)
@st.cache_resource
def get_database_connection():
    return init_database()

DB_CONN = get_database_connection()

# 3. Session State Management
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if 'current_region' not in st.session_state:
    st.session_state.current_region = 'NA'
if 'current_team' not in st.session_state:
    st.session_state.current_team = 'leadership'
if 'active_dashboard' not in st.session_state:
    st.session_state.active_dashboard = 'executive'

# 4. Expert UI Styling
st.markdown("""
<style>
    .stChatMessage { border-radius: 10px; margin-bottom: 10px; }
    .sql-code-block {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 5px;
        border-left: 5px solid #3b82f6;
        font-family: 'Source Code Pro', monospace;
        font-size: 0.85rem;
    }
    .data-table-container { margin-top: 15px; border: 1px solid #e6e9ef; border-radius: 5px; }
</style>
""", unsafe_allow_html=True)

# 5. Helper Functions
def get_dashboard_data(name):
    """Fetches high-level metrics for the sidebar overview."""
    queries = {
        'executive': "SELECT status, COUNT(*) as count FROM content_planning GROUP BY status",
        'workorders': "SELECT status, COUNT(*) as count FROM work_orders GROUP BY status",
        'deals': "SELECT vendor, SUM(deal_value) as total FROM deals GROUP BY vendor"
    }
    
    sql = queries.get(name)
    if not sql: return None
    
    df, error = execute_sql(sql, DB_CONN)
    if error or df.empty: return None
    
    return df

def auto_chart(df):
    """Smart charting logic based on data shape and content."""
    if df.shape[1] < 2: return None
    
    # Identify columns
    cat_col = df.columns[0]
    num_col = df.columns[1]
    
    # Use Pie for distributions under 6 items, Bar for others
    if len(df) <= 6:
        return px.pie(df, names=cat_col, values=num_col, title=f"{cat_col} Distribution", hole=0.4)
    else:
        return px.bar(df, x=cat_col, y=num_col, title=f"{cat_col} Breakdown", color=num_col)

# ------------------------------------------------------------------
# SIDEBAR: Context & Navigation
# ------------------------------------------------------------------
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.caption("Intelligent Media Supply Chain Assistant")
    st.divider()
    
    # Context Settings
    st.subheader("⚙️ User Context")
    st.session_state.current_region = st.selectbox("Market Region", ["NA", "APAC", "EMEA", "LATAM"])
    st.session_state.current_team = st.selectbox("Functional Team", ["leadership", "product", "content planning", "deals"])
    
    st.divider()
    
    # Dashboard Discovery
    st.subheader("📊 Active Insight Layer")
    dashboards = {
        "Executive Content": "executive",
        "Work Order Tracker": "workorders",
        "Deals Performance": "deals"
    }
    selected_name = st.radio("Switch Dashboard Context", options=list(dashboards.keys()))
    st.session_state.active_dashboard = dashboards[selected_name]
    
    # Render Sidebar Mini-Chart
    df_mini = get_dashboard_data(st.session_state.active_dashboard)
    if df_mini is not None:
        fig_mini = auto_chart(df_mini)
        st.plotly_chart(fig_mini, use_container_width=True, config={'displayModeBar': False})

# ------------------------------------------------------------------
# MAIN AREA: Conversational Interface
# ------------------------------------------------------------------
st.title("🔍 Ask Foundry Vantage")
st.info(f"Currently filtering for **{st.session_state.current_region}** market via the **{st.session_state.current_team}** lens.")

# Display Chat History using Native Components
for msg in st.session_state.chat_history:
    with st.chat_message("user"):
        st.write(msg["question"])
    
    with st.chat_message("assistant", avatar="🎥"):
        st.write(msg["answer"])
        if msg.get('sql'):
            with st.expander("View Generated Logic"):
                st.code(msg['sql'], language="sql")
        if msg.get('data') is not None:
            st.dataframe(msg['data'], use_container_width=True)
        if msg.get('chart'):
            st.plotly_chart(msg['chart'], use_container_width=True)

# Chat Input
if prompt := st.chat_input("How many work orders are delayed in APAC?"):
    # 1. Update UI immediately
    st.session_state.chat_history.append({"question": prompt, "answer": "Processing..."})
    
    # 2. Logic Execution
    with st.spinner("Translating natural language to data query..."):
        sql, error = parse_query(
            prompt, 
            st.session_state.current_region,
            st.session_state.current_team,
            st.session_state.active_dashboard
        )
        
        if error:
            final_answer = f"⚠️ {error}"
            res_df, res_chart, res_sql = None, None, None
        else:
            df_res, exec_error = execute_sql(sql, DB_CONN)
            if exec_error:
                final_answer = f"I encountered a technical error: {exec_error}"
                res_df, res_chart, res_sql = None, None, sql
            elif df_res.empty:
                final_answer = "I couldn't find any data matching those specific criteria."
                res_df, res_chart, res_sql = None, None, sql
            else:
                # Deterministic Answer Generation
                if len(df_res) == 1 and df_res.shape[1] == 1:
                    final_answer = f"The answer is **{df_res.iloc[0,0]}**."
                    res_df, res_chart = None, None
                else:
                    final_answer = f"I've retrieved **{len(df_res)}** records based on your request:"
                    res_df = df_res
                    res_chart = auto_chart(df_res)
                res_sql = sql

        # 3. Update History with Real Data
        st.session_state.chat_history[-1] = {
            'question': prompt,
            'answer': final_answer,
            'sql': res_sql,
            'data': res_df,
            'chart': res_chart
        }
        st.rerun()

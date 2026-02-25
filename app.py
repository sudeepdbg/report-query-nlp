import streamlit as st
import pandas as pd
import plotly.express as px
import streamlit.components.v1 as components
import time
from utils.database import init_database, execute_sql
from utils.query_parser import parse_query

st.set_page_config(page_title="Foundry Vantage", layout="wide")

@st.cache_resource
def get_db(): return init_database()
DB_CONN = get_db()

# 1. ENHANCED STATE MANAGEMENT
if 'current_region' not in st.session_state:
    st.session_state.current_region = 'NA'
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []

def sync_region_from_query(query_text):
    """Overrides global state if a specific region is mentioned in the query."""
    regions = ["NA", "APAC", "EMEA", "LATAM"]
    for r in regions:
        if r in query_text.upper():
            st.session_state.current_region = r
            return r
    return st.session_state.current_region

# 2. PRE-PROCESS INPUT (The Override)
user_input = st.chat_input("Search...")
# Check if a sidebar button was clicked or text was entered
active_q = st.session_state.get('active_query') or user_input

if active_q:
    # Force the global region to match the query content immediately
    sync_region_from_query(active_q)

# 3. SIDEBAR (Now guaranteed to be in sync)
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.caption("v4.0 State-Sync Active")
    
    region_options = ["NA", "APAC", "EMEA", "LATAM"]
    # Force the dropdown to follow the session state
    current_idx = region_options.index(st.session_state.current_region)
    
    selected_reg = st.selectbox(
        "Market Region", 
        region_options, 
        index=current_idx,
        key=f"reg_widget_{st.session_state.current_region}" # Key change forces redraw
    )
    
    if selected_reg != st.session_state.current_region:
        st.session_state.current_region = selected_reg
        st.rerun()

    persona = st.selectbox("View Persona", ["Leadership", "Product", "Operations", "Finance"])
    
    st.divider()
    st.subheader("💡 Suggestions")
    prompts = {
        "Leadership": ["Top vendors", "Market value", "Performance"],
        "Product": ["Content readiness", "SVOD rights", "Inventory"],
        "Operations": ["Delayed tasks", "Work orders", "Performance"],
        "Finance": ["Total spend", "Highest cost", "Deal value"]
    }
    
    for i, sug in enumerate(prompts.get(persona, [])):
        # Button label includes the current region for clarity
        if st.button(f"{sug} in {st.session_state.current_region}", use_container_width=True, key=f"sug_{i}"):
            st.session_state.active_query = f"{sug} in {st.session_state.current_region}"
            st.rerun()

# 4. MAIN INTERFACE
st.title(f"🔍 {persona} Intelligence: {st.session_state.current_region}")

# Render History
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"): st.write(msg["q"])
    with st.chat_message("assistant"):
        st.plotly_chart(msg["fig"], use_container_width=True, key=f"hist_fig_{i}")
        with st.expander("View Detailed Records"):
            st.dataframe(msg["full_df"], use_container_width=True)

# 5. EXECUTION LOGIC
if active_q:
    if 'active_query' in st.session_state: del st.session_state.active_query
        
    # Always use the synced session state region
    sql, err, c_type = parse_query(active_q, st.session_state.current_region)
    chart_df, _ = execute_sql(sql, DB_CONN)

    if chart_df is not None and not chart_df.empty:
        # Determine table for "Full Records"
        target_table = "deals"
        if any(x in active_q.lower() for x in ["task", "order", "performance", "delay"]):
            target_table = "work_orders"
        elif any(x in active_q.lower() for x in ["ready", "inventory", "status"]):
            target_table = "content_planning"

        # The Table Query now uses the OVERRIDDEN region
        full_sql = f"SELECT * FROM {target_table} WHERE UPPER(region) = '{st.session_state.current_region.upper()}'"
        full_df, _ = execute_sql(full_sql, DB_CONN)

        with st.chat_message("assistant"):
            label, val = chart_df.columns[0], chart_df.columns[1]
            if c_type == "pie":
                fig = px.pie(chart_df, names=label, values=val, hole=0.4, 
                             color_discrete_sequence=px.colors.qualitative.Pastel)
            else:
                fig = px.bar(chart_df, x=label, y=val, color=label, template="plotly_white")
            
            st.plotly_chart(fig, use_container_width=True, key=f"new_{time.time()}")
            
            st.subheader("Explore Data Source")
            st.dataframe(full_df, use_container_width=True)
            
            st.session_state.chat_history.append({"q": active_q, "fig": fig, "full_df": full_df})
            st.rerun()

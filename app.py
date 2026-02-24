import streamlit as st
import pandas as pd
import plotly.express as px
from utils.database import init_database, execute_sql
from utils.query_parser import parse_query

# Page config & UI Setup
st.set_page_config(page_title="Foundry Vantage", page_icon="🎥", layout="wide")

@st.cache_resource
def get_db():
    return init_database()

DB_CONN = get_db()

# Session State for persistence
if 'chat_history' not in st.session_state: st.session_state.chat_history = []
if 'current_region' not in st.session_state: st.session_state.current_region = 'APAC'

# --- Sidebar Configuration ---
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.caption("Intelligence Layer for Media Supply Chain")
    st.divider()
    
    # 1. DYNAMIC MARKET SELECTION
    # Indexing ensures the UI reflects the current session state
    market_options = ["NA", "APAC", "EMEA", "LATAM"]
    selected_index = market_options.index(st.session_state.current_region)
    
    st.session_state.current_region = st.selectbox(
        "Market Region", 
        market_options,
        index=selected_index
    )
    
    st.radio("Active Insight Layer", ["Executive Content", "Work Order Tracker", "Deals Performance"])

    st.divider()
    st.subheader("💡 Suggested for " + st.session_state.current_region)
    
    # 2. DYNAMIC SUGGESTIONS BASED ON MARKET
    # These strings now inject the active region dynamically
    sug_1 = f"Show active deals in {st.session_state.current_region}"
    sug_2 = f"List content status for MAX {st.session_state.current_region}"
    
    if st.button(sug_1, width='stretch'):
        st.session_state.pending_prompt = sug_1
        st.rerun()
    if st.button(sug_2, width='stretch'):
        st.session_state.pending_prompt = sug_2
        st.rerun()

# --- Main Application Logic ---
st.title("🔍 Ask Foundry Vantage")

# Handle user input (Manual or Suggested)
prompt = st.chat_input("Ask a question about your media supply chain...")
if st.session_state.get('pending_prompt'):
    prompt = st.session_state.pending_prompt
    del st.session_state.pending_prompt

if prompt:
    # REGION SYNC: Prioritize text mention, then fall back to sidebar
    active_reg = st.session_state.current_region
    for r in ["NA", "APAC", "EMEA", "LATAM"]:
        if r.lower() in prompt.lower():
            active_reg = r
            st.session_state.current_region = r 
    
    with st.spinner(f"Querying {active_reg} data..."):
        sql, error, chart_type = parse_query(prompt, active_reg)
        
        if error:
            st.error(error)
        else:
            res_df, exec_err = execute_sql(sql, DB_CONN)
            
            if res_df is not None and not res_df.empty:
                fig = None
                if chart_type == "pie" and 'status' in res_df.columns:
                    fig = px.pie(res_df, names='status', title=f"Status: {active_reg}", hole=0.4)
                elif chart_type == "bar" and 'vendor' in res_df.columns:
                    val_col = 'deal_value' if 'deal_value' in res_df.columns else 'vendor'
                    fig = px.bar(res_df, x='vendor', y=val_col, title=f"Analysis: {active_reg}")

                st.session_state.chat_history.append({
                    "question": prompt,
                    "answer": f"Found {len(res_df)} records for {active_reg}:",
                    "data": res_df,
                    "chart": fig,
                    "region": active_reg
                })
            else:
                st.warning(f"No records found for '{prompt}' in {active_reg}.")

# Display Chat History with unique keys to fix crashes
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"):
        st.write(msg["question"])
    
    with st.chat_message("assistant", avatar="🎥"):
        st.write(msg["answer"])
        if msg["chart"]:
            st.plotly_chart(msg["chart"], width='stretch', key=f"chart_id_{i}")
        with st.expander("View Detailed Records"):
            st.dataframe(msg["data"], width='stretch', key=f"data_id_{i}")

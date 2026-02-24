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
    
    # Global Region Filter
    st.session_state.current_region = st.selectbox("Market Region", ["NA", "APAC", "EMEA", "LATAM"])
    
    # Context Selection
    st.radio("Active Insight Layer", ["Executive Content", "Work Order Tracker", "Deals Performance"])

    st.divider()
    st.subheader("💡 Suggestions")
    if st.button("List content status for MAX Australia", width='stretch'):
        st.session_state.pending_prompt = "List content status for MAX Australia"
        st.rerun()

# --- Main Application Logic ---
st.title("🔍 Ask Foundry Vantage")

# Handle user input
prompt = st.chat_input("Ask a question about your media supply chain...")
if st.session_state.get('pending_prompt'):
    prompt = st.session_state.pending_prompt
    del st.session_state.pending_prompt

if prompt:
    # REGION LOGIC: Determine if we use Sidebar or Prompt intent
    active_reg = st.session_state.current_region
    for r in ["NA", "APAC", "EMEA", "LATAM"]:
        if r.lower() in prompt.lower():
            active_reg = r
    
    with st.spinner(f"Analyzing {active_reg} data..."):
        # Parse NL to SQL
        sql, error, chart_type = parse_query(prompt, active_reg)
        
        if error:
            st.error(error)
        else:
            # Execute SQL
            res_df, exec_err = execute_sql(sql, DB_CONN)
            
            if res_df is not None and not res_df.empty:
                # Chart Generation Logic
                fig = None
                if chart_type == "pie" and 'status' in res_df.columns:
                    fig = px.pie(res_df, names='status', title=f"Status Distribution: {active_reg}", hole=0.4)
                elif chart_type == "bar" and 'vendor' in res_df.columns:
                    val_col = 'deal_value' if 'deal_value' in res_df.columns else 'vendor'
                    fig = px.bar(res_df, x='vendor', y=val_col, title=f"Vendor Analysis: {active_reg}")

                # Save to history
                st.session_state.chat_history.append({
                    "question": prompt,
                    "answer": f"I found {len(res_df)} records for {active_reg}:",
                    "data": res_df,
                    "chart": fig
                })
            else:
                st.warning(f"No matching records found for that specific request in {active_reg}.")

# Display Chat History
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"):
        st.write(msg["question"])
    
    with st.chat_message("assistant", avatar="🎥"):
        st.write(msg["answer"])
        if msg["chart"]:
            st.plotly_chart(msg["chart"], width='stretch')
        with st.expander("View Data Records"):
            st.dataframe(msg["data"], width='stretch')

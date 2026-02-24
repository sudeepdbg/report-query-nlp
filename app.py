import streamlit as st
import pandas as pd
import plotly.express as px
from utils.database import init_database, execute_sql
from utils.query_parser import parse_query

st.set_page_config(page_title="Foundry Vantage", page_icon="🎥", layout="wide")

@st.cache_resource
def get_db():
    return init_database()

DB_CONN = get_db()

if 'chat_history' not in st.session_state: st.session_state.chat_history = []
if 'current_region' not in st.session_state: st.session_state.current_region = 'APAC'

# --- Logic for 5-6 Dynamic Suggestions ---
def get_expanded_suggestions(region):
    # This list changes based on the region and variety in your DB
    return [
        f"Show active deals in {region}",
        f"Top 5 vendors by value in {region}",
        f"What content is 'Not Ready' in {region}?",
        f"List delayed work orders for {region}",
        f"Show status for MAX Australia" if region == "APAC" else f"Show status for MAX Europe",
        f"Total deal value for {region} market"
    ]

# --- Sidebar ---
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    
    # Strictly bind the selector to session state
    market_options = ["NA", "APAC", "EMEA", "LATAM"]
    st.session_state.current_region = st.selectbox(
        "Market Region", 
        market_options,
        index=market_options.index(st.session_state.current_region)
    )
    
    st.divider()
    st.subheader("💡 Suggested Queries")
    
    # Generate 6 dynamic buttons
    suggestions = get_expanded_suggestions(st.session_state.current_region)
    for sug in suggestions:
        if st.button(sug, width='stretch'): # Fix: use width='stretch' per logs [cite: 47]
            st.session_state.pending_prompt = sug
            st.rerun()

# --- Main Chat ---
st.title("🔍 Ask Foundry Vantage")

prompt = st.chat_input("Ask about deals, content, or work orders...")
if st.session_state.get('pending_prompt'):
    prompt = st.session_state.pending_prompt
    del st.session_state.pending_prompt

if prompt:
    # FILTER FIX: Sync session state if a region is typed
    active_reg = st.session_state.current_region
    for r in ["NA", "APAC", "EMEA", "LATAM"]:
        if r.lower() in prompt.lower():
            active_reg = r
            st.session_state.current_region = r # Sync sidebar to text
    
    with st.spinner(f"Analyzing {active_reg}..."):
        sql, error, chart_type = parse_query(prompt, active_reg)
        
        if error:
            st.error(error)
        else:
            res_df, exec_err = execute_sql(sql, DB_CONN)
            if res_df is not None and not res_df.empty:
                fig = None
                # PIE: for Status/Categories | BAR: for Values/Comparisons
                if chart_type == "pie":
                    fig = px.pie(res_df, names='status', title=f"Status Breakdown: {active_reg}", hole=0.4)
                elif chart_type == "bar":
                    y_val = 'deal_value' if 'deal_value' in res_df.columns else res_df.columns[0]
                    fig = px.bar(res_df, x='vendor', y=y_val, title=f"Value Analysis: {active_reg}")

                st.session_state.chat_history.append({
                    "question": prompt,
                    "answer": f"Results for {active_reg}:",
                    "data": res_df,
                    "chart": fig
                })
            else:
                st.warning(f"No records found for '{prompt}' in {active_reg}.")

# Display History with Unique Keys to fix DuplicateElementId 
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"): st.write(msg["question"])
    with st.chat_message("assistant", avatar="🎥"):
        st.write(msg["answer"])
        if msg["chart"]: 
            st.plotly_chart(msg["chart"], width='stretch', key=f"chart_{i}")
        with st.expander("View Records"):
            st.dataframe(msg["data"], width='stretch', key=f"df_{i}")

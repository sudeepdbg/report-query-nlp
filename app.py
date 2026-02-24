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

# Initialize session states
if 'chat_history' not in st.session_state: st.session_state.chat_history = []
if 'current_region' not in st.session_state: st.session_state.current_region = 'APAC'

# --- THE CRITICAL FIX: REGION SYNC ---
def sync_region(new_reg):
    """Explicitly updates the sidebar widget value to prevent state-lag."""
    st.session_state.current_region = new_reg
    st.session_state["sidebar_market_selector"] = new_reg

# --- Sidebar Management ---
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.divider()
    
    market_options = ["NA", "APAC", "EMEA", "LATAM"]
    # We bind the selectbox directly to the 'sidebar_market_selector' key
    selected_market = st.selectbox(
        "Market Region", 
        market_options,
        key="sidebar_market_selector",
        index=market_options.index(st.session_state.current_region)
    )
    # Update global state based on manual sidebar change
    st.session_state.current_region = selected_market
    
    st.divider()
    # Dynamic Suggestions Logic
    def get_suggestions(reg):
        return [f"Show SVOD rights in {reg}", f"Localization status for {reg}", f"Top vendors in {reg}", 
                f"Duplo work orders in {reg}", f"Show readiness for MAX {reg}", f"Total deal value {reg}"]
    
    st.subheader("💡 Suggested Queries")
    for i, sug in enumerate(get_suggestions(st.session_state.current_region)):
        if st.button(sug, width='stretch', key=f"sug_btn_{i}"): 
            st.session_state.pending_prompt = sug
            st.rerun()

# --- Main Chat UI ---
st.title("🔍 Ask Foundry Vantage")

prompt = st.chat_input("Ask about deals, content, or localization...")

# Handle pending prompts from buttons
if st.session_state.get('pending_prompt'):
    prompt = st.session_state.pending_prompt
    del st.session_state.pending_prompt

if prompt:
    # 1. SCAN for region in text
    detected_reg = None
    for r in ["NA", "APAC", "EMEA", "LATAM"]:
        if r.lower() in prompt.lower():
            detected_reg = r
            break
    
    # 2. FORCE SYNC if a new region is mentioned
    if detected_reg and detected_reg != st.session_state.current_region:
        sync_region(detected_reg)
        st.rerun() # Hard reset to ensure UI snaps to new region

    # 3. EXECUTE QUERY
    active_reg = st.session_state.current_region
    with st.spinner(f"Analyzing {active_reg} infrastructure..."):
        sql, error, chart_type = parse_query(prompt, active_reg)
        
        if error:
            st.error(error)
        else:
            res_df, _ = execute_sql(sql, DB_CONN)
            if res_df is not None and not res_df.empty:
                fig = None
                if chart_type == "pie":
                    col = 'status' if 'status' in res_df.columns else res_df.columns[-1]
                    fig = px.pie(res_df, names=col, title=f"Distribution: {active_reg}", hole=0.4)
                else:
                    y_axis = 'deal_value' if 'deal_value' in res_df.columns else res_df.columns[0]
                    fig = px.bar(res_df, x=res_df.columns[1] if len(res_df.columns)>1 else res_df.columns[0], 
                                 y=y_axis, title=f"Analysis: {active_reg}")

                st.session_state.chat_history.append({
                    "question": prompt, 
                    "answer": f"Results for {active_reg}:", 
                    "data": res_df, 
                    "chart": fig
                })
            else:
                st.warning(f"No records found for '{prompt}' in {active_reg}.")

# --- Display History ---
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"): st.write(msg["question"])
    with st.chat_message("assistant", avatar="🎥"):
        st.write(msg["answer"])
        if msg["chart"]: st.plotly_chart(msg["chart"], width='stretch', key=f"c_{i}")
        with st.expander("View Data"): st.dataframe(msg["data"], width='stretch', key=f"d_{i}")

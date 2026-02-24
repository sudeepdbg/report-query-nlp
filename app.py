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

# Session State
if 'chat_history' not in st.session_state: st.session_state.chat_history = []
if 'current_region' not in st.session_state: st.session_state.current_region = 'APAC'

def get_dynamic_suggestions(last_query, region):
    """Generates intelligent follow-up queries based on context."""
    q = last_query.lower()
    # Context: DEALS
    if "deal" in q or "vendor" in q:
        return [
            f"Show active deals in {region}",
            f"Top vendors by deal value in {region}",
            f"What is the total deal value for {region}?"
        ]
    # Context: CONTENT
    if "content" in q or "max" in q or "status" in q:
        return [
            f"Which content is Not Ready in {region}?",
            f"Show all Delivered movies in {region}",
            f"Content delivery schedule for {region}"
        ]
    # Context: WORK ORDERS
    if "order" in q or "task" in q:
        return [
            f"List all delayed work orders in {region}",
            f"Subtitle vs Dubbing status in {region}",
            f"Show high priority tasks for {region}"
        ]
    # Default
    return [f"Show content status in {region}", f"List active deals in {region}"]

# --- Sidebar ---
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    
    # Sync Sidebar
    market_options = ["NA", "APAC", "EMEA", "LATAM"]
    st.session_state.current_region = st.selectbox(
        "Market Region", 
        market_options,
        index=market_options.index(st.session_state.current_region)
    )
    
    st.divider()
    st.subheader("💡 Suggested Queries")
    
    # Get context from the very last message in history
    last_q = st.session_state.chat_history[-1]["question"] if st.session_state.chat_history else ""
    suggestions = get_dynamic_suggestions(last_q, st.session_state.current_region)
    
    for sug in suggestions:
        if st.button(sug, width='stretch'):
            st.session_state.pending_prompt = sug
            st.rerun()

# --- Main Chat ---
st.title("🔍 Ask Foundry Vantage")

prompt = st.chat_input("Ask about deals, content, or work orders...")
if st.session_state.get('pending_prompt'):
    prompt = st.session_state.pending_prompt
    del st.session_state.pending_prompt

if prompt:
    # Auto-detect region from text to update sidebar
    active_reg = st.session_state.current_region
    for r in ["NA", "APAC", "EMEA", "LATAM"]:
        if r.lower() in prompt.lower():
            active_reg = r
            st.session_state.current_region = r
    
    with st.spinner(f"Analyzing {active_reg}..."):
        sql, error, chart_type = parse_query(prompt, active_reg)
        
        if error:
            st.error(error)
        else:
            res_df, exec_err = execute_sql(sql, DB_CONN)
            if res_df is not None and not res_df.empty:
                # Logic for Visualizations
                fig = None
                if chart_type == "pie":
                    # Best for: Status distribution, Type breakdown
                    label_col = 'status' if 'status' in res_df.columns else res_df.columns[1]
                    fig = px.pie(res_df, names=label_col, title=f"Distribution: {active_reg}", hole=0.4)
                elif chart_type == "bar":
                    # Best for: Financials, Vendor comparison, Volume
                    x_col = 'vendor' if 'vendor' in res_df.columns else res_df.columns[1]
                    y_col = 'deal_value' if 'deal_value' in res_df.columns else res_df.columns[0]
                    fig = px.bar(res_df, x=x_col, y=y_col, title=f"Comparison: {active_reg}")

                st.session_state.chat_history.append({
                    "question": prompt,
                    "answer": f"Results for {active_reg}:",
                    "data": res_df,
                    "chart": fig
                })
            else:
                st.warning(f"No data found for this request in {active_reg}.")

# Display History
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"): st.write(msg["question"])
    with st.chat_message("assistant", avatar="🎥"):
        st.write(msg["answer"])
        if msg["chart"]: st.plotly_chart(msg["chart"], width='stretch', key=f"c_{i}")
        with st.expander("View Data"):
            st.dataframe(msg["data"], width='stretch', key=f"d_{i}")

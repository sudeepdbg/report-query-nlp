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

# --- Enhanced Dynamic Suggestions for 6 Scenarios ---
def get_expanded_suggestions(last_query, region):
    q = last_query.lower()
    # Context: Localization & Duplo
    if any(word in q for word in ["local", "order", "task", "duplo"]):
        return [
            f"Show Duplo queue status for {region}",
            f"Which localization tasks are delayed in {region}?",
            f"List all Dubbing work orders for {region}",
            f"Subtitle vs Dubbing volume in {region}",
            f"Show high priority tasks for {region}",
            f"What is the Packaging status for {region}?"
        ]
    # Context: Deals & Rights
    elif any(word in q for word in ["deal", "rights", "value"]):
        return [
            f"Show SVOD Exclusive rights in {region}",
            f"Top vendors by deal value in {region}",
            f"List active negotiations in {region}",
            f"Show rights scope breakdown for {region}",
            f"Total budget for {region} market",
            f"Check pending acquisitions in {region}"
        ]
    # Default Context: Content Readiness
    return [
        f"Which content is 'Not Ready' in {region}?",
        f"Show localization status for {region}",
        f"List all 'Acquired' content in {region}",
        f"Content delivery schedule for {region}",
        f"Available languages in {region} market",
        f"Show readiness for MAX {region}"
    ]

# --- Sidebar ---
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    
    market_options = ["NA", "APAC", "EMEA", "LATAM"]
    st.session_state.current_region = st.selectbox(
        "Market Region", 
        market_options,
        index=market_options.index(st.session_state.current_region)
    )
    
    st.divider()
    st.subheader("💡 Suggested Queries")
    
    # Get last query for context-aware suggestions
    last_q = st.session_state.chat_history[-1]["question"] if st.session_state.chat_history else ""
    suggestions = get_expanded_suggestions(last_q, st.session_state.current_region)
    
    for sug in suggestions:
        # Fixed width='stretch' per deployment logs 
        if st.button(sug, width='stretch'): 
            st.session_state.pending_prompt = sug
            st.rerun()

# --- Main Chat ---
st.title("🔍 Ask Foundry Vantage")

prompt = st.chat_input("Ask about Rights, Localization, or Duplo status...")
if st.session_state.get('pending_prompt'):
    prompt = st.session_state.pending_prompt
    del st.session_state.pending_prompt

if prompt:
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
                fig = None
                if chart_type == "pie":
                    # Dynamic column selection for pie charts based on new schema
                    col = 'status' if 'status' in res_df.columns else res_df.columns[-1]
                    fig = px.pie(res_df, names=col, title=f"Distribution: {active_reg}", hole=0.4)
                elif chart_type == "bar":
                    y_val = 'deal_value' if 'deal_value' in res_df.columns else res_df.columns[0]
                    fig = px.bar(res_df, x=res_df.columns[1], y=y_val, title=f"Value Analysis: {active_reg}")

                st.session_state.chat_history.append({
                    "question": prompt,
                    "answer": f"Results for {active_reg}:",
                    "data": res_df,
                    "chart": fig
                })
            else:
                st.warning(f"No records found for '{prompt}' in {active_reg}.")

# Display History
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"): st.write(msg["question"])
    with st.chat_message("assistant", avatar="🎥"):
        st.write(msg["answer"])
        if msg["chart"]: 
            # Fixed width='stretch' per deployment logs 
            st.plotly_chart(msg["chart"], width='stretch', key=f"chart_{i}")
        with st.expander("View Records"):
            # Fixed width='stretch' per deployment logs 
            st.dataframe(msg["data"], width='stretch', key=f"df_{i}")

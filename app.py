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

# 1. Initialize session states
if 'chat_history' not in st.session_state: st.session_state.chat_history = []
if 'current_region' not in st.session_state: st.session_state.current_region = 'APAC'

# 2. PRE-EMPTIVE REGION DETECTION
# We capture the input here so we can adjust state BEFORE rendering the sidebar
temp_input = st.chat_input("Ask about deals, content, or localization...")

# Determine the active prompt (from input or button)
prompt = None
if st.session_state.get('pending_prompt'):
    prompt = st.session_state.pending_prompt
    del st.session_state.pending_prompt
elif temp_input:
    prompt = temp_input

# If a region is mentioned in the text, update the state immediately
if prompt:
    for r in ["NA", "APAC", "EMEA", "LATAM"]:
        if r.lower() in prompt.lower():
            st.session_state.current_region = r
            break

# 3. Sidebar Management
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.divider()
    
    market_options = ["NA", "APAC", "EMEA", "LATAM"]
    # Now this selectbox index is always in sync because state was updated at the top
    selected_market = st.selectbox(
        "Market Region", 
        market_options,
        index=market_options.index(st.session_state.current_region),
        key="sidebar_market_selector"
    )
    st.session_state.current_region = selected_market
    
    def get_suggestions(reg):
        return [f"Show SVOD rights in {reg}", f"Localization status for {reg}", f"Top vendors in {reg}", 
                f"Duplo work orders in {reg}", f"Show readiness for MAX {reg}", f"Total deal value {reg}"]
    
    st.subheader("💡 Suggested Queries")
    for i, sug in enumerate(get_suggestions(st.session_state.current_region)):
        if st.button(sug, width='stretch', key=f"sug_btn_{i}"): 
            st.session_state.pending_prompt = sug
            st.rerun()

# 4. Main Chat UI
st.title("🔍 Ask Foundry Vantage")

if prompt:
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
                    x_axis = res_df.columns[1] if len(res_df.columns) > 1 else res_df.columns[0]
                    fig = px.bar(res_df, x=x_axis, y=y_axis, title=f"Analysis: {active_reg}")

                st.session_state.chat_history.append({
                    "question": prompt, 
                    "answer": f"Results for {active_reg}:", 
                    "data": res_df, 
                    "chart": fig
                })
            else:
                st.warning(f"No records found for '{prompt}' in {active_reg}.")

# 5. Display History
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"): st.write(msg["question"])
    with st.chat_message("assistant", avatar="🎥"):
        st.write(msg["answer"])
        if msg["chart"]: 
            st.plotly_chart(msg["chart"], width='stretch', key=f"c_{i}")
        with st.expander("View Data"): 
            st.dataframe(msg["data"], width='stretch', key=f"d_{i}")

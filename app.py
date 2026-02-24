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

# --- 1. DYNAMIC SUGGESTION ENGINE ---
def get_dynamic_suggestions(last_query, region):
    q = last_query.lower()
    if any(word in q for word in ["local", "order", "task", "duplo", "qa"]):
        return [f"Show Duplo queue status for {region}", f"Which localization tasks are delayed in {region}?", f"List all Dubbing work orders for {region}", f"Subtitle vs Dubbing volume in {region}", f"Show high priority tasks for {region}", f"What is the Packaging status for {region}?"]
    elif any(word in q for word in ["deal", "rights", "svod", "value"]):
        return [f"Show SVOD Exclusive rights in {region}", f"Top vendors by deal value in {region}", f"List active negotiations in {region}", f"Show rights scope breakdown for {region}", f"Total budget for {region} market", f"Check pending acquisitions in {region}"]
    return [f"Which content is 'Not Ready' in {region}?", f"Show localization status for {region}", f"List all 'Acquired' content in {region}", f"Content delivery schedule for {region}", f"Available languages in {region} market", f"Show readiness for MAX {region}"]

# --- 2. SIDEBAR WITH DYNAMIC SYNC ---
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.caption("Supply Chain Intelligence Layer")
    st.divider()
    
    market_options = ["NA", "APAC", "EMEA", "LATAM"]
    # We use the index parameter to force the selectbox to match our session state
    selected_market = st.selectbox(
        "Market Region", 
        market_options,
        index=market_options.index(st.session_state.current_region),
        key="sidebar_market_selector"
    )
    # This ensures that manually changing the sidebar updates the state
    st.session_state.current_region = selected_market
    
    st.divider()
    st.subheader("💡 Suggested Queries")
    last_q = st.session_state.chat_history[-1]["question"] if st.session_state.chat_history else ""
    suggestions = get_dynamic_suggestions(last_q, st.session_state.current_region)
    
    for i, sug in enumerate(suggestions):
        if st.button(sug, width='stretch', key=f"sug_btn_{i}"): 
            st.session_state.pending_prompt = sug
            st.rerun()

# --- 3. MAIN CHAT & OVERRIDE LOGIC ---
st.title("🔍 Ask Foundry Vantage")

prompt = st.chat_input("Ask about deals, content readiness, or localization...")
if st.session_state.get('pending_prompt'):
    prompt = st.session_state.pending_prompt
    del st.session_state.pending_prompt

if prompt:
    # --- THE FIX: DETECT REGION AND FORCE RERUN ---
    new_reg = None
    for r in ["NA", "APAC", "EMEA", "LATAM"]:
        if r.lower() in prompt.lower():
            new_reg = r
            break
    
    # If the user mentioned a region different from the sidebar, update and REFRESH
    if new_reg and new_reg != st.session_state.current_region:
        st.session_state.current_region = new_reg
        st.rerun() # This restarts the script so the sidebar and logic are perfectly synced

    active_reg = st.session_state.current_region
    
    with st.spinner(f"Analyzing {active_reg} infrastructure..."):
        sql, error, chart_type = parse_query(prompt, active_reg)
        
        if error:
            st.error(error)
        else:
            res_df, exec_err = execute_sql(sql, DB_CONN)
            if res_df is not None and not res_df.empty:
                fig = None
                if chart_type == "pie":
                    label_col = 'status' if 'status' in res_df.columns else res_df.columns[-1]
                    fig = px.pie(res_df, names=label_col, title=f"Distribution: {active_reg}", hole=0.4)
                elif chart_type == "bar":
                    y_axis = 'deal_value' if 'deal_value' in res_df.columns else res_df.columns[0]
                    fig = px.bar(res_df, x=res_df.columns[1] if len(res_df.columns) > 1 else res_df.columns[0], 
                                 y=y_axis, title=f"Analysis: {active_reg}")

                st.session_state.chat_history.append({
                    "question": prompt,
                    "answer": f"Results for {active_reg}:",
                    "data": res_df,
                    "chart": fig
                })
            else:
                st.warning(f"No records found for '{prompt}' in {active_reg}.")

# --- 4. DISPLAY HISTORY ---
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"): st.write(msg["question"])
    with st.chat_message("assistant", avatar="🎥"):
        st.write(msg["answer"])
        if msg["chart"]: st.plotly_chart(msg["chart"], use_container_width=True, key=f"chart_{i}")
        with st.expander("View Data Records"):
            st.dataframe(msg["data"], use_container_width=True, key=f"data_{i}")

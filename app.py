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

# 1. Initialize State
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if 'current_region' not in st.session_state:
    st.session_state.current_region = 'APAC'

# 2. PRE-PROCESS INPUT (Before Sidebar)
# This is the "Traffic Controller" that catches region changes early
user_input = st.chat_input("Ask about deals, content readiness, or localization...")

active_prompt = None
if st.session_state.get('pending_prompt'):
    active_prompt = st.session_state.pending_prompt
    del st.session_state.pending_prompt
elif user_input:
    active_prompt = user_input

# Detect if the user typed a new region
if active_prompt:
    for r in ["NA", "APAC", "EMEA", "LATAM"]:
        if r.lower() in active_prompt.lower():
            if st.session_state.current_region != r:
                st.session_state.current_region = r
                # We don't rerun yet; we let the sidebar render with the new value

# 3. SIDEBAR (With Dynamic Key to Force Refresh)
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.divider()
    
    market_options = ["NA", "APAC", "EMEA", "LATAM"]
    # KEY FIX: By adding the region name to the key, the sidebar resets when region changes
    selected_market = st.selectbox(
        "Market Region", 
        market_options,
        index=market_options.index(st.session_state.current_region),
        key=f"sidebar_filter_{st.session_state.current_region}" 
    )
    st.session_state.current_region = selected_market
    
    st.divider()
    st.subheader("💡 Suggested Queries")
    def get_suggestions(reg):
        return [f"Show SVOD rights in {reg}", f"Localization status for {reg}", 
                f"Top vendors in {reg}", f"Duplo work orders in {reg}"]
    
    for i, sug in enumerate(get_suggestions(st.session_state.current_region)):
        if st.button(sug, width='stretch', key=f"sug_btn_{i}_{st.session_state.current_region}"): 
            st.session_state.pending_prompt = sug
            st.rerun()

# 4. MAIN UI (History first to fix scrolling)
st.title("🔍 Ask Foundry Vantage")

# Render history at the top
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"):
        st.write(msg["question"])
    with st.chat_message("assistant", avatar="🎥"):
        st.write(msg["answer"])
        if msg["chart"]:
            st.plotly_chart(msg["chart"], use_container_width=True, key=f"h_chart_{i}")
        with st.expander("Explore Dataset"):
            st.dataframe(msg["data"], use_container_width=True, key=f"h_data_{i}")

# 5. PROCESS NEW QUERY (Appears at the very bottom)
if active_prompt:
    active_reg = st.session_state.current_region
    
    with st.chat_message("user"):
        st.write(active_prompt)
        
    with st.chat_message("assistant", avatar="🎥"):
        with st.spinner(f"Analyzing {active_reg}..."):
            sql, error, chart_type = parse_query(active_prompt, active_reg)
            
            if error:
                st.error(error)
            else:
                res_df, _ = execute_sql(sql, DB_CONN)
                if res_df is not None and not res_df.empty:
                    # Logic for charts
                    if chart_type == "pie":
                        label = 'status' if 'status' in res_df.columns else res_df.columns[-1]
                        fig = px.pie(res_df, names=label, title=f"Inventory: {active_reg}", hole=0.4)
                    else:
                        y = 'deal_value' if 'deal_value' in res_df.columns else res_df.columns[0]
                        x = res_df.columns[1] if len(res_df.columns) > 1 else res_df.columns[0]
                        fig = px.bar(res_df, x=x, y=y, title=f"Analysis: {active_reg}")

                    st.write(f"Displaying {active_reg} Data:")
                    st.plotly_chart(fig, use_container_width=True, key=f"new_chart_{len(st.session_state.chat_history)}")
                    with st.expander("Explore Dataset", expanded=True):
                        st.dataframe(res_df, use_container_width=True)

                    # Update history
                    st.session_state.chat_history.append({
                        "question": active_prompt,
                        "answer": f"Displaying {active_reg} Data:",
                        "data": res_df,
                        "chart": fig
                    })
                    # Use a small rerun here only to refresh the history list above
                    st.rerun()
                else:
                    st.warning(f"No results found for {active_reg}.")

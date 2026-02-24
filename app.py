import streamlit as st
import pandas as pd
import plotly.express as px
from utils.database import init_database, execute_sql
from utils.query_parser import parse_query

# 1. Page Config must be first
st.set_page_config(page_title="Foundry Vantage", page_icon="🎥", layout="wide")

@st.cache_resource
def get_db():
    return init_database()

DB_CONN = get_db()

# 2. Initialize Core State
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if 'current_region' not in st.session_state:
    st.session_state.current_region = 'APAC'

# 3. THE PRE-RENDER LOGIC GATE
# We capture the input BEFORE rendering any widgets.
# This allows us to set the region so the sidebar 'snaps' to it immediately.
query_input = st.chat_input("Ask about deals, content readiness, or localization...")

# Detect prompt from input or sidebar button
active_prompt = None
if st.session_state.get('pending_prompt'):
    active_prompt = st.session_state.pending_prompt
    del st.session_state.pending_prompt
elif query_input:
    active_prompt = query_input

# CRITICAL: Detect region and update state BEFORE the sidebar renders
if active_prompt:
    for r in ["NA", "APAC", "EMEA", "LATAM"]:
        if r.lower() in active_prompt.lower():
            st.session_state.current_region = r
            break

# 4. SIDEBAR (Now synced with pre-detected region)
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.divider()
    
    market_options = ["NA", "APAC", "EMEA", "LATAM"]
    # We use the index of our pre-synced state
    selected_market = st.selectbox(
        "Market Region", 
        market_options,
        index=market_options.index(st.session_state.current_region),
        key="main_market_filter"
    )
    st.session_state.current_region = selected_market
    
    st.divider()
    st.subheader("💡 Suggested Queries")
    
    # Suggestions match the synchronized region
    def get_suggestions(reg):
        return [f"Show SVOD rights in {reg}", f"Localization status for {reg}", 
                f"Top vendors in {reg}", f"Duplo work orders in {reg}"]
    
    for i, sug in enumerate(get_suggestions(st.session_state.current_region)):
        if st.button(sug, width='stretch', key=f"sug_btn_{i}"): 
            st.session_state.pending_prompt = sug
            st.rerun()

# 5. MAIN EXECUTION
st.title("🔍 Ask Foundry Vantage")

if active_prompt:
    # Use the region that is now guaranteed to be in sync
    active_reg = st.session_state.current_region
    
    with st.spinner(f"Querying {active_reg} infrastructure..."):
        sql, error, chart_type = parse_query(active_prompt, active_reg)
        
        if error:
            st.error(error)
        else:
            res_df, _ = execute_sql(sql, DB_CONN)
            if res_df is not None and not res_df.empty:
                fig = None
                # Enhanced visualization logic
                if chart_type == "pie":
                    label_col = 'status' if 'status' in res_df.columns else res_df.columns[-1]
                    fig = px.pie(res_df, names=label_col, title=f"Inventory: {active_reg}", hole=0.4)
                else:
                    y_val = 'deal_value' if 'deal_value' in res_df.columns else res_df.columns[0]
                    x_val = res_df.columns[1] if len(res_df.columns) > 1 else res_df.columns[0]
                    fig = px.bar(res_df, x=x_val, y=y_val, title=f"Financials: {active_reg}")

                # Save history with the correct localized label
                st.session_state.chat_history.append({
                    "question": active_prompt,
                    "answer": f"Displaying {active_reg} Data:",
                    "data": res_df,
                    "chart": fig
                })
            else:
                st.warning(f"No records found for '{active_prompt}' in {active_reg}.")

# 6. RENDER HISTORY
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"):
        st.write(msg["question"])
    with st.chat_message("assistant", avatar="🎥"):
        st.write(msg["answer"])
        if msg["chart"]:
            st.plotly_chart(msg["chart"], use_container_width=True, key=f"chart_hist_{i}")
        with st.expander("Explore Dataset"):
            st.dataframe(msg["data"], use_container_width=True, key=f"data_hist_{i}")

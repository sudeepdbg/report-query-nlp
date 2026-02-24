import streamlit as st
import pandas as pd
from datetime import datetime
import plotly.express as px
from utils.database import init_database, execute_sql
from utils.query_parser import parse_query

st.set_page_config(page_title="Foundry Vantage", page_icon="🎥", layout="wide")

@st.cache_resource
def get_database_connection():
    return init_database()

DB_CONN = get_database_connection()

# --- Dynamic Suggestions Logic ---
SUGGESTIONS = {
    "executive": {
        "NA": ["How many content items are Delivered in NA?", "Show all MAX US content"],
        "APAC": ["List content status for MAX Australia", "Total content planned in APAC"],
        "EMEA": ["Show me MAX Europe content status", "How many items are Not Ready in EMEA?"]
    },
    "workorders": {
        "NA": ["Show all delayed work orders in NA", "List top vendors in NA"],
        "APAC": ["How many work orders are In Progress in APAC?", "Show delayed orders for Vendor B"],
        "EMEA": ["Pending review work orders in EMEA", "Show me priority A orders in EMEA"]
    },
    "deals": {
        "APAC": ["Show active deals in APAC", "Top vendors by deal value in APAC"],
        "NA": ["List all pending deals in NA", "Total deal value for Warner Bros"],
        "EMEA": ["Active deals in EMEA", "Show deals for Sky in EMEA"]
    }
}

# Session State
if 'chat_history' not in st.session_state: st.session_state.chat_history = []
if 'current_region' not in st.session_state: st.session_state.current_region = 'APAC'
if 'active_dashboard' not in st.session_state: st.session_state.active_dashboard = 'executive'

# --- Sidebar ---
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.divider()
    st.session_state.current_region = st.selectbox("Market Region", ["NA", "APAC", "EMEA", "LATAM"])
    
    dash_options = {"Executive Content": "executive", "Work Order Tracker": "workorders", "Deals Performance": "deals"}
    selected_dash = st.radio("Active Insight Layer", list(dash_options.keys()))
    st.session_state.active_dashboard = dash_options[selected_dash]

    # DYNAMIC SUGGESTIONS
    st.subheader("💡 Suggested for you")
    current_suggestions = SUGGESTIONS.get(st.session_state.active_dashboard, {}).get(st.session_state.current_region, ["Show me total count"])
    
    for sugg in current_suggestions:
        if st.button(sugg, use_container_width=True):
            st.session_state.pending_prompt = sugg
            st.rerun()

# --- Main Chat ---
st.title("🔍 Ask Foundry Vantage")

# Display Chat History
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"):
        st.write(msg["question"])
    
    with st.chat_message("assistant", avatar="🎥"):
        st.write(msg["answer"])
        if msg.get("data") is not None:
            st.dataframe(msg["data"], use_container_width=True)
        
        # FEEDBACK MECHANISM
        col1, col2, col3 = st.columns([1, 1, 10])
        with col1:
            if st.button("👍", key=f"up_{i}"): st.toast("Thanks for the feedback!")
        with col2:
            if st.button("👎", key=f"down_{i}"): st.toast("Feedback recorded for improvement.")

# Handle Input
if prompt := st.chat_input("Ask a question...") or st.session_state.get('pending_prompt'):
    if st.session_state.get('pending_prompt'):
        prompt = st.session_state.pending_prompt
        del st.session_state.pending_prompt

    with st.spinner("Analyzing..."):
        sql, error = parse_query(prompt, st.session_state.current_region)
        
        if error:
            ans, res_df = f"⚠️ {error}", None
        else:
            res_df, exec_err = execute_sql(sql, DB_CONN)
            if exec_err:
                ans, res_df = f"Query error: {exec_err}", None
            elif res_df.empty:
                ans, res_df = "No results found for that specific filter.", None
            else:
                ans = f"I found {len(res_df)} results:"

        st.session_state.chat_history.append({"question": prompt, "answer": ans, "data": res_df})
        st.rerun()

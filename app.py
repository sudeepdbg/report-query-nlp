import streamlit as st
import pandas as pd
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
    st.caption("Intelligence Layer for Media Supply Chain")
    st.divider()
    
    st.session_state.current_region = st.selectbox("Market Region", ["NA", "APAC", "EMEA", "LATAM"])
    
    dash_options = {"Executive Content": "executive", "Work Order Tracker": "workorders", "Deals Performance": "deals"}
    selected_dash = st.radio("Active Insight Layer", list(dash_options.keys()))
    st.session_state.active_dashboard = dash_options[selected_dash]

    st.subheader("💡 Suggested Queries")
    current_suggestions = SUGGESTIONS.get(st.session_state.active_dashboard, {}).get(st.session_state.current_region, ["Show total count"])
    
    for sugg in current_suggestions:
        if st.button(sugg, width='stretch'):
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
        
        # Render Graph if available
        if msg.get("chart") is not None:
            st.plotly_chart(msg["chart"], width='stretch')
            
        # Render Table
        if msg.get("data") is not None:
            with st.expander("View Detailed Records"):
                st.dataframe(msg["data"], width='stretch')
        
        # Feedback mechanism
        col1, col2, _ = st.columns([1, 1, 10])
        with col1:
            if st.button("👍", key=f"up_{i}"): st.toast("Feedback recorded!")
        with col2:
            if st.button("👎", key=f"down_{i}"): st.toast("Feedback recorded!")

# Handle Input
prompt = st.chat_input("Ask a question...")
if st.session_state.get('pending_prompt'):
    prompt = st.session_state.pending_prompt
    del st.session_state.pending_prompt

if prompt:
    with st.spinner("Querying Database..."):
        # The parser now returns the chart type required
        sql, error, chart_type = parse_query(prompt, st.session_state.current_region)
        
        if error:
            ans, res_df, fig = f"⚠️ {error}", None, None
        else:
            res_df, exec_err = execute_sql(sql, DB_CONN)
            if exec_err:
                ans, res_df, fig = f"Query error: {exec_err}", None, None
            elif res_df.empty:
                ans, res_df, fig = "No results found for this filter.", None, None
            else:
                ans = f"I found {len(res_df)} results in {st.session_state.current_region}:"
                
                # --- AUTOMATIC CHART GENERATION ---
                fig = None
                try:
                    if chart_type == "pie" and "status" in res_df.columns:
                        counts = res_df['status'].value_counts().reset_index()
                        counts.columns = ['Status', 'Count']
                        fig = px.pie(counts, names='Status', values='Count', hole=0.4, title="Status Breakdown")
                    
                    elif chart_type == "bar" and "vendor" in res_df.columns:
                        # Use deal_value for Deals, or simple count for Work Orders
                        val_col = 'deal_value' if 'deal_value' in res_df.columns else 'vendor'
                        chart_data = res_df.groupby('vendor')[val_col].count() if val_col == 'vendor' else res_df.groupby('vendor')[val_col].sum()
                        chart_data = chart_data.nlargest(5).reset_index()
                        chart_data.columns = ['Vendor', 'Metric']
                        fig = px.bar(chart_data, x='Vendor', y='Metric', title="Top Vendors Performance")
                except Exception as e:
                    print(f"Chart Error: {e}")

        st.session_state.chat_history.append({
            "question": prompt, 
            "answer": ans, 
            "data": res_df, 
            "chart": fig
        })
        st.rerun()

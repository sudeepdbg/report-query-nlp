import streamlit as st
import pandas as pd
import plotly.express as px
import streamlit.components.v1 as components
from utils.database import init_database, execute_sql
from utils.query_parser import parse_query

st.set_page_config(page_title="Foundry Vantage", layout="wide")

# Initialize DB once
@st.cache_resource
def get_db(): return init_database()
DB_CONN = get_db()

# Session State
if 'chat_history' not in st.session_state: st.session_state.chat_history = []
if 'region' not in st.session_state: st.session_state.region = 'APAC'

# SIDEBAR
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.session_state.region = st.selectbox("Market Region", ["NA", "APAC", "EMEA", "LATAM"])
    persona = st.selectbox("View Persona", ["Leadership", "Product", "Operations", "Finance"])
    
    st.divider()
    st.subheader("💡 Suggestions")
    
    # These strings now map exactly to the keyword clusters in query_parser.py
    prompts = {
        "Leadership": ["Top vendors by spend", "Vendor performance", "Market value overview"],
        "Product": ["Content readiness status", "SVOD rights breakdown", "Inventory status"],
        "Operations": ["Delayed work orders", "Task status overview", "Vendor performance"],
        "Finance": ["Total spend per vendor", "Highest value deals", "Spend by rights type"]
    }
    
    for sug in prompts.get(persona, ["Top vendors"]):
        if st.button(sug, use_container_width=True):
            st.session_state.pending = sug
            st.rerun()

# MAIN UI
st.title(f"🔍 {persona} Intelligence")

# Render History
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"): st.write(msg["q"])
    with st.chat_message("assistant"):
        st.plotly_chart(msg["fig"], use_container_width=True)

# Process New Query
prompt = st.chat_input("Ask about deals, vendors, or work orders...")
active_q = st.session_state.get('pending') or prompt

if active_q:
    if 'pending' in st.session_state: del st.session_state.pending
    
    sql, err, c_type = parse_query(active_q, st.session_state.region)
    res_df, _ = execute_sql(sql, DB_CONN)

    if res_df is not None and not res_df.empty:
        with st.chat_message("user"): st.write(active_q)
        with st.chat_message("assistant"):
            # KPI Logic
            if "total_value" in res_df.columns:
                st.metric("Region Financial Impact", f"${res_df['total_value'].sum():,.0f}")
            
            # Charting
            x, y = res_df.columns[0], res_df.columns[1]
            if c_type == "bar":
                fig = px.bar(res_df, x=x, y=y, color=x, template="plotly_white")
            else:
                fig = px.pie(res_df, names=x, values=y, hole=0.4)
            
            st.plotly_chart(fig, use_container_width=True)
            st.session_state.chat_history.append({"q": active_q, "fig": fig})
            
            # SCROLL FIX
            components.html("<script>window.parent.document.querySelector('section.main').scrollTo(0, 10000);</script>", height=0)
            st.rerun()
    else:
        st.error(f"Data mapping error for '{active_q}'. Check if data exists for {st.session_state.region}.")

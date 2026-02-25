import streamlit as st
import pandas as pd
import plotly.express as px
import streamlit.components.v1 as components
import uuid # For unique keys
from utils.database import init_database, execute_sql
from utils.query_parser import parse_query

st.set_page_config(page_title="Foundry Vantage", layout="wide")

@st.cache_resource
def get_db(): return init_database()
DB_CONN = get_db()

# State Management
if 'chat_history' not in st.session_state: st.session_state.chat_history = []
if 'region' not in st.session_state: st.session_state.region = 'APAC'

# Sidebar
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.session_state.region = st.selectbox("Market Region", ["NA", "APAC", "EMEA", "LATAM"])
    persona = st.selectbox("View Persona", ["Leadership", "Product", "Operations", "Finance"])
    
    st.divider()
    st.subheader("💡 Suggestions")
    
    prompts = {
        "Leadership": ["Top vendors", "Vendor performance", "Market value"],
        "Product": ["Content readiness", "SVOD rights", "Inventory status"],
        "Operations": ["Delayed tasks", "Work order status", "Vendor performance"],
        "Finance": ["Total spend per vendor", "Highest cost deals", "Rights breakdown"]
    }
    
    # Use unique keys for sidebar buttons to avoid duplicate ID errors there too
    for i, sug in enumerate(prompts.get(persona, ["Top vendors"])):
        if st.button(sug, use_container_width=True, key=f"sidebar_btn_{i}"):
            st.session_state.pending = sug
            st.rerun()

# Main Header
st.title(f"🔍 {persona} Intelligence")

# 1. RENDER HISTORY (Fix: Added Unique Keys)
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"): st.write(msg["q"])
    with st.chat_message("assistant"):
        # Added unique key using index and persona
        st.plotly_chart(msg["fig"], use_container_width=True, key=f"hist_chart_{i}_{persona}")

# 2. PROCESS INPUT
prompt = st.chat_input("Ask about deals, vendors, or work orders...")
active_q = st.session_state.get('pending') or prompt

if active_q:
    if 'pending' in st.session_state: del st.session_state.pending
    
    sql, err, c_type = parse_query(active_q, st.session_state.region)
    res_df, db_err = execute_sql(sql, DB_CONN)

    if res_df is not None and not res_df.empty:
        with st.chat_message("user"): st.write(active_q)
        with st.chat_message("assistant"):
            if "total_value" in res_df.columns:
                st.metric("Financial Impact", f"${res_df['total_value'].sum():,.0f}")
            
            x, y = res_df.columns[0], res_df.columns[1]
            if c_type == "bar":
                fig = px.bar(res_df, x=x, y=y, color=x, template="plotly_white")
            else:
                fig = px.pie(res_df, names=x, values=y, hole=0.4)
            
            # FIX: Use a unique UUID for the new chart to prevent Duplicate ID error
            unique_key = f"new_chart_{uuid.uuid4().hex}"
            st.plotly_chart(fig, use_container_width=True, key=unique_key)
            
            st.session_state.chat_history.append({"q": active_q, "fig": fig})
            
            # SCROLL FIX
            components.html("<script>window.parent.document.querySelector('section.main').scrollTo(0, 10000);</script>", height=0)
            st.rerun()
    else:
        st.error(f"Mapping error for '{active_q}'. No data found in {st.session_state.region}.")

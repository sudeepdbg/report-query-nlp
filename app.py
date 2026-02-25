import streamlit as st
import pandas as pd
import plotly.express as px
import streamlit.components.v1 as components
import time
from utils.database import init_database, execute_sql
from utils.query_parser import parse_query

st.set_page_config(page_title="Foundry Vantage", page_icon="🎥", layout="wide")

# PM Feature: Custom Styling for the "Vantage" look
st.markdown("""<style>
    .metric-card { background-color: #f0f2f6; padding: 15px; border-radius: 10px; border-left: 5px solid #ff4b4b; }
    .stChatFloatingInputContainer { bottom: 20px; }
</style>""", unsafe_allow_html=True)

@st.cache_resource
def get_db(): return init_database()
DB_CONN = get_db()

if 'chat_history' not in st.session_state: st.session_state.chat_history = []
if 'current_region' not in st.session_state: st.session_state.current_region = 'APAC'

# 1. SIDEBAR: The Control Center
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.caption("v2.0 - Decision Intelligence")
    st.divider()
    
    st.session_state.current_region = st.selectbox("Current Market Focus", ["NA", "APAC", "EMEA", "LATAM"])
    persona = st.selectbox("Intelligence View", ["Leadership", "Product", "Operations", "Finance"])

    st.divider()
    st.subheader(f"🚀 Quick Actions")
    # PM Flow: Common patterns for the selected persona
    suggestions = {
        "Leadership": ["Global spend by vendor", "Top vendors in " + st.session_state.current_region],
        "Product": ["Content readiness status", "Rights scope breakdown"],
        "Finance": ["Total deal value summary", "Highest cost deals"]
    }.get(persona, ["Show inventory"])

    for sug in suggestions:
        if st.button(sug, use_container_width=True):
            st.session_state.pending_prompt = sug
            st.rerun()

# 2. INTERFACE
st.title(f"🔍 {persona} Intelligence")

# 3. CHAT HISTORY RENDERER
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"): st.write(msg["question"])
    with st.chat_message("assistant", avatar="🎥"):
        st.write(msg["answer"])
        if msg["chart"]: st.plotly_chart(msg["chart"], use_container_width=True, key=f"h_{i}")
        with st.expander("Raw Data"): st.dataframe(msg["data"], use_container_width=True)

# 4. QUERY PROCESSING
user_input = st.chat_input("Ask a question...")
active_prompt = st.session_state.get('pending_prompt') or user_input
if st.session_state.get('pending_prompt'): del st.session_state.pending_prompt

if active_prompt:
    with st.chat_message("user"): st.write(active_prompt)
    with st.chat_message("assistant", avatar="🎥"):
        sql, err, c_type = parse_query(active_prompt, st.session_state.current_region)
        res_df, _ = execute_sql(sql, DB_CONN) if sql else (None, None)

        if res_df is not None and not res_df.empty:
            # PM FEATURE: High-level KPI Summary
            if "deal_value" in res_df.columns or "total_value" in res_df.columns:
                val_col = "total_value" if "total_value" in res_df.columns else "deal_value"
                c1, c2 = st.columns(2)
                c1.metric("Total Value", f"${res_df[val_col].sum():,.0f}")
                c2.metric("Records Found", len(res_df))

            # CHARTING
            x_col = res_df.columns[0]
            y_col = res_df.columns[1] if len(res_df.columns) > 1 else res_df.columns[0]
            
            if c_type == "bar":
                fig = px.bar(res_df, x=x_col, y=y_col, color=x_col, template="plotly_white")
            else:
                fig = px.pie(res_df, names=x_col, hole=0.5)
            
            st.plotly_chart(fig, use_container_width=True)
            st.session_state.chat_history.append({"question": active_prompt, "answer": "Analysis complete:", "data": res_df, "chart": fig})
            
            # Scroll Fix
            components.html("<script>var m = window.parent.document.querySelector('section.main'); m.scrollTo({top: m.scrollHeight, behavior:'smooth'});</script>", height=0)
            st.rerun()
        else:
            st.error("No data matches your criteria. Try adjusting the region or search term.")

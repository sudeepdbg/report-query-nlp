import streamlit as st
import pandas as pd
import plotly.express as px
import streamlit.components.v1 as components
import time
from utils.database import init_database, execute_sql
from utils.query_parser import parse_query

st.set_page_config(page_title="Foundry Vantage", page_icon="🎥", layout="wide")

# CSS for a polished PM Dashboard look
st.markdown("""<style>
    .stMetric { background: #f8f9fb; padding: 15px; border-radius: 10px; border: 1px solid #e6e9ef; }
    [data-testid="stSidebar"] { background-color: #0e1117; color: white; }
</style>""", unsafe_allow_html=True)

@st.cache_resource
def get_db(): return init_database()
DB_CONN = get_db()

# Session States
if 'chat_history' not in st.session_state: st.session_state.chat_history = []
if 'current_region' not in st.session_state: st.session_state.current_region = 'APAC'

def get_persona_suggestions(persona, reg):
    data = {
        "Leadership": [f"Vendor performance in {reg}", f"Top vendors by spend in {reg}", "Risk summary across all vendors"],
        "Finance": [f"Total spend per vendor in {reg}", f"Deal value breakdown", "High cost vendor analysis"],
        "Operations": [f"Delayed tasks in {reg}", f"Work order status by vendor", "Content readiness inventory"]
    }
    return data.get(persona, [f"Inventory in {reg}"])

# SIDEBAR
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.caption("Intelligence Engine v4.0")
    st.divider()
    st.session_state.current_region = st.selectbox("Market Focus", ["NA", "APAC", "EMEA", "LATAM"])
    persona = st.selectbox("Intelligence View", ["Leadership", "Finance", "Operations"])
    
    st.divider()
    st.subheader("💡 Suggested Insights")
    for i, sug in enumerate(get_persona_suggestions(persona, st.session_state.current_region)):
        if st.button(sug, key=f"side_{i}", use_container_width=True):
            st.session_state.pending_prompt = sug
            st.rerun()

# MAIN INTERFACE
st.title(f"🔍 {persona} Dashboard: {st.session_state.current_region}")

# Render Chat History
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"): st.write(msg["question"])
    with st.chat_message("assistant", avatar="🎥"):
        st.write(msg["answer"])
        st.plotly_chart(msg["chart"], use_container_width=True, key=f"hist_{i}")
        with st.expander("Explore Data Source"): st.dataframe(msg["data"], use_container_width=True)

# Query Input
user_input = st.chat_input("Search vendors, spend, or operational delays...")
active_prompt = st.session_state.get('pending_prompt') or user_input
if st.session_state.get('pending_prompt'): del st.session_state.pending_prompt

if active_prompt:
    with st.chat_message("user"): st.write(active_prompt)
    with st.chat_message("assistant", avatar="🎥"):
        sql, _, c_type = parse_query(active_prompt, st.session_state.current_region)
        res_df, _ = execute_sql(sql, DB_CONN) if sql else (None, None)

        if res_df is not None and not res_df.empty:
            # Impact KPI
            if any(c in res_df.columns for c in ["total_spend", "spend_at_risk"]):
                val = res_df.iloc[:, 1].sum()
                st.metric("Total Financial Impact", f"${val:,.2f}", delta="Financial View")

            # Chart Logic
            x, y = res_df.columns[0], res_df.columns[1] if len(res_df.columns) > 1 else res_df.columns[0]
            fig = px.bar(res_df, x=x, y=y, color=x, template="plotly_white") if c_type == "bar" else px.pie(res_df, names=x, hole=0.4)
            
            st.plotly_chart(fig, use_container_width=True)
            st.session_state.chat_history.append({"question": active_prompt, "answer": "Analysis Complete:", "data": res_df, "chart": fig})
            
            # Smooth Scroll Injection
            components.html("<script>window.parent.document.querySelector('section.main').scrollTo({top: 1000, behavior:'smooth'});</script>", height=0)
            st.rerun()
        else:
            st.error("No records found. Please try a different query or use the suggestions in the sidebar.")

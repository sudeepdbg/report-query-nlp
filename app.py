import streamlit as st
import pandas as pd
import plotly.express as px
import streamlit.components.v1 as components
import time
from utils.database import init_database, execute_sql
from utils.query_parser import parse_query

st.set_page_config(page_title="Foundry Vantage", page_icon="🎥", layout="wide")

@st.cache_resource
def get_db():
    return init_database()

DB_CONN = get_db()

# 1. Initialize Session States
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if 'current_region' not in st.session_state:
    st.session_state.current_region = 'APAC'
if 'persona' not in st.session_state:
    st.session_state.persona = 'Leadership'

# 2. PRE-RENDER LOGIC
user_input = st.chat_input("Ask about deals, vendors, or work orders...")

active_prompt = None
if st.session_state.get('pending_prompt'):
    active_prompt = st.session_state.pending_prompt
    del st.session_state.pending_prompt
elif user_input:
    active_prompt = user_input

# 3. SIDEBAR
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.divider()
    
    market_options = ["NA", "APAC", "EMEA", "LATAM"]
    st.session_state.current_region = st.selectbox(
        "Market Region", 
        market_options,
        index=market_options.index(st.session_state.current_region)
    )

    persona_options = ["Leadership", "Product", "Operations", "Finance"]
    st.session_state.persona = st.selectbox("View Persona", persona_options, index=persona_options.index(st.session_state.persona))

    st.divider()
    st.subheader(f"💡 {st.session_state.persona} Suggestions")
    
    def get_persona_suggestions(persona, reg):
        prompts = {
            "Leadership": [f"Top vendors in {reg}", f"Market value overview for {reg}", f"Vendor performance {reg}"],
            "Product": [f"Show SVOD rights in {reg}", f"Rights scope breakdown {reg}", f"Content readiness {reg}"],
            "Operations": [f"Work order status {reg}", f"Delayed tasks {reg}", f"Duplo queue status {reg}"],
            "Finance": [f"Total spend per vendor in {reg}", f"Highest cost deals {reg}", f"SVOD spend breakdown {reg}"]
        }
        return prompts.get(persona, prompts["Leadership"])

    for i, sug in enumerate(get_persona_suggestions(st.session_state.persona, st.session_state.current_region)):
        if st.button(sug, use_container_width=True, key=f"sug_btn_{i}"):
            st.session_state.pending_prompt = sug
            st.rerun()

# 4. RENDER INTERFACE
st.title(f"🔍 {st.session_state.persona} Insights")

# Render History
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"):
        st.write(msg["question"])
    with st.chat_message("assistant", avatar="🎥"):
        if msg.get("metrics"):
            m1, m2 = st.columns(2)
            m1.metric(msg["metrics"][0]["label"], msg["metrics"][0]["value"])
            m2.metric(msg["metrics"][1]["label"], msg["metrics"][1]["value"])
        st.plotly_chart(msg["chart"], use_container_width=True, key=f"hist_chart_{i}")
        with st.expander("View Records"):
            st.dataframe(msg["data"], use_container_width=True)

# 5. PROCESS NEW QUERY
if active_prompt:
    with st.chat_message("user"):
        st.write(active_prompt)
        
    with st.chat_message("assistant", avatar="🎥"):
        active_reg = st.session_state.current_region
        sql, error, chart_type = parse_query(active_prompt, active_reg)
        
        if sql:
            res_df, _ = execute_sql(sql, DB_CONN)
            
            if res_df is not None and not res_df.empty:
                x_col = res_df.columns[0]
                y_col = res_df.columns[1] if len(res_df.columns) > 1 else res_df.columns[0]
                
                if chart_type == "pie":
                    fig = px.pie(res_df, names=x_col, values=y_col if x_col != y_col else None, title=f"Distribution: {active_reg}", hole=0.4)
                else:
                    fig = px.bar(res_df, x=x_col, y=y_col, title=f"Analysis: {active_reg}", color=x_col)

                # Metrics logic
                metrics_data = None
                if "total_value" in res_df.columns or "deal_value" in res_df.columns:
                    val_col = "total_value" if "total_value" in res_df.columns else "deal_value"
                    v_sum = f"${res_df[val_col].sum():,.0f}"
                    v_avg = f"${res_df[val_col].mean():,.0f}"
                    m1, m2 = st.columns(2)
                    m1.metric("Total Value", v_sum)
                    m2.metric("Average Value", v_avg)
                    metrics_data = [{"label": "Total Value", "value": v_sum}, {"label": "Average Value", "value": v_avg}]
                
                st.plotly_chart(fig, use_container_width=True)
                
                st.session_state.chat_history.append({
                    "question": active_prompt, "data": res_df, "chart": fig, "metrics": metrics_data
                })
                
                # Auto Scroll
                components.html("<script>window.parent.document.querySelector('section.main').scrollTo(0, 10000);</script>", height=0)
                st.rerun()
            else:
                st.warning(f"No records found for '{active_prompt}' in {active_reg}. Check if data exists for this specific filter.")
        else:
            st.error("I couldn't parse that request. Try using keywords like 'Top vendors', 'Spend', or 'Ready status'.")

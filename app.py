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
    st.session_state.persona = 'Product'

# 2. PRE-RENDER LOGIC
user_input = st.chat_input("Ask about deals, vendors, or work orders...")

active_prompt = None
if st.session_state.get('pending_prompt'):
    active_prompt = st.session_state.pending_prompt
    del st.session_state.pending_prompt
elif user_input:
    active_prompt = user_input

# Auto-detect region to keep Sidebar and Query in sync
if active_prompt:
    for r in ["NA", "APAC", "EMEA", "LATAM"]:
        if r.lower() in active_prompt.lower():
            st.session_state.current_region = r
            break

# 3. SIDEBAR
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.divider()
    
    market_options = ["NA", "APAC", "EMEA", "LATAM"]
    st.session_state.current_region = st.selectbox(
        "Market Region", 
        market_options,
        index=market_options.index(st.session_state.current_region),
        key=f"sb_reg_{st.session_state.current_region}"
    )

    persona_options = ["Leadership", "Product", "Operations", "Finance"]
    st.session_state.persona = st.selectbox("View Persona", persona_options, index=persona_options.index(st.session_state.persona))

    st.divider()
    st.subheader(f"💡 {st.session_state.persona} Queries")
    def get_persona_suggestions(persona, reg):
        prompts = {
            "Leadership": [f"Top vendors in {reg}", f"Market value overview for {reg}"],
            "Product": [f"Show SVOD rights in {reg}", f"Rights scope breakdown {reg}"],
            "Operations": [f"Work order status {reg}", f"Delayed tasks {reg}"],
            "Finance": [f"Total spend per vendor in {reg}", f"Highest cost deals {reg}"]
        }
        return prompts.get(persona, prompts["Product"])

    for i, sug in enumerate(get_persona_suggestions(st.session_state.persona, st.session_state.current_region)):
        if st.button(sug, width='stretch', key=f"sug_btn_{i}"):
            st.session_state.pending_prompt = sug
            st.rerun()

# 4. RENDER HISTORY
st.title(f"🔍 {st.session_state.persona} Insights")

for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"):
        st.write(msg["question"])
    with st.chat_message("assistant", avatar="🎥"):
        st.write(msg["answer"])
        if msg.get("metrics"):
            m1, m2 = st.columns(2)
            m1.metric(msg["metrics"][0]["label"], msg["metrics"][0]["value"])
            m2.metric(msg["metrics"][1]["label"], msg["metrics"][1]["value"])
        if msg["chart"]:
            st.plotly_chart(msg["chart"], use_container_width=True, key=f"hist_chart_{i}")
        with st.expander("View Records"):
            st.dataframe(msg["data"], use_container_width=True, key=f"hist_df_{i}")

# 5. PROCESS NEW QUERY
# 5. PROCESS NEW QUERY (With Added Suggestions)
if active_prompt:
    with st.chat_message("user"):
        st.write(active_prompt)
        
    with st.chat_message("assistant", avatar="🎥"):
        active_reg = st.session_state.current_region
        
        with st.spinner(f"Querying {active_reg}..."):
            sql, error, chart_type = parse_query(active_prompt, active_reg)
            res_df, _ = execute_sql(sql, DB_CONN)
            
            if res_df is not None and not res_df.empty:
                # ... [Keep your existing successful rendering logic here] ...
                st.rerun()
            else:
                # --- NEW SUGGESTION LOGIC ---
                st.warning(f"No records found for '{active_prompt}' in {active_reg}.")
                st.write("### 💡 Try these instead:")
                
                # Generate dynamic suggestions based on Persona
                fallback_suggestions = get_persona_suggestions(st.session_state.persona, active_reg)
                
                cols = st.columns(len(fallback_suggestions))
                for idx, suggestion in enumerate(fallback_suggestions):
                    if cols[idx].button(suggestion, key=f"fail_sug_{idx}"):
                        st.session_state.pending_prompt = suggestion
                        st.rerun()
                
                st.info("Tip: Ensure the region mentioned in your question matches the 'Market Region' selected in the sidebar.")
        

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
if 'chat_history' not in st.session_state: st.session_state.chat_history = []
if 'current_region' not in st.session_state: st.session_state.current_region = 'APAC'
if 'persona' not in st.session_state: st.session_state.persona = 'Product'

# 2. HELPER: Persona Suggestions
def get_persona_suggestions(persona, reg):
    prompts = {
        "Leadership": [f"Top vendors in {reg}", f"Market value overview for {reg}"],
        "Product": [f"Show SVOD rights in {reg}", f"Rights scope breakdown {reg}"],
        "Operations": [f"Work order status {reg}", f"Delayed tasks {reg}"],
        "Finance": [f"Total spend per vendor in {reg}", f"Highest cost deals {reg}"]
    }
    return prompts.get(persona, prompts["Product"])

# 3. PRE-RENDER LOGIC (Catch Input)
user_input = st.chat_input("Ask about deals, vendors, or work orders...")
active_prompt = st.session_state.get('pending_prompt') or user_input
if st.session_state.get('pending_prompt'): del st.session_state.pending_prompt

# 4. SIDEBAR
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.divider()
    
    market_options = ["NA", "APAC", "EMEA", "LATAM"]
    st.session_state.current_region = st.selectbox("Market Region", market_options, 
        index=market_options.index(st.session_state.current_region))
    
    persona_options = ["Leadership", "Product", "Operations", "Finance"]
    st.session_state.persona = st.selectbox("View Persona", persona_options, 
        index=persona_options.index(st.session_state.persona))

    st.divider()
    st.subheader(f"💡 {st.session_state.persona} Suggestions")
    for i, sug in enumerate(get_persona_suggestions(st.session_state.persona, st.session_state.current_region)):
        if st.button(sug, width=280, key=f"sidebar_sug_{i}"):
            st.session_state.pending_prompt = sug
            st.rerun()

# 5. RENDER MAIN INTERFACE
st.title(f"🔍 {st.session_state.persona} Insights")

# Render History with UNIQUE KEYS
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"): st.write(msg["question"])
    with st.chat_message("assistant", avatar="🎥"):
        st.write(msg["answer"])
        if msg["chart"]:
            st.plotly_chart(msg["chart"], use_container_width=True, key=f"hist_chart_{i}_{time.time()}")
        with st.expander("View Records"): 
            st.dataframe(msg["data"], use_container_width=True, key=f"hist_df_{i}")

# 6. PROCESS NEW QUERY
if active_prompt:
    with st.chat_message("user"): st.write(active_prompt)
    with st.chat_message("assistant", avatar="🎥"):
        active_reg = st.session_state.current_region
        sql, err, c_type = parse_query(active_prompt, active_reg)
        
        res_df = None
        if sql: res_df, _ = execute_sql(sql, DB_CONN)
        
        if res_df is not None and not res_df.empty:
            # Dynamic Column Mapping
            x_col, y_col = res_df.columns[0], res_df.columns[1] if len(res_df.columns) > 1 else res_df.columns[0]
            
            if c_type == "bar":
                fig = px.bar(res_df, x=x_col, y=y_col, title=f"Analysis: {active_reg}", color=x_col)
            else:
                fig = px.pie(res_df, names=x_col, title=f"Distribution: {active_reg}", hole=0.4)
            
            st.plotly_chart(fig, use_container_width=True, key=f"new_res_chart_{time.time()}")
            
            st.session_state.chat_history.append({"question": active_prompt, "answer": "Displaying Results:", "data": res_df, "chart": fig})
            
            # JS SCROLL FIX
            components.html(
                """<script>
                var main = window.parent.document.querySelector('section.main');
                main.scrollTo({ top: main.scrollHeight, behavior: 'smooth' });
                </script>""", height=0
            )
            st.rerun()
        else:
            # --- SUGGESTION ENGINE ON FAIL ---
            st.warning(f"No records found for '{active_prompt}' in {active_reg}.")
            st.write("### 💡 Try one of these validated queries:")
            fallbacks = get_persona_suggestions(st.session_state.persona, active_reg)
            for j, f_sug in enumerate(fallbacks):
                if st.button(f_sug, key=f"fail_sug_{j}", use_container_width=True):
                    st.session_state.pending_prompt = f_sug
                    st.rerun()

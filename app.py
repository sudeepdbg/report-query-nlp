import streamlit as st
import pandas as pd
import plotly.express as px
import streamlit.components.v1 as components
from utils.database import init_database, execute_sql
from utils.query_parser import parse_query

st.set_page_config(page_title="Foundry Vantage", page_icon="🎥", layout="wide")

@st.cache_resource
def get_db():
    return init_database()

DB_CONN = get_db()

# Session State Initialization
if 'chat_history' not in st.session_state: st.session_state.chat_history = []
if 'current_region' not in st.session_state: st.session_state.current_region = 'APAC'
if 'persona' not in st.session_state: st.session_state.persona = 'Product'

# --- 1. SIDEBAR ---
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
    st.subheader(f"💡 {st.session_state.persona} Queries")
    
    # Dynamic button generation
    prompts = {
        "Leadership": ["Top vendors in APAC", "Market value overview APAC"],
        "Finance": ["Total spend per vendor in APAC", "Highest cost deals APAC"],
        "Product": ["Show SVOD rights in APAC", "Rights scope breakdown APAC"],
        "Operations": ["Work order status APAC", "Delayed tasks APAC"]
    }
    
    for sug in prompts.get(st.session_state.persona, prompts["Product"]):
        if st.button(sug, width=280):
            st.session_state.active_prompt = sug
            st.rerun()

# --- 2. MAIN INTERFACE ---
st.title(f"🔍 {st.session_state.persona} Insights")

# Render Chat History
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"): st.write(msg["question"])
    with st.chat_message("assistant", avatar="🎥"):
        st.write(msg["answer"])
        if msg["chart"]: st.plotly_chart(msg["chart"], use_container_width=True)
        with st.expander("View Records"): st.dataframe(msg["data"], use_container_width=True)

# Handle Input
prompt = st.chat_input("Ask about deals, vendors, or work orders...")
if st.session_state.get("active_prompt"):
    prompt = st.session_state.active_prompt
    del st.session_state.active_prompt

if prompt:
    with st.chat_message("user"): st.write(prompt)
    with st.chat_message("assistant", avatar="🎥"):
        sql, err, c_type = parse_query(prompt, st.session_state.current_region)
        df, db_err = execute_sql(sql, DB_CONN)
        
        if df is not None and not df.empty:
            # Dynamic Column Mapping for Charts
            x_col = df.columns[0]
            y_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
            
            if c_type == "bar":
                fig = px.bar(df, x=x_col, y=y_col, title=f"Analysis: {st.session_state.current_region}")
            else:
                fig = px.pie(df, names=x_col, title=f"Distribution: {st.session_state.current_region}", hole=0.4)
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Save to history
            st.session_state.chat_history.append({"question": prompt, "answer": "Results found:", "data": df, "chart": fig})
            
            # JS Scroll Fix
            components.html(
                """
                <script>
                var main = window.parent.document.querySelector('section.main');
                main.scrollTo({ top: main.scrollHeight, behavior: 'smooth' });
                </script>
                """, height=0
            )
            st.rerun()
        else:
            st.warning(f"No records found for '{prompt}' in {st.session_state.current_region}.")

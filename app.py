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

# 2. SIDEBAR
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.divider()
    
    st.session_state.current_region = st.selectbox(
        "Market Region", ["NA", "APAC", "EMEA", "LATAM"],
        key="region_selector"
    )

    persona = st.selectbox("View Persona", ["Leadership", "Product", "Operations", "Finance"])

    st.divider()
    st.subheader(f"💡 Suggestions")
    
    # Matching sidebar strings EXACTLY to parser keywords
    prompts = {
        "Leadership": ["Top vendors", "Market value", "Performance"],
        "Product": ["Content readiness", "SVOD rights", "Inventory"],
        "Operations": ["Delayed tasks", "Work orders", "Performance"],
        "Finance": ["Total spend", "Highest cost", "Deal value"]
    }
    
    for i, sug in enumerate(prompts.get(persona, [])):
        # Added unique key for buttons to prevent duplicate IDs in sidebar
        if st.button(sug, use_container_width=True, key=f"btn_{persona}_{i}"):
            st.session_state.active_query = sug
            st.rerun()

# 3. MAIN INTERFACE
st.title(f"🔍 {persona} Intelligence")

# Render History with unique chart keys
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"):
        st.write(msg["q"])
    with st.chat_message("assistant"):
        # Unique key is critical here to avoid StreamlitDuplicateElementId
        st.plotly_chart(msg["fig"], use_container_width=True, key=f"hist_{i}_{time.time()}")

# 4. PROCESS NEW QUERY
user_input = st.chat_input("Ask a question...")
query_to_run = st.session_state.get('active_query') or user_input

if query_to_run:
    if 'active_query' in st.session_state:
        del st.session_state.active_query
        
    sql, err, c_type = parse_query(query_to_run, st.session_state.current_region)
    res_df, db_err = execute_sql(sql, DB_CONN)

    if res_df is not None and not res_df.empty:
        with st.chat_message("assistant"):
            # Metric logic
            if "total_value" in res_df.columns:
                st.metric("Total Value", f"${res_df['total_value'].sum():,.0f}")
            
            # Dynamic Charting
            label_col, val_col = res_df.columns[0], res_df.columns[1]
            if c_type == "bar":
                fig = px.bar(res_df, x=label_col, y=val_col, color=label_col, template="plotly_white")
            else:
                fig = px.pie(res_df, names=label_col, values=val_col, hole=0.4)
            
            # Use timestamp in key to guarantee uniqueness
            st.plotly_chart(fig, use_container_width=True, key=f"new_{time.time()}")
            st.session_state.chat_history.append({"q": query_to_run, "fig": fig})
            
            # Auto Scroll
            components.html("<script>window.parent.document.querySelector('section.main').scrollTo(0,10000);</script>", height=0)
            st.rerun()
    else:
        st.error(f"No results for '{query_to_run}' in {st.session_state.current_region}.")

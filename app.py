import streamlit as st
import pandas as pd
import plotly.express as px
import streamlit.components.v1 as components
import time
from utils.database import init_database, execute_sql
from utils.query_parser import parse_query

st.set_page_config(page_title="Foundry Vantage", page_icon="🎥", layout="wide")

# Initialize Database with Denormalized Schema
@st.cache_resource
def get_db():
    return init_database()

DB_CONN = get_db()

# 1. SESSION STATE MANAGEMENT
# Using time-based seeds for history to prevent DuplicateElementId
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if 'market' not in st.session_state:
    st.session_state.market = 'APAC'
if 'view_persona' not in st.session_state:
    st.session_state.view_persona = 'Leadership'

# 2. SIDEBAR NAVIGATION
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.caption("Intelligence Engine v4.0")
    st.divider()
    
    # Market Selector (Updates global state)
    st.session_state.market = st.selectbox(
        "Market Focus", ["NA", "APAC", "EMEA", "LATAM"],
        key="sb_market_select"
    )

    st.session_state.view_persona = st.selectbox(
        "Intelligence View", ["Leadership", "Product", "Operations", "Finance"],
        key="sb_persona_select"
    )

    st.divider()
    st.subheader(f"💡 Suggested Insights")
    
    # Keywords here must match your query_parser clusters exactly
    persona_prompts = {
        "Leadership": ["Top vendors by spend", "Market value overview", "Vendor performance"],
        "Product": ["Content readiness status", "SVOD rights breakdown", "Inventory status"],
        "Operations": ["Delayed work orders", "Task status overview", "Vendor performance"],
        "Finance": ["Total spend per vendor", "Highest value deals", "Spend by rights type"]
    }
    
    # Fix: Unique keys for every button prevents Duplicate ID errors in sidebar
    for i, sug in enumerate(persona_prompts.get(st.session_state.view_persona, [])):
        if st.button(f"{sug} in {st.session_state.market}", use_container_width=True, key=f"sug_btn_{i}_{st.session_state.market}"):
            st.session_state.active_input = f"{sug} in {st.session_state.market}"
            st.rerun()

# 3. MAIN INTERFACE RENDERER
st.title(f"🔍 {st.session_state.view_persona} Intelligence")

# Render Chat History (Fix: Unique Keys for Plotly charts)
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"):
        st.write(msg["q"])
    with st.chat_message("assistant", avatar="🎥"):
        # We append the persona and index to the key to ensure no overlap
        st.plotly_chart(msg["fig"], use_container_width=True, key=f"hist_fig_{i}_{st.session_state.view_persona}")

# 4. QUERY PROCESSING
user_query = st.chat_input("Search vendors, spend, or operational delays...")
active_q = st.session_state.get('active_input') or user_query

if active_q:
    if 'active_input' in st.session_state:
        del st.session_state.active_input
        
    with st.chat_message("user"):
        st.write(active_q)
        
    with st.chat_message("assistant", avatar="🎥"):
        # 1. Parse NLP to SQL
        sql, err, chart_type = parse_query(active_q, st.session_state.market)
        
        # 2. Execute SQL against DB
        res_df, db_err = execute_sql(sql, DB_CONN)

        if res_df is not None and not res_df.empty:
            st.write("Analysis Complete:")
            
            # Setup Chart Columns
            x_col = res_df.columns[0]
            y_col = res_df.columns[1] if len(res_df.columns) > 1 else res_df.columns[0]
            
            # 3. Dynamic Visualization
            if chart_type == "pie":
                fig = px.pie(res_df, names=x_col, values=y_col, hole=0.4, color_discrete_sequence=px.colors.qualitative.Pastel)
            else:
                fig = px.bar(res_df, x=x_col, y=y_col, color=x_col, template="plotly_white")
            
            # FIX: Use a timestamp-based unique key to prevent ID Collision errors
            unique_chart_id = f"chart_{int(time.time() * 1000)}"
            st.plotly_chart(fig, use_container_width=True, key=unique_chart_id)
            
            with st.expander("Explore Data Source"):
                st.dataframe(res_df, use_container_width=True)
            
            # 4. Update History
            st.session_state.chat_history.append({"q": active_q, "fig": fig})
            
            # 5. Scroll Fix (Aggressive version)
            components.html("<script>window.parent.document.querySelector('section.main').scrollTo(0, 10000);</script>", height=0)
            st.rerun()
        else:
            st.error(f"No records found. Please try a different query or use the suggestions in the sidebar.")

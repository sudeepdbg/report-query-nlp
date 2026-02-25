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

# 1. SESSION STATE
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
    st.subheader("💡 Suggestions")
    
    prompts = {
        "Leadership": ["Top vendors", "Market value", "Performance"],
        "Product": ["Content readiness", "SVOD rights", "Inventory"],
        "Operations": ["Delayed tasks", "Work orders", "Performance"],
        "Finance": ["Total spend", "Highest cost", "Deal value"]
    }
    
    for i, sug in enumerate(prompts.get(persona, [])):
        if st.button(sug, use_container_width=True, key=f"sug_{persona}_{i}"):
            st.session_state.active_query = sug
            st.rerun()

# 3. MAIN INTERFACE
st.title(f"🔍 {persona} Intelligence")

# Render History (Includes restored Dataframe and Download)
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"):
        st.write(msg["q"])
    with st.chat_message("assistant"):
        st.plotly_chart(msg["fig"], use_container_width=True, key=f"hist_fig_{i}")
        with st.expander("View Records"):
            st.dataframe(msg["df"], use_container_width=True)

# 4. PROCESS NEW QUERY
user_input = st.chat_input("Ask about deals, vendors, or work orders...")
query_to_run = st.session_state.get('active_query') or user_input

if query_to_run:
    if 'active_query' in st.session_state:
        del st.session_state.active_query
        
    sql, err, c_type = parse_query(query_to_run, st.session_state.current_region)
    res_df, db_err = execute_sql(sql, DB_CONN)

    if res_df is not None and not res_df.empty:
        with st.chat_message("assistant"):
            # Metric logic for financials
            if any(col in res_df.columns for col in ["total_value", "deal_value"]):
                val_col = "total_value" if "total_value" in res_df.columns else "deal_value"
                st.metric("Total Impact", f"${res_df[val_col].sum():,.0f}")
            
            # Chart Rendering
            label_col, val_col = res_df.columns[0], res_df.columns[1] if len(res_df.columns) > 1 else res_df.columns[0]
            if c_type == "pie":
                fig = px.pie(res_df, names=label_col, values=val_col, hole=0.4)
            else:
                fig = px.bar(res_df, x=label_col, y=val_col, color=label_col, template="plotly_white")
            
            st.plotly_chart(fig, use_container_width=True, key=f"new_{time.time()}")
            
            # --- RESTORED TABULAR FEATURES ---
            st.subheader("Data Exploration")
            # 1. Tabular View with column selection/filtering
            st.dataframe(res_df, use_container_width=True)
            
            # 2. Download Option
            csv = res_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Data as CSV",
                data=csv,
                file_name=f"{query_to_run.replace(' ', '_')}.csv",
                mime='text/csv',
                key=f"dl_{time.time()}"
            )
            
            # Save to history including the dataframe
            st.session_state.chat_history.append({"q": query_to_run, "fig": fig, "df": res_df})
            
            # Scroll Fix
            components.html("<script>window.parent.document.querySelector('section.main').scrollTo(0,10000);</script>", height=0)
            st.rerun()
    else:
        st.error(f"No records found for '{query_to_run}' in {st.session_state.current_region}.")

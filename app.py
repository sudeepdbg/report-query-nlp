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
    st.caption("v4.0 Final Stable")
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
        # Sidebar buttons use the persona and market in the key to prevent ID collisions
        if st.button(sug, use_container_width=True, key=f"sug_{persona}_{i}_{st.session_state.current_region}"):
            st.session_state.active_query = sug
            st.rerun()

# 3. MAIN INTERFACE
st.title(f"🔍 {persona} Intelligence")

# Render History
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"):
        st.write(msg["q"])
    with st.chat_message("assistant"):
        st.plotly_chart(msg["fig"], use_container_width=True, key=f"hist_fig_{i}")
        # Restored the rich multi-field view in history
        with st.expander("View Detailed Records"):
            st.dataframe(msg["full_df"], use_container_width=True)

# 4. PROCESS NEW QUERY
user_input = st.chat_input("Ask about deals, vendors, or work orders...")
query_to_run = st.session_state.get('active_query') or user_input

if query_to_run:
    if 'active_query' in st.session_state:
        del st.session_state.active_query
        
    # Step A: Get Aggregated Data for the Chart
    sql, err, c_type = parse_query(query_to_run, st.session_state.current_region)
    chart_df, db_err = execute_sql(sql, DB_CONN)

    if chart_df is not None and not chart_df.empty:
        # Step B: FETCH FULL TRANSACTIONAL DATA (restores the multi-field table)
        # Identify source table based on query content
        target_table = "deals"
        low_q = query_to_run.lower()
        if any(x in low_q for x in ["task", "order", "performance", "delay"]):
            target_table = "work_orders"
        elif any(x in low_q for x in ["ready", "inventory", "status"]):
            target_table = "content_planning"

        full_sql = f"SELECT * FROM {target_table} WHERE UPPER(region) = '{st.session_state.current_region.upper()}'"
        full_df, _ = execute_sql(full_sql, DB_CONN)

        with st.chat_message("assistant"):
            # Metric logic
            if any(col in chart_df.columns for col in ["total_value", "deal_value"]):
                val_col = "total_value" if "total_value" in chart_df.columns else "deal_value"
                st.metric("Total Financial Impact", f"${chart_df[val_col].sum():,.0f}")
            
            # Chart Rendering
            label_col, val_col = chart_df.columns[0], chart_df.columns[1] if len(chart_df.columns) > 1 else chart_df.columns[0]
            if c_type == "pie":
                fig = px.pie(chart_df, names=label_col, values=val_col, hole=0.4)
            else:
                fig = px.bar(chart_df, x=label_col, y=val_col, color=label_col, template="plotly_white")
            
            # Fix: timestamp key prevents DuplicateElementId
            st.plotly_chart(fig, use_container_width=True, key=f"new_{time.time()}")
            
            # --- RESTORED RICH TABULAR FEATURES ---
            st.subheader("Data Exploration")
            # This shows the full multi-column table (date, scope, status, etc.)
            st.dataframe(full_df, use_container_width=True)
            
            # Download full dataset
            csv = full_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Full Report (CSV)",
                data=csv,
                file_name=f"{target_table}_detailed_report.csv",
                mime='text/csv',
                key=f"dl_{time.time()}"
            )
            
            # Save to history including the RICH dataframe
            st.session_state.chat_history.append({"q": query_to_run, "fig": fig, "full_df": full_df})
            
            # Scroll Fix
            components.html("<script>window.parent.document.querySelector('section.main').scrollTo(0,10000);</script>", height=0)
            st.rerun()
    else:
        st.error(f"No records found for '{query_to_run}' in {st.session_state.current_region}.")

import streamlit as st
import pandas as pd
import plotly.express as px
import streamlit.components.v1 as components
import time
from utils.database import init_database, execute_sql
from utils.query_parser import parse_query
from utils.tableau_sync import trigger_tableau_report 

st.set_page_config(page_title="Foundry Vantage", page_icon="🎥", layout="wide")

@st.cache_resource
def get_db():
    return init_database()
DB_CONN = get_db()

# --- 1. STATE MANAGEMENT ---
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []

# Callback function to force clean state when sidebar changes
def on_sidebar_change():
    st.session_state.chat_history = []  # Optional: Clear history to avoid region mixing
    # No need to do more, Streamlit reruns automatically

# --- 2. SIDEBAR (The Master Source) ---
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.caption("v6.2 Stable Enterprise")
    
    # Use on_change to ensure the app acknowledges the new filter immediately
    region = st.selectbox("Market Region", ["APAC", "EMEA", "NA", "LATAM"], 
                          key="sb_region", on_change=on_sidebar_change)
    persona = st.selectbox("View Persona", ["Leadership", "Product", "Operations", "Finance"], 
                           key="sb_persona")
    
    st.divider()
    if st.button("🗑️ Clear All", width="stretch"):
        st.session_state.chat_history = []
        st.rerun()

# --- 3. EXECUTION ENGINE ---
user_input = st.chat_input("Ask about deals, vendors, or work orders...")

# If the user clicks a suggestion or types a query
if user_input:
    # IMPORTANT: We use st.session_state.sb_region to guarantee the filter is fresh
    current_region = st.session_state.sb_region
    
    sql, _, c_type = parse_query(user_input, current_region)
    chart_df, _ = execute_sql(sql, DB_CONN)

    if chart_df is not None and not chart_df.empty:
        # Determine target table for Tableau data
        target = "work_orders" if "performance" in user_input.lower() else "deals"
        full_sql = f"SELECT * FROM {target} WHERE UPPER(region) = '{current_region.upper()}'"
        full_df, _ = execute_sql(full_sql, DB_CONN)

        # Plotly Logic
        if c_type == "pie":
            fig = px.pie(chart_df, names=chart_df.columns[0], values=chart_df.columns[1], hole=0.5)
        else:
            fig = px.bar(chart_df, y=chart_df.columns[0], x=chart_df.columns[1], orientation='h')
        
        # Save unique record
        st.session_state.chat_history.append({
            "query": user_input,
            "fig": fig,
            "full_df": full_df,
            "region": current_region,
            "id": time.time()
        })

# --- 4. RENDER UI ---
st.title(f"🔍 {st.session_state.sb_persona} Intelligence: {st.session_state.sb_region}")

for i, entry in enumerate(st.session_state.chat_history):
    with st.chat_message("user"):
        st.write(f"**{entry['query']}** (Region: {entry['region']})")
    
    with st.chat_message("assistant"):
        st.plotly_chart(entry["fig"], width="stretch", key=f"p_{entry['id']}")
        
        # Enterprise Actions
        st.markdown("### 📊 Enterprise Actions")
        c1, c2 = st.columns([3, 1])
        t_name = c1.text_input("Tableau Name", value=f"Report_{entry['region']}_{i}", key=f"in_{entry['id']}")
        
        if c2.button("🚀 Push to Tableau", key=f"btn_{entry['id']}", width="stretch"):
            with st.spinner("Pushing to Cloud..."):
                # Pass a COPY of the dataframe to avoid pointer errors
                success, msg = trigger_tableau_report(entry["full_df"].copy(), t_name)
                if success: st.success("Pushed Successfully!")
                else: st.error(f"Tableau Error: {msg}")

# --- 5. SCROLL ---
if user_input:
    components.html(
        f"""<script>
        var main = window.parent.document.querySelector('section.main');
        main.scrollTo({{ top: main.scrollHeight, behavior: 'smooth' }});
        </script>""", height=0
    )

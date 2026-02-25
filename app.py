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

if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []

# --- 1. SIDEBAR (Master Filter) ---
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.caption("v6.0 Enterprise Ready")
    
    # Selection directly updates session_state
    region = st.selectbox("Market Region", ["APAC", "EMEA", "NA", "LATAM"], key="sb_region")
    persona = st.selectbox("View Persona", ["Leadership", "Product", "Operations", "Finance"], key="sb_persona")
    
    st.divider()
    if st.button("Clear Chat", width="stretch"):
        st.session_state.chat_history = []
        st.rerun()

# --- 2. EXECUTION ---
chat_input = st.chat_input("Ask about deals, vendors, or work orders...")

if chat_input:
    # Use the SIDEBAR region for the parser
    sql, _, c_type = parse_query(chat_input, st.session_state.sb_region)
    chart_df, _ = execute_sql(sql, DB_CONN)

    if chart_df is not None and not chart_df.empty:
        # Determine target table
        target = "work_orders" if "performance" in chat_input.lower() else "deals"
        full_sql = f"SELECT * FROM {target} WHERE UPPER(region) = '{st.session_state.sb_region.upper()}'"
        full_df, _ = execute_sql(full_sql, DB_CONN)

        # Plot
        if c_type == "pie":
            fig = px.pie(chart_df, names=chart_df.columns[0], values=chart_df.columns[1], hole=0.5)
        else:
            fig = px.bar(chart_df, y=chart_df.columns[0], x=chart_df.columns[1], orientation='h')
        
        # Save
        st.session_state.chat_history.append({
            "query": chat_input,
            "fig": fig,
            "full_df": full_df,
            "region": st.session_state.sb_region,
            "id": time.time()
        })

# --- 3. DISPLAY FEED ---
st.title(f"🔍 {st.session_state.sb_persona} Intelligence: {st.session_state.sb_region}")

for i, entry in enumerate(st.session_state.chat_history):
    with st.chat_message("user"):
        st.write(entry["query"])
    
    with st.chat_message("assistant"):
        st.plotly_chart(entry["fig"], width="stretch", key=f"plot_{entry['id']}")
        
        st.markdown("### 📊 Enterprise Actions")
        c1, c2 = st.columns([3, 1])
        t_name = c1.text_input("Report Name", value=f"Report_{i}", key=f"in_{entry['id']}")
        if c2.button("🚀 Push to Tableau", key=f"pb_{entry['id']}", width="stretch"):
            with st.spinner("Pushing..."):
                success, msg = trigger_tableau_report(entry["full_df"], t_name)
                if success: st.success(msg)
                else: st.error(msg)
        
        with st.expander("📝 View Records"):
            st.dataframe(entry["full_df"], width="stretch")

# --- 4. SCROLL FIX ---
if chat_input:
    components.html(
        f"""<script>
        var main = window.parent.document.querySelector('section.main');
        main.scrollTo({{ top: main.scrollHeight, behavior: 'smooth' }});
        </script>""", height=0
    )

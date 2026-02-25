import streamlit as st
import pandas as pd
import plotly.express as px
import streamlit.components.v1 as components
from utils.database import init_database, execute_sql
from utils.query_parser import parse_query

st.set_page_config(page_title="Foundry Vantage", layout="wide")

@st.cache_resource
def get_db(): return init_database()
DB_CONN = get_db()

# Session Management
if 'chat_history' not in st.session_state: st.session_state.chat_history = []
if 'current_region' not in st.session_state: st.session_state.current_region = 'APAC'

# Sidebar UI
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.session_state.current_region = st.selectbox("Market Region", ["NA", "APAC", "EMEA", "LATAM"])
    persona = st.selectbox("View Persona", ["Leadership", "Finance", "Operations"])
    
    st.divider()
    st.subheader("💡 Quick Insights")
    
    # Pre-defined working queries
    suggestions = {
        "Leadership": ["Top vendors by spend", "Vendor delay analysis"],
        "Finance": ["Total spend per vendor", "High value deals"],
        "Operations": ["Delayed work orders", "Content readiness status"]
    }
    
    for sug in suggestions[persona]:
        if st.button(sug, use_container_width=True):
            st.session_state.pending_prompt = f"{sug} in {st.session_state.current_region}"
            st.rerun()

# Main Chat
st.title(f"🔍 {persona} Intelligence")

# Display History
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"): st.write(msg["question"])
    with st.chat_message("assistant"):
        st.plotly_chart(msg["chart"], use_container_width=True)
        with st.expander("View Data"): st.dataframe(msg["data"], use_container_width=True)

# Input Logic
prompt = st.chat_input("Ask a question...")
active_prompt = st.session_state.get('pending_prompt') or prompt

if active_prompt:
    if 'pending_prompt' in st.session_state: del st.session_state.pending_prompt
    
    with st.chat_message("user"): st.write(active_prompt)
    
    sql, _, c_type = parse_query(active_prompt, st.session_state.current_region)
    res_df, _ = execute_sql(sql, DB_CONN)

    if res_df is not None and not res_df.empty:
        with st.chat_message("assistant"):
            # Metric Header
            if "total_spend" in res_df.columns:
                st.metric("Region Total Value", f"${res_df['total_spend'].sum():,.0f}")
            
            # Chart
            label_col, val_col = res_df.columns[0], res_df.columns[1]
            if c_type == "bar":
                fig = px.bar(res_df, x=label_col, y=val_col, template="plotly_white", color=label_col)
            else:
                fig = px.pie(res_df, names=label_col, values=val_col, hole=0.4)
            
            st.plotly_chart(fig, use_container_width=True)
            st.session_state.chat_history.append({"question": active_prompt, "chart": fig, "data": res_df})
            
            # Auto-Scroll
            components.html("<script>window.parent.document.querySelector('section.main').scrollTo(0,10000);</script>", height=0)
            st.rerun()
    else:
        st.error("No records found for this query in this region. Try a suggested query from the sidebar.")

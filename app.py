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

# --- 1. SESSION STATE ---
if 'current_region' not in st.session_state:
    st.session_state.current_region = 'APAC'
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []

# --- 2. SIDEBAR ---
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.caption("v5.3 Enterprise Stable")
    
    # Simple region selection without forced reruns
    region_options = ["NA", "APAC", "EMEA", "LATAM"]
    selected_reg = st.selectbox("Market Region", region_options, 
                                index=region_options.index(st.session_state.current_region))
    st.session_state.current_region = selected_reg

    persona = st.selectbox("View Persona", ["Leadership", "Product", "Operations", "Finance"])
    st.divider()
    
    st.subheader(f"💡 {persona} Suggestions")
    prompts = {
        "Leadership": ["Top vendors", "Market value", "Vendor performance"],
        "Finance": ["Total spend", "Highest cost deals"]
    }
    
    # suggestions set the chat input effectively
    for i, sug in enumerate(prompts.get(persona, ["Performance"])):
        if st.button(f"{sug} in {st.session_state.current_region}", key=f"sug_{i}"):
            st.session_state.active_query = f"{sug} in {st.session_state.current_region}"

# --- 3. MAIN UI ---
st.title(f"🔍 {persona} Intelligence: {st.session_state.current_region}")

# Render History First
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"): st.write(msg["q"])
    with st.chat_message("assistant"):
        st.plotly_chart(msg["fig"], width="stretch", key=f"hist_{i}")
        with st.expander("View Records"):
            st.dataframe(msg["full_df"], width="stretch")

# --- 4. EXECUTION ---
# Check both chat_input and the sidebar buttons
u_input = st.chat_input("Ask about deals, vendors, or work orders...")
query = u_input or st.session_state.get('active_query')

if query:
    # Clear the button trigger immediately
    if 'active_query' in st.session_state:
        del st.session_state.active_query

    # Generate SQL
    sql, _, c_type = parse_query(query, st.session_state.current_region)
    chart_df, _ = execute_sql(sql, DB_CONN)

    if chart_df is not None and not chart_df.empty:
        # Get full data for Tableau
        target_table = "work_orders" if "performance" in query.lower() else "deals"
        full_sql = f"SELECT * FROM {target_table} WHERE UPPER(region) = '{st.session_state.current_region.upper()}'"
        full_df, _ = execute_sql(full_sql, DB_CONN)

        with st.chat_message("assistant"):
            # A. Draw Chart
            if c_type == "pie":
                fig = px.pie(chart_df, names=chart_df.columns[0], values=chart_df.columns[1], hole=0.5)
            else:
                fig = px.bar(chart_df, y=chart_df.columns[0], x=chart_df.columns[1], orientation='h', color_continuous_scale='Viridis')
            
            st.plotly_chart(fig, width="stretch", key=f"active_chart_{time.time()}")
            
            # B. TABLEAU ACTION SECTION (No Expander - make it visible!)
            st.markdown("### 📊 Enterprise Actions")
            t_col1, t_col2 = st.columns([3, 1])
            with t_col1:
                t_name = st.text_input("Report Name", value=f"Export_{st.session_state.current_region}", key="t_in")
            with t_col2:
                st.write(" ") # spacer
                if st.button("🚀 Push to Tableau", key="t_btn", width="stretch"):
                    success, m = trigger_tableau_report(full_df, t_name)
                    if success: st.success("Pushed!")
                    else: st.error(m)

            # C. Data Table
            with st.expander("📝 Source Records"):
                st.dataframe(full_df, width="stretch")

            # Save to history and Scroll
            st.session_state.chat_history.append({"q": query, "fig": fig, "full_df": full_df})
            
            # Simple Scroll Script
            components.html("<script>window.parent.document.querySelector('section.main').scrollTo(0,10000);</script>", height=0)
            
            # Final Rerun to refresh history
            st.rerun()

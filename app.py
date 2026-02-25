import streamlit as st
import pandas as pd
import plotly.express as px
import streamlit.components.v1 as components
import time
from utils.database import init_database, execute_sql
from utils.query_parser import parse_query
from utils.tableau_sync import trigger_tableau_report 

st.set_page_config(page_title="Foundry Vantage", page_icon="🎥", layout="wide")

# Database Initialization
@st.cache_resource
def get_db():
    return init_database()
DB_CONN = get_db()

# Session State Initialization
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []

# --- 1. SIDEBAR (Master Control) ---
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.caption("v5.8 Stable Build")
    
    # Selection keys are automatically added to st.session_state
    st.selectbox("Market Region", ["APAC", "EMEA", "NA", "LATAM"], key="sb_region")
    st.selectbox("View Persona", ["Leadership", "Product", "Operations", "Finance"], key="sb_persona")
    
    st.divider()
    st.subheader(f"💡 Suggestions")
    
    for i, sug in enumerate(["Top vendors", "Market value", "Vendor performance"]):
        if st.button(f"{sug} in {st.session_state.sb_region}", key=f"sug_{i}"):
            st.session_state.run_query = f"{sug} in {st.session_state.sb_region}"

# --- 2. MAIN HEADER ---
st.title(f"🔍 {st.session_state.sb_persona} Intelligence: {st.session_state.sb_region}")

# --- 3. INPUT HANDLING ---
chat_input = st.chat_input("Ask about deals, vendors, or work orders...")
query = chat_input or st.session_state.get('run_query')

if query:
    if 'run_query' in st.session_state: del st.session_state.run_query
    
    # Query logic uses the SIDEBAR state directly
    sql, _, c_type = parse_query(query, st.session_state.sb_region)
    chart_df, _ = execute_sql(sql, DB_CONN)

    if chart_df is not None and not chart_df.empty:
        # Fetch detailed data for Tableau
        target = "work_orders" if "performance" in query.lower() else "deals"
        full_sql = f"SELECT * FROM {target} WHERE UPPER(region) = '{st.session_state.sb_region.upper()}'"
        full_df, _ = execute_sql(full_sql, DB_CONN)

        # Plotting
        if c_type == "pie":
            fig = px.pie(chart_df, names=chart_df.columns[0], values=chart_df.columns[1], hole=0.5)
        else:
            fig = px.bar(chart_df, y=chart_df.columns[0], x=chart_df.columns[1], orientation='h')
        fig.update_layout(margin=dict(l=10, r=10, t=30, b=10))

        # Add unique record to history
        st.session_state.chat_history.append({
            "query": query,
            "fig": fig,
            "full_df": full_df,
            "region": st.session_state.sb_region,
            "key": time.time()
        })

# --- 4. DISPLAY FEED ---
for i, entry in enumerate(st.session_state.chat_history):
    with st.chat_message("user"):
        st.write(entry["query"])
    
    with st.chat_message("assistant"):
        st.plotly_chart(entry["fig"], width="stretch", key=f"plot_{entry['key']}")
        
        # TABLEAU ACTIONS
        st.markdown("### 📊 Enterprise Actions")
        c1, c2 = st.columns([3, 1])
        with c1:
            r_name = st.text_input("Tableau Name", value=f"Report_{i}", key=f"txt_{entry['key']}")
        with c2:
            st.write(" ")
            if st.button("🚀 Push to Tableau", key=f"btn_{entry['key']}", width="stretch"):
                with st.spinner("Uploading..."):
                    success, msg = trigger_tableau_report(entry["full_df"], r_name)
                    if success: st.success("Done!")
                    else: st.error(msg)
        
        with st.expander("📝 View Records"):
            st.dataframe(entry["full_df"], width="stretch")

# --- 5. SCROLL FIX ---
if query:
    components.html(
        f"""<script>
        var main = window.parent.document.querySelector('section.main');
        main.scrollTo({{ top: main.scrollHeight, behavior: 'smooth' }});
        </script>""", height=0
    )

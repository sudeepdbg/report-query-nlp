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

# Initialize history
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []

# --- 1. SIDEBAR ---
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.caption("v5.9 Stable Build")
    
    # Use on_change to force a clean update when the user clicks
    region = st.selectbox("Market Region", ["APAC", "EMEA", "NA", "LATAM"], key="sb_region")
    persona = st.selectbox("View Persona", ["Leadership", "Product", "Operations", "Finance"], key="sb_persona")
    
    st.divider()
    st.subheader("💡 Suggestions")
    for i, sug in enumerate(["Top vendors", "Market value", "Vendor performance"]):
        if st.button(f"{sug} in {region}", key=f"btn_sug_{i}"):
            # Injecting the query directly into the session state
            st.session_state.current_query = f"{sug} in {region}"

# --- 2. QUERY LOGIC ---
chat_input = st.chat_input("Ask about deals, vendors, or work orders...")
query = chat_input or st.session_state.get('current_query')

if query:
    # Clear the suggestion so it doesn't loop
    if 'current_query' in st.session_state:
        del st.session_state.current_query
    
    # ALWAYS use the sidebar variable (region) here to ensure the filter works
    sql, _, c_type = parse_query(query, region)
    chart_df, _ = execute_sql(sql, DB_CONN)

    if chart_df is not None and not chart_df.empty:
        # Get full data for Tableau
        target = "work_orders" if "performance" in query.lower() else "deals"
        full_sql = f"SELECT * FROM {target} WHERE UPPER(region) = '{region.upper()}'"
        full_df, _ = execute_sql(full_sql, DB_CONN)

        # Plot
        if c_type == "pie":
            fig = px.pie(chart_df, names=chart_df.columns[0], values=chart_df.columns[1], hole=0.5)
        else:
            fig = px.bar(chart_df, y=chart_df.columns[0], x=chart_df.columns[1], orientation='h')
        
        # Save to history
        st.session_state.chat_history.append({
            "query": query,
            "fig": fig,
            "full_df": full_df,
            "region": region,
            "id": time.time()
        })

# --- 3. RENDER ---
st.title(f"🔍 {persona} Intelligence: {region}")

for i, entry in enumerate(st.session_state.chat_history):
    with st.chat_message("user"):
        st.write(entry["query"])
    
    with st.chat_message("assistant"):
        st.plotly_chart(entry["fig"], width="stretch", key=f"plot_{entry['id']}")
        
        st.markdown("### 📊 Enterprise Actions")
        c1, c2 = st.columns([3, 1])
        t_name = c1.text_input("Tableau Name", value=f"Report_{i}", key=f"in_{entry['id']}")
        if c2.button("🚀 Push to Tableau", key=f"pb_{entry['id']}", width="stretch"):
            with st.spinner("Syncing..."):
                success, msg = trigger_tableau_report(entry["full_df"], t_name)
                if success: st.success(msg)
                else: st.error(msg)
        
        with st.expander("📝 View Records"):
            st.dataframe(entry["full_df"], width="stretch")

# --- 4. SCROLL ---
if query:
    components.html(
        f"""<script>
        var main = window.parent.document.querySelector('section.main');
        main.scrollTo({{ top: main.scrollHeight, behavior: 'smooth' }});
        </script>""", height=0
    )

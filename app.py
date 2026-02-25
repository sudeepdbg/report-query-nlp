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

# --- 1. ROBUST SESSION STATE ---
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if 'last_query' not in st.session_state:
    st.session_state.last_query = None

# --- 2. SIDEBAR (The Source of Truth) ---
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.caption("v5.4 Stable Build")
    
    # We use a key here so the sidebar state is managed directly by Streamlit
    region = st.selectbox("Market Region", ["APAC", "EMEA", "NA", "LATAM"], key="sb_region")
    persona = st.selectbox("View Persona", ["Leadership", "Product", "Operations", "Finance"], key="sb_persona")
    
    st.divider()
    st.subheader(f"💡 {persona} Suggestions")
    
    # Suggestion buttons now update a temporary state instead of forcing a rerun
    prompts = ["Top vendors", "Market value", "Vendor performance"]
    for i, sug in enumerate(prompts):
        if st.button(f"{sug} in {region}", key=f"sug_btn_{i}"):
            st.session_state.active_suggestion = f"{sug} in {region}"

# --- 3. MAIN UI HEADER ---
st.title(f"🔍 {persona} Intelligence: {region}")

# --- 4. CHAT & EXECUTION ENGINE ---
# We check the chat input OR the suggestion buttons
user_input = st.chat_input("Ask about deals, vendors, or work orders...")
query = user_input or st.session_state.get('active_suggestion')

if query:
    # Clear the suggestion so it doesn't repeat
    if 'active_suggestion' in st.session_state:
        del st.session_state.active_suggestion
    
    # 1. Parse & Execute
    sql, _, c_type = parse_query(query, region)
    chart_df, _ = execute_sql(sql, DB_CONN)

    if chart_df is not None and not chart_df.empty:
        # Determine the full dataset for Tableau
        target_table = "work_orders" if "performance" in query.lower() else "deals"
        full_sql = f"SELECT * FROM {target_table} WHERE UPPER(region) = '{region.upper()}'"
        full_df, _ = execute_sql(full_sql, DB_CONN)

        # 2. Create Visualization
        if c_type == "pie":
            fig = px.pie(chart_df, names=chart_df.columns[0], values=chart_df.columns[1], hole=0.5)
        else:
            fig = px.bar(chart_df, y=chart_df.columns[0], x=chart_df.columns[1], orientation='h')
        fig.update_layout(margin=dict(l=20, r=20, t=40, b=20))

        # 3. Store in History
        st.session_state.chat_history.append({
            "query": query,
            "fig": fig,
            "full_df": full_df,
            "region": region
        })

# --- 5. RENDER FEED (History + Newest) ---
for i, entry in enumerate(st.session_state.chat_history):
    with st.chat_message("user"):
        st.write(entry["query"])
    
    with st.chat_message("assistant"):
        st.plotly_chart(entry["fig"], width="stretch", key=f"chart_{i}")
        
        # --- PERMANENT TABLEAU SECTION ---
        st.markdown("### 📊 Enterprise Actions")
        t_col1, t_col2 = st.columns([3, 1])
        with t_col1:
            report_name = st.text_input("Tableau Report Name", 
                                       value=f"Foundry_{entry['region']}_{i}", 
                                       key=f"t_name_{i}")
        with t_col2:
            st.write(" ") # alignment spacer
            if st.button("🚀 Push to Tableau", key=f"t_btn_{i}", width="stretch"):
                with st.spinner("Syncing..."):
                    success, msg = trigger_tableau_report(entry["full_df"], report_name)
                    if success: st.success("Pushed to Tableau!")
                    else: st.error(f"Error: {msg}")

        with st.expander("📝 View Raw Records"):
            st.dataframe(entry["full_df"], width="stretch")

# --- 6. AUTO-SCROLL FIX ---
if query:
    components.html(
        f"""<script>
        var mainSec = window.parent.document.querySelector('section.main');
        mainSec.scrollTo({{ top: mainSec.scrollHeight, behavior: 'smooth' }});
        </script>""", 
        height=0
    )

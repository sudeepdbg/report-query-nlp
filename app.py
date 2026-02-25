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

# --- 1. PERSISTENT STATE ---
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []

# --- 2. SIDEBAR (Source of Truth) ---
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.caption("v5.7 Stable Build")
    
    # We use 'key' to bind these directly to session_state
    st.selectbox("Market Region", ["APAC", "EMEA", "NA", "LATAM"], key="sb_region")
    st.selectbox("View Persona", ["Leadership", "Product", "Operations", "Finance"], key="sb_persona")
    
    st.divider()
    st.subheader(f"💡 {st.session_state.sb_persona} Suggestions")
    
    prompts = ["Top vendors", "Market value", "Vendor performance"]
    for i, sug in enumerate(prompts):
        # Clicked buttons now set a 'pending_query'
        if st.button(f"{sug} in {st.session_state.sb_region}", key=f"sug_btn_{i}"):
            st.session_state.pending_query = f"{sug} in {st.session_state.sb_region}"

# --- 3. MAIN UI HEADER ---
st.title(f"🔍 {st.session_state.sb_persona} Intelligence: {st.session_state.sb_region}")

# --- 4. EXECUTION ENGINE ---
user_input = st.chat_input("Ask about deals, vendors, or work orders...")
# Priority: 1. Manual Input, 2. Sidebar Suggestions
active_query = user_input or st.session_state.get('pending_query')

if active_query:
    # Reset suggestion state
    if 'pending_query' in st.session_state:
        del st.session_state.pending_query
    
    # Parse SQL using the CURRENT sidebar region
    sql, _, c_type = parse_query(active_query, st.session_state.sb_region)
    chart_df, _ = execute_sql(sql, DB_CONN)

    if chart_df is not None and not chart_df.empty:
        # Determine full dataset for Tableau
        target_table = "work_orders" if "performance" in active_query.lower() else "deals"
        full_sql = f"SELECT * FROM {target_table} WHERE UPPER(region) = '{st.session_state.sb_region.upper()}'"
        full_df, _ = execute_sql(full_sql, DB_CONN)

        # Create Visualization
        if c_type == "pie":
            fig = px.pie(chart_df, names=chart_df.columns[0], values=chart_df.columns[1], hole=0.5)
        else:
            fig = px.bar(chart_df, y=chart_df.columns[0], x=chart_df.columns[1], orientation='h')
        fig.update_layout(margin=dict(l=20, r=20, t=40, b=20))

        # Store in History with a timestamp key
        st.session_state.chat_history.append({
            "query": active_query,
            "fig": fig,
            "full_df": full_df,
            "region": st.session_state.sb_region,
            "ts": time.time()
        })

# --- 5. RENDER FEED (History + Results) ---
for i, entry in enumerate(st.session_state.chat_history):
    with st.chat_message("user"):
        st.write(entry["query"])
    
    with st.chat_message("assistant"):
        st.plotly_chart(entry["fig"], width="stretch", key=f"plotly_{entry['ts']}")
        
        # --- TABLEAU SECTION ---
        st.markdown("### 📊 Enterprise Actions")
        t_col1, t_col2 = st.columns([3, 1])
        with t_col1:
            r_name = st.text_input("Tableau Report Name", 
                                  value=f"Foundry_{entry['region']}_{i}", 
                                  key=f"t_name_{entry['ts']}")
        with t_col2:
            st.write(" ") # alignment
            if st.button("🚀 Push to Tableau", key=f"t_btn_{entry['ts']}", width="stretch"):
                with st.spinner("Syncing to Cloud..."):
                    success, msg = trigger_tableau_report(entry["full_df"], r_name)
                    if success: st.success("Pushed!")
                    else: st.error(f"Sync Failed: {msg}")

        with st.expander("📝 View Raw Records"):
            st.dataframe(entry["full_df"], width="stretch")

# --- 6. SCROLL FIX ---
# Always place this at the very bottom of the file
if active_query:
    components.html(
        f"""<script>
        var main = window.parent.document.querySelector('section.main');
        main.scrollTo({{ top: main.scrollHeight, behavior: 'smooth' }});
        </script>""", 
        height=0
    )

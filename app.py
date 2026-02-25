import streamlit as st
import pandas as pd
import plotly.express as px
import streamlit.components.v1 as components
import time
from utils.database import init_database, execute_sql
from utils.query_parser import parse_query
from utils.tableau_sync import trigger_tableau_report 

# Force Wide Mode and Clear deprecated warnings
st.set_page_config(page_title="Foundry Vantage", page_icon="🎥", layout="wide")

@st.cache_resource
def get_db():
    return init_database()

DB_CONN = get_db()

# --- 1. SESSION STATE (The Engine) ---
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if 'scroll_trigger' not in st.session_state:
    st.session_state.scroll_trigger = False

# --- 2. SIDEBAR (Source of Truth) ---
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.caption("v5.6 Enterprise Stable")
    
    # Using 'key' connects these directly to st.session_state
    region = st.selectbox("Market Region", ["APAC", "EMEA", "NA", "LATAM"], key="sb_region")
    persona = st.selectbox("View Persona", ["Leadership", "Product", "Operations", "Finance"], key="sb_persona")
    
    st.divider()
    st.subheader(f"💡 Suggestions")
    
    prompts = ["Top vendors", "Market value", "Vendor performance"]
    for i, sug in enumerate(prompts):
        # When clicked, we inject the suggestion directly into the logic
        if st.button(f"{sug} in {region}", key=f"sug_btn_{i}"):
            st.session_state.query_to_run = f"{sug} in {region}"

# --- 3. MAIN UI HEADER ---
st.title(f"🔍 {st.session_state.sb_persona} Intelligence: {st.session_state.sb_region}")

# --- 4. EXECUTION LOGIC ---
user_input = st.chat_input("Ask about deals, vendors, or work orders...")
active_query = user_input or st.session_state.get('query_to_run')

if active_query:
    # Clear the suggestion trigger
    if 'query_to_run' in st.session_state:
        del st.session_state.query_to_run
        
    # Generate SQL using current sidebar state
    sql, _, c_type = parse_query(active_query, st.session_state.sb_region)
    chart_df, _ = execute_sql(sql, DB_CONN)

    if chart_df is not None and not chart_df.empty:
        # Determine full dataset for Tableau export
        target_table = "work_orders" if any(x in active_query.lower() for x in ["performance", "task"]) else "deals"
        full_sql = f"SELECT * FROM {target_table} WHERE UPPER(region) = '{st.session_state.sb_region.upper()}'"
        full_df, _ = execute_sql(full_sql, DB_CONN)

        # Create Visualization
        if c_type == "pie":
            fig = px.pie(chart_df, names=chart_df.columns[0], values=chart_df.columns[1], hole=0.5)
        else:
            fig = px.bar(chart_df, y=chart_df.columns[0], x=chart_df.columns[1], orientation='h')
        
        fig.update_layout(height=400, margin=dict(l=10, r=10, t=30, b=10))

        # Save to history with unique ID
        st.session_state.chat_history.append({
            "id": time.time(),
            "query": active_query,
            "fig": fig,
            "full_df": full_df,
            "region": st.session_state.sb_region
        })
        st.session_state.scroll_trigger = True

# --- 5. RENDER FEED ---
# We loop through history to keep everything on screen
for i, entry in enumerate(st.session_state.chat_history):
    with st.chat_message("user"):
        st.write(entry["query"])
    
    with st.chat_message("assistant"):
        st.plotly_chart(entry["fig"], width="stretch", key=f"plotly_{entry['id']}")
        
        # TABLEAU SECTION
        st.markdown("### 📊 Enterprise Actions")
        t_col1, t_col2 = st.columns([3, 1])
        
        with t_col1:
            report_name = st.text_input("Report Name", 
                                       value=f"Foundry_{entry['region']}_{i}", 
                                       key=f"t_in_{entry['id']}")
        with t_col2:
            st.write(" ") # align button
            if st.button("🚀 Push to Tableau", key=f"t_btn_{entry['id']}", width="stretch"):
                with st.spinner("Publishing..."):
                    # Pass the specific dataframe from history
                    success, msg = trigger_tableau_report(entry["full_df"], report_name)
                    if success:
                        st.success("Successfully pushed to Tableau Cloud!")
                    else:
                        st.error(f"Sync Failed: {msg}")

        with st.expander("📝 View Raw Records"):
            st.dataframe(entry["full_df"], width="stretch")

# --- 6. JAVASCRIPT AUTO-SCROLL ---
# This executes at the bottom of the script to ensure all content is loaded
if st.session_state.scroll_trigger:
    components.html(
        """
        <script>
        var main = window.parent.document.querySelector('section.main');
        main.scrollTo({ top: main.scrollHeight, behavior: 'smooth' });
        </script>
        """,
        height=0
    )
    st.session_state.scroll_trigger = False # Reset trigger

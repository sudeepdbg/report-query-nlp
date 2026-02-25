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

# --- 1. GLOBAL STATE & SYNC ---
if 'current_region' not in st.session_state:
    st.session_state.current_region = 'APAC'
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []

def sync_region_from_query(query_text):
    """Force override global region state if mentioned in search."""
    regions = ["NA", "APAC", "EMEA", "LATAM"]
    for r in regions:
        if r in query_text.upper():
            if st.session_state.current_region != r:
                st.session_state.current_region = r
                return True
    return False

# --- 2. INPUT HANDLING ---
user_input = st.chat_input("Ask about deals, vendors, or work orders...")
active_q = st.session_state.get('active_query') or user_input

# If region in query differs from sidebar, update and rerun to keep UI in sync
if active_q and sync_region_from_query(active_q):
    st.rerun()

# --- 3. SIDEBAR ---
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.caption("v5.2 Enterprise Stable")
    
    region_options = ["NA", "APAC", "EMEA", "LATAM"]
    selected_reg = st.selectbox("Market Region", region_options, 
                                index=region_options.index(st.session_state.current_region),
                                key="sidebar_region_select")
    
    if selected_reg != st.session_state.current_region:
        st.session_state.current_region = selected_reg
        st.rerun()

    persona = st.selectbox("View Persona", ["Leadership", "Product", "Operations", "Finance"])
    st.divider()
    
    st.subheader(f"💡 {persona} Suggestions")
    prompts = {
        "Leadership": ["Top vendors", "Market value", "Vendor performance"],
        "Finance": ["Total spend", "Highest cost deals"]
    }
    
    for i, sug in enumerate(prompts.get(persona, ["Performance"])):
        if st.button(f"{sug} in {st.session_state.current_region}", key=f"sug_btn_{i}"):
            st.session_state.active_query = f"{sug} in {st.session_state.current_region}"
            st.rerun()

# --- 4. MAIN INTERFACE & HISTORY ---
st.title(f"🔍 {persona} Intelligence: {st.session_state.current_region}")

for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"): st.write(msg["q"])
    with st.chat_message("assistant"):
        st.plotly_chart(msg["fig"], use_container_width=True, key=f"hist_chart_{i}_{time.time()}")
        with st.expander("View Records"):
            st.dataframe(msg["full_df"], use_container_width=True)

# --- 5. EXECUTION ENGINE ---
if active_q:
    if 'active_query' in st.session_state: del st.session_state.active_query
        
    sql, err, c_type = parse_query(active_q, st.session_state.current_region)
    chart_df, db_err = execute_sql(sql, DB_CONN)

    if chart_df is not None and not chart_df.empty:
        # Determine data context for Tableau push
        target_table = "deals"
        if any(x in active_q.lower() for x in ["task", "order", "performance"]): target_table = "work_orders"
        
        full_sql = f"SELECT * FROM {target_table} WHERE UPPER(region) = '{st.session_state.current_region.upper()}'"
        full_df, _ = execute_sql(full_sql, DB_CONN)

        with st.chat_message("assistant"):
            label, val = chart_df.columns[0], chart_df.columns[1] if len(chart_df.columns)>1 else chart_df.columns[0]
            
            # --- PROFESSIONAL VISUALS ---
            if c_type == "pie":
                fig = px.pie(chart_df, names=label, values=val, hole=0.5,
                             color_discrete_sequence=px.colors.qualitative.Prism,
                             title=f"Distribution: {st.session_state.current_region}")
            else:
                # Horizontal bars are best for ranking vendors/performance
                fig = px.bar(chart_df, y=label, x=val, orientation='h',
                             color=val, color_continuous_scale='Viridis',
                             text_auto='.2s', title=f"Analysis: {st.session_state.current_region}")
                fig.update_layout(yaxis={'categoryorder':'total ascending'}, showlegend=False)

            # Render Chart
            st.plotly_chart(fig, use_container_width=True, key=f"active_chart_{time.time()}")
            
            # --- ENTERPRISE ACTIONS (TABLEAU) ---
            st.markdown("---")
            st.subheader("📊 Enterprise Actions")
            t_col1, t_col2 = st.columns([3, 1])
            with t_col1:
                t_name = st.text_input("Tableau Destination Name", 
                                      value=f"Foundry_{st.session_state.current_region}_{int(time.time())}",
                                      key=f"t_input_{time.time()}")
            with t_col2:
                st.write(" ") # spacer
                if st.button("🚀 Push to Tableau", use_container_width=True, key=f"t_btn_{time.time()}"):
                    with st.spinner("Syncing to Tableau..."):
                        success, m = trigger_tableau_report(full_df, t_name)
                        if success: st.success("Successfully pushed to Tableau!")
                        else: st.error(f"Tableau Error: {m}")
            
            with st.expander("📝 View Detailed Transactional Data"):
                st.dataframe(full_df, use_container_width=True)

            # SAVE TO HISTORY
            st.session_state.chat_history.append({"q": active_q, "fig": fig, "full_df": full_df})
            
            # TRIGGER AUTO-SCROLL
            components.html(f"""
                <script>
                window.parent.document.querySelector('section.main').scrollTo({{ top: 10000, behavior: 'smooth' }});
                </script>""", height=0)
            
            time.sleep(0.1)
            st.rerun()
    else:
        # HANDLING NO RESULTS
        with st.chat_message("assistant"):
            st.warning(f"No records found for '{active_q}' in {st.session_state.current_region}. Try switching the region in the sidebar or adjusting your search.")
            if 'active_query' in st.session_state: del st.session_state.active_query

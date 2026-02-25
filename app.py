import streamlit as st
import pandas as pd
import plotly.express as px
import streamlit.components.v1 as components
import time
from utils.database import init_database, execute_sql
from utils.query_parser import parse_query
from utils.tableau_sync import trigger_tableau_report 

st.set_page_config(page_title="Foundry Vantage", page_icon="🎥", layout="wide")

# --- CUSTOM STYLING ---
st.markdown("""
    <style>
    .main { background-color: #f8f9fa; }
    .stButton>button { border-radius: 5px; height: 3em; }
    .stExpander { border: 1px solid #e6e6e6; border-radius: 8px; background-color: white; }
    </style>
    """, unsafe_allow_html=True)

@st.cache_resource
def get_db():
    return init_database()

DB_CONN = get_db()

# --- 1. GLOBAL STATE ---
if 'current_region' not in st.session_state:
    st.session_state.current_region = 'APAC'
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []

def sync_region_from_query(query_text):
    regions = ["NA", "APAC", "EMEA", "LATAM"]
    for r in regions:
        if r in query_text.upper():
            st.session_state.current_region = r
            return r
    return st.session_state.current_region

# --- 2. INPUT ---
user_input = st.chat_input("Ask about deals, vendors, or work orders...")
active_q = st.session_state.get('active_query') or user_input

if active_q:
    sync_region_from_query(active_q)

# --- 3. SIDEBAR ---
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.caption("v5.2 | Enterprise Analytics")
    
    region_options = ["NA", "APAC", "EMEA", "LATAM"]
    selected_reg = st.selectbox("Market Region", region_options, 
                                index=region_options.index(st.session_state.current_region),
                                key=f"sidebar_reg_{st.session_state.current_region}")
    
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
    
    for sug in prompts.get(persona, ["Performance", "Work orders"]):
        if st.button(f"{sug} in {st.session_state.current_region}", use_container_width=True):
            st.session_state.active_query = f"{sug} in {st.session_state.current_region}"
            st.rerun()

# --- 4. RENDER HISTORY ---
st.title(f"🔍 {persona} Intelligence: {st.session_state.current_region}")

for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"): st.write(msg["q"])
    with st.chat_message("assistant"):
        st.plotly_chart(msg["fig"], use_container_width=True)
        with st.expander("Records"): st.dataframe(msg["full_df"])

# --- 5. EXECUTION ENGINE ---
if active_q:
    if 'active_query' in st.session_state: del st.session_state.active_query
        
    sql, err, c_type = parse_query(active_q, st.session_state.current_region)
    chart_df, db_err = execute_sql(sql, DB_CONN)

    if chart_df is not None and not chart_df.empty:
        # Determine table for full records
        target_table = "deals"
        if any(x in active_q.lower() for x in ["task", "order", "performance"]): target_table = "work_orders"
        
        full_sql = f"SELECT * FROM {target_table} WHERE UPPER(region) = '{st.session_state.current_region.upper()}'"
        full_df, _ = execute_sql(full_sql, DB_CONN)

        with st.chat_message("assistant"):
            label, val = chart_df.columns[0], chart_df.columns[1] if len(chart_df.columns)>1 else chart_df.columns[0]
            
            # --- ENHANCED VISUALS ---
            if c_type == "pie":
                fig = px.pie(chart_df, names=label, values=val, hole=0.6,
                             color_discrete_sequence=px.colors.sequential.RdBu,
                             title=f"Distribution: {st.session_state.current_region}")
            elif c_type == "treemap":
                fig = px.treemap(chart_df, path=[label, chart_df.columns[1]], values=val,
                                 color=val, color_continuous_scale='Viridis')
            else:
                # Optimized Bar Chart for "Top Vendors" and "Performance"
                fig = px.bar(chart_df, x=label, y=val, color=val,
                             text_auto='.2s', height=500,
                             color_continuous_scale='Blues' if "vendor" in active_q.lower() else 'Reds',
                             title=f"Analysis: {active_q}")
                fig.update_layout(xaxis_tickangle=-45, showlegend=False, plot_bgcolor='rgba(0,0,0,0)')

            st.plotly_chart(fig, use_container_width=True)
            
            # --- NEW: TABLEAU ACTION CENTER (Always Visible) ---
            st.markdown("### 📊 Enterprise Actions")
            t_col1, t_col2 = st.columns([3, 1])
            with t_col1:
                t_name = st.text_input("Tableau Destination Name", value=f"Foundry_Export_{int(time.time())}")
            with t_col2:
                st.write(" ") # Padding
                if st.button("🚀 Push to Tableau", use_container_width=True):
                    with st.spinner("Syncing..."):
                        success, m = trigger_tableau_report(full_df, t_name)
                        if success: st.success("Pushed Successfully!")
                        else: st.error(m)
            
            with st.expander("📝 View Detailed Transactional Data", expanded=False):
                st.dataframe(full_df, use_container_width=True)

            # Store and Scroll
            st.session_state.chat_history.append({"q": active_q, "fig": fig, "full_df": full_df})
            
            # FORCE SCROLL SCRIPT
            components.html("""
                <script>
                window.parent.document.querySelector('section.main').scrollTo({ top: 10000, behavior: 'smooth' });
                </script>""", height=0)
            
            time.sleep(0.5)
            st.rerun()

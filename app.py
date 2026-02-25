import streamlit as st
import pandas as pd
import plotly.express as px
import streamlit.components.v1 as components
import time
from utils.database import init_database, execute_sql
from utils.query_parser import parse_query

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
            st.session_state.current_region = r
            return r
    return st.session_state.current_region

# --- 2. PRE-PROCESS INPUT ---
user_input = st.chat_input("Ask about deals, vendors, or work orders...")
active_q = st.session_state.get('active_query') or user_input

# CRITICAL: Sync region BEFORE the sidebar or charts are drawn
if active_q:
    sync_region_from_query(active_q)

# --- 3. SIDEBAR ---
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.caption("v5.0 Enterprise Analytics")
    
    region_options = ["NA", "APAC", "EMEA", "LATAM"]
    current_idx = region_options.index(st.session_state.current_region)
    
    selected_reg = st.selectbox(
        "Market Region", 
        region_options, 
        index=current_idx,
        key=f"reg_widget_{st.session_state.current_region}"
    )
    
    if selected_reg != st.session_state.current_region:
        st.session_state.current_region = selected_reg
        st.rerun()

    persona = st.selectbox("View Persona", ["Leadership", "Product", "Operations", "Finance"])
    
    st.divider()
    st.subheader(f"💡 {persona} Suggestions")
    prompts = {
        "Leadership": ["Top vendors", "Market value", "Vendor performance"],
        "Product": ["Content readiness", "SVOD rights", "Inventory status"],
        "Operations": ["Delayed tasks", "Work orders", "Performance"],
        "Finance": ["Total spend", "Highest cost deals", "Market value overview"]
    }
    
    for i, sug in enumerate(prompts.get(persona, [])):
        if st.button(f"{sug} in {st.session_state.current_region}", use_container_width=True, key=f"sug_{i}"):
            st.session_state.active_query = f"{sug} in {st.session_state.current_region}"
            st.rerun()

# --- 4. MAIN INTERFACE ---
st.title(f"🔍 {persona} Intelligence: {st.session_state.current_region}")

# Render History
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"):
        st.write(msg["q"])
    with st.chat_message("assistant"):
        st.plotly_chart(msg["fig"], use_container_width=True, key=f"hist_fig_{i}")
        with st.expander("View Detailed Records"):
            st.dataframe(msg["full_df"], use_container_width=True)

# --- 5. EXECUTION ---
if active_q:
    if 'active_query' in st.session_state:
        del st.session_state.active_query
        
    sql, err, c_type = parse_query(active_q, st.session_state.current_region)
    chart_df, db_err = execute_sql(sql, DB_CONN)

    if chart_df is not None and not chart_df.empty:
        # Table mapping for the detailed records view
        target_table = "deals"
        low_q = active_q.lower()
        if any(x in low_q for x in ["task", "order", "performance", "delay"]):
            target_table = "work_orders"
        elif any(x in low_q for x in ["ready", "inventory", "status"]):
            target_table = "content_planning"

        # The Table Query now strictly follows the synced session state
        full_sql = f"SELECT * FROM {target_table} WHERE UPPER(region) = '{st.session_state.current_region.upper()}'"
        full_df, _ = execute_sql(full_sql, DB_CONN)

        with st.chat_message("assistant"):
            label, val = chart_df.columns[0], chart_df.columns[1] if len(chart_df.columns) > 1 else chart_df.columns[0]
            
            # --- PROFESSIONAL MIX-AND-MATCH VIZ ---
            if c_type == "pie":
                fig = px.pie(chart_df, names=label, values=val, hole=0.5,
                             color_discrete_sequence=px.colors.qualitative.Prism,
                             title=f"Market Distribution ({st.session_state.current_region})")
            
            elif c_type == "treemap":
                fig = px.treemap(chart_df, path=['vendor_name', 'deal_name'], values='deal_value',
                                 color='deal_value', color_continuous_scale='Blues',
                                 title=f"Market Value Composition ({st.session_state.current_region})")
            
            elif c_type == "bar_v":
                fig = px.bar(chart_df, x=label, y=val, color=val, 
                             color_continuous_scale='Reds', template="plotly_white",
                             title=f"Top Individual Deals by Cost ({st.session_state.current_region})")
            
            else: # bar_h (Ranking)
                fig = px.bar(chart_df, y=label, x=val, orientation='h',
                             color=val, color_continuous_scale='Viridis', template="plotly_white",
                             title=f"Vendor Ranking ({st.session_state.current_region})")
                fig.update_layout(yaxis={'categoryorder':'total ascending'})

            st.plotly_chart(fig, use_container_width=True, key=f"new_{time.time()}")
            
            with st.expander("Explore Full Transactional Records", expanded=True):
                st.dataframe(full_df, use_container_width=True)
                st.download_button("📥 Download Report", data=full_df.to_csv(index=False), file_name="report.csv")

            st.session_state.chat_history.append({"q": active_q, "fig": fig, "full_df": full_df})
            components.html("<script>window.parent.document.querySelector('section.main').scrollTo(0,10000);</script>", height=0)
            st.rerun()
    else:
        st.error(f"No records found for '{active_q}' in {st.session_state.current_region}.")

import streamlit as st
import pandas as pd
import plotly.express as px
from utils.database import init_database, execute_sql
from utils.query_parser import parse_query

# 1. Page Configuration
st.set_page_config(page_title="Foundry Vantage", page_icon="🎥", layout="wide")

@st.cache_resource
def get_db():
    return init_database()

DB_CONN = get_db()

# 2. Initialize Session States
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if 'current_region' not in st.session_state:
    st.session_state.current_region = 'APAC'
if 'persona' not in st.session_state:
    st.session_state.persona = 'Product' # Default Persona

# 3. PRE-RENDER LOGIC GATE (Region & Persona Detection)
user_input = st.chat_input("Ask about deals, content readiness, or localization...")

active_prompt = None
if st.session_state.get('pending_prompt'):
    active_prompt = st.session_state.pending_prompt
    del st.session_state.pending_prompt
elif user_input:
    active_prompt = user_input

# Detect region in text to sync sidebar before it renders
if active_prompt:
    for r in ["NA", "APAC", "EMEA", "LATAM"]:
        if r.lower() in active_prompt.lower():
            st.session_state.current_region = r
            break

# 4. SIDEBAR (Persona & Region Management)
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.caption("Supply Chain Intelligence Layer")
    st.divider()
    
    # Region Filter with Hard-Reset Key
    market_options = ["NA", "APAC", "EMEA", "LATAM"]
    selected_market = st.selectbox(
        "Market Region", 
        market_options,
        index=market_options.index(st.session_state.current_region),
        key=f"sidebar_reg_{st.session_state.current_region}"
    )
    st.session_state.current_region = selected_market

    # Persona Filter
    persona_options = ["Leadership", "Product", "Operations", "Finance"]
    selected_persona = st.selectbox(
        "View Persona", 
        persona_options, 
        index=persona_options.index(st.session_state.persona),
        key="persona_selector"
    )
    st.session_state.persona = selected_persona

    st.divider()
    
    # PERSONA-BASED SUGGESTIONS
    st.subheader(f"💡 {st.session_state.persona} Queries")
    def get_persona_suggestions(persona, reg):
        prompts = {
            "Leadership": [f"Market value overview for {reg}", f"Top vendors in {reg}", f"Content readiness % for {reg}"],
            "Product": [f"Show SVOD rights in {reg}", f"Rights scope breakdown {reg}", f"Unacquired content in {reg}"],
            "Operations": [f"Delayed Duplo tasks in {reg}", f"Work order status {reg}", f"Packaging queue for {reg}"],
            "Finance": [f"Total spend per vendor in {reg}", f"Highest cost deals {reg}", f"Show deal value breakdown {reg}"]
        }
        return prompts.get(persona, prompts["Product"])

    for i, sug in enumerate(get_persona_suggestions(st.session_state.persona, st.session_state.current_region)):
        if st.button(sug, width='stretch', key=f"sug_{i}_{st.session_state.current_region}"):
            st.session_state.pending_prompt = sug
            st.rerun()

# 5. MAIN UI & CHAT HISTORY (Rendered first for scrolling)
st.title(f"🔍 {st.session_state.persona} Insights")

for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"):
        st.write(msg["question"])
    with st.chat_message("assistant", avatar="🎥"):
        st.write(msg["answer"])
        if msg.get("metrics"):
            m1, m2 = st.columns(2)
            m1.metric(msg["metrics"][0]["label"], msg["metrics"][0]["value"])
            m2.metric(msg["metrics"][1]["label"], msg["metrics"][1]["value"])
        if msg["chart"]:
            st.plotly_chart(msg["chart"], use_container_width=True, key=f"h_chart_{i}")
        with st.expander("View Records"):
            st.dataframe(msg["data"], use_container_width=True, key=f"h_data_{i}")

# 6. PROCESS NEW QUERY
if active_prompt:
    active_reg = st.session_state.current_region
    persona = st.session_state.persona
    
    with st.chat_message("user"):
        st.write(active_prompt)
        
    with st.chat_message("assistant", avatar="🎥"):
        with st.spinner(f"Generating {persona} view for {active_reg}..."):
            sql, error, chart_type = parse_query(active_prompt, active_reg)
            
            if error:
                st.error(error)
            else:
                res_df, _ = execute_sql(sql, DB_CONN)
                if res_df is not None and not res_df.empty:
                    # Initialize Chart
                    if chart_type == "pie":
                        col = 'status' if 'status' in res_df.columns else res_df.columns[-1]
                        fig = px.pie(res_df, names=col, title=f"Inventory: {active_reg}", hole=0.4)
                    else:
                        y = 'deal_value' if 'deal_value' in res_df.columns else ( 'total_value' if 'total_value' in res_df.columns else res_df.columns[0])
                        x = 'vendor_name' if 'vendor_name' in res_df.columns else (res_df.columns[1] if len(res_df.columns)>1 else res_df.columns[0])
                        fig = px.bar(res_df, x=x, y=y, title=f"Analysis: {active_reg}")

                    # --- PERSONA CUSTOMIZATION ---
                    metrics_data = None
                    ans_label = f"Displaying {active_reg} Data:"
                    
                    if persona == "Leadership":
                        st.subheader(f"Executive Summary: {active_reg}")
                        if "deal_value" in res_df.columns:
                            m1, m2 = st.columns(2)
                            v_sum = f"${res_df['deal_value'].sum():,.0f}"
                            v_avg = f"${res_df['deal_value'].mean():,.0f}"
                            m1.metric("Total Market Value", v_sum)
                            m2.metric("Avg Deal Size", v_avg)
                            metrics_data = [{"label": "Total Market Value", "value": v_sum}, {"label": "Avg Deal Size", "value": v_avg}]
                        st.plotly_chart(fig, use_container_width=True)
                        with st.expander("Detailed Backup Records"):
                            st.dataframe(res_df, use_container_width=True)

                    elif persona == "Operations":
                        st.info(f"Operational Queue: {active_reg}")
                        st.dataframe(res_df, use_container_width=True) # Table first
                        st.plotly_chart(fig, use_container_width=True)

                    else: # PRODUCT (Default) & FINANCE
                        st.write(ans_label)
                        st.plotly_chart(fig, use_container_width=True)
                        with st.expander("Explore Dataset", expanded=True):
                            st.dataframe(res_df, use_container_width=True)

                    # Save to history
                    st.session_state.chat_history.append({
                        "question": active_prompt,
                        "answer": ans_label,
                        "data": res_df,
                        "chart": fig,
                        "metrics": metrics_data
                    })
                    st.rerun()
                else:
                    st.warning(f"No records found for '{active_prompt}' in {active_reg}.")

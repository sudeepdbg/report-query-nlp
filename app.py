import streamlit as st
import pandas as pd
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

# Import your modules
from utils.database import init_database, execute_sql
from utils.query_parser import parse_query

# Page config must be first
st.set_page_config(
    page_title="Foundry Vantage",
    page_icon="🎥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize database (cached)
@st.cache_resource
def get_database_connection():
    return init_database()

DB_CONN = get_database_connection()

# Initialize session state
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if 'current_region' not in st.session_state:
    st.session_state.current_region = 'NA'
if 'current_team' not in st.session_state:
    st.session_state.current_team = 'leadership'
if 'active_dashboard' not in st.session_state:
    st.session_state.active_dashboard = 'executive'

# Custom CSS
st.markdown("""
<style>
    .chat-message {
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
    }
    .user-message {
        background-color: #e2e8f0;
        color: #1e293b;
    }
    .assistant-message {
        background-color: #3b82f6;
        color: white;
    }
    .sql-box {
        background-color: #1e293b;
        color: #e2e8f0;
        padding: 0.5rem;
        border-radius: 0.25rem;
        font-family: monospace;
        margin-top: 0.5rem;
    }
    .suggestion-chip {
        background: #e2e8f0;
        padding: 0.25rem 0.75rem;
        border-radius: 2rem;
        cursor: pointer;
        margin-right: 0.5rem;
        margin-bottom: 0.5rem;
        display: inline-block;
    }
    .suggestion-chip:hover {
        background: #cbd5e1;
    }
</style>
""", unsafe_allow_html=True)

# Dashboard data functions
def get_dashboard_data(name):
    if name == 'executive':
        sql = "SELECT status, COUNT(*) as count FROM content_planning GROUP BY status"
    elif name == 'workorders':
        sql = "SELECT status, COUNT(*) as count FROM work_orders GROUP BY status"
    elif name == 'deals':
        sql = "SELECT vendor, SUM(deal_value) as total FROM deals GROUP BY vendor"
    else:
        return None
    
    df, error = execute_sql(sql, DB_CONN)
    if error:
        return None
    
    if name == 'executive':
        return {'type': 'pie', 'labels': df['status'].tolist(), 'values': df['count'].tolist(), 'title': 'Content Status'}
    elif name == 'workorders':
        return {'type': 'bar', 'labels': df['status'].tolist(), 'values': df['count'].tolist(), 'title': 'Work Orders by Status'}
    elif name == 'deals':
        return {'type': 'bar', 'labels': df['vendor'].tolist(), 'values': df['total'].tolist(), 'title': 'Deals by Vendor'}

# Auto-chart function
def auto_chart(df):
    if df.shape[1] < 2:
        return None
    # Simple auto-chart logic
    if len(df) <= 10:
        fig = px.pie(df, names=df.columns[0], values=df.columns[1], title="Distribution")
    else:
        fig = px.bar(df, x=df.columns[0], y=df.columns[1], title="Breakdown")
    return fig

# ------------------------------------------------------------------
# UI Layout
# ------------------------------------------------------------------
with st.sidebar:
    st.title("🎥 Foundry Vantage")
    st.caption("AI-powered data assistant")
    st.divider()
    
    # Dashboard selector
    st.subheader("📊 Sample Reports")
    dashboard_options = {
        "Executive Content Dashboard": "executive",
        "Work Order Tracker": "workorders",
        "Deals Performance Dashboard": "deals"
    }
    selected_dashboard = st.radio(
        "Select dashboard",
        options=list(dashboard_options.keys()),
        index=0,
        label_visibility="collapsed"
    )
    st.session_state.active_dashboard = dashboard_options[selected_dashboard]
    
    # Display current dashboard chart
    dashboard_data = get_dashboard_data(st.session_state.active_dashboard)
    if dashboard_data:
        if dashboard_data['type'] == 'pie':
            fig = px.pie(values=dashboard_data['values'], names=dashboard_data['labels'], title=dashboard_data['title'])
        else:
            fig = px.bar(x=dashboard_data['labels'], y=dashboard_data['values'], title=dashboard_data['title'])
        st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # Filters
    st.subheader("🔧 Filters")
    st.session_state.current_region = st.selectbox(
        "Region",
        ["NA", "APAC", "EMEA", "LATAM"],
        index=["NA", "APAC", "EMEA", "LATAM"].index(st.session_state.current_region)
    )
    st.session_state.current_team = st.selectbox(
        "Team",
        ["leadership", "product", "content planning", "deals"],
        index=["leadership", "product", "content planning", "deals"].index(st.session_state.current_team)
    )
    
    st.divider()
    
    # Suggestions
    st.subheader("💡 Suggested queries")
    suggestions = {
        'executive': [
            "Show me all content for MAX Australia",
            "How many content items are Not Ready?",
            "List scheduled content for MAX US"
        ],
        'workorders': [
            "Show me all delayed work orders",
            "How many work orders are In Progress?",
            "List work orders for Vendor A"
        ],
        'deals': [
            "Show me all active deals",
            "How many deals are pending approval?",
            "List top vendors by deal value"
        ]
    }
    
    for suggestion in suggestions[st.session_state.active_dashboard]:
        if st.button(suggestion, key=f"sugg_{suggestion}", use_container_width=True):
            # Will handle in main area
            st.session_state['pending_question'] = suggestion
            st.rerun()

# ------------------------------------------------------------------
# Main Chat Area
# ------------------------------------------------------------------
st.title("🔍 Ask Foundry Vantage")
st.caption("Natural language queries for media supply chain data")

# Chat history display
chat_container = st.container()
with chat_container:
    for msg in st.session_state.chat_history:
        # User message
        st.markdown(f'<div class="chat-message user-message"><strong>You:</strong> {msg["question"]}</div>', unsafe_allow_html=True)
        
        # Assistant message
        assistant_html = f'<div class="chat-message assistant-message"><strong>Vantage:</strong> {msg["answer"]}'
        if msg.get('sql'):
            assistant_html += f'<div class="sql-box">{msg["sql"]}</div>'
        if msg.get('data'):
            assistant_html += f'<div class="mt-2">{msg["data"]}</div>'
        if msg.get('chart'):
            assistant_html += f'<div class="mt-2">{msg["chart"]}</div>'
        assistant_html += '</div>'
        st.markdown(assistant_html, unsafe_allow_html=True)

# Question input
question = st.chat_input("Ask a question...", key="question_input")

# Handle pending suggestion
if 'pending_question' in st.session_state:
    question = st.session_state.pending_question
    del st.session_state.pending_question

if question:
    # Process question
    with st.spinner("🔍 Analyzing query..."):
        # Parse query
        sql, error = parse_query(
            question, 
            st.session_state.current_region,
            st.session_state.current_team,
            st.session_state.active_dashboard
        )
        
        if error:
            answer = error
            data_html = None
            chart = None
            sql_display = None
        else:
            # Execute SQL
            df, exec_error = execute_sql(sql, DB_CONN)
            if exec_error:
                answer = f"Query execution failed: {exec_error}"
                data_html = None
                chart = None
                sql_display = sql
            else:
                # Format response
                if df.empty:
                    answer = "No results found."
                    data_html = None
                    chart = None
                elif len(df) == 1 and df.shape[1] == 1:
                    answer = f"**Result:** {df.iloc[0,0]}"
                    data_html = None
                    chart = None
                else:
                    answer = f"Found **{len(df)}** results:"
                    data_html = df.to_html(classes='table table-striped', index=False)
                    chart = auto_chart(df)
                sql_display = sql
        
        # Add to chat history
        st.session_state.chat_history.append({
            'question': question,
            'answer': answer,
            'sql': sql_display,
            'data': data_html,
            'chart': chart
        })
    
    st.rerun()

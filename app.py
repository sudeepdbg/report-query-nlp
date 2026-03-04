import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit.components.v1 as components
import time
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Optional, Dict, Any

# Import utilities
from utils.database import init_database, execute_sql, DatabaseManager, get_table_stats
from utils.query_parser import parse_query, QueryParser
from utils.tableau_sync import trigger_tableau_report
from utils.charting import SemanticCharting

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="Foundry Vantage - Enterprise Content Intelligence",
    page_icon="🎥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for enterprise look and feel
st.markdown("""
<style>
    /* Global Styles */
    .main {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        min-height: 100vh;
    }
    
    /* Glass morphism effect for cards */
    .glass-card {
        background: rgba(255, 255, 255, 0.95);
        backdrop-filter: blur(10px);
        border-radius: 20px;
        padding: 25px;
        box-shadow: 0 8px 32px rgba(31, 38, 135, 0.15);
        border: 1px solid rgba(255, 255, 255, 0.18);
        margin: 10px 0;
        transition: transform 0.3s ease, box-shadow 0.3s ease;
    }
    
    .glass-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 12px 40px rgba(31, 38, 135, 0.25);
    }
    
    /* Metric card styling */
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 15px;
        padding: 20px;
        color: white;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
    }
    
    .metric-card h3 {
        margin: 0;
        font-size: 14px;
        font-weight: 500;
        opacity: 0.9;
    }
    
    .metric-card .value {
        font-size: 32px;
        font-weight: 700;
        margin: 10px 0 5px;
    }
    
    .metric-card .delta {
        font-size: 14px;
        opacity: 0.8;
    }
    
    /* Chat message styling */
    .stChatMessage {
        background: white;
        border-radius: 15px;
        padding: 20px;
        margin: 15px 0;
        box-shadow: 0 2px 10px rgba(0,0,0,0.05);
        border-left: 4px solid #667eea;
    }
    
    /* Sidebar styling */
    .css-1d391kg {
        background: linear-gradient(180deg, #ffffff 0%, #f8f9fa 100%);
    }
    
    /* Button styling */
    .stButton > button {
        width: 100%;
        border-radius: 25px;
        border: none;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        font-weight: 600;
        padding: 10px 25px;
        transition: all 0.3s ease;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4);
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(102, 126, 234, 0.6);
    }
    
    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
        background: rgba(255,255,255,0.1);
        padding: 10px;
        border-radius: 30px;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 25px;
        padding: 10px 25px;
        background: white;
        font-weight: 600;
        color: #333;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
    }
    
    /* Progress bar styling */
    .stProgress > div > div {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    }
    
    /* Dataframe styling */
    .dataframe-container {
        border-radius: 15px;
        overflow: hidden;
        border: 1px solid #e0e0e0;
    }
    
    /* Footer styling */
    .footer {
        text-align: center;
        padding: 30px;
        color: rgba(255,255,255,0.8);
        font-size: 14px;
    }
</style>
""", unsafe_allow_html=True)

# Initialize database connection with caching
@st.cache_resource(ttl=3600, show_spinner="Connecting to Foundry database...")
def init_db():
    """Initialize database connection with retry logic"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            conn = init_database()
            # Verify connection with a simple query
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            logger.info("Database connection established successfully")
            return conn
        except Exception as e:
            logger.error(f"Database connection attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                st.error(f"Failed to connect to database after {max_retries} attempts. Please refresh the page.")
                raise

# Initialize session state
def init_session_state():
    """Initialize all session state variables"""
    defaults = {
        'chat_history': [],
        'current_region': 'APAC',
        'persona': 'Product',
        'favorite_queries': [],
        'date_range': 'Last 30 Days',
        'view_mode': 'Analytics',
        'selected_metrics': ['deals', 'vendors', 'content'],
        'chart_preferences': {},
        'notifications': [],
        'user_preferences': {
            'theme': 'light',
            'default_chart_type': 'auto',
            'show_sql': False,
            'auto_refresh': False,
            'refresh_interval': 300
        },
        'db_stats': {}
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

init_session_state()

# Initialize database connection
try:
    DB_CONN = init_db()
    # Update database stats
    st.session_state.db_stats = get_table_stats(DB_CONN)
except Exception as e:
    st.error(f"⚠️ Database connection error: {str(e)}")
    st.stop()

# Sidebar
with st.sidebar:
    st.image("https://via.placeholder.com/200x80/667eea/ffffff?text=FOUNDRY+VANTAGE", use_container_width=True)
    
    # User profile section
    with st.expander("👤 User Profile", expanded=False):
        st.text_input("Name", value="John Doe", key="user_name")
        st.text_input("Email", value="john.doe@foundry.com", key="user_email")
        st.selectbox("Role", ["Executive", "Analyst", "Manager", "Operator"], key="user_role")
    
    st.markdown("---")
    
    # Main controls in columns
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### 🎯 Market")
        market_options = ["NA", "APAC", "EMEA", "LATAM", "Global"]
        st.session_state.current_region = st.selectbox(
            "Region",
            market_options,
            index=market_options.index(st.session_state.current_region) if st.session_state.current_region in market_options else 1,
            key="region_selector",
            label_visibility="collapsed"
        )
    
    with col2:
        st.markdown("### 👤 Persona")
        persona_options = ["Leadership", "Product", "Operations", "Finance", "Analytics"]
        st.session_state.persona = st.selectbox(
            "View as",
            persona_options,
            index=persona_options.index(st.session_state.persona) if st.session_state.persona in persona_options else 2,
            key="persona_selector",
            label_visibility="collapsed"
        )
    
    # Quick stats
    st.markdown("### 📊 Quick Stats")
    stats_col1, stats_col2, stats_col3 = st.columns(3)
    with stats_col1:
        st.metric("Deals", f"{st.session_state.db_stats.get('deals', 0):,}", "+12%")
    with stats_col2:
        st.metric("Vendors", f"{st.session_state.db_stats.get('vendors', 0):,}", "+3")
    with stats_col3:
        st.metric("Content", f"{st.session_state.db_stats.get('content_planning', 0):,}", "+8%")
    
    st.markdown("---")
    
    # Persona-specific quick actions
    st.markdown(f"### 💡 {st.session_state.persona} Quick Actions")
    
    def get_persona_actions(persona, reg):
        """Get persona-specific suggested queries"""
        actions = {
            "Leadership": [
                ("📈 Executive Dashboard", f"Show executive dashboard for {reg}"),
                ("💰 Revenue Analysis", f"Revenue breakdown by region {reg}"),
                ("🎯 Strategic Deals", f"Top 10 strategic deals in {reg}"),
                ("📊 Market Overview", f"Market performance overview for {reg}")
            ],
            "Product": [
                ("🎬 Rights Analysis", f"SVOD vs AVOD rights in {reg}"),
                ("📺 Genre Performance", f"Content performance by genre in {reg}"),
                ("🆕 Release Pipeline", f"Upcoming releases in {reg}"),
                ("📈 Content Demand", f"Content demand forecast for {reg}")
            ],
            "Operations": [
                ("⚡ Work Order Status", f"Work order status breakdown for {reg}"),
                ("⏰ Delayed Tasks", f"Delayed work orders in {reg}"),
                ("👥 Vendor Performance", f"Vendor quality metrics for {reg}"),
                ("📋 Resource Allocation", f"Resource allocation by vendor in {reg}")
            ],
            "Finance": [
                ("💰 Deal Value Analysis", f"Deal value distribution in {reg}"),
                ("📊 Vendor Spend", f"Total spend per vendor in {reg}"),
                ("💱 Currency Impact", f"Currency impact analysis in {reg}"),
                ("📈 Budget vs Actual", f"Budget vs actual spend in {reg}")
            ],
            "Analytics": [
                ("📊 Trend Analysis", f"Market trends in {reg}"),
                ("🔮 Predictive Insights", f"Predictive analytics for {reg}"),
                ("📈 Correlation Analysis", f"Deal correlations in {reg}"),
                ("🎯 Anomaly Detection", f"Anomalies in {reg} market")
            ]
        }
        return actions.get(persona, actions["Product"])
    
    # Display action buttons
    for action_label, action_query in get_persona_actions(st.session_state.persona, st.session_state.current_region):
        if st.button(action_label, use_container_width=True, key=f"action_{action_label}"):
            st.session_state.pending_prompt = action_query
            st.rerun()
    
    st.markdown("---")
    
    # Favorites section
    st.markdown("### ⭐ Favorite Queries")
    if st.session_state.favorite_queries:
        for i, fav in enumerate(st.session_state.favorite_queries[-5:]):
            if st.button(f"📌 {fav[:30]}...", key=f"fav_{i}", use_container_width=True):
                st.session_state.pending_prompt = fav
                st.rerun()
    else:
        st.info("No favorites yet. Click ★ to save queries.")
    
    if st.button("➕ Add Current to Favorites", use_container_width=True, key="add_fav"):
        if st.session_state.chat_history:
            last_query = st.session_state.chat_history[-1]["question"]
            if last_query not in st.session_state.favorite_queries:
                st.session_state.favorite_queries.append(last_query)
                st.success("✅ Added to favorites!")
                time.sleep(1)
                st.rerun()
    
    st.markdown("---")
    
    # Settings
    with st.expander("⚙️ Settings", expanded=False):
        st.session_state.user_preferences['show_sql'] = st.toggle("Show SQL Queries", value=st.session_state.user_preferences['show_sql'])
        st.session_state.user_preferences['auto_refresh'] = st.toggle("Auto-refresh Data", value=st.session_state.user_preferences['auto_refresh'])
        if st.session_state.user_preferences['auto_refresh']:
            st.session_state.user_preferences['refresh_interval'] = st.slider("Refresh Interval (s)", 60, 3600, 300, step=60)
        
        st.selectbox("Default Chart Type", ["auto", "bar", "line", "pie", "area", "heatmap"], 
                    key="default_chart_type")

# Main content area
st.title(f"🎥 Foundry Vantage - {st.session_state.persona} Intelligence")
st.markdown(f"### {st.session_state.current_region} Market Analysis • {st.session_state.date_range}")

# Create tabs for different views
tab1, tab2, tab3, tab4 = st.tabs(["📊 Analytics", "📈 Insights", "🔮 Predictions", "📋 Reports"])

with tab1:
    # Analytics view - will be filled by chat responses
    pass

with tab2:
    # Insights view
    st.markdown("### 📊 Key Insights")
    
    if st.session_state.chat_history:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            with st.container():
                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                st.metric(
                    "Total Queries",
                    len(st.session_state.chat_history),
                    "+2 today",
                    delta_color="normal"
                )
                st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            with st.container():
                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                total_records = sum(len(msg["data"]) for msg in st.session_state.chat_history)
                st.metric("Records Analyzed", f"{total_records:,}", "+1.2K")
                st.markdown('</div>', unsafe_allow_html=True)
        
        with col3:
            with st.container():
                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                st.metric("Avg Response", "0.8s", "-0.2s")
                st.markdown('</div>', unsafe_allow_html=True)
        
        with col4:
            with st.container():
                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                st.metric("Success Rate", "98%", "+2%")
                st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.info("👆 Start by asking a question in the chat below!")

with tab3:
    # Predictions view
    st.markdown("### 🔮 Market Predictions")
    
    # Sample prediction widgets
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        st.subheader("📈 Revenue Forecast")
        
        # Create sample forecast data
        dates = pd.date_range(start=datetime.now(), periods=12, freq='M')
        forecast = np.random.normal(1000000, 200000, 12).cumsum()
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=dates,
            y=forecast,
            mode='lines+markers',
            name='Forecast',
            line=dict(color='#667eea', width=3),
            fill='tozeroy',
            fillcolor='rgba(102,126,234,0.1)'
        ))
        
        fig.update_layout(
            title="12-Month Revenue Forecast",
            xaxis_title="Month",
            yaxis_title="Revenue ($)",
            height=400,
            margin=dict(t=50, l=50, r=30, b=50),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        st.subheader("🎯 Risk Assessment")
        
        # Sample risk metrics
        risk_data = pd.DataFrame({
            'Category': ['Market Risk', 'Operational Risk', 'Financial Risk', 'Compliance Risk'],
            'Score': [65, 42, 78, 35],
            'Threshold': [70, 50, 80, 40]
        })
        
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=risk_data['Category'],
            y=risk_data['Score'],
            name='Current Score',
            marker_color='#667eea'
        ))
        fig.add_trace(go.Scatter(
            x=risk_data['Category'],
            y=risk_data['Threshold'],
            name='Threshold',
            mode='lines+markers',
            line=dict(color='red', width=2, dash='dash')
        ))
        
        fig.update_layout(
            title="Risk Assessment Scores",
            yaxis_title="Risk Score",
            height=400,
            margin=dict(t=50, l=50, r=30, b=50),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

with tab4:
    # Reports view
    st.markdown("### 📋 Saved Reports")
    
    reports = [
        {"name": "Monthly Deal Summary", "date": "2024-03-01", "type": "PDF", "size": "2.3 MB"},
        {"name": "Vendor Performance Q1", "date": "2024-02-28", "type": "Excel", "size": "1.1 MB"},
        {"name": "Content Rights Analysis", "date": "2024-02-25", "type": "PDF", "size": "3.7 MB"},
        {"name": "Work Order Efficiency", "date": "2024-02-20", "type": "CSV", "size": "0.8 MB"},
    ]
    
    for report in reports:
        col1, col2, col3, col4 = st.columns([3, 2

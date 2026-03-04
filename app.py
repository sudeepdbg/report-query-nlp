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
from utils.database import init_database, execute_sql, get_table_stats
from utils.query_parser import parse_query

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
    
    /* SQL Query box styling */
    .sql-query-box {
        background-color: #1e1e1e;
        color: #d4d4d4;
        border-radius: 10px;
        padding: 15px;
        font-family: 'Courier New', monospace;
        font-size: 14px;
        border-left: 4px solid #667eea;
        margin: 10px 0;
        overflow-x: auto;
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
    
    /* Region indicator */
    .region-indicator {
        display: inline-block;
        padding: 5px 15px;
        border-radius: 20px;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        font-weight: 600;
        margin-right: 10px;
    }
    
    /* Info box styling */
    .info-box {
        background: rgba(102, 126, 234, 0.1);
        border-left: 4px solid #667eea;
        border-radius: 5px;
        padding: 10px 15px;
        margin: 10px 0;
        color: #333;
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

# Function to get region-specific stats
def get_region_stats(conn, region):
    """Get statistics for a specific region"""
    stats = {}
    try:
        # Deals count
        df = pd.read_sql_query(f"SELECT COUNT(*) as count FROM deals WHERE UPPER(region) = '{region}'", conn)
        stats['deals'] = df.iloc[0]['count']
        
        # Vendors count (distinct vendors in the region)
        df = pd.read_sql_query(f"SELECT COUNT(DISTINCT vendor_id) as count FROM deals WHERE UPPER(region) = '{region}'", conn)
        stats['vendors'] = df.iloc[0]['count']
        
        # Content count
        df = pd.read_sql_query(f"SELECT COUNT(*) as count FROM content_planning WHERE UPPER(region) = '{region}'", conn)
        stats['content'] = df.iloc[0]['count']
        
        # Calculate deltas (compare with previous period)
        # This is simplified - in production you'd want actual historical comparison
        stats['deals_delta'] = "+12%"
        stats['vendors_delta'] = "+3"
        stats['content_delta'] = "+8%"
        
    except Exception as e:
        logger.error(f"Error getting region stats: {e}")
        stats = {'deals': 0, 'vendors': 0, 'content': 0, 'deals_delta': "0%", 'vendors_delta': "0", 'content_delta': "0%"}
    
    return stats

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
            'show_sql': True,
            'auto_refresh': False,
            'refresh_interval': 300
        },
        'db_stats': {},
        'region_stats': {},
        'last_query_sql': None,
        'last_query_error': None
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

init_session_state()

# Initialize database connection
try:
    DB_CONN = init_db()
    # Update global database stats
    st.session_state.db_stats = get_table_stats(DB_CONN)
except Exception as e:
    st.error(f"⚠️ Database connection error: {str(e)}")
    st.stop()

# ============================================================================
# Helper Functions
# ============================================================================

def format_currency_value(x):
    """Safely format currency values"""
    try:
        if pd.isna(x) or x is None:
            return "N/A"
        # Convert to float if it's a string
        if isinstance(x, str):
            x = float(x.replace(',', '').replace('$', ''))
        return f"${float(x):,.0f}"
    except (ValueError, TypeError):
        return str(x)

def safe_apply_format(series, formatter):
    """Safely apply formatting to a series"""
    return series.apply(lambda x: formatter(x) if pd.notnull(x) else "N/A")

# ============================================================================
# Charting Functions
# ============================================================================

def create_enhanced_chart(df, chart_type, title, region_context):
    """Create enhanced visualizations with better styling"""
    
    colors = px.colors.qualitative.Set3
    
    # Safely get columns
    if len(df.columns) == 0:
        return go.Figure()
    
    x_col = df.columns[0]
    y_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
    
    # Convert to numeric for y-axis if possible
    if y_col in df.columns:
        try:
            df[y_col] = pd.to_numeric(df[y_col], errors='coerce')
        except:
            pass
    
    if chart_type == "pie":
        fig = go.Figure(data=[go.Pie(
            labels=df[x_col],
            values=df[y_col] if len(df.columns) > 1 else [1] * len(df),
            hole=0.4,
            marker=dict(colors=colors, line=dict(color='white', width=2)),
            textinfo='label+percent',
            textposition='outside',
            hovertemplate="<b>%{label}</b><br>" +
                         "Value: %{value:,.0f}<br>" +
                         "Percentage: %{percent}<br>" +
                         "<extra></extra>"
        )])
        
        fig.update_layout(
            title={
                'text': f"{title} - {region_context}",
                'y': 0.95,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top',
                'font': dict(size=20, color='#1e3c72')
            },
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            margin=dict(t=100, l=50, r=50, b=50),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        
    elif chart_type == "bar":
        # Create text labels safely
        text_values = []
        for val in df[y_col]:
            try:
                if pd.notnull(val) and isinstance(val, (int, float)):
                    text_values.append(f"${val:,.0f}" if val > 1000 else f"{val:,.0f}")
                else:
                    text_values.append(str(val))
            except:
                text_values.append(str(val))
        
        fig = go.Figure(data=[
            go.Bar(
                x=df[x_col],
                y=df[y_col],
                marker_color=colors,
                text=text_values,
                textposition='outside',
                textfont=dict(size=12, color='#1e3c72'),
                hovertemplate="<b>%{x}</b><br>" +
                             "Value: %{y:,.0f}<br>" +
                             "<extra></extra>"
            )
        ])
        
        # Determine if we should show dollar signs
        has_currency = any(col in df.columns for col in ["deal_value", "total_value", "budget", "cost"])
        
        fig.update_layout(
            title={
                'text': f"{title} - {region_context}",
                'y': 0.95,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top',
                'font': dict(size=20, color='#1e3c72')
            },
            xaxis=dict(
                title="",
                tickangle=-45,
                tickfont=dict(size=12),
                gridcolor='rgba(0,0,0,0.1)'
            ),
            yaxis=dict(
                title="Value",
                tickfont=dict(size=12),
                gridcolor='rgba(0,0,0,0.1)',
                tickprefix="$" if has_currency else ""
            ),
            margin=dict(t=100, l=80, r=30, b=100),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            bargap=0.3
        )
        
    elif chart_type == "line":
        fig = go.Figure(data=[
            go.Scatter(
                x=df[x_col],
                y=df[y_col],
                mode='lines+markers',
                line=dict(color='#1e3c72', width=3),
                marker=dict(size=8, color='#ff6b6b'),
                fill='tozeroy',
                fillcolor='rgba(30,60,114,0.1)'
            )
        ])
        
        has_currency = any(col in df.columns for col in ["deal_value", "total_value", "budget", "cost"])
        
        fig.update_layout(
            title={
                'text': f"{title} - {region_context}",
                'y': 0.95,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top',
                'font': dict(size=20, color='#1e3c72')
            },
            xaxis=dict(
                title="Time",
                tickfont=dict(size=12),
                gridcolor='rgba(0,0,0,0.1)'
            ),
            yaxis=dict(
                title="Value",
                tickfont=dict(size=12),
                gridcolor='rgba(0,0,0,0.1)',
                tickprefix="$" if has_currency else ""
            ),
            margin=dict(t=100, l=80, r=30, b=50),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        
    elif chart_type == "heatmap" and len(df.columns) >= 3:
        try:
            pivot_df = df.pivot_table(
                index=df.columns[0],
                columns=df.columns[1],
                values=df.columns[2],
                aggfunc='sum'
            ).fillna(0)
            
            fig = go.Figure(data=go.Heatmap(
                z=pivot_df.values,
                x=pivot_df.columns,
                y=pivot_df.index,
                colorscale='Viridis',
                text=pivot_df.values.round(0),
                texttemplate='%{text}',
                textfont={"size": 10},
                hoverongaps=False,
                hovertemplate="<b>%{y}</b><br>" +
                             "<b>%{x}</b><br>" +
                             "Value: %{z:,.0f}<br>" +
                             "<extra></extra>"
            ))
            
            fig.update_layout(
                title={
                    'text': f"{title} - {region_context}",
                    'y': 0.95,
                    'x': 0.5,
                    'xanchor': 'center',
                    'yanchor': 'top',
                    'font': dict(size=20, color='#1e3c72')
                },
                xaxis=dict(tickangle=-45),
                margin=dict(t=100, l=80, r=30, b=100),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)'
            )
        except:
            fig = create_enhanced_chart(df, "bar", title, region_context)
    
    elif chart_type == "area":
        fig = go.Figure(data=[
            go.Scatter(
                x=df[x_col],
                y=df[y_col],
                mode='lines',
                line=dict(width=0.5, color='rgb(30,60,114)'),
                fill='tonexty',
                fillcolor='rgba(30,60,114,0.3)',
                name='Value'
            )
        ])
        
        has_currency = any(col in df.columns for col in ["deal_value", "total_value", "budget", "cost"])
        
        fig.update_layout(
            title={
                'text': f"{title} - {region_context}",
                'y': 0.95,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top',
                'font': dict(size=20, color='#1e3c72')
            },
            xaxis=dict(tickfont=dict(size=12)),
            yaxis=dict(
                tickfont=dict(size=12),
                tickprefix="$" if has_currency else ""
            ),
            margin=dict(t=100, l=80, r=30, b=50),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
    
    else:
        # Default to bar chart
        fig = create_enhanced_chart(df, "bar", title, region_context)
    
    return fig

# ============================================================================
# Sidebar
# ============================================================================

with st.sidebar:
    st.image("https://via.placeholder.com/200x80/667eea/ffffff?text=FOUNDRY+VANTAGE", width=200)
    
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
        market_options = ["NA", "APAC", "EMEA", "LATAM"]
        # Create a callback for region change
        def on_region_change():
            # Update region stats when region changes
            st.session_state.region_stats = get_region_stats(DB_CONN, st.session_state.region_selector)
        
        selected_region = st.selectbox(
            "Region",
            market_options,
            index=market_options.index(st.session_state.current_region) if st.session_state.current_region in market_options else 1,
            key="region_selector",
            label_visibility="collapsed",
            on_change=on_region_change
        )
        # Update current region
        st.session_state.current_region = selected_region
    
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
    
    # Get region-specific stats if not already loaded
    if not st.session_state.region_stats or st.session_state.region_stats.get('region') != st.session_state.current_region:
        st.session_state.region_stats = get_region_stats(DB_CONN, st.session_state.current_region)
        st.session_state.region_stats['region'] = st.session_state.current_region
    
    # Quick stats (now region-specific)
    st.markdown("### 📊 Quick Stats")
    stats_col1, stats_col2, stats_col3 = st.columns(3)
    with stats_col1:
        st.metric(
            "Deals", 
            f"{st.session_state.region_stats.get('deals', 0):,}", 
            st.session_state.region_stats.get('deals_delta', "0%")
        )
    with stats_col2:
        st.metric(
            "Vendors", 
            f"{st.session_state.region_stats.get('vendors', 0):,}", 
            st.session_state.region_stats.get('vendors_delta', "0")
        )
    with stats_col3:
        st.metric(
            "Content", 
            f"{st.session_state.region_stats.get('content', 0):,}", 
            st.session_state.region_stats.get('content_delta', "0%")
        )
    
    st.markdown("---")
    
    # Active filters indicator
    st.markdown(f"""
        <div style="background: rgba(102,126,234,0.1); padding: 10px; border-radius: 10px; margin-bottom: 10px;">
            <span class="region-indicator">{st.session_state.current_region}</span>
            <span style="color: #666;">Active Filter</span>
        </div>
        <div style="font-size: 12px; color: #666; margin-top: 5px; padding: 0 10px;">
            ℹ️ Query will use this filter unless you specify other regions in your question
        </div>
    """, unsafe_allow_html=True)
    
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
                ("🎬 Rights Analysis", f"SVOD rights breakdown in {reg}"),
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
    
    # Display action buttons with unique keys
    for idx, (action_label, action_query) in enumerate(get_persona_actions(st.session_state.persona, st.session_state.current_region)):
        if st.button(action_label, key=f"action_{idx}_{action_label[:10]}", use_container_width=True):
            st.session_state.pending_prompt = action_query
            st.rerun()
    
    st.markdown("---")
    
    # Favorites section
    st.markdown("### ⭐ Favorite Queries")
    if st.session_state.favorite_queries:
        for i, fav in enumerate(st.session_state.favorite_queries[-5:]):
            if st.button(f"📌 {fav[:30]}...", key=f"fav_{i}_{fav[:10]}", use_container_width=True):
                st.session_state.pending_prompt = fav
                st.rerun()
    else:
        st.info("No favorites yet. Click ★ to save queries.")
    
    if st.button("➕ Add Current to Favorites", key="add_fav", use_container_width=True):
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

# ============================================================================
# Main Content Area
# ============================================================================

# Header with region indicator
col1, col2 = st.columns([3, 1])
with col1:
    st.title(f"🎥 Foundry Vantage - {st.session_state.persona} Intelligence")
with col2:
    st.markdown(f"""
        <div style="text-align: right; padding: 10px;">
            <span class="region-indicator">{st.session_state.current_region}</span>
        </div>
    """, unsafe_allow_html=True)

st.markdown(f"### {st.session_state.date_range} Analysis")

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
                total_records = sum(len(msg["data"]) for msg in st.session_state.chat_history if "data" in msg)
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
        dates = pd.date_range(start=datetime.now(), periods=12, freq='ME')
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
            title=f"12-Month Revenue Forecast - {st.session_state.current_region}",
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
            title=f"Risk Assessment - {st.session_state.current_region}",
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
        {"name": f"Monthly Deal Summary - {st.session_state.current_region}", "date": "2024-03-01", "type": "PDF", "size": "2.3 MB"},
        {"name": f"Vendor Performance Q1 - {st.session_state.current_region}", "date": "2024-02-28", "type": "Excel", "size": "1.1 MB"},
        {"name": f"Content Rights Analysis - {st.session_state.current_region}", "date": "2024-02-25", "type": "PDF", "size": "3.7 MB"},
        {"name": f"Work Order Efficiency - {st.session_state.current_region}", "date": "2024-02-20", "type": "CSV", "size": "0.8 MB"},
    ]
    
    for i, report in enumerate(reports):
        col1, col2, col3, col4, col5 = st.columns([3, 2, 1, 1, 1])
        with col1:
            st.write(f"📄 {report['name']}")
        with col2:
            st.write(report['date'])
        with col3:
            st.write(report['type'])
        with col4:
            st.write(report['size'])
        with col5:
            st.button("📥", key=f"download_report_{i}", use_container_width=True)

# ============================================================================
# Chat Interface
# ============================================================================

# Chat input
user_input = st.chat_input("Ask anything about deals, vendors, content, or market performance...")

# Process pending prompts or new input
active_prompt = None
if st.session_state.get('pending_prompt'):
    active_prompt = st.session_state.pending_prompt
    del st.session_state.pending_prompt
elif user_input:
    active_prompt = user_input

# Display chat history
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"):
        st.markdown(f"**{msg['question']}**")
    
    with st.chat_message("assistant", avatar="🎥"):
        st.markdown(msg.get("answer", "Here are the results:"))
        
        # Show SQL if available and enabled
        if msg.get("sql") and st.session_state.user_preferences['show_sql']:
            with st.expander("🔍 View SQL Query", expanded=False):
                st.markdown(f'<div class="sql-query-box">{msg["sql"]}</div>', unsafe_allow_html=True)
        
        # Display metrics if available
        if msg.get("metrics"):
            metrics_cols = st.columns(len(msg["metrics"]))
            for col, metric in zip(metrics_cols, msg["metrics"]):
                with col:
                    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                    st.metric(metric["label"], metric["value"])
                    st.markdown('</div>', unsafe_allow_html=True)
        
        # Display chart
        if msg.get("chart"):
            st.plotly_chart(msg["chart"], use_container_width=True, key=f"chart_{i}_{time.time()}")
        
        # Enterprise actions
        with st.expander("📊 Enterprise Actions & Data Export", expanded=False):
            col1, col2 = st.columns([3, 1])
            with col1:
                tab_name = st.text_input(
                    "Report Name", 
                    value=f"Foundry_{msg.get('region', st.session_state.current_region)}_{datetime.now().strftime('%Y%m%d')}",
                    key=f"tab_name_{i}"
                )
            with col2:
                if st.button("🚀 Push to Tableau", key=f"push_{i}", use_container_width=True):
                    with st.spinner("Pushing to Tableau..."):
                        st.info("Tableau integration coming soon!")
                        time.sleep(1)
            
            # Export options
            col3, col4, col5 = st.columns(3)
            with col3:
                csv = msg["data"].to_csv(index=False)
                st.download_button(
                    "📥 Download CSV",
                    csv,
                    f"{tab_name}.csv",
                    "text/csv",
                    key=f"download_csv_{i}",
                    use_container_width=True
                )
            with col4:
                st.button("📊 Export to Excel", key=f"excel_{i}", use_container_width=True)
            with col5:
                st.button("📧 Email Report", key=f"email_{i}", use_container_width=True)
            
            # Data preview
            st.markdown("##### Data Preview")
            st.dataframe(
                msg["data"],
                use_container_width=True,
                height=300,
                column_config={
                    "deal_value": st.column_config.NumberColumn("Deal Value", format="$%.2f"),
                    "vendor_rating": st.column_config.NumberColumn("Rating", format="%.1f ⭐")
                }
            )

# Process new query
if active_prompt:
    with st.chat_message("user"):
        st.markdown(f"**{active_prompt}**")
    
    with st.chat_message("assistant", avatar="🎥"):
        with st.spinner(f"🔍 Analyzing market data..."):
            # Parse the query - now returns 4 values
            sql, error, chart_type, region_context = parse_query(active_prompt, st.session_state.current_region)
            
            if error:
                st.error(f"❌ Query parsing error: {error}")
            else:
                # Show SQL if enabled in preferences
                if st.session_state.user_preferences['show_sql']:
                    with st.expander("🔍 View SQL Query", expanded=True):
                        st.markdown(f'<div class="sql-query-box">{sql}</div>', unsafe_allow_html=True)
                
                # Execute the query
                res_df, db_err = execute_sql(sql, DB_CONN)
                
                if res_df is not None and not res_df.empty:
                    # Show region info
                    if 'region' in res_df.columns:
                        returned_regions = res_df['region'].unique().tolist()
                        if len(returned_regions) > 1:
                            st.markdown(f"""
                                <div class="info-box">
                                    📊 Showing data for <strong>{', '.join(returned_regions)}</strong> regions
                                </div>
                            """, unsafe_allow_html=True)
                        elif returned_regions[0] != st.session_state.current_region:
                            st.markdown(f"""
                                <div class="info-box">
                                    📊 Showing data for <strong>{returned_regions[0]}</strong> (override from query)
                                </div>
                            """, unsafe_allow_html=True)
                    
                    # Create enhanced chart based on data
                    fig = create_enhanced_chart(res_df, chart_type, active_prompt, region_context)
                    
                    # Calculate metrics
                    metrics_data = []
                    value_cols = [col for col in res_df.columns if any(x in col.lower() for x in ['value', 'total', 'budget', 'cost'])]
                    
                    if value_cols:
                        val_col = value_cols[0]
                        try:
                            # Convert to numeric safely
                            res_df[val_col] = pd.to_numeric(res_df[val_col], errors='coerce')
                            
                            total = res_df[val_col].sum()
                            avg = res_df[val_col].mean()
                            count = len(res_df)
                            max_val = res_df[val_col].max()
                            
                            # Display metrics in a grid
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                                st.metric("Total Value", f"${total:,.0f}" if pd.notnull(total) else "N/A")
                                st.markdown('</div>', unsafe_allow_html=True)
                            with col2:
                                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                                st.metric("Average Value", f"${avg:,.0f}" if pd.notnull(avg) else "N/A")
                                st.markdown('</div>', unsafe_allow_html=True)
                            with col3:
                                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                                st.metric("Record Count", f"{count:,}")
                                st.markdown('</div>', unsafe_allow_html=True)
                            with col4:
                                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                                st.metric("Maximum", f"${max_val:,.0f}" if pd.notnull(max_val) else "N/A")
                                st.markdown('</div>', unsafe_allow_html=True)
                            
                            metrics_data = [
                                {"label": "Total Value", "value": f"${total:,.0f}" if pd.notnull(total) else "N/A"},
                                {"label": "Average Value", "value": f"${avg:,.0f}" if pd.notnull(avg) else "N/A"},
                                {"label": "Record Count", "value": f"{count:,}"},
                                {"label": "Maximum", "value": f"${max_val:,.0f}" if pd.notnull(max_val) else "N/A"}
                            ]
                        except Exception as e:
                            logger.error(f"Error calculating metrics: {e}")
                    
                    # Display the main chart
                    st.plotly_chart(fig, use_container_width=True, key=f"new_chart_{time.time()}")
                    
                    # Enterprise actions
                    with st.expander("📊 Enterprise Actions & Data Export", expanded=True):
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            live_tab_name = st.text_input(
                                "Report Name",
                                value=f"Foundry_{region_context}_{datetime.now().strftime('%Y%m%d_%H%M')}",
                                key="live_tab_name"
                            )
                        with col2:
                            if st.button("🚀 Push to Tableau", key="live_push", use_container_width=True):
                                with st.spinner("Pushing to Tableau..."):
                                    st.info("Tableau integration coming soon!")
                                    time.sleep(1)
                        
                        # Data preview with enhanced formatting
                        st.markdown("##### Detailed Data View")
                        
                        # Format currency columns safely
                        display_df = res_df.copy()
                        for col in display_df.columns:
                            if any(x in col.lower() for x in ['value', 'price', 'budget', 'cost']):
                                display_df[col] = safe_apply_format(display_df[col], format_currency_value)
                        
                        st.dataframe(
                            display_df,
                            use_container_width=True,
                            height=400,
                            column_config={
                                "deal_date": st.column_config.DateColumn("Date", format="YYYY-MM-DD"),
                                "vendor_rating": st.column_config.NumberColumn("Rating", format="%.1f ⭐")
                            }
                        )
                        
                        # Download options
                        col3, col4, col5 = st.columns(3)
                        with col3:
                            csv = res_df.to_csv(index=False)
                            st.download_button(
                                "📥 Download as CSV",
                                csv,
                                f"{live_tab_name}.csv",
                                "text/csv",
                                use_container_width=True,
                                key="download_csv_live"
                            )
                        with col4:
                            st.button("📊 Export to Excel", key="excel_export_live", use_container_width=True)
                        with col5:
                            st.button("📧 Schedule Report", key="schedule_report_live", use_container_width=True)
                    
                    # Add to chat history
                    st.session_state.chat_history.append({
                        "question": active_prompt,
                        "answer": f"📊 Analysis complete. Here are the key insights:",
                        "data": res_df,
                        "chart": fig,
                        "metrics": metrics_data,
                        "sql": sql,
                        "region": region_context
                    })
                    
                    # Auto-scroll to bottom
                    components.html(
                        """
                        <script>
                        window.parent.document.querySelector('section.main').scrollTo({
                            top: window.parent.document.querySelector('section.main').scrollHeight,
                            behavior: 'smooth'
                        });
                        </script>
                        """,
                        height=0
                    )
                    
                    st.rerun()
                else:
                    st.warning(f"ℹ️ No records found for your query. Try a different query or region.")

# Footer
st.markdown("---")
st.markdown(
    """
    <div class="footer">
        <p>🎥 Foundry Vantage - Enterprise Content Intelligence Platform | v3.0</p>
        <p style='font-size: 12px;'>© 2024 Foundry. All rights reserved. | 
        <a href="#" style='color: white; text-decoration: none;'>Privacy</a> | 
        <a href="#" style='color: white; text-decoration: none;'>Terms</a> | 
        <a href="#" style='color: white; text-decoration: none;'>Support</a></p>
    </div>
    """,
    unsafe_allow_html=True
)

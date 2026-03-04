import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit.components.v1 as components
import time
import numpy as np
from datetime import datetime
from utils.database import init_database, execute_sql
from utils.query_parser import parse_query
from utils.tableau_sync import trigger_tableau_report

# Page configuration
st.set_page_config(
    page_title="Foundry Vantage - Content Intelligence Platform",
    page_icon="🎥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    /* Main container styling */
    .main {
        background-color: #f8f9fa;
    }
    
    /* Card styling for metrics */
    .metric-card {
        background: white;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin: 10px 0;
    }
    
    /* Header styling */
    .insight-header {
        color: #1e3c72;
        font-size: 24px;
        font-weight: 600;
        margin-bottom: 20px;
    }
    
    /* Chat message styling */
    .stChatMessage {
        background-color: white;
        border-radius: 10px;
        padding: 15px;
        margin: 10px 0;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    
    /* Sidebar styling */
    .css-1d391kg {
        background-color: white;
    }
    
    /* Button styling */
    .stButton > button {
        width: 100%;
        border-radius: 20px;
        border: none;
        background-color: #f0f2f6;
        color: #1e3c72;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        background-color: #1e3c72;
        color: white;
        box-shadow: 0 4px 8px rgba(30,60,114,0.2);
    }
    
    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 20px;
        padding: 8px 16px;
        background-color: white;
    }
    
    /* Metric styling */
    [data-testid="stMetricValue"] {
        font-size: 28px;
        font-weight: 700;
        color: #1e3c72;
    }
    
    [data-testid="stMetricLabel"] {
        font-size: 14px;
        font-weight: 500;
        color: #666;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_resource(ttl=3600)  # Cache DB connection for 1 hour
def get_db():
    return init_database()

DB_CONN = get_db()

# Initialize Session States
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if 'current_region' not in st.session_state:
    st.session_state.current_region = 'APAC'
if 'persona' not in st.session_state:
    st.session_state.persona = 'Product'
if 'favorite_queries' not in st.session_state:
    st.session_state.favorite_queries = []
if 'date_range' not in st.session_state:
    st.session_state.date_range = 'Last 30 Days'
if 'view_mode' not in st.session_state:
    st.session_state.view_mode = 'Analytics'

def create_enhanced_chart(df, chart_type, title, region):
    """Create enhanced visualizations with better styling"""
    
    colors = px.colors.qualitative.Set3
    
    if chart_type == "pie":
        fig = go.Figure(data=[go.Pie(
            labels=df[df.columns[0]],
            values=df[df.columns[1]] if len(df.columns) > 1 else [1] * len(df),
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
                'text': f"{title} - {region}",
                'y':0.95,
                'x':0.5,
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
        fig = go.Figure(data=[
            go.Bar(
                x=df[df.columns[0]],
                y=df[df.columns[1]],
                marker_color=colors,
                text=df[df.columns[1]].apply(lambda x: f'${x:,.0f}' if x > 1000 else f'{x:,.0f}'),
                textposition='outside',
                textfont=dict(size=12, color='#1e3c72'),
                hovertemplate="<b>%{x}</b><br>" +
                             "Value: %{y:,.0f}<br>" +
                             "<extra></extra>"
            )
        ])
        
        fig.update_layout(
            title={
                'text': f"{title} - {region}",
                'y':0.95,
                'x':0.5,
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
                tickprefix="$" if any(col in df.columns for col in ["deal_value", "total_value"]) else ""
            ),
            margin=dict(t=100, l=80, r=30, b=100),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            bargap=0.3
        )
        
    elif chart_type == "line":
        fig = go.Figure(data=[
            go.Scatter(
                x=df[df.columns[0]],
                y=df[df.columns[1]],
                mode='lines+markers',
                line=dict(color='#1e3c72', width=3),
                marker=dict(size=8, color='#ff6b6b'),
                fill='tozeroy',
                fillcolor='rgba(30,60,114,0.1)'
            )
        ])
        
        fig.update_layout(
            title={
                'text': f"{title} - {region}",
                'y':0.95,
                'x':0.5,
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
                tickprefix="$" if any(col in df.columns for col in ["deal_value", "total_value"]) else ""
            ),
            margin=dict(t=100, l=80, r=30, b=50),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        
    elif chart_type == "heatmap":
        # Create correlation matrix or pivot table
        if len(df.columns) >= 3:
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
                    'text': f"{title} - {region}",
                    'y':0.95,
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top',
                    'font': dict(size=20, color='#1e3c72')
                },
                xaxis=dict(tickangle=-45),
                margin=dict(t=100, l=80, r=30, b=100),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)'
            )
        else:
            fig = create_enhanced_chart(df, "bar", title, region)
    
    elif chart_type == "area":
        fig = go.Figure(data=[
            go.Scatter(
                x=df[df.columns[0]],
                y=df[df.columns[1]],
                mode='lines',
                line=dict(width=0.5, color='rgb(30,60,114)'),
                fill='tonexty',
                fillcolor='rgba(30,60,114,0.3)',
                name='Value'
            )
        ])
        
        fig.update_layout(
            title={
                'text': f"{title} - {region}",
                'y':0.95,
                'x':0.5,
                'xanchor': 'center',
                'yanchor': 'top',
                'font': dict(size=20, color='#1e3c72')
            },
            xaxis=dict(tickfont=dict(size=12)),
            yaxis=dict(
                tickfont=dict(size=12),
                tickprefix="$" if any(col in df.columns for col in ["deal_value", "total_value"]) else ""
            ),
            margin=dict(t=100, l=80, r=30, b=50),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
    
    else:
        # Default to bar chart
        fig = create_enhanced_chart(df, "bar", title, region)
    
    return fig

def create_dashboard_overview(df, region):
    """Create an executive dashboard overview"""
    
    # Calculate key metrics
    if 'deal_value' in df.columns:
        total_value = df['deal_value'].sum()
        avg_value = df['deal_value'].mean()
        max_value = df['deal_value'].max()
        min_value = df['deal_value'].min()
        deal_count = len(df)
        
        # Create gauge charts for KPIs
        fig_gauge = make_subplots(
            rows=1, cols=3,
            specs=[[{'type': 'indicator'}, {'type': 'indicator'}, {'type': 'indicator'}]],
            subplot_titles=('Total Value', 'Average Value', 'Deal Count')
        )
        
        fig_gauge.add_trace(
            go.Indicator(
                mode="number+gauge+delta",
                value=total_value,
                title={"text": "Total Value"},
                delta={'reference': 10000000},
                gauge={
                    'axis': {'range': [None, total_value * 1.5]},
                    'bar': {'color': "#1e3c72"},
                    'steps': [
                        {'range': [0, total_value * 0.5], 'color': "lightgray"},
                        {'range': [total_value * 0.5, total_value], 'color': "gray"}
                    ],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': total_value * 0.9
                    }
                },
                number={'prefix': "$", 'font': {'size': 20}}
            ),
            row=1, col=1
        )
        
        fig_gauge.add_trace(
            go.Indicator(
                mode="number",
                value=avg_value,
                title={"text": "Average Value"},
                number={'prefix': "$", 'font': {'size': 20}}
            ),
            row=1, col=2
        )
        
        fig_gauge.add_trace(
            go.Indicator(
                mode="number",
                value=deal_count,
                title={"text": "Deal Count"},
                number={'font': {'size': 20}}
            ),
            row=1, col=3
        )
        
        fig_gauge.update_layout(
            height=250,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            margin=dict(t=50, l=30, r=30, b=30)
        )
        
        return fig_gauge
    
    return None

# Sidebar
with st.sidebar:
    st.image("https://via.placeholder.com/200x80/1e3c72/ffffff?text=FOUNDRY+VANTAGE", use_container_width=True)
    
    st.markdown("### 🎯 Market Selection")
    market_options = ["NA", "APAC", "EMEA", "LATAM"]
    st.session_state.current_region = st.selectbox(
        "Region",
        market_options,
        index=market_options.index(st.session_state.current_region),
        key="region_selector"
    )
    
    st.markdown("### 👤 User Persona")
    persona_options = ["Leadership", "Product", "Operations", "Finance", "Analytics"]
    st.session_state.persona = st.selectbox(
        "View as",
        persona_options,
        index=persona_options.index(st.session_state.persona) if st.session_state.persona in persona_options else 0,
        key="persona_selector"
    )
    
    st.markdown("### 📅 Time Period")
    time_options = ["Last 7 Days", "Last 30 Days", "Last 90 Days", "Last 12 Months", "Year to Date", "All Time"]
    st.session_state.date_range = st.selectbox("Date Range", time_options, index=1)
    
    st.markdown("### 📊 View Mode")
    view_options = ["Analytics", "Dashboard", "Reports", "Forecast"]
    st.session_state.view_mode = st.selectbox("Mode", view_options, index=0)
    
    st.divider()
    
    # Persona-specific quick actions
    st.markdown(f"### 💡 {st.session_state.persona} Quick Actions")
    
    def get_persona_actions(persona, reg):
        actions = {
            "Leadership": [
                ("📈 Market Performance Overview", f"Show market performance overview for {reg}"),
                ("💰 Revenue by Region", f"Revenue breakdown by region {reg}"),
                ("🎯 Strategic Deals Analysis", f"Top strategic deals in {reg}"),
                ("📊 YTD vs Budget", f"Year to date performance vs budget in {reg}")
            ],
            "Product": [
                ("🎬 Content Rights Analysis", f"SVOD rights breakdown in {reg}"),
                ("📺 Genre Performance", f"Content performance by genre in {reg}"),
                ("🆕 New Releases", f"New content releases in {reg}"),
                ("📈 Pipeline Forecast", f"Content pipeline forecast for {reg}")
            ],
            "Operations": [
                ("⚡ Work Order Status", f"Work order status overview for {reg}"),
                ("⏰ Delayed Tasks", f"Delayed work orders in {reg}"),
                ("👥 Resource Allocation", f"Resource allocation by vendor in {reg}"),
                ("📋 Quality Metrics", f"Quality metrics for {reg}")
            ],
            "Finance": [
                ("💰 Deal Value Analysis", f"Deal value analysis for {reg}"),
                ("📊 Vendor Spend", f"Total spend per vendor in {reg}"),
                ("💱 Currency Impact", f"Currency impact analysis in {reg}"),
                ("📈 Forecast vs Actual", f"Forecast vs actual spend in {reg}")
            ],
            "Analytics": [
                ("📊 Trend Analysis", f"Market trends in {reg}"),
                ("🔮 Predictive Insights", f"Predictive analytics for {reg}"),
                ("📈 Correlation Analysis", f"Deal correlations in {reg}"),
                ("🎯 Anomaly Detection", f"Anomalies in {reg} market")
            ]
        }
        return actions.get(persona, actions["Product"])
    
    for action_label, action_query in get_persona_actions(st.session_state.persona, st.session_state.current_region):
        if st.button(action_label, use_container_width=True):
            st.session_state.pending_prompt = action_query
            st.rerun()
    
    st.divider()
    
    # Favorites section
    st.markdown("### ⭐ Favorite Queries")
    if st.session_state.favorite_queries:
        for i, fav in enumerate(st.session_state.favorite_queries[-5:]):  # Show last 5 favorites
            if st.button(f"📌 {fav[:30]}...", key=f"fav_{i}", use_container_width=True):
                st.session_state.pending_prompt = fav
                st.rerun()
    
    if st.button("➕ Add Current Query to Favorites", use_container_width=True):
        if st.session_state.chat_history:
            last_query = st.session_state.chat_history[-1]["question"]
            if last_query not in st.session_state.favorite_queries:
                st.session_state.favorite_queries.append(last_query)
                st.success("Added to favorites!")

# Main content area
st.title(f"🎥 Foundry Vantage - {st.session_state.persona} Intelligence")
st.markdown(f"### {st.session_state.current_region} Market Analysis • {st.session_state.date_range}")

# Create tabs for different views
tab1, tab2, tab3 = st.tabs(["📊 Analytics", "📈 Insights", "🔮 Predictions"])

with tab1:
    # Main analytics view
    pass  # Content will be filled by the chat responses

with tab2:
    # Insights view
    if st.session_state.chat_history:
        st.markdown("### 📊 Key Insights")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Total Queries", len(st.session_state.chat_history), "+2 today")
            st.markdown('</div>', unsafe_allow_html=True)
        with col2:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Data Points Analyzed", "12.5K", "+5.2%")
            st.markdown('</div>', unsafe_allow_html=True)
        with col3:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Avg Response Time", "0.8s", "-0.2s")
            st.markdown('</div>', unsafe_allow_html=True)

with tab3:
    # Predictions view
    st.markdown("### 🔮 Market Predictions")
    st.info("Enable predictive analytics to see forecasts and trends")

# Chat input
user_input = st.chat_input("Ask anything about deals, vendors, content, or market performance...")

# Process pending prompts or new input
active_prompt = None
if st.session_state.get('pending_prompt'):
    active_prompt = st.session_state.pending_prompt
    del st.session_state.pending_prompt
elif user_input:
    active_prompt = user_input

# Update region from query
if active_prompt:
    for r in ["NA", "APAC", "EMEA", "LATAM"]:
        if r.lower() in active_prompt.lower():
            st.session_state.current_region = r
            break

# Display chat history
for i, msg in enumerate(st.session_state.chat_history):
    with st.chat_message("user"):
        st.markdown(f"**{msg['question']}**")
    
    with st.chat_message("assistant", avatar="🎥"):
        st.markdown(msg["answer"])
        
        # Display metrics if available
        if msg.get("metrics"):
            cols = st.columns(len(msg["metrics"]))
            for col, metric in zip(cols, msg["metrics"]):
                with col:
                    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                    st.metric(metric["label"], metric["value"])
                    st.markdown('</div>', unsafe_allow_html=True)
        
        # Display chart
        if msg["chart"]:
            st.plotly_chart(msg["chart"], use_container_width=True, key=f"chart_{i}_{time.time()}")
        
        # Enterprise actions
        with st.expander("📊 Enterprise Actions & Data Export", expanded=False):
            col1, col2 = st.columns([3, 1])
            with col1:
                tab_name = st.text_input(
                    "Report Name", 
                    value=f"Foundry_{st.session_state.current_region}_{datetime.now().strftime('%Y%m%d')}",
                    key=f"tab_name_{i}"
                )
            with col2:
                if st.button("🚀 Push to Tableau", key=f"push_{i}"):
                    with st.spinner("Pushing to Tableau..."):
                        success, info = trigger_tableau_report(msg["data"], tab_name)
                        if success:
                            st.success("✅ Report pushed successfully!")
                        else:
                            st.error(f"❌ Failed: {info}")
            
            # Export options
            col3, col4, col5 = st.columns(3)
            with col3:
                csv = msg["data"].to_csv(index=False)
                st.download_button(
                    "📥 Download CSV",
                    csv,
                    f"{tab_name}.csv",
                    "text/csv",
                    use_container_width=True
                )
            with col4:
                st.button("📊 Export to Excel", use_container_width=True)
            with col5:
                st.button("📧 Email Report", use_container_width=True)
            
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
        active_reg = st.session_state.current_region
        
        with st.spinner(f"🔍 Analyzing {active_reg} market data..."):
            sql, error, chart_type = parse_query(active_prompt, active_reg)
            
            if error:
                st.error(f"❌ Query parsing error: {error}")
            else:
                # Show SQL for transparency (optional)
                with st.expander("View SQL Query", expanded=False):
                    st.code(sql, language="sql")
                
                res_df, db_err = execute_sql(sql, DB_CONN)
                
                if res_df is not None and not res_df.empty:
                    # Create enhanced chart
                    fig = create_enhanced_chart(res_df, chart_type, active_prompt, active_reg)
                    
                    # Calculate metrics
                    metrics_data = []
                    if any(col in res_df.columns for col in ["deal_value", "total_value", "value"]):
                        val_col = next(col for col in ["deal_value", "total_value", "value"] if col in res_df.columns)
                        
                        # Create dashboard overview
                        dashboard = create_dashboard_overview(res_df, active_reg)
                        if dashboard:
                            st.plotly_chart(dashboard, use_container_width=True)
                        
                        # Calculate key metrics
                        total = res_df[val_col].sum()
                        avg = res_df[val_col].mean()
                        count = len(res_df)
                        max_val = res_df[val_col].max()
                        
                        # Display metrics in a grid
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                            st.metric("Total Value", f"${total:,.0f}")
                            st.markdown('</div>', unsafe_allow_html=True)
                        with col2:
                            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                            st.metric("Average Value", f"${avg:,.0f}")
                            st.markdown('</div>', unsafe_allow_html=True)
                        with col3:
                            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                            st.metric("Deal Count", f"{count:,}")
                            st.markdown('</div>', unsafe_allow_html=True)
                        with col4:
                            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                            st.metric("Max Deal", f"${max_val:,.0f}")
                            st.markdown('</div>', unsafe_allow_html=True)
                        
                        metrics_data = [
                            {"label": "Total Value", "value": f"${total:,.0f}"},
                            {"label": "Average Value", "value": f"${avg:,.0f}"},
                            {"label": "Deal Count", "value": f"{count:,}"},
                            {"label": "Max Deal", "value": f"${max_val:,.0f}"}
                        ]
                    
                    # Display the main chart
                    st.plotly_chart(fig, use_container_width=True, key=f"new_chart_{time.time()}")
                    
                    # Enterprise actions
                    with st.expander("📊 Enterprise Actions & Data Export", expanded=True):
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            live_tab_name = st.text_input(
                                "Report Name",
                                value=f"Foundry_{active_reg}_{datetime.now().strftime('%Y%m%d_%H%M')}",
                                key="live_tab_name"
                            )
                        with col2:
                            if st.button("🚀 Push to Tableau", key="live_push", use_container_width=True):
                                with st.spinner("Pushing to Tableau..."):
                                    success, info = trigger_tableau_report(res_df, live_tab_name)
                                    if success:
                                        st.success("✅ Report pushed successfully!")
                                    else:
                                        st.error(f"❌ Failed: {info}")
                        
                        # Data preview with enhanced formatting
                        st.markdown("##### Detailed Data View")
                        
                        # Format currency columns
                        display_df = res_df.copy()
                        for col in display_df.columns:
                            if 'value' in col.lower() or 'price' in col.lower() or 'budget' in col.lower():
                                display_df[col] = display_df[col].apply(lambda x: f"${x:,.2f}")
                        
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
                                use_container_width=True
                            )
                        with col4:
                            # Excel export would require additional library
                            st.button("📊 Export to Excel", use_container_width=True)
                        with col5:
                            st.button("📧 Schedule Report", use_container_width=True)
                    
                    # Add to chat history
                    st.session_state.chat_history.append({
                        "question": active_prompt,
                        "answer": f"📊 Analysis for {active_reg} market complete. Here are the key insights:",
                        "data": res_df,
                        "chart": fig,
                        "metrics": metrics_data
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
                    st.warning(f"ℹ️ No records found for '{active_prompt}' in {active_reg}. Try a different query or region.")

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666; padding: 20px;'>
        <p>🎥 Foundry Vantage - Enterprise Content Intelligence Platform | v2.0</p>
        <p style='font-size: 12px;'>© 2024 Foundry. All rights reserved.</p>
    </div>
    """,
    unsafe_allow_html=True
)

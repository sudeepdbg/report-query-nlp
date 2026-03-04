import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import Dict, Any, Tuple, Optional
import numpy as np

class SemanticCharting:
    """Intelligent charting that automatically selects appropriate visualizations"""
    
    @staticmethod
    def analyze_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze dataframe to understand its structure and content"""
        analysis = {
            'row_count': len(df),
            'column_count': len(df.columns),
            'numeric_columns': [],
            'categorical_columns': [],
            'date_columns': [],
            'text_columns': [],
            'has_currency': False,
            'has_percentage': False,
            'has_geography': False,
            'suggested_chart': None,
            'x_axis': None,
            'y_axis': None,
            'color_by': None
        }
        
        currency_keywords = ['value', 'price', 'cost', 'budget', 'spend', 'revenue', 'deal', 'mg', 'guarantee']
        percentage_keywords = ['share', 'rate', 'score', 'rating', 'percent']
        geo_keywords = ['region', 'country', 'city', 'state', 'territory']
        
        for col in df.columns:
            col_lower = col.lower()
            
            # Detect column types
            if pd.api.types.is_numeric_dtype(df[col]):
                if any(keyword in col_lower for keyword in currency_keywords):
                    analysis['has_currency'] = True
                if any(keyword in col_lower for keyword in percentage_keywords):
                    analysis['has_percentage'] = True
                analysis['numeric_columns'].append(col)
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                analysis['date_columns'].append(col)
            elif df[col].nunique() < 20:  # Categorical if few unique values
                analysis['categorical_columns'].append(col)
                if any(keyword in col_lower for keyword in geo_keywords):
                    analysis['has_geography'] = True
            else:
                analysis['text_columns'].append(col)
        
        return analysis
    
    @staticmethod
    def suggest_chart(df: pd.DataFrame, analysis: Dict[str, Any]) -> Tuple[str, Dict[str, str]]:
        """Suggest the best chart type and axis mappings"""
        
        suggestions = {
            'chart_type': 'bar',
            'mappings': {}
        }
        
        # Time series detection
        if analysis['date_columns'] and analysis['numeric_columns']:
            suggestions['chart_type'] = 'line'
            suggestions['mappings'] = {
                'x': analysis['date_columns'][0],
                'y': analysis['numeric_columns'][0]
            }
            if len(analysis['categorical_columns']) > 0:
                suggestions['mappings']['color'] = analysis['categorical_columns'][0]
        
        # Distribution analysis
        elif len(analysis['numeric_columns']) == 1 and len(analysis['categorical_columns']) > 0:
            # Check if it's a small number of categories for pie
            if df[analysis['categorical_columns'][0]].nunique() <= 7:
                suggestions['chart_type'] = 'pie'
                suggestions['mappings'] = {
                    'names': analysis['categorical_columns'][0],
                    'values': analysis['numeric_columns'][0]
                }
            else:
                suggestions['chart_type'] = 'bar'
                suggestions['mappings'] = {
                    'x': analysis['categorical_columns'][0],
                    'y': analysis['numeric_columns'][0]
                }
        
        # Multi-category comparison
        elif len(analysis['categorical_columns']) >= 2 and len(analysis['numeric_columns']) >= 1:
            suggestions['chart_type'] = 'heatmap'
            suggestions['mappings'] = {
                'x': analysis['categorical_columns'][0],
                'y': analysis['categorical_columns'][1],
                'z': analysis['numeric_columns'][0]
            }
        
        # Geographic data
        elif analysis['has_geography'] and analysis['numeric_columns']:
            suggestions['chart_type'] = 'choropleth' if 'country' in str(analysis['categorical_columns']).lower() else 'bar'
            suggestions['mappings'] = {
                'locations': next((col for col in analysis['categorical_columns'] if 'country' in col.lower()), analysis['categorical_columns'][0]),
                'color': analysis['numeric_columns'][0]
            }
        
        # Default to bar chart
        else:
            if analysis['categorical_columns']:
                suggestions['mappings']['x'] = analysis['categorical_columns'][0]
            elif analysis['text_columns']:
                suggestions['mappings']['x'] = analysis['text_columns'][0]
            else:
                suggestions['mappings']['x'] = df.columns[0]
            
            if analysis['numeric_columns']:
                suggestions['mappings']['y'] = analysis['numeric_columns'][0]
            else:
                # Create count aggregation
                suggestions['chart_type'] = 'bar'
                suggestions['mappings']['y'] = 'count'
        
        return suggestions['chart_type'], suggestions['mappings']
    
    @staticmethod
    def create_chart(df: pd.DataFrame, title: str = "Data Visualization", 
                     chart_type: Optional[str] = None, 
                     mappings: Optional[Dict] = None) -> go.Figure:
        """Create an intelligent chart based on data analysis"""
        
        # Analyze data
        analysis = SemanticCharting.analyze_dataframe(df)
        
        # Auto-suggest if not provided
        if chart_type is None or mappings is None:
            suggested_type, suggested_mappings = SemanticCharting.suggest_chart(df, analysis)
            chart_type = chart_type or suggested_type
            mappings = mappings or suggested_mappings
        
        # Color palette
        colors = px.colors.qualitative.Set3
        
        # Create appropriate chart
        if chart_type == 'pie' and 'names' in mappings and 'values' in mappings:
            fig = px.pie(
                df, 
                names=mappings['names'], 
                values=mappings['values'],
                title=title,
                hole=0.4,
                color_discrete_sequence=colors
            )
            fig.update_traces(
                textposition='inside', 
                textinfo='percent+label',
                hovertemplate="<b>%{label}</b><br>Value: %{value:,.0f}<br>Percentage: %{percent}<extra></extra>"
            )
        
        elif chart_type == 'line' and 'x' in mappings and 'y' in mappings:
            fig = px.line(
                df, 
                x=mappings['x'], 
                y=mappings['y'],
                color=mappings.get('color'),
                title=title,
                markers=True,
                color_discrete_sequence=colors
            )
            fig.update_traces(
                line=dict(width=3),
                marker=dict(size=8),
                hovertemplate="<b>%{x}</b><br>Value: %{y:,.0f}<br><extra></extra>"
            )
        
        elif chart_type == 'heatmap' and all(k in mappings for k in ['x', 'y', 'z']):
            # Pivot data for heatmap
            pivot_df = df.pivot_table(
                index=mappings['y'],
                columns=mappings['x'],
                values=mappings['z'],
                aggfunc='mean'
            ).fillna(0)
            
            fig = go.Figure(data=go.Heatmap(
                z=pivot_df.values,
                x=pivot_df.columns,
                y=pivot

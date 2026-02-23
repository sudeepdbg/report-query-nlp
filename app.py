import os
import sqlite3
import pandas as pd
import random
import plotly
import plotly.express as px
import json
from flask import Flask, render_template, request, jsonify, session
from datetime import datetime
import logging

from query_parser import parse_query, execute_sql
from sql_validator import validate_sql

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key")  # Needed for session

# ------------------------------------------------------------------
# Database initialization (runs once at startup)
# ------------------------------------------------------------------
def init_database():
    # ... (your existing init_database code) ...
    pass

init_database()

# In-memory chat history (for demo only; lost on restart)
chat_history = []

# ------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------
@app.route('/')
def index():
    """Render the main chat interface."""
    region = request.args.get('region', 'NA')
    team = request.args.get('team', 'leadership')
    return render_template('index.html', chat_history=chat_history, current_region=region, current_team=team)

@app.route('/ask', methods=['POST'])
def ask():
    """Process a natural language query and return results, with filters."""
    data = request.json
    question = data.get('question', '')
    region = data.get('region', 'NA')
    team = data.get('team', 'leadership')
    dashboard = data.get('dashboard', 'executive')
    
    # Retrieve conversation context from session
    context = session.get('last_context', '')
    
    if not question:
        return jsonify({'error': 'No question provided'}), 400
    
    # Parse query using LLM-first approach
    sql, error = parse_query(question, region, team, dashboard, context)  # Note: we need to modify parse_query to accept context
    if error:
        return jsonify({
            'answer': error,
            'sql': None,
            'data': None,
            'chart': None
        })
    
    # Execute SQL
    df, exec_error = execute_sql(sql)
    if exec_error:
        return jsonify({
            'answer': f"Query execution failed: {exec_error}",
            'sql': sql,
            'data': None,
            'chart': None
        })
    
    # Format response
    if df.empty:
        answer = "No results found."
        data_html = None
        chart_json = None
    elif len(df) == 1 and df.shape[1] == 1:
        answer = f"**Result:** {df.iloc[0,0]}"
        data_html = None
        chart_json = None
    else:
        answer = f"Found **{len(df)}** results:"
        data_html = df.to_html(classes='table table-striped', index=False)
        # Generate chart if appropriate
        chart_json = auto_chart(df)
    
    # Store in chat history and session context
    chat_entry = {
        'question': question,
        'answer': answer,
        'sql': sql,
        'data': data_html,
        'chart': chart_json
    }
    chat_history.append(chat_entry)
    
    # Store context for follow-ups (last result)
    session['last_context'] = {
        'question': question,
        'sql': sql,
        'result': df.to_dict(orient='records') if not df.empty else []
    }
    
    return jsonify(chat_entry)

def auto_chart(df):
    """Automatically generate a Plotly chart based on dataframe shape."""
    try:
        if df.shape[1] < 2:
            return None
        # If first column is date-like and second numeric -> line chart
        if pd.api.types.is_datetime64_any_dtype(df.iloc[:,0]) or df.iloc[:,0].dtype == 'object' and len(df) > 1:
            # Attempt to convert to datetime
            try:
                pd.to_datetime(df.iloc[:,0])
                fig = px.line(df, x=df.columns[0], y=df.columns[1], title="Trend")
                return json.loads(fig.to_json())
            except:
                pass
        # If first column categorical and second numeric -> bar/pie based on cardinality
        if df.shape[1] == 2 and pd.api.types.is_numeric_dtype(df.iloc[:,1]):
            if len(df) <= 10:
                fig = px.pie(df, names=df.columns[0], values=df.columns[1], title="Distribution")
            else:
                fig = px.bar(df, x=df.columns[0], y=df.columns[1], title="Breakdown")
            return json.loads(fig.to_json())
        # If more columns, maybe a heatmap? For simplicity, return None.
        return None
    except Exception as e:
        logger.error(f"Chart generation failed: {e}")
        return None

@app.route('/feedback', methods=['POST'])
def feedback():
    """Collect user feedback on answers."""
    data = request.json
    feedback_type = data.get('feedback')  # 'up' or 'down'
    question = data.get('question')
    sql = data.get('sql')
    # Log to file or database (here we just print)
    logger.info(f"Feedback: {feedback_type} | Question: {question} | SQL: {sql}")
    return jsonify({'status': 'ok'})

@app.route('/dashboards/<name>')
def dashboard(name):
    # ... (your existing dashboard code) ...
    pass

@app.route('/healthz')
def health_check():
    # ... (your existing health check) ...
    pass

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))

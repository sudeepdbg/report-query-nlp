import os
import sqlite3
import pandas as pd
from flask import Flask, render_template, request, jsonify
from datetime import datetime
import json

# Import your rule-based query parser (should be in same directory)
from query_parser import parse_query, execute_sql

app = Flask(__name__)

# ------------------------------------------------------------------
# Database initialization (runs once at startup)
# ------------------------------------------------------------------
def init_database():
    """Create tables and insert sample data if they don't exist."""
    conn = sqlite3.connect('vantage.db')
    c = conn.cursor()
    
    # Content Planning table
    c.execute('''
        CREATE TABLE IF NOT EXISTS content_planning (
            id INTEGER PRIMARY KEY,
            network TEXT,
            content_title TEXT,
            status TEXT,
            planned_date TEXT,
            region TEXT
        )
    ''')
    
    # Work Orders table
    c.execute('''
        CREATE TABLE IF NOT EXISTS work_orders (
            id INTEGER PRIMARY KEY,
            work_order TEXT,
            offering TEXT,
            status TEXT,
            due_date TEXT,
            region TEXT,
            vendor TEXT,
            priority TEXT
        )
    ''')
    
    # Deals table
    c.execute('''
        CREATE TABLE IF NOT EXISTS deals (
            id INTEGER PRIMARY KEY,
            deal_name TEXT,
            vendor TEXT,
            deal_value REAL,
            deal_date TEXT,
            region TEXT,
            status TEXT
        )
    ''')
    
    # Sample data (only insert if empty)
    c.execute("SELECT COUNT(*) FROM content_planning")
    if c.fetchone()[0] == 0:
        sample_content = [
            (1, 'MAX Australia', 'The Last Kingdom', 'Delivered', '2024-03-15', 'APAC'),
            (2, 'MAX Australia', 'Dune: Prophecy', 'Scheduled', '2024-04-01', 'APAC'),
            (3, 'MAX US', 'House of the Dragon S2', 'Fulfilled', '2024-05-20', 'NA'),
            (4, 'MAX US', 'The Penguin', 'Not Ready', '2024-06-10', 'NA'),
            (5, 'MAX Europe', 'The White Lotus S3', 'Scheduled', '2024-07-01', 'EMEA')
        ]
        c.executemany("INSERT INTO content_planning VALUES (?,?,?,?,?,?)", sample_content)
    
    c.execute("SELECT COUNT(*) FROM work_orders")
    if c.fetchone()[0] == 0:
        sample_orders = [
            (1, 'WO-2024-001', 'MAX Australia - Migration', 'Delayed', '2024-03-20', 'APAC', 'Vendor A', 'A'),
            (2, 'WO-2024-002', 'MAX US - Encoding', 'In Progress', '2024-03-25', 'NA', 'Vendor B', 'A'),
            (3, 'WO-2024-003', 'MAX Europe - Subtitle', 'Completed', '2024-02-15', 'EMEA', 'Vendor C', 'B'),
            (4, 'WO-2024-004', 'MAX US - QC Review', 'In Progress', '2024-03-30', 'NA', 'Vendor B', 'A'),
            (5, 'WO-2024-005', 'MAX Australia - Audio', 'Delayed', '2024-03-10', 'APAC', 'Vendor A', 'A')
        ]
        c.executemany("INSERT INTO work_orders VALUES (?,?,?,?,?,?,?,?)", sample_orders)
    
    c.execute("SELECT COUNT(*) FROM deals")
    if c.fetchone()[0] == 0:
        sample_deals = [
            (1, 'Warner Bros 2024 Package', 'Warner Bros', 1500000, '2024-02-01', 'NA', 'Active'),
            (2, 'BBC Studios Renewal', 'BBC', 850000, '2024-02-15', 'EMEA', 'Completed'),
            (3, 'Sony Pictures Deal', 'Sony', 2200000, '2024-03-01', 'APAC', 'Pending'),
            (4, 'Paramount Animation', 'Paramount', 1200000, '2024-02-20', 'NA', 'Active'),
            (5, 'Studio Ghibli Classics', 'Ghibli', 650000, '2024-01-30', 'APAC', 'Completed')
        ]
        c.executemany("INSERT INTO deals VALUES (?,?,?,?,?,?,?)", sample_deals)
    
    conn.commit()
    conn.close()
    print("✅ Database initialized with sample data.")

# Initialize the database when the app starts
init_database()

# In-memory chat history (for demo only; lost on restart)
chat_history = []

# ------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------
@app.route('/')
def index():
    """Render the main chat interface."""
    return render_template('index.html', chat_history=chat_history)

@app.route('/ask', methods=['POST'])
def ask():
    """Process a natural language query and return results."""
    question = request.json.get('question', '')
    if not question:
        return jsonify({'error': 'No question provided'}), 400
    
    # Parse query using rule-based engine
    sql = parse_query(question)
    if sql is None:
        return jsonify({
            'answer': "I couldn't understand that query. Please try rephrasing.",
            'sql': None,
            'data': None
        })
    
    # Execute SQL
    df, error = execute_sql(sql)
    if error:
        return jsonify({
            'answer': f"Query execution failed: {error}",
            'sql': sql,
            'data': None
        })
    
    # Format response
    if df.empty:
        answer = "No results found."
        data = None
    elif len(df) == 1 and df.shape[1] == 1:
        # Single value answer (e.g., count)
        answer = f"**Result:** {df.iloc[0,0]}"
        data = None
    else:
        answer = f"Found **{len(df)}** results:"
        # Convert DataFrame to HTML table with Bootstrap classes
        data = df.to_html(classes='table table-striped', index=False)
    
    # Store in chat history
    chat_entry = {
        'question': question,
        'answer': answer,
        'sql': sql,
        'data': data
    }
    chat_history.append(chat_entry)
    
    return jsonify(chat_entry)

@app.route('/dashboards/<name>')
def dashboard(name):
    """Return chart data for the sidebar dashboards."""
    if name == 'executive':
        sql = "SELECT status, COUNT(*) as count FROM content_planning GROUP BY status"
        df, error = execute_sql(sql)
        if error:
            return jsonify({'error': f'Database error: {error}'}), 500
        labels = df['status'].tolist()
        values = df['count'].tolist()
        return jsonify({'type': 'pie', 'labels': labels, 'values': values, 'title': 'Content Status'})
    
    elif name == 'workorders':
        sql = "SELECT status, COUNT(*) as count FROM work_orders GROUP BY status"
        df, error = execute_sql(sql)
        if error:
            return jsonify({'error': f'Database error: {error}'}), 500
        labels = df['status'].tolist()
        values = df['count'].tolist()
        return jsonify({'type': 'bar', 'labels': labels, 'values': values, 'title': 'Work Orders by Status'})
    
    elif name == 'deals':
        sql = "SELECT vendor, SUM(deal_value) as total FROM deals GROUP BY vendor"
        df, error = execute_sql(sql)
        if error:
            return jsonify({'error': f'Database error: {error}'}), 500
        labels = df['vendor'].tolist()
        values = df['total'].tolist()
        return jsonify({'type': 'bar', 'labels': labels, 'values': values, 'title': 'Deals by Vendor'})
    
    else:
        return jsonify({'error': 'Dashboard not found'}), 404

@app.route('/healthz')
def health_check():
    """Health check endpoint for uptime monitoring (Render, UptimeRobot, etc.)."""
    return jsonify({
        'status': 'ok',
        'timestamp': datetime.now().isoformat(),
        'service': 'Foundry Vantage'
    })

# ------------------------------------------------------------------
# Run the app (only for local development; on Render, use gunicorn)
# ------------------------------------------------------------------
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))

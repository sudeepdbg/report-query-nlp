from flask import Flask, render_template, request, jsonify
import json
import pandas as pd
from query_parser import parse_query, execute_sql

app = Flask(__name__)

# For simplicity, we'll store chat history in memory (per session)
# In a real app you'd use sessions or a database
chat_history = []

@app.route('/')
def index():
    return render_template('index.html', chat_history=chat_history)

@app.route('/ask', methods=['POST'])
def ask():
    question = request.json.get('question', '')
    if not question:
        return jsonify({'error': 'No question provided'}), 400
    
    # Parse query
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
        # Single value answer
        answer = f"**Result:** {df.iloc[0,0]}"
        data = None
    else:
        answer = f"Found **{len(df)}** results:"
        # Convert DataFrame to HTML table for display
        data = df.to_html(classes='table table-striped', index=False)
    
    # Store in chat history (optional)
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
    # Serve dummy dashboard data for the sidebar
    if name == 'executive':
        df = pd.read_sql_query("SELECT status, COUNT(*) as count FROM content_planning GROUP BY status", 'vantage.db')
        labels = df['status'].tolist()
        values = df['count'].tolist()
        return jsonify({'type': 'pie', 'labels': labels, 'values': values, 'title': 'Content Status'})
    elif name == 'workorders':
        df = pd.read_sql_query("SELECT status, COUNT(*) as count FROM work_orders GROUP BY status", 'vantage.db')
        labels = df['status'].tolist()
        values = df['count'].tolist()
        return jsonify({'type': 'bar', 'labels': labels, 'values': values, 'title': 'Work Orders by Status'})
    elif name == 'deals':
        df = pd.read_sql_query("SELECT vendor, SUM(deal_value) as total FROM deals GROUP BY vendor", 'vantage.db')
        labels = df['vendor'].tolist()
        values = df['total'].tolist()
        return jsonify({'type': 'bar', 'labels': labels, 'values': values, 'title': 'Deals by Vendor'})
    else:
        return jsonify({'error': 'Dashboard not found'}), 404

if __name__ == '__main__':
    app.run(debug=True)

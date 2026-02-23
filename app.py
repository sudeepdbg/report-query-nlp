import os
import sqlite3
import pandas as pd
import random
import logging
from flask import Flask, render_template, request, jsonify, session
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key")

# ------------------------------------------------------------------
# Global in‑memory database connection
# ------------------------------------------------------------------
def get_db_connection():
    conn = sqlite3.connect(':memory:', check_same_thread=False)
    conn.row_factory = sqlite3.Row  # optional
    return conn

def init_database():
    """Create tables and insert enriched sample data into in‑memory DB."""
    conn = get_db_connection()
    c = conn.cursor()
    
    # Content Planning table
    c.execute('''
        CREATE TABLE content_planning (
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
        CREATE TABLE work_orders (
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
        CREATE TABLE deals (
            id INTEGER PRIMARY KEY,
            deal_name TEXT,
            vendor TEXT,
            deal_value REAL,
            deal_date TEXT,
            region TEXT,
            status TEXT
        )
    ''')
    
    # --- Enriched content planning data ---
    content_titles = [
        ("MAX US", "House of the Dragon S2", "Fulfilled", "NA"),
        ("MAX US", "The Penguin", "Not Ready", "NA"),
        ("MAX US", "The Last of Us S2", "Scheduled", "NA"),
        ("MAX US", "Dune: Prophecy", "Delivered", "NA"),
        ("MAX Europe", "The White Lotus S3", "Scheduled", "EMEA"),
        ("MAX Europe", "Industry S3", "Fulfilled", "EMEA"),
        ("MAX Europe", "Euphoria S3", "Not Ready", "EMEA"),
        ("MAX Australia", "The Last Kingdom", "Delivered", "APAC"),
        ("MAX Australia", "Dune: Prophecy", "Scheduled", "APAC"),
        ("MAX Australia", "The Gilded Age", "Fulfilled", "APAC"),
        ("MAX LatAm", "El Encargado", "Delivered", "LATAM"),
        ("MAX LatAm", "Iosi, el espía arrepentido", "Scheduled", "LATAM"),
        ("MAX Asia", "Oppenheimer", "Fulfilled", "APAC"),
        ("MAX Asia", "Godzilla Minus One", "Not Ready", "APAC"),
    ]
    for i, (net, title, status, reg) in enumerate(content_titles, start=1):
        c.execute('''
            INSERT INTO content_planning (id, network, content_title, status, planned_date, region)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (i, net, title, status, f"2024-{i%12+1:02d}-{i%28+1:02d}", reg))
    
    # --- Enriched work orders ---
    vendors = ["Vendor A", "Vendor B", "Vendor C", "Vendor D", "Vendor E"]
    statuses = ["Delayed", "In Progress", "Completed", "Pending Review"]
    regions = ["NA", "APAC", "EMEA", "LATAM"]
    priorities = ["A", "B", "C"]
    
    for i in range(1, 21):
        wo = f"WO-2024-{i:03d}"
        offering = f"MAX {random.choice(regions)} - {random.choice(['Migration', 'Encoding', 'Subtitle', 'QC Review', 'Audio'])}"
        status = random.choice(statuses)
        due = f"2024-{i%12+1:02d}-{i%28+1:02d}"
        region = random.choice(regions)
        vendor = random.choice(vendors)
        priority = random.choice(priorities)
        c.execute('''
            INSERT INTO work_orders (id, work_order, offering, status, due_date, region, vendor, priority)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (i, wo, offering, status, due, region, vendor, priority))
    
    # --- Enriched deals ---
    deal_names = [
        "Warner Bros 2024 Package", "BBC Studios Renewal", "Sony Pictures Deal",
        "Paramount Animation", "Studio Ghibli Classics", "A24 Film Slate",
        "Discovery+ Originals", "HBO Max Acquisitions", "CNN International",
        "Cartoon Network Library", "Adult Swim Series", "TNT Sports Rights",
        "TBS Comedy Specials", "Rooster Teeth Collection", "DC Universe Animated"
    ]
    vendors_deals = ["Warner Bros", "BBC", "Sony", "Paramount", "Ghibli", "A24", "Discovery", "HBO", "CNN", "Cartoon Network", "Adult Swim", "TNT Sports", "TBS", "Rooster Teeth", "DC"]
    statuses_deal = ["Active", "Completed", "Pending"]
    
    for i, (name, vendor) in enumerate(zip(deal_names, vendors_deals), start=1):
        value = round(random.uniform(500000, 5000000), 2)
        date = f"2024-{i%12+1:02d}-{i%28+1:02d}"
        region = random.choice(regions)
        status = random.choice(statuses_deal)
        c.execute('''
            INSERT INTO deals (id, deal_name, vendor, deal_value, deal_date, region, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (i, name, vendor, value, date, region, status))
    
    conn.commit()
    logger.info("✅ In‑memory database initialized with sample data.")
    return conn

# Initialize the in‑memory database and store the connection globally
DB_CONN = init_database()

def execute_sql(sql):
    """Execute SQL on the global in‑memory connection."""
    try:
        df = pd.read_sql_query(sql, DB_CONN)
        return df, None
    except Exception as e:
        return None, str(e)

# ------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------
@app.route('/')
def index():
    region = request.args.get('region', 'NA')
    team = request.args.get('team', 'leadership')
    return render_template('index.html', chat_history=[], current_region=region, current_team=team)

@app.route('/ask', methods=['POST'])
def ask():
    data = request.json
    question = data.get('question', '')
    region = data.get('region', 'NA')
    team = data.get('team', 'leadership')
    dashboard = data.get('dashboard', 'executive')
    
    if not question:
        return jsonify({'error': 'No question provided'}), 400
    
    # Import your query parser (ensure it's still in your project)
    from query_parser import parse_query  # adjust import as needed
    
    # Parse query using rule-based engine (you can later replace with LLM)
    sql, error = parse_query(question, region, team, dashboard)
    if error:
        return jsonify({
            'answer': error,
            'sql': None,
            'data': None,
            'chart': None
        })
    
    # Execute SQL using the global in‑memory connection
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
        chart_json = auto_chart(df)  # your auto_chart function
    
    # Store in session context (optional)
    session['last_context'] = {
        'question': question,
        'sql': sql,
        'result': df.to_dict(orient='records') if not df.empty else []
    }
    
    return jsonify({
        'question': question,
        'answer': answer,
        'sql': sql,
        'data': data_html,
        'chart': chart_json
    })

@app.route('/dashboards/<name>')
def dashboard(name):
    try:
        if name == 'executive':
            sql = "SELECT status, COUNT(*) as count FROM content_planning GROUP BY status"
        elif name == 'workorders':
            sql = "SELECT status, COUNT(*) as count FROM work_orders GROUP BY status"
        elif name == 'deals':
            sql = "SELECT vendor, SUM(deal_value) as total FROM deals GROUP BY vendor"
        else:
            return jsonify({'error': 'Dashboard not found'}), 404
        
        logger.info(f"Executing dashboard query: {sql}")
        df, error = execute_sql(sql)
        if error:
            logger.error(f"Dashboard SQL error: {error}")
            return jsonify({'error': f'Database error: {error}'}), 500
        
        if name == 'executive':
            labels = df['status'].tolist()
            values = df['count'].tolist()
            return jsonify({'type': 'pie', 'labels': labels, 'values': values, 'title': 'Content Status'})
        elif name == 'workorders':
            labels = df['status'].tolist()
            values = df['count'].tolist()
            return jsonify({'type': 'bar', 'labels': labels, 'values': values, 'title': 'Work Orders by Status'})
        elif name == 'deals':
            labels = df['vendor'].tolist()
            values = df['total'].tolist()
            return jsonify({'type': 'bar', 'labels': labels, 'values': values, 'title': 'Deals by Vendor'})
    except Exception as e:
        logger.exception("Unhandled exception in dashboard route")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/healthz')
def health_check():
    return jsonify({'status': 'ok', 'timestamp': datetime.now().isoformat()})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))

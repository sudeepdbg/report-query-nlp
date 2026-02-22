import re
import sqlite3

def parse_query(question):
    """Convert natural language to SQL using simple rules."""
    q = question.lower().strip()
    
    # Count queries
    if re.search(r'how many|count', q):
        if re.search(r'delayed|at risk', q):
            return "SELECT COUNT(*) FROM work_orders WHERE status = 'Delayed'"
        if re.search(r'max australia|max au', q):
            return "SELECT COUNT(*) FROM content_planning WHERE network = 'MAX Australia'"
        if re.search(r'max us', q):
            return "SELECT COUNT(*) FROM content_planning WHERE network = 'MAX US'"
        if re.search(r'active deals', q):
            return "SELECT COUNT(*) FROM deals WHERE status = 'Active'"
        if re.search(r'pending approval', q):
            return "SELECT COUNT(*) FROM deals WHERE status = 'Pending'"
        if re.search(r'in progress', q):
            return "SELECT COUNT(*) FROM work_orders WHERE status = 'In Progress'"
    
    # List queries
    if re.search(r'show|list|get|what', q):
        if re.search(r'delayed|at risk', q):
            return "SELECT * FROM work_orders WHERE status = 'Delayed'"
        if re.search(r'max australia', q):
            return "SELECT * FROM content_planning WHERE network = 'MAX Australia'"
        if re.search(r'max us', q):
            return "SELECT * FROM content_planning WHERE network = 'MAX US'"
        if re.search(r'vendor a', q) or re.search(r'top vendors', q):
            return "SELECT vendor, COUNT(*) as count FROM work_orders GROUP BY vendor ORDER BY count DESC"
        if re.search(r'active deals', q):
            return "SELECT * FROM deals WHERE status = 'Active'"
        if re.search(r'pending approval', q):
            return "SELECT * FROM deals WHERE status = 'Pending'"
        if re.search(r'in progress', q):
            return "SELECT * FROM work_orders WHERE status = 'In Progress'"
        if re.search(r'not ready', q):
            return "SELECT * FROM content_planning WHERE status = 'Not Ready'"
    
    # Default fallback
    return None

def execute_sql(sql):
    conn = sqlite3.connect('vantage.db')
    try:
        import pandas as pd
        df = pd.read_sql_query(sql, conn)
        return df, None
    except Exception as e:
        return None, str(e)
    finally:
        conn.close()

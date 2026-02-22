import re
import sqlite3
import pandas as pd

def parse_query(question, region='NA', team='leadership', dashboard='executive'):
    """
    Convert natural language to SQL using rules, with region and team filters.
    Returns (sql, error_message) – error_message is None if successful.
    """
    q = question.lower().strip()
    
    # Helper to add region and team filters
    def add_filters(sql, table):
        filters = []
        if region and region != 'GLOBAL':
            filters.append(f"{table}.region = '{region}'")
        # Team can influence filter, e.g., for deals team, maybe add vendor filter?
        # For simplicity, we'll just use region, but you can extend.
        # If team is 'deals', maybe restrict to deals table? We'll handle at selection level.
        if filters:
            if 'WHERE' in sql:
                sql += " AND " + " AND ".join(filters)
            else:
                sql += " WHERE " + " AND ".join(filters)
        return sql
    
    # Determine primary table based on team and dashboard
    # Team can hint at which table is most relevant
    team_table_map = {
        'leadership': None,  # all tables
        'product': 'work_orders',
        'content planning': 'content_planning',
        'deals': 'deals'
    }
    primary_table = team_table_map.get(team, None)
    
    # Count queries
    if re.search(r'how many|count', q):
        # Try to detect what to count
        if re.search(r'delayed|at risk', q):
            table = 'work_orders'
            condition = "status = 'Delayed'"
        elif re.search(r'max australia|max au', q):
            table = 'content_planning'
            condition = "network = 'MAX Australia'"
        elif re.search(r'max us', q):
            table = 'content_planning'
            condition = "network = 'MAX US'"
        elif re.search(r'active deals', q):
            table = 'deals'
            condition = "status = 'Active'"
        elif re.search(r'pending approval', q):
            table = 'deals'
            condition = "status = 'Pending'"
        elif re.search(r'in progress', q):
            table = 'work_orders'
            condition = "status = 'In Progress'"
        elif re.search(r'not ready', q):
            table = 'content_planning'
            condition = "status = 'Not Ready'"
        else:
            # If team hints a table, use that with a generic condition
            if primary_table:
                table = primary_table
                condition = "1=1"  # no specific condition
            else:
                return None, "I couldn't understand what to count."
        
        sql = f"SELECT COUNT(*) FROM {table} WHERE {condition}"
        sql = add_filters(sql, table)
        return sql, None
    
    # List queries
    if re.search(r'show|list|get|what', q):
        # Top vendors special case
        if re.search(r'vendor a', q) or re.search(r'top vendors', q):
            table = 'work_orders'
            sql = f"SELECT vendor, COUNT(*) as count FROM {table}"
            sql = add_filters(sql, table)
            sql += " GROUP BY vendor ORDER BY count DESC"
            return sql, None
        
        # Detect specific requests
        if re.search(r'delayed|at risk', q):
            table = 'work_orders'
            condition = "status = 'Delayed'"
        elif re.search(r'max australia', q):
            table = 'content_planning'
            condition = "network = 'MAX Australia'"
        elif re.search(r'max us', q):
            table = 'content_planning'
            condition = "network = 'MAX US'"
        elif re.search(r'active deals', q):
            table = 'deals'
            condition = "status = 'Active'"
        elif re.search(r'pending approval', q):
            table = 'deals'
            condition = "status = 'Pending'"
        elif re.search(r'in progress', q):
            table = 'work_orders'
            condition = "status = 'In Progress'"
        elif re.search(r'not ready', q):
            table = 'content_planning'
            condition = "status = 'Not Ready'"
        else:
            # If no specific pattern, use team hint
            if primary_table:
                table = primary_table
                condition = "1=1"
            else:
                return None, "I couldn't understand what to list. Try asking about delayed work orders, MAX Australia content, or active deals."
        
        sql = f"SELECT * FROM {table} WHERE {condition}"
        sql = add_filters(sql, table)
        return sql, None
    
    # If no pattern matched, return a helpful message with suggestions
    suggestions = [
        "Show me all delayed work orders",
        "How many content items for MAX Australia?",
        "List active deals",
        "Show top vendors by work orders"
    ]
    suggestion_text = " You could try: " + ", ".join(f'"{s}"' for s in suggestions)
    return None, f"I couldn't understand that query. Please try rephrasing.{suggestion_text}"

def execute_sql(sql):
    """Execute SQL and return (DataFrame, error)."""
    conn = None
    try:
        conn = sqlite3.connect('vantage.db')
        df = pd.read_sql_query(sql, conn)
        return df, None
    except Exception as e:
        return None, str(e)
    finally:
        if conn:
            conn.close()

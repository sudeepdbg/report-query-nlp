import re
import sqlite3

def parse_query(question, region='NA', team='leadership', dashboard='executive'):
    """
    Convert natural language to SQL using rules, and incorporate filters.
    Returns (sql, error_message) – error_message is None if successful.
    """
    q = question.lower().strip()
    
    # Determine target table based on dashboard (or detect from question)
    # If dashboard is specified, restrict to that table's domain.
    if dashboard == 'executive':
        primary_table = 'content_planning'
    elif dashboard == 'workorders':
        primary_table = 'work_orders'
    elif dashboard == 'deals':
        primary_table = 'deals'
    else:
        primary_table = None  # let parser decide
    
    # Base filters from region and team (team may map to a role/region)
    # For simplicity, we'll just use region filter where applicable.
    region_filter = f"region = '{region}'" if region else None
    
    # --- Count queries ---
    if re.search(r'how many|count', q):
        # Determine what to count
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
        else:
            return None, "I couldn't understand what to count."
        
        # If a primary table is set (from dashboard) and it conflicts, maybe restrict?
        # For now, we ignore dashboard for count queries.
        sql = f"SELECT COUNT(*) FROM {table}"
        where_clauses = [condition]
        if region_filter and table in ['content_planning', 'work_orders', 'deals']:
            where_clauses.append(region_filter)
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        return sql, None
    
    # --- List queries ---
    if re.search(r'show|list|get|what', q):
        # Determine what to list
        if re.search(r'delayed|at risk', q):
            table = 'work_orders'
            condition = "status = 'Delayed'"
        elif re.search(r'max australia', q):
            table = 'content_planning'
            condition = "network = 'MAX Australia'"
        elif re.search(r'max us', q):
            table = 'content_planning'
            condition = "network = 'MAX US'"
        elif re.search(r'vendor a', q) or re.search(r'top vendors', q):
            table = 'work_orders'
            # This is an aggregation, not a simple list
            sql = "SELECT vendor, COUNT(*) as count FROM work_orders GROUP BY vendor ORDER BY count DESC"
            # Add region filter if applicable
            if region_filter:
                sql = f"SELECT vendor, COUNT(*) as count FROM work_orders WHERE {region_filter} GROUP BY vendor ORDER BY count DESC"
            return sql, None
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
            return None, "I couldn't understand what to list."
        
        # For list queries, if a primary table is set from dashboard, we might want to restrict
        # to that table. But the detected table might differ. We'll trust the detected table.
        sql = f"SELECT * FROM {table}"
        where_clauses = [condition]
        if region_filter and table in ['content_planning', 'work_orders', 'deals']:
            where_clauses.append(region_filter)
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        return sql, None
    
    # --- No match ---
    return None, "I couldn't understand that query. Please try rephrasing."

def execute_sql(sql):
    """Execute SQL and return (DataFrame, error)."""
    conn = sqlite3.connect('vantage.db')
    try:
        df = pd.read_sql_query(sql, conn)
        return df, None
    except Exception as e:
        return None, str(e)
    finally:
        conn.close()

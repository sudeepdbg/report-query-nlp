import re
import sqlite3

def parse_query(question, region='NA', team='leadership', dashboard='executive'):
    """
    Convert natural language to SQL using rules.
    Returns (sql, error_message) – error_message is None if successful.
    """
    q = question.lower().strip()
    
    # Helper to add region filter if applicable
    def add_region_filter(sql, table):
        if region and region != 'GLOBAL':
            if 'WHERE' in sql:
                sql += f" AND {table}.region = '{region}'"
            else:
                sql += f" WHERE {table}.region = '{region}'"
        return sql
    
    # Team hint (can influence table selection – simplified)
    team_table_map = {
        'leadership': None,
        'product': 'work_orders',
        'content planning': 'content_planning',
        'deals': 'deals'
    }
    primary_table = team_table_map.get(team, None)
    
    # --- Count queries ---
    if re.search(r'how many|count', q):
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
            # Use team hint if available
            if primary_table:
                table = primary_table
                condition = "1=1"
            else:
                return None, "I couldn't understand what to count."
        
        sql = f"SELECT COUNT(*) FROM {table} WHERE {condition}"
        sql = add_region_filter(sql, table)
        return sql, None
    
    # --- List queries ---
    if re.search(r'show|list|get|what', q):
        # Special case: top vendors
        if re.search(r'vendor a', q) or re.search(r'top vendors', q):
            sql = "SELECT vendor, COUNT(*) as count FROM work_orders"
            if region and region != 'GLOBAL':
                sql += f" WHERE region = '{region}'"
            sql += " GROUP BY vendor ORDER BY count DESC"
            return sql, None
        
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
            if primary_table:
                table = primary_table
                condition = "1=1"
            else:
                return None, "I couldn't understand what to list. Try asking about delayed work orders, MAX Australia content, or active deals."
        
        sql = f"SELECT * FROM {table} WHERE {condition}"
        sql = add_region_filter(sql, table)
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

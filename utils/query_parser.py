import re

def parse_query(question, region):
    """
    Translates Natural Language to SQL and identifies the best visualization.
    Matches casing for SQLite ('APAC', 'EMEA', etc.)
    """
    q = question.lower()
    reg = region.upper()
    
    # 1. Logic for DEALS (Target: Bar Chart)
    if "deal" in q:
        table = "deals"
        if "active" in q:
            sql = f"SELECT * FROM {table} WHERE status = 'Active' AND region = '{reg}';"
        else:
            sql = f"SELECT * FROM {table} WHERE region = '{reg}';"
        return sql, None, "bar"

    # 2. Logic for WORK ORDERS (Target: Pie Chart)
    if "work order" in q or "delayed" in q:
        table = "work_orders"
        if "delayed" in q:
            sql = f"SELECT * FROM {table} WHERE status = 'Delayed' AND region = '{reg}';"
        elif "progress" in q:
            sql = f"SELECT * FROM {table} WHERE status = 'In Progress' AND region = '{reg}';"
        else:
            sql = f"SELECT * FROM {table} WHERE region = '{reg}';"
        return sql, None, "pie"

    # 3. Logic for CONTENT (Target: Pie Chart)
    if "content" in q:
        table = "content_planning"
        if "ready" in q:
            sql = f"SELECT * FROM {table} WHERE status = 'Not Ready' AND region = '{reg}';"
        else:
            sql = f"SELECT * FROM {table} WHERE region = '{reg}';"
        return sql, None, "pie"

    return None, "I couldn't identify the right table for that question. Try asking about Content, Work Orders, or Deals.", None

import re

def parse_query(question, region):
    """
    Translates English to SQL. 
    Crucial: Forces the SQL to match the SQLite schema casing.
    """
    q = question.lower()
    
    # 1. Logic for DEALS
    if "deal" in q:
        table = "deals"
        if "active" in q:
            return f"SELECT * FROM {table} WHERE status = 'Active' AND region = '{region.upper()}';", None
        return f"SELECT * FROM {table} WHERE region = '{region.upper()}';", None

    # 2. Logic for WORK ORDERS
    if "work order" in q or "delayed" in q:
        table = "work_orders"
        if "delayed" in q:
            return f"SELECT * FROM {table} WHERE status = 'Delayed' AND region = '{region.upper()}';", None
        return f"SELECT * FROM {table} WHERE region = '{region.upper()}';", None

    # 3. Logic for CONTENT
    if "content" in q:
        table = "content_planning"
        return f"SELECT * FROM {table} WHERE region = '{region.upper()}';", None

    return None, "I'm sorry, I couldn't translate that request. Try using one of the suggestions!"

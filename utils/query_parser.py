import re

def parse_query(question, region):
    """
    Translates natural language to targeted SQL for the Media Supply Chain.
    """
    q = question.lower()
    reg = region.upper()
    
    # List of known networks for specific filtering
    networks = ["MAX US", "MAX Europe", "MAX Australia", "MAX LatAm", "MAX Asia", "MAX Africa", "MAX India", "MAX UK"]

    # 1. DEALS INTENT
    if any(word in q for word in ["deal", "rights", "value"]):
        table = "deals"
        sql = f"SELECT * FROM {table} WHERE region = '{reg}'"
        if "active" in q: sql += " AND status = 'Active'"
        return sql + ";", None, "bar"

    # 2. WORK ORDERS INTENT
    if any(word in q for word in ["work order", "wo-", "task", "asset"]):
        table = "work_orders"
        sql = f"SELECT * FROM {table} WHERE region = '{reg}'"
        if "delayed" in q: sql += " AND status = 'Delayed'"
        if "priority a" in q: sql += " AND priority LIKE 'A%'"
        return sql + ";", None, "pie"

    # 3. CONTENT PLANNING INTENT (Executive View)
    if any(word in q for word in ["content", "show", "status", "max", "scheduled"]):
        table = "content_planning"
        # Start with strict regional filter to prevent data leakage
        sql = f"SELECT * FROM {table} WHERE region = '{reg}'"
        
        # Check if the user mentioned a specific Network (e.g., "MAX Australia")
        for net in networks:
            if net.lower() in q:
                sql += f" AND network = '{net}'"
        
        # Check for status keywords
        if "ready" in q or "pending" in q: sql += " AND status = 'Not Ready'"
        if "delivered" in q: sql += " AND status = 'Delivered'"
        
        # Title specific search
        match = re.search(r"(?:about|for|show)\s+(.*)", q)
        if match:
            potential_title = match.group(1).strip()
            # Exclude regions or common words from being treated as titles
            if potential_title.upper() not in ["APAC", "EMEA", "NA", "LATAM", "STATUS"]:
                sql += f" AND content_title LIKE '%{potential_title.title()}%'"

        return sql + ";", None, "pie"

    # Fallback for unhandled queries
    return None, "I'm sorry, I couldn't translate that request. Try asking about Content, Work Orders, or Deal values.", None

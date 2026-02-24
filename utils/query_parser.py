import re

def parse_query(question, region):
    q = question.lower()
    reg = region.upper()
    
    # 1. DEALS (Numerical/Comparison -> BAR CHART)
    if any(word in q for word in ["deal", "vendor", "value", "rights"]):
        sql = f"SELECT * FROM deals WHERE region = '{reg}'"
        if "active" in q: sql += " AND status = 'Active'"
        return sql + ";", None, "bar"

    # 2. WORK ORDERS (Status Distribution -> PIE CHART)
    if any(word in q for word in ["order", "task", "asset", "subtitle", "dub"]):
        sql = f"SELECT * FROM work_orders WHERE region = '{reg}'"
        if "delayed" in q: sql += " AND status = 'Delayed'"
        return sql + ";", None, "pie"

    # 3. CONTENT PLANNING (Status Distribution -> PIE CHART)
    if any(word in q for word in ["content", "show", "max", "status", "ready"]):
        sql = f"SELECT * FROM content_planning WHERE region = '{reg}'"
        
        # Search for specific networks
        networks = ["MAX US", "MAX Europe", "MAX Australia", "MAX LatAm", "MAX Asia", "MAX India", "MAX UK"]
        for net in networks:
            if net.lower() in q:
                sql += f" AND network = '{net}'"
        
        return sql + ";", None, "pie"

    return None, "I'm not sure what you're looking for. Try asking about Deals or Content Status.", None

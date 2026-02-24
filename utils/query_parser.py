import re

def parse_query(question, region):
    q = question.lower()
    reg = region.upper()
    
    # 1. DEALS
    if "deal" in q or "rights" in q:
        sql = f"SELECT * FROM deals WHERE region = '{reg}'"
        return sql + ";", None, "bar"

    # 2. WORK ORDERS
    if "order" in q or "task" in q:
        sql = f"SELECT * FROM work_orders WHERE region = '{reg}'"
        if "delayed" in q: sql += " AND status = 'Delayed'"
        return sql + ";", None, "pie"

    # 3. CONTENT (Executive)
    if any(word in q for word in ["content", "show", "max", "status"]):
        sql = f"SELECT * FROM content_planning WHERE region = '{reg}'"
        
        # Specific Network variety
        networks = ["MAX US", "MAX Europe", "MAX Australia", "MAX LatAm", "MAX Asia", "MAX India"]
        for net in networks:
            if net.lower() in q:
                sql += f" AND network = '{net}'"
        
        # Simple Title search
        match = re.search(r"(?:for|about)\s+(.*)", q)
        if match:
            title = match.group(1).strip().title()
            if title not in ["Apac", "Emea", "Latam", "Na"]:
                sql += f" AND content_title LIKE '%{title}%'"
                
        return sql + ";", None, "pie"

    return None, "Please specify if you want to see Content, Work Orders, or Deals.", None

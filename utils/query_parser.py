import re

def parse_query(question, region):
    """
    Foundry Vantage Intelligence Engine.
    Hardened for exact keyword matching to prevent empty results.
    """
    q = question.lower().strip()
    active_reg = region.upper()
    
    # 1. INTENT: VENDORS & SPEND
    vendor_keywords = ["vendor", "vendors", "spend", "cost", "rank", "who", "top", "studio", "studios"]
    if any(word in q for word in vendor_keywords):
        # Aggregation Logic
        if any(word in q for word in ["top", "total", "sum", "who", "spend", "biggest", "highest", "performance"]):
            sql = f"""
                SELECT vendor_name, SUM(deal_value) as total_value 
                FROM deals 
                WHERE UPPER(region) = '{active_reg}' 
                GROUP BY vendor_name 
                ORDER BY total_value DESC
            """
            return sql.strip() + ";", None, "bar"
        
        return f"SELECT vendor_name, deal_name, deal_value, status FROM deals WHERE UPPER(region) = '{active_reg}';", None, "bar"

    # 2. INTENT: RIGHTS & DEALS (SVOD focus)
    rights_keywords = ["rights", "svod", "avod", "tvod", "exclusive", "deal", "deals", "breakdown", "scope"]
    if any(word in q for word in rights_keywords):
        if "svod" in q:
            return f"SELECT * FROM deals WHERE UPPER(region) = '{active_reg}' AND UPPER(rights_scope) LIKE '%SVOD%';", None, "bar"
        
        if any(word in q for word in ["breakdown", "scope", "distribution", "count"]):
            sql = f"""
                SELECT rights_scope, COUNT(*) as count 
                FROM deals 
                WHERE UPPER(region) = '{active_reg}' 
                GROUP BY rights_scope
            """
            return sql.strip() + ";", None, "bar"
            
        return f"SELECT * FROM deals WHERE UPPER(region) = '{active_reg}' ORDER BY deal_value DESC;", None, "bar"

    # 3. INTENT: OPERATIONS & WORK ORDERS
    ops_keywords = ["order", "orders", "task", "tasks", "duplo", "delay", "delayed", "queue", "work"]
    if any(word in q for word in ops_keywords):
        if "delayed" in q or "delay" in q:
            return f"SELECT * FROM work_orders WHERE UPPER(region) = '{active_reg}' AND UPPER(status) = 'DELAYED';", None, "pie"
        return f"SELECT status, COUNT(*) as count FROM work_orders WHERE UPPER(region) = '{active_reg}' GROUP BY status;", None, "pie"

    # 4. INTENT: CONTENT & INVENTORY (Readiness)
    content_keywords = ["content", "ready", "unacquired", "inventory", "max", "title", "titles", "planning", "readiness"]
    if any(word in q for word in content_keywords):
        if any(word in q for word in ["ready", "status", "readiness", "unacquired"]):
            return f"SELECT status, COUNT(*) as count FROM content_planning WHERE UPPER(region) = '{active_reg}' GROUP BY status;", None, "pie"
            
        return f"SELECT * FROM content_planning WHERE UPPER(region) = '{active_reg}' LIMIT 50;", None, "pie"

    return None, None, None

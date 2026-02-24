import re

def parse_query(question, region):
    """
    Hardened Parser v3. 
    Added plural support and more aggressive keyword matching to prevent 'No records found'.
    """
    q = question.lower().strip()
    # Normalize region to match DB storage (e.g., 'apac' becomes 'APAC')
    active_reg = region.upper()
    
    # 1. INTENT: VENDORS & SPEND
    # Catching 'vendors' (plural), 'spend', 'top', etc.
    vendor_keywords = ["vendor", "vendors", "spend", "cost", "rank", "who", "top", "studio", "studios"]
    if any(word in q for word in vendor_keywords):
        
        # Aggregation Logic: For "Top", "Total", "Who is the biggest", etc.
        if any(word in q for word in ["top", "total", "sum", "who", "spend", "biggest", "highest"]):
            sql = f"""
                SELECT vendor_name, SUM(deal_value) as total_value 
                FROM deals 
                WHERE UPPER(region) = '{active_reg}' 
                GROUP BY vendor_name 
                ORDER BY total_value DESC
            """
            return sql.strip() + ";", None, "bar"
        
        # List View for Vendors: Ensure it pulls vendor_name specifically
        return f"SELECT vendor_name, deal_name, deal_value, status FROM deals WHERE UPPER(region) = '{active_reg}';", None, "bar"

    # 2. INTENT: RIGHTS & DEALS
    # Expanded keywords for SVOD, AVOD, TVOD, etc.
    rights_keywords = ["rights", "svod", "avod", "tvod", "exclusive", "deal", "deals", "breakdown", "scope", "territory"]
    if any(word in q for word in rights_keywords):
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
    # Matches 'orders' plural and 'tasks' plural
    ops_keywords = ["order", "orders", "task", "tasks", "duplo", "delay", "delayed", "queue", "work"]
    if any(word in q for word in ops_keywords):
        return f"SELECT * FROM work_orders WHERE UPPER(region) = '{active_reg}' ORDER BY due_date ASC;", None, "pie"

    # 4. INTENT: CONTENT & INVENTORY
    # Matches 'titles' and 'status'
    content_keywords = ["content", "ready", "unacquired", "inventory", "max", "title", "titles", "planning"]
    if any(word in q for word in content_keywords):
        if any(word in q for word in ["ready", "status", "readiness"]):
            return f"SELECT status, COUNT(*) as count FROM content_planning WHERE UPPER(region) = '{active_reg}' GROUP BY status;", None, "pie"
            
        return f"SELECT * FROM content_planning WHERE UPPER(region) = '{active_reg}' LIMIT 50;", None, "pie"

    # 5. ERROR FALLBACK
    # Returning None triggers the 'Suggestion Engine' in your app.py
    return None, None, None

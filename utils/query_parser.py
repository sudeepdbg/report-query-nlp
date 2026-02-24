import re

def parse_query(question, region):
    """
    Final Hardened Parser.
    Fixes the 'vendor_name' mismatch and adds fuzzy intent detection.
    """
    q = question.lower().strip()
    # Normalize region to match DB storage (e.g., 'apac' becomes 'APAC')
    active_reg = region.upper()
    
    # 1. INTENT: VENDORS & SPEND
    # Matches: 'vendor', 'spend', 'top', 'who', 'cost'
    if any(word in q for word in ["vendor", "spend", "cost", "rank", "who", "top"]):
        # Aggregation Logic: Ensuring column names match the generated DB exactly
        if any(word in q for word in ["top", "total", "sum", "who", "spend"]):
            sql = f"""
                SELECT vendor_name, SUM(deal_value) as total_value 
                FROM deals 
                WHERE UPPER(region) = '{active_reg}' 
                GROUP BY vendor_name 
                ORDER BY total_value DESC
            """
            return sql.strip() + ";", None, "bar"
        
        # List View for Vendors
        return f"SELECT vendor_name, deal_name, deal_value, status FROM deals WHERE UPPER(region) = '{active_reg}';", None, "bar"

    # 2. INTENT: RIGHTS & DEALS
    # Matches: 'rights', 'svod', 'exclusive', 'deal', 'breakdown'
    if any(word in q for word in ["rights", "svod", "exclusive", "deal", "breakdown", "scope"]):
        if "breakdown" in q or "scope" in q:
            sql = f"""
                SELECT rights_scope, COUNT(*) as count 
                FROM deals 
                WHERE UPPER(region) = '{active_reg}' 
                GROUP BY rights_scope
            """
            return sql.strip() + ";", None, "bar"
            
        return f"SELECT * FROM deals WHERE UPPER(region) = '{active_reg}' ORDER BY deal_value DESC;", None, "bar"

    # 3. INTENT: OPERATIONS & WORK ORDERS
    # Matches: 'duplo', 'order', 'task', 'delay', 'queue'
    if any(word in q for word in ["order", "task", "duplo", "delay", "queue", "work"]):
        return f"SELECT * FROM work_orders WHERE UPPER(region) = '{active_reg}' ORDER BY due_date ASC;", None, "pie"

    # 4. INTENT: CONTENT & INVENTORY
    # Matches: 'content', 'ready', 'unacquired', 'inventory'
    if any(word in q for word in ["content", "ready", "unacquired", "inventory", "max"]):
        if "ready" in q or "status" in q:
            return f"SELECT status, COUNT(*) as count FROM content_planning WHERE UPPER(region) = '{active_reg}' GROUP BY status;", None, "pie"
            
        return f"SELECT * FROM content_planning WHERE UPPER(region) = '{active_reg}' LIMIT 50;", None, "pie"

    # 5. ERROR FALLBACK & SUGGESTIONS
    # If no intent is matched, we provide a generic region query and trigger the 'No Records' UI logic
    return None, f"I couldn't identify the specific metric for '{question}'. Try asking about 'Top Vendors' or 'Content Readiness'.", None

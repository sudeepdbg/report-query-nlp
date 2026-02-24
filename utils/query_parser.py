import re

def parse_query(question, region):
    """
    Final Industry-Standard Parser for Foundry Vantage.
    Synchronized with 'vendor_name' and complex rights metadata.
    """
    q = question.lower()
    
    # 1. REGION SYNC
    # Forces uppercase to match Database storage (APAC, EMEA, etc.)
    active_reg = region.upper()
    for r in ["NA", "APAC", "EMEA", "LATAM"]:
        if r.lower() in q:
            active_reg = r
            break

    # 2. INTENT: FINANCIALS & VENDORS (The 'Deals' Table)
    # Checks for vendor_name specifically to avoid 'No records found'
    if any(word in q for word in ["vendor", "studio", "deal", "value", "cost", "spend", "rights", "budget"]):
        
        # Aggregated Vendor View (for 'Top Vendor' queries)
        if any(word in q for word in ["vendor", "who", "top", "rank"]):
            sql = f"""
                SELECT vendor_name, SUM(deal_value) as total_value 
                FROM deals 
                WHERE UPPER(region) = '{active_reg}' 
                GROUP BY vendor_name 
                ORDER BY total_value DESC
            """
            return sql.strip() + ";", None, "bar"
        
        # Rights & Complexity View
        if any(word in q for word in ["rights", "scope", "exclusive", "type"]):
            sql = f"""
                SELECT rights_scope, COUNT(*) as count 
                FROM deals 
                WHERE UPPER(region) = '{active_reg}' 
                GROUP BY rights_scope
            """
            return sql.strip() + ";", None, "bar"

        # Default: Full Deals list
        return f"SELECT * FROM deals WHERE UPPER(region) = '{active_reg}' ORDER BY deal_value DESC;", None, "bar"

    # 3. INTENT: SUPPLY CHAIN & OPERATIONS (The 'Work Orders' Table)
    if any(word in q for word in ["order", "work", "task", "duplo", "delay", "asset", "qa", "dub", "sub"]):
        
        # Localization specifically
        if any(word in q for word in ["dub", "sub", "language", "localization"]):
            sql = f"""
                SELECT language_target, COUNT(*) as count 
                FROM work_orders 
                WHERE UPPER(region) = '{active_reg}' 
                GROUP BY language_target
            """
            return sql.strip() + ";", None, "pie"
            
        return f"SELECT * FROM work_orders WHERE UPPER(region) = '{active_reg}' ORDER BY due_date ASC;", None, "pie"

    # 4. INTENT: CONTENT & SVOD (The 'Content Planning' Table)
    if any(word in q for word in ["content", "svod", "ready", "max", "inventory", "window", "holdback"]):
        
        # Windowing complexity
        if "window" in q or "holdback" in q:
            sql = f"""
                SELECT window_type, COUNT(*) as count 
                FROM content_planning 
                WHERE UPPER(region) = '{active_reg}' 
                GROUP BY window_type
            """
            return sql.strip() + ";", None, "bar"

        return f"SELECT * FROM content_planning WHERE UPPER(region) = '{active_reg}' LIMIT 100;", None, "pie"

    # 5. SAFETY FALLBACK 
    # If the user types something vague, we provide a preview of the most active table (Deals)
    # rather than showing "No results" or an error.
    fallback_sql = f"SELECT * FROM deals WHERE UPPER(region) = '{active_reg}' LIMIT 15;"
    return fallback_sql, None, "bar"

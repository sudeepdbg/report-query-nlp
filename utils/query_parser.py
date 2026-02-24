import re

def parse_query(question, region):
    q = question.lower()
    active_reg = region.upper()
    
    # 1. FINANCIALS & VENDORS
    if any(word in q for word in ["vendor", "deal", "spend", "cost", "value", "studio"]):
        # Specific Aggregation for 'Top Vendor' or 'Spend'
        if any(word in q for word in ["vendor", "who", "top", "rank", "spend"]):
            sql = f"""
                SELECT vendor_name, SUM(deal_value) as total_value 
                FROM deals 
                WHERE UPPER(region) = '{active_reg}' 
                GROUP BY vendor_name 
                ORDER BY total_value DESC
            """
            return sql.strip() + ";", None, "bar"
        
        # Default Deals list
        return f"SELECT * FROM deals WHERE UPPER(region) = '{active_reg}' ORDER BY deal_value DESC;", None, "bar"

    # 2. OPERATIONS & WORK ORDERS
    if any(word in q for word in ["order", "task", "work", "duplo", "delay"]):
        return f"SELECT * FROM work_orders WHERE UPPER(region) = '{active_reg}';", None, "pie"

    # 3. CONTENT & READINESS
    if any(word in q for word in ["content", "ready", "max", "svod", "title"]):
        return f"SELECT * FROM content_planning WHERE UPPER(region) = '{active_reg}';", None, "pie"

    # FALLBACK
    return f"SELECT * FROM deals WHERE UPPER(region) = '{active_reg}' LIMIT 15;", None, "bar"

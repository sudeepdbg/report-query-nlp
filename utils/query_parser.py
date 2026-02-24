import re

def parse_query(question, region):
    q = question.lower()
    active_reg = region.upper()
    
    # Sync region if mentioned in text
    for r in ["NA", "APAC", "EMEA", "LATAM"]:
        if r.lower() in q:
            active_reg = r
            break

    # 1. FINANCIALS / VENDORS / DEALS
    # Trigger on: vendor, spend, cost, deal, value
    if any(word in q for word in ["vendor", "deal", "value", "cost", "spend", "rights", "studio"]):
        
        # Scenario: Vendor Aggregation (Total Spend/Top Vendor)
        if any(word in q for word in ["vendor", "who", "top", "rank", "spend"]):
            sql = f"""
                SELECT vendor_name, SUM(deal_value) as total_value 
                FROM deals 
                WHERE UPPER(region) = '{active_reg}' 
                GROUP BY vendor_name 
                ORDER BY total_value DESC
            """
            return sql.strip() + ";", None, "bar"
        
        # Scenario: Rights Breakdown
        if "rights" in q or "scope" in q:
            return f"SELECT rights_scope, COUNT(*) as count FROM deals WHERE UPPER(region) = '{active_reg}' GROUP BY rights_scope;", None, "bar"

        # Default: Full Deals list
        return f"SELECT * FROM deals WHERE UPPER(region) = '{active_reg}' ORDER BY deal_value DESC;", None, "bar"

    # 2. OPERATIONS / WORK ORDERS
    if any(word in q for word in ["order", "task", "duplo", "queue", "delay", "asset", "work"]):
        return f"SELECT * FROM work_orders WHERE UPPER(region) = '{active_reg}' ORDER BY due_date ASC;", None, "pie"

    # 3. CONTENT / READINESS
    if any(word in q for word in ["content", "ready", "max", "inventory", "svod", "title"]):
        return f"SELECT * FROM content_planning WHERE UPPER(region) = '{active_reg}' LIMIT 50;", None, "pie"

    # FALLBACK
    return f"SELECT * FROM deals WHERE UPPER(region) = '{active_reg}' LIMIT 10;", None, "bar"

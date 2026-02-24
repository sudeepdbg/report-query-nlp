import re

def parse_query(question, region):
    """
    Final Query Parser with reinforced keyword detection for 
    Vendors, Deals, Localization, and Content Readiness.
    """
    q = question.lower()
    
    # 1. Detect Region (Prioritize text mention over sidebar)
    active_reg = region.upper()
    for r in ["NA", "APAC", "EMEA", "LATAM"]:
        if r.lower() in q:
            active_reg = r
            break

    # 2. Strict Network Mapping for specific regions
    network_map = {
        "LATAM": ["MAX LatAm"],
        "APAC": ["MAX Australia", "MAX Asia", "MAX India"],
        "EMEA": ["MAX Europe", "MAX Africa", "MAX UK"],
        "NA": ["MAX US"]
    }

    # 3. INTENT: DEALS, VENDORS, & FINANCIALS
    # Added 'vendor', 'budget', and 'negotiat' to the trigger list
    if any(word in q for word in ["deal", "rights", "value", "vendor", "budget", "negotiat", "cost"]):
        if "vendor" in q:
            # Aggregate deal value by vendor for the bar chart
            sql = f"""
                SELECT vendor_name, SUM(deal_value) as total_value 
                FROM deals 
                WHERE region = '{active_reg}' 
                GROUP BY vendor_name 
                ORDER BY total_value DESC
            """
            return sql.strip() + ";", None, "bar"
        
        if "scope" in q or "breakdown" in q:
            return f"SELECT rights_scope, count(*) as count FROM deals WHERE region = '{active_reg}' GROUP BY rights_scope;", None, "bar"
            
        # Default: Show list of deals
        sql = f"SELECT * FROM deals WHERE region = '{active_reg}'"
        if "active" in q: sql += " AND status = 'Active'"
        return sql + " ORDER BY deal_value DESC;", None, "bar"

    # 4. INTENT: LOCALIZATION & WORK ORDERS (Operational)
    if any(word in q for word in ["order", "task", "localiz", "dub", "sub", "duplo", "qa", "packaging"]):
        sql = f"SELECT * FROM work_orders WHERE region = '{active_reg}'"
        if "delayed" in q: sql += " AND status = 'Delayed'"
        if "duplo" in q: sql += " AND work_status LIKE '%Duplo%'"
        
        if "language" in q or "lang" in q:
            return f"SELECT language_target, count(*) as count FROM work_orders WHERE region = '{active_reg}' GROUP BY language_target;", None, "pie"
            
        return sql + ";", None, "pie"

    # 5. INTENT: CONTENT READINESS & SVOD (Inventory)
    if any(word in q for word in ["content", "ready", "max", "acqui", "inventory", "svod", "available"]):
        sql = f"SELECT * FROM content_planning WHERE region = '{active_reg}'"
        
        # Apply strict network filters
        if active_reg in network_map:
            nets = "', '".join(network_map[active_reg])
            sql += f" AND network IN ('{nets}')"
        
        if "ready" in q or "delivered" in q or "available" in q:
            sql += " AND status IN ('Delivered', 'Scheduled', 'Fulfilled')"
        if "acquired" in q:
            sql += " AND acquisition_status = 'Acquired'"
            
        return sql + ";", None, "pie"

    # 6. GLOBAL CATCH-ALL (Prevents the "Not Clear" error if a region is found)
    # If we know the region but keywords didn't match, default to showing content readiness
    sql = f"SELECT * FROM content_planning WHERE region = '{active_reg}' LIMIT 20;"
    return sql, None, "pie"

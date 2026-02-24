import re

def parse_query(question, region):
    """
    Refined Persona-Aware Parser.
    Handles Region detection, Persona-specific keywords, and 
    strict database mapping for Deals, Work Orders, and Content.
    """
    q = question.lower()
    
    # 1. REGION DETECTION (Text mentions override sidebar state)
    active_reg = region.upper()
    for r in ["NA", "APAC", "EMEA", "LATAM"]:
        if r.lower() in q:
            active_reg = r
            break

    # 2. INTENT: FINANCIALS & VENDORS (Leadership / Finance / Product)
    if any(word in q for word in ["deal", "rights", "value", "vendor", "budget", "negotiat", "cost", "spend", "kpi", "summary"]):
        # Vendor Ranking Logic
        if "vendor" in q or "who" in q:
            sql = f"""
                SELECT vendor_name, SUM(deal_value) as total_value 
                FROM deals 
                WHERE region = '{active_reg}' 
                GROUP BY vendor_name 
                ORDER BY total_value DESC
            """
            return sql.strip() + ";", None, "bar"
        
        # Rights breakdown for Product Persona
        if "scope" in q or "breakdown" in q or "type" in q:
            return f"SELECT rights_scope, count(*) as count FROM deals WHERE region = '{active_reg}' GROUP BY rights_scope;", None, "bar"
            
        # Summary/Executive Overview
        if "summary" in q or "overview" in q:
            return f"SELECT * FROM deals WHERE region = '{active_reg}' LIMIT 50;", None, "bar"

        # Default Deals list
        sql = f"SELECT * FROM deals WHERE region = '{active_reg}'"
        if "active" in q: sql += " AND status = 'Active'"
        return sql + " ORDER BY deal_value DESC;", None, "bar"

    # 3. INTENT: OPERATIONAL TASKS (Operations Persona)
    if any(word in q for word in ["order", "task", "localiz", "dub", "sub", "duplo", "qa", "packaging", "queue", "delay"]):
        sql = f"SELECT * FROM work_orders WHERE region = '{active_reg}'"
        
        if "delayed" in q or "delay" in q: 
            sql += " AND status = 'Delayed'"
        if "duplo" in q: 
            sql += " AND work_status LIKE '%Duplo%'"
        if "packaging" in q:
            sql += " AND work_status = 'Packaging'"
        
        if "language" in q or "lang" in q:
            return f"SELECT language_target, count(*) as count FROM work_orders WHERE region = '{active_reg}' GROUP BY language_target;", None, "pie"
            
        return sql + " ORDER BY id DESC;", None, "pie"

    # 4. INTENT: CONTENT & SVOD (Product Persona)
    if any(word in q for word in ["content", "ready", "max", "acqui", "inventory", "svod", "available", "show"]):
        sql = f"SELECT * FROM content_planning WHERE region = '{active_reg}'"
        
        # Filter for specific Readiness keywords
        if any(word in q for word in ["ready", "delivered", "available", "complete"]):
            sql += " AND status IN ('Delivered', 'Scheduled', 'Fulfilled')"
        
        if "acquired" in q:
            sql += " AND acquisition_status = 'Acquired'"
            
        # Specific search for a title
        match = re.search(r"(?:for|about|show)\s+([\w\s]+)", q)
        if match:
            potential_title = match.group(1).strip().title()
            # Filter out region names from being treated as content titles
            if potential_title.upper() not in ["APAC", "EMEA", "LATAM", "NA"]:
                sql += f" AND content_title LIKE '%{potential_title}%'"
                
        return sql + ";", None, "pie"

    # 5. GLOBAL FALLBACK (Guarantees a result instead of an error)
    # If a region is identified, we show the most relevant table for that region
    fallback_sql = f"SELECT * FROM content_planning WHERE region = '{active_reg}' LIMIT 20;"
    return fallback_sql, None, "pie"

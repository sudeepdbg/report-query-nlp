import re

def parse_query(question, region):
    """
    Final NL to SQL Parser.
    Features: Strict Network-Region mapping, Text-based region override, 
    and support for SVOD, Duplo, and Acquisition metadata.
    """
    q = question.lower()
    
    # 1. REGIONAL INTELLIGENCE: Detect if a region is named in the prompt
    # If found, it overrides the sidebar region for this specific query
    active_reg = region.upper()
    for r in ["NA", "APAC", "EMEA", "LATAM"]:
        if r.lower() in q:
            active_reg = r
            break

    # 2. NETWORK TOPOLOGY: Strict mapping to prevent regional data leakage
    network_map = {
        "LATAM": ["MAX LatAm"],
        "APAC": ["MAX Australia", "MAX Asia", "MAX India"],
        "EMEA": ["MAX Europe", "MAX Africa", "MAX UK"],
        "NA": ["MAX US"]
    }
    
    # 3. INTENT: RIGHTS & DEALS (Financials -> BAR CHART)
    if any(word in q for word in ["deal", "rights", "svod", "value", "cost", "negotiat"]):
        # Pivot: SVOD rights are stored in content_planning, not deals
        if "svod" in q:
            sql = f"SELECT content_title, network, rights_type, status, region FROM content_planning WHERE region = '{active_reg}'"
            if active_reg in network_map:
                nets = "', '".join(network_map[active_reg])
                sql += f" AND network IN ('{nets}')"
            
            if "ready" in q or "delivered" in q:
                sql += " AND status IN ('Delivered', 'Scheduled', 'Fulfilled')"
            return sql + ";", None, "bar"
        
        # Default Deals Query
        sql = f"SELECT * FROM deals WHERE region = '{active_reg}'"
        if "active" in q: sql += " AND status = 'Active'"
        sql += " ORDER BY deal_value DESC"
        return sql + ";", None, "bar"

    # 4. INTENT: LOCALIZATION & WORK ORDERS (Operational -> PIE CHART)
    if any(word in q for word in ["order", "task", "localiz", "dub", "sub", "duplo", "qa", "packaging"]):
        table = "work_orders"
        sql = f"SELECT * FROM {table} WHERE region = '{active_reg}'"
        
        if "delayed" in q: sql += " AND status = 'Delayed'"
        if "duplo" in q: sql += " AND work_status LIKE '%Duplo%'"
        if "packaging" in q: sql += " AND work_status = 'Packaging'"
        
        if "language" in q or "lang" in q:
            return f"SELECT language_target, count(*) as count FROM work_orders WHERE region = '{active_reg}' GROUP BY language_target;", None, "pie"
        
        return sql + ";", None, "pie"

    # 5. INTENT: CONTENT & ACQUISITION READINESS (Inventory -> PIE CHART)
    if any(word in q for word in ["content", "show", "max", "ready", "acqui", "inventory"]):
        table = "content_planning"
        sql = f"SELECT * FROM {table} WHERE region = '{active_reg}'"
        
        # Apply Network Filter to prevent cross-region results
        if active_reg in network_map:
            nets = "', '".join(network_map[active_reg])
            sql += f" AND network IN ('{nets}')"
        
        if "ready" in q or "delivered" in q:
            sql += " AND status IN ('Delivered', 'Scheduled', 'Fulfilled')"
        if "acquired" in q:
            sql += " AND acquisition_status = 'Acquired'"
            
        # Specific Title Search
        match = re.search(r"(?:for|about|show)\s+(.*)", q)
        if match:
            title = match.group(1).strip().title()
            if title.upper() not in ["APAC", "EMEA", "NA", "LATAM", "STATUS"]:
                sql += f" AND content_title LIKE '%{title}%'"
                
        return sql + ";", None, "pie"

    # Fallback
    return None, f"I understood the region is {active_reg}, but I'm not sure if you're asking about Deals, Localization, or Readiness. Please clarify.", None

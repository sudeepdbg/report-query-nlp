import re

def parse_query(question, region):
    """
    Advanced NL to SQL parser for Media Supply Chain.
    Handles Rights, Acquisition, Localization, and Duplo Work Status.
    """
    q = question.lower()
    reg = region.upper()
    
    # 1. RIGHTS & DEALS (Numerical/Financial -> BAR CHART)
    # Context: SVOD/Linear rights, Deal values, Negotiating status
    if any(word in q for word in ["deal", "rights", "value", "cost", "negotiat", "scope"]):
        table = "deals"
        sql = f"SELECT * FROM {table} WHERE region = '{reg}'"
        
        # Rights-specific logic: If asking about SVOD, check content_planning rights_type
        if "svod" in q: 
            return f"SELECT content_title, network, rights_type FROM content_planning WHERE region = '{reg}' AND rights_type = 'SVOD Exclusive';", None, "bar"
        
        if "active" in q: sql += " AND status = 'Active'"
        if "scope" in q: 
            return f"SELECT rights_scope, count(*) as total_deals FROM deals WHERE region = '{reg}' GROUP BY rights_scope;", None, "bar"
        
        sql += " ORDER BY deal_value DESC"
        return sql + ";", None, "bar"

    # 2. LOCALIZATION & WORK ORDERS (Operational -> PIE CHART)
    # Context: Duplo status, Subtitling/Dubbing tasks, Language targets
    if any(word in q for word in ["order", "task", "localiz", "dub", "sub", "duplo", "qa"]):
        table = "work_orders"
        sql = f"SELECT * FROM {table} WHERE region = '{reg}'"
        
        if "delayed" in q: sql += " AND status = 'Delayed'"
        if "duplo" in q: sql += " AND work_status LIKE '%Duplo%'"
        if "packaging" in q: sql += " AND work_status = 'Packaging'"
        if "language" in q or "lang" in q:
            return f"SELECT language_target, count(*) as count FROM work_orders WHERE region = '{reg}' GROUP BY language_target;", None, "pie"
        
        return sql + ";", None, "pie"

    # 3. CONTENT & ACQUISITION READINESS (Inventory -> PIE CHART)
    # Context: Acquisition status, Market-ready content, Localization available
    if any(word in q for word in ["content", "show", "max", "ready", "acqui", "localization status", "available"]):
        table = "content_planning"
        sql = f"SELECT * FROM {table} WHERE region = '{reg}'"
        
        # Acquisition filters
        if "acquired" in q: sql += " AND acquisition_status = 'Acquired'"
        if "pending" in q: sql += " AND acquisition_status = 'Pending Materials'"
        
        # Localization Readiness
        if "localization" in q:
            if "complete" in q: sql += " AND localization_status = 'Completed'"
            else: sql += " AND localization_status != 'Completed'"
        
        # Network filtering
        networks = ["MAX US", "MAX Europe", "MAX Australia", "MAX LatAm", "MAX Asia", "MAX India", "MAX UK"]
        for net in networks:
            if net.lower() in q:
                sql += f" AND network = '{net}'"

        # Content Title Extraction
        match = re.search(r"(?:for|about|show)\s+(.*)", q)
        if match:
            potential_title = match.group(1).strip().title()
            if potential_title.upper() not in ["APAC", "EMEA", "NA", "LATAM", "STATUS"]:
                sql += f" AND content_title LIKE '%{potential_title}%'"
            
        return sql + ";", None, "pie"

    # Fallback for unrecognized intent
    return None, "Intent not clear. Please ask about 'Rights', 'Localization Status', 'Acquisition Readiness', or 'Duplo Work Status'.", None

import re

def parse_query(question, region):
    q = question.lower()
    reg = region.upper()

    network_map = {
        "LATAM": ["MAX LatAm"],
        "APAC": ["MAX Australia", "MAX Asia", "MAX India"],
        "EMEA": ["MAX Europe", "MAX Africa", "MAX UK"],
        "NA": ["MAX US"]
    }
    
    # 1. RIGHTS & DEALS
    if any(word in q for word in ["deal", "rights", "value", "cost", "negotiat", "scope"]):
        if "svod" in q:
            # Added 'region' to the SELECT for visual confirmation
            sql = f"SELECT content_title, network, rights_type, status, region FROM content_planning WHERE region = '{reg}'"
            if reg in network_map:
                nets = "', '".join(network_map[reg])
                sql += f" AND network IN ('{nets}')"
            
            if "ready" in q or "delivered" in q:
                sql += " AND status IN ('Delivered', 'Scheduled', 'Fulfilled')"
            return sql + ";", None, "bar"
        
        table = "deals"
        sql = f"SELECT * FROM {table} WHERE region = '{reg}'"
        if "active" in q: sql += " AND status = 'Active'"
        if "scope" in q: 
            return f"SELECT rights_scope, count(*) as total_deals FROM deals WHERE region = '{reg}' GROUP BY rights_scope;", None, "bar"
        
        sql += " ORDER BY deal_value DESC"
        return sql + ";", None, "bar"

    # 2. LOCALIZATION & WORK ORDERS
    if any(word in q for word in ["order", "task", "localiz", "dub", "sub", "duplo", "qa"]):
        table = "work_orders"
        sql = f"SELECT * FROM {table} WHERE region = '{reg}'"
        if "delayed" in q: sql += " AND status = 'Delayed'"
        if "duplo" in q: sql += " AND work_status LIKE '%Duplo%'"
        if "packaging" in q: sql += " AND work_status = 'Packaging'"
        if "language" in q or "lang" in q:
            return f"SELECT language_target, count(*) as count FROM work_orders WHERE region = '{reg}' GROUP BY language_target;", None, "pie"
        return sql + ";", None, "pie"

    # 3. CONTENT & ACQUISITION READINESS
    if any(word in q for word in ["content", "show", "max", "ready", "acqui", "localization", "available"]):
        table = "content_planning"
        sql = f"SELECT * FROM {table} WHERE region = '{reg}'"
        if reg in network_map:
            nets = "', '".join(network_map[reg])
            sql += f" AND network IN ('{nets}')"
        
        if "acquired" in q: sql += " AND acquisition_status = 'Acquired'"
        if "pending" in q: sql += " AND acquisition_status = 'Pending Materials'"
        if "localization" in q:
            if "complete" in q: sql += " AND localization_status = 'Completed'"
            else: sql += " AND localization_status != 'Completed'"
        if "ready" in q or "delivered" in q:
            sql += " AND status IN ('Delivered', 'Scheduled', 'Fulfilled')"

        match = re.search(r"(?:for|about|show)\s+(.*)", q)
        if match:
            potential_title = match.group(1).strip().title()
            if potential_title.upper() not in ["APAC", "EMEA", "NA", "LATAM", "STATUS"]:
                sql += f" AND content_title LIKE '%{potential_title}%'"
            
        return sql + ";", None, "pie"

    return None, "Intent not clear. Please ask about 'Rights', 'Localization Status', 'Acquisition Readiness', or 'Duplo Status'.", None

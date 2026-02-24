import re

def parse_query(question, region):
    """
    Translates natural language to targeted SQL for the Media Supply Chain.
    Forces regional filtering and assigns specific chart types based on data nature.
    """
    q = question.lower()
    reg = region.upper()
    
    # 1. DEALS & VENDORS (Numerical/Financial -> BAR CHART)
    # Target words: deals, value, vendor, cost, money, budget
    if any(word in q for word in ["deal", "vendor", "value", "rights", "cost"]):
        table = "deals"
        # Start with strict regional isolation
        sql = f"SELECT * FROM {table} WHERE region = '{reg}'"
        
        # Specific status filtering
        if "active" in q: sql += " AND status = 'Active'"
        if "closed" in q: sql += " AND status = 'Closed'"
        
        # Sorting by value for better bar charts
        sql += " ORDER BY deal_value DESC"
        
        return sql + ";", None, "bar"

    # 2. WORK ORDERS (Operational Status -> PIE CHART)
    # Target words: work order, task, asset, subtitle, dub, qa
    if any(word in q for word in ["order", "task", "asset", "subtitle", "dub", "qa"]):
        table = "work_orders"
        sql = f"SELECT * FROM {table} WHERE region = '{reg}'"
        
        if "delayed" in q: sql += " AND status = 'Delayed'"
        if "completed" in q or "done" in q: sql += " AND status = 'Completed'"
        if "priority" in q: sql += " AND priority LIKE 'A%'"
        
        return sql + ";", None, "pie"

    # 3. CONTENT PLANNING (Inventory Readiness -> PIE CHART)
    # Target words: content, show, status, max, inventory, schedule
    if any(word in q for word in ["content", "show", "status", "max", "delivered", "ready"]):
        table = "content_planning"
        sql = f"SELECT * FROM {table} WHERE region = '{reg}'"
        
        # Variety Support: Mapping specific network typing patterns
        networks = ["MAX US", "MAX Europe", "MAX Australia", "MAX LatAm", "MAX Asia", "MAX Africa", "MAX India", "MAX UK"]
        for net in networks:
            if net.lower() in q:
                sql += f" AND network = '{net}'"
        
        # Readiness filters
        if "ready" in q or "pending" in q: sql += " AND status = 'Not Ready'"
        if "delivered" in q: sql += " AND status = 'Delivered'"
        
        # Content Title Extraction (e.g., "status for House of the Dragon")
        match = re.search(r"(?:for|about|show)\s+(.*)", q)
        if match:
            potential_title = match.group(1).strip().title()
            # Safety: Ensure we aren't searching for a region name as a title
            if potential_title.upper() not in ["APAC", "EMEA", "NA", "LATAM", "STATUS"]:
                sql += f" AND content_title LIKE '%{potential_title}%'"

        return sql + ";", None, "pie"

    # Fallback for unrecognized intent
    return None, "I'm sorry, I couldn't translate that. Please ask about 'Deals', 'Content Status', or 'Work Orders'.", None

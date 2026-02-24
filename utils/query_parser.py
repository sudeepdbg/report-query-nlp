import re

def parse_query(question, region):
    """
    Advanced Industry-Aware Parser.
    Handles Region sync, Persona-specific intent, and fuzzy keyword matching.
    """
    q = question.lower()
    
    # 1. REGION DETECTION (Overrides sidebar if mentioned in text)
    active_reg = region.upper()
    for r in ["NA", "APAC", "EMEA", "LATAM"]:
        if r.lower() in q:
            active_reg = r
            break

    # 2. INTENT: FINANCIALS, VENDORS & STUDIO DEALS
    # Triggers on: vendor, studio, deal, cost, spend, value, budget, payment
    if any(word in q for word in ["vendor", "studio", "deal", "cost", "spend", "value", "budget", "payment", "rights"]):
        
        # Scenario: Vendor Ranking (Aggregated)
        if any(word in q for word in ["vendor", "who", "top", "rank"]):
            sql = f"""
                SELECT vendor_name, SUM(deal_value) as total_value, COUNT(*) as deal_count
                FROM deals 
                WHERE region = '{active_reg}' 
                GROUP BY vendor_name 
                ORDER BY total_value DESC
            """
            return sql.strip() + ";", None, "bar"
        
        # Scenario: Payment Terms/Finance
        if "payment" in q or "term" in q or "net" in q:
            return f"SELECT payment_terms, COUNT(*) as count FROM deals WHERE region = '{active_reg}' GROUP BY payment_terms;", None, "pie"

        # Scenario: Rights Complexity (SVOD, Exclusive, etc.)
        if "rights" in q or "exclusive" in q or "scope" in q:
            return f"SELECT rights_scope, COUNT(*) as count FROM deals WHERE region = '{active_reg}' GROUP BY rights_scope;", None, "bar"

        # Default: List of Deals
        return f"SELECT * FROM deals WHERE region = '{active_reg}' ORDER BY deal_value DESC;", None, "bar"

    # 3. INTENT: SUPPLY CHAIN & OPERATIONS (Work Orders)
    # Triggers on: order, task, duplo, queue, delay, asset, mastering, qa, dub, sub
    if any(word in q for word in ["order", "task", "duplo", "queue", "delay", "asset", "mastering", "qa", "dub", "sub", "work"]):
        
        # Scenario: High Priority or Bottlenecks
        if "delay" in q or "critical" in q or "failed" in q:
            sql = f"SELECT * FROM work_orders WHERE region = '{active_reg}' AND (status = 'Delayed' OR priority = 'Critical')"
            return sql + ";", None, "pie"

        # Scenario: Language/Localization breakdown
        if "language" in q or "lang" in q or "target" in q:
            return f"SELECT language_target, COUNT(*) as count FROM work_orders WHERE region = '{active_reg}' GROUP BY language_target;", None, "pie"

        # Scenario: Specific Asset Types (Atmos, Mezzanine)
        if "asset" in q or "file" in q or "type" in q:
            return f"SELECT asset_type, COUNT(*) as count FROM work_orders WHERE region = '{active_reg}' GROUP BY asset_type;", None, "bar"

        # Default: Work Order List
        return f"SELECT * FROM work_orders WHERE region = '{active_reg}' ORDER BY due_date ASC;", None, "pie"

    # 4. INTENT: CONTENT STRATEGY & WINDOWING (Content Planning)
    # Triggers on: content, ready, max, inventory, window, holdback, schedule, title
    if any(word in q for word in ["content", "ready", "max", "inventory", "window", "holdback", "schedule", "title", "svod"]):
        
        # Scenario: Windowing and Holdbacks (Leadership/Product)
        if "window" in q or "holdback" in q:
            return f"SELECT window_type, COUNT(*) as count FROM content_planning WHERE region = '{active_reg}' GROUP BY window_type;", None, "bar"

        # Scenario: Readiness Status
        if "ready" in q or "status" in q or "available" in q:
            return f"SELECT status, COUNT(*) as count FROM content_planning WHERE region = '{active_reg}' GROUP BY status;", None, "pie"

        # Scenario: Specific Title Search
        # Uses regex to find title after 'show', 'find', or 'about'
        match = re.search(r"(?:show|find|about)\s+([\w\s]+)", q)
        if match:
            title_query = match.group(1).strip()
            # Avoid matching region names as titles
            if title_query.upper() not in ["APAC", "EMEA", "LATAM", "NA"]:
                return f"SELECT * FROM content_planning WHERE content_title LIKE '%{title_query}%' AND region = '{active_reg}';", None, "pie"

        # Default: Content Planning List
        return f"SELECT * FROM content_planning WHERE region = '{active_reg}' LIMIT 100;", None, "pie"

    # 5. GLOBAL FALLBACK
    # If a region is mentioned but no intent is clear, show the most critical data: Deals
    return f"SELECT * FROM deals WHERE region = '{active_reg}' LIMIT 20;", None, "bar"

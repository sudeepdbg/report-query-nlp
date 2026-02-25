import re

def parse_query(question, region):
    """
    Foundry Vantage V4: Relational Multi-Table Parser.
    Combines fuzzy keyword matching with complex JOIN logic.
    """
    q = question.lower().strip()
    reg = region.upper()

    # --- 1. INTENT: VENDOR PERFORMANCE (The Bridge) ---
    # Matches: "vendor performance", "reliability", "who is delayed and expensive"
    performance_keywords = ["performance", "reliability", "efficiency", "compare", "vs"]
    if any(word in q for word in performance_keywords) and "vendor" in q:
        sql = f"""
            SELECT v.vendor_name, COUNT(w.id) as delays, SUM(d.deal_value) as total_spend
            FROM vendor_master v
            LEFT JOIN work_orders w ON v.vendor_id = w.vendor_id AND w.status = 'Delayed'
            LEFT JOIN deals d ON v.vendor_id = d.vendor_id
            WHERE d.region = '{reg}'
            GROUP BY v.vendor_name 
            ORDER BY delays DESC
        """
        return sql.strip() + ";", None, "bar"

    # --- 2. INTENT: VENDORS & SPEND (Financials) ---
    vendor_keywords = ["vendor", "vendors", "spend", "cost", "rank", "who", "top", "studio"]
    if any(word in q for word in vendor_keywords):
        # Aggregation: Grouping by Vendor Name using JOIN
        if any(word in q for word in ["top", "total", "sum", "who", "spend", "biggest"]):
            sql = f"""
                SELECT v.vendor_name, SUM(d.deal_value) as total_value
                FROM deals d
                JOIN vendor_master v ON d.vendor_id = v.vendor_id
                WHERE d.region = '{reg}'
                GROUP BY v.vendor_name
                ORDER BY total_value DESC
            """
            return sql.strip() + ";", None, "bar"
        
        # Detail List
        sql = f"""
            SELECT v.vendor_name, d.deal_name, d.deal_value, d.status 
            FROM deals d
            JOIN vendor_master v ON d.vendor_id = v.vendor_id
            WHERE d.region = '{reg}'
        """
        return sql.strip() + ";", None, "bar"

    # --- 3. INTENT: OPERATIONS & WORK ORDERS ---
    ops_keywords = ["order", "orders", "task", "tasks", "duplo", "delay", "work"]
    if any(word in q for word in ops_keywords):
        # Join with title registry to show WHAT is being worked on
        sql = f"""
            SELECT t.content_title, w.work_order, w.status, w.due_date, v.vendor_name
            FROM work_orders w
            JOIN title_registry t ON w.title_id = t.title_id
            JOIN vendor_master v ON w.vendor_id = v.vendor_id
            WHERE w.region = '{reg}'
            ORDER BY w.due_date ASC
        """
        return sql.strip() + ";", None, "pie"

    # --- 4. INTENT: CONTENT & TITLES ---
    content_keywords = ["content", "ready", "inventory", "title", "titles", "planning"]
    if any(word in q for word in content_keywords):
        if any(word in q for word in ["ready", "status", "readiness"]):
            return f"SELECT status, COUNT(*) as count FROM content_planning WHERE region = '{reg}' GROUP BY status;", None, "pie"
        
        # Detailed Title View
        sql = f"""
            SELECT t.content_title, t.studio_owner, c.status, c.planned_date
            FROM content_planning c
            JOIN title_registry t ON c.title_id = t.title_id
            WHERE c.region = '{reg}'
        """
        return sql.strip() + ";", None, "pie"

    return None, None, None

def parse_query(question, region):
    q = question.lower().strip()
    reg = region.upper()

    # FINANCIALS (Matches: Top vendors, Total spend, Market value)
    if any(k in q for k in ["vendor", "spend", "value", "cost"]):
        sql = f"""
            SELECT vendor_name, SUM(deal_value) as total_value 
            FROM deals 
            WHERE UPPER(region) = '{reg}' 
            GROUP BY vendor_name 
            ORDER BY total_value DESC
        """
        return sql, None, "bar"

    # OPERATIONS (Matches: Delayed tasks, Work orders, Performance)
    if any(k in q for k in ["delay", "task", "order", "performance"]):
        status_filter = "AND UPPER(status) = 'DELAYED'" if "delay" in q else ""
        sql = f"""
            SELECT vendor_name, COUNT(*) as count 
            FROM work_orders 
            WHERE UPPER(region) = '{reg}' {status_filter}
            GROUP BY vendor_name
        """
        return sql, None, "bar"

    # CONTENT (Matches: Readiness, Inventory, SVOD, Rights)
    if any(k in q for k in ["ready", "status", "inventory", "rights", "svod"]):
        if "rights" in q or "svod" in q:
            sql = f"SELECT rights_scope, COUNT(*) FROM deals WHERE UPPER(region) = '{reg}' GROUP BY rights_scope"
            return sql, None, "bar"
        sql = f"SELECT status, COUNT(*) FROM content_planning WHERE UPPER(region) = '{reg}' GROUP BY status"
        return sql, None, "pie"

    # Catch-all fallback
    return f"SELECT vendor_name, deal_name FROM deals WHERE UPPER(region) = '{reg}' LIMIT 5", None, "bar"

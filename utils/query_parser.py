def parse_query(question, region):
    """
    Finalized Query Parser:
    - Pie/Donut for distributions (Status, Rights).
    - Treemap for hierarchical financial data (Market Value).
    - Horizontal Bar for rankings (Spend, Performance).
    """
    q = question.lower().strip()
    reg = region.upper()

    # 1. PIE/DONUT CLUSTER: Composition & Status
    if any(k in q for k in ["status", "ready", "rights", "svod", "inventory", "distribution", "breakdown"]):
        if "rights" in q or "svod" in q:
            sql = f"SELECT rights_scope, COUNT(*) as count FROM deals WHERE UPPER(region) = '{reg}' GROUP BY rights_scope"
            return sql.strip(), None, "pie"
        
        sql = f"SELECT status, COUNT(*) as count FROM content_planning WHERE UPPER(region) = '{reg}' GROUP BY status"
        return sql.strip(), None, "pie"

    # 2. TREEMAP CLUSTER: High-End Financial Composition
    if any(k in q for k in ["market value", "total value", "deal value"]):
        sql = f"""
            SELECT vendor_name, deal_name, deal_value 
            FROM deals 
            WHERE UPPER(region) = '{reg}' 
            ORDER BY deal_value DESC
        """
        return sql.strip(), None, "treemap"

    # 3. HORIZONTAL BAR CLUSTER: Rankings & Operational Performance
    if any(k in q for k in ["vendor", "spend", "cost", "performance", "top", "task", "order", "delay"]):
        if any(k in q for k in ["task", "order", "delay"]):
            status_filter = "AND UPPER(status) = 'DELAYED'" if "delay" in q else ""
            sql = f"""
                SELECT vendor_name, COUNT(*) as count 
                FROM work_orders 
                WHERE UPPER(region) = '{reg}' {status_filter}
                GROUP BY vendor_name ORDER BY count ASC
            """
        else:
            sql = f"""
                SELECT vendor_name, SUM(deal_value) as total_value 
                FROM deals 
                WHERE UPPER(region) = '{reg}' 
                GROUP BY vendor_name ORDER BY total_value ASC
            """
        return sql.strip(), None, "bar"

    # 4. FALLBACK
    return f"SELECT vendor_name, deal_name, deal_value FROM deals WHERE UPPER(region) = '{reg}' LIMIT 10", None, "bar"

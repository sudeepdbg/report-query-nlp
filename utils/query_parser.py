def parse_query(question, region):
    q = question.lower()
    reg = region.upper()

    # 1. Vendor Spend (Joins Deals + Vendors)
    if "spend" in q or "top vendor" in q:
        sql = f"""
            SELECT v.vendor_name, SUM(d.deal_value) as total_spend
            FROM deals d
            JOIN vendors v ON d.vendor_id = v.vendor_id
            WHERE d.region = '{reg}'
            GROUP BY v.vendor_name ORDER BY total_spend DESC
        """
        return sql, None, "bar"

    # 2. Operational Delays (Joins Work Orders + Vendors)
    if "delay" in q or "performance" in q:
        sql = f"""
            SELECT v.vendor_name, COUNT(w.id) as delay_count
            FROM work_orders w
            JOIN vendors v ON w.vendor_id = v.vendor_id
            WHERE w.region = '{reg}' AND w.status = 'Delayed'
            GROUP BY v.vendor_name ORDER BY delay_count DESC
        """
        return sql, None, "bar"

    # 3. Content Inventory
    if "status" in q or "ready" in q:
        sql = f"SELECT status, COUNT(*) as count FROM content_planning WHERE region = '{reg}' GROUP BY status"
        return sql, None, "pie"

    # 4. Default Fallback: Just show Deals for the region
    return f"SELECT deal_name, deal_value, status FROM deals WHERE region = '{reg}' LIMIT 20", None, "bar"

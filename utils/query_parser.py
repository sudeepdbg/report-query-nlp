def parse_query(question, region):
    """
    Robust Professional Parser:
    - Pie/Donut: Status & Distributional data.
    - Treemap: Hierarchical Market Value.
    - Vertical Bar: High-cost individual items.
    - Horizontal Bar: Vendor rankings & Operational performance.
    """
    q = question.lower().strip()
    reg = region.upper()

    # 1. PIE/DONUT: Composition & Status
    if any(k in q for k in ["status", "ready", "rights", "svod", "inventory", "distribution", "breakdown"]):
        if "rights" in q or "svod" in q:
            sql = f"SELECT rights_scope, COUNT(*) as count FROM deals WHERE UPPER(region) = '{reg}' GROUP BY rights_scope"
        else:
            sql = f"SELECT status, COUNT(*) as count FROM content_planning WHERE UPPER(region) = '{reg}' GROUP BY status"
        return sql.strip(), None, "pie"

    # 2. TREEMAP: Market Value (Hierarchical)
    if "market value" in q:
        sql = f"SELECT vendor_name, deal_name, deal_value FROM deals WHERE UPPER(region) = '{reg}'"
        return sql.strip(), None, "treemap"

    # 3. VERTICAL BAR: Highest Cost / Individual Deals
    # Distinct from 'Spend' because it doesn't GROUP BY vendor
    if any(k in q for k in ["highest cost", "costliest", "top deals", "expensive"]):
        sql = f"""
            SELECT deal_name, deal_value 
            FROM deals 
            WHERE UPPER(region) = '{reg}' 
            ORDER BY deal_value DESC LIMIT 10
        """
        return sql.strip(), None, "bar_v"

    # 4. HORIZONTAL BAR: Rankings (Vendor Spend & Operations)
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
            # Aggregate spend by vendor
            sql = f"""
                SELECT vendor_name, SUM(deal_value) as total_value 
                FROM deals 
                WHERE UPPER(region) = '{reg}' 
                GROUP BY vendor_name ORDER BY total_value ASC
            """
        return sql.strip(), None, "bar_h"

    # 5. FALLBACK
    return f"SELECT vendor_name, deal_name, deal_value FROM deals WHERE UPPER(region) = '{reg}' LIMIT 10", None, "bar_h"

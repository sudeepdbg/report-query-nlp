def parse_query(question, region):
    """
    Finalized Unified Parser:
    - Merges specific 'Delayed' filtering from original.
    - Merges 'Pie Chart' logic for distributions from suggested.
    - Ensures high-fidelity SQL for all sidebar suggestions.
    """
    q = question.lower().strip()
    reg = region.upper()

    # 1. PIE CHART CLUSTER: Distribution & Status
    # Targeted: Rights, Readiness, Inventory, Status
    if any(k in q for k in ["status", "ready", "rights", "svod", "inventory", "distribution"]):
        # Sub-intent: Rights Scope Breakdown (Deals Table)
        if "rights" in q or "svod" in q:
            sql = f"""
                SELECT rights_scope, COUNT(*) as count 
                FROM deals 
                WHERE UPPER(region) = '{reg}' 
                GROUP BY rights_scope
            """
            return sql.strip(), None, "pie"
        
        # Sub-intent: Content Readiness (Content Planning Table)
        sql = f"""
            SELECT status, COUNT(*) as count 
            FROM content_planning 
            WHERE UPPER(region) = '{reg}' 
            GROUP BY status
        """
        return sql.strip(), None, "pie"

    # 2. BAR CHART CLUSTER: Financials & Rankings
    # Targeted: Spend, Vendor rankings, Market value
    if any(k in q for k in ["vendor", "spend", "value", "cost"]):
        sql = f"""
            SELECT vendor_name, SUM(deal_value) as total_value 
            FROM deals 
            WHERE UPPER(region) = '{reg}' 
            GROUP BY vendor_name 
            ORDER BY total_value DESC
        """
        return sql.strip(), None, "bar"

    # 3. BAR CHART CLUSTER: Operational Performance
    # Targeted: Work orders, Tasks, Performance, Delays
    if any(k in q for k in ["delay", "task", "order", "performance"]):
        # Restoration of your original specific 'Delayed' filter
        status_filter = "AND UPPER(status) = 'DELAYED'" if "delay" in q else ""
        sql = f"""
            SELECT vendor_name, COUNT(*) as count 
            FROM work_orders 
            WHERE UPPER(region) = '{reg}' {status_filter}
            GROUP BY vendor_name
            ORDER BY count DESC
        """
        return sql.strip(), None, "bar"

    # 4. CATCH-ALL FALLBACK
    # High-detail fallback for general questions
    return f"SELECT vendor_name, deal_name, deal_value FROM deals WHERE UPPER(region) = '{reg}' LIMIT 10", None, "bar"

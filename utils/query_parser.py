import re

def parse_query(question, region):
    """
    Advanced Logic Parser.
    Uses flexible keyword clusters to ensure sidebar suggestions always return data.
    """
    q = question.lower().strip()
    reg = region.upper()

    # 1. CLUSTER: VENDOR SPEND / FINANCIALS
    # Matches: "Top vendors", "Spend per vendor", "Market value", "Highest cost"
    if any(word in q for word in ["vendor", "spend", "cost", "value", "deal"]):
        sql = f"""
            SELECT vendor_name, SUM(deal_value) as total_value 
            FROM deals 
            WHERE UPPER(region) = '{reg}' 
            GROUP BY vendor_name 
            ORDER BY total_value DESC
        """
        return sql.strip(), None, "bar"

    # 2. CLUSTER: OPERATIONAL DELAYS
    # Matches: "Delayed tasks", "Vendor performance", "Work order status"
    if any(word in q for word in ["delay", "performance", "task", "order"]):
        # Priority check for actual delays
        status_filter = "AND UPPER(status) = 'DELAYED'" if "delay" in q else ""
        sql = f"""
            SELECT vendor_name, COUNT(*) as task_count 
            FROM work_orders 
            WHERE UPPER(region) = '{reg}' {status_filter}
            GROUP BY vendor_name 
            ORDER BY task_count DESC
        """
        return sql.strip(), None, "bar"

    # 3. CLUSTER: CONTENT READINESS / RIGHTS
    # Matches: "Content readiness", "SVOD rights", "Inventory", "Status"
    if any(word in q for word in ["ready", "status", "rights", "inventory", "svod"]):
        # If looking for rights distribution
        if "rights" in q or "svod" in q:
            sql = f"""
                SELECT rights_scope, COUNT(*) as count 
                FROM deals 
                WHERE UPPER(region) = '{reg}' 
                GROUP BY rights_scope
            """
            return sql.strip(), None, "bar"
        
        # Default to Content Readiness
        sql = f"""
            SELECT status, COUNT(*) as count 
            FROM content_planning 
            WHERE UPPER(region) = '{reg}' 
            GROUP BY status
        """
        return sql.strip(), None, "pie"

    # 4. FALLBACK: If nothing matches, show raw deal list for the region
    return f"SELECT vendor_name, deal_name, deal_value FROM deals WHERE UPPER(region) = '{reg}' LIMIT 10", None, "bar"

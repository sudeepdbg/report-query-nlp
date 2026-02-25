import re

def parse_query(question, region):
    """
    Enhanced Enterprise Parser:
    - Auto-Region Detection: Prioritizes query text over sidebar state.
    - Pie/Donut: Status & Distributional data.
    - Treemap: Hierarchical Market Value.
    - Vertical Bar: High-cost individual items.
    - Horizontal Bar: Vendor rankings & Operational performance.
    """
    q = question.lower().strip()
    
    # --- 1. REGION DETECTION (Overrides sidebar if found in query) ---
    reg = region.upper()
    potential_regions = ["NA", "APAC", "EMEA", "LATAM"]
    for r in potential_regions:
        if r.lower() in q:
            reg = r
            break

    # --- 2. PIE/DONUT: Composition & Status ---
    if any(k in q for k in ["status", "ready", "rights", "svod", "inventory", "distribution", "breakdown"]):
        if "rights" in q or "svod" in q:
            sql = f"SELECT rights_scope, COUNT(*) as count FROM deals WHERE UPPER(region) = '{reg}' GROUP BY rights_scope"
        else:
            sql = f"SELECT status, COUNT(*) as count FROM content_planning WHERE UPPER(region) = '{reg}' GROUP BY status"
        return sql.strip(), None, "pie"

    # --- 3. TREEMAP: Market Value (Hierarchical) ---
    if "market value" in q:
        sql = f"SELECT vendor_name, deal_name, deal_value FROM deals WHERE UPPER(region) = '{reg}'"
        return sql.strip(), None, "treemap"

    # --- 4. VERTICAL BAR: Individual "Top" Items ---
    # Used for specific deal names where horizontal bars would have too-long labels
    if any(k in q for k in ["highest cost", "costliest", "top deals", "expensive"]):
        sql = f"""
            SELECT deal_name, deal_value 
            FROM deals 
            WHERE UPPER(region) = '{reg}' 
            ORDER BY deal_value DESC LIMIT 10
        """
        return sql.strip(), None, "bar_v"

    # --- 5. HORIZONTAL BAR: Professional Rankings ---
    # Preferred for Vendor and Performance comparisons
    if any(k in q for k in ["vendor", "spend", "cost", "performance", "top", "task", "order", "delay"]):
        if any(k in q for k in ["task", "order", "delay"]):
            status_filter = "AND UPPER(status) = 'DELAYED'" if "delay" in q else ""
            sql = f"""
                SELECT vendor_name, COUNT(*) as total_count 
                FROM work_orders 
                WHERE UPPER(region) = '{reg}' {status_filter}
                GROUP BY vendor_name ORDER BY total_count ASC
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

    # --- 6. FALLBACK ---
    return f"SELECT vendor_name, SUM(deal_value) as total_value FROM deals WHERE UPPER(region) = '{reg}' GROUP BY vendor_name ORDER BY total_value DESC LIMIT 10", None, "bar_h"

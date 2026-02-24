import re

def parse_query(question, region):
    """
    Translates Natural Language to SQL and identifies the best visualization.
    Integrates with the enriched WBD-style database schema.
    """
    q = question.lower()
    reg = region.upper()
    
    # --- 1. ENTITY EXTRACTION (Looking for specific metadata) ---
    # Check for content types
    c_type = None
    if "movie" in q: c_type = "Movie"
    elif "series" in q: c_type = "Original Series"
    elif "doc" in q: c_type = "Docuseries"
    elif "sport" in q: c_type = "Live Sport"

    # Check for resolution
    res_filter = None
    if "4k" in q or "ultra" in q: res_filter = "4K Dolby Vision"
    elif "hd" in q: res_filter = "HD"

    # Check for status
    status_filter = None
    if "delayed" in q: status_filter = "Delayed"
    elif "ready" in q or "pending" in q: status_filter = "Not Ready"
    elif "delivered" in q: status_filter = "Delivered"

    # --- 2. INTENT CLASSIFICATION & SQL GENERATION ---

    # LOGIC FOR DEALS (Bar Chart)
    if "deal" in q or "rights" in q:
        table = "deals"
        sql = f"SELECT * FROM {table} WHERE region = '{reg}'"
        
        if "svod" in q: sql += " AND rights_type = 'SVOD Exclusive'"
        if "linear" in q: sql += " AND rights_type = 'Linear Broadcast'"
        
        return sql + ";", None, "bar"

    # LOGIC FOR WORK ORDERS (Pie Chart)
    if "work order" in q or "wo-" in q or "order" in q:
        table = "work_orders"
        sql = f"SELECT * FROM {table} WHERE region = '{reg}'"
        
        if status_filter: sql += f" AND status = '{status_filter}'"
        if "subtitle" in q: sql += " AND asset_type = 'Subtitle File'"
        if "dub" in q: sql += " AND asset_type = 'Dubbing Audio'"
        
        return sql + ";", None, "pie"

    # LOGIC FOR CONTENT PLANNING (Pie Chart)
    if "content" in q or "program" in q or "show" in q or "max" in q:
        table = "content_planning"
        sql = f"SELECT * FROM {table} WHERE region = '{reg}'"
        
        if c_type: sql += f" AND content_type = '{c_type}'"
        if res_filter: sql += f" AND resolution = '{res_filter}'"
        if status_filter: sql += f" AND status = '{status_filter}'"
        
        # Keyword-based Title Search (e.g., "Show me House of the Dragon")
        match = re.search(r"(?:show me|about|for)\s+(['\"]?)(.*?)\1(?:$|\s+in)", q)
        if match:
            title_query = match.group(2).strip()
            # Basic validation to ensure the title isn't just a status word
            if title_query not in ["content", "status", "ready"]:
                sql += f" AND content_title LIKE '%{title_query}%'"

        return sql + ";", None, "pie"

    # 3. "I DON'T KNOW" HANDLER (As per Guiding Principles)
    return None, "I'm sorry, I couldn't translate that request. Could you please rephrase it or be more specific? Try asking about Content, Work Orders, or Deals.", None

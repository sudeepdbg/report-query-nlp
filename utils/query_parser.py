import re

def parse_query(question, region):
    q = question.lower()
    reg = region.upper()
    networks = ["MAX US", "MAX Europe", "MAX Australia", "MAX LatAm", "MAX Asia", "MAX Africa", "MAX India", "MAX UK"]

    if any(word in q for word in ["deal", "rights", "value"]):
        table = "deals"
        sql = f"SELECT * FROM {table} WHERE region = '{reg}'"
        return sql + ";", None, "bar"

    if any(word in q for word in ["work order", "wo-", "task", "asset"]):
        table = "work_orders"
        sql = f"SELECT * FROM {table} WHERE region = '{reg}'"
        return sql + ";", None, "pie"

    if any(word in q for word in ["content", "show", "status", "max"]):
        table = "content_planning"
        sql = f"SELECT * FROM {table} WHERE region = '{reg}'"
        
        for net in networks:
            if net.lower() in q:
                sql += f" AND network = '{net}'"
        
        match = re.search(r"(?:about|for|show)\s+(.*)", q)
        if match:
            potential_title = match.group(1).strip()
            if potential_title.upper() not in ["APAC", "EMEA", "NA", "LATAM", "STATUS"]:
                sql += f" AND content_title LIKE '%{potential_title.title()}%'"

        return sql + ";", None, "pie"

    return None, "Request not recognized. Try asking about Content, Work Orders, or Deals.", None

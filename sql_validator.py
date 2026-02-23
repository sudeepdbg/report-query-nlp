import sqlparse
from typing import Tuple

def validate_sql(sql: str) -> Tuple[bool, str]:
    """
    Validate that the SQL is safe and is a SELECT statement.
    Returns (is_valid, error_message).
    """
    if not sql:
        return False, "Empty SQL"

    # Parse the SQL
    try:
        parsed = sqlparse.parse(sql)
        if not parsed:
            return False, "Invalid SQL syntax"
        stmt = parsed[0]
    except Exception as e:
        return False, f"Parsing error: {str(e)}"

    # Check for forbidden keywords (case‑insensitive)
    forbidden = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "CREATE", "TRUNCATE", "REPLACE"]
    sql_upper = sql.upper()
    for word in forbidden:
        if word in sql_upper:
            return False, f"Forbidden operation: {word}"

    # Ensure it's a SELECT statement
    if stmt.get_type() != "SELECT":
        return False, f"Only SELECT queries are allowed, got {stmt.get_type()}"

    return True, None

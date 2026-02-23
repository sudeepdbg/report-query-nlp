import re
import sqlite3
import pandas as pd
from llm_sql import generate_sql
from sql_validator import validate_sql

def parse_query(question, region='NA', team='leadership', dashboard='executive'):
    """
    Convert natural language to SQL using LLM first, then fallback to rules.
    Returns (sql, error_message) – error_message is None if successful.
    """
    # Try LLM first
    sql, error = generate_sql(question, region, team, dashboard)
    if sql:
        # Validate the generated SQL
        is_valid, validation_error = validate_sql(sql)
        if is_valid:
            return sql, None
        else:
            print(f"LLM SQL validation failed: {validation_error}. Falling back to rules.")
    else:
        print(f"LLM failed: {error}. Falling back to rules.")

    # Fallback to rule-based parser
    q = question.lower().strip()
    
    # Helper to add region filter if applicable
    def add_region_filter(sql, table):
        if region and region != 'GLOBAL':
            if 'WHERE' in sql:
                sql += f" AND {table}.region = '{region}'"
            else:
                sql += f" WHERE {table}.region = '{region}'"
        return sql
    
    # Team hint (simplified)
    team_table_map = {
        'leadership': None,
        'product': 'work_orders',
        'content planning': 'content_planning',
        'deals': 'deals'
    }
    primary_table = team_table_map.get(team, None)
    
    # Count queries
    if re.search(r'how many|count', q):
        # ... (existing rule-based logic) ...
        # I'll keep it concise; you have the full version already.
        # For brevity, I'm omitting the full rule-based code here – use your existing one.
        # Ensure you return sql, None or None, error.
        pass
    
    # List queries
    if re.search(r'show|list|get|what', q):
        # ... (existing rule-based logic) ...
        pass
    
    return None, "I couldn't understand that query. Please try rephrasing."

def execute_sql(sql):
    """Execute SQL and return (DataFrame, error)."""
    conn = None
    try:
        conn = sqlite3.connect('vantage.db')
        df = pd.read_sql_query(sql, conn)
        return df, None
    except Exception as e:
        return None, str(e)
    finally:
        if conn:
            conn.close()

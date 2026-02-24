import re
import streamlit as st
from utils.llm_handler import call_llm

def parse_query(question, region='NA', team='leadership', dashboard='executive'):
    """
    Expert-level Text-to-SQL parser with built-in business logic grounding.
    """
    
    # Define the schema grounding for the model
    schema_context = """
    TABLES:
    - content_planning (network, content_title, status, planned_date, region)
    - work_orders (work_order, offering, status, due_date, region, vendor, priority)
    - deals (deal_name, vendor, deal_value, deal_date, region, status)

    ENUMERATIONS / ALLOWED VALUES:
    - content_planning.status: 'Scheduled', 'Fulfilled', 'Delivered', 'Not Ready'
    - work_orders.status: 'Delayed', 'In Progress', 'Completed', 'Pending Review'
    - deals.status: 'Active', 'Completed', 'Pending'

    BUSINESS RULES:
    - For 'AU' or 'Australia', always use network = 'MAX Australia'.
    - For 'US', 'USA', or 'United States', always use network = 'MAX US'.
    - Region is currently set to: '{region}'. Use this in WHERE clauses.
    """

    prompt = f"""You are a SQL generator for Foundry Vantage. Generate a valid SQL query based on the schema and rules provided.
    
    {schema_context}

    RULES:
    1. Output ONLY the SQL string. 
    2. Do not explain the query.
    3. Always filter by region if the column exists: region = '{region}'.
    4. If the question is ambiguous, prioritize the table most relevant to the '{team}' team.

    FEW-SHOT EXAMPLES:
    Question: "How many items are not ready for AU?"
    SQL: SELECT COUNT(*) FROM content_planning WHERE status = 'Not Ready' AND network = 'MAX Australia' AND region = '{region}';

    Question: "List all delayed orders"
    SQL: SELECT * FROM work_orders WHERE status = 'Delayed' AND region = '{region}';

    NOW GENERATE:
    Question: "{question}"
    SQL:"""

    # 1. Attempt Algorithmic Generation via LLM
    llm_sql = call_llm(prompt)
    
    if llm_sql and llm_sql.upper().startswith('SELECT'):
        # Safety: Block destructive commands
        forbidden = ['DROP', 'DELETE', 'TRUNCATE', 'UPDATE', 'INSERT']
        if any(cmd in llm_sql.upper() for cmd in forbidden):
            return None, "Security Alert: Destructive SQL detected. Query blocked."
        return llm_sql, None

    # 2. Automated Fallback to Rule-Based Logic
    st.info("🔄 LLM provided an invalid response. Using internal fallback logic...")
    return rule_based_parse(question, region, team, dashboard)

def rule_based_parse(question, region, team, dashboard):
    """
    Robust regex fallback for common patterns.
    """
    q = question.lower().strip()
    
    # Simplified region filter helper
    def wrap_sql(table, condition):
        return f"SELECT * FROM {table} WHERE {condition} AND region = '{region}';"

    # Match common patterns
    if "delayed" in q or "late" in q:
        return wrap_sql("work_orders", "status = 'Delayed'"), None
    
    if "not ready" in q:
        return wrap_sql("content_planning", "status = 'Not Ready'"), None

    if "active deal" in q:
        return wrap_sql("deals", "status = 'Active'"), None

    if "australia" in q or "au" in q:
        return f"SELECT * FROM content_planning WHERE network = 'MAX Australia' AND region = '{region}';", None

    return None, "I'm sorry, I couldn't translate that request. Could you please rephrase it or be more specific?"

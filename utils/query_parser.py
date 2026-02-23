import re
import streamlit as st
from utils.llm_handler import call_llm

def parse_query(question, region='NA', team='leadership', dashboard='executive'):
    """
    Convert natural language to SQL using LLM first, then rule‑based fallback.
    Returns (sql, error_message).
    """
    # Build prompt with schema and filters
    schema = """
Tables:
- content_planning (network, content_title, status, planned_date, region)
- work_orders (work_order, offering, status, due_date, region, vendor, priority)
- deals (deal_name, vendor, deal_value, deal_date, region, status)

Column descriptions:
- content_planning.status: Scheduled, Fulfilled, Delivered, Not Ready
- work_orders.status: Delayed, In Progress, Completed, Pending Review
- deals.status: Active, Completed, Pending
"""

    prompt = f"""You are an expert SQL assistant. Given the user's question and the database schema, generate a SQL query.

{schema}

Rules:
- Use only the tables and columns above.
- Always add filters for the user's region: region = '{region}' (if the table has a region column).
- If the question mentions a specific team or context, you can add relevant filters.
- Output ONLY the SQL query, no extra text, no markdown.

Examples:
Question: Show me all delayed work orders
SQL: SELECT * FROM work_orders WHERE status = 'Delayed' AND region = '{region}';

Question: How many content items for MAX Australia?
SQL: SELECT COUNT(*) FROM content_planning WHERE network = 'MAX Australia' AND region = '{region}';

Now answer:
Question: {question}
SQL:"""

    # Try LLM
    llm_sql = call_llm(prompt)
    if llm_sql:
        # Clean up possible markdown
        llm_sql = re.sub(r'^```sql\n|```$', '', llm_sql, flags=re.IGNORECASE).strip()
        # Basic validation: must start with SELECT
        if llm_sql.upper().startswith('SELECT'):
            return llm_sql, None
        else:
            st.warning(f"LLM returned non‑SQL, falling back to rules: {llm_sql}")
    else:
        st.info("LLM unavailable – using rule‑based parser.")

    # Fallback to rule‑based parser
    return rule_based_parse(question, region, team, dashboard)

def rule_based_parse(question, region='NA', team='leadership', dashboard='executive'):
    """Original rule‑based parser (expanded with synonyms)."""
    q = question.lower().strip()

    def add_region_filter(sql, table):
        if region and region != 'GLOBAL':
            if 'WHERE' in sql:
                sql += f" AND {table}.region = '{region}'"
            else:
                sql += f" WHERE {table}.region = '{region}'"
        return sql

    team_table_map = {
        'leadership': None,
        'product': 'work_orders',
        'content planning': 'content_planning',
        'deals': 'deals'
    }
    primary_table = team_table_map.get(team, None)

    # --- Count queries ---
    if re.search(r'how many|count|total number of', q):
        if re.search(r'delayed|at risk|late|behind schedule', q):
            table = 'work_orders'
            condition = "status = 'Delayed'"
        elif re.search(r'max australia|australia|max au', q):
            table = 'content_planning'
            condition = "network = 'MAX Australia'"
        elif re.search(r'max us|united states|usa', q):
            table = 'content_planning'
            condition = "network = 'MAX US'"
        elif re.search(r'active deals|active', q) and 'deal' in q:
            table = 'deals'
            condition = "status = 'Active'"
        elif re.search(r'pending approval|pending deals|pending', q):
            table = 'deals'
            condition = "status = 'Pending'"
        elif re.search(r'in progress|ongoing|current', q):
            table = 'work_orders'
            condition = "status = 'In Progress'"
        elif re.search(r'not ready|unfinished|pending', q):
            table = 'content_planning'
            condition = "status = 'Not Ready'"
        else:
            if primary_table:
                table = primary_table
                condition = "1=1"
            else:
                return None, "I couldn't understand what to count."

        sql = f"SELECT COUNT(*) FROM {table} WHERE {condition}"
        sql = add_region_filter(sql, table)
        return sql, None

    # --- List queries ---
    if re.search(r'show|list|get|what|find|display', q):
        # Special case: top vendors
        if re.search(r'vendor a|top vendors|vendor performance', q):
            sql = "SELECT vendor, COUNT(*) as count FROM work_orders"
            if region and region != 'GLOBAL':
                sql += f" WHERE region = '{region}'"
            sql += " GROUP BY vendor ORDER BY count DESC"
            return sql, None

        if re.search(r'delayed|at risk|late|behind schedule', q):
            table = 'work_orders'
            condition = "status = 'Delayed'"
        elif re.search(r'max australia|australia|max au', q):
            table = 'content_planning'
            condition = "network = 'MAX Australia'"
        elif re.search(r'max us|united states|usa', q):
            table = 'content_planning'
            condition = "network = 'MAX US'"
        elif re.search(r'active deals|active', q) and 'deal' in q:
            table = 'deals'
            condition = "status = 'Active'"
        elif re.search(r'pending approval|pending deals|pending', q):
            table = 'deals'
            condition = "status = 'Pending'"
        elif re.search(r'in progress|ongoing|current', q):
            table = 'work_orders'
            condition = "status = 'In Progress'"
        elif re.search(r'not ready|unfinished|pending', q):
            table = 'content_planning'
            condition = "status = 'Not Ready'"
        else:
            if primary_table:
                table = primary_table
                condition = "1=1"
            else:
                suggestions = []
                if dashboard == 'executive' or not primary_table:
                    suggestions.append('Show me all content for MAX Australia')
                    suggestions.append('How many content items are Not Ready?')
                if dashboard == 'workorders' or not primary_table:
                    suggestions.append('Show me all delayed work orders')
                    suggestions.append('List work orders in progress')
                if dashboard == 'deals' or not primary_table:
                    suggestions.append('Show me all active deals')
                    suggestions.append('List top vendors by work orders')
                suggestion_text = " You could try: " + ", ".join(f'"{s}"' for s in suggestions[:4])
                return None, f"I couldn't understand that query. Please try rephrasing.{suggestion_text}"

        sql = f"SELECT * FROM {table} WHERE {condition}"
        sql = add_region_filter(sql, table)
        return sql, None

    # --- No match ---
    suggestions = [
        "Show me all delayed work orders",
        "How many content items for MAX Australia?",
        "List active deals",
        "Show top vendors by work orders"
    ]
    suggestion_text = " You could try: " + ", ".join(f'"{s}"' for s in suggestions)
    return None, f"I couldn't understand that query. Please try rephrasing.{suggestion_text}"

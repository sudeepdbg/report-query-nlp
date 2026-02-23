import re
from utils.llm_handler import call_llm
from utils.schema_index import retrieve_relevant_schema

def parse_query(question, region='NA', team='leadership', dashboard='executive'):
    """
    Convert natural language to SQL using LLM first, then rule-based fallback.
    Returns (sql, error_message).
    """
    # Build prompt with schema, filters, and examples
    schema_info = retrieve_relevant_schema(question)
    schema_text = "\n".join(schema_info)
    
    prompt = f"""You are an expert SQL assistant for a media supply chain database. 
Given the user's question and the database schema below, generate a SQL query.

Database schema:
{schema_text}

Rules:
- Use only the tables and columns listed above.
- Always add filters for the user's region '{region}' and team '{team}' when applicable (e.g., region = '{region}').
- If the question involves time, use appropriate SQL functions.
- Output only the SQL query, no extra text, no markdown.

Examples:
Question: Show me all delayed work orders
SQL: SELECT * FROM work_orders WHERE status = 'Delayed';

Question: How many content items for MAX Australia?
SQL: SELECT COUNT(*) FROM content_planning WHERE network = 'MAX Australia';

Now answer:
Question: {question}
SQL:"""

    # Try LLM
    llm_sql = call_llm(prompt)
    if llm_sql:
        # Clean up any extra text
        llm_sql = re.sub(r'^```sql\n|```$', '', llm_sql, flags=re.IGNORECASE).strip()
        # Validate (basic) – ensure it's a SELECT
        if llm_sql.upper().startswith('SELECT'):
            return llm_sql, None
    
    # Fallback to rule‑based
    return rule_based_parse(question, region, team, dashboard)

def rule_based_parse(question, region, team, dashboard):
    # (Copy your existing rule‑based code here)
    # ... (I'll keep it short, but include your full logic)
    q = question.lower().strip()
    # ... your existing implementation
    # At the end, return (sql, None) or (None, error)
    return None, "I couldn't understand that query."

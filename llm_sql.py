import requests
import json
import re
import os
from typing import Optional, Tuple

# Hugging Face API configuration (free tier)
HF_API_TOKEN = os.environ.get("HF_API_TOKEN", "")  # Set this in Render environment
MODEL_ID = "mistralai/Mistral-7B-Instruct-v0.2"  # Free model, no rate limits for low usage

def generate_sql(question: str, region: str, team: str, dashboard: str, context: str = "") -> Tuple[Optional[str], Optional[str]]:
    """
    Call Hugging Face Inference API to convert natural language to SQL.
    Returns (sql, error_message).
    """
    # Build few‑shot examples dynamically based on the active dashboard?
    # For simplicity, we include generic examples.
    examples = """
Examples:
Question: "Show me all delayed work orders for the NA region."
SQL: SELECT * FROM work_orders WHERE status = 'Delayed' AND region = 'NA';

Question: "How many content items are planned for MAX Australia?"
SQL: SELECT COUNT(*) FROM content_planning WHERE network = 'MAX Australia';

Question: "What is the total deal value by vendor for active deals?"
SQL: SELECT vendor, SUM(deal_value) as total FROM deals WHERE status = 'Active' GROUP BY vendor;

Question: "Show me the top 5 vendors by number of work orders."
SQL: SELECT vendor, COUNT(*) as count FROM work_orders GROUP BY vendor ORDER BY count DESC LIMIT 5;
"""

    # Schema description
    schema = """
Tables:
- content_planning (network, content_title, status, planned_date, region)
- work_orders (work_order, offering, status, due_date, region, vendor, priority)
- deals (deal_name, vendor, deal_value, deal_date, region, status)
"""

    # Build the prompt with rules
    prompt = f"""{schema}

{examples}

Rules:
- Use only the tables and columns listed.
- Always add filters for the user's region ('{region}') and team ('{team}') when applicable. Team may map to a specific table or condition, but region should be added as a WHERE clause.
- If the question involves time, use appropriate SQL functions (e.g., date('now'), strftime).
- Output only the SQL query, no extra text, no markdown.

Question: {question}
SQL:"""

    # Call Hugging Face API
    headers = {"Authorization": f"Bearer {HF_API_TOKEN}"}
    payload = {
        "inputs": prompt,
        "parameters": {
            "max_new_tokens": 200,
            "temperature": 0.2,
            "return_full_text": False
        }
    }

    try:
        response = requests.post(
            f"https://api-inference.huggingface.co/models/{MODEL_ID}",
            headers=headers,
            json=payload,
            timeout=30
        )
        if response.status_code == 200:
            result = response.json()
            if isinstance(result, list) and len(result) > 0:
                generated = result[0].get("generated_text", "").strip()
            else:
                generated = result.get("generated_text", "").strip()
            # Sometimes the model returns extra text, try to extract SQL
            sql_match = re.search(r"(SELECT|select|WITH|with).*?(;|$)", generated, re.DOTALL | re.IGNORECASE)
            if sql_match:
                sql = sql_match.group(0).strip()
                return sql, None
            else:
                # If no SQL found, return the whole thing as error
                return None, f"Model did not generate valid SQL: {generated[:200]}"
        else:
            return None, f"Hugging Face API error {response.status_code}: {response.text}"
    except Exception as e:
        return None, f"LLM call failed: {str(e)}"

import requests
import streamlit as st
import re

def call_llm(prompt: str, model: str = "google/gemma-2-9b-it") -> str:
    """
    Enhanced LLM handler for Hugging Face Inference API.
    Uses Gemma-2-9b for better logic and includes robust SQL cleaning.
    """
    hf_token = st.secrets.get("HF_TOKEN")
    if not hf_token:
        st.error("❌ HF_TOKEN not found in secrets.")
        return ""

    API_URL = f"https://api-inference.huggingface.co/models/{model}"
    headers = {"Authorization": f"Bearer {hf_token}"}

    # Gemma chat template
    formatted_prompt = f"<start_of_turn>user\n{prompt}<end_of_turn>\n<start_of_turn>model\n"

    payload = {
        "inputs": formatted_prompt,
        "parameters": {
            "max_new_tokens": 300,
            "temperature": 0.01, # Keep it deterministic for SQL
            "return_full_text": False
        }
    }

    try:
        response = requests.post(API_URL, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            generated = result[0].get("generated_text", "").strip() if isinstance(result, list) else result.get("generated_text", "").strip()
            
            # CRITICAL: Strip markdown code blocks and conversational fluff
            # This regex looks for the first SELECT and captures until the semicolon
            sql_match = re.search(r"(SELECT.*?;?)", generated, re.DOTALL | re.IGNORECASE)
            if sql_match:
                clean_sql = sql_match.group(1).replace("```sql", "").replace("```", "").strip()
                return clean_sql
            return generated
        else:
            return ""
    except Exception as e:
        print(f"LLM Connection Error: {e}")
        return ""

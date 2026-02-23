import requests
import json
import streamlit as st

def call_openrouter_llm(prompt: str, model: str = "mistralai/mistral-small-3.1-24b-instruct:free") -> str:
    """
    Call OpenRouter's free Mistral endpoint.
    """
    api_key = st.secrets.get("OPENROUTER_API_KEY")
    if not api_key:
        st.warning("⚠️ OPENROUTER_API_KEY not found. LLM features disabled.")
        return ""
    
    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    data = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.2,
        "max_tokens": 400
    }
    
    try:
        response = requests.post(url, headers=headers, json=data, timeout=30)
        if response.status_code == 200:
            result = response.json()
            return result["choices"][0]["message"]["content"]
        else:
            st.warning(f"OpenRouter API error {response.status_code}")
            return ""
    except Exception as e:
        st.warning(f"OpenRouter call failed: {e}")
        return ""

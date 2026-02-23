import requests
import streamlit as st
import re

def call_llm(prompt: str, model: str = "mistralai/mistral-small-3.1-24b-instruct:free") -> str:
    """Call OpenRouter's free LLM API."""
    api_key = st.secrets.get("OPENROUTER_API_KEY")
    if not api_key:
        st.warning("⚠️ OPENROUTER_API_KEY not found. LLM disabled.")
        return ""
    
    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://your-app.streamlit.app",  # optional
        "X-Title": "Foundry Vantage"
    }
    
    data = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "max_tokens": 500
    }
    
    try:
        response = requests.post(url, headers=headers, json=data, timeout=30)
        if response.status_code == 200:
            result = response.json()
            return result["choices"][0]["message"]["content"].strip()
        else:
            st.warning(f"OpenRouter error {response.status_code}")
            return ""
    except Exception as e:
        st.warning(f"LLM call failed: {e}")
        return ""
import requests
import streamlit as st
import json

def call_llm(prompt: str, model: str = "mistralai/mistral-small-3.1-24b-instruct:free") -> str:
    """
    Call OpenRouter's free LLM API to generate SQL.
    """
    api_key = st.secrets.get("OPENROUTER_API_KEY")
    if not api_key:
        st.warning("⚠️ OPENROUTER_API_KEY not found. LLM features disabled.")
        return ""
    
    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://your-app.streamlit.app",  # optional
        "X-Title": "Foundry Vantage"
    }
    
    data = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "max_tokens": 400
    }
    
    try:
        response = requests.post(url, headers=headers, json=data, timeout=30)
        if response.status_code == 200:
            result = response.json()
            return result["choices"][0]["message"]["content"].strip()
        else:
            st.warning(f"OpenRouter API error {response.status_code}")
            return ""
    except Exception as e:
        st.warning(f"LLM call failed: {e}")
        return ""

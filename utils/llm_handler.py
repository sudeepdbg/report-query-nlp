import requests
import streamlit as st
import time

def call_llm(prompt: str, model: str = "meta-llama/llama-3-8b-instruct:free", retries: int = 2) -> str:
    """
    Call OpenRouter's free LLM API with retry logic and fallback models.
    """
    api_key = st.secrets.get("OPENROUTER_API_KEY")
    if not api_key:
        st.error("❌ OPENROUTER_API_KEY not found in secrets. Please add it.")
        return ""

    # Updated list of reliable free models (as of Feb 2026)
    fallback_models = [
        "meta-llama/llama-3-8b-instruct:free",          # Llama 3 8B
        "mistralai/mistral-7b-instruct:free",           # Mistral 7B
        "google/gemma-2-2b-it:free",                    # Gemma 2 2B (faster)
        "nousresearch/hermes-2-pro-llama-3-8b:free",    # Hermes 2 Pro
    ]
    # Ensure primary model is included
    if model not in fallback_models:
        fallback_models.insert(0, model)

    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://report-query-nlp-msc.streamlit.app",  # Replace with your app URL
        "X-Title": "Foundry Vantage"
    }

    for attempt in range(retries + 1):
        for current_model in fallback_models:
            try:
                data = {
                    "model": current_model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.1,
                    "max_tokens": 500
                }

                print(f"DEBUG: Trying model: {current_model}")
                response = requests.post(url, headers=headers, json=data, timeout=30)

                if response.status_code == 200:
                    result = response.json()
                    content = result["choices"][0]["message"]["content"].strip()
                    print(f"DEBUG: Success with model {current_model}")
                    return content
                elif response.status_code == 429:
                    error_info = response.json()
                    st.warning(f"Model {current_model} rate‑limited. Trying next...")
                    print(f"DEBUG: Model {current_model} rate‑limited: {error_info}")
                    continue
                else:
                    st.warning(f"OpenRouter error {response.status_code} with {current_model}")
                    print(f"DEBUG: OpenRouter error: {response.text}")
                    continue

            except requests.exceptions.Timeout:
                st.warning(f"Request to {current_model} timed out.")
                continue
            except Exception as e:
                st.warning(f"LLM call failed with {current_model}: {str(e)}")
                print(f"DEBUG: Exception: {str(e)}")
                continue

        # After all models, if still failing, wait and retry
        if attempt < retries:
            wait_time = 2 ** attempt
            print(f"DEBUG: All models failed. Retry {attempt+1}/{retries} after {wait_time}s")
            time.sleep(wait_time)
        else:
            break

    st.error("❌ All LLM models failed after retries. Falling back to rule‑based parser.")
    return ""

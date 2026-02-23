import requests
import streamlit as st
import time

def call_llm(prompt: str, model: str = "google/gemma-2-9b-it:free", retries: int = 2) -> str:
    """
    Call OpenRouter's free LLM API with retry logic.
    If the primary model is rate-limited, falls back to an alternative model.
    """
    api_key = st.secrets.get("OPENROUTER_API_KEY")
    if not api_key:
        st.error("❌ OPENROUTER_API_KEY not found in secrets. Please add it.")
        return ""

    # Alternative models to try in case of rate limiting
    fallback_models = [
        "mistralai/mistral-7b-instruct:free",
        "nousresearch/hermes-3-llama-3.1-405b:free",
        "google/gemma-2-9b-it:free"
    ]
    # Ensure the primary model is in the list
    if model not in fallback_models:
        fallback_models.insert(0, model)
    else:
        # Move primary model to front if already in list
        fallback_models.remove(model)
        fallback_models.insert(0, model)

    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://your-app.streamlit.app",  # Replace with your actual app URL
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
                    # Rate limited – try next model
                    error_info = response.json()
                    st.warning(f"Model {current_model} rate-limited. Trying next model...")
                    print(f"DEBUG: Model {current_model} rate-limited: {error_info}")
                    continue
                else:
                    # Other error – log and continue
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

        # After trying all models, if we still have retries left, wait and retry
        if attempt < retries:
            wait_time = 2 ** attempt  # exponential backoff
            print(f"DEBUG: All models failed. Retry {attempt+1}/{retries} after {wait_time}s")
            time.sleep(wait_time)
        else:
            break

    st.error("❌ All LLM models failed after retries. Please try again later.")
    return ""

import requests
import streamlit as st

def call_llm(prompt: str, model: str = "google/gemma-2-2b-it") -> str:
    """
    Call Google's Gemma model via Hugging Face's free Inference API.
    """
    hf_token = st.secrets.get("HF_TOKEN")
    if not hf_token:
        st.error("❌ HF_TOKEN not found in secrets. Please add your Hugging Face token.")
        return ""

    API_URL = f"https://api-inference.huggingface.co/models/{model}"
    headers = {"Authorization": f"Bearer {hf_token}"}

    # Gemma 2 expects a specific chat template
    formatted_prompt = f"""<start_of_turn>user
{prompt}<end_of_turn>
<start_of_turn>model
"""

    payload = {
        "inputs": formatted_prompt,
        "parameters": {
            "max_new_tokens": 500,
            "temperature": 0.1,
            "return_full_text": False
        }
    }

    try:
        response = requests.post(API_URL, headers=headers, json=payload, timeout=30)
        if response.status_code == 200:
            result = response.json()
            if isinstance(result, list):
                return result[0].get("generated_text", "").strip()
            return result.get("generated_text", "").strip()
        else:
            st.warning(f"Hugging Face API error {response.status_code}: {response.text}")
            return ""
    except Exception as e:
        st.warning(f"Hugging Face request failed: {e}")
        return ""

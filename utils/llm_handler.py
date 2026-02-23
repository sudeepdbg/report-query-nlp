import requests
import streamlit as st

def call_llm(prompt: str, model: str = "google/gemma-2-2b-it") -> str:
    """
    Call Google's Gemma model via Hugging Face's free Inference API.
    Prints debug info to the Streamlit logs.
    """
    hf_token = st.secrets.get("HF_TOKEN")
    if not hf_token:
        print("ERROR: HF_TOKEN not found in secrets.")
        st.error("❌ HF_TOKEN not found in secrets. Please add your Hugging Face token.")
        return ""

    print(f"DEBUG: HF_TOKEN found (length {len(hf_token)}). Attempting API call to {model}...")

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
        print("DEBUG: Sending request to Hugging Face...")
        response = requests.post(API_URL, headers=headers, json=payload, timeout=30)
        print(f"DEBUG: Response status code: {response.status_code}")

        if response.status_code == 200:
            result = response.json()
            if isinstance(result, list):
                generated = result[0].get("generated_text", "").strip()
            else:
                generated = result.get("generated_text", "").strip()
            print(f"DEBUG: Generated text (first 100 chars): {generated[:100]}")
            return generated
        else:
            error_text = response.text
            print(f"ERROR: Hugging Face API error {response.status_code}: {error_text}")
            st.warning(f"Hugging Face API error {response.status_code}. Check logs.")
            return ""
    except Exception as e:
        print(f"EXCEPTION: Hugging Face request failed: {e}")
        st.warning(f"Hugging Face request failed: {e}")
        return ""

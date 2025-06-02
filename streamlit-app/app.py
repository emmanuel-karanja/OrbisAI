import streamlit as st
import requests
import base64

st.title("OrbisAI Document Uploader with Summarization")

uploaded_file = st.file_uploader("Choose a file", type=["txt", "md", "pdf"])

if uploaded_file:
    file_bytes = uploaded_file.read()
    encoded = base64.b64encode(file_bytes).decode()

    with st.spinner("Processing and storing..."):
        response = requests.post(
            "http://ingestion:8001/ingest",
            json={"filename": uploaded_file.name, "content": encoded},
            timeout=60
        )
        if response.status_code == 200:
            result = response.json()
            st.success("Document successfully embedded and stored.")
            st.markdown(f"### Summary:\n{result.get('summary', '')}")
        else:
            st.error(f"Failed: {response.text}")

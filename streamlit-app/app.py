import streamlit as st
import requests
import base64

st.set_page_config(page_title="OrbisAI RAG System", layout="centered")
st.title("📄 OrbisAI Document Uploader + Query Interface")

st.header("📤 Upload Document")

uploaded_file = st.file_uploader("Choose a file", type=["txt", "md", "pdf"])

if uploaded_file:
    file_bytes = uploaded_file.read()
    encoded = base64.b64encode(file_bytes).decode()

    with st.spinner("Processing and storing..."):
        try:
            response = requests.post(
                "http://ingestion:8001/ingest",
                json={"filename": uploaded_file.name, "content": encoded},
                timeout=60
            )
            if response.status_code == 200:
                result = response.json()
                st.success("✅ Document successfully embedded and stored.")
                st.markdown(f"### ✨ Summary:\n{result.get('summary', '')}")
            else:
                st.error(f"❌ Failed: {response.text}")
        except Exception as e:
            st.error(f"❌ Request failed: {e}")

st.divider()

st.header("🔍 Query Your Documents")

query_text = st.text_input("Type your question about any uploaded document:")

if st.button("Search") and query_text:
    with st.spinner("Searching..."):
        try:
            qres = requests.post(
                "http://ingestion:8001/query",
                json={"query": query_text},
                timeout=30
            )
            if qres.status_code == 200:
                results = qres.json()
                chunks = results.get("matches", [])
                metadata = results.get("metadata", [])

                if chunks:
                    st.success("Found relevant content:")
                    for i, chunk in enumerate(chunks):
                        meta = metadata[i]
                        st.markdown(f"**📄 Page {meta.get('page')} | Paragraph {meta.get('paragraph')}**")
                        st.write(chunk)
                        st.markdown("---")
                else:
                    st.warning("No relevant content found.")
            else:
                st.error(f"Query failed: {qres.text}")
        except Exception as e:
            st.error(f"❌ Request failed: {e}")

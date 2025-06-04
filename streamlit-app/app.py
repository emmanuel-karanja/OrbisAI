import streamlit as st
import requests
import base64
import time
from logger import setup_logger

logger = setup_logger("streamlit-app")

st.set_page_config(page_title="OrbisAI RAG System", layout="centered")
st.title("📄 OrbisAI Document Uploader + Query Interface")

st.header("📤 Upload Document")

uploaded_file = st.file_uploader("Choose a file", type=["txt", "md", "pdf"])

if uploaded_file:
    file_bytes = uploaded_file.read()
    encoded = base64.b64encode(file_bytes).decode()
    logger.info(f"Uploading file: {uploaded_file.name}, size: {len(file_bytes)} bytes")

    with st.spinner("Processing and storing..."):
        try:
            response = requests.post(
                "http://ingestion:8001/ingest",
                json={"filename": uploaded_file.name, "content": encoded},
                timeout=60
            )
            if response.status_code == 200:
                result = response.json()
                logger.info(f"Ingestion started for {uploaded_file.name}")

                time.sleep(2)  # Wait briefly for async ingestion

                status_res = requests.get(
                    f"http://ingestion:8001/ingest-status/{uploaded_file.name}",
                    timeout=10
                )

                if status_res.status_code == 200:
                    msg = status_res.json().get("message", "Ingestion complete.")
                    logger.info(f"Ingest status: {msg}")
                    st.success(f"✅ {msg}")
                else:
                    st.warning("⚠️ Ingestion status not available yet.")

                if "summary" in result:
                    st.markdown(f"### ✨ Summary:\n{result.get('summary', '')}")
            else:
                logger.error(f"Ingestion failed for {uploaded_file.name}: {response.text}")
                st.error(f"❌ Failed: {response.text}")
        except Exception as e:
            logger.exception(f"Exception during ingestion for {uploaded_file.name}: {e}")
            st.error(f"❌ Request failed: {e}")

st.divider()

st.header("🔍 Query Your Documents")

query_text = st.text_input("Type your question about any uploaded document:")

if st.button("Search") and query_text:
    logger.info(f"Query submitted: {query_text}")
    with st.spinner("Searching..."):
        try:
            qres = requests.post(
                "http://ingestion:8001/query",
                json={"question": query_text},
                timeout=30
            )
            if qres.status_code == 200:
                results = qres.json()

                # Display the answer
                if "answer" in results:
                    st.markdown("### 💡 Answer")
                    st.success(results["answer"])

                ranked_matches = results.get("ranked_matches", [])

                if ranked_matches:
                    st.success("✅ Top Ranked Matches:")
                    logger.info(f"{len(ranked_matches)} ranked matches returned for query.")
                    for match in ranked_matches:
                        meta = match.get("metadata", {})
                        st.markdown(
                            f"**📄 {meta.get('doc_name', 'Unknown')} — Page {meta.get('page', '?')} | Paragraph {meta.get('paragraph', '?')}**"
                        )
                        st.markdown(f"**🔢 Similarity Score:** `{match.get('similarity', '?')}`")
                        st.write(match["text"])
                        st.markdown("---")
                else:
                    logger.warning("No content matched the query.")
                    st.warning("No relevant ranked content found.")
            else:
                logger.error(f"Query failed: {qres.text}")
                st.error(f"❌ Query failed: {qres.text}")
        except Exception as e:
            logger.exception(f"Query request failed: {e}")
            st.error(f"❌ Request failed: {e}")

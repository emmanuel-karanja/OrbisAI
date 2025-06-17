import streamlit as st
import requests
import base64
import time
import sqlite3
from logger import setup_logger

# ───────────────────────────────
# 🧠 SQLite Helpers
# ───────────────────────────────
DB_PATH = "query_history.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

def save_query_to_db(query):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("INSERT INTO history (query) VALUES (?)", (query,))
    conn.commit()
    conn.close()

def load_query_history_from_db(limit=10):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT id, query, timestamp FROM history ORDER BY timestamp DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows

def delete_query_from_db(query_id):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM history WHERE id = ?", (query_id,))
    conn.commit()
    conn.close()

# ───────────────────────────────
# 🚀 Streamlit App Start
# ───────────────────────────────
logger = setup_logger("streamlit-app")
init_db()

st.set_page_config(page_title="OrbisAI RAG System", layout="centered")
st.title("📄 OrbisAI Cicero")

# ───────────────────────────────
# 📤 Sidebar: Upload Document + Ingestion Status
# ───────────────────────────────
st.sidebar.header("📤 Upload Document")
uploaded_file = st.sidebar.file_uploader("Choose a file", type=["txt", "md", "pdf"])

if uploaded_file:
    file_bytes = uploaded_file.read()
    encoded = base64.b64encode(file_bytes).decode()
    logger.info(f"Uploading file: {uploaded_file.name}, size: {len(file_bytes)} bytes")

    with st.sidebar.spinner("📡 Uploading and processing..."):
        try:
            response = requests.post(
                "http://ingestion:8001/ingest",
                json={"filename": uploaded_file.name, "content": encoded},
                timeout=60
            )

            if response.status_code == 200:
                result = response.json()
                logger.info(f"Ingestion started for {uploaded_file.name}")

                time.sleep(2)

                status_res = requests.get(
                    f"http://ingestion:8001/ingest-status/{uploaded_file.name}",
                    timeout=10
                )

                if status_res.status_code == 200:
                    msg = status_res.json().get("message", "Ingestion complete.")
                    logger.info(f"Ingestion status: {msg}")
                    st.sidebar.success(f"✅ {msg}")
                else:
                    st.sidebar.warning("⚠️ Ingestion status not available yet.")

                if "summary" in result:
                    st.markdown(f"### ✨ Summary:\n{result.get('summary', '')}")

            else:
                logger.error(f"Ingestion failed: {response.text}")
                st.sidebar.error(f"❌ Failed: {response.text}")
        except Exception as e:
            logger.exception(f"Exception during ingestion: {e}")
            st.sidebar.error(f"❌ Request failed: {e}")

# ───────────────────────────────
# 📚 Sidebar: List All Uploaded Books (Paginated)
# ───────────────────────────────
st.sidebar.header("📚 Uploaded Books")

all_documents = []
try:
    doc_res = requests.get("http://ingestion:8001/list-documents", timeout=10)
    if doc_res.status_code == 200:
        doc_list = sorted(doc_res.json().get("documents", []))
        all_documents = doc_list
        docs_per_page = 10
        total_docs = len(doc_list)
        total_pages = (total_docs - 1) // docs_per_page + 1

        if "doc_page" not in st.session_state:
            st.session_state.doc_page = 0

        col1, col2 = st.sidebar.columns([1, 1])
        with col1:
            if st.button("⬅️ Prev", key="prev") and st.session_state.doc_page > 0:
                st.session_state.doc_page -= 1
        with col2:
            if st.button("Next ➡️", key="next") and st.session_state.doc_page < total_pages - 1:
                st.session_state.doc_page += 1

        start = st.session_state.doc_page * docs_per_page
        end = start + docs_per_page
        paged_docs = doc_list[start:end]

        st.sidebar.markdown(f"📄 Showing {start+1}-{min(end, total_docs)} of {total_docs}")

        for doc in paged_docs:
            st.sidebar.markdown(f"- `{doc}`")

    else:
        st.sidebar.warning("⚠️ Failed to retrieve documents.")
except Exception as e:
    st.sidebar.error(f"Error fetching documents: {e}")

# ───────────────────────────────
# 🔍 Query Interface + History
# ───────────────────────────────
st.divider()
st.header("🔍 Query Your Documents")

# For pre-filling input if history is clicked
if "selected_query" not in st.session_state:
    st.session_state.selected_query = ""

query_text = st.text_input(
    "Type your question about any uploaded document:",
    value=st.session_state.selected_query
)

if st.button("Search") and query_text:
    logger.info(f"Query submitted: {query_text}")
    save_query_to_db(query_text)
    with st.spinner("Searching..."):
        try:
            payload = {"question": query_text}
            qres = requests.post(
                "http://ingestion:8001/query",
                json=payload,
                timeout=30
            )

            if qres.status_code == 200:
                results = qres.json()

                if "answer" in results:
                    st.markdown("### 💡 Answer")
                    st.success(results["answer"])

                ranked_matches = results.get("ranked_matches", [])

                if ranked_matches:
                    st.success("✅ Top Ranked Matches:")
                    logger.info(f"{len(ranked_matches)} ranked matches returned for query.")
                    for match in ranked_matches:
                        meta = match.get("metadata", {})
                        doc_name = meta.get('doc_name', 'Unknown')
                        page = meta.get('page', '?')
                        para = meta.get('paragraph', '?')
                        sim = match.get('similarity', '?')

                        st.markdown(f"**📄 <span style='color:#2271b1;font-weight:bold'>{doc_name}</span> — Page {page}, Paragraph {para}**", unsafe_allow_html=True)
                        st.markdown(f"**🔢 Similarity Score:** `{sim}`")
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

# ───────────────────────────────
# 🕘 Query History Section (Clickable + Deletable)
# ───────────────────────────────
query_history = load_query_history_from_db()

if query_history:
    with st.expander("🕘 Query History (Last 10)"):
        for query_id, q, ts in query_history:
            col1, col2, col3 = st.columns([0.7, 0.2, 0.1])
            with col1:
                if st.button(f"{q}", key=f"requery_{query_id}"):
                    st.session_state.selected_query = q
                    st.experimental_rerun()
            with col2:
                st.markdown(f"<span style='font-size: 0.8em; color: gray'>🕓 {ts}</span>", unsafe_allow_html=True)
            with col3:
                if st.button("❌", key=f"delete_{query_id}"):
                    delete_query_from_db(query_id)
                    st.success(f"Deleted: '{q}'")
                    st.experimental_rerun()
